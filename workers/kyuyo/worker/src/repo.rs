//! 給与大臣 SQL Server の読み取り (Refs #322)。DO `KyuyoState` のロックの中からだけ呼ぶ。
//!
//! オンプレ版 (repo ルートの `src/kyuyo/repo.rs`) と同じ SQL 文 (`kyuyo_logic::sql`) を流し、同じ詰め方で
//! `kyuyo_logic::payroll::Raw*Row` にする: 文字列は `try_get::<&str>` を trim、数値は `try_get::<i32>`。
//! **型が合わなければ空文字 / 0** (オンプレ版の `get_str` / `get_i32` と同じ。CAST は SQL 側にある)。
//!
//! 1 リクエスト = 1 接続 ([`open`] で繋ぎ、終わったら [`Db::close`])。bb8 は wasm32 で使えないので pool は持たない。
//! 失敗は `DbError` (stage と種類だけ) で返し、エラーの本文・ホスト・ユーザー名はどこにも出さない。

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use kyuyo_logic::payroll::{
    normalize_company_code, RawEmployeeRow, RawKoumokuRow, RawKyuyoRow, RawShukeiRow,
    KINDATA_COLUMNS, MONEY_COLUMNS,
};
use kyuyo_logic::service::DbError;
use kyuyo_logic::sql::{
    employees_sql, koumoku_sql, payroll_month_sql, shukei_totals_sql, COMPANY_NAMES_SQL,
    DATABASES_WITH_ACCESS_SQL, DATABASE_NAMES_SQL,
};
use kyuyo_logic::{parse_creds, Creds, ErrKind, Stage};
use tiberius::error::Error as TdsError;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel, Row};
use tokio_util::compat::Compat;
use worker::{Delay, Env, Socket};

use crate::{text, transport};

/// TCP を開いてからログインが終わるまでの上限。応答しない相手で fetch を握り続けない。
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// 1 本のクエリの上限。`companies` の HAS_DBACCESS は AUTO_CLOSE の全 DB を開いて回るので 10 秒級になる。
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

pub(crate) type Conn = Client<Compat<Socket>>;

/// tiberius のエラーを失敗の種類に写す。`message` や表示文言は読まない (種類・番号・状態だけ)。
pub(crate) fn kind_of(e: &TdsError) -> ErrKind {
    match e {
        TdsError::Io { kind, .. } => ErrKind::Io(format!("{kind:?}")),
        TdsError::Server(t) => ErrKind::Server {
            code: t.code(),
            class: t.class(),
            state: t.state(),
        },
        TdsError::Protocol(_) => ErrKind::Protocol,
        TdsError::Encoding(_) => ErrKind::Encoding,
        TdsError::Tls(_) => ErrKind::Tls,
        TdsError::Routing { .. } => ErrKind::Routing,
        _ => ErrKind::Other,
    }
}

fn fail(stage: Stage, kind: ErrKind) -> DbError {
    DbError { stage, kind }
}

/// 資格情報を読み、TCP を開いてログインする (database `master`、平文 TDS)。`/probe` もこれを通る。
pub(crate) async fn connect(env: &Env) -> Result<Conn, DbError> {
    let creds = load_creds(env)
        .await
        .map_err(|stage| fail(stage, ErrKind::Other))?;

    let mut config = Config::new();
    config.authentication(AuthMethod::sql_server(&creds.user, &creds.pass));
    // 社内 LAN 区間の平文 TDS (SQL Server 2008 R2 は新しい TLS を話せない。オンプレ版と同じ)
    config.encryption(EncryptionLevel::NotSupported);
    // DB は SQL 側で [KYDATA…].dbo.* と完全修飾するので master 固定
    config.database("master");

    timeout(CONNECT_TIMEOUT, async {
        let stream = transport::open(env)
            .await
            .map_err(|_| fail(Stage::Connect, ErrKind::Transport))?;
        Client::connect(config, stream)
            .await
            .map_err(|e| fail(Stage::Login, kind_of(&e)))
    })
    .await
    .ok_or(fail(Stage::Connect, ErrKind::Timeout))?
}

/// 資格情報を読んで検証する (`kyuyo_logic::parse_creds`)。読めない・キー欠け・空は `Stage::Secret` (中身はどこにも出さない)。
async fn load_creds(env: &Env) -> Result<Creds, Stage> {
    // ローカル検証 (wrangler dev) だけ: Secrets Store が使えないので var LOCAL_KYUYO_SQL_JSON で代える。
    // 本番の vars には置かない (scripts/check-exposure.sh が検査する)
    let json = match text(env, "LOCAL_KYUYO_SQL_JSON") {
        Some(json) => json,
        None => env
            .secret_store("KYUYO_SQL")
            .map_err(|_| Stage::Secret)?
            .get()
            .await
            .map_err(|_| Stage::Secret)?
            .ok_or(Stage::Secret)?,
    };
    parse_creds(&json)
}

/// `fut` を `limit` で打ち切る。時間切れは `None`。
pub(crate) async fn timeout<T>(limit: Duration, fut: impl Future<Output = T>) -> Option<T> {
    match select(pin!(fut), pin!(Delay::from(limit))).await {
        Either::Left((v, _)) => Some(v),
        Either::Right(_) => None,
    }
}

fn get_str(row: &Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn get_i32(row: &Row, idx: usize) -> i32 {
    row.try_get::<i32, _>(idx).ok().flatten().unwrap_or(0)
}

/// 1 リクエストぶんの接続。
pub(crate) struct Db {
    conn: Conn,
}

/// [`connect`] して [`Db`] にする。
pub(crate) async fn open(env: &Env) -> Result<Db, DbError> {
    Ok(Db {
        conn: connect(env).await?,
    })
}

impl Db {
    /// 接続を閉じる (失敗は無視。どの経路でも最後に呼ぶ)。
    pub(crate) async fn close(self) {
        let _ = self.conn.close().await;
    }

    /// パラメータ無しの SQL (`simple_query`) の最初の結果セット。
    async fn simple(&mut self, sql: &str) -> Result<Vec<Row>, DbError> {
        let conn = &mut self.conn;
        let out = timeout(QUERY_TIMEOUT, async {
            conn.simple_query(sql)
                .await
                .map_err(|e| kind_of(&e))?
                .into_first_result()
                .await
                .map_err(|e| kind_of(&e))
        })
        .await;
        match out {
            None => Err(fail(Stage::Query, ErrKind::Timeout)),
            Some(r) => r.map_err(|kind| fail(Stage::Query, kind)),
        }
    }

    /// 賃金期間の半開区間 [@P1, @P2) を渡す SQL (`query`) の最初の結果セット。
    async fn with_period(&mut self, sql: &str, from: &str, to: &str) -> Result<Vec<Row>, DbError> {
        let conn = &mut self.conn;
        let out = timeout(QUERY_TIMEOUT, async {
            conn.query(sql, &[&from, &to])
                .await
                .map_err(|e| kind_of(&e))?
                .into_first_result()
                .await
                .map_err(|e| kind_of(&e))
        })
        .await;
        match out {
            None => Err(fail(Stage::Query, ErrKind::Timeout)),
            Some(r) => r.map_err(|kind| fail(Stage::Query, kind)),
        }
    }

    /// KYDATA DB 名だけの一覧 (メタデータのみ、ミリ秒)。
    pub(crate) async fn database_names(&mut self) -> Result<Vec<String>, DbError> {
        let rows = self.simple(DATABASE_NAMES_SQL).await?;
        Ok(rows.iter().map(|r| get_str(r, 0)).collect())
    }

    /// KYDATA DB の (名前, HAS_DBACCESS)。遅い (〜10 秒)。
    pub(crate) async fn databases_with_access(
        &mut self,
    ) -> Result<Vec<(String, Option<i32>)>, DbError> {
        let rows = self.simple(DATABASES_WITH_ACCESS_SQL).await?;
        Ok(rows
            .iter()
            .map(|r| (get_str(r, 0), r.try_get::<i32, _>(1).ok().flatten()))
            .collect())
    }

    /// 会社コード → 会社名 (`KYCOMSTD.SELDATA`)。
    pub(crate) async fn company_names(&mut self) -> Result<Vec<(String, String)>, DbError> {
        let rows = self.simple(COMPANY_NAMES_SQL).await?;
        Ok(rows
            .iter()
            .map(|r| (normalize_company_code(&get_str(r, 0)), get_str(r, 1)))
            .collect())
    }

    /// 指定 DB の `KYUYO` を賃金期間開始の半開区間 [from, to) で。
    pub(crate) async fn payroll_month(
        &mut self,
        db: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<RawKyuyoRow>, DbError> {
        let sql = payroll_month_sql(db).map_err(|_| fail(Stage::Query, ErrKind::Other))?;
        let rows = self.with_period(&sql, from, to).await?;
        Ok(rows
            .iter()
            .map(|r| RawKyuyoRow {
                shain: get_i32(r, 0),
                month_index: get_i32(r, 1),
                pay_date: get_str(r, 2),
                period_start: get_str(r, 3),
                period_end: get_str(r, 4),
                employee_code: get_str(r, 5),
                employee_name: get_str(r, 6),
                taikyu: get_i32(r, 7),
                department: get_str(r, 8),
                taikei: get_i32(r, 9),
                money: (0..MONEY_COLUMNS)
                    .map(|n| get_i32(r, 10 + n) as i64)
                    .collect(),
                kindata: (0..KINDATA_COLUMNS)
                    .map(|n| get_i32(r, 10 + MONEY_COLUMNS + n) as i64)
                    .collect(),
            })
            .collect())
    }

    /// 指定 DB の社員マスタ。
    pub(crate) async fn employees(&mut self, db: &str) -> Result<Vec<RawEmployeeRow>, DbError> {
        let sql = employees_sql(db).map_err(|_| fail(Stage::Query, ErrKind::Other))?;
        let rows = self.simple(&sql).await?;
        Ok(rows
            .iter()
            .map(|r| RawEmployeeRow {
                employee_code: get_str(r, 0),
                employee_name: get_str(r, 1),
                taikyu: get_i32(r, 2),
                department: get_str(r, 3),
                taikei: get_i32(r, 4),
                department_code: get_i32(r, 5),
                branch_name: get_str(r, 6),
                job_name: get_str(r, 7),
                kkubun: get_i32(r, 8),
                hire_date: get_str(r, 9),
                retire_date: get_str(r, 10),
                taikbn: get_i32(r, 11),
            })
            .collect())
    }

    /// 指定 DB の項目マスタ。
    pub(crate) async fn koumoku(&mut self, db: &str) -> Result<Vec<RawKoumokuRow>, DbError> {
        let sql = koumoku_sql(db).map_err(|_| fail(Stage::Query, ErrKind::Other))?;
        let rows = self.simple(&sql).await?;
        Ok(rows
            .iter()
            .map(|r| RawKoumokuRow {
                taikeikouno: get_str(r, 0),
                name: get_str(r, 1),
                kazei: get_i32(r, 2),
                meisai: get_i32(r, 3),
                gengaku: get_i32(r, 4),
            })
            .collect())
    }

    /// 指定 DB の `SHUKEI1` から支給回 `month_index` の集計。
    pub(crate) async fn shukei_totals(
        &mut self,
        db: &str,
        month_index: i32,
    ) -> Result<Vec<RawShukeiRow>, DbError> {
        let sql =
            shukei_totals_sql(db, month_index).map_err(|_| fail(Stage::Query, ErrKind::Other))?;
        let rows = self.simple(&sql).await?;
        Ok(rows
            .iter()
            .map(|r| RawShukeiRow {
                shain: get_i32(r, 0),
                month_index,
                soshikyu: get_i32(r, 1) as i64,
                kazei: get_i32(r, 2) as i64,
                hoken: get_i32(r, 3) as i64,
                zei: get_i32(r, 4) as i64,
                shokoujo: get_i32(r, 5) as i64,
            })
            .collect())
    }
}
