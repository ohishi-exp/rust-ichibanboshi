//! 給与大臣 (OHKEN) SQL Server の読み取り専用 DB 層 (Refs #82)。
//!
//! CAPE#01 用の `crate::repo` とは接続先が別 (給与大臣 PC の OHKEN インスタンス、
//! TCP 固定ポート) のため pool も trait も分離する。SELECT のみ・固定パラメータ
//! バインド (#80 vehicle-daily と同じ injection 対策方針)。
//!
//! SQL Server 2008 R2 は新しい TLS を話せないため `EncryptionLevel::NotSupported`
//! を明示する (LAN 内 + FW 送信元制限済みの前提、docs/kyuyo-daijin-schema.md)。

use std::sync::Arc;

use async_trait::async_trait;
use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use tiberius::{Config as TiberiusConfig, EncryptionLevel};

use super::logic::{
    normalize_company_code, RawKyuyoRow, RawShukeiRow, MAX_MONTH_INDEX, MONEY_COLUMNS,
};
use crate::config::KyuyoConfig;

pub type KyuyoDbPool = Pool<ConnectionManager>;

/// 給与 DB 層エラー。
#[derive(Debug)]
pub enum KyuyoRepoError {
    /// `[kyuyo]` 設定が無く機能が無効。
    NotConfigured,
    /// pool から接続が取れない (給与大臣 PC 停止等)。
    PoolError(String),
    /// クエリ実行エラー。
    QueryError(String),
}

impl std::fmt::Display for KyuyoRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(f, "kyuyo database is not configured"),
            Self::PoolError(m) => write!(f, "kyuyo pool error: {m}"),
            Self::QueryError(m) => write!(f, "kyuyo query error: {m}"),
        }
    }
}

/// 給与 DB 読み取り trait (テストで mock 差し替え)。
///
/// `db` 引数は呼び出し側 (handler) が [`super::logic::kydata_db_name`] で組み立てた
/// 検証済み DB 名。実装側でも英数字 + `_` のみを再検証する (defense in depth)。
#[async_trait]
pub trait KyuyoRepo: Send + Sync {
    /// `sys.databases` から KYDATA DB の (名前, HAS_DBACCESS) 一覧。
    async fn list_kydata_databases(&self) -> Result<Vec<(String, Option<i32>)>, KyuyoRepoError>;

    /// `KYCOMSTD.SELDATA` から会社コード → 会社名。
    async fn company_names(&self) -> Result<Vec<(String, String)>, KyuyoRepoError>;

    /// 指定 DB の `KYUYO` を賃金期間開始の半開区間 [from, to) で取得する。
    async fn payroll_month(
        &self,
        db: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<RawKyuyoRow>, KyuyoRepoError>;

    /// 指定 DB の `KOUMOKU` (TAIKEIKOUNO, NAME) 一覧。
    async fn koumoku(&self, db: &str) -> Result<Vec<(String, String)>, KyuyoRepoError>;

    /// 指定 DB の `SHUKEI1` から支給回インデックス `month_index` の計算済み集計。
    async fn shukei_totals(
        &self,
        db: &str,
        month_index: i32,
    ) -> Result<Vec<RawShukeiRow>, KyuyoRepoError>;
}

pub type DynKyuyoRepo = Arc<dyn KyuyoRepo>;

/// `[kyuyo]` 未設定時に Extension へ入れる stub。全メソッドが `NotConfigured`。
pub struct NotConfiguredKyuyoRepo;

#[async_trait]
impl KyuyoRepo for NotConfiguredKyuyoRepo {
    async fn list_kydata_databases(&self) -> Result<Vec<(String, Option<i32>)>, KyuyoRepoError> {
        Err(KyuyoRepoError::NotConfigured)
    }
    async fn company_names(&self) -> Result<Vec<(String, String)>, KyuyoRepoError> {
        Err(KyuyoRepoError::NotConfigured)
    }
    async fn payroll_month(
        &self,
        _db: &str,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<RawKyuyoRow>, KyuyoRepoError> {
        Err(KyuyoRepoError::NotConfigured)
    }
    async fn koumoku(&self, _db: &str) -> Result<Vec<(String, String)>, KyuyoRepoError> {
        Err(KyuyoRepoError::NotConfigured)
    }
    async fn shukei_totals(
        &self,
        _db: &str,
        _month_index: i32,
    ) -> Result<Vec<RawShukeiRow>, KyuyoRepoError> {
        Err(KyuyoRepoError::NotConfigured)
    }
}

/// OHKEN への接続プールを作る。
///
/// CAPE#01 用 `db::create_pool` と違い**起動時の接続テストはしない** — 給与大臣 PC
/// が停止していても本サービス全体の起動は妨げず、給与ルートだけ 503 にする。
pub async fn create_kyuyo_pool(
    config: &KyuyoConfig,
) -> Result<KyuyoDbPool, Box<dyn std::error::Error>> {
    let mut tib_config = TiberiusConfig::new();
    tib_config.host(&config.host);
    tib_config.port(config.port);
    // DB は接続後にクエリ側で [KYDATA...].dbo.* と完全修飾するため master 固定
    tib_config.database("master");
    tib_config.authentication(tiberius::AuthMethod::sql_server(
        &config.user,
        &config.password,
    ));
    // SQL Server 2008 R2 の TLS 1.0 問題を暗号化無効で回避 (LAN 内 + FW 制限済み)
    tib_config.encryption(EncryptionLevel::NotSupported);

    let manager = ConnectionManager::new(tib_config);
    let pool = Pool::builder()
        .max_size(2)
        .connection_timeout(std::time::Duration::from_secs(15))
        .build_unchecked(manager);

    tracing::info!(
        "kyuyo SQL Server pool created: {}:{}",
        config.host,
        config.port
    );
    Ok(pool)
}

/// tiberius 実装。
pub struct TiberiusKyuyoRepo {
    pool: KyuyoDbPool,
}

impl TiberiusKyuyoRepo {
    pub fn new(pool: KyuyoDbPool) -> Self {
        Self { pool }
    }
}

/// DB 名の再検証 (defense in depth)。`KYDATA0100_126C` / `KYCOMSTD` 形式のみ許可。
fn validate_db_name(db: &str) -> Result<(), KyuyoRepoError> {
    if db.is_empty() || db.len() > 64 || !db.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(KyuyoRepoError::QueryError(format!(
            "invalid database name: {db}"
        )));
    }
    Ok(())
}

fn get_str(row: &tiberius::Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn get_i32(row: &tiberius::Row, idx: usize) -> i32 {
    row.try_get::<i32, _>(idx).ok().flatten().unwrap_or(0)
}

#[async_trait]
impl KyuyoRepo for TiberiusKyuyoRepo {
    async fn list_kydata_databases(&self) -> Result<Vec<(String, Option<i32>)>, KyuyoRepoError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KyuyoRepoError::PoolError(e.to_string()))?;
        // HAS_DBACCESS: 1=可 / 0=不可 / NULL=DB名不正等。restore で作られた DB の
        // 権限抜け (model 継承が効かない) をここで検知する
        let stream = conn
            .simple_query(
                "SELECT name, HAS_DBACCESS(name) FROM sys.databases \
                 WHERE name LIKE 'KYDATA%' ORDER BY name",
            )
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| (get_str(r, 0), r.try_get::<i32, _>(1).ok().flatten()))
            .collect())
    }

    async fn company_names(&self) -> Result<Vec<(String, String)>, KyuyoRepoError> {
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KyuyoRepoError::PoolError(e.to_string()))?;
        // KCODE の実列型 (数値/固定長文字列等) によらず &str で取れるよう CAST する。
        // 素の KCODE を get_str (try_get::<&str>) に渡すと型不一致で Err → 空文字化し、
        // 全社が "" キーに衝突して名前が消える (#86)
        let stream = conn
            .simple_query("SELECT CAST(KCODE AS varchar(10)), CONAME1 FROM [KYCOMSTD].dbo.SELDATA")
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| (normalize_company_code(&get_str(r, 0)), get_str(r, 1)))
            .collect())
    }

    async fn payroll_month(
        &self,
        db: &str,
        from: &str,
        to: &str,
    ) -> Result<Vec<RawKyuyoRow>, KyuyoRepoError> {
        validate_db_name(db)?;
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KyuyoRepoError::PoolError(e.to_string()))?;

        // MONEY00..79 の 80 列は列名を code 側で生成 (T-SQL に動的列は無い)。
        // 全カラム NOT NULL (Btrieve 由来スキーマ) だが念のため ISNULL する。
        let money_select: String = (0..MONEY_COLUMNS)
            .map(|n| format!("CAST(ISNULL(k.MONEY{n:02}, 0) AS int)"))
            .collect::<Vec<_>>()
            .join(", ");

        // 日付は CONVERT(varchar(10), _, 120) = "YYYY-MM-DD" (2008 互換) に寄せて
        // tiberius の型差 (datetime/smalldatetime) を吸収する。月の特定は固定式でなく
        // CHINGINKIKANST の範囲照合 (#83 レビュー結論: 月内複数支給・欠月に強い)
        let query = format!(
            "SELECT CAST(k.SHAIN AS int), CAST(k.[MONTH] AS int), \
             CONVERT(varchar(10), k.SHIKYUBI, 120), \
             CONVERT(varchar(10), k.CHINGINKIKANST, 120), \
             CONVERT(varchar(10), k.CHINGINKIKANEN, 120), \
             s1.CODE, s1.NAME, CAST(ISNULL(s1.TAIKYU, 0) AS int), \
             ISNULL(sz.SNAME, ''), CAST(ISNULL(sz.TAIKEI, 0) AS int), \
             {money_select} \
             FROM [{db}].dbo.KYUYO k \
             JOIN [{db}].dbo.SHAIN1 s1 ON s1.INCODE = k.SHAIN \
             LEFT JOIN [{db}].dbo.SHOZOKU sz ON sz.INCODE = k.SHOZOKU \
             WHERE k.CHINGINKIKANST >= @P1 AND k.CHINGINKIKANST < @P2 \
             ORDER BY k.SHAIN, k.[MONTH]"
        );

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;

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
            })
            .collect())
    }

    async fn koumoku(&self, db: &str) -> Result<Vec<(String, String)>, KyuyoRepoError> {
        validate_db_name(db)?;
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KyuyoRepoError::PoolError(e.to_string()))?;
        let query = format!("SELECT TAIKEIKOUNO, NAME FROM [{db}].dbo.KOUMOKU");
        let stream = conn
            .simple_query(&query)
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|r| (get_str(r, 0), get_str(r, 1)))
            .collect())
    }

    async fn shukei_totals(
        &self,
        db: &str,
        month_index: i32,
    ) -> Result<Vec<RawShukeiRow>, KyuyoRepoError> {
        validate_db_name(db)?;
        if !(0..=MAX_MONTH_INDEX).contains(&month_index) {
            return Err(KyuyoRepoError::QueryError(format!(
                "invalid month index: {month_index}"
            )));
        }
        let mut conn = self
            .pool
            .get()
            .await
            .map_err(|e| KyuyoRepoError::PoolError(e.to_string()))?;

        // SHUKEI1 は支給回インデックスが列名に埋まっている (SOSHIKYU00..21 等) ため、
        // 検証済み index から列名を組み立てる
        let nn = format!("{month_index:02}");
        let query = format!(
            "SELECT CAST(SHAIN AS int), \
             CAST(ISNULL(SOSHIKYU{nn}, 0) AS int), CAST(ISNULL(KAZEI{nn}, 0) AS int), \
             CAST(ISNULL(HOKEN{nn}, 0) AS int), CAST(ISNULL(ZEI{nn}, 0) AS int), \
             CAST(ISNULL(SHOKOUJO{nn}, 0) AS int) \
             FROM [{db}].dbo.SHUKEI1"
        );
        let stream = conn
            .simple_query(&query)
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| KyuyoRepoError::QueryError(e.to_string()))?;
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
