//! 一番星 SQL Server (CAPE#01) への接続と `[運転日報明細]` の行の読み取り (Refs ohishi-exp/rust-ichibanboshi#322)。
//! 接続は共有 crate `vpc-tds` (workers/shared)。ここは繋ぎ先 (binding・database・secret) と行の読み方だけ。
//!
//! 行の読み方 (`decode_cp932`・`get_i64`・`get_i32`) は workers/ichiban/worker/src/rows/mod.rs と
//! オンプレ版 (root の `src/repo.rs`) の写し — 型が合わなければ 0 / 空文字。SQL と列の並びは uriage-logic の `sqlserver`。

use std::time::Duration;

use tiberius::numeric::Numeric;
use tiberius::Row;
use uriage_logic::compute::UriageRow;
use uriage_logic::routes::sql_kind;
use uriage_logic::sqlserver::{uriage_row, Cells};
use vpc_tds::{kind_of, timeout, Conn, DbError, ErrKind, Stage, Target};
use worker::Env;

/// 本番は Workers VPC の binding `URIAGE_SQL_VPC` (一番星 Worker の `ICHIBAN_VPC` と同じ VPC Service)、
/// ローカル検証は var `LOCAL_SQL_ADDR` / `LOCAL_URIAGE_SQL_JSON` (本番の vars には置かない。scripts/check-exposure.sh が検査する)。
pub(crate) const TARGET: Target = Target {
    binding: "URIAGE_SQL_VPC",
    nominal_addr: "uriage-sql:1433",
    local_addr_var: "LOCAL_SQL_ADDR",
    database: "CAPE#01",
    secret: "ICHIBAN_SQL",
    local_creds_var: "LOCAL_URIAGE_SQL_JSON",
};

/// 1 job (1 本) の上限。
const QUERY_TIMEOUT: Duration = Duration::from_secs(60);
/// `/probe` の `SELECT 1` の上限。
const PING_TIMEOUT: Duration = Duration::from_secs(10);

/// 失敗の `kind` (`<段>:<種類>`)。
pub(crate) fn label(e: &DbError) -> String {
    sql_kind(e.stage.as_str(), &e.kind.label())
}

/// 資格情報を読み、TCP を開いてログインする (database `CAPE#01`、平文 TDS)。
pub(crate) async fn connect(env: &Env) -> Result<Conn, DbError> {
    vpc_tds::connect(env, &TARGET).await
}

fn query_failed(kind: ErrKind) -> DbError {
    DbError {
        stage: Stage::Query,
        kind,
    }
}

/// `uriage_rows_sql` の SQL を `@P1` = from・`@P2` = to で流し、[`UriageRow`] にする (オンプレ版の `uriage_rows` と同じ)。
pub(crate) async fn uriage_rows(
    conn: &mut Conn,
    sql: &str,
    from: &str,
    to: &str,
) -> Result<Vec<UriageRow>, DbError> {
    let rows = timeout(QUERY_TIMEOUT, async {
        let stream = conn
            .query(sql, &[&from, &to])
            .await
            .map_err(|e| kind_of(&e))?;
        stream.into_first_result().await.map_err(|e| kind_of(&e))
    })
    .await
    .ok_or(query_failed(ErrKind::Timeout))?
    .map_err(query_failed)?;
    Ok(rows.iter().map(|r| uriage_row(&TdsRow(r))).collect())
}

/// `/probe`: ログインして `SELECT 1` が 1 を返すか。
pub(crate) async fn ping(env: &Env) -> Result<(), DbError> {
    let mut conn = connect(env).await?;
    let result = timeout(PING_TIMEOUT, async {
        let stream = conn
            .simple_query("SELECT 1")
            .await
            .map_err(|e| kind_of(&e))?;
        stream.into_first_result().await.map_err(|e| kind_of(&e))
    })
    .await;
    let _ = conn.close().await;
    let rows = result
        .ok_or(query_failed(ErrKind::Timeout))?
        .map_err(query_failed)?;
    match rows
        .first()
        .and_then(|r| r.try_get::<i32, _>(0).ok().flatten())
    {
        Some(1) => Ok(()),
        _ => Err(query_failed(ErrKind::Other)),
    }
}

struct TdsRow<'a>(&'a Row);

impl Cells for TdsRow<'_> {
    fn text(&self, idx: usize) -> String {
        decode_cp932(self.0, idx)
    }
    fn int(&self, idx: usize) -> i32 {
        get_i32(self.0, idx)
    }
    fn money(&self, idx: usize) -> i64 {
        get_i64(self.0, idx)
    }
}

/// 文字列の列 (varchar の CP932 は tiberius が読む)。NULL・型不一致は空文字、前後の空白は落とす。
fn decode_cp932(row: &Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// 金額の列。f64 → decimal → i32 の順で試し、小数は切り捨てる。読めなければ 0。
fn get_i64(row: &Row, idx: usize) -> i64 {
    row.try_get::<f64, _>(idx)
        .ok()
        .flatten()
        .map(|v| v as i64)
        .or_else(|| {
            row.try_get::<Numeric, _>(idx)
                .ok()
                .flatten()
                .and_then(|d| format!("{d}").parse::<f64>().ok())
                .map(|v| v as i64)
        })
        .or_else(|| row.try_get::<i32, _>(idx).ok().flatten().map(|v| v as i64))
        .unwrap_or(0)
}

/// int の列。i32 で読めなければ 0。
fn get_i32(row: &Row, idx: usize) -> i32 {
    row.try_get::<i32, _>(idx).ok().flatten().unwrap_or(0)
}
