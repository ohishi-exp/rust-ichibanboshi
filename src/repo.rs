use async_trait::async_trait;
use std::sync::Arc;

use crate::routes::uriage::UriageRow;
use uriage_logic::sqlserver::{uriage_row, uriage_rows_sql, Cells};

/// DB 操作の抽象化。本番は TiberiusRepo、テストは MockRepo を使う。
#[async_trait]
pub trait AppRepo: Send + Sync {
    // ── health ──
    async fn health_check(&self) -> Result<(), RepoError>;

    // ── uriage (担当者別売上、#762) ──
    /// `[運転日報明細]` から `compute_person_sum` の入力 1 行を取得する。
    /// PHP `UriageJyuchuDisplayController::make_arrays()` の **5 ケース UNION** を
    /// 1:1 で再現する (傭車 / 営業所傭車 / 傭車傭車 / sql_from_other_with_bumon /
    /// sql_from_other)。`bumon_codes` は受注/稼動部門の IN 条件、`persons_id_list`
    /// は `UriageJyuchuDisplayPersons_id` 相当 (営業所配下の担当者社員C 一覧)。
    /// `sql_options` 系は `入力担当C IN persons` で絞り、`sql_from_other_with_bumon`
    /// は逆に `入力担当C NOT IN persons` で絞る (PHP L1759)。
    async fn uriage_rows(
        &self,
        from: &str,
        to: &str,
        bumon_codes: &[String],
        persons_id_list: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError>;
}

pub type DynRepo = Arc<dyn AppRepo>;

#[derive(Debug)]
pub enum RepoError {
    PoolError,
    QueryError(String),
}

// ── TiberiusRepo: 本番用実装 ──

use crate::db::{DbConn, DbPool};

pub struct TiberiusRepo {
    /// `None` = この instance は SQL Server (CAPE#01) を使うと宣言していない
    /// (`[database] enabled = false`)。Cloud Run のように SQL Server へ到達できない
    /// 実行形態で使う。全メソッドが `RepoError::PoolError` を返し、routes は既存の
    /// マッピングどおり **503 fail-closed** になる (空の結果を返して「0 件」に
    /// 見せることは無い)。
    pool: Option<DbPool>,
}

impl TiberiusRepo {
    pub fn new(pool: DbPool) -> Self {
        Self { pool: Some(pool) }
    }

    /// SQL Server を使わないと宣言した実行形態向けの repo。
    ///
    /// 「繋ぎに行って失敗する」のではなく「そもそも宣言していない」ことを型で
    /// 表すのが要点 — 接続待ちの timeout を各リクエストで払わずに即 503 を返す。
    pub fn disabled() -> Self {
        Self { pool: None }
    }

    async fn conn(&self) -> Result<DbConn<'_>, RepoError> {
        let pool = self.pool.as_ref().ok_or(RepoError::PoolError)?;
        pool.get().await.map_err(|_| RepoError::PoolError)
    }
}

fn decode_cp932(row: &tiberius::Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn get_i64(row: &tiberius::Row, idx: usize) -> i64 {
    row.try_get::<f64, _>(idx)
        .ok()
        .flatten()
        .map(|v| v as i64)
        .or_else(|| {
            row.try_get::<tiberius::numeric::Numeric, _>(idx)
                .ok()
                .flatten()
                .and_then(|d| {
                    let s = format!("{}", d);
                    s.parse::<f64>().ok()
                })
                .map(|v| v as i64)
        })
        .or_else(|| row.try_get::<i32, _>(idx).ok().flatten().map(|v| v as i64))
        .unwrap_or(0)
}

fn get_i32(row: &tiberius::Row, idx: usize) -> i32 {
    row.try_get::<i32, _>(idx).ok().flatten().unwrap_or(0)
}

/// `/health` の生死確認。Worker 側 (`workers/ichiban/logic/src/sql.rs`) と同じ文字列。
const HEALTH_SQL: &str = "SELECT 1";

#[async_trait]
impl AppRepo for TiberiusRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        let mut conn = self.conn().await?;
        conn.simple_query(HEALTH_SQL)
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(())
    }

    async fn uriage_rows(
        &self,
        from: &str,
        to: &str,
        bumon_codes: &[String],
        persons_id_list: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        if bumon_codes.is_empty() {
            return Ok(vec![]);
        }
        let mut conn = self.conn().await?;

        // SQL の文字列 (英数字の whitelist・NULL の埋め方・5 ケースの UNION ALL・ORDER BY) は売上 Worker と共有
        // (`uriage_logic::sqlserver::uriage_rows_sql`)。部門が英数字に絞って空なら None = 0 行。
        // 空の判定は接続の後 (pool が死んでいれば先に 503 になる、従来の順序)
        let Some(query) = uriage_rows_sql(bumon_codes, persons_id_list) else {
            return Ok(vec![]);
        };

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_uriage(&rows))
    }
}

// ── Row → Raw 変換ヘルパー ──
// 列の並びと UriageRow への詰め方は売上 Worker と共有 (`uriage_logic::sqlserver::uriage_row`)。
// ここは tiberius の Row から値を読む方法 (cp932 の trim・数値型の揺れ) だけを持つ。
struct TdsRow<'a>(&'a tiberius::Row);

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

impl TiberiusRepo {
    fn rows_to_uriage(rows: &[tiberius::Row]) -> Vec<UriageRow> {
        rows.iter().map(|r| uriage_row(&TdsRow(r))).collect()
    }
}
