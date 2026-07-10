use async_trait::async_trait;
use std::sync::Arc;

use crate::routes::sales::*;
use crate::routes::schema::{ColumnInfo, SampleRow, TableInfo};
use crate::routes::surcharge::RawSurchargeRow;
use crate::routes::unchin::{
    RawUnchinCustomerNetDetailRow, RawUnchinCustomerNetRow, RawUnchinRow,
    RawUnchinSubcontractorNetDetailRow, RawUnchinSubcontractorNetRow, RawUnchinSummaryRow,
};
use crate::routes::uriage::UriageRow;

/// DB 操作の抽象化。本番は TiberiusRepo、テストは MockRepo を使う。
#[async_trait]
pub trait AppRepo: Send + Sync {
    // ── health ──
    async fn health_check(&self) -> Result<(), RepoError>;

    // ── schema ──
    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError>;
    async fn list_columns(&self, table: &str) -> Result<Vec<ColumnInfo>, RepoError>;
    async fn sample_data(&self, table: &str, limit: i32) -> Result<SampleRow, RepoError>;

    // ── sales ──
    async fn monthly(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
        exclude_dept: Option<&str>,
        include_dept: Option<&str>,
    ) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError>;

    async fn by_department(&self, from: &str, to: &str)
        -> Result<Vec<RawDepartmentRow>, RepoError>;

    async fn by_customer(
        &self,
        from: &str,
        to: &str,
        limit: i32,
    ) -> Result<Vec<RawCustomerRow>, RepoError>;

    async fn customer_yoy_data(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
    ) -> Result<(CodeTotalMap, CodeTotalMap), RepoError>;

    async fn yoy_data(
        &self,
        year: i32,
    ) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError>;

    async fn daily(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
        billing_filter: &str,
        dept_filter: &str,
        exclude_pattern: &str,
    ) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError>;

    async fn customer_trend_data(
        &self,
        from: &str,
        to: &str,
        limit: i32,
    ) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError>;

    async fn customer_detail_data(
        &self,
        code: &str,
    ) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError>;

    async fn customer_yoy_by_dept_data(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
        department_code: Option<&str>,
    ) -> Result<(Vec<RawCustomerDeptRow>, Vec<RawCustomerDeptRow>), RepoError>;

    async fn list_departments(&self) -> Result<Vec<(String, String)>, RepoError>;

    /// 車種ﾏｽﾀ (車種C, 車種N) の一覧。燃費マスタ (車種C キー) の編集 UI 用 (#12)。
    async fn vehicles(&self) -> Result<Vec<(String, String)>, RepoError>;

    // ── surcharge (燃料サーチャージ基礎データ、#12) ──
    async fn surcharge_base(
        &self,
        from: &str,
        to: &str,
        kind_filter: &str,
        limit: i32,
    ) -> Result<Vec<RawSurchargeRow>, RepoError>;

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

    // ── unchin (得意先・傭車先別運賃リスト、#57) ──
    /// `運転日報明細` から得意先 or 傭車先別の運賃候補行を取得する (raw 行、TOP 上限なし)。
    /// `partner_type`: `"customer"` (得意先) | `"subcontractor"` (傭車先)。
    /// `kind_filter`: `unchin_kind_filter()` が返す SQL WHERE フラグメント (`請求K` 絞り込み)。
    async fn unchin_candidates(
        &self,
        from: &str,
        to: &str,
        partner_type: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinRow>, RepoError>;

    /// `運転日報明細` から得意先 or 傭車先ごとの合計金額を `SUM`/`GROUP BY` で集計する。
    /// 結果は取引先数で決まるため raw 行 TOP-N 方式と違い行数による打ち切りが起きない。
    async fn unchin_summary(
        &self,
        from: &str,
        to: &str,
        partner_type: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinSummaryRow>, RepoError>;

    /// 傭車先ごとに、その傭車先が使われた運行の得意先請求合計 (`total_sales`) と
    /// その傭車先への支払合計 (`total_payment`) を同一行から同時集計する
    /// (2026-07-01 user 確認: 「同一運行内の両建て」— 名寄せではなく、
    /// 運転日報明細の同一行にある 得意先側金額 と 傭車先側金額 を突き合わせる)。
    /// `partner_type` は無い (常に傭車先 != '000000' の行のみ対象)。
    async fn unchin_subcontractor_net(
        &self,
        from: &str,
        to: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinSubcontractorNetRow>, RepoError>;

    /// `unchin_subcontractor_net` のドリルダウン。特定の傭車先 (`code`=傭車先C,
    /// `h`=傭車先H) について、運行 (行) 単位の得意先請求/傭車支払を返す。
    async fn unchin_subcontractor_net_detail(
        &self,
        from: &str,
        to: &str,
        code: &str,
        h: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinSubcontractorNetDetailRow>, RepoError>;

    /// 得意先ごとに、請求合計 (`total_sales`) と傭車を使った分の支払合計
    /// (`total_payment`) を同一行から同時集計する (`unchin_subcontractor_net`
    /// を得意先軸で見たもの、2026-07-01 user 確認)。`partner_type` は無い
    /// (常に得意先起点)。自社便 (傭車先C='000000') の行は常に除外する
    /// (2026-07-01 user 確認「トグルじゃない もとからなくして グラフも」)。
    async fn unchin_customer_net(
        &self,
        from: &str,
        to: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinCustomerNetRow>, RepoError>;

    /// `unchin_customer_net` のドリルダウン。特定の得意先 (`code`=得意先C,
    /// `h`=得意先H) について、運行 (行) 単位の請求/傭車支払を返す。
    async fn unchin_customer_net_detail(
        &self,
        from: &str,
        to: &str,
        code: &str,
        h: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinCustomerNetDetailRow>, RepoError>;
}

pub type DynRepo = Arc<dyn AppRepo>;

#[derive(Debug)]
pub enum RepoError {
    PoolError,
    QueryError(String),
}

// ── TiberiusRepo: 本番用実装 ──

use crate::db::DbPool;

pub struct TiberiusRepo {
    pool: DbPool,
}

impl TiberiusRepo {
    pub fn new(pool: DbPool) -> Self {
        Self { pool }
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

#[async_trait]
impl AppRepo for TiberiusRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        conn.simple_query("SELECT 1")
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let stream = conn
            .simple_query(
                "SELECT TABLE_SCHEMA, TABLE_NAME \
                 FROM INFORMATION_SCHEMA.TABLES \
                 WHERE TABLE_TYPE = 'BASE TABLE' \
                 ORDER BY TABLE_SCHEMA, TABLE_NAME",
            )
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| TableInfo {
                schema_name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                table_name: row.get::<&str, _>(1).unwrap_or("").to_string(),
            })
            .collect())
    }

    async fn list_columns(&self, table: &str) -> Result<Vec<ColumnInfo>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let stream = conn
            .query(
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME = @P1 \
                 ORDER BY ORDINAL_POSITION",
                &[&table],
            )
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| ColumnInfo {
                column_name: row.get::<&str, _>(0).unwrap_or("").to_string(),
                data_type: row.get::<&str, _>(1).unwrap_or("").to_string(),
                is_nullable: row.get::<&str, _>(2).unwrap_or("").to_string(),
                max_length: row.get::<i32, _>(3),
            })
            .collect())
    }

    async fn sample_data(&self, table: &str, limit: i32) -> Result<SampleRow, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let col_stream = conn
            .query(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME = @P1 ORDER BY ORDINAL_POSITION",
                &[&table],
            )
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let col_rows = col_stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let columns: Vec<String> = col_rows
            .iter()
            .map(|row| row.get::<&str, _>(0).unwrap_or("").to_string())
            .collect();

        let query = format!("SELECT TOP {} * FROM [{}]", limit, table);
        let data_stream = conn
            .simple_query(&query)
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let data_rows = data_stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows: Vec<Vec<Option<String>>> = data_rows
            .iter()
            .map(|row| {
                (0..columns.len())
                    .map(|i| {
                        row.try_get::<&str, _>(i)
                            .ok()
                            .flatten()
                            .map(|s| s.to_string())
                    })
                    .collect()
            })
            .collect();

        Ok(SampleRow { columns, rows })
    }

    async fn monthly(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
        exclude_dept: Option<&str>,
        include_dept: Option<&str>,
    ) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        if let Some(code) = include_dept {
            // 指定された営業所コードで絞り込み（部門別月計）
            let sql = "SELECT m.[年月度], \
                 SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
                 FROM [部門別月計] m \
                 WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
                   AND m.[部門C] = @P3 \
                 GROUP BY m.[年月度] \
                 ORDER BY m.[年月度]";

            let stream = conn
                .query(sql, &[&from, &to, &code])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let cur = stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;

            let prev_stream = conn
                .query(sql, &[&prev_from, &prev_to, &code])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let prev = prev_stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;

            Ok((
                format!("部門別月計 (部門C={})", code),
                Self::rows_to_monthly(&cur),
                Self::rows_to_monthly(&prev),
            ))
        } else if let Some(dept) = exclude_dept {
            let exclude_pattern = format!("%{}%", dept);
            let sql = "SELECT m.[年月度], \
                 SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
                 FROM [部門別月計] m \
                 LEFT JOIN [部門ﾏｽﾀ] d ON m.[部門C] = d.[部門C] \
                 WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
                   AND ISNULL(d.[部門N], '') NOT LIKE @P3 \
                 GROUP BY m.[年月度] \
                 ORDER BY m.[年月度]";

            let stream = conn
                .query(sql, &[&from, &to, &exclude_pattern.as_str()])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let cur = stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;

            let prev_stream = conn
                .query(sql, &[&prev_from, &prev_to, &exclude_pattern.as_str()])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let prev = prev_stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;

            Ok((
                format!("部門別月計 ({}除く)", dept),
                Self::rows_to_monthly(&cur),
                Self::rows_to_monthly(&prev),
            ))
        } else {
            let stream = conn
                .query(
                    "SELECT [年月度], [自車売上], [傭車売上], [輸送回数] \
                 FROM [種別別月計] \
                 WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
                 ORDER BY [年月度]",
                    &[&from, &to],
                )
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let cur = stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;

            let prev_stream = conn
                .query(
                    "SELECT [年月度], ISNULL([自車売上], 0), ISNULL([傭車売上], 0) \
                 FROM [種別別月計] \
                 WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
                 ORDER BY [年月度]",
                    &[&prev_from, &prev_to],
                )
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let prev = prev_stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;

            Ok((
                "種別別月計 (種別C=99)".to_string(),
                Self::rows_to_monthly(&cur),
                Self::rows_to_monthly_prev(&prev),
            ))
        }
    }

    async fn by_department(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<RawDepartmentRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let stream = conn.query(
            "SELECT m.[部門C], ISNULL(d.[部門N], ''), \
             SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
             FROM [部門別月計] m \
             LEFT JOIN [部門ﾏｽﾀ] d ON m.[部門C] = d.[部門C] \
             WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
             GROUP BY m.[部門C], d.[部門N] \
             ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC",
            &[&from, &to],
        ).await.map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| RawDepartmentRow {
                department_code: decode_cp932(row, 0),
                department_name: decode_cp932(row, 1),
                own_sales: get_i64(row, 2),
                charter_sales: get_i64(row, 3),
                transport_count: get_i64(row, 4),
            })
            .collect())
    }

    async fn by_customer(
        &self,
        from: &str,
        to: &str,
        limit: i32,
    ) -> Result<Vec<RawCustomerRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let query = format!(
            "SELECT TOP {} m.[得意先C], ISNULL(c.[得意先N], ''), \
             SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
             FROM [得意先別月計] m \
             LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
             WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
             GROUP BY m.[得意先C], c.[得意先N] \
             ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC",
            limit.min(100)
        );
        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| RawCustomerRow {
                customer_code: decode_cp932(row, 0),
                customer_name: decode_cp932(row, 1),
                own_sales: get_i64(row, 2),
                charter_sales: get_i64(row, 3),
                transport_count: get_i64(row, 4),
            })
            .collect())
    }

    async fn customer_yoy_data(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
    ) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let sql = "SELECT m.[得意先C], ISNULL(c.[得意先N], ''), \
                   SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) \
                   FROM [得意先別月計] m \
                   LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
                   WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
                   GROUP BY m.[得意先C], c.[得意先N]";

        let stream = conn
            .query(sql, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let cur_rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let prev_stream = conn
            .query(sql, &[&prev_from, &prev_to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let prev_rows = prev_stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok((
            Self::rows_to_code_total_map(&cur_rows),
            Self::rows_to_code_total_map(&prev_rows),
        ))
    }

    async fn yoy_data(
        &self,
        year: i32,
    ) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let current_from = format!("{}-01-01", year);
        let current_to = format!("{}-12-01", year);
        let prev_from = format!("{}-01-01", year - 1);
        let prev_to = format!("{}-12-01", year - 1);

        let sql = "SELECT MONTH([年月度]) as m, \
                   SUM(ISNULL([自車売上], 0)) + SUM(ISNULL([傭車売上], 0)) as total \
                   FROM [種別別月計] \
                   WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
                   GROUP BY MONTH([年月度]) \
                   ORDER BY MONTH([年月度])";

        let stream = conn
            .query(sql, &[&current_from.as_str(), &current_to.as_str()])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let cur = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let stream2 = conn
            .query(sql, &[&prev_from.as_str(), &prev_to.as_str()])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let prev = stream2
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok((
            cur.iter()
                .map(|r| RawMonthTotalRow {
                    month: get_i32(r, 0),
                    total: get_i64(r, 1),
                })
                .collect(),
            prev.iter()
                .map(|r| RawMonthTotalRow {
                    month: get_i32(r, 0),
                    total: get_i64(r, 1),
                })
                .collect(),
        ))
    }

    async fn daily(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
        billing_filter: &str,
        dept_filter: &str,
        exclude_pattern: &str,
    ) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        let query = format!(
            "SELECT [売上年月日], \
             SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0)), \
             SUM(ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), \
             SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0)), \
             SUM(ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0)), \
             COUNT(*) \
             FROM [運転日報明細] \
             WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 {} {} \
             GROUP BY [売上年月日] \
             ORDER BY [売上年月日]",
            billing_filter, dept_filter
        );
        let prev_query = format!(
            "SELECT [売上年月日], \
             SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0)), \
             SUM(ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), \
             SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0)), \
             SUM(ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0)) \
             FROM [運転日報明細] \
             WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 {} {} \
             GROUP BY [売上年月日] \
             ORDER BY [売上年月日]",
            billing_filter, dept_filter
        );

        let has_dept = !exclude_pattern.is_empty();
        let (cur_rows, prev_rows) = if has_dept {
            let s = conn
                .query(&query, &[&from, &to, &exclude_pattern])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let c = s
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let ps = conn
                .query(&prev_query, &[&prev_from, &prev_to, &exclude_pattern])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let p = ps
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            (c, p)
        } else {
            let s = conn
                .query(&query, &[&from, &to])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let c = s
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let ps = conn
                .query(&prev_query, &[&prev_from, &prev_to])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let p = ps
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            (c, p)
        };

        Ok((
            cur_rows
                .iter()
                .map(|r| RawDailyRow {
                    date: r.get(0).unwrap_or_default(),
                    own_sales: get_i64(r, 1),
                    charter_sales: get_i64(r, 2),
                    own_sales_raw: get_i64(r, 3),
                    charter_sales_raw: get_i64(r, 4),
                    transport_count: get_i32(r, 5),
                })
                .collect(),
            prev_rows
                .iter()
                .map(|r| RawDailyPrevRow {
                    date: r.get(0).unwrap_or_default(),
                    own_sales: get_i64(r, 1),
                    charter_sales: get_i64(r, 2),
                    own_sales_raw: get_i64(r, 3),
                    charter_sales_raw: get_i64(r, 4),
                })
                .collect(),
        ))
    }

    async fn customer_trend_data(
        &self,
        from: &str,
        to: &str,
        limit: i32,
    ) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let top_query = format!(
            "SELECT TOP {} m.[得意先C], ISNULL(c.[得意先N], '') \
             FROM [得意先別月計] m \
             LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
             WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
             GROUP BY m.[得意先C], c.[得意先N] \
             ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC",
            limit.min(50)
        );
        let top_stream = conn
            .query(&top_query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let top_rows = top_stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let top: Vec<(String, String)> = top_rows
            .iter()
            .map(|r| (decode_cp932(r, 0), decode_cp932(r, 1)))
            .collect();

        if top.is_empty() {
            return Ok((vec![], vec![]));
        }

        let monthly_stream = conn
            .query(
                "SELECT m.[得意先C], m.[年月度], \
             SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) as total \
             FROM [得意先別月計] m \
             WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
             GROUP BY m.[得意先C], m.[年月度] \
             ORDER BY m.[年月度], total DESC",
                &[&from, &to],
            )
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let monthly_rows = monthly_stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok((
            top,
            monthly_rows
                .iter()
                .map(|r| RawCustomerMonthlyRow {
                    customer_code: decode_cp932(r, 0),
                    year_month: r.get(1).unwrap_or_default(),
                    total: get_i64(r, 2),
                })
                .collect(),
        ))
    }

    async fn customer_detail_data(
        &self,
        code: &str,
    ) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let name_stream = conn
            .query(
                "SELECT TOP 1 ISNULL(c.[得意先N], '') FROM [得意先ﾏｽﾀ] c WHERE c.[得意先C] = @P1",
                &[&code],
            )
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let name_rows = name_stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let name = name_rows
            .first()
            .map(|r| decode_cp932(r, 0))
            .unwrap_or_default();

        let stream = conn.query(
            "SELECT m.[年月度], \
             SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
             FROM [得意先別月計] m \
             WHERE m.[得意先C] = @P1 \
             GROUP BY m.[年月度] \
             ORDER BY m.[年月度]",
            &[&code],
        ).await.map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok((
            name,
            rows.iter()
                .map(|r| RawCustomerDetailRow {
                    year_month: r.get(0).unwrap_or_default(),
                    own_sales: get_i64(r, 1),
                    charter_sales: get_i64(r, 2),
                    transport_count: get_i64(r, 3),
                })
                .collect(),
        ))
    }

    async fn customer_yoy_by_dept_data(
        &self,
        from: &str,
        to: &str,
        prev_from: &str,
        prev_to: &str,
        department_code: Option<&str>,
    ) -> Result<(Vec<RawCustomerDeptRow>, Vec<RawCustomerDeptRow>), RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // 月計テーブルとの完全一致条件: 請求K IN ('0','2')、自車/傭車の売上計算
        // 運転日報明細から [受注部門] でグルーピングして営業所×得意先の売上を算出
        let base_select = "SELECT t.[受注部門], ISNULL(d.[部門N], ''), \
                             t.[得意先C], ISNULL(c.[得意先N], ''), \
                             SUM(ISNULL(t.[税抜金額],0) + ISNULL(t.[税抜割増],0) + ISNULL(t.[税抜実費],0) - ISNULL(t.[値引],0)) \
                             + SUM(ISNULL(t.[税抜傭車金額],0) + ISNULL(t.[税抜傭車割増],0) + ISNULL(t.[税抜傭車実費],0) - ISNULL(t.[傭車値引],0)) \
                           FROM [運転日報明細] t \
                           LEFT JOIN [部門ﾏｽﾀ] d ON t.[受注部門] = d.[部門C] \
                           LEFT JOIN [得意先ﾏｽﾀ] c ON t.[得意先C] = c.[得意先C] \
                           WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
                             AND t.[請求K] IN ('0','2')";
        let group_order = " GROUP BY t.[受注部門], d.[部門N], t.[得意先C], c.[得意先N]";

        let (cur_rows, prev_rows) = if let Some(dept) = department_code {
            let sql = format!("{} AND t.[受注部門] = @P3 {}", base_select, group_order);
            let stream = conn
                .query(&sql, &[&from, &to, &dept])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let c = stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let prev_stream = conn
                .query(&sql, &[&prev_from, &prev_to, &dept])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let p = prev_stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            (c, p)
        } else {
            let sql = format!("{}{}", base_select, group_order);
            let stream = conn
                .query(&sql, &[&from, &to])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let c = stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let prev_stream = conn
                .query(&sql, &[&prev_from, &prev_to])
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            let p = prev_stream
                .into_first_result()
                .await
                .map_err(|e| RepoError::QueryError(e.to_string()))?;
            (c, p)
        };

        Ok((
            Self::rows_to_customer_dept(&cur_rows),
            Self::rows_to_customer_dept(&prev_rows),
        ))
    }

    async fn list_departments(&self) -> Result<Vec<(String, String)>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let stream = conn
            .simple_query("SELECT [部門C], ISNULL([部門N], '') FROM [部門ﾏｽﾀ] ORDER BY [部門C]")
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| (decode_cp932(row, 0), decode_cp932(row, 1)))
            .collect())
    }

    async fn vehicles(&self) -> Result<Vec<(String, String)>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;
        let stream = conn
            .simple_query("SELECT [車種C], ISNULL([車種N], '') FROM [車種ﾏｽﾀ] ORDER BY [車種C]")
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(rows
            .iter()
            .map(|row| (decode_cp932(row, 0), decode_cp932(row, 1)))
            .collect())
    }

    async fn surcharge_base(
        &self,
        from: &str,
        to: &str,
        kind_filter: &str,
        limit: i32,
    ) -> Result<Vec<RawSurchargeRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // 調査 #12 で実機検証した SELECT。請求対象行を県・車種付きで取り出す。
        // 県正規化 (地域N → 都道府県) はロジック層 (normalize_prefecture) に任せ、
        // ここでは 地域ﾏｽﾀ.地域N の生値を返す。運賃 (fare) は #12 の確定式 金額+割増+実費。
        //
        // fuel_surcharge (末尾列) は #24 で正本に決定した「割増方式 (割増C='19' = 燃料ｻｰﾁｬｰｼﾞ)」
        // の実額。本体 [割増] には燃料SC以外 (中継料/オガ処理料/深夜等) が混在するため、
        // 内訳テーブル [運転日報割増明細] の 割増C1/2/3='19' 枠だけを SUM して分離する (Refs #25)。
        // 運転日報明細 ⇔ 運転日報割増明細 は明細行 1:1 対応 (管理年月日+管理C+自車傭車K='0') 前提。
        //
        // マスタ参照は LEFT JOIN ではなくスカラサブクエリ (TOP 1) で引く。得意先ﾏｽﾀ /
        // 車種ﾏｽﾀ / 地域ﾏｽﾀ がコードに対し複数行を持つと LEFT JOIN は行を掛け算し、
        // 1 明細が N 重複して返る (請求明細のファンアウト)。スカラサブクエリなら
        // 「運転日報明細 1 行 = 出力 1 行」を保証でき、本物の重複配送 (同条件 2 回運行)
        // を潰してしまう DISTINCT の副作用も無い。列順は変更しない (rows_to_surcharge 依存)。
        let query = format!(
            "SELECT TOP {} \
             t.[請求K], t.[得意先C], \
             ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c WHERE c.[得意先C] = t.[得意先C]), ''), \
             ISNULL((SELECT TOP 1 o.[地域N] FROM [地域ﾏｽﾀ] o WHERE o.[地域C] = t.[発地域C]), ''), \
             ISNULL((SELECT TOP 1 d.[地域N] FROM [地域ﾏｽﾀ] d WHERE d.[地域C] = t.[着地域C]), ''), \
             t.[車種C], \
             ISNULL((SELECT TOP 1 v.[車種N] FROM [車種ﾏｽﾀ] v WHERE v.[車種C] = t.[車種C]), ''), \
             t.[売上年月日], \
             ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
             t.[入金予定日], \
             ISNULL(t.[傭車先C], ''), \
             ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
             ISNULL(t.[車輌C], ''), \
             ISNULL((SELECT SUM(\
               CASE WHEN za.[割増C1] = '19' THEN ISNULL(za.[割増金額1], 0) ELSE 0 END \
             + CASE WHEN za.[割増C2] = '19' THEN ISNULL(za.[割増金額2], 0) ELSE 0 END \
             + CASE WHEN za.[割増C3] = '19' THEN ISNULL(za.[割増金額3], 0) ELSE 0 END) \
               FROM [運転日報割増明細] za \
               WHERE za.[管理年月日] = t.[管理年月日] \
                 AND za.[管理C] = t.[管理C] \
                 AND za.[自車傭車K] = '0'), 0), \
             CONCAT(CONVERT(varchar(8), t.[管理年月日], 112), '-', t.[管理C]), \
             ISNULL(t.[入力担当C], ''), \
             ISNULL((SELECT TOP 1 s.[社員N] FROM [社員ﾏｽﾀ] s WHERE s.[社員C] = t.[入力担当C]), '') \
             FROM [運転日報明細] t \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 {} \
             ORDER BY t.[入金予定日], t.[得意先C], t.[売上年月日]",
            limit.clamp(1, 10000),
            kind_filter
        );

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_surcharge(&rows))
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
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // 受注部門 IN (...) を動的に組む。bumon_codes は呼び出し側で whitelist 済み
        // (営業所マスタから引いた `'010'`,`'011'` 等の固定形式) のため SQL injection
        // 上は安全だが、念のため英数字のみに絞ってから組み立てる。
        let safe_bumon: Vec<String> = bumon_codes
            .iter()
            .filter(|c| {
                !c.is_empty() && c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            })
            .cloned()
            .collect();
        if safe_bumon.is_empty() {
            return Ok(vec![]);
        }
        let bumon_in = safe_bumon
            .iter()
            .map(|c| format!("'{}'", c))
            .collect::<Vec<_>>()
            .join(",");

        // 入力担当C IN (...) — persons_id_list は数値なのでそのまま組み立てる
        // (型は i32、untrusted 入力ではないが念のため format で固定)。
        // 空リストでも SQL は valid であるべき: IN () は SQL Server で構文エラーに
        // なるので NULL (= 全件 false) で埋める fallback を入れる。
        let persons_in = if persons_id_list.is_empty() {
            "NULL".to_string()
        } else {
            persons_id_list
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(",")
        };

        // PHP `UriageJyuchuDisplayController::make_arrays()` の **5 ケース UNION** を再現。
        //
        // PHP L1769-1791:
        //   foreach ($UriageJyuchuDisplayPersons_id as $ddp) {
        //       $this->make_array(['入力担当C in' => $ddp]);  // sql_options の 3 ケース
        //   }
        //   sql_from_other          → 受注∉ AND 稼動∈ AND 傭車先≠000000
        //   sql_from_other_with_bumon → 受注∈ AND 稼動∉ AND 傭車先=000000 AND 入力担当C ∉ persons
        //
        // sql_options (PHP L1827-1850) は `make_yosha_sql` を base に 3 ケース:
        //   - 傭車:       稼動∈ AND 配車K=1 AND 入力担当C ∈ persons    → 横横=0
        //   - 営業所傭車: 稼動∉ AND 配車K=0 AND 入力担当C ∈ persons    → 横横=1
        //   - 傭車傭車:   稼動∉ AND 配車K=1 AND 入力担当C ∈ persons    → 横横=1
        //
        // つまり 5 つの subquery を UNION ALL する (PHP は array_push なので重複なし
        // のはずだが、PHP の row レベルでも 5 case 互いに排他 = 配車K=0/1 + 稼動部門
        // ∈/∉ で組み分けされている)。
        //
        // 香月 NG 行 (#762、user 2026-06-30) の原因:
        //   営業所 10、入力担当C=1180 (∈ persons)、傭車先=000000、配車K=9 (未配車)、
        //   受注∈ AND 稼動∉。PHP 5 ケース全部 hit せず → $sum 0 円。
        //   旧 Rust SQL は `(受注∈ AND (稼動∉ OR 配車K=1))` で拾ってしまっていた。
        //   配車K=9 は sql_options の 3 ケース (配車K=0/1) からも漏れ、
        //   sql_from_other_with_bumon は 入力担当C ∉ persons が必要で hit せず。
        //
        // 細部:
        // - 品名N の調整行除外は全角空白 U+3000 (PHP L1708-1709 と同形)
        // - `日報K != 3` (PHP L1710)
        // - `請求K=2 → 備考2='表示' or LIKE '売上%'` (PHP L1712/1738/1760 OR superset)
        // - `NOT (請求K='1' AND 備考2='請求のみ')` (PHP L1713)
        // - 社員R は **TOP 1 スカラサブクエリ** で引く (社員ﾏｽﾀ 複数行/社員C による
        //   JOIN ファンアウト防止、surcharge.rs と同型)
        // - 順序は print テンプレ表示順 (運行年月日 ASC, 管理C ASC, LC ASC)
        // - 請求K / 入力担当C は varchar の可能性あり `TRY_CAST(... AS INT)` で int 化
        // - 共通 WHERE 句を repeat する代わりに subquery を inline で書き、外側で
        //   ORDER BY する形 (UNION ALL は ORDER BY を最外殻に置く制約があるため)
        let select_cols = "\
                 t.[横横] AS [横横], \
                 ISNULL(TRY_CAST(t.[請求K] AS INT), 0) AS [請求K], \
                 ISNULL(t.[備考2], '') AS [備考2], \
                 ISNULL(TRY_CAST(t.[入力担当C] AS INT), 0) AS [入力担当C], \
                 ISNULL(t.[稼動部門], '') AS [稼動部門], \
                 ISNULL(t.[金額], 0) AS [金額], \
                 ISNULL(t.[値引], 0) AS [値引], \
                 ISNULL(t.[割増], 0) AS [割増], \
                 ISNULL(t.[実費], 0) AS [実費], \
                 ISNULL(t.[傭車金額], 0) AS [傭車金額], \
                 ISNULL(t.[傭車値引], 0) AS [傭車値引], \
                 ISNULL(t.[傭車割増], 0) AS [傭車割増], \
                 ISNULL(t.[傭車実費], 0) AS [傭車実費], \
                 ISNULL((SELECT TOP 1 e.[社員R] FROM [社員ﾏｽﾀ] e WHERE e.[社員C] = t.[入力担当C]), '') AS [社員R], \
                 ISNULL(t.[傭車先C], '000000') AS [傭車先C], \
                 CONVERT(varchar(10), t.[運行年月日], 23) AS [運行年月日], \
                 CONVERT(varchar(10), t.[売上年月日], 23) AS [売上年月日], \
                 CONCAT(ISNULL(t.[得意先C], ''), '-', ISNULL(t.[得意先H], '')) AS [得意先複合キー], \
                 ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c \
                   WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]), '') AS [得意先N], \
                 CONCAT(ISNULL(t.[傭車先C], ''), '-', ISNULL(t.[傭車先H], '')) AS [傭車先複合キー], \
                 ISNULL((SELECT TOP 1 y.[傭車先N] FROM [傭車先ﾏｽﾀ] y \
                   WHERE y.[傭車先C] = t.[傭車先C] AND y.[傭車先H] = t.[傭車先H]), '') AS [傭車先N], \
                 t.[管理C] AS [管理C], t.[LC] AS [LC]";

        // 各 case 共通の WHERE 述語 (日付・品名・日報K・請求K・備考2 系)
        let common_where = "\
                 [品名N] NOT IN ('※\u{3000}請求一括調整明細\u{3000}※', '※\u{3000}傭車一括調整明細\u{3000}※') \
                 AND ISNULL([日報K], 0) != 3 \
                 AND NOT ([請求K] = '1' AND [備考2] = '請求のみ') \
                 AND ([請求K] != '2' OR [備考2] = '表示' OR [備考2] LIKE '売上%') \
                 AND [運行年月日] >= @P1 AND [運行年月日] <= @P2";

        // Case 1: 傭車 (横横=0)
        //   make_yosha_sql + sql_options('傭車'):
        //   受注∈ AND 稼動∈ AND 配車K='1' AND 入力担当C ∈ persons
        let case_yosha = format!(
            "SELECT 0 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] IN ({bumon_in}) \
               AND [配車K] = '1' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 2: 営業所傭車 (横横=1)
        //   受注∈ AND 稼動∉ AND 配車K='0' AND 入力担当C ∈ persons
        let case_eigyosho_yosha = format!(
            "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND [配車K] = '0' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 3: 傭車傭車 (横横=1)
        //   受注∈ AND 稼動∉ AND 配車K='1' AND 入力担当C ∈ persons
        let case_yosha_yosha = format!(
            "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND [配車K] = '1' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 4: sql_from_other_with_bumon (横横=1、PHP L1747-1767)
        //   受注∈ AND 稼動∉ AND 傭車先=000000 AND 入力担当C ∉ persons
        //   ※ 配車K 条件は無い (= 配車K=9 等もここで拾われる、ただし 入力担当C ∉ persons の人だけ)
        let case_with_bumon = format!(
            "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND ISNULL([傭車先C], '000000') = '000000' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) NOT IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 5: sql_from_other (横横=0、PHP L1725-1745)
        //   受注∉ AND 稼動∈ AND 傭車先≠000000
        let case_from_other = format!(
            "SELECT 0 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] NOT IN ({bumon_in}) AND [稼動部門] IN ({bumon_in}) \
               AND ISNULL([傭車先C], '000000') != '000000' \
               AND {common_where}"
        );

        let query = format!(
            "SELECT {select_cols} FROM ( \
                 {case_yosha} \
                 UNION ALL {case_eigyosho_yosha} \
                 UNION ALL {case_yosha_yosha} \
                 UNION ALL {case_with_bumon} \
                 UNION ALL {case_from_other} \
             ) AS t \
             ORDER BY t.[運行年月日] ASC, t.[管理C] ASC, t.[LC] ASC"
        );

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

    async fn unchin_candidates(
        &self,
        from: &str,
        to: &str,
        partner_type: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // #57 実機調査で確定した抽出ロジック:
        // - 取引先は C+H の複合キー必須 (H は固定値ではなく変動する)
        // - 運賃額は customer: 金額+割増+実費 / subcontractor: 傭車金額+傭車割増+傭車実費
        //   (金額 は既に税抜のため値引は無視可、#57 確定式)
        // - 発地N/着地N は自由入力の生文字列をそのまま使う (県正規化はしない)
        // - 品名C IN ('9003','9998') (消費税調整/端数調整) は除外
        // - kind_filter (請求K IN (0,1) or (0,2)) でユーザーが請求区分を選べるようにする
        //   (#57 実害: TOP 上限 + 得意先C 昇順ソートの組み合わせで「運行記録簿用」等の
        //   非請求ダミー得意先が行数を食い潰し、本物の得意先が一切表示されない問題が
        //   起きていたため、TOP 上限を撤廃。summary endpoint (SQL 側 GROUP BY) を
        //   主に使う設計に変更したことで、行数による打ち切り自体が起きなくなった)
        // マスタ参照は surcharge_base と同様にスカラサブクエリ (or OUTER APPLY) で引き、
        // LEFT JOIN によるファンアウト (1 明細行が N 重複して返る) を避ける。
        // `部門C`/`部門N` (自社側の受注部門/営業所) は #92 follow-up で追加:
        // 得意先ﾏｽﾀ/傭車先ﾏｽﾀ 側の `部門C` は得意先・傭車先自身の拠点とは別軸で、
        // 実機調査 (#93) 確定。OUTER APPLY で得意先N/傭車先N と部門C をまとめて引き、
        // 部門ﾏｽﾀ を LEFT JOIN して部門N を解決する (m.部門C は最大1行に絞られているため
        // 通常の LEFT JOIN でもファンアウトしない)。
        let query = if partner_type == "subcontractor" {
            format!(
                "SELECT \
                 CONCAT(t.[傭車先C], '-', t.[傭車先H]), \
                 ISNULL(m.[傭車先N], ''), \
                 ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
                 ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0), \
                 ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
                 t.[売上年月日], \
                 ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], ''), \
                 CONCAT(ISNULL(t.[車輌C], ''), '-', ISNULL(t.[車輌H], '')) \
                 FROM [運転日報明細] t \
                 OUTER APPLY (SELECT TOP 1 c.[傭車先N], c.[部門C] FROM [傭車先ﾏｽﾀ] c \
                   WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) m \
                 LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
                 WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
                   AND t.[品名C] NOT IN ('9003', '9998') \
                   AND ISNULL(t.[傭車先C], '000000') != '000000' \
                   {} \
                 ORDER BY t.[傭車先C], t.[傭車先H], t.[品名C], t.[金額]",
                kind_filter
            )
        } else {
            format!(
                "SELECT \
                 CONCAT(t.[得意先C], '-', t.[得意先H]), \
                 ISNULL(m.[得意先N], ''), \
                 ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
                 ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
                 ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
                 t.[売上年月日], \
                 ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], ''), \
                 CONCAT(ISNULL(t.[車輌C], ''), '-', ISNULL(t.[車輌H], '')) \
                 FROM [運転日報明細] t \
                 OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
                   WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
                 LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
                 WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
                   AND t.[品名C] NOT IN ('9003', '9998') \
                   {} \
                 ORDER BY t.[得意先C], t.[得意先H], t.[品名C], t.[金額]",
                kind_filter
            )
        };

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_unchin(&rows))
    }

    async fn unchin_summary(
        &self,
        from: &str,
        to: &str,
        partner_type: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinSummaryRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // 得意先 (or 傭車先) ごとに SUM/GROUP BY で集計する。結果行数 = 取引先数なので
        // raw 行 TOP-N 方式と違い一部の取引先が行数を食い潰して他が消える問題が起きない。
        // `部門C`/`部門N` は #92 follow-up で追加 (OUTER APPLY の詳細は unchin_candidates
        // のコメント参照)。APPLY/JOIN の出力列は非集約列なので GROUP BY にも含める必要がある。
        let query = if partner_type == "subcontractor" {
            format!(
                "SELECT t.[傭車先C], t.[傭車先H], \
                 ISNULL(m.[傭車先N], ''), \
                 SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)), \
                 ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
                 FROM [運転日報明細] t \
                 OUTER APPLY (SELECT TOP 1 c.[傭車先N], c.[部門C] FROM [傭車先ﾏｽﾀ] c \
                   WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) m \
                 LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
                 WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
                   AND t.[品名C] NOT IN ('9003', '9998') \
                   AND ISNULL(t.[傭車先C], '000000') != '000000' \
                   {} \
                 GROUP BY t.[傭車先C], t.[傭車先H], m.[傭車先N], m.[部門C], bm.[部門N] \
                 ORDER BY SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC",
                kind_filter
            )
        } else {
            format!(
                "SELECT t.[得意先C], t.[得意先H], \
                 ISNULL(m.[得意先N], ''), \
                 SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)), \
                 ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
                 FROM [運転日報明細] t \
                 OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
                   WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
                 LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
                 WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
                   AND t.[品名C] NOT IN ('9003', '9998') \
                   {} \
                 GROUP BY t.[得意先C], t.[得意先H], m.[得意先N], m.[部門C], bm.[部門N] \
                 ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) DESC",
                kind_filter
            )
        };

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_unchin_summary(&rows))
    }

    async fn unchin_subcontractor_net(
        &self,
        from: &str,
        to: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinSubcontractorNetRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // 傭車先C-傭車先H ごとに GROUP BY し、同一行にある得意先側金額
        // (金額+割増+実費) と傭車先側金額 (傭車金額+傭車割増+傭車実費) を
        // 同時に SUM する (= 「その傭車先を使った運行の得意先請求合計」と
        // 「その傭車先への支払合計」、2026-07-01 user 確認「同一運行内の両建て」)。
        // フォーミュラは unchin_summary と同じ式に揃える (税抜カラムを使う
        // uriage 側の月計一致式とは別物、#57 確定式のまま踏襲)。
        let query = format!(
            "SELECT t.[傭車先C], t.[傭車先H], \
             ISNULL(m.[傭車先N], ''), \
             SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)), \
             SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)), \
             ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
             FROM [運転日報明細] t \
             OUTER APPLY (SELECT TOP 1 c.[傭車先N], c.[部門C] FROM [傭車先ﾏｽﾀ] c \
               WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) m \
             LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND t.[品名C] NOT IN ('9003', '9998') \
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             GROUP BY t.[傭車先C], t.[傭車先H], m.[傭車先N], m.[部門C], bm.[部門N] \
             ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) \
               - SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC",
            kind_filter
        );

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_unchin_subcontractor_net(&rows))
    }

    async fn unchin_subcontractor_net_detail(
        &self,
        from: &str,
        to: &str,
        code: &str,
        h: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinSubcontractorNetDetailRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // unchin_subcontractor_net のドリルダウン。特定の傭車先C+H に絞り込み、
        // 集計せず運行 (行) 単位で 得意先側金額 と 傭車先側金額 を両方読む。
        // 得意先N はその行の 得意先C/得意先H から OUTER APPLY で引く
        // (unchin_summary の customer 側と同じ引き方)。
        let query = format!(
            "SELECT \
             ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
             ISNULL(cm.[得意先N], ''), \
             ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
             ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0), \
             ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
             t.[売上年月日], \
             ISNULL(sm.[部門C], ''), ISNULL(bm.[部門N], '') \
             FROM [運転日報明細] t \
             OUTER APPLY (SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c \
               WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) cm \
             OUTER APPLY (SELECT TOP 1 c.[部門C] FROM [傭車先ﾏｽﾀ] c \
               WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) sm \
             LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = sm.[部門C] \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND t.[品名C] NOT IN ('9003', '9998') \
               AND t.[傭車先C] = @P3 AND t.[傭車先H] = @P4 \
               {} \
             ORDER BY t.[売上年月日] DESC",
            kind_filter
        );

        let stream = conn
            .query(&query, &[&from, &to, &code, &h])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_unchin_subcontractor_net_detail(&rows))
    }

    async fn unchin_customer_net(
        &self,
        from: &str,
        to: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinCustomerNetRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // 得意先C-得意先H ごとに GROUP BY し、請求合計 (金額+割増+実費) と
        // 傭車支払合計 (傭車金額+傭車割増+傭車実費) を同時に SUM する
        // (= unchin_subcontractor_net を得意先軸で見たもの、2026-07-01 user 確認
        // 「傭車先じゃなくて得意先にグラフ直して」)。自社便 (傭車先C='000000') の行は
        // 常に除外する — 自社便を含めるかのトグルは設けず、SQL 側で最初から対象外にする
        // (2026-07-01 user 確認「トグルじゃない もとからなくして グラフも」)。
        let query = format!(
            "SELECT t.[得意先C], t.[得意先H], \
             ISNULL(m.[得意先N], ''), \
             SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)), \
             SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)), \
             ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
             FROM [運転日報明細] t \
             OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
               WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
             LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND t.[品名C] NOT IN ('9003', '9998') \
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             GROUP BY t.[得意先C], t.[得意先H], m.[得意先N], m.[部門C], bm.[部門N] \
             ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) \
               - SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC",
            kind_filter
        );

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_unchin_customer_net(&rows))
    }

    async fn unchin_customer_net_detail(
        &self,
        from: &str,
        to: &str,
        code: &str,
        h: &str,
        kind_filter: &str,
    ) -> Result<Vec<RawUnchinCustomerNetDetailRow>, RepoError> {
        let mut conn = self.pool.get().await.map_err(|_| RepoError::PoolError)?;

        // unchin_customer_net のドリルダウン。特定の得意先C+H に絞り込み、
        // 集計せず運行 (行) 単位で 得意先側金額 と 傭車先側金額 を両方読む。
        // 傭車先N はその行の 傭車先C/傭車先H から OUTER APPLY で引く。自社便
        // (傭車先C='000000') の行は unchin_customer_net と同じガードで常に除外する
        // (2026-07-01 user 確認「トグルじゃない もとからなくして グラフも」)。
        let query = format!(
            "SELECT \
             ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
             ISNULL(sm.[傭車先N], ''), \
             ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
             ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0), \
             ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
             t.[売上年月日], \
             ISNULL(cm.[部門C], ''), ISNULL(bm.[部門N], '') \
             FROM [運転日報明細] t \
             OUTER APPLY (SELECT TOP 1 c.[傭車先N] FROM [傭車先ﾏｽﾀ] c \
               WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) sm \
             OUTER APPLY (SELECT TOP 1 c.[部門C] FROM [得意先ﾏｽﾀ] c \
               WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) cm \
             LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = cm.[部門C] \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND t.[品名C] NOT IN ('9003', '9998') \
               AND t.[得意先C] = @P3 AND t.[得意先H] = @P4 \
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             ORDER BY t.[売上年月日] DESC",
            kind_filter
        );

        let stream = conn
            .query(&query, &[&from, &to, &code, &h])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_unchin_customer_net_detail(&rows))
    }
}

// ── Row → Raw 変換ヘルパー ──
impl TiberiusRepo {
    fn rows_to_monthly(rows: &[tiberius::Row]) -> Vec<RawMonthlyRow> {
        rows.iter()
            .map(|r| RawMonthlyRow {
                year_month: r.get(0).unwrap_or_default(),
                own_sales: get_i64(r, 1),
                charter_sales: get_i64(r, 2),
                transport_count: get_i32(r, 3),
            })
            .collect()
    }

    fn rows_to_monthly_prev(rows: &[tiberius::Row]) -> Vec<RawMonthlyRow> {
        rows.iter()
            .map(|r| RawMonthlyRow {
                year_month: r.get(0).unwrap_or_default(),
                own_sales: get_i64(r, 1),
                charter_sales: get_i64(r, 2),
                transport_count: 0,
            })
            .collect()
    }

    fn rows_to_code_total_map(rows: &[tiberius::Row]) -> CodeTotalMap {
        let mut map = std::collections::HashMap::new();
        for row in rows {
            map.insert(
                decode_cp932(row, 0),
                (decode_cp932(row, 1), get_i64(row, 2)),
            );
        }
        map
    }

    fn rows_to_customer_dept(rows: &[tiberius::Row]) -> Vec<RawCustomerDeptRow> {
        rows.iter()
            .map(|r| RawCustomerDeptRow {
                department_code: decode_cp932(r, 0),
                department_name: decode_cp932(r, 1),
                customer_code: decode_cp932(r, 2),
                customer_name: decode_cp932(r, 3),
                total: get_i64(r, 4),
            })
            .collect()
    }

    fn rows_to_uriage(rows: &[tiberius::Row]) -> Vec<UriageRow> {
        rows.iter()
            .map(|r| UriageRow {
                yokoyoko: get_i32(r, 0),
                seikyu_k: get_i32(r, 1),
                biko2: decode_cp932(r, 2),
                nyuryoku_tanto_c: get_i32(r, 3),
                kado_bumon: decode_cp932(r, 4),
                kingaku: get_i64(r, 5),
                nebiki: get_i64(r, 6),
                warimashi: get_i64(r, 7),
                jippi: get_i64(r, 8),
                yosha_kingaku: get_i64(r, 9),
                yosha_nebiki: get_i64(r, 10),
                yosha_warimashi: get_i64(r, 11),
                yosha_jippi: get_i64(r, 12),
                shain_r: decode_cp932(r, 13),
                yoshasaki_c: decode_cp932(r, 14),
                // CONVERT(varchar(10), …, 23) で 'YYYY-MM-DD' 文字列が返る (locale 非依存)
                unko_date: decode_cp932(r, 15),
                uriage_date: decode_cp932(r, 16),
                tokuisaki_key: decode_cp932(r, 17),
                tokuisaki_n: decode_cp932(r, 18),
                yoshasaki_key: decode_cp932(r, 19),
                yoshasaki_n: decode_cp932(r, 20),
            })
            .collect()
    }

    fn rows_to_unchin_summary(rows: &[tiberius::Row]) -> Vec<RawUnchinSummaryRow> {
        rows.iter()
            .map(|r| RawUnchinSummaryRow {
                partner_code: format!("{}-{}", decode_cp932(r, 0), decode_cp932(r, 1)),
                partner_name: decode_cp932(r, 2),
                total: get_i64(r, 3),
                bumon_code: decode_cp932(r, 4),
                bumon_name: decode_cp932(r, 5),
            })
            .collect()
    }

    fn rows_to_unchin_subcontractor_net(
        rows: &[tiberius::Row],
    ) -> Vec<RawUnchinSubcontractorNetRow> {
        rows.iter()
            .map(|r| RawUnchinSubcontractorNetRow {
                partner_code: format!("{}-{}", decode_cp932(r, 0), decode_cp932(r, 1)),
                partner_name: decode_cp932(r, 2),
                total_sales: get_i64(r, 3),
                total_payment: get_i64(r, 4),
                bumon_code: decode_cp932(r, 5),
                bumon_name: decode_cp932(r, 6),
            })
            .collect()
    }

    fn rows_to_unchin_subcontractor_net_detail(
        rows: &[tiberius::Row],
    ) -> Vec<RawUnchinSubcontractorNetDetailRow> {
        rows.iter()
            .map(|r| RawUnchinSubcontractorNetDetailRow {
                item_code: decode_cp932(r, 0),
                item_name: decode_cp932(r, 1),
                customer_name: decode_cp932(r, 2),
                sales: get_i64(r, 3),
                payment: get_i64(r, 4),
                origin: decode_cp932(r, 5),
                dest: decode_cp932(r, 6),
                sale_date: r.get(7).unwrap_or_default(),
                bumon_code: decode_cp932(r, 8),
                bumon_name: decode_cp932(r, 9),
            })
            .collect()
    }

    fn rows_to_unchin_customer_net(rows: &[tiberius::Row]) -> Vec<RawUnchinCustomerNetRow> {
        rows.iter()
            .map(|r| RawUnchinCustomerNetRow {
                partner_code: format!("{}-{}", decode_cp932(r, 0), decode_cp932(r, 1)),
                partner_name: decode_cp932(r, 2),
                total_sales: get_i64(r, 3),
                total_payment: get_i64(r, 4),
                bumon_code: decode_cp932(r, 5),
                bumon_name: decode_cp932(r, 6),
            })
            .collect()
    }

    fn rows_to_unchin_customer_net_detail(
        rows: &[tiberius::Row],
    ) -> Vec<RawUnchinCustomerNetDetailRow> {
        rows.iter()
            .map(|r| RawUnchinCustomerNetDetailRow {
                item_code: decode_cp932(r, 0),
                item_name: decode_cp932(r, 1),
                subcontractor_name: decode_cp932(r, 2),
                sales: get_i64(r, 3),
                payment: get_i64(r, 4),
                origin: decode_cp932(r, 5),
                dest: decode_cp932(r, 6),
                sale_date: r.get(7).unwrap_or_default(),
                bumon_code: decode_cp932(r, 8),
                bumon_name: decode_cp932(r, 9),
            })
            .collect()
    }

    fn rows_to_unchin(rows: &[tiberius::Row]) -> Vec<RawUnchinRow> {
        rows.iter()
            .map(|r| RawUnchinRow {
                partner_code: decode_cp932(r, 0),
                partner_name: decode_cp932(r, 1),
                item_code: decode_cp932(r, 2),
                item_name: decode_cp932(r, 3),
                fare: get_i64(r, 4),
                origin: decode_cp932(r, 5),
                dest: decode_cp932(r, 6),
                sale_date: r.get(7).unwrap_or_default(),
                bumon_code: decode_cp932(r, 8),
                bumon_name: decode_cp932(r, 9),
                vehicle_code: decode_cp932(r, 10),
            })
            .collect()
    }

    fn rows_to_surcharge(rows: &[tiberius::Row]) -> Vec<RawSurchargeRow> {
        rows.iter()
            .map(|r| RawSurchargeRow {
                request_kind: decode_cp932(r, 0),
                customer_code: decode_cp932(r, 1),
                customer_name: decode_cp932(r, 2),
                origin_area_name: decode_cp932(r, 3),
                dest_area_name: decode_cp932(r, 4),
                vehicle_code: decode_cp932(r, 5),
                vehicle_name: decode_cp932(r, 6),
                sale_date: r.get(7).unwrap_or_default(),
                fare: get_i64(r, 8),
                billing_date: r.get(9),
                subcontractor_code: decode_cp932(r, 10),
                item_code: decode_cp932(r, 11),
                item_name: decode_cp932(r, 12),
                vehicle_number: decode_cp932(r, 13),
                fuel_surcharge: get_i64(r, 14),
                row_id: decode_cp932(r, 15),
                input_staff_code: decode_cp932(r, 16),
                input_staff_name: decode_cp932(r, 17),
            })
            .collect()
    }
}
