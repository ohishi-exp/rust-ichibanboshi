use async_trait::async_trait;
use std::sync::Arc;

use crate::routes::sales::*;
use crate::routes::schema::{ColumnInfo, SampleRow, TableInfo};
use crate::routes::surcharge::RawSurchargeRow;

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
                 AND za.[自車傭車K] = '0'), 0) \
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
            })
            .collect()
    }
}
