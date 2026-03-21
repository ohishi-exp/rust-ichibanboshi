use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::{Deserialize, Serialize};
use tiberius::Row;

use crate::db::DbPool;

/// CP932 文字列をデコード
fn decode_cp932(row: &Row, idx: usize) -> String {
    // tiberius は varchar を &str で返す（サーバーのコレーションに依存）
    // CP932 の場合、バイト列として取得してデコードする必要がある場合がある
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn get_i64(row: &Row, idx: usize) -> i64 {
    // money → f64, decimal → Numeric, int → i32 のいずれか
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

fn get_i32(row: &Row, idx: usize) -> i32 {
    row.try_get::<i32, _>(idx).ok().flatten().unwrap_or(0)
}

// ── 月別売上推移 ──

#[derive(Serialize)]
pub struct MonthlySales {
    pub year_month: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Deserialize)]
pub struct MonthlyQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

/// GET /api/sales/monthly — 全社月別売上推移（種別C=99）
pub async fn monthly(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<MonthlyQuery>,
) -> Result<Json<Vec<MonthlySales>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let stream = conn
        .query(
            "SELECT [年月度], [自車売上], [傭車売上], [輸送回数] \
             FROM [種別別月計] \
             WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
             ORDER BY [年月度]",
            &[&from_date.as_str(), &to_date.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let data: Vec<MonthlySales> = rows
        .iter()
        .map(|row| {
            let dt: chrono::NaiveDateTime = row.get(0).unwrap_or_default();
            let own = get_i64(row, 1);
            let charter = get_i64(row, 2);
            MonthlySales {
                year_month: dt.format("%Y-%m").to_string(),
                own_sales: own,
                charter_sales: charter,
                total_sales: own + charter,
                transport_count: get_i32(row, 3),
            }
        })
        .collect();

    Ok(Json(data))
}

// ── 部門別売上 ──

#[derive(Serialize)]
pub struct DepartmentSales {
    pub department_code: String,
    pub department_name: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Deserialize)]
pub struct PeriodQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

/// GET /api/sales/by-department — 部門別売上
pub async fn by_department(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<PeriodQuery>,
) -> Result<Json<Vec<DepartmentSales>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let stream = conn
        .query(
            "SELECT m.[部門C], ISNULL(d.[部門N], ''), \
             SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
             FROM [部門別月計] m \
             LEFT JOIN [部門ﾏｽﾀ] d ON m.[部門C] = d.[部門C] \
             WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
             GROUP BY m.[部門C], d.[部門N] \
             ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC",
            &[&from_date.as_str(), &to_date.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let data: Vec<DepartmentSales> = rows
        .iter()
        .map(|row| {
            let own = get_i64(row, 2);
            let charter = get_i64(row, 3);
            DepartmentSales {
                department_code: decode_cp932(row, 0),
                department_name: decode_cp932(row, 1),
                own_sales: own,
                charter_sales: charter,
                total_sales: own + charter,
                transport_count: get_i64(row, 4) as i32,
            }
        })
        .collect();

    Ok(Json(data))
}

// ── 得意先別売上 ──

#[derive(Serialize)]
pub struct CustomerSales {
    pub customer_code: String,
    pub customer_name: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Deserialize)]
pub struct CustomerQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i32>,
}

/// GET /api/sales/by-customer — 得意先別売上ランキング
pub async fn by_customer(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<CustomerQuery>,
) -> Result<Json<Vec<CustomerSales>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let limit = params.limit.unwrap_or(20);
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

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
        .query(&query, &[&from_date.as_str(), &to_date.as_str()])
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let data: Vec<CustomerSales> = rows
        .iter()
        .map(|row| {
            let own = get_i64(row, 2);
            let charter = get_i64(row, 3);
            CustomerSales {
                customer_code: decode_cp932(row, 0),
                customer_name: decode_cp932(row, 1),
                own_sales: own,
                charter_sales: charter,
                total_sales: own + charter,
                transport_count: get_i64(row, 4) as i32,
            }
        })
        .collect();

    Ok(Json(data))
}

// ── 前年同月比較 ──

#[derive(Serialize)]
pub struct YoyComparison {
    pub month: String,
    pub current_year: i64,
    pub previous_year: i64,
    pub diff: i64,
    pub diff_percent: f64,
}

#[derive(Deserialize)]
pub struct YoyQuery {
    pub year: Option<i32>,
}

/// GET /api/sales/yoy — 前年同月比較
pub async fn yoy(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<YoyQuery>,
) -> Result<Json<Vec<YoyComparison>>, StatusCode> {
    let year = params.year.unwrap_or(2026);

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // 今年度と前年度のデータを取得
    let current_from = format!("{}-01-01", year);
    let current_to = format!("{}-12-01", year);
    let prev_from = format!("{}-01-01", year - 1);
    let prev_to = format!("{}-12-01", year - 1);

    let stream = conn
        .query(
            "SELECT MONTH([年月度]) as m, \
             SUM(ISNULL([自車売上], 0)) + SUM(ISNULL([傭車売上], 0)) as total \
             FROM [種別別月計] \
             WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
             GROUP BY MONTH([年月度]) \
             ORDER BY MONTH([年月度])",
            &[&current_from.as_str(), &current_to.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let current_rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let stream2 = conn
        .query(
            "SELECT MONTH([年月度]) as m, \
             SUM(ISNULL([自車売上], 0)) + SUM(ISNULL([傭車売上], 0)) as total \
             FROM [種別別月計] \
             WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
             GROUP BY MONTH([年月度]) \
             ORDER BY MONTH([年月度])",
            &[&prev_from.as_str(), &prev_to.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let prev_rows = stream2.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 月をキーにしたマップ
    let mut prev_map = std::collections::HashMap::new();
    for row in &prev_rows {
        let m = get_i32(row, 0);
        let total = get_i64(row, 1);
        prev_map.insert(m, total);
    }

    let data: Vec<YoyComparison> = current_rows
        .iter()
        .map(|row| {
            let m = get_i32(row, 0);
            let current = get_i64(row, 1);
            let previous = prev_map.get(&m).copied().unwrap_or(0);
            let diff = current - previous;
            let diff_percent = if previous > 0 {
                (diff as f64 / previous as f64) * 100.0
            } else {
                0.0
            };
            YoyComparison {
                month: format!("{:02}", m),
                current_year: current,
                previous_year: previous,
                diff,
                diff_percent: (diff_percent * 10.0).round() / 10.0,
            }
        })
        .collect();

    Ok(Json(data))
}
