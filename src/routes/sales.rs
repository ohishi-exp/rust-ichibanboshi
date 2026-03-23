use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::Datelike;
use serde::{Deserialize, Serialize};
use tiberius::Row;

use crate::db::DbPool;

/// 全エンドポイント共通のレスポンスラッパー
#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub source_table: String,
    pub data: T,
}

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
    pub prev_year_total: i64,
}

#[derive(Deserialize)]
pub struct MonthlyQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

/// GET /api/sales/monthly — 全社月別売上推移（種別C=99）+ 前年同月
pub async fn monthly(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<MonthlyQuery>,
) -> Result<Json<ApiResponse<Vec<MonthlySales>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);

    // 前年の期間を計算（1年前）
    let prev_from = format!(
        "{}-{}-01",
        from.split('-').next().unwrap_or("2024").parse::<i32>().unwrap_or(2024) - 1,
        from.split('-').nth(1).unwrap_or("04")
    );
    let prev_to = format!(
        "{}-{}-01",
        to.split('-').next().unwrap_or("2025").parse::<i32>().unwrap_or(2025) - 1,
        to.split('-').nth(1).unwrap_or("03")
    );

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // 今年のデータ
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

    // 前年のデータ
    let prev_stream = conn
        .query(
            "SELECT [年月度], ISNULL([自車売上], 0) + ISNULL([傭車売上], 0) as total \
             FROM [種別別月計] \
             WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
             ORDER BY [年月度]",
            &[&prev_from.as_str(), &prev_to.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error (prev): {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let prev_rows = prev_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error (prev): {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 前年データを月(MM)でマッピング
    let mut prev_map = std::collections::HashMap::new();
    for row in &prev_rows {
        let dt: chrono::NaiveDateTime = row.get(0).unwrap_or_default();
        let month = dt.format("%m").to_string();
        let total = get_i64(row, 1);
        prev_map.insert(month, total);
    }

    let data: Vec<MonthlySales> = rows
        .iter()
        .map(|row| {
            let dt: chrono::NaiveDateTime = row.get(0).unwrap_or_default();
            let own = get_i64(row, 1);
            let charter = get_i64(row, 2);
            let month = dt.format("%m").to_string();
            MonthlySales {
                year_month: dt.format("%Y-%m").to_string(),
                own_sales: own,
                charter_sales: charter,
                total_sales: own + charter,
                transport_count: get_i32(row, 3),
                prev_year_total: prev_map.get(&month).copied().unwrap_or(0),
            }
        })
        .collect();

    Ok(Json(ApiResponse {
        source_table: "種別別月計 (種別C=99)".to_string(),
        data,
    }))
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
) -> Result<Json<ApiResponse<Vec<DepartmentSales>>>, StatusCode> {
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

    Ok(Json(ApiResponse {
        source_table: "部門別月計 + 部門ﾏｽﾀ".to_string(),
        data,
    }))
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
) -> Result<Json<ApiResponse<Vec<CustomerSales>>>, StatusCode> {
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

    Ok(Json(ApiResponse {
        source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(),
        data,
    }))
}

// ── 得意先別前年同期比ランキング ──

#[derive(Serialize, Clone)]
pub struct CustomerYoy {
    pub customer_code: String,
    pub customer_name: String,
    pub current_total: i64,
    pub prev_total: i64,
    pub diff: i64,
    pub yoy_percent: f64,
}

#[derive(Serialize)]
pub struct CustomerYoyResponse {
    pub positive: Vec<CustomerYoy>,
    pub negative: Vec<CustomerYoy>,
    pub min_prev: i64,
    pub months: i64,
}

#[derive(Deserialize)]
pub struct CustomerYoyQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<usize>,
    pub min_prev: Option<i64>,
}

/// GET /api/sales/customer-yoy — 得意先別 YoY% ランキング（増加/減少）
pub async fn customer_yoy(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<CustomerYoyQuery>,
) -> Result<Json<ApiResponse<CustomerYoyResponse>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let limit = params.limit.unwrap_or(10).min(50);

    // 期間の月数を計算して min_prev を自動調整（月あたり約4万 = 年間50万相当）
    let from_parts: Vec<&str> = from.split('-').collect();
    let to_parts: Vec<&str> = to.split('-').collect();
    let from_y = from_parts[0].parse::<i32>().unwrap_or(2025);
    let from_m = from_parts.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(4);
    let to_y = to_parts[0].parse::<i32>().unwrap_or(2026);
    let to_m = to_parts.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(3);
    let months = ((to_y - from_y) * 12 + (to_m - from_m) + 1).max(1) as i64;
    let min_prev = params.min_prev.unwrap_or(months * 40_000); // 月あたり4万円

    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);

    // 前年の期間を計算
    let prev_from = format!(
        "{}-{}-01",
        from.split('-').next().unwrap_or("2024").parse::<i32>().unwrap_or(2024) - 1,
        from.split('-').nth(1).unwrap_or("04")
    );
    let prev_to = format!(
        "{}-{}-01",
        to.split('-').next().unwrap_or("2025").parse::<i32>().unwrap_or(2025) - 1,
        to.split('-').nth(1).unwrap_or("03")
    );

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let sql = "SELECT m.[得意先C], ISNULL(c.[得意先N], ''), \
               SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) \
               FROM [得意先別月計] m \
               LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
               WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
               GROUP BY m.[得意先C], c.[得意先N]";

    // 今期
    let stream = conn
        .query(sql, &[&from_date.as_str(), &to_date.as_str()])
        .await
        .map_err(|e| {
            tracing::error!("Query error (current): {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let cur_rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error (current): {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 前年
    let prev_stream = conn
        .query(sql, &[&prev_from.as_str(), &prev_to.as_str()])
        .await
        .map_err(|e| {
            tracing::error!("Query error (prev): {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let prev_rows = prev_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error (prev): {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 今期マップ: code -> (name, total)
    let mut cur_map = std::collections::HashMap::<String, (String, i64)>::new();
    for row in &cur_rows {
        let code = decode_cp932(row, 0);
        let name = decode_cp932(row, 1);
        let total = get_i64(row, 2);
        cur_map.insert(code, (name, total));
    }

    // 前年マップ: code -> (name, total)
    let mut prev_map = std::collections::HashMap::<String, (String, i64)>::new();
    for row in &prev_rows {
        let code = decode_cp932(row, 0);
        let name = decode_cp932(row, 1);
        let total = get_i64(row, 2);
        prev_map.insert(code, (name, total));
    }

    // マージして YoY% 計算
    let mut all_codes: std::collections::HashSet<String> = cur_map.keys().cloned().collect();
    all_codes.extend(prev_map.keys().cloned());

    let entries: Vec<CustomerYoy> = all_codes
        .into_iter()
        .filter_map(|code| {
            let (cur_name, cur_total) = cur_map.get(&code).cloned().unwrap_or_default();
            let (prev_name, prev_total) = prev_map.get(&code).cloned().unwrap_or_default();
            let name = if !cur_name.is_empty() { cur_name } else { prev_name };

            if prev_total < min_prev {
                return None;
            }

            let diff = cur_total - prev_total;
            let pct = (diff as f64 / prev_total as f64) * 100.0;
            let pct = (pct * 10.0).round() / 10.0;

            Some(CustomerYoy {
                customer_code: code,
                customer_name: name,
                current_total: cur_total,
                prev_total,
                diff,
                yoy_percent: pct,
            })
        })
        .collect();

    // positive: 前年売上 降順（大口から）
    let mut pos: Vec<CustomerYoy> = entries.iter().filter(|e| e.yoy_percent > 0.0).cloned().collect();
    pos.sort_by(|a, b| b.prev_total.cmp(&a.prev_total));
    let positive: Vec<CustomerYoy> = pos.into_iter().take(limit).collect();

    // negative: YoY% 昇順 → 同率なら前年売上 降順
    let mut neg: Vec<CustomerYoy> = entries.iter().filter(|e| e.yoy_percent < 0.0).cloned().collect();
    neg.sort_by(|a, b| {
        a.yoy_percent.partial_cmp(&b.yoy_percent).unwrap_or(std::cmp::Ordering::Equal)
            .then(b.prev_total.cmp(&a.prev_total))
    });
    let negative: Vec<CustomerYoy> = neg.into_iter().take(limit).collect();

    Ok(Json(ApiResponse {
        source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(),
        data: CustomerYoyResponse { positive, negative, min_prev, months },
    }))
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
) -> Result<Json<ApiResponse<Vec<YoyComparison>>>, StatusCode> {
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

    Ok(Json(ApiResponse {
        source_table: "種別別月計 (種別C=99)".to_string(),
        data,
    }))
}

// ── 日別売上 ──

#[derive(Serialize)]
pub struct DailySales {
    pub date: String,
    pub weekday: String,
    /// 税抜金額+税抜割増+税抜実費-値引（月計テーブルと一致する値）
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    /// 金額+割増+実費-値引（税処理混在、参考値）
    pub own_sales_raw: i64,
    pub charter_sales_raw: i64,
    pub total_sales_raw: i64,
    pub transport_count: i32,
    pub prev_year_total: i64,
    pub prev_year_total_raw: i64,
}

#[derive(Deserialize)]
pub struct DailyQuery {
    pub month: Option<String>,
    /// billing: 請求(0,1) / non_billing: 非請求(0,2) / all: 全て
    pub mode: Option<String>,
}

/// GET /api/sales/daily?month=2025-04&mode=billing — 日別売上 + 前年同日
/// mode: billing (請求K IN 0,1), non_billing (請求K IN 0,2), all (全て, デフォルト)
pub async fn daily(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<DailyQuery>,
) -> Result<Json<ApiResponse<Vec<DailySales>>>, StatusCode> {
    let month = params.month.unwrap_or_else(|| "2026-03".to_string());
    let mode = params.mode.unwrap_or_else(|| "all".to_string());
    let from_date = format!("{}-01", month);
    let parts: Vec<&str> = month.split('-').collect();
    let y: i32 = parts[0].parse().unwrap_or(2026);
    let m: i32 = parts[1].parse().unwrap_or(3);
    let (ny, nm) = if m >= 12 { (y + 1, 1) } else { (y, m + 1) };
    let to_date = format!("{}-{:02}-01", ny, nm);

    // 請求K フィルタ
    let billing_filter = match mode.as_str() {
        "billing" => "AND [請求K] IN ('0', '1')",
        "non_billing" => "AND [請求K] IN ('0', '2')",
        _ => "",
    };

    let mode_label = match mode.as_str() {
        "billing" => "請求+請求のみ",
        "non_billing" => "請求+非請求",
        _ => "全て",
    };

    // 前年同月の期間
    let prev_from = format!("{}-{:02}-01", y - 1, m);
    let (pny, pnm) = if m >= 12 { (y, 1) } else { (y - 1, m + 1) };
    let prev_to = format!("{}-{:02}-01", pny, pnm);

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // 今年 — 税抜(月計一致) + raw(金額ベース) を同時取得
    let query = format!(
        "SELECT [売上年月日], \
         SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0)), \
         SUM(ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), \
         SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0)), \
         SUM(ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0)), \
         COUNT(*) \
         FROM [運転日報明細] \
         WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 {} \
         GROUP BY [売上年月日] \
         ORDER BY [売上年月日]",
        billing_filter
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

    // 前年同月 — 両方取得
    let prev_query = format!(
        "SELECT [売上年月日], \
         SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0) \
            +ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), \
         SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0) \
            +ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0)) \
         FROM [運転日報明細] \
         WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 {} \
         GROUP BY [売上年月日] \
         ORDER BY [売上年月日]",
        billing_filter
    );

    let prev_stream = conn
        .query(&prev_query, &[&prev_from.as_str(), &prev_to.as_str()])
        .await
        .map_err(|e| {
            tracing::error!("Query error (prev): {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let prev_rows = prev_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error (prev): {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 前年データを日(dd)でマッピング (税抜, raw)
    let mut prev_map = std::collections::HashMap::new();
    let mut prev_raw_map = std::collections::HashMap::new();
    for row in &prev_rows {
        let dt: chrono::NaiveDateTime = row.get(0).unwrap_or_default();
        let day = dt.format("%d").to_string();
        prev_map.insert(day.clone(), get_i64(row, 1));
        prev_raw_map.insert(day, get_i64(row, 2));
    }

    let weekdays = ["日", "月", "火", "水", "木", "金", "土"];

    let data: Vec<DailySales> = rows
        .iter()
        .map(|row| {
            let dt: chrono::NaiveDateTime = row.get(0).unwrap_or_default();
            let own = get_i64(row, 1);         // 税抜自車
            let charter = get_i64(row, 2);     // 税抜傭車
            let own_raw = get_i64(row, 3);     // raw自車
            let charter_raw = get_i64(row, 4); // raw傭車
            let day = dt.format("%d").to_string();
            let wd = dt.weekday().num_days_from_sunday() as usize;
            DailySales {
                date: dt.format("%Y-%m-%d").to_string(),
                weekday: weekdays[wd].to_string(),
                own_sales: own,
                charter_sales: charter,
                total_sales: own + charter,
                own_sales_raw: own_raw,
                charter_sales_raw: charter_raw,
                total_sales_raw: own_raw + charter_raw,
                transport_count: get_i32(row, 5),
                prev_year_total: prev_map.get(&day).copied().unwrap_or(0),
                prev_year_total_raw: prev_raw_map.get(&day).copied().unwrap_or(0),
            }
        })
        .collect();

    Ok(Json(ApiResponse {
        source_table: format!("運転日報明細 [{}]", mode_label),
        data,
    }))
}

// ── 得意先別月別推移（バンプチャート用） ──

#[derive(Serialize)]
pub struct CustomerMonthly {
    pub customer_code: String,
    pub customer_name: String,
    pub months: Vec<CustomerMonthData>,
}

#[derive(Serialize)]
pub struct CustomerMonthData {
    pub year_month: String,
    pub total_sales: i64,
    pub rank: i32,
}

#[derive(Deserialize)]
pub struct CustomerTrendQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i32>,
}

/// GET /api/sales/customer-trend — 得意先別月別推移（上位N社の順位変動）
pub async fn customer_trend(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<CustomerTrendQuery>,
) -> Result<Json<ApiResponse<Vec<CustomerMonthly>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let limit = params.limit.unwrap_or(20);
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // まず期間合計で上位N社を特定
    let top_query = format!(
        "SELECT TOP {} m.[得意先C], ISNULL(c.[得意先N], '') \
         FROM [得意先別月計] m \
         LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
         WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
         GROUP BY m.[得意先C], c.[得意先N] \
         ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC",
        limit.min(30)
    );

    let top_stream = conn
        .query(&top_query, &[&from_date.as_str(), &to_date.as_str()])
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let top_rows = top_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let top_customers: Vec<(String, String)> = top_rows
        .iter()
        .map(|row| (decode_cp932(row, 0), decode_cp932(row, 1)))
        .collect();

    if top_customers.is_empty() {
        return Ok(Json(ApiResponse {
            source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(),
            data: vec![],
        }));
    }

    // 全得意先の月別データを取得（順位計算のため）
    let monthly_stream = conn
        .query(
            "SELECT m.[得意先C], m.[年月度], \
             SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) as total \
             FROM [得意先別月計] m \
             WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
             GROUP BY m.[得意先C], m.[年月度] \
             ORDER BY m.[年月度], total DESC",
            &[&from_date.as_str(), &to_date.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let monthly_rows = monthly_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    // 月別 → 得意先の売上マップ + 月別順位
    let mut month_data: std::collections::BTreeMap<String, Vec<(String, i64)>> =
        std::collections::BTreeMap::new();

    for row in &monthly_rows {
        let code = decode_cp932(row, 0);
        let dt: chrono::NaiveDateTime = row.get(1).unwrap_or_default();
        let ym = dt.format("%Y-%m").to_string();
        let total = get_i64(row, 2);
        month_data.entry(ym).or_default().push((code, total));
    }

    // 各月の順位を計算
    let mut month_ranks: std::collections::HashMap<String, std::collections::HashMap<String, (i64, i32)>> =
        std::collections::HashMap::new();

    for (ym, mut entries) in &mut month_data {
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        let ranked: std::collections::HashMap<String, (i64, i32)> = entries
            .iter()
            .enumerate()
            .map(|(i, (code, total))| (code.clone(), (*total, (i + 1) as i32)))
            .collect();
        month_ranks.insert(ym.clone(), ranked);
    }

    let months: Vec<String> = month_data.keys().cloned().collect();

    // 上位N社のデータを組み立て
    let data: Vec<CustomerMonthly> = top_customers
        .iter()
        .map(|(code, name)| {
            let month_entries: Vec<CustomerMonthData> = months
                .iter()
                .map(|ym| {
                    let (total, rank) = month_ranks
                        .get(ym)
                        .and_then(|m| m.get(code))
                        .copied()
                        .unwrap_or((0, 0));
                    CustomerMonthData {
                        year_month: ym.clone(),
                        total_sales: total,
                        rank,
                    }
                })
                .collect();
            CustomerMonthly {
                customer_code: code.clone(),
                customer_name: name.clone(),
                months: month_entries,
            }
        })
        .collect();

    Ok(Json(ApiResponse {
        source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(),
        data,
    }))
}

// ── 得意先別月別売上詳細（通年） ──

#[derive(Serialize)]
pub struct CustomerDetailMonth {
    pub year_month: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Serialize)]
pub struct CustomerDetailResponse {
    pub customer_code: String,
    pub customer_name: String,
    pub months: Vec<CustomerDetailMonth>,
}

#[derive(Deserialize)]
pub struct CustomerDetailQuery {
    pub code: String,
}

/// GET /api/sales/customer-detail?code=XXXXXX — 特定得意先の全期間月別売上
pub async fn customer_detail(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<CustomerDetailQuery>,
) -> Result<Json<ApiResponse<CustomerDetailResponse>>, StatusCode> {
    let code = params.code;

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // 得意先名を取得
    let name_stream = conn
        .query(
            "SELECT TOP 1 ISNULL(c.[得意先N], '') \
             FROM [得意先ﾏｽﾀ] c \
             WHERE c.[得意先C] = @P1",
            &[&code.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error (name): {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let name_rows = name_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error (name): {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let customer_name = name_rows.first().map(|r| decode_cp932(r, 0)).unwrap_or_default();

    // 全期間の月別売上
    let stream = conn
        .query(
            "SELECT m.[年月度], \
             SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
             FROM [得意先別月計] m \
             WHERE m.[得意先C] = @P1 \
             GROUP BY m.[年月度] \
             ORDER BY m.[年月度]",
            &[&code.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error (detail): {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    let rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error (detail): {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let months: Vec<CustomerDetailMonth> = rows
        .iter()
        .map(|row| {
            let dt: chrono::NaiveDateTime = row.get(0).unwrap_or_default();
            let own = get_i64(row, 1);
            let charter = get_i64(row, 2);
            CustomerDetailMonth {
                year_month: dt.format("%Y-%m").to_string(),
                own_sales: own,
                charter_sales: charter,
                total_sales: own + charter,
                transport_count: get_i64(row, 3) as i32,
            }
        })
        .collect();

    Ok(Json(ApiResponse {
        source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(),
        data: CustomerDetailResponse {
            customer_code: code,
            customer_name,
            months,
        },
    }))
}
