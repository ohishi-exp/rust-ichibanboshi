use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::{Datelike, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::repo::{DynRepo, RepoError};

/// 全エンドポイント共通のレスポンスラッパー
#[derive(Serialize)]
pub struct ApiResponse<T: Serialize> {
    pub source_table: String,
    pub data: T,
}

fn map_repo_err(e: RepoError) -> StatusCode {
    match &e {
        RepoError::PoolError => StatusCode::SERVICE_UNAVAILABLE,
        RepoError::QueryError(msg) => {
            tracing::error!("Query error: {msg}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

// ══════════════════════════════════════════════════════════════
// 共通ロジック (純粋関数 — テスト可能)
// ══════════════════════════════════════════════════════════════

/// 月文字列 "YYYY-MM" から前年同期間の日付文字列を計算
pub fn calc_prev_period(from: &str, to: &str) -> (String, String) {
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
    (prev_from, prev_to)
}

/// daily の翌月初日を計算
pub fn calc_next_month(y: i32, m: i32) -> (i32, i32) {
    if m >= 12 { (y + 1, 1) } else { (y, m + 1) }
}

/// customer_yoy の月数計算
pub fn calc_months(from: &str, to: &str) -> i64 {
    let from_parts: Vec<&str> = from.split('-').collect();
    let to_parts: Vec<&str> = to.split('-').collect();
    let from_y = from_parts[0].parse::<i32>().unwrap_or(2025);
    let from_m = from_parts.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(4);
    let to_y = to_parts[0].parse::<i32>().unwrap_or(2026);
    let to_m = to_parts.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(3);
    ((to_y - from_y) * 12 + (to_m - from_m) + 1).max(1) as i64
}

// ══════════════════════════════════════════════════════════════
// Raw 中間構造体 (DB 層 → ロジック層 の橋渡し)
// ══════════════════════════════════════════════════════════════

#[derive(Debug, Clone)]
pub struct RawMonthlyRow {
    pub year_month: NaiveDateTime,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub transport_count: i32,
}

#[derive(Debug, Clone)]
pub struct RawDepartmentRow {
    pub department_code: String,
    pub department_name: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub transport_count: i64,
}

#[derive(Debug, Clone)]
pub struct RawCustomerRow {
    pub customer_code: String,
    pub customer_name: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub transport_count: i64,
}

pub type CodeTotalMap = HashMap<String, (String, i64)>;

#[derive(Debug, Clone)]
pub struct RawMonthTotalRow {
    pub month: i32,
    pub total: i64,
}

#[derive(Debug, Clone)]
pub struct RawDailyRow {
    pub date: NaiveDateTime,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub own_sales_raw: i64,
    pub charter_sales_raw: i64,
    pub transport_count: i32,
}

#[derive(Debug, Clone)]
pub struct RawDailyPrevRow {
    pub date: NaiveDateTime,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub own_sales_raw: i64,
    pub charter_sales_raw: i64,
}

#[derive(Debug, Clone)]
pub struct RawCustomerMonthlyRow {
    pub customer_code: String,
    pub year_month: NaiveDateTime,
    pub total: i64,
}

#[derive(Debug, Clone)]
pub struct RawCustomerDetailRow {
    pub year_month: NaiveDateTime,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub transport_count: i64,
}

#[derive(Debug, Clone)]
pub struct RawCustomerDeptRow {
    pub department_code: String,
    pub department_name: String,
    pub customer_code: String,
    pub customer_name: String,
    pub total: i64,
}

/// (department_code, customer_code) → (department_name, customer_name, total)
pub type DeptCustomerTotalMap = HashMap<(String, String), (String, String, i64)>;

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct MonthlySales { pub year_month: String, pub own_sales: i64, pub charter_sales: i64, pub total_sales: i64, pub transport_count: i32, pub prev_year_own: i64, pub prev_year_charter: i64, pub prev_year_total: i64 }

#[derive(Serialize, Debug, PartialEq)]
pub struct DepartmentSales { pub department_code: String, pub department_name: String, pub own_sales: i64, pub charter_sales: i64, pub total_sales: i64, pub transport_count: i32 }

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerSales { pub customer_code: String, pub customer_name: String, pub own_sales: i64, pub charter_sales: i64, pub total_sales: i64, pub transport_count: i32 }

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct CustomerYoy { pub customer_code: String, pub customer_name: String, pub current_total: i64, pub prev_total: i64, pub diff: i64, pub yoy_percent: f64 }

#[derive(Serialize, Debug)]
pub struct CustomerYoyResponse { pub positive: Vec<CustomerYoy>, pub negative: Vec<CustomerYoy>, pub min_prev: i64, pub months: i64 }

#[derive(Serialize, Debug, PartialEq)]
pub struct YoyComparison { pub month: String, pub current_year: i64, pub previous_year: i64, pub diff: i64, pub diff_percent: f64 }

#[derive(Serialize, Debug, PartialEq)]
pub struct DailySales { pub date: String, pub weekday: String, pub own_sales: i64, pub charter_sales: i64, pub total_sales: i64, pub own_sales_raw: i64, pub charter_sales_raw: i64, pub total_sales_raw: i64, pub transport_count: i32, pub prev_year_own: i64, pub prev_year_charter: i64, pub prev_year_total: i64, pub prev_year_own_raw: i64, pub prev_year_charter_raw: i64, pub prev_year_total_raw: i64 }

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerMonthly { pub customer_code: String, pub customer_name: String, pub months: Vec<CustomerMonthData> }

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerMonthData { pub year_month: String, pub total_sales: i64, pub rank: i32 }

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerDetailMonth { pub year_month: String, pub own_sales: i64, pub charter_sales: i64, pub total_sales: i64, pub transport_count: i32 }

#[derive(Serialize, Debug)]
pub struct CustomerDetailResponse { pub customer_code: String, pub customer_name: String, pub months: Vec<CustomerDetailMonth> }

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct CustomerYoyWithDept {
    pub department_code: String,
    pub department_name: String,
    pub customer_code: String,
    pub customer_name: String,
    pub current_total: i64,
    pub prev_total: i64,
    pub diff: i64,
    pub yoy_percent: f64,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct DepartmentOption {
    pub department_code: String,
    pub department_name: String,
}

#[derive(Serialize, Debug)]
pub struct CustomerYoyByDeptResponse {
    pub positive: Vec<CustomerYoyWithDept>,
    pub negative: Vec<CustomerYoyWithDept>,
    pub months: i64,
    pub min_prev: i64,
    pub department_code: Option<String>,
    pub departments: Vec<DepartmentOption>,
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct MonthlyQuery { pub from: Option<String>, pub to: Option<String>, pub exclude_dept: Option<String> }
#[derive(Deserialize)]
pub struct PeriodQuery { pub from: Option<String>, pub to: Option<String> }
#[derive(Deserialize)]
pub struct CustomerQuery { pub from: Option<String>, pub to: Option<String>, pub limit: Option<i32> }
#[derive(Deserialize)]
pub struct CustomerYoyQuery { pub from: Option<String>, pub to: Option<String>, pub limit: Option<usize>, pub min_prev: Option<i64> }
#[derive(Deserialize)]
pub struct YoyQuery { pub year: Option<i32> }
#[derive(Deserialize)]
pub struct DailyQuery { pub month: Option<String>, pub mode: Option<String>, pub exclude_dept: Option<String> }
#[derive(Deserialize)]
pub struct CustomerTrendQuery { pub from: Option<String>, pub to: Option<String>, pub limit: Option<i32> }
#[derive(Deserialize)]
pub struct CustomerDetailQuery { pub code: String }
#[derive(Deserialize)]
pub struct CustomerYoyByDeptQuery { pub from: Option<String>, pub to: Option<String>, pub limit: Option<usize>, pub min_prev: Option<i64>, pub department_code: Option<String> }

// ══════════════════════════════════════════════════════════════
// ロジック層 (純粋関数)
// ══════════════════════════════════════════════════════════════

pub fn build_monthly_sales(current: &[RawMonthlyRow], prev: &[RawMonthlyRow]) -> Vec<MonthlySales> {
    let mut prev_map = HashMap::new();
    for r in prev {
        prev_map.insert(r.year_month.format("%m").to_string(), (r.own_sales, r.charter_sales));
    }
    current.iter().map(|r| {
        let month = r.year_month.format("%m").to_string();
        MonthlySales {
            year_month: r.year_month.format("%Y-%m").to_string(),
            own_sales: r.own_sales, charter_sales: r.charter_sales, total_sales: r.own_sales + r.charter_sales,
            transport_count: r.transport_count,
            prev_year_own: prev_map.get(&month).map(|v| v.0).unwrap_or(0),
            prev_year_charter: prev_map.get(&month).map(|v| v.1).unwrap_or(0),
            prev_year_total: prev_map.get(&month).map(|v| v.0 + v.1).unwrap_or(0),
        }
    }).collect()
}

pub fn build_department_sales(raw: &[RawDepartmentRow]) -> Vec<DepartmentSales> {
    raw.iter().map(|r| DepartmentSales {
        department_code: r.department_code.clone(), department_name: r.department_name.clone(),
        own_sales: r.own_sales, charter_sales: r.charter_sales, total_sales: r.own_sales + r.charter_sales,
        transport_count: r.transport_count as i32,
    }).collect()
}

pub fn build_customer_sales(raw: &[RawCustomerRow]) -> Vec<CustomerSales> {
    raw.iter().map(|r| CustomerSales {
        customer_code: r.customer_code.clone(), customer_name: r.customer_name.clone(),
        own_sales: r.own_sales, charter_sales: r.charter_sales, total_sales: r.own_sales + r.charter_sales,
        transport_count: r.transport_count as i32,
    }).collect()
}

pub fn calc_yoy_entries(cur_map: &CodeTotalMap, prev_map: &CodeTotalMap, min_prev: i64) -> Vec<CustomerYoy> {
    let mut all_codes: HashSet<String> = cur_map.keys().cloned().collect();
    all_codes.extend(prev_map.keys().cloned());
    all_codes.into_iter().filter_map(|code| {
        let (cur_name, cur_total) = cur_map.get(&code).cloned().unwrap_or_default();
        let (prev_name, prev_total) = prev_map.get(&code).cloned().unwrap_or_default();
        let name = if !cur_name.is_empty() { cur_name } else { prev_name };
        if prev_total < min_prev { return None; }
        let diff = cur_total - prev_total;
        let pct = ((diff as f64 / prev_total as f64) * 1000.0).round() / 10.0;
        Some(CustomerYoy { customer_code: code, customer_name: name, current_total: cur_total, prev_total, diff, yoy_percent: pct })
    }).collect()
}

pub fn split_and_sort_yoy(entries: Vec<CustomerYoy>, limit: usize) -> (Vec<CustomerYoy>, Vec<CustomerYoy>) {
    let mut pos: Vec<_> = entries.iter().filter(|e| e.yoy_percent > 0.0).cloned().collect();
    pos.sort_by(|a, b| b.prev_total.cmp(&a.prev_total));
    let mut neg: Vec<_> = entries.iter().filter(|e| e.yoy_percent < 0.0).cloned().collect();
    neg.sort_by(|a, b| a.yoy_percent.partial_cmp(&b.yoy_percent).unwrap_or(std::cmp::Ordering::Equal).then(b.prev_total.cmp(&a.prev_total)));
    (pos.into_iter().take(limit).collect(), neg.into_iter().take(limit).collect())
}

pub fn build_yoy_comparison(current: &[RawMonthTotalRow], prev: &[RawMonthTotalRow]) -> Vec<YoyComparison> {
    let mut prev_map = HashMap::new();
    for r in prev { prev_map.insert(r.month, r.total); }
    current.iter().map(|r| {
        let previous = prev_map.get(&r.month).copied().unwrap_or(0);
        let diff = r.total - previous;
        let diff_percent = if previous > 0 { (diff as f64 / previous as f64) * 100.0 } else { 0.0 };
        YoyComparison { month: format!("{:02}", r.month), current_year: r.total, previous_year: previous, diff, diff_percent: (diff_percent * 10.0).round() / 10.0 }
    }).collect()
}

static WEEKDAYS: [&str; 7] = ["日", "月", "火", "水", "木", "金", "土"];

pub fn build_daily_sales(current: &[RawDailyRow], prev: &[RawDailyPrevRow]) -> Vec<DailySales> {
    let mut prev_map = HashMap::new();
    for r in prev { prev_map.insert(r.date.format("%d").to_string(), (r.own_sales, r.charter_sales, r.own_sales_raw, r.charter_sales_raw)); }
    current.iter().map(|r| {
        let day = r.date.format("%d").to_string();
        let wd = r.date.weekday().num_days_from_sunday() as usize;
        DailySales {
            date: r.date.format("%Y-%m-%d").to_string(), weekday: WEEKDAYS[wd].to_string(),
            own_sales: r.own_sales, charter_sales: r.charter_sales, total_sales: r.own_sales + r.charter_sales,
            own_sales_raw: r.own_sales_raw, charter_sales_raw: r.charter_sales_raw, total_sales_raw: r.own_sales_raw + r.charter_sales_raw,
            transport_count: r.transport_count,
            prev_year_own: prev_map.get(&day).map(|v| v.0).unwrap_or(0),
            prev_year_charter: prev_map.get(&day).map(|v| v.1).unwrap_or(0),
            prev_year_total: prev_map.get(&day).map(|v| v.0 + v.1).unwrap_or(0),
            prev_year_own_raw: prev_map.get(&day).map(|v| v.2).unwrap_or(0),
            prev_year_charter_raw: prev_map.get(&day).map(|v| v.3).unwrap_or(0),
            prev_year_total_raw: prev_map.get(&day).map(|v| v.2 + v.3).unwrap_or(0),
        }
    }).collect()
}

pub fn mode_label(mode: &str) -> &'static str {
    match mode { "billing" => "請求+請求のみ", "non_billing" => "請求+非請求", _ => "全て" }
}

pub fn build_customer_trend(top_customers: &[(String, String)], monthly_raw: &[RawCustomerMonthlyRow]) -> Vec<CustomerMonthly> {
    if top_customers.is_empty() { return vec![]; }
    let mut month_data: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
    for r in monthly_raw { month_data.entry(r.year_month.format("%Y-%m").to_string()).or_default().push((r.customer_code.clone(), r.total)); }
    let mut month_ranks: HashMap<String, HashMap<String, (i64, i32)>> = HashMap::new();
    for (ym, entries) in &mut month_data {
        entries.sort_by(|a, b| b.1.cmp(&a.1));
        month_ranks.insert(ym.clone(), entries.iter().enumerate().map(|(i, (c, t))| (c.clone(), (*t, (i + 1) as i32))).collect());
    }
    let months: Vec<String> = month_data.keys().cloned().collect();
    top_customers.iter().map(|(code, name)| {
        CustomerMonthly {
            customer_code: code.clone(), customer_name: name.clone(),
            months: months.iter().map(|ym| {
                let (total, rank) = month_ranks.get(ym).and_then(|m| m.get(code)).copied().unwrap_or((0, 0));
                CustomerMonthData { year_month: ym.clone(), total_sales: total, rank }
            }).collect(),
        }
    }).collect()
}

pub fn build_customer_detail(raw: &[RawCustomerDetailRow]) -> Vec<CustomerDetailMonth> {
    raw.iter().map(|r| CustomerDetailMonth {
        year_month: r.year_month.format("%Y-%m").to_string(),
        own_sales: r.own_sales, charter_sales: r.charter_sales, total_sales: r.own_sales + r.charter_sales,
        transport_count: r.transport_count as i32,
    }).collect()
}

// ══════════════════════════════════════════════════════════════
// ハンドラ (薄い — param 解析 → repo → build → JSON)
// ══════════════════════════════════════════════════════════════

pub async fn monthly(Extension(repo): Extension<DynRepo>, Query(params): Query<MonthlyQuery>) -> Result<Json<ApiResponse<Vec<MonthlySales>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);
    let (prev_from, prev_to) = calc_prev_period(&from, &to);
    let (source_table, current, prev) = repo.monthly(&from_date, &to_date, &prev_from, &prev_to, params.exclude_dept.as_deref()).await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse { source_table, data: build_monthly_sales(&current, &prev) }))
}

pub async fn by_department(Extension(repo): Extension<DynRepo>, Query(params): Query<PeriodQuery>) -> Result<Json<ApiResponse<Vec<DepartmentSales>>>, StatusCode> {
    let from_date = format!("{}-01", params.from.unwrap_or_else(|| "2025-04".to_string()));
    let to_date = format!("{}-01", params.to.unwrap_or_else(|| "2026-03".to_string()));
    let raw = repo.by_department(&from_date, &to_date).await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse { source_table: "部門別月計 + 部門ﾏｽﾀ".to_string(), data: build_department_sales(&raw) }))
}

pub async fn by_customer(Extension(repo): Extension<DynRepo>, Query(params): Query<CustomerQuery>) -> Result<Json<ApiResponse<Vec<CustomerSales>>>, StatusCode> {
    let from_date = format!("{}-01", params.from.unwrap_or_else(|| "2025-04".to_string()));
    let to_date = format!("{}-01", params.to.unwrap_or_else(|| "2026-03".to_string()));
    let limit = params.limit.unwrap_or(20);
    let raw = repo.by_customer(&from_date, &to_date, limit).await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse { source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(), data: build_customer_sales(&raw) }))
}

pub async fn customer_yoy(Extension(repo): Extension<DynRepo>, Query(params): Query<CustomerYoyQuery>) -> Result<Json<ApiResponse<CustomerYoyResponse>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let limit = params.limit.unwrap_or(10).min(50);
    let months = calc_months(&from, &to);
    let min_prev = params.min_prev.unwrap_or(months * 40_000);
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);
    let (prev_from, prev_to) = calc_prev_period(&from, &to);
    let (cur_map, prev_map) = repo.customer_yoy_data(&from_date, &to_date, &prev_from, &prev_to).await.map_err(map_repo_err)?;
    let entries = calc_yoy_entries(&cur_map, &prev_map, min_prev);
    let (positive, negative) = split_and_sort_yoy(entries, limit);
    Ok(Json(ApiResponse { source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(), data: CustomerYoyResponse { positive, negative, min_prev, months } }))
}

pub async fn yoy(Extension(repo): Extension<DynRepo>, Query(params): Query<YoyQuery>) -> Result<Json<ApiResponse<Vec<YoyComparison>>>, StatusCode> {
    let year = params.year.unwrap_or(2026);
    let (current, prev) = repo.yoy_data(year).await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse { source_table: "種別別月計 (種別C=99)".to_string(), data: build_yoy_comparison(&current, &prev) }))
}

pub async fn daily(Extension(repo): Extension<DynRepo>, Query(params): Query<DailyQuery>) -> Result<Json<ApiResponse<Vec<DailySales>>>, StatusCode> {
    let month = params.month.unwrap_or_else(|| "2026-03".to_string());
    let mode = params.mode.unwrap_or_else(|| "all".to_string());
    let exclude_dept = params.exclude_dept;
    let from_date = format!("{}-01", month);
    let parts: Vec<&str> = month.split('-').collect();
    let y: i32 = parts[0].parse().unwrap_or(2026);
    let m: i32 = parts[1].parse().unwrap_or(3);
    let (ny, nm) = calc_next_month(y, m);
    let to_date = format!("{}-{:02}-01", ny, nm);
    let billing_filter = match mode.as_str() { "billing" => "AND [請求K] IN ('0', '1')", "non_billing" => "AND [請求K] IN ('0', '2')", _ => "" };
    let dept_filter = if exclude_dept.is_some() { "AND [受注部門] NOT IN (SELECT [部門C] FROM [部門ﾏｽﾀ] WHERE [部門N] LIKE @P3)" } else { "" };
    let dept_label = exclude_dept.as_deref().unwrap_or("");
    let exclude_pattern = exclude_dept.as_ref().map(|d| format!("%{}%", d)).unwrap_or_default();
    let prev_from = format!("{}-{:02}-01", y - 1, m);
    let (pny, pnm) = calc_next_month(y - 1, m);
    let prev_to = format!("{}-{:02}-01", pny, pnm);
    let (current, prev) = repo.daily(&from_date, &to_date, &prev_from, &prev_to, billing_filter, dept_filter, &exclude_pattern).await.map_err(map_repo_err)?;
    let ml = mode_label(&mode);
    Ok(Json(ApiResponse {
        source_table: if dept_label.is_empty() { format!("運転日報明細 [{}]", ml) } else { format!("運転日報明細 [{}, {}除く]", ml, dept_label) },
        data: build_daily_sales(&current, &prev),
    }))
}

pub async fn customer_trend(Extension(repo): Extension<DynRepo>, Query(params): Query<CustomerTrendQuery>) -> Result<Json<ApiResponse<Vec<CustomerMonthly>>>, StatusCode> {
    let from_date = format!("{}-01", params.from.unwrap_or_else(|| "2025-04".to_string()));
    let to_date = format!("{}-01", params.to.unwrap_or_else(|| "2026-03".to_string()));
    let limit = params.limit.unwrap_or(20);
    let (top, monthly_raw) = repo.customer_trend_data(&from_date, &to_date, limit).await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse { source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(), data: build_customer_trend(&top, &monthly_raw) }))
}

pub async fn customer_detail(Extension(repo): Extension<DynRepo>, Query(params): Query<CustomerDetailQuery>) -> Result<Json<ApiResponse<CustomerDetailResponse>>, StatusCode> {
    let (customer_name, raw) = repo.customer_detail_data(&params.code).await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse {
        source_table: "得意先別月計 + 得意先ﾏｽﾀ".to_string(),
        data: CustomerDetailResponse { customer_code: params.code, customer_name, months: build_customer_detail(&raw) },
    }))
}

// ══════════════════════════════════════════════════════════════
// customer-yoy-by-dept (営業所×得意先の前年同期比)
// ══════════════════════════════════════════════════════════════

/// Raw 行リストを (営業所, 得意先) キーの DeptCustomerTotalMap に変換
pub fn rows_to_dept_customer_map(rows: &[RawCustomerDeptRow]) -> DeptCustomerTotalMap {
    let mut map: DeptCustomerTotalMap = HashMap::new();
    for r in rows {
        map.insert(
            (r.department_code.clone(), r.customer_code.clone()),
            (r.department_name.clone(), r.customer_name.clone(), r.total),
        );
    }
    map
}

/// current / prev の DeptCustomerTotalMap を突き合わせて前年同期比エントリ生成
pub fn calc_yoy_with_dept_entries(
    cur_map: &DeptCustomerTotalMap,
    prev_map: &DeptCustomerTotalMap,
    min_prev: i64,
) -> Vec<CustomerYoyWithDept> {
    let mut all_keys: HashSet<(String, String)> = cur_map.keys().cloned().collect();
    all_keys.extend(prev_map.keys().cloned());
    all_keys
        .into_iter()
        .filter_map(|key| {
            let cur = cur_map.get(&key).cloned().unwrap_or_default();
            let prev = prev_map.get(&key).cloned().unwrap_or_default();
            let (dept_code, cust_code) = key;
            let dept_name = if !cur.0.is_empty() { cur.0 } else { prev.0 };
            let cust_name = if !cur.1.is_empty() { cur.1 } else { prev.1 };
            let cur_total = cur.2;
            let prev_total = prev.2;
            if prev_total < min_prev {
                return None;
            }
            let diff = cur_total - prev_total;
            let pct = ((diff as f64 / prev_total as f64) * 1000.0).round() / 10.0;
            Some(CustomerYoyWithDept {
                department_code: dept_code,
                department_name: dept_name,
                customer_code: cust_code,
                customer_name: cust_name,
                current_total: cur_total,
                prev_total,
                diff,
                yoy_percent: pct,
            })
        })
        .collect()
}

pub fn split_and_sort_yoy_with_dept(
    entries: Vec<CustomerYoyWithDept>,
    limit: usize,
) -> (Vec<CustomerYoyWithDept>, Vec<CustomerYoyWithDept>) {
    let mut pos: Vec<_> = entries.iter().filter(|e| e.yoy_percent > 0.0).cloned().collect();
    pos.sort_by(|a, b| b.prev_total.cmp(&a.prev_total));
    let mut neg: Vec<_> = entries.iter().filter(|e| e.yoy_percent < 0.0).cloned().collect();
    neg.sort_by(|a, b| {
        a.yoy_percent
            .partial_cmp(&b.yoy_percent)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(b.prev_total.cmp(&a.prev_total))
    });
    (pos.into_iter().take(limit).collect(), neg.into_iter().take(limit).collect())
}

pub async fn customer_yoy_by_dept(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<CustomerYoyByDeptQuery>,
) -> Result<Json<ApiResponse<CustomerYoyByDeptResponse>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let limit = params.limit.unwrap_or(10).min(50);
    let months = calc_months(&from, &to);
    let min_prev = params.min_prev.unwrap_or(months * 40_000);
    let from_date = format!("{}-01", from);
    let to_date = format!("{}-01", to);
    let (prev_from, prev_to) = calc_prev_period(&from, &to);

    let dept_code = params
        .department_code
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    let (cur_rows, prev_rows) = repo
        .customer_yoy_by_dept_data(
            &from_date,
            &to_date,
            &prev_from,
            &prev_to,
            dept_code.as_deref(),
        )
        .await
        .map_err(map_repo_err)?;

    let cur_map = rows_to_dept_customer_map(&cur_rows);
    let prev_map = rows_to_dept_customer_map(&prev_rows);
    let entries = calc_yoy_with_dept_entries(&cur_map, &prev_map, min_prev);
    let (positive, negative) = split_and_sort_yoy_with_dept(entries, limit);

    let departments = repo
        .list_departments()
        .await
        .map_err(map_repo_err)?
        .into_iter()
        .map(|(code, name)| DepartmentOption { department_code: code, department_name: name })
        .collect();

    Ok(Json(ApiResponse {
        source_table: "運転日報明細 + 部門ﾏｽﾀ + 得意先ﾏｽﾀ".to_string(),
        data: CustomerYoyByDeptResponse {
            positive,
            negative,
            months,
            min_prev,
            department_code: dept_code,
            departments,
        },
    }))
}

pub async fn list_departments_handler(
    Extension(repo): Extension<DynRepo>,
) -> Result<Json<ApiResponse<Vec<DepartmentOption>>>, StatusCode> {
    let rows = repo.list_departments().await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse {
        source_table: "部門ﾏｽﾀ".to_string(),
        data: rows
            .into_iter()
            .map(|(code, name)| DepartmentOption { department_code: code, department_name: name })
            .collect(),
    }))
}
