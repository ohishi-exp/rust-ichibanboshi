//! `/api/sales/daily`・`/api/sales/customer-trend`・`/api/sales/customer-detail` の SQL・Raw 型・応答型・Query・組み立て。
//! オンプレ版 `src/routes/sales.rs` の `daily`・`customer_trend`・`customer_detail` と `src/repo.rs` の
//! `daily`・`customer_trend_data`・`customer_detail_data` を同じ挙動のまま写した (Refs #322)。
//!
//! SQL の文字列はオンプレ版と 1 文字も変えない。mode・除外部門で切り替える部分は、オンプレ版が生の SQL 片を
//! 渡していたところを [`DailyMode`] と bool からの `&'static str` に置き換えた (組み上がる文字列は同じ)。
//! 400 にする入力はオンプレ版が panic / 500 になるものだけ: daily の `month` に `-` が無い
//! ([`DailyQuery::plan`])、customer-trend の `limit` ≤ 0 ([`CustomerTrendQuery::plan`])。

use std::collections::{BTreeMap, HashMap};

use chrono::{Datelike, NaiveDateTime};
use serde::{Deserialize, Serialize};

use crate::period::calc_next_month;

/// customer-trend・customer-detail の `source_table`。
pub const CUSTOMER_SOURCE: &str = "得意先別月計 + 得意先ﾏｽﾀ";

// ══════════════════════════════════════════════════════════════
// SQL
// ══════════════════════════════════════════════════════════════

/// daily の当期・前年で共通の SELECT (日付 + 自車・傭車の税抜 + 自車・傭車の `_raw`)。
/// **`_raw` の 2 列だけは意図的に `金額` 系を使う** (CLAUDE.md の「金額カラムは使わない」の例外。式を変えないこと)。
const DAILY_SUMS: &str = "SELECT [売上年月日], SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0)), SUM(ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0)), SUM(ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0))";
/// daily の FROM と期間 (@P1 以上 @P2 未満)。この後ろに請求区分・除外部門の条件が空白区切りで続く。
const DAILY_WHERE: &str = " FROM [運転日報明細] WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 ";
const DAILY_TAIL: &str = " GROUP BY [売上年月日] ORDER BY [売上年月日]";

/// customer-trend の TOP n の `TOP n` より後ろ。
const TREND_TOP_BODY: &str = " m.[得意先C], ISNULL(c.[得意先N], '') FROM [得意先別月計] m LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 GROUP BY m.[得意先C], c.[得意先N] ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC";

/// customer-trend の全得意先の月別合計 (@P1 from, @P2 to)。列: 0 得意先C, 1 年月度, 2 合計。
pub const CUSTOMER_TREND_MONTHLY_SQL: &str = "SELECT m.[得意先C], m.[年月度], SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) as total FROM [得意先別月計] m WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 GROUP BY m.[得意先C], m.[年月度] ORDER BY m.[年月度], total DESC";

/// customer-detail の得意先名 (@P1 code)。列: 0 得意先N。
pub const CUSTOMER_DETAIL_NAME_SQL: &str =
    "SELECT TOP 1 ISNULL(c.[得意先N], '') FROM [得意先ﾏｽﾀ] c WHERE c.[得意先C] = @P1";

/// customer-detail の月別 (@P1 code)。列: 0 年月度, 1 自車売上, 2 傭車売上, 3 輸送回数。
pub const CUSTOMER_DETAIL_MONTHS_SQL: &str = "SELECT m.[年月度], SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) FROM [得意先別月計] m WHERE m.[得意先C] = @P1 GROUP BY m.[年月度] ORDER BY m.[年月度]";

/// daily の集計の範囲 (`mode`)。`billing`・`non_billing` 以外は全部 [`DailyMode::All`] (オンプレ版と同じ)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DailyMode {
    All,
    Billing,
    NonBilling,
}

impl DailyMode {
    pub fn from_param(mode: &str) -> Self {
        match mode {
            "billing" => DailyMode::Billing,
            "non_billing" => DailyMode::NonBilling,
            _ => DailyMode::All,
        }
    }

    /// `source_table` に入れる名前。
    pub fn label(self) -> &'static str {
        match self {
            DailyMode::Billing => "請求+請求のみ",
            DailyMode::NonBilling => "請求+非請求",
            DailyMode::All => "全て",
        }
    }

    /// 請求区分の条件 (`All` は空)。
    pub fn billing_filter(self) -> &'static str {
        match self {
            DailyMode::Billing => "AND [請求K] IN ('0', '1')",
            DailyMode::NonBilling => "AND [請求K] IN ('0', '2')",
            DailyMode::All => "",
        }
    }
}

/// オンプレ版の `mode_label` (文字列の mode → 名前)。
pub fn mode_label(mode: &str) -> &'static str {
    DailyMode::from_param(mode).label()
}

/// 除外部門の条件 (@P3 に `%部門名%`)。除外しないときは空。
pub fn dept_filter(exclude: bool) -> &'static str {
    if exclude {
        "AND [受注部門] NOT IN (SELECT [部門C] FROM [部門ﾏｽﾀ] WHERE [部門N] LIKE @P3)"
    } else {
        ""
    }
}

/// daily の SQL。`prev` は前年側 (件数の列が無い)。列: 0 日付, 1 自車, 2 傭車, 3 自車 raw, 4 傭車 raw, (5 件数)。
pub fn daily_sql(prev: bool, mode: DailyMode, exclude: bool) -> String {
    let count = if prev { "" } else { ", COUNT(*)" };
    let (billing, dept) = (mode.billing_filter(), dept_filter(exclude));
    format!("{DAILY_SUMS}{count}{DAILY_WHERE}{billing} {dept}{DAILY_TAIL}")
}

/// customer-trend の TOP n 得意先 (@P1 from, @P2 to)。列: 0 得意先C, 1 得意先N。`n` は呼び手が clamp 済み。
pub fn customer_trend_top_sql(n: i32) -> String {
    format!("SELECT TOP {n}{TREND_TOP_BODY}")
}

// ══════════════════════════════════════════════════════════════
// Raw 中間構造体 (DB 層 → ロジック層 の橋渡し)
// ══════════════════════════════════════════════════════════════

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

// ══════════════════════════════════════════════════════════════
// レスポンス構造体 (オンプレ版と同じ JSON の形。フィールドの順を変えないこと)
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct DailySales {
    pub date: String,
    pub weekday: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub own_sales_raw: i64,
    pub charter_sales_raw: i64,
    pub total_sales_raw: i64,
    pub transport_count: i32,
    pub prev_year_own: i64,
    pub prev_year_charter: i64,
    pub prev_year_total: i64,
    pub prev_year_own_raw: i64,
    pub prev_year_charter_raw: i64,
    pub prev_year_total_raw: i64,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerMonthly {
    pub customer_code: String,
    pub customer_name: String,
    pub months: Vec<CustomerMonthData>,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerMonthData {
    pub year_month: String,
    pub total_sales: i64,
    pub rank: i32,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerDetailMonth {
    pub year_month: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerDetailResponse {
    pub customer_code: String,
    pub customer_name: String,
    pub months: Vec<CustomerDetailMonth>,
}

// ══════════════════════════════════════════════════════════════
// Query と、そこから決まる SQL・bind・source_table
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize, Debug, Default)]
pub struct DailyQuery {
    pub month: Option<String>,
    pub mode: Option<String>,
    pub exclude_dept: Option<String>,
}

/// daily の 2 本 (当期・前年) の SQL と bind と `source_table`。
#[derive(Debug, PartialEq)]
pub struct DailyPlan {
    /// 当期 @P1・@P2 (`YYYY-MM-01` と翌月 1 日)
    pub from: String,
    pub to: String,
    /// 前年 @P1・@P2
    pub prev_from: String,
    pub prev_to: String,
    pub current_sql: String,
    pub prev_sql: String,
    /// 除外部門があれば両方の @P3 (`%部門名%`)
    pub exclude_pattern: Option<String>,
    pub source_table: String,
}

impl DailyQuery {
    /// オンプレ版の `daily` と同じ既定値 (month 2026-03・mode all) と組み立て。
    /// `month` に `-` が無いとき (オンプレ版は `parts[1]` で panic) だけ `None` (400)。
    /// 年・月が数字でなければ 2026・3 に落ち、日付として読めない文字列はそのまま bind する (オンプレ版と同じ)。
    pub fn plan(&self) -> Option<DailyPlan> {
        let month = self.month.as_deref().unwrap_or("2026-03");
        let mode = DailyMode::from_param(self.mode.as_deref().unwrap_or("all"));
        let parts: Vec<&str> = month.split('-').collect();
        let m: i32 = parts.get(1)?.parse().unwrap_or(3);
        let y: i32 = parts[0].parse().unwrap_or(2026);
        let (ny, nm) = calc_next_month(y, m);
        let (pny, pnm) = calc_next_month(y - 1, m);
        let exclude = self.exclude_dept.as_deref();
        let label = exclude.unwrap_or("");
        let ml = mode.label();
        let source_table = if label.is_empty() {
            format!("運転日報明細 [{ml}]")
        } else {
            format!("運転日報明細 [{ml}, {label}除く]")
        };
        Some(DailyPlan {
            from: format!("{month}-01"),
            to: format!("{ny}-{nm:02}-01"),
            prev_from: format!("{}-{m:02}-01", y - 1),
            prev_to: format!("{pny}-{pnm:02}-01"),
            current_sql: daily_sql(false, mode, exclude.is_some()),
            prev_sql: daily_sql(true, mode, exclude.is_some()),
            exclude_pattern: exclude.map(|d| format!("%{d}%")),
            source_table,
        })
    }
}

#[derive(Deserialize, Debug, Default)]
pub struct CustomerTrendQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i32>,
}

/// customer-trend の bind (@P1 from, @P2 to。2 本で共通) と TOP n の SQL。
#[derive(Debug, PartialEq)]
pub struct CustomerTrendPlan {
    pub from: String,
    pub to: String,
    pub top_sql: String,
}

impl CustomerTrendQuery {
    /// オンプレ版と同じ既定値 (2025-04〜2026-03、limit 20) と上限 50。
    /// `limit` ≤ 0 (オンプレ版は `TOP` に負数・0 をそのまま入れる) は `None` (400)。
    pub fn plan(&self) -> Option<CustomerTrendPlan> {
        let limit = self.limit.unwrap_or(20);
        if limit <= 0 {
            return None;
        }
        let from = self.from.as_deref().unwrap_or("2025-04");
        let to = self.to.as_deref().unwrap_or("2026-03");
        Some(CustomerTrendPlan {
            from: format!("{from}-01"),
            to: format!("{to}-01"),
            top_sql: customer_trend_top_sql(limit.min(50)),
        })
    }
}

/// customer-detail の Query。`code` は必須 (無ければ 400。オンプレ版の axum の Query と同じ)。
#[derive(Deserialize, Debug)]
pub struct CustomerDetailQuery {
    pub code: String,
}

// ══════════════════════════════════════════════════════════════
// 組み立て
// ══════════════════════════════════════════════════════════════

static WEEKDAYS: [&str; 7] = ["日", "月", "火", "水", "木", "金", "土"];

/// 当期の日ごとに、前年の同じ「日」(`%d`) の値を並べる。前年に無い日は 0。
pub fn build_daily_sales(current: &[RawDailyRow], prev: &[RawDailyPrevRow]) -> Vec<DailySales> {
    let mut prev_map = HashMap::new();
    for r in prev {
        let v = (
            r.own_sales,
            r.charter_sales,
            r.own_sales_raw,
            r.charter_sales_raw,
        );
        prev_map.insert(r.date.format("%d").to_string(), v);
    }
    current
        .iter()
        .map(|r| {
            let day = r.date.format("%d").to_string();
            let wd = r.date.weekday().num_days_from_sunday() as usize;
            let p = prev_map.get(&day).copied().unwrap_or((0, 0, 0, 0));
            DailySales {
                date: r.date.format("%Y-%m-%d").to_string(),
                weekday: WEEKDAYS[wd].to_string(),
                own_sales: r.own_sales,
                charter_sales: r.charter_sales,
                total_sales: r.own_sales + r.charter_sales,
                own_sales_raw: r.own_sales_raw,
                charter_sales_raw: r.charter_sales_raw,
                total_sales_raw: r.own_sales_raw + r.charter_sales_raw,
                transport_count: r.transport_count,
                prev_year_own: p.0,
                prev_year_charter: p.1,
                prev_year_total: p.0 + p.1,
                prev_year_own_raw: p.2,
                prev_year_charter_raw: p.3,
                prev_year_total_raw: p.2 + p.3,
            }
        })
        .collect()
}

/// TOP n の得意先ごとに、全得意先の月別合計の中での順位を付ける。その月に無い得意先は合計 0・順位 0。
pub fn build_customer_trend(
    top_customers: &[(String, String)],
    monthly_raw: &[RawCustomerMonthlyRow],
) -> Vec<CustomerMonthly> {
    if top_customers.is_empty() {
        return vec![];
    }
    let mut month_data: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
    for r in monthly_raw {
        month_data
            .entry(r.year_month.format("%Y-%m").to_string())
            .or_default()
            .push((r.customer_code.clone(), r.total));
    }
    let mut month_ranks: HashMap<String, HashMap<String, (i64, i32)>> = HashMap::new();
    for (ym, entries) in &mut month_data {
        entries.sort_by_key(|e| std::cmp::Reverse(e.1));
        let ranks = entries
            .iter()
            .enumerate()
            .map(|(i, (c, t))| (c.clone(), (*t, (i + 1) as i32)))
            .collect();
        month_ranks.insert(ym.clone(), ranks);
    }
    let months: Vec<String> = month_data.keys().cloned().collect();
    top_customers
        .iter()
        .map(|(code, name)| CustomerMonthly {
            customer_code: code.clone(),
            customer_name: name.clone(),
            months: months
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
                .collect(),
        })
        .collect()
}

pub fn build_customer_detail(raw: &[RawCustomerDetailRow]) -> Vec<CustomerDetailMonth> {
    raw.iter()
        .map(|r| CustomerDetailMonth {
            year_month: r.year_month.format("%Y-%m").to_string(),
            own_sales: r.own_sales,
            charter_sales: r.charter_sales,
            total_sales: r.own_sales + r.charter_sales,
            transport_count: r.transport_count as i32,
        })
        .collect()
}
