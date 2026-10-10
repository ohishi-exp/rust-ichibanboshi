//! `/api/sales/monthly`・`/api/sales/by-department`・`/api/sales/by-customer`・`/api/sales/yoy` の
//! SQL・Raw 型・応答型・Query・組み立て (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版の `src/routes/sales.rs` (`build_*`・Raw 型・Query) と `src/repo.rs` (`monthly`・`by_department`・
//! `by_customer`・`yoy_data` の SQL) を写している。売上は月計テーブル (`種別別月計`・`部門別月計`・`得意先別月計`)
//! の `自車売上`/`傭車売上` を足すだけで、明細からの集計 (`税抜金額 + …`) は無い。
//! 値はすべてバインド (`@P1..`)。SQL を切り替えるのは [`MonthlyScope`] の `&'static str` だけ。

use std::collections::HashMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::period::calc_prev_period;

/// `/api/sales/monthly` (部門指定なし) の `source_table`。`/api/sales/yoy` も同じ。
pub const MONTHLY_ALL_SOURCE: &str = "種別別月計 (種別C=99)";
/// `/api/sales/by-department` の `source_table`。
pub const DEPARTMENT_SOURCE: &str = "部門別月計 + 部門ﾏｽﾀ";
/// `/api/sales/by-customer` の `source_table`。
pub const CUSTOMER_SOURCE: &str = "得意先別月計 + 得意先ﾏｽﾀ";

/// 部門指定 (`include_dept`)。`@P1` from, `@P2` to, `@P3` 部門C。当期・前年とも同じ文。
pub const MONTHLY_DEPT_SQL: &str = "SELECT m.[年月度], \
     SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
     FROM [部門別月計] m \
     WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
       AND m.[部門C] = @P3 \
     GROUP BY m.[年月度] \
     ORDER BY m.[年月度]";

/// 部門除外 (`exclude_dept`)。`@P3` は `%部門名%`。当期・前年とも同じ文。
pub const MONTHLY_EXCLUDE_SQL: &str = "SELECT m.[年月度], \
     SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
     FROM [部門別月計] m \
     LEFT JOIN [部門ﾏｽﾀ] d ON m.[部門C] = d.[部門C] \
     WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
       AND ISNULL(d.[部門N], '') NOT LIKE @P3 \
     GROUP BY m.[年月度] \
     ORDER BY m.[年月度]";

/// 部門指定なし・当期 (輸送回数あり)。
pub const MONTHLY_ALL_SQL: &str = "SELECT [年月度], [自車売上], [傭車売上], [輸送回数] \
     FROM [種別別月計] \
     WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
     ORDER BY [年月度]";

/// 部門指定なし・前年 (輸送回数は読まない)。
pub const MONTHLY_ALL_PREV_SQL: &str =
    "SELECT [年月度], ISNULL([自車売上], 0), ISNULL([傭車売上], 0) \
     FROM [種別別月計] \
     WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
     ORDER BY [年月度]";

/// `/api/sales/by-department` の SQL。`@P1` from, `@P2` to。
pub const BY_DEPARTMENT_SQL: &str = "SELECT m.[部門C], ISNULL(d.[部門N], ''), \
     SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
     FROM [部門別月計] m \
     LEFT JOIN [部門ﾏｽﾀ] d ON m.[部門C] = d.[部門C] \
     WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
     GROUP BY m.[部門C], d.[部門N] \
     ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC";

/// `SELECT TOP n ` の後ろ。`@P1` from, `@P2` to。
const BY_CUSTOMER_SQL_BODY: &str = "m.[得意先C], ISNULL(c.[得意先N], ''), \
     SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) \
     FROM [得意先別月計] m \
     LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
     WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
     GROUP BY m.[得意先C], c.[得意先N] \
     ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC";

/// `/api/sales/yoy` の SQL。当期・前年で同じ文を 2 回流す。`@P1` from, `@P2` to。
pub const YOY_SQL: &str = "SELECT MONTH([年月度]) as m, \
     SUM(ISNULL([自車売上], 0)) + SUM(ISNULL([傭車売上], 0)) as total \
     FROM [種別別月計] \
     WHERE [種別C] = '99' AND [年月度] >= @P1 AND [年月度] <= @P2 \
     GROUP BY MONTH([年月度]) \
     ORDER BY MONTH([年月度])";

/// `/api/sales/by-customer` の SQL。`top` は呼び手が 0..=100 に収めた件数 ([`CustomerQuery::top`])。
pub fn by_customer_sql(top: i32) -> String {
    format!("SELECT TOP {top} {BY_CUSTOMER_SQL_BODY}")
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

#[derive(Debug, Clone)]
pub struct RawMonthTotalRow {
    pub month: i32,
    pub total: i64,
}

// ══════════════════════════════════════════════════════════════
// 応答の型 (オンプレ版 `routes::sales` と同じ JSON)
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct MonthlySales {
    pub year_month: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
    pub prev_year_own: i64,
    pub prev_year_charter: i64,
    pub prev_year_total: i64,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct DepartmentSales {
    pub department_code: String,
    pub department_name: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerSales {
    pub customer_code: String,
    pub customer_name: String,
    pub own_sales: i64,
    pub charter_sales: i64,
    pub total_sales: i64,
    pub transport_count: i32,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct YoyComparison {
    pub month: String,
    pub current_year: i64,
    pub previous_year: i64,
    pub diff: i64,
    pub diff_percent: f64,
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

/// `from`/`to` が無いときの既定 (オンプレ版と同じ)。
const DEFAULT_FROM: &str = "2025-04";
const DEFAULT_TO: &str = "2026-03";

/// `"YYYY-MM"` の `from`/`to` (既定つき) から月初日の文字列 `"YYYY-MM-01"` を作る。値の検査はしない (オンプレ版と同じ)。
fn month_start_dates(from: &Option<String>, to: &Option<String>) -> (String, String) {
    let from = from.as_deref().unwrap_or(DEFAULT_FROM);
    let to = to.as_deref().unwrap_or(DEFAULT_TO);
    (format!("{from}-01"), format!("{to}-01"))
}

/// `/api/sales/monthly` の Query。
#[derive(Deserialize, Debug, Default)]
pub struct MonthlyQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub exclude_dept: Option<String>,
    pub include_dept: Option<String>,
}

/// 当期と前年同期間の月初日 (`"YYYY-MM-01"`)。
#[derive(Debug, PartialEq)]
pub struct MonthlyRange {
    pub from: String,
    pub to: String,
    pub prev_from: String,
    pub prev_to: String,
}

/// `/api/sales/monthly` の絞り込み。`include_dept` が `exclude_dept` に優先する (空文字も指定として扱う、オンプレ版と同じ)。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MonthlyScope<'a> {
    /// 指定した部門C だけ。
    Dept(&'a str),
    /// 部門名に含まれる文字列の部門を除く。
    Exclude(&'a str),
    /// `種別別月計` の 種別C=99 (部門の絞り込みなし)。
    All,
}

impl MonthlyScope<'_> {
    /// 当期の SQL。
    pub fn current_sql(&self) -> &'static str {
        match self {
            MonthlyScope::Dept(_) => MONTHLY_DEPT_SQL,
            MonthlyScope::Exclude(_) => MONTHLY_EXCLUDE_SQL,
            MonthlyScope::All => MONTHLY_ALL_SQL,
        }
    }

    /// 前年の SQL。部門指定・除外は当期と同じ文。
    pub fn prev_sql(&self) -> &'static str {
        match self {
            MonthlyScope::All => MONTHLY_ALL_PREV_SQL,
            other => other.current_sql(),
        }
    }

    /// 前年の SQL が `輸送回数` の列を持つか (持たなければ 0 で詰める)。
    pub fn prev_has_transport_count(&self) -> bool {
        !matches!(self, MonthlyScope::All)
    }

    /// `@P3` に渡す値。`All` は無い。
    pub fn param(&self) -> Option<String> {
        match self {
            MonthlyScope::Dept(code) => Some(code.to_string()),
            MonthlyScope::Exclude(dept) => Some(format!("%{dept}%")),
            MonthlyScope::All => None,
        }
    }

    /// 応答の `source_table`。部門指定・除外は値を含む (オンプレ版と同じ文字列)。
    pub fn source_table(&self) -> String {
        match self {
            MonthlyScope::Dept(code) => format!("部門別月計 (部門C={code})"),
            MonthlyScope::Exclude(dept) => format!("部門別月計 ({dept}除く)"),
            MonthlyScope::All => MONTHLY_ALL_SOURCE.to_string(),
        }
    }
}

impl MonthlyQuery {
    pub fn range(&self) -> MonthlyRange {
        let (from, to) = month_start_dates(&self.from, &self.to);
        let (prev_from, prev_to) = calc_prev_period(
            self.from.as_deref().unwrap_or(DEFAULT_FROM),
            self.to.as_deref().unwrap_or(DEFAULT_TO),
        );
        MonthlyRange {
            from,
            to,
            prev_from,
            prev_to,
        }
    }

    pub fn scope(&self) -> MonthlyScope<'_> {
        if let Some(code) = self.include_dept.as_deref() {
            MonthlyScope::Dept(code)
        } else if let Some(dept) = self.exclude_dept.as_deref() {
            MonthlyScope::Exclude(dept)
        } else {
            MonthlyScope::All
        }
    }
}

/// `/api/sales/by-department` の Query。
#[derive(Deserialize, Debug, Default)]
pub struct PeriodQuery {
    pub from: Option<String>,
    pub to: Option<String>,
}

impl PeriodQuery {
    /// `(from, to)` の月初日。
    pub fn dates(&self) -> (String, String) {
        month_start_dates(&self.from, &self.to)
    }
}

/// `/api/sales/by-customer` の Query。
#[derive(Deserialize, Debug, Default)]
pub struct CustomerQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<i32>,
}

impl CustomerQuery {
    /// `(from, to)` の月初日。
    pub fn dates(&self) -> (String, String) {
        month_start_dates(&self.from, &self.to)
    }

    /// `TOP n` の n (既定 20、上限 100)。0 は `TOP 0` で空配列 (オンプレ版と同じ)。
    /// 負数は `None` — オンプレ版は `TOP -1` で 500 になるので Worker は 400 にする。
    pub fn top(&self) -> Option<i32> {
        let limit = self.limit.unwrap_or(20);
        (limit >= 0).then(|| limit.min(100))
    }
}

/// `/api/sales/yoy` の Query。
#[derive(Deserialize, Debug, Default)]
pub struct YoyQuery {
    pub year: Option<i32>,
}

/// 当期・前年の `[from, to]` (1 月〜12 月の月初日)。
#[derive(Debug, PartialEq)]
pub struct YoyRange {
    pub from: String,
    pub to: String,
    pub prev_from: String,
    pub prev_to: String,
}

impl YoyQuery {
    /// 既定 2026。
    pub fn year(&self) -> i32 {
        self.year.unwrap_or(2026)
    }

    pub fn range(&self) -> YoyRange {
        let year = self.year();
        // i32::MIN でも panic しない (SQL 側が日付として読めず失敗する)
        let prev = year.wrapping_sub(1);
        YoyRange {
            from: format!("{year}-01-01"),
            to: format!("{year}-12-01"),
            prev_from: format!("{prev}-01-01"),
            prev_to: format!("{prev}-12-01"),
        }
    }
}

// ══════════════════════════════════════════════════════════════
// ロジック層 (純粋関数)
// ══════════════════════════════════════════════════════════════

pub fn build_monthly_sales(current: &[RawMonthlyRow], prev: &[RawMonthlyRow]) -> Vec<MonthlySales> {
    let mut prev_map = HashMap::new();
    for r in prev {
        prev_map.insert(
            r.year_month.format("%m").to_string(),
            (r.own_sales, r.charter_sales),
        );
    }
    current
        .iter()
        .map(|r| {
            let month = r.year_month.format("%m").to_string();
            MonthlySales {
                year_month: r.year_month.format("%Y-%m").to_string(),
                own_sales: r.own_sales,
                charter_sales: r.charter_sales,
                total_sales: r.own_sales + r.charter_sales,
                transport_count: r.transport_count,
                prev_year_own: prev_map.get(&month).map(|v| v.0).unwrap_or(0),
                prev_year_charter: prev_map.get(&month).map(|v| v.1).unwrap_or(0),
                prev_year_total: prev_map.get(&month).map(|v| v.0 + v.1).unwrap_or(0),
            }
        })
        .collect()
}

pub fn build_department_sales(raw: &[RawDepartmentRow]) -> Vec<DepartmentSales> {
    raw.iter()
        .map(|r| DepartmentSales {
            department_code: r.department_code.clone(),
            department_name: r.department_name.clone(),
            own_sales: r.own_sales,
            charter_sales: r.charter_sales,
            total_sales: r.own_sales + r.charter_sales,
            transport_count: r.transport_count as i32,
        })
        .collect()
}

pub fn build_customer_sales(raw: &[RawCustomerRow]) -> Vec<CustomerSales> {
    raw.iter()
        .map(|r| CustomerSales {
            customer_code: r.customer_code.clone(),
            customer_name: r.customer_name.clone(),
            own_sales: r.own_sales,
            charter_sales: r.charter_sales,
            total_sales: r.own_sales + r.charter_sales,
            transport_count: r.transport_count as i32,
        })
        .collect()
}

pub fn build_yoy_comparison(
    current: &[RawMonthTotalRow],
    prev: &[RawMonthTotalRow],
) -> Vec<YoyComparison> {
    let mut prev_map = HashMap::new();
    for r in prev {
        prev_map.insert(r.month, r.total);
    }
    current
        .iter()
        .map(|r| {
            let previous = prev_map.get(&r.month).copied().unwrap_or(0);
            let diff = r.total - previous;
            let diff_percent = if previous > 0 {
                (diff as f64 / previous as f64) * 100.0
            } else {
                0.0
            };
            YoyComparison {
                month: format!("{:02}", r.month),
                current_year: r.total,
                previous_year: previous,
                diff,
                diff_percent: (diff_percent * 10.0).round() / 10.0,
            }
        })
        .collect()
}
