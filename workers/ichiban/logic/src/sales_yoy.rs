//! `/api/sales/customer-yoy`・`/api/sales/customer-yoy-by-dept` の SQL・Raw 型・応答型・Query・組み立て (Refs #322)。
//!
//! オンプレ版 `src/routes/sales.rs` の `customer_yoy`・`customer_yoy_by_dept` と `src/repo.rs` の
//! `customer_yoy_data`・`customer_yoy_by_dept_data` を写した。SQL 文・既定値・応答の JSON は同じ。
//!
//! 違いは 1 つだけ: オンプレ版は得意先を `HashSet` 経由で並べるため、並べ替えの値が同じものどうしの順が
//! 実行ごとに変わり得る。ここでは同じ値どうしを得意先コードの昇順 (by-dept はさらに部門コードの昇順) に決める。

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::api::Department;
use crate::period::{calc_months, calc_prev_period};

/// `/api/sales/customer-yoy` の `source_table`。
pub const CUSTOMER_YOY_SOURCE: &str = "得意先別月計 + 得意先ﾏｽﾀ";
/// `/api/sales/customer-yoy-by-dept` の `source_table`。
pub const CUSTOMER_YOY_BY_DEPT_SOURCE: &str = "運転日報明細 + 部門ﾏｽﾀ + 得意先ﾏｽﾀ";

/// 得意先別の売上合計。当期・前年で 2 回流す (@P1 from, @P2 to。どちらも "YYYY-MM-01" で両端を含む)。
/// 列: 0 得意先C, 1 得意先N, 2 合計。
pub const CUSTOMER_YOY_SQL: &str = "SELECT m.[得意先C], ISNULL(c.[得意先N], ''), \
                   SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) \
                   FROM [得意先別月計] m \
                   LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] \
                   WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 \
                   GROUP BY m.[得意先C], c.[得意先N]";

/// 営業所 (受注部門) × 得意先の売上合計の本体。月計テーブルとの完全一致条件 (`請求K IN ('0','2')`・税抜の自車/傭車)。
/// @P1 from, @P2 to ("YYYY-MM-01"。to は含まない)。列: 0 受注部門, 1 部門N, 2 得意先C, 3 得意先N, 4 合計。
const BY_DEPT_BASE_SQL: &str = "SELECT t.[受注部門], ISNULL(d.[部門N], ''), \
                             t.[得意先C], ISNULL(c.[得意先N], ''), \
                             SUM(ISNULL(t.[税抜金額],0) + ISNULL(t.[税抜割増],0) + ISNULL(t.[税抜実費],0) - ISNULL(t.[値引],0)) \
                             + SUM(ISNULL(t.[税抜傭車金額],0) + ISNULL(t.[税抜傭車割増],0) + ISNULL(t.[税抜傭車実費],0) - ISNULL(t.[傭車値引],0)) \
                           FROM [運転日報明細] t \
                           LEFT JOIN [部門ﾏｽﾀ] d ON t.[受注部門] = d.[部門C] \
                           LEFT JOIN [得意先ﾏｽﾀ] c ON t.[得意先C] = c.[得意先C] \
                           WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
                             AND t.[請求K] IN ('0','2')";
/// 部門の指定があるときだけ足す絞り込み (@P3 部門コード)。
const BY_DEPT_FILTER_SQL: &str = " AND t.[受注部門] = @P3 ";
const BY_DEPT_GROUP_SQL: &str = " GROUP BY t.[受注部門], d.[部門N], t.[得意先C], c.[得意先N]";

/// customer-yoy-by-dept の SQL。オンプレ版の `format!` と同じ文字列になる。
pub fn customer_yoy_by_dept_sql(with_department: bool) -> String {
    let filter = if with_department {
        BY_DEPT_FILTER_SQL
    } else {
        ""
    };
    format!("{BY_DEPT_BASE_SQL}{filter}{BY_DEPT_GROUP_SQL}")
}

/// `CUSTOMER_YOY_SQL` の 1 行。
#[derive(Debug, Clone, PartialEq)]
pub struct RawCustomerTotalRow {
    pub customer_code: String,
    pub customer_name: String,
    pub total: i64,
}

/// `customer_yoy_by_dept_sql` の 1 行。
#[derive(Debug, Clone, PartialEq)]
pub struct RawCustomerDeptRow {
    pub department_code: String,
    pub department_name: String,
    pub customer_code: String,
    pub customer_name: String,
    pub total: i64,
}

/// customer_code → (customer_name, total)
pub type CodeTotalMap = HashMap<String, (String, i64)>;
/// (department_code, customer_code) → (department_name, customer_name, total)
pub type DeptCustomerTotalMap = HashMap<(String, String), (String, String, i64)>;

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct CustomerYoy {
    pub customer_code: String,
    pub customer_name: String,
    pub current_total: i64,
    pub prev_total: i64,
    pub diff: i64,
    pub yoy_percent: f64,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerYoyResponse {
    pub positive: Vec<CustomerYoy>,
    pub negative: Vec<CustomerYoy>,
    pub min_prev: i64,
    pub months: i64,
}

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

#[derive(Serialize, Debug, PartialEq)]
pub struct CustomerYoyByDeptResponse {
    pub positive: Vec<CustomerYoyWithDept>,
    pub negative: Vec<CustomerYoyWithDept>,
    pub months: i64,
    pub min_prev: i64,
    pub department_code: Option<String>,
    pub departments: Vec<Department>,
}

#[derive(Deserialize, Debug, Default)]
pub struct CustomerYoyQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<usize>,
    pub min_prev: Option<i64>,
}

#[derive(Deserialize, Debug, Default)]
pub struct CustomerYoyByDeptQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub limit: Option<usize>,
    pub min_prev: Option<i64>,
    pub department_code: Option<String>,
}

/// 両方の口で共通の、クエリから決まる値 (オンプレ版のハンドラ冒頭と同じ既定値)。
#[derive(Debug, Clone, PartialEq)]
pub struct YoyPeriod {
    /// 当期の始め ("YYYY-MM-01")
    pub from_date: String,
    /// 当期の終わり ("YYYY-MM-01")
    pub to_date: String,
    pub prev_from: String,
    pub prev_to: String,
    /// 正・負それぞれの上限件数 (既定 10、最大 50)
    pub limit: usize,
    pub months: i64,
    /// 前年の合計がこれ未満の得意先を除く (既定 月数 × 40,000)
    pub min_prev: i64,
}

fn yoy_period(
    from: Option<&str>,
    to: Option<&str>,
    limit: Option<usize>,
    min_prev: Option<i64>,
) -> YoyPeriod {
    let from = from.unwrap_or("2025-04");
    let to = to.unwrap_or("2026-03");
    let months = calc_months(from, to);
    let (prev_from, prev_to) = calc_prev_period(from, to);
    YoyPeriod {
        from_date: format!("{from}-01"),
        to_date: format!("{to}-01"),
        prev_from,
        prev_to,
        limit: limit.unwrap_or(10).min(50),
        months,
        min_prev: min_prev.unwrap_or(months * 40_000),
    }
}

impl CustomerYoyQuery {
    pub fn period(&self) -> YoyPeriod {
        yoy_period(
            self.from.as_deref(),
            self.to.as_deref(),
            self.limit,
            self.min_prev,
        )
    }
}

impl CustomerYoyByDeptQuery {
    pub fn period(&self) -> YoyPeriod {
        yoy_period(
            self.from.as_deref(),
            self.to.as_deref(),
            self.limit,
            self.min_prev,
        )
    }

    /// 部門の指定 (前後の空白を落とし、空なら指定なし)。SQL の @P3 と応答の `department_code` に使う。
    pub fn department(&self) -> Option<String> {
        self.department_code
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    }
}

/// 行を得意先コードの map にする (同じコードが複数行あれば後の行が勝つ。オンプレ版と同じ)。
pub fn rows_to_code_total_map(rows: &[RawCustomerTotalRow]) -> CodeTotalMap {
    let mut map = HashMap::new();
    for r in rows {
        map.insert(r.customer_code.clone(), (r.customer_name.clone(), r.total));
    }
    map
}

/// 行を (営業所, 得意先) の map にする (同じキーが複数行あれば後の行が勝つ。オンプレ版と同じ)。
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

/// 増減率 (%、小数 1 桁に丸める)。前年 0 なら inf / NaN (JSON では null) になるのもオンプレ版と同じ。
fn yoy_percent(diff: i64, prev_total: i64) -> f64 {
    ((diff as f64 / prev_total as f64) * 1000.0).round() / 10.0
}

/// 当期・前年の map を突き合わせ、前年の合計が `min_prev` 以上の得意先の増減を出す (得意先コード順)。
pub fn calc_yoy_entries(
    cur_map: &CodeTotalMap,
    prev_map: &CodeTotalMap,
    min_prev: i64,
) -> Vec<CustomerYoy> {
    let all_codes: BTreeSet<&String> = cur_map.keys().chain(prev_map.keys()).collect();
    all_codes
        .into_iter()
        .filter_map(|code| {
            let (cur_name, cur_total) = cur_map.get(code).cloned().unwrap_or_default();
            let (prev_name, prev_total) = prev_map.get(code).cloned().unwrap_or_default();
            let name = if !cur_name.is_empty() {
                cur_name
            } else {
                prev_name
            };
            if prev_total < min_prev {
                return None;
            }
            let diff = cur_total - prev_total;
            Some(CustomerYoy {
                customer_code: code.clone(),
                customer_name: name,
                current_total: cur_total,
                prev_total,
                diff,
                yoy_percent: yoy_percent(diff, prev_total),
            })
        })
        .collect()
}

/// 増 (前年の合計の降順) と減 (増減率の昇順 → 前年の合計の降順) に分けて `limit` 件ずつ。0% と NaN はどちらにも入らない。
/// 同じ値どうしは得意先コードの昇順。
pub fn split_and_sort_yoy(
    entries: Vec<CustomerYoy>,
    limit: usize,
) -> (Vec<CustomerYoy>, Vec<CustomerYoy>) {
    let (mut pos, mut neg): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .filter(|e| e.yoy_percent != 0.0 && !e.yoy_percent.is_nan())
        .partition(|e| e.yoy_percent > 0.0);
    pos.sort_by(|a, b| {
        b.prev_total
            .cmp(&a.prev_total)
            .then_with(|| a.customer_code.cmp(&b.customer_code))
    });
    neg.sort_by(|a, b| {
        a.yoy_percent
            .total_cmp(&b.yoy_percent)
            .then(b.prev_total.cmp(&a.prev_total))
            .then_with(|| a.customer_code.cmp(&b.customer_code))
    });
    pos.truncate(limit);
    neg.truncate(limit);
    (pos, neg)
}

/// [`calc_yoy_entries`] の営業所 × 得意先版 ((部門コード, 得意先コード) 順)。
pub fn calc_yoy_with_dept_entries(
    cur_map: &DeptCustomerTotalMap,
    prev_map: &DeptCustomerTotalMap,
    min_prev: i64,
) -> Vec<CustomerYoyWithDept> {
    let all_keys: BTreeSet<&(String, String)> = cur_map.keys().chain(prev_map.keys()).collect();
    all_keys
        .into_iter()
        .filter_map(|key| {
            let cur = cur_map.get(key).cloned().unwrap_or_default();
            let prev = prev_map.get(key).cloned().unwrap_or_default();
            let (dept_code, cust_code) = key.clone();
            let dept_name = if !cur.0.is_empty() { cur.0 } else { prev.0 };
            let cust_name = if !cur.1.is_empty() { cur.1 } else { prev.1 };
            let cur_total = cur.2;
            let prev_total = prev.2;
            if prev_total < min_prev {
                return None;
            }
            let diff = cur_total - prev_total;
            Some(CustomerYoyWithDept {
                department_code: dept_code,
                department_name: dept_name,
                customer_code: cust_code,
                customer_name: cust_name,
                current_total: cur_total,
                prev_total,
                diff,
                yoy_percent: yoy_percent(diff, prev_total),
            })
        })
        .collect()
}

/// [`split_and_sort_yoy`] の営業所 × 得意先版。同じ値どうしは得意先コード → 部門コードの昇順。
pub fn split_and_sort_yoy_with_dept(
    entries: Vec<CustomerYoyWithDept>,
    limit: usize,
) -> (Vec<CustomerYoyWithDept>, Vec<CustomerYoyWithDept>) {
    let (mut pos, mut neg): (Vec<_>, Vec<_>) = entries
        .into_iter()
        .filter(|e| e.yoy_percent != 0.0 && !e.yoy_percent.is_nan())
        .partition(|e| e.yoy_percent > 0.0);
    let tie = |a: &CustomerYoyWithDept, b: &CustomerYoyWithDept| {
        a.customer_code
            .cmp(&b.customer_code)
            .then_with(|| a.department_code.cmp(&b.department_code))
    };
    pos.sort_by(|a, b| b.prev_total.cmp(&a.prev_total).then_with(|| tie(a, b)));
    neg.sort_by(|a, b| {
        a.yoy_percent
            .total_cmp(&b.yoy_percent)
            .then(b.prev_total.cmp(&a.prev_total))
            .then_with(|| tie(a, b))
    });
    pos.truncate(limit);
    neg.truncate(limit);
    (pos, neg)
}

/// `/api/sales/customer-yoy` の `data`。
pub fn build_customer_yoy(
    p: &YoyPeriod,
    cur_rows: &[RawCustomerTotalRow],
    prev_rows: &[RawCustomerTotalRow],
) -> CustomerYoyResponse {
    let cur_map = rows_to_code_total_map(cur_rows);
    let prev_map = rows_to_code_total_map(prev_rows);
    let entries = calc_yoy_entries(&cur_map, &prev_map, p.min_prev);
    let (positive, negative) = split_and_sort_yoy(entries, p.limit);
    CustomerYoyResponse {
        positive,
        negative,
        min_prev: p.min_prev,
        months: p.months,
    }
}

/// `/api/sales/customer-yoy-by-dept` の `data`。`departments` は部門ﾏｽﾀの一覧 (選択肢)。
pub fn build_customer_yoy_by_dept(
    p: &YoyPeriod,
    department_code: Option<String>,
    cur_rows: &[RawCustomerDeptRow],
    prev_rows: &[RawCustomerDeptRow],
    departments: Vec<Department>,
) -> CustomerYoyByDeptResponse {
    let cur_map = rows_to_dept_customer_map(cur_rows);
    let prev_map = rows_to_dept_customer_map(prev_rows);
    let entries = calc_yoy_with_dept_entries(&cur_map, &prev_map, p.min_prev);
    let (positive, negative) = split_and_sort_yoy_with_dept(entries, p.limit);
    CustomerYoyByDeptResponse {
        positive,
        negative,
        months: p.months,
        min_prev: p.min_prev,
        department_code,
        departments,
    }
}
