use chrono::NaiveDate;
use rust_ichibanboshi::routes::sales::*;
use std::collections::HashMap;

fn dt(y: i32, m: u32, d: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

// ══════════════════════════════════════════════════════════════
// calc_prev_period
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_prev_period_standard() {
    let (from, to) = calc_prev_period("2025-04", "2026-03");
    assert_eq!(from, "2024-04-01");
    assert_eq!(to, "2025-03-01");
}

#[test]
fn test_calc_prev_period_single_month() {
    let (from, to) = calc_prev_period("2025-01", "2025-01");
    assert_eq!(from, "2024-01-01");
    assert_eq!(to, "2024-01-01");
}

#[test]
fn test_calc_prev_period_calendar_year() {
    let (from, to) = calc_prev_period("2026-01", "2026-12");
    assert_eq!(from, "2025-01-01");
    assert_eq!(to, "2025-12-01");
}

// ══════════════════════════════════════════════════════════════
// calc_next_month
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_next_month_normal() {
    assert_eq!(calc_next_month(2025, 3), (2025, 4));
    assert_eq!(calc_next_month(2025, 11), (2025, 12));
}

#[test]
fn test_calc_next_month_december() {
    assert_eq!(calc_next_month(2025, 12), (2026, 1));
}

// ══════════════════════════════════════════════════════════════
// calc_months
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_months_full_year() {
    assert_eq!(calc_months("2025-04", "2026-03"), 12);
}

#[test]
fn test_calc_months_single() {
    assert_eq!(calc_months("2025-04", "2025-04"), 1);
}

#[test]
fn test_calc_months_half_year() {
    assert_eq!(calc_months("2025-01", "2025-06"), 6);
}

#[test]
fn test_calc_months_minimum_one() {
    // 逆転しても最低1
    assert_eq!(calc_months("2026-03", "2025-04"), 1);
}

// ══════════════════════════════════════════════════════════════
// build_monthly_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_monthly_sales_with_prev() {
    let current = vec![
        RawMonthlyRow { year_month: dt(2025, 4, 1), own_sales: 1_000_000, charter_sales: 500_000, transport_count: 50 },
        RawMonthlyRow { year_month: dt(2025, 5, 1), own_sales: 1_200_000, charter_sales: 600_000, transport_count: 55 },
    ];
    let prev = vec![
        RawMonthlyRow { year_month: dt(2024, 4, 1), own_sales: 900_000, charter_sales: 400_000, transport_count: 0 },
    ];

    let result = build_monthly_sales(&current, &prev);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].year_month, "2025-04");
    assert_eq!(result[0].own_sales, 1_000_000);
    assert_eq!(result[0].charter_sales, 500_000);
    assert_eq!(result[0].total_sales, 1_500_000);
    assert_eq!(result[0].transport_count, 50);
    assert_eq!(result[0].prev_year_own, 900_000);
    assert_eq!(result[0].prev_year_charter, 400_000);
    assert_eq!(result[0].prev_year_total, 1_300_000);
    // 5月は前年データなし
    assert_eq!(result[1].prev_year_own, 0);
    assert_eq!(result[1].prev_year_total, 0);
}

#[test]
fn test_build_monthly_sales_empty() {
    let result = build_monthly_sales(&[], &[]);
    assert!(result.is_empty());
}

#[test]
fn test_build_monthly_sales_no_prev() {
    let current = vec![
        RawMonthlyRow { year_month: dt(2025, 4, 1), own_sales: 100, charter_sales: 50, transport_count: 10 },
    ];
    let result = build_monthly_sales(&current, &[]);
    assert_eq!(result[0].prev_year_own, 0);
    assert_eq!(result[0].prev_year_charter, 0);
    assert_eq!(result[0].prev_year_total, 0);
}

// ══════════════════════════════════════════════════════════════
// build_department_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_department_sales() {
    let raw = vec![
        RawDepartmentRow { department_code: "01".into(), department_name: "本社".into(), own_sales: 500, charter_sales: 200, transport_count: 10 },
        RawDepartmentRow { department_code: "02".into(), department_name: "支店".into(), own_sales: 300, charter_sales: 100, transport_count: 5 },
    ];
    let result = build_department_sales(&raw);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].total_sales, 700);
    assert_eq!(result[1].total_sales, 400);
    assert_eq!(result[0].department_name, "本社");
}

#[test]
fn test_build_department_sales_empty() {
    assert!(build_department_sales(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// build_customer_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_customer_sales() {
    let raw = vec![
        RawCustomerRow { customer_code: "001".into(), customer_name: "得意先A".into(), own_sales: 1000, charter_sales: 500, transport_count: 20 },
    ];
    let result = build_customer_sales(&raw);
    assert_eq!(result[0].total_sales, 1500);
    assert_eq!(result[0].transport_count, 20);
}

// ══════════════════════════════════════════════════════════════
// calc_yoy_entries + split_and_sort_yoy
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_yoy_entries_basic() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("A".into(), ("顧客A".into(), 1_200_000));
    cur.insert("B".into(), ("顧客B".into(), 800_000));

    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("A".into(), ("顧客A".into(), 1_000_000));
    prev.insert("B".into(), ("顧客B".into(), 1_000_000));

    let entries = calc_yoy_entries(&cur, &prev, 100_000);
    assert_eq!(entries.len(), 2);

    let a = entries.iter().find(|e| e.customer_code == "A").unwrap();
    assert_eq!(a.yoy_percent, 20.0);
    assert_eq!(a.diff, 200_000);

    let b = entries.iter().find(|e| e.customer_code == "B").unwrap();
    assert_eq!(b.yoy_percent, -20.0);
}

#[test]
fn test_calc_yoy_entries_min_prev_filter() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("A".into(), ("A".into(), 500_000));
    cur.insert("B".into(), ("B".into(), 100_000));

    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("A".into(), ("A".into(), 400_000));
    prev.insert("B".into(), ("B".into(), 30_000)); // min_prev 未満

    let entries = calc_yoy_entries(&cur, &prev, 40_000);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].customer_code, "A");
}

#[test]
fn test_calc_yoy_entries_no_prev_data() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("NEW".into(), ("新規".into(), 500_000));
    let prev: CodeTotalMap = HashMap::new();

    // prev_total=0 < min_prev=1 → 除外
    let entries = calc_yoy_entries(&cur, &prev, 1);
    assert!(entries.is_empty());
}

#[test]
fn test_calc_yoy_entries_only_in_prev() {
    let cur: CodeTotalMap = HashMap::new();
    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("OLD".into(), ("旧顧客".into(), 500_000));

    let entries = calc_yoy_entries(&cur, &prev, 100_000);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].current_total, 0);
    assert_eq!(entries[0].yoy_percent, -100.0);
}

#[test]
fn test_split_and_sort_yoy() {
    let entries = vec![
        CustomerYoy { customer_code: "A".into(), customer_name: "A".into(), current_total: 120, prev_total: 100, diff: 20, yoy_percent: 20.0 },
        CustomerYoy { customer_code: "B".into(), customer_name: "B".into(), current_total: 80, prev_total: 100, diff: -20, yoy_percent: -20.0 },
        CustomerYoy { customer_code: "C".into(), customer_name: "C".into(), current_total: 50, prev_total: 200, diff: -150, yoy_percent: -75.0 },
        CustomerYoy { customer_code: "D".into(), customer_name: "D".into(), current_total: 150, prev_total: 50, diff: 100, yoy_percent: 200.0 },
    ];

    let (pos, neg) = split_and_sort_yoy(entries, 10);

    // positive: 前年売上降順
    assert_eq!(pos.len(), 2);
    assert_eq!(pos[0].customer_code, "A"); // prev=100
    assert_eq!(pos[1].customer_code, "D"); // prev=50

    // negative: YoY%昇順
    assert_eq!(neg.len(), 2);
    assert_eq!(neg[0].customer_code, "C"); // -75%
    assert_eq!(neg[1].customer_code, "B"); // -20%
}

#[test]
fn test_split_and_sort_yoy_with_limit() {
    let entries = vec![
        CustomerYoy { customer_code: "A".into(), customer_name: "A".into(), current_total: 200, prev_total: 100, diff: 100, yoy_percent: 100.0 },
        CustomerYoy { customer_code: "B".into(), customer_name: "B".into(), current_total: 150, prev_total: 100, diff: 50, yoy_percent: 50.0 },
        CustomerYoy { customer_code: "C".into(), customer_name: "C".into(), current_total: 130, prev_total: 100, diff: 30, yoy_percent: 30.0 },
    ];

    let (pos, neg) = split_and_sort_yoy(entries, 2);
    assert_eq!(pos.len(), 2); // limit で切られる
    assert!(neg.is_empty());
}

#[test]
fn test_split_and_sort_yoy_zero_percent_excluded() {
    let entries = vec![
        CustomerYoy { customer_code: "X".into(), customer_name: "X".into(), current_total: 100, prev_total: 100, diff: 0, yoy_percent: 0.0 },
    ];
    let (pos, neg) = split_and_sort_yoy(entries, 10);
    assert!(pos.is_empty()); // 0% は positive でも negative でもない
    assert!(neg.is_empty());
}

// ══════════════════════════════════════════════════════════════
// build_yoy_comparison
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_yoy_comparison() {
    let current = vec![
        RawMonthTotalRow { month: 1, total: 1_000_000 },
        RawMonthTotalRow { month: 2, total: 1_200_000 },
        RawMonthTotalRow { month: 3, total: 900_000 },
    ];
    let prev = vec![
        RawMonthTotalRow { month: 1, total: 900_000 },
        RawMonthTotalRow { month: 2, total: 1_200_000 },
    ];

    let result = build_yoy_comparison(&current, &prev);

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].month, "01");
    assert_eq!(result[0].diff, 100_000);
    assert_eq!(result[0].diff_percent, 11.1); // 100k/900k*100 = 11.11 → 11.1

    assert_eq!(result[1].diff_percent, 0.0); // 同額

    // 3月は前年データなし → previous=0, diff_percent=0.0
    assert_eq!(result[2].previous_year, 0);
    assert_eq!(result[2].diff_percent, 0.0);
}

#[test]
fn test_build_yoy_comparison_empty() {
    assert!(build_yoy_comparison(&[], &[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// build_daily_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_daily_sales() {
    // 2025-04-01 は火曜日
    let current = vec![
        RawDailyRow { date: dt(2025, 4, 1), own_sales: 100, charter_sales: 50, own_sales_raw: 110, charter_sales_raw: 55, transport_count: 10 },
        RawDailyRow { date: dt(2025, 4, 2), own_sales: 200, charter_sales: 80, own_sales_raw: 220, charter_sales_raw: 88, transport_count: 15 },
    ];
    let prev = vec![
        RawDailyPrevRow { date: dt(2024, 4, 1), own_sales: 90, charter_sales: 40, own_sales_raw: 95, charter_sales_raw: 42 },
    ];

    let result = build_daily_sales(&current, &prev);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].date, "2025-04-01");
    assert_eq!(result[0].weekday, "火");
    assert_eq!(result[0].total_sales, 150);
    assert_eq!(result[0].total_sales_raw, 165);
    assert_eq!(result[0].prev_year_own, 90);
    assert_eq!(result[0].prev_year_total, 130);
    assert_eq!(result[0].prev_year_total_raw, 137);

    // 2日は前年データなし
    assert_eq!(result[1].prev_year_total, 0);
}

#[test]
fn test_build_daily_sales_empty() {
    assert!(build_daily_sales(&[], &[]).is_empty());
}

#[test]
fn test_build_daily_sales_sunday() {
    // 2025-04-06 は日曜日
    let current = vec![
        RawDailyRow { date: dt(2025, 4, 6), own_sales: 0, charter_sales: 0, own_sales_raw: 0, charter_sales_raw: 0, transport_count: 0 },
    ];
    let result = build_daily_sales(&current, &[]);
    assert_eq!(result[0].weekday, "日");
}

// ══════════════════════════════════════════════════════════════
// build_customer_trend
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_customer_trend() {
    let top = vec![
        ("A".to_string(), "顧客A".to_string()),
        ("B".to_string(), "顧客B".to_string()),
    ];
    let monthly = vec![
        RawCustomerMonthlyRow { customer_code: "A".into(), year_month: dt(2025, 4, 1), total: 1000 },
        RawCustomerMonthlyRow { customer_code: "B".into(), year_month: dt(2025, 4, 1), total: 800 },
        RawCustomerMonthlyRow { customer_code: "C".into(), year_month: dt(2025, 4, 1), total: 500 },
        RawCustomerMonthlyRow { customer_code: "A".into(), year_month: dt(2025, 5, 1), total: 700 },
        RawCustomerMonthlyRow { customer_code: "B".into(), year_month: dt(2025, 5, 1), total: 900 },
    ];

    let result = build_customer_trend(&top, &monthly);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].customer_code, "A");
    assert_eq!(result[0].months.len(), 2);
    assert_eq!(result[0].months[0].year_month, "2025-04");
    assert_eq!(result[0].months[0].rank, 1); // A=1000 > B=800
    assert_eq!(result[0].months[1].rank, 2); // A=700 < B=900

    assert_eq!(result[1].customer_code, "B");
    assert_eq!(result[1].months[0].rank, 2);
    assert_eq!(result[1].months[1].rank, 1);
}

#[test]
fn test_build_customer_trend_empty_top() {
    let result = build_customer_trend(&[], &[]);
    assert!(result.is_empty());
}

#[test]
fn test_build_customer_trend_missing_month() {
    let top = vec![("A".to_string(), "顧客A".to_string())];
    let monthly = vec![
        RawCustomerMonthlyRow { customer_code: "A".into(), year_month: dt(2025, 4, 1), total: 1000 },
        // 5月はBのみ
        RawCustomerMonthlyRow { customer_code: "B".into(), year_month: dt(2025, 5, 1), total: 500 },
    ];

    let result = build_customer_trend(&top, &monthly);
    assert_eq!(result[0].months[1].total_sales, 0); // A は5月データなし
    assert_eq!(result[0].months[1].rank, 0);
}

// ══════════════════════════════════════════════════════════════
// build_customer_detail
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_customer_detail() {
    let raw = vec![
        RawCustomerDetailRow { year_month: dt(2025, 4, 1), own_sales: 100, charter_sales: 50, transport_count: 10 },
        RawCustomerDetailRow { year_month: dt(2025, 5, 1), own_sales: 200, charter_sales: 80, transport_count: 15 },
    ];
    let result = build_customer_detail(&raw);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].year_month, "2025-04");
    assert_eq!(result[0].total_sales, 150);
    assert_eq!(result[1].total_sales, 280);
}

#[test]
fn test_build_customer_detail_empty() {
    assert!(build_customer_detail(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// mode_label
// ══════════════════════════════════════════════════════════════

#[test]
fn test_mode_label() {
    assert_eq!(mode_label("billing"), "請求+請求のみ");
    assert_eq!(mode_label("non_billing"), "請求+非請求");
    assert_eq!(mode_label("all"), "全て");
    assert_eq!(mode_label("unknown"), "全て");
}

// ══════════════════════════════════════════════════════════════
// customer-yoy-by-dept: rows_to_dept_customer_map
// ══════════════════════════════════════════════════════════════

#[test]
fn test_rows_to_dept_customer_map_basic() {
    let rows = vec![
        RawCustomerDeptRow {
            department_code: "01".into(),
            department_name: "本社".into(),
            customer_code: "A".into(),
            customer_name: "顧客A".into(),
            total: 1_000_000,
        },
        RawCustomerDeptRow {
            department_code: "02".into(),
            department_name: "大阪".into(),
            customer_code: "B".into(),
            customer_name: "顧客B".into(),
            total: 500_000,
        },
    ];
    let map = rows_to_dept_customer_map(&rows);
    assert_eq!(map.len(), 2);
    let v = map.get(&("01".into(), "A".into())).cloned().unwrap();
    assert_eq!(v.0, "本社");
    assert_eq!(v.1, "顧客A");
    assert_eq!(v.2, 1_000_000);
}

#[test]
fn test_rows_to_dept_customer_map_empty() {
    assert!(rows_to_dept_customer_map(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// customer-yoy-by-dept: calc_yoy_with_dept_entries
// ══════════════════════════════════════════════════════════════

fn make_dept_map(entries: &[(&str, &str, &str, &str, i64)]) -> std::collections::HashMap<(String, String), (String, String, i64)> {
    let mut map = HashMap::new();
    for (dc, dn, cc, cn, total) in entries {
        map.insert(
            ((*dc).to_string(), (*cc).to_string()),
            ((*dn).to_string(), (*cn).to_string(), *total),
        );
    }
    map
}

#[test]
fn test_calc_yoy_with_dept_entries_growth() {
    let cur = make_dept_map(&[("01", "本社", "A", "顧客A", 1_200_000)]);
    let prev = make_dept_map(&[("01", "本社", "A", "顧客A", 1_000_000)]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.department_code, "01");
    assert_eq!(e.department_name, "本社");
    assert_eq!(e.customer_code, "A");
    assert_eq!(e.current_total, 1_200_000);
    assert_eq!(e.prev_total, 1_000_000);
    assert_eq!(e.diff, 200_000);
    assert!((e.yoy_percent - 20.0).abs() < 1e-6);
}

#[test]
fn test_calc_yoy_with_dept_entries_filter_min_prev() {
    let cur = make_dept_map(&[("01", "本社", "A", "顧客A", 100)]);
    let prev = make_dept_map(&[("01", "本社", "A", "顧客A", 100)]);
    // min_prev=1000 → filtered out
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 1000);
    assert!(entries.is_empty());
}

#[test]
fn test_calc_yoy_with_dept_entries_prev_only_uses_prev_names() {
    let cur = HashMap::new();
    let prev = make_dept_map(&[("01", "本社", "A", "顧客A", 500_000)]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].department_name, "本社");
    assert_eq!(entries[0].customer_name, "顧客A");
    assert_eq!(entries[0].current_total, 0);
    assert_eq!(entries[0].prev_total, 500_000);
    assert_eq!(entries[0].diff, -500_000);
}

#[test]
fn test_calc_yoy_with_dept_entries_distinct_by_dept() {
    // 同じ顧客コードでも営業所が違えば別エントリ
    let cur = make_dept_map(&[
        ("01", "本社", "A", "顧客A", 1_000),
        ("02", "大阪", "A", "顧客A", 2_000),
    ]);
    let prev = make_dept_map(&[
        ("01", "本社", "A", "顧客A", 800),
        ("02", "大阪", "A", "顧客A", 3_000),
    ]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(entries.len(), 2);
}

// ══════════════════════════════════════════════════════════════
// split_and_sort_yoy_with_dept
// ══════════════════════════════════════════════════════════════

#[test]
fn test_split_and_sort_yoy_with_dept_basic() {
    let entries = vec![
        CustomerYoyWithDept { department_code: "01".into(), department_name: "本社".into(), customer_code: "A".into(), customer_name: "A".into(), current_total: 120, prev_total: 100, diff: 20, yoy_percent: 20.0 },
        CustomerYoyWithDept { department_code: "01".into(), department_name: "本社".into(), customer_code: "B".into(), customer_name: "B".into(), current_total: 80, prev_total: 100, diff: -20, yoy_percent: -20.0 },
        CustomerYoyWithDept { department_code: "02".into(), department_name: "大阪".into(), customer_code: "C".into(), customer_name: "C".into(), current_total: 100, prev_total: 100, diff: 0, yoy_percent: 0.0 },
    ];
    let (pos, neg) = split_and_sort_yoy_with_dept(entries, 10);
    assert_eq!(pos.len(), 1);
    assert_eq!(pos[0].customer_code, "A");
    assert_eq!(neg.len(), 1);
    assert_eq!(neg[0].customer_code, "B");
}

#[test]
fn test_split_and_sort_yoy_with_dept_limit() {
    let entries: Vec<CustomerYoyWithDept> = (0..20)
        .map(|i| CustomerYoyWithDept {
            department_code: "01".into(),
            department_name: "本社".into(),
            customer_code: format!("{:03}", i),
            customer_name: format!("顧客{}", i),
            current_total: 100 + i as i64,
            prev_total: 100,
            diff: i as i64,
            yoy_percent: i as f64,
        })
        .collect();
    let (pos, _) = split_and_sort_yoy_with_dept(entries, 5);
    assert_eq!(pos.len(), 5);
}

#[test]
fn test_split_and_sort_yoy_with_dept_neg_sort_by_percent() {
    let entries = vec![
        CustomerYoyWithDept { department_code: "01".into(), department_name: "D1".into(), customer_code: "A".into(), customer_name: "A".into(), current_total: 0, prev_total: 100, diff: -100, yoy_percent: -100.0 },
        CustomerYoyWithDept { department_code: "01".into(), department_name: "D1".into(), customer_code: "B".into(), customer_name: "B".into(), current_total: 90, prev_total: 100, diff: -10, yoy_percent: -10.0 },
    ];
    let (_, neg) = split_and_sort_yoy_with_dept(entries, 10);
    assert_eq!(neg.len(), 2);
    // 最も減少率が大きいものが先頭
    assert_eq!(neg[0].customer_code, "A");
}
