//! `/api/sales/monthly`・`/by-department`・`/by-customer`・`/yoy` の純粋部分
//! (オンプレ版の tests/sales_logic_test.rs の該当分を写し、Query・SQL の切り替えを足した)。

use chrono::{NaiveDate, NaiveDateTime};
use ichiban_logic::sales_monthly::*;

fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn monthly_row(y: i32, m: u32, own: i64, charter: i64, count: i32) -> RawMonthlyRow {
    RawMonthlyRow {
        year_month: dt(y, m, 1),
        own_sales: own,
        charter_sales: charter,
        transport_count: count,
    }
}

// ══════════════════════════════════════════════════════════════
// build_monthly_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_monthly_sales_with_prev() {
    let current = vec![
        monthly_row(2025, 4, 1_000_000, 500_000, 50),
        monthly_row(2025, 5, 1_200_000, 600_000, 55),
    ];
    let prev = vec![monthly_row(2024, 4, 900_000, 400_000, 0)];

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
    assert!(build_monthly_sales(&[], &[]).is_empty());
}

#[test]
fn test_build_monthly_sales_no_prev() {
    let current = vec![monthly_row(2025, 4, 100, 50, 10)];
    let result = build_monthly_sales(&current, &[]);
    assert_eq!(result[0].prev_year_own, 0);
    assert_eq!(result[0].prev_year_charter, 0);
    assert_eq!(result[0].prev_year_total, 0);
}

#[test]
fn test_monthly_sales_json_shape() {
    let current = vec![monthly_row(2025, 4, 100, 50, 10)];
    let json = serde_json::to_string(&build_monthly_sales(&current, &[])).unwrap();
    assert_eq!(
        json,
        r#"[{"year_month":"2025-04","own_sales":100,"charter_sales":50,"total_sales":150,"transport_count":10,"prev_year_own":0,"prev_year_charter":0,"prev_year_total":0}]"#
    );
}

// ══════════════════════════════════════════════════════════════
// build_department_sales / build_customer_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_department_sales() {
    let raw = vec![
        RawDepartmentRow {
            department_code: "01".into(),
            department_name: "本社".into(),
            own_sales: 500,
            charter_sales: 200,
            transport_count: 10,
        },
        RawDepartmentRow {
            department_code: "02".into(),
            department_name: "支店".into(),
            own_sales: 300,
            charter_sales: 100,
            transport_count: 5,
        },
    ];
    let result = build_department_sales(&raw);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].total_sales, 700);
    assert_eq!(result[1].total_sales, 400);
    assert_eq!(result[0].department_name, "本社");
    assert_eq!(result[1].transport_count, 5);
}

#[test]
fn test_build_department_sales_empty() {
    assert!(build_department_sales(&[]).is_empty());
}

#[test]
fn test_build_customer_sales() {
    let raw = vec![RawCustomerRow {
        customer_code: "001".into(),
        customer_name: "得意先A".into(),
        own_sales: 1000,
        charter_sales: 500,
        transport_count: 20,
    }];
    let result = build_customer_sales(&raw);
    assert_eq!(result[0].customer_code, "001");
    assert_eq!(result[0].customer_name, "得意先A");
    assert_eq!(result[0].total_sales, 1500);
    assert_eq!(result[0].transport_count, 20);
}

// ══════════════════════════════════════════════════════════════
// build_yoy_comparison
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_yoy_comparison() {
    let current = vec![
        RawMonthTotalRow {
            month: 1,
            total: 1_000_000,
        },
        RawMonthTotalRow {
            month: 2,
            total: 1_200_000,
        },
        RawMonthTotalRow {
            month: 3,
            total: 900_000,
        },
    ];
    let prev = vec![
        RawMonthTotalRow {
            month: 1,
            total: 900_000,
        },
        RawMonthTotalRow {
            month: 2,
            total: 1_200_000,
        },
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
// MonthlyQuery: 既定値・期間・部門の絞り込み
// ══════════════════════════════════════════════════════════════

#[test]
fn test_monthly_range_defaults() {
    let r = MonthlyQuery::default().range();
    assert_eq!(
        r,
        MonthlyRange {
            from: "2025-04-01".into(),
            to: "2026-03-01".into(),
            prev_from: "2024-04-01".into(),
            prev_to: "2025-03-01".into(),
        }
    );
}

#[test]
fn test_monthly_range_given() {
    let q = MonthlyQuery {
        from: Some("2024-01".into()),
        to: Some("2024-12".into()),
        ..Default::default()
    };
    let r = q.range();
    assert_eq!(r.from, "2024-01-01");
    assert_eq!(r.to, "2024-12-01");
    assert_eq!(r.prev_from, "2023-01-01");
    assert_eq!(r.prev_to, "2023-12-01");
}

#[test]
fn test_monthly_scope_all() {
    let q = MonthlyQuery::default();
    let s = q.scope();
    assert_eq!(s, MonthlyScope::All);
    assert_eq!(s.current_sql(), MONTHLY_ALL_SQL);
    assert_eq!(s.prev_sql(), MONTHLY_ALL_PREV_SQL);
    assert!(!s.prev_has_transport_count());
    assert_eq!(s.param(), None);
    assert_eq!(s.source_table(), "種別別月計 (種別C=99)");
}

#[test]
fn test_monthly_scope_include_dept() {
    let q = MonthlyQuery {
        include_dept: Some("03".into()),
        ..Default::default()
    };
    let s = q.scope();
    assert_eq!(s, MonthlyScope::Dept("03"));
    assert_eq!(s.current_sql(), MONTHLY_DEPT_SQL);
    assert_eq!(s.prev_sql(), MONTHLY_DEPT_SQL);
    assert!(s.prev_has_transport_count());
    assert_eq!(s.param(), Some("03".to_string()));
    assert_eq!(s.source_table(), "部門別月計 (部門C=03)");
}

#[test]
fn test_monthly_scope_exclude_dept() {
    let q = MonthlyQuery {
        exclude_dept: Some("釧路".into()),
        ..Default::default()
    };
    let s = q.scope();
    assert_eq!(s, MonthlyScope::Exclude("釧路"));
    assert_eq!(s.current_sql(), MONTHLY_EXCLUDE_SQL);
    assert_eq!(s.prev_sql(), MONTHLY_EXCLUDE_SQL);
    assert!(s.prev_has_transport_count());
    assert_eq!(s.param(), Some("%釧路%".to_string()));
    assert_eq!(s.source_table(), "部門別月計 (釧路除く)");
}

#[test]
fn test_monthly_scope_include_wins_over_exclude() {
    let q = MonthlyQuery {
        include_dept: Some(String::new()),
        exclude_dept: Some("釧路".into()),
        ..Default::default()
    };
    // 空文字も指定として扱う (オンプレ版は Option<&str> の Some で分岐)
    assert_eq!(q.scope(), MonthlyScope::Dept(""));
}

// ══════════════════════════════════════════════════════════════
// PeriodQuery / CustomerQuery
// ══════════════════════════════════════════════════════════════

#[test]
fn test_period_dates() {
    assert_eq!(
        PeriodQuery::default().dates(),
        ("2025-04-01".to_string(), "2026-03-01".to_string())
    );
    let q = PeriodQuery {
        from: Some("2026-01".into()),
        to: Some("2026-02".into()),
    };
    assert_eq!(
        q.dates(),
        ("2026-01-01".to_string(), "2026-02-01".to_string())
    );
}

#[test]
fn test_customer_dates_and_top() {
    let q = CustomerQuery::default();
    assert_eq!(
        q.dates(),
        ("2025-04-01".to_string(), "2026-03-01".to_string())
    );
    // 既定 20
    assert_eq!(q.top(), Some(20));
    let top = |limit| {
        CustomerQuery {
            limit: Some(limit),
            ..Default::default()
        }
        .top()
    };
    assert_eq!(top(1), Some(1));
    assert_eq!(top(100), Some(100));
    // 上限 100
    assert_eq!(top(101), Some(100));
    assert_eq!(top(i32::MAX), Some(100));
    // 0 は TOP 0 で 200 + 空配列 (オンプレ版と同じ)
    assert_eq!(top(0), Some(0));
    assert!(by_customer_sql(0).starts_with("SELECT TOP 0 "));
    // 負数は 400 (オンプレ版は TOP -1 で 500)
    assert_eq!(top(-1), None);
}

#[test]
fn test_by_customer_sql_top() {
    let sql = by_customer_sql(20);
    assert!(sql.starts_with("SELECT TOP 20 m.[得意先C], "));
    assert!(sql.contains("FROM [得意先別月計] m"));
    assert!(sql.ends_with("DESC"));
}

// ══════════════════════════════════════════════════════════════
// YoyQuery
// ══════════════════════════════════════════════════════════════

#[test]
fn test_yoy_year_default_and_range() {
    let q = YoyQuery::default();
    assert_eq!(q.year(), 2026);
    assert_eq!(
        q.range(),
        YoyRange {
            from: "2026-01-01".into(),
            to: "2026-12-01".into(),
            prev_from: "2025-01-01".into(),
            prev_to: "2025-12-01".into(),
        }
    );
}

#[test]
fn test_yoy_range_given_year() {
    let r = YoyQuery { year: Some(2024) }.range();
    assert_eq!(r.from, "2024-01-01");
    assert_eq!(r.prev_to, "2023-12-01");
}

#[test]
fn test_yoy_range_min_year_does_not_panic() {
    let r = YoyQuery {
        year: Some(i32::MIN),
    }
    .range();
    assert_eq!(r.from, format!("{}-01-01", i32::MIN));
}

// ══════════════════════════════════════════════════════════════
// SQL: 値はバインドだけ (文字列で埋め込まない)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_sql_binds_values() {
    for sql in [
        MONTHLY_DEPT_SQL,
        MONTHLY_EXCLUDE_SQL,
        MONTHLY_ALL_SQL,
        MONTHLY_ALL_PREV_SQL,
        BY_DEPARTMENT_SQL,
        YOY_SQL,
    ] {
        assert!(sql.contains("@P1") && sql.contains("@P2"));
    }
    assert!(MONTHLY_DEPT_SQL.contains("@P3"));
    assert!(MONTHLY_EXCLUDE_SQL.contains("NOT LIKE @P3"));
    assert!(!MONTHLY_ALL_SQL.contains("@P3"));
}

#[test]
fn test_source_constants() {
    assert_eq!(MONTHLY_ALL_SOURCE, "種別別月計 (種別C=99)");
    assert_eq!(DEPARTMENT_SOURCE, "部門別月計 + 部門ﾏｽﾀ");
    assert_eq!(CUSTOMER_SOURCE, "得意先別月計 + 得意先ﾏｽﾀ");
}
