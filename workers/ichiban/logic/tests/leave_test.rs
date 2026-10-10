//! `/api/leave/days`・`/api/leave/employees` の純粋部分 (SQL の形・Query の検証・行の詰め直し)。

use chrono::NaiveDate;
use ichiban_logic::api::{ListResponse, EMPLOYEES_SOURCE};
use ichiban_logic::leave::{
    build_leave_days, build_leave_employees, LeaveDaysPlan, LeaveDaysQuery, RawLeaveDayRow,
    RawLeaveEmployeeRow, LEAVE_CUSTOMER_CODE, LEAVE_DAYS_SOURCE, LEAVE_DAYS_SQL,
    LEAVE_EMPLOYEES_SQL, MAX_DAYS,
};

fn query(from: Option<&str>, to: Option<&str>) -> LeaveDaysQuery {
    LeaveDaysQuery {
        from: from.map(str::to_string),
        to: to.map(str::to_string),
    }
}

fn dt(y: i32, m: u32, d: u32, h: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, 0, 0)
        .unwrap()
}

// ── SQL ──

#[test]
fn test_days_sql_binds_every_value() {
    for p in ["@P1", "@P2", "@P3"] {
        assert!(LEAVE_DAYS_SQL.contains(p), "{p}");
    }
    assert!(LEAVE_DAYS_SQL.contains("[得意先C] = @P1"));
    // 得意先C も日付もリテラルで埋めない
    assert!(!LEAVE_DAYS_SQL.contains("000002"));
    assert_eq!(LEAVE_CUSTOMER_CODE, "000002");
    // 部門C の絞りは呼ぶ側の仕事
    assert!(!LEAVE_DAYS_SQL.contains("部門C"));
    assert!(LEAVE_DAYS_SQL.ends_with("ORDER BY [運行年月日], [運転手C]"));
}

#[test]
fn test_employees_sql_selects_only_needed_columns() {
    for col in ["社員C", "社員N", "部門C", "入社年月日", "退職年月日"] {
        assert!(LEAVE_EMPLOYEES_SQL.contains(col), "{col}");
    }
    for col in ["住所", "電話", "携帯", "生年月日", "免許", "性別", "血液"] {
        assert!(!LEAVE_EMPLOYEES_SQL.contains(col), "{col}");
    }
    assert!(LEAVE_EMPLOYEES_SQL.contains("GROUP BY [社員C]"));
    assert!(!LEAVE_EMPLOYEES_SQL.contains('*'));
    // 値を取らない SQL なので bind も無い
    assert!(!LEAVE_EMPLOYEES_SQL.contains("@P"));
}

// ── Query ──

#[test]
fn test_plan_ok_and_exclusive_end() {
    let p = query(Some("2026-10-01"), Some("2026-10-31")).plan();
    assert_eq!(
        p,
        Some(LeaveDaysPlan {
            from: "2026-10-01".into(),
            to_exclusive: "2026-11-01".into(),
        })
    );
    // 1 日だけ・年跨ぎ
    let p = query(Some("2026-12-31"), Some("2026-12-31"))
        .plan()
        .unwrap();
    assert_eq!(p.to_exclusive, "2027-01-01");
}

#[test]
fn test_plan_rejects_missing() {
    assert!(query(None, Some("2026-10-31")).plan().is_none());
    assert!(query(Some("2026-10-01"), None).plan().is_none());
    assert!(query(None, None).plan().is_none());
}

#[test]
fn test_plan_rejects_bad_dates() {
    for bad in [
        "",
        "abc",
        "2026-13-01",
        "2026-02-30",
        "2026-1-5",
        "20261001",
        "2026/10/01",
    ] {
        assert!(
            query(Some(bad), Some("2026-10-31")).plan().is_none(),
            "{bad}"
        );
        assert!(
            query(Some("2026-10-01"), Some(bad)).plan().is_none(),
            "{bad}"
        );
    }
}

#[test]
fn test_plan_rejects_reversed() {
    assert!(query(Some("2026-10-02"), Some("2026-10-01"))
        .plan()
        .is_none());
}

#[test]
fn test_plan_span_limit() {
    assert_eq!(MAX_DAYS, 92);
    // 2026-10-01 から数えて 92 日目は 2026-12-31、93 日目は 2027-01-01
    assert!(query(Some("2026-10-01"), Some("2026-12-31"))
        .plan()
        .is_some());
    assert!(query(Some("2026-10-01"), Some("2027-01-01"))
        .plan()
        .is_none());
}

// ── 組み立て ──

#[test]
fn test_build_leave_days_trims_and_formats() {
    let raw = vec![
        RawLeaveDayRow {
            employee_code: " 1021 ".into(),
            date: dt(2026, 10, 1, 0),
            item_name: "有休 ".into(),
        },
        RawLeaveDayRow {
            employee_code: "1022".into(),
            date: dt(2026, 10, 2, 13),
            item_name: "".into(),
        },
    ];
    let out = build_leave_days(&raw);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].employee_code, "1021");
    assert_eq!(out[0].date, "2026-10-01");
    assert_eq!(out[0].item_name, "有休");
    assert_eq!(out[1].date, "2026-10-02"); // 時刻部は捨てる
    assert_eq!(out[1].item_name, "");
}

#[test]
fn test_days_json_shape() {
    let raw = vec![RawLeaveDayRow {
        employee_code: "1021".into(),
        date: dt(2026, 10, 1, 0),
        item_name: "有休".into(),
    }];
    let body = ListResponse {
        source_table: LEAVE_DAYS_SOURCE.to_string(),
        data: build_leave_days(&raw),
    };
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        r#"{"source_table":"運転日報明細","data":[{"employee_code":"1021","date":"2026-10-01","item_name":"有休"}]}"#
    );
}

#[test]
fn test_build_leave_employees_null_dates_and_trim() {
    let raw = vec![
        RawLeaveEmployeeRow {
            employee_code: "1021 ".into(),
            employee_name: " 大石 太郎".into(),
            dept_code: "010 ".into(),
            hire_date: Some(dt(2010, 4, 1, 0)),
            retire_date: None,
        },
        RawLeaveEmployeeRow {
            employee_code: "1022".into(),
            employee_name: "".into(),
            dept_code: "030".into(),
            hire_date: None,
            retire_date: Some(dt(2020, 3, 31, 9)),
        },
    ];
    let out = build_leave_employees(&raw);
    assert_eq!(out[0].employee_code, "1021");
    assert_eq!(out[0].employee_name, "大石 太郎");
    assert_eq!(out[0].dept_code, "010");
    assert_eq!(out[0].hire_date.as_deref(), Some("2010-04-01"));
    assert_eq!(out[0].retire_date, None);
    assert_eq!(out[1].hire_date, None);
    assert_eq!(out[1].retire_date.as_deref(), Some("2020-03-31"));
}

#[test]
fn test_employees_json_shape() {
    let raw = vec![RawLeaveEmployeeRow {
        employee_code: "1021".into(),
        employee_name: "山田".into(),
        dept_code: "010".into(),
        hire_date: Some(dt(2010, 4, 1, 0)),
        retire_date: None,
    }];
    let body = ListResponse {
        source_table: EMPLOYEES_SOURCE.to_string(),
        data: build_leave_employees(&raw),
    };
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        r#"{"source_table":"社員ﾏｽﾀ","data":[{"employee_code":"1021","employee_name":"山田","dept_code":"010","hire_date":"2010-04-01","retire_date":null}]}"#
    );
}
