//! 応答の型の JSON の形。オンプレ版の `ApiResponse`・`DepartmentOption` と同じ出力であることを固定する
//! (並走期間にオンプレ版と Worker の応答を sha256 で比べるため)。

use ichiban_logic::api::{
    Department, EmployeeRow, ListResponse, VehicleOption, COSTS_DAILY_SOURCE, DEPARTMENTS_SOURCE,
    EMPLOYEES_SOURCE, VEHICLES_SOURCE, VEHICLE_DAILY_SOURCE,
};
use ichiban_logic::{clamp_limit, normalize_filter};

#[test]
fn test_departments_json() {
    let res = ListResponse {
        source_table: DEPARTMENTS_SOURCE.to_string(),
        data: vec![Department {
            department_code: "010".into(),
            department_name: "本社".into(),
        }],
    };
    assert_eq!(
        serde_json::to_string(&res).unwrap(),
        r#"{"source_table":"部門ﾏｽﾀ","data":[{"department_code":"010","department_name":"本社"}]}"#
    );
}

#[test]
fn test_employees_json() {
    let res = ListResponse {
        source_table: EMPLOYEES_SOURCE.to_string(),
        data: vec![EmployeeRow {
            employee_code: "1656".into(),
            employee_name: "西島 健太".into(),
            employee_r: "西島".into(),
        }],
    };
    assert_eq!(
        serde_json::to_string(&res).unwrap(),
        r#"{"source_table":"社員ﾏｽﾀ","data":[{"employee_code":"1656","employee_name":"西島 健太","employee_r":"西島"}]}"#
    );
}

#[test]
fn test_vehicles_json() {
    let res = ListResponse {
        source_table: VEHICLES_SOURCE.to_string(),
        data: vec![VehicleOption {
            vehicle_code: "01".into(),
            vehicle_name: "大型".into(),
        }],
    };
    assert_eq!(
        serde_json::to_string(&res).unwrap(),
        r#"{"source_table":"車種ﾏｽﾀ","data":[{"vehicle_code":"01","vehicle_name":"大型"}]}"#
    );
}

#[test]
fn test_source_tables() {
    assert_eq!(
        VEHICLE_DAILY_SOURCE,
        "運転日報明細 + 得意先ﾏｽﾀ + 地域ﾏｽﾀ + 社員ﾏｽﾀ"
    );
    assert_eq!(COSTS_DAILY_SOURCE, "経費明細 + 経費ﾏｽﾀ + 経費種別ﾏｽﾀ");
}

#[test]
fn test_normalize_filter() {
    assert_eq!(normalize_filter(&None), None);
    assert_eq!(normalize_filter(&Some(String::new())), None);
    assert_eq!(normalize_filter(&Some("  ".into())), None);
    assert_eq!(normalize_filter(&Some(" 8504 ".into())), Some("8504"));
}

#[test]
fn test_clamp_limit() {
    assert_eq!(clamp_limit(None), 500);
    assert_eq!(clamp_limit(Some(0)), 1);
    assert_eq!(clamp_limit(Some(7)), 7);
    assert_eq!(clamp_limit(Some(5001)), 5000);
}
