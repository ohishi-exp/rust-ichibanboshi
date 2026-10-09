//! 6 本の SQL 文。文字列そのものはオンプレ版から一字も変えずに移した (移動時に diff で確認済み)。
//! ここでは組み立て (TOP の丸め) と、売上集計ルールの式・自車/傭車の列が落ちていないことを固定する。

use ichiban_logic::sql::{
    costs_daily_sql, like_pattern, vehicle_daily_sql, COSTS_DAILY_SQL_BODY, DEPARTMENTS_SQL,
    EMPLOYEES_SQL, HEALTH_SQL, VEHICLES_SQL, VEHICLE_DAILY_SQL_BODY,
};

#[test]
fn test_vehicle_daily_sql_top_is_clamped() {
    assert!(vehicle_daily_sql(0).starts_with("SELECT TOP 1 t.[売上年月日], "));
    assert!(vehicle_daily_sql(500).starts_with("SELECT TOP 500 t.[売上年月日], "));
    assert!(vehicle_daily_sql(9999).starts_with("SELECT TOP 5000 t.[売上年月日], "));
    assert_eq!(
        vehicle_daily_sql(500),
        format!("SELECT TOP 500 {VEHICLE_DAILY_SQL_BODY}")
    );
}

#[test]
fn test_costs_daily_sql_top_is_clamped() {
    assert!(costs_daily_sql(-1).starts_with("SELECT TOP 1 t.[運行年月日], "));
    assert!(costs_daily_sql(5001).starts_with("SELECT TOP 5000 t.[運行年月日], "));
    assert_eq!(
        costs_daily_sql(42),
        format!("SELECT TOP 42 {COSTS_DAILY_SQL_BODY}")
    );
}

#[test]
fn test_vehicle_daily_uses_tax_excluded_amounts() {
    // 月計一致ルール (CLAUDE.md)。`金額` 列は使わない
    assert!(VEHICLE_DAILY_SQL_BODY.contains(
        "ISNULL(t.[税抜金額],0)+ISNULL(t.[税抜割増],0)+ISNULL(t.[税抜実費],0)-ISNULL(t.[値引],0)"
    ));
    assert!(VEHICLE_DAILY_SQL_BODY.contains(
        "ISNULL(t.[税抜傭車金額],0)+ISNULL(t.[税抜傭車割増],0)+ISNULL(t.[税抜傭車実費],0)-ISNULL(t.[傭車値引],0)"
    ));
    assert!(VEHICLE_DAILY_SQL_BODY.contains("ISNULL(t.[傭車先C], '')"));
    assert!(!VEHICLE_DAILY_SQL_BODY.contains("t.[金額]"));
    assert!(COSTS_DAILY_SQL_BODY.contains("ISNULL(t.[税抜金額], 0)"));
    assert!(!COSTS_DAILY_SQL_BODY.contains("t.[金額]"));
}

#[test]
fn test_master_sqls() {
    assert_eq!(HEALTH_SQL, "SELECT 1");
    assert!(DEPARTMENTS_SQL.contains("FROM [部門ﾏｽﾀ]"));
    assert!(VEHICLES_SQL.contains("FROM [車種ﾏｽﾀ]"));
    assert!(EMPLOYEES_SQL.contains("FROM [社員ﾏｽﾀ] GROUP BY [社員C]"));
}

#[test]
fn test_like_pattern() {
    assert_eq!(like_pattern(None), None);
    assert_eq!(like_pattern(Some("釧路")), Some("%釧路%".to_string()));
}
