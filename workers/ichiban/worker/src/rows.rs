//! `tiberius::Row` から ichiban-logic の型を詰める (6 本の行の詰め直し)。
//!
//! **オンプレ版 (repo ルートの `src/repo.rs`) の `decode_cp932`・`get_i64`・`get_f64`・`rows_to_vehicle_daily`・
//! `rows_to_costs_daily` と、employees / vehicles / list_departments の詰め方を列番号まで同じに写している。**
//! 列の並びは `ichiban_logic::sql` の各定数と 1 対 1 — SQL の列を変えるときはオンプレ版とここの両方を直す。
//! オンプレ版の 6 本を削除するまでの一時的な重複 (差は並走期間の応答の比較で検知する)。
//!
//! 違いは 1 つだけ: 日付の列はオンプレ版の `get` (型が合わなければ panic) ではなく `try_get` で読み、
//! 型が合わなければ既定値にする (Worker の panic はインスタンスごと落とすため)。型が合う行の値は同じ。

use chrono::NaiveDateTime;
use ichiban_logic::api::{Department, EmployeeRow, VehicleOption};
use ichiban_logic::costs_daily::RawCostsDailyRow;
use ichiban_logic::vehicle_daily::RawVehicleDailyRow;
use tiberius::numeric::Numeric;
use tiberius::Row;

/// 文字列の列 (varchar の CP932 は tiberius が読む)。NULL・型不一致は空文字、前後の空白は落とす。
fn decode_cp932(row: &Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

/// 金額の列。f64 → decimal → i32 の順で試し、小数は切り捨てる。読めなければ 0。
fn get_i64(row: &Row, idx: usize) -> i64 {
    row.try_get::<f64, _>(idx)
        .ok()
        .flatten()
        .map(|v| v as i64)
        .or_else(|| {
            row.try_get::<Numeric, _>(idx)
                .ok()
                .flatten()
                .and_then(|d| format!("{d}").parse::<f64>().ok())
                .map(|v| v as i64)
        })
        .or_else(|| row.try_get::<i32, _>(idx).ok().flatten().map(|v| v as i64))
        .unwrap_or(0)
}

/// `get_i64` と同じ decimal/f64/i32 の順で試すが、端数を切り捨てない (`単価`/`数量` 用)。
fn get_f64(row: &Row, idx: usize) -> f64 {
    row.try_get::<f64, _>(idx)
        .ok()
        .flatten()
        .or_else(|| {
            row.try_get::<Numeric, _>(idx)
                .ok()
                .flatten()
                .and_then(|d| format!("{d}").parse::<f64>().ok())
        })
        .or_else(|| row.try_get::<i32, _>(idx).ok().flatten().map(|v| v as f64))
        .unwrap_or(0.0)
}

/// 日付の列 (オンプレ版は `r.get(0).unwrap_or_default()`)。NULL・型不一致は既定値。
fn get_datetime(row: &Row, idx: usize) -> NaiveDateTime {
    row.try_get::<NaiveDateTime, _>(idx)
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// `EMPLOYEES_SQL` の列: 0 社員C, 1 社員N, 2 社員R。
pub(crate) fn employees(rows: &[Row]) -> Vec<EmployeeRow> {
    rows.iter()
        .map(|r| EmployeeRow {
            employee_code: decode_cp932(r, 0),
            employee_name: decode_cp932(r, 1),
            employee_r: decode_cp932(r, 2),
        })
        .collect()
}

/// `VEHICLES_SQL` の列: 0 車種C, 1 車種N。
pub(crate) fn vehicles(rows: &[Row]) -> Vec<VehicleOption> {
    rows.iter()
        .map(|r| VehicleOption {
            vehicle_code: decode_cp932(r, 0),
            vehicle_name: decode_cp932(r, 1),
        })
        .collect()
}

/// `DEPARTMENTS_SQL` の列: 0 部門C, 1 部門N。
pub(crate) fn departments(rows: &[Row]) -> Vec<Department> {
    rows.iter()
        .map(|r| Department {
            department_code: decode_cp932(r, 0),
            department_name: decode_cp932(r, 1),
        })
        .collect()
}

/// `VEHICLE_DAILY_SQL_BODY` の列 (オンプレ版 `rows_to_vehicle_daily` と同じ番号)。
pub(crate) fn vehicle_daily(rows: &[Row]) -> Vec<RawVehicleDailyRow> {
    rows.iter()
        .map(|r| RawVehicleDailyRow {
            sale_date: get_datetime(r, 0),
            vehicle_number: decode_cp932(r, 1),
            customer_code: decode_cp932(r, 2),
            customer_name: decode_cp932(r, 3),
            origin_area_name: decode_cp932(r, 4),
            dest_area_name: decode_cp932(r, 5),
            origin: decode_cp932(r, 6),
            dest: decode_cp932(r, 7),
            subcontractor_code: decode_cp932(r, 8),
            self_amount: get_i64(r, 9),
            subcontract_amount: get_i64(r, 10),
            item_code: decode_cp932(r, 11),
            item_name: decode_cp932(r, 12),
            quantity: get_f64(r, 13),
            unit_price: get_f64(r, 14),
            unit: decode_cp932(r, 15),
            row_id: decode_cp932(r, 16),
            vehicle_branch: decode_cp932(r, 17),
            driver_code: decode_cp932(r, 18),
            driver_name: decode_cp932(r, 19),
            request_kind: decode_cp932(r, 20),
        })
        .collect()
}

/// `COSTS_DAILY_SQL_BODY` の列 (オンプレ版 `rows_to_costs_daily` と同じ番号)。
pub(crate) fn costs_daily(rows: &[Row]) -> Vec<RawCostsDailyRow> {
    rows.iter()
        .map(|r| RawCostsDailyRow {
            operation_date: get_datetime(r, 0),
            vehicle_number: decode_cp932(r, 1),
            vehicle_branch: decode_cp932(r, 2),
            driver_code: decode_cp932(r, 3),
            cost_code: decode_cp932(r, 4),
            cost_name: decode_cp932(r, 5),
            cost_kind: decode_cp932(r, 6),
            cost_kind_name: decode_cp932(r, 7),
            quantity: get_f64(r, 8),
            unit_price: get_f64(r, 9),
            amount: get_i64(r, 10),
            diesel_tax: get_i64(r, 11),
            km: get_f64(r, 12),
            fixed_cost_flag: decode_cp932(r, 13),
            row_id: decode_cp932(r, 14),
            remarks: decode_cp932(r, 15),
            vendor_code: decode_cp932(r, 16),
            vendor_branch: decode_cp932(r, 17),
            vendor_name: decode_cp932(r, 18),
            // NULL も型不一致も None (空文字で返る)。オンプレ版と同じ
            entered_date: r.try_get::<NaiveDateTime, _>(19).ok().flatten(),
        })
        .collect()
}
