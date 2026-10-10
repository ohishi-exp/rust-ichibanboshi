//! `/api/sales/monthly`・`/api/sales/by-department`・`/api/sales/by-customer`・`/api/sales/yoy` の
//! `tiberius::Row` → `ichiban_logic::sales_monthly` の Raw 型 (Refs #322)。
//! 列の並びは `ichiban_logic::sales_monthly` の各 SQL と 1 対 1 (オンプレ版 `src/repo.rs` の `rows_to_monthly` 等と同じ番号)。

use ichiban_logic::sales_monthly::{
    RawCustomerRow, RawDepartmentRow, RawMonthTotalRow, RawMonthlyRow,
};
use tiberius::Row;

use super::{decode_cp932, get_datetime, get_i32, get_i64};

/// 0 年月度, 1 自車売上, 2 傭車売上, 3 輸送回数。
pub(crate) fn monthly(rows: &[Row]) -> Vec<RawMonthlyRow> {
    rows.iter()
        .map(|r| RawMonthlyRow {
            year_month: get_datetime(r, 0),
            own_sales: get_i64(r, 1),
            charter_sales: get_i64(r, 2),
            transport_count: get_i32(r, 3),
        })
        .collect()
}

/// `MONTHLY_ALL_PREV_SQL` の列: 0 年月度, 1 自車売上, 2 傭車売上 (輸送回数は無いので 0)。
pub(crate) fn monthly_prev(rows: &[Row]) -> Vec<RawMonthlyRow> {
    rows.iter()
        .map(|r| RawMonthlyRow {
            year_month: get_datetime(r, 0),
            own_sales: get_i64(r, 1),
            charter_sales: get_i64(r, 2),
            transport_count: 0,
        })
        .collect()
}

/// `BY_DEPARTMENT_SQL` の列: 0 部門C, 1 部門N, 2 自車売上, 3 傭車売上, 4 輸送回数。
pub(crate) fn by_department(rows: &[Row]) -> Vec<RawDepartmentRow> {
    rows.iter()
        .map(|r| RawDepartmentRow {
            department_code: decode_cp932(r, 0),
            department_name: decode_cp932(r, 1),
            own_sales: get_i64(r, 2),
            charter_sales: get_i64(r, 3),
            transport_count: get_i64(r, 4),
        })
        .collect()
}

/// `by_customer_sql` の列: 0 得意先C, 1 得意先N, 2 自車売上, 3 傭車売上, 4 輸送回数。
pub(crate) fn by_customer(rows: &[Row]) -> Vec<RawCustomerRow> {
    rows.iter()
        .map(|r| RawCustomerRow {
            customer_code: decode_cp932(r, 0),
            customer_name: decode_cp932(r, 1),
            own_sales: get_i64(r, 2),
            charter_sales: get_i64(r, 3),
            transport_count: get_i64(r, 4),
        })
        .collect()
}

/// `YOY_SQL` の列: 0 月, 1 合計。
pub(crate) fn month_totals(rows: &[Row]) -> Vec<RawMonthTotalRow> {
    rows.iter()
        .map(|r| RawMonthTotalRow {
            month: get_i32(r, 0),
            total: get_i64(r, 1),
        })
        .collect()
}
