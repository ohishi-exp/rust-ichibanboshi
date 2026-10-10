//! `/api/sales/customer-yoy`・`/api/sales/customer-yoy-by-dept` の `tiberius::Row` → `ichiban_logic::sales_yoy` の Raw 型。
//! オンプレ版 `src/repo.rs` の `rows_to_code_total_map`・`rows_to_customer_dept` と同じ列番号。

use ichiban_logic::sales_yoy::{RawCustomerDeptRow, RawCustomerTotalRow};
use tiberius::Row;

use super::{decode_cp932, get_i64};

/// `CUSTOMER_YOY_SQL` の列: 0 得意先C, 1 得意先N, 2 合計。
pub(crate) fn customer_totals(rows: &[Row]) -> Vec<RawCustomerTotalRow> {
    rows.iter()
        .map(|r| RawCustomerTotalRow {
            customer_code: decode_cp932(r, 0),
            customer_name: decode_cp932(r, 1),
            total: get_i64(r, 2),
        })
        .collect()
}

/// `customer_yoy_by_dept_sql` の列: 0 受注部門, 1 部門N, 2 得意先C, 3 得意先N, 4 合計。
pub(crate) fn customer_dept_totals(rows: &[Row]) -> Vec<RawCustomerDeptRow> {
    rows.iter()
        .map(|r| RawCustomerDeptRow {
            department_code: decode_cp932(r, 0),
            department_name: decode_cp932(r, 1),
            customer_code: decode_cp932(r, 2),
            customer_name: decode_cp932(r, 3),
            total: get_i64(r, 4),
        })
        .collect()
}
