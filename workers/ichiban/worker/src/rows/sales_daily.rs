//! `/api/sales/daily`・`/api/sales/customer-trend`・`/api/sales/customer-detail` の `tiberius::Row` → `ichiban_logic::sales_daily` の Raw 型。
//! 列番号はオンプレ版 `src/repo.rs` の `daily`・`customer_trend_data`・`customer_detail_data` の詰め方と同じ
//! (SQL の列の並びは `ichiban_logic::sales_daily` の各 SQL と 1 対 1)。日付は panic しない `get_datetime`。

use ichiban_logic::sales_daily::{
    RawCustomerDetailRow, RawCustomerMonthlyRow, RawDailyPrevRow, RawDailyRow,
};
use tiberius::Row;

use super::{decode_cp932, get_datetime, get_i32, get_i64};

/// daily 当期の列: 0 売上年月日, 1 自車, 2 傭車, 3 自車 raw, 4 傭車 raw, 5 件数。
pub(crate) fn daily(rows: &[Row]) -> Vec<RawDailyRow> {
    rows.iter()
        .map(|r| RawDailyRow {
            date: get_datetime(r, 0),
            own_sales: get_i64(r, 1),
            charter_sales: get_i64(r, 2),
            own_sales_raw: get_i64(r, 3),
            charter_sales_raw: get_i64(r, 4),
            transport_count: get_i32(r, 5),
        })
        .collect()
}

/// daily 前年の列: 0 売上年月日, 1 自車, 2 傭車, 3 自車 raw, 4 傭車 raw。
pub(crate) fn daily_prev(rows: &[Row]) -> Vec<RawDailyPrevRow> {
    rows.iter()
        .map(|r| RawDailyPrevRow {
            date: get_datetime(r, 0),
            own_sales: get_i64(r, 1),
            charter_sales: get_i64(r, 2),
            own_sales_raw: get_i64(r, 3),
            charter_sales_raw: get_i64(r, 4),
        })
        .collect()
}

/// customer-trend の TOP n の列: 0 得意先C, 1 得意先N。
pub(crate) fn top_customers(rows: &[Row]) -> Vec<(String, String)> {
    rows.iter()
        .map(|r| (decode_cp932(r, 0), decode_cp932(r, 1)))
        .collect()
}

/// customer-trend の月別の列: 0 得意先C, 1 年月度, 2 合計。
pub(crate) fn customer_monthly(rows: &[Row]) -> Vec<RawCustomerMonthlyRow> {
    rows.iter()
        .map(|r| RawCustomerMonthlyRow {
            customer_code: decode_cp932(r, 0),
            year_month: get_datetime(r, 1),
            total: get_i64(r, 2),
        })
        .collect()
}

/// customer-detail の得意先名 (最初の行の 0 列目)。行が無ければ空文字。
pub(crate) fn customer_name(rows: &[Row]) -> String {
    rows.first().map(|r| decode_cp932(r, 0)).unwrap_or_default()
}

/// customer-detail の月別の列: 0 年月度, 1 自車売上, 2 傭車売上, 3 輸送回数。
pub(crate) fn customer_detail(rows: &[Row]) -> Vec<RawCustomerDetailRow> {
    rows.iter()
        .map(|r| RawCustomerDetailRow {
            year_month: get_datetime(r, 0),
            own_sales: get_i64(r, 1),
            charter_sales: get_i64(r, 2),
            transport_count: get_i64(r, 3),
        })
        .collect()
}
