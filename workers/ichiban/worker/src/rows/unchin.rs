//! `/api/unchin/candidates`・`/api/unchin/summary`・`/api/unchin/customer-net`・`/api/unchin/customer-net-detail` の
//! `tiberius::Row` → `ichiban_logic::unchin` の Raw 型。オンプレ版 `src/repo.rs` の `rows_to_unchin*` を列番号まで同じに写した。
//! 列の並びは `ichiban_logic::unchin` の各 SQL と 1 対 1。日付は panic しない `get_datetime` で読む。

use ichiban_logic::unchin::{
    RawUnchinCustomerNetDetailRow, RawUnchinCustomerNetRow, RawUnchinRow, RawUnchinSummaryRow,
};
use tiberius::Row;

use super::{decode_cp932, get_datetime, get_i64};

/// `candidates_sql` の列 (オンプレ版 `rows_to_unchin` と同じ番号)。
pub(crate) fn candidates(rows: &[Row]) -> Vec<RawUnchinRow> {
    rows.iter()
        .map(|r| RawUnchinRow {
            partner_code: decode_cp932(r, 0),
            partner_name: decode_cp932(r, 1),
            item_code: decode_cp932(r, 2),
            item_name: decode_cp932(r, 3),
            fare: get_i64(r, 4),
            origin: decode_cp932(r, 5),
            dest: decode_cp932(r, 6),
            sale_date: get_datetime(r, 7),
            bumon_code: decode_cp932(r, 8),
            bumon_name: decode_cp932(r, 9),
            vehicle_code: decode_cp932(r, 10),
        })
        .collect()
}

/// 列 0 (C) と列 1 (H) を `C-H` にする (オンプレ版の `format!("{}-{}", …)` と同じ)。
fn partner_code(r: &Row) -> String {
    [decode_cp932(r, 0), decode_cp932(r, 1)].join("-")
}

/// `summary_sql` の列 (オンプレ版 `rows_to_unchin_summary` と同じ番号)。
pub(crate) fn summary(rows: &[Row]) -> Vec<RawUnchinSummaryRow> {
    rows.iter()
        .map(|r| RawUnchinSummaryRow {
            partner_code: partner_code(r),
            partner_name: decode_cp932(r, 2),
            total: get_i64(r, 3),
            bumon_code: decode_cp932(r, 4),
            bumon_name: decode_cp932(r, 5),
        })
        .collect()
}

/// `customer_net_sql` の列 (オンプレ版 `rows_to_unchin_customer_net` と同じ番号)。
pub(crate) fn customer_net(rows: &[Row]) -> Vec<RawUnchinCustomerNetRow> {
    rows.iter()
        .map(|r| RawUnchinCustomerNetRow {
            partner_code: partner_code(r),
            partner_name: decode_cp932(r, 2),
            total_sales: get_i64(r, 3),
            total_payment: get_i64(r, 4),
            bumon_code: decode_cp932(r, 5),
            bumon_name: decode_cp932(r, 6),
        })
        .collect()
}

/// `customer_net_detail_sql` の列 (オンプレ版 `rows_to_unchin_customer_net_detail` と同じ番号)。
pub(crate) fn customer_net_detail(rows: &[Row]) -> Vec<RawUnchinCustomerNetDetailRow> {
    rows.iter()
        .map(|r| RawUnchinCustomerNetDetailRow {
            item_code: decode_cp932(r, 0),
            item_name: decode_cp932(r, 1),
            subcontractor_name: decode_cp932(r, 2),
            sales: get_i64(r, 3),
            payment: get_i64(r, 4),
            origin: decode_cp932(r, 5),
            dest: decode_cp932(r, 6),
            sale_date: get_datetime(r, 7),
            bumon_code: decode_cp932(r, 8),
            bumon_name: decode_cp932(r, 9),
        })
        .collect()
}
