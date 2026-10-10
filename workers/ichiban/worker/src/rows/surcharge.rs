//! `/api/surcharge/base` の `tiberius::Row` → `ichiban_logic::surcharge` の Raw 型。
//! オンプレ版 `src/repo.rs` の `rows_to_surcharge` と同じ列番号 (`SURCHARGE_SQL_BODY` の並びと 1 対 1)。

use chrono::NaiveDateTime;
use ichiban_logic::surcharge::RawSurchargeRow;
use tiberius::Row;

use super::{decode_cp932, get_datetime, get_i64};

/// 18 列 (0 請求K … 17 入力者N)。9 入金予定日は NULL があり得るので Option。
pub(crate) fn base_rows(rows: &[Row]) -> Vec<RawSurchargeRow> {
    rows.iter()
        .map(|r| RawSurchargeRow {
            request_kind: decode_cp932(r, 0),
            customer_code: decode_cp932(r, 1),
            customer_name: decode_cp932(r, 2),
            origin_area_name: decode_cp932(r, 3),
            dest_area_name: decode_cp932(r, 4),
            vehicle_code: decode_cp932(r, 5),
            vehicle_name: decode_cp932(r, 6),
            sale_date: get_datetime(r, 7),
            fare: get_i64(r, 8),
            billing_date: r.try_get::<NaiveDateTime, _>(9).ok().flatten(),
            subcontractor_code: decode_cp932(r, 10),
            item_code: decode_cp932(r, 11),
            item_name: decode_cp932(r, 12),
            vehicle_number: decode_cp932(r, 13),
            fuel_surcharge: get_i64(r, 14),
            row_id: decode_cp932(r, 15),
            input_staff_code: decode_cp932(r, 16),
            input_staff_name: decode_cp932(r, 17),
        })
        .collect()
}
