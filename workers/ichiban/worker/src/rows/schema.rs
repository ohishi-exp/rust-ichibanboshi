//! `/api/schema/columns` の `tiberius::Row` → `ichiban_logic::schema` の Raw 型。
//! オンプレ版 `src/repo.rs` の `list_columns` と同じ列番号 (`COLUMNS_SQL` の並びと 1 対 1)。
//! INFORMATION_SCHEMA の文字列はオンプレ版どおり trim せずそのまま読む。

use ichiban_logic::schema::RawColumnRow;
use tiberius::Row;

/// 文字列の列 (NULL・型不一致は空文字。trim しない)。
fn text(row: &Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .unwrap_or("")
        .to_string()
}

/// 0 COLUMN_NAME, 1 DATA_TYPE, 2 IS_NULLABLE, 3 CHARACTER_MAXIMUM_LENGTH (NULL は None)。
pub(crate) fn columns(rows: &[Row]) -> Vec<RawColumnRow> {
    rows.iter()
        .map(|r| RawColumnRow {
            column_name: text(r, 0),
            data_type: text(r, 1),
            is_nullable: text(r, 2),
            max_length: r.try_get::<i32, _>(3).ok().flatten(),
        })
        .collect()
}
