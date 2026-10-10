//! `/api/schema/columns` の SQL・Raw 型・応答型・Query・組み立て (Refs #322)。
//!
//! オンプレ版 `src/routes/schema.rs` の `list_columns` と `src/repo.rs` の `list_columns` を写した。
//! オンプレ版は任意のテーブル名を受けるが、Worker は呼び手 (seikyu) が使う `運転日報明細` 1 本だけに絞る
//! (それ以外・欠落は 400)。`運転日報明細` のときの応答はオンプレ版と同じ。

use serde::{Deserialize, Serialize};

/// 列一覧を返してよい唯一のテーブル。
pub const ALLOWED_TABLE: &str = "運転日報明細";

/// 列の一覧 (@P1 テーブル名。バインドで渡す)。列: 0 COLUMN_NAME, 1 DATA_TYPE, 2 IS_NULLABLE, 3 CHARACTER_MAXIMUM_LENGTH。
pub const COLUMNS_SQL: &str =
    "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH \
                 FROM INFORMATION_SCHEMA.COLUMNS \
                 WHERE TABLE_NAME = @P1 \
                 ORDER BY ORDINAL_POSITION";

/// `COLUMNS_SQL` の 1 行。
#[derive(Debug, Clone, PartialEq)]
pub struct RawColumnRow {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub max_length: Option<i32>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub max_length: Option<i32>,
}

#[derive(Deserialize, Debug, Default)]
pub struct ColumnsQuery {
    pub table: Option<String>,
    /// オンプレ版の `TableQuery` と同じ型。数字でない値を 400 にするために持つ (使わない)
    pub limit: Option<i32>,
}

impl ColumnsQuery {
    /// 列を返してよいテーブル名。欠落・`運転日報明細` 以外は `None` (= 400)。
    pub fn table(&self) -> Option<&'static str> {
        match self.table.as_deref() {
            Some(ALLOWED_TABLE) => Some(ALLOWED_TABLE),
            _ => None,
        }
    }
}

/// Raw 行を応答行に変換 (並びは `ORDINAL_POSITION` のまま)。
pub fn build_columns(raw: &[RawColumnRow]) -> Vec<ColumnInfo> {
    raw.iter()
        .map(|r| ColumnInfo {
            column_name: r.column_name.clone(),
            data_type: r.data_type.clone(),
            is_nullable: r.is_nullable.clone(),
            max_length: r.max_length,
        })
        .collect()
}
