//! `/api/schema/columns` の純粋部分 (オンプレ版の tests/schema_test.rs から移し、テーブルの絞り込みを足した)。

use ichiban_logic::schema::{build_columns, ColumnsQuery, RawColumnRow, COLUMNS_SQL};

fn query(table: Option<&str>) -> ColumnsQuery {
    ColumnsQuery {
        table: table.map(str::to_string),
        limit: None,
    }
}

#[test]
fn test_table_allowed() {
    assert_eq!(query(Some("運転日報明細")).table(), Some("運転日報明細"));
}

#[test]
fn test_table_missing_is_rejected() {
    assert_eq!(query(None).table(), None);
}

#[test]
fn test_table_other_is_rejected() {
    // オンプレ版は受けるが Worker は 400 にする (呼び手 seikyu は 運転日報明細 だけ)
    assert_eq!(query(Some("得意先ﾏｽﾀ")).table(), None);
    assert_eq!(query(Some("")).table(), None);
    assert_eq!(query(Some("運転日報明細 ")).table(), None); // 前後の空白も別物
    assert_eq!(query(Some("INFORMATION_SCHEMA.COLUMNS")).table(), None);
}

#[test]
fn test_columns_sql_binds_table() {
    assert!(COLUMNS_SQL.contains("WHERE TABLE_NAME = @P1"));
    assert!(COLUMNS_SQL.ends_with("ORDER BY ORDINAL_POSITION"));
}

#[test]
fn test_build_columns_keeps_order_and_null_length() {
    let raw = vec![
        RawColumnRow {
            column_name: "管理C".into(),
            data_type: "varchar".into(),
            is_nullable: "NO".into(),
            max_length: Some(4),
        },
        RawColumnRow {
            column_name: "売上年月日".into(),
            data_type: "datetime".into(),
            is_nullable: "YES".into(),
            max_length: None,
        },
    ];
    let cols = build_columns(&raw);
    assert_eq!(cols.len(), 2);
    assert_eq!(cols[0].column_name, "管理C");
    assert_eq!(cols[0].max_length, Some(4));
    assert_eq!(cols[1].data_type, "datetime");
    assert_eq!(cols[1].max_length, None);
    // 素の配列 (包まない)。フィールド名・並びはオンプレ版の ColumnInfo と同じ
    assert_eq!(
        serde_json::to_string(&cols).unwrap(),
        "[{\"column_name\":\"管理C\",\"data_type\":\"varchar\",\"is_nullable\":\"NO\",\"max_length\":4},\
         {\"column_name\":\"売上年月日\",\"data_type\":\"datetime\",\"is_nullable\":\"YES\",\"max_length\":null}]"
    );
}

#[test]
fn test_build_columns_empty() {
    assert!(build_columns(&[]).is_empty());
}
