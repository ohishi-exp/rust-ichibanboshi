use rust_ichibanboshi::routes::schema::is_valid_table_name;

#[test]
fn test_valid_table_names() {
    assert!(is_valid_table_name("users"));
    assert!(is_valid_table_name("INFORMATION_SCHEMA"));
    assert!(is_valid_table_name("table_name"));
    assert!(is_valid_table_name("Table123"));
    assert!(is_valid_table_name("CAPE#01")); // # は許可
}

#[test]
fn test_invalid_table_names() {
    assert!(!is_valid_table_name("")); // 空文字
    assert!(!is_valid_table_name("foo;DROP TABLE")); // セミコロン
    assert!(!is_valid_table_name("table name")); // スペース
    assert!(!is_valid_table_name("table--name")); // ハイフン
    assert!(!is_valid_table_name("table.name")); // ドット
    assert!(!is_valid_table_name("table'name")); // クオート
    assert!(!is_valid_table_name("a=b")); // イコール
    assert!(!is_valid_table_name("[bracketed]")); // ブラケット
}
