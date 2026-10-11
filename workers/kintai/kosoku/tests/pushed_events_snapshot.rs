//! fold が写し (`kintai.kintai_events`) を読む SQL が、root の `src/kintai_pg_repo.rs` から移す前 (2b98104) と同じであることを縛る
//! (Refs #322)。基点の値は移す前の root の 3 定数の本文 (raw string の中身) の sha256。

use kintai_kosoku::pushed_events::{ALL_EVENTS_SQL, HEAD_PUNCHES_SQL, HEAD_RUN_ENDS_SQL};

/// (名前, SQL, 基点の sha256)。
const SQLS: [(&str, &str, &str); 3] = [
    (
        "ALL_EVENTS_SQL",
        ALL_EVENTS_SQL,
        "473561ceb43f005b99b191aa136142aaada09c15332a8faf88ccd9195957c670",
    ),
    (
        "HEAD_RUN_ENDS_SQL",
        HEAD_RUN_ENDS_SQL,
        "5c7c2c38a3de7fe9eac077f2e2430dc0bd44a604e26df8bc04603fc4dc459447",
    ),
    (
        "HEAD_PUNCHES_SQL",
        HEAD_PUNCHES_SQL,
        "ff310632b73fbdff6a946ceeb3a32d3ac43cee5bac20e826004fe434943243a4",
    ),
];

#[test]
fn the_sql_text_is_byte_identical_to_the_root_one() {
    use sha2::Digest;
    for (name, sql, base) in SQLS {
        let got = format!("{:x}", sha2::Sha256::digest(sql.as_bytes()));
        assert_eq!(got, base, "{name}");
    }
}

/// テナントは `$1` で固定 (接続ロールは行単位の権限を素通りするので、これが他テナントに触れない唯一の担保)。
#[test]
fn every_sql_pins_the_tenant_to_dollar_one() {
    for (name, sql, _) in SQLS {
        assert!(sql.contains("tenant_id = $1"), "{name}");
        assert!(sql.contains("FROM kintai.kintai_events"), "{name}");
    }
}
