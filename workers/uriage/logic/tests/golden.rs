//! 移す前 (root の `src/repo.rs`・`src/routes/uriage.rs`、main 6efe30c) と同じ文字列・バイト列を出すことの固定 (Refs #322)。
//!
//! - SQL: 代表的な入力で `uriage_rows_sql` が生成した文字列の sha256。値は移す前のコードを切り出して同じ入力で
//!   生成したものと突き合わせて確かめた (英数字以外の部門・空の担当者 (NULL で埋める)・負数と i32::MAX)
//! - ndjson と fingerprint: 固定の行 → バイト列そのものと sha256。オンプレ版が `recalc_jobs` に記録した
//!   fingerprint と Worker の再計算が一致しないと、全 bucket が「変化あり」になる
//!
//! **ここの期待値は書き換えない。** 落ちたら SQL か fingerprint が変わっている (= 移送したデータと食い違う)。
//! 例外: SQL の sha256 は、最外殻の ORDER BY を出力列で全順序にした変更 (Refs #322 #88-3、同点の並びが
//! 実行ごとに入れ替わって fingerprint が揺れていた) で 1 度だけ取り直した。行の中身は変わらない。

use sha2::{Digest, Sha256};
use uriage_logic::compute::UriageRow;
use uriage_logic::raw::{gzip, r2_key, raw_file_name, serialize_ndjson_and_fingerprint};
use uriage_logic::sqlserver::uriage_rows_sql;

fn s(v: &[&str]) -> Vec<String> {
    v.iter().map(|x| x.to_string()).collect()
}

fn sql_sha(bumon: &[&str], persons: &[i32]) -> String {
    let q = uriage_rows_sql(&s(bumon), persons).expect("SQL が組める");
    format!("{:x}", Sha256::digest(q.as_bytes()))
}

#[test]
fn sql_is_byte_identical_to_the_onprem_version() {
    let cases: [(&[&str], &[i32], &str); 6] = [
        (
            &["010"],
            &[],
            "c8319e37f856d25839f8f12a7966491f3cc2816bacebf0100d05a511f616330b",
        ),
        (
            &["010"],
            &[1499],
            "11f62af6d6d94f6b4aaab0d0a86186e06b580af26da384ca52ed717d6dc7f44a",
        ),
        (
            &["010", "011", "030"],
            &[1499],
            "0b7df079e55e1fc74dbc37469f8f5f74afb16fbc858cf9146f1ee2815d43929e",
        ),
        (
            &["010", "011", "030"],
            &[1499, 1180, -3, i32::MAX],
            "0d859eae4ca5477ed76f6f357ab3dd85a6b2fa9ffbd8b28bd2a9c30d2d30deb0",
        ),
        // 英数字と `_` 以外を含む部門は落とす (空文字も)。残った "ab_C" だけで組む
        (
            &["x-1", "", "ab_C", "0 1"],
            &[1499],
            "341e0d40b7becbeffd8bb03b27e2ad4745f4c3d8f35136ea1702f7065e300191",
        ),
        (
            &["Z9", "_"],
            &[1499, 1180, -3, i32::MAX],
            "469d84fbd995ac661f5bbde5fa81b35ccf8bdddf6bd8ebdd5eb58d81652e9020",
        ),
    ];
    for (bumon, persons, want) in cases {
        assert_eq!(sql_sha(bumon, persons), want, "{bumon:?} {persons:?}");
    }
}

#[test]
fn sql_drops_a_quoted_bumon_rather_than_escaping_it() {
    // "011'" は whitelist で落ちるので ["010"] と同じ文字列になる
    assert_eq!(
        uriage_rows_sql(&s(&["010", "011'"]), &[1499]),
        uriage_rows_sql(&s(&["010"]), &[1499])
    );
}

#[test]
fn sql_is_none_when_no_bumon_survives_the_whitelist() {
    assert_eq!(uriage_rows_sql(&s(&[]), &[1499]), None);
    assert_eq!(uriage_rows_sql(&s(&["'; DROP", "--", ""]), &[1499]), None);
}

#[test]
fn sql_shape() {
    let q = uriage_rows_sql(&s(&["010", "011"]), &[]).unwrap();
    // 5 ケースの UNION ALL・空の担当者は NULL で埋める・引数は @P1/@P2・並びは表示順
    assert_eq!(q.matches("UNION ALL").count(), 4);
    assert!(q.contains("IN (NULL)"));
    assert!(q.contains("[受注部門] IN ('010','011')"));
    assert!(q.contains("[運行年月日] >= @P1 AND [運行年月日] <= @P2"));
    assert!(q.contains("ORDER BY t.[運行年月日] ASC, t.[管理C] ASC, t.[LC] ASC, [横横] ASC,"));
}

#[test]
fn sql_order_by_is_a_total_order_over_the_output_columns() {
    // 表示順の 3 列の後ろに、SELECT の別名を select_cols の並びで全部足す (同点を実行ごとに入れ替えない)
    let q = uriage_rows_sql(&s(&["010"]), &[1499]).unwrap();
    let order_by = &q[q.rfind("ORDER BY").unwrap()..];
    let cols = [
        "t.[運行年月日]",
        "t.[管理C]",
        "t.[LC]",
        "[横横]",
        "[請求K]",
        "[備考2]",
        "[入力担当C]",
        "[稼動部門]",
        "[金額]",
        "[値引]",
        "[割増]",
        "[実費]",
        "[傭車金額]",
        "[傭車値引]",
        "[傭車割増]",
        "[傭車実費]",
        "[社員R]",
        "[傭車先C]",
        "[売上年月日]",
        "[得意先複合キー]",
        "[得意先N]",
        "[傭車先複合キー]",
        "[傭車先N]",
    ];
    let want = format!("ORDER BY {}", cols.map(|c| format!("{c} ASC")).join(", "));
    assert_eq!(
        order_by.split_whitespace().collect::<Vec<_>>().join(" "),
        want
    );
}

/// 架空の 2 行 (エスケープの要る文字・全角空白・負数を含む)。
fn fixed_rows() -> Vec<UriageRow> {
    vec![
        UriageRow {
            yokoyoko: 0,
            seikyu_k: 2,
            biko2: "売上 架空太郎".to_string(),
            nyuryoku_tanto_c: 9001,
            kado_bumon: "010".to_string(),
            kingaku: 12345,
            nebiki: -100,
            warimashi: 50,
            jippi: 7,
            yosha_kingaku: 0,
            yosha_nebiki: 0,
            yosha_warimashi: 0,
            yosha_jippi: 0,
            shain_r: "架空".to_string(),
            yoshasaki_c: "000000".to_string(),
            unko_date: "2026-01-05".to_string(),
            uriage_date: "2026-01-06".to_string(),
            tokuisaki_key: "T001-0".to_string(),
            tokuisaki_n: "架空商事".to_string(),
            yoshasaki_key: "000000-0".to_string(),
            yoshasaki_n: String::new(),
        },
        UriageRow {
            yokoyoko: 1,
            seikyu_k: 0,
            biko2: "甲→乙 \"引用\"\n\t\u{3000}".to_string(),
            nyuryoku_tanto_c: 9002,
            kado_bumon: "021".to_string(),
            kingaku: 0,
            nebiki: 0,
            warimashi: 0,
            jippi: 0,
            yosha_kingaku: 8000,
            yosha_nebiki: 1,
            yosha_warimashi: -2,
            yosha_jippi: 3,
            shain_r: String::new(),
            yoshasaki_c: "123456".to_string(),
            unko_date: "2026-01-31".to_string(),
            uriage_date: String::new(),
            tokuisaki_key: "-".to_string(),
            tokuisaki_n: String::new(),
            yoshasaki_key: "123456-1".to_string(),
            yoshasaki_n: "架空運輸".to_string(),
        },
    ]
}

#[test]
fn ndjson_and_fingerprint_are_byte_identical_to_the_onprem_version() {
    let (bytes, fp) = serialize_ndjson_and_fingerprint(&fixed_rows());
    let want = concat!(
        r#"{"unko_date":"2026-01-05","yokoyoko":0,"seikyu_k":2,"biko2":"売上 架空太郎","nyuryoku_tanto_c":9001,"kado_bumon":"010","kingaku":12345,"nebiki":-100,"warimashi":50,"jippi":7,"yosha_kingaku":0,"yosha_nebiki":0,"yosha_warimashi":0,"yosha_jippi":0,"shain_r":"架空","yoshasaki_c":"000000","tokuisaki_key":"T001-0","tokuisaki_n":"架空商事","yoshasaki_key":"000000-0","yoshasaki_n":""}"#,
        "\n",
        r#"{"unko_date":"2026-01-31","yokoyoko":1,"seikyu_k":0,"biko2":"甲→乙 \"引用\"\n\t　","nyuryoku_tanto_c":9002,"kado_bumon":"021","kingaku":0,"nebiki":0,"warimashi":0,"jippi":0,"yosha_kingaku":8000,"yosha_nebiki":1,"yosha_warimashi":-2,"yosha_jippi":3,"shain_r":"","yoshasaki_c":"123456","tokuisaki_key":"-","tokuisaki_n":"","yoshasaki_key":"123456-1","yoshasaki_n":"架空運輸"}"#,
        "\n",
    );
    assert_eq!(String::from_utf8(bytes.clone()).unwrap(), want);
    // uriage_date は raw に出さない (fingerprint に入らない)
    assert!(!want.contains("uriage_date"));
    assert_eq!(fp, format!("{:x}", Sha256::digest(&bytes)));
    assert_eq!(fp, GOLDEN_FP);
}

/// `fixed_rows()` の fingerprint。移す前のコード (main 6efe30c) で同じ行から出した値と同じ。
const GOLDEN_FP: &str = "0879701b31bcf5fda6887da63b411aa84daa4f95319f1613c6ef9cb568fb35f2";

#[test]
fn gzip_round_trips_and_is_deterministic() {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let (bytes, _) = serialize_ndjson_and_fingerprint(&fixed_rows());
    let gz = gzip(&bytes).unwrap();
    assert_eq!(gz, gzip(&bytes).unwrap());
    // flate2 の既定のヘッダ (mtime 0・OS 不明) — 時刻に依存しない
    assert_eq!(&gz[..10], &[0x1f, 0x8b, 8, 0, 0, 0, 0, 0, 0, 0xff]);
    let mut out = Vec::new();
    GzDecoder::new(&gz[..]).read_to_end(&mut out).unwrap();
    assert_eq!(out, bytes);
}

#[test]
fn raw_names() {
    assert_eq!(raw_file_name(9), "eigyosho-9.ndjson.gz");
    assert_eq!(
        r2_key("2026-06", 12),
        "uriage/2026-06/eigyosho-12.ndjson.gz"
    );
}
