//! 移す前 (root の `src/repo.rs`・`src/routes/uriage.rs`、main 6efe30c) と同じ文字列・バイト列を出すことの固定 (Refs #322)。
//!
//! - SQL: 代表的な入力で `uriage_rows_sql` が生成した文字列の sha256。値は移す前のコードを切り出して同じ入力で
//!   生成したものと突き合わせて確かめた (英数字以外の部門・空の担当者 (NULL で埋める)・負数と i32::MAX)
//! - ndjson と fingerprint: 固定の行 → バイト列そのものと sha256。オンプレ版が `recalc_jobs` に記録した
//!   fingerprint と Worker の再計算が一致しないと、全 bucket が「変化あり」になる
//!
//! **ここの期待値は書き換えない。** 落ちたら SQL か fingerprint が変わっている (= 移送したデータと食い違う)。

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
            "b674b1a6b19c1f2f9d8672615769931894f473765993c4d98694abbd3cc156e8",
        ),
        (
            &["010"],
            &[1499],
            "89285b659047665d13e3698f4a219292390c4cadd1ff2ac036602ad11e51d4b3",
        ),
        (
            &["010", "011", "030"],
            &[1499],
            "d913be2f08d43b6b50263942898656e3c6a809edb5935984146db650cf8e57ad",
        ),
        (
            &["010", "011", "030"],
            &[1499, 1180, -3, i32::MAX],
            "767692c534cb455f854f8fd61712b6bdbfbbfe8ab52d4379117492dec03729f6",
        ),
        // 英数字と `_` 以外を含む部門は落とす (空文字も)。残った "ab_C" だけで組む
        (
            &["x-1", "", "ab_C", "0 1"],
            &[1499],
            "202ba32f29a9c34eae7b8bd520a4c6be1185f40b06a4ae48e54f8afc81b6088e",
        ),
        (
            &["Z9", "_"],
            &[1499, 1180, -3, i32::MAX],
            "c2e05c4e16142e58096b5ca291ac5f5edc0a4694805fdc091918abedb9e1537d",
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
    assert!(q.ends_with("ORDER BY t.[運行年月日] ASC, t.[管理C] ASC, t.[LC] ASC"));
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
