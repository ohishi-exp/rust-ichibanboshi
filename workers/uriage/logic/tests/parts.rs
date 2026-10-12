//! 列の読み方・口の型・verify の差分・月の扱いの試験 (Refs #322)。値はすべて架空。

use std::collections::HashMap;

use uriage_logic::api::{
    annotate_r2_pending, default_cal, DailyQuery, R2PendingRow, VerifyDebugRow,
};
use uriage_logic::compute::{compute_person_sum, PersonAccum, UriageRow};
use uriage_logic::period::days_in_month;
use uriage_logic::sqlserver::{uriage_row, Cells};
use uriage_logic::verify::{compute_verify_diff, parse_php_sum};

/// 列番号をそのまま値に埋める偽の行 (どの列がどのフィールドに入るかを見る)。
struct Fake;

impl Cells for Fake {
    fn text(&self, idx: usize) -> String {
        format!("c{idx}")
    }
    fn int(&self, idx: usize) -> i32 {
        idx as i32 + 100
    }
    fn money(&self, idx: usize) -> i64 {
        idx as i64 * 1000
    }
}

#[test]
fn uriage_row_reads_the_columns_in_select_order() {
    let r = uriage_row(&Fake);
    assert_eq!(r.yokoyoko, 100);
    assert_eq!(r.seikyu_k, 101);
    assert_eq!(r.biko2, "c2");
    assert_eq!(r.nyuryoku_tanto_c, 103);
    assert_eq!(r.kado_bumon, "c4");
    assert_eq!(
        [r.kingaku, r.nebiki, r.warimashi, r.jippi],
        [5000, 6000, 7000, 8000]
    );
    assert_eq!(
        [
            r.yosha_kingaku,
            r.yosha_nebiki,
            r.yosha_warimashi,
            r.yosha_jippi
        ],
        [9000, 10000, 11000, 12000]
    );
    assert_eq!(r.shain_r, "c13");
    assert_eq!(r.yoshasaki_c, "c14");
    assert_eq!(r.unko_date, "c15");
    assert_eq!(r.uriage_date, "c16");
    assert_eq!(r.tokuisaki_key, "c17");
    assert_eq!(r.tokuisaki_n, "c18");
    assert_eq!(r.yoshasaki_key, "c19");
    assert_eq!(r.yoshasaki_n, "c20");
}

fn pending(month: &str, count: i64, ng: i64) -> R2PendingRow {
    R2PendingRow {
        month: month.to_string(),
        eigyosho_id: 3,
        raw_path: "/raw/x.ndjson.gz".to_string(),
        fingerprint_after: "fp".to_string(),
        computed_at: "2026-02-01T00:00:00Z".to_string(),
        verified_count: count,
        verified_ok: count - ng,
        verified_ng: ng,
    }
}

#[test]
fn annotate_r2_pending_gates() {
    // 2026-02 は 28 日 → 56 cell
    let ready = annotate_r2_pending(pending("2026-02", 56, 0));
    assert_eq!(ready.expected_count, 56);
    assert!(ready.ready);
    assert_eq!(ready.blocker, None);
    assert_eq!(ready.raw_path, "/raw/x.ndjson.gz");
    assert_eq!(ready.fingerprint_after, "fp");
    assert_eq!(ready.computed_at, "2026-02-01T00:00:00Z");
    assert_eq!(
        (ready.eigyosho_id, ready.verified_ok, ready.month.as_str()),
        (3, 56, "2026-02")
    );

    let ng = annotate_r2_pending(pending("2026-02", 56, 1));
    assert!(!ng.ready);
    assert_eq!(ng.blocker.as_deref(), Some("ng_present"));

    let short = annotate_r2_pending(pending("2026-02", 55, 0));
    assert!(!short.ready);
    assert_eq!(short.blocker.as_deref(), Some("unverified"));

    let bad = annotate_r2_pending(pending("2026/02", 99, 0));
    assert_eq!(bad.expected_count, 0);
    assert!(!bad.ready);
    assert_eq!(bad.blocker.as_deref(), Some("unverified"));
}

#[test]
fn cal_defaults_to_true() {
    assert!(default_cal());
    let q: DailyQuery = serde_json::from_str(r#"{"month":"2026-01","eigyosho_id":1}"#).unwrap();
    assert!(q.cal);
}

#[test]
fn verify_debug_row_pairs_the_row_with_its_decision() {
    let mut persons = HashMap::new();
    persons.insert(9001, "架空".to_string());
    let row = UriageRow {
        yokoyoko: 1,
        seikyu_k: 2,
        biko2: "表示".to_string(),
        nyuryoku_tanto_c: 9001,
        kado_bumon: "010".to_string(),
        kingaku: 1,
        nebiki: 2,
        warimashi: 3,
        jippi: 4,
        yosha_kingaku: 5,
        yosha_nebiki: 6,
        yosha_warimashi: 7,
        yosha_jippi: 8,
        shain_r: "R".to_string(),
        yoshasaki_c: "000001".to_string(),
        unko_date: "2026-01-02".to_string(),
        uriage_date: "2026-01-03".to_string(),
        ..UriageRow::default()
    };
    let res = compute_person_sum(std::slice::from_ref(&row), &persons, &HashMap::new(), true);
    let d = VerifyDebugRow::new(&row, &res.rows[0]);
    let v = serde_json::to_value(&d).unwrap();
    assert_eq!(
        v,
        serde_json::json!({
            "yokoyoko": 1, "seikyu_k": 2, "biko2": "表示", "nyuryoku_tanto_c": 9001,
            "kado_bumon": "010", "kingaku": 1, "nebiki": 2, "warimashi": 3, "jippi": 4,
            "yosha_kingaku": 5, "yosha_nebiki": 6, "yosha_warimashi": 7, "yosha_jippi": 8,
            "shain_r": "R", "yosha_saki_c": "000001", "unko_date": "2026-01-02",
            "uriage_date": "2026-01-03",
            "decision": {
                "skip": false, "ck": true, "matched": true, "tanto": "架空",
                "aggregated": false, "kingaku_sum": 10, "yosha_sum": 10
            }
        })
    );
}

fn pa(kingaku: i64, yosha: i64, kensuu: i64) -> PersonAccum {
    PersonAccum {
        kingaku,
        yosha_kingaku: yosha,
        kensuu,
        ..Default::default()
    }
}

#[test]
fn parse_php_sum_reads_null_object_and_rejects_garbage() {
    assert!(parse_php_sum(&serde_json::Value::Null).unwrap().is_empty());
    let m = parse_php_sum(&serde_json::json!({"架空": {"金額": 10, "傭車金額": 5, "件数": 1}}))
        .unwrap();
    assert_eq!(m["架空"], pa(10, 5, 1));
    let e = parse_php_sum(&serde_json::json!([1, 2])).unwrap_err();
    assert!(e.starts_with("PHP print-json sum parse failed: "), "{e}");
}

#[test]
fn verify_diff_ignores_zero_entries_and_y0() {
    let mut php = HashMap::new();
    php.insert("甲".to_string(), pa(10, 5, 1));
    php.insert("零".to_string(), pa(0, 0, 0));
    let mut rust = HashMap::new();
    rust.insert(
        "甲".to_string(),
        PersonAccum {
            kingaku_y0: 99,
            ..pa(10, 5, 1)
        },
    );
    assert!(compute_verify_diff(&php, &rust).is_none());

    rust.insert("乙".to_string(), pa(1, 0, 1));
    php.insert("甲".to_string(), pa(11, 5, 1));
    let d = compute_verify_diff(&php, &rust).unwrap();
    assert_eq!(d.php_only.len(), 1);
    assert_eq!(d.php_only["甲"], pa(11, 5, 1));
    assert_eq!(d.rust_only.len(), 2);
    assert_eq!(d.rust_only["甲"], pa(10, 5, 1));
    assert_eq!(d.rust_only["乙"], pa(1, 0, 1));
}

#[test]
fn days_in_month_edges() {
    assert_eq!(days_in_month("2024-02"), Some(29));
    assert_eq!(days_in_month("2026-02"), Some(28));
    assert_eq!(days_in_month("2026-04"), Some(30));
    assert_eq!(days_in_month("2026-12"), Some(31));
    for bad in [
        "", "2026-1", "2026/01", "abcd-01", "2026-ab", "2026-00", "2026-13",
    ] {
        assert_eq!(days_in_month(bad), None, "{bad}");
    }
}
