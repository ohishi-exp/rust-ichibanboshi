//! 勤怠 Worker の CakePHP 中継 3 本の純粋部分 (`kintai_logic::cakephp_relay`) の単体テスト: 経路・検査の順と 400 の文言・
//! daily の `source` の印・失敗の写像 (オンプレ版の `routes/kintai.rs` の写しと 503 の読み替え・autoload の timeout)・
//! ③ の材料を数える SQL と件数の読み方。

use kintai_logic::cakephp::{CakephpError, DTAKO_AUTOLOAD_TIMEOUT_SECS};
use kintai_logic::cakephp_relay::{
    autoload_fail, map_cakephp_err, material_count, material_fail, material_sql, parse_autoload,
    synced_at, with_source_meta, CakephpRead, ReadRequest, AUTOLOAD_TIMEOUT, BODY_TOO_LARGE,
    CAKEPHP_VPC_BINDING, DRIVER_INVALID, MONTH_INVALID, NOT_CONFIGURED, TIMEOUT, TIMEOUT_SECS,
};
use kintai_logic::common::Fail;
use kintai_logic::dtako_autoload::{MaterialQuery, MAX_ZIP_BYTES, UNKO_NO_INVALID};
use serde_json::{json, Value};

const U: &str = "26060507533000000042861";

fn fail(status: u16, body: &str) -> Fail {
    Fail::new(status, body)
}

#[test]
fn routes_are_the_onprem_paths() {
    assert_eq!(
        CakephpRead::from_path("/api/kintai/daily"),
        Some(CakephpRead::Daily)
    );
    assert_eq!(
        CakephpRead::from_path("/api/kintai/pdf-json"),
        Some(CakephpRead::PdfJson)
    );
    assert_eq!(CakephpRead::from_path("/api/kintai/daily/x"), None);
    assert_eq!(CakephpRead::Daily.as_str(), "daily");
    assert_eq!(CakephpRead::PdfJson.as_str(), "pdf-json");
    assert_eq!(CAKEPHP_VPC_BINDING, "KINTAI_CAKEPHP_VPC");
    assert_eq!((TIMEOUT_SECS, DTAKO_AUTOLOAD_TIMEOUT_SECS), (30, 120));
}

#[test]
fn daily_checks_the_month_and_ignores_refresh() {
    let r = CakephpRead::Daily.parse("month=2026-06&refresh=1").unwrap();
    assert_eq!(r.path, "/time-card/daily-json?month=2026-06");
    for q in ["", "month=2026-13", "month=2026-6", "month="] {
        assert_eq!(
            CakephpRead::Daily.parse(q),
            Err(fail(400, MONTH_INVALID)),
            "{q:?}"
        );
    }
    let e = CakephpRead::Daily
        .parse("month=2026-06&month=2026-07")
        .unwrap_err();
    assert_eq!(e.status, 400);
    assert!(
        e.body.starts_with("Failed to deserialize query string: "),
        "{}",
        e.body
    );
}

#[test]
fn pdf_json_checks_month_then_driver_and_always_sends_recalc_0() {
    let all = CakephpRead::PdfJson.parse("month=2026-04").unwrap();
    assert_eq!(all.path, "/time-card/pdf-json?month=2026-04&recalc=0");
    let one = CakephpRead::PdfJson
        .parse("month=2026-04&driver=1021&view=x")
        .unwrap();
    assert_eq!(
        one.path,
        "/time-card/pdf-json?month=2026-04&driver_id=1021&recalc=0"
    );
    for q in [
        "month=2026-04&driver=",
        "month=2026-04&driver=a1",
        "month=2026-04&driver=-1",
    ] {
        assert_eq!(
            CakephpRead::PdfJson.parse(q),
            Err(fail(400, DRIVER_INVALID)),
            "{q:?}"
        );
    }
    assert_eq!(
        CakephpRead::PdfJson.parse("driver=x"),
        Err(fail(400, MONTH_INVALID)),
        "month が先"
    );
}

#[test]
fn daily_is_always_live_with_rows_first_like_onprem() {
    let r = CakephpRead::Daily.parse("month=2026-06").unwrap();
    let body = br#"{"rows":[{"driver_id":1}],"month":"2026-06","a":true}"#;
    let out = r.respond(body, 1_791_651_600_123).unwrap();
    assert_eq!(
        String::from_utf8(out).unwrap(),
        r#"{"rows":[{"driver_id":1}],"a":true,"month":"2026-06","source":"live","synced_at":"2026-10-10T17:00:00.123+00:00"}"#
    );
    let bad = r.respond(b"<html>", 0).unwrap_err();
    assert_eq!(bad.status, 502);
    assert!(bad.body.starts_with("CakePHP response parse failed: "));
}

#[test]
fn pdf_json_is_relayed_as_is() {
    let r = ReadRequest {
        read: CakephpRead::PdfJson,
        path: String::new(),
    };
    let out = r.respond(br#"{"b":1,"a":[2]}"#, 0).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&out).unwrap(),
        json!({"a": [2], "b": 1})
    );
    assert_eq!(r.respond(b"", 0).unwrap_err().status, 502);
}

#[test]
fn source_meta_and_synced_at() {
    let resp = serde_json::from_value(json!({"rows": []})).unwrap();
    let resp = with_source_meta(resp, "live", "t");
    assert_eq!(resp.extra["source"], json!("live"));
    assert_eq!(resp.extra["synced_at"], json!("t"));
    assert_eq!(synced_at(0), "1970-01-01T00:00:00+00:00");
    assert_eq!(
        synced_at(u64::MAX),
        "1970-01-01T00:00:00+00:00",
        "範囲外は epoch"
    );
}

#[test]
fn cakephp_errors_map_like_onprem_with_the_binding_wording() {
    assert_eq!(
        map_cakephp_err(CakephpError::NotConfigured),
        fail(503, NOT_CONFIGURED)
    );
    assert_eq!(
        map_cakephp_err(CakephpError::RequestFailed(TIMEOUT.into())),
        fail(502, "CakePHP fetch failed: timeout")
    );
    let e = CakephpError::StatusError {
        status: 500,
        body_excerpt: "boom".into(),
    };
    assert_eq!(map_cakephp_err(e), fail(502, "CakePHP returned 500: boom"));
    assert_eq!(
        map_cakephp_err(CakephpError::JsonError("x".into())),
        fail(502, "CakePHP response parse failed: x")
    );
}

#[test]
fn autoload_checks_query_then_size_then_unko_no_then_body() {
    let e = parse_autoload("preview=1", 3).unwrap_err();
    assert_eq!(e.status, 400);
    assert!(
        e.body
            .starts_with("Failed to deserialize query string: preview: "),
        "{}",
        e.body
    );
    assert_eq!(
        parse_autoload("", MAX_ZIP_BYTES + 1),
        Err(fail(413, BODY_TOO_LARGE)),
        "unko_no より先"
    );
    assert_eq!(parse_autoload("", 3), Err(fail(400, UNKO_NO_INVALID)));
    assert_eq!(
        parse_autoload(&format!("unko_no={U}"), 0),
        Err(fail(
            400,
            "body が空です。csvdata.zip の中身を送ってください"
        ))
    );
    let r = parse_autoload(
        &format!("unko_no={U}&preview=true&reset_timecard=true"),
        MAX_ZIP_BYTES,
    )
    .unwrap();
    assert!(r.preview && r.reset_timecard);
    assert_eq!(r.file_name, "csvdata.zip");
}

#[test]
fn autoload_timeout_is_unknown_not_failed() {
    let f = autoload_fail(CakephpError::RequestFailed(TIMEOUT.into()));
    assert_eq!(f, fail(502, AUTOLOAD_TIMEOUT));
    assert!(f.body.contains("不明"));
    assert_eq!(
        autoload_fail(CakephpError::RequestFailed("fetch".into())),
        fail(502, "nginx への接続に失敗: fetch")
    );
    assert_eq!(
        autoload_fail(CakephpError::NotConfigured),
        fail(503, NOT_CONFIGURED)
    );
}

#[test]
fn material_sql_quotes_the_two_variants() {
    let sql = material_sql(&MaterialQuery::new(U).unwrap()).unwrap();
    assert_eq!(
        sql.matches("IN ('26060507533000000042861', '26060507533000000042862')")
            .count(),
        2
    );
    assert!(!sql.contains(":v1") && !sql.contains(":from"));
}

#[test]
fn material_count_reads_one_integer() {
    assert_eq!(material_count(&[]), Ok(0), "行が無ければ 0");
    assert_eq!(material_count(&[vec![Some(b"3".to_vec())]]), Ok(3));
    let bad = Err("MariaDB query failed: rows:int".to_string());
    assert_eq!(material_count(&[vec![None]]), bad);
    assert_eq!(material_count(&[vec![Some(b"x".to_vec())]]), bad);
    assert_eq!(material_count(&[vec![Some(vec![0xff])]]), bad);
    assert_eq!(material_count(&[vec![]]), bad);
    assert_eq!(
        material_fail("query:timeout"),
        "MariaDB query failed: query:timeout"
    );
}
