//! 社内 CakePHP への中継の純粋部分 (`kintai_logic::cakephp`) の単体テスト: URL・クエリ・multipart の本文・応答の読み取り・
//! エラーの種別。オンプレ版 (reqwest) が送るリクエストと 1 バイトも違わないことは、root の
//! `cakephp::tests::wire_snapshot_matches_the_baseline` が基点 (dd2b9c4) の実物の snapshot で縛る。

use kintai_logic::cakephp::{
    autoload_multipart, autoload_response, boundary, daily_json_path, excerpt, is_success, join,
    parse_json, pdf_json_path, reset_multipart, reset_timecard_path, status_error, urlencode,
    CakephpError, TimecardDailyResponse, AUTOLOAD_PATH, DTAKO_AUTOLOAD_MIME,
    DTAKO_AUTOLOAD_TIMEOUT_SECS,
};

#[test]
fn paths_are_relative_and_pdf_json_always_sends_recalc_0() {
    assert_eq!(
        daily_json_path("2026-06"),
        "/time-card/daily-json?month=2026-06"
    );
    assert_eq!(
        pdf_json_path("2026-04", Some(1021)),
        "/time-card/pdf-json?month=2026-04&driver_id=1021&recalc=0"
    );
    assert_eq!(
        pdf_json_path("2026-04", None),
        "/time-card/pdf-json?month=2026-04&recalc=0"
    );
    assert_eq!(
        reset_timecard_path("26060507533000000042861"),
        "/time-card-dtako/resetby-unko-no/26060507533000000042861"
    );
    assert_eq!(AUTOLOAD_PATH, "/dtako-events/autoload");
    assert_eq!(DTAKO_AUTOLOAD_MIME, "application/x-zip-compressed");
    assert_eq!(DTAKO_AUTOLOAD_TIMEOUT_SECS, 120);
}

#[test]
fn join_drops_trailing_slashes_of_the_base() {
    assert_eq!(join("http://h:120//", "/a?b=1"), "http://h:120/a?b=1");
    assert_eq!(join("http://h", "/a"), "http://h/a");
}

#[test]
fn urlencode_matches_the_onprem_quirks() {
    assert_eq!(urlencode("2026-06-29"), "2026-06-29");
    assert_eq!(urlencode("abc.XYZ_123~"), "abc.XYZ_123~");
    assert_eq!(urlencode("a b"), "a%20b");
    assert_eq!(urlencode("a+b"), "a%2Bb");
    // 非 ASCII は UTF-8 のバイトではなくコードポイントの hex (元のまま)
    assert_eq!(urlencode("あ"), "%3042");
}

#[test]
fn excerpt_counts_chars_not_bytes() {
    assert_eq!(excerpt(&"あ".repeat(3), 2), "ああ");
    assert_eq!(excerpt("ab", 10), "ab");
}

#[test]
fn success_is_2xx_only() {
    assert!(is_success(200) && is_success(299));
    assert!(!is_success(199) && !is_success(307) && !is_success(500));
}

#[test]
fn status_error_keeps_500_chars_of_the_body() {
    let e = status_error(503, &"x".repeat(600));
    match &e {
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => assert_eq!((*status, body_excerpt.len()), (503, 500)),
        _ => panic!("{e:?}"),
    }
    assert!(e
        .to_string()
        .starts_with("CakePHP returned status 503, body excerpt: xxx"));
}

#[test]
fn errors_display_like_the_onprem_client() {
    assert_eq!(
        CakephpError::NotConfigured.to_string(),
        "CakePHP base_url is not configured"
    );
    assert_eq!(
        CakephpError::RequestFailed("dns".into()).to_string(),
        "CakePHP request failed: dns"
    );
    assert_eq!(
        CakephpError::JsonError("bad".into()).to_string(),
        "CakePHP response parse failed: bad"
    );
    let boxed: Box<dyn std::error::Error> = Box::new(CakephpError::NotConfigured);
    assert!(boxed.to_string().contains("not configured"));
}

#[test]
fn parse_json_keeps_unknown_top_level_fields_and_rejects_bad_json() {
    let body = br#"{"rows":[{"driver_id":1021}],"month":"2026-06","z":null}"#;
    let r: TimecardDailyResponse = parse_json(body).unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(
        serde_json::to_string(&r).unwrap(),
        r#"{"rows":[{"driver_id":1021}],"month":"2026-06","z":null}"#
    );
    let e = parse_json::<TimecardDailyResponse>(b"<html>").unwrap_err();
    assert!(matches!(e, CakephpError::JsonError(_)), "{e:?}");
}

#[test]
fn autoload_response_keeps_2000_chars_and_the_location() {
    let r = autoload_response(307, &"x".repeat(2100), Some("/".into()));
    assert_eq!((r.status, r.body_excerpt.len()), (307, 2000));
    assert_eq!(r.location.as_deref(), Some("/"));
}

#[test]
fn boundary_has_the_reqwest_shape_and_length() {
    let b = boundary([1, 0xabc, u64::MAX, 0]);
    assert_eq!(
        b,
        "0000000000000001-0000000000000abc-ffffffffffffffff-0000000000000000"
    );
    assert_eq!(b.len(), 67);
}

#[test]
fn autoload_multipart_is_api_then_file_with_the_fixed_mime() {
    let m = autoload_multipart("B", "csvdata.zip", b"PK\x03\x04\xff");
    assert_eq!(m.content_type, "multipart/form-data; boundary=B");
    let want: &[u8] = b"--B\r\nContent-Disposition: form-data; name=\"api\"\r\n\r\n1\r\n\
--B\r\nContent-Disposition: form-data; name=\"file[]\"; filename=\"csvdata.zip\"\r\n\
Content-Type: application/x-zip-compressed\r\n\r\nPK\x03\x04\xff\r\n--B--\r\n";
    assert_eq!(m.body, want);
}

#[test]
fn autoload_multipart_escapes_the_file_name_like_reqwest() {
    let m = autoload_multipart("B", "a\"b\\c\r\n日.zip", b"z");
    let head = "filename=\"a\\\"b\\\\c\\\r\\\n日.zip\"";
    assert!(String::from_utf8(m.body).unwrap().contains(head));
}

#[test]
fn reset_multipart_is_api_only() {
    let m = reset_multipart("B");
    assert_eq!(m.content_type, "multipart/form-data; boundary=B");
    let want: &[u8] = b"--B\r\nContent-Disposition: form-data; name=\"api\"\r\n\r\n1\r\n--B--\r\n";
    assert_eq!(m.body, want);
}
