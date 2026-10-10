//! 書き込みの口 (`POST /api/kintai/timecard`・`POST /api/kintai/wage-snapshot`) と `GET /api/kintai/timecard/signatures` の
//! 純粋部分の単体テスト: 認可 (`write_auth`)・本文の読み方 (`common::parse_json`、axum の `Json` と同じ)・入力の検査・応答。
//! axum の実物との突き合わせは `workers/kintai/pg/tests/axum_parity.rs`。

use std::collections::BTreeMap;

use chrono::NaiveDate;
use kintai_kosoku::kintai_push::{TimecardBatch, TimecardBatchResult};
use kintai_logic::common::{no_write_db, parse_json, write_preflight, MAX_JSON_BODY_BYTES};
use kintai_logic::timecard_write::{
    batch_respond, parse_batch, parse_signatures, signatures_respond, DB_WHAT,
};
use kintai_logic::wage_write::parse_snapshot;
use kintai_logic::write_auth::{authorize, FORBIDDEN, UNCONFIGURED};
use serde_json::json;

const JSON: Option<&str> = Some("application/json");

// ── 認可 ──

#[test]
fn the_right_token_passes() {
    assert_eq!(authorize(Some("s3cret"), Some("s3cret")), Ok(()));
}

#[test]
fn a_missing_or_wrong_token_is_forbidden_with_a_fixed_body() {
    for header in [
        None,
        Some(""),
        Some("s3cre"),
        Some("s3cret "),
        Some("S3CRET"),
    ] {
        let f = authorize(Some("s3cret"), header).unwrap_err();
        assert_eq!((f.status, f.body.as_str()), (403, FORBIDDEN), "{header:?}");
    }
}

#[test]
fn without_a_readable_secret_nothing_is_written() {
    for secret in [None, Some("")] {
        // 空の secret と空のヘッダーを一致させない
        for header in [None, Some(""), Some("x")] {
            let f = authorize(secret, header).unwrap_err();
            assert_eq!((f.status, f.body.as_str()), (503, UNCONFIGURED));
        }
    }
}

// ── 本文 (axum の Json と同じ) ──

#[test]
fn parse_json_follows_axum() {
    let ok: serde_json::Value = parse_json(JSON, b"{\"a\":1}").unwrap();
    assert_eq!(ok, json!({"a": 1}));
    for ct in [
        Some("application/json; charset=utf-8"),
        Some("application/cloudevents+json"),
    ] {
        assert!(parse_json::<serde_json::Value>(ct, b"{}").is_ok(), "{ct:?}");
    }
    for ct in [
        None,
        Some("text/json"),
        Some("text/plain"),
        Some("not a mime"),
    ] {
        let f = parse_json::<serde_json::Value>(ct, b"{}").unwrap_err();
        assert_eq!(f.status, 415, "{ct:?}");
        assert_eq!(
            f.body,
            "Expected request with `Content-Type: application/json`"
        );
    }
    let big = vec![b' '; MAX_JSON_BODY_BYTES + 1];
    let f = parse_json::<serde_json::Value>(JSON, &big).unwrap_err();
    assert_eq!(
        (f.status, f.body.as_str()),
        (
            413,
            "Failed to buffer the request body: length limit exceeded"
        )
    );
    let f = parse_json::<serde_json::Value>(JSON, b"{").unwrap_err();
    assert_eq!(f.status, 400);
    assert!(
        f.body
            .starts_with("Failed to parse the request body as JSON: "),
        "{}",
        f.body
    );
    let f = parse_json::<serde_json::Value>(JSON, b"{} x").unwrap_err();
    assert_eq!(f.status, 400);
    assert!(f.body.contains("trailing characters"), "{}", f.body);
}

#[test]
fn a_body_of_the_wrong_shape_is_422_with_the_path() {
    let f = parse_batch(JSON, br#"{"month":"2026-06","driver_cd":"x"}"#).unwrap_err();
    assert_eq!(f.status, 422);
    assert!(
        f.body
            .starts_with("Failed to deserialize the JSON body into the target type: driver_cd: "),
        "{}",
        f.body
    );
}

// ── POST /api/kintai/timecard ──

#[test]
fn a_batch_needs_a_month() {
    let f = parse_batch(JSON, br#"{"month":"2026-6","driver_cd":1130}"#).unwrap_err();
    assert_eq!(
        (f.status, f.body.as_str()),
        (400, "month は YYYY-MM で指定してください")
    );
    let b = parse_batch(
        JSON,
        br#"{"month":"2026-06","driver_cd":1130,"days":{"2026-06-01":[{"x":1}]},"delete_dates":["2026-06-02"]}"#,
    )
    .unwrap();
    assert_eq!(b.driver_cd, 1130);
    assert_eq!(b.days.len(), 1);
    // days・delete_dates は省略できる (元の serde(default))
    let b: TimecardBatch = parse_batch(JSON, br#"{"month":"2026-06","driver_cd":1}"#).unwrap();
    assert!(b.is_empty());
}

#[test]
fn the_batch_response_is_the_result_as_is() {
    let r = TimecardBatchResult {
        days_written: 1,
        misplaced: 2,
        ..Default::default()
    };
    assert_eq!(
        batch_respond(&r),
        json!({"days_written": 1, "days_deleted": 0, "events_written": 0, "deduped": 0,
               "rejected": {}, "unknown_states": [], "misplaced": 2})
    );
    assert_eq!(DB_WHAT, "kintai push db");
}

// ── GET /api/kintai/timecard/signatures ──

#[test]
fn signatures_check_month_then_driver() {
    for (q, want) in [
        ("", "month は YYYY-MM で指定してください"),
        (
            "month=2026-13&driver_cd=1",
            "month は YYYY-MM で指定してください",
        ),
        ("month=2026-06", "driver_cd は必須です"),
    ] {
        let f = parse_signatures(q).unwrap_err();
        assert_eq!((f.status, f.body.as_str()), (400, want), "{q}");
    }
    let f = parse_signatures("month=2026-06&driver_cd=abc").unwrap_err();
    assert_eq!(f.status, 400);
    assert!(
        f.body.starts_with("Failed to deserialize query string: "),
        "{}",
        f.body
    );
}

#[test]
fn signatures_cover_the_month_in_jst() {
    let req = parse_signatures("month=2026-12&driver_cd=1130").unwrap();
    assert_eq!(req.from.to_rfc3339(), "2026-12-01T00:00:00+09:00");
    assert_eq!(req.to.to_rfc3339(), "2027-01-01T00:00:00+09:00");
    let sigs = BTreeMap::from([(
        NaiveDate::from_ymd_opt(2026, 12, 3).unwrap(),
        "ab".to_string(),
    )]);
    assert_eq!(
        signatures_respond(&req, &sigs),
        json!({"month": "2026-12", "driver_cd": 1130, "signatures": {"2026-12-03": "ab"}})
    );
}

// ── POST /api/kintai/wage-snapshot ──

#[test]
fn a_snapshot_is_validated_in_the_original_order() {
    let base = json!({"comp_id": "c1", "month": "2026-06", "restraint_source": "gcp",
                      "wage_logic_version": "w1", "rows": []});
    let (valid, synced) = parse_snapshot(JSON, base.to_string().as_bytes()).unwrap();
    assert_eq!(valid.comp_id, "c1");
    assert_eq!(synced, None);

    let mut bad = base.clone();
    bad["comp_id"] = json!(" ");
    let f = parse_snapshot(JSON, bad.to_string().as_bytes()).unwrap_err();
    assert_eq!((f.status, f.body.as_str()), (400, "comp_id は必須です"));

    let mut bad = base.clone();
    bad["masters"] = json!({"payroll_synced_at": "2026/07/03"});
    let f = parse_snapshot(JSON, bad.to_string().as_bytes()).unwrap_err();
    assert_eq!(
        (f.status, f.body.as_str()),
        (
            400,
            "masters.payroll_synced_at は RFC3339 で指定してください"
        )
    );

    let f = parse_snapshot(Some("text/plain"), b"{}").unwrap_err();
    assert_eq!(f.status, 415);
}

// ── binding とテナント ──

#[test]
fn write_preflight_says_write_when_the_binding_is_missing() {
    assert_eq!(
        write_preflight(false, Some("x")).unwrap_err(),
        no_write_db()
    );
    assert_eq!(no_write_db().status, 503);
    assert!(no_write_db().body.contains("書き先"));
    assert_eq!(write_preflight(true, None).unwrap_err().status, 503);
    // テナントの値は実在しない作り物 (リテラルを書かず、数から作る)
    let t = uuid::Uuid::from_u128(0x5ca1_ab1e).to_string();
    assert_eq!(write_preflight(true, Some(&t)).unwrap().to_string(), t);
}
