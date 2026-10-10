//! `kintai_logic::shift_days` の単体テスト。元 (root の `src/routes/shift_days.rs`) の 5 本の写し
//! (handler を叩いていた 3 本は、同じ入力を `parse` に通す形に書き直した) と応答の形。

use kintai_logic::common::Fail;
use kintai_logic::shift_days::{parse, parse_driver_cd, respond, Binds, Request, Row};
use postgres_types::Type;
use uuid::Uuid;

fn query(month: Option<&str>, driver: Option<&str>) -> String {
    let mut parts = Vec::new();
    if let Some(m) = month {
        parts.push(format!("month={m}"));
    }
    if let Some(d) = driver {
        parts.push(format!("driver={d}"));
    }
    parts.join("&")
}

#[test]
fn a_numeric_driver_is_accepted() {
    assert_eq!(parse_driver_cd(Some("9001")), Some(9001));
    assert_eq!(parse_driver_cd(Some("0")), Some(0));
}

#[test]
fn a_missing_or_non_numeric_driver_is_rejected() {
    // 末尾 2 つは桁溢れ (u64 に入らない / u64 には入るが BIGINT に入らない)
    for bad in [
        None,
        Some(""),
        Some("abc"),
        Some("-1"),
        Some("90 01"),
        Some("99999999999999999999"),
        Some("9999999999999999999"),
    ] {
        assert_eq!(parse_driver_cd(bad), None, "{bad:?}");
    }
}

#[test]
fn a_malformed_month_is_bad_request() {
    for bad in [None, Some(""), Some("2026-4"), Some("2026-13")] {
        let f = parse(&query(bad, Some("9001"))).expect_err("must reject a bad month");
        assert_eq!(
            f,
            Fail::new(400, "month は YYYY-MM で指定してください"),
            "{bad:?}"
        );
    }
}

#[test]
fn a_missing_or_malformed_driver_is_bad_request() {
    for bad in [None, Some(""), Some("abc"), Some("-1")] {
        let f = parse(&query(Some("2026-04"), bad)).expect_err("must reject a bad driver");
        assert_eq!(
            f,
            Fail::new(400, "driver は乗務員CD (数字) で指定してください"),
            "{bad:?}"
        );
    }
}

/// 元: the_handler_fails_closed_without_a_store。正しい入力は検査を通る (503 は binding の段で worker が返す)。
#[test]
fn valid_input_passes_to_the_next_stage() {
    assert_eq!(
        parse(&query(Some("2026-04"), Some("9001"))).unwrap(),
        Request {
            month: "2026-04".into(),
            driver_cd: 9001
        }
    );
}

#[test]
fn binds_are_typed() {
    let b = Binds::new(
        Uuid::from_u128(1),
        &parse("month=2026-04&driver=9001").unwrap(),
    );
    assert_eq!(b.driver_cd, 9001);
    assert_eq!(b.from.to_rfc3339(), "2026-04-01T00:00:00+09:00");
    assert_eq!(b.to.to_rfc3339(), "2026-05-01T00:00:00+09:00");
    let types: Vec<Type> = b.params().into_iter().map(|(_, t)| t).collect();
    assert_eq!(
        types,
        vec![Type::UUID, Type::INT8, Type::TIMESTAMPTZ, Type::TIMESTAMPTZ]
    );
}

/// 元の module docs の応答例と同じ形。`summary` / `non_working` の null はそのまま null。
#[test]
fn response_shape_matches_the_original() {
    let req = parse("month=2026-04&driver=9001").unwrap();
    let rows = vec![
        Row {
            start_at: "2026-04-03 22:10:00".into(),
            end_at: "2026-04-04 09:05:00".into(),
            shift_source: "timecard".into(),
            summary: Some(serde_json::json!({"restraint_minutes": 655, "working_minutes": 595})),
            non_working: Some(serde_json::json!([
                {"start": "2026-04-04 02:00:00", "end": "2026-04-04 03:00:00", "kind": "break_event"}
            ])),
            parts: serde_json::json!([{"date": "2026-04-03", "restraint_minutes": 110}]),
        },
        Row {
            start_at: "2026-04-05 08:00:00".into(),
            end_at: "2026-04-05 17:00:00".into(),
            shift_source: "rest".into(),
            summary: None,
            non_working: None,
            parts: serde_json::json!([]),
        },
    ];
    assert_eq!(
        serde_json::to_string(&respond(&req, rows)).unwrap(),
        concat!(
            r#"{"driver_cd":9001,"items":[{"end_at":"2026-04-04 09:05:00","non_working":[{"end":"2026-04-04 03:00:00","#,
            r#""kind":"break_event","start":"2026-04-04 02:00:00"}],"parts":[{"date":"2026-04-03","restraint_minutes":110}],"#,
            r#""shift_source":"timecard","start_at":"2026-04-03 22:10:00","summary":{"restraint_minutes":655,"working_minutes":595}},"#,
            r#"{"end_at":"2026-04-05 17:00:00","non_working":null,"parts":[],"shift_source":"rest","#,
            r#""start_at":"2026-04-05 08:00:00","summary":null}],"month":"2026-04"}"#
        )
    );
}
