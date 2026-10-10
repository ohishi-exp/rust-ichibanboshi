//! `kintai_logic::change_log` の単体テスト。元 (root の `src/routes/change_log.rs`) の 5 本のうち
//! テナント 3 本は `tests/common.rs` に畳んだ。期間の検査と「store が無ければ 503」 (の手前まで) はここ。

use chrono::NaiveDate;
use kintai_logic::change_log::{parse, parse_range, respond, Binds, ChangeLogQuery, Request, Row};
use kintai_logic::common::Fail;
use postgres_types::Type;
use uuid::Uuid;

fn q(from: Option<&str>, to: Option<&str>) -> ChangeLogQuery {
    ChangeLogQuery {
        driver: None,
        from: from.map(str::to_string),
        to: to.map(str::to_string),
    }
}

fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

#[test]
fn the_range_is_checked() {
    let ok = parse_range(&q(Some("2026-02-01"), Some("2027-03-07")));
    assert!(ok.is_ok(), "400 日ちょうどは通す");
    for (from, to, want) in [
        (None, Some("2026-02-01"), "YYYY-MM-DD"),
        (Some("2026-02-01"), Some("2026/02/02"), "YYYY-MM-DD"),
        (Some("2026-02-02"), Some("2026-02-01"), "以前"),
        (Some("2026-02-01"), Some("2027-03-08"), "400"),
    ] {
        let f = parse_range(&q(from, to)).expect_err("must reject");
        assert_eq!(f.status, 400);
        assert!(f.body.contains(want), "{}", f.body);
    }
}

/// 元: the_handler_fails_closed_without_a_store。正しい期間は検査を通る (503 は binding の段で worker が返す)。
#[test]
fn a_valid_range_passes_to_the_next_stage() {
    assert_eq!(
        parse("from=2026-02-01&to=2026-02-28").unwrap(),
        Request {
            driver: None,
            from: ymd(2026, 2, 1),
            to: ymd(2026, 2, 28)
        }
    );
    // driver は数値で読む (元と同じく負の数も型としては通る)
    assert_eq!(
        parse("driver=-3&from=2026-02-01&to=2026-02-01")
            .unwrap()
            .driver,
        Some(-3)
    );
}

/// driver は `Option<i64>` で読むので、数でなければ Query の段で 400 (axum の Query と同じ本文)。
#[test]
fn a_non_numeric_driver_is_rejected_like_axum() {
    assert_eq!(
        parse("driver=abc&from=2026-02-01&to=2026-02-28").unwrap_err(),
        Fail::new(
            400,
            "Failed to deserialize query string: driver: invalid digit found in string"
        )
    );
}

#[test]
fn binds_are_typed() {
    let b = Binds::new(
        Uuid::from_u128(1),
        &parse("driver=1051&from=2026-02-01&to=2026-02-28").unwrap(),
    );
    assert_eq!(
        (b.from, b.to, b.driver),
        (ymd(2026, 2, 1), ymd(2026, 2, 28), Some(1051))
    );
    let types: Vec<Type> = b.params().into_iter().map(|(_, t)| t).collect();
    assert_eq!(types, vec![Type::UUID, Type::DATE, Type::DATE, Type::INT8]);
}

#[test]
fn response_shape_matches_the_original() {
    let req = parse("driver=1051&from=2026-02-01&to=2026-02-28").unwrap();
    let rows = vec![Row {
        driver_cd: 1051,
        date: "2026-02-03".into(),
        recorded_at: "2026-02-04 09:00:00".into(),
        before: Some(serde_json::json!([{"state": "start"}])),
        after: None,
    }];
    assert_eq!(
        serde_json::to_string(&respond(&req, Some("2026-01-15 10:00:00".into()), rows)).unwrap(),
        concat!(
            r#"{"changes":[{"after":null,"before":[{"state":"start"}],"date":"2026-02-03","driver_cd":1051,"#,
            r#""recorded_at":"2026-02-04 09:00:00"}],"driver":1051,"from":"2026-02-01","#,
            r#""recording_since":"2026-01-15 10:00:00","to":"2026-02-28"}"#
        )
    );
    // driver 省略・記録なし → null
    let req = parse("from=2026-02-01&to=2026-02-01").unwrap();
    assert_eq!(
        respond(&req, None, vec![]),
        serde_json::json!({
            "driver": null, "from": "2026-02-01", "to": "2026-02-01",
            "recording_since": null, "changes": []
        })
    );
}
