//! `kintai_logic::shift_overlaps` の単体テスト。元 (root の `src/routes/shift_overlaps.rs`) の 7 本のうち
//! 月の境界 2 本・テナント 3 本は `tests/common.rs` に畳んだ。月の検査と「store が無ければ 503」はここ
//! (後者は「月が正しければ検査を通り、次の段 (binding) へ進む」ことまで)。

use chrono::{TimeZone, Utc};
use kintai_logic::common::Fail;
use kintai_logic::shift_overlaps::{parse, respond, Binds, Request, Row};
use postgres_types::Type;
use uuid::Uuid;

/// 元: a_malformed_month_is_bad_request (None / "" / "2026-6" / "2026-13")。
#[test]
fn a_malformed_month_is_bad_request() {
    for q in ["", "month=", "month=2026-6", "month=2026-13"] {
        assert_eq!(
            parse(q).unwrap_err(),
            Fail::new(400, "month は YYYY-MM で指定してください"),
            "{q:?}"
        );
    }
}

/// 元: the_handler_fails_closed_without_a_store。正しい月は検査を通る (503 は binding の段で worker が返す)。
#[test]
fn a_valid_month_passes_to_the_next_stage() {
    assert_eq!(
        parse("month=2026-06").unwrap(),
        Request {
            month: "2026-06".into()
        }
    );
}

#[test]
fn binds_are_jst_midnights_as_timestamptz() {
    let tenant = Uuid::from_u128(1);
    let b = Binds::new(tenant, &parse("month=2026-06").unwrap());
    assert_eq!(
        b.from.with_timezone(&Utc),
        Utc.with_ymd_and_hms(2026, 5, 31, 15, 0, 0).unwrap()
    );
    assert_eq!(
        b.to.with_timezone(&Utc),
        Utc.with_ymd_and_hms(2026, 6, 30, 15, 0, 0).unwrap()
    );
    let types: Vec<Type> = b.params().into_iter().map(|(_, t)| t).collect();
    assert_eq!(
        types,
        vec![Type::UUID, Type::TIMESTAMPTZ, Type::TIMESTAMPTZ]
    );
}

#[test]
fn response_shape_matches_the_original() {
    let rows = vec![Row {
        driver_cd: 1051,
        a_start: "2026-06-01 08:00:00".into(),
        a_end: "2026-06-01 18:00:00".into(),
        b_start: "2026-06-01 10:00:00".into(),
        b_end: "2026-06-01 20:00:00".into(),
    }];
    assert_eq!(
        serde_json::to_string(&respond("2026-06", &rows)).unwrap(),
        concat!(
            r#"{"items":[{"a_end":"2026-06-01 18:00:00","a_start":"2026-06-01 08:00:00","#,
            r#""b_end":"2026-06-01 20:00:00","b_start":"2026-06-01 10:00:00","driver_cd":1051}],"#,
            r#""month":"2026-06"}"#
        )
    );
    assert_eq!(
        respond("2026-06", &[]),
        serde_json::json!({"month": "2026-06", "items": []})
    );
}
