//! `kintai_logic::day_summaries` の単体テスト。元 (root の `src/routes/kintai_day_summaries.rs`) の 6 本のうち
//! 月の境界 2 本・テナント 3 本・store 1 本は `tests/common.rs` に畳んだ (写しを 1 つにしたため)。
//! ここは入力の検査 (元の handler の 400) と応答の形。

use chrono::NaiveDate;
use kintai_logic::common::Fail;
use kintai_logic::day_summaries::{parse, respond, Binds, Request, Row, MINUTE_COLUMNS};
use postgres_types::Type;
use uuid::Uuid;

#[test]
fn month_and_optional_driver_are_read() {
    assert_eq!(
        parse("month=2026-06").unwrap(),
        Request {
            month: "2026-06".into(),
            driver: None
        }
    );
    assert_eq!(
        parse("month=2026-06&driver=1051").unwrap(),
        Request {
            month: "2026-06".into(),
            driver: Some(1051)
        }
    );
}

#[test]
fn a_malformed_month_is_bad_request() {
    for q in ["", "month=", "month=2026-6", "month=2026-13", "driver=1"] {
        assert_eq!(
            parse(q).unwrap_err(),
            Fail::new(400, "month は YYYY-MM で指定してください"),
            "{q:?}"
        );
    }
}

#[test]
fn a_malformed_driver_is_bad_request() {
    for q in [
        "month=2026-06&driver=",
        "month=2026-06&driver=abc",
        "month=2026-06&driver=-1",
    ] {
        assert_eq!(
            parse(q).unwrap_err(),
            Fail::new(400, "driver は乗務員CD (数字) で指定してください"),
            "{q:?}"
        );
    }
    // u64 には入るが BIGINT に入らない: 元と同じく TryFromIntError の文を足す
    assert_eq!(
        parse("month=2026-06&driver=9999999999999999999").unwrap_err(),
        Fail::new(
            400,
            "driver は乗務員CD (数字) で指定してください: out of range integral type conversion attempted"
        )
    );
}

#[test]
fn binds_are_typed_date_and_int8() {
    let tenant = Uuid::from_u128(1);
    let b = Binds::new(tenant, &parse("month=2026-12&driver=7").unwrap());
    assert_eq!(
        b,
        Binds {
            tenant,
            from: NaiveDate::from_ymd_opt(2026, 12, 1).unwrap(),
            to: NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
            driver: Some(7),
        }
    );
    let types: Vec<Type> = b.params().into_iter().map(|(_, t)| t).collect();
    assert_eq!(types, vec![Type::UUID, Type::DATE, Type::DATE, Type::INT8]);
}

fn row(driver_cd: i64, date: &str, start: &str, base: i32) -> Row {
    let mut minutes = [0; 11];
    for (i, m) in minutes.iter_mut().enumerate() {
        *m = base + i as i32;
    }
    Row {
        driver_cd,
        date: date.into(),
        shift_start_at: start.into(),
        shift_source: "timecard".into(),
        minutes,
    }
}

/// 応答の形 (キーは `乗務員CD|暦日|開始時刻`、列名は表と同じ 12 個)。キーの並びは元と同じ辞書順。
#[test]
fn response_shape_matches_the_original() {
    let rows = vec![row(1051, "2026-06-01", "2026-06-01 08:00:00", 100)];
    let got = respond("2026-06", &rows);
    let summary = &got["summaries"]["1051|2026-06-01|2026-06-01 08:00:00"];
    assert_eq!(summary["shift_source"], "timecard");
    for (i, name) in MINUTE_COLUMNS.iter().enumerate() {
        assert_eq!(summary[*name], 100 + i as i64, "{name}");
    }
    assert_eq!(
        serde_json::to_string(&got).unwrap(),
        concat!(
            r#"{"month":"2026-06","rows":1,"summaries":{"1051|2026-06-01|2026-06-01 08:00:00":{"#,
            r#""break_minutes":102,"legal_holiday_minutes":107,"legal_holiday_night_minutes":110,"#,
            r#""night_minutes":108,"overtime_minutes":106,"overtime_night_minutes":109,"#,
            r#""rest_minus_minutes":103,"restraint_minutes":100,"shift_source":"timecard","#,
            r#""statutory_minutes":104,"within_statutory_overtime_minutes":105,"working_minutes":101}}}"#
        )
    );
}

/// 0 件の月は 404 ではなく 200 + 空の `summaries`。同じキーの行は後勝ちで `rows` は畳んだ数 (元と同じ)。
#[test]
fn empty_month_and_duplicate_keys() {
    assert_eq!(
        respond("2026-06", &[]),
        serde_json::json!({"month": "2026-06", "rows": 0, "summaries": {}})
    );
    let rows = vec![
        row(1, "2026-06-01", "2026-06-01 08:00:00", 1),
        row(1, "2026-06-01", "2026-06-01 08:00:00", 50),
    ];
    let got = respond("2026-06", &rows);
    assert_eq!(got["rows"], 1);
    assert_eq!(
        got["summaries"]["1|2026-06-01|2026-06-01 08:00:00"]["restraint_minutes"],
        50
    );
}
