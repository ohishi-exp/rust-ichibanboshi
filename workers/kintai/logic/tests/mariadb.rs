//! 社内 MariaDB を読む 4 本 (`mariadb_reads`) と行 → JSON (`mariadb_rows`) の単体テスト (DB 不要)。
//!
//! 元 (root の `src/routes/kintai.rs` の handler・`src/kintai_repo.rs` の `*_to_json`) と同じ検査・同じキー・
//! 同じ null 扱いであることを固定する。

use chrono::NaiveDate;
use kintai_logic::common::{mariadb_fail, mariadb_unconfigured, Fail};
use kintai_logic::mariadb_reads::{jst_today, MariadbRead, Request, DRIVER_MSG, MONTH_MSG};
use kintai_logic::mariadb_rows::{
    all_event_row, event_row, reading_date_row, rest_row, rows_to_json,
};
use kintai_mysql::bind::{names, Value as Bind};
use kintai_mysql::response::Row;
use serde_json::json;

const ALL: [MariadbRead; 4] = [
    MariadbRead::Events,
    MariadbRead::RestDiff,
    MariadbRead::ReadingDates,
    MariadbRead::TailGapProbe,
];

fn row(cells: &[Option<&str>]) -> Row {
    cells
        .iter()
        .map(|c| c.map(|s| s.as_bytes().to_vec()))
        .collect()
}

fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).unwrap()
}

fn bad(msg: &str) -> Fail {
    Fail::new(400, msg)
}

fn keys(v: &serde_json::Value) -> Vec<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

// ── 経路 ──

#[test]
fn paths_and_names() {
    for read in ALL {
        let path = format!("/api/kintai/{}", read.as_str());
        assert_eq!(MariadbRead::from_path(&path), Some(read));
    }
    assert_eq!(MariadbRead::from_path("/api/kintai/kosoku-daily"), None);
}

// ── 失敗の形 ──

#[test]
fn mariadb_failures_carry_only_the_kind() {
    let f = mariadb_unconfigured();
    assert_eq!(
        (f.status, f.body.as_str()),
        (503, "MariaDB 接続設定が未設定")
    );
    let f = mariadb_fail("connect:timeout");
    assert_eq!(
        (f.status, f.body.as_str()),
        (502, "MariaDB query failed: connect:timeout")
    );
}

// ── 入力の検査 (元の handler と同じ順・同じ文言) ──

#[test]
fn events_requires_valid_month_then_driver() {
    let p = |q: &str| MariadbRead::Events.parse(q);
    // month が先 (driver も不正でも month の文言)
    assert_eq!(p(""), Err(bad(MONTH_MSG)));
    assert_eq!(p("month=2026-13&driver=x"), Err(bad(MONTH_MSG)));
    // is_valid_month は 7 文字ちょうどを要る
    assert_eq!(p("month=2026-06-01&driver=1"), Err(bad(MONTH_MSG)));
    // driver は必須
    assert_eq!(p("month=2026-06"), Err(bad(DRIVER_MSG)));
    assert_eq!(p("month=2026-06&driver="), Err(bad(DRIVER_MSG)));
    assert_eq!(p("month=2026-06&driver=-1"), Err(bad(DRIVER_MSG)));
    let req = p("month=2026-06&driver=1078&view=compare").unwrap();
    assert_eq!(
        req,
        Request {
            read: MariadbRead::Events,
            month: "2026-06".into(),
            driver: Some(1078),
            from: "2026-06-01 00:00:00".into(),
            to: "2026-07-02 00:00:00".into(),
        }
    );
}

#[test]
fn optional_driver_reads_use_their_windows() {
    for read in [MariadbRead::RestDiff, MariadbRead::ReadingDates] {
        let req = read.parse("month=2026-12").unwrap();
        assert_eq!(req.driver, None);
        // month_range = 翌月 2 日まで
        assert_eq!(
            (req.from.as_str(), req.to.as_str()),
            ("2026-12-01 00:00:00", "2027-01-02 00:00:00")
        );
    }
    // exact_month_range = 翌月初まで
    let req = MariadbRead::TailGapProbe
        .parse("month=2026-12&driver=1078")
        .unwrap();
    assert_eq!(req.driver, Some(1078));
    assert_eq!(
        (req.from.as_str(), req.to.as_str()),
        ("2026-12-01 00:00:00", "2027-01-01 00:00:00")
    );
}

#[test]
fn optional_driver_reads_reject_bad_month_then_bad_driver() {
    for read in [
        MariadbRead::RestDiff,
        MariadbRead::ReadingDates,
        MariadbRead::TailGapProbe,
    ] {
        assert_eq!(read.parse(""), Err(bad(MONTH_MSG)));
        assert_eq!(read.parse("month=2026-13&driver=x"), Err(bad(MONTH_MSG)));
        // `driver=` (空) は省略ではなく不正
        assert_eq!(read.parse("month=2026-06&driver="), Err(bad(DRIVER_MSG)));
        assert_eq!(read.parse("month=2026-06&driver=abc"), Err(bad(DRIVER_MSG)));
    }
}

#[test]
fn duplicate_query_fields_are_axum_400() {
    for read in ALL {
        let err = read.parse("month=2026-06&month=2026-07").unwrap_err();
        assert_eq!(err.status, 400);
        assert!(
            err.body.starts_with("Failed to deserialize query string: "),
            "{}",
            err.body
        );
    }
}

// ── 名前付き引数 ──

#[test]
fn binds_match_the_sql_names_exactly() {
    for read in ALL {
        let req = read.parse("month=2026-06&driver=1078").unwrap();
        let mut passed: Vec<&str> = req.binds().iter().map(|(n, _)| *n).collect();
        passed.sort();
        let mut used = names(read.sql()).unwrap();
        used.sort();
        assert_eq!(passed, used, "{}", read.as_str());
        let sql = req.sql_text().unwrap();
        assert!(names(&sql).unwrap().is_empty());
        assert!(sql.contains("'2026-06-01 00:00:00'"));
    }
}

#[test]
fn driver_is_null_when_omitted() {
    let req = MariadbRead::RestDiff.parse("month=2026-06").unwrap();
    assert!(req.binds().contains(&("driver", Bind::Null)));
    let sql = req.sql_text().unwrap();
    assert!(sql.contains("(NULL IS NULL OR t.driver_id = NULL)"));
    let req = MariadbRead::ReadingDates
        .parse("month=2026-06&driver=1107")
        .unwrap();
    assert!(req
        .sql_text()
        .unwrap()
        .contains("(1107 IS NULL OR r.`対象乗務員CD` = 1107)"));
}

// ── 行 → JSON ──

#[test]
fn event_row_keeps_nulls_and_keys() {
    let v = event_row(&row(&[
        Some("2026-07-23 06:11:45"),
        None,
        Some("1051"),
        Some("timecard"),
        Some("始業"),
        None,
        None,
    ]))
    .unwrap();
    assert_eq!(
        v,
        json!({
            "datetime": "2026-07-23 06:11:45", "end_datetime": null, "driver_id": 1051,
            "source": "timecard", "state": "始業", "unko_no": null, "vehicle": null,
        })
    );
    assert_eq!(keys(&v).len(), 7);
}

#[test]
fn all_event_row_omits_unread_columns() {
    let v = all_event_row(&row(&[
        Some("2026-06-02 06:00:00"),
        Some("2026-06-02 06:20:00"),
        None,
        Some("dtako_events"),
        Some("休憩"),
    ]))
    .unwrap();
    assert_eq!(
        keys(&v),
        ["datetime", "driver_id", "end_datetime", "source", "state"]
    );
    assert!(v["driver_id"].is_null());
    assert!(v.get("unko_no").is_none() && v.get("vehicle").is_none());
}

#[test]
fn rest_row_has_no_vehicle() {
    let v = rest_row(&row(&[
        Some("2026-06-02 06:00:00"),
        None,
        Some("-3"),
        Some("dtako"),
        None,
        Some("26060200000000000000001"),
    ]))
    .unwrap();
    assert_eq!(
        v,
        json!({
            "datetime": "2026-06-02 06:00:00", "end_datetime": null, "driver_id": -3,
            "source": "dtako", "state": null, "unko_no": "26060200000000000000001",
        })
    );
}

#[test]
fn reading_date_row_keys() {
    let v = reading_date_row(&row(&[
        Some("1107"),
        Some("26062400000000000000001"),
        Some("2026-07-06"),
        Some("2026-06-24"),
        None,
        Some("2026-06-25 10:00:00"),
    ]))
    .unwrap();
    assert_eq!(
        v,
        json!({
            "driver_cd": 1107, "unko_no": "26062400000000000000001", "reading_date": "2026-07-06",
            "run_date": "2026-06-24", "departure_at": null, "return_at": "2026-06-25 10:00:00",
        })
    );
}

#[test]
fn row_errors_are_502_without_values() {
    let shape = event_row(&row(&[Some("x")])).unwrap_err();
    assert_eq!(shape, mariadb_fail("rows:shape"));
    let null = all_event_row(&row(&[None, None, None, Some("t"), None])).unwrap_err();
    assert_eq!(null, mariadb_fail("rows:null"));
    let null = reading_date_row(&row(&[None, None, None, None, None, None])).unwrap_err();
    assert_eq!(null, mariadb_fail("rows:null"));
    let int = rest_row(&row(&[Some("d"), None, Some("1.5"), Some("s"), None, None])).unwrap_err();
    assert_eq!(int, mariadb_fail("rows:int"));
    // 不正な UTF-8 は NULL と区別する
    let mut bad = row(&[Some("d"), None, None, Some("s"), None]);
    bad[4] = Some(vec![0xFF, 0xFE]);
    assert_eq!(all_event_row(&bad).unwrap_err(), mariadb_fail("rows:utf8"));
    // 1 行でも失敗すれば全体がエラー
    let ok = row(&[Some("d"), None, None, Some("s"), None]);
    assert_eq!(
        rows_to_json(&[ok.clone(), bad], all_event_row).unwrap_err(),
        mariadb_fail("rows:utf8")
    );
    assert_eq!(rows_to_json(&[ok], all_event_row).unwrap().len(), 1);
}

// ── 応答 ──

#[test]
fn events_response_is_rows_only() {
    let req = MariadbRead::Events
        .parse("month=2026-06&driver=1051")
        .unwrap();
    let r = row(&[
        Some("2026-06-01 06:00:00"),
        None,
        Some("1051"),
        Some("timecard"),
        Some("始業"),
        None,
        None,
    ]);
    let v = req.respond(&[r], ymd(2026, 6, 10)).unwrap();
    assert_eq!(keys(&v), ["rows"]);
    assert_eq!(v["rows"][0]["driver_id"], 1051);
    let bad = row(&[None, None, None, None, None, None, None]);
    assert_eq!(
        req.respond(&[bad], ymd(2026, 6, 10)).unwrap_err(),
        mariadb_fail("rows:null")
    );
}

#[test]
fn rest_diff_response_shape() {
    let req = MariadbRead::RestDiff.parse("month=2026-06").unwrap();
    let rows = [
        row(&[
            Some("2026-06-02 10:00:00"),
            None,
            Some("1445"),
            Some("dtako"),
            Some("休息"),
            Some("26060200000000000000001"),
        ]),
        row(&[
            Some("2026-06-02 10:00:00"),
            Some("2026-06-02 18:00:00"),
            Some("1445"),
            Some("dtako_events"),
            Some("休息"),
            Some("26060200000000000000001"),
        ]),
    ];
    let v = req.respond(&rows, ymd(2026, 6, 10)).unwrap();
    assert_eq!(
        keys(&v),
        [
            "by_driver",
            "driver",
            "from",
            "items",
            "max_items",
            "mismatch_total",
            "month",
            "scanned_unko",
            "skipped_rows",
            "to",
            "total",
            "total_by_kind",
        ]
    );
    assert_eq!(v["month"], "2026-06");
    assert!(v["driver"].is_null());
    assert_eq!(v["to"], "2026-07-02 00:00:00");
    assert_eq!(v["max_items"], 500);
    assert_eq!(v["scanned_unko"], 1);
    let bad = row(&[Some("x")]);
    assert_eq!(
        req.respond(&[bad], ymd(2026, 6, 10)).unwrap_err(),
        mariadb_fail("rows:shape")
    );
}

#[test]
fn reading_dates_response_shape() {
    let req = MariadbRead::ReadingDates
        .parse("month=2026-06&driver=1107")
        .unwrap();
    let rows = [row(&[
        Some("1107"),
        Some("26062400000000000000001"),
        Some("2026-07-06"),
        Some("2026-06-24"),
        None,
        None,
    ])];
    let v = req.respond(&rows, ymd(2026, 6, 10)).unwrap();
    assert_eq!(
        keys(&v),
        [
            "by_reading_date",
            "driver",
            "from",
            "items",
            "max_items",
            "month",
            "skipped_rows",
            "to",
            "total",
            "unknown_reading_date",
        ]
    );
    assert_eq!(v["driver"], 1107);
    assert_eq!(v["total"], 1);
    assert_eq!(v["max_items"], 2000);
    assert_eq!(
        v["by_reading_date"]["2026-07-06"][0],
        "26062400000000000000001"
    );
    let bad = row(&[Some("x")]);
    assert_eq!(
        req.respond(&[bad], ymd(2026, 6, 10)).unwrap_err(),
        mariadb_fail("rows:shape")
    );
}

#[test]
fn tail_gap_probe_expected_is_min_of_month_end_and_yesterday() {
    let req = MariadbRead::TailGapProbe.parse("month=2026-06").unwrap();
    let rows = [row(&[
        Some("2026-06-03 08:00:00"),
        None,
        Some("1078"),
        Some("timecard"),
        Some("始業"),
    ])];
    // 進行中の月: today - 1 日
    let v = req.respond(&rows, ymd(2026, 6, 20)).unwrap();
    assert_eq!(v["expected"], "2026-06-19");
    // 過ぎた月: 月末
    let v = req.respond(&rows, ymd(2026, 8, 1)).unwrap();
    assert_eq!(v["expected"], "2026-06-30");
    assert_eq!(
        keys(&v),
        [
            "driver",
            "drivers",
            "expected",
            "from",
            "month",
            "over_threshold_total",
            "over_threshold_unpunched_total",
            "population",
            "threshold_days",
            "to",
        ]
    );
    assert_eq!(v["month"], "2026-06");
    assert_eq!(v["to"], "2026-07-01 00:00:00");
    assert!(v["driver"].is_null());
    let bad = row(&[Some("x")]);
    assert_eq!(
        req.respond(&[bad], ymd(2026, 6, 20)).unwrap_err(),
        mariadb_fail("rows:shape")
    );
}

#[test]
fn jst_today_turns_at_15_utc() {
    // 2026-06-30T14:59:59.999Z = JST 06-30 23:59:59.999
    assert_eq!(jst_today(1_782_831_599_999), ymd(2026, 6, 30));
    // 2026-06-30T15:00:00Z = JST 07-01 00:00
    assert_eq!(jst_today(1_782_831_600_000), ymd(2026, 7, 1));
}
