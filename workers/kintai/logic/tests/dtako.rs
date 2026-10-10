//! 社内 MariaDB を読む day-events・dtako/worktime (`dtako_reads`) の単体テスト (DB 不要)。
//!
//! 元 (root の `src/routes/dtako_day.rs`・`src/routes/dtako_worktime.rs` の handler) と同じ検査の順・同じ 400 の文言・
//! 同じ SQL であること、応答が共有 crate `kintai-dtako` に元と同じ行の JSON を渡したものと一致することを固定する。

use chrono::NaiveDate;
use kintai_dtako::day::{body, build_operations, DATE_INVALID};
use kintai_dtako::worktime::{aggregate, parse_dt};
use kintai_kosoku::sql::{ALL_EVENTS_SQL, EVENTS_SQL};
use kintai_logic::common::{mariadb_fail, Fail};
use kintai_logic::dtako_reads::{
    DtakoRead, Links, Request, DTAKO_BASE_URL_VAR, RYOHI_BASE_URL_VAR,
};
use kintai_logic::mariadb_reads::{DRIVER_MSG, MONTH_MSG};
use kintai_logic::mariadb_rows::{all_event_row, event_row, rows_to_json};
use kintai_mysql::bind::{names, Value as Bind};
use kintai_mysql::response::Row;
use serde_json::json;

fn row(cells: &[Option<&str>]) -> Row {
    cells
        .iter()
        .map(|c| c.map(|s| s.as_bytes().to_vec()))
        .collect()
}

fn bad(msg: &str) -> Fail {
    Fail::new(400, msg)
}

fn links() -> Links {
    Links {
        ryohi_base_url: "https://ryohi.example/".to_string(),
        dtako_base_url: String::new(),
    }
}

/// `EVENTS_SQL` の 7 列 (datetime, end_datetime, driver_id, source, state, unko_no, vehicle)。
fn events_rows() -> Vec<Row> {
    vec![
        row(&[
            Some("2026-06-05 07:00:00"),
            None,
            Some("1021"),
            Some("timecard"),
            Some("始業"),
            None,
            None,
        ]),
        row(&[
            Some("2026-06-05 07:53:30"),
            None,
            Some("1021"),
            Some("dtako"),
            Some("運行開始"),
            Some("26060507533000000042861"),
            None,
        ]),
        row(&[
            Some("2026-06-05 08:00:00"),
            Some("2026-06-05 09:10:00"),
            Some("1021"),
            Some("dtako_events"),
            Some("運転"),
            Some("26060507533000000042861"),
            Some("長崎100か4286"),
        ]),
    ]
}

/// `ALL_EVENTS_SQL` の 5 列 (datetime, end_datetime, driver_id, source, state)。
fn all_rows() -> Vec<Row> {
    vec![
        row(&[
            Some("2026-05-31 22:00:00"),
            Some("2026-06-01 04:00:00"),
            Some("1041"),
            Some("dtako_events"),
            Some("休息"),
        ]),
        row(&[
            Some("2026-06-02 10:00:00"),
            Some("2026-06-02 10:30:00"),
            Some("1368"),
            Some("dtako_events"),
            Some("積み"),
        ]),
        row(&[
            Some("2026-06-02 10:00:00"),
            Some("2026-06-02 10:30:00"),
            Some("1368"),
            Some("dtako_events"),
            Some("高速道"),
        ]),
    ]
}

// ── 経路 ──

#[test]
fn paths_and_names() {
    assert_eq!(
        DtakoRead::from_path("/api/kintai/day-events"),
        Some(DtakoRead::DayEvents)
    );
    assert_eq!(
        DtakoRead::from_path("/api/dtako/worktime"),
        Some(DtakoRead::Worktime)
    );
    assert_eq!(DtakoRead::from_path("/api/kintai/events"), None);
    assert_eq!(DtakoRead::DayEvents.as_str(), "day-events");
    assert_eq!(DtakoRead::Worktime.as_str(), "dtako-worktime");
    assert_eq!(RYOHI_BASE_URL_VAR, "KINTAI_RYOHI_BASE_URL");
    assert_eq!(DTAKO_BASE_URL_VAR, "KINTAI_DTAKO_BASE_URL");
}

// ── 検査 (元の handler と同じ順・同じ文言) ──

#[test]
fn day_events_checks_driver_then_date() {
    let p = |q: &str| DtakoRead::DayEvents.parse(q);
    assert_eq!(p("date=2026-06-05").unwrap_err(), bad(DRIVER_MSG));
    assert_eq!(
        p("driver=abc&date=2026-06-05").unwrap_err(),
        bad(DRIVER_MSG)
    );
    assert_eq!(p("driver=").unwrap_err(), bad(DRIVER_MSG));
    // driver が先: 両方だめなら driver の文言
    assert_eq!(p("date=bad").unwrap_err(), bad(DRIVER_MSG));
    assert_eq!(p("driver=1021").unwrap_err(), bad(DATE_INVALID));
    assert_eq!(
        p("driver=1021&date=2026-13-40").unwrap_err(),
        bad(DATE_INVALID)
    );
    assert_eq!(
        p("driver=1021&date=2026-02-30").unwrap_err(),
        bad(DATE_INVALID)
    );
    assert_eq!(
        p("driver=1021&date=2026-06-05&x=1").unwrap(),
        Request::DayEvents {
            driver: 1021,
            date: NaiveDate::from_ymd_opt(2026, 6, 5).unwrap(),
            from: "2026-06-05 00:00:00".to_string(),
            to: "2026-06-06 00:00:00".to_string(),
        },
        "知らない欄は読まない (axum の Query と同じ)"
    );
}

#[test]
fn worktime_checks_month_then_driver() {
    let p = |q: &str| DtakoRead::Worktime.parse(q);
    assert_eq!(p("").unwrap_err(), bad(MONTH_MSG));
    assert_eq!(p("month=2026-13").unwrap_err(), bad(MONTH_MSG));
    // month が先: 両方だめなら month の文言
    assert_eq!(p("month=x&driver=abc").unwrap_err(), bad(MONTH_MSG));
    assert_eq!(p("month=2026-06&driver=").unwrap_err(), bad(DRIVER_MSG));
    assert_eq!(p("month=2026-06&driver=abc").unwrap_err(), bad(DRIVER_MSG));
    assert_eq!(
        p("month=2026-06").unwrap(),
        Request::Worktime {
            month: "2026-06".to_string(),
            driver: None,
            from: "2026-06-01 00:00:00".to_string(),
            to: "2026-07-01 00:00:00".to_string(),
        }
    );
    assert_eq!(
        p("month=2026-12&driver=1041").unwrap(),
        Request::Worktime {
            month: "2026-12".to_string(),
            driver: Some(1041),
            from: "2026-12-01 00:00:00".to_string(),
            to: "2027-01-01 00:00:00".to_string(),
        }
    );
}

#[test]
fn duplicate_query_fields_are_axum_400() {
    for (read, q) in [
        (DtakoRead::DayEvents, "driver=1&driver=2&date=2026-06-05"),
        (DtakoRead::Worktime, "month=2026-06&month=2026-07"),
    ] {
        let err = read.parse(q).unwrap_err();
        assert_eq!(err.status, 400);
        assert!(
            err.body.starts_with("Failed to deserialize query string: "),
            "{}",
            err.body
        );
    }
}

// ── SQL と名前付き引数 ──

#[test]
fn sql_follows_the_original_repo_calls() {
    let day = DtakoRead::DayEvents
        .parse("driver=1021&date=2026-06-05")
        .unwrap();
    assert_eq!(day.sql(), EVENTS_SQL, "fetch_events_between");
    let one = DtakoRead::Worktime
        .parse("month=2026-06&driver=1041")
        .unwrap();
    assert_eq!(one.sql(), EVENTS_SQL, "fetch_events_between");
    let all = DtakoRead::Worktime.parse("month=2026-06").unwrap();
    assert_eq!(all.sql(), ALL_EVENTS_SQL, "fetch_all_events_between");
}

#[test]
fn binds_match_the_sql_names_exactly() {
    for (read, q) in [
        (DtakoRead::DayEvents, "driver=1021&date=2026-06-05"),
        (DtakoRead::Worktime, "month=2026-06&driver=1041"),
        (DtakoRead::Worktime, "month=2026-06"),
    ] {
        let req = read.parse(q).unwrap();
        let mut passed: Vec<&str> = req.binds().iter().map(|(n, _)| *n).collect();
        passed.sort();
        let mut used = names(req.sql()).unwrap();
        used.sort();
        assert_eq!(passed, used, "{q}");
        let sql = req.sql_text().unwrap();
        assert!(names(&sql).unwrap().is_empty());
    }
    let day = DtakoRead::DayEvents
        .parse("driver=1021&date=2026-06-05")
        .unwrap();
    assert!(day.binds().contains(&("driver", Bind::UInt(1021))));
    let sql = day.sql_text().unwrap();
    assert!(sql.contains("'2026-06-05 00:00:00'") && sql.contains("'2026-06-06 00:00:00'"));
    let all = DtakoRead::Worktime.parse("month=2026-06").unwrap();
    assert!(all.sql_text().unwrap().contains("'2026-07-01 00:00:00'"));
}

// ── 応答 (元と同じ行の JSON を kintai-dtako に渡したものと一致) ──

#[test]
fn day_events_body_matches_the_shared_crate() {
    let req = DtakoRead::DayEvents
        .parse("driver=1021&date=2026-06-05")
        .unwrap();
    let rows = events_rows();
    let got = req.respond(&rows, &links()).unwrap();
    let json_rows = rows_to_json(&rows, event_row).unwrap();
    let ops = build_operations(&json_rows, "https://ryohi.example/", "");
    let date = NaiveDate::from_ymd_opt(2026, 6, 5).unwrap();
    assert_eq!(got, body(1021, date, ops, json_rows));
    assert_eq!(got["operations"].as_array().unwrap().len(), 1);
    assert_eq!(got["events"].as_array().unwrap().len(), 3);
    let op = &got["operations"][0];
    assert_eq!(
        op["links"]["ryohi"],
        json!("https://ryohi.example/ryohi-rows/view/26060507533000000042861")
    );
    assert_eq!(
        op["links"]["search"],
        json!(null),
        "空の base URL はリンクを省く"
    );
    assert_eq!(op["vehicle"], json!("長崎100か4286"));
    assert_eq!(op["zip_request"]["ope_no"], json!("2606050753300000004286"));
}

#[test]
fn day_events_with_no_rows_is_empty_arrays() {
    let req = DtakoRead::DayEvents
        .parse("driver=1021&date=2026-06-05")
        .unwrap();
    let got = req.respond(&[], &Links::default()).unwrap();
    assert_eq!(got["operations"], json!([]));
    assert_eq!(got["events"], json!([]));
    assert_eq!(got["driver_cd"], json!(1021));
    assert_eq!(got["date"], json!("2026-06-05"));
}

#[test]
fn worktime_for_one_driver_reads_the_7_column_rows() {
    let req = DtakoRead::Worktime
        .parse("month=2026-06&driver=1021")
        .unwrap();
    let rows = events_rows();
    let got = req.respond(&rows, &Links::default()).unwrap();
    let (f, t) = ("2026-06-01 00:00:00", "2026-07-01 00:00:00");
    let json_rows = rows_to_json(&rows, event_row).unwrap();
    let want = aggregate(&json_rows, parse_dt(f).unwrap(), parse_dt(t).unwrap()).to_json(
        "2026-06",
        Some(1021),
        f,
        t,
    );
    assert_eq!(got, want);
    assert_eq!(got["days"][0]["seconds_by_state"]["運転"], json!(4200));
    assert_eq!(got["ignored_rows"]["other_source"], json!(2));
}

#[test]
fn worktime_for_all_drivers_reads_the_5_column_rows() {
    let req = DtakoRead::Worktime.parse("month=2026-06").unwrap();
    let rows = all_rows();
    let got = req.respond(&rows, &Links::default()).unwrap();
    let (f, t) = ("2026-06-01 00:00:00", "2026-07-01 00:00:00");
    let json_rows = rows_to_json(&rows, all_event_row).unwrap();
    let want = aggregate(&json_rows, parse_dt(f).unwrap(), parse_dt(t).unwrap())
        .to_json("2026-06", None, f, t);
    assert_eq!(got, want);
    assert_eq!(got["driver"], json!(null));
    assert_eq!(got["days"].as_array().unwrap().len(), 2);
    assert_eq!(got["clipped_outside_window_seconds"], json!(7200));
    assert_eq!(got["ignored_rows"]["layer_b"], json!(1));
}

#[test]
fn an_unreadable_row_fails_the_whole_response() {
    // 7 列の SQL に 5 列の行 (列数違い) / String の列に NULL
    let short = vec![all_rows().remove(0)];
    let null_source = vec![row(&[
        Some("2026-06-05 08:00:00"),
        None,
        Some("1021"),
        None,
        Some("運転"),
        None,
        None,
    ])];
    let day = DtakoRead::DayEvents
        .parse("driver=1021&date=2026-06-05")
        .unwrap();
    assert_eq!(
        day.respond(&short, &Links::default()).unwrap_err(),
        mariadb_fail("rows:shape")
    );
    let one = DtakoRead::Worktime
        .parse("month=2026-06&driver=1021")
        .unwrap();
    assert_eq!(
        one.respond(&null_source, &Links::default()).unwrap_err(),
        mariadb_fail("rows:null")
    );
    let all = DtakoRead::Worktime.parse("month=2026-06").unwrap();
    assert_eq!(
        all.respond(&events_rows(), &Links::default()).unwrap_err(),
        mariadb_fail("rows:shape")
    );
}
