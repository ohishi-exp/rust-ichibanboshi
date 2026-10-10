//! 社内 MariaDB を読む残りの 4 本 (`kosoku_reads`: kosoku-daily・version・timecard/drivers・timecard/events) と、
//! そのための行 → 型 (`mariadb_rows` の 5 つ) の単体テスト (DB 不要)。
//!
//! 元 (root の handler・`kintai_repo.rs` / `kintai_version.rs` の行の型) と同じ検査・同じ文言・同じ null 扱いであること、
//! 全員版の書き出しが共有 crate の `write_all_drivers` (全行を一度に渡す版) と同じバイト列であることを固定する。

use kintai_kosoku::kintai_version::{version_etag, SourceMarker};
use kintai_kosoku::kosoku::KosokuParams;
use kintai_kosoku::kosoku_daily::{build_driver, write_all_drivers, ResponseView};
use kintai_kosoku::window::HeadPunch;
use kintai_logic::common::{mariadb_fail, mariadb_unconfigured, Fail};
use kintai_logic::kosoku_reads::{
    anchors_from, ferry_or_empty, head_sqls, parse_kosoku_daily, parse_timecard_drivers,
    parse_timecard_events, parse_version, timecard_fail, version_respond, version_sql,
    DailyRequest, HeadAnchors, KosokuRead, MAX_ALL_DRIVERS_BYTES, MONTHS_INVALID, TOO_LARGE,
};
use kintai_logic::mariadb_reads::{DRIVER_MSG, MONTH_MSG};
use kintai_logic::mariadb_rows::{
    all_event_row, event_row, ferry_row, head_punch_row, head_run_end_row, rows_to_json,
    timecard_driver_row, version_row,
};
use kintai_mysql::response::Row;
use serde_json::{json, Value};

fn row(cells: &[Option<&str>]) -> Row {
    cells
        .iter()
        .map(|c| c.map(|s| s.as_bytes().to_vec()))
        .collect()
}

fn bad(msg: &str) -> Fail {
    Fail::new(400, msg)
}

fn anchors(pairs: &[(u64, &str)]) -> HeadAnchors {
    pairs.iter().map(|(d, a)| (*d, a.to_string())).collect()
}

// ── 経路 ──

#[test]
fn paths_and_names() {
    for (path, read, name) in [
        (
            "/api/kintai/kosoku-daily",
            KosokuRead::KosokuDaily,
            "kosoku-daily",
        ),
        ("/api/kintai/version", KosokuRead::Version, "version"),
        (
            "/api/kintai/timecard/drivers",
            KosokuRead::TimecardDrivers,
            "timecard-drivers",
        ),
        (
            "/api/kintai/timecard/events",
            KosokuRead::TimecardEvents,
            "timecard-events",
        ),
    ] {
        assert_eq!(KosokuRead::from_path(path), Some(read));
        assert_eq!(read.as_str(), name);
    }
    // POST の 2 本 (diff・window) は移していない
    assert_eq!(KosokuRead::from_path("/api/kintai/timecard/diff"), None);
}

// ── 行 → 型 (元のタプルと同じ null 扱い) ──

#[test]
fn head_rows_follow_the_original_tuples() {
    let r = head_run_end_row(&row(&[Some("1194"), Some("26033121394700000043241")])).unwrap();
    assert_eq!(r, (1194, "26033121394700000043241".to_string()));
    // 負の CD は 0 に寄せる (元の d.max(0) as u64)
    assert_eq!(
        head_run_end_row(&row(&[Some("-3"), Some("u")])).unwrap().0,
        0
    );
    // i64・String の列の NULL / 整数でない値はエラー
    let null = Err(mariadb_fail("rows:null"));
    assert_eq!(head_run_end_row(&row(&[None, Some("u")])), null);
    assert_eq!(head_run_end_row(&row(&[Some("1"), None])), null);
    let not_int = Err(mariadb_fail("rows:int"));
    assert_eq!(head_run_end_row(&row(&[Some("x"), Some("u")])), not_int);

    let p = head_punch_row(&row(&[Some("1194"), Some("終業"), Some("a"), None])).unwrap();
    let want = HeadPunch {
        driver: 1194,
        first_state: Some("終業".into()),
        last_start: Some("a".into()),
        last_end: None,
    };
    assert_eq!(p, want);
    assert_eq!(
        head_punch_row(&row(&[None, None, None, None])),
        Err(mariadb_fail("rows:null"))
    );
    assert_eq!(
        head_punch_row(&row(&[Some("1")])),
        Err(mariadb_fail("rows:shape"))
    );
}

#[test]
fn ferry_version_and_driver_rows() {
    let f = ferry_row(&row(&[
        Some("2026-06-02 10:00:00"),
        Some("2026-06-02 11:00:00"),
        None,
    ]))
    .unwrap();
    assert_eq!(
        f,
        json!({"start_datetime": "2026-06-02 10:00:00", "end_datetime": "2026-06-02 11:00:00", "driver_id": null})
    );
    assert_eq!(
        ferry_row(&row(&[Some("a"), None, Some("1")])),
        Err(mariadb_fail("rows:null"))
    );

    let m = version_row(&row(&[Some("dtako_events"), Some("20"), Some("67890")])).unwrap();
    let want = SourceMarker {
        source: "dtako_events".into(),
        count: "20".into(),
        fingerprint: "67890".into(),
    };
    assert_eq!(m, want);
    assert_eq!(
        version_row(&row(&[Some("t"), None, Some("0")])),
        Err(mariadb_fail("rows:null"))
    );

    assert_eq!(timecard_driver_row(&row(&[Some("1018")])), Ok(1018));
    assert_eq!(
        timecard_driver_row(&row(&[None])),
        Err(mariadb_fail("rows:null"))
    );
    // 元は u64 に読むので負も数でない値もエラー
    assert_eq!(
        timecard_driver_row(&row(&[Some("-1")])),
        Err(mariadb_fail("rows:int"))
    );
}

// ── 遡り起点 ──

#[test]
fn head_sqls_bind_the_month_range() {
    let [runs, punches] = head_sqls("2026-04").unwrap();
    assert!(
        runs.contains("t.datetime >= '2026-04-01 00:00:00' AND t.datetime < '2026-05-02 00:00:00'")
    );
    assert!(punches.contains("b.datetime < '2026-04-01 00:00:00'"));
    assert!(!runs.contains(':') || !runs.contains(":from"));
    assert_eq!(head_sqls("2026-13"), Err(mariadb_fail("bad_month")));
}

#[test]
fn anchors_come_from_both_rows() {
    // 運行: 月初より前に始まった運行の運行終了 → その開始日時。打刻: 月初時点で開いた始業 → その始業
    let runs = [row(&[Some("1194"), Some("26033121394700000043241")])];
    let punches = [row(&[
        Some("1300"),
        Some("終業"),
        Some("2026-03-30 08:00:00"),
        Some("2026-03-29 17:00:00"),
    ])];
    let got = anchors_from("2026-04", &runs, &punches).unwrap();
    assert_eq!(
        got,
        anchors(&[(1194, "2026-03-31 21:39:47"), (1300, "2026-03-30 08:00:00")])
    );
    assert_eq!(
        anchors_from("2026-04", &[row(&[None, Some("u")])], &[]),
        Err(mariadb_fail("rows:null"))
    );
    assert_eq!(anchors_from("x", &[], &[]), Err(mariadb_fail("bad_month")));
}

// ── kosoku-daily の検査 ──

#[test]
fn kosoku_daily_checks_month_then_driver() {
    let p = parse_kosoku_daily;
    assert_eq!(p(""), Err(bad(MONTH_MSG)));
    assert_eq!(p("month=2026-13&driver=x"), Err(bad(MONTH_MSG)));
    assert_eq!(p("month=2026-06&driver="), Err(bad(DRIVER_MSG)));
    assert_eq!(p("month=2026-06&driver=-1"), Err(bad(DRIVER_MSG)));
    let dup = p("month=2026-06&month=2026-07").unwrap_err();
    assert_eq!(dup.status, 400);
    assert!(
        dup.body.starts_with("Failed to deserialize query string: "),
        "{}",
        dup.body
    );

    let all = p("month=2026-06").unwrap();
    assert_eq!(
        all,
        DailyRequest {
            month: "2026-06".into(),
            driver: None,
            view: ResponseView::Full
        }
    );
    let one = p("month=2026-06&driver=1442&view=compare").unwrap();
    assert_eq!((one.driver, one.view), (Some(1442), ResponseView::Compare));
    // 未知の view は全項目
    assert_eq!(p("month=2026-06&view=x").unwrap().view, ResponseView::Full);
}

#[test]
fn kosoku_daily_sql_follows_the_anchor() {
    let a = anchors(&[(1194, "2026-05-31 21:36:28"), (1300, "2026-05-30 08:00:00")]);
    let one = parse_kosoku_daily("month=2026-06&driver=1194").unwrap();
    let sql = one.events_sql(&a).unwrap();
    assert!(
        sql.contains("'2026-05-31 21:36:28'"),
        "単一版はその乗務員の起点から"
    );
    assert!(sql.contains("'2026-07-02 00:00:00'"));
    assert!(sql.contains("1194"));
    let all = parse_kosoku_daily("month=2026-06").unwrap();
    let sql = all.events_sql(&a).unwrap();
    assert!(
        sql.contains("'2026-05-30 08:00:00'"),
        "全員版は最小の起点から"
    );
    assert!(!sql.contains("dtako_cars"), "全員版は ALL_EVENTS_SQL");

    // フェリーは月ちょうど。全員版の driver は NULL
    let f = one.ferry_sql().unwrap();
    assert!(f.contains("'2026-06-01 00:00:00'") && f.contains("'2026-07-01 00:00:00'"));
    assert!(f.contains("(1194 IS NULL"));
    assert!(all.ferry_sql().unwrap().contains("(NULL IS NULL"));

    // 検査を通らない月は (到達しないが) 502
    let broken = DailyRequest {
        month: "x".into(),
        driver: None,
        view: ResponseView::Full,
    };
    assert_eq!(broken.events_sql(&a), Err(mariadb_fail("bad_month")));
    assert_eq!(broken.ferry_sql(), Err(mariadb_fail("bad_month")));
}

// ── kosoku-daily の応答 ──

fn ev7(driver: &str, at: &str, end: Option<&str>, source: &str, state: &str) -> Row {
    row(&[
        Some(at),
        end,
        Some(driver),
        Some(source),
        Some(state),
        None,
        None,
    ])
}

fn ev5(driver: Option<&str>, at: &str, end: Option<&str>, source: &str, state: &str) -> Row {
    row(&[Some(at), end, driver, Some(source), Some(state)])
}

#[test]
fn ferry_failures_become_no_ferry() {
    let ok = [row(&[
        Some("2026-06-02 10:00:00"),
        Some("2026-06-02 11:00:00"),
        Some("1"),
    ])];
    assert_eq!(ferry_or_empty(&ok).len(), 1);
    let broken = [ok[0].clone(), row(&[None, None, None])];
    assert!(ferry_or_empty(&broken).is_empty());
}

#[test]
fn single_driver_matches_the_shared_builder() {
    let events = vec![
        ev7("1442", "2026-06-02 06:00:00", None, "timecard", "始業"),
        ev7(
            "1442",
            "2026-06-02 10:00:00",
            Some("2026-06-02 11:00:00"),
            "dtako_events",
            "休憩",
        ),
        ev7(
            "1442",
            "2026-06-02 10:00:00",
            Some("2026-06-02 11:00:00"),
            "dtako_events",
            "休憩",
        ),
        ev7("1442", "2026-06-02 20:00:00", None, "timecard", "終業"),
    ];
    let ferry = vec![
        json!({"start_datetime": "2026-06-02 12:00:00", "end_datetime": "2026-06-02 12:30:00", "driver_id": 1442}),
    ];
    for view in ["", "&view=compare", "&view=timecard"] {
        let req = parse_kosoku_daily(&format!("month=2026-06&driver=1442{view}")).unwrap();
        let (got, days) = req.respond_single(1442, &events, &ferry).unwrap();
        let rows = rows_to_json(&events, event_row).unwrap();
        let built = build_driver(rows, &ferry, "2026-06", &KosokuParams::default(), req.view);
        assert_eq!(days, 1);
        assert_eq!(got, built.into_single("2026-06", 1442, req.view));
    }
    let req = parse_kosoku_daily("month=2026-06&driver=1442").unwrap();
    let broken = [row(&[None, None, None, None, None, None, None])];
    assert_eq!(
        req.respond_single(1442, &broken, &[]),
        Err(mariadb_fail("rows:null"))
    );
}

fn all_events() -> Vec<Row> {
    vec![
        ev5(
            Some("1119"),
            "2026-06-02 06:00:00",
            None,
            "timecard",
            "始業",
        ),
        ev5(
            Some("1018"),
            "2026-06-02 09:25:00",
            None,
            "timecard",
            "始業",
        ),
        ev5(
            Some("1018"),
            "2026-06-02 19:39:00",
            None,
            "timecard",
            "終業",
        ),
        ev5(
            Some("1119"),
            "2026-06-02 18:00:00",
            None,
            "timecard",
            "終業",
        ),
        ev5(Some("0"), "2026-06-02 07:00:00", None, "timecard", "始業"),
        ev5(Some("0"), "2026-06-02 17:00:00", None, "timecard", "終業"),
        ev5(None, "2026-06-02 08:00:00", None, "dtako", "運行開始"),
        ev5(Some("-5"), "2026-06-02 08:00:00", None, "dtako", "運行開始"),
        ev5(
            Some("1600"),
            "2026-06-02 08:00:00",
            None,
            "dtako",
            "運行開始",
        ),
        ev5(
            Some("1194"),
            "2026-05-31 21:36:28",
            None,
            "timecard",
            "始業",
        ),
        ev5(
            Some("1194"),
            "2026-06-01 09:00:00",
            None,
            "timecard",
            "終業",
        ),
        ev5(
            Some("1194"),
            "2026-06-03 08:00:00",
            None,
            "timecard",
            "始業",
        ),
        ev5(
            Some("1194"),
            "2026-06-03 17:00:00",
            None,
            "timecard",
            "終業",
        ),
        ev5(
            Some("1021"),
            "2026-06-04 18:00:00",
            Some("2026-06-05 05:00:00"),
            "dtako_events",
            "休息",
        ),
        ev5(
            Some("1021"),
            "2026-06-05 12:00:00",
            Some("2026-06-05 12:40:00"),
            "dtako_events",
            "休憩",
        ),
        ev5(
            Some("1021"),
            "2026-06-05 17:00:00",
            Some("2026-06-06 05:00:00"),
            "dtako_events",
            "休息",
        ),
    ]
}

fn all_ferry() -> Vec<Value> {
    vec![
        json!({"start_datetime": "2026-06-02 10:00:00", "end_datetime": "2026-06-02 11:00:00", "driver_id": 1119}),
        json!({"start_datetime": "2026-06-02 10:00:00", "end_datetime": "2026-06-02 10:30:00", "driver_id": 1018}),
        json!({"start_datetime": "2026-06-01 02:30:00", "end_datetime": "2026-06-01 04:00:00", "driver_id": 1194}),
        json!({"start_datetime": "2026-06-05 13:00:00", "end_datetime": "2026-06-05 13:30:00", "driver_id": 9999}),
    ]
}

/// 乗務員ごとに束ねて書いたものが、共有 crate の全行版 (`write_all_drivers`) と同じバイト列になる。
#[test]
fn all_drivers_written_per_driver_equal_the_shared_writer() {
    let a = anchors(&[(1194, "2026-05-31 21:36:28")]);
    for view in ["", "&view=compare", "&view=timecard"] {
        let req = parse_kosoku_daily(&format!("month=2026-06{view}")).unwrap();
        let (got, n) = req
            .write_all(&a, all_events(), all_ferry(), MAX_ALL_DRIVERS_BYTES)
            .unwrap();
        let rows = rows_to_json(&all_events(), all_event_row).unwrap();
        let mut want = Vec::new();
        let p = KosokuParams::default();
        let m =
            write_all_drivers(&mut want, rows, all_ferry(), "2026-06", &a, &p, req.view).unwrap();
        assert_eq!(n, m);
        assert_eq!(
            n, 4,
            "1018・1021・1119・1194 (0・負・NULL・勤務も打刻も無い 1600 は落とす)"
        );
        assert_eq!(
            String::from_utf8(got).unwrap(),
            String::from_utf8(want).unwrap()
        );
    }
    // 0 人
    let req = parse_kosoku_daily("month=2026-06").unwrap();
    let (got, n) = req
        .write_all(&a, vec![], vec![], MAX_ALL_DRIVERS_BYTES)
        .unwrap();
    assert_eq!(
        (got.as_slice(), n),
        (br#"{"drivers":[],"month":"2026-06"}"#.as_slice(), 0)
    );
}

#[test]
fn all_drivers_fail_whole_on_a_bad_row_or_a_large_body() {
    let a = HeadAnchors::new();
    let req = parse_kosoku_daily("month=2026-06").unwrap();
    // 落とす行 (driver NULL) でも読めなければ全体が 502 (元の exec と同じ)
    let mut bad_rows = all_events();
    bad_rows.push(ev5(None, "x", None, "dtako", "運行開始"));
    bad_rows.push(row(&[None, None, None, Some("dtako"), None]));
    assert_eq!(
        req.write_all(&a, bad_rows, vec![], MAX_ALL_DRIVERS_BYTES),
        Err(mariadb_fail("rows:null"))
    );
    let mut bad_int = all_events();
    bad_int.push(ev5(
        Some("x"),
        "2026-06-02 08:00:00",
        None,
        "dtako",
        "運行開始",
    ));
    assert_eq!(
        req.write_all(&a, bad_int, vec![], MAX_ALL_DRIVERS_BYTES),
        Err(mariadb_fail("rows:int"))
    );
    // 上限を超えたら途中までの本文を返さず 503 (頭・1 人目・閉じの各所で)
    let whole = req
        .write_all(&a, all_events(), vec![], MAX_ALL_DRIVERS_BYTES)
        .unwrap()
        .0
        .len();
    for max in [0, 20, whole - 12, whole - 1] {
        let got = req.write_all(&a, all_events(), vec![], max);
        assert_eq!(got, Err(Fail::new(503, TOO_LARGE)), "max={max}");
    }
}

// ── version ──

#[test]
fn version_checks_and_ranges() {
    assert_eq!(parse_version(""), Err(bad(MONTH_MSG)));
    assert_eq!(parse_version("month=2026-6"), Err(bad(MONTH_MSG)));
    assert_eq!(parse_version("month=2026-04&driver=1").unwrap(), "2026-04");
    assert_eq!(parse_version("month=a&month=b").unwrap_err().status, 400);

    let sql = version_sql("2026-04", &anchors(&[(1300, "2026-03-30 08:00:00")])).unwrap();
    // 打刻 2 表は起点から、dtako_events は始端の属する月の前月初から、フェリー・daily 系は月ちょうど
    assert!(
        sql.contains("d.datetime >= '2026-03-30 08:00:00' AND d.datetime < '2026-05-02 00:00:00'")
    );
    assert!(sql.contains("e.`開始日時` >= '2026-02-01 00:00:00'"));
    assert!(sql.contains(
        "f.`開始日時` >= '2026-04-01 00:00:00' AND f.`開始日時` < '2026-05-01 00:00:00'"
    ));
    assert_eq!(
        version_sql("x", &HeadAnchors::new()),
        Err(mariadb_fail("bad_month"))
    );
}

#[test]
fn version_etag_folds_the_markers_with_the_given_build() {
    let rows = [
        row(&[Some("time_card_dstate"), Some("10"), Some("123")]),
        row(&[Some("dtako_events"), Some("20"), Some("456")]),
    ];
    let (body, etag) = version_respond("2026-07", &rows, "abc").unwrap();
    let markers = rows_to_json(&rows, version_row).unwrap();
    assert_eq!(
        etag,
        version_etag("2026-07", "abc", &KosokuParams::default(), &markers)
    );
    assert_eq!(body, json!({"month": "2026-07", "etag": etag}));
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    // 版が違えば etag も違う
    assert_ne!(version_respond("2026-07", &rows, "abd").unwrap().1, etag);
    assert_eq!(
        version_respond("2026-07", &[row(&[Some("t")])], "abc"),
        Err(mariadb_fail("rows:shape"))
    );
}

// ── timecard ──

#[test]
fn timecard_failures_are_all_502() {
    let f = timecard_fail(mariadb_unconfigured());
    assert_eq!(
        (f.status, f.body.as_str()),
        (502, "kintai events read failed: MariaDB 接続設定が未設定")
    );
    let f = timecard_fail(mariadb_fail("connect:timeout"));
    assert_eq!(
        f.body,
        "kintai events read failed: MariaDB query failed: connect:timeout"
    );
}

#[test]
fn timecard_drivers_checks_and_pages() {
    let p = parse_timecard_drivers;
    assert_eq!(p(""), Err(bad(MONTH_MSG)));
    assert_eq!(p("month=2026-07&max_drivers=x").unwrap_err().status, 400);
    assert_eq!(
        p("month=2026-07&after_driver_cd=-1").unwrap_err().status,
        400
    );
    let req = p("month=2026-07").unwrap();
    assert_eq!((req.after, req.max), (None, 50));
    // 元 (usize 64 bit) が通す大きな値も通す (丸めは page_drivers)
    assert_eq!(
        p("month=2026-07&max_drivers=5000000000").unwrap().max,
        5_000_000_000usize
    );

    let sql = req.sql().unwrap();
    assert!(sql.contains("'2026-07-01 00:00:00'") && sql.contains("'2026-08-01 00:00:00'"));
    let broken = kintai_logic::kosoku_reads::DriversRequest {
        month: "x".into(),
        after: None,
        max: 1,
    };
    assert_eq!(broken.sql(), Err(bad("bad month: x")));

    let rows = [
        row(&[Some("1018")]),
        row(&[Some("1119")]),
        row(&[Some("1194")]),
    ];
    let req = p("month=2026-07&after_driver_cd=1018&max_drivers=1").unwrap();
    assert_eq!(
        req.respond(&rows, 7).unwrap().to_string(),
        r#"{"drivers":[1119],"elapsed_ms":7,"month":"2026-07","next_after_driver_cd":1119}"#
    );
    let got = req.respond(&[row(&[None])], 0).unwrap_err();
    assert_eq!(got, timecard_fail(mariadb_fail("rows:null")));
}

#[test]
fn timecard_events_checks_and_responds() {
    let p = parse_timecard_events;
    assert_eq!(
        p(""),
        Err(bad("months は YYYY-MM をカンマ区切りで指定してください"))
    );
    assert_eq!(
        p("months=2026-06,nope"),
        Err(bad("month は YYYY-MM です: nope"))
    );
    let req = p("months=2026-07,2026-06,2026-07").unwrap();
    assert_eq!(
        req.months,
        vec!["2026-06".to_string(), "2026-07".to_string()]
    );
    assert_eq!(
        (req.from.as_str(), req.to.as_str()),
        ("2026-06-01 00:00:00", "2026-08-01 00:00:00")
    );
    let sql = req.sql().unwrap();
    assert!(sql.contains("'2026-06-01 00:00:00'") && sql.contains("'2026-08-01 00:00:00'"));
    assert_eq!(MONTHS_INVALID, "months が不正です");

    let rows = [
        ev7("1200", "2026-07-01 08:00:00", None, "timecard", "始業"),
        ev7("1018", "2026-07-01 09:00:00", None, "timecard", "始業"),
    ];
    let v = req.respond(&rows, 3).unwrap();
    assert_eq!(v["drivers"], json!([1018, 1200]));
    assert_eq!(v["events"][0]["vehicle"], Value::Null);
    assert_eq!(v["elapsed_ms"], 3);
    let got = req.respond(&[row(&[None])], 0).unwrap_err();
    assert_eq!(got, timecard_fail(mariadb_fail("rows:shape")));
}
