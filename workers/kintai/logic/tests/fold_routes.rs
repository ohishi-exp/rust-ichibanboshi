//! `kintai_logic::fold_routes` (fold の 4 口の殻: 経路・入力の検査・失敗の写し・時計・fold の設定) と、stale-months の
//! Query の読み (`stale_months::parse`) の検査 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 400 の文言は root の `src/routes/kintai_recalc.rs`・`kintai_timecard.rs` の `receive_window`・`stale_months.rs` と同じ。

use chrono::{Datelike, NaiveDate, Timelike};
use kintai_kosoku::fold_flow::FoldClock;
use kintai_kosoku::kintai_fold::logic_version as root_logic_version;
use kintai_kosoku::kosoku::KosokuParams;
use kintai_logic::common::{write_preflight, Fail, TenantInput};
use kintai_logic::fold_routes::{
    fold_ctx, jst_from_ms, kind_only, logic_version, not_configured, params,
    parse_alc_events_query, parse_recalc_body, parse_recalc_query, parse_window, read_failed,
    AlcEventsRequest, FoldRoute, DB_WHAT, STALE_DB_WHAT,
};
use kintai_logic::stale_months;
use kintai_logic::write_auth::{authorize, FORBIDDEN};

const RECALC: &str = "/api/kintai/recalc";
const WINDOW: &str = "/api/kintai/timecard/window";
const STALE: &str = "/api/kintai/stale-months";
const ALC_EVENTS: &str = "/api/kintai/alc-events";
const JSON: Option<&str> = Some("application/json");

// ── 経路と認可 ──

/// **同じ path で method が違う 2 本**: GET recalc は読みの口 (認可なし)、POST recalc は書き込みの口、PUT は 405。
#[test]
fn the_recalc_path_splits_by_method() {
    let get = FoldRoute::from_request("GET", RECALC);
    assert_eq!(get, Some(Some(FoldRoute::RecalcPreview)));
    assert!(!FoldRoute::RecalcPreview.is_write());
    let post = FoldRoute::from_request("POST", RECALC);
    assert_eq!(post, Some(Some(FoldRoute::Recalc)));
    assert!(FoldRoute::Recalc.is_write());
    for m in ["PUT", "DELETE", "PATCH", "HEAD"] {
        assert_eq!(FoldRoute::from_request(m, RECALC), Some(None), "{m}");
    }
}

#[test]
fn window_is_a_post_only_write_and_stale_months_a_get_only_read() {
    assert_eq!(
        FoldRoute::from_request("POST", WINDOW),
        Some(Some(FoldRoute::TimecardWindow))
    );
    assert!(FoldRoute::TimecardWindow.is_write());
    assert_eq!(FoldRoute::from_request("GET", WINDOW), Some(None));
    assert_eq!(
        FoldRoute::from_request("GET", STALE),
        Some(Some(FoldRoute::StaleMonths))
    );
    assert!(!FoldRoute::StaleMonths.is_write());
    assert_eq!(FoldRoute::from_request("POST", STALE), Some(None));
}

/// 診断の alc-events は GET だけの読みの口 (認可なし)。
#[test]
fn alc_events_is_a_get_only_read() {
    assert_eq!(
        FoldRoute::from_request("GET", ALC_EVENTS),
        Some(Some(FoldRoute::AlcEvents))
    );
    assert!(!FoldRoute::AlcEvents.is_write());
    assert_eq!(FoldRoute::from_request("POST", ALC_EVENTS), Some(None));
}

/// alc-events の Query: 日付は `YYYY-MM-DD`、`to` の日を含む (`[from, to+1)`)、62 日まで。
#[test]
fn the_alc_events_query_reads_dates_and_the_range() {
    assert_eq!(
        parse_alc_events_query("driver=12&from=2026-06-01&to=2026-06-24").unwrap(),
        AlcEventsRequest {
            driver: 12,
            from: "2026-06-01 00:00:00".into(),
            to: "2026-06-25 00:00:00".into(),
        }
    );
    // 1 日だけ・上限ちょうど (62 日 = from..from+61)
    assert!(parse_alc_events_query("driver=1&from=2026-06-01&to=2026-06-01").is_ok());
    assert!(parse_alc_events_query("driver=1&from=2026-06-01&to=2026-08-01").is_ok());
    let err = |q: &str| parse_alc_events_query(q).unwrap_err();
    let bad = err("driver=1&from=2026-06-01&to=2026-08-02");
    assert_eq!(
        (bad.status, bad.body.as_str()),
        (400, "期間は 62 日までにしてください")
    );
    let bad = err("driver=1&from=2026-06-02&to=2026-06-01");
    assert_eq!(
        (bad.status, bad.body.as_str()),
        (400, "from は to 以前にしてください")
    );
    let bad = err("driver=1&from=2026/06/01&to=2026-06-01");
    assert_eq!(
        (bad.status, bad.body.as_str()),
        (400, "from は YYYY-MM-DD で指定してください")
    );
    let bad = err("driver=1&from=2026-06-01&to=2026-06-31");
    assert_eq!(
        (bad.status, bad.body.as_str()),
        (400, "to は YYYY-MM-DD で指定してください")
    );
    let bad = err("from=2026-06-01&to=2026-06-01");
    assert_eq!(bad.status, 400);
    assert!(
        bad.body.starts_with("Failed to deserialize query string: "),
        "{}",
        bad.body
    );
}

/// 4 口以外の path は他の経路に任せる (`None`)。前方一致で拾わない。
#[test]
fn other_paths_are_not_fold_routes() {
    for p in [
        "/api/kintai/recalc/",
        "/api/kintai/timecard",
        "/api/kintai/timecard/windows",
        "/api/kintai/stale-month",
        "/probe",
    ] {
        assert_eq!(FoldRoute::from_request("GET", p), None, "{p}");
        assert_eq!(FoldRoute::from_request("POST", p), None, "{p}");
    }
}

#[test]
fn the_route_names_are_for_logs() {
    let names: Vec<&str> = [
        FoldRoute::RecalcPreview,
        FoldRoute::Recalc,
        FoldRoute::TimecardWindow,
        FoldRoute::StaleMonths,
        FoldRoute::AlcEvents,
    ]
    .iter()
    .map(|r| r.as_str())
    .collect();
    assert_eq!(
        names,
        [
            "recalc (preview)",
            "recalc",
            "timecard/window",
            "stale-months",
            "alc-events",
        ]
    );
}

/// 書き込みの口 (POST の 2 本) は token が無い・違えば 403 (Worker は `is_write` の口だけ `authorize` を通す)。
#[test]
fn write_routes_refuse_a_missing_or_wrong_token() {
    let forbidden = Err(Fail::new(403, FORBIDDEN));
    assert_eq!(authorize(Some("s3cret"), None), forbidden);
    assert_eq!(authorize(Some("s3cret"), Some("other")), forbidden);
    assert_eq!(authorize(Some("s3cret"), Some("s3cret")), Ok(()));
}

/// binding が無ければ 503 (root の書き先の store と同じ「書き先がありません」。stale-months も root は書き先の store を見る)。
#[test]
fn a_missing_binding_is_service_unavailable() {
    let f = write_preflight(false, &TenantInput::pinned(Some("pin"))).unwrap_err();
    assert_eq!(f.status, 503);
    assert_eq!(
        f.body,
        "[KINTAI_HYPERDRIVE] が無効です (書き先がありません)"
    );
}

// ── 入力の検査 (400 の文言は root と同じ) ──

#[test]
fn the_recalc_query_validates_the_month_like_root() {
    let bad = Fail::new(400, "month は YYYY-MM で指定してください");
    for q in ["", "month=", "month=2026-7", "month=2026-13", "month=nope"] {
        assert_eq!(parse_recalc_query(q).unwrap_err(), bad, "{q:?}");
    }
    let req =
        parse_recalc_query("month=2026-07&after_driver_cd=1130&max_drivers=50&stale_only=true")
            .unwrap();
    assert_eq!(req.month, "2026-07");
    assert_eq!(req.after_driver_cd, Some(1130));
    assert_eq!(req.max_drivers, Some(50));
    assert!(req.stale_only);
    // GET は apply を持たない (読みの口から書けない)
    assert!(!req.apply);
    let req = parse_recalc_query("month=2026-07&apply=true").unwrap();
    assert!(!req.apply);
}

#[test]
fn an_unreadable_recalc_query_is_axum_s_rejection() {
    let f = parse_recalc_query("month=2026-07&max_drivers=x").unwrap_err();
    assert_eq!(f.status, 400);
    assert!(
        f.body
            .starts_with("Failed to deserialize query string: max_drivers"),
        "{}",
        f.body
    );
}

#[test]
fn the_recalc_body_is_read_like_axum_json_then_the_month() {
    let ok = parse_recalc_body(
        JSON,
        br#"{"month":"2026-07","apply":true,"max_drivers":150}"#,
    )
    .unwrap();
    assert!(ok.apply);
    assert_eq!(ok.max_drivers, Some(150));
    assert_eq!(ok.today, None);
    let bad = parse_recalc_body(JSON, br#"{"month":"2026-7","apply":true}"#).unwrap_err();
    assert_eq!(bad, Fail::new(400, "month は YYYY-MM で指定してください"));
    assert_eq!(parse_recalc_body(None, b"{}").unwrap_err().status, 415);
    assert_eq!(parse_recalc_body(JSON, b"{").unwrap_err().status, 400);
    // month が無い = 型に合わない (axum の Json と同じ 422)
    assert_eq!(parse_recalc_body(JSON, b"{}").unwrap_err().status, 422);
    // HTTP の本文から「今日」は上書きできない (`#[serde(skip)]`)
    let skip = parse_recalc_body(JSON, br#"{"month":"2026-07","today":"2026-07-01"}"#).unwrap();
    assert_eq!(skip.today, None);
}

#[test]
fn the_window_body_checks_months_like_root() {
    let w = parse_window(
        JSON,
        br#"{"months":["2026-06","2026-07"],"drivers":[1130],"events":[]}"#,
    )
    .unwrap();
    assert_eq!(w.months, ["2026-06", "2026-07"]);
    assert!(w.fold, "既定は畳む");
    assert!(!w.dry_run);
    assert_eq!(
        parse_window(JSON, br#"{"months":[]}"#).unwrap_err(),
        Fail::new(400, "months が空です")
    );
    assert_eq!(
        parse_window(JSON, br#"{"months":["2026-06","2026-6","x"]}"#).unwrap_err(),
        Fail::new(400, "month は YYYY-MM です: 2026-6")
    );
    assert_eq!(
        parse_window(JSON, br#"{"drivers":[]}"#).unwrap_err().status,
        422
    );
}

#[test]
fn stale_months_reads_the_query_then_the_range_like_root() {
    let today = NaiveDate::from_ymd_opt(2026, 6, 17).unwrap();
    let ym = |y, m| NaiveDate::from_ymd_opt(y, m, 1).unwrap();
    assert_eq!(
        stale_months::parse("", today),
        Ok((ym(2025, 7), ym(2026, 7)))
    );
    assert_eq!(
        stale_months::parse("from=2026-01&to=2026-03", today),
        Ok((ym(2026, 1), ym(2026, 4)))
    );
    assert_eq!(
        stale_months::parse("to=2026-13", today),
        Err(Fail::new(400, "to は YYYY-MM で指定してください"))
    );
    assert_eq!(
        stale_months::parse("from=2026-04&to=2026-03", today),
        Err(Fail::new(400, "from は to 以前にしてください"))
    );
    let f = stale_months::parse("from=a&from=b", today).unwrap_err();
    assert_eq!(f.status, 400);
    assert!(
        f.body.starts_with("Failed to deserialize query string"),
        "{}",
        f.body
    );
}

// ── 失敗の写し (502 の本文に DB や上流の message を出さない) ──

/// 上流の本文の抜粋・serde の文言・宛先 (`.` を含む) は落とし、固定の頭と種別の語だけを残す。
#[test]
fn kind_only_drops_upstream_text() {
    let cases = [
        (
            r#"alc dtako-events status 500: {"error":"db.internal:5432 refused"}"#,
            "alc dtako-events status 500",
        ),
        (
            "alc dtako-events parse: expected value at line 1 column 1",
            "alc dtako-events parse",
        ),
        (
            "alc dtako-etags status 503: 10.0.0.5",
            "alc dtako-etags status 503",
        ),
        (
            "MariaDB query failed: connect:timeout",
            "MariaDB query failed: connect:timeout",
        ),
        (
            "MariaDB query failed: query:server:1146",
            "MariaDB query failed: query:server:1146",
        ),
        ("bad month: 2026-13", "bad month: 2026-13"),
        ("MariaDB 接続設定が未設定", "MariaDB 接続設定が未設定"),
        ("x: ", "x"),
        ("x: a/b", "x"),
    ];
    for (body, want) in cases {
        assert_eq!(kind_only(body), want, "{body}");
    }
    let long = format!("head: {}", "a".repeat(65));
    assert_eq!(kind_only(&long), "head");
    let edge = format!("head: {}", "a".repeat(64));
    assert_eq!(kind_only(&edge), edge);
}

/// fold の読みの失敗は root の `KintaiPushError::Read` と同じく **502** の `kintai events read failed: …`
/// (資格情報や binding が無い 503 の失敗も 502 にする。root の `map_push_err` と同じ)。
#[test]
fn read_failures_are_502_with_the_kind_only() {
    let upstream = Fail::new(
        502,
        r#"alc dtako-events status 500: {"message":"password authentication failed for user x"}"#,
    );
    let f = read_failed(&upstream);
    assert_eq!(f.status, 502);
    assert_eq!(
        f.body,
        "kintai events read failed: alc dtako-events status 500"
    );
    assert!(!f.body.contains("password"));
    let f = read_failed(&Fail::new(503, "MariaDB 接続設定が未設定"));
    assert_eq!(
        f,
        Fail::new(502, "kintai events read failed: MariaDB 接続設定が未設定")
    );
}

#[test]
fn flow_level_bad_months_are_503_like_root() {
    assert_eq!(
        not_configured("bad month: 2026-13"),
        Fail::new(503, "bad month: 2026-13")
    );
    assert_eq!(DB_WHAT, "kintai push db");
    assert_eq!(STALE_DB_WHAT, "kintai.day_summaries stale read");
}

// ── fold の設定と時計 ──

struct Fixed;

impl FoldClock for Fixed {
    fn now_jst(&self) -> chrono::DateTime<chrono::FixedOffset> {
        jst_from_ms(0)
    }
    fn monotonic_ms(&self) -> u64 {
        7
    }
}

/// **Worker の fold の ctx の版は本番 (root の Cloud Run 版) と同じ** (`KosokuParams` の既定値)。
#[test]
fn the_worker_fold_ctx_has_the_production_logic_version() {
    let ctx = fold_ctx("worker-sha", &Fixed);
    assert_eq!(root_logic_version(ctx.params), "15cd24a0775385e7");
    assert_eq!(logic_version(), "15cd24a0775385e7");
    assert_eq!(
        format!("{:?}", params()),
        format!("{:?}", KosokuParams::default())
    );
    assert_eq!(ctx.code_sha, "worker-sha");
    assert_eq!(ctx.clock.monotonic_ms(), 7);
}

#[test]
fn the_clock_is_jst_from_epoch_millis() {
    // 2026-06-30 15:00:00.250 UTC = 2026-07-01 00:00:00.250 JST
    let t = jst_from_ms(1_782_831_600_250);
    assert_eq!((t.year(), t.month(), t.day()), (2026, 7, 1));
    assert_eq!((t.hour(), t.minute(), t.second()), (0, 0, 0));
    assert_eq!(t.timestamp_subsec_millis(), 250);
    assert_eq!(t.offset().local_minus_utc(), 9 * 3600);
    assert_eq!(Fixed.now_jst().to_rfc3339(), "1970-01-01T09:00:00+09:00");
}

/// 休息の開始の補正: GET は読みの口 (認可なし)、PUT は書き込みの口、それ以外は 405。
#[test]
fn rest_start_overrides_is_a_get_read_and_a_put_write() {
    const P: &str = "/api/kintai/rest-start-overrides";
    let list = FoldRoute::from_request("GET", P);
    assert_eq!(list, Some(Some(FoldRoute::RestStartOverridesList)));
    assert!(!FoldRoute::RestStartOverridesList.is_write());
    let put = FoldRoute::from_request("PUT", P);
    assert_eq!(put, Some(Some(FoldRoute::RestStartOverridesPut)));
    assert!(FoldRoute::RestStartOverridesPut.is_write());
    assert_eq!(FoldRoute::from_request("POST", P), Some(None));
    assert_eq!(
        FoldRoute::RestStartOverridesList.as_str(),
        "rest-start-overrides (list)"
    );
    assert_eq!(
        FoldRoute::RestStartOverridesPut.as_str(),
        "rest-start-overrides (put)"
    );
}
