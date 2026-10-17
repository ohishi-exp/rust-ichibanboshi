//! 勤怠 Worker の fold の読み 3 本 (`kintai_logic::fold_events`) の単体テスト (DB も RPC も要らない。往復は fake)。
//!
//! 本番の Cloud Run 版 (root の `HttpKintaiEventsRepo` + fallback の `PgKintaiEventsRepo`) と同じ読み方であること — 読みの順番
//! (alc のページング → 送った写しの全員行、遡り起点は写しだけ、指紋は etags だけ)・写しに渡す期間 (JST の `TIMESTAMPTZ`)・
//! `dtakoEvents` / `dtakoEtags` に渡す query・404 / 非 2xx の扱い — を固定する。社内 MariaDB の往復は型に無い。
//! root と同じ写し・同じ上流の応答を与えた突き合わせは `workers/kintai/pg/tests/fold_events_parity.rs` (実 DB)。

use std::cell::RefCell;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use chrono::NaiveDate;
use kintai_kosoku::alc_events::{AlcEventsGet, EventsPage};
use kintai_kosoku::fold_flow::{month_anchors, BadMonth, DtakoMonthRead, FoldEventsIo};
use kintai_kosoku::pushed_events::all_event_json;
use kintai_kosoku::window::HeadPunch;
use kintai_logic::common::Fail;
use kintai_logic::fold_events::{
    driver_alc_events, events_search, pushed_window, read_events, AlcRpc, PushedEventsRead, Span,
    WorkerFoldEvents,
};
use kintai_logic::fold_routes::AlcEventsRequest;
use kintai_logic::rest_overrides::RestStartOverride;
use kintai_logic::unko_gaps::RpcResult;
use serde_json::{json, Value};

/// fake の往復は待たずに返すので、1 回 poll すれば終わる。
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fake の往復は待たない"),
    }
}

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn res(status: u16, body: &str) -> RpcResult {
    RpcResult {
        status,
        body: body.to_string(),
    }
}

/// 往復の記録 (どちらの口を・どの引数で・どの順に)。
type Log = RefCell<Vec<String>>;

/// RPC の fake。`events` は呼ばれた順に返す (尽きたら空の最終ページ)。
#[derive(Default)]
struct Alc<'a> {
    log: Option<&'a Log>,
    events: RefCell<Vec<Result<RpcResult, Fail>>>,
    etags: RefCell<Option<Result<RpcResult, Fail>>>,
}

impl AlcRpc for Alc<'_> {
    async fn dtako_events(&self, search: &str) -> Result<RpcResult, Fail> {
        self.log
            .unwrap()
            .borrow_mut()
            .push(format!("events?{search}"));
        let mut q = self.events.borrow_mut();
        match q.is_empty() {
            true => Ok(res(200, "{}")),
            false => q.remove(0),
        }
    }

    async fn dtako_etags(&self, search: &str) -> Result<RpcResult, Fail> {
        self.log
            .unwrap()
            .borrow_mut()
            .push(format!("etags?{search}"));
        self.etags.borrow_mut().take().expect("etags は 1 回")
    }
}

/// 1 回だけ返す戻り値。
type Once<T> = RefCell<Option<Result<T, Fail>>>;

/// 写しの読みの fake。呼ばれた口と期間を記録し、口ごとに 1 回だけ返す。
#[derive(Default)]
struct Pushed<'a> {
    log: Option<&'a Log>,
    all: Once<Vec<Value>>,
    runs: Once<Vec<(u64, String)>>,
    punches: Once<Vec<HeadPunch>>,
    overrides: RefCell<Vec<RestStartOverride>>,
}

impl Pushed<'_> {
    fn note(&self, what: &str, (from, to): Span) {
        let line = format!("{what} {}..{}", from.to_rfc3339(), to.to_rfc3339());
        self.log.unwrap().borrow_mut().push(line);
    }
}

impl PushedEventsRead for Pushed<'_> {
    async fn pushed_all_events(&self, span: Span) -> Result<Vec<Value>, Fail> {
        self.note("pushed all", span);
        // 1 回目は仕込んだ値、2 回目以降 (遡り起点の読み) は空
        self.all.borrow_mut().take().unwrap_or(Ok(Vec::new()))
    }

    async fn pushed_head_run_ends(&self, span: Span) -> Result<Vec<(u64, String)>, Fail> {
        self.note("pushed runs", span);
        self.runs.borrow_mut().take().expect("運行終了は 1 回")
    }

    async fn pushed_head_punches(&self, span: Span) -> Result<Vec<HeadPunch>, Fail> {
        self.note("pushed punches", span);
        self.punches.borrow_mut().take().expect("打刻の姿は 1 回")
    }

    async fn rest_start_overrides(&self, span: Span) -> Result<Vec<RestStartOverride>, Fail> {
        self.note("overrides", span);
        Ok(self.overrides.borrow().clone())
    }
}

fn pushed(log: &Log) -> Pushed<'_> {
    Pushed {
        log: Some(log),
        ..Pushed::default()
    }
}

fn pushed_all(log: &Log, all: Result<Vec<Value>, Fail>) -> Pushed<'_> {
    let p = pushed(log);
    *p.all.borrow_mut() = Some(all);
    p
}

fn alc(log: &Log) -> Alc<'_> {
    Alc {
        log: Some(log),
        ..Alc::default()
    }
}

/// 写しの全員行 (root の `PgKintaiEventsRepo` と同じ 5 キー)。
fn copy_row(at: &str, driver: i64, source: &str, state: &str) -> Value {
    all_event_json(at.into(), driver, source.into(), state.into())
}

const TODAY: NaiveDate = NaiveDate::from_ymd_opt(2026, 10, 11).unwrap();
const FROM: &str = "2026-06-01 00:00:00";
const TO: &str = "2026-07-02 00:00:00";
/// [`FROM`]・[`TO`] を JST の `TIMESTAMPTZ` にした期間 (写しに渡るもの)。
const SPAN: &str = "2026-06-01T00:00:00+09:00..2026-07-02T00:00:00+09:00";

fn events_body(driver: &str, start: &str, next: Option<&str>) -> String {
    json!({
        "drivers": [{"operations": [{
            "unko_no": format!("OP-{driver}"),
            "headers": ["対象乗務員CD", "開始日時", "イベント名"],
            "rows": [[driver, start, "運行開始"]],
        }]}],
        "next_after_driver_cd": next,
        "warnings": [format!("w{driver}")],
    })
    .to_string()
}

// ── 全乗務員の行 ──

/// **読みの順番**: alc のページング (chunk 1 の 2 ページ → chunk 2) を回しきってから写しの全員行を 1 本。
/// `dtakoEvents` の query は root の reqwest と同じ鍵・同じ順 (`after_driver_cd` は 2 ページ目から、値は urlencode)。
#[test]
fn all_events_reads_alc_pages_then_the_pushed_copy() {
    let log = Log::default();
    let a = alc(&log);
    *a.events.borrow_mut() = vec![
        Ok(res(
            200,
            &events_body("1101", "2026/06/01 08:00:00", Some("11 01")),
        )),
        Ok(res(200, &events_body("1102", "2026/06/02 08:00:00", None))),
        Ok(res(200, &events_body("1103", "2026/07/01 08:00:00", None))),
    ];
    let m = pushed_all(
        &log,
        Ok(vec![
            copy_row("2026-06-01 07:30:00", 1101, "timecard", "始業"),
            copy_row("2026-06-01 08:00:00", 1101, "dtako", "運行開始"),
        ]),
    );
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let (rows, warnings) = block_on(w.all_events_between(FROM, TO)).unwrap();
    assert_eq!(
        *log.borrow(),
        vec![
            "events?date_from=2026-05-30&date_to=2026-06-29&page_size=50".to_string(),
            "events?date_from=2026-05-30&date_to=2026-06-29&page_size=50&after_driver_cd=11+01"
                .to_string(),
            "events?date_from=2026-06-30&date_to=2026-07-01&page_size=50".to_string(),
            format!("pushed all {SPAN}"),
            format!("overrides {SPAN}"),
        ]
    );
    let got: Vec<(i64, &str, &str)> = rows
        .iter()
        .map(|r| {
            let (dt, src) = (
                r["datetime"].as_str().unwrap(),
                r["source"].as_str().unwrap(),
            );
            (r["driver_id"].as_i64().unwrap(), dt, src)
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (1101, "2026-06-01 07:30:00", "timecard"),
            (1101, "2026-06-01 08:00:00", "dtako"),
            (1101, "2026-06-01 08:00:00", "dtako_events"),
            (1102, "2026-06-02 08:00:00", "dtako_events"),
            (1103, "2026-07-01 08:00:00", "dtako_events"),
        ]
    );
    let texts: Vec<&str> = warnings.iter().map(|w| w.text.as_str()).collect();
    assert_eq!(texts, vec!["w1101", "w1102", "w1103"]);
}

/// **生イベントの非 2xx は 404 を含めて全部 502** (root と同じ)。失敗したら写しは読まない。
#[test]
fn all_events_fails_on_any_non_2xx_before_reading_the_copy() {
    for status in [404, 500, 302, 199] {
        let log = Log::default();
        let a = alc(&log);
        *a.events.borrow_mut() = vec![Ok(res(status, "nope"))];
        let m = pushed(&log);
        let w = WorkerFoldEvents {
            alc: &a,
            pushed: &m,
            today: TODAY,
        };
        let err = block_on(w.all_events_between(FROM, TO)).unwrap_err();
        let want = format!("alc dtako-events status {status}: nope");
        assert_eq!(err, Fail::new(502, want));
        assert_eq!(log.borrow().len(), 1, "写しは読まない");
    }
}

/// RPC そのものの失敗 (binding が無い 503 など) と写しの読みの失敗はそのまま返す。
#[test]
fn all_events_passes_rpc_and_copy_failures_through() {
    let log = Log::default();
    let a = alc(&log);
    *a.events.borrow_mut() = vec![Err(Fail::new(503, "no binding"))];
    let m = pushed(&log);
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    assert_eq!(
        block_on(w.all_events_between(FROM, TO)),
        Err(Fail::new(503, "no binding"))
    );

    let a = alc(&log);
    let boom = Fail::new(502, "kintai_events read failed: 57014");
    let m = pushed_all(&log, Err(boom.clone()));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    assert_eq!(block_on(w.all_events_between(FROM, TO)), Err(boom));
}

/// 期間が読めなければ往復の前に 502 (root の `bad range` と同じ文面)。壊れた本文は 502 (`parse`)。
#[test]
fn all_events_rejects_bad_windows_and_bodies() {
    let log = Log::default();
    let a = alc(&log);
    let m = pushed(&log);
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let err = block_on(w.all_events_between("bad", TO)).unwrap_err();
    assert_eq!(err.status, 502);
    assert!(err.body.starts_with("bad range \"bad\": "), "{err:?}");
    assert!(log.borrow().is_empty());

    let e = read_events(&res(200, "not json")).unwrap_err();
    assert_eq!(e.status, 502);
    assert!(e.body.starts_with("alc dtako-events parse: "), "{e:?}");
    // 本文は 200 字で切る
    let long = "x".repeat(300);
    let e = read_events(&res(500, &long)).unwrap_err();
    assert_eq!(
        e.body,
        format!("alc dtako-events status 500: {}", "x".repeat(200))
    );
    assert!(read_events(&res(204, "{}")).unwrap().drivers.is_empty());
}

/// 写しに渡す期間は JST の壁時計を +09:00 の `TIMESTAMPTZ` にしたもの (root の `jst_at`)。後ろが読めなくても 502。
#[test]
fn pushed_window_reads_both_ends_as_jst() {
    let (from, to) = pushed_window(FROM, TO).unwrap();
    assert_eq!(format!("{}..{}", from.to_rfc3339(), to.to_rfc3339()), SPAN);
    let err = pushed_window(FROM, "2026-07-02").unwrap_err();
    assert_eq!(err.status, 502);
    assert!(
        err.body.starts_with("bad range \"2026-07-02\": "),
        "{err:?}"
    );
}

#[test]
fn events_search_encodes_the_page_like_a_query_string() {
    let page = EventsPage {
        date_from: d(2026, 6, 1),
        date_to: d(2026, 6, 30),
        after_driver_cd: Some("a&b=c".into()),
    };
    let want = "date_from=2026-06-01&date_to=2026-06-30&page_size=50&after_driver_cd=a%26b%3Dc";
    assert_eq!(events_search(&page), want);
}

// ── 遡り起点 ──

/// 遡り起点は写しの 2 本 (運行終了 → 打刻の姿の順、窓は fold の `month_range` そのまま) と、写しを持たない乗務員の
/// alc の行 (月初の前後を読む)。規則は root と同じ `month_head_anchors` (運行NO の先頭 12 桁が月初より前なら、
/// その開始時刻まで遡る)。
#[test]
fn month_head_anchors_reads_the_two_head_queries_from_the_copy() {
    let log = Log::default();
    let a = alc(&log);
    let m = pushed(&log);
    *m.runs.borrow_mut() = Some(Ok(vec![(1194, "26053121394700000043241".into())]));
    *m.punches.borrow_mut() = Some(Ok(vec![]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let got = block_on(month_anchors(&w, "2026-06")).unwrap();
    assert_eq!(
        got.get(&1194).map(String::as_str),
        Some("2026-05-31 21:39:47")
    );
    assert_eq!(
        log.borrow()[..2],
        [
            format!("pushed runs {SPAN}"),
            format!("pushed punches {SPAN}")
        ]
    );
    // 続けて月初の前後 1 日 ([5/31, 6/2)) を alc と写しから読む (行が無いので alc の起点は無い)
    assert_eq!(
        log.borrow().last().map(String::as_str),
        Some("overrides 2026-05-31T00:00:00+09:00..2026-06-02T00:00:00+09:00")
    );

    // 月が壊れていれば読む前に落とす (fold の BadMonth → 502)
    let err = block_on(month_anchors(&w, "2026-13")).unwrap_err();
    assert_eq!(err, Fail::new(502, "bad month: 2026-13"));
    assert_eq!(Fail::from(BadMonth("x".into())).to_string(), "bad month: x");
}

#[test]
fn month_head_anchors_passes_failures_through() {
    let log = Log::default();
    let a = alc(&log);
    let w = |m| WorkerFoldEvents {
        alc: &a,
        pushed: m,
        today: TODAY,
    };
    let boom = Fail::new(502, "kintai_events read failed: closed");
    let m = pushed(&log);
    *m.runs.borrow_mut() = Some(Err(boom.clone()));
    assert_eq!(
        block_on(w(&m).month_head_anchors(FROM, TO)),
        Err(boom.clone())
    );
    let m = pushed(&log);
    *m.runs.borrow_mut() = Some(Ok(vec![]));
    *m.punches.borrow_mut() = Some(Err(boom.clone()));
    assert_eq!(block_on(w(&m).month_head_anchors(FROM, TO)), Err(boom));

    log.borrow_mut().clear();
    let m = pushed(&log);
    let err = block_on(w(&m).month_head_anchors("bad", TO)).unwrap_err();
    assert!(err.body.starts_with("bad range \"bad\": "), "{err:?}");
    let err = block_on(w(&m).month_head_anchors(FROM, "bad")).unwrap_err();
    assert!(err.body.starts_with("bad range \"bad\": "), "{err:?}");
    assert!(log.borrow().is_empty(), "期間が読めなければ写しは読まない");
}

/// 月初より前の打刻の姿からも遡る (始業で終わっていれば、その始業まで)。
#[test]
fn month_head_anchors_uses_the_punches_too() {
    let log = Log::default();
    let a = alc(&log);
    let m = pushed(&log);
    *m.runs.borrow_mut() = Some(Ok(vec![]));
    *m.punches.borrow_mut() = Some(Ok(vec![HeadPunch {
        driver: 1101,
        first_state: Some("終業".into()),
        last_start: Some("2026-05-31 20:00:00".into()),
        last_end: Some("2026-05-30 18:00:00".into()),
    }]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let got = block_on(w.month_head_anchors(FROM, TO)).unwrap();
    assert_eq!(
        got.get(&1101).map(String::as_str),
        Some("2026-05-31 20:00:00")
    );
}

/// alc の運行 1 本ぶんの応答 (開始・終了・イベント名)。
fn alc_rows_body(driver: &str, rows: &[(&str, &str, &str)]) -> String {
    let rows: Vec<Value> = rows
        .iter()
        .map(|(start, end, state)| json!([driver, start, end, state]))
        .collect();
    json!({
        "drivers": [{"operations": [{
            "unko_no": format!("OP-{driver}"),
            "headers": ["対象乗務員CD", "開始日時", "終了日時", "イベント名"],
            "rows": rows,
        }]}],
    })
    .to_string()
}

/// 写しを持たない乗務員は、**月初の時点で開いている勤務の始まり**まで遡る (alc の行で勤務を組む)。
/// 写しの起点より早ければそちらを使う。
#[test]
fn month_head_anchors_adds_the_open_shift_start_from_alc_rows() {
    let log = Log::default();
    let a = alc(&log);
    *a.events.borrow_mut() = vec![Ok(res(
        200,
        &alc_rows_body(
            "12",
            &[
                ("2026/05/31 20:00:00", "2026/05/31 20:00:00", "運行開始"),
                ("2026/05/31 20:00:00", "2026/06/01 03:00:00", "運転"),
                ("2026/06/01 03:00:00", "2026/06/01 03:00:00", "運行終了"),
            ],
        ),
    ))];
    let m = pushed(&log);
    // 写しの起点 (同じ乗務員、alc より遅い) は alc の起点に置き換わる
    *m.runs.borrow_mut() = Some(Ok(vec![(12, "26053122000000000000121".into())]));
    *m.punches.borrow_mut() = Some(Ok(vec![]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let got = block_on(w.month_head_anchors(FROM, TO)).unwrap();
    assert_eq!(
        got.get(&12).map(String::as_str),
        Some("2026-05-31 20:00:00")
    );
}

/// 最初の勤務が読んだ範囲の端から始まる (勤務の途中から読んでいる) なら、1 日ずつ遡って読み直す (日数はデータで決める)。
#[test]
fn month_head_anchors_reads_further_back_while_the_open_shift_starts_at_the_edge() {
    let log = Log::default();
    let a = alc(&log);
    // 1 日前 (5/31 00:00) からの読みでは、運転が 5/30 23:00 から続いていて勤務の途中から読んでいる
    let edge = alc_rows_body(
        "12",
        &[
            ("2026/05/30 23:00:00", "2026/05/31 21:00:00", "運転"),
            ("2026/05/31 21:00:00", "2026/06/01 03:00:00", "運転"),
            ("2026/06/01 03:00:00", "2026/06/01 03:00:00", "運行終了"),
        ],
    );
    // 2 日前からの読みで始業 (5/30 22:00 の運行開始) が見える。休息期間が無いので 24 時間後の 5/31 22:00 で日締めし、
    // 月初で開いている勤務は 5/31 22:00 始まり
    let earlier = alc_rows_body(
        "12",
        &[
            ("2026/05/30 22:00:00", "2026/05/30 22:00:00", "運行開始"),
            ("2026/05/30 22:00:00", "2026/05/31 21:00:00", "運転"),
            ("2026/05/31 21:00:00", "2026/06/01 03:00:00", "運転"),
            ("2026/06/01 03:00:00", "2026/06/01 03:00:00", "運行終了"),
        ],
    );
    *a.events.borrow_mut() = vec![Ok(res(200, &edge)), Ok(res(200, &earlier))];
    let m = pushed(&log);
    *m.runs.borrow_mut() = Some(Ok(vec![]));
    *m.punches.borrow_mut() = Some(Ok(vec![]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let got = block_on(w.month_head_anchors(FROM, TO)).unwrap();
    assert_eq!(
        got.get(&12).map(String::as_str),
        Some("2026-05-31 22:00:00")
    );
    assert_eq!(
        log.borrow().last().map(String::as_str),
        Some("overrides 2026-05-30T00:00:00+09:00..2026-06-02T00:00:00+09:00")
    );
}

// ── dtako の指紋 ──

/// etags だけを読む (写しも生イベントも読まない)。期間は `[min(since, 月初), 翌月初]`、指紋と warnings は共有 crate。
#[test]
fn dtako_month_read_reads_only_the_etags() {
    let log = Log::default();
    let a = alc(&log);
    let body = json!({
        "items": [{"unko_no": "26060108000000000011011", "etag": "e1", "driver_cds": ["1101"]}],
        "warnings": [], "unsplit": [], "unsplit_total": 0,
    });
    *a.etags.borrow_mut() = Some(Ok(res(200, &body.to_string())));
    let m = pushed(&log);
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let read = block_on(w.dtako_month_read_since("2026-06", Some(d(2026, 5, 20)))).unwrap();
    assert_eq!(
        *log.borrow(),
        vec!["etags?date_from=2026-05-20&date_to=2026-07-01"]
    );
    assert!(read.digest.is_some());
    assert!(read
        .etags
        .unwrap()
        .unko_nos
        .contains("26060108000000000011011"));
    // 1101 の末尾が 30 日空く → 末尾ギャップ (封は止めない)
    assert_eq!(read.warnings.len(), 1);
    assert!(!read.warnings[0].blocks_seal);
}

/// **etags は 404 だけ「口なし」** (指紋 `None` = 判定できない)。他の非 2xx と parse 失敗は 502 (root の fold と同じ)。
#[test]
fn dtako_month_read_degrades_only_on_404() {
    let read_with = |r: Result<RpcResult, Fail>| {
        let log = Log::default();
        let a = alc(&log);
        *a.etags.borrow_mut() = Some(r);
        let m = pushed(&log);
        let w = WorkerFoldEvents {
            alc: &a,
            pushed: &m,
            today: TODAY,
        };
        block_on(w.dtako_month_read_since("2026-06", None))
    };
    assert_eq!(
        read_with(Ok(res(404, ""))),
        Ok(DtakoMonthRead::digest_only(None))
    );
    let err = read_with(Ok(res(500, "boom"))).unwrap_err();
    assert_eq!(err, Fail::new(502, "alc dtako-etags status 500: boom"));
    let err = read_with(Ok(res(200, "not json"))).unwrap_err();
    assert!(err.body.starts_with("alc dtako-etags parse: "), "{err:?}");
    assert_eq!(
        read_with(Err(Fail::new(503, "no binding"))),
        Err(Fail::new(503, "no binding"))
    );
}

/// 壊れた月は往復の前に 502 (`bad month: …`)。
#[test]
fn dtako_month_read_rejects_a_broken_month_before_any_request() {
    let log = Log::default();
    let a = alc(&log);
    let m = pushed(&log);
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let err = block_on(w.dtako_month_read_since("nope", None)).unwrap_err();
    assert_eq!(err, Fail::new(502, "bad month: nope"));
    assert!(log.borrow().is_empty());
}

/// `AlcEventsGet` は単独でも使える (etags の期間をそのまま渡す)。
#[test]
fn alc_events_get_passes_the_dates_through() {
    let log = Log::default();
    let a = alc(&log);
    *a.etags.borrow_mut() = Some(Ok(res(404, "")));
    let m = pushed(&log);
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    assert!(block_on(w.etags(d(2026, 1, 2), d(2026, 1, 3)))
        .unwrap()
        .is_none());
    assert_eq!(
        *log.borrow(),
        vec!["etags?date_from=2026-01-02&date_to=2026-01-03"]
    );
}

#[test]
fn fail_displays_its_body() {
    assert_eq!(Fail::new(502, "boom").to_string(), "boom");
}

// ── 診断の口 (1 人ぶんの alc の行と勤務) ──

fn alc_events_req() -> AlcEventsRequest {
    AlcEventsRequest {
        driver: 12,
        from: FROM.into(),
        to: TO.into(),
    }
}

/// 名指しの乗務員の alc の行 (運行NO・車輌つき)・運行ごとの要約・fold と同じ組み方の勤務を返す。
/// 他の乗務員の行と、写しの他の乗務員の行は落とす。
#[test]
fn driver_alc_events_returns_the_driver_rows_operations_and_shifts() {
    let log = Log::default();
    let a = alc(&log);
    let body = json!({
        "drivers": [
            {"operations": [{
                "unko_no": "OP-12",
                "headers": ["対象乗務員CD", "開始日時", "終了日時", "イベント名", "車輌名"],
                "rows": [
                    ["12", "2026/06/02 05:00:00", "2026/06/02 05:00:00", "運行開始", "帯広100け200"],
                    ["12", "2026/06/02 05:00:00", "2026/06/02 08:00:00", "運転", "帯広100け200"],
                    ["12", "2026/06/02 08:00:00", "2026/06/02 08:00:00", "運行終了", "帯広100け200"],
                ],
            }]},
            {"operations": [{
                "unko_no": "OP-13",
                "headers": ["対象乗務員CD", "開始日時", "イベント名"],
                "rows": [["13", "2026/06/02 06:00:00", "運行開始"]],
            }]},
        ],
        "warnings": ["w"],
    });
    *a.events.borrow_mut() = vec![Ok(res(200, &body.to_string()))];
    let m = pushed_all(
        &log,
        Ok(vec![copy_row(
            "2026-06-02 06:00:00",
            13,
            "timecard",
            "始業",
        )]),
    );
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let got = block_on(driver_alc_events(&w, &alc_events_req())).unwrap();
    assert_eq!(got["driver"], 12);
    assert_eq!(
        (got["from"].as_str(), got["to"].as_str()),
        (Some(FROM), Some(TO))
    );
    assert_eq!(got["alc_only"], true);
    assert_eq!(
        got["shifts"],
        json!([{"start": "2026-06-02 05:00:00", "end": "2026-06-02 08:00:00", "minutes": 180}])
    );
    assert_eq!(
        got["operations"],
        json!([{
            "unko_no": "OP-12",
            "first": "2026-06-02 05:00:00",
            "last": "2026-06-02 08:00:00",
            "events": 3,
            "states": {"運行開始": 1, "運行終了": 1, "運転": 1},
            "vehicle": "帯広100け200",
        }])
    );
    assert_eq!(got["events"].as_array().unwrap().len(), 3);
    assert_eq!(got["events"][0]["unko_no"], "OP-12");
    assert_eq!(got["borrowed"], json!([]));
    assert_eq!(got["warnings"], json!(["w"]));
}

/// 行が 1 件も無ければ勤務も運行も空 (alc だけの乗務員でもない)。
#[test]
fn driver_alc_events_of_no_rows_is_empty() {
    let log = Log::default();
    let a = alc(&log);
    let m = pushed_all(&log, Ok(vec![]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let got = block_on(driver_alc_events(&w, &alc_events_req())).unwrap();
    assert_eq!(got["alc_only"], false);
    assert_eq!(got["shifts"], json!([]));
    assert_eq!(got["operations"], json!([]));
}

/// alc の読みと写しの読みの失敗はそのまま返す。
#[test]
fn driver_alc_events_passes_read_failures_through() {
    let log = Log::default();
    let a = alc(&log);
    *a.events.borrow_mut() = vec![Ok(res(500, "boom"))];
    let m = pushed(&log);
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    assert_eq!(
        block_on(driver_alc_events(&w, &alc_events_req()))
            .unwrap_err()
            .status,
        502
    );
    let a = alc(&log);
    let m = pushed_all(&log, Err(Fail::new(502, "kintai push db failed: x")));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    assert_eq!(
        block_on(driver_alc_events(&w, &alc_events_req()))
            .unwrap_err()
            .status,
        502
    );
    let bad = AlcEventsRequest {
        from: "x".into(),
        ..alc_events_req()
    };
    assert_eq!(
        block_on(driver_alc_events(&w, &bad)).unwrap_err().status,
        502
    );
}

/// 同じ乗務員の 2 台の運行が重なっていたら、全員の読みにも 1 人ぶんの読みにもエラーの warning が出る (封は止めない)。
#[test]
fn overlapping_operations_come_back_as_an_error_warning() {
    let body = json!({
        "drivers": [{"operations": [
            {
                "unko_no": "OP-245",
                "headers": ["対象乗務員CD", "開始日時", "終了日時", "イベント名", "車輌名"],
                "rows": [["15", "2026/06/03 03:07:33", "2026/06/06 12:02:12", "運転", "帯広100け245"]],
            },
            {
                "unko_no": "OP-226",
                "headers": ["対象乗務員CD", "開始日時", "終了日時", "イベント名", "車輌名"],
                "rows": [["15", "2026/06/03 03:26:45", "2026/06/03 16:55:46", "運転", "帯広100け226"]],
            },
        ]}],
    })
    .to_string();
    let log = Log::default();
    let a = alc(&log);
    *a.events.borrow_mut() = vec![Ok(res(200, &body))];
    let m = pushed_all(&log, Ok(vec![]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let (_, warnings) = block_on(FoldEventsIo::all_events_between(&w, FROM, TO)).unwrap();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0]
        .text
        .starts_with("エラー: 2 台の運行が同じ時間に重なっています — 乗務員 15 の OP-245"));
    assert!(!warnings[0].blocks_seal);

    let a = alc(&log);
    *a.events.borrow_mut() = vec![Ok(res(200, &body))];
    let m = pushed_all(&log, Ok(vec![]));
    let w = WorkerFoldEvents {
        alc: &a,
        pushed: &m,
        today: TODAY,
    };
    let req = AlcEventsRequest {
        driver: 15,
        from: FROM.into(),
        to: TO.into(),
    };
    let got = block_on(driver_alc_events(&w, &req)).unwrap();
    assert_eq!(got["warnings"].as_array().unwrap().len(), 1);
}

/// 休息の開始の補正があれば、fold の読みで `休息` の行の開始を最後の動きに差し替える (勤務の終わりが 1 分前の分になる)。
/// 北海 2026-06-17 乗務員 6 の形 (最後の動き 19:52:59 / 休息 19:53:00)。補正は応答にも載る。
#[test]
fn rest_start_overrides_move_the_rest_start_in_the_fold_read() {
    let body = json!({
        "drivers": [{"operations": [{
            "unko_no": "OP-12",
            "headers": ["対象乗務員CD", "開始日時", "終了日時", "イベント名"],
            "rows": [
                ["12", "2026/06/17 11:25:56", "2026/06/17 11:25:56", "運行開始"],
                ["12", "2026/06/17 11:25:56", "2026/06/17 19:53:00", "運転"],
                ["12", "2026/06/17 19:53:00", "2026/06/18 05:25:53", "休息"],
                ["12", "2026/06/18 05:25:53", "2026/06/18 07:00:00", "運転"],
                ["12", "2026/06/18 07:00:00", "2026/06/18 07:00:00", "運行終了"],
            ],
        }]}],
    })
    .to_string();
    let run = |with: bool| {
        let log = Log::default();
        let a = alc(&log);
        *a.events.borrow_mut() = vec![Ok(res(200, &body))];
        let m = pushed_all(&log, Ok(vec![]));
        if with {
            *m.overrides.borrow_mut() = vec![RestStartOverride {
                driver_cd: 12,
                rest_start: chrono::NaiveDate::from_ymd_opt(2026, 6, 17)
                    .unwrap()
                    .and_hms_opt(19, 53, 0)
                    .unwrap(),
                last_move: chrono::NaiveDate::from_ymd_opt(2026, 6, 17)
                    .unwrap()
                    .and_hms_opt(19, 52, 59)
                    .unwrap(),
                unko_no: "2606171125560000000237".into(),
            }];
        }
        let w = WorkerFoldEvents {
            alc: &a,
            pushed: &m,
            today: TODAY,
        };
        block_on(driver_alc_events(&w, &alc_events_req())).unwrap()
    };
    let before = run(false);
    assert_eq!(before["shifts"][0]["end"], "2026-06-17 19:53:00");
    assert_eq!(before["rest_start_overrides"], json!([]));
    let after = run(true);
    // 勤務の端は分に揃うので、1 分短くなる (theearth の管理表の 19:52 と同じ)
    assert_eq!(after["shifts"][0]["end"], "2026-06-17 19:52:00");
    assert_eq!(
        after["rest_start_overrides"],
        json!([{"driver_cd": 12, "rest_start_at": "2026-06-17 19:53:00", "last_move_at": "2026-06-17 19:52:59", "unko_no": "2606171125560000000237"}])
    );
}
