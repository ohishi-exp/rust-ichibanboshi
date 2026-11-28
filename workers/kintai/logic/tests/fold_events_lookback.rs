//! 遡り起点の読み (`WorkerFoldEvents::alc_head_anchors`) を広い読み 1 回にしても、**起点も回ごとの行も変わらない**ことの
//! 前後比較 (#p676)。DB も RPC も要らない。
//!
//! alc の fake は上流 (alc-dtako の `collect_all`) の選び方をまねる: 「運行日か読取日が `[date_from−1, date_to+1]` に入る
//! 運行」を、乗務員CD の順のページ (50 人) で、乗務員ごとに出発・運行NO の順に返す。比べる相手は修正前の遡り (1 日ずつ
//! `all_events_between` で全部を読み直す) をテストの中に書いたもの。

use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use kintai_kosoku::alc_events::{fetch_all_events, RawEvent};
use kintai_kosoku::fold_flow::FoldEventsIo;
use kintai_kosoku::kosoku::alc_open_shift_starts;
use kintai_kosoku::pushed_events::all_event_json;
use kintai_kosoku::window::{window_holds, HeadPunch};
use kintai_logic::common::Fail;
use kintai_logic::fold_events::{
    wide_lookback_days, AlcRpc, PushedEventsRead, Span, WorkerFoldEvents,
    ALC_HEAD_LOOKBACK_MAX_DAYS, ALC_HEAD_WIDE_FROM_DAYS,
};
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

const FMT: &str = "%Y-%m-%d %H:%M:%S";

fn at(s: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, FMT).unwrap()
}

fn day(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

/// 上流の 1 運行 (dtako_operations の 1 行 + R2 の KUDGIVT.csv)。行は (対象乗務員CD, 開始, 終了, イベント名)。
struct Op {
    drivers: Vec<&'static str>,
    unko_no: &'static str,
    operation_date: &'static str,
    reading_date: &'static str,
    departure_at: &'static str,
    rows: Vec<[&'static str; 4]>,
}

/// alc の `GET /api/dtako/events` (全乗務員版) をまねる fake。読んだ期間を記録する。
struct Upstream {
    ops: Vec<Op>,
    reads: RefCell<Vec<(String, String)>>,
}

impl Upstream {
    fn new(ops: Vec<Op>) -> Self {
        Upstream {
            ops,
            reads: RefCell::default(),
        }
    }

    fn body(&self, search: &str) -> String {
        let q: BTreeMap<String, String> = form_urlencoded::parse(search.as_bytes())
            .into_owned()
            .collect();
        let (from, to) = (day(&q["date_from"]), day(&q["date_to"]));
        // 上流の widened (前後 1 日)
        let (from, to) = (from - Duration::days(1), to + Duration::days(1));
        let size: usize = q["page_size"].parse().unwrap();
        let after = q.get("after_driver_cd");
        let hit = |s: &str| (from..=to).contains(&day(s));
        let selected: Vec<&Op> = self
            .ops
            .iter()
            .filter(|o| hit(o.operation_date) || hit(o.reading_date))
            .collect();
        let mut cds: Vec<&str> = selected.iter().flat_map(|o| o.drivers.clone()).collect();
        cds.sort();
        cds.dedup();
        let page: Vec<&str> = cds
            .into_iter()
            .filter(|cd| after.is_none_or(|a| *cd > a.as_str()))
            .take(size)
            .collect();
        let next = (page.len() == size).then(|| page.last().unwrap().to_string());
        let groups: Vec<Value> = page
            .iter()
            .map(|cd| {
                let mut mine: Vec<&&Op> =
                    selected.iter().filter(|o| o.drivers.contains(cd)).collect();
                mine.sort_by_key(|o| (o.departure_at, o.unko_no));
                let operations: Vec<Value> = mine
                    .iter()
                    .map(|o| {
                        json!({
                            "unko_no": o.unko_no,
                            "headers": ["対象乗務員CD", "開始日時", "終了日時", "イベント名"],
                            "rows": o.rows,
                        })
                    })
                    .collect();
                json!({"driver": {"cd": cd, "name": ""}, "operations": operations})
            })
            .collect();
        json!({"drivers": groups, "next_after_driver_cd": next, "warnings": []}).to_string()
    }
}

impl AlcRpc for Upstream {
    async fn dtako_events(&self, search: &str) -> Result<RpcResult, Fail> {
        let q: BTreeMap<String, String> = form_urlencoded::parse(search.as_bytes())
            .into_owned()
            .collect();
        let range = (q["date_from"].clone(), q["date_to"].clone());
        self.reads.borrow_mut().push(range);
        Ok(RpcResult {
            status: 200,
            body: self.body(search),
        })
    }

    async fn dtako_etags(&self, _search: &str) -> Result<RpcResult, Fail> {
        unreachable!("遡りは etags を読まない")
    }
}

/// 写しの fake。期間に入る行だけを返す (打刻のある乗務員の行が混ざる形)。読んだ回数を数える。
#[derive(Default)]
struct Copy {
    rows: Vec<Value>,
    calls: Cell<usize>,
}

impl PushedEventsRead for Copy {
    async fn pushed_all_events(&self, (from, to): Span) -> Result<Vec<Value>, Fail> {
        self.calls.set(self.calls.get() + 1);
        let (from, to) = (from.naive_local(), to.naive_local());
        let inside = |r: &&Value| {
            let t = at(r["datetime"].as_str().unwrap());
            t >= from && t < to
        };
        Ok(self.rows.iter().filter(inside).cloned().collect())
    }

    async fn pushed_head_run_ends(&self, _span: Span) -> Result<Vec<(u64, String)>, Fail> {
        Ok(Vec::new())
    }

    async fn pushed_head_punches(&self, _span: Span) -> Result<Vec<HeadPunch>, Fail> {
        Ok(Vec::new())
    }

    async fn rest_start_overrides(&self, _span: Span) -> Result<Vec<RestStartOverride>, Fail> {
        Ok(Vec::new())
    }
}

const MONTH_START: &str = "2026-06-01 00:00:00";
const MONTH_TO: &str = "2026-07-02 00:00:00";

fn fmt(t: NaiveDateTime) -> String {
    t.format(FMT).to_string()
}

/// 修正前の遡り — 1 日ずつ `[月初−d, 月初+1)` を全部読み直す (比べる相手)。
fn old_lookback<A: AlcRpc, P: PushedEventsRead>(
    w: &WorkerFoldEvents<'_, A, P>,
) -> BTreeMap<u64, NaiveDateTime> {
    let month_start = at(MONTH_START);
    let to = fmt(month_start + Duration::days(1));
    let mut days = 1;
    loop {
        let from = month_start - Duration::days(days);
        let (rows, _) = block_on(w.all_events_between(&fmt(from), &to)).unwrap();
        let (open, edges) = alc_open_shift_starts(rows, month_start, from);
        if days >= ALC_HEAD_LOOKBACK_MAX_DAYS || edges.is_empty() {
            return open;
        }
        days += 1;
    }
}

fn worker<'a>(alc: &'a Upstream, copy: &'a Copy) -> WorkerFoldEvents<'a, Upstream, Copy> {
    WorkerFoldEvents {
        alc,
        pushed: copy,
        today: day("2026-10-11"),
        lookback_notes: Default::default(),
    }
}

/// 起点の比較 (alc の起点だけ。写しの起点は空)。
fn anchors_of(w: &WorkerFoldEvents<'_, Upstream, Copy>) -> BTreeMap<u64, String> {
    block_on(w.month_head_anchors(MONTH_START, MONTH_TO))
        .unwrap()
        .into_iter()
        .collect()
}

fn op(
    drivers: Vec<&'static str>,
    unko_no: &'static str,
    dates: (&'static str, &'static str),
    departure_at: &'static str,
    rows: Vec<[&'static str; 4]>,
) -> Op {
    Op {
        drivers,
        unko_no,
        operation_date: dates.0,
        reading_date: dates.1,
        departure_at,
        rows,
    }
}

/// 月初の前後の形を一通り入れた上流:
/// - 12: 月初をまたぐ運行 (起点が月初より前)
/// - 13: 5/25 出庫 → 5/31 帰庫の長い運行に休息が無く、勤務の途中から読んでいる回が続く (深い回まで遡る)
/// - 14・15: 2マン (同じ運行が 2 人に紐づく)。14 は同じ時刻に別の運行の行が並ぶ (並べ替えで順が効く)
/// - 16: 行が全部 alc なのに運行開始・終了が無い (運行の境界なし) — 遡りを上限まで引っ張る
/// - 17: 遡りの深いところにだけある運行 (浅い回の読みには入らない運行が、広い読みには入る)
/// - 30: 写しに打刻を持つ乗務員 (alc だけの乗務員ではない)
fn month_edge_ops() -> Vec<Op> {
    vec![
        op(
            vec!["12"],
            "260531200000000000001201",
            ("2026-05-31", "2026-06-01"),
            "2026-05-31T11:00:00Z",
            vec![
                [
                    "12",
                    "2026/05/31 20:00:00",
                    "2026/05/31 20:00:00",
                    "運行開始",
                ],
                ["12", "2026/05/31 20:00:00", "2026/06/01 03:00:00", "運転"],
                [
                    "12",
                    "2026/06/01 03:00:00",
                    "2026/06/01 03:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["13"],
            "260525060000000000001301",
            ("2026-05-25", "2026-05-31"),
            "2026-05-24T21:00:00Z",
            vec![
                [
                    "13",
                    "2026/05/25 06:00:00",
                    "2026/05/25 06:00:00",
                    "運行開始",
                ],
                ["13", "2026/05/25 06:00:00", "2026/05/31 22:00:00", "運転"],
                [
                    "13",
                    "2026/05/31 22:00:00",
                    "2026/05/31 22:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["13"],
            "260601010000000000001301",
            ("2026-06-01", "2026-06-01"),
            "2026-05-31T16:00:00Z",
            vec![
                [
                    "13",
                    "2026/06/01 01:00:00",
                    "2026/06/01 01:00:00",
                    "運行開始",
                ],
                ["13", "2026/06/01 01:00:00", "2026/06/01 09:00:00", "運転"],
                [
                    "13",
                    "2026/06/01 09:00:00",
                    "2026/06/01 09:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["14", "15"],
            "260530080000000000001401",
            ("2026-05-30", "2026-06-01"),
            "2026-05-29T23:00:00Z",
            vec![
                [
                    "14",
                    "2026/05/30 08:00:00",
                    "2026/05/30 08:00:00",
                    "運行開始",
                ],
                ["14", "2026/05/30 08:00:00", "2026/05/31 12:00:00", "運転"],
                [
                    "15",
                    "2026/05/30 08:00:00",
                    "2026/05/30 08:00:00",
                    "運行開始",
                ],
                ["15", "2026/05/30 08:00:00", "2026/06/01 02:00:00", "運転"],
                [
                    "14",
                    "2026/05/31 12:00:00",
                    "2026/05/31 12:00:00",
                    "運行終了",
                ],
                [
                    "15",
                    "2026/06/01 02:00:00",
                    "2026/06/01 02:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["14"],
            "260531120000000000001402",
            ("2026-05-31", "2026-06-01"),
            "2026-05-31T03:00:00Z",
            vec![
                [
                    "14",
                    "2026/05/31 12:00:00",
                    "2026/05/31 12:00:00",
                    "運行開始",
                ],
                ["14", "2026/05/31 12:00:00", "2026/06/01 04:00:00", "運転"],
                [
                    "14",
                    "2026/06/01 04:00:00",
                    "2026/06/01 04:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["16"],
            "260501000000000000001601",
            ("2026-05-01", "2026-06-02"),
            "2026-04-30T15:00:00Z",
            vec![
                ["16", "2026/05/01 00:00:00", "2026/06/01 06:00:00", "運転"],
                ["16", "2026/06/01 06:00:00", "2026/06/01 18:00:00", "休息"],
            ],
        ),
        op(
            vec!["17"],
            "260505080000000000001701",
            ("2026-05-05", "2026-05-06"),
            "2026-05-04T23:00:00Z",
            vec![
                [
                    "17",
                    "2026/05/05 08:00:00",
                    "2026/05/05 08:00:00",
                    "運行開始",
                ],
                ["17", "2026/05/05 08:00:00", "2026/05/06 03:00:00", "運転"],
                [
                    "17",
                    "2026/05/06 03:00:00",
                    "2026/05/06 03:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["17"],
            "260531230000000000001701",
            ("2026-05-31", "2026-06-01"),
            "2026-05-31T14:00:00Z",
            vec![
                [
                    "17",
                    "2026/05/31 23:00:00",
                    "2026/05/31 23:00:00",
                    "運行開始",
                ],
                ["17", "2026/05/31 23:00:00", "2026/06/01 05:00:00", "運転"],
                [
                    "17",
                    "2026/06/01 05:00:00",
                    "2026/06/01 05:00:00",
                    "運行終了",
                ],
            ],
        ),
        op(
            vec!["30"],
            "260531180000000000003001",
            ("2026-05-31", "2026-06-01"),
            "2026-05-31T09:00:00Z",
            vec![
                ["30", "2026/05/31 18:00:00", "2026/06/01 02:00:00", "運転"],
                ["30", "2026/06/01 02:00:00", "2026/06/01 10:00:00", "休息"],
            ],
        ),
    ]
}

fn copy_rows() -> Vec<Value> {
    let row = |t: &str, state: &str| all_event_json(t.into(), 30, "timecard".into(), state.into());
    vec![
        row("2026-05-31 17:30:00", "始業"),
        row("2026-06-01 02:30:00", "終業"),
    ]
}

/// 遡りを上限まで引っ張る上流で、**起点は修正前と同じ**。alc の読みは 31 回 → 浅い 2 回 + 広い 1 回 + 2 chunk の回だけになる。
#[test]
fn the_wide_read_gives_the_same_anchors_with_fewer_reads() {
    let (old_alc, new_alc) = (
        Upstream::new(month_edge_ops()),
        Upstream::new(month_edge_ops()),
    );
    let (old_copy, new_copy) = (
        Copy {
            rows: copy_rows(),
            ..Copy::default()
        },
        Copy {
            rows: copy_rows(),
            ..Copy::default()
        },
    );
    let want: BTreeMap<u64, String> = old_lookback(&worker(&old_alc, &old_copy))
        .into_iter()
        .map(|(cd, t)| (cd, fmt(t)))
        .collect();
    let w = worker(&new_alc, &new_copy);
    let got = anchors_of(&w);
    assert_eq!(got, want);
    // 月初をまたぐ勤務の始まりが起点に入っている (比較が空同士でないこと)
    assert_eq!(
        got.get(&12).map(String::as_str),
        Some("2026-05-31 20:00:00")
    );
    // 修正前は 1 回 1 往復で 31 回。修正後は 1・2 日目 + 広い読み (28 日) + 2 chunk に割れる 29〜31 日目 (各 2 往復)
    let wide = wide_lookback_days(at(MONTH_START));
    assert_eq!(wide, 28);
    assert_eq!(old_alc.reads.borrow().len(), 31 + 3);
    let deep = (ALC_HEAD_LOOKBACK_MAX_DAYS - wide) as usize;
    assert_eq!(new_alc.reads.borrow().len(), 2 + 1 + deep * 2);
    assert_eq!(
        new_alc.reads.borrow()[2],
        ("2026-05-02".into(), "2026-06-01".into())
    );
    // 写しは今までどおり回ごとに読む
    assert_eq!(new_copy.calls.get(), old_copy.calls.get());
    // 深い回まで遡った理由も同じく出る
    let note = w.lookback_notes.borrow();
    assert!(note[0]
        .text
        .starts_with("遡りの理由 (31 日): 16 運行の境界なし"));
}

/// 前提の確認: 広い読みを回の窓で絞った行は、その回を直接読んだ行と**並びまで**同じ (2マン・同時刻・深い運行を含む)。
#[test]
fn the_wide_read_narrowed_to_each_day_matches_the_direct_read() {
    let alc = Upstream::new(month_edge_ops());
    let month_start = at(MONTH_START);
    let to_at = month_start + Duration::days(1);
    let to = fmt(to_at);
    let wide = wide_lookback_days(month_start);
    let wide_from = fmt(month_start - Duration::days(wide));
    let (held, _) = block_on(fetch_all_events(
        &worker(&alc, &Copy::default()),
        &wide_from,
        &to,
    ))
    .unwrap();
    for days in ALC_HEAD_WIDE_FROM_DAYS..=wide {
        let from = month_start - Duration::days(days);
        let narrowed: Vec<RawEvent> = held
            .iter()
            .filter(|e| window_holds(e.start, e.end, from, to_at))
            .cloned()
            .collect();
        let (direct, _) = block_on(fetch_all_events(
            &worker(&alc, &Copy::default()),
            &fmt(from),
            &to,
        ))
        .unwrap();
        assert_eq!(narrowed, direct, "{days} 日目");
    }
}

/// 浅い回 (1・2 日目) で止まる会社は、広い読みを足さない (今までと同じ往復)。
#[test]
fn a_short_lookback_does_not_read_wide() {
    let ops = vec![op(
        vec!["12"],
        "260531200000000000001201",
        ("2026-05-31", "2026-06-01"),
        "2026-05-31T11:00:00Z",
        vec![
            [
                "12",
                "2026/05/31 20:00:00",
                "2026/05/31 20:00:00",
                "運行開始",
            ],
            ["12", "2026/05/31 20:00:00", "2026/06/01 03:00:00", "運転"],
            [
                "12",
                "2026/06/01 03:00:00",
                "2026/06/01 03:00:00",
                "運行終了",
            ],
        ],
    )];
    let alc = Upstream::new(ops);
    let copy = Copy::default();
    let got = anchors_of(&worker(&alc, &copy));
    assert_eq!(
        got.get(&12).map(String::as_str),
        Some("2026-05-31 20:00:00")
    );
    assert_eq!(
        *alc.reads.borrow(),
        vec![("2026-05-29".to_string(), "2026-06-01".to_string())]
    );
}

/// 広い読みで賄うのは 1 chunk に収まる回まで (月の長さに依らない)。
#[test]
fn the_wide_read_stops_where_the_upstream_range_splits() {
    for m in [
        "2026-03-01 00:00:00",
        "2026-02-01 00:00:00",
        "2026-12-01 00:00:00",
    ] {
        assert_eq!(wide_lookback_days(at(m)), 28, "{m}");
    }
}
