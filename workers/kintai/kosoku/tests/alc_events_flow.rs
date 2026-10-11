//! `kintai_kosoku::alc_events` の読みの流れ (`fetch_all_events`・`fetch_dtako_month_read`) の単体テスト (往復は fake)。
//!
//! root (reqwest) と勤怠 Worker (auth-worker の RPC) は往復 ([`AlcEventsGet`]) だけを実装し、ページングの順・指紋・
//! warnings の組み立てはここを通る。root と同じ上流の応答を与えた突き合わせは `workers/kintai/pg/tests/fold_events_parity.rs`。

use std::cell::RefCell;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use chrono::{NaiveDate, NaiveDateTime};
use kintai_kosoku::alc_events::{
    digest_from_pairs, fetch_all_events, fetch_dtako_month_read, merge_all_rows, AlcEventsError,
    AlcEventsGet, EtagPair, EventsPage, InputCoverage, RawEvent, UpstreamAll, UpstreamDriverGroup,
    UpstreamEtagItem, UpstreamEtags, UpstreamOperation, MAX_PAGES,
};
use kintai_kosoku::fold_flow::{DtakoMonthRead, UnsplitOperation};

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn dt(s: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
}

fn headers(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

fn row(list: &[&str]) -> Vec<String> {
    list.iter().map(|s| s.to_string()).collect()
}

// ── 読みの流れ (往復は fake) ──────────────────────────────────────────

/// fake の往復は待たずに返すので、1 回 poll すれば終わる。
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fake の往復は待たない"),
    }
}

#[derive(Debug, PartialEq)]
enum FakeError {
    Flow(String),
    Upstream(&'static str),
}

impl From<AlcEventsError> for FakeError {
    fn from(e: AlcEventsError) -> Self {
        Self::Flow(e.to_string())
    }
}

/// 受けた要求を記録し、ページを順に返す fake。
#[derive(Default)]
struct Fake {
    pages: RefCell<Vec<Result<UpstreamAll, FakeError>>>,
    seen_pages: RefCell<Vec<EventsPage>>,
    etags: RefCell<Option<Result<Option<UpstreamEtags>, FakeError>>>,
    seen_etags: RefCell<Vec<(NaiveDate, NaiveDate)>>,
    /// ページが尽きたら next を返し続ける (打ち切りの確認用)
    endless: bool,
}

impl AlcEventsGet for Fake {
    type Error = FakeError;

    async fn events_page(&self, page: &EventsPage) -> Result<UpstreamAll, FakeError> {
        self.seen_pages.borrow_mut().push(page.clone());
        let mut pages = self.pages.borrow_mut();
        if pages.is_empty() {
            let next = self.endless.then(|| "more".to_string());
            return Ok(all_page(vec![], next, vec![]));
        }
        pages.remove(0)
    }

    async fn etags(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Option<UpstreamEtags>, FakeError> {
        self.seen_etags.borrow_mut().push((date_from, date_to));
        self.etags
            .borrow_mut()
            .take()
            .expect("etags は 1 回だけ読む")
    }
}

fn op(unko_no: &str, driver: &str, start: &str, state: &str) -> UpstreamOperation {
    UpstreamOperation {
        unko_no: unko_no.to_string(),
        headers: headers(&["運行NO", "対象乗務員CD", "開始日時", "イベント名"]),
        rows: vec![row(&[unko_no, driver, start, state])],
    }
}

fn all_page(ops: Vec<UpstreamOperation>, next: Option<String>, warnings: Vec<&str>) -> UpstreamAll {
    UpstreamAll {
        drivers: vec![UpstreamDriverGroup { operations: ops }],
        next_after_driver_cd: next,
        warnings: warnings.into_iter().map(str::to_string).collect(),
    }
}

/// **31 日 chunk × keyset ページングの順番**: 1 か月 (+2 日の遡り) は 2 chunk に割れ、chunk ごとに
/// `after_driver_cd` を空から辿り直す。運行NO の重複 (2 名乗務) は 1 回だけ数え、warnings は並びのまま。
#[test]
fn fetch_all_events_walks_the_chunks_and_pages_in_order() {
    let fake = Fake::default();
    let first = op("OP-1", "1", "2026/07/02 08:00:00", "出庫");
    let dup = op("OP-1", "2", "2026/07/02 08:00:00", "出庫");
    let second = op("OP-2", "3", "2026/08/01 09:00:00", "休息");
    *fake.pages.borrow_mut() = vec![
        Ok(all_page(vec![first], Some("1".into()), vec!["w1"])),
        Ok(all_page(vec![dup], None, vec![])),
        Ok(all_page(vec![second], None, vec!["w2"])),
    ];
    let read = fetch_all_events(&fake, "2026-07-01 00:00:00", "2026-08-02 00:00:00");
    let (events, warnings) = block_on(read).unwrap();
    let pages = fake.seen_pages.borrow();
    let got: Vec<(String, String, Option<String>)> = pages
        .iter()
        .map(|p| {
            let (f, t) = (p.date_from.to_string(), p.date_to.to_string());
            (f, t, p.after_driver_cd.clone())
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("2026-06-29".into(), "2026-07-29".into(), None),
            ("2026-06-29".into(), "2026-07-29".into(), Some("1".into())),
            ("2026-07-30".into(), "2026-08-01".into(), None),
        ]
    );
    let ids: Vec<Option<i64>> = events.iter().map(|e| e.driver_id).collect();
    assert_eq!(ids, vec![Some(1), Some(3)], "OP-1 は 1 回だけ");
    let texts: Vec<&str> = warnings.iter().map(|w| w.text.as_str()).collect();
    assert_eq!(texts, vec!["w1", "w2"]);
}

#[test]
fn fetch_all_events_stops_on_paging_that_never_terminates() {
    let fake = Fake {
        endless: true,
        ..Fake::default()
    };
    let read = fetch_all_events(&fake, "2026-07-05 00:00:00", "2026-07-06 00:00:00");
    let want = FakeError::Flow("alc dtako-events paging did not terminate".into());
    assert_eq!(block_on(read).unwrap_err(), want);
    assert_eq!(fake.seen_pages.borrow().len(), MAX_PAGES);
}

/// 期間が読めなければ往復の前に落とす。往復の失敗はそのまま返す。
#[test]
fn fetch_all_events_fails_before_any_request_and_passes_upstream_errors_through() {
    let fake = Fake::default();
    let err = block_on(fetch_all_events(&fake, "bad", "2026-08-02 00:00:00")).unwrap_err();
    let want = "bad range \"bad\": ";
    assert!(
        matches!(&err, FakeError::Flow(m) if m.starts_with(want)),
        "{err:?}"
    );
    assert!(fake.seen_pages.borrow().is_empty());

    *fake.pages.borrow_mut() = vec![Err(FakeError::Upstream("status 404"))];
    let read = fetch_all_events(&fake, "2026-07-05 00:00:00", "2026-07-06 00:00:00");
    assert_eq!(
        block_on(read).unwrap_err(),
        FakeError::Upstream("status 404")
    );
}

#[test]
fn events_page_params_keep_the_root_order() {
    let mut page = EventsPage {
        date_from: d(2026, 6, 29),
        date_to: d(2026, 7, 29),
        after_driver_cd: None,
    };
    let pairs = |p: &EventsPage| -> Vec<String> {
        p.params()
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect()
    };
    let want = vec!["date_from=2026-06-29", "date_to=2026-07-29", "page_size=50"];
    assert_eq!(pairs(&page), want);
    page.after_driver_cd = Some("1130".into());
    assert_eq!(pairs(&page)[3], "after_driver_cd=1130");
}

/// 合流: alc の行に委譲先の行のうち `dtako_events` 以外を足して `driver_id, datetime, source` に並べる。
#[test]
fn merge_all_rows_borrows_only_what_alc_does_not_have() {
    let alc = vec![RawEvent {
        start: dt("2026-07-05 08:00:00"),
        end: None,
        driver_id: Some(1130),
        state: Some("出庫".into()),
        unko_no: Some("OP-1".into()),
        vehicle: None,
    }];
    let borrowed = vec![
        serde_json::json!({"datetime": "2026-07-05 07:00:00", "driver_id": 1130, "source": "timecard"}),
        serde_json::json!({"datetime": "2026-07-05 08:00:00", "driver_id": 1130, "source": "dtako_events"}),
    ];
    let rows = merge_all_rows(alc.clone(), Some(borrowed));
    let got: Vec<&str> = rows.iter().map(|r| r["source"].as_str().unwrap()).collect();
    assert_eq!(got, vec!["timecard", "dtako_events"]);
    assert_eq!(merge_all_rows(alc, None).len(), 1);
}

fn etags_body(items: Vec<(&str, Option<&str>, Vec<&str>)>) -> UpstreamEtags {
    UpstreamEtags {
        items: items
            .into_iter()
            .map(|(u, e, ds)| UpstreamEtagItem {
                unko_no: u.to_string(),
                etag: e.map(str::to_string),
                driver_cds: ds.into_iter().map(str::to_string).collect(),
            })
            .collect(),
        warnings: vec!["upstream".to_string()],
        unsplit: vec![UnsplitOperation {
            unko_no: "U9".into(),
            driver_cd: "9".into(),
            reading_date: "2026-07-02".into(),
        }],
        unsplit_total: 3,
    }
}

/// etags → 指紋・控え・warnings (上流の warning → 入力欠けの検知の順)。期間は `[min(since, 月初), 翌月初]`。
#[test]
fn fetch_dtako_month_read_builds_the_digest_and_the_warnings() {
    let fake = Fake::default();
    let body = etags_body(vec![
        ("26070110000000000023021", Some("e1"), vec!["D1"]),
        ("26073010000000000023022", None, vec![]),
    ]);
    let pairs: Vec<EtagPair> = body
        .items
        .iter()
        .map(|i| (i.unko_no.clone(), i.etag.clone(), i.driver_cds.clone()))
        .collect();
    *fake.etags.borrow_mut() = Some(Ok(Some(body)));
    let since = Some(d(2026, 6, 20));
    let read = block_on(fetch_dtako_month_read(
        &fake,
        "2026-07",
        since,
        d(2026, 8, 10),
    ));
    let read = read.unwrap();
    assert_eq!(
        *fake.seen_etags.borrow(),
        vec![(d(2026, 6, 20), d(2026, 8, 1))]
    );
    assert_eq!(read.digest, Some(digest_from_pairs(&pairs)));
    let etags = read.etags.unwrap();
    assert_eq!(etags.unsplit_total, 3);
    assert_eq!(etags.unsplit.len(), 1);
    assert!(etags.unko_nos.contains("26073010000000000023022"));
    let crew = &etags.driver_cds["26070110000000000023021"];
    assert_eq!(crew, &vec!["D1".to_string()]);
    let texts: Vec<&str> = read.warnings.iter().map(|w| w.text.as_str()).collect();
    assert_eq!(texts[0], "upstream");
    assert!(texts[1].contains("R2 に CSV の無い運行 1 件"), "{texts:?}");
    assert!(texts[2].contains("dtako 末尾ギャップ"), "{texts:?}");
    let blocks: Vec<bool> = read.warnings.iter().map(|w| w.blocks_seal).collect();
    assert_eq!(
        blocks,
        vec![true, true, false],
        "末尾ギャップだけ封を止めない"
    );

    // since が月初より後なら月初のまま (遡らない月)
    *fake.etags.borrow_mut() = Some(Ok(Some(etags_body(vec![]))));
    let since = Some(d(2026, 7, 5));
    block_on(fetch_dtako_month_read(
        &fake,
        "2026-07",
        since,
        d(2026, 8, 10),
    ))
    .unwrap();
    assert_eq!(fake.seen_etags.borrow()[1], (d(2026, 7, 1), d(2026, 8, 1)));
}

/// 404 (口なし) は「判定できない」: 指紋 `None`・控え無し・warnings 無し。壊れた月は往復の前に落とす。
#[test]
fn fetch_dtako_month_read_degrades_without_the_etags_endpoint() {
    let fake = Fake::default();
    *fake.etags.borrow_mut() = Some(Ok(None));
    let read = block_on(fetch_dtako_month_read(
        &fake,
        "2026-07",
        None,
        d(2026, 8, 10),
    ));
    assert_eq!(read.unwrap(), DtakoMonthRead::digest_only(None));

    let err = block_on(fetch_dtako_month_read(
        &fake,
        "2026-13",
        None,
        d(2026, 8, 10),
    ));
    assert_eq!(
        err.unwrap_err(),
        FakeError::Flow("bad month: 2026-13".into())
    );
    assert_eq!(fake.seen_etags.borrow().len(), 1);

    *fake.etags.borrow_mut() = Some(Err(FakeError::Upstream("status 500")));
    let err = block_on(fetch_dtako_month_read(
        &fake,
        "2026-07",
        None,
        d(2026, 8, 10),
    ));
    assert_eq!(err.unwrap_err(), FakeError::Upstream("status 500"));
}

#[test]
fn driver_cd_source_is_exposed() {
    let cov = InputCoverage::measure(&[], d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
    assert!(!cov.driver_cd_source());
}
