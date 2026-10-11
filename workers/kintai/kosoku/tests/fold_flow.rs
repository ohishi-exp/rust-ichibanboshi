//! fold の I/O を含む流れ (`kintai_kosoku::fold_flow`) を fake の I/O で通す (Refs ohishi-exp/rust-ichibanboshi#322)。
//! DB も tokio も要らない。root の pg テスト (`tests/kintai_fold_pg_test.rs`) が同じ流れを本物の Postgres で通す。
//!
//! ここで固定するのは task_local をやめて値渡しにしたときに落としやすい規則:
//! 月ゲートの封の判定 (封を止める warning が gate 段・畳み段のどちらで立っても書かない / 診断だけなら書く)・
//! 上限と重複排除・突合を測るのは `measuring_unko_diff` の束だけ・etags は 1 回だけ引く・読めない gate は Unavailable。

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone};
use kintai_kosoku::anchors::HeadAnchors;
use kintai_kosoku::fold_flow::{
    fold_month, month_anchors, month_gate_report, month_gate_report_with_anchors,
    push_window_gap_warning, recalc_drivers, recalc_drivers_from_units, recalc_month,
    write_fold_gate_best_effort, BadMonth, DtakoMonthRead, EtagsRead, FoldClock, FoldCtx,
    FoldEventsIo, FoldGateRow, FoldSink, FoldStoreIo, UnsplitOperation, Warning, Warnings,
    MAX_COLLECTED_WARNINGS,
};
use kintai_kosoku::kintai_fold::{
    fold_driver_month, logic_version, FoldUnit, MonthGate, StoredState,
};
use kintai_kosoku::kosoku::KosokuParams;
use serde_json::{json, Value};

const SHA: &str = "0123456789abcdef";
const MONTH: &str = "2026-06";
const DRIVER: u64 = 1078;

/// fake の I/O は待たずに返すので、1 回 poll すれば終わる。
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fake の I/O は待たない"),
    }
}

// ── fake ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
struct Fail(String);

impl std::fmt::Display for Fail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<BadMonth> for Fail {
    fn from(e: BadMonth) -> Self {
        Fail(e.to_string())
    }
}

/// 生イベントの読み 3 本の fake。
#[derive(Default)]
struct Repo {
    anchors: HeadAnchors,
    anchors_fail: bool,
    rows: Vec<Value>,
    upstream: Vec<Warning>,
    rows_fail: bool,
    dtako: RefCell<Option<Result<DtakoMonthRead, Fail>>>,
    reads: Cell<usize>,
    etag_reads: Cell<usize>,
    since: RefCell<Vec<Option<NaiveDate>>>,
}

impl FoldEventsIo for Repo {
    type Error = Fail;

    async fn month_head_anchors(&self, _month_start: &str, _to: &str) -> Result<HeadAnchors, Fail> {
        match self.anchors_fail {
            true => Err(Fail("anchors down".into())),
            false => Ok(self.anchors.clone()),
        }
    }

    async fn all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<(Vec<Value>, Vec<Warning>), Fail> {
        self.reads.set(self.reads.get() + 1);
        match self.rows_fail {
            true => Err(Fail("events down".into())),
            false => Ok((self.rows.clone(), self.upstream.clone())),
        }
    }

    async fn dtako_month_read_since(
        &self,
        _month: &str,
        since: Option<NaiveDate>,
    ) -> Result<DtakoMonthRead, Fail> {
        self.etag_reads.set(self.etag_reads.get() + 1);
        self.since.borrow_mut().push(since);
        self.dtako
            .borrow()
            .clone()
            .unwrap_or_else(|| Ok(DtakoMonthRead::digest_only(None)))
    }
}

/// Supabase の読み書きの fake。
#[derive(Default)]
struct Store {
    stored: BTreeMap<i64, StoredState>,
    stored_fail: bool,
    written: RefCell<Vec<(String, i64, String, String)>>,
    write_fail: bool,
    gate: RefCell<Option<FoldGateRow>>,
    gate_read_fail: bool,
    gate_write_fail: bool,
    punch: Option<String>,
    operations: Option<Vec<(i64, String, NaiveDate, NaiveDate)>>,
    ever: Option<HashSet<i64>>,
}

impl FoldStoreIo for Store {
    type Error = Fail;

    async fn stored_states(
        &self,
        drivers: &[i64],
        _month: &str,
    ) -> Result<BTreeMap<i64, StoredState>, Fail> {
        match self.stored_fail {
            true => Err(Fail("stored down".into())),
            false => Ok(drivers
                .iter()
                .filter_map(|d| self.stored.get(d).map(|s| (*d, s.clone())))
                .collect()),
        }
    }

    async fn write_unit(
        &self,
        month: &str,
        unit: &FoldUnit,
        fingerprint: &str,
        logic_version: &str,
    ) -> Result<(), Fail> {
        if self.write_fail {
            return Err(Fail("write down".into()));
        }
        let row = (
            month.to_string(),
            unit.driver_cd,
            fingerprint.to_string(),
            logic_version.to_string(),
        );
        self.written.borrow_mut().push(row);
        Ok(())
    }

    async fn read_fold_gate(&self, _month: &str) -> Result<Option<FoldGateRow>, Fail> {
        match self.gate_read_fail {
            true => Err(Fail("gate read down".into())),
            false => Ok(self.gate.borrow().clone()),
        }
    }

    async fn write_fold_gate(&self, _month: &str, gate: &FoldGateRow) -> Result<(), Fail> {
        if self.gate_write_fail {
            return Err(Fail("gate write down".into()));
        }
        *self.gate.borrow_mut() = Some(gate.clone());
        Ok(())
    }

    async fn month_punch_digest(
        &self,
        _from: DateTime<FixedOffset>,
        _to: DateTime<FixedOffset>,
    ) -> Result<String, Fail> {
        self.punch.clone().ok_or_else(|| Fail("punch down".into()))
    }

    async fn month_operations(
        &self,
        _from: DateTime<FixedOffset>,
        _to: DateTime<FixedOffset>,
    ) -> Result<Vec<(i64, String, NaiveDate, NaiveDate)>, Fail> {
        self.operations
            .clone()
            .ok_or_else(|| Fail("operations down".into()))
    }

    async fn operation_driver_cds(&self) -> Result<HashSet<i64>, Fail> {
        self.ever.clone().ok_or_else(|| Fail("ever down".into()))
    }
}

/// 時計の fake (今日は 2026-07-15 の JST、単調時計は呼ぶたびに 5 ms 進む)。
struct Clock(AtomicU64);

impl FoldClock for Clock {
    fn now_jst(&self) -> DateTime<FixedOffset> {
        FixedOffset::east_opt(9 * 3600)
            .unwrap()
            .with_ymd_and_hms(2026, 7, 15, 9, 0, 0)
            .unwrap()
    }

    fn monotonic_ms(&self) -> u64 {
        self.0.fetch_add(5, Ordering::Relaxed)
    }
}

static CLOCK: Clock = Clock(AtomicU64::new(100));

fn params() -> KosokuParams {
    KosokuParams::default()
}

fn ctx(params: &KosokuParams) -> FoldCtx<'_> {
    FoldCtx {
        params,
        output_sha: SHA,
        clock: &CLOCK,
    }
}

fn punch(driver: u64, at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": driver, "source": "timecard", "state": state, "unko_no": null})
}

/// 6 月の 1 日ぶんの始業・終業と、翌月 1 日の打刻 (push 窓ずれを鳴らさない)。
fn june_rows() -> Vec<Value> {
    vec![
        punch(DRIVER, "2026-06-10 08:00:00", "始業"),
        punch(DRIVER, "2026-06-10 18:00:00", "終業"),
        punch(DRIVER, "2026-07-01 08:00:00", "始業"),
    ]
}

/// etags の控え付きの読み (指紋は `digest`)。
fn etags_read(digest: &str, warnings: Vec<Warning>) -> DtakoMonthRead {
    DtakoMonthRead {
        digest: Some(digest.to_string()),
        etags: Some(EtagsRead {
            unsplit: vec![UnsplitOperation {
                unko_no: "U9".into(),
                driver_cd: "1078".into(),
                reading_date: "2026-06-15".into(),
            }],
            unsplit_total: 3,
            unko_nos: HashSet::from(["2606101000000000002302".to_string()]),
            driver_cds: HashMap::from([(
                "2606101000000000002302".to_string(),
                vec!["1078".to_string()],
            )]),
        }),
        warnings,
    }
}

fn repo_with(read: DtakoMonthRead) -> Repo {
    Repo {
        rows: june_rows(),
        dtako: RefCell::new(Some(Ok(read))),
        ..Repo::default()
    }
}

fn store_with_punch() -> Store {
    Store {
        punch: Some("punch-digest".into()),
        operations: Some(Vec::new()),
        ever: Some(HashSet::new()),
        ..Store::default()
    }
}

fn texts(w: &Warnings) -> Vec<String> {
    w.texts()
}

// ── warnings の束 ───────────────────────────────────────────────────────────

/// **同じ文面は 1 回だけ、上限まで** (task_local の頃の `record_warning` と同じ規則)。
#[test]
fn warnings_are_deduped_and_capped_over_the_whole_bundle() {
    let mut w = Warnings::default();
    assert!(w.is_empty());
    w.extend(vec![Warning::blocking("same"); 3]);
    w.extend((0..MAX_COLLECTED_WARNINGS + 10).map(|i| Warning::diagnostic(format!("w{i}"))));
    assert_eq!(w.texts().iter().filter(|t| *t == "same").count(), 1);
    assert_eq!(w.items().len(), MAX_COLLECTED_WARNINGS);
    assert!(!w.is_empty());
}

/// **封を止める warning が 1 本でもあれば止める。診断専用だけなら止めない** (Refs #205-51)。
#[test]
fn only_a_blocking_warning_blocks_the_seal() {
    let mut w = Warnings::default();
    assert!(!w.blocks_seal(), "空は止めない");
    w.push(Warning::diagnostic("tail gap"));
    assert!(!w.blocks_seal(), "診断だけなら止めない");
    w.push(Warning::blocking("NoSuchKey"));
    assert!(w.blocks_seal());
    assert_eq!(
        w.texts(),
        vec!["tail gap".to_string(), "NoSuchKey".to_string()],
        "表示は両方"
    );
}

/// **上限で切られた後の封を止める warning は数えない** — 束に入ったものだけが判定に効く (task_local の頃と同じ)。
#[test]
fn a_blocking_warning_dropped_by_the_cap_does_not_count() {
    let mut w = Warnings::default();
    w.extend((0..MAX_COLLECTED_WARNINGS).map(|i| Warning::diagnostic(format!("d{i}"))));
    w.push(Warning::blocking("late"));
    assert!(!w.blocks_seal());
}

#[test]
fn bad_month_names_the_month() {
    assert_eq!(BadMonth("nope".into()).to_string(), "bad month: nope");
}

// ── push 窓ずれの検知 (Refs #205 の 30) ─────────────────────────────────────

fn ev(source: &str, at: &str) -> Value {
    json!({"datetime": at, "source": source, "state": "始業"})
}

fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
}

#[test]
fn push_window_gap_warns_when_the_spill_day_has_no_punch() {
    // 2026-06 の fold は 07-01 まで読むが、push が書くのは 06-30 まで。
    // 07-01 に打刻が 1 行も無い = 7 月が未 push の疑い (実測の 1021 の形)
    let rows = vec![ev("timecard", "2026-06-21 06:12:34")];
    let w = push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15));
    assert!(
        w.is_some_and(|w| w.contains("2026-07-01")),
        "07-01 を名指しする"
    );
}

#[test]
fn push_window_gap_is_quiet_when_the_spill_day_has_a_punch() {
    // 7 月が push 済み = はみ出した先に打刻が在る
    let rows = vec![
        ev("timecard", "2026-06-21 06:12:34"),
        ev("timecard", "2026-07-01 14:54:29"),
    ];
    assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15)).is_none());
}

#[test]
fn push_window_gap_is_quiet_while_the_spill_day_is_still_ahead() {
    // 当月を畳んでいる最中。はみ出した先が未来なので打刻が無いのが正常
    let rows = vec![ev("timecard", "2026-06-21 06:12:34")];
    assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 6, 25)).is_none());
    // 翌月初そのものの日もまだ鳴らさない (その日はまだ進行中)
    assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 1)).is_none());
}

#[test]
fn push_window_gap_ignores_dtako_events_in_the_spill_day() {
    // `dtako_events` は push を通らず alc から直に来るので push の証拠にならない
    let rows = vec![
        ev("timecard", "2026-06-21 06:12:34"),
        ev("dtako_events", "2026-07-01 09:00:00"),
    ];
    assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15)).is_some());
}

#[test]
fn push_window_gap_counts_the_other_pushed_source() {
    // `dtako` (運行開始/終了) も push が運ぶので、在れば push 済みの証拠になる
    let rows = vec![ev("dtako", "2026-07-01 09:00:00")];
    assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15)).is_none());
}

#[test]
fn push_window_gap_returns_none_for_a_bad_month() {
    assert!(push_window_gap_warning("nope", &[], ymd(2026, 7, 15)).is_none());
}

// ── 畳み ────────────────────────────────────────────────────────────────────

#[test]
fn month_anchors_refuses_a_bad_month_before_reading() {
    let repo = Repo::default();
    assert_eq!(
        block_on(month_anchors(&repo, "nope")),
        Err(Fail("bad month: nope".into()))
    );
    assert_eq!(
        block_on(month_anchors(&repo, MONTH)),
        Ok(HeadAnchors::new())
    );
}

/// 全乗務員を 1 回読みで畳み、上流の warnings と push 窓ずれを束に積む。
#[test]
fn fold_month_reads_once_and_collects_the_warnings() {
    let p = params();
    let repo = Repo {
        rows: vec![
            punch(DRIVER, "2026-06-10 08:00:00", "始業"),
            punch(DRIVER, "2026-06-10 18:00:00", "終業"),
        ],
        upstream: vec![Warning::blocking("NoSuchKey: U1")],
        ..Repo::default()
    };
    let mut w = Warnings::default();
    let units = block_on(fold_month::<_, Fail>(
        &repo,
        &ctx(&p),
        MONTH,
        None,
        None,
        &mut w,
    ))
    .unwrap();
    assert_eq!(repo.reads.get(), 1);
    assert_eq!(units.len(), 1);
    assert_eq!(units[0].0, DRIVER);
    let (unit, fp) = fold_driver_month(DRIVER as i64, MONTH, &p, repo.rows.clone(), SHA);
    assert_eq!((units[0].1.clone(), units[0].2.clone()), (unit, fp));
    let t = texts(&w);
    assert_eq!(t[0], "NoSuchKey: U1", "上流の warning が先");
    assert!(
        t[1].contains("push 窓ずれ"),
        "時計の今日 (07-15) で 07-01 が欠けている: {t:?}"
    );
    assert!(w.blocks_seal());
}

/// 固定した今日を渡せば push 窓ずれは時計を見ない (Refs #286-1)。
#[test]
fn fold_month_uses_the_given_today() {
    let p = params();
    let repo = Repo {
        rows: june_rows()[..2].to_vec(),
        ..Repo::default()
    };
    let mut w = Warnings::default();
    block_on(fold_month::<_, Fail>(
        &repo,
        &ctx(&p),
        MONTH,
        None,
        Some(ymd(2026, 6, 20)),
        &mut w,
    ))
    .unwrap();
    assert!(w.is_empty(), "{:?}", w);
}

/// `driver` を名指しすれば他の乗務員は落とし、行が無い乗務員にも空の単位を返す。
#[test]
fn fold_month_for_one_driver_returns_an_empty_unit_when_there_are_no_rows() {
    let p = params();
    let repo = Repo {
        rows: june_rows(),
        ..Repo::default()
    };
    let mut w = Warnings::default();
    let one = block_on(fold_month::<_, Fail>(
        &repo,
        &ctx(&p),
        MONTH,
        Some(DRIVER),
        None,
        &mut w,
    ))
    .unwrap();
    assert_eq!(one.len(), 1);
    let none = block_on(fold_month::<_, Fail>(
        &repo,
        &ctx(&p),
        MONTH,
        Some(4242),
        None,
        &mut w,
    ))
    .unwrap();
    assert_eq!(none.len(), 1);
    assert_eq!(none[0].0, 4242);
    assert!(none[0].1.is_empty());
}

/// 遡った乗務員は診断として 1 行ずつ出る (封は止めない)。起点が読めない行は 0 日前。
#[test]
fn fold_month_reports_the_lookback_as_diagnostics() {
    let p = params();
    let repo = Repo {
        rows: june_rows(),
        anchors: HeadAnchors::from([
            (DRIVER, "2026-05-30 21:00:00".to_string()),
            (9, "garbage".to_string()),
        ]),
        ..Repo::default()
    };
    let mut w = Warnings::default();
    block_on(fold_month::<_, Fail>(
        &repo,
        &ctx(&p),
        MONTH,
        None,
        None,
        &mut w,
    ))
    .unwrap();
    let t = texts(&w);
    assert!(t.iter().any(|x| x.contains("1078 を 2 日前")), "{t:?}");
    assert!(t.iter().any(|x| x.contains("9 を 0 日前")), "{t:?}");
    assert!(!w.blocks_seal(), "遡りは診断");
}

#[test]
fn fold_month_fails_on_a_bad_month_or_a_failed_read() {
    let p = params();
    let mut w = Warnings::default();
    let repo = Repo::default();
    let bad = block_on(fold_month::<_, Fail>(
        &repo,
        &ctx(&p),
        "nope",
        None,
        None,
        &mut w,
    ));
    assert_eq!(bad.unwrap_err(), Fail("bad month: nope".into()));
    let down = Repo {
        anchors_fail: true,
        ..Repo::default()
    };
    let e = block_on(fold_month::<_, Fail>(
        &down,
        &ctx(&p),
        MONTH,
        None,
        None,
        &mut w,
    ));
    assert_eq!(e.unwrap_err(), Fail("anchors down".into()));
    let down = Repo {
        rows_fail: true,
        ..Repo::default()
    };
    let e = block_on(fold_month::<_, Fail>(
        &down,
        &ctx(&p),
        MONTH,
        None,
        None,
        &mut w,
    ));
    assert_eq!(e.unwrap_err(), Fail("events down".into()));
}

// ── 月ゲート ────────────────────────────────────────────────────────────────

#[test]
fn the_gate_is_unavailable_without_an_etags_route() {
    let p = params();
    let repo = Repo::default();
    let store = store_with_punch();
    let mut sink = FoldSink::default();
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut sink,
    ))
    .unwrap();
    assert_eq!(gate, MonthGate::Unavailable);
    assert!(sink.unsplit.is_empty());
}

#[test]
fn the_gate_is_unavailable_when_the_etags_read_fails() {
    let p = params();
    let repo = Repo {
        dtako: RefCell::new(Some(Err(Fail("etags down".into())))),
        ..Repo::default()
    };
    let store = store_with_punch();
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut FoldSink::default(),
    ));
    assert_eq!(gate.unwrap(), MonthGate::Unavailable);
}

/// **打刻側 (Pg) が読めなくても `Err` にしない** (Refs #205 の 20)。
#[test]
fn the_gate_is_unavailable_when_the_punch_digest_fails() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = Store::default();
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut FoldSink::default(),
    ));
    assert_eq!(gate.unwrap(), MonthGate::Unavailable);
}

/// **`kintai.fold_gate` が読めなくても `Err` にしない** (Refs #205 の 20)。
#[test]
fn the_gate_is_unavailable_when_the_gate_row_cannot_be_read() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = Store {
        gate_read_fail: true,
        ..store_with_punch()
    };
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut FoldSink::default(),
    ));
    assert_eq!(gate.unwrap(), MonthGate::Unavailable);
}

/// 起点が引けなければ判定しない (安全側)。月が壊れていれば Err。
#[test]
fn the_gate_needs_the_anchors_and_a_valid_month() {
    let p = params();
    let repo = Repo {
        anchors_fail: true,
        ..repo_with(etags_read("d1", Vec::new()))
    };
    let store = store_with_punch();
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut FoldSink::default(),
    ));
    assert_eq!(gate.unwrap(), MonthGate::Unavailable);
    let bad = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        "nope",
        true,
        &mut FoldSink::default(),
    ));
    assert_eq!(bad.unwrap_err(), Fail("bad month: nope".into()));
}

/// Miss は判定した瞬間の指紋を持ち、Hit は版と時刻だけの報告を返す。etags の warnings と unsplit は束に積む。
#[test]
fn the_gate_misses_then_hits_on_the_same_digests() {
    let p = params();
    let repo = repo_with(etags_read("d1", vec![Warning::diagnostic("tail gap")]));
    let store = store_with_punch();
    let mut sink = FoldSink::default();
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut sink,
    ))
    .unwrap();
    let version = logic_version(&p, SHA);
    let miss = MonthGate::Miss {
        dtako_digest: "d1".into(),
        punch_digest: "punch-digest".into(),
        logic_version: version.clone(),
    };
    assert_eq!(gate, miss);
    assert_eq!(texts(&sink.warnings), vec!["tail gap".to_string()]);
    assert_eq!(sink.unsplit_total, 3);
    assert_eq!(sink.unsplit[0].unko_no, "U9");
    assert_eq!(sink.unko_diff.total, 0, "既定の束では突合しない");
    assert_eq!(repo.reads.get(), 0, "判定は畳みの読みをしない");

    *store.gate.borrow_mut() = Some(FoldGateRow {
        dtako_digest: "d1".into(),
        punch_digest: "punch-digest".into(),
        logic_version: version.clone(),
    });
    let hit = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        false,
        &mut FoldSink::default(),
    ))
    .unwrap();
    match hit {
        MonthGate::Hit(r) => {
            assert!(r.dry_run);
            assert_eq!(r.logic_version, version);
            assert_eq!(r.calculated_at, "2026-07-15T09:00:00+09:00");
        }
        other => panic!("hit のはず: {other:?}"),
    }
}

/// **突合を測るのは `measuring_unko_diff` の束だけ** (task_local の頃の `with_unko_diff_sink` の内側)。
/// etags は 1 回だけ引き、その運行一覧で押し込み済みと突き合わせる。突合の warning は封を止める。
#[test]
fn the_unko_diff_is_measured_only_for_a_measuring_sink() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = Store {
        operations: Some(vec![
            (
                1078,
                "26061010000000000023021".into(),
                ymd(2026, 6, 10),
                ymd(2026, 6, 10),
            ),
            (
                1517,
                "26061210000000000042291".into(),
                ymd(2026, 6, 12),
                ymd(2026, 6, 12),
            ),
        ]),
        ever: Some(HashSet::from([1078])),
        ..store_with_punch()
    };
    let mut sink = FoldSink::measuring_unko_diff();
    block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut sink,
    ))
    .unwrap();
    assert_eq!(repo.etag_reads.get(), 1, "etags は 1 回だけ");
    assert_eq!(sink.unko_diff.total, 1, "{:?}", sink.unko_diff);
    assert_eq!(sink.unko_diff.items[0].driver_cd, 1517);
    assert!(sink.warnings.blocks_seal(), "{:?}", sink.warnings);
}

/// 押し込み済みの運行が読めなければ突合を諦めて先へ進む (判定は変えない)。乗務員の履歴だけ読めなくても突合は出る。
#[test]
fn the_unko_diff_gives_up_quietly_when_the_store_cannot_be_read() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = Store {
        operations: None,
        ..store_with_punch()
    };
    let mut sink = FoldSink::measuring_unko_diff();
    let gate = block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut sink,
    ))
    .unwrap();
    assert!(matches!(gate, MonthGate::Miss { .. }));
    assert_eq!(sink.unko_diff.total, 0);
    assert!(sink.warnings.is_empty());

    let store = Store {
        operations: Some(vec![(
            1517,
            "26061210000000000042291".into(),
            ymd(2026, 6, 12),
            ymd(2026, 6, 12),
        )]),
        ever: None,
        ..store_with_punch()
    };
    let mut sink = FoldSink::measuring_unko_diff();
    block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut sink,
    ))
    .unwrap();
    assert_eq!(sink.unko_diff.total, 1);
}

/// 起点がある月は dtako 側の読みも始端を下げる (`since`)。無い月は今までどおり。
#[test]
fn the_gate_reads_back_to_the_anchor() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = store_with_punch();
    let anchors = HeadAnchors::from([(DRIVER, "2026-05-30 21:00:00".to_string())]);
    block_on(month_gate_report_with_anchors(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &anchors,
        &mut FoldSink::default(),
    ))
    .unwrap();
    block_on(month_gate_report(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        true,
        &mut FoldSink::default(),
    ))
    .unwrap();
    assert_eq!(*repo.since.borrow(), vec![Some(ymd(2026, 5, 30)), None]);
}

#[test]
fn a_failed_gate_write_does_not_fail_the_caller() {
    let gate = FoldGateRow {
        dtako_digest: "d".into(),
        punch_digest: "p".into(),
        logic_version: "v".into(),
    };
    let store = Store {
        gate_write_fail: true,
        ..Store::default()
    };
    block_on(write_fold_gate_best_effort(&store, MONTH, &gate));
    assert!(store.gate.borrow().is_none());
    let store = Store::default();
    block_on(write_fold_gate_best_effort(&store, MONTH, &gate));
    assert_eq!(store.gate.borrow().clone(), Some(gate));
}

// ── recalc_month: 月ゲートの 5 条件のうち CLI 経路の分 (apply・封を止める warning が無い) ───────────

fn recalc(
    repo: &Repo,
    store: &Store,
    apply: bool,
    sink: &mut FoldSink,
) -> Result<kintai_kosoku::kintai_fold::FoldReport, Fail> {
    let p = params();
    block_on(recalc_month(
        repo,
        store,
        &ctx(&p),
        MONTH,
        None,
        apply,
        Some(ymd(2026, 7, 15)),
        sink,
    ))
}

/// **書く: 束に封を止める warning が無ければ gate を刻み、2 回目は読まずに返す。**
#[test]
fn recalc_month_seals_the_month_when_nothing_blocks() {
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = store_with_punch();
    let mut sink = FoldSink::default();
    let r = recalc(&repo, &store, true, &mut sink).unwrap();
    assert_eq!(r.drivers_written, 1);
    assert_eq!(store.written.borrow().len(), 1);
    // 単調時計 2 回の差 (時計は並列のテストと共有なので 5 の倍数で見る)
    assert!(
        r.elapsed_ms >= 5 && r.elapsed_ms.is_multiple_of(5),
        "{}",
        r.elapsed_ms
    );
    assert_eq!(
        store.gate.borrow().as_ref().map(|g| g.dtako_digest.clone()),
        Some("d1".into())
    );
    let again = recalc(&repo, &store, true, &mut FoldSink::default()).unwrap();
    assert_eq!(repo.reads.get(), 1, "gate が刺さったら読みを丸ごと省く");
    assert!(!again.wrote_anything());
}

/// **書かない: 月ゲートの判定 (etags) で封を止める warning が立った。**
#[test]
fn recalc_month_does_not_seal_when_the_gate_stage_saw_missing_input() {
    let repo = repo_with(etags_read(
        "d1",
        vec![Warning::blocking("R2 に CSV の無い運行 1 件")],
    ));
    let store = store_with_punch();
    let r = recalc(&repo, &store, true, &mut FoldSink::default()).unwrap();
    assert_eq!(r.drivers_written, 1, "書くこと自体は行う");
    assert!(store.gate.borrow().is_none());
}

/// **書かない: 畳みの読みで封を止める warning が立った。**
#[test]
fn recalc_month_does_not_seal_when_the_fold_stage_saw_missing_input() {
    let repo = Repo {
        upstream: vec![Warning::blocking("NoSuchKey: U1")],
        ..repo_with(etags_read("d1", Vec::new()))
    };
    let store = store_with_punch();
    recalc(&repo, &store, true, &mut FoldSink::default()).unwrap();
    assert!(store.gate.borrow().is_none());
}

/// **書かない: 呼び出し側が先に積んだ封を止める warning も数える** (束ぜんたいを見る)。
#[test]
fn recalc_month_counts_what_the_caller_already_collected() {
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = store_with_punch();
    let mut sink = FoldSink::default();
    sink.warnings.push(Warning::blocking("NoSuchKey: U1"));
    recalc(&repo, &store, true, &mut sink).unwrap();
    assert!(store.gate.borrow().is_none());
}

/// **書く: 診断専用 (tail gap・遡り) だけなら封は止めない** (Refs #205-51)。
#[test]
fn recalc_month_seals_when_only_diagnostics_were_collected() {
    let repo = Repo {
        anchors: HeadAnchors::from([(DRIVER, "2026-05-30 21:00:00".to_string())]),
        ..repo_with(etags_read("d1", vec![Warning::diagnostic("tail gap")]))
    };
    let store = store_with_punch();
    let mut sink = FoldSink::default();
    recalc(&repo, &store, true, &mut sink).unwrap();
    assert!(sink.warnings.items().len() >= 2, "{:?}", sink.warnings);
    assert!(store.gate.borrow().is_some());
}

/// **書かない: dry-run** (1 行も保存しないので「最新」を主張する権利が無い)。
#[test]
fn recalc_month_does_not_seal_on_a_dry_run() {
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = store_with_punch();
    let r = recalc(&repo, &store, false, &mut FoldSink::default()).unwrap();
    assert!(r.dry_run);
    assert_eq!(r.drivers_written, 1, "計画は数える");
    assert!(store.written.borrow().is_empty());
    assert!(store.gate.borrow().is_none());
}

/// 判定できない月 (alc に口が無い) は今までどおり読んで畳み、gate は書かない。
#[test]
fn recalc_month_folds_without_a_gate_when_it_is_unavailable() {
    let repo = Repo {
        rows: june_rows(),
        ..Repo::default()
    };
    let store = store_with_punch();
    let r = recalc(&repo, &store, true, &mut FoldSink::default()).unwrap();
    assert_eq!(r.drivers_written, 1);
    assert!(store.gate.borrow().is_none());
}

/// `driver` を名指しした再計算は月ゲートを通らない (月まるごとではない)。
#[test]
fn recalc_month_for_one_driver_skips_the_gate() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = store_with_punch();
    let mut sink = FoldSink::default();
    block_on(recalc_month(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        Some(DRIVER),
        true,
        None,
        &mut sink,
    ))
    .unwrap();
    assert_eq!(repo.etag_reads.get(), 0);
    assert!(store.gate.borrow().is_none());
}

#[test]
fn recalc_month_fails_on_a_bad_month_or_failed_reads() {
    let p = params();
    let store = store_with_punch();
    let repo = Repo::default();
    let bad = block_on(recalc_month(
        &repo,
        &store,
        &ctx(&p),
        "nope",
        None,
        true,
        None,
        &mut FoldSink::default(),
    ));
    assert_eq!(bad.unwrap_err(), Fail("bad month: nope".into()));
    let down = Repo {
        anchors_fail: true,
        ..Repo::default()
    };
    let e = block_on(recalc_month(
        &down,
        &store,
        &ctx(&p),
        MONTH,
        None,
        true,
        None,
        &mut FoldSink::default(),
    ));
    assert_eq!(e.unwrap_err(), Fail("anchors down".into()));
    let down = Repo {
        rows_fail: true,
        ..repo_with(etags_read("d1", Vec::new()))
    };
    let e = block_on(recalc_month(
        &down,
        &store,
        &ctx(&p),
        MONTH,
        None,
        true,
        None,
        &mut FoldSink::default(),
    ));
    assert_eq!(e.unwrap_err(), Fail("events down".into()));
}

// ── 保存済みとの突き合わせ ─────────────────────────────────────────────────

/// 保存済みが今回と同じ指紋・行数なら書かない。保存が読めなければ Err、書けなければ Err。
#[test]
fn store_units_skips_current_drivers_and_surfaces_store_failures() {
    let p = params();
    let (unit, fp) = fold_driver_month(DRIVER as i64, MONTH, &p, june_rows(), SHA);
    let current = StoredState {
        fingerprints: vec![fp.clone()],
        shifts: unit.shifts.len() as i64,
        day_summaries: unit.day_summaries.len() as i64,
        day_parts: unit.day_parts.len() as i64,
    };
    let store = Store {
        stored: BTreeMap::from([(DRIVER as i64, current)]),
        ..Store::default()
    };
    let all = vec![(DRIVER, unit.clone(), fp.clone())];
    let r = block_on(recalc_drivers_from_units(
        &store,
        &ctx(&p),
        MONTH,
        &[DRIVER],
        all.clone(),
        true,
    ))
    .unwrap();
    assert_eq!(
        (r.drivers, r.drivers_unchanged, r.drivers_written),
        (1, 1, 0)
    );

    let store = Store {
        stored_fail: true,
        ..Store::default()
    };
    let e = block_on(recalc_drivers_from_units(
        &store,
        &ctx(&p),
        MONTH,
        &[DRIVER],
        all.clone(),
        true,
    ));
    assert_eq!(e.unwrap_err(), Fail("stored down".into()));
    let store = Store {
        write_fail: true,
        ..Store::default()
    };
    let e = block_on(recalc_drivers_from_units(
        &store,
        &ctx(&p),
        MONTH,
        &[DRIVER],
        all,
        true,
    ));
    assert_eq!(e.unwrap_err(), Fail("write down".into()));
}

/// 名指しした乗務員だけを書き、行が 1 つも無い乗務員にも空の単位を立てる。書く版は `logic_version`。
#[test]
fn recalc_drivers_from_units_writes_only_the_named_drivers() {
    let p = params();
    let store = Store::default();
    let (unit, fp) = fold_driver_month(DRIVER as i64, MONTH, &p, june_rows(), SHA);
    let all = vec![(DRIVER, unit, fp), (2000, FoldUnit::default(), "x".into())];
    let r = block_on(recalc_drivers_from_units(
        &store,
        &ctx(&p),
        MONTH,
        &[4242, DRIVER],
        all,
        true,
    ))
    .unwrap();
    assert_eq!(r.drivers, 2);
    let written = store.written.borrow();
    let cds: Vec<i64> = written.iter().map(|w| w.1).collect();
    assert_eq!(
        cds,
        vec![DRIVER as i64, 4242],
        "乗務員CD 順。2000 は名指ししていない"
    );
    assert_eq!(written[0].3, logic_version(&p, SHA));
    let empty = block_on(recalc_drivers_from_units(
        &store,
        &ctx(&p),
        MONTH,
        &[],
        Vec::new(),
        true,
    ))
    .unwrap();
    assert_eq!(empty.drivers, 0);
}

/// `recalc_drivers` は月ゲートを**読むだけで書かない**。名指しが空なら 1 往復もしない。
#[test]
fn recalc_drivers_reads_the_gate_but_never_writes_it() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = store_with_punch();
    let empty = block_on(recalc_drivers(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        &[],
        true,
        &mut FoldSink::default(),
    ))
    .unwrap();
    assert_eq!((empty.drivers, repo.etag_reads.get()), (0, 0));

    let miss = block_on(recalc_drivers(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        &[DRIVER],
        true,
        &mut FoldSink::default(),
    ))
    .unwrap();
    assert_eq!(miss.drivers_written, 1);
    assert_eq!(repo.reads.get(), 1);
    assert!(store.gate.borrow().is_none(), "書かない");

    *store.gate.borrow_mut() = Some(FoldGateRow {
        dtako_digest: "d1".into(),
        punch_digest: "punch-digest".into(),
        logic_version: logic_version(&p, SHA),
    });
    let hit = block_on(recalc_drivers(
        &repo,
        &store,
        &ctx(&p),
        MONTH,
        &[DRIVER, 4242],
        true,
        &mut FoldSink::default(),
    ))
    .unwrap();
    assert_eq!((hit.drivers, hit.drivers_unchanged), (2, 2));
    assert_eq!(repo.reads.get(), 1, "gate 一致で読まない");

    let bad = block_on(recalc_drivers(
        &repo,
        &store,
        &ctx(&p),
        "nope",
        &[DRIVER],
        true,
        &mut FoldSink::default(),
    ));
    assert_eq!(bad.unwrap_err(), Fail("bad month: nope".into()));
}
