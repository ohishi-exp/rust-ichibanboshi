//! fold の流れ (`fold_flow` / `route_flow`) のテストが共有する fake の I/O と固定入力 (Refs ohishi-exp/rust-ichibanboshi#322)。
#![allow(dead_code)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone};
use kintai_kosoku::anchors::HeadAnchors;
use kintai_kosoku::fold_flow::{
    BadMonth, DtakoMonthRead, EtagsRead, FoldClock, FoldCtx, FoldEventsIo, FoldGateRow,
    FoldStoreIo, UnsplitOperation, Warning, Warnings,
};
use kintai_kosoku::kintai_fold::{FoldUnit, StaleReport, StoredState};
use kintai_kosoku::kosoku::KosokuParams;
use serde_json::{json, Value};

pub const SHA: &str = "0123456789abcdef";
pub const MONTH: &str = "2026-06";
pub const DRIVER: u64 = 1078;

/// fake の I/O は待たずに返すので、1 回 poll すれば終わる。
pub fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fake の I/O は待たない"),
    }
}

// ── fake ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fail(pub String);

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
pub struct Repo {
    pub anchors: HeadAnchors,
    pub anchors_fail: bool,
    pub rows: Vec<Value>,
    pub upstream: Vec<Warning>,
    pub rows_fail: bool,
    pub dtako: RefCell<Option<Result<DtakoMonthRead, Fail>>>,
    pub reads: Cell<usize>,
    pub etag_reads: Cell<usize>,
    pub since: RefCell<Vec<Option<NaiveDate>>>,
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
pub struct Store {
    pub stored: BTreeMap<i64, StoredState>,
    pub stored_fail: bool,
    pub written: RefCell<Vec<(String, i64, String, String)>>,
    pub write_fail: bool,
    pub gate: RefCell<Option<FoldGateRow>>,
    pub gate_read_fail: bool,
    pub gate_write_fail: bool,
    pub punch: Option<String>,
    pub operations: Option<Vec<(i64, String, NaiveDate, NaiveDate)>>,
    pub ever: Option<HashSet<i64>>,
    /// `stale_state` が返す stale 乗務員数 (`None` は失敗)。呼ばれた月の並びを `stale_calls` に積む
    pub stale_drivers: Option<usize>,
    pub stale_calls: RefCell<Vec<Vec<String>>>,
    /// `recalc_driver_page` の Postgres 側の母集団 (`extra` と和を取る)。`page_fail` で失敗
    pub population: BTreeSet<i64>,
    /// `stale_only` で残す乗務員 (現行版の行を持たない)
    pub stale_population: BTreeSet<i64>,
    pub page_fail: bool,
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

    async fn stale_state(
        &self,
        months: &[String],
        logic_version: &str,
    ) -> Result<StaleReport, Fail> {
        self.stale_calls.borrow_mut().push(months.to_vec());
        let drivers = self
            .stale_drivers
            .ok_or_else(|| Fail("stale down".into()))?;
        Ok(StaleReport {
            logic_version: logic_version.to_string(),
            drivers,
            versions: vec![logic_version.to_string()],
        })
    }

    async fn recalc_driver_page(
        &self,
        _month: &str,
        _logic_version: &str,
        extra: &BTreeSet<i64>,
        after: Option<i64>,
        stale_only: bool,
        limit: usize,
    ) -> Result<Vec<i64>, Fail> {
        if self.page_fail {
            return Err(Fail("page down".into()));
        }
        let after = after.unwrap_or(i64::MIN);
        Ok(self
            .population
            .union(extra)
            .copied()
            .filter(|cd| *cd > after && *cd > 0)
            .filter(|cd| !stale_only || self.stale_population.contains(cd))
            .take(limit)
            .collect())
    }
}

/// 時計の fake (今日は 2026-07-15 の JST、単調時計は呼ぶたびに 5 ms 進む)。
pub struct Clock(pub AtomicU64);

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

pub static CLOCK: Clock = Clock(AtomicU64::new(100));

pub fn params() -> KosokuParams {
    KosokuParams::default()
}

pub fn ctx(params: &KosokuParams) -> FoldCtx<'_> {
    FoldCtx {
        params,
        code_sha: SHA,
        clock: &CLOCK,
    }
}

pub fn punch(driver: u64, at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": driver, "source": "timecard", "state": state, "unko_no": null})
}

/// 6 月の 1 日ぶんの始業・終業と、翌月 1 日の打刻 (push 窓ずれを鳴らさない)。
pub fn june_rows() -> Vec<Value> {
    vec![
        punch(DRIVER, "2026-06-10 08:00:00", "始業"),
        punch(DRIVER, "2026-06-10 18:00:00", "終業"),
        punch(DRIVER, "2026-07-01 08:00:00", "始業"),
    ]
}

/// etags の控え付きの読み (指紋は `digest`)。
pub fn etags_read(digest: &str, warnings: Vec<Warning>) -> DtakoMonthRead {
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

pub fn repo_with(read: DtakoMonthRead) -> Repo {
    Repo {
        rows: june_rows(),
        dtako: RefCell::new(Some(Ok(read))),
        ..Repo::default()
    }
}

pub fn store_with_punch() -> Store {
    Store {
        punch: Some("punch-digest".into()),
        operations: Some(Vec::new()),
        ever: Some(HashSet::new()),
        ..Store::default()
    }
}

pub fn texts(w: &Warnings) -> Vec<String> {
    w.texts()
}
