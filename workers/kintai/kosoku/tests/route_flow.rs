//! 勤怠の口の段取り (`kintai_kosoku::route_flow`) を fake の I/O で通す (Refs ohishi-exp/rust-ichibanboshi#322)。
//! DB も tokio も要らない。root の pg テスト (`tests/kintai_fold_pg_test.rs` の HTTP 経路) が同じ段取りを本物の Postgres で通す。
//!
//! ここで固定するのは: 月ゲートが刺さった回は畳みの読みをしない・gate を刻む 5 条件・ページング・`stale_only`・
//! 応答のキーの一覧・窓の畳み直しの失敗を 200 のまま応答に載せること・打刻の置き換えの dry-run。

mod common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, FixedOffset, NaiveDate};
use common::*;
use kintai_kosoku::fold_flow::{DtakoMonthRead, FoldGateRow, Warning};
use kintai_kosoku::kintai_fold::{logic_version, FoldReport, SkipReason};
use kintai_kosoku::kintai_push::{DriverPlan, TimecardWindow, TimecardWindowResult};
use kintai_kosoku::route_flow::{
    apply_timecard_window, fold_into, fold_window, merge_fold, page_size, recalc_page, BadWindow,
    RecalcQuery, RecalcRequest, TimecardWindowIo, DEFAULT_MAX_FOLD_DRIVERS, MAX_MAX_FOLD_DRIVERS,
};
use serde_json::{json, Value};

fn req(apply: bool) -> RecalcRequest {
    RecalcRequest {
        month: MONTH.to_string(),
        apply,
        today: Some(NaiveDate::from_ymd_opt(2026, 7, 15).unwrap()),
        ..RecalcRequest::default()
    }
}

/// 月ゲートが Miss で、突合も測れる store (stale は 0 人)。
fn miss_store() -> Store {
    Store {
        stale_drivers: Some(0),
        ..store_with_punch()
    }
}

fn gate_row() -> FoldGateRow {
    FoldGateRow {
        dtako_digest: "d1".into(),
        punch_digest: "punch-digest".into(),
        logic_version: logic_version(&params()),
    }
}

fn run(repo: &Repo, store: &Store, r: &RecalcRequest) -> Result<Value, Fail> {
    let p = params();
    block_on(recalc_page(repo, store, &ctx(&p), r))
}

// ── 入力 ─────────────────────────────────────────────────────────────────────

#[test]
fn the_page_size_is_clamped() {
    assert_eq!(page_size(None), DEFAULT_MAX_FOLD_DRIVERS);
    assert_eq!(page_size(Some(0)), 1);
    assert_eq!(page_size(Some(999)), MAX_MAX_FOLD_DRIVERS);
    assert_eq!(page_size(Some(42)), 42);
}

/// **GET は `apply` を持たない。** query から作った要求は必ず dry-run で、「今日」も上書きできない。
#[test]
fn the_get_query_never_applies() {
    let q: RecalcQuery = serde_json::from_value(json!({
        "month": "2026-06", "after_driver_cd": 5, "max_drivers": 7, "stale_only": true,
    }))
    .unwrap();
    let r = q.into_request();
    assert_eq!(
        (
            r.month.as_str(),
            r.after_driver_cd,
            r.max_drivers,
            r.stale_only
        ),
        (MONTH, Some(5), Some(7), true)
    );
    assert!(!r.apply && r.today.is_none());
    assert_eq!(RecalcQuery::default().into_request().month, "");
    // JSON の本文から「今日」は入らない
    let body: RecalcRequest =
        serde_json::from_value(json!({"month": "2026-06", "apply": true, "today": "2026-01-01"}))
            .unwrap();
    assert!(body.apply && body.today.is_none());
}

// ── 全量再計算の 1 ページ ─────────────────────────────────────────────────────

/// 応答のキーは月ゲートの刺さり方によらず同じ 23 個 (root の応答と同じ)。
const KEYS: [&str; 23] = [
    "month",
    "apply",
    "drivers",
    "next_after_driver_cd",
    "fold",
    "stale",
    "elapsed_ms",
    "unsplit",
    "unsplit_total",
    "unko_diff",
    "unko_diff_total",
    "unko_diff_gcp_only",
    "unko_diff_gcp_only_in_month",
    "unko_diff_gcp_only_by_month",
    "unko_diff_gcp_only_in_month_by_day",
    "unko_diff_gcp_only_in_month_by_driver",
    "unko_diff_gcp_only_in_month_unknown_driver",
    "unko_diff_gcp_only_driver_split",
    "unko_diff_gcp_only_sample",
    "unko_diff_onprem_sample",
    "unko_diff_onprem_shape",
    "unko_diff_gcp_shape",
    "unko_diff_trials",
];

fn keys(v: &Value) -> BTreeSet<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

fn expected_keys() -> BTreeSet<String> {
    KEYS.iter().map(|k| k.to_string()).collect()
}

/// **1 ページで月まるごと回りきった apply は gate を刻む。** 応答に畳んだ報告・stale・etags の素通しが載る。
#[test]
fn a_whole_month_in_one_page_writes_the_gate() {
    let repo = repo_with(etags_read("d1", vec![Warning::diagnostic("tail gap")]));
    let store = miss_store();
    let v = run(&repo, &store, &req(true)).unwrap();
    assert_eq!(keys(&v), expected_keys());
    assert_eq!(v["month"], MONTH);
    assert_eq!(v["apply"], true);
    assert_eq!(v["drivers"], json!([DRIVER]));
    assert_eq!(v["next_after_driver_cd"], Value::Null);
    assert_eq!(v["fold"]["drivers_written"], 1);
    assert_eq!(v["fold"]["warnings"], json!(["tail gap"]));
    assert_eq!(v["stale"]["drivers"], 0);
    assert_eq!(v["unsplit_total"], 3);
    assert_eq!(v["unsplit"][0]["unko_no"], "U9");
    assert!(v["elapsed_ms"].as_u64().unwrap() > 0);
    assert_eq!(repo.reads.get(), 1, "生イベントは 1 回だけ読む");
    assert_eq!(store.written.borrow().len(), 1);
    assert_eq!(*store.gate.borrow(), Some(gate_row()), "診断だけなら刻む");
    assert_eq!(
        store.stale_calls.borrow().clone(),
        vec![vec![MONTH.to_string()]]
    );
}

/// **月ゲートが刺さったら畳みの読みも書き込みもしない。** gate の束の warnings は応答に出す。
#[test]
fn a_gate_hit_skips_the_read_and_still_reports_stale() {
    let repo = repo_with(etags_read("d1", vec![Warning::blocking("NoSuchKey")]));
    let store = Store {
        stale_drivers: Some(4),
        ..store_with_punch()
    };
    *store.gate.borrow_mut() = Some(gate_row());
    let v = run(&repo, &store, &req(true)).unwrap();
    assert_eq!(keys(&v), expected_keys());
    assert_eq!(v["drivers"], json!([]));
    assert_eq!(v["next_after_driver_cd"], Value::Null);
    assert_eq!(v["fold"]["drivers"], 0);
    assert_eq!(v["fold"]["warnings"], json!(["NoSuchKey"]));
    assert_eq!(v["stale"]["drivers"], 4);
    assert_eq!(v["unsplit_total"], 3);
    assert_eq!(repo.reads.get(), 0);
    assert!(store.written.borrow().is_empty());
}

/// 5 条件のどれか 1 つでも欠けたら gate を刻まない (apply 無し・2 ページ目・回りきらない・stale_only・封を止める warning)。
#[test]
fn the_gate_is_sealed_only_when_all_five_conditions_hold() {
    let cases: Vec<(&str, RecalcRequest, Store, Vec<Warning>)> = vec![
        ("dry-run", req(false), miss_store(), Vec::new()),
        (
            "2 ページ目",
            RecalcRequest {
                after_driver_cd: Some(1),
                ..req(true)
            },
            miss_store(),
            Vec::new(),
        ),
        (
            "回りきらない",
            RecalcRequest {
                max_drivers: Some(1),
                ..req(true)
            },
            Store {
                population: BTreeSet::from([500]),
                ..miss_store()
            },
            Vec::new(),
        ),
        (
            "stale_only",
            RecalcRequest {
                stale_only: true,
                ..req(true)
            },
            Store {
                stale_population: BTreeSet::from([DRIVER as i64]),
                ..miss_store()
            },
            Vec::new(),
        ),
        (
            "畳み段の封を止める warning",
            req(true),
            miss_store(),
            vec![Warning::blocking("R2 404")],
        ),
    ];
    for (name, r, store, upstream) in cases {
        let repo = Repo {
            upstream,
            ..repo_with(etags_read("d1", Vec::new()))
        };
        let v = run(&repo, &store, &r).unwrap();
        assert_eq!(keys(&v), expected_keys(), "{name}");
        assert!(store.gate.borrow().is_none(), "{name}");
    }
}

/// gate 段 (指紋を取る読み) で立った封を止める warning でも刻まない。応答では畳み段の後ろに並ぶ。
#[test]
fn a_blocking_warning_from_the_gate_read_blocks_the_seal() {
    let repo = Repo {
        upstream: vec![Warning::diagnostic("fold side")],
        ..repo_with(etags_read("d1", vec![Warning::blocking("gate side")]))
    };
    let store = miss_store();
    let v = run(&repo, &store, &req(true)).unwrap();
    assert_eq!(v["fold"]["warnings"], json!(["fold side", "gate side"]));
    assert!(store.gate.borrow().is_none());
}

/// 月ゲートが判定できない (dtako の指紋が無い) 回は、apply で回りきっても刻まない。
#[test]
fn an_unavailable_gate_is_never_sealed() {
    let repo = repo_with(DtakoMonthRead::digest_only(None));
    let store = miss_store();
    let v = run(&repo, &store, &req(true)).unwrap();
    assert_eq!(v["fold"]["drivers_written"], 1);
    assert!(store.gate.borrow().is_none());
}

/// **ページング**: 1 ページちょうど埋まったら最後の乗務員CD が次の起点。母集団は Postgres 側と読んだ乗務員の和。
/// 0 以下の乗務員CD は畳まない。stale_only は現行版の行を持たない乗務員だけ。
#[test]
fn pages_are_cut_by_driver_cd_and_stale_only_narrows_them() {
    let repo = repo_with(etags_read("d1", Vec::new()));
    let store = Store {
        population: BTreeSet::from([1000, 2000]),
        stale_population: BTreeSet::from([2000]),
        ..miss_store()
    };
    let first = run(
        &repo,
        &store,
        &RecalcRequest {
            max_drivers: Some(2),
            ..req(false)
        },
    )
    .unwrap();
    assert_eq!(first["drivers"], json!([1000, DRIVER]));
    assert_eq!(first["next_after_driver_cd"], json!(DRIVER));
    assert_eq!(first["fold"]["drivers"], 2);
    let second = run(
        &repo,
        &store,
        &RecalcRequest {
            max_drivers: Some(2),
            after_driver_cd: Some(DRIVER as i64),
            ..req(false)
        },
    )
    .unwrap();
    assert_eq!(second["drivers"], json!([2000]));
    assert_eq!(second["next_after_driver_cd"], Value::Null);
    let stale = run(
        &repo,
        &store,
        &RecalcRequest {
            stale_only: true,
            ..req(false)
        },
    )
    .unwrap();
    assert_eq!(stale["drivers"], json!([2000]));
    assert!(store.written.borrow().is_empty(), "dry-run は書かない");
}

/// 読みの失敗・母集団の失敗・stale の失敗はどれも `Err` (HTTP の殻が 502 / 503 に写す)。
#[test]
fn failures_are_returned_as_errors() {
    let anchors = Repo {
        anchors_fail: true,
        ..repo_with(etags_read("d1", Vec::new()))
    };
    let e = run(&anchors, &miss_store(), &req(true)).unwrap_err();
    assert_eq!(e, Fail("anchors down".into()));

    let rows = Repo {
        rows_fail: true,
        ..repo_with(etags_read("d1", Vec::new()))
    };
    assert_eq!(
        run(&rows, &miss_store(), &req(true)).unwrap_err(),
        Fail("events down".into())
    );

    let repo = repo_with(etags_read("d1", Vec::new()));
    let page = Store {
        page_fail: true,
        ..miss_store()
    };
    assert_eq!(
        run(&repo, &page, &req(true)).unwrap_err(),
        Fail("page down".into())
    );

    let stale = store_with_punch();
    assert_eq!(
        run(&repo, &stale, &req(false)).unwrap_err(),
        Fail("stale down".into())
    );
    *stale.gate.borrow_mut() = Some(gate_row());
    assert_eq!(
        run(&repo, &stale, &req(false)).unwrap_err(),
        Fail("stale down".into()),
        "gate が刺さった回も"
    );

    let fold = Store {
        stored_fail: true,
        ..miss_store()
    };
    assert_eq!(
        run(&repo, &fold, &req(true)).unwrap_err(),
        Fail("stored down".into())
    );
}

// ── 窓の受け口 ───────────────────────────────────────────────────────────────

fn window(months: &[&str], dry_run: bool) -> TimecardWindow {
    serde_json::from_value(json!({
        "months": months, "drivers": [DRIVER], "dry_run": dry_run,
        "events": [
            {"datetime": "2026-06-10 08:00:00", "end_datetime": null, "driver_id": DRIVER, "source": "timecard", "state": "始業", "unko_no": null},
        ],
    }))
    .unwrap()
}

fn changed(drivers: &[i64]) -> TimecardWindowResult {
    TimecardWindowResult {
        drivers_changed: drivers.to_vec(),
        ..TimecardWindowResult::default()
    }
}

/// 月ごとの報告を足し合わせる。窓は複数月を覆う。
#[test]
fn fold_reports_add_up_across_months() {
    let june = FoldReport {
        drivers: 1,
        drivers_written: 1,
        shifts: 3,
        day_summaries: 3,
        day_parts: 4,
        elapsed_ms: 2,
        logic_version: "old".to_string(),
        ..Default::default()
    };
    let july = FoldReport {
        drivers: 2,
        drivers_unchanged: 2,
        shifts: 1,
        day_summaries: 1,
        day_parts: 1,
        elapsed_ms: 3,
        dry_run: true,
        logic_version: "new".to_string(),
        calculated_at: "t".to_string(),
        skipped: vec![SkipReason::DegenerateShift {
            start: "a".to_string(),
            end: "a".to_string(),
        }],
        ..Default::default()
    };
    let sum = merge_fold(june, july);
    assert_eq!(
        (sum.drivers, sum.drivers_written, sum.drivers_unchanged),
        (3, 1, 2)
    );
    assert_eq!((sum.shifts, sum.day_summaries, sum.day_parts), (4, 4, 5));
    assert_eq!(sum.skipped.len(), 1, "落とした行は畳まずに残す");
    assert_eq!(sum.elapsed_ms, 5);
    // 版と時刻は月で変わらないので後勝ちでよい
    assert_eq!(sum.logic_version, "new");
    assert_eq!(sum.calculated_at, "t");
    assert!(sum.dry_run);
}

/// 変わった乗務員だけを月ごとに畳み、束は窓ぜんたいで 1 つ。0 以下の乗務員CD は畳まない。
#[test]
fn the_window_folds_only_the_changed_drivers_month_by_month() {
    let p = params();
    let repo = Repo {
        upstream: vec![Warning::blocking("R2 404")],
        ..repo_with(etags_read("d1", Vec::new()))
    };
    let store = miss_store();
    let w = window(&["2026-06", "2026-07"], false);
    let r = block_on(fold_window(
        &repo,
        &store,
        &ctx(&p),
        &w,
        &changed(&[DRIVER as i64, 0]),
    ))
    .unwrap();
    assert_eq!(r.drivers, 2, "2 か月 × 1 名");
    assert!(!r.dry_run);
    assert_eq!(r.warnings, vec!["R2 404".to_string()], "重複は 1 回");
    assert!(store.gate.borrow().is_none(), "窓は gate を書かない");

    let dry = block_on(fold_window(
        &repo,
        &store,
        &ctx(&p),
        &window(&["2026-06"], true),
        &changed(&[]),
    ))
    .unwrap();
    assert_eq!(dry.drivers, 0);
    assert!(dry.dry_run);
}

/// **畳めても畳めなくても 200 の本文に書き足す。** 失敗は `status_of` が写した status と文面。stale は独立に試す。
#[test]
fn fold_into_writes_the_fold_or_the_error_and_the_stale_independently() {
    let p = params();
    let repo = repo_with(etags_read("d1", Vec::new()));
    let w = window(&["2026-06"], false);
    let status_of = |e: Fail| (502_u16, e.0);

    let store = Store {
        stale_drivers: Some(3),
        ..store_with_punch()
    };
    let mut body = json!({"days_written": 1});
    block_on(fold_into(
        &repo,
        &store,
        &ctx(&p),
        &w,
        &changed(&[DRIVER as i64]),
        &mut body,
        status_of,
    ));
    assert_eq!(body["days_written"], 1);
    assert_eq!(body["fold"]["drivers_written"], 1);
    assert_eq!(body["stale"]["drivers"], 3);
    assert!(body.get("fold_error").is_none());

    let broken = Store {
        write_fail: true,
        ..store_with_punch()
    };
    let mut body = json!({});
    block_on(fold_into(
        &repo,
        &broken,
        &ctx(&p),
        &w,
        &changed(&[DRIVER as i64]),
        &mut body,
        status_of,
    ));
    assert_eq!(body["fold_error"], "write down");
    assert_eq!(body["fold_error_status"], 502);
    assert_eq!(body["stale_error"], "stale down");
    assert!(body.get("fold").is_none() && body.get("stale").is_none());

    let quiet = miss_store();
    let mut body = json!({});
    block_on(fold_into(
        &repo,
        &quiet,
        &ctx(&p),
        &w,
        &changed(&[]),
        &mut body,
        status_of,
    ));
    assert_eq!(body["fold"]["drivers"], 0);
    assert_eq!(body["stale"]["drivers"], 0);
}

// ── 打刻の置き換え ───────────────────────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
struct WFail(String);

impl std::fmt::Display for WFail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<BadWindow> for WFail {
    fn from(e: BadWindow) -> Self {
        WFail(e.0)
    }
}

#[derive(Default)]
struct Events {
    sig_fail: bool,
    replace_fail: bool,
    asked: RefCell<Vec<i64>>,
    replaced: Cell<usize>,
}

impl TimecardWindowIo for Events {
    type Error = WFail;

    async fn stored_window_signatures(
        &self,
        drivers: &[i64],
        _from: DateTime<FixedOffset>,
        _to: DateTime<FixedOffset>,
    ) -> Result<BTreeMap<i64, BTreeMap<NaiveDate, String>>, WFail> {
        if self.sig_fail {
            return Err(WFail("sig down".into()));
        }
        self.asked.borrow_mut().extend_from_slice(drivers);
        Ok(BTreeMap::new())
    }

    async fn replace_window(&self, plans: &BTreeMap<i64, DriverPlan>) -> Result<(), WFail> {
        if self.replace_fail {
            return Err(WFail("replace down".into()));
        }
        self.replaced.set(self.replaced.get() + plans.len());
        Ok(())
    }
}

/// 名乗った乗務員の署名だけを引き、変わった日を書く。dry-run は書かない。月が読めなければ書く前に落とす。
#[test]
fn the_window_replaces_changed_days_unless_dry_run() {
    let store = Events::default();
    let r = block_on(apply_timecard_window(
        &store,
        &window(&["2026-06"], false),
        &CLOCK,
    ))
    .unwrap();
    assert_eq!(store.asked.borrow().clone(), vec![DRIVER as i64]);
    assert_eq!((r.drivers_written, r.dry_run), (1, false));
    assert_eq!(r.drivers_changed, vec![DRIVER as i64]);
    assert!(r.elapsed_ms > 0);
    assert_eq!(store.replaced.get(), 1);

    let dry = block_on(apply_timecard_window(
        &store,
        &window(&["2026-06"], true),
        &CLOCK,
    ))
    .unwrap();
    assert!(dry.dry_run);
    assert_eq!(store.replaced.get(), 1, "dry-run は書かない");

    let bad = block_on(apply_timecard_window(&store, &window(&[], false), &CLOCK));
    assert_eq!(bad.unwrap_err(), WFail("months が空です".into()));

    let sig = Events {
        sig_fail: true,
        ..Events::default()
    };
    let e = block_on(apply_timecard_window(
        &sig,
        &window(&["2026-06"], false),
        &CLOCK,
    ));
    assert_eq!(e.unwrap_err(), WFail("sig down".into()));
    let rep = Events {
        replace_fail: true,
        ..Events::default()
    };
    let e = block_on(apply_timecard_window(
        &rep,
        &window(&["2026-06"], false),
        &CLOCK,
    ));
    assert_eq!(e.unwrap_err(), WFail("replace down".into()));
}
