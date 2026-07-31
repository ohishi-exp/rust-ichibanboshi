//! 畳む前の**読み方**を固定する (Refs #205 実装計画 05)。
//!
//! `kintai_fold::fold_month` は対象月を `fetch_all_events_between` **1 回**で読み、
//! `split_by_driver` で分けてから乗務員ごとに畳む。ここで確かめたいのは 2 つ:
//!
//! 1. 畳んだ 3 表の行が、**乗務員ごとに読んでいた旧経路と一致する**こと
//! 2. 生イベントの読みが**月あたり 1 回**に減っていること
//!
//! 1 が要るのは、全乗務員版の行が `unko_no` / `vehicle` をキーごと持たないため。
//! 行 JSON が変われば指紋は変わるが、**畳んだ値は変わってはいけない**。
//! DB は要らない (`recalc_month` の保存側は `kintai_fold_pg_test.rs` の担当)。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use rust_ichibanboshi::kintai_fold::{fold_driver_month, fold_month, FoldUnit};
use rust_ichibanboshi::kintai_repo::{
    month_range, DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError,
};
use rust_ichibanboshi::kosoku::{split_by_driver, KosokuParams};
use serde_json::{json, Value};

const MONTH: &str = "2026-07";

// ── fixture ───────────────────────────────────────────────────────────────

/// 生イベント 1 件。`unko_no` / `vehicle` は**単一乗務員版にしか出ない**列。
struct Ev {
    driver: u64,
    at: &'static str,
    end: Option<&'static str>,
    source: &'static str,
    state: &'static str,
    unko_no: Option<&'static str>,
    vehicle: Option<&'static str>,
}

fn tc(driver: u64, at: &'static str, state: &'static str) -> Ev {
    Ev {
        driver,
        at,
        end: None,
        source: "timecard",
        state,
        unko_no: None,
        vehicle: None,
    }
}

fn dtako(driver: u64, at: &'static str, state: &'static str, unko_no: &'static str) -> Ev {
    Ev {
        driver,
        at,
        end: None,
        source: "dtako",
        state,
        unko_no: Some(unko_no),
        vehicle: None,
    }
}

fn span(
    driver: u64,
    at: &'static str,
    end: &'static str,
    state: &'static str,
    unko_no: &'static str,
) -> Ev {
    Ev {
        driver,
        at,
        end: Some(end),
        source: "dtako_events",
        state,
        unko_no: Some(unko_no),
        vehicle: Some("大型 1 号"),
    }
}

/// 月ぶんの生イベント。実際に効く形を一通り混ぜる。
///
/// - 1130: ふつうの日勤 + 日跨ぎ勤務 (`day_parts` が立つ)
/// - 1526: **同時刻・同イベントで運行NO だけ違う 2 行** (実測 `…011` / `…012`)。
///   全乗務員版では区別が付かず 1 行に潰れる — ここが一致すれば主張が立つ
/// - 1726: 休息イベントで境界が決まる勤務 (`shift_source = "rest"`)
/// - 1999: 対象月の外にしか行が無い乗務員 (期間の絞りが効くか)
fn fixture() -> Vec<Ev> {
    vec![
        // 1130 — 日勤
        tc(1130, "2026-07-01 08:00:00", "始業"),
        span(
            1130,
            "2026-07-01 12:00:00",
            "2026-07-01 13:00:00",
            "休憩",
            "OP-A",
        ),
        tc(1130, "2026-07-01 18:30:00", "終業"),
        // 1130 — 日跨ぎ
        tc(1130, "2026-07-10 21:00:00", "始業"),
        span(
            1130,
            "2026-07-11 01:00:00",
            "2026-07-11 01:40:00",
            "休憩",
            "OP-B",
        ),
        tc(1130, "2026-07-11 09:00:00", "終業"),
        // 1526 — 運行NO だけ違う重複
        tc(1526, "2026-07-02 06:00:00", "始業"),
        dtako(
            1526,
            "2026-07-02 12:00:00",
            "休息",
            "26022506251200000023011",
        ),
        dtako(
            1526,
            "2026-07-02 12:00:00",
            "休息",
            "26022506251200000023012",
        ),
        tc(1526, "2026-07-02 20:00:00", "終業"),
        // 1726 — 休息で切れる勤務 (打刻が無い)
        span(
            1726,
            "2026-07-03 02:00:00",
            "2026-07-03 11:00:00",
            "休息",
            "OP-C",
        ),
        dtako(1726, "2026-07-03 13:00:00", "運行開始", "OP-C"),
        span(
            1726,
            "2026-07-03 15:00:00",
            "2026-07-03 15:30:00",
            "休憩",
            "OP-C",
        ),
        dtako(1726, "2026-07-03 22:00:00", "運行終了", "OP-C"),
        span(
            1726,
            "2026-07-04 02:00:00",
            "2026-07-04 11:00:00",
            "休息",
            "OP-D",
        ),
        // 1999 — 対象月の外だけ
        tc(1999, "2026-06-15 08:00:00", "始業"),
        tc(1999, "2026-06-15 17:00:00", "終業"),
    ]
}

/// 単一乗務員版の行 (`kintai_repo::row_to_json` と同じキー構成)。
fn single_row(e: &Ev) -> Value {
    json!({
        "datetime": e.at,
        "end_datetime": e.end,
        "driver_id": e.driver,
        "source": e.source,
        "state": e.state,
        "unko_no": e.unko_no,
        "vehicle": e.vehicle,
    })
}

/// 全乗務員版の行。`unko_no` / `vehicle` は**キーごと出さない**
/// (`kintai_repo::all_row_to_json` / `kintai_http_repo::event_to_all_json`)。
fn all_row(e: &Ev) -> Value {
    json!({
        "datetime": e.at,
        "end_datetime": e.end,
        "driver_id": e.driver,
        "source": e.source,
        "state": e.state,
    })
}

fn in_window(e: &Ev, from: &str, to: &str) -> bool {
    // 点イベントは開始で、区間イベントは「期間内に終わる」も拾う (EVENTS_SQL と同じ)
    if e.at >= from && e.at < to {
        return true;
    }
    e.end
        .is_some_and(|end| e.at < from && end >= from && end < to)
}

// ── 読みの回数を数える fake ────────────────────────────────────────────────

#[derive(Default)]
struct Counts {
    all: AtomicUsize,
    single: AtomicUsize,
}

struct CountingRepo {
    counts: Arc<Counts>,
}

#[async_trait]
impl KintaiEventsApi for CountingRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.counts.single.fetch_add(1, Ordering::SeqCst);
        Ok(fixture()
            .iter()
            .filter(|e| e.driver == driver && in_window(e, from, to))
            .map(single_row)
            .collect())
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.counts.all.fetch_add(1, Ordering::SeqCst);
        Ok(fixture()
            .iter()
            .filter(|e| in_window(e, from, to))
            .map(all_row)
            .collect())
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(Vec::new())
    }
}

fn repo() -> (DynKintaiEventsRepo, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    (
        Arc::new(CountingRepo {
            counts: counts.clone(),
        }),
        counts,
    )
}

/// 旧経路 — 乗務員を全量読みで列挙し、**乗務員ごとにもう 1 回読んで**畳む。
async fn fold_month_per_driver(
    repo: &DynKintaiEventsRepo,
    params: &KosokuParams,
    month: &str,
) -> Vec<(u64, FoldUnit, String)> {
    let (from, to) = month_range(month).unwrap();
    let drivers: Vec<u64> =
        split_by_driver(repo.fetch_all_events_between(&from, &to).await.unwrap())
            .into_iter()
            .map(|(d, _)| d)
            .collect();
    let mut out = Vec::new();
    for cd in drivers {
        let rows = repo.fetch_events_between(&from, &to, cd).await.unwrap();
        let (unit, fp) = fold_driver_month(cd as i64, month, params, rows);
        out.push((cd, unit, fp));
    }
    out
}

// ── 1. 畳んだ行が旧経路と一致する ──────────────────────────────────────────

#[tokio::test]
async fn folding_from_one_read_matches_the_per_driver_read() {
    let params = KosokuParams::default();
    let (repo, _) = repo();

    let old = fold_month_per_driver(&repo, &params, MONTH).await;
    let new = fold_month(&repo, &params, MONTH, None).await.unwrap();

    assert!(!old.is_empty(), "fixture が空では何も確かめていない");
    assert_eq!(
        old.iter().map(|(d, ..)| *d).collect::<Vec<_>>(),
        new.iter().map(|(d, ..)| *d).collect::<Vec<_>>(),
        "乗務員の並び (CD 昇順) まで同じ"
    );

    for ((cd, o, _), (_, n, _)) in old.iter().zip(new.iter()) {
        assert_eq!(o.shifts, n.shifts, "shifts が割れた: 乗務員 {cd}");
        assert_eq!(
            o.day_summaries, n.day_summaries,
            "day_summaries が割れた: 乗務員 {cd}"
        );
        assert_eq!(o.day_parts, n.day_parts, "day_parts が割れた: 乗務員 {cd}");
        assert_eq!(o.skipped, n.skipped, "落とした理由が割れた: 乗務員 {cd}");
    }

    // fixture が空回りしていないことの裏取り
    let total: usize = new.iter().map(|(_, u, _)| u.shifts.len()).sum();
    assert!(total >= 4, "勤務が {total} 本しか立っていない");
    assert!(
        new.iter().any(|(_, u, _)| !u.day_parts.is_empty()),
        "日跨ぎ勤務 (day_parts) が 1 つも無い"
    );
    assert!(
        new.iter()
            .any(|(_, u, _)| u.shifts.iter().any(|s| s.shift_source == "rest")),
        "休息由来の勤務が 1 本も無い"
    );
}

#[tokio::test]
async fn the_duplicate_unko_no_row_changes_the_fingerprint_but_not_the_values() {
    // 全乗務員版は運行NO を持たないので、1526 の …011 / …012 は 1 行に潰れる。
    // 潰れても拘束・実働・深夜は動かない (休息も休憩も区間を畳んで数えるため)
    let params = KosokuParams::default();
    let (repo, _) = repo();

    let old = fold_month_per_driver(&repo, &params, MONTH).await;
    let new = fold_month(&repo, &params, MONTH, None).await.unwrap();
    let pick = |v: &[(u64, FoldUnit, String)]| {
        v.iter()
            .find(|(cd, ..)| *cd == 1526)
            .map(|(_, u, fp)| (u.clone(), fp.clone()))
            .expect("1526")
    };
    let (o, o_fp) = pick(&old);
    let (n, n_fp) = pick(&new);

    assert_eq!(o.day_summaries, n.day_summaries, "畳んだ値は変わらない");
    assert_ne!(
        o_fp, n_fp,
        "行 JSON が違うので指紋は変わる — だから fold は経路を混ぜない"
    );
}

// ── 2. 読みが月 1 回に減っている ───────────────────────────────────────────

#[tokio::test]
async fn folding_a_month_reads_the_events_once() {
    let params = KosokuParams::default();
    let (repo, counts) = repo();

    let units = fold_month(&repo, &params, MONTH, None).await.unwrap();

    assert_eq!(counts.all.load(Ordering::SeqCst), 1, "全量読みは月 1 回");
    assert_eq!(
        counts.single.load(Ordering::SeqCst),
        0,
        "乗務員ごとの読みは 1 回も起きない"
    );
    assert!(units.len() >= 3, "乗務員が {} 名しか居ない", units.len());
}

#[tokio::test]
async fn the_old_shape_paid_one_round_trip_per_driver() {
    // 直そうとしているものを固定する。乗務員が増えるほど往復が増えていた
    let params = KosokuParams::default();
    let (repo, counts) = repo();

    let units = fold_month_per_driver(&repo, &params, MONTH).await;

    assert_eq!(counts.all.load(Ordering::SeqCst), 1);
    assert_eq!(
        counts.single.load(Ordering::SeqCst),
        units.len(),
        "乗務員 1 名につき 1 往復"
    );
}

#[tokio::test]
async fn naming_one_driver_still_reads_once() {
    let params = KosokuParams::default();
    let (repo, counts) = repo();

    let units = fold_month(&repo, &params, MONTH, Some(1130)).await.unwrap();

    assert_eq!(units.len(), 1);
    assert_eq!(units[0].0, 1130);
    assert_eq!(counts.all.load(Ordering::SeqCst), 1);
    assert_eq!(
        counts.single.load(Ordering::SeqCst),
        0,
        "単一指定でも全乗務員版で読む — 指紋を経路で割らないため"
    );
}

#[tokio::test]
async fn naming_a_driver_with_no_rows_still_yields_an_empty_unit() {
    // 打刻が消えた乗務員の古い行を消せるように、空の単位を返す
    let params = KosokuParams::default();
    let (repo, _) = repo();

    let units = fold_month(&repo, &params, MONTH, Some(4242)).await.unwrap();

    assert_eq!(units.len(), 1);
    assert_eq!(units[0].0, 4242);
    assert!(units[0].1.shifts.is_empty());
    assert!(units[0].1.day_summaries.is_empty());
    assert!(units[0].1.day_parts.is_empty());
}

#[tokio::test]
async fn a_driver_outside_the_month_is_not_folded() {
    let params = KosokuParams::default();
    let (repo, _) = repo();

    let units = fold_month(&repo, &params, MONTH, None).await.unwrap();

    assert!(
        !units.iter().any(|(cd, ..)| *cd == 1999),
        "対象月の外にしか行が無い乗務員は出てこない"
    );
}

#[tokio::test]
async fn a_bad_month_is_rejected_before_reading() {
    let params = KosokuParams::default();
    let (repo, counts) = repo();

    assert!(fold_month(&repo, &params, "nope", None).await.is_err());
    assert_eq!(counts.all.load(Ordering::SeqCst), 0, "読む前に落とす");
}
