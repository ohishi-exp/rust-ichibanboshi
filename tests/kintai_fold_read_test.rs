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
use rust_ichibanboshi::kintai_fold::{
    fold_driver_month, fold_month, fold_month_with_anchors, FoldUnit,
};
use rust_ichibanboshi::kintai_repo::{
    month_head_anchors, month_range, DynKintaiEventsRepo, HeadPunch, KintaiEventsApi,
    KintaiRepoError,
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
    let new = fold_month(&repo, &params, MONTH, None, None).await.unwrap();

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
    let new = fold_month(&repo, &params, MONTH, None, None).await.unwrap();
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

    let units = fold_month(&repo, &params, MONTH, None, None).await.unwrap();

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

    let units = fold_month(&repo, &params, MONTH, Some(1130), None)
        .await
        .unwrap();

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

    let units = fold_month(&repo, &params, MONTH, Some(4242), None)
        .await
        .unwrap();

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

    let units = fold_month(&repo, &params, MONTH, None, None).await.unwrap();

    assert!(
        !units.iter().any(|(cd, ..)| *cd == 1999),
        "対象月の外にしか行が無い乗務員は出てこない"
    );
}

#[tokio::test]
async fn a_bad_month_is_rejected_before_reading() {
    let params = KosokuParams::default();
    let (repo, counts) = repo();

    assert!(fold_month(&repo, &params, "nope", None, None)
        .await
        .is_err());
    assert_eq!(counts.all.load(Ordering::SeqCst), 0, "読む前に落とす");
}

// ── 3. 月初をまたぐ運行・勤務 (Refs ohishi-exp/nuxt-dtako-admin#1123) ──────────

/// 7 月を畳むと月初をまたぐ形を一通り混ぜる。
///
/// - 1194: **6/30 に始業・運行開始し、7/1 に休息 2 本を挟んで運行終了・終業**
///   (2026-04 の実物を 1 か月ずらしたもの)。月初 0:00 から読むと休息由来の勤務が
///   7/1 に 2 本立つ
/// - 1130: 月をまたがない。6/30 夜に 6 月の勤務も持つ (7 月の窓の外)
/// - 1400: 6/20 の始業を打ち忘れで閉じていない。7 月の最初の打刻は始業 → 遡らない
/// - 1401: 6/30 22:00 に始業、7 月の最初の打刻が終業 → 遡る
/// - 1500: 6/30 夜にだけ行がある (広げた窓でしか見えない)
/// - 1600: 6/29 20:00〜7/2 08:00 の 36 時間勤務 (休息 2 本で割れる)
/// - 1731: 6/21 開始の閉じ忘れ運行が 7/2 に運行終了 (日数の上限なしで遡る)
fn cross_fixture() -> Vec<Ev> {
    const U1194: &str = "26063021394700000043241";
    const U1600: &str = "26062920300000000016001";
    const U1731: &str = "26062105000000000017311";
    vec![
        tc(1194, "2026-06-30 21:36:28", "始業"),
        dtako(1194, "2026-06-30 21:39:47", "運行開始", U1194),
        span(
            1194,
            "2026-06-30 21:39:47",
            "2026-07-01 00:28:25",
            "運転",
            U1194,
        ),
        span(
            1194,
            "2026-07-01 00:28:25",
            "2026-07-01 04:38:56",
            "休息",
            U1194,
        ),
        span(
            1194,
            "2026-07-01 04:38:56",
            "2026-07-01 04:45:36",
            "運転",
            U1194,
        ),
        span(
            1194,
            "2026-07-01 04:45:36",
            "2026-07-01 08:30:26",
            "休息",
            U1194,
        ),
        span(
            1194,
            "2026-07-01 08:30:26",
            "2026-07-01 16:15:46",
            "運転",
            U1194,
        ),
        dtako(1194, "2026-07-01 16:15:46", "運行終了", U1194),
        tc(1194, "2026-07-01 17:07:48", "終業"),
        tc(1130, "2026-06-30 20:00:00", "始業"),
        tc(1130, "2026-06-30 23:00:00", "終業"),
        tc(1130, "2026-07-02 08:00:00", "始業"),
        tc(1130, "2026-07-02 18:00:00", "終業"),
        tc(1400, "2026-06-20 08:00:00", "始業"),
        tc(1400, "2026-07-02 08:00:00", "始業"),
        tc(1400, "2026-07-02 17:00:00", "終業"),
        tc(1401, "2026-06-30 22:00:00", "始業"),
        tc(1401, "2026-07-01 06:00:00", "終業"),
        tc(1500, "2026-06-30 22:30:00", "始業"),
        tc(1500, "2026-06-30 23:30:00", "終業"),
        tc(1600, "2026-06-29 20:00:00", "始業"),
        dtako(1600, "2026-06-29 20:30:00", "運行開始", U1600),
        span(
            1600,
            "2026-06-30 06:00:00",
            "2026-06-30 14:00:00",
            "休息",
            U1600,
        ),
        span(
            1600,
            "2026-07-01 02:00:00",
            "2026-07-01 10:00:00",
            "休息",
            U1600,
        ),
        dtako(1600, "2026-07-02 07:30:00", "運行終了", U1600),
        tc(1600, "2026-07-02 08:00:00", "終業"),
        dtako(1731, "2026-06-21 05:00:00", "運行開始", U1731),
        span(
            1731,
            "2026-07-02 00:00:00",
            "2026-07-02 06:00:00",
            "休息",
            U1731,
        ),
        dtako(1731, "2026-07-02 10:00:00", "運行終了", U1731),
    ]
}

/// 窓で絞る fake。遡り起点は MariaDB / Pg と同じ規則 (`month_head_anchors`) で
/// fixture から求める。
struct CrossRepo {
    counts: Arc<Counts>,
}

#[async_trait]
impl KintaiEventsApi for CrossRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.counts.single.fetch_add(1, Ordering::SeqCst);
        Ok(cross_fixture()
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
        Ok(cross_fixture()
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

    async fn fetch_month_head_anchors(
        &self,
        month_start: &str,
        to: &str,
    ) -> Result<std::collections::BTreeMap<u64, String>, KintaiRepoError> {
        let evs = cross_fixture();
        let inside = |e: &&Ev| e.at >= month_start && e.at < to;
        let runs: Vec<(u64, String)> = evs
            .iter()
            .filter(inside)
            .filter(|e| e.source == "dtako" && e.state == "運行終了")
            .map(|e| (e.driver, e.unko_no.unwrap().to_string()))
            .collect();
        let drivers: std::collections::BTreeSet<u64> = evs
            .iter()
            .filter(inside)
            .filter(|e| e.source == "timecard")
            .map(|e| e.driver)
            .collect();
        let punches: Vec<HeadPunch> = drivers
            .into_iter()
            .map(|d| {
                let mine = || {
                    evs.iter()
                        .filter(move |e| e.driver == d && e.source == "timecard")
                };
                let last = |state: &str| {
                    mine()
                        .filter(|e| e.state == state && e.at < month_start)
                        .map(|e| e.at.to_string())
                        .max()
                };
                HeadPunch {
                    driver: d,
                    first_state: mine()
                        .filter(|e| e.at >= month_start && e.at < to)
                        .min_by_key(|e| e.at)
                        .map(|e| e.state.to_string()),
                    last_start: last("始業"),
                    last_end: last("終業"),
                }
            })
            .collect();
        Ok(month_head_anchors(month_start, &runs, &punches))
    }
}

fn cross_repo() -> (DynKintaiEventsRepo, Arc<Counts>) {
    let counts = Arc::new(Counts::default());
    (
        Arc::new(CrossRepo {
            counts: counts.clone(),
        }),
        counts,
    )
}

fn unit_of(units: &[(u64, FoldUnit, String)], cd: u64) -> Option<(FoldUnit, String)> {
    units
        .iter()
        .find(|(d, ..)| *d == cd)
        .map(|(_, u, fp)| (u.clone(), fp.clone()))
}

fn starts(units: &[(u64, FoldUnit, String)], cd: u64) -> Vec<String> {
    unit_of(units, cd)
        .map(|(u, _)| u.shifts.iter().map(|s| s.start_at.to_string()).collect())
        .unwrap_or_default()
}

/// (a) **7 月の fold は 6/30 始業の勤務の続きを 7/1 始業の勤務として立てない。**
/// 起点なし (= 修正前の窓) では休息由来の勤務が 2 本立っていたことも固定する。
#[tokio::test]
async fn a_shift_begun_last_month_is_not_restarted_this_month() {
    let params = KosokuParams::default();
    let (repo, _) = cross_repo();

    let july = fold_month(&repo, &params, MONTH, None, None).await.unwrap();
    assert!(starts(&july, 1194).is_empty(), "{:?}", starts(&july, 1194));
    assert!(starts(&july, 1401).is_empty(), "打刻だけで遡る形も同じ");

    let base = fold_month_with_anchors(&repo, &params, MONTH, None, None, &Default::default())
        .await
        .unwrap();
    assert_eq!(
        starts(&base, 1194),
        vec!["2026-07-01 04:38:00", "2026-07-01 08:30:00"],
        "修正前の窓では休息の終わりを始業とする勤務が 2 本立つ"
    );
}

/// (b) 6 月の fold は変わらない — 6/30 始業の勤務が 1 本、暦日は 6/30・7/1。
#[tokio::test]
async fn last_months_fold_is_unchanged() {
    let params = KosokuParams::default();
    let (repo, _) = cross_repo();

    let june = fold_month(&repo, &params, "2026-06", None, None)
        .await
        .unwrap();
    let base = fold_month_with_anchors(&repo, &params, "2026-06", None, None, &Default::default())
        .await
        .unwrap();
    assert_eq!(june, base, "6 月には遡る起点が無い");
    let (u, _) = unit_of(&june, 1194).unwrap();
    assert_eq!(starts(&june, 1194), vec!["2026-06-30 21:36:00"]);
    let dates: Vec<String> = u.day_parts.iter().map(|p| p.date.to_string()).collect();
    assert_eq!(dates, vec!["2026-06-30", "2026-07-01"]);
}

/// (c) 窓が広がっても読みは月 1 回のまま。
#[tokio::test]
async fn looking_back_still_reads_the_events_once() {
    let params = KosokuParams::default();
    let (repo, counts) = cross_repo();
    fold_month(&repo, &params, MONTH, None, None).await.unwrap();
    assert_eq!(counts.all.load(Ordering::SeqCst), 1);
    assert_eq!(counts.single.load(Ordering::SeqCst), 0);
}

/// (d)(e) **陰性対照**: 月をまたがない乗務員は、他人の起点で窓が広がっても行も
/// 指紋も修正前と同じ。広がった窓でしか見えない乗務員 (1500) は単位を持たない。
#[tokio::test]
async fn other_drivers_anchors_do_not_move_the_rest() {
    let params = KosokuParams::default();
    let (repo, _) = cross_repo();

    let new = fold_month(&repo, &params, MONTH, None, None).await.unwrap();
    let base = fold_month_with_anchors(&repo, &params, MONTH, None, None, &Default::default())
        .await
        .unwrap();
    for cd in [1130, 1400] {
        let n = unit_of(&new, cd).expect("new");
        assert!(!n.0.shifts.is_empty(), "乗務員 {cd} の勤務が立っていない");
        assert_eq!(Some(n), unit_of(&base, cd), "乗務員 {cd} が動いた");
    }
    assert!(unit_of(&new, 1500).is_none(), "広げた窓だけで見える乗務員");
    assert!(unit_of(&base, 1500).is_none());
}

/// (f)(g) 打刻の遡りは「開いた始業 + 当月の最初が終業」だけ。閉じ忘れ運行は
/// 日数の上限なしで遡り、遡った乗務員と日数が warnings に 1 行ずつ出る (封は止めない)。
#[tokio::test]
async fn the_anchors_follow_the_rules_and_are_reported() {
    let params = KosokuParams::default();
    let (repo, _) = cross_repo();
    let anchors = rust_ichibanboshi::kintai_fold::month_anchors(&repo, MONTH)
        .await
        .unwrap();
    let got: Vec<(u64, &str)> = anchors.iter().map(|(d, a)| (*d, a.as_str())).collect();
    assert_eq!(
        got,
        vec![
            (1194, "2026-06-30 21:36:28"),
            (1401, "2026-06-30 22:00:00"),
            (1600, "2026-06-29 20:00:00"),
            (1731, "2026-06-21 05:00:00"),
        ],
        "1400 (当月の最初が始業) は遡らない"
    );

    let (units, warnings) = rust_ichibanboshi::kintai_http_repo::with_warning_sink(fold_month(
        &repo, &params, MONTH, None, None,
    ))
    .await;
    units.unwrap();
    let lookback: Vec<&String> = warnings.iter().filter(|w| w.contains("遡り")).collect();
    assert_eq!(lookback.len(), 4, "{warnings:?}");
    assert!(lookback
        .iter()
        .any(|w| w.contains("1731") && w.contains("10 日前")));
    assert!(lookback
        .iter()
        .any(|w| w.contains("1194") && w.contains("1 日前")));
    // 診断なので封は止めない (今日を固定して push 窓ずれの警告を混ぜない)
    let today = chrono::NaiveDate::from_ymd_opt(2026, 7, 15);
    let (_, _, blocks) = rust_ichibanboshi::kintai_http_repo::with_warning_sink_blocking(
        fold_month(&repo, &params, MONTH, None, today),
    )
    .await;
    assert!(!blocks, "遡りの warning は封を止めない");
}

/// 前月始業の長い勤務を割った欠片のうち、当月始業のものだけが当月に出て、前月の
/// 出力と区間が重ならない (`split_long_shift` は月フィルタより前に効く)。
#[tokio::test]
async fn split_fragments_of_a_long_shift_do_not_overlap_across_months() {
    let params = KosokuParams::default();
    let (repo, _) = cross_repo();
    let june = fold_month(&repo, &params, "2026-06", None, None)
        .await
        .unwrap();
    let july = fold_month(&repo, &params, MONTH, None, None).await.unwrap();
    let spans = |v: &[(u64, FoldUnit, String)]| -> Vec<(String, String)> {
        unit_of(v, 1600)
            .map(|(u, _)| {
                u.shifts
                    .iter()
                    .map(|s| (s.start_at.to_string(), s.end_at.to_string()))
                    .collect()
            })
            .unwrap_or_default()
    };
    let (j6, j7) = (spans(&june), spans(&july));
    assert!(
        !j6.is_empty() && !j7.is_empty(),
        "6 月 {j6:?} / 7 月 {j7:?}"
    );
    assert!(j7.iter().all(|(s, _)| s.as_str() >= "2026-07-01"), "{j7:?}");
    for (a0, a1) in &j6 {
        for (b0, b1) in &j7 {
            assert!(
                a1 <= b0 || b1 <= a0,
                "重なった: 6 月 {a0}〜{a1} / 7 月 {b0}〜{b1}"
            );
        }
    }
}
