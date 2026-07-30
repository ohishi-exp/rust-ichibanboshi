//! 畳んだ勤怠の保存を**実 Postgres** で確かめる (Refs #205 実装計画 05 / 06)。
//!
//! ここでしか確かめられないのは、`kosoku.rs` の出力が **DB の制約を実際に通るか**。
//! `day_parts` の `BETWEEN 0 AND 1440` も `shifts` の `end_at > start_at` も
//! `day_summaries` の FK / CHECK (002) も、行を投げるまで分からない。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ丸ごと skip する
//! (`tests/kintai_push_pg_test.rs` と同じ)。

use async_trait::async_trait;
use chrono::NaiveDate;
use rust_ichibanboshi::kintai_fold::{recalc_month, sync_month};
use rust_ichibanboshi::kintai_push::{KintaiPgStore, PushOptions};
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use rust_ichibanboshi::kosoku::{KosokuParams, RestraintRounding};
use serde_json::json;

// ── 前提 ──────────────────────────────────────────────────────────────────

async fn store() -> Option<(KintaiPgStore, sqlx::PgPool)> {
    let url = std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    ensure_schema(&pool).await;
    Some((
        KintaiPgStore::from_pool(pool.clone(), uuid::Uuid::new_v4()),
        pool,
    ))
}

async fn ensure_schema(pool: &sqlx::PgPool) {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(205_050_001_i64)
        .execute(pool)
        .await
        .expect("lock");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'kintai')")
            .fetch_one(pool)
            .await
            .expect("probe");
    if !exists {
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir("migrations")
            .expect("migrations")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
            .collect();
        files.sort();
        for f in files {
            let sql = std::fs::read_to_string(&f).expect("read");
            sqlx::raw_sql(&sql)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", f.display()));
        }
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(205_050_001_i64)
        .execute(pool)
        .await
        .expect("unlock");
}

macro_rules! require_db {
    () => {
        match store().await {
            Some(v) => v,
            None => return,
        }
    };
}

struct StubRepo {
    rows: std::sync::Mutex<Vec<serde_json::Value>>,
}

impl StubRepo {
    fn new(rows: Vec<serde_json::Value>) -> Self {
        Self {
            rows: std::sync::Mutex::new(rows),
        }
    }
    fn all(&self) -> Vec<serde_json::Value> {
        self.rows.lock().unwrap().clone()
    }
}

#[async_trait]
impl KintaiEventsApi for StubRepo {
    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self
            .all()
            .into_iter()
            .filter(|r| r["driver_id"].as_u64() == Some(driver))
            .collect())
    }
    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self.all())
    }
    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(Vec::new())
    }
}

const DRIVER: u64 = 1194;

fn punch(at: &str, state: &str) -> serde_json::Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "timecard", "state": state, "unko_no": null})
}

fn repo(rows: Vec<serde_json::Value>) -> DynKintaiEventsRepo {
    std::sync::Arc::new(StubRepo::new(rows))
}

fn params() -> KosokuParams {
    KosokuParams::default()
}

async fn shift_count(pool: &sqlx::PgPool, t: uuid::Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM kintai.shifts WHERE tenant_id = $1")
        .bind(t)
        .fetch_one(pool)
        .await
        .expect("count shifts")
}

// ── 日跨ぎ・長時間拘束が DB の制約を通るか ─────────────────────────────────

/// **日跨ぎ勤務は `day_parts` が 2 行以上になり、各行は 1440 分以内。**
///
/// 1440 の上限は DB の CHECK なので、INSERT が通ること自体が検査になる。
#[tokio::test]
async fn a_shift_crossing_midnight_splits_into_day_parts() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-01 22:00:00", "始業"),
        punch("2026-07-02 09:00:00", "終業"),
    ]);
    let r = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");
    assert_eq!(r.shifts, 1);
    assert!(r.day_parts >= 2, "暦日で割れる: {}", r.day_parts);

    let rows: Vec<(NaiveDate, i32)> = sqlx::query_as(
        "SELECT date, restraint_minutes FROM kintai.day_parts WHERE tenant_id = $1 ORDER BY date",
    )
    .bind(store.tenant_id())
    .fetch_all(&pool)
    .await
    .expect("day_parts");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    assert_eq!(rows[1].0, NaiveDate::from_ymd_opt(2026, 7, 2).unwrap());
    for (_, m) in &rows {
        assert!((0..=1440).contains(m), "1440 を超えた: {m}");
    }
}

/// **24 時間を超える拘束は打ち切らない** (Refs #152)。3 暦日に分かれ、
/// `day_parts` の合計が `day_summaries.restraint_minutes` と一致する。
#[tokio::test]
async fn a_39_hour_shift_spans_three_days_and_the_parts_add_up() {
    let (store, pool) = require_db!();
    // 実測 (乗務員 1674 / 2026-04-07 01:08 → 04-08 16:22 = 39.2 時間) と同じ形
    let repo = repo(vec![
        punch("2026-07-05 22:00:00", "始業"),
        punch("2026-07-07 13:00:00", "終業"),
    ]);
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");

    let total: i32 = sqlx::query_scalar(
        "SELECT restraint_minutes FROM kintai.day_summaries WHERE tenant_id = $1",
    )
    .bind(store.tenant_id())
    .fetch_one(&pool)
    .await
    .expect("summary");
    assert_eq!(total, 39 * 60, "39 時間そのまま (打ち切らない)");

    let parts: Vec<(NaiveDate, i32)> = sqlx::query_as(
        "SELECT date, restraint_minutes FROM kintai.day_parts WHERE tenant_id = $1 ORDER BY date",
    )
    .bind(store.tenant_id())
    .fetch_all(&pool)
    .await
    .expect("parts");
    assert_eq!(parts.len(), 3, "3 暦日: {parts:?}");
    let sum: i32 = parts.iter().map(|(_, m)| m).sum();
    assert_eq!(sum, total, "暦日の合計が勤務の拘束と一致する");
}

/// **月跨ぎ勤務は始業月にだけ勤務が立ち、暦日ビューは両月にまたがる。**
#[tokio::test]
async fn a_shift_crossing_the_month_belongs_to_the_starting_month() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-06-30 23:00:00", "始業"),
        punch("2026-07-01 10:00:00", "終業"),
    ]);

    // 7 月として畳んでも勤務は立たない (始業日が 6 月)
    let july = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("july");
    assert_eq!(july.shifts, 0, "始業月ではないので勤務は出ない");

    let june = recalc_month(&repo, &store, &params(), "2026-06", None, true)
        .await
        .expect("june");
    assert_eq!(june.shifts, 1);

    let dates: Vec<NaiveDate> =
        sqlx::query_scalar("SELECT date FROM kintai.day_parts WHERE tenant_id = $1 ORDER BY date")
            .bind(store.tenant_id())
            .fetch_all(&pool)
            .await
            .expect("parts");
    assert_eq!(
        dates,
        vec![
            NaiveDate::from_ymd_opt(2026, 6, 30).unwrap(),
            NaiveDate::from_ymd_opt(2026, 7, 1).unwrap()
        ],
        "暦日ビューは両月にまたがる"
    );
    // 勤務ビューは 6 月にだけ
    let starts: Vec<NaiveDate> =
        sqlx::query_scalar("SELECT date_start FROM kintai.shifts WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_all(&pool)
            .await
            .expect("shifts");
    assert_eq!(starts, vec![NaiveDate::from_ymd_opt(2026, 6, 30).unwrap()]);
}

// ── 指紋 ──────────────────────────────────────────────────────────────────

/// **2 回連続で走らせると 2 回目は何も書かない。**
#[tokio::test]
async fn a_second_recalc_is_a_no_op() {
    let (store, _pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-10 08:00:00", "始業"),
        punch("2026-07-10 18:00:00", "終業"),
    ]);
    let first = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("1st");
    assert_eq!(first.drivers_written, 1);

    let second = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("2nd");
    assert_eq!(second.drivers_written, 0, "指紋が一致するので書かない");
    assert_eq!(second.drivers_unchanged, 1);
    assert!(!second.wrote_anything());
}

/// **TOML の `restraint_rounding` を切り替えると全単位が stale になる。**
///
/// 指紋に `KosokuParams` が入っていることの検証 — 入れ忘れると、丸め方を変えても
/// 古い集計が永久にスキップされる (#205 のリスク欄)。
#[tokio::test]
async fn changing_the_rounding_makes_every_unit_stale() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-12 08:00:30", "始業"),
        punch("2026-07-12 18:00:20", "終業"),
    ]);
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("1st");
    let before: Vec<String> =
        sqlx::query_scalar("SELECT fingerprint FROM kintai.shifts WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_all(&pool)
            .await
            .expect("fp");

    let other = KosokuParams {
        restraint_rounding: RestraintRounding::TruncateElapsed,
        ..params()
    };
    let again = recalc_month(&repo, &store, &other, "2026-07", None, true)
        .await
        .expect("2nd");
    assert_eq!(again.drivers_written, 1, "丸め方を変えたら書き直す");

    let after: Vec<String> =
        sqlx::query_scalar("SELECT fingerprint FROM kintai.shifts WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_all(&pool)
            .await
            .expect("fp");
    assert_ne!(before, after, "指紋が変わる");
}

/// **`logic_version` に `KINTAI_OUTPUT_SHA` が入る。**
///
/// `kosoku.rs` を 1 バイト直せば必ず変わる値なので、「上げ忘れ」が存在しない。
#[tokio::test]
async fn logic_version_is_the_build_hash() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-14 08:00:00", "始業"),
        punch("2026-07-14 18:00:00", "終業"),
    ]);
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");
    let v: String =
        sqlx::query_scalar("SELECT logic_version FROM kintai.shifts WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("logic_version");
    assert_eq!(v, rust_ichibanboshi::kintai_fold::logic_version());
    assert_eq!(v.len(), 16);
}

/// **`--dry-run` は 1 行も書かない。**
#[tokio::test]
async fn recalc_dry_run_writes_nothing() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-16 08:00:00", "始業"),
        punch("2026-07-16 18:00:00", "終業"),
    ]);
    let r = recalc_month(&repo, &store, &params(), "2026-07", None, false)
        .await
        .expect("dry run");
    assert_eq!(r.drivers_written, 1, "書く対象としては数える");
    assert_eq!(
        shift_count(&pool, store.tenant_id()).await,
        0,
        "書いていない"
    );
}

/// **入力が変わったら畳んだ値も追随する。**
#[tokio::test]
async fn changing_the_input_rewrites_the_folded_rows() {
    let (store, pool) = require_db!();
    let stub = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-18 08:00:00", "始業"),
        punch("2026-07-18 18:00:00", "終業"),
    ]));
    let repo: DynKintaiEventsRepo = stub.clone();
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("1st");
    let before: i32 = sqlx::query_scalar(
        "SELECT restraint_minutes FROM kintai.day_summaries WHERE tenant_id = $1",
    )
    .bind(store.tenant_id())
    .fetch_one(&pool)
    .await
    .expect("before");
    assert_eq!(before, 600);

    *stub.rows.lock().unwrap() = vec![
        punch("2026-07-18 08:00:00", "始業"),
        punch("2026-07-18 19:00:00", "終業"),
    ];
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("2nd");
    let after: i32 = sqlx::query_scalar(
        "SELECT restraint_minutes FROM kintai.day_summaries WHERE tenant_id = $1",
    )
    .bind(store.tenant_id())
    .fetch_one(&pool)
    .await
    .expect("after");
    assert_eq!(after, 660, "1 時間伸びた");
    assert_eq!(
        shift_count(&pool, store.tenant_id()).await,
        1,
        "古い行は残らない"
    );
}

/// **勤務を消すと `day_summaries` / `day_parts` も消える** (002 の FK CASCADE)。
///
/// 残ると月合計に二重に載る。
#[tokio::test]
async fn deleting_a_shift_cascades_to_the_derived_rows() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-20 22:00:00", "始業"),
        punch("2026-07-21 09:00:00", "終業"),
    ]);
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM kintai.day_parts WHERE tenant_id = $1")
        .bind(store.tenant_id())
        .fetch_one(&pool)
        .await
        .expect("parts");
    assert!(n >= 2);

    sqlx::query("DELETE FROM kintai.shifts WHERE tenant_id = $1")
        .bind(store.tenant_id())
        .execute(&pool)
        .await
        .expect("delete shifts");
    for table in ["day_summaries", "day_parts"] {
        let left: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM kintai.{table} WHERE tenant_id = $1"
        ))
        .bind(store.tenant_id())
        .fetch_one(&pool)
        .await
        .expect("left");
        assert_eq!(left, 0, "{table} が残った");
    }
}

// ── 06: sync ──────────────────────────────────────────────────────────────

/// **`sync` は push と再計算を 1 回で回す。**
///
/// 「push しただけで計算していない」状態を作らないのが 06 の目的。
#[tokio::test]
async fn sync_pushes_and_folds_in_one_pass() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-22 08:00:00", "始業"),
        punch("2026-07-22 18:00:00", "終業"),
    ]);
    let opts = PushOptions {
        month: "2026-07".to_string(),
        driver: None,
        apply: true,
    };
    let r = sync_month(&repo, &store, &params(), &opts)
        .await
        .expect("sync");
    assert_eq!(r.push.days_changed, 1);
    assert_eq!(r.fold.drivers_written, 1);
    assert!(!r.has_unexpected());

    // 入力層と畳んだ層の両方が埋まっている
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("events");
    assert_eq!(events, 2);
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 1);

    // 2 回目は両方とも据え置き
    let again = sync_month(&repo, &store, &params(), &opts)
        .await
        .expect("2nd");
    assert!(!again.push.wrote_anything());
    assert!(!again.fold.wrote_anything());
}

/// **`sync --dry-run` はどちらの層にも書かない。**
#[tokio::test]
async fn sync_dry_run_writes_to_neither_layer() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-24 08:00:00", "始業"),
        punch("2026-07-24 18:00:00", "終業"),
    ]);
    let opts = PushOptions {
        month: "2026-07".to_string(),
        driver: None,
        apply: false,
    };
    sync_month(&repo, &store, &params(), &opts)
        .await
        .expect("sync");
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("events");
    assert_eq!(events, 0);
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
}

/// **1 乗務員に絞れる。**
#[tokio::test]
async fn driver_filter_touches_only_that_driver() {
    let (store, pool) = require_db!();
    let other = |at: &str, state: &str| json!({"datetime": at, "end_datetime": null, "driver_id": 1131, "source": "timecard", "state": state, "unko_no": null});
    let repo = repo(vec![
        punch("2026-07-26 08:00:00", "始業"),
        punch("2026-07-26 18:00:00", "終業"),
        other("2026-07-26 08:00:00", "始業"),
        other("2026-07-26 18:00:00", "終業"),
    ]);
    let r = recalc_month(&repo, &store, &params(), "2026-07", Some(DRIVER), true)
        .await
        .expect("recalc");
    assert_eq!(r.drivers, 1);

    let drivers: Vec<i64> =
        sqlx::query_scalar("SELECT driver_cd FROM kintai.shifts WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_all(&pool)
            .await
            .expect("drivers");
    assert_eq!(drivers, vec![DRIVER as i64]);
}
