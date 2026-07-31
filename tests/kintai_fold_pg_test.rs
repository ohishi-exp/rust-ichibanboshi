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
use rust_ichibanboshi::kintai_fold::{
    fold_driver_month, fold_month, month_gate_report, recalc_driver_page, recalc_drivers,
    recalc_month, stale_state, stored_state, stored_states, sync_month, write_unit, DayPartRow,
    DaySummaryRow, FoldUnit, MonthGate, ShiftRow,
};
use rust_ichibanboshi::kintai_http_repo::with_warning_sink;
use rust_ichibanboshi::kintai_push::{KintaiPgStore, PushOptions, TimecardWindow};
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use rust_ichibanboshi::kosoku::{KosokuParams, RestraintRounding};
use rust_ichibanboshi::routes::kintai_recalc::{recalc, RecalcRequest};
use rust_ichibanboshi::routes::kintai_timecard::{receive_window, ReadTenant};
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

/// psql の**クライアント側変数** (`:'name'`) を使う migration か。
///
/// `sqlx::raw_sql` は psql ではないので `:'…'` がそのままサーバへ行き
/// `syntax error at or near ":"` になる。003
/// (`ALTER ROLE kintai_writer WITH PASSWORD :'kintai_writer_password'`) がこれ。
///
/// **この harness では飛ばしてよい** — 用意するのは `kintai` スキーマで、資格情報は
/// スキーマではない。詳細は `kintai_push_pg_test.rs` の同名関数。
fn needs_psql_variables(sql: &str) -> bool {
    sql.contains(":'") || sql.contains(":\"")
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
            if needs_psql_variables(&sql) {
                // **黙って飛ばさない** — 何を流していないかはログに出す
                eprintln!("skip {} (psql の変数を使う migration)", f.display());
                continue;
            }
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

/// `dtako_events` の休息区間。**`source` が `timecard` / `dtako` のどちらでもない**
/// ので、push / diff 用の読み (`fetch_timecard_events_between`) からは落ちる。
fn rest(start: &str, end: &str) -> serde_json::Value {
    json!({"datetime": start, "end_datetime": end, "driver_id": DRIVER, "source": "dtako_events", "state": "休息", "unko_no": null})
}

/// `time_card_dtako` の運行イベント。
fn run(at: &str, state: &str, unko: &str) -> serde_json::Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "dtako", "state": state, "unko_no": unko})
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

// ── 畳む側は全イベントを読む ───────────────────────────────────────────────

/// **打刻が 1 件も無くても休息イベントで勤務が立つ** (Refs #118 / #205)。
///
/// 畳む側が `kintai_push::read_driver_events` を呼ぶと、#225 で「打刻 2 表だけ」に
/// 絞られた読みが返り `dtako_events` の休息が落ちる。`kosoku::daily_summary` は
/// それで勤務を切るので、休息由来の勤務 (長距離・日跨ぎの乗務員はこちらしか無い)
/// が**丸ごと消えて静かに 0 になる**。ここが 0 に戻ったらその回帰。
#[tokio::test]
async fn rest_events_still_produce_shifts_after_the_fold() {
    let (store, pool) = require_db!();
    // 打刻は 1 件も無い。休息の終了 = 始業、次の休息の開始 = 終業
    let repo = repo(vec![
        rest("2026-07-01 16:19:00", "2026-07-02 04:42:00"),
        run("2026-07-02 06:00:00", "運行開始", "A"),
        run("2026-07-02 14:10:00", "運行終了", "A"),
        rest("2026-07-02 16:18:00", "2026-07-03 06:01:00"),
    ]);
    let r = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");
    assert_eq!(r.shifts, 1, "休息で切った勤務が立つ");

    let source: String =
        sqlx::query_scalar("SELECT shift_source FROM kintai.shifts WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("shift_source");
    assert_eq!(source, "rest", "打刻ではなく休息が境界を決めた");
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

/// **保存行の `logic_version` はコードと設定を畳んだ値。**
///
/// `kosoku.rs` を 1 バイト直しても、TOML の閾値を変えても必ず変わる。
#[tokio::test]
async fn logic_version_is_the_code_and_settings_hash() {
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
    assert_eq!(v, rust_ichibanboshi::kintai_fold::logic_version(&params()));
    assert_eq!(v.len(), 16);
}

/// **設定を変えた stale が `SELECT DISTINCT logic_version` 1 発で捕まる。**
///
/// 上のテストが確かめているのは「指紋が変わって畳み直される」こと。こちらは
/// 「畳み直しが要ることを**読むだけで判る**」ことで、`logic_version` の材料に
/// `KosokuParams` を入れたのはそのため (入れないと保存行の版が据え置きになる)。
#[tokio::test]
async fn changing_the_rounding_shows_up_in_the_stored_logic_version() {
    let (store, _pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-21 08:00:00", "始業"),
        punch("2026-07-21 18:00:00", "終業"),
    ]);
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");

    let months = ["2026-07".to_string()];
    let fresh = stale_state(&store, &months, &params())
        .await
        .expect("fresh");
    assert_eq!(fresh.drivers, 0, "畳んだ直後は古い版が無い");
    assert_eq!(fresh.versions, vec![fresh.logic_version.clone()]);

    // TOML だけを変える (再ビルドしていない = KINTAI_OUTPUT_SHA は同じ)
    let other = KosokuParams {
        restraint_rounding: RestraintRounding::TruncateElapsed,
        ..params()
    };
    let stale = stale_state(&store, &months, &other).await.expect("stale");
    assert_ne!(stale.logic_version, fresh.logic_version, "版が変わる");
    assert_eq!(stale.drivers, 1, "保存済みの乗務員が丸ごと stale になる");
    assert_eq!(
        stale.versions,
        vec![fresh.logic_version],
        "保存行は据え置き"
    );
}

/// **全量再計算のページングは既に畳んである乗務員を Postgres 側だけで拾える。**
///
/// `extra` を空で渡しても、一度でも畳んだ乗務員 (`day_summaries` に行がある)
/// は母集団に残る。rest-only 乗務員のための `extra` の埋め方は
/// [`the_recalc_page_pulls_in_rest_only_drivers_via_extra`] が確かめる。
#[tokio::test]
async fn the_recalc_page_walks_drivers_from_postgres() {
    let (store, _pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-23 08:00:00", "始業"),
        punch("2026-07-23 18:00:00", "終業"),
    ]);
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");

    let none = std::collections::BTreeSet::new();
    let all = recalc_driver_page(&store, "2026-07", &params(), &none, None, false, 50)
        .await
        .expect("page");
    assert_eq!(all, vec![DRIVER as i64], "畳んだ乗務員が母集団に居る");

    // 現行版で畳んであるので stale_only では出てこない
    let stale = recalc_driver_page(&store, "2026-07", &params(), &none, None, true, 50)
        .await
        .expect("stale page");
    assert!(stale.is_empty(), "{stale:?}");

    // 設定を変えると stale 側に出る
    let other = KosokuParams {
        restraint_rounding: RestraintRounding::TruncateElapsed,
        ..params()
    };
    let stale = recalc_driver_page(&store, "2026-07", &other, &none, None, true, 50)
        .await
        .expect("stale page");
    assert_eq!(stale, vec![DRIVER as i64]);

    // after_driver_cd は「その先」だけ
    let after = recalc_driver_page(
        &store,
        "2026-07",
        &params(),
        &none,
        Some(DRIVER as i64),
        false,
        50,
    )
    .await
    .expect("after");
    assert!(after.is_empty(), "{after:?}");
}

/// **打刻ゼロ・rest-only 乗務員は Postgres だけでは母集団に入らず、`extra` で拾う** (#205-12)。
///
/// `kintai_events` (打刻由来) にも `day_summaries` (畳んだ結果) にも 1 行も無い
/// 乗務員は、月次バッチが「まだ一度も見たことが無い」乗務員そのもの。休息イベント
/// だけで勤務が立つ乗務員 (長距離・日跨ぎ) がこれに当たる
/// ([`rest_events_still_produce_shifts_after_the_fold`] と同じ入力の形)。
#[tokio::test]
async fn the_recalc_page_pulls_in_rest_only_drivers_via_extra() {
    let (store, _pool) = require_db!();
    const REST_ONLY: u64 = 1777;
    let rest_of = |driver: u64, start: &str, end: &str| json!({"datetime": start, "end_datetime": end, "driver_id": driver, "source": "dtako_events", "state": "休息", "unko_no": null});
    let repo = repo(vec![
        rest_of(REST_ONLY, "2026-07-01 16:19:00", "2026-07-02 04:42:00"),
        rest_of(REST_ONLY, "2026-07-02 16:18:00", "2026-07-03 06:01:00"),
    ]);
    let units = fold_month(&repo, &params(), "2026-07", None)
        .await
        .expect("fold_month");
    assert!(
        units.iter().any(|(cd, _, _)| *cd == REST_ONLY),
        "前提: rest-only 乗務員は fold_month の split_by_driver には出る"
    );

    // Postgres 側だけ (extra 空) では、まだ 1 度も畳んでいないので漏れる — 修正前の回帰
    let none = std::collections::BTreeSet::new();
    let missing = recalc_driver_page(&store, "2026-07", &params(), &none, None, false, 50)
        .await
        .expect("page without extra");
    assert!(
        !missing.contains(&(REST_ONLY as i64)),
        "Postgres だけでは rest-only 乗務員は母集団に無い: {missing:?}"
    );

    // extra に split_by_driver のキーを渡すと拾える
    let extra: std::collections::BTreeSet<i64> =
        units.iter().map(|(cd, _, _)| *cd as i64).collect();
    let page = recalc_driver_page(&store, "2026-07", &params(), &extra, None, false, 50)
        .await
        .expect("page with extra");
    assert!(
        page.contains(&(REST_ONLY as i64)),
        "extra で rest-only 乗務員が母集団に入る: {page:?}"
    );
}

/// **driver_cd 0 は `extra` に混ぜても母集団に出ない** (push 経路と同じ「0 以下は捨てる」)。
#[tokio::test]
async fn driver_cd_zero_is_dropped_even_via_extra() {
    let (store, _pool) = require_db!();
    let extra: std::collections::BTreeSet<i64> = [0_i64].into_iter().collect();
    let page = recalc_driver_page(&store, "2026-07", &params(), &extra, None, false, 50)
        .await
        .expect("page");
    assert!(!page.contains(&0), "{page:?}");
}

/// **名指しした乗務員だけを畳む。** 窓の受け口が apply 後に呼ぶ形。
///
/// 空の並びでは読み先を 1 度も叩かない — 差分ゼロの窓で月ぶんの生イベントを
/// 毎回引かないため。
#[tokio::test]
async fn recalc_drivers_only_touches_the_named_ones() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-24 08:00:00", "始業"),
        punch("2026-07-24 18:00:00", "終業"),
    ]);

    let none = recalc_drivers(&repo, &store, &params(), "2026-07", &[], true)
        .await
        .expect("empty");
    assert_eq!(none.drivers, 0);
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
    assert_eq!(none.logic_version.len(), 16, "版は空でも載せる");
    assert!(!none.calculated_at.is_empty(), "計算時刻も載せる");

    let one = recalc_drivers(&repo, &store, &params(), "2026-07", &[DRIVER], true)
        .await
        .expect("one");
    assert_eq!(one.drivers, 1);
    assert_eq!(one.drivers_written, 1);
    assert!(!one.dry_run);
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 1);

    // 2 回目は指紋一致で据え置き (#205 テスト計画「2 回連続で 2 回目差分ゼロ」)
    let twice = recalc_drivers(&repo, &store, &params(), "2026-07", &[DRIVER], true)
        .await
        .expect("twice");
    assert_eq!(twice.drivers_written, 0, "2 回目は 1 行も書かない");
    assert_eq!(twice.drivers_unchanged, 1);
}

// ── 窓の受け口が畳むところまで束ねるか (06 の HTTP 版) ─────────────────────

/// `POST /api/kintai/timecard/window` を直接呼ぶ。
async fn post_window(
    store: &KintaiPgStore,
    repo: &DynKintaiEventsRepo,
    read_tenant: Option<uuid::Uuid>,
    window: TimecardWindow,
) -> Result<serde_json::Value, (axum::http::StatusCode, String)> {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "X-Tenant-ID",
        store.tenant_id().to_string().parse().unwrap(),
    );
    receive_window(
        headers,
        axum::Extension(Some(std::sync::Arc::new(
            store.for_tenant(store.tenant_id()),
        ))),
        axum::Extension(repo.clone()),
        axum::Extension(std::sync::Arc::new(params())),
        axum::Extension(ReadTenant(read_tenant)),
        axum::Json(window),
    )
    .await
    .map(|j| j.0)
}

fn window(dry_run: bool) -> TimecardWindow {
    TimecardWindow {
        months: vec!["2026-07".to_string()],
        drivers: vec![DRIVER as i64],
        events: vec![
            punch("2026-07-27 08:00:00", "始業"),
            punch("2026-07-27 18:00:00", "終業"),
        ],
        dry_run,
        fold: true,
    }
}

/// **打刻を運んだら、その場で畳むところまで行く** (#205 のリスク欄の筆頭)。
///
/// 「push しただけで計算していない」状態を作らない。読み出しは計算しないので、
/// 畳んだ値が古いままだと遅いのではなく静かに間違う。
#[tokio::test]
async fn the_window_folds_what_it_just_wrote() {
    let (store, pool) = require_db!();
    let repo = repo(window(false).events);

    let got = post_window(&store, &repo, Some(store.tenant_id()), window(false))
        .await
        .expect("window");
    assert_eq!(got["days_written"], 1, "打刻は書けた");
    assert_eq!(got["drivers_changed"], serde_json::json!([DRIVER as i64]));
    assert_eq!(got["fold"]["drivers"], 1, "変わった乗務員だけ畳む");
    assert_eq!(got["fold"]["drivers_written"], 1);
    assert_eq!(got["fold"]["dry_run"], false);
    assert_eq!(got["fold"]["logic_version"].as_str().unwrap().len(), 16);
    assert!(!got["fold"]["calculated_at"].as_str().unwrap().is_empty());
    assert_eq!(
        shift_count(&pool, store.tenant_id()).await,
        1,
        "畳んで書けた"
    );

    // 畳んだ直後なので stale は無い
    assert_eq!(got["stale"]["drivers"], 0);
    assert_eq!(got["stale"]["logic_version"], got["fold"]["logic_version"]);

    // **同じ窓をもう一度 → 打刻も畳みも動かない** (#205 テスト計画)
    let again = post_window(&store, &repo, Some(store.tenant_id()), window(false))
        .await
        .expect("again");
    assert_eq!(again["days_written"], 0, "打刻に差分が無い");
    assert_eq!(again["fold"]["drivers"], 0, "差分ゼロなら読み先も叩かない");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 1);
}

/// **dry-run の窓は 1 行も書かないが、畳んだらどうなるかは報告する。**
#[tokio::test]
async fn a_dry_run_window_reports_the_fold_without_writing() {
    let (store, pool) = require_db!();
    let repo = repo(window(true).events);

    let got = post_window(&store, &repo, Some(store.tenant_id()), window(true))
        .await
        .expect("dry window");
    assert_eq!(got["dry_run"], true);
    assert_eq!(got["days_written"], 1, "書く対象としては数える");
    assert_eq!(got["fold"]["dry_run"], true, "件数を実績と読み違えさせない");
    assert_eq!(
        shift_count(&pool, store.tenant_id()).await,
        0,
        "1 行も書かない"
    );

    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("count events");
    assert_eq!(events, 0, "打刻も書いていない");
}

/// **畳めなくても打刻の反映は成功として返す。**
///
/// `[kintai_events]` が未設定の instance では読み先が `NotConfigured` を返す
/// (2026-07-31 の本番 revision 00008 が実際にこの状態だった)。ここで
/// リクエスト全体を 5xx にすると relay は「窓ごと失敗」と読んで**同じ窓を再送し
/// 続ける** — 打刻は毎回冪等に書き直され、fold は毎回同じ理由で落ちる。
///
/// 200 のまま `fold_error` に載せる。「push しただけで計算していない状態を
/// 作らない」とは矛盾しない — **畳めなかったことが応答に明示される**のが、
/// その状態を黙って作らないということ。
#[tokio::test]
async fn a_fold_failure_does_not_hide_a_successful_apply() {
    let (store, pool) = require_db!();
    // 読み先が無い instance (= 本番の KINTAI_EVENTS_* 未投入と同じ状態)
    let dead: DynKintaiEventsRepo =
        std::sync::Arc::new(rust_ichibanboshi::kintai_repo::DisabledKintaiEventsRepo);

    let got = post_window(&store, &dead, Some(store.tenant_id()), window(false))
        .await
        .expect("窓ごと失敗にしない");

    assert_eq!(got["days_written"], 1, "打刻は反映されている");
    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("count events");
    assert_eq!(events, 2, "打刻はコミット済み");

    // 畳めなかったことが応答に出ている
    assert!(
        got.get("fold").is_none(),
        "畳めていないので報告は無い: {got}"
    );
    let err = got["fold_error"].as_str().expect("fold_error: {got}");
    assert!(err.contains("kintai events"), "{err}");
    assert_eq!(got["fold_error_status"], 502);

    // stale は別の口なので生きている — 「畳み直しが要る」ことが読める
    assert_eq!(got["stale"]["drivers"], 0, "まだ 1 行も畳んでいない");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
}

/// **読みと書きが別テナントなら、打刻を書く前に 403。**
///
/// 書いてから断ると、書いた打刻だけが残って畳まれない状態ができる。
#[tokio::test]
async fn a_split_tenant_is_refused_before_anything_is_written() {
    let (store, pool) = require_db!();
    let repo = repo(window(false).events);
    let other = uuid::Uuid::new_v4();

    let (code, msg) = post_window(&store, &repo, Some(other), window(false))
        .await
        .unwrap_err();
    assert_eq!(code, axum::http::StatusCode::FORBIDDEN);
    assert!(msg.contains("kintai_events"), "{msg}");

    let events: i64 =
        sqlx::query_scalar("SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("count events");
    assert_eq!(events, 0, "打刻を書く前に断っている");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
}

/// **`fold = false` の窓は運ぶだけ。** テナントの突き合わせも起きない。
#[tokio::test]
async fn a_window_can_carry_without_folding() {
    let (store, pool) = require_db!();
    let repo = repo(window(false).events);
    let got = post_window(
        &store,
        &repo,
        Some(uuid::Uuid::new_v4()),
        TimecardWindow {
            fold: false,
            ..window(false)
        },
    )
    .await
    .expect("carry only");
    assert_eq!(got["days_written"], 1);
    assert!(got.get("fold").is_none(), "畳んでいないので報告も無い");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
}

/// **`apply` が無ければ 1 行も書かず、報告が dry-run だと名乗る。**
#[tokio::test]
async fn recalc_drivers_dry_run_writes_nothing_and_says_so() {
    let (store, pool) = require_db!();
    let repo = repo(vec![
        punch("2026-07-25 08:00:00", "始業"),
        punch("2026-07-25 18:00:00", "終業"),
    ]);
    let r = recalc_drivers(&repo, &store, &params(), "2026-07", &[DRIVER], false)
        .await
        .expect("dry");
    assert!(r.dry_run, "件数を実績と読み違えさせない");
    assert_eq!(r.drivers_written, 1, "書く対象としては数える");
    assert_eq!(
        shift_count(&pool, store.tenant_id()).await,
        0,
        "書いていない"
    );
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

// ── 書き込みを unnest に畳む (Refs #231 と同型) ─────────────────────────────

/// `day_summaries` の分の列 1 本。DB の列名と、畳んだ行から同じ値を取る関数。
type MinuteColumn = (&'static str, fn(&DaySummaryRow) -> i64);

/// **列ごとの配列に畳んでも 1 行 1 INSERT と同じものが入る。**
///
/// `unnest` は列を配列に分解して渡すので、**並べる順を 1 本間違えると
/// 「実働の列に休憩が入る」ような静かな取り違え**になる。型が全部 `int4` なので
/// DB も気付かない。畳む前の [`FoldUnit`] と DB の中身を列ごとに突き合わせる。
#[tokio::test]
async fn the_unnest_write_matches_the_folded_unit_column_by_column() {
    use sqlx::Row;

    let (store, pool) = require_db!();
    // 1 か月ぶん。日ごとに終業をずらして、全列が日ごとに違う値になるようにする
    let mut rows = Vec::new();
    for d in 1..=28_u32 {
        rows.push(punch(&format!("2026-07-{d:02} 08:00:00"), "始業"));
        rows.push(punch(&format!("2026-07-{d:02} 18:{:02}:00", d), "終業"));
    }
    let repo = repo(rows.clone());
    let r = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");
    assert_eq!(r.shifts, 28);

    // 畳んだ結果そのもの。DB に入った姿と 1 列ずつ比べる
    let (unit, _fp) = fold_driver_month(DRIVER as i64, "2026-07", &params(), rows);
    assert_eq!(unit.day_summaries.len(), 28);

    let stored = sqlx::query(
        "SELECT date::text AS d,
                to_char(shift_start_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS s,
                shift_source, restraint_minutes, working_minutes, break_minutes,
                rest_minus_minutes, statutory_minutes, within_statutory_overtime_minutes,
                overtime_minutes, legal_holiday_minutes, night_minutes,
                overtime_night_minutes, legal_holiday_night_minutes
           FROM kintai.day_summaries WHERE tenant_id = $1 ORDER BY shift_start_at",
    )
    .bind(store.tenant_id())
    .fetch_all(&pool)
    .await
    .expect("day_summaries");
    assert_eq!(stored.len(), unit.day_summaries.len());

    let minutes: [MinuteColumn; 11] = [
        ("restraint_minutes", |d| d.restraint_minutes),
        ("working_minutes", |d| d.working_minutes),
        ("break_minutes", |d| d.break_minutes),
        ("rest_minus_minutes", |d| d.rest_minus_minutes),
        ("statutory_minutes", |d| d.statutory_minutes),
        ("within_statutory_overtime_minutes", |d| {
            d.within_statutory_overtime_minutes
        }),
        ("overtime_minutes", |d| d.overtime_minutes),
        ("legal_holiday_minutes", |d| d.legal_holiday_minutes),
        ("night_minutes", |d| d.night_minutes),
        ("overtime_night_minutes", |d| d.overtime_night_minutes),
        ("legal_holiday_night_minutes", |d| {
            d.legal_holiday_night_minutes
        }),
    ];
    for (got, want) in stored.iter().zip(&unit.day_summaries) {
        assert_eq!(got.get::<String, _>("d"), want.date.to_string());
        assert_eq!(
            got.get::<String, _>("s"),
            want.shift_start_at.format("%Y-%m-%d %H:%M:%S").to_string()
        );
        assert_eq!(got.get::<String, _>("shift_source"), want.shift_source);
        for (col, f) in minutes {
            assert_eq!(got.get::<i32, _>(col) as i64, f(want), "{col} が食い違う");
        }
    }

    // 拘束が日ごとに違う = 列を取り違えたら必ず落ちるだけの分散がある
    let distinct: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT restraint_minutes) FROM kintai.day_summaries WHERE tenant_id = $1",
    )
    .bind(store.tenant_id())
    .fetch_one(&pool)
    .await
    .expect("distinct");
    assert!(distinct > 1, "全日同じ値では取り違えを検知できない");

    // 2 回目は指紋が一致して 1 行も書かない
    let again = recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("2nd");
    assert_eq!(again.drivers_written, 0, "unnest 化しても据え置きになる");
    assert_eq!(again.drivers_unchanged, 1);
    assert!(!again.wrote_anything());
}

/// **INSERT の刻みを跨いでも落ちない。**
///
/// [`write_unit`] は push 側と同じ `INSERT_CHUNK` (2000) 行で刻む。刻んでも同じ
/// トランザクションの中に居るので、全か無かは変わらない。`kosoku.rs` を通すと
/// 2000 勤務は作れないので、畳んだ形を直に渡す。
#[tokio::test]
async fn write_unit_survives_crossing_the_insert_chunk() {
    let (store, pool) = require_db!();
    const N: i64 = 2100; // INSERT_CHUNK = 2000 を跨ぐ
    let base = NaiveDate::from_ymd_opt(2026, 7, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();

    let mut unit = FoldUnit {
        driver_cd: DRIVER as i64,
        ..Default::default()
    };
    for i in 0..N {
        let start = base + chrono::Duration::minutes(i * 2);
        let end = start + chrono::Duration::minutes(1);
        unit.shifts.push(ShiftRow {
            driver_cd: DRIVER as i64,
            start_at: start,
            end_at: end,
            shift_source: "timecard",
        });
        unit.day_summaries.push(DaySummaryRow {
            driver_cd: DRIVER as i64,
            date: start.date(),
            shift_start_at: start,
            shift_source: "timecard",
            restraint_minutes: i,
            working_minutes: 0,
            break_minutes: 0,
            rest_minus_minutes: 0,
            statutory_minutes: 0,
            within_statutory_overtime_minutes: 0,
            overtime_minutes: 0,
            legal_holiday_minutes: 0,
            night_minutes: 0,
            overtime_night_minutes: 0,
            legal_holiday_night_minutes: 0,
        });
        unit.day_parts.push(DayPartRow {
            driver_cd: DRIVER as i64,
            shift_start_at: start,
            date: start.date(),
            restraint_minutes: 1,
            working_minutes: 1,
            night_minutes: 0,
        });
    }

    write_unit(&store, "2026-07", &unit, &"a".repeat(64), &params())
        .await
        .expect("write");

    assert_eq!(shift_count(&pool, store.tenant_id()).await, N);
    for table in ["day_summaries", "day_parts"] {
        let n: i64 = sqlx::query_scalar(&format!(
            "SELECT count(*) FROM kintai.{table} WHERE tenant_id = $1"
        ))
        .bind(store.tenant_id())
        .fetch_one(&pool)
        .await
        .expect("count");
        assert_eq!(n, N, "{table} が刻みを跨げていない");
    }
    // 刻みの境目 (2000 行目の前後) が取り違わっていない
    let last: i32 = sqlx::query_scalar(
        "SELECT restraint_minutes FROM kintai.day_summaries
          WHERE tenant_id = $1 ORDER BY shift_start_at DESC LIMIT 1",
    )
    .bind(store.tenant_id())
    .fetch_one(&pool)
    .await
    .expect("last");
    assert_eq!(last as i64, N - 1);
}

// ── 畳めるものが無い乗務員 ─────────────────────────────────────────────────

/// **勤務が 1 本も立たない乗務員は「書いた」に数えない。**
///
/// 空の単位は指紋を 1 つも保存しないので、指紋の一致だけで判定すると毎回
/// stale になる。毎回 `drivers_written` に乗ると [`FoldReport::wrote_anything`]
/// が誤検知し、`sync` が「何か書いた」と言い続ける。
#[tokio::test]
async fn a_driver_with_nothing_to_fold_is_not_counted_as_written() {
    let (store, pool) = require_db!();
    let repo = repo(Vec::new());
    for pass in ["1st", "2nd"] {
        let r = recalc_month(&repo, &store, &params(), "2026-07", Some(DRIVER), true)
            .await
            .expect(pass);
        assert_eq!(r.drivers, 1, "{pass}: 対象には数える");
        assert_eq!(r.drivers_written, 0, "{pass}: 書くものが無い");
        assert_eq!(r.drivers_unchanged, 1, "{pass}");
        assert!(!r.wrote_anything(), "{pass}");
    }
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
}

/// **保存済みの行が残っているなら、今回が空でも消しに行く。**
///
/// 「空 = 空は current」を入れたときに、消す側まで黙らせていないことの確認。
#[tokio::test]
async fn an_emptied_month_still_clears_the_stored_rows() {
    let (store, pool) = require_db!();
    let stub = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-28 08:00:00", "始業"),
        punch("2026-07-28 18:00:00", "終業"),
    ]));
    let repo: DynKintaiEventsRepo = stub.clone();
    recalc_month(&repo, &store, &params(), "2026-07", Some(DRIVER), true)
        .await
        .expect("1st");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 1);

    // 上流からイベントが消えた (取り込みの取り消し等)
    stub.rows.lock().unwrap().clear();
    let r = recalc_month(&repo, &store, &params(), "2026-07", Some(DRIVER), true)
        .await
        .expect("2nd");
    assert_eq!(r.drivers_written, 1, "空にするのも書き込み");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 0);
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

// ── 13: 月ゲート ─────────────────────────────────────────────────────────
//
// `StubRepo` は `fetch_dtako_month_digest` の既定実装 (`Ok(None)`) のままなので、
// 上のテストは全て「alc に口が無い環境」を通しており、月ゲートに一切触れない
// (既存の回帰を壊さないことの裏付けでもある)。ここだけ `GatedRepo` に差し替えて
// gate の hit / miss を作る。

/// [`StubRepo`] + 差し替え可能な dtako 側 digest。`fetch_all_events_between` の
/// 呼び出し回数を数え、gate が刺さったときに本当に**読みそのものを省いているか**
/// (指紋が一致して書かないだけでなく、fold_month の R2 相当の読みも通らないか) を
/// 確かめられるようにする。
struct GatedRepo {
    inner: StubRepo,
    digest: std::sync::Mutex<String>,
    read_calls: std::sync::atomic::AtomicUsize,
}

impl GatedRepo {
    fn new(rows: Vec<serde_json::Value>, digest: &str) -> Self {
        Self {
            inner: StubRepo::new(rows),
            digest: std::sync::Mutex::new(digest.to_string()),
            read_calls: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn set_digest(&self, digest: &str) {
        *self.digest.lock().unwrap() = digest.to_string();
    }

    fn read_calls(&self) -> usize {
        self.read_calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl KintaiEventsApi for GatedRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_events_between(from, to, driver).await
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.read_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.fetch_all_events_between(from, to).await
    }

    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_ferry_between(from, to, driver).await
    }

    async fn fetch_dtako_month_digest(
        &self,
        _month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        Ok(Some(self.digest.lock().unwrap().clone()))
    }
}

/// `Arc<GatedRepo>` (呼び出し回数・digest 差し替え用) と、そこから作った
/// `DynKintaiEventsRepo` (recalc_month / recalc_drivers に渡す trait object) の組。
/// 呼び出し先が要求する型がトレイトオブジェクトなので、具体型のまま渡せない。
fn gated(
    rows: Vec<serde_json::Value>,
    digest: &str,
) -> (std::sync::Arc<GatedRepo>, DynKintaiEventsRepo) {
    let r = std::sync::Arc::new(GatedRepo::new(rows, digest));
    let dyn_r: DynKintaiEventsRepo = r.clone();
    (r, dyn_r)
}

/// テスト用の疑似 digest。**実物 (sha256 hex) と同じ 64 桁固定長**にしておく —
/// `kintai.fold_gate.dtako_digest` は `CHAR(64)` (`shifts.fingerprint` と同じ規約)
/// なので、64 桁に満たない値は Postgres が読み出し時に空白パディングして返す。
/// 実物の sha256 hex は常にちょうど 64 桁なのでこの罠を踏まないが、短い偽値の
/// ままだと `==` 比較がテストの都合だけで落ちる。
fn digest(label: &str) -> String {
    format!("{label:0<64}")
}

async fn stored_gate_digest(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    month: &str,
) -> Option<String> {
    sqlx::query_scalar(
        "SELECT dtako_digest FROM kintai.fold_gate WHERE tenant_id = $1 AND month = $2",
    )
    .bind(tenant)
    .bind(month)
    .fetch_optional(pool)
    .await
    .expect("fold_gate query")
}

/// **月ゲートが刺さると、2 回目以降は fold_month の読みそのものを省く。**
///
/// 打刻 (Pg) 側は 1 回目も 2 回目も未変更 (どちらのテストも push を挟まないので
/// `kintai.kintai_events` は空のまま) なので、punch_digest は両方とも同じ固定値。
/// dtako 側の digest だけを操作して gate の hit / miss を作る。
#[tokio::test]
async fn month_gate_skips_the_read_when_the_input_is_unchanged() {
    let (store, pool) = require_db!();
    let (repo, dyn_repo) = gated(
        vec![
            punch("2026-07-14 08:00:00", "始業"),
            punch("2026-07-14 18:00:00", "終業"),
        ],
        &digest("v1"),
    );

    // gate を書くのは `warnings_seen() == Some(false)` のときだけなので、
    // `main.rs` と同じく `with_warning_sink` の内側で呼ぶ (Refs #205-17)
    let first = with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await
    .0
    .expect("1st");
    assert_eq!(
        first.drivers_written, 1,
        "初回は gate が無いので普通に読んで書く"
    );
    assert_eq!(repo.read_calls(), 1);
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("v1")),
        "apply=true の全量再計算は gate を書く"
    );

    // 同じ digest で 2 回目 — gate が刺さり、fold_month の読みが増えない
    let second = recalc_month(&dyn_repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("2nd");
    assert_eq!(
        repo.read_calls(),
        1,
        "gate が刺さったら fold_month の読みを丸ごと省く"
    );
    assert_eq!(second.drivers, 0, "母集団の列挙すらしていない");
    assert!(!second.wrote_anything());

    // dtako 側が変わった (alc の etag が変化) — gate が外れて読み直す
    repo.set_digest(&digest("v2"));
    let third = with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await
    .0
    .expect("3rd");
    assert_eq!(repo.read_calls(), 2, "digest が変われば読み直す");
    assert_eq!(
        third.drivers_written, 0,
        "行の中身自体は変わっていないので書かない"
    );
    assert_eq!(third.drivers_unchanged, 1);
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("v2")),
        "外れて読み直したら gate も新しい digest に更新する"
    );
}

/// **`apply = false` (dry-run) の全量再計算は gate を書かない。**
///
/// dry-run はそもそも 1 行も保存しないので、「この digest で最新」を主張する
/// 権利が無い — 書いてしまうと次の本当の (apply=true) 再計算が誤って gate に
/// 引っかかり、1 行も書かれていない状態を「最新」としてスキップしてしまう。
#[tokio::test]
async fn month_gate_is_not_written_on_a_dry_run() {
    let (store, pool) = require_db!();
    let (repo, dyn_repo) = gated(
        vec![
            punch("2026-07-15 08:00:00", "始業"),
            punch("2026-07-15 18:00:00", "終業"),
        ],
        &digest("dry"),
    );
    recalc_month(&dyn_repo, &store, &params(), "2026-07", None, false)
        .await
        .expect("dry-run");
    assert_eq!(
        repo.read_calls(),
        1,
        "dry-run でも読みそのものは行う (preview のため)"
    );
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "1 行も書いていないので gate も書かない"
    );
}

/// **[`recalc_drivers`] は月ゲートを読むだけで書かない。**
///
/// ページングされた再計算 (`POST /api/kintai/recalc` の 1 ページ) が gate を
/// 書いてしまうと、1 ページ目だけ処理した時点で「この月は最新」になり、
/// 未処理のページの乗務員が古い fingerprint のまま取り残される
/// (#225 / #234 と同型の事故) — それが起きていないことを確かめる。
#[tokio::test]
async fn recalc_drivers_reads_the_gate_but_never_writes_it() {
    let (store, pool) = require_db!();
    let (repo, dyn_repo) = gated(
        vec![
            punch("2026-07-16 08:00:00", "始業"),
            punch("2026-07-16 18:00:00", "終業"),
        ],
        &digest("a"),
    );

    // recalc_month (driver 省略) だけが gate を書ける。ここで最初の gate を立てる
    // (gate を書くのは Some(false) のときだけなので with_warning_sink で包む)
    with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await
    .0
    .expect("establish gate");
    assert_eq!(repo.read_calls(), 1);
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("a"))
    );

    // 同じ digest のまま recalc_drivers — gate が刺さり、読みを増やさず即返る
    let hit = recalc_drivers(&dyn_repo, &store, &params(), "2026-07", &[DRIVER], true)
        .await
        .expect("recalc_drivers hit");
    assert_eq!(repo.read_calls(), 1, "gate 一致で fold_month を呼ばない");
    assert_eq!(hit.drivers, 1);
    assert_eq!(hit.drivers_unchanged, 1);
    assert!(!hit.wrote_anything());

    // dtako 側が変わった状態で recalc_drivers — gate は外れて読み直すが、
    // **書くのは recalc_month だけ**なので gate 自体は "a" のまま動かない
    repo.set_digest(&digest("b"));
    let miss = recalc_drivers(&dyn_repo, &store, &params(), "2026-07", &[DRIVER], true)
        .await
        .expect("recalc_drivers miss");
    assert_eq!(
        repo.read_calls(),
        2,
        "digest が変われば recalc_drivers も読み直す"
    );
    assert_eq!(miss.drivers, 1);
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("a")),
        "recalc_drivers は gate を書かない — 古い digest のまま残る"
    );

    // recalc_month が改めて全量を通れば、そのときだけ gate が新しい digest に動く
    with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await
    .0
    .expect("recalc_month catches up");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("b"))
    );
}

/// **`driver` を指定した `recalc_month` は月ゲートを使わない。**
///
/// 1 名だけの呼び出しは「月まるごと処理した」ことにならないので、gate の対象外
/// (読み・書きどちらもしない) — 対象外であることそのものが検査になる。
#[tokio::test]
async fn month_gate_is_skipped_when_a_single_driver_is_named() {
    let (store, pool) = require_db!();
    let (repo, dyn_repo) = gated(
        vec![
            punch("2026-07-17 08:00:00", "始業"),
            punch("2026-07-17 18:00:00", "終業"),
        ],
        &digest("single"),
    );
    recalc_month(&dyn_repo, &store, &params(), "2026-07", Some(DRIVER), true)
        .await
        .expect("single-driver recalc");
    assert_eq!(repo.read_calls(), 1);
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "driver 指定は月まるごとではないので gate を書かない"
    );

    // gate に何も書かれていないので、2 回目も普通に読む (省かれない)
    recalc_month(&dyn_repo, &store, &params(), "2026-07", Some(DRIVER), true)
        .await
        .expect("single-driver recalc again");
    assert_eq!(repo.read_calls(), 2);
}

/// **[`month_gate_report`] はページングされた HTTP 経路が読むだけの公開口。**
///
/// `crate::routes::kintai_recalc::run` が `fold_month` を直接呼ぶ (#205-12 の
/// 統合で母集団の決定と畳みを 1 回読みで済ませるようになったため) ので、
/// `recalc_drivers` を経由しないこの経路にも同じ判定を独立して提供する。
#[tokio::test]
async fn month_gate_report_mirrors_recalc_month_hit_and_miss() {
    let (store, _pool) = require_db!();
    let (repo, dyn_repo) = gated(
        vec![
            punch("2026-07-18 08:00:00", "始業"),
            punch("2026-07-18 18:00:00", "終業"),
        ],
        &digest("report"),
    );

    // gate がまだ無い — Miss (「判定できない」ではなく「一致しない」= 通常どおり進め)
    let first = month_gate_report(&dyn_repo, &store, &params(), "2026-07", true)
        .await
        .expect("no gate yet");
    assert!(matches!(first, MonthGate::Miss { .. }));
    assert_eq!(
        repo.read_calls(),
        0,
        "月ゲートの判定自体は fold_month を呼ばない"
    );

    // recalc_month (driver 省略) だけが gate を書ける
    with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await
    .0
    .expect("establish gate");
    assert_eq!(repo.read_calls(), 1);

    // 同じ digest — 刺さる。fold_month はやはり呼ばれない
    let hit = month_gate_report(&dyn_repo, &store, &params(), "2026-07", true)
        .await
        .expect("hit");
    match hit {
        MonthGate::Hit(report) => assert!(!report.wrote_anything()),
        other => panic!("gate should hit, got {other:?}"),
    }
    assert_eq!(repo.read_calls(), 1, "gate の判定だけでは読みが増えない");

    // dtako 側が変わった — 外れる
    repo.set_digest(&digest("report-v2"));
    let miss = month_gate_report(&dyn_repo, &store, &params(), "2026-07", true)
        .await
        .expect("miss");
    assert!(
        matches!(miss, MonthGate::Miss { .. }),
        "digest が変われば Miss (呼び出し側が通常どおり読みに進む)"
    );
    assert_eq!(repo.read_calls(), 1, "判定だけなら外れても読みには進まない");
}

// ── 13c: recalc_month が gate を書く条件 (warnings の有無) (Refs #205-17) ────
//
// `routes::kintai_recalc::run` は #242 で「上流 warnings が空」を書く条件の
// 1 つに入れたが、`recalc_month` (CLI の Recalc / Sync 経由) は取り残されていた
// — R2 の分割遅れ中に欠けた入力を「最新」と刻む同じリスクがある。
// `recalc_month` は `main.rs` で `with_warning_sink` に包まれて呼ばれる前提
// なので、`kintai_http_repo::warnings_seen()` を覗いて判断する。

/// **書く: 収集器の中で warnings ゼロなら gate が立ち、2 回目は gate hit になる。**
#[tokio::test]
async fn recalc_month_writes_the_gate_when_warnings_are_confirmed_empty() {
    let (store, pool) = require_db!();
    let (repo, dyn_repo) = gated(
        vec![
            punch("2026-07-19 08:00:00", "始業"),
            punch("2026-07-19 18:00:00", "終業"),
        ],
        &digest("no-warnings"),
    );

    let (first, warnings) = with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await;
    assert!(warnings.is_empty(), "GatedRepo は warnings を出さない");
    assert_eq!(first.expect("1st").drivers_written, 1);
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("no-warnings")),
        "warnings が確認できたので gate を書く"
    );

    // gate が立っているので 2 回目は fold_month を呼ばずに hit する
    with_warning_sink(recalc_month(
        &dyn_repo,
        &store,
        &params(),
        "2026-07",
        None,
        true,
    ))
    .await
    .0
    .expect("2nd");
    assert_eq!(repo.read_calls(), 1, "gate hit で読みが増えない");
}

/// **書かない: 収集器の中でも warnings があれば gate は空のまま。**
#[tokio::test]
async fn recalc_month_does_not_write_the_gate_when_warnings_are_present() {
    let (store, pool) = require_db!();
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-20 08:00:00", "始業"),
            punch("2026-07-20 18:00:00", "終業"),
        ],
        &digest("with-warnings"),
    );

    let (r, warnings) = with_warning_sink(async {
        rust_ichibanboshi::kintai_http_repo::record_warning_for_test("NoSuchKey: U1/KUDGIVT.csv");
        recalc_month(&dyn_repo, &store, &params(), "2026-07", None, true).await
    })
    .await;
    assert!(!warnings.is_empty());
    assert_eq!(r.expect("recalc").drivers_written, 1, "書くこと自体は行う");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "warnings ありでは gate を書かない (欠けた入力を最新と刻まない)"
    );
}

/// **書かない: 収集器の外 (`with_warning_sink` に包まれていない) では判断できない
/// ので gate を書かない。**「無い」と「分からない」を混同しないための分岐。
#[tokio::test]
async fn recalc_month_does_not_write_the_gate_outside_the_warning_sink() {
    let (store, pool) = require_db!();
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-21 08:00:00", "始業"),
            punch("2026-07-21 18:00:00", "終業"),
        ],
        &digest("no-sink"),
    );

    // with_warning_sink に包まずそのまま呼ぶ — main.rs の呼び方から外れた形
    let r = recalc_month(&dyn_repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("recalc");
    assert_eq!(r.drivers_written, 1, "書くこと自体は行う");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "収集器の外では判断がつかないので gate を書かない"
    );
}

// ── 13b: HTTP 経路 (routes::kintai_recalc) が gate を書く条件 ────────────────
//
// #205 親の決定 (2026-07-31): GCP 本番で gate に 1 行でも入る経路は
// `POST /api/kintai/recalc` の 1 ページが月まるごとを完結させたときだけ
// (`recalc_month` を呼ぶのは封印済みのオンプレ CLI だけなので、本番では
// この経路が無いと fold_gate が永久に空のまま = 定常ゼロ読みが 1 度も成立しない)。

async fn call_recalc(
    store: &KintaiPgStore,
    repo: &DynKintaiEventsRepo,
    params: &KosokuParams,
    req: RecalcRequest,
) -> Result<serde_json::Value, (axum::http::StatusCode, String)> {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "X-Tenant-ID",
        store.tenant_id().to_string().parse().unwrap(),
    );
    // pin は nil (多テナント構成) — ヘッダの X-Tenant-ID がそのまま書き先になる
    let pg: rust_ichibanboshi::routes::kintai_timecard::DynKintaiPgStore =
        Some(std::sync::Arc::new(KintaiPgStore::from_pool(
            store.pool().clone(),
            uuid::Uuid::nil(),
        )));
    let resp = recalc(
        headers,
        axum::Extension(pg),
        axum::Extension(repo.clone()),
        axum::Extension(std::sync::Arc::new(*params)),
        axum::Extension(ReadTenant(None)),
        axum::Json(req),
    )
    .await?;
    Ok(resp.0)
}

fn recalc_req(month: &str, apply: bool) -> RecalcRequest {
    RecalcRequest {
        month: month.to_string(),
        after_driver_cd: None,
        max_drivers: None,
        stale_only: false,
        apply,
    }
}

/// **書く: apply + 1 ページ目から + 回りきる + stale_only でない + warnings 空。**
/// 母集団が `DEFAULT_MAX_FOLD_DRIVERS` (100) 未満の 1 名だけなので必ず 1 ページで終わる。
#[tokio::test]
async fn http_recalc_writes_the_gate_when_the_whole_month_completes_in_one_page() {
    let (store, pool) = require_db!();
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-20 08:00:00", "始業"),
            punch("2026-07-20 18:00:00", "終業"),
        ],
        &digest("http-write"),
    );

    let resp = call_recalc(&store, &dyn_repo, &params(), recalc_req("2026-07", true))
        .await
        .expect("recalc");
    assert!(
        resp["next_after_driver_cd"].is_null(),
        "1 名だけの母集団は 1 ページで回りきる: {resp}"
    );
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        Some(digest("http-write")),
        "月まるごと完結したページは gate を書く"
    );

    // 2 回目の同じ呼び出しは gate hit — fold_month (全量読み) を経ずに終わる
    let second = call_recalc(&store, &dyn_repo, &params(), recalc_req("2026-07", true))
        .await
        .expect("recalc again");
    assert_eq!(second["drivers"], serde_json::json!([]));
    assert!(second["next_after_driver_cd"].is_null());
}

/// **書かない: `apply=false` (dry-run/preview)。**
#[tokio::test]
async fn http_recalc_does_not_write_the_gate_on_dry_run() {
    let (store, pool) = require_db!();
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-21 08:00:00", "始業"),
            punch("2026-07-21 18:00:00", "終業"),
        ],
        &digest("http-dry"),
    );
    call_recalc(&store, &dyn_repo, &params(), recalc_req("2026-07", false))
        .await
        .expect("preview");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "apply=false は書かない"
    );
}

/// **書かない: `after_driver_cd` 指定あり (1 ページ目からではない)。**
#[tokio::test]
async fn http_recalc_does_not_write_the_gate_mid_walk() {
    let (store, pool) = require_db!();
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-22 08:00:00", "始業"),
            punch("2026-07-22 18:00:00", "終業"),
        ],
        &digest("http-mid"),
    );
    let mut req = recalc_req("2026-07", true);
    req.after_driver_cd = Some(0);
    call_recalc(&store, &dyn_repo, &params(), req)
        .await
        .expect("recalc");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "1 ページ目からでなければ書かない"
    );
}

/// **書かない: 回りきらない (`next_after_driver_cd` が残る)。**
/// `max_drivers` を母集団 (2 名) より小さくして 2 ページ以上に割る。
#[tokio::test]
async fn http_recalc_does_not_write_the_gate_when_more_pages_remain() {
    let (store, pool) = require_db!();
    let other = |at: &str, state: &str| {
        serde_json::json!({"datetime": at, "end_datetime": null, "driver_id": 1195,
            "source": "timecard", "state": state, "unko_no": null})
    };
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-23 08:00:00", "始業"),
            punch("2026-07-23 18:00:00", "終業"),
            other("2026-07-23 08:00:00", "始業"),
            other("2026-07-23 18:00:00", "終業"),
        ],
        &digest("http-more-pages"),
    );
    let mut req = recalc_req("2026-07", true);
    req.max_drivers = Some(1);
    let resp = call_recalc(&store, &dyn_repo, &params(), req)
        .await
        .expect("recalc");
    assert!(
        !resp["next_after_driver_cd"].is_null(),
        "1 ページ 1 名では 2 名の母集団が回りきらない: {resp}"
    );
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "回りきっていないので書かない"
    );
}

/// **書かない: `stale_only=true` (母集団を現行版未達だけに絞っている)。**
#[tokio::test]
async fn http_recalc_does_not_write_the_gate_when_stale_only() {
    let (store, pool) = require_db!();
    let (_repo, dyn_repo) = gated(
        vec![
            punch("2026-07-24 08:00:00", "始業"),
            punch("2026-07-24 18:00:00", "終業"),
        ],
        &digest("http-stale-only"),
    );
    let mut req = recalc_req("2026-07", true);
    req.stale_only = true;
    call_recalc(&store, &dyn_repo, &params(), req)
        .await
        .expect("recalc");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "stale_only は月全体を畳んでいないので書かない"
    );
}

/// **書かない: 上流 warnings が非空 (R2 の分割遅れ等)。**
struct WarningRepo {
    inner: GatedRepo,
}

#[async_trait]
impl KintaiEventsApi for WarningRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_events_between(from, to, driver).await
    }
    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        // 上流 warnings を模す — 実物 (HttpKintaiEventsRepo) は R2 の分割遅れ中に
        // これを task-local sink 経由で呼び出し側へ運ぶ
        rust_ichibanboshi::kintai_http_repo::record_warning_for_test("NoSuchKey: U1/KUDGIVT.csv");
        self.inner.fetch_all_events_between(from, to).await
    }
    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_ferry_between(from, to, driver).await
    }
    async fn fetch_dtako_month_digest(
        &self,
        month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        self.inner.fetch_dtako_month_digest(month).await
    }
}

#[tokio::test]
async fn http_recalc_does_not_write_the_gate_when_upstream_warnings_are_non_empty() {
    let (store, pool) = require_db!();
    let dyn_repo: DynKintaiEventsRepo = std::sync::Arc::new(WarningRepo {
        inner: GatedRepo::new(
            vec![
                punch("2026-07-25 08:00:00", "始業"),
                punch("2026-07-25 18:00:00", "終業"),
            ],
            &digest("http-warnings"),
        ),
    });
    let resp = call_recalc(&store, &dyn_repo, &params(), recalc_req("2026-07", true))
        .await
        .expect("recalc");
    assert!(
        resp["next_after_driver_cd"].is_null(),
        "1 名の母集団は回りきる: {resp}"
    );
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "warnings が非空なら (回りきっていても) 書かない"
    );
}

// ── #205 の 21: 指紋を取ったときの warning が gate を止めること ───────────
//
// `routes::kintai_recalc::run` は `fold_month` だけを `with_warning_sink` に入れて
// いた。入力の欠けを最初に見るのは指紋側 (`fetch_dtako_month_digest` →
// `fetch_etags`) なので、そこが立てた warning は sink の外で黙って捨てられ、
// 5 条件の `warnings.is_empty()` が素通りしていた。ここで固定するのはその穴。

/// 指紋を取る側だけが warning を立てる repo (`fold_month` は静か)。
struct DigestWarningRepo {
    inner: GatedRepo,
}

#[async_trait]
impl KintaiEventsApi for DigestWarningRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_events_between(from, to, driver).await
    }
    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_all_events_between(from, to).await
    }
    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        self.inner.fetch_ferry_between(from, to, driver).await
    }
    async fn fetch_dtako_month_digest(
        &self,
        month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        // 実物 (HttpKintaiEventsRepo) が etags の一覧から入力の欠けを見つけた形
        rust_ichibanboshi::kintai_http_repo::record_warning_for_test("dtako 入力欠け: 運行が …");
        self.inner.fetch_dtako_month_digest(month).await
    }
}

#[tokio::test]
async fn http_recalc_does_not_write_the_gate_when_the_digest_saw_missing_input() {
    let (store, pool) = require_db!();
    let dyn_repo: DynKintaiEventsRepo = std::sync::Arc::new(DigestWarningRepo {
        inner: GatedRepo::new(
            vec![
                punch("2026-07-25 08:00:00", "始業"),
                punch("2026-07-25 18:00:00", "終業"),
            ],
            &digest("digest-warnings"),
        ),
    });
    let resp = call_recalc(&store, &dyn_repo, &params(), recalc_req("2026-07", true))
        .await
        .expect("recalc");
    assert!(
        resp["next_after_driver_cd"].is_null(),
        "1 名の母集団は回りきる: {resp}"
    );
    let warnings = resp["fold"]["warnings"].as_array().expect("warnings");
    assert_eq!(warnings.len(), 1, "指紋側の warning が応答に出る: {resp}");
    assert_eq!(
        stored_gate_digest(&pool, store.tenant_id(), "2026-07").await,
        None,
        "指紋側だけが立てた warning でも封をしない (Refs #205 の 21)"
    );
}

// ── 保存済みの姿を 1 往復で読む (Refs #205 の 25) ──────────────────────────

/// 任意の乗務員の打刻。[`punch`] は `DRIVER` 固定なので複数名を並べるときはこちら。
fn punch_of(driver: u64, at: &str, state: &str) -> serde_json::Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": driver, "source": "timecard", "state": state, "unko_no": null})
}

/// 1 日ぶんの始業/終業を人数ぶん並べる。
fn punches_for(drivers: &[u64]) -> Vec<serde_json::Value> {
    drivers
        .iter()
        .flat_map(|d| {
            vec![
                punch_of(*d, "2026-07-06 08:00:00", "始業"),
                punch_of(*d, "2026-07-06 18:00:00", "終業"),
            ]
        })
        .collect()
}

/// 複数乗務員版が単数版と**1 名ずつ突き合わせて完全に同じ**か。
///
/// 式を写し間違えると「中身は同じなのに毎回全乗務員が stale」になり、静かに
/// 毎回全書き直しになる (`kintai_push` の窓署名 SQL と同じ縛り)。SQL の文字列
/// 一致は lib 側の単体テストで縛ってあるので、ここは**実 Postgres で返る値**を見る。
#[tokio::test]
async fn stored_states_matches_stored_state_driver_by_driver() {
    let (store, _pool) = require_db!();
    let saved = [1101_u64, 1102, 1103];
    let repo = repo(punches_for(&saved));
    recalc_month(&repo, &store, &params(), "2026-07", None, true)
        .await
        .expect("fold");

    // 空: 1 往復もせず空の map
    assert!(
        stored_states(&store, &[], "2026-07")
            .await
            .expect("empty")
            .is_empty(),
        "空の乗務員リストは空の map"
    );

    // 1 名 / 複数名 / **保存が 1 件も無い乗務員が混ざる** の 3 通り。
    // 9999 はどの表にも 1 行も無い — 単数版が「空の姿」を返すのと同じく、
    // 複数版でも鍵ごと落ちずに残らないといけない
    for want in [
        vec![1101_i64],
        vec![1101, 1102, 1103],
        vec![9999, 1102, 8888],
    ] {
        let batch = stored_states(&store, &want, "2026-07")
            .await
            .expect("batch");
        assert_eq!(batch.len(), want.len(), "全乗務員ぶんの行が返る: {want:?}");
        for cd in &want {
            let one = stored_state(&store, *cd, "2026-07").await.expect("single");
            assert_eq!(batch[cd], one, "driver {cd} の姿が単数版と違う");
        }
    }

    // 保存が無い乗務員は「空の姿」で入る (鍵が無い = 読めていない、と混同しない)
    let batch = stored_states(&store, &[9999], "2026-07")
        .await
        .expect("gap");
    assert!(
        batch[&9999].is_empty(),
        "保存の無い乗務員は空の姿: {batch:?}"
    );
}

/// 保存が無い乗務員が混ざっても**畳んだ結果は 1 人ぶんも落ちない**。
///
/// 複数乗務員版が「保存の無い乗務員の行ごと落とす」形だと、`store_units` から
/// 見て姿が引けず、is_current の判定が壊れる (どちらへ倒しても静かに間違う)。
#[tokio::test]
async fn a_driver_with_no_stored_rows_still_gets_folded_in_a_batch() {
    let (store, pool) = require_db!();
    // 1104 は打刻あり、1105 は打刻ゼロ (畳んでも書くものが無い)
    let repo = repo(punches_for(&[1104]));
    let r = recalc_drivers(&repo, &store, &params(), "2026-07", &[1104, 1105], true)
        .await
        .expect("fold");
    assert_eq!(r.drivers, 2, "2 名とも数える");
    assert_eq!(r.drivers_written, 1, "書くものがあるのは 1104 だけ");
    assert_eq!(shift_count(&pool, store.tenant_id()).await, 1);

    // 2 回目は 2 名とも据え置き — 打刻ゼロの 1105 が毎回 stale に見えると
    // `wrote_anything` が誤検知し続ける (StoredState::is_current の「空 = 空」)
    let again = recalc_drivers(&repo, &store, &params(), "2026-07", &[1104, 1105], true)
        .await
        .expect("again");
    assert_eq!(again.drivers_written, 0, "2 回目は 1 行も書かない");
    assert_eq!(again.drivers_unchanged, 2);
}

/// sqlx が 1 クエリごとに出す `sqlx::query` の tracing event を数える層。
///
/// **往復回数が費用**なので、往復が減ったことを機械で確かめられる形にしておく。
#[derive(Clone, Default)]
struct QueryCounter(std::sync::Arc<std::sync::Mutex<Vec<String>>>);

impl QueryCounter {
    /// 保存済みの姿を読んだクエリの回数 (単数版・複数版のどちらも `n_parts` を持つ)。
    fn stored_state_reads(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.contains("n_parts"))
            .count()
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for QueryCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        if event.metadata().target() != "sqlx::query" {
            return;
        }
        struct Collect<'a>(&'a mut String);
        impl tracing::field::Visit for Collect<'_> {
            fn record_debug(&mut self, _f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                self.0.push_str(&format!("{v:?}"));
            }
            fn record_str(&mut self, _f: &tracing::field::Field, v: &str) {
                self.0.push_str(v);
            }
        }
        let mut sql = String::new();
        event.record(&mut Collect(&mut sql));
        self.0.lock().unwrap().push(sql);
    }
}

/// **乗務員が何人でも保存済みの姿を読むのは 1 クエリ**。
///
/// 元は乗務員 1 人につき 1 往復で、Pg 読みは窓の受け口と pool
/// (`max_connections=1`) を共有しているため完全に直列 — 137 名なら 137 回ぶんの
/// 待ちがそのまま全量再計算の時間に乗っていた。
#[tokio::test]
async fn the_stored_state_read_is_one_query_however_many_drivers() {
    let (store, _pool) = require_db!();

    async fn count_reads(store: &KintaiPgStore, drivers: &[u64]) -> usize {
        use tracing_subscriber::layer::SubscriberExt;
        let repo = repo(punches_for(drivers));
        let counter = QueryCounter::default();
        let subscriber = tracing_subscriber::registry().with(counter.clone());
        // `#[tokio::test]` は current-thread runtime なので、この future は
        // このスレッドで進む = スレッド局所の subscriber で拾える
        let guard = tracing::subscriber::set_default(subscriber);
        // **callsite の interest は process 大域にキャッシュされる。** subscriber の
        // 無いまま sqlx が 1 度でも走ると「never」で焼き付き、後から subscriber を
        // 挿しても event が飛ばない (数え方の側の罠で、0 と出る)
        tracing::callsite::rebuild_interest_cache();
        recalc_drivers(&repo, store, &params(), "2026-07", drivers, true)
            .await
            .expect("fold");
        drop(guard);
        counter.stored_state_reads()
    }

    // 1 名でも 8 名でも 1 回。**人数に比例しない**ことがこの PR の全て
    assert_eq!(count_reads(&store, &[1201]).await, 1, "1 名で 1 クエリ");
    assert_eq!(
        count_reads(&store, &[1301, 1302, 1303, 1304, 1305, 1306, 1307, 1308]).await,
        1,
        "8 名でも 1 クエリ (乗務員ごとに読んでいたら 8)"
    );
}
