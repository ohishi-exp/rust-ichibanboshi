//! 打刻の push が残す変更記録 (`kintai.event_changes`) と、その読み口
//! `GET /api/kintai/change-log` の**実 Postgres** に対する検証
//! (Refs ohishi-exp/nuxt-dtako-admin#1133)。
//!
//! 記録は `replace_window` のトランザクションの中で旧 events を読んで作るので、
//! 実 DB を往復させないと確かめられない (`tests/shift_overlaps_pg_test.rs` と同じ理由)。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-1133-3 -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:16
//! docker port kintai-pg-1133-3 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:<port>/postgres \
//!   cargo test --test change_log_pg_test
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use chrono::{NaiveDate, NaiveDateTime};
use rust_ichibanboshi::kintai_push::{KintaiPgStore, PushEvent, DATETIME_FORMAT};
use rust_ichibanboshi::routes::change_log::{change_log, ChangeLogQuery};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;

// ── 前提 (tests/shift_overlaps_pg_test.rs と同じ形) ──────────────────────────

fn database_url() -> Option<String> {
    std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn needs_psql_variables(sql: &str) -> bool {
    sql.contains(":'") || sql.contains(":\"")
}

fn sorted_migrations() -> Vec<std::path::PathBuf> {
    let mut files: Vec<std::path::PathBuf> = std::fs::read_dir("migrations")
        .expect("migrations dir")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
        .collect();
    files.sort();
    files
}

async fn ensure_schema(pool: &sqlx::PgPool) {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(1_121_016_001_i64)
        .execute(pool)
        .await
        .expect("advisory lock");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'kintai')")
            .fetch_one(pool)
            .await
            .expect("schema probe");
    if !exists {
        for entry in sorted_migrations() {
            let sql = std::fs::read_to_string(&entry).expect("read migration");
            if needs_psql_variables(&sql) {
                eprintln!("skip {} (psql の変数を使う migration)", entry.display());
                continue;
            }
            sqlx::raw_sql(&sql)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", entry.display()));
        }
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(1_121_016_001_i64)
        .execute(pool)
        .await
        .expect("advisory unlock");
}

/// テスト 1 本ぶんの store。テナントは毎回新しい UUID。
async fn store() -> Option<Arc<KintaiPgStore>> {
    let url = database_url()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    ensure_schema(&pool).await;
    let tenant = uuid::Uuid::new_v4();
    Some(Arc::new(KintaiPgStore::from_pool(pool, tenant)))
}

macro_rules! require_db {
    () => {
        match store().await {
            Some(v) => v,
            None => return,
        }
    };
}

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

fn ev(driver: i64, at: &str, state: &str) -> PushEvent {
    PushEvent {
        driver_cd: driver,
        occurred_at: NaiveDateTime::parse_from_str(at, DATETIME_FORMAT).unwrap(),
        state: state.to_string(),
        source: "timecard".to_string(),
        unko_no: None,
        raw: serde_json::json!({}),
    }
}

/// 1 日ぶんを push と同じ経路 (`replace_window`) で置き換える。
async fn push_day(store: &KintaiPgStore, driver: i64, date: &str, events: Vec<PushEvent>) {
    let changed = BTreeMap::from([(d(date), events)]);
    store
        .replace_days(driver, &changed, &[])
        .await
        .expect("replace_days");
}

async fn delete_day(store: &KintaiPgStore, driver: i64, date: &str) {
    store
        .replace_days(driver, &BTreeMap::new(), &[d(date)])
        .await
        .expect("replace_days (delete)");
}

async fn count_changes(store: &KintaiPgStore) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM kintai.event_changes WHERE tenant_id = $1")
        .bind(store.tenant_id())
        .fetch_one(store.pool())
        .await
        .expect("count")
}

fn query(driver: Option<i64>, from: &str, to: &str) -> Query<ChangeLogQuery> {
    Query(ChangeLogQuery {
        driver,
        from: Some(from.to_string()),
        to: Some(to.to_string()),
    })
}

async fn get(
    store: &Arc<KintaiPgStore>,
    tenant: uuid::Uuid,
    driver: Option<i64>,
) -> serde_json::Value {
    let read = Extension(ReadTenant(Some(tenant)));
    change_log(
        query(driver, "2026-02-01", "2026-02-28"),
        Extension(Some(store.clone())),
        read,
    )
    .await
    .expect("handler")
    .0
}

// ── (a) 旧 08:00 始業 → 新 07:30 始業 は前後付きで 1 行 ────────────────────────

#[tokio::test]
async fn test_a_corrected_start_punch_is_recorded_with_before_and_after() {
    let store = require_db!();
    push_day(
        &store,
        1194,
        "2026-02-06",
        vec![ev(1194, "2026-02-06 08:00:00", "始業")],
    )
    .await;
    push_day(
        &store,
        1194,
        "2026-02-06",
        vec![ev(1194, "2026-02-06 07:30:00", "始業")],
    )
    .await;

    let got = get(&store, store.tenant_id(), Some(1194)).await;
    let changes = got["changes"].as_array().expect("changes");
    assert_eq!(changes.len(), 1, "got: {got}");
    let c = &changes[0];
    assert_eq!(c["driver_cd"], 1194);
    assert_eq!(c["date"], "2026-02-06");
    let before = serde_json::json!([{"occurred_at": "2026-02-06 08:00:00", "state": "始業", "source": "timecard", "unko_no": null}]);
    let after = serde_json::json!([{"occurred_at": "2026-02-06 07:30:00", "state": "始業", "source": "timecard", "unko_no": null}]);
    assert_eq!(c["before"], before);
    assert_eq!(c["after"], after);
    assert!(
        c["recorded_at"].as_str().is_some_and(|s| s.len() == 19),
        "got: {got}"
    );
    assert_eq!(got["recording_since"], c["recorded_at"]);
    assert_eq!(got["driver"], 1194);
    assert_eq!(got["from"], "2026-02-01");
    assert_eq!(got["to"], "2026-02-28");
}

// ── (b) 同じ events を 2 回 push しても 2 行目は増えない ──────────────────────

#[tokio::test]
async fn test_pushing_the_same_events_again_adds_no_row() {
    let store = require_db!();
    let day = || {
        vec![
            ev(1, "2026-02-06 08:00:00", "始業"),
            ev(1, "2026-02-06 17:00:00", "終業"),
        ]
    };
    push_day(&store, 1, "2026-02-06", day()).await;
    push_day(
        &store,
        1,
        "2026-02-06",
        vec![ev(1, "2026-02-06 07:30:00", "始業")],
    )
    .await;
    assert_eq!(count_changes(&store).await, 1);
    // 同じ中身をもう 1 回 (並びを逆にしても同じ) — 行は増えない
    push_day(
        &store,
        1,
        "2026-02-06",
        vec![ev(1, "2026-02-06 07:30:00", "始業")],
    )
    .await;
    assert_eq!(count_changes(&store).await, 1);
    let mut rev = day();
    rev.reverse();
    push_day(&store, 1, "2026-02-06", rev).await;
    assert_eq!(count_changes(&store).await, 2, "中身が変われば記録する");
    push_day(&store, 1, "2026-02-06", day()).await;
    assert_eq!(count_changes(&store).await, 2);
}

// ── (c) 日ごと消えたら after = NULL ─────────────────────────────────────────

#[tokio::test]
async fn test_a_deleted_day_is_recorded_with_a_null_after() {
    let store = require_db!();
    push_day(
        &store,
        2,
        "2026-02-10",
        vec![ev(2, "2026-02-10 08:00:00", "始業")],
    )
    .await;
    delete_day(&store, 2, "2026-02-10").await;

    let got = get(&store, store.tenant_id(), None).await;
    let changes = got["changes"].as_array().expect("changes");
    assert_eq!(changes.len(), 1, "got: {got}");
    assert_eq!(
        changes[0]["before"][0]["occurred_at"],
        "2026-02-10 08:00:00"
    );
    assert_eq!(changes[0]["after"], serde_json::Value::Null);
}

// ── (d) 初回取り込み (旧なし) は記録しない ──────────────────────────────────

#[tokio::test]
async fn test_a_first_import_is_not_recorded() {
    let store = require_db!();
    push_day(
        &store,
        3,
        "2026-02-11",
        vec![ev(3, "2026-02-11 08:00:00", "始業")],
    )
    .await;
    // 何も無い日を消しても記録しない
    delete_day(&store, 3, "2026-02-12").await;
    assert_eq!(count_changes(&store).await, 0);

    let got = get(&store, store.tenant_id(), None).await;
    assert_eq!(got["changes"], serde_json::json!([]));
    assert_eq!(got["recording_since"], serde_json::Value::Null);
    assert_eq!(got["driver"], serde_json::Value::Null);
}

// ── (e) 別 tenant の行は読み口から見えない / RLS も切る ──────────────────────

#[tokio::test]
async fn test_another_tenants_changes_are_invisible() {
    let store = require_db!();
    let other = Arc::new(store.for_tenant(uuid::Uuid::new_v4()));
    for s in [&store, &other] {
        push_day(
            s,
            5,
            "2026-02-06",
            vec![ev(5, "2026-02-06 08:00:00", "始業")],
        )
        .await;
        push_day(
            s,
            5,
            "2026-02-06",
            vec![ev(5, "2026-02-06 07:30:00", "始業")],
        )
        .await;
    }
    push_day(
        &other,
        6,
        "2026-02-07",
        vec![ev(6, "2026-02-07 08:00:00", "始業")],
    )
    .await;
    push_day(
        &other,
        6,
        "2026-02-07",
        vec![ev(6, "2026-02-07 09:00:00", "始業")],
    )
    .await;

    let mine = get(&store, store.tenant_id(), None).await;
    assert_eq!(
        mine["changes"].as_array().map(Vec::len),
        Some(1),
        "got: {mine}"
    );
    let theirs = get(&other, other.tenant_id(), None).await;
    assert_eq!(
        theirs["changes"].as_array().map(Vec::len),
        Some(2),
        "got: {theirs}"
    );
    // 乗務員の絞り
    let only6 = get(&other, other.tenant_id(), Some(6)).await;
    assert_eq!(
        only6["changes"].as_array().map(Vec::len),
        Some(1),
        "got: {only6}"
    );
    assert_eq!(only6["changes"][0]["driver_cd"], 6);

    // reader (NOBYPASSRLS) で繋いだときも自テナントの行しか見えない
    let mut tx = store.pool().begin().await.expect("begin");
    sqlx::query("SET LOCAL ROLE kintai_reader")
        .execute(&mut *tx)
        .await
        .expect("set role");
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(store.tenant_id().to_string())
        .execute(&mut *tx)
        .await
        .expect("set tenant");
    let tenants: Vec<uuid::Uuid> =
        sqlx::query_scalar("SELECT DISTINCT tenant_id FROM kintai.event_changes")
            .fetch_all(&mut *tx)
            .await
            .expect("reader select");
    assert_eq!(tenants, vec![store.tenant_id()]);
    tx.rollback().await.expect("rollback");
}

// ── 期間の外は返さない ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_changes_outside_the_range_are_not_returned() {
    let store = require_db!();
    push_day(
        &store,
        8,
        "2026-03-01",
        vec![ev(8, "2026-03-01 08:00:00", "始業")],
    )
    .await;
    push_day(
        &store,
        8,
        "2026-03-01",
        vec![ev(8, "2026-03-01 07:00:00", "始業")],
    )
    .await;
    let got = get(&store, store.tenant_id(), None).await;
    assert_eq!(got["changes"], serde_json::json!([]));
    assert!(
        got["recording_since"].is_string(),
        "記録の始まりは期間に依らない: {got}"
    );
}

// ── DB が読めないときは 502 (db_err の経路) ─────────────────────────────────

#[tokio::test]
async fn test_a_closed_pool_is_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let read = Extension(ReadTenant(Some(store.tenant_id())));
    let (status, msg) = change_log(
        query(None, "2026-02-01", "2026-02-28"),
        Extension(Some(store.clone())),
        read,
    )
    .await
    .expect_err("must fail on a closed pool");
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(msg.contains("event_changes"), "{msg}");
}
