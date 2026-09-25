//! `GET /api/kintai/shift-overlaps` の**実 Postgres** に対する検証
//! (Refs ohishi-exp/nuxt-dtako-admin#1123)。
//!
//! ここでしか確かめられないのは、**開区間の重なり判定 (接しているだけは重ならない)**、
//! **前月末に始まり対象月へ跨る組が正しい月に出るか**、**互いに重なる 3 本が全組
//! (3 組) 出るか**、**テナント/乗務員の分離**。どれも実 DB を往復させないと分からない
//! (`tests/day_parts_pg_test.rs` と同じ理由)。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-1121-16 -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:16
//! docker port kintai-pg-1121-16 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:<port>/postgres \
//!   cargo test --test shift_overlaps_pg_test
//! ```

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use rust_ichibanboshi::routes::shift_overlaps::{shift_overlaps, ShiftOverlapsQuery};

// ── 前提 (tests/day_parts_pg_test.rs と同じ形) ───────────────────────────────

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

/// `kintai.shifts` に 1 本入れる。
async fn insert_shift(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    start_at: &str,
    end_at: &str,
) {
    sqlx::query(
        "INSERT INTO kintai.shifts \
           (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version) \
         VALUES ($1, $2, $3, $4, 'rest', repeat('a', 64), repeat('0', 16))",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(jst_at(start_at).expect("start_at"))
    .bind(jst_at(end_at).expect("end_at"))
    .execute(pool)
    .await
    .expect("insert shift");
}

fn query(month: &str) -> Query<ShiftOverlapsQuery> {
    Query(ShiftOverlapsQuery {
        month: Some(month.to_string()),
    })
}

/// 読み先のテナント (`[kintai_events] tenant_id`)。本番と同じく、これが正。
fn read(tenant: uuid::Uuid) -> Extension<ReadTenant> {
    Extension(ReadTenant(Some(tenant)))
}

async fn get(store: &Arc<KintaiPgStore>, tenant: uuid::Uuid, month: &str) -> serde_json::Value {
    shift_overlaps(query(month), Extension(Some(store.clone())), read(tenant))
        .await
        .expect("handler")
        .0
}

fn pair(
    driver_cd: i64,
    a_start: &str,
    a_end: &str,
    b_start: &str,
    b_end: &str,
) -> serde_json::Value {
    serde_json::json!({
        "driver_cd": driver_cd,
        "a_start": a_start,
        "a_end": a_end,
        "b_start": b_start,
        "b_end": b_end,
    })
}

// ── 1. 08:00〜18:00 と 10:00〜20:00 → 1 組 (a = 08:00 側、b = 10:00 側) ──────────

#[tokio::test]
async fn test_two_overlapping_shifts_form_one_pair() {
    let store = require_db!();
    let t = store.tenant_id();
    insert_shift(
        store.pool(),
        t,
        1610,
        "2026-06-10 08:00:00",
        "2026-06-10 18:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1610,
        "2026-06-10 10:00:00",
        "2026-06-10 20:00:00",
    )
    .await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got,
        serde_json::json!({
            "month": "2026-06",
            "items": [pair(
                1610,
                "2026-06-10 08:00:00",
                "2026-06-10 18:00:00",
                "2026-06-10 10:00:00",
                "2026-06-10 20:00:00",
            )],
        }),
        "got: {got}"
    );
}

// ── 2. a_end = b_start (接しているだけ) → 0 組 ─────────────────────────────────

#[tokio::test]
async fn test_touching_shifts_are_not_an_overlap() {
    let store = require_db!();
    let t = store.tenant_id();
    insert_shift(
        store.pool(),
        t,
        1611,
        "2026-06-10 08:00:00",
        "2026-06-10 12:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1611,
        "2026-06-10 12:00:00",
        "2026-06-10 20:00:00",
    )
    .await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got,
        serde_json::json!({ "month": "2026-06", "items": [] }),
        "got: {got}"
    );
}

// ── 3. 前月末に始まる a と対象月開始の b → 6 月に 1 組、5 月には出ない ─────────────

#[tokio::test]
async fn test_a_shift_starting_in_the_prior_month_still_pairs_in_the_target_month() {
    let store = require_db!();
    let t = store.tenant_id();
    insert_shift(
        store.pool(),
        t,
        1612,
        "2026-05-31 22:00:00",
        "2026-06-01 09:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1612,
        "2026-06-01 08:00:00",
        "2026-06-01 17:00:00",
    )
    .await;

    let june = get(&store, t, "2026-06").await;
    assert_eq!(
        june,
        serde_json::json!({
            "month": "2026-06",
            "items": [pair(
                1612,
                "2026-05-31 22:00:00",
                "2026-06-01 09:00:00",
                "2026-06-01 08:00:00",
                "2026-06-01 17:00:00",
            )],
        }),
        "got: {june}"
    );

    let may = get(&store, t, "2026-05").await;
    assert_eq!(
        may,
        serde_json::json!({ "month": "2026-05", "items": [] }),
        "got: {may}"
    );
}

// ── 4. 別乗務員どうし・別テナント → 組にならない ────────────────────────────────

#[tokio::test]
async fn test_different_drivers_and_different_tenants_never_pair() {
    let store = require_db!();
    let t = store.tenant_id();
    let other_tenant = uuid::Uuid::new_v4();

    // 別乗務員 (同じテナント、同じ時間帯)
    insert_shift(
        store.pool(),
        t,
        1613,
        "2026-06-10 08:00:00",
        "2026-06-10 18:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1614,
        "2026-06-10 10:00:00",
        "2026-06-10 20:00:00",
    )
    .await;

    // 別テナント (同じ乗務員CD、同じ時間帯)
    insert_shift(
        store.pool(),
        other_tenant,
        1613,
        "2026-06-10 08:00:00",
        "2026-06-10 18:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        other_tenant,
        1613,
        "2026-06-10 10:00:00",
        "2026-06-10 20:00:00",
    )
    .await;

    let got = get(&store, t, "2026-06").await;
    assert_eq!(
        got,
        serde_json::json!({ "month": "2026-06", "items": [] }),
        "got: {got}"
    );

    // 他テナント側は自テナント内で重なるので 1 組出る (混ざっていないことの裏取り)
    let got_other = get(&store, other_tenant, "2026-06").await;
    assert_eq!(
        got_other,
        serde_json::json!({
            "month": "2026-06",
            "items": [pair(
                1613,
                "2026-06-10 08:00:00",
                "2026-06-10 18:00:00",
                "2026-06-10 10:00:00",
                "2026-06-10 20:00:00",
            )],
        }),
        "got: {got_other}"
    );
}

// ── 5. 3 本が互いに重なる → 3 組 (全組) ───────────────────────────────────────

#[tokio::test]
async fn test_three_mutually_overlapping_shifts_yield_all_three_pairs() {
    let store = require_db!();
    let t = store.tenant_id();
    // A: 08:00-14:00, B: 10:00-16:00, C: 12:00-18:00
    // A×B, B×C, A×C が全部重なる (開区間)
    insert_shift(
        store.pool(),
        t,
        1615,
        "2026-06-10 08:00:00",
        "2026-06-10 14:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1615,
        "2026-06-10 10:00:00",
        "2026-06-10 16:00:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1615,
        "2026-06-10 12:00:00",
        "2026-06-10 18:00:00",
    )
    .await;

    let got = get(&store, t, "2026-06").await;
    assert_eq!(
        got,
        serde_json::json!({
            "month": "2026-06",
            "items": [
                pair(1615, "2026-06-10 08:00:00", "2026-06-10 14:00:00", "2026-06-10 10:00:00", "2026-06-10 16:00:00"),
                pair(1615, "2026-06-10 08:00:00", "2026-06-10 14:00:00", "2026-06-10 12:00:00", "2026-06-10 18:00:00"),
                pair(1615, "2026-06-10 10:00:00", "2026-06-10 16:00:00", "2026-06-10 12:00:00", "2026-06-10 18:00:00"),
            ],
        }),
        "got: {got}"
    );
}

// ── 6a. 不正な month は 400 ───────────────────────────────────────────────────

#[tokio::test]
async fn test_a_malformed_month_is_bad_request() {
    let store = require_db!();
    for bad in ["2026-6", "2026-13", "nope", ""] {
        let (status, msg) = shift_overlaps(
            query(bad),
            Extension(Some(store.clone())),
            read(store.tenant_id()),
        )
        .await
        .expect_err("must reject a bad month");
        assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}");
        assert!(msg.contains("month"), "{msg}");
    }
}

// ── 6b. DB が読めないときは 502 (db_err の経路) ─────────────────────────────────

#[tokio::test]
async fn test_a_closed_pool_is_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let (status, msg) = shift_overlaps(
        query("2026-06"),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect_err("must fail on a closed pool");
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(msg.contains("shifts"), "{msg}");
}
