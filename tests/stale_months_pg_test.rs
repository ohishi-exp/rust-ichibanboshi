//! `GET /api/kintai/stale-months` の**実 Postgres** に対する検証 (Refs #620 の 1)。
//!
//! ここでしか確かめられないのは、**月ごとの `GROUP BY` が正しく割れているか**と
//! **データの無い月が 0 件で埋まるか**、**テナント分離が効いているか**。どれも
//! 実 DB を往復させないと分からない (`tests/kintai_day_summaries_pg_test.rs` と
//! 同じ理由)。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、名前に `620-1` を入れる、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-620-1 -e POSTGRES_PASSWORD=postgres -p 127.0.0.1::5432 postgres:17
//! docker port kintai-pg-620-1 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:<port>/postgres \
//!   cargo test --test stale_months_pg_test
//! ```

use std::sync::Arc;

use axum::extract::Query;
use axum::Extension;
use rust_ichibanboshi::kintai_fold::logic_version;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::kosoku::KosokuParams;
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use rust_ichibanboshi::routes::stale_months::{stale_months, StaleMonthsQuery};

// ── 前提 (tests/kintai_day_summaries_pg_test.rs と同じ形) ───────────────────────

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
        .bind(620_001_001_i64)
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
        .bind(620_001_001_i64)
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

/// `kintai.shifts` に 1 本入れる。`day_summaries` の FK が要求するので先に要る。
async fn insert_shift(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    start_at: &str,
    end_at: &str,
    logic_version: &str,
) {
    sqlx::query(
        "INSERT INTO kintai.shifts \
           (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version) \
         VALUES ($1, $2, $3, $4, 'rest', repeat('a', 64), $5)",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(jst_at(start_at).expect("start_at"))
    .bind(jst_at(end_at).expect("end_at"))
    .bind(logic_version)
    .execute(pool)
    .await
    .expect("insert shift");
}

/// `kintai.day_summaries` に 1 行入れる (分数は全部ゼロで良い — この口は分数を
/// 見ない)。対応する `shifts` 行は [`insert_shift`] で先に入れておくこと。
async fn insert_day_summary(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    date: &str,
    shift_start_at: &str,
    logic_version: &str,
) {
    sqlx::query(
        "INSERT INTO kintai.day_summaries \
           (tenant_id, driver_cd, date, shift_start_at, shift_source, \
            restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, \
            statutory_minutes, within_statutory_overtime_minutes, overtime_minutes, \
            legal_holiday_minutes, night_minutes, overtime_night_minutes, \
            legal_holiday_night_minutes, fingerprint, logic_version) \
         VALUES ($1, $2, $3, $4, 'rest', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                 repeat('a', 64), $5)",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("date"))
    .bind(jst_at(shift_start_at).expect("shift_start_at"))
    .bind(logic_version)
    .execute(pool)
    .await
    .expect("insert day_summary");
}

/// 1 乗務員・1 暦日ぶんを指定した `logic_version` で入れる (shift + day_summary)。
async fn seed(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    date: &str,
    hh: &str,
    logic_version: &str,
) {
    let start = format!("{date} {hh}:00:00");
    let end = format!("{date} {hh}:30:00");
    insert_shift(pool, tenant, driver_cd, &start, &end, logic_version).await;
    insert_day_summary(pool, tenant, driver_cd, date, &start, logic_version).await;
}

fn q(from: Option<&str>, to: Option<&str>) -> Query<StaleMonthsQuery> {
    Query(StaleMonthsQuery {
        from: from.map(str::to_string),
        to: to.map(str::to_string),
        today: None,
    })
}

fn read(tenant: uuid::Uuid) -> Extension<ReadTenant> {
    Extension(ReadTenant(Some(tenant)))
}

fn params() -> Extension<Arc<KosokuParams>> {
    Extension(Arc::new(KosokuParams::default()))
}

fn current_version() -> String {
    logic_version(&KosokuParams::default())
}

// ── 1. 月ごとに正しく割れる。データの無い月は 0 で埋まる ─────────────────────────

#[tokio::test]
async fn test_stale_drivers_are_grouped_per_month_and_empty_months_are_zero() {
    let store = require_db!();
    let tenant = store.tenant_id();
    let current = current_version();
    let old = "1111111111111111"; // 16 桁

    // 2026-04: データ無し (0 件のまま返るはず)
    // 2026-05: 2 乗務員 — 1 人は旧版 (stale)、1 人は現行版 (stale ではない)
    seed(store.pool(), tenant, 1001, "2026-05-10", "09", old).await;
    seed(store.pool(), tenant, 1002, "2026-05-11", "10", &current).await;
    // 2026-06: 1 乗務員が旧版
    seed(store.pool(), tenant, 1003, "2026-06-01", "08", old).await;

    let got = stale_months(
        q(Some("2026-04"), Some("2026-06")),
        Extension(Some(store.clone())),
        read(tenant),
        params(),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["logic_version"], current);
    assert_eq!(got["from"], "2026-04");
    assert_eq!(got["to"], "2026-06");
    let months = got["months"].as_array().expect("months array");
    assert_eq!(months.len(), 3, "got: {got}");
    assert_eq!(months[0]["month"], "2026-04");
    assert_eq!(months[0]["stale_drivers"], 0);
    assert_eq!(months[1]["month"], "2026-05");
    assert_eq!(months[1]["stale_drivers"], 1, "got: {got}");
    assert_eq!(months[2]["month"], "2026-06");
    assert_eq!(months[2]["stale_drivers"], 1, "got: {got}");
}

// ── 2. 現行版だけの乗務員は stale に数えない ──────────────────────────────────

#[tokio::test]
async fn test_a_driver_with_only_current_version_rows_is_not_stale() {
    let store = require_db!();
    let tenant = store.tenant_id();
    let current = current_version();
    seed(store.pool(), tenant, 2001, "2026-07-05", "09", &current).await;

    let got = stale_months(
        q(Some("2026-07"), Some("2026-07")),
        Extension(Some(store.clone())),
        read(tenant),
        params(),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["months"][0]["stale_drivers"], 0, "got: {got}");
}

// ── 3. 別テナントの行が混ざらない ─────────────────────────────────────────────

#[tokio::test]
async fn test_other_tenant_rows_do_not_leak_in() {
    let store = require_db!();
    let other_tenant = uuid::Uuid::new_v4();
    seed(
        store.pool(),
        other_tenant,
        3001,
        "2026-08-01",
        "09",
        "1111111111111111",
    )
    .await;
    // 自テナント側は何も入れていない

    let got = stale_months(
        q(Some("2026-08"), Some("2026-08")),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
        params(),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(
        got["months"][0]["stale_drivers"], 0,
        "他テナントの行が混ざった: {got}"
    );
}

// ── 4. `today` を注入した既定範囲 (from/to 省略) ──────────────────────────────

#[tokio::test]
async fn test_default_range_uses_the_injected_today() {
    let store = require_db!();
    let tenant = store.tenant_id();
    seed(
        store.pool(),
        tenant,
        4001,
        "2026-06-15",
        "09",
        "1111111111111111",
    )
    .await;

    let got = stale_months(
        Query(StaleMonthsQuery {
            from: None,
            to: None,
            today: chrono::NaiveDate::from_ymd_opt(2026, 6, 20),
        }),
        Extension(Some(store.clone())),
        read(tenant),
        params(),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["to"], "2026-06");
    assert_eq!(got["from"], "2025-07");
    assert_eq!(got["default_window_months"], 12);
    let months = got["months"].as_array().unwrap();
    assert_eq!(months.len(), 12);
    let june = months.iter().find(|m| m["month"] == "2026-06").unwrap();
    assert_eq!(june["stale_drivers"], 1, "got: {got}");
}

// ── 5. 読み先も書き先も決まらない形は 503 ─────────────────────────────────────

#[tokio::test]
async fn test_no_tenant_anywhere_is_service_unavailable() {
    let configured = require_db!();
    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    let (status, msg) = stale_months(
        q(Some("2026-06"), Some("2026-06")),
        Extension(Some(store)),
        Extension(ReadTenant(None)),
        params(),
    )
    .await
    .expect_err("must fail when no tenant is configured at all");
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(msg.contains("kintai_events"), "{msg}");
}

// ── 6. **本番と同じ形** — 書き先の pin が空でも読める ─────────────────────────

#[tokio::test]
async fn test_rows_are_readable_when_the_write_pin_is_empty() {
    let configured = require_db!();
    let tenant = configured.tenant_id();
    seed(
        configured.pool(),
        tenant,
        5001,
        "2026-09-01",
        "09",
        "1111111111111111",
    )
    .await;

    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    assert!(store.tenant_id().is_nil(), "前提: 書き先の pin は nil");

    let got = stale_months(
        q(Some("2026-09"), Some("2026-09")),
        Extension(Some(store)),
        read(tenant),
        params(),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["months"][0]["stale_drivers"], 1, "got: {got}");
}

// ── 7. DB が引けない (pool が閉じている) は 502 ───────────────────────────────

#[tokio::test]
async fn test_db_error_maps_to_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let (status, msg) = stale_months(
        q(Some("2026-06"), Some("2026-06")),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
        params(),
    )
    .await
    .expect_err("closed pool must surface as an error");
    assert_eq!(status, axum::http::StatusCode::BAD_GATEWAY);
    assert!(msg.contains("day_summaries"));
}
