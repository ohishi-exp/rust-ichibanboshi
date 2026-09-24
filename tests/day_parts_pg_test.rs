//! `GET /api/kintai/day-parts` の**実 Postgres** に対する検証
//! (Refs ohishi-exp/nuxt-dtako-admin#1123)。
//!
//! ここでしか確かめられないのは、**乗務員 × 暦日の `SUM` が正しく割れているか**
//! (同じ日の別勤務は足され、0 時をまたぐ勤務は暦日ごとに分かれる)、**日内で終わる勤務
//! (day_parts を持たず day_summaries の行だけ) も足され、0 時をまたぐ勤務の
//! day_summaries は二重に足されないか**、**テナント分離・月の境界が効いているか**。
//! どれも実 DB を往復させないと分からない (`tests/kintai_day_summaries_pg_test.rs` と同じ理由)。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-1121-15 -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:16
//! docker port kintai-pg-1121-15 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:<port>/postgres \
//!   cargo test --test day_parts_pg_test
//! ```

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::routes::day_parts::{day_parts, DayPartsQuery};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;

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
        .bind(1_121_012_001_i64)
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
        .bind(1_121_012_001_i64)
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

/// `kintai.shifts` に 1 本入れる。`day_parts` の FK が要求するので、暦日の行より先に要る。
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

/// `kintai.day_parts` に 1 行入れる。対応する `shifts` 行は [`insert_shift`] で先に入れる。
async fn insert_day_part(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    shift_start_at: &str,
    date: &str,
    restraint: i32,
) {
    sqlx::query(
        "INSERT INTO kintai.day_parts \
           (tenant_id, driver_cd, shift_start_at, date, \
            restraint_minutes, working_minutes, night_minutes) \
         VALUES ($1, $2, $3, $4, $5, 0, 0)",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(jst_at(shift_start_at).expect("shift_start_at"))
    .bind(chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("date"))
    .bind(restraint)
    .execute(pool)
    .await
    .expect("insert day_part");
}

/// `kintai.day_summaries` に 1 行入れる (勤務 1 本 = 1 行)。`date` は始業日
/// (`migrations/002` の CHECK) なので `shift_start_at` の日付部分を使う。
/// 対応する `shifts` 行は [`insert_shift`] で先に入れる。
async fn insert_day_summary(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    shift_start_at: &str,
    restraint: i32,
) {
    sqlx::query(
        "INSERT INTO kintai.day_summaries \
           (tenant_id, driver_cd, date, shift_start_at, shift_source, \
            restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, \
            statutory_minutes, within_statutory_overtime_minutes, overtime_minutes, \
            legal_holiday_minutes, night_minutes, overtime_night_minutes, \
            legal_holiday_night_minutes, fingerprint, logic_version) \
         VALUES ($1, $2, $3, $4, 'rest', $5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, \
                 repeat('a', 64), repeat('0', 16))",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(chrono::NaiveDate::parse_from_str(&shift_start_at[..10], "%Y-%m-%d").expect("date"))
    .bind(jst_at(shift_start_at).expect("shift_start_at"))
    .bind(restraint)
    .execute(pool)
    .await
    .expect("insert day_summary");
}

fn query(month: &str) -> Query<DayPartsQuery> {
    Query(DayPartsQuery {
        month: Some(month.to_string()),
    })
}

/// 読み先のテナント (`[kintai_events] tenant_id`)。本番と同じく、これが正。
fn read(tenant: uuid::Uuid) -> Extension<ReadTenant> {
    Extension(ReadTenant(Some(tenant)))
}

async fn get(store: &Arc<KintaiPgStore>, tenant: uuid::Uuid, month: &str) -> serde_json::Value {
    day_parts(query(month), Extension(Some(store.clone())), read(tenant))
        .await
        .expect("handler")
        .0
}

// ── 1. 同じ乗務員・同じ暦日の別勤務 2 本は SUM される ───────────────────────────

/// 二重 (条件 3) の検知はこの SUM が 1440 を超えるかで見る。844 + 948 = 1792。
#[tokio::test]
async fn test_two_shifts_on_the_same_date_are_summed() {
    let store = require_db!();
    let t = store.tenant_id();
    insert_shift(
        store.pool(),
        t,
        1518,
        "2026-06-10 00:10:00",
        "2026-06-10 14:14:00",
    )
    .await;
    insert_shift(
        store.pool(),
        t,
        1518,
        "2026-06-10 08:00:00",
        "2026-06-10 23:48:00",
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1518,
        "2026-06-10 00:10:00",
        "2026-06-10",
        844,
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1518,
        "2026-06-10 08:00:00",
        "2026-06-10",
        948,
    )
    .await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got,
        serde_json::json!({
            "month": "2026-06",
            "items": [
                { "driver_cd": 1518, "date": "2026-06-10", "restraint_minutes": 1792 },
            ],
        }),
        "got: {got}"
    );
}

// ── 2. 0 時をまたぐ勤務 1 本は暦日ごとに別の item になる ─────────────────────────

#[tokio::test]
async fn test_a_shift_across_midnight_is_split_per_date() {
    let store = require_db!();
    let t = store.tenant_id();
    insert_shift(
        store.pool(),
        t,
        1740,
        "2026-06-17 19:00:00",
        "2026-06-18 07:30:00",
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1740,
        "2026-06-17 19:00:00",
        "2026-06-17",
        300,
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1740,
        "2026-06-17 19:00:00",
        "2026-06-18",
        450,
    )
    .await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got["items"],
        serde_json::json!([
            { "driver_cd": 1740, "date": "2026-06-17", "restraint_minutes": 300 },
            { "driver_cd": 1740, "date": "2026-06-18", "restraint_minutes": 450 },
        ]),
        "got: {got}"
    );
}

// ── 3. 他テナントの行・対象月外の行は返らない ─────────────────────────────────

#[tokio::test]
async fn test_other_tenants_and_other_months_are_excluded() {
    let store = require_db!();
    let t = store.tenant_id();
    let other = uuid::Uuid::new_v4();

    // 対象: 2026-06-30 (月末)。7-01 に掛かる部分は翌月なので返らない
    insert_shift(
        store.pool(),
        t,
        1051,
        "2026-06-30 20:00:00",
        "2026-07-01 05:00:00",
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1051,
        "2026-06-30 20:00:00",
        "2026-06-30",
        240,
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1051,
        "2026-06-30 20:00:00",
        "2026-07-01",
        300,
    )
    .await;
    // 前月末 (5-31) は返らない
    insert_shift(
        store.pool(),
        t,
        1051,
        "2026-05-31 08:00:00",
        "2026-05-31 17:00:00",
    )
    .await;
    insert_day_part(
        store.pool(),
        t,
        1051,
        "2026-05-31 08:00:00",
        "2026-05-31",
        540,
    )
    .await;
    // 他テナントの同じ乗務員・同じ日は返らない (足されてもいけない)
    insert_shift(
        store.pool(),
        other,
        1051,
        "2026-06-30 08:00:00",
        "2026-06-30 17:00:00",
    )
    .await;
    insert_day_part(
        store.pool(),
        other,
        1051,
        "2026-06-30 08:00:00",
        "2026-06-30",
        540,
    )
    .await;

    let got = get(&store, t, "2026-06").await;
    assert_eq!(
        got["items"],
        serde_json::json!([
            { "driver_cd": 1051, "date": "2026-06-30", "restraint_minutes": 240 },
        ]),
        "got: {got}"
    );

    // 他テナントから見ると自分の 1 行だけ
    let got_other = get(&store, other, "2026-06").await;
    assert_eq!(
        got_other["items"],
        serde_json::json!([
            { "driver_cd": 1051, "date": "2026-06-30", "restraint_minutes": 540 },
        ]),
        "got: {got_other}"
    );

    // データの無い月は 200 + 空の items
    let empty = get(&store, t, "2026-04").await;
    assert_eq!(
        empty,
        serde_json::json!({ "month": "2026-04", "items": [] }),
        "got: {empty}"
    );
}

// ── 4. 不正な month は 400 ───────────────────────────────────────────────────

#[tokio::test]
async fn test_a_malformed_month_is_bad_request() {
    let store = require_db!();
    for bad in ["2026-6", "2026-13", "nope", ""] {
        let (status, msg) = day_parts(
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

// ── 5. DB が読めないときは 502 (db_err の経路) ─────────────────────────────────

#[tokio::test]
async fn test_a_closed_pool_is_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let (status, msg) = day_parts(
        query("2026-06"),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect_err("must fail on a closed pool");
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(msg.contains("day_parts"), "{msg}");
}

// ── 6. 日内で終わる勤務 2 本 (day_parts 0 行) も SUM される ──────────────────────

/// `kosoku.rs` の `daily_summary` は 1 日で終わる勤務の内訳を出さないので、
/// その勤務は day_parts を 1 行も持たない。day_summaries の行がそのまま暦日の値。
/// 600 + 900 = 1500。
#[tokio::test]
async fn test_two_single_day_shifts_are_summed_from_day_summaries() {
    let store = require_db!();
    let t = store.tenant_id();
    let p = store.pool();
    insert_shift(p, t, 1018, "2026-06-05 03:10:00", "2026-06-05 13:10:00").await;
    insert_shift(p, t, 1018, "2026-06-05 08:30:00", "2026-06-05 23:30:00").await;
    insert_day_summary(p, t, 1018, "2026-06-05 03:10:00", 600).await;
    insert_day_summary(p, t, 1018, "2026-06-05 08:30:00", 900).await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got,
        serde_json::json!({
            "month": "2026-06",
            "items": [
                { "driver_cd": 1018, "date": "2026-06-05", "restraint_minutes": 1500 },
            ],
        }),
        "got: {got}"
    );
}

// ── 7. 日内の勤務 + 同じ暦日に掛かる 0 時またぎ勤務の day_parts は足される ────────

#[tokio::test]
async fn test_a_single_day_shift_and_a_midnight_part_are_summed() {
    let store = require_db!();
    let t = store.tenant_id();
    let p = store.pool();
    // 0 時またぎ: 06-11 に 240、06-12 に 240。勤務全体の day_summaries (480) は足さない
    insert_shift(p, t, 1300, "2026-06-11 20:00:00", "2026-06-12 04:00:00").await;
    insert_day_part(p, t, 1300, "2026-06-11 20:00:00", "2026-06-11", 240).await;
    insert_day_part(p, t, 1300, "2026-06-11 20:00:00", "2026-06-12", 240).await;
    insert_day_summary(p, t, 1300, "2026-06-11 20:00:00", 480).await;
    // 日内: 06-12 に 540 (day_parts なし)
    insert_shift(p, t, 1300, "2026-06-12 06:00:00", "2026-06-12 15:00:00").await;
    insert_day_summary(p, t, 1300, "2026-06-12 06:00:00", 540).await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got["items"],
        serde_json::json!([
            { "driver_cd": 1300, "date": "2026-06-11", "restraint_minutes": 240 },
            { "driver_cd": 1300, "date": "2026-06-12", "restraint_minutes": 780 },
        ]),
        "got: {got}"
    );
}

// ── 8. 0 時またぎ勤務の day_summaries は二重に足さない ─────────────────────────

#[tokio::test]
async fn test_a_midnight_shift_counts_only_its_day_parts() {
    let store = require_db!();
    let t = store.tenant_id();
    let p = store.pool();
    insert_shift(p, t, 1740, "2026-06-17 19:00:00", "2026-06-18 07:30:00").await;
    insert_day_part(p, t, 1740, "2026-06-17 19:00:00", "2026-06-17", 300).await;
    insert_day_part(p, t, 1740, "2026-06-17 19:00:00", "2026-06-18", 450).await;
    insert_day_summary(p, t, 1740, "2026-06-17 19:00:00", 750).await;

    let got = get(&store, t, "2026-06").await;

    assert_eq!(
        got["items"],
        serde_json::json!([
            { "driver_cd": 1740, "date": "2026-06-17", "restraint_minutes": 300 },
            { "driver_cd": 1740, "date": "2026-06-18", "restraint_minutes": 450 },
        ]),
        "got: {got}"
    );
}

// ── 9. 月末に始まり翌月へまたぐ勤務は、月ごとに day_parts の分だけ ──────────────

/// day_summaries の行 (始業日 = 当月) は、day_parts がどの月にあっても足さない。
/// 1052 は始業日側の day_parts が 0 分で積まれず (`kintai_fold::fold_days` の
/// 「全部 0 の暦日は保存しない」)、day_parts が**翌月にしか無い**勤務。NOT EXISTS に
/// 月の条件を入れると、当月で day_summaries の 480 を数え直してしまう。
#[tokio::test]
async fn test_a_shift_across_the_month_end_splits_by_month() {
    let store = require_db!();
    let t = store.tenant_id();
    let p = store.pool();
    insert_shift(p, t, 1051, "2026-06-30 20:00:00", "2026-07-01 05:00:00").await;
    insert_day_part(p, t, 1051, "2026-06-30 20:00:00", "2026-06-30", 240).await;
    insert_day_part(p, t, 1051, "2026-06-30 20:00:00", "2026-07-01", 300).await;
    insert_day_summary(p, t, 1051, "2026-06-30 20:00:00", 540).await;
    insert_shift(p, t, 1052, "2026-06-30 23:59:40", "2026-07-01 08:00:00").await;
    insert_day_part(p, t, 1052, "2026-06-30 23:59:40", "2026-07-01", 480).await;
    insert_day_summary(p, t, 1052, "2026-06-30 23:59:40", 480).await;

    let june = get(&store, t, "2026-06").await;
    assert_eq!(
        june["items"],
        serde_json::json!([
            { "driver_cd": 1051, "date": "2026-06-30", "restraint_minutes": 240 },
        ]),
        "got: {june}"
    );

    let july = get(&store, t, "2026-07").await;
    assert_eq!(
        july["items"],
        serde_json::json!([
            { "driver_cd": 1051, "date": "2026-07-01", "restraint_minutes": 300 },
            { "driver_cd": 1052, "date": "2026-07-01", "restraint_minutes": 480 },
        ]),
        "got: {july}"
    );
}
