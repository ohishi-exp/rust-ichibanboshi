//! `GET /api/kintai/day-summaries` の**実 Postgres** に対する検証 (Refs #205 の 18)。
//!
//! ここでしか確かめられないのは、**書いた行がそのままの形 (キー・列名) で戻るか**と
//! **テナント分離が効いているか**。どちらも実 DB を往復させないと分からない
//! (`tests/kintai_events_pg_test.rs` と同じ理由)。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら:
//!
//! ```text
//! docker run -d --name kintai-pg-205-18 -e POSTGRES_PASSWORD=postgres -p 127.0.0.1::5432 postgres:17
//! docker port kintai-pg-205-18 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:<port>/postgres \
//!   cargo test --test kintai_day_summaries_pg_test
//! ```
//!
//! テストは**テナントで分離**する (`kintai.*` の主キーは全て `tenant_id` 始まり)。

use std::sync::Arc;

use axum::extract::Query;
use axum::Extension;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::routes::kintai_day_summaries::{day_summaries, DaySummariesQuery};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;

// ── 前提 (tests/kintai_events_pg_test.rs / kintai_push_pg_test.rs と同じ形) ──────

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
        .bind(205_180_001_i64)
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
        .bind(205_180_001_i64)
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

/// `kintai.shifts` に 1 本入れる。`day_summaries` の FK
/// (`day_summaries_shift_fkey`) が要求するので、日別サマリより先に要る。
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

/// 1 勤務ぶんの分数 (`kintai.day_summaries` の 11 個の分数列、DDL の並び順)。
struct Minutes {
    restraint: i32,
    working: i32,
    break_: i32,
    rest_minus: i32,
    statutory: i32,
    within_statutory_overtime: i32,
    overtime: i32,
    legal_holiday: i32,
    night: i32,
    overtime_night: i32,
    legal_holiday_night: i32,
}

/// `kintai.day_summaries` に 1 行入れる。対応する `shifts` 行は
/// [`insert_shift`] で先に入れておくこと。
#[allow(clippy::too_many_arguments)]
async fn insert_day_summary(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    date: &str,
    shift_start_at: &str,
    shift_source: &str,
    m: &Minutes,
) {
    sqlx::query(
        "INSERT INTO kintai.day_summaries \
           (tenant_id, driver_cd, date, shift_start_at, shift_source, \
            restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, \
            statutory_minutes, within_statutory_overtime_minutes, overtime_minutes, \
            legal_holiday_minutes, night_minutes, overtime_night_minutes, \
            legal_holiday_night_minutes, fingerprint, logic_version) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, \
                 repeat('a', 64), repeat('0', 16))",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("date"))
    .bind(jst_at(shift_start_at).expect("shift_start_at"))
    .bind(shift_source)
    .bind(m.restraint)
    .bind(m.working)
    .bind(m.break_)
    .bind(m.rest_minus)
    .bind(m.statutory)
    .bind(m.within_statutory_overtime)
    .bind(m.overtime)
    .bind(m.legal_holiday)
    .bind(m.night)
    .bind(m.overtime_night)
    .bind(m.legal_holiday_night)
    .execute(pool)
    .await
    .expect("insert day_summary");
}

fn some_minutes() -> Minutes {
    Minutes {
        restraint: 23,
        working: 24,
        break_: 0,
        rest_minus: 0,
        statutory: 24,
        within_statutory_overtime: 0,
        overtime: 0,
        legal_holiday: 0,
        night: 0,
        overtime_night: 0,
        legal_holiday_night: 0,
    }
}

fn query(month: &str, driver: Option<&str>) -> Query<DaySummariesQuery> {
    Query(DaySummariesQuery {
        month: Some(month.to_string()),
        driver: driver.map(str::to_string),
    })
}

/// 読み先のテナント (`[kintai_events] tenant_id`)。**本番と同じく、これが正**
/// — `[kintai_push]` の pin ではない (Refs #205 の 23)。
fn read(tenant: uuid::Uuid) -> Extension<ReadTenant> {
    Extension(ReadTenant(Some(tenant)))
}

// ── 1. 月を指定して、入れた行がそのままの形で返る ─────────────────────────────

/// キーの作り方 (`乗務員CD|暦日|開始時刻`) と列名がオンプレ基準ファイル
/// (`onprem_day_summaries_2026-06.json`) と一致することを golden で固定する。
#[tokio::test]
async fn test_row_round_trips_with_the_onprem_key_and_column_shape() {
    let store = require_db!();
    insert_shift(
        store.pool(),
        store.tenant_id(),
        1518,
        "2026-06-17 19:09:00",
        "2026-06-17 19:33:00",
    )
    .await;
    insert_day_summary(
        store.pool(),
        store.tenant_id(),
        1518,
        "2026-06-17",
        "2026-06-17 19:09:00",
        "rest",
        &some_minutes(),
    )
    .await;

    let got = day_summaries(
        query("2026-06", None),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["month"], "2026-06");
    assert_eq!(got["rows"], 1);
    let entry = &got["summaries"]["1518|2026-06-17|2026-06-17 19:09:00"];
    assert_eq!(
        *entry,
        serde_json::json!({
            "shift_source": "rest",
            "restraint_minutes": 23,
            "working_minutes": 24,
            "break_minutes": 0,
            "rest_minus_minutes": 0,
            "statutory_minutes": 24,
            "within_statutory_overtime_minutes": 0,
            "overtime_minutes": 0,
            "legal_holiday_minutes": 0,
            "night_minutes": 0,
            "overtime_night_minutes": 0,
            "legal_holiday_night_minutes": 0,
        }),
        "got: {got}"
    );
}

// ── 2. driver 指定でその乗務員だけに絞られる ──────────────────────────────────

#[tokio::test]
async fn test_driver_filters_to_a_single_driver() {
    let store = require_db!();
    for (driver, hh) in [(1518_i64, "19"), (1740_i64, "07")] {
        insert_shift(
            store.pool(),
            store.tenant_id(),
            driver,
            &format!("2026-06-17 {hh}:09:00"),
            &format!("2026-06-17 {hh}:33:00"),
        )
        .await;
        insert_day_summary(
            store.pool(),
            store.tenant_id(),
            driver,
            "2026-06-17",
            &format!("2026-06-17 {hh}:09:00"),
            "rest",
            &some_minutes(),
        )
        .await;
    }

    let got = day_summaries(
        query("2026-06", Some("1518")),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["rows"], 1);
    assert!(got["summaries"]
        .as_object()
        .unwrap()
        .contains_key("1518|2026-06-17|2026-06-17 19:09:00"));
    assert!(!got["summaries"]
        .as_object()
        .unwrap()
        .contains_key("1740|2026-06-17|2026-06-17 07:09:00"));
}

// ── 3. 別テナントの行が混ざらない ─────────────────────────────────────────────

#[tokio::test]
async fn test_other_tenant_rows_do_not_leak_in() {
    let store = require_db!();
    let other_tenant = uuid::Uuid::new_v4();
    insert_shift(
        store.pool(),
        other_tenant,
        1518,
        "2026-06-17 19:09:00",
        "2026-06-17 19:33:00",
    )
    .await;
    insert_day_summary(
        store.pool(),
        other_tenant,
        1518,
        "2026-06-17",
        "2026-06-17 19:09:00",
        "rest",
        &some_minutes(),
    )
    .await;
    // 自テナント側は何も入れていない

    let got = day_summaries(
        query("2026-06", None),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["rows"], 0, "他テナントの行が混ざった: {got}");
    assert_eq!(got["summaries"], serde_json::json!({}));
}

// ── 3b. **本番と同じ形** — 書き先の pin が空でも読める (Refs #205 の 23) ───────

/// **このタスクで直したバグそのものの回帰テスト。**
///
/// 本番 GCP は `KINTAI_PUSH_TENANT_ID` を設定しない運用 (受け口が `X-Tenant-ID` で
/// 名乗る) なので、`KintaiPgStore::tenant_id()` は **nil UUID** になる。かつて
/// handler がそれを `WHERE tenant_id = $1` に bind していたため、行がいくら
/// 入っていても**本番では常に 0 件**が返っていた。
///
/// 読み先は `[kintai_events] tenant_id` の pin (`ReadTenant`) が正 — 畳んだ行が
/// そのテナントで書かれていることは `assert_same_tenant` が保証している。
#[tokio::test]
async fn test_rows_are_readable_when_the_write_pin_is_empty() {
    let configured = require_db!();
    let tenant = configured.tenant_id();
    insert_shift(
        configured.pool(),
        tenant,
        1518,
        "2026-06-17 19:09:00",
        "2026-06-17 19:33:00",
    )
    .await;
    insert_day_summary(
        configured.pool(),
        tenant,
        1518,
        "2026-06-17",
        "2026-06-17 19:09:00",
        "rest",
        &some_minutes(),
    )
    .await;

    // 本番 GCP の形: `[kintai_push] tenant_id` が空 = store の pin は nil
    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    assert!(store.tenant_id().is_nil(), "前提: 書き先の pin は nil");

    let got = day_summaries(query("2026-06", None), Extension(Some(store)), read(tenant))
        .await
        .expect("handler")
        .0;

    assert_eq!(got["rows"], 1, "書き先の pin が空だと 0 件になる: {got}");
    assert!(got["summaries"]
        .as_object()
        .unwrap()
        .contains_key("1518|2026-06-17|2026-06-17 19:09:00"));
}

/// 上と同じ「pin が nil」の形でも、**読み先のテナントが違えば引けない** —
/// `ReadTenant` に落としたことでテナント分離が緩んでいないことを固定する。
#[tokio::test]
async fn test_other_tenant_rows_do_not_leak_in_with_an_empty_write_pin() {
    let configured = require_db!();
    let other_tenant = uuid::Uuid::new_v4();
    insert_shift(
        configured.pool(),
        other_tenant,
        1518,
        "2026-06-17 19:09:00",
        "2026-06-17 19:33:00",
    )
    .await;
    insert_day_summary(
        configured.pool(),
        other_tenant,
        1518,
        "2026-06-17",
        "2026-06-17 19:09:00",
        "rest",
        &some_minutes(),
    )
    .await;

    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    let got = day_summaries(
        query("2026-06", None),
        Extension(Some(store)),
        read(configured.tenant_id()),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["rows"], 0, "他テナントの行が混ざった: {got}");
}

/// 読み先も書き先も決まらない形は **503**。nil で引いて 0 件を返すと
/// 「設定が無い」と「その月の勤務が無い」が区別できない。
#[tokio::test]
async fn test_no_tenant_anywhere_is_service_unavailable() {
    let configured = require_db!();
    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    let (status, msg) = day_summaries(
        query("2026-06", None),
        Extension(Some(store)),
        Extension(ReadTenant(None)),
    )
    .await
    .expect_err("must fail when no tenant is configured at all");
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(msg.contains("kintai_events"), "{msg}");
}

// ── 4. month 不正は 400 ───────────────────────────────────────────────────────

#[tokio::test]
async fn test_invalid_month_is_bad_request() {
    let store = require_db!();
    for month in ["2026-6", "", "2026-06-01"] {
        let (status, msg) = day_summaries(
            query(month, None),
            Extension(Some(store.clone())),
            read(store.tenant_id()),
        )
        .await
        .expect_err(&format!("{month:?} should be rejected"));
        assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
        assert!(msg.contains("month"));
    }
}

// ── 5. データ 0 件の月は 200 + 空 (404 にしない) ──────────────────────────────

#[tokio::test]
async fn test_empty_month_is_200_with_empty_summaries_not_404() {
    let store = require_db!();
    let got = day_summaries(
        query("2026-01", None),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect("handler must not error on an empty month");

    assert_eq!(got.0["rows"], 0);
    assert_eq!(got.0["summaries"], serde_json::json!({}));
}

// ── 6. store が挿さっていない instance は 503 ─────────────────────────────────

#[tokio::test]
async fn test_no_store_is_service_unavailable() {
    let (status, _msg) = day_summaries(
        query("2026-06", None),
        Extension(None),
        read(uuid::Uuid::new_v4()),
    )
    .await
    .expect_err("must fail without a store");
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
}

// ── 7. driver が不正な形 (数字以外 / u64 には収まるが i64 には収まらない) ────────

#[tokio::test]
async fn test_non_numeric_driver_is_bad_request() {
    let store = require_db!();
    let (status, msg) = day_summaries(
        query("2026-06", Some("abc")),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect_err("non-numeric driver must be rejected");
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    assert!(msg.contains("driver"));
}

/// `parse_driver` は `u64` まで受けるが、DB 列は `BIGINT` (= `i64`) — その隙間
/// (`i64::MAX` を超えるが `u64` には収まる値) を弾けているかを確かめる。
#[tokio::test]
async fn test_driver_overflowing_i64_is_bad_request() {
    let store = require_db!();
    let too_big = (i64::MAX as u64 + 1).to_string();
    let (status, msg) = day_summaries(
        query("2026-06", Some(&too_big)),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect_err("driver overflowing i64 must be rejected");
    assert_eq!(status, axum::http::StatusCode::BAD_REQUEST);
    assert!(msg.contains("driver"));
}

// ── 8. DB が引けない (pool が閉じている) は 502 ───────────────────────────────

#[tokio::test]
async fn test_db_error_maps_to_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let (status, msg) = day_summaries(
        query("2026-06", None),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect_err("closed pool must surface as an error");
    assert_eq!(status, axum::http::StatusCode::BAD_GATEWAY);
    assert!(msg.contains("day_summaries"));
}
