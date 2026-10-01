//! `GET /api/kintai/shift-days` の**実 Postgres** に対する検証
//! (Refs ohishi-exp/nuxt-dtako-admin#1133)。
//!
//! ここでしか確かめられないのは、**3 表の束ね方** (勤務 1 本 = 1 要素、日別サマリの
//! 突き合わせと `null`、暦日の按分の昇順と 0 行)、**月の境界** (始業の月に出る)、
//! **テナント/乗務員の分離**。どれも実 DB を往復させないと分からない
//! (`tests/shift_overlaps_pg_test.rs` と同じ理由)。fixture は架空値だけ。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-1133-37 -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:16
//! docker port kintai-pg-1133-37 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:<port>/postgres \
//!   cargo test --test shift_days_pg_test
//! ```

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use rust_ichibanboshi::routes::shift_days::{shift_days, ShiftDaysQuery};

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

/// `kintai.shifts` に 1 本入れる。日別サマリと暦日の按分の FK が要求するので先に要る。
async fn insert_shift(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    start_at: &str,
    end_at: &str,
    shift_source: &str,
) {
    sqlx::query(
        "INSERT INTO kintai.shifts \
           (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version) \
         VALUES ($1, $2, $3, $4, $5, repeat('a', 64), repeat('0', 16))",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(jst_at(start_at).expect("start_at"))
    .bind(jst_at(end_at).expect("end_at"))
    .bind(shift_source)
    .execute(pool)
    .await
    .expect("insert shift");
}

/// `kintai.day_summaries` の 11 個の分数 (DDL の並び順)。
const SUMMARY_KEYS: [&str; 11] = [
    "restraint_minutes",
    "working_minutes",
    "break_minutes",
    "rest_minus_minutes",
    "statutory_minutes",
    "within_statutory_overtime_minutes",
    "overtime_minutes",
    "legal_holiday_minutes",
    "night_minutes",
    "overtime_night_minutes",
    "legal_holiday_night_minutes",
];

/// `kintai.day_summaries` に 1 行入れる。`minutes` は [`SUMMARY_KEYS`] の順。
async fn insert_day_summary(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    shift_start_at: &str,
    shift_source: &str,
    minutes: [i32; 11],
) {
    let date = chrono::NaiveDate::parse_from_str(&shift_start_at[..10], "%Y-%m-%d").expect("date");
    let mut q = sqlx::query(
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
    .bind(date)
    .bind(jst_at(shift_start_at).expect("shift_start_at"))
    .bind(shift_source);
    for m in minutes {
        q = q.bind(m);
    }
    q.execute(pool).await.expect("insert day_summary");
}

/// `kintai.day_parts` に 1 行入れる。`minutes` は 拘束 / 実働 / 深夜 の順。
async fn insert_day_part(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    shift_start_at: &str,
    date: &str,
    minutes: [i32; 3],
) {
    sqlx::query(
        "INSERT INTO kintai.day_parts \
           (tenant_id, driver_cd, shift_start_at, date, \
            restraint_minutes, working_minutes, night_minutes) \
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(jst_at(shift_start_at).expect("shift_start_at"))
    .bind(chrono::NaiveDate::parse_from_str(date, "%Y-%m-%d").expect("date"))
    .bind(minutes[0])
    .bind(minutes[1])
    .bind(minutes[2])
    .execute(pool)
    .await
    .expect("insert day_part");
}

fn query(month: &str, driver: &str) -> Query<ShiftDaysQuery> {
    Query(ShiftDaysQuery {
        month: Some(month.to_string()),
        driver: Some(driver.to_string()),
    })
}

/// 読み先のテナント (`[kintai_events] tenant_id`)。本番と同じく、これが正。
fn read(tenant: uuid::Uuid) -> Extension<ReadTenant> {
    Extension(ReadTenant(Some(tenant)))
}

async fn get(
    store: &Arc<KintaiPgStore>,
    tenant: uuid::Uuid,
    month: &str,
    driver: &str,
) -> serde_json::Value {
    shift_days(
        query(month, driver),
        Extension(Some(store.clone())),
        read(tenant),
    )
    .await
    .expect("handler")
    .0
}

fn summary(minutes: [i32; 11]) -> serde_json::Value {
    let map: serde_json::Map<String, serde_json::Value> = SUMMARY_KEYS
        .iter()
        .zip(minutes)
        .map(|(k, m)| (k.to_string(), serde_json::json!(m)))
        .collect();
    serde_json::Value::Object(map)
}

fn part(date: &str, minutes: [i32; 3]) -> serde_json::Value {
    serde_json::json!({
        "date": date,
        "restraint_minutes": minutes[0],
        "working_minutes": minutes[1],
        "night_minutes": minutes[2],
    })
}

fn item(
    start_at: &str,
    end_at: &str,
    shift_source: &str,
    summary: serde_json::Value,
    parts: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "start_at": start_at,
        "end_at": end_at,
        "shift_source": shift_source,
        "summary": summary,
        "parts": parts,
    })
}

// 日をまたぐ勤務 (22:10 → 翌 09:05、拘束 655 = 実働 595 + 休憩 60)
const OVERNIGHT: [i32; 11] = [655, 595, 60, 0, 450, 30, 115, 0, 350, 0, 0];
// 1 日で終わる勤務 (08:00 → 17:00、拘束 540 = 実働 480 + 休憩 60)
const DAYTIME: [i32; 11] = [540, 480, 60, 0, 450, 30, 0, 0, 0, 0, 0];

// ── 1. 始業の昇順・日別サマリの突き合わせと null・暦日の按分の昇順と 0 行 ────────

#[tokio::test]
async fn test_shifts_come_back_in_start_order_with_summary_and_parts() {
    let store = require_db!();
    let t = store.tenant_id();
    let pool = store.pool();

    // 入れる順は始業の順と変える (並びが SQL の ORDER BY で決まることを見る)
    // (a) 1 日で終わる勤務。日別サマリ在り・暦日の按分は 0 行
    insert_shift(
        pool,
        t,
        9001,
        "2026-04-10 08:00:00",
        "2026-04-10 17:00:00",
        "timecard",
    )
    .await;
    insert_day_summary(pool, t, 9001, "2026-04-10 08:00:00", "timecard", DAYTIME).await;
    // (b) 1 日で終わる勤務。**日別サマリが無い** (→ null)・暦日の按分が 1 行だけ在る
    insert_shift(
        pool,
        t,
        9001,
        "2026-04-20 09:00:00",
        "2026-04-20 18:00:00",
        "rest",
    )
    .await;
    insert_day_part(
        pool,
        t,
        9001,
        "2026-04-20 09:00:00",
        "2026-04-20",
        [540, 480, 0],
    )
    .await;
    // (c) 日をまたぐ勤務。暦日の按分は 2 行 (**後の暦日から入れる**)
    insert_shift(
        pool,
        t,
        9001,
        "2026-04-03 22:10:00",
        "2026-04-04 09:05:00",
        "timecard",
    )
    .await;
    insert_day_summary(pool, t, 9001, "2026-04-03 22:10:00", "timecard", OVERNIGHT).await;
    insert_day_part(
        pool,
        t,
        9001,
        "2026-04-03 22:10:00",
        "2026-04-04",
        [545, 485, 240],
    )
    .await;
    insert_day_part(
        pool,
        t,
        9001,
        "2026-04-03 22:10:00",
        "2026-04-03",
        [110, 110, 110],
    )
    .await;

    let got = get(&store, t, "2026-04", "9001").await;
    // 応答の実物 (架空値)。`cargo test -- --nocapture` で見える
    println!("{}", serde_json::to_string_pretty(&got).unwrap());

    assert_eq!(
        got,
        serde_json::json!({
            "month": "2026-04",
            "driver_cd": 9001,
            "items": [
                item(
                    "2026-04-03 22:10:00",
                    "2026-04-04 09:05:00",
                    "timecard",
                    summary(OVERNIGHT),
                    vec![
                        part("2026-04-03", [110, 110, 110]),
                        part("2026-04-04", [545, 485, 240]),
                    ],
                ),
                item(
                    "2026-04-10 08:00:00",
                    "2026-04-10 17:00:00",
                    "timecard",
                    summary(DAYTIME),
                    vec![],
                ),
                item(
                    "2026-04-20 09:00:00",
                    "2026-04-20 18:00:00",
                    "rest",
                    serde_json::Value::Null,
                    vec![part("2026-04-20", [540, 480, 0])],
                ),
            ],
        }),
        "got: {got}"
    );
}

// ── 2. 月の境界: 勤務は始業 (JST) の月に出る ───────────────────────────────────

#[tokio::test]
async fn test_a_shift_belongs_to_the_month_its_start_falls_in() {
    let store = require_db!();
    let t = store.tenant_id();
    let pool = store.pool();

    // 前月末に始業して当月へまたぐ → 3 月に出る (当月ぶんの暦日の按分ごと)
    insert_shift(
        pool,
        t,
        9001,
        "2026-03-31 23:00:00",
        "2026-04-01 08:00:00",
        "rest",
    )
    .await;
    insert_day_part(
        pool,
        t,
        9001,
        "2026-03-31 23:00:00",
        "2026-03-31",
        [60, 60, 60],
    )
    .await;
    insert_day_part(
        pool,
        t,
        9001,
        "2026-03-31 23:00:00",
        "2026-04-01",
        [480, 480, 300],
    )
    .await;
    // 月初の 0 時ちょうどに始業 → 4 月に出る (下限は含む)
    insert_shift(
        pool,
        t,
        9001,
        "2026-04-01 00:00:00",
        "2026-04-01 09:00:00",
        "rest",
    )
    .await;
    // 翌月初の 0 時ちょうどに始業 → 4 月には出ない (上限は含まない)
    insert_shift(
        pool,
        t,
        9001,
        "2026-05-01 00:00:00",
        "2026-05-01 09:00:00",
        "rest",
    )
    .await;

    let null = serde_json::Value::Null;
    let march = get(&store, t, "2026-03", "9001").await;
    assert_eq!(
        march,
        serde_json::json!({
            "month": "2026-03",
            "driver_cd": 9001,
            "items": [item(
                "2026-03-31 23:00:00",
                "2026-04-01 08:00:00",
                "rest",
                null.clone(),
                vec![
                    part("2026-03-31", [60, 60, 60]),
                    part("2026-04-01", [480, 480, 300]),
                ],
            )],
        }),
        "got: {march}"
    );

    let april = get(&store, t, "2026-04", "9001").await;
    assert_eq!(
        april,
        serde_json::json!({
            "month": "2026-04",
            "driver_cd": 9001,
            "items": [item(
                "2026-04-01 00:00:00",
                "2026-04-01 09:00:00",
                "rest",
                null.clone(),
                vec![],
            )],
        }),
        "got: {april}"
    );

    let may = get(&store, t, "2026-05", "9001").await;
    assert_eq!(
        may,
        serde_json::json!({
            "month": "2026-05",
            "driver_cd": 9001,
            "items": [item(
                "2026-05-01 00:00:00",
                "2026-05-01 09:00:00",
                "rest",
                null,
                vec![],
            )],
        }),
        "got: {may}"
    );
}

// ── 3. 他の乗務員・他のテナントの行は出ない ─────────────────────────────────────

#[tokio::test]
async fn test_other_drivers_and_other_tenants_never_leak() {
    let store = require_db!();
    let t = store.tenant_id();
    let other_tenant = uuid::Uuid::new_v4();
    let pool = store.pool();
    let start = "2026-04-03 22:10:00";
    let end = "2026-04-04 09:05:00";

    // 同じ始業時刻の勤務を 3 つ: 自分 / 同じテナントの別の乗務員 / 別のテナントの同じ乗務員CD。
    // 分数を変えておき、突き合わせが (テナント, 乗務員, 始業) で行われることを見る
    for (tenant, driver_cd, restraint) in
        [(t, 9001, 655), (t, 9002, 600), (other_tenant, 9001, 500)]
    {
        let mut minutes = OVERNIGHT;
        minutes[0] = restraint;
        insert_shift(pool, tenant, driver_cd, start, end, "timecard").await;
        insert_day_summary(pool, tenant, driver_cd, start, "timecard", minutes).await;
        insert_day_part(
            pool,
            tenant,
            driver_cd,
            start,
            "2026-04-03",
            [restraint, 110, 110],
        )
        .await;
    }

    let expect = |driver_cd: i64, restraint: i32| {
        let mut minutes = OVERNIGHT;
        minutes[0] = restraint;
        serde_json::json!({
            "month": "2026-04",
            "driver_cd": driver_cd,
            "items": [item(
                start,
                end,
                "timecard",
                summary(minutes),
                vec![part("2026-04-03", [restraint, 110, 110])],
            )],
        })
    };

    let mine = get(&store, t, "2026-04", "9001").await;
    assert_eq!(mine, expect(9001, 655), "got: {mine}");
    let colleague = get(&store, t, "2026-04", "9002").await;
    assert_eq!(colleague, expect(9002, 600), "got: {colleague}");
    let other = get(&store, other_tenant, "2026-04", "9001").await;
    assert_eq!(other, expect(9001, 500), "got: {other}");

    // 勤務の無い乗務員は 200 + 空の items
    let nobody = get(&store, t, "2026-04", "9003").await;
    assert_eq!(
        nobody,
        serde_json::json!({ "month": "2026-04", "driver_cd": 9003, "items": [] }),
        "got: {nobody}"
    );
}

// ── 4a. 不正な month / driver は 400 ──────────────────────────────────────────

#[tokio::test]
async fn test_a_malformed_month_or_driver_is_bad_request() {
    let store = require_db!();
    for (month, driver, word) in [
        ("2026-4", "9001", "month"),
        ("nope", "9001", "month"),
        ("2026-04", "", "driver"),
        ("2026-04", "abc", "driver"),
    ] {
        let (status, msg) = shift_days(
            query(month, driver),
            Extension(Some(store.clone())),
            read(store.tenant_id()),
        )
        .await
        .expect_err("must reject a bad query");
        assert_eq!(status, StatusCode::BAD_REQUEST, "{month:?} {driver:?}");
        assert!(msg.contains(word), "{msg}");
    }
}

// ── 4b. DB が読めないときは 502 (db_err の経路) ─────────────────────────────────

#[tokio::test]
async fn test_a_closed_pool_is_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let (status, msg) = shift_days(
        query("2026-04", "9001"),
        Extension(Some(store.clone())),
        read(store.tenant_id()),
    )
    .await
    .expect_err("must fail on a closed pool");
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(msg.contains("shift-days"), "{msg}");
}
