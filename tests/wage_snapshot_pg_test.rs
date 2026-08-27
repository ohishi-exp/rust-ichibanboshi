//! `POST /api/kintai/wage-snapshot` → `GET /api/kintai/wage-range` の**実 Postgres**
//! に対する往復検証 (Refs ohishi-exp/nuxt-dtako-admin#986 / #980)。
//!
//! ここでしか確かめられないのは、**`timecard_kosoku` が本当に保存されて読み出しで
//! 戻るか**と、**`timecard_kosoku` を送らない既存クライアントが壊れないか**。
//! 純ロジックのテスト (`src/wage_snapshot.rs`) は SQL を通らないので、列の綴り・
//! `unnest` の番号ずれ・CHECK 制約はここでしか落ちない。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、名前に `986-1` を入れる、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-986-1 -e POSTGRES_PASSWORD=postgres -p 127.0.0.1::5432 postgres:17
//! docker port kintai-pg-986-1 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:<port>/postgres \
//!   cargo test --test wage_snapshot_pg_test
//! ```

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use rust_ichibanboshi::kintai_push::KintaiPgStore;
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use rust_ichibanboshi::routes::wage_snapshot::{put_wage_snapshot, wage_range, RangeQuery};
use rust_ichibanboshi::wage_snapshot::{MonthMasters, SnapshotRequest, WageSnapshotRow};

// ── 前提 (tests/stale_months_pg_test.rs と同じ形) ──────────────────────────────

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
        .bind(986_001_001_i64)
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
        .bind(986_001_001_i64)
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

// ── 材料 ──────────────────────────────────────────────────────────────────────

fn row(driver_cd: i64) -> WageSnapshotRow {
    WageSnapshotRow {
        driver_cd,
        driver_name: "山田".to_string(),
        company: Some("0200".to_string()),
        branch_name: Some("本社".to_string()),
        branch_code: Some(210),
        job_name: Some("乗務員".to_string()),
        pay_kubun: Some(1),
        hourly_rate: Some(1420),
        calc_base: Some(200_000),
        calc_overtime: Some(80_000),
        calc_total: Some(280_000),
        paid_base: Some(198_000),
        paid_overtime: Some(78_000),
        working_minutes: Some(11_820),
        restraint_missing: false,
    }
}

fn req(comp: &str, timecard_kosoku: Option<&str>) -> SnapshotRequest {
    SnapshotRequest {
        comp_id: comp.to_string(),
        month: "2026-01".to_string(),
        restraint_source: "gcp".to_string(),
        timecard_kosoku: timecard_kosoku.map(str::to_string),
        wage_logic_version: "wage-1".to_string(),
        masters: MonthMasters::default(),
        rows: vec![row(1035)],
    }
}

async fn save(
    store: &Arc<KintaiPgStore>,
    tenant: uuid::Uuid,
    req: SnapshotRequest,
) -> serde_json::Value {
    put_wage_snapshot(
        Extension(Some(store.clone())),
        Extension(ReadTenant(Some(tenant))),
        axum::Json(req),
    )
    .await
    .expect("save")
    .0
}

/// その月の `months[0]` を引く (期間は 1 か月)。
async fn read_month(
    store: &Arc<KintaiPgStore>,
    tenant: uuid::Uuid,
    comp: &str,
) -> serde_json::Value {
    let q = RangeQuery {
        comp: Some(comp.to_string()),
        from: Some("2026-01".to_string()),
        to: Some("2026-01".to_string()),
        source: Some("gcp".to_string()),
        ..Default::default()
    };
    let body = wage_range(
        Query(q),
        Extension(Some(store.clone())),
        Extension(ReadTenant(Some(tenant))),
    )
    .await
    .expect("read")
    .0;
    body["months"][0].clone()
}

// ── 1. 3 値がそのまま往復する ─────────────────────────────────────────────────

/// **`no` と `unreadable` を畳まない** — 処方が逆 (前者は読み直せば入る / 後者は
/// 上流の応答の形が変わっている) なので、保存も読み出しも別の値のまま運ぶ。
#[tokio::test]
async fn test_timecard_kosoku_round_trips_for_every_state() {
    let store = require_db!();
    let tenant = store.tenant_id();

    for (i, state) in ["yes", "no", "unreadable"].iter().enumerate() {
        let comp = format!("comp-{i}");
        let saved = save(&store, tenant, req(&comp, Some(state))).await;
        assert_eq!(saved["saved"], 1, "{state}");
        assert_eq!(saved["timecard_kosoku"], *state, "{state}");

        let month = read_month(&store, tenant, &comp).await;
        assert_eq!(month["ym"], "2026-01", "{state}");
        assert_eq!(month["timecard_kosoku"], *state, "{state}");
    }
}

// ── 2. 送ってこない既存クライアントが壊れない ────────────────────────────────

/// **これが後方互換の要**。`timecard_kosoku` を送らない payload は保存でき、
/// 読み出しでは**列ごと出ない** (`"yes"` にも `false` 相当にも化けない)。
#[tokio::test]
async fn test_omitted_timecard_kosoku_saves_and_reads_back_as_absent() {
    let store = require_db!();
    let tenant = store.tenant_id();

    // 画面が送ってくる JSON をそのまま (この列を知らない既存クライアントの形)
    let body = r#"{"comp_id":"comp-omit","month":"2026-01","restraint_source":"gcp",
                   "wage_logic_version":"wage-1",
                   "rows":[{"driver_cd":1035,"driver_name":"山田",
                            "calc_base":200000,"calc_total":280000,"paid_base":198000}]}"#;
    let parsed: SnapshotRequest = serde_json::from_str(body).expect("既存の payload は通る");
    assert_eq!(parsed.timecard_kosoku, None);

    let saved = save(&store, tenant, parsed).await;
    assert_eq!(saved["saved"], 1);
    assert!(saved["timecard_kosoku"].is_null());

    let month = read_month(&store, tenant, "comp-omit").await;
    assert_eq!(month["drivers"], 1, "行はちゃんと保存されている");
    assert!(
        month.get("timecard_kosoku").is_none(),
        "見ていない月は列ごと出さない (揃っていた と混ぜない)"
    );
}

// ── 3. 土台の取得可否だけが変わった保存を捨てない ────────────────────────────

/// `skipped_unchanged` の判定に `timecard_kosoku` が入っていないと、**行が同じなら
/// 土台の取得可否の訂正が黙って捨てられる** — この issue と同じ「送っているのに
/// 残らない」型の穴になる。
#[tokio::test]
async fn test_changing_only_timecard_kosoku_is_not_skipped() {
    let store = require_db!();
    let tenant = store.tenant_id();

    let first = save(&store, tenant, req("comp-chg", Some("no"))).await;
    assert_eq!(first["skipped_unchanged"], false);

    // 全く同じ payload は書かない (既存の挙動)
    let again = save(&store, tenant, req("comp-chg", Some("no"))).await;
    assert_eq!(again["skipped_unchanged"], true);
    assert_eq!(again["timecard_kosoku"], "no");

    // 土台の取得可否だけを訂正した保存は通る
    let fixed = save(&store, tenant, req("comp-chg", Some("yes"))).await;
    assert_eq!(fixed["skipped_unchanged"], false);
    assert_eq!(
        read_month(&store, tenant, "comp-chg").await["timecard_kosoku"],
        "yes"
    );
}

// ── 4. 知らない値は 400 で止まる (DB の CHECK まで行かせない) ────────────────

#[tokio::test]
async fn test_unknown_timecard_kosoku_is_rejected_before_the_db() {
    let store = require_db!();
    let tenant = store.tenant_id();

    let err = put_wage_snapshot(
        Extension(Some(store.clone())),
        Extension(ReadTenant(Some(tenant))),
        axum::Json(req("comp-bad", Some("missing"))),
    )
    .await
    .expect_err("知らない値は弾く");
    assert_eq!(err.0, StatusCode::BAD_REQUEST);
    assert!(err.1.contains("timecard_kosoku"), "{}", err.1);
}
