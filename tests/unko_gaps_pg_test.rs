//! `GET /api/kintai/unko-gaps` の**実 Postgres + alc モック**に対する検証
//! (Refs `ohishi-exp/nuxt-dtako-admin#623` の 1)。
//!
//! ここでしか確かめられないのは、**`MONTH_OPERATIONS_SQL` を自前で bind したときに
//! テナント分離と「書き込み pin が空でも読める」が保たれているか**と
//! **alc (wiremock) の etags 応答から実際に運行NO が出るか**。どちらも実 DB /
//! 実 HTTP 往復を通さないと分からない (`tests/stale_months_pg_test.rs` と同じ理由)。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら (**このタスク専用の
//! コンテナ**、名前に `623-1` を入れる、ホストポートはエフェメラル):
//!
//! ```text
//! docker run -d --name kintai-pg-623-1 -e POSTGRES_PASSWORD=postgres -p 127.0.0.1::5432 postgres:17
//! docker port kintai-pg-623-1 5432
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:<port>/postgres \
//!   cargo test --test unko_gaps_pg_test
//! ```
//!
//! wiremock (alc の etags モック) は接続先を持たないので `KINTAI_TEST_DATABASE_URL`
//! に関わらず動く — DB が無い環境では該当テストだけ skip される。

use std::sync::Arc;

use axum::extract::Query;
use axum::Extension;
use rust_ichibanboshi::config::{KintaiEventsConfig, KintaiEventsSource};
use rust_ichibanboshi::kintai_http_repo::HttpKintaiEventsRepo;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::kintai_repo::{DisabledKintaiEventsRepo, DynKintaiEventsRepo};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use rust_ichibanboshi::routes::unko_gaps::{unko_gaps, UnkoGapsQuery};

// ── 前提 (tests/stale_months_pg_test.rs と同じ形) ───────────────────────────

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
        .bind(623_001_001_i64)
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
        .bind(623_001_001_i64)
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

/// `kintai.kintai_events` に `unko_no` 付きの `dtako` 行を 1 本入れる
/// (`MONTH_OPERATIONS_SQL` が読むのはこの形だけ — `source = 'dtako'` かつ
/// `unko_no` が非 NULL/非空)。
async fn insert_dtako_event(
    pool: &sqlx::PgPool,
    tenant: uuid::Uuid,
    driver_cd: i64,
    occurred_at: &str,
    unko_no: &str,
) {
    sqlx::query(
        "INSERT INTO kintai.kintai_events \
           (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw) \
         VALUES ($1, $2, $3, '運行開始', 'dtako', $4, '{}'::jsonb)",
    )
    .bind(tenant)
    .bind(driver_cd)
    .bind(jst_at(occurred_at).expect("occurred_at"))
    .bind(unko_no)
    .execute(pool)
    .await
    .expect("insert dtako event");
}

fn q(month: &str, driver_cd: Option<i64>) -> Query<UnkoGapsQuery> {
    Query(UnkoGapsQuery {
        month: Some(month.to_string()),
        driver_cd,
    })
}

fn read(tenant: uuid::Uuid) -> Extension<ReadTenant> {
    Extension(ReadTenant(Some(tenant)))
}

/// 22 桁の GCP 側 `unko_no`。先頭 6 桁が `YYMMDD` (運行開始日)。
fn u(ymd: &str, seq: u32) -> String {
    format!("{ymd}{seq:016}")
}

/// wiremock を alc の etags 口として立て、`items` をそのまま返す repo を作る。
async fn wiremocked_repo(items_json: &str) -> (wiremock::MockServer, DynKintaiEventsRepo) {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/dtako/events/etags"))
        .respond_with(ResponseTemplate::new(200).set_body_string(items_json.to_string()))
        .mount(&server)
        .await;
    let cfg = KintaiEventsConfig {
        source: KintaiEventsSource::Http,
        base_url: server.uri(),
        tenant_id: "test-tenant".to_string(),
        ..Default::default()
    };
    let repo: DynKintaiEventsRepo =
        Arc::new(HttpKintaiEventsRepo::new(&cfg, None).expect("http repo"));
    (server, repo)
}

fn disabled_repo() -> DynKintaiEventsRepo {
    Arc::new(DisabledKintaiEventsRepo)
}

// ── 1. also_in_month の候補だけが出る (省略時) ────────────────────────────────

#[tokio::test]
async fn test_default_mode_returns_only_also_in_month_candidates() {
    let store = require_db!();
    let tenant = store.tenant_id();
    // 1445: 対象月にオンプレの運行が在る (= also_in_month の候補)
    insert_dtako_event(
        store.pool(),
        tenant,
        1445,
        "2026-06-05 09:00:00",
        "unrelated-onprem-1445",
    )
    .await;
    // 9999: 対象月にオンプレの運行が無い (= 候補ではない)

    let gap_1445 = u("260610", 1);
    let gap_9999 = u("260611", 1);
    let items = serde_json::json!({
        "items": [
            {"unko_no": gap_1445, "driver_cds": ["1445"]},
            {"unko_no": gap_9999, "driver_cds": ["9999"]},
        ]
    })
    .to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    assert!(got["gcp_etags_available"].as_bool().unwrap(), "{got}");
    let drivers = got["drivers"].as_array().expect("drivers array");
    assert_eq!(drivers.len(), 1, "9999 は候補ではないので出ない: {got}");
    assert_eq!(drivers[0]["driver_cd"], "1445");
    assert_eq!(drivers[0]["unko_nos"], serde_json::json!([gap_1445]));
}

// ── 2. 明示的な driver_cd はバケットを問わず返す ───────────────────────────────

#[tokio::test]
async fn test_explicit_driver_cd_bypasses_the_also_in_month_bucket() {
    let store = require_db!();
    let tenant = store.tenant_id();
    // 9999 は対象月にオンプレの運行が無いが、明示指定なら返る

    let gap = u("260611", 1);
    let items =
        serde_json::json!({"items": [{"unko_no": gap, "driver_cds": ["9999"]}]}).to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", Some(9999)),
        Extension(Some(store.clone())),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    let drivers = got["drivers"].as_array().expect("drivers array");
    assert_eq!(drivers.len(), 1, "{got}");
    assert_eq!(drivers[0]["driver_cd"], "9999");
}

// ── 3. オンプレに一致する運行があれば漏れとして出ない ─────────────────────────

#[tokio::test]
async fn test_a_matched_onprem_operation_is_not_reported_as_a_gap() {
    let store = require_db!();
    let tenant = store.tenant_id();
    let gap_22 = u("260610", 1);
    let onprem_23 = format!("{gap_22}1"); // 対象CD 1 桁を足した 23 桁 (一致する形)
    insert_dtako_event(
        store.pool(),
        tenant,
        1445,
        "2026-06-10 09:00:00",
        &onprem_23,
    )
    .await;

    let items =
        serde_json::json!({"items": [{"unko_no": gap_22, "driver_cds": ["1445"]}]}).to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    let drivers = got["drivers"].as_array().expect("drivers array");
    assert!(drivers.is_empty(), "一致した運行は漏れではない: {got}");
}

// ── 4. alc が driver_cds を返さない環境は unknown_driver に集める ──────────────

#[tokio::test]
async fn test_missing_driver_cds_falls_into_unknown_driver_bucket() {
    let store = require_db!();
    let tenant = store.tenant_id();
    insert_dtako_event(
        store.pool(),
        tenant,
        1445,
        "2026-06-05 09:00:00",
        "unrelated-onprem-1445",
    )
    .await;

    let gap = u("260610", 1);
    // driver_cds を持たない item (alc の前方互換フィールドの既定 = 省略)
    let items = serde_json::json!({"items": [{"unko_no": gap}]}).to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    assert!(
        !got["driver_cds_available"].as_bool().unwrap()
            || got["drivers"].as_array().unwrap().is_empty()
    );
    let unknown = got["unknown_driver_unko_nos"]
        .as_array()
        .expect("unknown array");
    assert_eq!(unknown, &vec![serde_json::Value::String(gap)], "{got}");
}

// ── 5. alc の etags 口そのものが使えない環境は「引けていない」と分かる ────────────

#[tokio::test]
async fn test_gcp_etags_unavailable_is_distinguishable_from_no_candidates() {
    let store = require_db!();
    let tenant = store.tenant_id();
    insert_dtako_event(
        store.pool(),
        tenant,
        1445,
        "2026-06-05 09:00:00",
        "unrelated-onprem-1445",
    )
    .await;

    // DisabledKintaiEventsRepo は fetch_dtako_month_digest の既定実装 (Ok(None)) しか
    // 持たない = alc の etags 口を持たない環境と同型
    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(disabled_repo()),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    assert_eq!(got["gcp_etags_available"], false, "{got}");
    assert!(
        got["drivers"].as_array().unwrap().is_empty(),
        "空だが '候補が居ない' ではなく '判定できない' 側: {got}"
    );
}

// ── 5b. alc への往復そのものが失敗した (5xx) 場合も「引けていない」に倒す ────────

#[tokio::test]
async fn test_gcp_etags_upstream_error_is_treated_as_unavailable_not_a_hard_failure() {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    let store = require_db!();
    let tenant = store.tenant_id();
    insert_dtako_event(
        store.pool(),
        tenant,
        1445,
        "2026-06-05 09:00:00",
        "unrelated-onprem-1445",
    )
    .await;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/dtako/events/etags"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let cfg = rust_ichibanboshi::config::KintaiEventsConfig {
        source: KintaiEventsSource::Http,
        base_url: server.uri(),
        tenant_id: "test-tenant".to_string(),
        ..Default::default()
    };
    let repo: DynKintaiEventsRepo =
        Arc::new(HttpKintaiEventsRepo::new(&cfg, None).expect("http repo"));

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("upstream error must not become a hard failure — it degrades softly")
    .0;

    assert_eq!(got["gcp_etags_available"], false, "{got}");
    assert!(got["drivers"].as_array().unwrap().is_empty());
}

// ── 6. 別テナントのオンプレ行が混ざらない ─────────────────────────────────────

#[tokio::test]
async fn test_other_tenant_onprem_rows_do_not_leak_in() {
    let store = require_db!();
    let other_tenant = uuid::Uuid::new_v4();
    // 他テナント側にだけ 1445 の当月運行を入れる
    insert_dtako_event(
        store.pool(),
        other_tenant,
        1445,
        "2026-06-05 09:00:00",
        "other-tenant-op",
    )
    .await;

    let gap = u("260610", 1);
    let items =
        serde_json::json!({"items": [{"unko_no": gap, "driver_cds": ["1445"]}]}).to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(repo),
        read(store.tenant_id()),
    )
    .await
    .expect("handler")
    .0;

    let drivers = got["drivers"].as_array().expect("drivers array");
    assert!(
        drivers.is_empty(),
        "他テナントの運行で also_in_month を誤判定した: {got}"
    );
}

// ── 7. **本番と同じ形** — 書き込み pin が空でも読める (Refs #205 の 23 の再現防止) ──

#[tokio::test]
async fn test_rows_are_readable_when_the_write_pin_is_empty() {
    let configured = require_db!();
    let tenant = configured.tenant_id();
    insert_dtako_event(
        configured.pool(),
        tenant,
        1445,
        "2026-06-05 09:00:00",
        "unrelated-onprem-1445",
    )
    .await;

    // 本番 GCP は [kintai_push] tenant_id を設定しない = 書き込み pin は nil
    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    assert!(store.tenant_id().is_nil(), "前提: 書き先の pin は nil");

    let gap = u("260610", 1);
    let items =
        serde_json::json!({"items": [{"unko_no": gap, "driver_cds": ["1445"]}]}).to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store)),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    let drivers = got["drivers"].as_array().expect("drivers array");
    assert_eq!(drivers.len(), 1, "書き込み pin が nil でも読める: {got}");
    assert_eq!(drivers[0]["driver_cd"], "1445");
}

// ── 8. 読み先も書き先も決まらない形は 503 ─────────────────────────────────────

#[tokio::test]
async fn test_no_tenant_anywhere_is_service_unavailable() {
    let configured = require_db!();
    let store = Arc::new(configured.for_tenant(uuid::Uuid::nil()));
    let (status, msg) = unko_gaps(
        q("2026-06", None),
        Extension(Some(store)),
        Extension(disabled_repo()),
        Extension(ReadTenant(None)),
    )
    .await
    .expect_err("must fail when no tenant is configured at all");
    assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
    assert!(msg.contains("kintai_events"), "{msg}");
}

// ── 9. DB が引けない (pool が閉じている) は 502 ───────────────────────────────

#[tokio::test]
async fn test_db_error_maps_to_bad_gateway() {
    let store = require_db!();
    store.pool().close().await;
    let (status, msg) = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(disabled_repo()),
        read(store.tenant_id()),
    )
    .await
    .expect_err("closed pool must surface as an error");
    assert_eq!(status, axum::http::StatusCode::BAD_GATEWAY);
    assert!(msg.contains("kintai_events"), "{msg}");
}

// ── 10. 実測 — Postgres 側 (自前クエリ 1 発) だけの所要時間 ──────────────────────
//
// alc への往復 (wiremock なので実ネットワークではないが、HTTP のラウンドトリップは
// 本物) を含めた `elapsed_ms` をログに出す。**本番の alc 往復コストの代わりにはならない**
// (モジュール docs 参照) — ローカルの Postgres + ローカルの wiremock だけの数字。

#[tokio::test]
async fn test_measures_elapsed_ms_for_the_local_pg_plus_wiremock_path() {
    let store = require_db!();
    let tenant = store.tenant_id();
    for i in 0..5 {
        insert_dtako_event(
            store.pool(),
            tenant,
            1445,
            &format!("2026-06-{:02} 09:00:00", 5 + i),
            &format!("unrelated-onprem-1445-{i}"),
        )
        .await;
    }
    let gap = u("260610", 1);
    let items =
        serde_json::json!({"items": [{"unko_no": gap, "driver_cds": ["1445"]}]}).to_string();
    let (_server, repo) = wiremocked_repo(&items).await;

    let got = unko_gaps(
        q("2026-06", None),
        Extension(Some(store.clone())),
        Extension(repo),
        read(tenant),
    )
    .await
    .expect("handler")
    .0;

    let elapsed = got["elapsed_ms"].as_u64().expect("elapsed_ms");
    eprintln!(
        "#623-1 実測 (ローカル Postgres + ローカル wiremock, alc 実網は含まない): {elapsed}ms"
    );
    assert!(got["drivers"].as_array().unwrap().len() == 1, "{got}");
}
