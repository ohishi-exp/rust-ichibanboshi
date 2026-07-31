//! 打刻の**読み返し**を実 Postgres で確かめる (Refs #205 の G6)。
//!
//! ここでしか確かめられないのは、**書いた打刻がそのままの壁時計で戻るか**と、
//! **戻る行の形が MariaDB 経路と同じか**。どちらも Rust 側だけ・Postgres 側だけでは
//! 検証にならない:
//!
//! - `TIMESTAMPTZ` を往復して 9 時間ずれると、拘束が丸ごと別の日に乗る。
//!   `to_char(... AT TIME ZONE 'Asia/Tokyo')` と書き込み側の `jst_at` が同じ解釈を
//!   していることは、実際に往復させないと分からない
//! - 行のキーが 1 つ違うだけで [`kosoku::drop_duplicate_rows`] の重複判定と
//!   `kintai_fold` の指紋が変わる (指紋は**行まるごと**を材料にする)。
//!   静かに「毎回 stale」へ倒れるので、キー構成は golden で固定する
//! - 合流 (`HttpKintaiEventsRepo` + Pg fallback) で `dtako_events` が二重にならないか
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する
//! (`tests/kintai_push_pg_test.rs` / `tests/kintai_fold_pg_test.rs` と同じ)。
//!
//! ```text
//! docker run -d --name kintai-test -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:17-alpine
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres \
//!   cargo test --test kintai_events_pg_test
//! ```
//!
//! テストは**テナントで分離**する (`kintai.*` の主キーは全て `tenant_id` 始まり)。

use std::sync::Arc;

use rust_ichibanboshi::config::{Config, KintaiEventsConfig, KintaiEventsSource};
use rust_ichibanboshi::kintai_http_repo::HttpKintaiEventsRepo;
use rust_ichibanboshi::kintai_pg_repo::PgKintaiEventsRepo;
use rust_ichibanboshi::kintai_push::{dedup_events, group_by_date, parse_rows, KintaiPgStore};
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use rust_ichibanboshi::server::build_kintai_events_repo;
use serde_json::{json, Value};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ── 前提 ──────────────────────────────────────────────────────────────────

fn database_url() -> Option<String> {
    std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// psql の**クライアント側変数** (`:'name'`) を使う migration か。
///
/// `sqlx::raw_sql` は psql ではないので `:'…'` がそのままサーバへ行き
/// `syntax error at or near ":"` になる。003 がこれ。**この harness では飛ばして
/// よい** — 用意するのは `kintai` スキーマで、資格情報はスキーマではない
/// (詳細は `kintai_push_pg_test.rs` の同名関数)。
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
        .bind(205_060_001_i64)
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
                // **黙って飛ばさない** — 何を流していないかはログに出す
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
        .bind(205_060_001_i64)
        .execute(pool)
        .await
        .expect("advisory unlock");
}

/// テスト 1 本ぶんの読み書き。テナントは毎回新しい UUID。
///
/// 読み側にも書き側と**同じテナント**を渡す。実運用では読みが
/// `[kintai_events] tenant_id`、書きが `X-Tenant-ID` と別経路で来るので、
/// 「一致しているのは前提」であることを束ねのタスクが assert する
/// (`kintai_pg_repo` のモジュール docs)。
async fn repos() -> Option<(Arc<KintaiPgStore>, PgKintaiEventsRepo)> {
    let url = database_url()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    ensure_schema(&pool).await;
    let tenant = uuid::Uuid::new_v4();
    let store = Arc::new(KintaiPgStore::from_pool(pool, tenant));
    let repo = PgKintaiEventsRepo::new(store.clone(), tenant);
    Some((store, repo))
}

macro_rules! require_db {
    () => {
        match repos().await {
            Some(v) => v,
            None => return,
        }
    };
}

/// MariaDB の打刻ブランチが返す 1 行。**この形のまま push 経路に食わせる** —
/// 書いたものが読み返せることを確かめたいので、入力は本番と同じ生行にする。
fn timecard_row(datetime: &str, driver: i64, state: &str) -> Value {
    json!({
        "datetime": datetime,
        "end_datetime": null,
        "driver_id": driver,
        "source": "timecard",
        "state": state,
        "unko_no": null,
        "vehicle": null,
    })
}

fn dtako_row(datetime: &str, driver: i64, state: &str, unko_no: &str) -> Value {
    json!({
        "datetime": datetime,
        "end_datetime": null,
        "driver_id": driver,
        "source": "dtako",
        "state": state,
        "unko_no": unko_no,
        "vehicle": null,
    })
}

/// 生行を**本番の書き込み経路で**入れる。raw INSERT にしないのは、読み返しの
/// 相手が「push が書いた行」でなければ往復の検証にならないため。
async fn push(store: &KintaiPgStore, rows: &[Value]) {
    let parsed = parse_rows(rows);
    assert!(
        parsed.rejected.is_empty(),
        "テストの入力が push に弾かれた: {:?}",
        parsed.rejected
    );
    let events = dedup_events(parsed.events);
    let mut by_driver: std::collections::BTreeMap<i64, Vec<_>> = std::collections::BTreeMap::new();
    for ev in events {
        by_driver.entry(ev.driver_cd).or_default().push(ev);
    }
    for (driver, evs) in by_driver {
        let changed = group_by_date(&evs);
        store
            .replace_days(driver, &changed, &[])
            .await
            .expect("replace_days");
    }
}

// ── 1. JST の壁時計が 9 時間ずれない ───────────────────────────────────────

#[tokio::test]
async fn test_jst_wall_clock_survives_the_round_trip() {
    let (store, repo) = require_db!();
    // 日の両端を入れる。ずれれば必ず暦日をまたぐので、9 時間ずれを見逃さない
    let rows = vec![
        timecard_row("2026-06-01 00:00:00", 1130, "始業"),
        timecard_row("2026-06-01 09:30:00", 1130, "終業"),
        timecard_row("2026-06-30 23:59:59", 1130, "始業"),
    ];
    push(&store, &rows).await;

    let got = repo
        .fetch_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00", 1130)
        .await
        .expect("fetch");

    let times: Vec<&str> = got
        .iter()
        .map(|r| r["datetime"].as_str().unwrap())
        .collect();
    assert_eq!(
        times,
        vec![
            "2026-06-01 00:00:00",
            "2026-06-01 09:30:00",
            "2026-06-30 23:59:59"
        ]
    );
}

#[tokio::test]
async fn test_window_is_half_open_in_jst() {
    let (store, repo) = require_db!();
    push(
        &store,
        &[
            timecard_row("2026-06-01 00:00:00", 1130, "始業"),
            timecard_row("2026-07-01 00:00:00", 1130, "始業"),
        ],
    )
    .await;

    // `[from, to)` — from ちょうどは入り、to ちょうどは入らない
    let got = repo
        .fetch_events_between("2026-06-01 00:00:00", "2026-07-01 00:00:00", 1130)
        .await
        .expect("fetch");
    assert_eq!(got.len(), 1);
    assert_eq!(got[0]["datetime"], "2026-06-01 00:00:00");
}

// ── 2. 行のキー構成が MariaDB ブランチ互換 ────────────────────────────────

/// 単一乗務員版は `kintai_repo` の `TIMECARD_EVENTS_SQL` → `row_to_json` と同じ 7 キー。
///
/// **`raw` は載らない。** 保存はしてあるが、載せると指紋の材料が膨らむうえ
/// MariaDB 経路に無いキーなので形が割れる。
#[tokio::test]
async fn test_single_driver_row_matches_the_mariadb_timecard_branch() {
    let (store, repo) = require_db!();
    push(
        &store,
        &[
            timecard_row("2026-06-02 08:00:00", 1130, "始業"),
            dtako_row(
                "2026-06-02 08:30:00",
                1130,
                "運行開始",
                "2606021025060000000272",
            ),
        ],
    )
    .await;

    let got = repo
        .fetch_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00", 1130)
        .await
        .expect("fetch");

    assert_eq!(
        got,
        vec![
            json!({
                "datetime": "2026-06-02 08:00:00",
                "end_datetime": null,
                "driver_id": 1130,
                "source": "timecard",
                "state": "始業",
                "unko_no": null,
                "vehicle": null,
            }),
            json!({
                "datetime": "2026-06-02 08:30:00",
                "end_datetime": null,
                "driver_id": 1130,
                "source": "dtako",
                "state": "運行開始",
                "unko_no": "2606021025060000000272",
                "vehicle": null,
            }),
        ]
    );
}

/// 全乗務員版は `ALL_EVENTS_SQL` → `all_row_to_json` と同じ 5 キー。
/// `unko_no` / `vehicle` は**キーごと出さない** (読んでいない列を `null` で埋めない)。
/// 並びは `ORDER BY driver_id, datetime, source`。
#[tokio::test]
async fn test_all_driver_rows_match_the_mariadb_all_events_shape() {
    let (store, repo) = require_db!();
    push(
        &store,
        &[
            timecard_row("2026-06-02 08:00:00", 1130, "始業"),
            dtako_row(
                "2026-06-02 07:00:00",
                1740,
                "運行開始",
                "2606021025060000000272",
            ),
        ],
    )
    .await;

    let got = repo
        .fetch_all_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00")
        .await
        .expect("fetch");

    assert_eq!(
        got,
        vec![
            json!({
                "datetime": "2026-06-02 08:00:00",
                "end_datetime": null,
                "driver_id": 1130,
                "source": "timecard",
                "state": "始業",
            }),
            json!({
                "datetime": "2026-06-02 07:00:00",
                "end_datetime": null,
                "driver_id": 1740,
                "source": "dtako",
                "state": "運行開始",
            }),
        ]
    );
}

/// `source` は push が作る 2 つに絞る。DDL は `alc_app` も許すので、絞らないと
/// 別経路が書いた行が打刻として混ざる (署名側 `STORED_SIGNATURES_SQL` と同じ絞り)。
#[tokio::test]
async fn test_only_pushed_sources_are_returned() {
    let (store, repo) = require_db!();
    push(&store, &[timecard_row("2026-06-02 08:00:00", 1130, "始業")]).await;
    sqlx::query(
        "INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source) \
         VALUES ($1, $2, $3, '始業', 'alc_app')",
    )
    .bind(store.tenant_id())
    .bind(1130_i64)
    .bind(
        chrono::DateTime::parse_from_str("2026-06-02 09:00:00 +0900", "%Y-%m-%d %H:%M:%S %z")
            .unwrap(),
    )
    .execute(store.pool())
    .await
    .expect("insert alc_app row");

    let one = repo
        .fetch_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00", 1130)
        .await
        .expect("fetch");
    let all = repo
        .fetch_all_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00")
        .await
        .expect("fetch all");
    assert_eq!(one.len(), 1, "alc_app の行が混ざった: {one:?}");
    assert_eq!(all.len(), 1, "alc_app の行が混ざった: {all:?}");
}

/// フェリーは `kintai` スキーマに無い。**空配列ではなく 503 で fail-closed** —
/// 突合はオンプレ専用 (#205 の決定 8) で、0 件と「口が無い」を混ぜない。
#[tokio::test]
async fn test_ferry_is_not_configured() {
    let (_store, repo) = require_db!();
    let err = repo
        .fetch_ferry_between("2026-06-01 00:00:00", "2026-07-01 00:00:00", Some(1130))
        .await
        .expect_err("ferry must fail closed");
    assert!(matches!(err, KintaiRepoError::NotConfigured));
}

// ── 3. 合流 — 打刻は Pg から、dtako_events は上流から (二重にならない) ─────

const KUDGIVT_HEADERS: &[&str] = &[
    "運行NO",
    "車輌名",
    "乗務員CD1",
    "対象乗務員CD",
    "開始日時",
    "終了日時",
    "イベントCD",
    "イベント名",
];

fn events_cfg(base_url: &str, tenant: uuid::Uuid) -> KintaiEventsConfig {
    KintaiEventsConfig {
        source: KintaiEventsSource::Http,
        base_url: base_url.to_string(),
        tenant_id: tenant.to_string(),
        timeout_secs: 10,
        auth_token: "test-id-token".to_string(),
        auth_token_command: String::new(),
        auth_token_metadata: false,
        auth_token_ttl_secs: 900,
    }
}

/// 上流 (`rust-alc-api` の `GET /api/dtako/events`) を 1 運行だけ返す形で stub する。
async fn stub_upstream(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/dtako/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "driver": {"cd": "1130", "name": "テスト乗務員"},
            "period": {"date_from": "2026-05-30", "date_to": "2026-07-01"},
            "operations": [{
                "unko_no": "2606021025060000000272",
                "crew_role": 1,
                "departure_at": null,
                "return_at": null,
                "headers": KUDGIVT_HEADERS,
                "rows": [[
                    "2606021025060000000272",
                    "帯広100け272",
                    "1740",
                    "1130",
                    "2026/06/02 12:00:00",
                    "2026/06/02 12:40:00",
                    "301",
                    "休憩",
                ]],
            }],
            "warnings": [],
        })))
        .mount(server)
        .await;
}

/// GCP の形そのもの: `dtako_events` は上流から、打刻は `kintai.kintai_events` から。
///
/// 上流には打刻の口が無く、Pg 側は `dtako_events` を持たないので、**両者は交わらない**。
/// 交わらせないのは `is_borrowed_source` の役目 — fallback から借りるのは
/// 上流に口が無い source だけで、`dtako_events` を借りると行が二重になる。
#[tokio::test]
async fn test_http_repo_merges_pg_timecard_without_duplicating_dtako_events() {
    let (store, pg) = require_db!();
    push(
        &store,
        &[
            timecard_row("2026-06-02 08:00:00", 1130, "始業"),
            dtako_row(
                "2026-06-02 08:30:00",
                1130,
                "運行開始",
                "2606021025060000000272",
            ),
            timecard_row("2026-06-02 19:00:00", 1130, "終業"),
        ],
    )
    .await;

    let server = MockServer::start().await;
    stub_upstream(&server).await;
    let fallback: DynKintaiEventsRepo = Arc::new(pg);
    let repo = HttpKintaiEventsRepo::new(
        &events_cfg(&server.uri(), store.tenant_id()),
        Some(fallback),
    )
    .expect("build http repo");

    let got = repo
        .fetch_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00", 1130)
        .await
        .expect("fetch");

    // 打刻 3 件 + 上流の休憩 1 件。時刻順に混ざる
    let seen: Vec<(&str, &str, &str)> = got
        .iter()
        .map(|r| {
            (
                r["datetime"].as_str().unwrap(),
                r["source"].as_str().unwrap(),
                r["state"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        vec![
            ("2026-06-02 08:00:00", "timecard", "始業"),
            ("2026-06-02 08:30:00", "dtako", "運行開始"),
            ("2026-06-02 12:00:00", "dtako_events", "休憩"),
            ("2026-06-02 19:00:00", "timecard", "終業"),
        ]
    );
    // 二重取りが起きていたら `dtako_events` が 2 行になる
    assert_eq!(
        got.iter().filter(|r| r["source"] == "dtako_events").count(),
        1
    );
    // 区間イベントは上流の行だけが `end_datetime` を持つ (打刻は点)
    assert_eq!(got[2]["end_datetime"], "2026-06-02 12:40:00");
    assert!(got[0]["end_datetime"].is_null());
}

/// 全乗務員経路でも同じ (`kintai_fold` の全乗務員 recalc と
/// `/api/kintai/kosoku-daily` が通る道)。
#[tokio::test]
async fn test_http_repo_merges_pg_timecard_for_all_drivers() {
    let (store, pg) = require_db!();
    push(
        &store,
        &[
            timecard_row("2026-06-02 08:00:00", 1130, "始業"),
            timecard_row("2026-06-02 07:00:00", 1740, "始業"),
        ],
    )
    .await;

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/dtako/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "period": {"date_from": "2026-05-30", "date_to": "2026-07-01"},
            "drivers": [],
            "next_after_driver_cd": null,
            "warnings": [],
        })))
        .mount(&server)
        .await;
    let fallback: DynKintaiEventsRepo = Arc::new(pg);
    let repo = HttpKintaiEventsRepo::new(
        &events_cfg(&server.uri(), store.tenant_id()),
        Some(fallback),
    )
    .expect("build http repo");

    let got = repo
        .fetch_all_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00")
        .await
        .expect("fetch all");

    let seen: Vec<(i64, &str)> = got
        .iter()
        .map(|r| {
            (
                r["driver_id"].as_i64().unwrap(),
                r["datetime"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        vec![(1130, "2026-06-02 08:00:00"), (1740, "2026-06-02 07:00:00")]
    );
}

// ── 4. 配線 — どの読み先が挿さったかが `/health` に出る ────────────────────

/// GCP の形の設定 (MariaDB 無し、上流は HTTP)。
fn gcp_toml(base_url: &str, tenant: uuid::Uuid) -> Config {
    toml::from_str(&format!(
        r#"
[kintai_events]
source = "http"
base_url = "{base_url}"
tenant_id = "{tenant}"
auth_token = "test-id-token"
"#
    ))
    .expect("config")
}

/// GCP の形では `fallback` に Pg が挿さり、`/health` の `backends.kintai_events` が
/// `http+pg` になる。**名前だけでなく実際に打刻が返ることまで見る** — 名前が付いた
/// のに読めていない状態は「静かに 0 件」で、`/health` が嘘をつく形になる。
#[tokio::test]
async fn test_gcp_shape_reads_timecard_through_the_pg_fallback() {
    let (store, _pg) = require_db!();
    push(&store, &[timecard_row("2026-06-02 08:00:00", 1130, "始業")]).await;

    let server = MockServer::start().await;
    stub_upstream(&server).await;
    let (repo, backend) = build_kintai_events_repo(
        &gcp_toml(&server.uri(), store.tenant_id()),
        Some(store.clone()),
    )
    .expect("build repo");

    assert_eq!(backend, "http+pg");
    let got = repo
        .fetch_events_between("2026-06-01 00:00:00", "2026-07-02 00:00:00", 1130)
        .await
        .expect("fetch");
    assert!(
        got.iter().any(|r| r["source"] == "timecard"),
        "打刻が読めていない: {got:?}"
    );
}

/// MariaDB があるなら Pg は挿さらない。オンプレでは MariaDB が打刻の正で、
/// Supabase 側はそれを写した結果 — 両方挿すと同じ打刻が二重に返る。
#[tokio::test]
async fn test_mariadb_wins_over_pg_when_both_are_declared() {
    let (store, _pg) = require_db!();
    let mut config = gcp_toml("http://127.0.0.1:1", store.tenant_id());
    config.mariadb.host = "db1".to_string();
    config.mariadb.database = "kintai".to_string();
    config.mariadb.password = "x".to_string();

    let (_repo, backend) = build_kintai_events_repo(&config, Some(store)).expect("build repo");
    assert_eq!(backend, "http");
}

/// どちらも無い形は今までどおり `http` のまま (打刻は読めない)。
/// **`http+pg` を名乗らない** — 読めないのに読めるように見せない。
#[tokio::test]
async fn test_http_without_any_fallback_stays_http() {
    let (_repo, backend) =
        build_kintai_events_repo(&gcp_toml("http://127.0.0.1:1", uuid::Uuid::nil()), None)
            .expect("build repo");
    assert_eq!(backend, "http");
}
