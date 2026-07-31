//! 月ゲートを**本番と同じロール**で触る (Refs ohishi-exp/rust-ichibanboshi#205 の 20)。
//!
//! ## なぜ既存の pg テストでは捕まらなかったか
//!
//! `tests/kintai_fold_pg_test.rs` は `KINTAI_TEST_DATABASE_URL` の**所有者**で繋ぐ。
//! 所有者は自分の表に何でもできるので、`GRANT` の有無を 1 度も通らない。本番
//! (`[kintai_push]`、`kintai_writer`) だけが権限検査を受ける。
//!
//! 2026-07-31 の実害: `migrations/004_fold_gate.sql` が `kintai.fold_gate` を
//! CREATE するだけで GRANT を書いていなかった (「001 の
//! `GRANT ... ON ALL TABLES IN SCHEMA kintai` がスキーマ単位で効く」という誤解。
//! 実際は実行時点の表にしか効かない)。`kintai_writer` から
//! `SELECT ... FROM kintai.fold_gate` が `permission denied` になり、
//! `POST /api/kintai/recalc` が **ログ 1 行も出さずに 502** を返し続けた。
//! alc が `GET /api/dtako/events/etags` を本番へ出すまでは
//! `compute_month_digests` が 404 で手前 return していたので、**新口が出た瞬間に
//! 初めて踏んだ**。
//!
//! ここで縛るのは 2 つ:
//!
//! 1. **`kintai_writer` で gate が読めて書ける** (migrations/005 の回帰)
//! 2. **gate が読めない / 書けないときは口を落とさず degrade する**
//!    (`MonthGate::Unavailable` + loud warn。最適化の失敗で 502 にしない)
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ丸ごと skip する (他の pg テストと同じ)。
//! 接続先は **superuser 相当**である必要がある (`CREATE ROLE` / `ALTER ROLE` を使う。
//! `scripts/verify_kintai_rls.sh` が CI で同じことをしているのと同じ前提)。

use async_trait::async_trait;
use rust_ichibanboshi::kintai_fold::{month_gate_report, MonthGate};
use rust_ichibanboshi::kintai_push::KintaiPgStore;
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use rust_ichibanboshi::kosoku::KosokuParams;
use rust_ichibanboshi::routes::kintai_recalc::{recalc, RecalcRequest};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use std::str::FromStr;

const MONTH: &str = "2026-06";
const DRIVER: u64 = 1194;

/// 検証用に付けるパスワード。migration はパスワードを持たない (003 が psql の変数で
/// 与える) ので、テスト側で付けてから繋ぎ直す — `verify_kintai_rls.sh` と同じ手。
const VERIFY_PW: &str = "verify-only-not-a-real-password";

// ── 前提 ──────────────────────────────────────────────────────────────────

fn test_url() -> Option<String> {
    std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

async fn owner_pool() -> Option<sqlx::PgPool> {
    let url = test_url()?;
    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    ensure_schema(&pool).await;
    Some(pool)
}

/// psql の**クライアント側変数** (`:'name'`) を使う migration か
/// (`tests/kintai_fold_pg_test.rs` の同名関数と同じ理由で飛ばす)。
fn needs_psql_variables(sql: &str) -> bool {
    sql.contains(":'") || sql.contains(":\"")
}

/// **lock も migration も unlock も 1 本の接続で回す。** pool 越しに打つと lock と
/// unlock が別接続に当たり、解放できないまま次のテストが待ち続ける。
async fn ensure_schema(pool: &sqlx::PgPool) {
    use sqlx::Acquire;
    let mut conn = pool.acquire().await.expect("acquire");
    let c = conn.acquire().await.expect("conn");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(205_050_001_i64)
        .execute(&mut *c)
        .await
        .expect("lock");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'kintai')")
            .fetch_one(&mut *c)
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
                eprintln!("skip {} (psql の変数を使う migration)", f.display());
                continue;
            }
            sqlx::raw_sql(&sql)
                .execute(&mut *c)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", f.display()));
        }
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(205_050_001_i64)
        .execute(&mut *c)
        .await
        .expect("unlock");
}

macro_rules! require_db {
    () => {
        match owner_pool().await {
            Some(v) => v,
            None => return,
        }
    };
}

/// ロール操作 (`CREATE`/`DROP`/`ALTER ROLE`、`GRANT ... ON DATABASE`) を**直列化**する。
///
/// これらは `pg_authid` / `pg_database` という**共有カタログ**を書く。テストが既定の
/// 並列 (`cargo test` は 1 ファイル内をスレッドで回す) のまま同時に打つと
/// `ERROR: tuple concurrently updated` で落ちる (実害: 2026-07-31、`--test-threads=1`
/// では出ず `make cov-check` で初めて出た)。
///
/// **advisory lock は 1 本の接続に固定して取る** — pool 越しだと lock と unlock が
/// 別接続に当たって解放できない。
async fn locked_ddl(owner: &sqlx::PgPool, stmts: &[String]) {
    use sqlx::Acquire;
    let mut conn = owner.acquire().await.expect("acquire");
    let c = conn.acquire().await.expect("conn");
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(205_200_001_i64)
        .execute(&mut *c)
        .await
        .expect("lock");
    let mut failed = None;
    for s in stmts {
        if let Err(e) = sqlx::query(s).execute(&mut *c).await {
            failed = Some(format!("{s}: {e}"));
            break;
        }
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(205_200_001_i64)
        .execute(&mut *c)
        .await
        .expect("unlock");
    if let Some(msg) = failed {
        panic!("{msg}");
    }
}

/// `role` にパスワードを付けて繋ぎ直す。`KINTAI_TEST_DATABASE_URL` の host / port /
/// database はそのまま使う (文字列を組み立て直すと ?sslmode= 等を落とすため
/// [`PgConnectOptions`] を差し替える)。
async fn connect_as(owner: &sqlx::PgPool, role: &str) -> sqlx::PgPool {
    let opts = PgConnectOptions::from_str(&test_url().expect("url")).expect("parse url");
    let db = opts.get_database().unwrap_or("postgres").to_string();
    locked_ddl(
        owner,
        &[
            format!("ALTER ROLE {role} WITH PASSWORD '{VERIFY_PW}'"),
            format!("GRANT CONNECT ON DATABASE \"{db}\" TO {role}"),
        ],
    )
    .await;
    PgPoolOptions::new()
        .max_connections(2)
        .connect_with(opts.username(role).password(VERIFY_PW))
        .await
        .unwrap_or_else(|e| panic!("connect as {role}: {e}"))
}

/// `kintai_writer` と同じ形 (BYPASSRLS) で、**`fold_gate` の権限だけ持たない**ロール。
/// 004 だけを適用した本番 (= migrations/005 の前) と同じ状態を作る。
fn role_without_fold_gate_sql(role: &str) -> Vec<String> {
    // 権限を持ったままの DROP ROLE は依存で落ちるので DROP OWNED BY を先に打つ
    // (存在しないロールへは打てないので DO ブロックで包む)
    let drop = format!(
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') \
         THEN EXECUTE 'DROP OWNED BY {role}'; EXECUTE 'DROP ROLE {role}'; END IF; END $$"
    );
    vec![
        drop,
        format!("CREATE ROLE {role} NOINHERIT BYPASSRLS LOGIN"),
        format!("GRANT USAGE ON SCHEMA kintai TO {role}"),
        format!("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA kintai TO {role}"),
        format!("REVOKE ALL ON kintai.fold_gate FROM {role}"),
    ]
}

async fn role_without_fold_gate(owner: &sqlx::PgPool, role: &str) {
    locked_ddl(owner, &role_without_fold_gate_sql(role)).await;
}

/// `fold_gate` を**読めるが書けない**ロール (GRANT を SELECT だけにした形)。
async fn role_with_readonly_fold_gate(owner: &sqlx::PgPool, role: &str) {
    let mut stmts = role_without_fold_gate_sql(role);
    stmts.push(format!("GRANT SELECT ON kintai.fold_gate TO {role}"));
    locked_ddl(owner, &stmts).await;
}

// ── 上流スタブ ────────────────────────────────────────────────────────────

/// 月ゲートの dtako 側 digest を固定で返す読み先 (`tests/kintai_fold_pg_test.rs` の
/// `GatedRepo` と同じ役割。上流は 1 度も叩かない)。
struct GateRepo {
    rows: Vec<serde_json::Value>,
    digest: String,
}

#[async_trait]
impl KintaiEventsApi for GateRepo {
    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self
            .rows
            .iter()
            .filter(|r| r["driver_id"].as_u64() == Some(driver))
            .cloned()
            .collect())
    }
    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self.rows.clone())
    }
    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(Vec::new())
    }
    async fn fetch_dtako_month_digest(
        &self,
        _month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        Ok(Some(self.digest.clone()))
    }
}

/// 休息 2 本で勤務が 1 本立つ最小の材料 (打刻は無い)。
fn rest(start: &str, end: &str) -> serde_json::Value {
    json!({"datetime": start, "end_datetime": end, "driver_id": DRIVER, "source": "dtako_events", "state": "休息", "unko_no": null})
}

fn punch(at: &str, state: &str) -> serde_json::Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "timecard", "state": state, "unko_no": null})
}

fn gate_repo(digest_label: &str) -> DynKintaiEventsRepo {
    std::sync::Arc::new(GateRepo {
        rows: vec![
            rest("2026-06-01 16:19:00", "2026-06-02 04:42:00"),
            rest("2026-06-02 16:18:00", "2026-06-03 06:01:00"),
            // **この打刻を消すと gate が書かれなくなり、下の 2 本が落ちる**
            // (Refs #205 の 30)。fold は `month_range` = `[月初, 翌月 2 日)` まで
            // 読むが、push は `exact_month_range` = `[月初, 翌月初)` までしか
            // 書かない。はみ出した 1 日 (翌月 1 日) に `timecard` / `dtako` の行が
            // 1 つも無いと `kintai_fold::push_window_gap_warning` が
            // 「翌月が未 push かもしれない」と鳴らし、gate を書く条件の
            // `warnings.is_empty()` が false になって `fold_gate` が 1 行も
            // 書かれない。
            //
            // **実データでは打刻が 0 件の暦日は 1 日も無い** (2026-06 の実測で
            // 月初 84 件 / 最小 30 件) ので、「翌月ぶんが push 済み」の普通の状態を
            // ここでも作っておく。休息だけで打刻ゼロの月は実在しない形。
            punch("2026-07-01 07:00:00", "始業"),
        ],
        // `fold_gate.dtako_digest` は CHAR(64) なので 64 桁に揃える
        // (短いと読み出しで空白パディングされ、比較がテストの都合で落ちる)
        digest: format!("{digest_label:0<64}"),
    })
}

fn params() -> KosokuParams {
    KosokuParams::default()
}

async fn call_recalc(
    store: &KintaiPgStore,
    repo: &DynKintaiEventsRepo,
    apply: bool,
) -> Result<serde_json::Value, (axum::http::StatusCode, String)> {
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        "X-Tenant-ID",
        store.tenant_id().to_string().parse().unwrap(),
    );
    let pg: rust_ichibanboshi::routes::kintai_timecard::DynKintaiPgStore =
        Some(std::sync::Arc::new(KintaiPgStore::from_pool(
            store.pool().clone(),
            uuid::Uuid::nil(),
        )));
    let resp = recalc(
        headers,
        axum::Extension(pg),
        axum::Extension(repo.clone()),
        axum::Extension(std::sync::Arc::new(params())),
        axum::Extension(ReadTenant(None)),
        axum::Json(RecalcRequest {
            month: MONTH.to_string(),
            after_driver_cd: None,
            max_drivers: None,
            stale_only: false,
            apply,
        }),
    )
    .await?;
    Ok(resp.0)
}

async fn stored_gate_digest(pool: &sqlx::PgPool, tenant: uuid::Uuid) -> Option<String> {
    sqlx::query_scalar(
        "SELECT dtako_digest FROM kintai.fold_gate WHERE tenant_id = $1 AND month = $2",
    )
    .bind(tenant)
    .bind(MONTH)
    .fetch_optional(pool)
    .await
    .expect("fold_gate query")
}

// ── 1. 本番のロールで gate が使えること (migrations/005 の回帰) ─────────────

/// **`kintai_writer` で `kintai.fold_gate` を読み書きできる。**
///
/// 004 だけの状態 (GRANT 無し) だとここが落ちる — `month_gate_report` が
/// `permission denied` を踏んで `Unavailable` に degrade し、gate が 1 行も
/// 書かれないため 2 回目も `Hit` にならない。これが本番の 502 の正体だった。
#[tokio::test]
async fn the_production_writer_role_can_use_the_fold_gate() {
    let owner = require_db!();
    let tenant = uuid::Uuid::new_v4();
    let writer = connect_as(&owner, "kintai_writer").await;
    let store = KintaiPgStore::from_pool(writer, tenant);
    let repo = gate_repo("writer");

    // 1 回目は Miss。**Unavailable ではない** = SELECT が権限で弾かれていない
    let gate = month_gate_report(&repo, &store, &params(), MONTH, true)
        .await
        .expect("gate");
    assert!(
        matches!(gate, MonthGate::Miss { .. }),
        "kintai_writer で fold_gate が読めていない (GRANT 漏れ): {gate:?}"
    );

    // 月まるごと 1 ページで完結する呼び出しなので gate が書かれる
    let resp = call_recalc(&store, &repo, true).await.expect("recalc");
    assert!(
        resp["next_after_driver_cd"].is_null(),
        "1 名だけの母集団は 1 ページで回りきる: {resp}"
    );
    assert_eq!(
        stored_gate_digest(&owner, tenant).await,
        Some(format!("{:0<64}", "writer")),
        "kintai_writer で fold_gate に書けていない。GRANT 漏れが第一候補だが、\
         **warnings が 1 本でも立つと gate は書かれない** ので、まず fixture が \
         push 被覆の warning を踏んでいないかを見ること (Refs #205 の 30)"
    );

    // 2 回目は Hit — 全量読みごと省ける (この経路が本番で成立してほしかった形)
    let gate = month_gate_report(&repo, &store, &params(), MONTH, true)
        .await
        .expect("gate again");
    assert!(matches!(gate, MonthGate::Hit(_)), "2 回目は Hit: {gate:?}");
}

// ── 2. 読めないときは degrade する (502 にしない) ──────────────────────────

/// **`fold_gate` が読めなくても `POST /api/kintai/recalc` は 200 で返る。**
///
/// 落とすと「最適化が使えない」が「再計算が失敗した」に化ける。しかも
/// `map_push_err` の 502 は元々ログを 1 行も出さなかったので、本番では
/// 「コンテナが落ちている」ようにしか見えなかった。
#[tokio::test]
async fn an_unreadable_fold_gate_degrades_instead_of_failing_the_request() {
    let owner = require_db!();
    let role = "t205_20_nogate";
    role_without_fold_gate(&owner, role).await;
    let tenant = uuid::Uuid::new_v4();
    let pool = connect_as(&owner, role).await;
    let store = KintaiPgStore::from_pool(pool, tenant);
    let repo = gate_repo("nogate");

    let gate = month_gate_report(&repo, &store, &params(), MONTH, true)
        .await
        .expect("gate は Err にならない");
    assert_eq!(
        gate,
        MonthGate::Unavailable,
        "読めない gate は Unavailable に倒す (Err にしない)"
    );

    // 口ぜんたいは生きている — 畳みも保存も通る
    let resp = call_recalc(&store, &repo, true).await.expect("recalc");
    assert_eq!(resp["month"], MONTH);
    assert_eq!(
        resp["fold"]["drivers_written"], 1,
        "gate が使えなくても畳んで保存する: {resp}"
    );
    assert_eq!(
        stored_gate_digest(&owner, tenant).await,
        None,
        "判定できない月に gate を刻まない (安全側)"
    );
}

// ── 3. 書けないときも degrade する ────────────────────────────────────────

/// **`fold_gate` に書けなくても応答は 200。**
///
/// 書き込みは畳み終わった**後**に走るので、ここで落とすと「再計算は全部成功したのに
/// 呼び出し側には失敗に見える」が起きる。
#[tokio::test]
async fn an_unwritable_fold_gate_does_not_fail_a_finished_recalc() {
    let owner = require_db!();
    let role = "t205_20_readonly_gate";
    role_with_readonly_fold_gate(&owner, role).await;
    let tenant = uuid::Uuid::new_v4();
    let pool = connect_as(&owner, role).await;
    let store = KintaiPgStore::from_pool(pool, tenant);
    let repo = gate_repo("rogate");

    // 読めるので Miss まで進む (Unavailable ではない)
    let gate = month_gate_report(&repo, &store, &params(), MONTH, true)
        .await
        .expect("gate");
    assert!(matches!(gate, MonthGate::Miss { .. }), "{gate:?}");

    let resp = call_recalc(&store, &repo, true).await.expect("recalc");
    assert_eq!(
        resp["fold"]["drivers_written"], 1,
        "書き込みの失敗で応答を落とさない: {resp}"
    );
    assert_eq!(
        stored_gate_digest(&owner, tenant).await,
        None,
        "書けなかったのだから gate は立たない (次回また全量読み)"
    );
}
