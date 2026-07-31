//! 打刻 push の**実 Postgres** に対する検証 (Refs #205 実装計画 04)。
//!
//! `kintai_push` の単体テストは純粋な写しと署名だけを見ている。ここで確かめるのは
//! **Rust 側と Postgres 側で答えが一致するか**で、これは片方だけでは検証できない:
//!
//! - 署名の式 (`day_signature` と `STORED_SIGNATURES_SQL`) が同じ値を出すか。
//!   collation と `AT TIME ZONE` の解釈がずれると、中身が同じでも毎回全日を
//!   書き直し続ける (静かに動き続けるので単体テストでは気付けない)
//! - JST の壁時計が `TIMESTAMPTZ` を往復して同じ時刻に戻るか (9 時間ずれない)
//! - `--dry-run` が本当に 1 行も書かないか
//!
//! ## 動かし方
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**丸ごと skip** する (CI の test job は
//! postgres service を持つので実際に走る)。手元で回すなら:
//!
//! ```text
//! docker run -d --name kintai-test -e POSTGRES_PASSWORD=postgres -p 55432:5432 postgres:17-alpine
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55432/postgres cargo test --test kintai_push_pg_test
//! ```
//!
//! テストは**テナントで分離**する。`kintai.*` の主キーは全て `tenant_id` 始まりで、
//! テストごとに新しい UUID を採るので、同じスキーマを共有したまま並列に回せる。

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::NaiveDate;
use rust_ichibanboshi::kintai_push::{
    day_signature, dedup_events, group_by_date, jst_day_bounds, parse_rows, push_month,
    KintaiPgStore, PushOptions,
};
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use serde_json::json;
use sqlx::Row;

// ── 前提 ──────────────────────────────────────────────────────────────────

fn database_url() -> Option<String> {
    std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// スキーマを 1 度だけ用意する。並列テストが同時に流し込まないよう advisory lock で囲む。
async fn ensure_schema(pool: &sqlx::PgPool) {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(205_040_001_i64)
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
        .bind(205_040_001_i64)
        .execute(pool)
        .await
        .expect("advisory unlock");
}

/// psql の**クライアント側変数** (`:'name'`) を使う migration か。
///
/// `sqlx::raw_sql` は psql ではないので `:'…'` がそのままサーバへ行き
/// `syntax error at or near ":"` になる。003
/// (`ALTER ROLE kintai_writer WITH PASSWORD :'kintai_writer_password'`) がこれ。
///
/// **この harness では飛ばしてよい。** ここが用意するのは `kintai` スキーマで、
/// テストは `KINTAI_TEST_DATABASE_URL` の所有者として繋ぐ (`kintai_writer` では
/// 繋がない) — 資格情報はスキーマではない。実適用は
/// `scripts/migrate_kintai.sh` が `-v kintai_writer_password=…` を渡して行い、
/// 空のまま流れないよう適用前に弾く。
///
/// `::` の型キャストとは衝突しない (psql の変数参照は `:` の直後が引用符)。
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

/// テスト 1 本ぶんの store。テナントは毎回新しい UUID。
async fn store() -> Option<(KintaiPgStore, sqlx::PgPool)> {
    let url = database_url()?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    ensure_schema(&pool).await;
    let tenant = uuid::Uuid::new_v4();
    Some((KintaiPgStore::from_pool(pool.clone(), tenant), pool))
}

/// 受け口が読む `X-Tenant-ID`。relay が alc へ渡しているのと同じヘッダ。
fn tenant_header(tenant: uuid::Uuid) -> axum::http::HeaderMap {
    let mut h = axum::http::HeaderMap::new();
    h.insert("X-Tenant-ID", tenant.to_string().parse().unwrap());
    h
}

/// `KINTAI_TEST_DATABASE_URL` が無ければ早期 return するためのマクロ。
macro_rules! require_db {
    () => {
        match store().await {
            Some(v) => v,
            None => return,
        }
    };
}

// ── 生イベントの stub ──────────────────────────────────────────────────────

/// 決まった行を返すだけの読み出し口。`fetch_events_between` は乗務員で絞る。
struct StubRepo {
    rows: std::sync::Mutex<Vec<serde_json::Value>>,
}

impl StubRepo {
    fn new(rows: Vec<serde_json::Value>) -> Self {
        Self {
            rows: std::sync::Mutex::new(rows),
        }
    }

    fn set(&self, rows: Vec<serde_json::Value>) {
        *self.rows.lock().unwrap() = rows;
    }

    fn all(&self) -> Vec<serde_json::Value> {
        self.rows.lock().unwrap().clone()
    }
}

#[async_trait]
impl KintaiEventsApi for StubRepo {
    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self
            .all()
            .into_iter()
            .filter(|r| r["driver_id"].as_u64() == Some(driver))
            .collect())
    }

    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self.all())
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(Vec::new())
    }
}

fn punch(at: &str, state: &str) -> serde_json::Value {
    json!({
        "datetime": at,
        "end_datetime": null,
        "driver_id": 1130,
        "source": "timecard",
        "state": state,
        "unko_no": null,
    })
}

fn run_event(at: &str, state: &str, unko_no: &str) -> serde_json::Value {
    json!({
        "datetime": at,
        "end_datetime": null,
        "driver_id": 1130,
        "source": "dtako",
        "state": state,
        "unko_no": unko_no,
    })
}

fn july() -> PushOptions {
    PushOptions {
        month: "2026-07".to_string(),
        driver: None,
        apply: true,
    }
}

async fn count_events(pool: &sqlx::PgPool, tenant: uuid::Uuid) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = $1")
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("count")
}

// ── ここから本題 ──────────────────────────────────────────────────────────

/// **署名の式が両側で一致するか。** これが割れると差分検知が毎回「全日が違う」に
/// 倒れ、静かに全件を書き直し続ける。
#[tokio::test]
async fn signature_matches_between_rust_and_postgres() {
    let (store, pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![
        // 日本語の state を複数混ぜる — collation がずれると並び順が変わる
        punch("2026-07-01 08:00:00", "始業"),
        run_event("2026-07-01 08:30:00", "運行開始", "OP-1"),
        run_event("2026-07-01 12:00:00", "休息開始", "OP-1"),
        run_event("2026-07-01 13:00:00", "休息終了", "OP-1"),
        run_event("2026-07-01 17:30:00", "運行終了", "OP-1"),
        punch("2026-07-01 18:00:00", "終業"),
        // 同秒・別 state。並べ替えの第 2 キーが効く
        run_event("2026-07-02 09:00:00", "運行開始", "OP-2"),
        run_event("2026-07-02 09:00:00", "除外", "OP-2"),
    ]));

    push_month(&repo, &store, &july()).await.expect("push");

    let events =
        dedup_events(parse_rows(&repo.fetch_all_events_between("", "").await.unwrap()).events);
    let local: BTreeMap<NaiveDate, String> = group_by_date(&events)
        .iter()
        .map(|(d, evs)| (*d, day_signature(evs)))
        .collect();

    let (from, to) = jst_day_bounds(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let (_, to_end) = jst_day_bounds(NaiveDate::from_ymd_opt(2026, 7, 31).unwrap());
    let stored = store
        .stored_day_signatures(1130, from, to_end.max(to))
        .await
        .expect("stored signatures");

    assert_eq!(stored, local, "Rust と Postgres の署名が一致しない");
    assert_eq!(local.len(), 2, "2 日ぶん");
    let _ = pool;
}

/// **JST が 9 時間ずれない。** `TIMESTAMPTZ` を往復して同じ壁時計に戻るか。
#[tokio::test]
async fn jst_wall_clock_survives_the_round_trip() {
    let (store, pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![
        // 0 時台と 23 時台 — ずれると暦日ごと隣の日へ移る
        punch("2026-07-01 00:30:00", "始業"),
        punch("2026-07-01 23:45:00", "終業"),
    ]));
    push_month(&repo, &store, &july()).await.expect("push");

    let rows = sqlx::query(
        "SELECT to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS wall,
                (occurred_at AT TIME ZONE 'Asia/Tokyo')::date AS d
           FROM kintai.kintai_events WHERE tenant_id = $1 ORDER BY occurred_at",
    )
    .bind(store.tenant_id())
    .fetch_all(&pool)
    .await
    .expect("read back");

    let wall: Vec<String> = rows.iter().map(|r| r.get::<String, _>("wall")).collect();
    assert_eq!(wall, ["2026-07-01 00:30:00", "2026-07-01 23:45:00"]);
    let dates: Vec<NaiveDate> = rows.iter().map(|r| r.get::<NaiveDate, _>("d")).collect();
    let jul1 = NaiveDate::from_ymd_opt(2026, 7, 1).unwrap();
    assert_eq!(dates, [jul1, jul1], "両方とも 07-01 に乗る");
}

/// **2 回連続で走らせると 2 回目は何も書かない。**
#[tokio::test]
async fn second_run_writes_nothing() {
    let (store, _pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-03 08:00:00", "始業"),
        punch("2026-07-03 18:00:00", "終業"),
    ]));

    let first = push_month(&repo, &store, &july()).await.expect("1st");
    assert_eq!(first.days_changed, 1);
    assert!(first.wrote_anything());

    let second = push_month(&repo, &store, &july()).await.expect("2nd");
    assert_eq!(second.days_changed, 0, "2 回目に差分が出た");
    assert_eq!(second.days_deleted, 0);
    assert_eq!(second.days_unchanged, 1);
    assert!(!second.wrote_anything());
}

/// **1 行を更新すると、その日だけが差分になる。**
#[tokio::test]
async fn updating_one_row_marks_only_that_day() {
    let (store, _pool) = require_db!();
    let stub = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-05 08:00:00", "始業"),
        punch("2026-07-05 18:00:00", "終業"),
        punch("2026-07-06 08:00:00", "始業"),
        punch("2026-07-06 18:00:00", "終業"),
    ]));
    let repo: DynKintaiEventsRepo = stub.clone();
    push_month(&repo, &store, &july()).await.expect("1st");

    // 07-06 の終業を 1 分ずらす
    stub.set(vec![
        punch("2026-07-05 08:00:00", "始業"),
        punch("2026-07-05 18:00:00", "終業"),
        punch("2026-07-06 08:00:00", "始業"),
        punch("2026-07-06 18:01:00", "終業"),
    ]);
    let again = push_month(&repo, &store, &july()).await.expect("2nd");
    assert_eq!(again.days_changed, 1, "変えた 1 日だけが差分");
    assert_eq!(again.days_unchanged, 1);
    assert_eq!(again.days_deleted, 0);
}

/// **元の 1 行を消すと Supabase 側からも消える。**
///
/// 消えないと「古い値が正常に見える」— #205 のリスク欄の筆頭。
#[tokio::test]
async fn deleting_the_source_row_removes_it_downstream() {
    let (store, pool) = require_db!();
    let stub = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-08 08:00:00", "始業"),
        punch("2026-07-08 18:00:00", "終業"),
        punch("2026-07-09 08:00:00", "始業"),
    ]));
    let repo: DynKintaiEventsRepo = stub.clone();
    push_month(&repo, &store, &july()).await.expect("1st");
    assert_eq!(count_events(&pool, store.tenant_id()).await, 3);

    // 07-09 を丸ごと消す
    stub.set(vec![
        punch("2026-07-08 08:00:00", "始業"),
        punch("2026-07-08 18:00:00", "終業"),
    ]);
    let again = push_month(&repo, &store, &july()).await.expect("2nd");
    assert_eq!(again.days_deleted, 1);
    assert_eq!(count_events(&pool, store.tenant_id()).await, 2);

    // 日の中の 1 行だけ消した場合も追随する
    stub.set(vec![punch("2026-07-08 08:00:00", "始業")]);
    push_month(&repo, &store, &july()).await.expect("3rd");
    assert_eq!(count_events(&pool, store.tenant_id()).await, 1);
}

/// **`--dry-run` (既定) は 1 行も書かない。** 差分の報告だけは同じものが出る。
#[tokio::test]
async fn dry_run_writes_nothing_but_still_reports() {
    let (store, pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-11 08:00:00", "始業"),
        punch("2026-07-11 18:00:00", "終業"),
    ]));
    let opts = PushOptions {
        apply: false,
        ..july()
    };
    let report = push_month(&repo, &store, &opts).await.expect("dry run");
    assert_eq!(report.days_changed, 1, "差分は報告する");
    assert_eq!(
        count_events(&pool, store.tenant_id()).await,
        0,
        "書いていない"
    );
}

/// **テナントが違えば互いに見えない。** RLS 以前に、全ての読み書きが
/// `tenant_id` で絞られていることの確認。
#[tokio::test]
async fn tenants_do_not_see_each_other() {
    let (store_a, pool) = require_db!();
    let store_b = KintaiPgStore::from_pool(pool.clone(), uuid::Uuid::new_v4());
    let repo: DynKintaiEventsRepo =
        std::sync::Arc::new(StubRepo::new(vec![punch("2026-07-13 08:00:00", "始業")]));

    push_month(&repo, &store_a, &july()).await.expect("push a");
    assert_eq!(count_events(&pool, store_a.tenant_id()).await, 1);
    assert_eq!(count_events(&pool, store_b.tenant_id()).await, 0);

    // B で走らせても A の行は消えない (B から見れば「相手に無い日」ですらない)
    push_month(&repo, &store_b, &july()).await.expect("push b");
    assert_eq!(count_events(&pool, store_a.tenant_id()).await, 1);
    assert_eq!(count_events(&pool, store_b.tenant_id()).await, 1);
}

/// **DDL の CHECK 制約に無い state は送る前に弾く。**
///
/// 送ってしまうとトランザクションごと巻き戻り、その乗務員の月が丸ごと書けなくなる。
#[tokio::test]
async fn unknown_states_are_rejected_before_they_reach_the_check_constraint() {
    let (store, pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![
        punch("2026-07-15 08:00:00", "始業"),
        // time_card_dtako.event_name は自由記述なので、制約に無い値が来うる
        run_event("2026-07-15 09:00:00", "点呼", "OP-9"),
        punch("2026-07-15 18:00:00", "終業"),
    ]));
    let report = push_month(&repo, &store, &july()).await.expect("push");
    assert_eq!(report.events_pushed, 2, "点呼 は落ちる");
    assert!(report.has_unexpected(), "想定外として報告する");
    assert!(report.unknown_states.contains("点呼"), "実値を残す");
    // 残り 2 行は書けている (1 行のせいで月が丸ごと落ちない)
    assert_eq!(count_events(&pool, store.tenant_id()).await, 2);
}

/// **主キーの衝突を Rust 側で決着させている。**
///
/// 同じ秒・同じ state が `timecard` と `dtako` の両方にあると PK が衝突する。
#[tokio::test]
async fn primary_key_collisions_keep_the_timecard_row() {
    let (store, pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![
        json!({"datetime": "2026-07-17 08:00:00", "driver_id": 1130, "source": "dtako", "state": "始業", "unko_no": "OP-3"}),
        punch("2026-07-17 08:00:00", "始業"),
    ]));
    push_month(&repo, &store, &july()).await.expect("push");

    let row = sqlx::query("SELECT source, unko_no FROM kintai.kintai_events WHERE tenant_id = $1")
        .bind(store.tenant_id())
        .fetch_one(&pool)
        .await
        .expect("single row");
    assert_eq!(row.get::<String, _>("source"), "timecard");
    assert!(row.get::<Option<String>, _>("unko_no").is_none());
}

/// **`raw` に元の生行が残る。** 出力が変わったとき入力へ遡るための層 (決定 6)。
#[tokio::test]
async fn raw_keeps_the_original_row() {
    let (store, pool) = require_db!();
    let repo: DynKintaiEventsRepo = std::sync::Arc::new(StubRepo::new(vec![run_event(
        "2026-07-19 08:00:00",
        "運行開始",
        "OP-7",
    )]));
    push_month(&repo, &store, &july()).await.expect("push");

    let raw: serde_json::Value =
        sqlx::query_scalar("SELECT raw FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("raw");
    assert_eq!(raw["unko_no"], "OP-7");
    assert_eq!(raw["source"], "dtako");
}

// ── 04b: 打刻の受け口 (オンプレ → GCP) ──────────────────────────────────────

use rust_ichibanboshi::kintai_push::{
    apply_timecard_batch, apply_timecard_window, plan_batch, TimecardBatch, TimecardWindow,
};

fn d(y: i32, m: u32, day: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, day).unwrap()
}

fn batch_of(days: Vec<(NaiveDate, Vec<serde_json::Value>)>) -> TimecardBatch {
    TimecardBatch {
        month: "2026-07".to_string(),
        driver_cd: 1130,
        days: days.into_iter().collect(),
        delete_dates: Vec::new(),
    }
}

/// **送り主を信用しない。** 日のキーと中身が食い違う行は落として数える。
/// 信用すると 1 リクエストで別の日・別の乗務員を静かに書き換えられる。
#[tokio::test]
async fn a_batch_rejects_rows_that_do_not_belong_to_their_key() {
    let (store, pool) = require_db!();
    let other_driver = json!({
        "datetime": "2026-07-01 08:00:00", "driver_id": 9999,
        "source": "timecard", "state": "始業", "unko_no": null,
    });
    let b = batch_of(vec![(
        d(2026, 7, 1),
        vec![
            punch("2026-07-01 08:00:00", "始業"),
            // 日のキーは 07-01 なのに中身は 07-02
            punch("2026-07-02 08:00:00", "始業"),
            other_driver,
        ],
    )]);
    let r = apply_timecard_batch(&store, &b).await.expect("apply");
    assert_eq!(r.misplaced, 2, "日違いと乗務員違いの 2 行");
    assert_eq!(r.events_written, 1);
    assert!(r.has_unexpected());
    assert_eq!(count_events(&pool, store.tenant_id()).await, 1);
}

/// 対象月から外れた日は丸ごと落とす。
#[tokio::test]
async fn a_batch_rejects_days_outside_its_month() {
    let (store, pool) = require_db!();
    let mut b = batch_of(vec![(
        d(2026, 8, 1),
        vec![punch("2026-08-01 08:00:00", "始業")],
    )]);
    b.delete_dates = vec![d(2026, 8, 5), d(2026, 7, 9)];
    let r = apply_timecard_batch(&store, &b).await.expect("apply");
    assert_eq!(r.misplaced, 1);
    assert_eq!(r.days_written, 0);
    assert_eq!(r.days_deleted, 1, "月内の削除だけ通る");
    assert_eq!(count_events(&pool, store.tenant_id()).await, 0);
}

/// 受け側でも PK 衝突を決着させる (送り側と同じ規則)。
#[tokio::test]
async fn a_batch_dedups_on_the_receiving_side() {
    let (store, pool) = require_db!();
    let b = batch_of(vec![(
        d(2026, 7, 1),
        vec![
            json!({"datetime": "2026-07-01 08:00:00", "driver_id": 1130, "source": "dtako", "state": "始業", "unko_no": "OP-1"}),
            punch("2026-07-01 08:00:00", "始業"),
        ],
    )]);
    let r = apply_timecard_batch(&store, &b).await.expect("apply");
    assert_eq!(r.deduped, 1);
    assert_eq!(count_events(&pool, store.tenant_id()).await, 1);
    let source: String =
        sqlx::query_scalar("SELECT source FROM kintai.kintai_events WHERE tenant_id = $1")
            .bind(store.tenant_id())
            .fetch_one(&pool)
            .await
            .expect("source");
    assert_eq!(source, "timecard", "人が確定させた打刻を残す");
}

/// **送る → 相手の署名を引く → 差分ゼロ**。転送の往復が収束することの確認。
#[tokio::test]
async fn planning_against_the_remote_signatures_converges() {
    let (store, _pool) = require_db!();
    let rows = vec![
        punch("2026-07-01 08:00:00", "始業"),
        punch("2026-07-01 18:00:00", "終業"),
        punch("2026-07-02 08:00:00", "始業"),
    ];
    let local = group_by_date(&dedup_events(parse_rows(&rows).events));

    // 相手は空 → 全日が差分
    let (from, to) = jst_day_bounds(d(2026, 7, 1));
    let (_, to_end) = jst_day_bounds(d(2026, 7, 31));
    let remote = store
        .stored_day_signatures(1130, from, to_end.max(to))
        .await
        .expect("sigs");
    let first = plan_batch("2026-07", 1130, &local, &remote);
    assert_eq!(first.days.len(), 2);
    assert!(first.delete_dates.is_empty());
    assert!(!first.is_empty());

    apply_timecard_batch(&store, &first).await.expect("apply");

    // もう一度計画すると差分ゼロ
    let remote = store
        .stored_day_signatures(1130, from, to_end.max(to))
        .await
        .expect("sigs");
    let second = plan_batch("2026-07", 1130, &local, &remote);
    assert!(second.is_empty(), "2 回目に差分が出た: {second:?}");

    // 手元から 1 日消すと、その日が delete に載る
    let mut shrunk = local.clone();
    shrunk.remove(&d(2026, 7, 2));
    let third = plan_batch("2026-07", 1130, &shrunk, &remote);
    assert_eq!(third.delete_dates, vec![d(2026, 7, 2)]);
    assert!(third.days.is_empty());

    apply_timecard_batch(&store, &third).await.expect("apply");
    let left = store
        .stored_day_signatures(1130, from, to_end.max(to))
        .await
        .expect("sigs");
    assert_eq!(left.len(), 1, "消えた日は相手からも消える");
}

/// **ワイヤ形式が往復して壊れない。** relay を挟むので JSON で通る必要がある。
#[test]
fn the_wire_format_round_trips() {
    let b = batch_of(vec![(
        d(2026, 7, 1),
        vec![punch("2026-07-01 08:00:00", "始業")],
    )]);
    let json = serde_json::to_string(&b).unwrap();
    let back: TimecardBatch = serde_json::from_str(&json).unwrap();
    assert_eq!(back.month, "2026-07");
    assert_eq!(back.driver_cd, 1130);
    assert_eq!(back.days[&d(2026, 7, 1)].len(), 1);

    // 省略可能なキーは既定で埋まる
    let minimal: TimecardBatch =
        serde_json::from_str(r#"{"month":"2026-07","driver_cd":1}"#).unwrap();
    assert!(minimal.is_empty());
}

/// **ルート越しの往復。** ハンドラが返す JSON の形はここでしか固定できない。
#[tokio::test]
async fn the_routes_answer_with_the_expected_json() {
    use axum::extract::Query;
    use axum::Extension;
    use rust_ichibanboshi::routes::kintai_timecard::{receive, signatures, SignaturesQuery};

    let (store, _pool) = require_db!();
    let tenant = store.tenant_id();
    let pg = Some(std::sync::Arc::new(store));
    let sig_query = || {
        Query(SignaturesQuery {
            month: Some("2026-07".to_string()),
            driver_cd: Some(1130),
        })
    };

    // 空の相手 → signatures は空
    let got = signatures(tenant_header(tenant), sig_query(), Extension(pg.clone()))
        .await
        .expect("signatures");
    assert_eq!(got.0["month"], "2026-07");
    assert_eq!(got.0["driver_cd"], 1130);
    assert_eq!(got.0["signatures"].as_object().unwrap().len(), 0);

    // 1 日送る
    let b = batch_of(vec![(
        d(2026, 7, 1),
        vec![
            punch("2026-07-01 08:00:00", "始業"),
            punch("2026-07-01 18:00:00", "終業"),
        ],
    )]);
    let got = receive(tenant_header(tenant), Extension(pg.clone()), axum::Json(b))
        .await
        .expect("receive");
    assert_eq!(got.0["days_written"], 1);
    assert_eq!(got.0["events_written"], 2);
    assert_eq!(got.0["misplaced"], 0);

    // 署名が生えている
    let got = signatures(tenant_header(tenant), sig_query(), Extension(pg))
        .await
        .expect("signatures");
    let sigs = got.0["signatures"].as_object().unwrap();
    assert_eq!(sigs.len(), 1);
    assert_eq!(sigs["2026-07-01"].as_str().unwrap().len(), 64);
}

/// **`X-Tenant-ID` が書き先を決める。** `[kintai_push] tenant_id` を書いていない
/// instance (= GCP 側の想定) では、relay が名乗ったテナントの行として積まれる。
///
/// 単体テストでは「ハンドラが 403 / 400 を返すか」までしか見られない。ここで
/// 確かめるのは**実際に別テナントの行として書かれるか**で、`for_tenant` が
/// pool だけ複製して tenant を差し替えられているかは Postgres 側でしか分からない。
#[tokio::test]
async fn the_header_decides_which_tenant_the_rows_land_in() {
    use axum::extract::Query;
    use axum::Extension;
    use rust_ichibanboshi::routes::kintai_timecard::{receive, signatures, SignaturesQuery};

    let (store, pool) = require_db!();
    // pin 無し (tenant_id を設定していない instance)
    let pg = Some(std::sync::Arc::new(KintaiPgStore::from_pool(
        pool.clone(),
        uuid::Uuid::nil(),
    )));
    let a = store.tenant_id();
    let b = uuid::Uuid::new_v4();

    let day = || {
        batch_of(vec![(
            d(2026, 7, 1),
            vec![punch("2026-07-01 08:00:00", "始業")],
        )])
    };
    for t in [a, b] {
        let got = receive(tenant_header(t), Extension(pg.clone()), axum::Json(day()))
            .await
            .expect("receive");
        assert_eq!(got.0["days_written"], 1, "tenant={t}");
    }

    // 同じ (乗務員, 暦日) が 2 テナントぶん立っている
    let n: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM kintai.kintai_events
          WHERE tenant_id = ANY($1) AND driver_cd = 1130",
    )
    .bind(vec![a, b])
    .fetch_one(&pool)
    .await
    .expect("count");
    assert_eq!(n, 2);

    // 名乗り分の署名しか見えない (別テナントの行は混ざらない)
    let sigs = |t| {
        signatures(
            tenant_header(t),
            Query(SignaturesQuery {
                month: Some("2026-07".to_string()),
                driver_cd: Some(1130),
            }),
            Extension(pg.clone()),
        )
    };
    let (sa, sb) = (
        sigs(a).await.expect("signatures a"),
        sigs(b).await.expect("signatures b"),
    );
    assert_eq!(sa.0["signatures"].as_object().unwrap().len(), 1);
    assert_eq!(sb.0["signatures"].as_object().unwrap().len(), 1);
    assert_eq!(sa.0["signatures"], sb.0["signatures"]);
}

// ── 窓ぶんをまるごと運ぶ経路 (Refs #205 の 04b) ────────────────────────────

/// **JST の壁時計でその日の中の 1 点**を作る。
///
/// `jst_day_bounds(d).1` は**翌日 00:00** なので、「その日に置く」つもりで掴むと
/// 隣の日に乗る (2026-07-31 に実際に間違えた)。時刻を明示する形にして塞ぐ。
fn jst_at(date: NaiveDate, hour: i64) -> chrono::DateTime<chrono::FixedOffset> {
    jst_day_bounds(date).0 + chrono::Duration::hours(hour)
}

/// 窓ぶんの生行 (乗務員を混ぜられる形)。
fn win_punch(driver: i64, at: &str, state: &str) -> serde_json::Value {
    json!({
        "datetime": at,
        "end_datetime": null,
        "driver_id": driver,
        "source": "timecard",
        "state": state,
        "unko_no": null,
    })
}

fn window_of(months: &[&str], drivers: &[i64], events: Vec<serde_json::Value>) -> TimecardWindow {
    TimecardWindow {
        months: months.iter().map(|m| m.to_string()).collect(),
        drivers: drivers.to_vec(),
        events,
        dry_run: false,
    }
}

/// **複数乗務員版の署名が 1 名版と一致するか。**
///
/// 突き合わせを受け側へ移した以上、この 2 つが割れると「毎回全日が違う」に倒れる。
/// 式を写した先が本当に同じ値を出すかは、実 Postgres でしか確かめられない。
#[tokio::test]
async fn window_signatures_match_the_single_driver_query() {
    let (store, _pool) = require_db!();
    let events = vec![
        win_punch(1130, "2026-06-01 08:00:00", "始業"),
        win_punch(1130, "2026-06-01 18:00:00", "終業"),
        win_punch(1200, "2026-06-01 09:00:00", "始業"),
        win_punch(1200, "2026-06-02 09:00:00", "始業"),
    ];
    let w = window_of(&["2026-06"], &[1130, 1200], events);
    apply_timecard_window(&store, &w).await.expect("apply");

    let (from, _) = jst_day_bounds(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());
    let (to, _) = jst_day_bounds(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let all = store
        .stored_window_signatures(&[1130, 1200], from, to)
        .await
        .expect("window signatures");

    for driver in [1130_i64, 1200] {
        let one = store
            .stored_day_signatures(driver, from, to)
            .await
            .expect("single signatures");
        assert_eq!(all[&driver], one, "乗務員 {driver} で 2 つの式が割れた");
    }
    assert_eq!(all[&1130].len(), 1);
    assert_eq!(all[&1200].len(), 2);
}

/// **同じ窓を送り直しても 1 行も書かない。** 窓を毎回まるごと送る設計の前提。
#[tokio::test]
async fn resending_the_same_window_writes_nothing() {
    let (store, pool) = require_db!();
    let events = vec![
        win_punch(1130, "2026-06-03 08:00:00", "始業"),
        win_punch(1130, "2026-06-03 18:00:00", "終業"),
    ];
    let w = window_of(&["2026-06"], &[1130], events);

    let first = apply_timecard_window(&store, &w).await.expect("1st");
    assert_eq!(first.days_written, 1);
    assert_eq!(first.drivers_written, 1);

    let second = apply_timecard_window(&store, &w).await.expect("2nd");
    assert_eq!(second.days_written, 0, "2 回目に差分が出た");
    assert_eq!(second.drivers_written, 0);
    assert_eq!(second.days_deleted, 0);
    assert_eq!(count_events(&pool, store.tenant_id()).await, 2);
}

/// **始業が後から直ると、その日だけ書き直る。** 打刻はほとんど戻らないが、
/// 始業 / 終業 の修正だけはありうる — 窓を送り直す理由そのもの。
#[tokio::test]
async fn an_edited_punch_rewrites_only_that_day() {
    let (store, _pool) = require_db!();
    let base = vec![
        win_punch(1130, "2026-06-05 08:00:00", "始業"),
        win_punch(1130, "2026-06-05 18:00:00", "終業"),
        win_punch(1130, "2026-06-06 08:00:00", "始業"),
    ];
    apply_timecard_window(&store, &window_of(&["2026-06"], &[1130], base))
        .await
        .expect("1st");

    let edited = vec![
        win_punch(1130, "2026-06-05 08:00:00", "始業"),
        win_punch(1130, "2026-06-05 18:00:00", "終業"),
        // 06-06 の始業が 07:30 に直された
        win_punch(1130, "2026-06-06 07:30:00", "始業"),
    ];
    let again = apply_timecard_window(&store, &window_of(&["2026-06"], &[1130], edited))
        .await
        .expect("2nd");
    assert_eq!(again.days_written, 1, "直した 1 日だけ");
    assert_eq!(again.days_deleted, 0);
}

/// **窓の外は 1 行も触らない。** 前月ぶんを送り直しても、それ以前が消えない。
#[tokio::test]
async fn the_window_never_touches_months_outside_it() {
    let (store, pool) = require_db!();
    // 5 月と 6 月を入れておく
    apply_timecard_window(
        &store,
        &window_of(
            &["2026-05", "2026-06"],
            &[1130],
            vec![
                win_punch(1130, "2026-05-10 08:00:00", "始業"),
                win_punch(1130, "2026-06-10 08:00:00", "始業"),
            ],
        ),
    )
    .await
    .expect("seed");
    assert_eq!(count_events(&pool, store.tenant_id()).await, 2);

    // 6 月だけの窓を、6 月が空の状態で送り直す → 6 月だけ消えて 5 月は残る
    let result = apply_timecard_window(&store, &window_of(&["2026-06"], &[1130], vec![]))
        .await
        .expect("narrow window");
    assert_eq!(result.days_deleted, 1);
    assert_eq!(
        count_events(&pool, store.tenant_id()).await,
        1,
        "5 月まで消えた"
    );
}

/// **名乗っていない乗務員は触らない。** 送り主が知らない乗務員の日を
/// 「元が消えた」と見なして消しにいかない。
#[tokio::test]
async fn drivers_the_sender_did_not_declare_are_left_alone() {
    let (store, pool) = require_db!();
    apply_timecard_window(
        &store,
        &window_of(
            &["2026-06"],
            &[1130, 1200],
            vec![
                win_punch(1130, "2026-06-11 08:00:00", "始業"),
                win_punch(1200, "2026-06-11 08:00:00", "始業"),
            ],
        ),
    )
    .await
    .expect("seed");

    // 1130 しか名乗らずに空を送る → 1130 だけ消え、1200 は残る
    let result = apply_timecard_window(&store, &window_of(&["2026-06"], &[1130], vec![]))
        .await
        .expect("apply");
    assert_eq!(result.days_deleted, 1);
    assert_eq!(
        count_events(&pool, store.tenant_id()).await,
        1,
        "1200 まで消えた"
    );
}

/// **`dry_run` は 1 行も書かないが、件数は返す。** MCP tool が名乗る
/// 「apply を付けない限り書かない」を口の側で担保する。
#[tokio::test]
async fn a_dry_run_window_reports_without_writing() {
    let (store, pool) = require_db!();
    let events = vec![
        win_punch(1130, "2026-06-20 08:00:00", "始業"),
        win_punch(1130, "2026-06-20 18:00:00", "終業"),
    ];
    let mut w = window_of(&["2026-06"], &[1130], events);
    w.dry_run = true;

    let planned = apply_timecard_window(&store, &w).await.expect("dry run");
    assert!(planned.dry_run);
    assert_eq!(planned.days_written, 1, "計画は返る");
    assert_eq!(planned.events_written, 2);
    assert_eq!(
        count_events(&pool, store.tenant_id()).await,
        0,
        "書いてしまった"
    );

    // 同じ窓を apply すると、dry-run が言ったとおりに書ける
    w.dry_run = false;
    let applied = apply_timecard_window(&store, &w).await.expect("apply");
    assert!(!applied.dry_run);
    assert_eq!(applied.days_written, planned.days_written);
    assert_eq!(count_events(&pool, store.tenant_id()).await, 2);
}

/// **多数の乗務員・多数の日を 1 トランザクションで書く。**
///
/// 1 日 1 DELETE・1 イベント 1 INSERT で往復していた頃は、2 か月 95 名の初回投入が
/// 10,157 往復になり Cloudflare の 524 (100 秒) を超えた。畳んだあとも同じ結果に
/// なることを実 DB で確かめる (件数・中身・署名の一致)。
#[tokio::test]
async fn a_wide_window_is_written_in_one_go() {
    let (store, pool) = require_db!();
    let mut events = Vec::new();
    for driver in 0..40_i64 {
        for day in 1..=28_u32 {
            events.push(win_punch(
                2000 + driver,
                &format!("2026-06-{day:02} 08:00:00"),
                "始業",
            ));
            events.push(win_punch(
                2000 + driver,
                &format!("2026-06-{day:02} 18:00:00"),
                "終業",
            ));
        }
    }
    let drivers: Vec<i64> = (0..40).map(|d| 2000 + d).collect();
    let w = window_of(&["2026-06"], &drivers, events);

    let r = apply_timecard_window(&store, &w).await.expect("apply");
    assert_eq!(r.drivers_written, 40);
    assert_eq!(r.days_written, 40 * 28);
    assert_eq!(r.events_written, 40 * 28 * 2);
    assert_eq!(count_events(&pool, store.tenant_id()).await, 40 * 28 * 2);

    // 署名も 1 名ずつ書いたときと同じ = 畳んでも中身が変わらない
    let (from, _) = jst_day_bounds(NaiveDate::from_ymd_opt(2026, 6, 1).unwrap());
    let (to, _) = jst_day_bounds(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
    let sigs = store
        .stored_window_signatures(&drivers, from, to)
        .await
        .expect("signatures");
    assert_eq!(sigs.len(), 40);
    assert_eq!(sigs[&2000].len(), 28);

    // 送り直しても 1 行も書かない (差分が出ない)
    let again = apply_timecard_window(&store, &w).await.expect("2nd");
    assert_eq!(again.days_written, 0);
    assert_eq!(again.drivers_written, 0);
}

/// **INSERT の刻みを跨いでも落ちない。** `raw` を積むと 1 文が数 MB に育つので
/// `INSERT_CHUNK` 行で刻んでいるが、刻んでも同じトランザクションの中に居る。
#[tokio::test]
async fn writes_survive_crossing_the_insert_chunk() {
    let (store, pool) = require_db!();
    // 2 名 × 28 日 × 40 件 = 2240 行 (INSERT_CHUNK = 2000 を跨ぐ)
    let mut events = Vec::new();
    for driver in [3001_i64, 3002] {
        for day in 1..=28_u32 {
            for i in 0..40_u32 {
                events.push(win_punch(
                    driver,
                    &format!("2026-06-{day:02} {:02}:{:02}:00", 8 + i / 60, i % 60),
                    if i % 2 == 0 { "始業" } else { "終業" },
                ));
            }
        }
    }
    let total = events.len() as i64;
    assert!(total > 2000, "刻みを跨がないテストになっている: {total}");
    let w = window_of(&["2026-06"], &[3001, 3002], events);

    let r = apply_timecard_window(&store, &w).await.expect("apply");
    assert_eq!(r.events_written as i64, total);
    assert_eq!(count_events(&pool, store.tenant_id()).await, total);
}

/// **この経路が作っていない行を巻き添えで消さない。**
///
/// DDL は `source IN ('timecard','dtako','alc_app')` を許すが、ここが作るのは
/// 前 2 つだけ。日ごと消して入れ直すときに絞らないと、他が書いた `alc_app` の行が
/// 消えて二度と戻せない (手元の payload から再生できない)。
#[tokio::test]
async fn the_window_leaves_rows_from_other_sources_alone() {
    let (store, pool) = require_db!();
    let tenant = store.tenant_id();
    // 他の書き手が入れた行を 1 件仕込む (同じ乗務員・同じ日)
    sqlx::query(
        "INSERT INTO kintai.kintai_events
                (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw)
         VALUES ($1, 4001, $2, '始業', 'alc_app', NULL, '{}'::jsonb)",
    )
    .bind(tenant)
    .bind(jst_at(NaiveDate::from_ymd_opt(2026, 6, 9).unwrap(), 22))
    .execute(&pool)
    .await
    .expect("seed alc_app row");

    // 同じ日を this 経路で 2 回書く (1 回目で入れ、2 回目で入れ直す)
    let w = window_of(
        &["2026-06"],
        &[4001],
        vec![win_punch(4001, "2026-06-09 08:00:00", "始業")],
    );
    apply_timecard_window(&store, &w).await.expect("1st");
    // 中身を変えて、その日を確実に書き直させる
    let w2 = window_of(
        &["2026-06"],
        &[4001],
        vec![win_punch(4001, "2026-06-09 07:30:00", "始業")],
    );
    let again = apply_timecard_window(&store, &w2).await.expect("2nd");
    assert_eq!(again.days_written, 1, "書き直しが起きていない");

    let survivors: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM kintai.kintai_events
          WHERE tenant_id = $1 AND source = 'alc_app'",
    )
    .bind(tenant)
    .fetch_one(&pool)
    .await
    .expect("count alc_app");
    assert_eq!(survivors, 1, "alc_app の行が巻き添えで消えた");
}

/// **署名も同じ `source` で絞る。** 片方だけ絞ると、他の書き手の行が署名に混ざって
/// 「中身は同じなのに毎回全日が違う」に倒れ、静かに全件を書き直し続ける。
#[tokio::test]
async fn other_sources_do_not_leak_into_the_signature() {
    let (store, pool) = require_db!();
    let tenant = store.tenant_id();
    let w = window_of(
        &["2026-06"],
        &[4002],
        vec![win_punch(4002, "2026-06-12 08:00:00", "始業")],
    );
    apply_timecard_window(&store, &w).await.expect("seed");

    // **同じ日の中**に別の書き手が 1 件足す (翌日に乗せると別経路の検査になる)
    sqlx::query(
        "INSERT INTO kintai.kintai_events
                (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw)
         VALUES ($1, 4002, $2, '終業', 'alc_app', NULL, '{}'::jsonb)",
    )
    .bind(tenant)
    .bind(jst_at(NaiveDate::from_ymd_opt(2026, 6, 12).unwrap(), 18))
    .execute(&pool)
    .await
    .expect("seed alc_app row");

    // 署名は変わらない = 差分が出ない
    let again = apply_timecard_window(&store, &w).await.expect("re-send");
    assert_eq!(again.days_written, 0, "他の書き手の行で差分が出た");
    assert_eq!(again.drivers_written, 0);
}

/// **payload が覆っていない日に他の書き手の行があっても、幻の削除を起こさない。**
///
/// 上の 2 本とは別経路 — あちらは「同じ日のハッシュが汚れるか」、こちらは
/// 「手元に無い日が `Deleted` と判定されるか」。絞りが無いと格納側にだけ
/// その日が現れ、消しにいってしまう。
#[tokio::test]
async fn other_sources_on_other_days_do_not_look_deleted() {
    let (store, pool) = require_db!();
    let tenant = store.tenant_id();
    let w = window_of(
        &["2026-06"],
        &[4003],
        vec![win_punch(4003, "2026-06-15 08:00:00", "始業")],
    );
    apply_timecard_window(&store, &w).await.expect("seed");

    // payload に無い日 (06-16) に別の書き手が置く
    sqlx::query(
        "INSERT INTO kintai.kintai_events
                (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw)
         VALUES ($1, 4003, $2, '始業', 'alc_app', NULL, '{}'::jsonb)",
    )
    .bind(tenant)
    .bind(jst_at(NaiveDate::from_ymd_opt(2026, 6, 16).unwrap(), 9))
    .execute(&pool)
    .await
    .expect("seed alc_app row");

    let again = apply_timecard_window(&store, &w).await.expect("re-send");
    assert_eq!(again.days_deleted, 0, "手元に無い日を消しにいった");
    assert_eq!(again.drivers_written, 0);

    let survivors: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM kintai.kintai_events
          WHERE tenant_id = $1 AND source = 'alc_app'",
    )
    .bind(tenant)
    .fetch_one(&pool)
    .await
    .expect("count alc_app");
    assert_eq!(survivors, 1);
}

/// **PK が衝突したら黙って上書きせず、丸ごと落ちる。**
///
/// PK は `(tenant_id, driver_cd, occurred_at, state)` で **`source` を含まない**。
/// DELETE を自分の source に絞った結果、他の書き手の行と同じ鍵になると INSERT が
/// 衝突する。絞る前は「他人の行を消してから入れる」ので通っていた = 静かに
/// 消していた。
///
/// どちらが正かの規則が無いので **loud fail** にしてある。窓ぜんたいで
/// 1 トランザクションなので、**1 行も書かれない**ことまで確かめる。
#[tokio::test]
async fn a_key_collision_with_another_source_fails_loudly() {
    let (store, pool) = require_db!();
    let tenant = store.tenant_id();
    let at = jst_at(NaiveDate::from_ymd_opt(2026, 6, 18).unwrap(), 8);
    sqlx::query(
        "INSERT INTO kintai.kintai_events
                (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw)
         VALUES ($1, 4004, $2, '始業', 'alc_app', NULL, '{}'::jsonb)",
    )
    .bind(tenant)
    .bind(at)
    .execute(&pool)
    .await
    .expect("seed alc_app row");

    // 同じ (乗務員, 時刻, state) を打刻として送る
    let w = window_of(
        &["2026-06"],
        &[4004],
        vec![
            win_punch(4004, "2026-06-18 08:00:00", "始業"),
            win_punch(4004, "2026-06-18 18:00:00", "終業"),
        ],
    );
    let err = apply_timecard_window(&store, &w).await.unwrap_err();
    assert!(err.to_string().contains("duplicate key"), "{err}");

    // 他の書き手の行は残り、こちらは 1 行も入っていない (1 トランザクション)
    let ours: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM kintai.kintai_events
          WHERE tenant_id = $1 AND source <> 'alc_app'",
    )
    .bind(tenant)
    .fetch_one(&pool)
    .await
    .expect("count ours");
    assert_eq!(ours, 0, "衝突したのに一部だけ書けている");
    assert_eq!(count_events(&pool, tenant).await, 1);
}
