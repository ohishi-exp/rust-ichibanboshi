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
