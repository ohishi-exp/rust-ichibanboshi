//! 勤怠 Worker の書き込み部品 (`kintai_pg`、tokio-postgres の `query_typed` / `execute_typed`) と、root の Cloud Run 版
//! (sqlx) の経路に**同じ入力**を与え、書いた後の表の中身と応答が一致することを実 Postgres で確かめる (Refs #322)。
//!
//! - 表: `kintai.kintai_events`・`kintai.event_changes`・`kintai.wage_snapshot` (時刻の既定値 `ingested_at`・
//!   `recorded_at`・`computed_at` とテナントは除く)。int8[]・timestamptz[]・text[]・jsonb[] (NULL 入り)・date[]・int2[]・
//!   int4[] (NULL 入り)・bool[] の型付けをここで通す
//! - テナントは経路ごとに新しい UUID (root = A、Worker = B)。同じ DB・同じ migration
//! - Worker 側は口と同じく、本文を `kintai_logic` の `parse_batch` / `parse_snapshot` に通してから書く
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**失敗する** (skip して緑にしない)。CI は worker-kintai.yml の
//! `pg-parity` job (postgres service。ci.yml の `kintai migration + RLS` job と同じ形)。手元では:
//!
//! ```text
//! docker run -d --name kintai-pg-parity -e POSTGRES_PASSWORD=pw -p 127.0.0.1::5432 postgres:17
//! KINTAI_TEST_DATABASE_URL=postgres://postgres:pw@127.0.0.1:<port>/postgres \
//!   cargo test --manifest-path workers/kintai/Cargo.toml -p kintai-pg --test root_parity
//! ```

use std::collections::BTreeMap;
use std::sync::Arc;

use alc_worker_db::PgClient;
use axum::{Extension, Json};
use chrono::NaiveDate;
use kintai_kosoku::kintai_push::{jst_day_bounds, TimecardBatch};
use kintai_logic::timecard_write::parse_batch;
use kintai_logic::wage_snapshot::{MonthMasters, SnapshotRequest, WageSnapshotRow};
use kintai_logic::wage_write::parse_snapshot;
use rust_ichibanboshi::kintai_push::{apply_timecard_batch, KintaiPgStore};
use rust_ichibanboshi::routes::kintai_timecard::ReadTenant;
use rust_ichibanboshi::routes::wage_snapshot::put_wage_snapshot;
use serde_json::{json, Value};
use uuid::Uuid;

const JSON: Option<&str> = Some("application/json");

// ── 前提 (root の tests/*_pg_test.rs と同じ形。migration は repo ルートの migrations/) ──────────────

fn database_url() -> String {
    std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .expect("KINTAI_TEST_DATABASE_URL が要ります (skip して緑にしない)")
}

fn needs_psql_variables(sql: &str) -> bool {
    let ident = |s: &str| {
        s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    [":'", ":\""].iter().any(|open| {
        sql.match_indices(open)
            .map(|(i, _)| &sql[i + 2..])
            .any(|rest| rest.find(&open[1..]).is_some_and(|n| ident(&rest[..n])))
    })
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
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../migrations");
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
            .expect("migrations dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
            .collect();
        files.sort();
        for entry in files {
            let sql = std::fs::read_to_string(&entry).expect("read migration");
            if needs_psql_variables(&sql) {
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

/// root の経路 (テナント A) と Worker の経路 (テナント B)。
struct Both {
    pool: sqlx::PgPool,
    root: Arc<KintaiPgStore>,
    worker: PgClient,
    a: Uuid,
    b: Uuid,
}

async fn both() -> Both {
    let url = database_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Both {
        root: Arc::new(KintaiPgStore::from_pool(pool.clone(), a)),
        pool,
        worker: PgClient::new(client),
        a,
        b,
    }
}

/// 1 つの SQL (結果は 1 行 1 列の jsonb) で表の中身を読む。
async fn dump(pool: &sqlx::PgPool, sql: &str, tenant: Uuid) -> Value {
    sqlx::query_scalar::<_, Value>(sql)
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("dump")
}

const EVENTS: &str = r#"
SELECT coalesce(jsonb_agg(jsonb_build_object(
         'driver_cd', driver_cd, 'occurred_at', occurred_at, 'state', state, 'source', source,
         'unko_no', unko_no, 'raw', raw)
       ORDER BY driver_cd, occurred_at, state, source), '[]'::jsonb)
  FROM kintai.kintai_events WHERE tenant_id = $1
"#;

const CHANGES: &str = r#"
SELECT coalesce(jsonb_agg(jsonb_build_object(
         'driver_cd', driver_cd, 'date', date, 'before', before, 'after', after)
       ORDER BY driver_cd, date, recorded_at), '[]'::jsonb)
  FROM kintai.event_changes WHERE tenant_id = $1
"#;

const WAGES: &str = r#"
SELECT coalesce(jsonb_agg(to_jsonb(w) - 'tenant_id' - 'computed_at'
       ORDER BY comp_id, ym, restraint_source, driver_cd), '[]'::jsonb)
  FROM kintai.wage_snapshot w WHERE tenant_id = $1
"#;

// ── 打刻 (POST /api/kintai/timecard と GET /api/kintai/timecard/signatures) ──────────────────────────

fn raw(driver: i64, at: &str, state: &str, source: &str, unko: Option<&str>) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": driver, "source": source,
           "state": state, "unko_no": unko, "vehicle": "1234"})
}

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

fn batch(days: Vec<(&str, Vec<Value>)>, delete: &[&str]) -> TimecardBatch {
    TimecardBatch {
        month: "2026-06".to_string(),
        driver_cd: 1130,
        days: days.into_iter().map(|(k, v)| (d(k), v)).collect(),
        delete_dates: delete.iter().map(|s| d(s)).collect(),
    }
}

/// 初回取り込み → 1 日の打刻の修正と 1 日の削除と新しい日 → 同じものの再送、を両方の経路に流し、毎回の応答と
/// 最後の表の中身・署名が一致すること。
#[tokio::test(flavor = "multi_thread")]
async fn timecard_writes_the_same_rows_as_the_root_path() {
    let mut t = both().await;
    let unko = "26060109153000000012341";
    let steps = [
        batch(
            vec![
                (
                    "2026-06-01",
                    vec![
                        raw(1130, "2026-06-01 08:00:00", "始業", "timecard", None),
                        // 同じ (時刻, state) の dtako は timecard に負ける (重複の決着)
                        raw(1130, "2026-06-01 08:00:00", "始業", "dtako", Some(unko)),
                        raw(1130, "2026-06-01 09:15:30", "運行開始", "dtako", Some(unko)),
                        raw(1130, "2026-06-01 18:02:05", "終業", "timecard", None),
                        // DDL に無い state (数えて落とす)
                        raw(1130, "2026-06-01 09:00:00", "点呼", "timecard", None),
                    ],
                ),
                // 日のキーと違う行・別の乗務員 (misplaced)。**月の中の最後の日に置く** — 元の
                // `apply_timecard_batch` は `deduped` を累計の `misplaced` で引くので、misplaced の後に
                // 月の中の日が続くと引き算が負になる (debug は panic、release は折り返す。元からの挙動)。
                // misplaced を含む日の後に日が続く入力は避けている (#361)
                (
                    "2026-06-02",
                    vec![
                        raw(1130, "2026-06-02 08:00:00", "始業", "timecard", None),
                        raw(1130, "2026-06-03 08:00:00", "始業", "timecard", None),
                        raw(9999, "2026-06-02 08:00:00", "始業", "timecard", None),
                    ],
                ),
                // 月の外の日
                (
                    "2026-07-01",
                    vec![raw(1130, "2026-07-01 08:00:00", "始業", "timecard", None)],
                ),
            ],
            &[],
        ),
        batch(
            vec![
                (
                    "2026-06-01",
                    vec![
                        raw(1130, "2026-06-01 07:30:00", "始業", "timecard", None),
                        raw(1130, "2026-06-01 09:15:30", "運行開始", "dtako", Some(unko)),
                        raw(1130, "2026-06-01 18:02:05", "終業", "timecard", None),
                    ],
                ),
                (
                    "2026-06-03",
                    vec![raw(
                        1130,
                        "2026-06-03 23:59:59",
                        "運行終了",
                        "dtako",
                        Some(unko),
                    )],
                ),
            ],
            &["2026-06-02", "2026-07-01"],
        ),
        batch(
            vec![(
                "2026-06-01",
                vec![
                    raw(1130, "2026-06-01 18:02:05", "終業", "timecard", None),
                    raw(1130, "2026-06-01 07:30:00", "始業", "timecard", None),
                    raw(1130, "2026-06-01 09:15:30", "運行開始", "dtako", Some(unko)),
                ],
            )],
            &[],
        ),
    ];
    for (i, step) in steps.iter().enumerate() {
        let want = apply_timecard_batch(&t.root, step).await.expect("root");
        let body = serde_json::to_vec(step).unwrap();
        let parsed = parse_batch(JSON, &body).expect("parse_batch");
        let got = kintai_pg::apply_timecard_batch(&mut t.worker, t.b, &parsed)
            .await
            .expect("worker");
        assert_eq!(got, want, "step {i} の応答");
    }

    let events = dump(&t.pool, EVENTS, t.a).await;
    assert_eq!(dump(&t.pool, EVENTS, t.b).await, events);
    assert_eq!(events.as_array().unwrap().len(), 4, "{events}");
    assert_eq!(events[1]["raw"]["vehicle"], "1234", "raw は jsonb のまま");
    assert_eq!(events[1]["unko_no"], unko);

    let changes = dump(&t.pool, CHANGES, t.a).await;
    assert_eq!(dump(&t.pool, CHANGES, t.b).await, changes);
    // 06-01 の修正 (前後あり) と 06-02 の削除 (after = NULL)。初回取り込み・06-03・再送は記録しない
    assert_eq!(changes.as_array().unwrap().len(), 2, "{changes}");
    assert_eq!(changes[1]["after"], Value::Null);

    let (from, to) = (
        jst_day_bounds(d("2026-06-01")).0,
        jst_day_bounds(d("2026-07-01")).0,
    );
    let want = t.root.stored_day_signatures(1130, from, to).await.unwrap();
    let got = kintai_pg::stored_day_signatures(&mut t.worker, t.b, 1130, from, to)
        .await
        .unwrap();
    assert_eq!(got, want);
    assert_eq!(
        got.keys().copied().collect::<Vec<_>>(),
        vec![d("2026-06-01"), d("2026-06-03")]
    );
    // 書いていない乗務員・テナントの署名は空
    let other = kintai_pg::stored_day_signatures(&mut t.worker, Uuid::new_v4(), 1130, from, to)
        .await
        .unwrap();
    assert_eq!(other, BTreeMap::new());
}

// ── 賃金スナップショット (POST /api/kintai/wage-snapshot) ─────────────────────────────────────────

fn wage_row(driver_cd: i64, total: Option<i32>) -> WageSnapshotRow {
    WageSnapshotRow {
        driver_cd,
        driver_name: format!("乗務員{driver_cd}"),
        company: Some("本社".to_string()),
        branch_name: None,
        branch_code: Some(3),
        job_name: None,
        pay_kubun: Some(2),
        hourly_rate: Some(1250),
        calc_base: Some(200_000),
        calc_overtime: None,
        calc_total: total,
        paid_base: Some(190_000),
        paid_overtime: Some(30_000),
        working_minutes: Some(10_500),
        restraint_missing: total.is_none(),
    }
}

fn snapshot(rows: Vec<WageSnapshotRow>, synced: &str) -> SnapshotRequest {
    SnapshotRequest {
        comp_id: "comp-1".to_string(),
        month: "2026-06".to_string(),
        restraint_source: "gcp".to_string(),
        timecard_kosoku: Some("yes".to_string()),
        wage_logic_version: "wage-1".to_string(),
        masters: MonthMasters {
            salary_item_sha: Some("sha-1".to_string()),
            payroll_synced_at: Some(synced.to_string()),
        },
        rows,
    }
}

/// `computed_at` は書いた時刻 (経路ごとに違う) なので、比べる前に形だけ確かめて外す。
fn without_computed_at(mut v: Value) -> Value {
    if let Some(at) = v.get("computed_at") {
        assert!(at.is_string(), "{v}");
        v.as_object_mut().unwrap().remove("computed_at");
    }
    v
}

/// 保存 → 同じものの再送 (書かない) → 行と版の変更 → 0 行 (DELETE だけ)、を両方の経路に流し、毎回の応答と
/// 表の中身が一致すること。
#[tokio::test(flavor = "multi_thread")]
async fn wage_snapshot_writes_the_same_rows_as_the_root_path() {
    let mut t = both().await;
    let steps = vec![
        snapshot(
            vec![wage_row(1130, Some(231_000)), wage_row(1702, None)],
            "2026-07-03T09:12:00Z",
        ),
        snapshot(
            vec![wage_row(1702, None), wage_row(1130, Some(231_000))],
            "2026-07-03T09:12:00Z",
        ),
        snapshot(
            vec![wage_row(1130, Some(240_000))],
            "2026-07-04T00:00:00+09:00",
        ),
        snapshot(vec![], "2026-07-04T00:00:00+09:00"),
    ];
    let mut tables = Vec::new();
    for (i, step) in steps.into_iter().enumerate() {
        let Json(want) = put_wage_snapshot(
            Extension(Some(t.root.clone())),
            Extension(ReadTenant(Some(t.a))),
            Json(step.clone()),
        )
        .await
        .expect("root");
        let body = serde_json::to_vec(&step).unwrap();
        let (valid, synced_at) = parse_snapshot(JSON, &body).expect("parse_snapshot");
        let got = kintai_pg::put_wage_snapshot(&mut t.worker, t.b, valid, synced_at)
            .await
            .expect("worker");
        assert_eq!(
            without_computed_at(got),
            without_computed_at(want),
            "step {i} の応答"
        );
        let wages = dump(&t.pool, WAGES, t.a).await;
        assert_eq!(dump(&t.pool, WAGES, t.b).await, wages, "step {i} の表");
        tables.push(wages);
    }
    assert_eq!(tables[0].as_array().unwrap().len(), 2);
    assert_eq!(tables[1], tables[0], "同じ内容の再送は書かない");
    assert_eq!(tables[0][1]["calc_total"], Value::Null, "int4[] の NULL");
    assert_eq!(tables[0][0]["pay_kubun"], 2, "int2[]");
    assert_eq!(
        tables[2].as_array().unwrap().len(),
        1,
        "消えた乗務員の行は残らない"
    );
    assert_eq!(tables[3], json!([]));
}
