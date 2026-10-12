//! 勤怠 Worker の書き込み部品 (`kintai_pg`、tokio-postgres の `query_typed` / `execute_typed`) に固定の入力を与え、応答と
//! 書いた後の表の中身が固定した期待値になることを実 Postgres で確かめる (Refs #322)。
//!
//! - 期待値は、root の Cloud Run 版 (sqlx) の経路に同じ入力を与えて一致していた値を書き出したもの。root を撤去した後は
//!   この表が Worker の書き込みの正本。ファイル名の `root_parity` は、比較の相手を外す前の名前を残している
//! - 表: `kintai.kintai_events`・`kintai.event_changes`・`kintai.wage_snapshot` (時刻の既定値 `ingested_at`・
//!   `recorded_at`・`computed_at` とテナントは除く)。int8[]・timestamptz[]・text[]・jsonb[] (NULL 入り)・date[]・int2[]・
//!   int4[] (NULL 入り)・bool[] の型付けをここで通す
//! - テナントはテストごとに新しい UUID。同じ DB・同じ migration
//! - 本文は口と同じく `kintai_logic` の `parse_batch` / `parse_snapshot` に通してから書く
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

use alc_worker_db::PgClient;
use chrono::NaiveDate;
use kintai_kosoku::kintai_push::{jst_day_bounds, TimecardBatch};
use kintai_logic::timecard_write::parse_batch;
use kintai_logic::wage_snapshot::{MonthMasters, SnapshotRequest, WageSnapshotRow};
use kintai_logic::wage_write::parse_snapshot;
use serde_json::{json, Value};
use uuid::Uuid;

const JSON: Option<&str> = Some("application/json");

mod common;

use common::{database_url, ensure_schema};

/// Worker の経路 (テナントはテストごとに新しい UUID)。
struct Worker {
    pool: sqlx::PgPool,
    worker: PgClient,
    tenant: Uuid,
}

async fn worker() -> Worker {
    let url = database_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Worker {
        pool,
        worker: PgClient::new(client),
        tenant: Uuid::new_v4(),
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

/// 初回取り込み → 1 日の打刻の修正と 1 日の削除と新しい日 → 同じものの再送、を流し、毎回の応答と最後の表の中身・署名が
/// 固定した値になること。
#[tokio::test(flavor = "multi_thread")]
async fn timecard_writes_the_fixed_rows() {
    let mut t = worker().await;
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
                        // 別の乗務員 (misplaced)。この日の後に 06-02 が続く (#361 で折り返していた形)
                        raw(9999, "2026-06-01 08:00:00", "始業", "timecard", None),
                    ],
                ),
                // 日のキーと違う行 (misplaced)
                (
                    "2026-06-02",
                    vec![
                        raw(1130, "2026-06-02 08:00:00", "始業", "timecard", None),
                        raw(1130, "2026-06-03 08:00:00", "始業", "timecard", None),
                    ],
                ),
                // 月の外の日。月の中の日より前 (#361 で折り返していたもう 1 つの形) と後の両方
                (
                    "2026-05-31",
                    vec![raw(1130, "2026-05-31 08:00:00", "始業", "timecard", None)],
                ),
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
    let want = [
        json!({"days_deleted": 0, "days_written": 2, "deduped": 1, "events_written": 4,
               "misplaced": 4, "rejected": {"UnknownState": 1}, "unknown_states": ["点呼"]}),
        json!({"days_deleted": 1, "days_written": 2, "deduped": 0, "events_written": 4,
               "misplaced": 0, "rejected": {}, "unknown_states": []}),
        json!({"days_deleted": 0, "days_written": 1, "deduped": 0, "events_written": 3,
               "misplaced": 0, "rejected": {}, "unknown_states": []}),
    ];
    for (i, step) in steps.iter().enumerate() {
        let body = serde_json::to_vec(step).unwrap();
        let parsed = parse_batch(JSON, &body).expect("parse_batch");
        let got = kintai_pg::apply_timecard_batch(&mut t.worker, t.tenant, &parsed)
            .await
            .expect("worker");
        assert_eq!(json!(got), want[i], "step {i} の応答");
    }

    // 06-01 の修正後 (07:30 始業・09:15:30 運行開始・18:02:05 終業) と 06-03 の運行終了。06-02 は消え、月の外 (05-31・07-01)
    // と別の乗務員は入らない。`raw` は jsonb のまま、`occurred_at` は UTC
    let events = dump(&t.pool, EVENTS, t.tenant).await;
    let ev = |at: &str, utc: &str, state: &str, source: &str, unko: Option<&str>| {
        json!({"driver_cd": 1130, "occurred_at": utc, "state": state, "source": source,
               "unko_no": unko,
               "raw": {"datetime": at, "driver_id": 1130, "end_datetime": null, "source": source,
                       "state": state, "unko_no": unko, "vehicle": "1234"}})
    };
    assert_eq!(
        events,
        json!([
            ev(
                "2026-06-01 07:30:00",
                "2026-05-31T22:30:00+00:00",
                "始業",
                "timecard",
                None
            ),
            ev(
                "2026-06-01 09:15:30",
                "2026-06-01T00:15:30+00:00",
                "運行開始",
                "dtako",
                Some(unko)
            ),
            ev(
                "2026-06-01 18:02:05",
                "2026-06-01T09:02:05+00:00",
                "終業",
                "timecard",
                None
            ),
            ev(
                "2026-06-03 23:59:59",
                "2026-06-03T14:59:59+00:00",
                "運行終了",
                "dtako",
                Some(unko)
            ),
        ])
    );

    // 06-01 の修正 (前後あり) と 06-02 の削除 (after = NULL)。初回取り込み・06-03・再送は記録しない
    let changes = dump(&t.pool, CHANGES, t.tenant).await;
    let snap = |at: &str, state: &str, source: &str, unko: Option<&str>| json!({"occurred_at": at, "source": source, "state": state, "unko_no": unko});
    assert_eq!(
        changes,
        json!([
            {"driver_cd": 1130, "date": "2026-06-01",
             "before": [snap("2026-06-01 08:00:00", "始業", "timecard", None),
                        snap("2026-06-01 09:15:30", "運行開始", "dtako", Some(unko)),
                        snap("2026-06-01 18:02:05", "終業", "timecard", None)],
             "after": [snap("2026-06-01 07:30:00", "始業", "timecard", None),
                       snap("2026-06-01 09:15:30", "運行開始", "dtako", Some(unko)),
                       snap("2026-06-01 18:02:05", "終業", "timecard", None)]},
            {"driver_cd": 1130, "date": "2026-06-02",
             "before": [snap("2026-06-02 08:00:00", "始業", "timecard", None)],
             "after": null},
        ])
    );

    let (from, to) = (
        jst_day_bounds(d("2026-06-01")).0,
        jst_day_bounds(d("2026-07-01")).0,
    );
    let got = kintai_pg::stored_day_signatures(&mut t.worker, t.tenant, 1130, from, to)
        .await
        .unwrap();
    assert_eq!(
        got,
        BTreeMap::from([
            (
                d("2026-06-01"),
                "438bc351294be18cb18aded34749acb278be1136d1f25c1412c000f21fbdeee4".to_string()
            ),
            (
                d("2026-06-03"),
                "50687cea1daadb0e3173fac0af76c895eed59f166637ee4222994e7d66f8ce86".to_string()
            ),
        ])
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

/// `computed_at` は書いた時刻なので、比べる前に形だけ確かめて外す。
fn without_computed_at(mut v: Value) -> Value {
    if let Some(at) = v.get("computed_at") {
        assert!(at.is_string(), "{v}");
        v.as_object_mut().unwrap().remove("computed_at");
    }
    v
}

/// 期待する 1 行 (`ym` は月初、`restraint_source`・`comp_id` などは `snapshot` の固定値)。
fn wage_want(driver_cd: i64, total: Value, missing: bool, synced: &str) -> Value {
    json!({"branch_code": 3, "branch_name": null, "calc_base": 200000, "calc_overtime": null,
           "calc_total": total, "comp_id": "comp-1", "company": "本社", "driver_cd": driver_cd,
           "driver_name": format!("乗務員{driver_cd}"), "hourly_rate": 1250, "job_name": null,
           "min_wage_sha": null, "paid_base": 190000, "paid_overtime": 30000, "pay_kubun": 2,
           "payroll_synced_at": synced, "restraint_missing": missing, "restraint_source": "gcp",
           "salary_item_sha": "sha-1", "timecard_kosoku": "yes", "wage_logic_version": "wage-1",
           "working_minutes": 10500, "ym": "2026-06-01"})
}

/// 保存 → 同じものの再送 (書かない) → 行と版の変更 → 0 行 (DELETE だけ)、を流し、毎回の応答と表の中身が固定した値になること。
#[tokio::test(flavor = "multi_thread")]
async fn wage_snapshot_writes_the_fixed_rows() {
    let mut t = worker().await;
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
    let synced1 = "2026-07-03T09:12:00+00:00";
    let synced2 = "2026-07-03T15:00:00+00:00";
    let want_responses = [
        json!({"saved": 2, "skipped_unchanged": false, "timecard_kosoku": "yes"}),
        json!({"saved": 2, "skipped_unchanged": true, "timecard_kosoku": "yes"}),
        json!({"saved": 1, "skipped_unchanged": false, "timecard_kosoku": "yes"}),
        json!({"saved": 0, "skipped_unchanged": false, "timecard_kosoku": "yes"}),
    ];
    let want_tables = [
        json!([
            wage_want(1130, json!(231000), false, synced1),
            wage_want(1702, Value::Null, true, synced1),
        ]),
        // 同じ内容の再送は書かない (表も動かない)
        json!([
            wage_want(1130, json!(231000), false, synced1),
            wage_want(1702, Value::Null, true, synced1),
        ]),
        // 行と版の変更: 消えた乗務員の行は残らない
        json!([wage_want(1130, json!(240000), false, synced2)]),
        json!([]),
    ];
    for (i, step) in steps.into_iter().enumerate() {
        let body = serde_json::to_vec(&step).unwrap();
        let (valid, synced_at) = parse_snapshot(JSON, &body).expect("parse_snapshot");
        let got = kintai_pg::put_wage_snapshot(&mut t.worker, t.tenant, valid, synced_at)
            .await
            .expect("worker");
        assert_eq!(
            without_computed_at(got),
            want_responses[i],
            "step {i} の応答"
        );
        let wages = dump(&t.pool, WAGES, t.tenant).await;
        assert_eq!(wages, want_tables[i], "step {i} の表");
    }
}
