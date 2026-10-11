//! 打刻の置き換え `TimecardWindowIo` の Worker 実装 (`kintai_pg::FoldStore`、tokio-postgres の `tenant_tx` + `query_typed` /
//! `execute_typed`) と root の `KintaiPgStore` (sqlx) に**同じ窓**を与え、`apply_timecard_window` の応答・書いた後の表
//! (`kintai.kintai_events`・`event_changes`)・`stored_window_signatures` の戻りが一致することを実 Postgres で確かめる
//! (Refs #322、`POST /api/kintai/timecard/window`)。
//!
//! あわせて fold の口の失敗の写し (`FoldStoreError::into_fail`): 上流 (alc) の本文も DB の message も 502 の本文と
//! `fold_error` に出ないこと。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**失敗する** (skip して緑にしない)。CI は worker-kintai.yml の `pg-parity` job。

mod common;

use alc_worker_db::PgClient;
use chrono::{DateTime, FixedOffset, NaiveDate};
use common::{database_url, ensure_schema};
use kintai_kosoku::anchors::HeadAnchors;
use kintai_kosoku::fold_flow::{DtakoMonthRead, FoldClock, FoldEventsIo, Warning};
use kintai_kosoku::kintai_push::{jst_day_bounds, TimecardWindow};
use kintai_kosoku::route_flow::{apply_timecard_window, fold_into, TimecardWindowIo};
use kintai_logic::common::Fail;
use kintai_logic::fold_routes::{fold_ctx, jst_from_ms, parse_window};
use kintai_pg::{FoldStore, FoldStoreError};
use rust_ichibanboshi::kintai_push::KintaiPgStore;
use serde_json::{json, Value};
use uuid::Uuid;

const JSON: Option<&str> = Some("application/json");

/// 経過時間が 0 になる時計 (応答の `elapsed_ms` を比べられるように)。
struct Fixed;

impl FoldClock for Fixed {
    fn now_jst(&self) -> DateTime<FixedOffset> {
        jst_from_ms(1_782_831_600_000)
    }
    fn monotonic_ms(&self) -> u64 {
        0
    }
}

/// root の経路 (テナント A) と Worker の経路 (テナント B)。
struct Both {
    pool: sqlx::PgPool,
    root: KintaiPgStore,
    worker: FoldStore,
    a: Uuid,
    b: Uuid,
}

async fn client(url: &str) -> tokio_postgres::Client {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    client
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
    Both {
        root: KintaiPgStore::from_pool(pool.clone(), a),
        pool,
        worker: FoldStore::new(PgClient::new(client(&url).await), b),
        a,
        b,
    }
}

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

fn raw(driver: i64, at: &str, state: &str, source: &str, unko: Option<&str>) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": driver, "source": source,
           "state": state, "unko_no": unko, "vehicle": "1234"})
}

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

/// 口と同じく本文を `parse_window` に通した窓 (畳み直しは別に試すので `fold: false`)。
fn window(drivers: &[i64], events: Vec<Value>, dry_run: bool) -> TimecardWindow {
    let body = json!({"months": ["2026-05", "2026-06"], "drivers": drivers, "events": events,
                      "dry_run": dry_run, "fold": false});
    parse_window(JSON, &serde_json::to_vec(&body).unwrap()).expect("parse_window")
}

/// 応答の JSON (`elapsed_ms` は経路の時計で違うので 0 に揃える)。
fn result_json(mut v: Value) -> Value {
    v["elapsed_ms"] = json!(0);
    v
}

/// 2 か月の窓を dry-run → 初回 → 1 日の修正と 1 日の消去 → 同じものの再送、の順に両方の経路へ流し、毎回の応答・
/// 最後の表の中身・署名が一致すること。名乗っていない乗務員と窓の外の行は落として数える。
#[tokio::test(flavor = "multi_thread")]
async fn window_writes_the_same_rows_as_the_root_path() {
    let t = both().await;
    let unko = "26053109153000000012341";
    let first = vec![
        raw(1130, "2026-05-31 08:00:00", "始業", "timecard", None),
        raw(1130, "2026-05-31 09:15:30", "運行開始", "dtako", Some(unko)),
        raw(1130, "2026-06-01 02:00:00", "運行終了", "dtako", Some(unko)),
        raw(1130, "2026-06-01 03:00:00", "終業", "timecard", None),
        raw(2001, "2026-06-15 08:00:00", "始業", "timecard", None),
        raw(2001, "2026-06-15 17:00:00", "終業", "timecard", None),
        raw(2001, "2026-06-15 12:00:00", "点呼", "timecard", None),
        // 名乗っていない乗務員・窓の外 (misplaced)
        raw(9999, "2026-06-01 08:00:00", "始業", "timecard", None),
        raw(1130, "2026-07-01 08:00:00", "始業", "timecard", None),
    ];
    let second = vec![
        raw(1130, "2026-05-31 07:30:00", "始業", "timecard", None),
        raw(1130, "2026-05-31 09:15:30", "運行開始", "dtako", Some(unko)),
        raw(1130, "2026-06-01 02:00:00", "運行終了", "dtako", Some(unko)),
        raw(1130, "2026-06-01 03:00:00", "終業", "timecard", None),
    ];
    let steps = [
        window(&[1130, 2001], first.clone(), true),
        window(&[1130, 2001], first, false),
        window(&[1130, 2001], second.clone(), false),
        window(&[1130, 2001], second, false),
    ];
    for (i, w) in steps.iter().enumerate() {
        let want = rust_ichibanboshi::kintai_push::apply_timecard_window(&t.root, w)
            .await
            .expect("root");
        let got = apply_timecard_window(&t.worker, w, &Fixed)
            .await
            .expect("worker");
        assert_eq!(
            result_json(json!(got)),
            result_json(json!(want)),
            "step {i} の応答"
        );
    }

    let events = dump(&t.pool, EVENTS, t.a).await;
    assert_eq!(dump(&t.pool, EVENTS, t.b).await, events);
    assert_eq!(events.as_array().unwrap().len(), 4, "{events}");
    let changes = dump(&t.pool, CHANGES, t.a).await;
    assert_eq!(dump(&t.pool, CHANGES, t.b).await, changes);
    // 05-31 の修正と 06-15 の消去 (after = NULL)。初回・dry-run・再送は記録しない
    assert_eq!(changes.as_array().unwrap().len(), 2, "{changes}");

    let (from, to) = (
        jst_day_bounds(d("2026-05-01")).0,
        jst_day_bounds(d("2026-07-01")).0,
    );
    for drivers in [vec![1130, 2001, 4242], vec![2001], vec![]] {
        let want = t
            .root
            .stored_window_signatures(&drivers, from, to)
            .await
            .unwrap();
        let got = TimecardWindowIo::stored_window_signatures(&t.worker, &drivers, from, to)
            .await
            .unwrap();
        assert_eq!(got, want, "drivers {drivers:?}");
    }
    let got = TimecardWindowIo::stored_window_signatures(&t.worker, &[1130], from, to)
        .await
        .unwrap();
    assert_eq!(
        got[&1130].keys().copied().collect::<Vec<_>>(),
        vec![d("2026-05-31"), d("2026-06-01")]
    );
}

/// 上流の本文 (接続先を含む) を返して落ちる読み。
struct Failing;

const LEAK: &str =
    r#"alc dtako-events status 500: {"detail":"connect to db.internal.example:5432 failed"}"#;

impl FoldEventsIo for Failing {
    type Error = Fail;

    async fn month_head_anchors(&self, _: &str, _: &str) -> Result<HeadAnchors, Fail> {
        Err(Fail::new(502, LEAK))
    }

    async fn all_events_between(
        &self,
        _: &str,
        _: &str,
    ) -> Result<(Vec<Value>, Vec<Warning>), Fail> {
        Err(Fail::new(502, LEAK))
    }

    async fn dtako_month_read_since(
        &self,
        _: &str,
        _: Option<NaiveDate>,
    ) -> Result<DtakoMonthRead, Fail> {
        Err(Fail::new(502, LEAK))
    }
}

/// 窓を書いた後の畳み直しが上流の失敗で落ちても 200 のまま `fold_error` に載る — **種別だけ** (上流の本文を出さない)。
/// stale は独立に試して載る。
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_fold_reports_the_kind_only() {
    let t = both().await;
    let w = window(
        &[1130],
        vec![raw(1130, "2026-06-02 08:00:00", "始業", "timecard", None)],
        false,
    );
    let result = apply_timecard_window(&t.worker, &w, &Fixed).await.unwrap();
    assert_eq!(result.drivers_changed, vec![1130]);
    let mut body = json!(result);
    let ctx = fold_ctx("test", &Fixed);
    let status_of = |e: FoldStoreError| {
        let f = e.into_fail();
        (f.status, f.body)
    };
    fold_into(&Failing, &t.worker, &ctx, &w, &result, &mut body, status_of).await;
    assert_eq!(body["fold_error_status"], 502);
    assert_eq!(
        body["fold_error"],
        "kintai events read failed: alc dtako-events status 500"
    );
    assert!(!body.to_string().contains("db.internal"), "{body}");
    assert!(body.get("fold").is_none());
    assert_eq!(body["stale"]["logic_version"], "1b8f00ba48e9a77b");
    assert!(body.get("stale_error").is_none());
}

/// DB の失敗は `kintai push db failed: <SQLSTATE>` だけ (DB の message を 502 の本文にも Display にも出さない)。
#[tokio::test(flavor = "multi_thread")]
async fn a_db_failure_reports_the_sqlstate_only() {
    let url = database_url();
    let pool = sqlx::postgres::PgPool::connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let c = client(&url).await;
    c.batch_execute("SET default_transaction_read_only = on")
        .await
        .expect("read only");
    let store = FoldStore::new(PgClient::new(c), Uuid::new_v4());
    let w = window(
        &[1130],
        vec![raw(1130, "2026-06-02 08:00:00", "始業", "timecard", None)],
        false,
    );
    let e = apply_timecard_window(&store, &w, &Fixed).await.unwrap_err();
    let f = e.into_fail();
    assert_eq!(f, Fail::new(502, "kintai push db failed: 25006"));
    assert_eq!(e.to_string(), f.body);
    assert!(!format!("{e:?}").is_empty());
}

/// 段取りが月・窓を読めなかった (口が先に検査するので通常は起きない) のは root と同じ 503 と文言。
#[test]
fn bad_months_are_503_with_the_root_wording() {
    use kintai_kosoku::fold_flow::BadMonth;
    use kintai_kosoku::route_flow::BadWindow;
    let e = FoldStoreError::from(BadMonth("2026-13".to_string()));
    assert_eq!(e.into_fail(), Fail::new(503, "bad month: 2026-13"));
    let e = FoldStoreError::from(BadWindow("months が空です".to_string()));
    assert_eq!(e.into_fail(), Fail::new(503, "months が空です"));
    let e = FoldStoreError::from(Fail::new(503, "MariaDB 接続設定が未設定"));
    assert_eq!(
        e.to_string(),
        "kintai events read failed: MariaDB 接続設定が未設定"
    );
    assert_eq!(e.into_fail().status, 502);
}
