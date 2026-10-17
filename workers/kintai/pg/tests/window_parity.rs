//! 打刻の置き換え `TimecardWindowIo` の Worker 実装 (`kintai_pg::FoldStore`、tokio-postgres の `tenant_tx` + `query_typed` /
//! `execute_typed`) に固定の窓を与え、`apply_timecard_window` の応答・書いた後の表 (`kintai.kintai_events`・
//! `event_changes`)・`stored_window_signatures` の戻りが固定した期待値になることを実 Postgres で確かめる
//! (Refs #322、`POST /api/kintai/timecard/window`)。期待値は、root の `KintaiPgStore` (sqlx) に同じ窓を与えて一致していた値。
//! ファイル名の `window_parity` は、比較の相手を外す前の名前を残している。
//!
//! あわせて fold の口の失敗の写し (`FoldStoreError::into_fail`): 上流 (alc) の本文も DB の message も 502 の本文と
//! `fold_error` に出ないこと。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**失敗する** (skip して緑にしない)。CI は worker-kintai.yml の `pg-parity` job。

mod common;

use std::collections::BTreeMap;

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

/// Worker の経路 (テナントはテストごとに新しい UUID)。
struct Env {
    pool: sqlx::PgPool,
    worker: FoldStore,
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

async fn env() -> Env {
    let url = database_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let b = Uuid::new_v4();
    Env {
        pool,
        worker: FoldStore::new(PgClient::new(client(&url).await), b),
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
async fn window_writes_the_fixed_rows() {
    let t = env().await;
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
    let want = [
        json!({"days_deleted": 0, "days_written": 3, "deduped": 0, "drivers": 2,
               "drivers_changed": [1130, 2001], "drivers_written": 2, "dry_run": true,
               "elapsed_ms": 0, "events_written": 6, "misplaced": 2,
               "rejected": {"UnknownState": 1}, "unknown_states": ["点呼"]}),
        json!({"days_deleted": 0, "days_written": 3, "deduped": 0, "drivers": 2,
               "drivers_changed": [1130, 2001], "drivers_written": 2, "dry_run": false,
               "elapsed_ms": 0, "events_written": 6, "misplaced": 2,
               "rejected": {"UnknownState": 1}, "unknown_states": ["点呼"]}),
        json!({"days_deleted": 1, "days_written": 1, "deduped": 0, "drivers": 2,
               "drivers_changed": [1130, 2001], "drivers_written": 2, "dry_run": false,
               "elapsed_ms": 0, "events_written": 2, "misplaced": 0, "rejected": {},
               "unknown_states": []}),
        json!({"days_deleted": 0, "days_written": 0, "deduped": 0, "drivers": 2,
               "drivers_changed": [], "drivers_written": 0, "dry_run": false,
               "elapsed_ms": 0, "events_written": 0, "misplaced": 0, "rejected": {},
               "unknown_states": []}),
    ];
    for (i, w) in steps.iter().enumerate() {
        let got = apply_timecard_window(&t.worker, w, &Fixed)
            .await
            .expect("worker");
        assert_eq!(result_json(json!(got)), want[i], "step {i} の応答");
    }

    // 2 回目の窓 (05-31 の始業の修正・06-15 の乗務員 2001 の消去) で上書きされた後の 4 行。dry-run は書かない
    let events = dump(&t.pool, EVENTS, t.b).await;
    let ev = |at: &str, utc: &str, state: &str, source: &str, unko: Option<&str>| {
        json!({"driver_cd": 1130, "occurred_at": utc, "state": state, "source": source,
               "unko_no": unko,
               "raw": {"datetime": at, "driver_id": 1130, "end_datetime": null, "source": source,
                       "state": state, "unko_no": unko, "vehicle": "1234"}})
    };
    let unko = Some("26053109153000000012341");
    assert_eq!(
        events,
        json!([
            ev(
                "2026-05-31 07:30:00",
                "2026-05-30T22:30:00+00:00",
                "始業",
                "timecard",
                None
            ),
            ev(
                "2026-05-31 09:15:30",
                "2026-05-31T00:15:30+00:00",
                "運行開始",
                "dtako",
                unko
            ),
            ev(
                "2026-06-01 02:00:00",
                "2026-05-31T17:00:00+00:00",
                "運行終了",
                "dtako",
                unko
            ),
            ev(
                "2026-06-01 03:00:00",
                "2026-05-31T18:00:00+00:00",
                "終業",
                "timecard",
                None
            ),
        ])
    );

    // 05-31 の修正と 06-15 の消去 (after = NULL)。初回・dry-run・再送は記録しない
    let changes = dump(&t.pool, CHANGES, t.b).await;
    let snap = |at: &str, state: &str, source: &str, unko: Option<&str>| json!({"occurred_at": at, "source": source, "state": state, "unko_no": unko});
    assert_eq!(
        changes,
        json!([
            {"driver_cd": 1130, "date": "2026-05-31",
             "before": [snap("2026-05-31 08:00:00", "始業", "timecard", None),
                        snap("2026-05-31 09:15:30", "運行開始", "dtako", unko)],
             "after": [snap("2026-05-31 07:30:00", "始業", "timecard", None),
                       snap("2026-05-31 09:15:30", "運行開始", "dtako", unko)]},
            {"driver_cd": 2001, "date": "2026-06-15",
             "before": [snap("2026-06-15 08:00:00", "始業", "timecard", None),
                        snap("2026-06-15 17:00:00", "終業", "timecard", None)],
             "after": null},
        ])
    );

    let (from, to) = (
        jst_day_bounds(d("2026-05-01")).0,
        jst_day_bounds(d("2026-07-01")).0,
    );
    let sig = |s: &str| s.to_string();
    let got = TimecardWindowIo::stored_window_signatures(&t.worker, &[1130, 2001, 4242], from, to)
        .await
        .unwrap();
    assert_eq!(
        got,
        BTreeMap::from([(
            1130,
            BTreeMap::from([
                (
                    d("2026-05-31"),
                    sig("c8c712739cfb9e1da301844ab2bf7a119ab0627f1afdacbe9fa179960e96785f")
                ),
                (
                    d("2026-06-01"),
                    sig("d88a4b874c7bdc3e8f6afb19657c3914d07b4e3345f18b33a0b2375a9668fdf4")
                ),
            ])
        )])
    );
    // 消去済み (2001)・保存の無い乗務員・空の並びは空
    for drivers in [vec![2001], vec![]] {
        let got = TimecardWindowIo::stored_window_signatures(&t.worker, &drivers, from, to)
            .await
            .unwrap();
        assert!(got.is_empty(), "drivers {drivers:?}: {got:?}");
    }
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
    let t = env().await;
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
    assert_eq!(body["stale"]["logic_version"], "d9be7d58d164f300");
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

/// 段取りが月・窓を読めなかった (口が先に検査するので通常は起きない) のは 503 と固定の文言 (root の文言と同じだった)。
#[test]
fn bad_months_are_503_with_fixed_wording() {
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
