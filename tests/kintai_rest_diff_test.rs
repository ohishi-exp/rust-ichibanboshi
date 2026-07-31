//! `/api/kintai/rest-diff` のテスト (Refs #205 の 41)。
//!
//! `/events` と同じくデータ源は社内 MariaDB の直読みなので、`KintaiEventsApi` の
//! mock を挿して **DB 無しで** route の振る舞いを固定する。
//!
//! 主眼は 2 つ:
//!
//! - **乗務員を省略できること** — 1 回叩けば月ぶんの対象が全部出る形であること
//! - **判定に入らないこと** — 突合の結果だけを返し、拘束も勤務も畳まないこと

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::kintai_repo::{
    DisabledKintaiEventsRepo, DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError,
};
use rust_ichibanboshi::routes;
use serde_json::{json, Value};
use tower::ServiceExt;

/// 呼び出し引数を記録し、仕込んだ結果を返す mock。
struct MockRestRepo {
    rows: Vec<Value>,
    fail: Option<String>,
    /// 記録するのは `(from, to, driver)` — route が当てた窓まで固定する
    calls: Mutex<Vec<(String, String, Option<u64>)>>,
}

impl MockRestRepo {
    fn with_rows(rows: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            rows,
            fail: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn failing(msg: &str) -> Arc<Self> {
        Arc::new(Self {
            rows: Vec::new(),
            fail: Some(msg.to_string()),
            calls: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl KintaiEventsApi for MockRestRepo {
    async fn fetch_rest_events_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.calls
            .lock()
            .unwrap()
            .push((from.to_string(), to.to_string(), driver));
        match &self.fail {
            Some(m) => Err(KintaiRepoError::QueryFailed(m.clone())),
            None => Ok(self.rows.clone()),
        }
    }

    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        panic!("/api/kintai/rest-diff は生イベントを読まない")
    }

    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        panic!("/api/kintai/rest-diff は生イベントを読まない")
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        panic!("/api/kintai/rest-diff はフェリーを読まない")
    }
}

fn app(repo: DynKintaiEventsRepo) -> Router {
    Router::new()
        .route("/api/kintai/rest-diff", get(routes::kintai::rest_diff))
        .layer(Extension(repo))
}

async fn call(repo: DynKintaiEventsRepo, uri: &str) -> (StatusCode, String) {
    let res = app(repo)
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

const UNKO: &str = "26061409573000000034471";

fn tc(at: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": 1445,
           "source": "dtako", "state": "休息", "unko_no": UNKO})
}

fn ev(start: &str, end: &str) -> Value {
    json!({"datetime": start, "end_datetime": end, "driver_id": 1445,
           "source": "dtako_events", "state": "休息", "unko_no": UNKO})
}

/// 実証された事故 (運行 `26061409573000000034471` / 乗務員 1445 / 2026-06) が
/// **運行NO・乗務員CD・運行日つきで名指しされる**こと。
#[tokio::test]
async fn a_stale_run_is_named_with_its_driver_and_date() {
    let repo = MockRestRepo::with_rows(vec![
        ev("2026-06-17 13:32:38", "2026-06-19 13:22:00"),
        tc("2026-06-18 07:50:36"),
        tc("2026-06-18 07:52:04"),
        tc("2026-06-19 13:22:22"),
        tc("2026-06-19 16:27:55"),
        tc("2026-06-22 10:01:57"),
    ]);
    let (status, body) = call(repo.clone(), "/api/kintai/rest-diff?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["month"], "2026-06");
    assert_eq!(v["driver"], Value::Null);
    assert_eq!(v["total"], 1);
    // **押す対象の数**は `total` と別に出す (2026-06 の実測では total 239 / mismatch 0)
    assert_eq!(v["mismatch_total"], 1);
    assert_eq!(
        v["total_by_kind"],
        json!({"mismatch": 1, "dtako_missing": 0, "events_missing": 0})
    );
    assert_eq!(v["scanned_unko"], 1);
    assert_eq!(v["skipped_rows"], 0);
    let it = &v["items"][0];
    assert_eq!(it["kind"], "mismatch");
    assert_eq!(it["unko_no"], UNKO);
    assert_eq!(it["driver_cds"], json!([1445]));
    assert_eq!(it["run_date"], "2026-06-14");
    assert_eq!(it["dtako_rest_rows"], 5);
    assert_eq!(it["dtako_events_rest_intervals"], 1);
    assert_eq!(
        it["dtako_events_only"],
        json!(["2026-06-17 13:32:38", "2026-06-19 13:22:00"])
    );
    assert_eq!(v["by_driver"]["1445"], 1);
    // 窓は /events と同じ [月初, 翌月+1日)
    let calls = repo.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![(
            "2026-06-01 00:00:00".to_string(),
            "2026-07-02 00:00:00".to_string(),
            None
        )]
    );
    assert_eq!(v["from"], "2026-06-01 00:00:00");
    assert_eq!(v["to"], "2026-07-02 00:00:00");
}

/// **秒まで一致していれば 1 件も出ない** (「勤務時間再登録」を押した後の姿)。
#[tokio::test]
async fn a_run_whose_two_tables_agree_is_not_listed() {
    let repo = MockRestRepo::with_rows(vec![
        ev("2026-06-17 13:32:38", "2026-06-19 13:22:00"),
        tc("2026-06-17 13:32:38"),
        tc("2026-06-19 13:22:00"),
    ]);
    let (status, body) = call(repo, "/api/kintai/rest-diff?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["total"], 0);
    assert_eq!(v["items"], json!([]));
    assert_eq!(v["by_driver"], json!({}));
    assert_eq!(v["mismatch_total"], 0);
}

/// **2026-06 の本番実測がこの形**だった — `time_card_dtako` に休息が 1 行も無い
/// 運行が 239 件で、`mismatch` は 0 件。`total` だけ見ると「239 運行がずれている」と
/// 読めてしまうので、**押す対象の数を別に出す**。
#[tokio::test]
async fn a_run_with_no_writeback_is_not_counted_as_something_to_fix() {
    let repo = MockRestRepo::with_rows(vec![ev("2026-06-17 13:32:38", "2026-06-19 13:22:00")]);
    let (status, body) = call(repo, "/api/kintai/rest-diff?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["total"], 1);
    // **押す対象は 0 件**
    assert_eq!(v["mismatch_total"], 0);
    assert_eq!(
        v["total_by_kind"],
        json!({"mismatch": 0, "dtako_missing": 1, "events_missing": 0})
    );
    assert_eq!(v["items"][0]["kind"], "dtako_missing");
    assert_eq!(v["items"][0]["dtako_rest_rows"], 0);
}

/// **乗務員は省略可** (`kosoku-daily` と同じ)。指定すれば repo までそのまま届く。
#[tokio::test]
async fn the_driver_is_optional_and_is_passed_down() {
    let repo = MockRestRepo::with_rows(Vec::new());
    let (status, body) = call(
        repo.clone(),
        "/api/kintai/rest-diff?month=2026-06&driver=1445",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["driver"], 1445);
    assert_eq!(repo.calls.lock().unwrap()[0].2, Some(1445));
}

/// `driver=` (空) は省略ではなく**不正**。黙って全乗務員を返さない。
#[tokio::test]
async fn an_empty_driver_is_rejected() {
    for uri in [
        "/api/kintai/rest-diff?month=2026-06&driver=",
        "/api/kintai/rest-diff?month=2026-06&driver=abc",
    ] {
        let repo = MockRestRepo::with_rows(Vec::new());
        let (status, body) = call(repo.clone(), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body.contains("乗務員CD"), "{body}");
        assert!(repo.calls.lock().unwrap().is_empty(), "DB を見に行かない");
    }
}

/// 壊れた月は **DB を見に行く前に** 400。
#[tokio::test]
async fn a_bad_month_is_rejected_before_the_db() {
    for m in ["", "2026-6", "2026-13", "nope"] {
        let repo = MockRestRepo::with_rows(Vec::new());
        let (status, body) = call(repo.clone(), &format!("/api/kintai/rest-diff?month={m}")).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "month={m:?}");
        assert!(body.contains("YYYY-MM"), "{body}");
        assert!(repo.calls.lock().unwrap().is_empty(), "month={m:?}");
    }
}

/// repo の失敗は 502 で、原因の文面を落とさない。
#[tokio::test]
async fn a_repo_failure_becomes_502() {
    let (status, body) = call(
        MockRestRepo::failing("boom"),
        "/api/kintai/rest-diff?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("boom"), "{body}");
}

/// **`dtako_events` を持たない実行形態では 503** (既定の `NotConfigured`)。
/// 「ずれが無い」と「答えられない」を取り違えないため。
#[tokio::test]
async fn a_backend_without_dtako_events_is_fail_closed() {
    let repo: DynKintaiEventsRepo = Arc::new(DisabledKintaiEventsRepo);
    let (status, body) = call(repo, "/api/kintai/rest-diff?month=2026-06").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"), "{body}");
}
