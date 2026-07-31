//! `/api/kintai/reading-dates` のテスト (Refs #205 の 42)。
//!
//! `/events` / `/rest-diff` と同じくデータ源は社内 MariaDB の直読みなので、
//! `KintaiEventsApi` の mock を挿して **DB 無しで** route の振る舞いを固定する。
//!
//! 主眼:
//!
//! - **読取日は勤務の日と一致しない** — 運行が覆う範囲つきで返ること
//! - **`by_reading_date` (取り直す日) が答え**で、上限で切られないこと
//! - 乗務員を省略できること

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

struct MockOpsRepo {
    rows: Vec<Value>,
    fail: Option<String>,
    calls: Mutex<Vec<(String, String, Option<u64>)>>,
}

impl MockOpsRepo {
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
impl KintaiEventsApi for MockOpsRepo {
    async fn fetch_operation_reading_dates_between(
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
        panic!("/api/kintai/reading-dates は生イベントを読まない")
    }

    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        panic!("/api/kintai/reading-dates は生イベントを読まない")
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        panic!("/api/kintai/reading-dates はフェリーを読まない")
    }
}

fn app(repo: DynKintaiEventsRepo) -> Router {
    Router::new()
        .route(
            "/api/kintai/reading-dates",
            get(routes::kintai::reading_dates),
        )
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

fn op(driver: i64, unko: &str, reading: &str, dep: &str, ret: &str) -> Value {
    json!({
        "driver_cd": driver,
        "unko_no": unko,
        "reading_date": reading,
        "run_date": dep.split(' ').next().unwrap_or(dep),
        "departure_at": dep,
        "return_at": ret,
    })
}

/// **長距離は読取日が運行終了の後に付く** (実測: 運行日 06-24 → 読取日 07-06)。
/// ずれた `乗務員CD | 暦日` から運行を引けるように、覆う範囲も返す。
#[tokio::test]
async fn a_long_run_maps_to_a_reading_date_days_later() {
    let repo = MockOpsRepo::with_rows(vec![op(
        1107,
        "26062408000000000011071",
        "2026-07-06",
        "2026-06-24 08:00:00",
        "2026-07-04 19:30:00",
    )]);
    let (status, body) = call(repo.clone(), "/api/kintai/reading-dates?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["month"], "2026-06");
    assert_eq!(v["driver"], Value::Null);
    assert_eq!(v["total"], 1);
    assert_eq!(v["unknown_reading_date"], 0);
    assert_eq!(v["skipped_rows"], 0);
    // 取り直す日の一覧が答え
    assert_eq!(
        v["by_reading_date"],
        json!({"2026-07-06": ["26062408000000000011071"]})
    );
    let it = &v["items"][0];
    assert_eq!(it["driver_cd"], 1107);
    assert_eq!(it["reading_date"], "2026-07-06");
    assert_eq!(it["run_start_date"], "2026-06-24");
    assert_eq!(it["run_end_date"], "2026-07-04");
    assert_eq!(it["departure_at"], "2026-06-24 08:00:00");
    // 窓は /events と同じ [月初, 翌月+1日)
    assert_eq!(
        repo.calls.lock().unwrap().clone(),
        vec![(
            "2026-06-01 00:00:00".to_string(),
            "2026-07-02 00:00:00".to_string(),
            None
        )]
    );
    assert_eq!(v["from"], "2026-06-01 00:00:00");
    assert_eq!(v["to"], "2026-07-02 00:00:00");
}

/// **読取日が引けない運行は数える。推測で埋めない。**
#[tokio::test]
async fn a_run_without_a_reading_date_is_counted() {
    let repo = MockOpsRepo::with_rows(vec![json!({
        "driver_cd": 1412,
        "unko_no": "26060208000000000014121",
        "reading_date": Value::Null,
        "departure_at": "2026-06-02 08:00:00",
    })]);
    let (status, body) = call(repo, "/api/kintai/reading-dates?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["total"], 1);
    assert_eq!(v["unknown_reading_date"], 1);
    assert_eq!(v["by_reading_date"], json!({}));
    assert_eq!(v["items"][0]["reading_date"], Value::Null);
}

/// **乗務員は省略可**。指定すれば repo までそのまま届く。
#[tokio::test]
async fn the_driver_is_optional_and_is_passed_down() {
    let repo = MockOpsRepo::with_rows(Vec::new());
    let (status, body) = call(
        repo.clone(),
        "/api/kintai/reading-dates?month=2026-06&driver=1107",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["driver"], 1107);
    assert_eq!(v["total"], 0);
    assert_eq!(repo.calls.lock().unwrap()[0].2, Some(1107));
}

/// `driver=` (空) と数字でない値は 400。黙って全乗務員を返さない。
#[tokio::test]
async fn an_empty_or_bad_driver_is_rejected() {
    for uri in [
        "/api/kintai/reading-dates?month=2026-06&driver=",
        "/api/kintai/reading-dates?month=2026-06&driver=abc",
    ] {
        let repo = MockOpsRepo::with_rows(Vec::new());
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
        let repo = MockOpsRepo::with_rows(Vec::new());
        let (status, body) = call(
            repo.clone(),
            &format!("/api/kintai/reading-dates?month={m}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "month={m:?}");
        assert!(body.contains("YYYY-MM"), "{body}");
        assert!(repo.calls.lock().unwrap().is_empty(), "month={m:?}");
    }
}

/// **GRANT が無ければここに落ちる** — MariaDB のエラー文をそのまま載せた 502。
/// 黙って 0 件にはならない (Refs #205 の 42 の未確認事項)。
#[tokio::test]
async fn a_repo_failure_becomes_502_with_the_cause() {
    let (status, body) = call(
        MockOpsRepo::failing("SELECT command denied to user 'kintai_reader' for column '読取日'"),
        "/api/kintai/reading-dates?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("kintai_reader"), "{body}");
    assert!(body.contains("読取日"), "{body}");
}

/// **`dtako_rows` を持たない実行形態では 503** (既定の `NotConfigured`)。
/// 「運行が無い」と「答えられない」を取り違えないため。
#[tokio::test]
async fn a_backend_without_dtako_rows_is_fail_closed() {
    let repo: DynKintaiEventsRepo = Arc::new(DisabledKintaiEventsRepo);
    let (status, body) = call(repo, "/api/kintai/reading-dates?month=2026-06").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"), "{body}");
}
