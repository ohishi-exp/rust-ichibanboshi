//! `/api/kintai/tail-gap-probe` のテスト (Refs #205)。
//!
//! `/events` / `/rest-diff` / `/reading-dates` と同じくデータ源は社内 MariaDB の
//! 直読みなので、`KintaiEventsApi` の mock を挿して **DB 無しで** route の
//! 振る舞いを固定する。
//!
//! 月は過去月 (2026-06) に固定する — `today_jst()` (実行時の実日付) が窓の末尾
//! より後になることを保証して、「進行中の月クランプ」に紛れさせないため
//! (`kintai_http_repo_test.rs` の `test_month_digest_*` と同じ方針)。
//!
//! 純粋ロジック (母集団の絞り方・punched_in_gap の判定) の網羅は
//! `src/kintai_tail_gap_probe.rs` の単体テストが持つ。ここでは route の配線
//! (400/503/502・driver の省略可・応答の形) だけを見る。

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

struct MockAllEventsRepo {
    rows: Vec<Value>,
    fail: Option<String>,
    calls: Mutex<Vec<(String, String)>>,
}

impl MockAllEventsRepo {
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
impl KintaiEventsApi for MockAllEventsRepo {
    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.calls
            .lock()
            .unwrap()
            .push((from.to_string(), to.to_string()));
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
        panic!("/api/kintai/tail-gap-probe は 1 乗務員ずつの生イベントを読まない")
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        panic!("/api/kintai/tail-gap-probe はフェリーを読まない")
    }
}

fn app(repo: DynKintaiEventsRepo) -> Router {
    Router::new()
        .route(
            "/api/kintai/tail-gap-probe",
            get(routes::kintai::tail_gap_probe),
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

fn start(driver: u64, date: &str) -> Value {
    json!({
        "datetime": format!("{date} 08:00:00"),
        "driver_id": driver,
        "source": "dtako_events",
        "state": "運行開始",
    })
}

fn punch(driver: u64, date: &str) -> Value {
    json!({
        "datetime": format!("{date} 07:45:00"),
        "driver_id": driver,
        "source": "timecard",
        "state": "始業",
    })
}

/// **閾値超え・打刻無し**の乗務員が名指しで返ること (合計だけで終わらない)。
#[tokio::test]
async fn a_driver_past_the_threshold_without_a_punch_is_named() {
    let repo = MockAllEventsRepo::with_rows(vec![start(1517, "2026-06-05")]);
    let (status, body) = call(repo.clone(), "/api/kintai/tail-gap-probe?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["month"], "2026-06");
    assert_eq!(v["driver"], Value::Null);
    assert_eq!(v["expected"], "2026-06-30");
    assert_eq!(v["population"], 1);
    assert_eq!(v["over_threshold_total"], 1);
    assert_eq!(v["over_threshold_unpunched_total"], 1);
    let d = &v["drivers"][0];
    assert_eq!(d["driver_cd"], 1517);
    assert_eq!(d["last_start_date"], "2026-06-05");
    assert_eq!(d["gap_days"], 25);
    assert_eq!(d["over_threshold"], true);
    assert_eq!(d["punched_in_gap"], false);
    // 窓は /events と同じ月の範囲 (exact_month_range)
    assert_eq!(
        repo.calls.lock().unwrap().clone(),
        vec![(
            "2026-06-01 00:00:00".to_string(),
            "2026-07-01 00:00:00".to_string(),
        )]
    );
}

/// 空き期間に打刻がある乗務員は `punched_in_gap=true` になり、
/// 「働いていない」候補 (`over_threshold_unpunched_total`) には数えない。
#[tokio::test]
async fn a_driver_with_a_punch_in_the_gap_is_not_counted_as_unpunched() {
    let repo =
        MockAllEventsRepo::with_rows(vec![start(1688, "2026-06-05"), punch(1688, "2026-06-20")]);
    let (status, body) = call(repo, "/api/kintai/tail-gap-probe?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["over_threshold_total"], 1);
    assert_eq!(v["over_threshold_unpunched_total"], 0);
    assert_eq!(v["drivers"][0]["punched_in_gap"], true);
}

/// **乗務員は省略可**。指定すればその 1 人だけに絞られ、repo までそのまま届く。
#[tokio::test]
async fn the_driver_is_optional_and_narrows_the_population() {
    let repo =
        MockAllEventsRepo::with_rows(vec![start(1517, "2026-06-05"), start(1688, "2026-06-05")]);
    let (status, body) = call(repo, "/api/kintai/tail-gap-probe?month=2026-06&driver=1517").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["driver"], 1517);
    assert_eq!(v["population"], 1);
    assert_eq!(v["drivers"][0]["driver_cd"], 1517);
}

/// `driver=` (空) と数字でない値は 400。黙って全乗務員を返さない。
#[tokio::test]
async fn an_empty_or_bad_driver_is_rejected() {
    for uri in [
        "/api/kintai/tail-gap-probe?month=2026-06&driver=",
        "/api/kintai/tail-gap-probe?month=2026-06&driver=abc",
    ] {
        let repo = MockAllEventsRepo::with_rows(Vec::new());
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
        let repo = MockAllEventsRepo::with_rows(Vec::new());
        let (status, body) = call(
            repo.clone(),
            &format!("/api/kintai/tail-gap-probe?month={m}"),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "month={m:?}");
        assert!(body.contains("YYYY-MM"), "{body}");
        assert!(repo.calls.lock().unwrap().is_empty(), "month={m:?}");
    }
}

/// **GRANT が無ければここに落ちる** — MariaDB のエラー文をそのまま載せた 502。
/// 黙って 0 件にはならない。
#[tokio::test]
async fn a_repo_failure_becomes_502_with_the_cause() {
    let (status, body) = call(
        MockAllEventsRepo::failing("SELECT command denied to user 'kintai_reader'"),
        "/api/kintai/tail-gap-probe?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("kintai_reader"), "{body}");
}

/// **`[mariadb]` を持たない実行形態では 503** (既定の `NotConfigured`)。
/// 「運行が無い」と「答えられない」を取り違えない。
#[tokio::test]
async fn a_backend_without_mariadb_is_fail_closed() {
    let repo: DynKintaiEventsRepo = Arc::new(DisabledKintaiEventsRepo);
    let (status, body) = call(repo, "/api/kintai/tail-gap-probe?month=2026-06").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"), "{body}");
}

/// 母集団の絞り: 運行が 1 件も無い乗務員 (打刻だけ) は population に入らない。
#[tokio::test]
async fn a_driver_with_only_punches_is_excluded_from_the_population() {
    let repo = MockAllEventsRepo::with_rows(vec![punch(9999, "2026-06-10")]);
    let (status, body) = call(repo, "/api/kintai/tail-gap-probe?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["population"], 0);
    assert_eq!(v["drivers"].as_array().unwrap().len(), 0);
}
