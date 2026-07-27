//! /api/kintai/kosoku-daily のテスト (Refs #118、拘束時間の打刻基準化 Phase 2)。
//!
//! `KintaiEventsApi` の mock を挿して **DB 無しで** route の振る舞いを固定する。
//! 畳み込みの規則そのものは `src/kosoku.rs` の unit test 側で網羅しているので、
//! ここで見るのは「route が期間を正しく組み、純粋ロジックへ渡し、応答の形を
//! 保つか」— つまり配線と検証と失敗の写し方。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::kintai_repo::{
    DisabledKintaiEventsRepo, DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError,
};
use rust_ichibanboshi::kosoku::KosokuParams;
use rust_ichibanboshi::routes;
use serde_json::{json, Value};
use tower::ServiceExt;

struct MockRepo {
    rows: Vec<Value>,
    fail: Option<KintaiRepoError>,
    calls: Mutex<Vec<(String, String, u64)>>,
}

impl MockRepo {
    fn with_rows(rows: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            rows,
            fail: None,
            calls: Mutex::new(Vec::new()),
        })
    }

    fn failing(e: KintaiRepoError) -> Arc<Self> {
        Arc::new(Self {
            rows: Vec::new(),
            fail: Some(e),
            calls: Mutex::new(Vec::new()),
        })
    }
}

#[async_trait]
impl KintaiEventsApi for MockRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.calls
            .lock()
            .unwrap()
            .push((from.to_string(), to.to_string(), driver));
        match &self.fail {
            Some(KintaiRepoError::NotConfigured) => Err(KintaiRepoError::NotConfigured),
            Some(KintaiRepoError::QueryFailed(m)) => Err(KintaiRepoError::QueryFailed(m.clone())),
            None => Ok(self.rows.clone()),
        }
    }
}

fn app(repo: DynKintaiEventsRepo) -> Router {
    Router::new()
        .route(
            "/api/kintai/kosoku-daily",
            get(routes::kintai::kosoku_daily),
        )
        .layer(Extension(repo))
        .layer(Extension(Arc::new(KosokuParams::default())))
}

async fn call(app: Router, uri: &str) -> (StatusCode, String) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

/// rows を仕込んで 1 回叩き、JSON を返す。
async fn serve(rows: Vec<Value>, uri: &str) -> (StatusCode, Value) {
    let (status, body) = call(app(MockRepo::with_rows(rows)), uri).await;
    let json = serde_json::from_str(&body).unwrap_or(Value::Null);
    (status, json)
}

fn tc(datetime: &str, state: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "source": "timecard", "state": state})
}

fn ev(start: &str, end: &str, state: &str) -> Value {
    json!({"datetime": start, "end_datetime": end, "source": "dtako_events", "state": state})
}

fn dtako(datetime: &str, state: &str, unko: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "source": "dtako",
           "state": state, "unko_no": unko})
}

// --- 期間の組み立て ---

#[tokio::test]
async fn reads_a_week_before_the_month() {
    let repo = MockRepo::with_rows(vec![]);
    let (status, _) = call(
        app(repo.clone()),
        "/api/kintai/kosoku-daily?month=2026-07&driver=0012",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // 月初の勤務は前月末の休息を要するので 7 日遡る。終わりは events と同じ翌月 2 日
    assert_eq!(
        *repo.calls.lock().unwrap(),
        vec![(
            "2026-06-24 00:00:00".to_string(),
            "2026-08-02 00:00:00".to_string(),
            12
        )]
    );
}

#[tokio::test]
async fn lookback_crosses_year_boundary() {
    let repo = MockRepo::with_rows(vec![]);
    let (status, _) = call(
        app(repo.clone()),
        "/api/kintai/kosoku-daily?month=2026-01&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(repo.calls.lock().unwrap()[0].0, "2025-12-25 00:00:00");
}

// --- 打刻のみ (日帰り) ---

#[tokio::test]
async fn timecard_only_shift() {
    let (status, body) = serve(
        vec![
            tc("2026-06-02 09:25:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:36:00", "休憩"),
            tc("2026-06-02 19:39:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1018",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["month"], "2026-06");
    assert_eq!(body["driver"], 1018);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-02");
    assert_eq!(days[0]["source"], "timecard");
    assert_eq!(days[0]["restraint_minutes"], 614);
    assert_eq!(days[0]["break_minutes"], 96);
    assert_eq!(days[0]["working_minutes"], 518);
    assert_eq!(days[0]["over_24h"], false);
}

// --- 運行のみ (打刻欠け → 休息で切る) ---

#[tokio::test]
async fn rest_only_shift_when_timecard_is_missing() {
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            dtako("2026-06-02 06:00:00", "運行開始", "A"),
            dtako("2026-06-02 14:10:00", "運行終了", "A"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["source"], "rest");
    assert_eq!(days[0]["start"], "2026-06-02 04:42:00");
    assert_eq!(days[0]["end"], "2026-06-02 16:18:00");
    assert_eq!(days[0]["restraint_minutes"], 696);
}

// --- 同日 2 運行 ---

#[tokio::test]
async fn two_runs_same_day_without_rest_stay_one_shift() {
    // 運行終了 → 8 分後に運行開始。運行では切らないので 1 勤務のまま
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            dtako("2026-06-02 06:00:00", "運行開始", "A"),
            dtako("2026-06-02 13:47:00", "運行終了", "A"),
            dtako("2026-06-02 13:55:00", "運行開始", "B"),
            dtako("2026-06-02 15:00:00", "運行終了", "B"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["restraint_minutes"], 696);
}

#[tokio::test]
async fn two_runs_same_day_with_rest_between_are_two_shifts() {
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 10:00:00", "2026-06-02 20:00:00", "休息"),
            ev("2026-06-02 23:00:00", "2026-06-03 09:00:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 2);
    assert_eq!(days[0]["start"], "2026-06-02 04:42:00");
    assert_eq!(days[0]["end"], "2026-06-02 10:00:00");
    assert_eq!(days[1]["start"], "2026-06-02 20:00:00");
    assert_eq!(days[1]["end"], "2026-06-02 23:00:00");
}

// --- 日跨ぎ ---

#[tokio::test]
async fn overnight_shift_is_filed_under_start_date() {
    let (status, body) = serve(
        vec![
            tc("2026-06-30 22:00:00", "始業"),
            tc("2026-07-01 08:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-30");
    assert_eq!(days[0]["restraint_minutes"], 600);
    // 深夜 22:00〜05:00 の 420 分
    assert_eq!(days[0]["night_minutes"], 420);
}

#[tokio::test]
async fn previous_month_shift_is_not_returned() {
    // 遡って読んだ前月分は当月の応答に混ぜない
    let (status, body) = serve(
        vec![
            tc("2026-05-28 09:00:00", "始業"),
            tc("2026-05-28 18:00:00", "終業"),
            tc("2026-06-01 09:00:00", "始業"),
            tc("2026-06-01 18:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-01");
}

// --- 法定休日 / 24 時間超 ---

#[tokio::test]
async fn sunday_is_legal_holiday() {
    let (status, body) = serve(
        vec![
            tc("2026-06-07 08:00:00", "始業"), // 日曜
            tc("2026-06-07 20:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["is_legal_holiday"], true);
    assert_eq!(d["legal_holiday_minutes"], 720);
    assert_eq!(d["overtime_minutes"], 0);
}

#[tokio::test]
async fn over_24h_is_capped_and_flagged() {
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"), // 38.1 時間
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["over_24h"], true);
    assert_eq!(d["restraint_minutes"], 1440);
}

// --- 検証 ---

#[tokio::test]
async fn rejects_bad_month() {
    for uri in [
        "/api/kintai/kosoku-daily?driver=1051",
        "/api/kintai/kosoku-daily?month=&driver=1051",
        "/api/kintai/kosoku-daily?month=2026-13&driver=1051",
        "/api/kintai/kosoku-daily?month=2026/06&driver=1051",
    ] {
        let repo = MockRepo::with_rows(vec![]);
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body.contains("YYYY-MM"));
        // 検証で弾いた分は DB を叩かない
        assert!(repo.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn rejects_bad_driver() {
    for uri in [
        "/api/kintai/kosoku-daily?month=2026-06",
        "/api/kintai/kosoku-daily?month=2026-06&driver=",
        "/api/kintai/kosoku-daily?month=2026-06&driver=10a1",
        "/api/kintai/kosoku-daily?month=2026-06&driver=-1",
    ] {
        let repo = MockRepo::with_rows(vec![]);
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body.contains("乗務員CD"));
        assert!(repo.calls.lock().unwrap().is_empty());
    }
}

// --- 失敗の写し方 ---

#[tokio::test]
async fn query_failure_is_502() {
    let (status, body) = call(
        app(MockRepo::failing(KintaiRepoError::QueryFailed(
            "boom".into(),
        ))),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("boom"));
}

#[tokio::test]
async fn not_configured_is_503() {
    let (status, body) = call(
        app(MockRepo::failing(KintaiRepoError::NotConfigured)),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"));
}

#[tokio::test]
async fn disabled_repo_is_503() {
    // `[mariadb]` 未設定で起動したときと同じ状態 — 空配列で「0 件」に見せない
    let (status, body) = call(
        app(Arc::new(DisabledKintaiEventsRepo)),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"));
}

#[tokio::test]
async fn no_events_is_empty_days_not_error() {
    let (status, body) = serve(vec![], "/api/kintai/kosoku-daily?month=2026-06&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["days"].as_array().unwrap().len(), 0);
}
