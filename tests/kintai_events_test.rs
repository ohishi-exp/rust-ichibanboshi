//! /api/kintai/events のテスト (Refs #114 / #116)。
//!
//! データ源は社内 MariaDB の直読みなので、`KintaiEventsApi` の mock を挿して
//! **DB 無しで** route の振る舞いを固定する (`DynRepo` / MockRepo と同じ形)。
//!
//! 主眼は「解釈しないこと」— repo が返した行を並べ替えず・畳まず・欠損を埋めずに
//! そのまま出すこと。ここが崩れると、Phase 2 で規則を決めるための材料
//! (同日 2 運行の切れ目、細切れ休憩、日跨ぎの終業) が中継段階で消える。

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
struct MockEventsRepo {
    rows: Vec<Value>,
    fail: Option<String>,
    calls: Mutex<Vec<(String, u64)>>,
}

impl MockEventsRepo {
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
impl KintaiEventsApi for MockEventsRepo {
    async fn fetch_events(&self, month: &str, driver: u64) -> Result<Vec<Value>, KintaiRepoError> {
        self.calls.lock().unwrap().push((month.to_string(), driver));
        match &self.fail {
            Some(m) => Err(KintaiRepoError::QueryFailed(m.clone())),
            None => Ok(self.rows.clone()),
        }
    }
}

fn app(repo: DynKintaiEventsRepo) -> Router {
    Router::new()
        .route("/api/kintai/events", get(routes::kintai::events))
        .layer(Extension(repo))
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
    let (status, body) = call(app(MockEventsRepo::with_rows(rows)), uri).await;
    let v = serde_json::from_str(&body).unwrap_or(Value::Null);
    (status, v)
}

fn timecard(datetime: &str, state: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "driver_id": 1051,
           "source": "timecard", "state": state, "unko_no": null, "vehicle": null})
}

fn dtako(datetime: &str, state: &str, unko_no: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "driver_id": 1051,
           "source": "dtako", "state": state, "unko_no": unko_no,
           "vehicle": "長崎100か4132"})
}

#[tokio::test]
async fn returns_rows_verbatim() {
    let rows = vec![
        timecard("2026-07-23 06:11:45", "始業"),
        dtako("2026-07-23 06:16:24", "運行開始", "20260723-001"),
        json!({"datetime": "2026-07-23 12:04:00", "end_datetime": "2026-07-23 12:11:00",
               "driver_id": 1051, "source": "dtako_events", "state": "休憩",
               "unko_no": "20260723-001", "vehicle": "長崎100か4132"}),
        timecard("2026-07-23 18:20:03", "終業"),
    ];
    let (status, v) = serve(rows.clone(), "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    // 行をいじらない — 並びも値も repo が返したまま
    assert_eq!(v["rows"], Value::Array(rows));
    // 7 分の休憩を「短いから」と落としたり丸めたりしない (閾値は規則側の話)
    assert_eq!(v["rows"][2]["end_datetime"], "2026-07-23 12:11:00");
    // 非 ASCII が壊れない
    assert_eq!(v["rows"][1]["vehicle"], "長崎100か4132");
}

#[tokio::test]
async fn month_and_driver_reach_the_repo() {
    let repo = MockEventsRepo::with_rows(vec![]);
    let (status, _) = call(
        app(repo.clone()),
        "/api/kintai/events?month=2026-07&driver=0012",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // 乗務員CD は整数にしてから渡す (前ゼロは落ちる)
    assert_eq!(
        *repo.calls.lock().unwrap(),
        vec![("2026-07".to_string(), 12)]
    );
}

#[tokio::test]
async fn empty_rows_is_ok() {
    let (status, v) = serve(vec![], "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    assert!(v["rows"].as_array().unwrap().is_empty());
}

/// 打刻のみの日 (デジタコに乗らない事務員 / 車に乗らなかった日)
#[tokio::test]
async fn timecard_only_day() {
    let rows = vec![
        timecard("2026-07-06 08:02:11", "始業"),
        timecard("2026-07-06 17:31:45", "終業"),
    ];
    let (status, v) = serve(rows, "/api/kintai/events?month=2026-07&driver=1670").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    // 運行が無い項目は null のまま (欠損を 0 や "" に化かさない)
    assert!(rows[0]["unko_no"].is_null());
}

/// 運行のみの日 (打刻を忘れて出庫した日) — 打刻を補完しない
#[tokio::test]
async fn dtako_only_day() {
    let rows = vec![
        dtako("2026-07-07 05:58:02", "運行開始", "20260707-014"),
        dtako("2026-07-07 19:44:30", "運行終了", "20260707-014"),
    ];
    let (status, v) = serve(rows, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["source"] == "dtako"));
}

/// 同日 2 運行 — 運行の切れ目 (休息) も行として残る
#[tokio::test]
async fn two_trips_in_one_day() {
    let rows = vec![
        dtako("2026-07-08 05:30:00", "運行開始", "20260708-001"),
        dtako("2026-07-08 11:10:00", "運行終了", "20260708-001"),
        dtako("2026-07-08 11:20:00", "休息", "20260708-001"),
        dtako("2026-07-08 13:05:00", "運行開始", "20260708-002"),
        dtako("2026-07-08 20:15:00", "運行終了", "20260708-002"),
    ];
    let (status, v) = serve(rows, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 5);
    // 運行NO が 2 つ残る = 1 日 1 行に畳んでいない
    assert_eq!(rows[0]["unko_no"], "20260708-001");
    assert_eq!(rows[3]["unko_no"], "20260708-002");
}

/// 日跨ぎ — 月末に始まり翌月に終わる勤務。月で切らない
#[tokio::test]
async fn overnight_shift() {
    let rows = vec![
        timecard("2026-07-31 21:40:00", "始業"),
        dtako("2026-07-31 22:05:00", "運行開始", "20260731-020"),
        dtako("2026-08-01 07:12:00", "運行終了", "20260731-020"),
        timecard("2026-08-01 07:30:00", "終業"),
    ];
    let (status, v) = serve(rows, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    // 翌月に出た終業も落ちない (月で切ると拘束の終わりが消える)
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[3]["datetime"], "2026-08-01 07:30:00");
}

#[tokio::test]
async fn month_is_required_and_validated() {
    // repo が呼ばれないことを failing mock で担保する (呼ばれたら 502 になる)
    let repo = MockEventsRepo::failing("should not be called");
    for uri in [
        "/api/kintai/events?driver=1051",
        "/api/kintai/events?month=&driver=1051",
        "/api/kintai/events?month=2026-7&driver=1051",
        "/api/kintai/events?month=2026-13&driver=1051",
        "/api/kintai/events?month=2026/07&driver=1051",
    ] {
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri={uri}");
        assert!(body.contains("YYYY-MM"), "uri={uri}");
    }
    assert!(repo.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn driver_is_required_and_validated() {
    let repo = MockEventsRepo::failing("should not be called");
    for uri in [
        "/api/kintai/events?month=2026-07",
        "/api/kintai/events?month=2026-07&driver=",
        "/api/kintai/events?month=2026-07&driver=abc",
        "/api/kintai/events?month=2026-07&driver=-1",
    ] {
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri={uri}");
        assert!(body.contains("乗務員CD"), "uri={uri}");
    }
    assert!(repo.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn mariadb_unconfigured_is_503() {
    // 空配列を返して「0 件」に見せず fail-closed であること
    let (status, body) = call(
        app(Arc::new(DisabledKintaiEventsRepo)),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"));
}

#[tokio::test]
async fn query_failure_is_502_with_cause() {
    let (status, body) = call(
        app(MockEventsRepo::failing("Connection refused (os error 111)")),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    // DB 停止の原因が応答に残る (ログにも同じ文字列が出る)
    assert!(body.contains("Connection refused"));
}
