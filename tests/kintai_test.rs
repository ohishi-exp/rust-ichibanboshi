//! /api/kintai/daily の中継テスト (Refs #99)。
//!
//! CakePHP は wiremock で stub する (uriage の /recalc 統合テストと同じ方針)。
//! 「素通しであること」— 未知フィールドを落とさない・行を解釈しない — を固定するのが
//! 主眼。ここが崩れると、上流 (yhonda-ohishi/nginx) が項目を足したときに中継で
//! 情報が消え、受け手 (nuxt-dtako-admin) からは原因が見えなくなる。

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::routes;
use tower::ServiceExt;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app(base_url: String) -> Router {
    Router::new()
        .route("/api/kintai/daily", get(routes::kintai::daily))
        .layer(Extension(Arc::new(
            CakephpClient::new(base_url, 5).expect("client"),
        )))
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

/// 実機の応答形 (nginx#776 後)。`sessions` は中抜けの内訳で必ず存在する。
const UPSTREAM_BODY: &str = r#"{
  "rows": [
    {
      "driver_id": 1670,
      "name": "松永　寿乃",
      "date": "2026-06-11",
      "start": "2026-06-11 07:44:15",
      "end": "2026-06-11 16:49:11",
      "restraint_minutes": 544,
      "sessions": [
        {"start": "2026-06-11 07:44:15", "end": "2026-06-11 11:41:10"},
        {"start": "2026-06-11 13:14:38", "end": "2026-06-11 16:49:11"}
      ],
      "holiday": "weekday",
      "office": "大石運輸倉庫㈱ 本社",
      "future_field": {"nested": [1, 2, 3]}
    }
  ],
  "generated_at": "2026-07-26T00:00:00+09:00"
}"#;

#[tokio::test]
async fn relays_rows_and_preserves_unknown_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/daily-json"))
        .and(query_param("month", "2026-06"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let row = &v["rows"][0];
    assert_eq!(row["driver_id"], 1670);
    assert_eq!(row["restraint_minutes"], 544);
    assert_eq!(row["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(row["holiday"], "weekday");
    // 行の未知フィールド (将来の追加項目) が落ちない
    assert_eq!(row["future_field"]["nested"][2], 3);
    // トップレベルの未知フィールドも落ちない
    assert_eq!(v["generated_at"], "2026-07-26T00:00:00+09:00");
    // 氏名の非 ASCII が壊れない
    assert_eq!(row["name"], "松永　寿乃");
}

#[tokio::test]
async fn empty_rows_is_ok() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"rows":[]}"#))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, r#"{"rows":[]}"#);
}

#[tokio::test]
async fn month_is_required_and_validated() {
    // 上流が呼ばれないことを mount 無しの server で担保する
    let server = MockServer::start().await;
    for uri in [
        "/api/kintai/daily",
        "/api/kintai/daily?month=",
        "/api/kintai/daily?month=2026-6",
        "/api/kintai/daily?month=2026-13",
        "/api/kintai/daily?month=2026/06",
    ] {
        let (status, body) = call(app(server.uri()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri={uri}");
        assert!(body.contains("YYYY-MM"), "uri={uri}");
    }
}

#[tokio::test]
async fn base_url_unset_is_503() {
    let (status, body) = call(app(String::new()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("base_url"));
}

#[tokio::test]
async fn upstream_5xx_is_502() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("500"));
}

#[tokio::test]
async fn upstream_non_json_is_502() {
    // allowlist 登録漏れでログイン画面の HTML が返るケース (nginx#773 で指摘した罠)
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("<!DOCTYPE html><html>login</html>"),
        )
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("parse failed"));
}

#[tokio::test]
async fn upstream_unreachable_is_502() {
    // 到達不能なポート (listen していない) → RequestFailed
    let (status, body) = call(
        app("http://127.0.0.1:1".to_string()),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("fetch failed"));
}
