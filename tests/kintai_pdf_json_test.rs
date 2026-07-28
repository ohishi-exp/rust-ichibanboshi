//! /api/kintai/pdf-json の中継テスト (Refs #143、yhonda-ohishi/nginx#782)。
//!
//! CakePHP は wiremock で stub する (kintai_test.rs と同じ方針)。
//! 主眼は 2 つ:
//!
//! 1. **素通しであること** — 上流の形は nginx#782 で確定していないので、こちらは
//!    解釈も型付けもしない。未知フィールドが落ちないことを固定する。
//! 2. **`driver` の有無で上流の URL が変わること** — 省略時に `driver_id` を付けて
//!    しまうと全乗務員が取れず、MCP の一括チェックが月 1 リクエストで済まなくなる。

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::routes;
use tower::ServiceExt;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app(base_url: String) -> Router {
    Router::new()
        .route("/api/kintai/pdf-json", get(routes::kintai::pdf_json))
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

/// nginx#782 の仕様どおりの応答 (実応答での差し替えは #782 完了後)。
///
/// `kosoku_minutes` が **負のまま**入っているのは意図的 — 拘束列が負になる日を
/// 検出するのが目的なので、上流も中継も clamp しない (yhonda-ohishi/nginx#783)。
const UPSTREAM_BODY: &str = r#"{
  "month": "2026-04",
  "drivers": [
    {
      "driver_id": 1021,
      "name": "テスト 乗務員",
      "days": [
        {
          "day": 1,
          "attendance": ["08:00", "17:30", null, null],
          "kosoku_minutes": 570,
          "kosoku_by_type": {"デジタコ": 510, "TC_DC": 60},
          "remarks": "",
          "leaves": []
        },
        {
          "day": 2,
          "attendance": ["08:00", null, null, null],
          "kosoku_minutes": -30,
          "kosoku_by_type": {"デジタコ": 0, "TC_DC": -30},
          "remarks": "",
          "leaves": []
        }
      ],
      "totals": {
        "shukkin": 2,
        "kyuka": 0,
        "kyujitsu_shukkin": 0,
        "kyujitsu_shukkin_raw": -1,
        "total_kosoku": 540
      },
      "future_field": {"nested": [1, 2, 3]}
    }
  ],
  "generated_at": "2026-07-28T00:00:00+09:00"
}"#;

#[tokio::test]
async fn relays_verbatim_and_preserves_unknown_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/pdf-json"))
        .and(query_param("month", "2026-04"))
        .and(query_param("driver_id", "1021"))
        // 再計算 = 本番の time_card_kosoku への書き込みを起こさせない (nginx#786)
        .and(query_param("recalc", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;

    let (status, body) = call(
        app(server.uri()),
        "/api/kintai/pdf-json?month=2026-04&driver=1021",
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let driver = &v["drivers"][0];
    // 上流が足した項目を落とさない
    assert_eq!(driver["future_field"]["nested"][2], 3);
    // 負の拘束を clamp しない (#783 の検出が目的)
    assert_eq!(driver["days"][1]["kosoku_minutes"], -30);
    assert_eq!(driver["days"][1]["kosoku_by_type"]["TC_DC"], -30);
    // clamp 前の生値も素通しする
    assert_eq!(driver["totals"]["kyujitsu_shukkin_raw"], -1);
}

#[tokio::test]
async fn driver_omitted_fetches_all_drivers() {
    let server = MockServer::start().await;
    // 省略時に driver_id を付けてはいけない — 付くと 1 名しか返らない
    Mock::given(method("GET"))
        .and(path("/time-card/pdf-json"))
        .and(query_param("month", "2026-04"))
        .and(query_param_is_missing("driver_id"))
        .and(query_param("recalc", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/pdf-json?month=2026-04").await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["drivers"].as_array().unwrap().len(), 1);
}

/// 突合は**読み取り口**。上流は既定 (`recalc=1`) だと拘束時間を再計算して
/// `time_card_kosoku` を書き換えるので、呼び出し側の付け忘れが起きない位置で固定する。
#[tokio::test]
async fn always_sends_recalc_zero_so_upstream_never_writes() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/pdf-json"))
        .and(query_param("recalc", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;

    // 1 名指定・省略のどちらでも付く (1 名は上流の既定ゲートで必ず再計算が走る形だった)
    for uri in [
        "/api/kintai/pdf-json?month=2026-04&driver=1021",
        "/api/kintai/pdf-json?month=2026-04",
    ] {
        let (status, _) = call(app(server.uri()), uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
    }
}

#[tokio::test]
async fn invalid_month_is_rejected() {
    let server = MockServer::start().await;
    let (status, body) = call(app(server.uri()), "/api/kintai/pdf-json?month=2026-4").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("YYYY-MM"));

    // month 自体が無い
    let (status, _) = call(app(server.uri()), "/api/kintai/pdf-json").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn empty_driver_is_rejected_not_treated_as_all() {
    let server = MockServer::start().await;
    // front が値を入れ忘れたときに黙って全員ぶん返さない (kosoku-daily と同じ規則)
    let (status, body) = call(
        app(server.uri()),
        "/api/kintai/pdf-json?month=2026-04&driver=",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.contains("乗務員CD"));

    let (status, _) = call(
        app(server.uri()),
        "/api/kintai/pdf-json?month=2026-04&driver=10a1",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn upstream_error_becomes_bad_gateway() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/pdf-json"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let (status, body) = call(
        app(server.uri()),
        "/api/kintai/pdf-json?month=2026-04&driver=1021",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("500") && body.contains("boom"));
}

#[tokio::test]
async fn unparseable_upstream_json_becomes_bad_gateway() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/pdf-json"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&server)
        .await;

    let (status, _) = call(
        app(server.uri()),
        "/api/kintai/pdf-json?month=2026-04&driver=1021",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn not_configured_becomes_service_unavailable() {
    let (status, body) = call(
        app(String::new()),
        "/api/kintai/pdf-json?month=2026-04&driver=1021",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("base_url"));
}
