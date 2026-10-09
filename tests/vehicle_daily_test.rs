mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/sales/vehicle-daily
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vehicle_daily_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// **請求区分をそのまま返す** (2026-08-22)。`請求K=1` は「請求のみ」= 運送を伴わない
/// 請求行で、車輌収支に足すと二重計上になる。実データ (2026-07) では中継の通し請求
/// `釧路 → ユナイテッド牧場 ¥43,750` (請求K=1) と、実際に走った 2 本
/// `釧路 → 駒場 ¥21,750` + `駒場 → ユナイテッド牧場 ¥22,000` (どちらも請求K=2) が
/// 同じ荷の表裏になっていた。**絞り込みはせず、消費側が判断できるように出す。**
#[tokio::test]
async fn test_vehicle_daily_returns_request_kind() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&customer=000001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let kinds: Vec<&str> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["request_kind"].as_str().unwrap())
        .collect();
    // 得意先 000001 の 2 行 = 通常運送 (請求K=0) と 請求のみ (請求K=1)。どちらも落ちない。
    assert_eq!(kinds, vec!["0", "1"]);
}

#[tokio::test]
async fn test_vehicle_daily_ok_with_limit() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_vehicle_daily_no_filters_is_bad_request() {
    // vehicle/customer/origin/dest が 1 つも無い → ハンドラ側の絞り込み必須チェックで 400
    // (#79: vehicle は任意化されたため、以前と違い axum の Query extractor では拒否されない)
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_vehicle_daily_blank_filters_is_bad_request() {
    // 全パラメータはあるが空文字/空白のみ → trim().is_empty() で無視され結局 0 件、400
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri(
                    "/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01\
                     &vehicle=%20&driver=%20&customer=&origin=&dest=",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_vehicle_daily_customer_only_searches_across_vehicles() {
    // #79 の主目的: vehicle を指定せず customer だけで車輌を横断して検索できること
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&customer=000001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let data = json["data"].as_array().unwrap();
    // customer=000001 は車輌 8504 と 9012 の両方に存在する (mock フィクスチャ)
    assert_eq!(data.len(), 2);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
}

#[tokio::test]
async fn test_vehicle_daily_driver_only_searches_across_vehicles() {
    // #741 の主目的: 同じ乗務員の売上が日によって別の車番に載る (デジタコを積んで
    // いない車輌等) ため、**車番ではなく乗務員CD で横断して引けること**。
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&driver=1656")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let data = json["data"].as_array().unwrap();
    // 乗務員 1656 は車輌 8504 と 9012 の両方で走っている (mock フィクスチャ)
    assert_eq!(data.len(), 2);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
    assert_eq!(data[0]["driver_code"], "1656");
}

#[tokio::test]
async fn test_vehicle_daily_origin_partial_match() {
    // origin は地域ﾏｽﾀ由来 (origin_area_name) と自由入力 (origin) のいずれかへの部分一致
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&origin=%E9%95%B7%E5%B4%8E")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let data = json["data"].as_array().unwrap();
    // "長崎" は "長崎県" (row1) と "長崎県佐世保市" (row3) の両方に部分一致する
    assert_eq!(data.len(), 2);
}

#[tokio::test]
async fn test_vehicle_daily_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_vehicle_daily_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
