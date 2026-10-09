mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/costs/vehicle-daily
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_costs_daily_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    assert_eq!(json["source_table"], "経費明細 + 経費ﾏｽﾀ + 経費種別ﾏｽﾀ");
    // 車輌 8504 は 燃料 / 通行料 / 固定経費 の 3 行 (mock フィクスチャ)
    assert_eq!(json["data"].as_array().unwrap().len(), 3);
}

/// 消費側 (運行手当タブ) が種別で内訳を分けるため、**燃料と通行料が種別付きで
/// 揃って返る**こと。片方でも落ちると粗利の内訳が出せない。
#[tokio::test]
async fn test_costs_daily_returns_fuel_and_toll_kinds() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let kinds: Vec<&str> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["cost_kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"01")); // 燃料
    assert!(kinds.contains(&"04")); // 通行料
                                    // 固定経費は is_fixed=true で 1 行だけ (按分の判断材料)
    let fixed: Vec<&serde_json::Value> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["is_fixed"].as_bool().unwrap())
        .collect();
    assert_eq!(fixed.len(), 1);
    assert_eq!(fixed[0]["cost_kind"], "09");
}

/// #760-11: JSON に 備考 / 未払先 / 入力日 が載り、既存キーも揃ったまま (追加のみ)。
#[tokio::test]
async fn test_costs_daily_json_has_remarks_vendor_entered_date() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let data = json["data"].as_array().unwrap();
    let fuel = data
        .iter()
        .find(|r| r["row_id"] == "20260621-2001")
        .unwrap();
    // 既存キーは全て残る (消費側の互換)
    for key in [
        "operation_date",
        "vehicle_number",
        "vehicle_branch",
        "driver_code",
        "cost_code",
        "cost_name",
        "cost_kind",
        "cost_kind_name",
        "quantity",
        "unit_price",
        "amount",
        "diesel_tax",
        "km",
        "is_fixed",
        "row_id",
    ] {
        assert!(fuel.get(key).is_some(), "missing existing key {key}");
    }
    assert_eq!(fuel["amount"], 19_339);
    // 追加キー
    assert_eq!(fuel["remarks"], "");
    assert_eq!(fuel["vendor_code"], "001234");
    assert_eq!(fuel["vendor_branch"], "00");
    assert_eq!(fuel["vendor_name"], "○○石油");
    assert_eq!(fuel["entered_date"], "2026-06-23");

    // 備考あり・未払先名 無し (マスタ未登録) の行
    let toll = data
        .iter()
        .find(|r| r["row_id"] == "20260620-2002")
        .unwrap();
    assert_eq!(toll["remarks"], "ETC");
    assert_eq!(toll["vendor_name"], "");
    // 固定経費は 未払先も入力日も無い → 全部空文字 (null ではない)
    let fixed = data
        .iter()
        .find(|r| r["row_id"] == "20260601-2003")
        .unwrap();
    assert_eq!(fixed["vendor_code"], "");
    assert_eq!(fixed["entered_date"], "");
    assert!(fixed["entered_date"].is_string());
}

#[tokio::test]
async fn test_costs_daily_kind_filter() {
    // kind だけの絞り込み (vehicle/driver 無し) でも 200。燃料は 2 車輌にまたがる
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&kind=01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let data = json["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
}

#[tokio::test]
async fn test_costs_daily_driver_only_searches_across_vehicles() {
    // 同じ乗務員の経費が日によって別の車番に載る (#741 と同じ形) ため、
    // **車番ではなく乗務員CD で横断して引けること**
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&driver=1656")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let data = json["data"].as_array().unwrap();
    // 乗務員 1656 は 燃料(8504) / 通行料(8504) / 燃料(9012) の 3 行。
    // 固定経費は乗務員が紐付かない (driver_code="") ので落ちる
    assert_eq!(data.len(), 3);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
}

#[tokio::test]
async fn test_costs_daily_ok_with_limit() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_costs_daily_no_filters_is_bad_request() {
    // vehicle/driver/kind が 1 つも無い → 全件スキャン防止のチェックで 400
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_costs_daily_blank_filters_is_bad_request() {
    // 全パラメータはあるが空文字/空白のみ → trim().is_empty() で無視され結局 0 件、400
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri(
                    "/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01\
                     &vehicle=%20&driver=&kind=",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_costs_daily_driver_only_is_ok() {
    // 必須チェックの 2 つ目の枝: vehicle が無くても driver があれば通る
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&driver=1656")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_costs_daily_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_costs_daily_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

async fn body_json(res: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}
