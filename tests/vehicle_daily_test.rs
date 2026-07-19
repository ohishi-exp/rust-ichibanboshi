mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::vehicle_daily::{build_vehicle_daily_rows, RawVehicleDailyRow};
use tower::ServiceExt;

use common::dt;

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_vehicle_daily_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_vehicle_daily_rows_self_and_subcontract() {
    let raw = vec![
        // 自車 (傭車先C='000000') → self_amount を使う。origin_area_name は #12 実機調査の
        // 例 (神奈川県横浜市、市区町村レベル)。
        RawVehicleDailyRow {
            sale_date: dt(2026, 6, 21),
            vehicle_number: "8504".into(),
            customer_code: "000001".into(),
            customer_name: "㈱田浦畜産".into(),
            origin_area_name: "長崎県".into(),
            dest_area_name: "神奈川県横浜市".into(),
            origin: "釧路".into(),
            dest: "福岡県北九州市".into(),
            subcontractor_code: "000000".into(),
            self_amount: 65_000,
            subcontract_amount: 999_999, // 自車なので使われないはず
            row_id: "20260621-1001".into(),
        },
        // 傭車 (傭車先C!='000000') → subcontract_amount を使う
        RawVehicleDailyRow {
            sale_date: dt(2026, 6, 20),
            vehicle_number: "8504".into(),
            customer_code: "000002".into(),
            customer_name: "".into(),
            origin_area_name: "".into(),
            dest_area_name: "".into(),
            origin: "".into(),
            dest: "".into(),
            subcontractor_code: "001234".into(),
            self_amount: 999_999, // 傭車なので使われないはず
            subcontract_amount: 40_000,
            row_id: "20260620-1002".into(),
        },
    ];

    let rows = build_vehicle_daily_rows(&raw);
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    assert_eq!(first.sale_date, "2026-06-21");
    assert_eq!(first.vehicle_number, "8504");
    assert_eq!(first.customer_code, "000001");
    assert_eq!(first.customer_name, "㈱田浦畜産");
    assert_eq!(first.origin_area_name, "長崎県");
    assert_eq!(first.dest_area_name, "神奈川県横浜市");
    assert_eq!(first.origin, "釧路");
    assert_eq!(first.dest, "福岡県北九州市");
    assert!(!first.is_subcontracted);
    assert_eq!(first.amount, 65_000);
    assert_eq!(first.row_id, "20260621-1001");

    let second = &rows[1];
    assert_eq!(second.sale_date, "2026-06-20");
    assert!(second.is_subcontracted);
    assert_eq!(second.amount, 40_000);
    // 積地・卸地・得意先名は空文字のまま passthrough (surcharge_base 同様に県正規化しない)
    assert_eq!(second.origin_area_name, "");
    assert_eq!(second.dest_area_name, "");
    assert_eq!(second.origin, "");
    assert_eq!(second.dest, "");
    assert_eq!(second.customer_name, "");
}

#[test]
fn test_build_vehicle_daily_rows_empty() {
    assert!(build_vehicle_daily_rows(&[]).is_empty());
}

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
async fn test_vehicle_daily_missing_vehicle_param_is_bad_request() {
    // vehicle が必須パラメータ自体無い場合は axum の Query extractor が 400 を返す
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
async fn test_vehicle_daily_empty_vehicle_is_bad_request() {
    // vehicle パラメータはあるが空文字/空白のみ → ハンドラ側の trim().is_empty() チェック
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=%20")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
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
