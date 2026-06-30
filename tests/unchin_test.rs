mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::unchin::{build_unchin_rows, normalize_partner_type, RawUnchinRow};
use tower::ServiceExt;

use common::dt;

// ══════════════════════════════════════════════════════════════
// 純粋関数: normalize_partner_type
// ══════════════════════════════════════════════════════════════

#[test]
fn test_normalize_partner_type_subcontractor() {
    assert_eq!(normalize_partner_type("subcontractor"), "subcontractor");
}

#[test]
fn test_normalize_partner_type_customer_and_fallback() {
    assert_eq!(normalize_partner_type("customer"), "customer");
    // 未知の値・空文字は customer にフォールバック
    assert_eq!(normalize_partner_type(""), "customer");
    assert_eq!(normalize_partner_type("xxx"), "customer");
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_unchin_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_rows_normal_and_edges() {
    let raw = vec![
        RawUnchinRow {
            partner_code: "034760-015".into(),
            partner_name: "全農物流㈱　九州支店".into(),
            item_code: "6301".into(),
            item_name: "フレコン".into(),
            fare: 30_000,
            origin: "釧路".into(),
            dest: "八代".into(),
            sale_date: dt(2026, 6, 20),
        },
        // エッジ: 空品名コード・空積地
        RawUnchinRow {
            partner_code: "034760-015".into(),
            partner_name: "全農物流㈱　九州支店".into(),
            item_code: "0000".into(),
            item_name: "".into(),
            fare: 140_000,
            origin: "".into(),
            dest: "福岡県北九州市".into(),
            sale_date: dt(2026, 6, 19),
        },
    ];

    let rows = build_unchin_rows(&raw);
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    assert_eq!(first.partner_code, "034760-015");
    assert_eq!(first.partner_name, "全農物流㈱　九州支店");
    assert_eq!(first.item_code, "6301");
    assert_eq!(first.item_name, "フレコン");
    assert_eq!(first.fare, 30_000);
    assert_eq!(first.origin, "釧路");
    assert_eq!(first.dest, "八代");
    assert_eq!(first.sale_date, "2026-06-20");

    let second = &rows[1];
    assert_eq!(second.item_code, "0000");
    assert_eq!(second.item_name, "");
    assert_eq!(second.origin, "");
    assert_eq!(second.dest, "福岡県北九州市");
}

#[test]
fn test_build_unchin_rows_empty() {
    assert!(build_unchin_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/candidates
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_candidates_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_ok_customer_with_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates?from=2026-04-01&to=2026-07-01&partner_type=customer&limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_ok_subcontractor() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates?partner_type=subcontractor")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_ok_unknown_partner_type_falls_back() {
    // partner_type が不正な値 → customer にフォールバックし 200 を返す
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates?partner_type=xxx&limit=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_candidates_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
