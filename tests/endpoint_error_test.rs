mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

// ── PoolError → 503 SERVICE_UNAVAILABLE ──

#[tokio::test]
async fn test_health_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ── QueryError → 500 INTERNAL_SERVER_ERROR ──

#[tokio::test]
async fn test_health_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE); // health maps both to 503
}

// ── MockRepo: 正常系ハンドラテスト ──

#[tokio::test]
async fn test_health_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
