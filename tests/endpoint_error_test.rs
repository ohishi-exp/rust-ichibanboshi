mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

// ── PoolError → 503 SERVICE_UNAVAILABLE ──

#[tokio::test]
async fn test_health_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_schema_tables_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/tables").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_schema_columns_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/columns?table=test").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_schema_sample_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/sample?table=valid_table").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_monthly_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/monthly").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_by_department_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/by-department").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_by_customer_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/by-customer").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_yoy_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/yoy").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_daily_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/daily").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_customer_trend_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-trend").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_customer_yoy_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-yoy").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_customer_detail_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-detail?code=000001").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

// ── QueryError → 500 INTERNAL_SERVER_ERROR ──

#[tokio::test]
async fn test_health_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE); // health maps both to 503
}

#[tokio::test]
async fn test_schema_tables_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/tables").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_sales_monthly_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/monthly").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ── Param validation (no DB needed) ──

#[tokio::test]
async fn test_schema_columns_missing_param() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/columns").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_schema_sample_missing_param() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/sample").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_schema_sample_invalid_table_name() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/sample?table=foo;DROP%20TABLE").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ── MockRepo: 正常系ハンドラテスト ──

#[tokio::test]
async fn test_health_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/health").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_schema_tables_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/tables").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_schema_columns_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/columns?table=test").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_schema_sample_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/schema/sample?table=test_table&limit=5").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_monthly_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/monthly?from=2025-04&to=2026-03").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_by_department_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/by-department").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_by_customer_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/by-customer?limit=10").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_yoy_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/yoy?year=2026").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_daily_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/daily?month=2025-04&mode=billing").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_daily_with_exclude_dept() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/daily?month=2025-04&exclude_dept=宮崎").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_customer_trend_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-trend").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_customer_yoy_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-yoy?from=2025-04&to=2026-03").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_customer_detail_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-detail?code=000001").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

// ── customer-yoy-by-dept / departments ──

#[tokio::test]
async fn test_sales_customer_yoy_by_dept_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-yoy-by-dept?from=2025-04&to=2026-03").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_customer_yoy_by_dept_with_department_code_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-yoy-by-dept?from=2025-04&to=2026-03&department_code=01").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_customer_yoy_by_dept_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-yoy-by-dept").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_sales_customer_yoy_by_dept_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/customer-yoy-by-dept").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_sales_departments_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/departments").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_sales_departments_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app.oneshot(Request::builder().uri("/api/sales/departments").body(Body::empty()).unwrap()).await.unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}
