#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::routing::get;
use axum::{Extension, Router};
use chrono::{NaiveDate, Utc};
use jsonwebtoken::{encode, EncodingKey, Header};
use rust_ichibanboshi::auth::{AppClaims, JwtSecret};
use rust_ichibanboshi::repo::{AppRepo, DynRepo, RepoError};
use rust_ichibanboshi::routes;
use rust_ichibanboshi::routes::sales::*;
use rust_ichibanboshi::routes::schema::{ColumnInfo, SampleRow, TableInfo};
use uuid::Uuid;

pub const TEST_JWT_SECRET: &str = "test-jwt-secret-ichibanboshi";

// ── MockRepo: テスト用 ──

pub struct MockRepo;

#[async_trait]
impl AppRepo for MockRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Ok(())
    }

    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> {
        Ok(vec![
            TableInfo { schema_name: "dbo".into(), table_name: "種別別月計".into() },
        ])
    }

    async fn list_columns(&self, _table: &str) -> Result<Vec<ColumnInfo>, RepoError> {
        Ok(vec![
            ColumnInfo { column_name: "年月度".into(), data_type: "datetime".into(), is_nullable: "NO".into(), max_length: None },
        ])
    }

    async fn sample_data(&self, _table: &str, _limit: i32) -> Result<SampleRow, RepoError> {
        Ok(SampleRow {
            columns: vec!["col1".into(), "col2".into()],
            rows: vec![vec![Some("val1".into()), Some("val2".into())]],
        })
    }

    async fn monthly(&self, _from: &str, _to: &str, _prev_from: &str, _prev_to: &str, _exclude: Option<&str>) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> {
        Ok(("種別別月計 (種別C=99)".into(), vec![
            RawMonthlyRow { year_month: dt(2025, 4, 1), own_sales: 1_000_000, charter_sales: 500_000, transport_count: 50 },
        ], vec![
            RawMonthlyRow { year_month: dt(2024, 4, 1), own_sales: 900_000, charter_sales: 400_000, transport_count: 0 },
        ]))
    }

    async fn by_department(&self, _from: &str, _to: &str) -> Result<Vec<RawDepartmentRow>, RepoError> {
        Ok(vec![RawDepartmentRow { department_code: "01".into(), department_name: "本社".into(), own_sales: 500, charter_sales: 200, transport_count: 10 }])
    }

    async fn by_customer(&self, _from: &str, _to: &str, _limit: i32) -> Result<Vec<RawCustomerRow>, RepoError> {
        Ok(vec![RawCustomerRow { customer_code: "001".into(), customer_name: "得意先A".into(), own_sales: 1000, charter_sales: 500, transport_count: 20 }])
    }

    async fn customer_yoy_data(&self, _from: &str, _to: &str, _prev_from: &str, _prev_to: &str) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> {
        let mut cur = std::collections::HashMap::new();
        cur.insert("A".into(), ("顧客A".into(), 1_200_000i64));
        let mut prev = std::collections::HashMap::new();
        prev.insert("A".into(), ("顧客A".into(), 1_000_000i64));
        Ok((cur, prev))
    }

    async fn yoy_data(&self, _year: i32) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> {
        Ok((
            vec![RawMonthTotalRow { month: 1, total: 1_000_000 }],
            vec![RawMonthTotalRow { month: 1, total: 900_000 }],
        ))
    }

    async fn daily(&self, _from: &str, _to: &str, _prev_from: &str, _prev_to: &str, _bf: &str, _df: &str, _ep: &str) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> {
        Ok((
            vec![RawDailyRow { date: dt(2025, 4, 1), own_sales: 100, charter_sales: 50, own_sales_raw: 110, charter_sales_raw: 55, transport_count: 10 }],
            vec![],
        ))
    }

    async fn customer_trend_data(&self, _from: &str, _to: &str, _limit: i32) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> {
        Ok((
            vec![("A".into(), "顧客A".into())],
            vec![RawCustomerMonthlyRow { customer_code: "A".into(), year_month: dt(2025, 4, 1), total: 1000 }],
        ))
    }

    async fn customer_detail_data(&self, _code: &str) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> {
        Ok(("得意先A".into(), vec![
            RawCustomerDetailRow { year_month: dt(2025, 4, 1), own_sales: 100, charter_sales: 50, transport_count: 10 },
        ]))
    }
}

// ── ErrorRepo: 全メソッドがエラーを返す ──

pub struct ErrorRepo;

#[async_trait]
impl AppRepo for ErrorRepo {
    async fn health_check(&self) -> Result<(), RepoError> { Err(RepoError::PoolError) }
    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> { Err(RepoError::PoolError) }
    async fn list_columns(&self, _: &str) -> Result<Vec<ColumnInfo>, RepoError> { Err(RepoError::PoolError) }
    async fn sample_data(&self, _: &str, _: i32) -> Result<SampleRow, RepoError> { Err(RepoError::PoolError) }
    async fn monthly(&self, _: &str, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> { Err(RepoError::PoolError) }
    async fn by_department(&self, _: &str, _: &str) -> Result<Vec<RawDepartmentRow>, RepoError> { Err(RepoError::PoolError) }
    async fn by_customer(&self, _: &str, _: &str, _: i32) -> Result<Vec<RawCustomerRow>, RepoError> { Err(RepoError::PoolError) }
    async fn customer_yoy_data(&self, _: &str, _: &str, _: &str, _: &str) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> { Err(RepoError::PoolError) }
    async fn yoy_data(&self, _: i32) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> { Err(RepoError::PoolError) }
    async fn daily(&self, _: &str, _: &str, _: &str, _: &str, _: &str, _: &str, _: &str) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> { Err(RepoError::PoolError) }
    async fn customer_trend_data(&self, _: &str, _: &str, _: i32) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> { Err(RepoError::PoolError) }
    async fn customer_detail_data(&self, _: &str) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> { Err(RepoError::PoolError) }
}

// ── QueryErrorRepo: QueryError を返す ──

pub struct QueryErrorRepo;

#[async_trait]
impl AppRepo for QueryErrorRepo {
    async fn health_check(&self) -> Result<(), RepoError> { Err(RepoError::QueryError("test query error".into())) }
    async fn list_tables(&self) -> Result<Vec<TableInfo>, RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn list_columns(&self, _: &str) -> Result<Vec<ColumnInfo>, RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn sample_data(&self, _: &str, _: i32) -> Result<SampleRow, RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn monthly(&self, _: &str, _: &str, _: &str, _: &str, _: Option<&str>) -> Result<(String, Vec<RawMonthlyRow>, Vec<RawMonthlyRow>), RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn by_department(&self, _: &str, _: &str) -> Result<Vec<RawDepartmentRow>, RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn by_customer(&self, _: &str, _: &str, _: i32) -> Result<Vec<RawCustomerRow>, RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn customer_yoy_data(&self, _: &str, _: &str, _: &str, _: &str) -> Result<(CodeTotalMap, CodeTotalMap), RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn yoy_data(&self, _: i32) -> Result<(Vec<RawMonthTotalRow>, Vec<RawMonthTotalRow>), RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn daily(&self, _: &str, _: &str, _: &str, _: &str, _: &str, _: &str, _: &str) -> Result<(Vec<RawDailyRow>, Vec<RawDailyPrevRow>), RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn customer_trend_data(&self, _: &str, _: &str, _: i32) -> Result<(Vec<(String, String)>, Vec<RawCustomerMonthlyRow>), RepoError> { Err(RepoError::QueryError("test".into())) }
    async fn customer_detail_data(&self, _: &str) -> Result<(String, Vec<RawCustomerDetailRow>), RepoError> { Err(RepoError::QueryError("test".into())) }
}

// ── ヘルパー ──

pub fn dt(y: i32, m: u32, d: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d).unwrap().and_hms_opt(0, 0, 0).unwrap()
}

pub fn build_app(repo: DynRepo) -> Router {
    let jwt_secret = JwtSecret(TEST_JWT_SECRET.to_string());
    let api_routes = Router::new()
        .route("/sales/monthly", get(routes::sales::monthly))
        .route("/sales/by-department", get(routes::sales::by_department))
        .route("/sales/by-customer", get(routes::sales::by_customer))
        .route("/sales/yoy", get(routes::sales::yoy))
        .route("/sales/daily", get(routes::sales::daily))
        .route("/sales/customer-trend", get(routes::sales::customer_trend))
        .route("/sales/customer-yoy", get(routes::sales::customer_yoy))
        .route("/sales/customer-detail", get(routes::sales::customer_detail));
    let schema_routes = Router::new()
        .route("/schema/tables", get(routes::schema::list_tables))
        .route("/schema/columns", get(routes::schema::list_columns))
        .route("/schema/sample", get(routes::schema::sample_data));
    Router::new()
        .route("/health", get(routes::health::health))
        .nest("/api", api_routes)
        .nest("/api", schema_routes)
        .layer(Extension(repo))
        .layer(Extension(jwt_secret))
}

pub fn mock_repo() -> DynRepo { Arc::new(MockRepo) }
pub fn error_repo() -> DynRepo { Arc::new(ErrorRepo) }
pub fn query_error_repo() -> DynRepo { Arc::new(QueryErrorRepo) }

pub fn create_test_jwt(tenant_id: Uuid, role: &str) -> String {
    let claims = AppClaims {
        sub: Uuid::new_v4(), email: "test@example.com".into(), name: "Test User".into(),
        tenant_id, role: role.into(), org_slug: None,
        iat: Utc::now().timestamp(), exp: Utc::now().timestamp() + 3600,
    };
    encode(&Header::default(), &claims, &EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes())).unwrap()
}
