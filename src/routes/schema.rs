use axum::extract::Query;
use axum::http::StatusCode;
use axum::Json;
use axum::Extension;
use serde::{Deserialize, Serialize};

use crate::repo::{DynRepo, RepoError};

/// テーブル名のバリデーション（SQL injection 防止）
pub fn is_valid_table_name(name: &str) -> bool {
    !name.is_empty() && name.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '#')
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct TableInfo {
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct ColumnInfo {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub max_length: Option<i32>,
}

#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SampleRow {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Deserialize)]
pub struct TableQuery {
    pub table: Option<String>,
    pub limit: Option<i32>,
}

fn map_repo_err(e: RepoError) -> StatusCode {
    match &e {
        RepoError::PoolError => StatusCode::SERVICE_UNAVAILABLE,
        RepoError::QueryError(msg) => {
            tracing::error!("Query error: {msg}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// GET /api/schema/tables
pub async fn list_tables(
    Extension(repo): Extension<DynRepo>,
) -> Result<Json<Vec<TableInfo>>, StatusCode> {
    let tables = repo.list_tables().await.map_err(map_repo_err)?;
    Ok(Json(tables))
}

/// GET /api/schema/columns?table=TABLE_NAME
pub async fn list_columns(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<TableQuery>,
) -> Result<Json<Vec<ColumnInfo>>, StatusCode> {
    let table = params.table.ok_or(StatusCode::BAD_REQUEST)?;
    let columns = repo.list_columns(&table).await.map_err(map_repo_err)?;
    Ok(Json(columns))
}

/// GET /api/schema/sample?table=TABLE_NAME&limit=10
pub async fn sample_data(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<TableQuery>,
) -> Result<Json<SampleRow>, StatusCode> {
    let table = params.table.ok_or(StatusCode::BAD_REQUEST)?;
    let limit = params.limit.unwrap_or(10).min(100);

    if !is_valid_table_name(&table) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let data = repo.sample_data(&table, limit).await.map_err(map_repo_err)?;
    Ok(Json(data))
}
