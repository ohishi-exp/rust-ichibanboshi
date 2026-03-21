use axum::extract::Query;
use axum::http::StatusCode;
use axum::Json;
use axum::Extension;
use serde::{Deserialize, Serialize};

use crate::db::DbPool;

#[derive(Serialize)]
pub struct TableInfo {
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Serialize)]
pub struct ColumnInfo {
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub max_length: Option<i32>,
}

#[derive(Serialize)]
pub struct SampleRow {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Deserialize)]
pub struct TableQuery {
    pub table: Option<String>,
    pub limit: Option<i32>,
}

/// GET /api/schema/tables — テーブル一覧
pub async fn list_tables(
    Extension(pool): Extension<DbPool>,
) -> Result<Json<Vec<TableInfo>>, StatusCode> {
    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let stream = conn
        .simple_query(
            "SELECT TABLE_SCHEMA, TABLE_NAME \
             FROM INFORMATION_SCHEMA.TABLES \
             WHERE TABLE_TYPE = 'BASE TABLE' \
             ORDER BY TABLE_SCHEMA, TABLE_NAME",
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let tables: Vec<TableInfo> = rows
        .iter()
        .map(|row| TableInfo {
            schema_name: row.get::<&str, _>(0).unwrap_or("").to_string(),
            table_name: row.get::<&str, _>(1).unwrap_or("").to_string(),
        })
        .collect();

    Ok(Json(tables))
}

/// GET /api/schema/columns?table=TABLE_NAME — カラム一覧
pub async fn list_columns(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<TableQuery>,
) -> Result<Json<Vec<ColumnInfo>>, StatusCode> {
    let table = params.table.ok_or(StatusCode::BAD_REQUEST)?;

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let stream = conn
        .query(
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH \
             FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = @P1 \
             ORDER BY ORDINAL_POSITION",
            &[&table.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let rows = stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let columns: Vec<ColumnInfo> = rows
        .iter()
        .map(|row| ColumnInfo {
            column_name: row.get::<&str, _>(0).unwrap_or("").to_string(),
            data_type: row.get::<&str, _>(1).unwrap_or("").to_string(),
            is_nullable: row.get::<&str, _>(2).unwrap_or("").to_string(),
            max_length: row.get::<i32, _>(3),
        })
        .collect();

    Ok(Json(columns))
}

/// GET /api/schema/sample?table=TABLE_NAME&limit=10 — サンプルデータ
pub async fn sample_data(
    Extension(pool): Extension<DbPool>,
    Query(params): Query<TableQuery>,
) -> Result<Json<SampleRow>, StatusCode> {
    let table = params.table.ok_or(StatusCode::BAD_REQUEST)?;
    let limit = params.limit.unwrap_or(10).min(100);

    // テーブル名のバリデーション（SQL injection 防止）
    if !table.chars().all(|c| c.is_alphanumeric() || c == '_' || c == '#') {
        return Err(StatusCode::BAD_REQUEST);
    }

    let mut conn = pool.get().await.map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    // まずカラム名を取得
    let col_stream = conn
        .query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS \
             WHERE TABLE_NAME = @P1 ORDER BY ORDINAL_POSITION",
            &[&table.as_str()],
        )
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let col_rows = col_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let columns: Vec<String> = col_rows
        .iter()
        .map(|row| row.get::<&str, _>(0).unwrap_or("").to_string())
        .collect();

    // サンプルデータ取得
    let query = format!("SELECT TOP {} * FROM [{}]", limit, table);
    let data_stream = conn
        .simple_query(&query)
        .await
        .map_err(|e| {
            tracing::error!("Query error: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let data_rows = data_stream.into_first_result().await.map_err(|e| {
        tracing::error!("Result error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let rows: Vec<Vec<Option<String>>> = data_rows
        .iter()
        .map(|row| {
            (0..columns.len())
                .map(|i| {
                    row.try_get::<&str, _>(i)
                        .ok()
                        .flatten()
                        .map(|s| s.to_string())
                })
                .collect()
        })
        .collect();

    Ok(Json(SampleRow { columns, rows }))
}
