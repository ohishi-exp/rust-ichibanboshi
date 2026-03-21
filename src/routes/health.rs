use axum::http::StatusCode;
use axum::Extension;

use crate::db::DbPool;

/// GET /health — DB 接続確認
pub async fn health(Extension(pool): Extension<DbPool>) -> Result<&'static str, StatusCode> {
    let mut conn = pool.get().await.map_err(|e| {
        tracing::error!("DB pool error: {e}");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    conn.simple_query("SELECT 1")
        .await
        .map_err(|e| {
            tracing::error!("DB query error: {e}");
            StatusCode::SERVICE_UNAVAILABLE
        })?;

    Ok("OK")
}
