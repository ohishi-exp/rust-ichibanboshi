use axum::http::StatusCode;
use axum::Extension;

use crate::repo::{DynRepo, RepoError};

/// GET /health — DB 接続確認
pub async fn health(Extension(repo): Extension<DynRepo>) -> Result<&'static str, StatusCode> {
    repo.health_check().await.map_err(|e| {
        match &e {
            RepoError::PoolError => tracing::error!("DB pool error"),
            RepoError::QueryError(msg) => tracing::error!("DB query error: {msg}"),
        }
        StatusCode::SERVICE_UNAVAILABLE
    })?;
    Ok("OK")
}
