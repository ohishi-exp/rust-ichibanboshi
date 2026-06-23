use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Extension;

use crate::repo::{DynRepo, RepoError};

// build.rs が焼き込む build 識別情報 (commit SHA + build 時刻)。
// どの build がデプロイされているか /health で判別するため (Refs #14)。
const BUILD_INFO: &str = concat!(
    "{\"status\":\"ok\",\"commit\":\"",
    env!("BUILD_SHA"),
    "\",\"built_at\":\"",
    env!("BUILD_TIME"),
    "\"}"
);

/// GET /health — DB 接続確認 + build 情報 (commit / built_at) を返す
pub async fn health(Extension(repo): Extension<DynRepo>) -> Result<impl IntoResponse, StatusCode> {
    repo.health_check().await.map_err(|e| {
        match &e {
            RepoError::PoolError => tracing::error!("DB pool error"),
            RepoError::QueryError(msg) => tracing::error!("DB query error: {msg}"),
        }
        StatusCode::SERVICE_UNAVAILABLE
    })?;
    Ok(([(header::CONTENT_TYPE, "application/json")], BUILD_INFO))
}
