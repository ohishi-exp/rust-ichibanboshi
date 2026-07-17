//! 社員ﾏｽﾀ一覧エンドポイント (Refs ohishi-exp/rust-ichibanboshi#74)
//!
//! nuxt-trouble (トラブル管理) の担当者マスタを一番星 [社員ﾏｽﾀ] から手動同期する
//! ための read-only エンドポイント。経路は
//! nuxt-trouble → (service binding) → nuxt-ichibanboshi `/api/employees`
//! → CF Tunnel (Service Token) → 本 API。
//!
//! 名前の突合・表示には `社員R` を使う想定 (uriage の担当者表示 fallback と同じ)。
//! `社員N` は参考情報として併せて返す。

use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Serialize;

use crate::repo::{DynRepo, RepoError};
use crate::routes::sales::ApiResponse;

fn map_repo_err(e: RepoError) -> StatusCode {
    match &e {
        RepoError::PoolError => StatusCode::SERVICE_UNAVAILABLE,
        RepoError::QueryError(msg) => {
            tracing::error!("Query error: {msg}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// 社員ﾏｽﾀ 1 件。
#[derive(Serialize, Debug, PartialEq)]
pub struct EmployeeRow {
    /// 社員C (コード)。数値型でも varchar に寄せた文字列で返す。
    pub employee_code: String,
    /// 社員N (氏名)。
    pub employee_name: String,
    /// 社員R (表示名)。nuxt-trouble 側の担当者名はこれを使う。
    pub employee_r: String,
}

/// GET /api/employees — 社員ﾏｽﾀ (社員C, 社員N, 社員R) の一覧。
pub async fn employees(
    Extension(repo): Extension<DynRepo>,
) -> Result<Json<ApiResponse<Vec<EmployeeRow>>>, StatusCode> {
    let rows = repo.employees().await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse {
        source_table: "社員ﾏｽﾀ".to_string(),
        data: rows
            .into_iter()
            .map(|(code, name, r)| EmployeeRow {
                employee_code: code,
                employee_name: name,
                employee_r: r,
            })
            .collect(),
    }))
}
