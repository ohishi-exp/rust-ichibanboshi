//! auth-worker introspect ベースの給与ルート認可 (Refs #82)。
//!
//! 給与ルートは CF Access (edge) に頼らず、リクエストごとに
//! auth-worker `POST /auth/introspect` で browser JWT を検証し、応答の `email` を
//! allowlist と照合する。**LAN 内からの直叩きも同じゲートを通る**のが狙い。
//!
//! fail-closed 原則: 設定不備 (introspect 未設定 / allowlist 空) や introspect
//! 不達では一切データを返さない。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use serde::Deserialize;

use super::logic::{email_allowed, normalize_emails};
use crate::config::KyuyoConfig;

/// introspect 呼び出しエラー。
#[derive(Debug)]
pub enum IntrospectError {
    /// HTTP request 失敗 (DNS / 接続 / timeout 等)。
    RequestFailed(String),
    /// HTTP non-2xx (shared secret 不一致の 401 等)。
    StatusError(u16),
    /// レスポンス JSON parse 失敗。
    ParseError(String),
}

impl std::fmt::Display for IntrospectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RequestFailed(m) => write!(f, "introspect request failed: {m}"),
            Self::StatusError(s) => write!(f, "introspect returned status {s}"),
            Self::ParseError(m) => write!(f, "introspect response parse failed: {m}"),
        }
    }
}

/// `POST /auth/introspect` の応答 (RFC 7662 風、auth-worker `auth-introspect.ts`)。
#[derive(Debug, Clone, Deserialize, Default)]
pub struct IntrospectResult {
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub email: String,
    #[serde(default)]
    pub tenant_id: String,
    #[serde(default)]
    pub role: String,
}

/// introspect API 抽象 (テストで mock 差し替え)。
#[async_trait]
pub trait IntrospectApi: Send + Sync {
    async fn introspect(&self, token: &str) -> Result<IntrospectResult, IntrospectError>;
}

/// reqwest 実装。auth-worker の `/auth/introspect` を叩く。
pub struct HttpIntrospect {
    client: reqwest::Client,
    endpoint: String,
    /// `INTERNAL_SHARED_SECRET*` の生値 (Bearer prefix なし、auth-worker 仕様)。
    secret: String,
    /// APP_TENANT_ACL の per-app 判定に使う呼び出しアプリ origin (必須)。
    app_origin: String,
    /// リクエスト単位の timeout (builder 全体設定でなく per-request に付ける —
    /// `Client::new()` を infallible に保つため)。
    timeout: Duration,
}

impl HttpIntrospect {
    pub fn new(
        auth_worker_origin: &str,
        secret: &str,
        app_origin: &str,
        timeout_secs: u64,
    ) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: format!(
                "{}/auth/introspect",
                auth_worker_origin.trim_end_matches('/')
            ),
            secret: secret.to_string(),
            app_origin: app_origin.to_string(),
            timeout: Duration::from_secs(timeout_secs),
        }
    }
}

#[async_trait]
impl IntrospectApi for HttpIntrospect {
    async fn introspect(&self, token: &str) -> Result<IntrospectResult, IntrospectError> {
        let res = self
            .client
            .post(&self.endpoint)
            .timeout(self.timeout)
            .header("Authorization", &self.secret)
            .json(&serde_json::json!({ "token": token, "origin": self.app_origin }))
            .send()
            .await
            .map_err(|e| IntrospectError::RequestFailed(e.to_string()))?;

        let status = res.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(IntrospectError::StatusError(status));
        }
        res.json::<IntrospectResult>()
            .await
            .map_err(|e| IntrospectError::ParseError(e.to_string()))
    }
}

/// 給与ルートの認可状態。`api == None` または allowlist 空 = 未設定 (全拒否)。
pub struct KyuyoAuthState {
    pub api: Option<Arc<dyn IntrospectApi>>,
    pub allowed_emails: Vec<String>,
}

impl KyuyoAuthState {
    /// config から構築。introspect 設定が欠けていれば `api` は None (fail-closed)。
    pub fn from_config(config: &KyuyoConfig) -> Self {
        let api: Option<Arc<dyn IntrospectApi>> = if config.auth_configured() {
            Some(Arc::new(HttpIntrospect::new(
                &config.auth_worker_origin,
                &config.introspect_secret,
                &config.app_origin,
                config.timeout_secs,
            )))
        } else {
            None
        };
        Self {
            api,
            allowed_emails: normalize_emails(&config.allowed_emails),
        }
    }

    /// テスト用コンストラクタ。
    pub fn new(api: Option<Arc<dyn IntrospectApi>>, allowed_emails: &[String]) -> Self {
        Self {
            api,
            allowed_emails: normalize_emails(allowed_emails),
        }
    }
}

/// `Authorization: Bearer <JWT>` を検証し、許可された email を返す。
///
/// - 未設定 (introspect 無し / allowlist 空) → 503 (fail-closed)
/// - token 無し / introspect `active:false` → 401
/// - introspect 不達・応答異常 → 503 (認可判断ができないので拒否)
/// - allowlist 外 email → 403
pub async fn authorize(
    headers: &HeaderMap,
    state: &KyuyoAuthState,
) -> Result<String, (StatusCode, String)> {
    let Some(api) = state.api.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "kyuyo 認可が未設定です (introspect 設定を確認してください)".to_string(),
        ));
    };
    if state.allowed_emails.is_empty() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "kyuyo 認可が未設定です (allowed_emails が空です)".to_string(),
        ));
    }

    let token = headers
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .filter(|t| !t.is_empty());
    let Some(token) = token else {
        return Err((
            StatusCode::UNAUTHORIZED,
            "Authorization: Bearer <JWT> が必要です".to_string(),
        ));
    };

    let result = match api.introspect(token).await {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!("kyuyo introspect error: {e}");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                "認可サービスに到達できません".to_string(),
            ));
        }
    };

    if !result.active {
        return Err((StatusCode::UNAUTHORIZED, "token が無効です".to_string()));
    }
    if !email_allowed(&state.allowed_emails, &result.email) {
        tracing::warn!("kyuyo access denied for email: {}", result.email);
        return Err((
            StatusCode::FORBIDDEN,
            "このユーザーには給与データへのアクセス権がありません".to_string(),
        ));
    }
    Ok(result.email)
}
