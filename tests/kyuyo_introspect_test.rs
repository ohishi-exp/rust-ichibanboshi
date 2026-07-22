//! kyuyo::introspect (auth-worker introspect + allowlist 認可) のテスト (Refs #82)。

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use rust_ichibanboshi::config::KyuyoConfig;
use rust_ichibanboshi::kyuyo::introspect::{
    authorize, HttpIntrospect, IntrospectApi, IntrospectError, IntrospectResult, KyuyoAuthState,
};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn bearer(token: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        "Authorization",
        HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
    );
    h
}

fn allowed() -> Vec<String> {
    vec!["keiri@example.com".to_string()]
}

/// 固定応答を返す mock introspect。
struct StubIntrospect(Result<IntrospectResult, ()>);

#[async_trait]
impl IntrospectApi for StubIntrospect {
    async fn introspect(&self, _token: &str) -> Result<IntrospectResult, IntrospectError> {
        match &self.0 {
            Ok(r) => Ok(r.clone()),
            Err(()) => Err(IntrospectError::RequestFailed("stub".to_string())),
        }
    }
}

fn active_user(email: &str) -> IntrospectResult {
    IntrospectResult {
        active: true,
        email: email.to_string(),
        tenant_id: "t".to_string(),
        role: "admin".to_string(),
    }
}

// ══════════════════════════════════════════════════════════════
// authorize の分岐
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_authorize_ok() {
    let state = KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(Ok(active_user(
            "Keiri@Example.com",
        ))))),
        &allowed(),
    );
    let email = authorize(&bearer("tok"), &state).await.unwrap();
    assert_eq!(email, "Keiri@Example.com");
}

#[tokio::test]
async fn test_authorize_not_configured_api() {
    // introspect 未設定 → 503 (fail-closed)
    let state = KyuyoAuthState::new(None, &allowed());
    let (status, msg) = authorize(&bearer("tok"), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(msg.contains("introspect"));
}

#[tokio::test]
async fn test_authorize_empty_allowlist() {
    // allowlist 空 → 503 (fail-closed)
    let state = KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(Ok(active_user("a@example.com"))))),
        &[],
    );
    let (status, msg) = authorize(&bearer("tok"), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(msg.contains("allowed_emails"));
}

#[tokio::test]
async fn test_authorize_missing_or_malformed_token() {
    let state = KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(Ok(active_user("a@example.com"))))),
        &allowed(),
    );
    // ヘッダ無し
    let (status, _) = authorize(&HeaderMap::new(), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Bearer prefix 無し
    let mut h = HeaderMap::new();
    h.insert("Authorization", HeaderValue::from_static("raw-token"));
    let (status, _) = authorize(&h, &state).await.unwrap_err();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    // Bearer だけで token 空
    let (status, _) = authorize(&bearer(""), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_authorize_introspect_unreachable() {
    // introspect 不達 → 503 (認可判断ができないので拒否)
    let state = KyuyoAuthState::new(Some(Arc::new(StubIntrospect(Err(())))), &allowed());
    let (status, msg) = authorize(&bearer("tok"), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(msg.contains("到達"));
}

#[tokio::test]
async fn test_authorize_inactive_token() {
    let state = KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(Ok(IntrospectResult::default())))),
        &allowed(),
    );
    let (status, _) = authorize(&bearer("tok"), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_authorize_email_not_allowed() {
    let state = KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(Ok(active_user(
            "other@example.com",
        ))))),
        &allowed(),
    );
    let (status, msg) = authorize(&bearer("tok"), &state).await.unwrap_err();
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(msg.contains("アクセス権"));
}

// ══════════════════════════════════════════════════════════════
// HttpIntrospect (wiremock)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_http_introspect_ok() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/introspect"))
        // auth-worker 仕様: shared secret は生値 (Bearer prefix なし)
        .and(header("Authorization", "shared-secret"))
        .and(body_partial_json(serde_json::json!({
            "token": "tok",
            "origin": "https://dtako.ippoan.org"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "active": true,
            "tenant_id": "tenant-1",
            "role": "admin",
            "email": "keiri@example.com",
            "sub": "u-1",
            "exp": 9999999999i64
        })))
        .mount(&server)
        .await;

    // origin 末尾スラッシュは除去される
    let api = HttpIntrospect::new(
        &format!("{}/", server.uri()),
        "shared-secret",
        "https://dtako.ippoan.org",
        5,
    );
    let result = api.introspect("tok").await.unwrap();
    assert!(result.active);
    assert_eq!(result.email, "keiri@example.com");
    assert_eq!(result.tenant_id, "tenant-1");
    assert_eq!(result.role, "admin");
}

#[tokio::test]
async fn test_http_introspect_inactive_and_missing_fields() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/introspect"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "active": false
        })))
        .mount(&server)
        .await;

    let api = HttpIntrospect::new(&server.uri(), "s", "https://dtako.ippoan.org", 5);
    let result = api.introspect("tok").await.unwrap();
    assert!(!result.active);
    assert!(result.email.is_empty());
}

#[tokio::test]
async fn test_http_introspect_non_2xx() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/introspect"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&server)
        .await;

    let api = HttpIntrospect::new(&server.uri(), "wrong", "https://dtako.ippoan.org", 5);
    match api.introspect("tok").await.unwrap_err() {
        IntrospectError::StatusError(401) => {}
        other => panic!("unexpected: {other}"),
    }
}

#[tokio::test]
async fn test_http_introspect_bad_json() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/auth/introspect"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
        .mount(&server)
        .await;

    let api = HttpIntrospect::new(&server.uri(), "s", "https://dtako.ippoan.org", 5);
    match api.introspect("tok").await.unwrap_err() {
        IntrospectError::ParseError(_) => {}
        other => panic!("unexpected: {other}"),
    }
}

#[tokio::test]
async fn test_http_introspect_unreachable() {
    // 存在しないポートへ → RequestFailed
    let api = HttpIntrospect::new("http://127.0.0.1:1", "s", "https://dtako.ippoan.org", 1);
    match api.introspect("tok").await.unwrap_err() {
        IntrospectError::RequestFailed(_) => {}
        other => panic!("unexpected: {other}"),
    }
}

// ══════════════════════════════════════════════════════════════
// KyuyoAuthState::from_config / IntrospectError Display
// ══════════════════════════════════════════════════════════════

#[test]
fn test_from_config_disabled_and_enabled() {
    // 未設定 → api None (fail-closed)
    let config = KyuyoConfig::default();
    let state = KyuyoAuthState::from_config(&config);
    assert!(state.api.is_none());
    assert!(state.allowed_emails.is_empty());

    // introspect 設定が揃えば api Some + allowlist 正規化
    let config: KyuyoConfig = toml::from_str(
        r#"
auth_worker_origin = "https://auth.example.com"
introspect_secret = "sec"
allowed_emails = [" Keiri@Example.com "]
"#,
    )
    .unwrap();
    let state = KyuyoAuthState::from_config(&config);
    assert!(state.api.is_some());
    assert_eq!(state.allowed_emails, vec!["keiri@example.com"]);
}

#[test]
fn test_introspect_error_display() {
    assert_eq!(
        IntrospectError::RequestFailed("x".to_string()).to_string(),
        "introspect request failed: x"
    );
    assert_eq!(
        IntrospectError::StatusError(500).to_string(),
        "introspect returned status 500"
    );
    assert_eq!(
        IntrospectError::ParseError("y".to_string()).to_string(),
        "introspect response parse failed: y"
    );
}
