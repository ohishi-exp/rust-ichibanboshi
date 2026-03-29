mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware;
use axum::routing::get;
use axum::{Extension, Router};
use chrono::Utc;
use jsonwebtoken::{encode, EncodingKey, Header};
use rust_ichibanboshi::auth::{require_jwt, verify_access_token, AppClaims, JwtSecret};
use tower::ServiceExt;
use uuid::Uuid;

const TEST_SECRET: &str = common::TEST_JWT_SECRET;

// ══════════════════════════════════════════════════════════════
// verify_access_token
// ══════════════════════════════════════════════════════════════

#[test]
fn test_verify_valid_token() {
    let secret = JwtSecret(TEST_SECRET.to_string());
    let token = common::create_test_jwt(Uuid::new_v4(), "admin");
    let claims = verify_access_token(&token, &secret).unwrap();
    assert_eq!(claims.email, "test@example.com");
    assert_eq!(claims.name, "Test User");
    assert_eq!(claims.role, "admin");
}

#[test]
fn test_verify_token_with_org_slug() {
    let secret = JwtSecret(TEST_SECRET.to_string());
    let claims = AppClaims {
        sub: Uuid::new_v4(),
        email: "user@org.com".to_string(),
        name: "Org User".to_string(),
        tenant_id: Uuid::new_v4(),
        role: "member".to_string(),
        org_slug: Some("my-org".to_string()),
        iat: Utc::now().timestamp(),
        exp: Utc::now().timestamp() + 3600,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap();
    let result = verify_access_token(&token, &secret).unwrap();
    assert_eq!(result.org_slug, Some("my-org".to_string()));
}

#[test]
fn test_verify_wrong_secret() {
    let secret = JwtSecret("wrong-secret".to_string());
    let token = common::create_test_jwt(Uuid::new_v4(), "admin");
    assert!(verify_access_token(&token, &secret).is_err());
}

#[test]
fn test_verify_expired_token() {
    let secret = JwtSecret(TEST_SECRET.to_string());
    let claims = AppClaims {
        sub: Uuid::new_v4(),
        email: "expired@example.com".to_string(),
        name: "Expired".to_string(),
        tenant_id: Uuid::new_v4(),
        role: "admin".to_string(),
        org_slug: None,
        iat: Utc::now().timestamp() - 7200,
        exp: Utc::now().timestamp() - 3600,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap();
    assert!(verify_access_token(&token, &secret).is_err());
}

#[test]
fn test_verify_malformed_token() {
    let secret = JwtSecret(TEST_SECRET.to_string());
    assert!(verify_access_token("not-a-jwt", &secret).is_err());
}

#[test]
fn test_verify_empty_token() {
    let secret = JwtSecret(TEST_SECRET.to_string());
    assert!(verify_access_token("", &secret).is_err());
}

#[test]
fn test_verify_truncated_token() {
    let secret = JwtSecret(TEST_SECRET.to_string());
    let token = common::create_test_jwt(Uuid::new_v4(), "admin");
    let truncated = &token[..token.len() / 2];
    assert!(verify_access_token(truncated, &secret).is_err());
}

// ══════════════════════════════════════════════════════════════
// require_jwt middleware (via oneshot)
// ══════════════════════════════════════════════════════════════

fn build_auth_test_app() -> Router {
    Router::new()
        .route("/test", get(|| async { "ok" }))
        .layer(middleware::from_fn(require_jwt))
        .layer(Extension(JwtSecret(TEST_SECRET.to_string())))
}

#[tokio::test]
async fn test_require_jwt_no_header() {
    let app = build_auth_test_app();
    let req = Request::builder()
        .uri("/test")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_require_jwt_invalid_token() {
    let app = build_auth_test_app();
    let req = Request::builder()
        .uri("/test")
        .header("Authorization", "Bearer invalid-token")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_require_jwt_not_bearer() {
    let app = build_auth_test_app();
    let req = Request::builder()
        .uri("/test")
        .header("Authorization", "Basic dXNlcjpwYXNz")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_require_jwt_no_space_after_bearer() {
    let app = build_auth_test_app();
    let req = Request::builder()
        .uri("/test")
        .header("Authorization", "Bearertoken")
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_require_jwt_valid_token() {
    let app = build_auth_test_app();
    let token = common::create_test_jwt(Uuid::new_v4(), "admin");
    let req = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_require_jwt_expired_token() {
    let app = build_auth_test_app();
    let claims = AppClaims {
        sub: Uuid::new_v4(),
        email: "test@example.com".to_string(),
        name: "Test".to_string(),
        tenant_id: Uuid::new_v4(),
        role: "admin".to_string(),
        org_slug: None,
        iat: Utc::now().timestamp() - 7200,
        exp: Utc::now().timestamp() - 3600,
    };
    let token = encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
    )
    .unwrap();
    let req = Request::builder()
        .uri("/test")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let res = app.oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
