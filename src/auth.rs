use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response, Extension};
use uuid::Uuid;

// JWT の claims / 検証は rust-alc-api の leaf crate `alc-auth-jwt` が SoT (本 repo #4)。
// 手動コピーをやめて re-export することで、alc 側の検証仕様変更 (env claim 等) に
// 自動追従する。既存の `crate::auth::...` import は無変更で通る。
pub use alc_auth_jwt::{current_env_label, verify_access_token, AppClaims, JwtSecret};

/// 認証済みユーザー情報
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
}

/// JWT 必須ミドルウェア
pub async fn require_jwt(
    Extension(jwt_secret): Extension<JwtSecret>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let token = extract_bearer_token(&req).ok_or(StatusCode::UNAUTHORIZED)?;

    let claims = verify_access_token(token, &jwt_secret).map_err(|e| {
        tracing::warn!("JWT verification failed: {e}");
        StatusCode::UNAUTHORIZED
    })?;

    let auth_user = AuthUser {
        user_id: claims.sub,
        email: claims.email,
        name: claims.name.clone(),
        tenant_id: claims.tenant_id,
        role: claims.role,
    };

    req.extensions_mut().insert(auth_user);
    Ok(next.run(req).await)
}

/// Authorization ヘッダーから Bearer トークンを抽出
fn extract_bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get("Authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};

    const SECRET: &str = "test-secret";

    /// STAGING_MODE を触る/読むテストの直列化 (set_var はプロセス全体に効くため)
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn make_token(env: Option<&str>) -> String {
        let now = chrono::Utc::now().timestamp();
        let claims = AppClaims {
            sub: Uuid::new_v4(),
            email: "t@example.com".to_string(),
            name: "t".to_string(),
            tenant_id: Uuid::new_v4(),
            role: "admin".to_string(),
            org_slug: None,
            env: env.map(|s| s.to_string()),
            iat: now,
            exp: now + 3600,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap()
    }

    // alc-auth-jwt への contract test — 依存先の env claim 検証仕様 (rust-alc-api #218)
    // が将来の更新で変わったら本 repo の CI で気付けるよう re-export 経由で固定する。

    #[test]
    fn verify_accepts_matching_env() {
        let _g = ENV_LOCK.lock().unwrap();
        let token = make_token(Some("prod"));
        let secret = JwtSecret(SECRET.to_string());
        assert!(verify_access_token(&token, &secret).is_ok());
    }

    #[test]
    fn verify_accepts_legacy_token_without_env() {
        let _g = ENV_LOCK.lock().unwrap();
        let token = make_token(None);
        let secret = JwtSecret(SECRET.to_string());
        assert!(verify_access_token(&token, &secret).is_ok());
    }

    #[test]
    fn verify_rejects_cross_env_token() {
        let _g = ENV_LOCK.lock().unwrap();
        let token = make_token(Some("staging"));
        let secret = JwtSecret(SECRET.to_string());
        let err = verify_access_token(&token, &secret).unwrap_err();
        assert!(matches!(
            err.kind(),
            jsonwebtoken::errors::ErrorKind::InvalidIssuer
        ));
    }

    #[test]
    fn current_env_label_staging_mode() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("STAGING_MODE", "true");
        assert_eq!(current_env_label(), "staging");
        std::env::remove_var("STAGING_MODE");
        assert_eq!(current_env_label(), "prod");
    }
}
