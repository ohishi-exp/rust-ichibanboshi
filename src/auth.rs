use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response, Extension};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// JWT シークレットのラッパー（rust-alc-api と同じ）
#[derive(Clone)]
pub struct JwtSecret(pub String);

/// App JWT のクレーム（rust-alc-api と同じ構造）
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppClaims {
    pub sub: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_slug: Option<String>,
    /// 発行環境 (staging/prod 分離、rust-alc-api #218)。旧 token 互換のため Option
    #[serde(skip_serializing_if = "Option::is_none")]
    pub env: Option<String>,
    pub iat: i64,
    pub exp: i64,
}

/// 現在の環境ラベル (rust-alc-api `current_env_label()` と同じ判定)
///
/// `STAGING_MODE=true` → "staging"、それ以外 → "prod"。
/// 本サービスは prod 運用のみだが、verify 側の比較基準として同じ規約に従う。
pub fn current_env_label() -> &'static str {
    match std::env::var("STAGING_MODE").as_deref() {
        Ok("true") => "staging",
        _ => "prod",
    }
}

/// 認証済みユーザー情報
#[derive(Debug, Clone)]
pub struct AuthUser {
    pub user_id: Uuid,
    pub email: String,
    pub name: String,
    pub tenant_id: Uuid,
    pub role: String,
}

/// Access token を検証してクレームを返す
pub fn verify_access_token(
    token: &str,
    secret: &JwtSecret,
) -> Result<AppClaims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;

    let token_data = decode::<AppClaims>(
        token,
        &DecodingKey::from_secret(secret.0.as_bytes()),
        &validation,
    )?;

    // rust-alc-api #218: token の env claim と現在環境の一致を強制する
    // (staging token を prod が受理しない)。旧 token 互換のため env 未設定は通す。
    if let Some(token_env) = token_data.claims.env.as_deref() {
        let expected = current_env_label();
        if token_env != expected {
            return Err(jsonwebtoken::errors::Error::from(
                jsonwebtoken::errors::ErrorKind::InvalidIssuer,
            ));
        }
    }

    Ok(token_data.claims)
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
    use jsonwebtoken::{encode, EncodingKey, Header};

    const SECRET: &str = "test-secret";

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

    // 以下のテストは STAGING_MODE 未設定 (= "prod") 前提。env::set_var による
    // 切替はテスト並列実行で race するため行わない。

    #[test]
    fn verify_accepts_matching_env() {
        let token = make_token(Some("prod"));
        let secret = JwtSecret(SECRET.to_string());
        assert!(verify_access_token(&token, &secret).is_ok());
    }

    #[test]
    fn verify_accepts_legacy_token_without_env() {
        let token = make_token(None);
        let secret = JwtSecret(SECRET.to_string());
        assert!(verify_access_token(&token, &secret).is_ok());
    }

    #[test]
    fn verify_rejects_cross_env_token() {
        let token = make_token(Some("staging"));
        let secret = JwtSecret(SECRET.to_string());
        let err = verify_access_token(&token, &secret).unwrap_err();
        assert!(matches!(
            err.kind(),
            jsonwebtoken::errors::ErrorKind::InvalidIssuer
        ));
    }
}
