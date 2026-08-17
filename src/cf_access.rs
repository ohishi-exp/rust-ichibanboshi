//! Cloudflare Access が発行した JWT (`Cf-Access-Jwt-Assertion` ヘッダ) を検証する。
//!
//! 中継 (`bin/rdp_relay.rs`) は Cloudflare Tunnel の後ろに置く。Tunnel の前段に立つ
//! Access アプリが利用者を認証し、origin へ転送する要求にこのヘッダを挿す。中継は
//! **ヘッダを信用せず**、team の JWKS で RS256 署名・`iss`・`aud`・`exp` を検証する。
//! Access の設定漏れやヘッダ偽装で素通りさせないための二段目の壁。
//!
//! 型は先例 (rust-alc-api の `crates/alc-core/src/auth_google.rs`、Google OIDC を
//! 同じ形で検証しており 100% 登録済み) から借りている。違うのは JWKS の出所と
//! claim の並びだけ:
//!
//! - JWKS   … `https://<team>.cloudflareaccess.com/cdn-cgi/access/certs`
//! - `iss`  … `https://<team>.cloudflareaccess.com`
//! - `aud`  … Access アプリの AUD タグ (**配列**で入っている)
//! - 持ち主 … 利用者は `email`、サービストークンは `common_name`
//!
//! JWKS の取得だけが I/O で、残りは純粋。テストは wiremock で JWKS を差し替える。

use core::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::RwLock;

/// JWKS を持ち回す時間。CF の署名鍵は定期的に回るので抱え込みすぎない。
const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(3600);

/// team 名だけ渡されたときに補う既定のドメイン。
const ACCESS_DOMAIN: &str = "cloudflareaccess.com";

#[derive(Debug)]
pub enum CfAccessError {
    /// `--cf-access-team-domain` から JWKS の URL を組み立てられない。
    BadTeamDomain(String),
    /// AUD タグが空。空のまま起動すると全部 401 になって原因が分かりにくいので、
    /// 起動時に鳴らす (`--allow` 未指定を拒否するのと同じ姿勢)。
    MissingAud,
    /// JWT として壊れている、または `kid` が無い。
    Malformed,
    /// JWKS を取れない (通信不能 / JSON でない)。
    JwksUnavailable,
    /// `kid` に対応する鍵が JWKS に無い。
    UnknownKey,
    /// JWKS の鍵成分 (n, e) が鍵として成立しない。
    BadKey,
    /// 署名・`iss`・`aud`・`exp` のいずれかが通らない。
    Rejected,
}

impl fmt::Display for CfAccessError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadTeamDomain(d) => write!(f, "team ドメインを解釈できない: {d}"),
            Self::MissingAud => write!(f, "Access アプリの AUD タグが空"),
            Self::Malformed => write!(f, "Access トークンが JWT として壊れている"),
            Self::JwksUnavailable => write!(f, "Access の JWKS を取得できない"),
            Self::UnknownKey => write!(f, "トークンの kid が JWKS に無い"),
            Self::BadKey => write!(f, "JWKS の鍵成分が壊れている"),
            Self::Rejected => write!(f, "Access トークンが検証を通らない"),
        }
    }
}

impl std::error::Error for CfAccessError {}

/// `--cf-access-team-domain` の受け取り方を 1 つに揃え、`(iss, JWKS URL)` を返す。
///
/// `example` / `example.cloudflareaccess.com` / `https://example.cloudflareaccess.com`
/// のどれで渡しても同じ組を返す。運用者が CF の管理画面から拾ってきた文字列を
/// そのまま貼れるようにするため。
pub fn team_endpoints(team_domain: &str) -> Result<(String, String), CfAccessError> {
    let trimmed = team_domain.trim().trim_end_matches('/');
    let host = trimmed
        .strip_prefix("https://")
        .or_else(|| trimmed.strip_prefix("http://"))
        .unwrap_or(trimmed);

    if host.is_empty() {
        return Err(CfAccessError::BadTeamDomain("空".to_owned()));
    }
    if host.contains('/') {
        // format! は 1 行に収める (折り返すと行カバレッジに乗らず 100% gate が落ちる)。
        return Err(CfAccessError::BadTeamDomain(format!("path 付き: {host}")));
    }

    // team 名だけなら既定ドメインを補う。既にドメインの形なら触らない。
    let host = if host.contains('.') {
        host.to_owned()
    } else {
        format!("{host}.{ACCESS_DOMAIN}")
    };

    let issuer = format!("https://{host}");
    let certs_url = format!("{issuer}/cdn-cgi/access/certs");
    Ok((issuer, certs_url))
}

/// 検証を通ったトークンの持ち主。ログに残して「誰が繋いだか」を追えるようにする。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccessIdentity {
    pub subject: String,
}

/// 中継が見る claim だけを拾う。`aud` / `iss` / `exp` は jsonwebtoken 側が検証する。
#[derive(Debug, Deserialize)]
struct AccessClaims {
    /// 利用者トークンに入る。
    #[serde(default)]
    email: String,
    /// サービストークンに入る (利用者トークンには無い)。
    #[serde(default)]
    common_name: String,
}

impl AccessClaims {
    fn subject(&self) -> String {
        if !self.email.is_empty() {
            return self.email.clone();
        }
        if !self.common_name.is_empty() {
            return self.common_name.clone();
        }
        "(識別子なし)".to_owned()
    }
}

#[derive(Debug, Deserialize, Clone)]
struct JwkKey {
    kid: String,
    n: String,
    e: String,
}

#[derive(Debug, Deserialize)]
struct JwksResponse {
    keys: Vec<JwkKey>,
}

struct CachedJwks {
    keys: Vec<JwkKey>,
    fetched_at: Instant,
}

/// Access トークンの検証器。JWKS を抱えて使い回す。
#[derive(Clone)]
pub struct CfAccessVerifier {
    issuer: String,
    certs_url: String,
    /// Access アプリの AUD タグ。これを固定しないと、同じ team の別アプリの
    /// トークンで中継に入られる (confused deputy)。
    aud: String,
    http: reqwest::Client,
    cache: Arc<RwLock<Option<CachedJwks>>>,
    cache_ttl: Duration,
}

impl CfAccessVerifier {
    /// team ドメインと AUD タグから作る。
    pub fn new(team_domain: &str, aud: String) -> Result<Self, CfAccessError> {
        if aud.trim().is_empty() {
            return Err(CfAccessError::MissingAud);
        }
        let (issuer, certs_url) = team_endpoints(team_domain)?;
        Ok(Self::with_endpoints(
            issuer,
            certs_url,
            aud,
            DEFAULT_CACHE_TTL,
        ))
    }

    /// JWKS の出所を直接指定して作る。テストで wiremock を差し込むための入口。
    pub fn with_endpoints(
        issuer: String,
        certs_url: String,
        aud: String,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            issuer,
            certs_url,
            aud,
            http: reqwest::Client::new(),
            cache: Arc::new(RwLock::new(None)),
            cache_ttl,
        }
    }

    /// トークンを検証し、通れば持ち主を返す。
    pub async fn verify(&self, token: &str) -> Result<AccessIdentity, CfAccessError> {
        let header = decode_header(token).map_err(|_| CfAccessError::Malformed)?;
        let kid = header.kid.ok_or(CfAccessError::Malformed)?;
        let key = self.key_for(&kid).await?;

        let decoding_key =
            DecodingKey::from_rsa_components(&key.n, &key.e).map_err(|_| CfAccessError::BadKey)?;

        // RS256 に固定する (alg を相手任せにすると alg confusion で素通りする)。
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.aud]);

        let data = decode::<AccessClaims>(token, &decoding_key, &validation).map_err(|e| {
            tracing::warn!("Access トークンを拒否: {e}");
            CfAccessError::Rejected
        })?;

        Ok(AccessIdentity {
            subject: data.claims.subject(),
        })
    }

    /// 期限内のキャッシュから `kid` を引く。
    async fn cached_key(&self, kid: &str) -> Option<JwkKey> {
        let cache = self.cache.read().await;
        let cached = cache.as_ref()?;
        if cached.fetched_at.elapsed() >= self.cache_ttl {
            return None;
        }
        cached.keys.iter().find(|k| k.kid == kid).cloned()
    }

    /// `kid` に対応する鍵を得る。キャッシュに無ければ JWKS を取り直す。
    async fn key_for(&self, kid: &str) -> Result<JwkKey, CfAccessError> {
        if let Some(key) = self.cached_key(kid).await {
            return Ok(key);
        }

        let response = self
            .http
            .get(&self.certs_url)
            .send()
            .await
            .map_err(|_| CfAccessError::JwksUnavailable)?;
        let jwks: JwksResponse = response
            .json()
            .await
            .map_err(|_| CfAccessError::JwksUnavailable)?;

        let key = jwks
            .keys
            .iter()
            .find(|k| k.kid == kid)
            .cloned()
            .ok_or(CfAccessError::UnknownKey)?;

        *self.cache.write().await = Some(CachedJwks {
            keys: jwks.keys,
            fetched_at: Instant::now(),
        });

        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine as _;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use rsa::pkcs1::EncodeRsaPrivateKey as _;
    use rsa::traits::PublicKeyParts as _;
    use rsa::{RsaPrivateKey, RsaPublicKey};
    use serde_json::json;
    use std::sync::OnceLock;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TEST_KID: &str = "test-kid-1";
    const TEST_AUD: &str = "0123456789abcdef";
    const TEST_ISSUER: &str = "https://example.cloudflareaccess.com";

    /// 鍵生成は重いので 1 回だけ。PEM (署名用) と JWK の成分 (検証用) を揃えて持つ。
    struct TestKey {
        pem: Vec<u8>,
        n: String,
        e: String,
    }

    fn test_key() -> &'static TestKey {
        static KEY: OnceLock<TestKey> = OnceLock::new();
        KEY.get_or_init(|| {
            let private = RsaPrivateKey::new(&mut rsa::rand_core::OsRng, 2048).unwrap();
            let public = RsaPublicKey::from(&private);
            TestKey {
                pem: private
                    .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                    .unwrap()
                    .as_bytes()
                    .to_vec(),
                n: URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),
                e: URL_SAFE_NO_PAD.encode(public.e().to_bytes_be()),
            }
        })
    }

    /// CF が返すのと同じ形の JWKS。
    fn jwks_body(n: &str, e: &str) -> serde_json::Value {
        json!({"keys": [{"kid": TEST_KID, "kty": "RSA", "alg": "RS256", "use": "sig", "n": n, "e": e}]})
    }

    /// CF が発行するのと同じ形のトークンを署名して作る。`aud` は配列。
    fn sign(claims: &serde_json::Value, kid: &str) -> String {
        let key = EncodingKey::from_rsa_pem(&test_key().pem).unwrap();
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_owned());
        encode(&header, claims, &key).unwrap()
    }

    fn future_exp() -> i64 {
        chrono::Utc::now().timestamp() + 3600
    }

    /// 利用者トークン (email を持つ)。
    fn user_claims() -> serde_json::Value {
        json!({
            "aud": [TEST_AUD],
            "iss": TEST_ISSUER,
            "exp": future_exp(),
            "email": "taro@example.com",
            "sub": "abc123",
        })
    }

    /// JWKS を返す MockServer を立て、それを向いた検証器を作る。
    async fn verifier_with_jwks(
        body: serde_json::Value,
        ttl: Duration,
    ) -> (MockServer, CfAccessVerifier) {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/cdn-cgi/access/certs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let certs_url = format!("{}/cdn-cgi/access/certs", server.uri());
        let verifier = CfAccessVerifier::with_endpoints(
            TEST_ISSUER.to_owned(),
            certs_url,
            TEST_AUD.to_owned(),
            ttl,
        );
        (server, verifier)
    }

    // --- team_endpoints ---

    #[test]
    fn team_endpoints_accepts_a_bare_team_name() {
        let (issuer, certs) = team_endpoints("example").unwrap();
        assert_eq!(issuer, TEST_ISSUER);
        assert_eq!(certs, format!("{TEST_ISSUER}/cdn-cgi/access/certs"));
    }

    #[test]
    fn team_endpoints_accepts_a_full_url_with_trailing_slash() {
        let (issuer, certs) = team_endpoints("  https://example.cloudflareaccess.com/  ").unwrap();
        assert_eq!(issuer, TEST_ISSUER);
        assert_eq!(certs, format!("{TEST_ISSUER}/cdn-cgi/access/certs"));
    }

    #[test]
    fn team_endpoints_accepts_a_bare_domain_and_http_prefix() {
        assert_eq!(
            team_endpoints("example.cloudflareaccess.com").unwrap().0,
            TEST_ISSUER
        );
        assert_eq!(
            team_endpoints("http://example.cloudflareaccess.com")
                .unwrap()
                .0,
            TEST_ISSUER
        );
    }

    #[test]
    fn team_endpoints_rejects_empty() {
        let err = team_endpoints("   ").unwrap_err();
        assert!(matches!(err, CfAccessError::BadTeamDomain(_)));
        assert!(err.to_string().contains("空"));
    }

    #[test]
    fn team_endpoints_rejects_a_url_with_a_path() {
        let err = team_endpoints("https://example.cloudflareaccess.com/cdn-cgi").unwrap_err();
        assert!(matches!(err, CfAccessError::BadTeamDomain(_)));
        assert!(err.to_string().contains("path"));
    }

    // --- new ---

    #[test]
    fn new_builds_from_a_team_name() {
        let verifier = CfAccessVerifier::new("example", TEST_AUD.to_owned()).unwrap();
        assert_eq!(verifier.issuer, TEST_ISSUER);
        assert_eq!(
            verifier.certs_url,
            format!("{TEST_ISSUER}/cdn-cgi/access/certs")
        );
        assert_eq!(verifier.cache_ttl, DEFAULT_CACHE_TTL);
    }

    #[test]
    fn new_propagates_a_bad_team_domain() {
        // 変種の判別は team_endpoints_rejects_empty 側で見ている。ここは
        // 「verifier が作られずに失敗が出てくる」ことだけ。`unwrap_err` を使わないのは
        // CfAccessVerifier に Debug を要求しないため (derive しても呼ばれず、
        // llvm-cov では未カバー行に見えて 100% gate を落とす)。
        assert!(CfAccessVerifier::new("", TEST_AUD.to_owned()).is_err());
    }

    #[test]
    fn new_rejects_an_empty_aud() {
        // 空のまま上がると「全部 401」になって切り分けが効かないので起動時に鳴らす。
        assert!(CfAccessVerifier::new("example", "   ".to_owned()).is_err());
        assert!(CfAccessError::MissingAud.to_string().contains("AUD"));
    }

    // --- verify: 通る道 ---

    #[tokio::test]
    async fn verify_accepts_a_user_token_and_reports_the_email() {
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;

        let identity = verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .unwrap();

        assert_eq!(identity.subject, "taro@example.com");
    }

    #[tokio::test]
    async fn verify_accepts_a_service_token_and_reports_the_common_name() {
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;
        let claims = json!({
            "aud": [TEST_AUD],
            "iss": TEST_ISSUER,
            "exp": future_exp(),
            "common_name": "deploy-token",
        });

        let identity = verifier.verify(&sign(&claims, TEST_KID)).await.unwrap();

        assert_eq!(identity.subject, "deploy-token");
    }

    #[tokio::test]
    async fn verify_falls_back_when_the_token_names_nobody() {
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;
        let claims = json!({"aud": [TEST_AUD], "iss": TEST_ISSUER, "exp": future_exp()});

        let identity = verifier.verify(&sign(&claims, TEST_KID)).await.unwrap();

        assert_eq!(identity.subject, "(識別子なし)");
    }

    // --- verify: 蹴る道 ---

    #[tokio::test]
    async fn verify_rejects_a_non_jwt() {
        let verifier = CfAccessVerifier::new("example", TEST_AUD.to_owned()).unwrap();
        let err = verifier.verify("not-a-jwt").await.unwrap_err();
        assert!(matches!(err, CfAccessError::Malformed));
        assert!(err.to_string().contains("壊れている"));
    }

    #[tokio::test]
    async fn verify_rejects_a_token_without_a_kid() {
        let key = EncodingKey::from_rsa_pem(&test_key().pem).unwrap();
        // kid を入れない = どの鍵で検証すべきか決められない。
        let token = encode(&Header::new(Algorithm::RS256), &user_claims(), &key).unwrap();

        let verifier = CfAccessVerifier::new("example", TEST_AUD.to_owned()).unwrap();
        assert!(matches!(
            verifier.verify(&token).await.unwrap_err(),
            CfAccessError::Malformed
        ));
    }

    #[tokio::test]
    async fn verify_rejects_a_token_signed_for_another_audience() {
        // 同じ team の別 Access アプリのトークンで中継に入れないこと。
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;
        let claims = json!({
            "aud": ["別のアプリの AUD"],
            "iss": TEST_ISSUER,
            "exp": future_exp(),
            "email": "taro@example.com",
        });

        assert!(matches!(
            verifier.verify(&sign(&claims, TEST_KID)).await.unwrap_err(),
            CfAccessError::Rejected
        ));
    }

    #[tokio::test]
    async fn verify_rejects_a_token_from_another_issuer() {
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;
        let claims = json!({
            "aud": [TEST_AUD],
            "iss": "https://attacker.cloudflareaccess.com",
            "exp": future_exp(),
            "email": "taro@example.com",
        });

        assert!(matches!(
            verifier.verify(&sign(&claims, TEST_KID)).await.unwrap_err(),
            CfAccessError::Rejected
        ));
    }

    #[tokio::test]
    async fn verify_rejects_an_expired_token() {
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;
        let claims = json!({
            "aud": [TEST_AUD],
            "iss": TEST_ISSUER,
            "exp": 1_000_000_000,
            "email": "taro@example.com",
        });

        assert!(matches!(
            verifier.verify(&sign(&claims, TEST_KID)).await.unwrap_err(),
            CfAccessError::Rejected
        ));
    }

    #[tokio::test]
    async fn verify_rejects_when_the_kid_is_not_in_the_jwks() {
        let key = test_key();
        let (_server, verifier) =
            verifier_with_jwks(jwks_body(&key.n, &key.e), DEFAULT_CACHE_TTL).await;

        let err = verifier
            .verify(&sign(&user_claims(), "知らない kid"))
            .await
            .unwrap_err();

        assert!(matches!(err, CfAccessError::UnknownKey));
        assert!(err.to_string().contains("kid"));
    }

    #[tokio::test]
    async fn verify_rejects_when_the_jwks_key_components_are_broken() {
        let broken = json!({"keys": [{"kid": TEST_KID, "kty": "RSA", "n": "", "e": ""}]});
        let (_server, verifier) = verifier_with_jwks(broken, DEFAULT_CACHE_TTL).await;

        let err = verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            CfAccessError::BadKey | CfAccessError::Rejected
        ));
    }

    #[tokio::test]
    async fn verify_reports_when_the_jwks_host_is_unreachable() {
        // 誰も listen していないポート = 取得そのものが失敗する。
        let verifier = CfAccessVerifier::with_endpoints(
            TEST_ISSUER.to_owned(),
            "http://127.0.0.1:1/cdn-cgi/access/certs".to_owned(),
            TEST_AUD.to_owned(),
            DEFAULT_CACHE_TTL,
        );

        let err = verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .unwrap_err();

        assert!(matches!(err, CfAccessError::JwksUnavailable));
        assert!(err.to_string().contains("JWKS"));
    }

    #[tokio::test]
    async fn verify_reports_when_the_jwks_is_not_json() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/cdn-cgi/access/certs"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>login</html>"))
            .mount(&server)
            .await;
        let verifier = CfAccessVerifier::with_endpoints(
            TEST_ISSUER.to_owned(),
            format!("{}/cdn-cgi/access/certs", server.uri()),
            TEST_AUD.to_owned(),
            DEFAULT_CACHE_TTL,
        );

        assert!(matches!(
            verifier
                .verify(&sign(&user_claims(), TEST_KID))
                .await
                .unwrap_err(),
            CfAccessError::JwksUnavailable
        ));
    }

    // --- JWKS キャッシュ ---

    #[tokio::test]
    async fn the_jwks_is_fetched_once_while_the_cache_is_warm() {
        let key = test_key();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/cdn-cgi/access/certs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&key.n, &key.e)))
            .expect(1)
            .mount(&server)
            .await;
        let verifier = CfAccessVerifier::with_endpoints(
            TEST_ISSUER.to_owned(),
            format!("{}/cdn-cgi/access/certs", server.uri()),
            TEST_AUD.to_owned(),
            DEFAULT_CACHE_TTL,
        );

        assert!(verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .is_ok());
        assert!(verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .is_ok());
        // expect(1) を MockServer の drop 時に検証する。
    }

    #[tokio::test]
    async fn the_jwks_is_refetched_once_the_cache_goes_stale() {
        let key = test_key();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/cdn-cgi/access/certs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(jwks_body(&key.n, &key.e)))
            .expect(2)
            .mount(&server)
            .await;
        // TTL 0 = 毎回取り直す。
        let verifier = CfAccessVerifier::with_endpoints(
            TEST_ISSUER.to_owned(),
            format!("{}/cdn-cgi/access/certs", server.uri()),
            TEST_AUD.to_owned(),
            Duration::ZERO,
        );

        assert!(verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .is_ok());
        assert!(verifier
            .verify(&sign(&user_claims(), TEST_KID))
            .await
            .is_ok());
    }
}
