//! /api/kyuyo/* ハンドラのテスト (Refs #82)。MockKyuyoRepo + StubIntrospect で
//! DB / auth-worker 不要。

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::kyuyo::introspect::{
    IntrospectApi, IntrospectError, IntrospectResult, KyuyoAuthState,
};
use rust_ichibanboshi::kyuyo::logic::{RawKyuyoRow, RawShukeiRow, MONEY_COLUMNS};
use rust_ichibanboshi::kyuyo::repo::{
    DynKyuyoRepo, KyuyoRepo, KyuyoRepoError, NotConfiguredKyuyoRepo,
};
use rust_ichibanboshi::routes;
use rust_ichibanboshi::routes::kyuyo::KyuyoLimiter;
use tower::ServiceExt;

// ══════════════════════════════════════════════════════════════
// テスト部品
// ══════════════════════════════════════════════════════════════

struct StubIntrospect(IntrospectResult);

#[async_trait]
impl IntrospectApi for StubIntrospect {
    async fn introspect(&self, _token: &str) -> Result<IntrospectResult, IntrospectError> {
        Ok(self.0.clone())
    }
}

fn auth_ok() -> KyuyoAuthState {
    KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(IntrospectResult {
            active: true,
            email: "keiri@example.com".to_string(),
            tenant_id: "t".to_string(),
            role: "admin".to_string(),
        }))),
        &["keiri@example.com".to_string()],
    )
}

#[derive(Default)]
struct MockKyuyoRepo {
    databases: Vec<(String, Option<i32>)>,
    databases_pool_error: bool,
    databases_query_error: bool,
    names: Vec<(String, String)>,
    names_error: bool,
    payroll: Vec<RawKyuyoRow>,
    /// Some(message) で payroll_month が QueryError(message) を返す。
    payroll_error: Option<String>,
    koumoku: Vec<(String, String)>,
    koumoku_error: bool,
    shukei: Vec<RawShukeiRow>,
    shukei_error: bool,
}

#[async_trait]
impl KyuyoRepo for MockKyuyoRepo {
    async fn list_kydata_databases(&self) -> Result<Vec<(String, Option<i32>)>, KyuyoRepoError> {
        if self.databases_pool_error {
            return Err(KyuyoRepoError::PoolError("pool down".to_string()));
        }
        if self.databases_query_error {
            return Err(KyuyoRepoError::QueryError("boom".to_string()));
        }
        Ok(self.databases.clone())
    }
    async fn list_kydata_database_names(&self) -> Result<Vec<String>, KyuyoRepoError> {
        if self.databases_query_error {
            return Err(KyuyoRepoError::QueryError("boom".to_string()));
        }
        Ok(self
            .databases
            .iter()
            .map(|(name, _)| name.clone())
            .collect())
    }
    async fn company_names(&self) -> Result<Vec<(String, String)>, KyuyoRepoError> {
        if self.names_error {
            return Err(KyuyoRepoError::QueryError("no kycomstd".to_string()));
        }
        Ok(self.names.clone())
    }
    async fn payroll_month(
        &self,
        _db: &str,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<RawKyuyoRow>, KyuyoRepoError> {
        if let Some(message) = &self.payroll_error {
            return Err(KyuyoRepoError::QueryError(message.clone()));
        }
        Ok(self.payroll.clone())
    }
    async fn koumoku(&self, _db: &str) -> Result<Vec<(String, String)>, KyuyoRepoError> {
        if self.koumoku_error {
            return Err(KyuyoRepoError::QueryError("boom".to_string()));
        }
        Ok(self.koumoku.clone())
    }
    async fn shukei_totals(
        &self,
        _db: &str,
        month_index: i32,
    ) -> Result<Vec<RawShukeiRow>, KyuyoRepoError> {
        if self.shukei_error {
            return Err(KyuyoRepoError::QueryError("boom".to_string()));
        }
        Ok(self
            .shukei
            .iter()
            .filter(|s| s.month_index == month_index)
            .cloned()
            .collect())
    }
}

fn build_app(repo: DynKyuyoRepo, auth: KyuyoAuthState) -> Router {
    Router::new()
        .route("/api/kyuyo/companies", get(routes::kyuyo::companies))
        .route("/api/kyuyo/databases", get(routes::kyuyo::databases))
        .route("/api/kyuyo/payroll", get(routes::kyuyo::payroll))
        .layer(Extension(repo))
        .layer(Extension(Arc::new(auth)))
        .layer(Extension(Arc::new(KyuyoLimiter::default())))
}

async fn get_json(app: Router, uri: &str, with_token: bool) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder().uri(uri);
    if with_token {
        builder = builder.header("Authorization", "Bearer tok");
    }
    let res = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    (status, json)
}

fn raw_row(shain: i32, code: &str, month_index: i32, base_pay: i64) -> RawKyuyoRow {
    let mut money = vec![0i64; MONEY_COLUMNS];
    money[0] = base_pay;
    RawKyuyoRow {
        shain,
        month_index,
        pay_date: "2026-06-15".to_string(),
        period_start: "2026-05-01".to_string(),
        period_end: "2026-05-31".to_string(),
        employee_code: code.to_string(),
        employee_name: format!("社員{shain}"),
        taikyu: 0,
        department: "本社　乗務員".to_string(),
        taikei: 1,
        money,
    }
}

// ══════════════════════════════════════════════════════════════
// 認可まわり (handler 経由)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_companies_requires_token() {
    let app = build_app(Arc::new(MockKyuyoRepo::default()), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/companies", false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(body["error"].as_str().unwrap().contains("Bearer"));
}

#[tokio::test]
async fn test_payroll_email_not_allowed() {
    let auth = KyuyoAuthState::new(
        Some(Arc::new(StubIntrospect(IntrospectResult {
            active: true,
            email: "other@example.com".to_string(),
            tenant_id: "t".to_string(),
            role: "member".to_string(),
        }))),
        &["keiri@example.com".to_string()],
    );
    let app = build_app(Arc::new(MockKyuyoRepo::default()), auth);
    let (status, _) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/companies
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_companies_ok_with_warnings() {
    let repo = MockKyuyoRepo {
        databases: vec![
            ("KYDATA0100_125C".to_string(), Some(1)),
            ("KYDATA0100_126C".to_string(), Some(1)),
            ("KYDATA0200_126C".to_string(), Some(0)), // 権限抜け → warning
            ("KYDATA0900_116C".to_string(), Some(1)), // 廃業済み → 無視
        ],
        names: vec![("0100".to_string(), "有限会社 大石運輸".to_string())],
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/companies", true).await;
    assert_eq!(status, StatusCode::OK);

    let companies = body["companies"].as_array().unwrap();
    assert_eq!(companies.len(), 1);
    assert_eq!(companies[0]["company"], "0100");
    assert_eq!(companies[0]["name"], "有限会社 大石運輸");
    assert_eq!(
        companies[0]["years"].as_array().unwrap(),
        &vec![serde_json::json!(2025), serde_json::json!(2026)]
    );
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].as_str().unwrap().contains("KYDATA0200_126C"));
}

#[tokio::test]
async fn test_companies_names_error_is_warning_not_failure() {
    let repo = MockKyuyoRepo {
        databases: vec![("KYDATA0100_126C".to_string(), Some(1))],
        names_error: true,
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/companies", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["companies"][0]["name"], "");
    assert!(body["warnings"][0].as_str().unwrap().contains("KYCOMSTD"));
}

#[tokio::test]
async fn test_companies_not_configured() {
    let app = build_app(Arc::new(NotConfiguredKyuyoRepo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/companies", true).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body["error"].as_str().unwrap().contains("[kyuyo]"));
}

#[tokio::test]
async fn test_companies_pool_error() {
    let repo = MockKyuyoRepo {
        databases_pool_error: true,
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/companies", true).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body["error"].as_str().unwrap().contains("給与大臣 PC"));
}

#[tokio::test]
async fn test_companies_query_error() {
    let repo = MockKyuyoRepo {
        databases_query_error: true,
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/companies", true).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/databases (高速一覧)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_databases_ok() {
    let repo = MockKyuyoRepo {
        databases: vec![
            ("KYDATA0100_126C".to_string(), Some(1)),
            ("KYDATA0200_126C".to_string(), Some(0)), // 権限に関係なく名前は返る
        ],
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/databases", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["databases"].as_array().unwrap(),
        &vec![
            serde_json::json!("KYDATA0100_126C"),
            serde_json::json!("KYDATA0200_126C"),
        ]
    );
}

#[tokio::test]
async fn test_databases_requires_token_and_maps_errors() {
    // 認可ゲートは他の kyuyo ルートと同一
    let app = build_app(Arc::new(MockKyuyoRepo::default()), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/databases", false).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let repo = MockKyuyoRepo {
        databases_query_error: true,
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/databases", true).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    let app = build_app(Arc::new(NotConfiguredKyuyoRepo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/databases", true).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/payroll
// ══════════════════════════════════════════════════════════════

fn repo_with_june_data() -> MockKyuyoRepo {
    MockKyuyoRepo {
        databases: vec![("KYDATA0100_126C".to_string(), Some(1))],
        payroll: vec![
            raw_row(4, "1771  ", 5, 83_418),
            raw_row(2, "0941  ", 5, 90_000),
        ],
        koumoku: vec![("01018".to_string(), "基本給".to_string())],
        shukei: vec![
            RawShukeiRow {
                shain: 4,
                month_index: 5,
                soshikyu: 404_045,
                kazei: 300_000,
                hoken: 56_398,
                zei: 7_830,
                shokoujo: 30_500,
            },
            // 別 month_index の行は使われない (shukei_totals が index で絞る)
            RawShukeiRow {
                shain: 2,
                month_index: 4,
                soshikyu: 1,
                kazei: 0,
                hoken: 0,
                zei: 0,
                shokoujo: 0,
            },
        ],
        ..Default::default()
    }
}

#[tokio::test]
async fn test_payroll_ok() {
    let app = build_app(Arc::new(repo_with_june_data()), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["company"], "0100");
    assert_eq!(body["month"], "2026-06");
    assert_eq!(body["database"], "KYDATA0100_126C");

    let rows = body["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    // 突合キーの数値順 (941 < 1771)
    assert_eq!(rows[0]["employee_code_key"], "941");
    assert_eq!(rows[1]["employee_code_key"], "1771");
    assert_eq!(rows[1]["amounts"]["基本給"], 83_418);
    assert_eq!(rows[1]["totals"]["soshikyu"], 404_045);
    assert_eq!(rows[1]["totals"]["net_pay"], 309_317);
    // SHAIN=2 は SHUKEI1 に month_index=5 の行が無い → totals null + warning
    assert!(rows[0]["totals"].is_null());
    let warnings = body["warnings"].as_array().unwrap();
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].as_str().unwrap().contains("SHAIN=2"));
}

#[tokio::test]
async fn test_payroll_december_uses_next_nendo_db() {
    // 12 月分は翌年度 DB (#81)。2026-12 → KYDATA0100_127C
    let repo = MockKyuyoRepo {
        databases: vec![
            ("KYDATA0100_126C".to_string(), Some(1)),
            ("KYDATA0100_127C".to_string(), Some(1)),
        ],
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-12", true).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["database"], "KYDATA0100_127C");
    // 空データ → 支給回なし warning
    assert!(body["rows"].as_array().unwrap().is_empty());
    let warnings = body["warnings"].as_array().unwrap();
    assert!(warnings[0].as_str().unwrap().contains("支給回がありません"));
}

#[tokio::test]
async fn test_payroll_bad_company_and_month() {
    let app = build_app(Arc::new(MockKyuyoRepo::default()), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/payroll?company=0500&month=2026-06", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("company"));

    let app = build_app(Arc::new(MockKyuyoRepo::default()), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/payroll?company=0100&month=202606", true).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body["error"].as_str().unwrap().contains("YYYY-MM"));
}

#[tokio::test]
async fn test_payroll_db_open_error_is_404() {
    // 存在しない年度 DB / restore 由来の権限抜けは SQL Server error 4060
    // ("Cannot open database") として現れる → 404 に変換 (500 にしない)
    let repo = MockKyuyoRepo {
        payroll_error: Some(
            "Cannot open database \"KYDATA0100_126C\" requested by the login.".to_string(),
        ),
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, body) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let message = body["error"].as_str().unwrap();
    assert!(message.contains("KYDATA0100_126C"));
    assert!(message.contains("権限の再付与"));

    // エラー番号 (4060) だけでメッセージ文言が違う場合も 404 に拾う
    let repo = MockKyuyoRepo {
        payroll_error: Some("Error 4060: database unavailable".to_string()),
        ..Default::default()
    };
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_payroll_query_errors_bubble_up() {
    // payroll_month 失敗 (DB open 系でない一般エラーは 500 のまま)
    let mut repo = repo_with_june_data();
    repo.payroll_error = Some("boom".to_string());
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // koumoku 失敗
    let mut repo = repo_with_june_data();
    repo.koumoku_error = true;
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    // shukei_totals 失敗
    let mut repo = repo_with_june_data();
    repo.shukei_error = true;
    let app = build_app(Arc::new(repo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn test_payroll_not_configured() {
    let app = build_app(Arc::new(NotConfiguredKyuyoRepo), auth_ok());
    let (status, _) = get_json(app, "/api/kyuyo/payroll?company=0100&month=2026-06", true).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

// ══════════════════════════════════════════════════════════════
// KyuyoRepoError Display / NotConfiguredKyuyoRepo の残メソッド
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_not_configured_repo_all_methods() {
    let repo = NotConfiguredKyuyoRepo;
    assert!(matches!(
        repo.company_names().await.unwrap_err(),
        KyuyoRepoError::NotConfigured
    ));
    assert!(matches!(
        repo.payroll_month("KYDATA0100_126C", "2026-06-01", "2026-07-01")
            .await
            .unwrap_err(),
        KyuyoRepoError::NotConfigured
    ));
    assert!(matches!(
        repo.koumoku("KYDATA0100_126C").await.unwrap_err(),
        KyuyoRepoError::NotConfigured
    ));
    assert!(matches!(
        repo.shukei_totals("KYDATA0100_126C", 5).await.unwrap_err(),
        KyuyoRepoError::NotConfigured
    ));
}

#[test]
fn test_kyuyo_repo_error_display() {
    assert_eq!(
        KyuyoRepoError::NotConfigured.to_string(),
        "kyuyo database is not configured"
    );
    assert_eq!(
        KyuyoRepoError::PoolError("p".to_string()).to_string(),
        "kyuyo pool error: p"
    );
    assert_eq!(
        KyuyoRepoError::QueryError("q".to_string()).to_string(),
        "kyuyo query error: q"
    );
}
