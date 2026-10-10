#![allow(dead_code)]

use std::sync::Arc;

use async_trait::async_trait;
use axum::routing::{get, post};
use axum::{Extension, Router};
use chrono::NaiveDate;
use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::config::RawConfig;
use rust_ichibanboshi::repo::{AppRepo, DynRepo, RepoError};
use rust_ichibanboshi::routes;
use rust_ichibanboshi::routes::uriage::UriageRow;
use rust_ichibanboshi::sqlite::{DynLocalStore, LocalStore};
use uuid::Uuid;

pub const TEST_JWT_SECRET: &str = "test-jwt-secret-ichibanboshi";

// ── MockRepo: テスト用 ──

pub struct MockRepo;

#[async_trait]
impl AppRepo for MockRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Ok(())
    }

    async fn uriage_rows(
        &self,
        _from: &str,
        _to: &str,
        _bumon_codes: &[String],
        _persons_id_list: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        // 担当者振替で B5 経路 (入力担当C=1499 → "青井") に当たる 1 行と
        // B6 経路 (マスタ外、表示のみ) に当たる 1 行を返す。
        // 横横=0 で 傭車金額は独立。
        Ok(vec![
            UriageRow {
                yokoyoko: 0,
                seikyu_k: 0,
                biko2: String::new(),
                nyuryoku_tanto_c: 1499,
                kado_bumon: "010".into(),
                kingaku: 50_000,
                nebiki: 0,
                warimashi: 1_000,
                jippi: 500,
                yosha_kingaku: 30_000,
                yosha_nebiki: 0,
                yosha_warimashi: 600,
                yosha_jippi: 200,
                shain_r: "青井".into(),
                yoshasaki_c: "000000".into(),
                unko_date: "2026-06-15".into(),
                uriage_date: "2026-06-15".into(),
                tokuisaki_key: "TESTCUST-0".into(),
                tokuisaki_n: "テスト得意先".into(),
                yoshasaki_key: "000000-0".into(),
                yoshasaki_n: "".into(),
            },
            // 入力担当 9999 (マスタ外) → B6 で表示のみ、$sum に積まない
            UriageRow {
                yokoyoko: 0,
                seikyu_k: 0,
                biko2: String::new(),
                nyuryoku_tanto_c: 9999,
                kado_bumon: "010".into(),
                kingaku: 8_000,
                nebiki: 0,
                warimashi: 0,
                jippi: 0,
                yosha_kingaku: 6_000,
                yosha_nebiki: 0,
                yosha_warimashi: 0,
                yosha_jippi: 0,
                shain_r: "無関係".into(),
                yoshasaki_c: "021970".into(),
                unko_date: "2026-06-16".into(),
                uriage_date: "2026-06-16".into(),
                tokuisaki_key: "TESTCUST2-0".into(),
                tokuisaki_n: "テスト得意先2".into(),
                yoshasaki_key: "021970-0".into(),
                yoshasaki_n: "テスト傭車先".into(),
            },
        ])
    }
}

// ── ErrorRepo: 全メソッドがエラーを返す ──

pub struct ErrorRepo;

#[async_trait]
impl AppRepo for ErrorRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Err(RepoError::PoolError)
    }
    async fn uriage_rows(
        &self,
        _: &str,
        _: &str,
        _: &[String],
        _: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        Err(RepoError::PoolError)
    }
}

// ── QueryErrorRepo: QueryError を返す ──

pub struct QueryErrorRepo;

#[async_trait]
impl AppRepo for QueryErrorRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        Err(RepoError::QueryError("test query error".into()))
    }
    async fn uriage_rows(
        &self,
        _: &str,
        _: &str,
        _: &[String],
        _: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        Err(RepoError::QueryError("test".into()))
    }
}

// ── ヘルパー ──

pub fn dt(y: i32, m: u32, d: u32) -> chrono::NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

pub fn local_store() -> DynLocalStore {
    Arc::new(LocalStore::open(":memory:").expect("in-memory sqlite"))
}

/// テスト用 raw dir。**test 毎にユニーク**な path を作って衝突を避ける (各 test が
/// 同 PID + 同時刻 nanos でぶつかる可能性を考慮し、UUID もまぶす)。
pub fn temp_raw_dir() -> Arc<RawConfig> {
    let dir = std::env::temp_dir().join(format!(
        "ichibanboshi-test-raw-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        Uuid::new_v4()
    ));
    Arc::new(RawConfig {
        dir: dir.to_string_lossy().into_owned(),
    })
}

/// CakePHP 未配線 (base_url 空) の client。/recalc が 503 を返す経路用。
pub fn disabled_cakephp() -> Arc<CakephpClient> {
    Arc::new(CakephpClient::new(String::new(), 30).expect("cakephp client build"))
}

/// 既定はオンプレの形 (SQL Server を使うと宣言済み) — 既存の /health テストの
/// 意味を変えないため。GCP 側の形 (`sqlserver = false`) は
/// `tests/health_backends_test.rs` が明示的に組む。
pub fn health_state() -> routes::health::HealthState {
    routes::health::HealthState {
        sqlserver: true,
        mariadb: false,
        kintai_events: "disabled",
    }
}

pub fn build_app(repo: DynRepo) -> Router {
    build_app_full(repo, local_store(), disabled_cakephp(), temp_raw_dir())
}

pub fn build_app_with_store(repo: DynRepo, store: DynLocalStore) -> Router {
    build_app_full(repo, store, disabled_cakephp(), temp_raw_dir())
}

pub fn build_app_full(
    repo: DynRepo,
    store: DynLocalStore,
    cakephp: Arc<CakephpClient>,
    raw_cfg: Arc<RawConfig>,
) -> Router {
    let api_routes = Router::new()
        .route("/uriage/by-person", post(routes::uriage::by_person))
        .route("/uriage/recalc", post(routes::uriage::recalc))
        .route("/uriage/daily", get(routes::uriage::daily))
        .route(
            "/uriage/person-monthly-totals",
            get(routes::uriage::person_monthly_totals),
        )
        .route(
            "/uriage/person-partner-totals",
            get(routes::uriage::person_partner_totals),
        )
        .route("/uriage/r2/pending", get(routes::uriage::r2_pending))
        .route(
            "/uriage/raw/{month}/{eigyosho_id}",
            get(routes::uriage::raw_get),
        )
        .route(
            "/uriage/raw/{month}/{eigyosho_id}/ack",
            post(routes::uriage::raw_ack),
        )
        .route("/uriage/admin/delete", post(routes::uriage::admin_delete))
        .route("/uriage/admin/rebuild", post(routes::uriage::admin_rebuild))
        .route("/uriage/verify", get(routes::uriage::verify))
        .route(
            "/uriage/verify-history",
            get(routes::uriage::verify_history),
        )
        .route("/uriage/recalc-jobs", get(routes::uriage::list_recalc_jobs));
    Router::new()
        .route("/health", get(routes::health::health))
        .nest("/api", api_routes)
        .layer(Extension(health_state()))
        .layer(Extension(repo))
        .layer(Extension(store))
        .layer(Extension(cakephp))
        .layer(Extension(raw_cfg))
}

pub fn mock_repo() -> DynRepo {
    Arc::new(MockRepo)
}
pub fn error_repo() -> DynRepo {
    Arc::new(ErrorRepo)
}
pub fn query_error_repo() -> DynRepo {
    Arc::new(QueryErrorRepo)
}
