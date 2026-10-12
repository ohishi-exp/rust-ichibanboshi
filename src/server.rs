use std::sync::Arc;

use axum::{
    routing::{get, post},
    Extension, Router,
};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::cakephp::CakephpClient;
use crate::config::{Config, RawConfig};
use crate::db;
use crate::repo::TiberiusRepo;
use crate::routes;
use crate::sqlite::{DynLocalStore, LocalStore};

/// HTTP サーバーを起動し、shutdown token が cancel されるまでブロック
pub async fn run(
    config: Config,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    // SQL Server (CAPE#01) を**使うと宣言したか**で分岐する (オンプレ / GCP 両対応)。
    // 宣言した場合は create_pool が起動時に `SELECT 1` を打ち、繋がらなければ
    // ここで Err → 起動失敗 (オンプレの既定・従来どおり)。宣言していない場合は
    // pool を作らず、SQL Server 依存ルートは全て 503 fail-closed になる。
    let repo: crate::repo::DynRepo = if config.database.enabled {
        let pool = db::create_pool(&config.database).await?;
        Arc::new(TiberiusRepo::new(pool))
    } else {
        tracing::warn!(
            "[database] enabled = false — this instance does not use SQL Server (CAPE#01). \
             /api/uriage/* \
             return 503. /health reports backends.sqlserver=disabled."
        );
        Arc::new(TiberiusRepo::disabled())
    };

    // SQLite local store (Phase 2、担当者別売上 summary 永続化)。
    // **path が空なら file を作らない** — Cloud Run の揮発 FS に空の state.db を
    // 作ると `/api/uriage/*` が 0 件を返し、`recalc_jobs` (fingerprint と R2 同期
    // 状態の唯一の記録) が毎回まっさらから始まる (Refs #205 の G4)
    let local_store: DynLocalStore = if config.sqlite.path.trim().is_empty() {
        tracing::info!("sqlite path is empty — local store disabled (/api/uriage/* returns 503)");
        Arc::new(LocalStore::disabled())
    } else {
        Arc::new(LocalStore::open(&config.sqlite.path)?)
    };

    // CakePHP fetch client (Phase 2、masters / editable-months pull)
    // base_url 空でも build は成功し、各 endpoint で is_enabled() を見て 503 を返す
    let cakephp_client = Arc::new(CakephpClient::new(
        config.cakephp.base_url.clone(),
        config.cakephp.timeout_secs,
    )?);

    // 生 NDJSON.gz 出力先 (Phase 2、R2 warm backup の input)
    let raw_cfg = Arc::new(RawConfig {
        dir: config.raw.dir.clone(),
    });

    // 宣言したバックエンドの一覧を /health に出す (実行形態を外から判別可能にする)。
    let health_state = crate::routes::health::HealthState {
        sqlserver: config.database.enabled,
    };

    let origins: Vec<_> = config
        .cors
        .allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(AllowMethods::any())
        .allow_headers(AllowHeaders::any());

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
        .route("/uriage/verify-debug", get(routes::uriage::verify_debug))
        .route(
            "/uriage/verify-history",
            get(routes::uriage::verify_history),
        )
        .route("/uriage/recalc-jobs", get(routes::uriage::list_recalc_jobs));

    let app = Router::new()
        .route("/health", get(routes::health::health))
        .nest("/api", api_routes)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(Extension(repo))
        .layer(Extension(health_state))
        .layer(Extension(local_store))
        .layer(Extension(cakephp_client))
        .layer(Extension(raw_cfg));

    let addr = config.addr();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Listening on {addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            tracing::info!("Shutdown signal received");
        })
        .await?;

    Ok(())
}
