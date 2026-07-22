use std::sync::Arc;

use axum::{
    routing::{get, post},
    Extension, Router,
};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::auth::JwtSecret;
use crate::cakephp::CakephpClient;
use crate::config::{Config, RawConfig};
use crate::db;
use crate::kyuyo;
use crate::repo::TiberiusRepo;
use crate::routes;
use crate::sqlite::{DynLocalStore, LocalStore};

/// HTTP サーバーを起動し、shutdown token が cancel されるまでブロック
pub async fn run(
    config: Config,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = db::create_pool(&config.database).await?;
    let repo: crate::repo::DynRepo = Arc::new(TiberiusRepo::new(pool));

    // SQLite local store (Phase 2、担当者別売上 summary 永続化)
    let local_store: DynLocalStore = Arc::new(LocalStore::open(&config.sqlite.path)?);

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

    // 給与大臣 (OHKEN) 読み取り (#82)。未設定なら stub を挿して該当ルートだけ 503。
    // pool は起動時テストなし — 給与大臣 PC 停止でも本サービス全体は起動する
    let kyuyo_repo: kyuyo::repo::DynKyuyoRepo = if config.kyuyo.db_enabled() {
        let pool = kyuyo::repo::create_kyuyo_pool(&config.kyuyo).await?;
        Arc::new(kyuyo::repo::TiberiusKyuyoRepo::new(pool))
    } else {
        tracing::info!("kyuyo database not configured — /api/kyuyo/* returns 503");
        Arc::new(kyuyo::repo::NotConfiguredKyuyoRepo)
    };
    let kyuyo_auth = Arc::new(kyuyo::introspect::KyuyoAuthState::from_config(
        &config.kyuyo,
    ));
    // 給与 DB (OHKEN、非力な PC) を触る区間の同時実行制限 (Refs #369)
    let kyuyo_limiter = Arc::new(routes::kyuyo::KyuyoLimiter::new());

    let jwt_secret = JwtSecret(config.auth.jwt_secret.clone());

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
        .route("/sales/monthly", get(routes::sales::monthly))
        .route("/sales/by-department", get(routes::sales::by_department))
        .route("/sales/by-customer", get(routes::sales::by_customer))
        .route("/sales/yoy", get(routes::sales::yoy))
        .route("/sales/daily", get(routes::sales::daily))
        .route("/sales/customer-trend", get(routes::sales::customer_trend))
        .route("/sales/customer-yoy", get(routes::sales::customer_yoy))
        .route(
            "/sales/customer-yoy-by-dept",
            get(routes::sales::customer_yoy_by_dept),
        )
        .route(
            "/sales/departments",
            get(routes::sales::list_departments_handler),
        )
        .route(
            "/sales/customer-detail",
            get(routes::sales::customer_detail),
        )
        .route(
            "/sales/vehicle-daily",
            get(routes::vehicle_daily::vehicle_daily),
        )
        .route("/surcharge/base", get(routes::surcharge::surcharge_base))
        .route("/vehicles", get(routes::surcharge::vehicles))
        .route("/employees", get(routes::employees::employees))
        .route("/unchin/candidates", get(routes::unchin::unchin_candidates))
        .route("/unchin/summary", get(routes::unchin::unchin_summary))
        .route(
            "/unchin/subcontractor-net",
            get(routes::unchin::unchin_subcontractor_net),
        )
        .route(
            "/unchin/subcontractor-net-detail",
            get(routes::unchin::unchin_subcontractor_net_detail),
        )
        .route(
            "/unchin/customer-net",
            get(routes::unchin::unchin_customer_net),
        )
        .route(
            "/unchin/customer-net-detail",
            get(routes::unchin::unchin_customer_net_detail),
        )
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
        .route("/uriage/recalc-jobs", get(routes::uriage::list_recalc_jobs))
        .route("/kyuyo/companies", get(routes::kyuyo::companies))
        .route("/kyuyo/databases", get(routes::kyuyo::databases))
        .route("/kyuyo/payroll", get(routes::kyuyo::payroll));

    let schema_routes = Router::new()
        .route("/schema/tables", get(routes::schema::list_tables))
        .route("/schema/columns", get(routes::schema::list_columns))
        .route("/schema/sample", get(routes::schema::sample_data));

    let app = Router::new()
        .route("/health", get(routes::health::health))
        .nest("/api", api_routes)
        .nest("/api", schema_routes)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(Extension(repo))
        .layer(Extension(local_store))
        .layer(Extension(cakephp_client))
        .layer(Extension(raw_cfg))
        .layer(Extension(jwt_secret))
        .layer(Extension(kyuyo_repo))
        .layer(Extension(kyuyo_auth))
        .layer(Extension(kyuyo_limiter));

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
