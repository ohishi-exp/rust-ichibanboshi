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

    // タイムカードの SQLite derived store (Refs #106 Phase 2)。open 失敗は Noop に
    // 落として CakePHP 素通し中継を維持する
    let kintai_store: crate::kintai_store::DynKintaiStore = if config.cakephp.sqlite_path.is_empty()
    {
        tracing::info!("kintai sqlite_path is empty — derived store disabled (relay only)");
        Arc::new(crate::kintai_store::NoopKintaiStore)
    } else {
        match crate::kintai_store::KintaiStore::open(&config.cakephp.sqlite_path) {
            Ok(store) => Arc::new(store),
            Err(e) => {
                tracing::warn!("kintai store open failed — falling back to relay only: {e}");
                Arc::new(crate::kintai_store::NoopKintaiStore)
            }
        }
    };

    // 勤怠の生イベント読み取り (社内 MariaDB 直読み、Refs #116)。未設定なら
    // Disabled を挿して `/api/kintai/events` だけ 503 — 空配列を返して「0 件」に
    // 見せない。pool は lazy なので DB 停止中でも起動は失敗しない
    let kintai_events_repo: crate::kintai_repo::DynKintaiEventsRepo = if config.mariadb.enabled() {
        Arc::new(crate::kintai_repo::MariadbKintaiEventsRepo::new(
            &config.mariadb,
        ))
    } else {
        tracing::info!("mariadb not configured — /api/kintai/events returns 503");
        Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo)
    };

    // 拘束サマリの計算パラメータ (所定 7.5h / 法定 8h / 休憩 10 分。Refs #118)。
    // 就業規則が変わったら TOML で追随できるよう config から取る。
    let kosoku_params = Arc::new(crate::kosoku::KosokuParams {
        break_threshold_minutes: config.kosoku.break_threshold_minutes,
        prescribed_minutes: config.kosoku.prescribed_minutes,
        legal_minutes: config.kosoku.legal_minutes,
    });

    // 拘束サマリ store (Refs #106 Phase 3)。open 失敗は Disabled (route が 503) —
    // このストアはキャッシュではなく relay push の一次置き場のため fail-closed
    let restraint_store: crate::restraint_store::DynRestraintStore =
        if config.restraint.sqlite_path.is_empty() {
            tracing::info!("restraint sqlite_path is empty — /api/restraint/* returns 503");
            Arc::new(crate::restraint_store::DisabledRestraintStore)
        } else {
            match crate::restraint_store::RestraintStore::open(&config.restraint.sqlite_path) {
                Ok(store) => Arc::new(store),
                Err(e) => {
                    tracing::warn!(
                        "restraint store open failed — /api/restraint/* returns 503: {e}"
                    );
                    Arc::new(crate::restraint_store::DisabledRestraintStore)
                }
            }
        };

    // 給与の SQLite derived store (Refs #106 Phase 1)。open 失敗は Noop に落として
    // live 読みへフォールバック — キャッシュの健全性で読み機能を殺さない
    let kyuyo_store: kyuyo::store::DynKyuyoStore = if config.kyuyo.sqlite_path.is_empty() {
        tracing::info!("kyuyo sqlite_path is empty — derived store disabled (live reads only)");
        Arc::new(kyuyo::store::NoopKyuyoStore)
    } else {
        match kyuyo::store::KyuyoStore::open(&config.kyuyo.sqlite_path) {
            Ok(store) => Arc::new(store),
            Err(e) => {
                tracing::warn!("kyuyo store open failed — falling back to live reads: {e}");
                Arc::new(kyuyo::store::NoopKyuyoStore)
            }
        }
    };

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
        .route("/kintai/daily", get(routes::kintai::daily))
        .route("/kintai/events", get(routes::kintai::events))
        .route("/kintai/kosoku-daily", get(routes::kintai::kosoku_daily))
        .route("/kintai/pdf-json", get(routes::kintai::pdf_json))
        .route("/kyuyo/companies", get(routes::kyuyo::companies))
        .route("/kyuyo/databases", get(routes::kyuyo::databases))
        .route("/kyuyo/payroll", get(routes::kyuyo::payroll))
        .route("/kyuyo/employees", get(routes::kyuyo::employees))
        .route("/kyuyo/sync", post(routes::kyuyo::sync))
        .route("/kyuyo/synced-months", get(routes::kyuyo::synced_months))
        .route(
            "/restraint/summaries",
            axum::routing::put(routes::restraint::put_summaries),
        )
        .route(
            "/restraint/wage-source",
            get(routes::restraint::wage_source),
        )
        .route(
            "/restraint/synced-months",
            get(routes::restraint::synced_months),
        );

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
        .layer(Extension(kyuyo_limiter))
        .layer(Extension(kyuyo_store))
        .layer(Extension(kintai_store))
        .layer(Extension(kintai_events_repo))
        .layer(Extension(kosoku_params))
        .layer(Extension(restraint_store));

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
