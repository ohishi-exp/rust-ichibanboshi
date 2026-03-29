use std::sync::Arc;

use axum::{routing::get, Extension, Router};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::auth::JwtSecret;
use crate::config::Config;
use crate::db;
use crate::repo::TiberiusRepo;
use crate::routes;

/// HTTP サーバーを起動し、shutdown token が cancel されるまでブロック
pub async fn run(
    config: Config,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = db::create_pool(&config.database).await?;
    let repo: crate::repo::DynRepo = Arc::new(TiberiusRepo::new(pool));

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
        .route(
            "/sales/customer-trend",
            get(routes::sales::customer_trend),
        )
        .route("/sales/customer-yoy", get(routes::sales::customer_yoy))
        .route(
            "/sales/customer-detail",
            get(routes::sales::customer_detail),
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
        .layer(Extension(jwt_secret));

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
