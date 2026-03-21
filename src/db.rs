use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use tiberius::Config as TiberiusConfig;

use crate::config::DatabaseConfig;

pub type DbPool = Pool<ConnectionManager>;

/// SQL Server 接続プールを作成
pub async fn create_pool(config: &DatabaseConfig) -> Result<DbPool, Box<dyn std::error::Error>> {
    let mut tib_config = TiberiusConfig::new();

    tib_config.host(&config.host);
    tib_config.database(&config.database);

    // Named instance or direct port
    if let Some(port) = config.port {
        tib_config.port(port);
    } else {
        tib_config.instance_name(&config.instance);
    }

    // Authentication
    if !config.user.is_empty() {
        tib_config.authentication(tiberius::AuthMethod::sql_server(
            &config.user,
            &config.password,
        ));
    }

    // TLS — trust self-signed certs (on-premise SQL Server)
    if config.trust_server_certificate {
        tib_config.trust_cert();
    }

    let manager = ConnectionManager::new(tib_config);
    let pool = Pool::builder().max_size(4).build(manager).await?;

    tracing::info!(
        "SQL Server pool created: {}\\{} / {}",
        config.host,
        config.instance,
        config.database
    );

    Ok(pool)
}
