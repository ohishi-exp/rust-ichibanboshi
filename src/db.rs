use bb8::Pool;
use bb8_tiberius::ConnectionManager;
use tiberius::{Config as TiberiusConfig, EncryptionLevel};

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

    // TLS — PHP の encrypt=optional 相当。オンプレ SQL Server は暗号化不要
    if config.trust_server_certificate {
        tib_config.encryption(EncryptionLevel::NotSupported);
    }

    // 名前付きインスタンスの場合は using_named_connection() で SQL Browser 経由接続
    let manager = if config.port.is_none() && !config.instance.is_empty() {
        ConnectionManager::new(tib_config).using_named_connection()
    } else {
        ConnectionManager::new(tib_config)
    };

    let pool = Pool::builder()
        .max_size(4)
        .connection_timeout(std::time::Duration::from_secs(30))
        .build(manager)
        .await?;

    tracing::info!(
        "SQL Server pool created: {}\\{} / {}",
        config.host,
        config.instance,
        config.database
    );

    // 起動時に接続テスト
    match pool.get().await {
        Ok(mut conn) => {
            conn.simple_query("SELECT 1").await?;
            tracing::info!("SQL Server connection test: OK");
        }
        Err(e) => {
            tracing::error!("SQL Server connection test FAILED: {e}");
            return Err(format!("DB connection failed: {e}").into());
        }
    }

    Ok(pool)
}
