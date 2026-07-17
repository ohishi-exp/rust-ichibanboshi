use clap::Parser;
use serde::Deserialize;
use tracing::info;

/// CLI arguments
#[derive(Parser, Debug, Clone)]
#[command(name = "ichibanboshi")]
#[command(about = "一番星 売上データ API — SQL Server bridge")]
pub struct AppArgs {
    /// Run in console mode instead of Windows Service mode
    #[arg(long, default_value_t = false)]
    pub console: bool,

    /// Path to config file (TOML)
    #[arg(long)]
    pub config: Option<String>,

    /// HTTP server port (overrides config file)
    #[arg(long)]
    pub port: Option<u16>,

    /// HTTP bind address (overrides config file)
    #[arg(long)]
    pub bind_addr: Option<String>,
}

/// Database configuration
#[derive(Debug, Clone, Deserialize)]
pub struct DatabaseConfig {
    #[serde(default = "default_db_host")]
    pub host: String,

    #[serde(default = "default_db_instance")]
    pub instance: String,

    #[serde(default = "default_db_name")]
    pub database: String,

    #[serde(default)]
    pub user: String,

    #[serde(default)]
    pub password: String,

    /// Direct TCP port (use instead of named instance if set)
    pub port: Option<u16>,

    #[serde(default = "default_true")]
    pub trust_server_certificate: bool,
}

/// Auth configuration
#[derive(Debug, Clone, Deserialize, Default)]
pub struct AuthConfig {
    /// JWT secret — must match rust-alc-api's JWT_SECRET
    #[serde(default)]
    pub jwt_secret: String,
}

/// CORS configuration
#[derive(Debug, Clone, Deserialize)]
pub struct CorsConfig {
    /// Allowed origins for CORS
    #[serde(default = "default_allowed_origins")]
    pub allowed_origins: Vec<String>,
}

/// SQLite local store configuration (Phase 2: 担当者別売上 summary 永続化、issue #762)
#[derive(Debug, Clone, Deserialize)]
pub struct SqliteConfig {
    /// SQLite データベースファイルのパス。`:memory:` で in-memory (テスト用)。
    /// 本番デフォルトは `/var/lib/ichibanboshi/state.db`。
    #[serde(default = "default_sqlite_path")]
    pub path: String,
}

/// CakePHP fetch configuration (Phase 2: masters / editable-months pull、issue #762)
#[derive(Debug, Clone, Deserialize)]
pub struct CakephpConfig {
    /// CakePHP base URL (例: `https://ohishi-dev.ohishi.local/uriage-jyuchu-display`)。
    /// 社内 LAN 内で到達可、token 不要。空文字なら `/recalc` などの依存 endpoint が 503 を返す。
    #[serde(default)]
    pub base_url: String,
    /// HTTP request timeout (秒)。default 30 秒
    #[serde(default = "default_cakephp_timeout_secs")]
    pub timeout_secs: u64,
}

/// Raw NDJSON.gz 出力 configuration (Phase 2: R2 warm backup の input、issue #762)
#[derive(Debug, Clone, Deserialize)]
pub struct RawConfig {
    /// 生 NDJSON.gz の出力ディレクトリ (例: `/opt/ichibanboshi/raw/`)。
    /// `recalc_jobs.raw_path = ${dir}/YYYY-MM/eigyosho-{id}.ndjson.gz`。
    /// 親 dir が無い場合は auto-create。
    #[serde(default = "default_raw_dir")]
    pub dir: String,
}

/// Runtime configuration
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,

    #[serde(default)]
    #[cfg_attr(not(windows), allow(dead_code))]
    pub log_dir: String,

    #[serde(default)]
    pub database: DatabaseConfig,

    #[serde(default)]
    pub auth: AuthConfig,

    #[serde(default)]
    pub cors: CorsConfig,

    #[serde(default)]
    pub sqlite: SqliteConfig,

    #[serde(default)]
    pub cakephp: CakephpConfig,

    #[serde(default)]
    pub raw: RawConfig,
}

fn default_port() -> u16 {
    3100
}
fn default_bind_addr() -> String {
    "127.0.0.1".to_string()
}
fn default_db_host() -> String {
    "localhost".to_string()
}
fn default_db_instance() -> String {
    "softec".to_string()
}
fn default_db_name() -> String {
    "CAPE#01".to_string()
}
fn default_true() -> bool {
    true
}
fn default_allowed_origins() -> Vec<String> {
    vec!["https://ichibanboshi.mtamaramu.com".to_string()]
}
fn default_sqlite_path() -> String {
    // VPS では `/opt/ichibanboshi/` が binary 配置先 (ubuntu 所有、CLAUDE.md 参照)。
    // ここに state.db を置けば追加の mkdir / chown 無しで動く。`/var/lib/ichibanboshi/`
    // は root 所有ディレクトリ配下で ubuntu が mkdir できず crash-loop した実害あり (#33 後)。
    "/opt/ichibanboshi/state.db".to_string()
}
fn default_cakephp_timeout_secs() -> u64 {
    30
}
fn default_raw_dir() -> String {
    "/opt/ichibanboshi/raw".to_string()
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            host: default_db_host(),
            instance: default_db_instance(),
            database: default_db_name(),
            user: String::new(),
            password: String::new(),
            port: None,
            trust_server_certificate: true,
        }
    }
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allowed_origins: default_allowed_origins(),
        }
    }
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: default_sqlite_path(),
        }
    }
}

impl Default for CakephpConfig {
    fn default() -> Self {
        Self {
            base_url: String::new(),
            timeout_secs: default_cakephp_timeout_secs(),
        }
    }
}

impl Default for RawConfig {
    fn default() -> Self {
        Self {
            dir: default_raw_dir(),
        }
    }
}

impl Config {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind_addr, self.port)
    }

    /// Load config from file, then apply CLI overrides
    pub fn from_args_and_file(args: &AppArgs) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = if let Some(ref path) = args.config {
            let content = std::fs::read_to_string(path)?;
            info!("Loaded config from {}", path);
            toml::from_str(&content)?
        } else {
            Self::load_default_locations()?
        };

        // CLI overrides
        if let Some(port) = args.port {
            config.port = port;
        }
        if let Some(ref addr) = args.bind_addr {
            config.bind_addr = addr.clone();
        }

        Ok(config)
    }

    /// Load from standard locations (service mode)
    pub fn load_default_locations() -> Result<Self, Box<dyn std::error::Error>> {
        let exe_config = std::env::current_exe()
            .ok()
            .and_then(|p| p.parent().map(|d| d.join("ichibanboshi.toml")));

        if let Some(path) = exe_config.filter(|p| p.exists()) {
            let content = std::fs::read_to_string(&path)?;
            info!("Loaded config from {}", path.display());
            return Ok(toml::from_str(&content)?);
        }

        // Fall back to defaults
        Ok(Config {
            port: default_port(),
            bind_addr: default_bind_addr(),
            log_dir: String::new(),
            database: DatabaseConfig::default(),
            auth: AuthConfig::default(),
            cors: CorsConfig::default(),
            sqlite: SqliteConfig::default(),
            cakephp: CakephpConfig::default(),
            raw: RawConfig::default(),
        })
    }
}
