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
#[derive(Debug, Clone, Deserialize)]
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

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            jwt_secret: String::new(),
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
        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let exe_config = exe_dir.join("ichibanboshi.toml");
                if exe_config.exists() {
                    let content = std::fs::read_to_string(&exe_config)?;
                    info!("Loaded config from {}", exe_config.display());
                    return Ok(toml::from_str(&content)?);
                }
            }
        }

        // Fall back to defaults
        Ok(Config {
            port: default_port(),
            bind_addr: default_bind_addr(),
            log_dir: String::new(),
            database: DatabaseConfig::default(),
            auth: AuthConfig::default(),
            cors: CorsConfig::default(),
        })
    }
}
