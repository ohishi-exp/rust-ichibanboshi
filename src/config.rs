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

    /// タイムカード derived store (SQLite、Refs #106 Phase 2) のパス。
    /// 空 = 無効 (常に CakePHP 素通し)。キャッシュ扱い — 消して再 sync で再構築可。
    #[serde(default = "default_kintai_sqlite_path")]
    pub sqlite_path: String,
}

/// 拘束サマリ store configuration (Refs #106 Phase 3)
#[derive(Debug, Clone, Deserialize)]
pub struct RestraintConfig {
    /// 拘束サマリ SQLite のパス。空 = 無効 (push / wage-source は 503)。
    /// relay の resummarize (全月) で再構築できる写し (docs/plan-kyuyo-sqlite-store.md)。
    #[serde(default = "default_restraint_sqlite_path")]
    pub sqlite_path: String,
}

impl Default for RestraintConfig {
    fn default() -> Self {
        Self {
            sqlite_path: default_restraint_sqlite_path(),
        }
    }
}

fn default_restraint_sqlite_path() -> String {
    "/opt/ichibanboshi/restraint_local.sqlite".to_string()
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

/// 給与大臣 (OHKEN) 読み取り configuration (Refs #82)。
///
/// ホスト/ポート/パスワード等の実値は repo に置かず、デプロイ先の
/// `ichibanboshi.toml` で注入する (secrets-inventory 登録)。未設定なら
/// `/api/kyuyo/*` は fail-closed (503)。
#[derive(Debug, Clone, Deserialize)]
pub struct KyuyoConfig {
    /// 給与大臣 PC のホスト (IP / ホスト名)。空 = 機能無効。
    #[serde(default)]
    pub host: String,

    /// OHKEN インスタンスの TCP 固定ポート。
    #[serde(default = "default_kyuyo_port")]
    pub port: u16,

    /// 読み取り専用 SQL ログイン。
    #[serde(default = "default_kyuyo_user")]
    pub user: String,

    #[serde(default)]
    pub password: String,

    /// auth-worker origin (introspect 先)。実稼働 AS は auth-staging 側
    /// (auth.ippoan.org は DCR 503) — デプロイ設定で明示する。
    #[serde(default)]
    pub auth_worker_origin: String,

    /// `/auth/introspect` の shared secret (`INTERNAL_SHARED_SECRET_KYUYO` の生値)。
    #[serde(default)]
    pub introspect_secret: String,

    /// introspect に渡す呼び出しアプリ origin (APP_TENANT_ACL の per-app 判定)。
    #[serde(default = "default_kyuyo_app_origin")]
    pub app_origin: String,

    /// 給与データへのアクセスを許可する email (allowlist)。空 = 全拒否。
    #[serde(default)]
    pub allowed_emails: Vec<String>,

    /// introspect HTTP timeout (秒)。
    #[serde(default = "default_kyuyo_timeout_secs")]
    pub timeout_secs: u64,

    /// 給与 derived store (SQLite) のパス。空 = 無効 (常に live 読み)。
    /// キャッシュ扱い — 消して再 sync で全量再構築できる
    /// (docs/plan-kyuyo-sqlite-store.md)。
    #[serde(default = "default_kyuyo_sqlite_path")]
    pub sqlite_path: String,
}

impl KyuyoConfig {
    /// DB 接続設定が揃っているか (揃っていなければ給与ルートは 503)。
    pub fn db_enabled(&self) -> bool {
        !self.host.is_empty() && !self.user.is_empty() && !self.password.is_empty()
    }

    /// introspect 認可設定が揃っているか (揃っていなければ給与ルートは 503)。
    pub fn auth_configured(&self) -> bool {
        !self.auth_worker_origin.is_empty() && !self.introspect_secret.is_empty()
    }
}

/// 社内 MariaDB (勤怠の生イベント) 読み取り configuration (Refs #116)。
///
/// 同一ホストの docker `db` コンテナ (172.18.21.35 をマスタにしたレプリカ) を
/// loopback で読む。CakePHP が `127.0.0.1:120` で同居しているのと同じ 1 hop で、
/// **DNS も TLS も経路に入らない**。実値は repo に置かず、デプロイ先の
/// `ichibanboshi.toml` で注入する (`KyuyoConfig` と同じ作法)。未設定なら
/// `/api/kintai/events` は fail-closed (503)。
#[derive(Debug, Clone, Deserialize)]
pub struct MariadbConfig {
    /// MariaDB のホスト。空 = 機能無効。
    #[serde(default = "default_mariadb_host")]
    pub host: String,

    #[serde(default = "default_mariadb_port")]
    pub port: u16,

    /// 読み取り専用ユーザー (SELECT のみを GRANT した専用アカウント)。
    #[serde(default = "default_mariadb_user")]
    pub user: String,

    #[serde(default)]
    pub password: String,

    /// CakePHP が使っている DB 名。空 = 機能無効。
    #[serde(default)]
    pub database: String,
}

impl MariadbConfig {
    /// 接続設定が揃っているか (揃っていなければ `/api/kintai/events` は 503)。
    pub fn enabled(&self) -> bool {
        !self.host.is_empty() && !self.database.is_empty() && !self.password.is_empty()
    }
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

    #[serde(default)]
    pub kyuyo: KyuyoConfig,

    #[serde(default)]
    pub restraint: RestraintConfig,

    #[serde(default)]
    pub mariadb: MariadbConfig,

    #[serde(default)]
    pub kosoku: KosokuConfigToml,
}

/// 拘束時間サマリ (`/api/kintai/kosoku-daily`) の計算パラメータ (Refs #118)。
///
/// 既定は就業規則どおり (所定 7.5 時間 / 法定 8 時間 / 休憩は 10 分以上)。
/// 規則が変わったときに再ビルドせず TOML で追随できるよう外に出してある。
#[derive(Debug, Clone, Deserialize)]
pub struct KosokuConfigToml {
    /// 休憩として数える最小の長さ (分)。
    #[serde(default = "default_break_threshold_minutes")]
    pub break_threshold_minutes: i64,

    /// 所定労働時間 (分)。既定 450 = 7.5 時間。
    #[serde(default = "default_prescribed_minutes")]
    pub prescribed_minutes: i64,

    /// 法定労働時間 (分)。既定 480 = 8 時間。所定との差が法定内残業 (割増 1.0)。
    #[serde(default = "default_legal_minutes")]
    pub legal_minutes: i64,

    /// 秒の落とし方。`"truncate_elapsed"` (既定、紙のタイムカード表と同じ) か
    /// `"floor_endpoints"` (従来)。突合で 1 分ずれる原因だったので設定にした
    /// (Refs ohishi-exp/nuxt-dtako-admin#501)。
    #[serde(default = "default_restraint_rounding")]
    pub restraint_rounding: RestraintRoundingToml,
}

/// [`crate::kosoku::RestraintRounding`] の TOML 表現。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestraintRoundingToml {
    /// 経過時間を切り捨てる (紙と同じ)。
    TruncateElapsed,
    /// 両端をそれぞれ切り捨てる (従来)。
    FloorEndpoints,
}

impl From<RestraintRoundingToml> for crate::kosoku::RestraintRounding {
    fn from(v: RestraintRoundingToml) -> Self {
        match v {
            RestraintRoundingToml::TruncateElapsed => Self::TruncateElapsed,
            RestraintRoundingToml::FloorEndpoints => Self::FloorEndpoints,
        }
    }
}

impl Default for KosokuConfigToml {
    fn default() -> Self {
        Self {
            break_threshold_minutes: default_break_threshold_minutes(),
            prescribed_minutes: default_prescribed_minutes(),
            legal_minutes: default_legal_minutes(),
            restraint_rounding: default_restraint_rounding(),
        }
    }
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
fn default_kyuyo_port() -> u16 {
    14330
}
fn default_kyuyo_user() -> String {
    "kyuyo_reader".to_string()
}
fn default_kyuyo_app_origin() -> String {
    "https://dtako.ippoan.org".to_string()
}
fn default_kintai_sqlite_path() -> String {
    "/opt/ichibanboshi/kintai_local.sqlite".to_string()
}

fn default_kyuyo_sqlite_path() -> String {
    "/opt/ichibanboshi/kyuyo_local.sqlite".to_string()
}

fn default_kyuyo_timeout_secs() -> u64 {
    10
}

fn default_mariadb_host() -> String {
    // CakePHP と同居しているホストの docker `db` コンテナ (loopback 1 hop)
    "127.0.0.1".to_string()
}

fn default_mariadb_port() -> u16 {
    3306
}

fn default_mariadb_user() -> String {
    "kintai_reader".to_string()
}

fn default_break_threshold_minutes() -> i64 {
    10
}

fn default_prescribed_minutes() -> i64 {
    // 所定 7.5 時間
    450
}

fn default_legal_minutes() -> i64 {
    // 法定 8 時間
    480
}

fn default_restraint_rounding() -> RestraintRoundingToml {
    // 紙のタイムカード表に合わせる
    RestraintRoundingToml::TruncateElapsed
}

impl Default for MariadbConfig {
    fn default() -> Self {
        Self {
            host: default_mariadb_host(),
            port: default_mariadb_port(),
            user: default_mariadb_user(),
            password: String::new(),
            database: String::new(),
        }
    }
}

impl Default for KyuyoConfig {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: default_kyuyo_port(),
            user: default_kyuyo_user(),
            password: String::new(),
            auth_worker_origin: String::new(),
            introspect_secret: String::new(),
            app_origin: default_kyuyo_app_origin(),
            allowed_emails: Vec::new(),
            timeout_secs: default_kyuyo_timeout_secs(),
            sqlite_path: default_kyuyo_sqlite_path(),
        }
    }
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
            sqlite_path: default_kintai_sqlite_path(),
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
            kyuyo: KyuyoConfig::default(),
            restraint: RestraintConfig::default(),
            mariadb: MariadbConfig::default(),
            kosoku: KosokuConfigToml::default(),
        })
    }
}
