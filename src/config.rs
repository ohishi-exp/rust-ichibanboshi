use clap::Parser;
use serde::Deserialize;
use tracing::info;

/// CLI arguments
#[derive(Parser, Debug, Clone)]
#[command(name = "ichibanboshi")]
#[command(about = "一番星 売上データ API — SQL Server bridge")]
pub struct AppArgs {
    /// 互換のため受け付ける (現在は常に console)
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
    /// この instance が SQL Server (CAPE#01) を**使うと宣言するか**。
    ///
    /// 既定 `true` = オンプレ (ohishi-data / systemd) の形。宣言したものには
    /// 起動時に必ず接続でき、繋がらなければ**起動失敗**する (早期に気付ける)。
    ///
    /// `false` = SQL Server を持たない実行形態 (Cloud Run 等) の形。pool を
    /// 作らず、SQL Server 依存ルートは全て 503 fail-closed になり、`/health`
    /// は `backends.sqlserver = "disabled"` と明示して 200 を返す。
    /// 「起動はするが実は繋がっていない」という静かな degraded は作らない。
    #[serde(default = "default_true")]
    pub enabled: bool,

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
    /// 既定は `/opt/ichibanboshi/state.db` ([`default_sqlite_path`])。
    ///
    /// **空 = 置き場を持たないと宣言する。** file を作らず `/api/uriage/*` が 503 に
    /// なる (Refs #205 の G4)。GCP (Cloud Run) の instance 向け — SQL Server
    /// (CAPE#01) に到達できないので売上を計算できず、揮発 FS に空の `state.db` を
    /// 作ると「壊れている」ではなく「0 件」を返してしまう。とくに `recalc_jobs` は
    /// `fingerprint` と R2 同期状態の唯一の記録なので、空で立ち上がると差分再計算の
    /// 基準が消える。オンプレは既定のまま (宣言済み) で挙動不変。
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
///
/// **知らない節は読み飛ばす** (`deny_unknown_fields` を付けない)。オンプレの
/// `ichibanboshi.toml` には撤去済みの節 (`[auth]` `[kyuyo]` と勤怠の `[mariadb]`
/// `[kintai_events]` `[kintai_push]` `[kosoku]` `[restraint]` `[dtako_day_links]`、
/// `[cakephp] sqlite_path`) が残っており、読めないと起動ごと落ちて売上も止まる
/// (Refs #322。`tests/config_test.rs` が固定する)。
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,

    #[serde(default)]
    pub database: DatabaseConfig,

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
            enabled: true,
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

/// 環境変数の引き当て。`std::env::var` を直接呼ばずに関数で受けるのは、
/// テストが**プロセス全体の環境変数を汚さずに済む**ようにするため
/// (`cargo test` はスレッド並列なので `set_var` は他のテストとレースする)。
pub type EnvLookup<'a> = &'a dyn Fn(&str) -> Option<String>;

/// 環境変数が**在るかどうか**だけで上書きを決める (値が空でも空で上書きする)。
///
/// 空を「未設定」に落とすと、`secretKeyRef` の配線ミスで空が入ったときに
/// TOML の値へ静かに戻ってしまい、どちらが効いているのか外から分からなくなる。
/// 空で上書きすれば「password が空 → 起動時接続に失敗 → 起動失敗」と loud に出る。
fn env_str(get: EnvLookup, key: &str) -> Option<String> {
    get(key)
}

/// カンマ区切りのリスト。前後の空白は落とし、空要素は捨てる。
/// `KEY=""` は「空リストを明示した」と解釈する (例: CORS 全拒否)。
fn env_list(get: EnvLookup, key: &str) -> Option<Vec<String>> {
    get(key).map(|raw| {
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    })
}

/// 数値。壊れた値は**黙って無視せず起動を失敗させる** (静かな誤設定を作らない)。
fn env_u16(get: EnvLookup, key: &str) -> Result<Option<u16>, String> {
    match get(key) {
        None => Ok(None),
        Some(raw) => raw
            .trim()
            .parse::<u16>()
            .map(Some)
            .map_err(|e| format!("{key}: invalid number {raw:?} ({e})")),
    }
}

/// `Option<u16>` の項目 (`[database] port`) 用。空文字は「未指定に戻す」
/// = 名前付きインスタンス経由に切り替える、という意味を持たせる。
#[allow(clippy::type_complexity)]
fn env_opt_u16(get: EnvLookup, key: &str) -> Result<Option<Option<u16>>, String> {
    match get(key) {
        None => Ok(None),
        Some(raw) if raw.trim().is_empty() => Ok(Some(None)),
        Some(raw) => raw
            .trim()
            .parse::<u16>()
            .map(|v| Some(Some(v)))
            .map_err(|e| format!("{key}: invalid number {raw:?} ({e})")),
    }
}

/// 真偽値。`true` / `false` / `1` / `0` (大文字小文字を問わない) のみ受ける。
fn env_bool(get: EnvLookup, key: &str) -> Result<Option<bool>, String> {
    match get(key) {
        None => Ok(None),
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(Some(true)),
            "1" => Ok(Some(true)),
            "false" => Ok(Some(false)),
            "0" => Ok(Some(false)),
            _ => Err(format!(
                "{key}: invalid boolean {raw:?} (expected true/false/1/0)"
            )),
        },
    }
}

impl Config {
    pub fn addr(&self) -> String {
        format!("{}:{}", self.bind_addr, self.port)
    }

    /// 環境変数による上書き。
    ///
    /// TOML と環境変数は**対等な入力経路**で、どちらか片方だけで完結して起動できる
    /// (オンプレ = TOML 主体 / Cloud Run = env 主体)。両方に値がある場合の優先順位は
    /// **CLI 引数 > 環境変数 > TOML > 既定値**。環境変数を TOML より上に置くのは、
    /// コンテナに焼かれた TOML が古いままでも、プラットフォームが注入した値
    /// (Secret Manager → `secretKeyRef`) が必ず勝つようにするため。オンプレは
    /// 環境変数を一切設定しないので TOML が完全に主導権を持ち、挙動は変わらない。
    ///
    /// 対象は**秘匿値と、実行形態の判別に要る項目**に絞ってある。
    pub fn apply_env_overrides(&mut self, get: EnvLookup) -> Result<(), String> {
        // ── HTTP listener (Cloud Run は PORT を注入し、0.0.0.0 での listen を要求する) ──
        if let Some(v) = env_u16(get, "PORT")? {
            self.port = v;
        }
        if let Some(v) = env_str(get, "BIND_ADDR") {
            self.bind_addr = v;
        }

        // ── ローカル状態の置き場 (Refs #205 の G4) ──
        // **`SQLITE_PATH=""` で「置き場を持たない」と宣言できる**必要がある。
        // イメージに TOML を焼かない方針だったので、env で宣言できないと
        // Cloud Run は既定の `/opt/ichibanboshi/state.db` を使い、揮発 FS に空の
        // state.db を作って `/api/uriage/*` が 0 件を返す。`env_str` は空でも
        // 上書きするので、空文字が「未設定」に落ちない
        if let Some(v) = env_str(get, "SQLITE_PATH") {
            self.sqlite.path = v;
        }

        // ── SQL Server (CAPE#01) ──
        // alc は DATABASE_URL 1 本だが、こちらは項目別にした。tiberius は
        // 名前付きインスタンス (`using_named_connection()`) と暗号化レベルを
        // 個別に組む必要があり、URL 1 本で表せる形になっていないため。
        if let Some(v) = env_bool(get, "DATABASE_ENABLED")? {
            self.database.enabled = v;
        }
        if let Some(v) = env_str(get, "DATABASE_HOST") {
            self.database.host = v;
        }
        if let Some(v) = env_str(get, "DATABASE_INSTANCE") {
            self.database.instance = v;
        }
        if let Some(v) = env_str(get, "DATABASE_NAME") {
            self.database.database = v;
        }
        if let Some(v) = env_str(get, "DATABASE_USER") {
            self.database.user = v;
        }
        if let Some(v) = env_str(get, "DATABASE_PASSWORD") {
            self.database.password = v;
        }
        if let Some(v) = env_opt_u16(get, "DATABASE_PORT")? {
            self.database.port = v;
        }
        if let Some(v) = env_bool(get, "DATABASE_TRUST_SERVER_CERTIFICATE")? {
            self.database.trust_server_certificate = v;
        }

        // ── その他の秘匿値 / 外部接続先 ──
        // JWT_SECRET は持たない。到達不能だった HS256 自前検証は #207 で撤去済みで、
        // 認可は Cloudflare Access (オンプレ) / Cloud Run IAM (GCP) が担う
        if let Some(v) = env_str(get, "CAKEPHP_BASE_URL") {
            self.cakephp.base_url = v;
        }
        if let Some(v) = env_list(get, "CORS_ALLOWED_ORIGINS") {
            self.cors.allowed_origins = v;
        }

        Ok(())
    }

    /// Load config from file, then apply env var and CLI overrides
    ///
    /// 優先順位は **CLI 引数 > 環境変数 > TOML > 既定値** (`apply_env_overrides` 参照)。
    pub fn from_args_and_file(args: &AppArgs) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = if let Some(ref path) = args.config {
            let content = std::fs::read_to_string(path)?;
            info!("Loaded config from {}", path);
            toml::from_str(&content)?
        } else {
            Self::load_default_locations()?
        };

        // env overrides (TOML より上、CLI より下)
        config.apply_env_overrides(&|k| std::env::var(k).ok())?;

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
            database: DatabaseConfig::default(),
            cors: CorsConfig::default(),
            sqlite: SqliteConfig::default(),
            cakephp: CakephpConfig::default(),
            raw: RawConfig::default(),
        })
    }
}
