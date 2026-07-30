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

/// 勤怠の生イベントをどこから読むか (Refs #205 実装計画 02)。
///
/// オンプレ (`ohishi-data`) は社内 MariaDB を直読みできるが、GCP (Cloud Run) からは
/// 到達できない。そこで**読み先を宣言で切り替える** — `source = "http"` にすると
/// `rust-alc-api` の `GET /api/dtako/events` (ippoan/rust-alc-api#578) から生 CSV 行を
/// 取り、`kintai_repo` の戻り値の形に写す ([`crate::kintai_http_repo`])。
///
/// **既定は `mariadb`** なのでオンプレの挙動は 1 バイトも変わらない。MariaDB 実装は
/// 撤去せず残す — 上流に口が無い読み出し (打刻 / フェリー) の委譲先になり、
/// HTTP 経路の出力を MariaDB と突き合わせて検証する足場でもある (#205 の G6)。
#[derive(Debug, Clone, Deserialize)]
pub struct KintaiEventsConfig {
    /// 読み先の宣言。`"mariadb"` (既定) / `"http"`。
    #[serde(default)]
    pub source: KintaiEventsSource,

    /// `rust-alc-api` の origin (例 `https://rust-alc-api-xxxx.asia-northeast1.run.app`)。
    /// 末尾の `/` は付けても付けなくてもよい。`source = "http"` では必須。
    #[serde(default)]
    pub base_url: String,

    /// 上流に渡す `X-Tenant-ID` (一番星のテナント UUID)。`source = "http"` では必須。
    ///
    /// 上流の tenant 経路は「前段が検証済み identity をヘッダーで注入する」前提の
    /// dumb backend (`alc-core` の `require_tenant_header`) なので、直叩きする
    /// こちらがテナントを名乗る。網層のロックダウンは Cloud Run IAM が担う。
    #[serde(default)]
    pub tenant_id: String,

    /// HTTP request timeout (秒)。全乗務員 1 か月は R2 GET 約 1,100 回を上流が
    /// 並列で回すため、CakePHP 中継 (30 秒) より長めの既定にしてある。
    #[serde(default = "default_kintai_events_timeout_secs")]
    pub timeout_secs: u64,

    /// 静的な Bearer token。**取得方法をコードに焼かないための 2 経路のうち片方**で、
    /// device JWT へ差し替える 07 の受け皿でもある (#205 実装計画 07)。
    #[serde(default)]
    pub auth_token: String,

    /// token を吐くコマンド (例 `gcloud auth print-identity-token`)。当面の Google
    /// 認証はこちら。**シェルは経由しない** — 空白で argv に分割して直接 exec する
    /// ので、パイプ・リダイレクト・クォートは使えない (使えたら設定が注入経路になる)。
    #[serde(default)]
    pub auth_token_command: String,

    /// `auth_token_command` の結果をキャッシュする秒数。Google の identity token は
    /// 1 時間有効なので、既定 900 秒なら毎リクエストで `gcloud` を起こさずに済む。
    #[serde(default = "default_kintai_events_token_ttl_secs")]
    pub auth_token_ttl_secs: u64,
}

/// [`KintaiEventsConfig::source`] の TOML / env 表現。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum KintaiEventsSource {
    /// 社内 MariaDB 直読み (既定 = オンプレの形、Refs #116)。
    #[default]
    Mariadb,
    /// `rust-alc-api` の `GET /api/dtako/events` 経由 (GCP の形、Refs #205 の 02)。
    Http,
}

impl KintaiEventsConfig {
    /// HTTP 読み先を使うと宣言したか。
    pub fn http_enabled(&self) -> bool {
        self.source == KintaiEventsSource::Http
    }

    /// 宣言の整合性検査。**足りない設定を黙って既定へ落とさず起動を失敗させる**
    /// (`[database] enabled` と同じ方針 — 「起動はしているが実は読めていない」を作らない)。
    pub fn validate(&self) -> Result<(), String> {
        if !self.http_enabled() {
            return Ok(());
        }
        if self.base_url.trim().is_empty() {
            return Err("[kintai_events] source = \"http\" requires base_url".to_string());
        }
        if self.tenant_id.trim().is_empty() {
            return Err("[kintai_events] source = \"http\" requires tenant_id".to_string());
        }
        if !self.auth_token.is_empty() && !self.auth_token_command.is_empty() {
            return Err(
                "[kintai_events] set only one of auth_token / auth_token_command".to_string(),
            );
        }
        Ok(())
    }
}

impl Default for KintaiEventsConfig {
    fn default() -> Self {
        Self {
            source: KintaiEventsSource::default(),
            base_url: String::new(),
            tenant_id: String::new(),
            timeout_secs: default_kintai_events_timeout_secs(),
            auth_token: String::new(),
            auth_token_command: String::new(),
            auth_token_ttl_secs: default_kintai_events_token_ttl_secs(),
        }
    }
}

fn default_kintai_events_timeout_secs() -> u64 {
    120
}

fn default_kintai_events_token_ttl_secs() -> u64 {
    900
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

    /// 生イベントの読み先の宣言 (Refs #205 実装計画 02)。既定は MariaDB 直読み。
    #[serde(default)]
    pub kintai_events: KintaiEventsConfig,

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

    /// 秒の落とし方。`"paper_per_segment"` (既定、紙のタイムカード表と同じ区分ごとの
    /// 切り捨て。Refs #182) / `"truncate_elapsed"` (従来方式 = 勤務単位の経過切り捨て) /
    /// `"floor_endpoints"` (両端床)。突合で 1 分ずれる原因だったので設定にした
    /// (Refs ohishi-exp/nuxt-dtako-admin#501)。
    #[serde(default = "default_restraint_rounding")]
    pub restraint_rounding: RestraintRoundingToml,
}

/// [`crate::kosoku::RestraintRounding`] の TOML 表現。
#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RestraintRoundingToml {
    /// 区分ごとに紙の流儀で切り捨てる (紙と同じ、Refs #182)。
    PaperPerSegment,
    /// 経過時間を切り捨てる (勤務単位の近似。#182 の「従来方式」)。
    TruncateElapsed,
    /// 両端をそれぞれ切り捨てる (従来)。
    FloorEndpoints,
}

impl From<RestraintRoundingToml> for crate::kosoku::RestraintRounding {
    fn from(v: RestraintRoundingToml) -> Self {
        match v {
            RestraintRoundingToml::PaperPerSegment => Self::PaperPerSegment,
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
    // 紙のタイムカード表に合わせる (区分ごとの切り捨て、Refs #182)
    RestraintRoundingToml::PaperPerSegment
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

/// 秒数などの `u64` 項目。`env_u16` と同じく壊れた値で起動を失敗させる。
fn env_u64(get: EnvLookup, key: &str) -> Result<Option<u64>, String> {
    match get(key) {
        None => Ok(None),
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|e| format!("{key}: invalid number {raw:?} ({e})")),
    }
}

/// 生イベントの読み先の宣言。**知らない値は既定へ落とさず起動を失敗させる**
/// (`KINTAI_EVENTS_SOURCE=htpp` が静かに MariaDB 読みになると事故る)。
fn env_kintai_events_source(
    get: EnvLookup,
    key: &str,
) -> Result<Option<KintaiEventsSource>, String> {
    match get(key) {
        None => Ok(None),
        Some(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "mariadb" => Ok(Some(KintaiEventsSource::Mariadb)),
            "http" => Ok(Some(KintaiEventsSource::Http)),
            _ => Err(format!(
                "{key}: invalid source {raw:?} (expected mariadb/http)"
            )),
        },
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
    /// 対象は**秘匿値と、実行形態の判別に要る項目**に絞ってある。SQLite のパスや
    /// `[kosoku]` の就業規則パラメータは意図的に対象外 (理由は docs ではなく
    /// 各フィールドのコメント、および `[kosoku]` は `kintai_version` の ETag に
    /// 畳み込まれているので環境ごとに変わると意味が壊れる)。
    pub fn apply_env_overrides(&mut self, get: EnvLookup) -> Result<(), String> {
        // ── HTTP listener (Cloud Run は PORT を注入し、0.0.0.0 での listen を要求する) ──
        if let Some(v) = env_u16(get, "PORT")? {
            self.port = v;
        }
        if let Some(v) = env_str(get, "BIND_ADDR") {
            self.bind_addr = v;
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

        // ── 勤怠の生イベント (社内 MariaDB、Refs #116) ──
        if let Some(v) = env_str(get, "MARIADB_HOST") {
            self.mariadb.host = v;
        }
        if let Some(v) = env_u16(get, "MARIADB_PORT")? {
            self.mariadb.port = v;
        }
        if let Some(v) = env_str(get, "MARIADB_USER") {
            self.mariadb.user = v;
        }
        if let Some(v) = env_str(get, "MARIADB_PASSWORD") {
            self.mariadb.password = v;
        }
        if let Some(v) = env_str(get, "MARIADB_DATABASE") {
            self.mariadb.database = v;
        }

        // ── 生イベントの読み先 (Refs #205 実装計画 02) ──
        // GCP instance は MariaDB に到達できないので、ここを env だけで宣言できる
        // 必要がある (`secretKeyRef` が token を入れる形も含む)
        if let Some(v) = env_kintai_events_source(get, "KINTAI_EVENTS_SOURCE")? {
            self.kintai_events.source = v;
        }
        if let Some(v) = env_str(get, "KINTAI_EVENTS_BASE_URL") {
            self.kintai_events.base_url = v;
        }
        if let Some(v) = env_str(get, "KINTAI_EVENTS_TENANT_ID") {
            self.kintai_events.tenant_id = v;
        }
        if let Some(v) = env_u64(get, "KINTAI_EVENTS_TIMEOUT_SECS")? {
            self.kintai_events.timeout_secs = v;
        }
        if let Some(v) = env_str(get, "KINTAI_EVENTS_AUTH_TOKEN") {
            self.kintai_events.auth_token = v;
        }
        if let Some(v) = env_str(get, "KINTAI_EVENTS_AUTH_TOKEN_COMMAND") {
            self.kintai_events.auth_token_command = v;
        }
        if let Some(v) = env_u64(get, "KINTAI_EVENTS_AUTH_TOKEN_TTL_SECS")? {
            self.kintai_events.auth_token_ttl_secs = v;
        }

        // ── 給与大臣 (OHKEN) + introspect 認可 (Refs #82) ──
        if let Some(v) = env_str(get, "KYUYO_HOST") {
            self.kyuyo.host = v;
        }
        if let Some(v) = env_u16(get, "KYUYO_PORT")? {
            self.kyuyo.port = v;
        }
        if let Some(v) = env_str(get, "KYUYO_USER") {
            self.kyuyo.user = v;
        }
        if let Some(v) = env_str(get, "KYUYO_PASSWORD") {
            self.kyuyo.password = v;
        }
        if let Some(v) = env_str(get, "KYUYO_AUTH_WORKER_ORIGIN") {
            self.kyuyo.auth_worker_origin = v;
        }
        if let Some(v) = env_str(get, "KYUYO_INTROSPECT_SECRET") {
            self.kyuyo.introspect_secret = v;
        }
        if let Some(v) = env_str(get, "KYUYO_APP_ORIGIN") {
            self.kyuyo.app_origin = v;
        }
        if let Some(v) = env_list(get, "KYUYO_ALLOWED_EMAILS") {
            self.kyuyo.allowed_emails = v;
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
            log_dir: String::new(),
            database: DatabaseConfig::default(),
            cors: CorsConfig::default(),
            sqlite: SqliteConfig::default(),
            cakephp: CakephpConfig::default(),
            raw: RawConfig::default(),
            kyuyo: KyuyoConfig::default(),
            restraint: RestraintConfig::default(),
            mariadb: MariadbConfig::default(),
            kintai_events: KintaiEventsConfig::default(),
            kosoku: KosokuConfigToml::default(),
        })
    }
}
