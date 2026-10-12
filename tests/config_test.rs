use clap::Parser;
use rust_ichibanboshi::config::{AppArgs, Config};
use std::io::Write;

/// exe 隣接 `ichibanboshi.toml` を書くテストと、default locations を読むテストの
/// 直列化。並列実行だと書いた一時 toml を別テストが読んでしまう。
static EXE_TOML_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn test_config_defaults_from_empty_toml() {
    let config: Config = toml::from_str("").unwrap();
    assert_eq!(config.port, 3100);
    assert_eq!(config.bind_addr, "127.0.0.1");
    assert_eq!(config.database.host, "localhost");
    assert_eq!(config.database.instance, "softec");
    assert_eq!(config.database.database, "CAPE#01");
    assert!(config.database.trust_server_certificate);
    assert!(config.database.user.is_empty());
    assert!(config.database.password.is_empty());
    assert!(config.database.port.is_none());
    assert_eq!(
        config.cors.allowed_origins,
        vec!["https://ichibanboshi.mtamaramu.com"]
    );
}

#[test]
fn test_config_full_toml() {
    let toml_str = r#"
port = 8080
bind_addr = "0.0.0.0"

[database]
host = "192.168.1.1"
instance = "MSSQL"
database = "TestDB"
user = "sa"
password = "secret"
port = 1433
trust_server_certificate = false

[auth]
jwt_secret = "my-secret-key"

[cors]
allowed_origins = ["http://localhost:3000", "https://example.com"]
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(config.port, 8080);
    assert_eq!(config.bind_addr, "0.0.0.0");
    assert_eq!(config.database.host, "192.168.1.1");
    assert_eq!(config.database.instance, "MSSQL");
    assert_eq!(config.database.database, "TestDB");
    assert_eq!(config.database.user, "sa");
    assert_eq!(config.database.password, "secret");
    assert_eq!(config.database.port, Some(1433));
    assert!(!config.database.trust_server_certificate);
    assert_eq!(config.cors.allowed_origins.len(), 2);
}

#[test]
fn test_config_partial_toml() {
    let toml_str = r#"
port = 9999

[database]
host = "10.0.0.1"
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(config.port, 9999);
    assert_eq!(config.bind_addr, "127.0.0.1"); // default
    assert_eq!(config.database.host, "10.0.0.1");
    assert_eq!(config.database.instance, "softec"); // default
}

#[test]
fn test_config_addr() {
    let config: Config = toml::from_str("port = 9999\nbind_addr = \"0.0.0.0\"").unwrap();
    assert_eq!(config.addr(), "0.0.0.0:9999");
}

#[test]
fn test_config_addr_default() {
    let config: Config = toml::from_str("").unwrap();
    assert_eq!(config.addr(), "127.0.0.1:3100");
}

#[test]
fn test_config_from_args_override_port() {
    let args = AppArgs {
        console: true,
        config: None,
        port: Some(9999),
        bind_addr: None,
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 9999);
}

#[test]
fn test_config_from_args_override_bind_addr() {
    let args = AppArgs {
        console: true,
        config: None,
        port: None,
        bind_addr: Some("0.0.0.0".to_string()),
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.bind_addr, "0.0.0.0");
}

#[test]
fn test_config_from_args_override_both() {
    let args = AppArgs {
        console: true,
        config: None,
        port: Some(8080),
        bind_addr: Some("10.0.0.1".to_string()),
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 8080);
    assert_eq!(config.bind_addr, "10.0.0.1");
}

#[test]
fn test_config_file_not_found() {
    let args = AppArgs {
        console: true,
        config: Some("/nonexistent/path/config.toml".to_string()),
        port: None,
        bind_addr: None,
    };
    assert!(Config::from_args_and_file(&args).is_err());
}

#[test]
fn test_config_from_args_no_overrides() {
    let _g = EXE_TOML_LOCK.lock().unwrap();
    let args = AppArgs {
        console: true,
        config: None,
        port: None,
        bind_addr: None,
    };
    // load_default_locations fallback
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 3100);
}

#[test]
fn test_config_invalid_toml() {
    let result: Result<Config, _> = toml::from_str("port = \"not_a_number\"");
    assert!(result.is_err());
}

#[test]
fn test_database_config_default() {
    let db = rust_ichibanboshi::config::DatabaseConfig::default();
    assert_eq!(db.host, "localhost");
    assert_eq!(db.instance, "softec");
    assert_eq!(db.database, "CAPE#01");
    assert!(db.user.is_empty());
    assert!(db.port.is_none());
    assert!(db.trust_server_certificate);
}

#[test]
fn test_cors_config_default() {
    let cors = rust_ichibanboshi::config::CorsConfig::default();
    assert_eq!(cors.allowed_origins.len(), 1);
    assert_eq!(
        cors.allowed_origins[0],
        "https://ichibanboshi.mtamaramu.com"
    );
}

#[test]
fn test_sqlite_config_default() {
    let s = rust_ichibanboshi::config::SqliteConfig::default();
    assert_eq!(s.path, "/opt/ichibanboshi/state.db");
}

#[test]
fn test_cakephp_config_default() {
    let c = rust_ichibanboshi::config::CakephpConfig::default();
    assert!(c.base_url.is_empty()); // 空文字 = 機能無効
    assert_eq!(c.timeout_secs, 30);
}

#[test]
fn test_raw_config_default() {
    let r = rust_ichibanboshi::config::RawConfig::default();
    assert_eq!(r.dir, "/opt/ichibanboshi/raw");
}

#[test]
fn test_config_phase2_sections_from_toml() {
    let toml_str = r#"
[sqlite]
path = "/var/tmp/test.db"

[cakephp]
base_url = "https://ohishi-dev.local"
timeout_secs = 10

[raw]
dir = "/data/raw"
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(config.sqlite.path, "/var/tmp/test.db");
    assert_eq!(config.cakephp.base_url, "https://ohishi-dev.local");
    assert_eq!(config.cakephp.timeout_secs, 10);
    assert_eq!(config.raw.dir, "/data/raw");
}

#[test]
fn test_config_phase2_defaults_when_omitted() {
    // empty TOML → SqliteConfig/CakephpConfig/RawConfig すべて default 値で埋まる
    let config: Config = toml::from_str("").unwrap();
    assert_eq!(config.sqlite.path, "/opt/ichibanboshi/state.db");
    assert!(config.cakephp.base_url.is_empty());
    assert_eq!(config.cakephp.timeout_secs, 30);
    assert_eq!(config.raw.dir, "/opt/ichibanboshi/raw");
}

#[test]
fn test_config_from_args_with_config_file() {
    // temp ファイルに TOML を書き出して --config で読み込み
    let dir = std::env::temp_dir().join("ichibanboshi_test");
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("test_config.toml");
    let mut f = std::fs::File::create(&config_path).unwrap();
    write!(
        f,
        "port = 7777\nbind_addr = \"10.0.0.1\"\n[auth]\njwt_secret = \"file-secret\"\n"
    )
    .unwrap();

    let args = AppArgs {
        console: true,
        config: Some(config_path.to_str().unwrap().to_string()),
        port: None,
        bind_addr: None,
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 7777);
    assert_eq!(config.bind_addr, "10.0.0.1");

    std::fs::remove_file(&config_path).ok();
}

#[test]
fn test_config_from_args_with_config_file_and_overrides() {
    let dir = std::env::temp_dir().join("ichibanboshi_test");
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("test_config2.toml");
    let mut f = std::fs::File::create(&config_path).unwrap();
    writeln!(f, "port = 5555").unwrap();

    let args = AppArgs {
        console: true,
        config: Some(config_path.to_str().unwrap().to_string()),
        port: Some(9999),                       // override
        bind_addr: Some("0.0.0.0".to_string()), // override
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 9999); // CLI override wins
    assert_eq!(config.bind_addr, "0.0.0.0");

    std::fs::remove_file(&config_path).ok();
}

#[test]
fn test_load_default_locations_exe_adjacent() {
    let _g = EXE_TOML_LOCK.lock().unwrap();
    // テストバイナリの隣に ichibanboshi.toml を置いてカバー
    let exe_path = std::env::current_exe().unwrap();
    let exe_dir = exe_path.parent().unwrap();
    let config_path = exe_dir.join("ichibanboshi.toml");
    let existed = config_path.exists();

    {
        let mut f = std::fs::File::create(&config_path).unwrap();
        std::io::Write::write_all(
            &mut f,
            b"port = 6666\n[auth]\njwt_secret = \"exe-adjacent\"\n",
        )
        .unwrap();
    }

    let config = Config::load_default_locations().unwrap();
    assert_eq!(config.port, 6666);

    if !existed {
        std::fs::remove_file(&config_path).ok();
    }
}

// ══════════════════════════════════════════════════════════════
// 撤去済みの [kyuyo] 節
// ══════════════════════════════════════════════════════════════

#[test]
fn test_removed_kyuyo_section_is_ignored() {
    // 本番ホストの toml には旧 [kyuyo] 節が残る。読めなくなって起動失敗しないこと
    let toml_str = r#"
port = 3101

[kyuyo]
host = "kyuyo-pc.example"
password = "secret"
allowed_emails = ["keiri@example.com"]
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(config.port, 3101);
}

// ══════════════════════════════════════════════════════════════
// 撤去済みの勤怠の節 (Refs #322)
// ══════════════════════════════════════════════════════════════

/// オンプレの `ichibanboshi.toml` の形。勤怠をオンプレ版から撤去した後も、ここに残った
/// 勤怠の節 (`[mariadb]` `[kintai_events]` `[kintai_push]` `[kosoku]` `[restraint]`
/// `[dtako_day_links]`、`[cakephp] sqlite_path`) と旧 `[auth]` `[kyuyo]` はそのまま残る。
/// merge でそのまま deploy され巻き戻しが無いので、**読めずに起動が落ちると売上ごと止まる**。
/// 値は撤去前の型では弾かれていたもの (`restraint_rounding = "nearest"`、6543 の URL) も混ぜてある —
/// 節の中身を一切解釈しないことの固定。
const ONPREM_TOML: &str = r#"
port = 3100
bind_addr = "0.0.0.0"

[database]
host = "sql.example"
instance = "softec"
database = "CAPE#01"
user = "reader"
password = "db-pass"
trust_server_certificate = true

[auth]
jwt_secret = "legacy"

[cors]
allowed_origins = ["https://a.example", "http://localhost:3000"]

[cakephp]
base_url = "http://127.0.0.1:120"
timeout_secs = 20
sqlite_path = "/opt/ichibanboshi/kintai_local.sqlite"

[kyuyo]
host = "kyuyo-pc.example"
password = "secret"

[mariadb]
host = "127.0.0.1"
port = 3306
user = "kintai_reader"
password = "maria-pass"
database = "cake"

[dtako_day_links]
ryohi_base_url = "http://nginx.example"
dtako_base_url = "https://dtako.example"

[restraint]
sqlite_path = "/opt/ichibanboshi/restraint_local.sqlite"

[kintai_events]
source = "mariadb"
timeout_secs = "not-a-number"

[kintai_push]
enabled = true
database_url = "postgres://w:pw@pooler.example:6543/postgres"

[kosoku]
break_threshold_minutes = 10
restraint_rounding = "nearest"
"#;

fn assert_onprem_values(config: &Config) {
    assert_eq!(config.port, 3100);
    assert_eq!(config.bind_addr, "0.0.0.0");
    assert!(config.database.enabled);
    assert_eq!(config.database.host, "sql.example");
    assert_eq!(config.database.password, "db-pass");
    assert_eq!(config.cors.allowed_origins.len(), 2);
    assert_eq!(config.cakephp.base_url, "http://127.0.0.1:120");
    assert_eq!(config.cakephp.timeout_secs, 20);
    // 売上の置き場は省略時の既定のまま
    assert_eq!(config.sqlite.path, "/opt/ichibanboshi/state.db");
    assert_eq!(config.raw.dir, "/opt/ichibanboshi/raw");
}

#[test]
fn test_onprem_toml_with_removed_kintai_sections_still_parses() {
    let config: Config = toml::from_str(ONPREM_TOML).unwrap();
    assert_onprem_values(&config);
}

#[test]
fn test_onprem_startup_path_reads_the_kintai_era_toml() {
    // 起動と同じ経路: systemd の ExecStart の引数 (`--console --config <path>`) を clap に通し、
    // ファイルから読んで env の上書きまで掛ける
    let _g = EXE_TOML_LOCK.lock().unwrap();
    let dir = std::env::temp_dir().join(format!("ichibanboshi_onprem_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("ichibanboshi.toml");
    std::fs::write(&path, ONPREM_TOML).unwrap();

    let args = AppArgs::try_parse_from([
        "ichibanboshi",
        "--console",
        "--config",
        path.to_str().unwrap(),
    ])
    .unwrap();
    assert!(args.console);
    let config = Config::from_args_and_file(&args).unwrap();
    assert_onprem_values(&config);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_removed_batch_subcommands_are_rejected() {
    // push / recalc / sync は撤去した。誤って呼ばれても HTTP サーバーとしては起動しない
    let err = AppArgs::try_parse_from(["ichibanboshi", "sync", "--month", "2026-07"]).unwrap_err();
    assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
}
