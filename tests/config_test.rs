use rust_ichibanboshi::config::{AppArgs, Config};
use std::io::Write;

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
    assert!(config.auth.jwt_secret.is_empty());
    assert_eq!(config.cors.allowed_origins, vec!["https://ichibanboshi.mtamaramu.com"]);
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
    assert_eq!(config.auth.jwt_secret, "my-secret-key");
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
fn test_auth_config_default() {
    let auth = rust_ichibanboshi::config::AuthConfig::default();
    assert!(auth.jwt_secret.is_empty());
}

#[test]
fn test_cors_config_default() {
    let cors = rust_ichibanboshi::config::CorsConfig::default();
    assert_eq!(cors.allowed_origins.len(), 1);
    assert_eq!(cors.allowed_origins[0], "https://ichibanboshi.mtamaramu.com");
}

#[test]
fn test_config_from_args_with_config_file() {
    // temp ファイルに TOML を書き出して --config で読み込み
    let dir = std::env::temp_dir().join("ichibanboshi_test");
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("test_config.toml");
    let mut f = std::fs::File::create(&config_path).unwrap();
    write!(f, "port = 7777\nbind_addr = \"10.0.0.1\"\n[auth]\njwt_secret = \"file-secret\"\n").unwrap();

    let args = AppArgs {
        console: true,
        config: Some(config_path.to_str().unwrap().to_string()),
        port: None,
        bind_addr: None,
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 7777);
    assert_eq!(config.bind_addr, "10.0.0.1");
    assert_eq!(config.auth.jwt_secret, "file-secret");

    std::fs::remove_file(&config_path).ok();
}

#[test]
fn test_config_from_args_with_config_file_and_overrides() {
    let dir = std::env::temp_dir().join("ichibanboshi_test");
    std::fs::create_dir_all(&dir).unwrap();
    let config_path = dir.join("test_config2.toml");
    let mut f = std::fs::File::create(&config_path).unwrap();
    write!(f, "port = 5555\n").unwrap();

    let args = AppArgs {
        console: true,
        config: Some(config_path.to_str().unwrap().to_string()),
        port: Some(9999),        // override
        bind_addr: Some("0.0.0.0".to_string()), // override
    };
    let config = Config::from_args_and_file(&args).unwrap();
    assert_eq!(config.port, 9999); // CLI override wins
    assert_eq!(config.bind_addr, "0.0.0.0");

    std::fs::remove_file(&config_path).ok();
}

#[test]
fn test_load_default_locations_exe_adjacent() {
    // テストバイナリの隣に ichibanboshi.toml を置いてカバー
    let exe_path = std::env::current_exe().unwrap();
    let exe_dir = exe_path.parent().unwrap();
    let config_path = exe_dir.join("ichibanboshi.toml");
    let existed = config_path.exists();

    {
        let mut f = std::fs::File::create(&config_path).unwrap();
        std::io::Write::write_all(&mut f, b"port = 6666\n[auth]\njwt_secret = \"exe-adjacent\"\n").unwrap();
    }

    let config = Config::load_default_locations().unwrap();
    assert_eq!(config.port, 6666);
    assert_eq!(config.auth.jwt_secret, "exe-adjacent");

    if !existed {
        std::fs::remove_file(&config_path).ok();
    }
}
