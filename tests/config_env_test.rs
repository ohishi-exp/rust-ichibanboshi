//! 環境変数による設定の上書き (オンプレ = TOML / GCP = env var の両対応)。
//!
//! `Config::apply_env_overrides` は引き当て関数を引数で受ける形なので、
//! ここのテストは**プロセスの環境変数を汚さずに**回る (`cargo test` は
//! スレッド並列なので `set_var` は他のテストとレースする)。
//! 実際に `std::env` を読む経路 (`from_args_and_file`) だけは、この
//! テストバイナリ**専用のプロセス**で mutex を取って直列に検証する。

use rust_ichibanboshi::config::{AppArgs, Config};
use std::collections::HashMap;

/// `std::env` を触るテストの直列化 (このテストバイナリ内でのみ有効)。
static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn lookup(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn apply(config: &mut Config, pairs: &[(&str, &str)]) -> Result<(), String> {
    let map = lookup(pairs);
    config.apply_env_overrides(&|k| map.get(k).cloned())
}

fn base() -> Config {
    toml::from_str("").unwrap()
}

#[test]
fn test_env_unset_changes_nothing() {
    // オンプレは環境変数を一切設定しない。TOML が完全に主導権を持つことの固定。
    let toml_str = r#"
port = 3100
bind_addr = "127.0.0.1"

[database]
host = "172.18.21.102"
user = "pbi"
password = "onprem-secret"

[mariadb]
password = "maria-secret"
database = "cake"
"#;
    // [mariadb] は撤去済みの勤怠の節 (Refs #322)。オンプレの toml に残っていても読み飛ばす
    let mut config: Config = toml::from_str(toml_str).unwrap();
    apply(&mut config, &[]).unwrap();

    assert!(
        config.database.enabled,
        "既定は SQL Server を使う (オンプレ)"
    );
    assert_eq!(config.database.host, "172.18.21.102");
    assert_eq!(config.database.password, "onprem-secret");
    assert_eq!(config.port, 3100);
    assert_eq!(config.bind_addr, "127.0.0.1");
}

#[test]
fn test_env_overrides_every_supported_key() {
    // TOML を一切書かずに env var だけで完結して起動できること (= GCP 主経路)。
    let mut config = base();
    apply(
        &mut config,
        &[
            ("PORT", "8080"),
            ("BIND_ADDR", "0.0.0.0"),
            ("DATABASE_ENABLED", "false"),
            ("DATABASE_HOST", "sql.example"),
            ("DATABASE_INSTANCE", "MSSQL"),
            ("DATABASE_NAME", "OtherDB"),
            ("DATABASE_USER", "reader"),
            ("DATABASE_PASSWORD", "db-pass"),
            ("DATABASE_PORT", "1433"),
            ("DATABASE_TRUST_SERVER_CERTIFICATE", "false"),
            ("CAKEPHP_BASE_URL", "http://127.0.0.1:120"),
            (
                "CORS_ALLOWED_ORIGINS",
                "https://x.example, https://y.example",
            ),
        ],
    )
    .unwrap();

    assert_eq!(config.port, 8080);
    assert_eq!(config.bind_addr, "0.0.0.0");
    assert!(!config.database.enabled);
    assert_eq!(config.database.host, "sql.example");
    assert_eq!(config.database.instance, "MSSQL");
    assert_eq!(config.database.database, "OtherDB");
    assert_eq!(config.database.user, "reader");
    assert_eq!(config.database.password, "db-pass");
    assert_eq!(config.database.port, Some(1433));
    assert!(!config.database.trust_server_certificate);
    assert_eq!(config.cakephp.base_url, "http://127.0.0.1:120");
    assert_eq!(
        config.cors.allowed_origins,
        vec!["https://x.example", "https://y.example"]
    );
}

#[test]
fn test_env_empty_value_overwrites_instead_of_falling_back() {
    // secretKeyRef の配線ミスで空が入ったとき、TOML の値へ静かに戻らないこと。
    // 空のまま起動 → 接続に失敗 → 起動失敗、と loud に倒れるのが狙い。
    let mut config: Config = toml::from_str(
        r#"
[database]
password = "onprem-secret"
"#,
    )
    .unwrap();
    apply(&mut config, &[("DATABASE_PASSWORD", "")]).unwrap();
    assert_eq!(config.database.password, "");
}

#[test]
fn test_env_database_port_empty_clears_to_named_instance() {
    // 空文字だけは「未指定に戻す」= 名前付きインスタンス経由に切り替える意味を持つ。
    let mut config: Config = toml::from_str(
        r#"
[database]
port = 1433
"#,
    )
    .unwrap();
    assert_eq!(config.database.port, Some(1433));
    apply(&mut config, &[("DATABASE_PORT", "")]).unwrap();
    assert_eq!(config.database.port, None);
}

#[test]
fn test_env_list_empty_means_empty_list() {
    let mut config = base();
    apply(&mut config, &[("CORS_ALLOWED_ORIGINS", "")]).unwrap();
    assert!(config.cors.allowed_origins.is_empty());
}

#[test]
fn test_env_bool_accepts_true_false_one_zero_case_insensitively() {
    for (raw, expected) in [
        ("true", true),
        ("TRUE", true),
        ("1", true),
        ("false", false),
        ("False", false),
        ("0", false),
    ] {
        let mut config = base();
        apply(&mut config, &[("DATABASE_ENABLED", raw)]).unwrap();
        assert_eq!(config.database.enabled, expected, "raw={raw}");
    }
}

#[test]
fn test_env_bool_invalid_is_loud() {
    let mut config = base();
    let err = apply(&mut config, &[("DATABASE_ENABLED", "maybe")]).unwrap_err();
    assert!(err.contains("DATABASE_ENABLED"), "{err}");
    assert!(err.contains("invalid boolean"), "{err}");
}

#[test]
fn test_env_number_invalid_is_loud() {
    let mut config = base();
    let err = apply(&mut config, &[("PORT", "http")]).unwrap_err();
    assert!(err.contains("PORT"), "{err}");
    assert!(err.contains("invalid number"), "{err}");

    let mut config = base();
    let err = apply(&mut config, &[("PORT", "-1")]).unwrap_err();
    assert!(err.contains("PORT"), "{err}");

    let mut config = base();
    let err = apply(&mut config, &[("DATABASE_PORT", "nope")]).unwrap_err();
    assert!(err.contains("DATABASE_PORT"), "{err}");
    assert!(err.contains("invalid number"), "{err}");
}

#[test]
fn test_env_trust_server_certificate_invalid_is_loud() {
    let mut config = base();
    let err = apply(&mut config, &[("DATABASE_TRUST_SERVER_CERTIFICATE", "x")]).unwrap_err();
    assert!(err.contains("DATABASE_TRUST_SERVER_CERTIFICATE"), "{err}");
}

// ── 実際に std::env を読む経路 (from_args_and_file) の優先順位 ──
// CLI 引数 > 環境変数 > TOML > 既定値

#[test]
fn test_from_args_and_file_env_beats_toml() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("ichibanboshi-env-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("env-beats-toml.toml");
    std::fs::write(
        &path,
        "port = 3100\n\n[database]\npassword = \"from-toml\"\n",
    )
    .unwrap();

    std::env::set_var("PORT", "8080");
    std::env::set_var("DATABASE_PASSWORD", "from-env");
    let args = AppArgs {
        console: true,
        config: Some(path.to_string_lossy().to_string()),
        port: None,
        bind_addr: None,
    };
    let config = Config::from_args_and_file(&args).unwrap();
    std::env::remove_var("PORT");
    std::env::remove_var("DATABASE_PASSWORD");
    std::fs::remove_file(&path).ok();

    assert_eq!(config.port, 8080, "env が TOML に勝つ");
    assert_eq!(config.database.password, "from-env");
}

#[test]
fn test_from_args_and_file_cli_beats_env() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("ichibanboshi-env-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("cli-beats-env.toml");
    std::fs::write(&path, "port = 3100\nbind_addr = \"127.0.0.1\"\n").unwrap();

    std::env::set_var("PORT", "8080");
    std::env::set_var("BIND_ADDR", "0.0.0.0");
    let args = AppArgs {
        console: true,
        config: Some(path.to_string_lossy().to_string()),
        port: Some(9999),
        bind_addr: Some("192.0.2.1".to_string()),
    };
    let config = Config::from_args_and_file(&args).unwrap();
    std::env::remove_var("PORT");
    std::env::remove_var("BIND_ADDR");
    std::fs::remove_file(&path).ok();

    assert_eq!(config.port, 9999, "CLI が env に勝つ");
    assert_eq!(config.bind_addr, "192.0.2.1");
}

#[test]
fn test_from_args_and_file_invalid_env_fails_startup() {
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let dir = std::env::temp_dir().join(format!("ichibanboshi-env-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("invalid-env.toml");
    std::fs::write(&path, "port = 3100\n").unwrap();

    std::env::set_var("PORT", "not-a-number");
    let args = AppArgs {
        console: true,
        config: Some(path.to_string_lossy().to_string()),
        port: None,
        bind_addr: None,
    };
    let result = Config::from_args_and_file(&args);
    std::env::remove_var("PORT");
    std::fs::remove_file(&path).ok();

    assert!(result.is_err(), "壊れた env は起動を失敗させる");
}

/// **`SQLITE_PATH=""` で「ローカル状態の置き場を持たない」と宣言できる**
/// (Refs #205 の G4)。
///
/// イメージに TOML を焼かない方針なので、env で宣言できないと Cloud Run は既定の
/// `/opt/ichibanboshi/state.db` を使い、揮発 FS に空の state.db を作って
/// `/api/uriage/*` が「壊れている」ではなく「0 件」を返す。
#[test]
fn test_sqlite_path_can_be_declared_empty_from_env() {
    // 既定は宣言済み (オンプレの形)
    let config = base();
    assert!(!config.sqlite.path.is_empty());

    // 空で上書きできる = 置き場を持たない宣言。「未設定」に落ちてはいけない
    let mut config = base();
    apply(&mut config, &[("SQLITE_PATH", "")]).unwrap();
    assert_eq!(config.sqlite.path, "");

    // 値を入れれば置き場の差し替えとして効く
    let mut config = base();
    apply(&mut config, &[("SQLITE_PATH", "/mnt/state/state.db")]).unwrap();
    assert_eq!(config.sqlite.path, "/mnt/state/state.db");

    // TOML の値も env が勝つ
    let mut config: Config = toml::from_str("[sqlite]\npath = \"/opt/a.db\"\n").unwrap();
    apply(&mut config, &[("SQLITE_PATH", "")]).unwrap();
    assert_eq!(config.sqlite.path, "");
}

#[test]
fn test_removed_kintai_env_vars_are_ignored() {
    // 勤怠の env (MARIADB_* / KINTAI_EVENTS_* / KINTAI_PUSH_* / DTAKO_DAY_*) は撤去した (Refs #322)。
    // 撤去前は壊れた値で起動を落としていたが、もう読まないので残っていても起動は止まらない
    let mut config = base();
    apply(
        &mut config,
        &[
            ("MARIADB_PORT", "-1"),
            ("KINTAI_EVENTS_SOURCE", "htpp"),
            ("KINTAI_EVENTS_TIMEOUT_SECS", "soon"),
            ("KINTAI_PUSH_ENABLED", "maybe"),
            ("DTAKO_DAY_RYOHI_BASE_URL", "https://ryohi.example"),
        ],
    )
    .unwrap();
    assert_eq!(config.port, 3100);
}
