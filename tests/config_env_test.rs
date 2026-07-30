//! 環境変数による設定の上書き (オンプレ = TOML / GCP = env var の両対応)。
//!
//! `Config::apply_env_overrides` は引き当て関数を引数で受ける形なので、
//! ここのテストは**プロセスの環境変数を汚さずに**回る (`cargo test` は
//! スレッド並列なので `set_var` は他のテストとレースする)。
//! 実際に `std::env` を読む経路 (`from_args_and_file`) だけは、この
//! テストバイナリ**専用のプロセス**で mutex を取って直列に検証する。

use rust_ichibanboshi::config::{AppArgs, Config, KintaiEventsSource};
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

[kyuyo]
host = "10.0.0.9"
password = "kyuyo-secret"
"#;
    let mut config: Config = toml::from_str(toml_str).unwrap();
    apply(&mut config, &[]).unwrap();

    assert!(
        config.database.enabled,
        "既定は SQL Server を使う (オンプレ)"
    );
    assert_eq!(config.database.host, "172.18.21.102");
    assert_eq!(config.database.password, "onprem-secret");
    assert_eq!(config.mariadb.password, "maria-secret");
    assert_eq!(config.kyuyo.password, "kyuyo-secret");
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
            ("MARIADB_HOST", "maria.example"),
            ("MARIADB_PORT", "3307"),
            ("MARIADB_USER", "maria_reader"),
            ("MARIADB_PASSWORD", "maria-pass"),
            ("MARIADB_DATABASE", "cakephp"),
            ("KYUYO_HOST", "kyuyo.example"),
            ("KYUYO_PORT", "14331"),
            ("KYUYO_USER", "kyuyo_reader2"),
            ("KYUYO_PASSWORD", "kyuyo-pass"),
            ("KYUYO_AUTH_WORKER_ORIGIN", "https://auth.example"),
            ("KYUYO_INTROSPECT_SECRET", "introspect-secret"),
            ("KYUYO_APP_ORIGIN", "https://app.example"),
            ("KYUYO_ALLOWED_EMAILS", "a@example.com, b@example.com"),
            ("KINTAI_EVENTS_SOURCE", "http"),
            ("KINTAI_EVENTS_BASE_URL", "https://alc.example"),
            (
                "KINTAI_EVENTS_TENANT_ID",
                "11111111-2222-3333-4444-555555555555",
            ),
            ("KINTAI_EVENTS_TIMEOUT_SECS", "90"),
            ("KINTAI_EVENTS_AUTH_TOKEN", "id-token"),
            ("KINTAI_EVENTS_AUTH_TOKEN_METADATA", "false"),
            ("KINTAI_EVENTS_AUTH_TOKEN_TTL_SECS", "600"),
            ("KINTAI_PUSH_ENABLED", "true"),
            (
                "KINTAI_PUSH_DATABASE_URL",
                "postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres",
            ),
            (
                "KINTAI_PUSH_TENANT_ID",
                "11111111-2222-3333-4444-555555555555",
            ),
            ("KINTAI_PUSH_CONNECT_TIMEOUT_SECS", "15"),
            ("KINTAI_PUSH_STATEMENT_TIMEOUT_SECS", "600"),
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
    assert_eq!(config.mariadb.host, "maria.example");
    assert_eq!(config.mariadb.port, 3307);
    assert_eq!(config.mariadb.user, "maria_reader");
    assert_eq!(config.mariadb.password, "maria-pass");
    assert_eq!(config.mariadb.database, "cakephp");
    assert!(config.mariadb.enabled());
    assert_eq!(config.kyuyo.host, "kyuyo.example");
    assert_eq!(config.kyuyo.port, 14331);
    assert_eq!(config.kyuyo.user, "kyuyo_reader2");
    assert_eq!(config.kyuyo.password, "kyuyo-pass");
    assert_eq!(config.kyuyo.auth_worker_origin, "https://auth.example");
    assert_eq!(config.kyuyo.introspect_secret, "introspect-secret");
    assert_eq!(config.kyuyo.app_origin, "https://app.example");
    assert_eq!(
        config.kyuyo.allowed_emails,
        vec!["a@example.com", "b@example.com"]
    );
    assert!(config.kyuyo.db_enabled());
    assert!(config.kyuyo.auth_configured());
    assert_eq!(config.cakephp.base_url, "http://127.0.0.1:120");
    assert_eq!(
        config.cors.allowed_origins,
        vec!["https://x.example", "https://y.example"]
    );
    assert_eq!(config.kintai_events.source, KintaiEventsSource::Http);
    assert!(config.kintai_events.http_enabled());
    assert_eq!(config.kintai_events.base_url, "https://alc.example");
    assert_eq!(
        config.kintai_events.tenant_id,
        "11111111-2222-3333-4444-555555555555"
    );
    assert_eq!(config.kintai_events.timeout_secs, 90);
    assert_eq!(config.kintai_events.auth_token, "id-token");
    assert_eq!(config.kintai_events.auth_token_ttl_secs, 600);
    assert!(config.kintai_push.enabled);
    assert_eq!(
        config.kintai_push.database_url,
        "postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres"
    );
    assert_eq!(
        config.kintai_push.tenant_id,
        "11111111-2222-3333-4444-555555555555"
    );
    assert_eq!(config.kintai_push.connect_timeout_secs, 15);
    assert_eq!(config.kintai_push.statement_timeout_secs, 600);
    // env だけで完結して起動できる = 宣言が揃っている
    config.kintai_events.validate().unwrap();
    config
        .kintai_push
        .validate(&config.kintai_events.tenant_id)
        .unwrap();
}

#[test]
fn test_kintai_events_defaults_to_mariadb() {
    // オンプレの既定。TOML に [kintai_events] を書かなければ挙動は変わらない。
    let config = base();
    assert_eq!(config.kintai_events.source, KintaiEventsSource::Mariadb);
    assert!(!config.kintai_events.http_enabled());
    assert_eq!(config.kintai_events.timeout_secs, 120);
    assert_eq!(config.kintai_events.auth_token_ttl_secs, 900);
    // MariaDB 読みなら接続先の宣言は要らない
    config.kintai_events.validate().unwrap();
}

#[test]
fn test_kintai_events_source_from_toml() {
    let config: Config = toml::from_str(
        r#"
[kintai_events]
source = "http"
base_url = "https://alc.example"
tenant_id = "11111111-2222-3333-4444-555555555555"
auth_token_command = "gcloud auth print-identity-token"
"#,
    )
    .unwrap();
    assert!(config.kintai_events.http_enabled());
    assert_eq!(
        config.kintai_events.auth_token_command,
        "gcloud auth print-identity-token"
    );
    config.kintai_events.validate().unwrap();
}

#[test]
fn test_kintai_events_source_invalid_is_loud() {
    // 打ち間違いが静かに MariaDB 読みへ落ちると、読み先を取り違えたまま動く
    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_EVENTS_SOURCE", "htpp")]).unwrap_err();
    assert!(err.contains("KINTAI_EVENTS_SOURCE"), "{err}");
    assert!(err.contains("invalid source"), "{err}");

    // 大文字小文字は問わない
    let mut config = base();
    apply(&mut config, &[("KINTAI_EVENTS_SOURCE", "HTTP")]).unwrap();
    assert_eq!(config.kintai_events.source, KintaiEventsSource::Http);
    let mut config = base();
    apply(&mut config, &[("KINTAI_EVENTS_SOURCE", "MariaDB")]).unwrap();
    assert_eq!(config.kintai_events.source, KintaiEventsSource::Mariadb);
}

#[test]
fn test_kintai_events_timeout_invalid_is_loud() {
    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_EVENTS_TIMEOUT_SECS", "soon")]).unwrap_err();
    assert!(err.contains("KINTAI_EVENTS_TIMEOUT_SECS"), "{err}");
    assert!(err.contains("invalid number"), "{err}");

    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_EVENTS_AUTH_TOKEN_TTL_SECS", "-1")]).unwrap_err();
    assert!(err.contains("KINTAI_EVENTS_AUTH_TOKEN_TTL_SECS"), "{err}");
}

#[test]
fn test_kintai_events_http_without_target_fails_startup() {
    // 「使うと宣言したのに接続先が無い」を黙って既定へ落とさない
    // ([database] enabled と同じ流儀)。
    let mut config = base();
    apply(&mut config, &[("KINTAI_EVENTS_SOURCE", "http")]).unwrap();
    let err = config.kintai_events.validate().unwrap_err();
    assert!(err.contains("base_url"), "{err}");

    let mut config = base();
    apply(
        &mut config,
        &[
            ("KINTAI_EVENTS_SOURCE", "http"),
            ("KINTAI_EVENTS_BASE_URL", "https://alc.example"),
        ],
    )
    .unwrap();
    let err = config.kintai_events.validate().unwrap_err();
    assert!(err.contains("tenant_id"), "{err}");
}

#[test]
fn test_kintai_events_two_token_routes_at_once_fails_startup() {
    // どちらが効いているか外から分からない状態を作らない
    let mut config = base();
    apply(
        &mut config,
        &[
            ("KINTAI_EVENTS_SOURCE", "http"),
            ("KINTAI_EVENTS_BASE_URL", "https://alc.example"),
            ("KINTAI_EVENTS_TENANT_ID", "t"),
            ("KINTAI_EVENTS_AUTH_TOKEN", "static"),
            (
                "KINTAI_EVENTS_AUTH_TOKEN_COMMAND",
                "gcloud auth print-identity-token",
            ),
        ],
    )
    .unwrap();
    let err = config.kintai_events.validate().unwrap_err();
    assert!(err.contains("only one of"), "{err}");
}

#[test]
fn test_kintai_events_metadata_route_is_exclusive_too() {
    // Cloud Run の中では metadata server が唯一の経路 (gcloud も curl も image に無い)。
    // 静的 token / コマンドと同時に宣言されたら、どれが効くか外から分からないので落とす
    let mut config = base();
    apply(
        &mut config,
        &[
            ("KINTAI_EVENTS_SOURCE", "http"),
            ("KINTAI_EVENTS_BASE_URL", "https://alc.example"),
            ("KINTAI_EVENTS_TENANT_ID", "t"),
            ("KINTAI_EVENTS_AUTH_TOKEN_METADATA", "true"),
        ],
    )
    .unwrap();
    assert!(config.kintai_events.auth_token_metadata);
    // metadata だけなら通る
    config.kintai_events.validate().unwrap();

    // command と併記したら落ちる
    apply(
        &mut config,
        &[(
            "KINTAI_EVENTS_AUTH_TOKEN_COMMAND",
            "gcloud auth print-identity-token",
        )],
    )
    .unwrap();
    let err = config.kintai_events.validate().unwrap_err();
    assert!(err.contains("only one of"), "{err}");
    assert!(err.contains("auth_token_metadata"), "{err}");
}

#[test]
fn test_kintai_events_metadata_invalid_bool_is_loud() {
    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_EVENTS_AUTH_TOKEN_METADATA", "yes")]).unwrap_err();
    assert!(err.contains("KINTAI_EVENTS_AUTH_TOKEN_METADATA"), "{err}");
}

#[test]
fn test_kintai_events_env_empty_value_overwrites() {
    let mut config: Config = toml::from_str(
        r#"
[kintai_events]
auth_token = "from-toml"
"#,
    )
    .unwrap();
    apply(&mut config, &[("KINTAI_EVENTS_AUTH_TOKEN", "")]).unwrap();
    assert_eq!(config.kintai_events.auth_token, "");
}

#[test]
fn test_kintai_push_defaults_to_disabled() {
    // オンプレでも既定は無効。TOML に [kintai_push] を書かなければ何も書きに行かない。
    let config = base();
    assert!(!config.kintai_push.enabled);
    assert!(config.kintai_push.database_url.is_empty());
    assert!(config.kintai_push.tenant_id.is_empty());
    assert_eq!(config.kintai_push.connect_timeout_secs, 30);
    // 月単位のバッチなので読み出し経路より長い
    assert_eq!(config.kintai_push.statement_timeout_secs, 300);
    // 無効なら接続先の宣言は要らない ([kintai_events] tenant_id も空)
    config.kintai_push.validate("").unwrap();
}

#[test]
fn test_kintai_push_from_toml() {
    let config: Config = toml::from_str(
        r#"
[kintai_push]
enabled = true
database_url = "postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres"
tenant_id = "11111111-2222-3333-4444-555555555555"
connect_timeout_secs = 10
statement_timeout_secs = 900
"#,
    )
    .unwrap();
    assert!(config.kintai_push.enabled);
    assert_eq!(
        config.kintai_push.database_url,
        "postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres"
    );
    assert_eq!(config.kintai_push.connect_timeout_secs, 10);
    assert_eq!(config.kintai_push.statement_timeout_secs, 900);
    config
        .kintai_push
        .validate(&config.kintai_events.tenant_id)
        .unwrap();
}

#[test]
fn test_kintai_push_enabled_invalid_is_loud() {
    // 打ち間違いが静かに false へ落ちると「push したつもりで何も書いていない」になる
    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_PUSH_ENABLED", "maybe")]).unwrap_err();
    assert!(err.contains("KINTAI_PUSH_ENABLED"), "{err}");
    assert!(err.contains("invalid boolean"), "{err}");
}

#[test]
fn test_kintai_push_timeout_invalid_is_loud() {
    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_PUSH_CONNECT_TIMEOUT_SECS", "soon")]).unwrap_err();
    assert!(err.contains("KINTAI_PUSH_CONNECT_TIMEOUT_SECS"), "{err}");
    assert!(err.contains("invalid number"), "{err}");

    let mut config = base();
    let err = apply(&mut config, &[("KINTAI_PUSH_STATEMENT_TIMEOUT_SECS", "-1")]).unwrap_err();
    assert!(err.contains("KINTAI_PUSH_STATEMENT_TIMEOUT_SECS"), "{err}");
}

#[test]
fn test_kintai_push_env_empty_value_overwrites() {
    // secretKeyRef の配線ミスで空が入ったとき TOML の接続文字列へ静かに戻らないこと。
    // 空のまま validate → 起動失敗、と loud に倒れるのが狙い。
    let mut config: Config = toml::from_str(
        r#"
[kintai_push]
enabled = true
database_url = "postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres"
tenant_id = "11111111-2222-3333-4444-555555555555"
"#,
    )
    .unwrap();
    apply(&mut config, &[("KINTAI_PUSH_DATABASE_URL", "")]).unwrap();
    assert_eq!(config.kintai_push.database_url, "");
    let err = config.kintai_push.validate("").unwrap_err();
    assert!(err.contains("database_url"), "{err}");
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
    let err = apply(&mut config, &[("MARIADB_PORT", "-1")]).unwrap_err();
    assert!(err.contains("MARIADB_PORT"), "{err}");

    let mut config = base();
    let err = apply(&mut config, &[("KYUYO_PORT", "99999999")]).unwrap_err();
    assert!(err.contains("KYUYO_PORT"), "{err}");

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
