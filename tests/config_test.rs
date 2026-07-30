use rust_ichibanboshi::config::{AppArgs, Config, RestraintRoundingToml};
use rust_ichibanboshi::kosoku::RestraintRounding;
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
    // 畳んだ勤怠の書き先 (#205 の 04〜06) は宣言するまで無効
    assert!(!config.kintai_push.enabled);
    assert!(config.kintai_push.database_url.is_empty());
    assert!(config.kintai_push.tenant_id.is_empty());
    assert_eq!(config.kintai_push.connect_timeout_secs, 30);
    assert_eq!(config.kintai_push.statement_timeout_secs, 300);
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
// [kyuyo] (給与大臣読み取り、Refs #82)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_kyuyo_config_defaults_fail_closed() {
    let config: Config = toml::from_str("").unwrap();
    assert_eq!(config.kyuyo.port, 14330);
    assert_eq!(config.kyuyo.user, "kyuyo_reader");
    assert_eq!(config.kyuyo.app_origin, "https://dtako.ippoan.org");
    assert_eq!(config.kyuyo.timeout_secs, 10);
    assert!(config.kyuyo.host.is_empty());
    assert!(config.kyuyo.allowed_emails.is_empty());
    // 未設定は無効 (fail-closed)
    assert!(!config.kyuyo.db_enabled());
    assert!(!config.kyuyo.auth_configured());
}

#[test]
fn test_kyuyo_config_enabled() {
    let toml_str = r#"
[kyuyo]
host = "kyuyo-pc.example"
port = 14330
password = "secret"
auth_worker_origin = "https://auth.example.com"
introspect_secret = "shared"
allowed_emails = ["keiri@example.com"]
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert!(config.kyuyo.db_enabled());
    assert!(config.kyuyo.auth_configured());
    assert_eq!(config.kyuyo.allowed_emails, vec!["keiri@example.com"]);
}

#[test]
fn test_kyuyo_config_partial_is_disabled() {
    // host だけ / password 無しでは db_enabled にならない
    let config: Config = toml::from_str("[kyuyo]\nhost = \"x\"\n").unwrap();
    assert!(!config.kyuyo.db_enabled());
    // introspect_secret 無しでは auth_configured にならない
    let config: Config = toml::from_str("[kyuyo]\nauth_worker_origin = \"https://a\"\n").unwrap();
    assert!(!config.kyuyo.auth_configured());
}

// ══════════════════════════════════════════════════════════════
// [mariadb] (勤怠の生イベント直読み、Refs #116)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_mariadb_config_defaults_fail_closed() {
    let config: Config = toml::from_str("").unwrap();
    // 接続先は同一ホストの docker `db` コンテナ (loopback 1 hop)
    assert_eq!(config.mariadb.host, "127.0.0.1");
    assert_eq!(config.mariadb.port, 3306);
    assert_eq!(config.mariadb.user, "kintai_reader");
    assert!(config.mariadb.password.is_empty());
    assert!(config.mariadb.database.is_empty());
    // 未設定は無効 (fail-closed — /api/kintai/events が 503)
    assert!(!config.mariadb.enabled());
}

#[test]
fn test_mariadb_config_enabled() {
    let toml_str = r#"
[mariadb]
host = "127.0.0.1"
port = 3306
user = "kintai_reader"
password = "secret"
database = "ohishi"
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert!(config.mariadb.enabled());
    assert_eq!(config.mariadb.database, "ohishi");
}

#[test]
fn test_mariadb_config_partial_is_disabled() {
    // password 無し / database 無しはいずれも無効
    let config: Config = toml::from_str("[mariadb]\ndatabase = \"ohishi\"\n").unwrap();
    assert!(!config.mariadb.enabled());
    let config: Config = toml::from_str("[mariadb]\npassword = \"secret\"\n").unwrap();
    assert!(!config.mariadb.enabled());
    // host を空にすれば明示的に無効化できる
    let config: Config =
        toml::from_str("[mariadb]\nhost = \"\"\npassword = \"s\"\ndatabase = \"d\"\n").unwrap();
    assert!(!config.mariadb.enabled());
}

// ══════════════════════════════════════════════════════════════
// [kintai_push] (畳んだ勤怠の書き先、Refs #205 実装計画 04〜06)
// ══════════════════════════════════════════════════════════════

/// `[kintai_push]` だけを書いた TOML を組み立てる補助。
fn push_config(body: &str) -> Config {
    toml::from_str(&format!("[kintai_push]\n{body}")).unwrap()
}

#[test]
fn test_kintai_push_disabled_requires_nothing() {
    // 宣言していない = 何も書きに行かないので、接続先も tenant_id も要らない。
    let config: Config = toml::from_str("").unwrap();
    config.kintai_push.validate("").unwrap();
    // 揃っていない値が残っていても、無効なら起動を止めない
    let config = push_config("enabled = false\ndatabase_url = \"\"\ntenant_id = \"\"\n");
    config
        .kintai_push
        .validate("11111111-2222-3333-4444-555555555555")
        .unwrap();
}

#[test]
fn test_kintai_push_enabled_without_database_url_fails_startup() {
    // 「使うと宣言したのに書き先が無い」を黙って既定へ落とさない
    // ([database] enabled と同じ流儀)。
    let config = push_config("enabled = true\n");
    let err = config.kintai_push.validate("").unwrap_err();
    assert!(err.contains("database_url"), "{err}");

    // 空白だけも「無い」扱い
    let config = push_config("enabled = true\ndatabase_url = \"   \"\n");
    let err = config.kintai_push.validate("").unwrap_err();
    assert!(err.contains("database_url"), "{err}");
}

#[test]
fn test_kintai_push_transaction_pooler_fails_startup() {
    // transaction mode (6543) は prepared statement の扱いが sqlx の既定と噛み合わない。
    // 繋がってしまうので、実行時に散発的に壊れる前に起動で止める。
    let config = push_config(
        "enabled = true
database_url = \"postgres://kintai_writer:pw@aws-0.pooler.supabase.com:6543/postgres\"
tenant_id = \"11111111-2222-3333-4444-555555555555\"
",
    );
    let err = config.kintai_push.validate("").unwrap_err();
    assert!(err.contains("6543") || err.contains("session"), "{err}");
}

#[test]
fn test_kintai_push_enabled_without_tenant_id_fails_startup() {
    let config = push_config(
        "enabled = true
database_url = \"postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres\"
",
    );
    let err = config.kintai_push.validate("").unwrap_err();
    assert!(err.contains("tenant_id"), "{err}");
}

#[test]
fn test_kintai_push_tenant_id_must_be_a_uuid() {
    // alc_api.tenants.id をそのまま使う (新規採番しない) ので UUID 以外は打ち間違い。
    let config = push_config(
        "enabled = true
database_url = \"postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres\"
tenant_id = \"ichibanboshi\"
",
    );
    let err = config.kintai_push.validate("").unwrap_err();
    assert!(err.contains("UUID"), "{err}");
}

#[test]
fn test_kintai_push_tenant_id_must_match_kintai_events() {
    // テナントが 2 つの値に割れると、読み (alc) と書き (kintai) が別テナントを指す。
    // RLS が静かに全件遮断するか、別テナントへ書く。
    let config = push_config(
        "enabled = true
database_url = \"postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres\"
tenant_id = \"11111111-2222-3333-4444-555555555555\"
",
    );
    let err = config
        .kintai_push
        .validate("99999999-8888-7777-6666-555555555555")
        .unwrap_err();
    assert!(err.contains("match"), "{err}");
}

#[test]
fn test_kintai_push_tenant_id_matching_is_case_insensitive() {
    // UUID の大文字小文字は同じ値。表記揺れで起動を落とさない。
    let config = push_config(
        "enabled = true
database_url = \"postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres\"
tenant_id = \"AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE\"
",
    );
    config
        .kintai_push
        .validate("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        .unwrap();
}

#[test]
fn test_kintai_push_skips_matching_when_events_tenant_is_empty() {
    // [kintai_events] が MariaDB 直読み (= tenant_id 空) のオンプレでも push はできる。
    let config = push_config(
        "enabled = true
database_url = \"postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres\"
tenant_id = \"11111111-2222-3333-4444-555555555555\"
",
    );
    config.kintai_push.validate("").unwrap();
    // 空白だけの [kintai_events] tenant_id も「未設定」扱い
    config.kintai_push.validate("  ").unwrap();
}

#[test]
fn test_kintai_push_fully_configured_is_ok() {
    let toml_str = r#"
[kintai_events]
source = "http"
base_url = "https://alc.example"
tenant_id = "11111111-2222-3333-4444-555555555555"

[kintai_push]
enabled = true
database_url = "postgres://kintai_writer:pw@aws-0.pooler.supabase.com:5432/postgres"
tenant_id = "11111111-2222-3333-4444-555555555555"
connect_timeout_secs = 10
statement_timeout_secs = 900
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert!(config.kintai_push.enabled);
    assert_eq!(config.kintai_push.connect_timeout_secs, 10);
    assert_eq!(config.kintai_push.statement_timeout_secs, 900);
    config.kintai_events.validate().unwrap();
    config
        .kintai_push
        .validate(&config.kintai_events.tenant_id)
        .unwrap();
    // Debug / Clone を通しておく
    let cloned = config.kintai_push.clone();
    assert!(format!("{cloned:?}").contains("kintai_writer"));
}

#[test]
fn test_kosoku_config_defaults() {
    let config: Config = toml::from_str("").unwrap();
    // 就業規則どおり — 所定 7.5h / 法定 8h / 休憩は 10 分以上 (Refs #118)
    assert_eq!(config.kosoku.break_threshold_minutes, 10);
    assert_eq!(config.kosoku.prescribed_minutes, 450);
    assert_eq!(config.kosoku.legal_minutes, 480);
}

#[test]
fn test_kosoku_config_override() {
    let toml_str = r#"
[kosoku]
break_threshold_minutes = 5
prescribed_minutes = 480
legal_minutes = 480
"#;
    let config: Config = toml::from_str(toml_str).unwrap();
    assert_eq!(config.kosoku.break_threshold_minutes, 5);
    assert_eq!(config.kosoku.prescribed_minutes, 480);
    // 所定 = 法定 なら法定内残業は生じない
    assert_eq!(config.kosoku.legal_minutes, 480);
}

#[test]
fn test_kosoku_config_partial_keeps_other_defaults() {
    let config: Config = toml::from_str("[kosoku]\nbreak_threshold_minutes = 15\n").unwrap();
    assert_eq!(config.kosoku.break_threshold_minutes, 15);
    assert_eq!(config.kosoku.prescribed_minutes, 450);
    assert_eq!(config.kosoku.legal_minutes, 480);
    // Debug / Clone を通しておく
    let c = config.kosoku.clone();
    assert!(format!("{c:?}").contains("15"));
}

#[test]
fn test_restraint_rounding_defaults_to_the_paper_rule() {
    // 既定は紙のタイムカード表と同じ「区分ごとの切り捨て」(Refs #182)
    let config: Config = toml::from_str("").unwrap();
    assert_eq!(
        config.kosoku.restraint_rounding,
        RestraintRoundingToml::PaperPerSegment
    );
    assert_eq!(
        RestraintRounding::from(config.kosoku.restraint_rounding),
        RestraintRounding::PaperPerSegment
    );
}

#[test]
fn test_restraint_rounding_accepts_paper_per_segment_explicitly() {
    let config: Config = toml::from_str(
        "[kosoku]
restraint_rounding = \"paper_per_segment\"
",
    )
    .unwrap();
    assert_eq!(
        config.kosoku.restraint_rounding,
        RestraintRoundingToml::PaperPerSegment
    );
}

#[test]
fn test_restraint_rounding_can_go_back_to_floor_endpoints() {
    // 従来の丸めに TOML だけで戻せる (再デプロイのみ、コード変更不要)
    let config: Config = toml::from_str(
        "[kosoku]
restraint_rounding = \"floor_endpoints\"
",
    )
    .unwrap();
    assert_eq!(
        config.kosoku.restraint_rounding,
        RestraintRoundingToml::FloorEndpoints
    );
    assert_eq!(
        RestraintRounding::from(config.kosoku.restraint_rounding),
        RestraintRounding::FloorEndpoints
    );
    // 他の値は既定のまま
    assert_eq!(config.kosoku.break_threshold_minutes, 10);
}

#[test]
fn test_restraint_rounding_can_go_back_to_truncate_elapsed() {
    // #182 の「従来方式」に TOML だけで戻せる (再デプロイのみ、コード変更不要)
    let config: Config = toml::from_str(
        "[kosoku]
restraint_rounding = \"truncate_elapsed\"
",
    )
    .unwrap();
    assert_eq!(
        config.kosoku.restraint_rounding,
        RestraintRoundingToml::TruncateElapsed
    );
    assert_eq!(
        RestraintRounding::from(config.kosoku.restraint_rounding),
        RestraintRounding::TruncateElapsed
    );
    // Debug / Clone / Copy を通しておく
    let c = config.kosoku.restraint_rounding;
    assert!(format!("{c:?}").contains("TruncateElapsed"));
}

#[test]
fn test_restraint_rounding_rejects_an_unknown_value() {
    // 綴り間違いを黙って既定に落とさない (丸めは金額に効くので fail-loud)
    let err = toml::from_str::<Config>(
        "[kosoku]
restraint_rounding = \"nearest\"
",
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("unknown variant"),
        "unexpected error: {err}"
    );
}

// ── [kintai_send] (Refs #205 の 04b、送信側) ──

#[test]
fn test_kintai_send_disabled_requires_nothing() {
    let c: Config = toml::from_str("").unwrap();
    assert!(!c.kintai_send.enabled);
    assert_eq!(c.kintai_send.target_url, "");
    assert_eq!(c.kintai_send.auth_token, "");
    assert_eq!(c.kintai_send.timeout_secs, 120);
    c.kintai_send.validate().unwrap();
}

#[test]
fn test_kintai_send_enabled_requires_a_target() {
    let c: Config = toml::from_str("[kintai_send]\nenabled = true\n").unwrap();
    let err = c.kintai_send.validate().unwrap_err();
    assert!(err.contains("target_url"), "{err}");

    // 空白だけも空扱い
    let c: Config =
        toml::from_str("[kintai_send]\nenabled = true\ntarget_url = \"   \"\n").unwrap();
    assert!(c.kintai_send.validate().is_err());
}

#[test]
fn test_kintai_send_target_must_be_an_http_url() {
    // ホスト名だけ渡すと reqwest が実行時に落ちる。起動時に弾く
    for bad in ["relay.example", "ftp://relay.example", "/api/kintai"] {
        let toml_s = format!("[kintai_send]\nenabled = true\ntarget_url = \"{bad}\"\n");
        let c: Config = toml::from_str(&toml_s).unwrap();
        let err = c.kintai_send.validate().unwrap_err();
        assert!(err.contains("http"), "{bad}: {err}");
    }
    for ok in ["http://relay.example", "https://relay.example/"] {
        let toml_s = format!("[kintai_send]\nenabled = true\ntarget_url = \"{ok}\"\n");
        let c: Config = toml::from_str(&toml_s).unwrap();
        c.kintai_send.validate().unwrap();
    }
}

#[test]
fn test_kintai_send_full_config() {
    let c: Config = toml::from_str(
        r#"
[kintai_send]
enabled = true
target_url = "https://relay.example"
auth_token = "tok"
timeout_secs = 60
"#,
    )
    .unwrap();
    assert!(c.kintai_send.enabled);
    assert_eq!(c.kintai_send.auth_token, "tok");
    assert_eq!(c.kintai_send.timeout_secs, 60);
    c.kintai_send.validate().unwrap();
    // Debug / Clone も通る (Config が要求する)
    let _ = format!("{:?}", c.kintai_send.clone());
}
