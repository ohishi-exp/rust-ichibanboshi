use std::sync::Arc;

use axum::{
    routing::{get, post},
    Extension, Router,
};
use tokio_util::sync::CancellationToken;
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, CorsLayer};
use tower_http::trace::TraceLayer;

use crate::cakephp::CakephpClient;
use crate::config::{Config, RawConfig};
use crate::db;
use crate::kyuyo;
use crate::repo::TiberiusRepo;
use crate::routes;
use crate::sqlite::{DynLocalStore, LocalStore};

/// 生イベントの読み先を宣言から組み立てる。
///
/// **HTTP サーバーと CLI (`push` / `recalc` / `sync`) の共有点。** どちらから来ても
/// 同じ宣言で同じ読み先になるので、「画面では MariaDB を読んでいるのにバッチは
/// HTTP を読んでいた」という食い違いが構造的に起きない。
///
/// 戻り値の `&'static str` は `/health` の `backends.kintai_events` に出す名前。
/// pool は lazy なので、ここでは DB 接続は張られない。
pub fn build_kintai_events_repo(
    config: &Config,
) -> Result<(crate::kintai_repo::DynKintaiEventsRepo, &'static str), Box<dyn std::error::Error>> {
    config.kintai_events.validate()?;
    let mariadb_events: Option<crate::kintai_repo::DynKintaiEventsRepo> =
        if config.mariadb.enabled() {
            Some(Arc::new(crate::kintai_repo::MariadbKintaiEventsRepo::new(
                &config.mariadb,
            )))
        } else {
            None
        };
    if config.kintai_events.http_enabled() {
        if mariadb_events.is_none() {
            tracing::warn!("kintai events via HTTP without mariadb — 打刻とフェリーは読めない");
        }
        let repo = crate::kintai_http_repo::HttpKintaiEventsRepo::new(
            &config.kintai_events,
            mariadb_events,
        )?;
        return Ok((Arc::new(repo), "http"));
    }
    if let Some(repo) = mariadb_events {
        return Ok((repo, "mariadb"));
    }
    tracing::info!("mariadb not configured — /api/kintai/events returns 503");
    Ok((
        Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo),
        "disabled",
    ))
}

/// 拘束サマリの計算パラメータを設定から作る。サーバーと CLI の共有点。
pub fn build_kosoku_params(config: &Config) -> crate::kosoku::KosokuParams {
    crate::kosoku::KosokuParams {
        break_threshold_minutes: config.kosoku.break_threshold_minutes,
        prescribed_minutes: config.kosoku.prescribed_minutes,
        legal_minutes: config.kosoku.legal_minutes,
        restraint_rounding: config.kosoku.restraint_rounding.into(),
    }
}

/// HTTP サーバーを起動し、shutdown token が cancel されるまでブロック
pub async fn run(
    config: Config,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    // SQL Server (CAPE#01) を**使うと宣言したか**で分岐する (オンプレ / GCP 両対応)。
    // 宣言した場合は create_pool が起動時に `SELECT 1` を打ち、繋がらなければ
    // ここで Err → 起動失敗 (オンプレの既定・従来どおり)。宣言していない場合は
    // pool を作らず、SQL Server 依存ルートは全て 503 fail-closed になる。
    let repo: crate::repo::DynRepo = if config.database.enabled {
        let pool = db::create_pool(&config.database).await?;
        Arc::new(TiberiusRepo::new(pool))
    } else {
        tracing::warn!(
            "[database] enabled = false — this instance does not use SQL Server (CAPE#01). \
             /api/sales/*, /api/schema/*, /api/surcharge/*, /api/unchin/*, /api/uriage/*, \
             /api/employees, /api/vehicles return 503. /health reports backends.sqlserver=disabled."
        );
        Arc::new(TiberiusRepo::disabled())
    };

    // SQLite local store (Phase 2、担当者別売上 summary 永続化)
    let local_store: DynLocalStore = Arc::new(LocalStore::open(&config.sqlite.path)?);

    // CakePHP fetch client (Phase 2、masters / editable-months pull)
    // base_url 空でも build は成功し、各 endpoint で is_enabled() を見て 503 を返す
    let cakephp_client = Arc::new(CakephpClient::new(
        config.cakephp.base_url.clone(),
        config.cakephp.timeout_secs,
    )?);

    // 生 NDJSON.gz 出力先 (Phase 2、R2 warm backup の input)
    let raw_cfg = Arc::new(RawConfig {
        dir: config.raw.dir.clone(),
    });

    // 給与大臣 (OHKEN) 読み取り (#82)。未設定なら stub を挿して該当ルートだけ 503。
    // pool は起動時テストなし — 給与大臣 PC 停止でも本サービス全体は起動する
    let kyuyo_repo: kyuyo::repo::DynKyuyoRepo = if config.kyuyo.db_enabled() {
        let pool = kyuyo::repo::create_kyuyo_pool(&config.kyuyo).await?;
        Arc::new(kyuyo::repo::TiberiusKyuyoRepo::new(pool))
    } else {
        tracing::info!("kyuyo database not configured — /api/kyuyo/* returns 503");
        Arc::new(kyuyo::repo::NotConfiguredKyuyoRepo)
    };
    let kyuyo_auth = Arc::new(kyuyo::introspect::KyuyoAuthState::from_config(
        &config.kyuyo,
    ));
    // 給与 DB (OHKEN、非力な PC) を触る区間の同時実行制限 (Refs #369)
    let kyuyo_limiter = Arc::new(routes::kyuyo::KyuyoLimiter::new());

    // タイムカードの SQLite derived store (Refs #106 Phase 2)。open 失敗は Noop に
    // 落として CakePHP 素通し中継を維持する
    let kintai_store: crate::kintai_store::DynKintaiStore = if config.cakephp.sqlite_path.is_empty()
    {
        tracing::info!("kintai sqlite_path is empty — derived store disabled (relay only)");
        Arc::new(crate::kintai_store::NoopKintaiStore)
    } else {
        match crate::kintai_store::KintaiStore::open(&config.cakephp.sqlite_path) {
            Ok(store) => Arc::new(store),
            Err(e) => {
                tracing::warn!("kintai store open failed — falling back to relay only: {e}");
                Arc::new(crate::kintai_store::NoopKintaiStore)
            }
        }
    };

    // 勤怠の生イベント読み取り。読み先は宣言で決まる (Refs #116 / #205 実装計画 02)。
    //
    //   [kintai_events] source = "mariadb"  → 社内 MariaDB 直読み (既定 = オンプレ)
    //   [kintai_events] source = "http"     → rust-alc-api の /api/dtako/events (GCP)
    //
    // 宣言が足りなければ**黙って既定へ落とさず起動を失敗させる** ([database] enabled
    // と同じ流儀)。MariaDB も無い形態では Disabled を挿して `/api/kintai/events` だけ
    // 503 — 空配列を返して「0 件」に見せない。pool は lazy なので DB 停止中でも
    // 起動は失敗しない
    let (kintai_events_repo, kintai_events_backend) = build_kintai_events_repo(&config)?;

    // 勤怠の月別バージョン (ETag) 読み取り (Refs #184)。events と同じ MariaDB を
    // 読むが trait / pool は分離 — 未設定なら Disabled で 503 fail-closed
    let kintai_version_repo: crate::kintai_version::DynKintaiVersionRepo =
        if config.mariadb.enabled() {
            Arc::new(crate::kintai_version::MariadbKintaiVersionRepo::new(
                &config.mariadb,
            ))
        } else {
            tracing::info!("mariadb not configured — /api/kintai/version returns 503");
            Arc::new(crate::kintai_version::DisabledKintaiVersionRepo)
        };

    // 畳んだ勤怠の書き先 (Refs #205 の 04b)。**宣言したら起動時に必ず繋ぐ** —
    // 「受け取ったがどこにも書いていない」を作らないため ([database] enabled と
    // 同じ流儀)。宣言していなければ挿さらず、打刻の受け口は 503 で fail-closed
    let kintai_pg_store: routes::kintai_timecard::DynKintaiPgStore = if config.kintai_push.enabled {
        config
            .kintai_push
            .validate(&config.kintai_events.tenant_id)?;
        Some(Arc::new(
            crate::kintai_push::KintaiPgStore::connect(&config.kintai_push).await?,
        ))
    } else {
        tracing::info!("kintai_push not enabled — /api/kintai/timecard returns 503");
        None
    };

    // 拘束サマリの計算パラメータ (所定 7.5h / 法定 8h / 休憩 10 分。Refs #118)。
    // 就業規則が変わったら TOML で追随できるよう config から取る。
    // 打刻の転送先 (Refs #205 の 04b、送信側)。宣言していなければ挿さらず
    // `/api/kintai/timecard/send` は 503 で fail-closed
    config.kintai_send.validate()?;
    let kintai_send_target: Option<crate::kintai_send::DynTimecardTarget> =
        if config.kintai_send.enabled {
            Some(Arc::new(crate::kintai_send::HttpTimecardTarget::new(
                &config.kintai_send,
            )?))
        } else {
            tracing::info!("kintai_send not enabled — /api/kintai/timecard/send returns 503");
            None
        };

    let kosoku_params = Arc::new(build_kosoku_params(&config));

    // 拘束サマリ store (Refs #106 Phase 3)。open 失敗は Disabled (route が 503) —
    // このストアはキャッシュではなく relay push の一次置き場のため fail-closed
    let restraint_store: crate::restraint_store::DynRestraintStore =
        if config.restraint.sqlite_path.is_empty() {
            tracing::info!("restraint sqlite_path is empty — /api/restraint/* returns 503");
            Arc::new(crate::restraint_store::DisabledRestraintStore)
        } else {
            match crate::restraint_store::RestraintStore::open(&config.restraint.sqlite_path) {
                Ok(store) => Arc::new(store),
                Err(e) => {
                    tracing::warn!(
                        "restraint store open failed — /api/restraint/* returns 503: {e}"
                    );
                    Arc::new(crate::restraint_store::DisabledRestraintStore)
                }
            }
        };

    // 給与の SQLite derived store (Refs #106 Phase 1)。open 失敗は Noop に落として
    // live 読みへフォールバック — キャッシュの健全性で読み機能を殺さない
    let kyuyo_store: kyuyo::store::DynKyuyoStore = if config.kyuyo.sqlite_path.is_empty() {
        tracing::info!("kyuyo sqlite_path is empty — derived store disabled (live reads only)");
        Arc::new(kyuyo::store::NoopKyuyoStore)
    } else {
        match kyuyo::store::KyuyoStore::open(&config.kyuyo.sqlite_path) {
            Ok(store) => Arc::new(store),
            Err(e) => {
                tracing::warn!("kyuyo store open failed — falling back to live reads: {e}");
                Arc::new(kyuyo::store::NoopKyuyoStore)
            }
        }
    };

    // 宣言したバックエンドの一覧を /health に出す (実行形態を外から判別可能にする)。
    let health_state = crate::routes::health::HealthState {
        sqlserver: config.database.enabled,
        mariadb: config.mariadb.enabled(),
        kyuyo: config.kyuyo.db_enabled(),
        kintai_events: kintai_events_backend,
    };

    let origins: Vec<_> = config
        .cors
        .allowed_origins
        .iter()
        .filter_map(|o| o.parse().ok())
        .collect();

    let cors = CorsLayer::new()
        .allow_origin(AllowOrigin::list(origins))
        .allow_methods(AllowMethods::any())
        .allow_headers(AllowHeaders::any());

    let api_routes = Router::new()
        .route("/sales/monthly", get(routes::sales::monthly))
        .route("/sales/by-department", get(routes::sales::by_department))
        .route("/sales/by-customer", get(routes::sales::by_customer))
        .route("/sales/yoy", get(routes::sales::yoy))
        .route("/sales/daily", get(routes::sales::daily))
        .route("/sales/customer-trend", get(routes::sales::customer_trend))
        .route("/sales/customer-yoy", get(routes::sales::customer_yoy))
        .route(
            "/sales/customer-yoy-by-dept",
            get(routes::sales::customer_yoy_by_dept),
        )
        .route(
            "/sales/departments",
            get(routes::sales::list_departments_handler),
        )
        .route(
            "/sales/customer-detail",
            get(routes::sales::customer_detail),
        )
        .route(
            "/sales/vehicle-daily",
            get(routes::vehicle_daily::vehicle_daily),
        )
        .route("/surcharge/base", get(routes::surcharge::surcharge_base))
        .route("/vehicles", get(routes::surcharge::vehicles))
        .route("/employees", get(routes::employees::employees))
        .route("/unchin/candidates", get(routes::unchin::unchin_candidates))
        .route("/unchin/summary", get(routes::unchin::unchin_summary))
        .route(
            "/unchin/subcontractor-net",
            get(routes::unchin::unchin_subcontractor_net),
        )
        .route(
            "/unchin/subcontractor-net-detail",
            get(routes::unchin::unchin_subcontractor_net_detail),
        )
        .route(
            "/unchin/customer-net",
            get(routes::unchin::unchin_customer_net),
        )
        .route(
            "/unchin/customer-net-detail",
            get(routes::unchin::unchin_customer_net_detail),
        )
        .route("/uriage/by-person", post(routes::uriage::by_person))
        .route("/uriage/recalc", post(routes::uriage::recalc))
        .route("/uriage/daily", get(routes::uriage::daily))
        .route(
            "/uriage/person-monthly-totals",
            get(routes::uriage::person_monthly_totals),
        )
        .route(
            "/uriage/person-partner-totals",
            get(routes::uriage::person_partner_totals),
        )
        .route("/uriage/r2/pending", get(routes::uriage::r2_pending))
        .route(
            "/uriage/raw/{month}/{eigyosho_id}",
            get(routes::uriage::raw_get),
        )
        .route(
            "/uriage/raw/{month}/{eigyosho_id}/ack",
            post(routes::uriage::raw_ack),
        )
        .route("/uriage/admin/delete", post(routes::uriage::admin_delete))
        .route("/uriage/admin/rebuild", post(routes::uriage::admin_rebuild))
        .route("/uriage/verify", get(routes::uriage::verify))
        .route("/uriage/verify-debug", get(routes::uriage::verify_debug))
        .route(
            "/uriage/verify-history",
            get(routes::uriage::verify_history),
        )
        .route("/uriage/recalc-jobs", get(routes::uriage::list_recalc_jobs))
        .route("/kintai/daily", get(routes::kintai::daily))
        .route("/kintai/events", get(routes::kintai::events))
        .route("/kintai/kosoku-daily", get(routes::kintai::kosoku_daily))
        .route("/kintai/pdf-json", get(routes::kintai::pdf_json))
        .route("/kintai/version", get(routes::kintai_version::version))
        // 打刻の受け口 (Refs #205 の 04b)。GCP 側だけが使う — オンプレは
        // [kintai_push] が無効なので両方 503 で fail-closed
        .route("/kintai/timecard", post(routes::kintai_timecard::receive))
        .route(
            "/kintai/timecard/signatures",
            get(routes::kintai_timecard::signatures),
        )
        // 送信側 (オンプレが GCP へ渡す)。relay が起動する
        .route("/kintai/timecard/send", post(routes::kintai_timecard::send))
        .route("/kyuyo/companies", get(routes::kyuyo::companies))
        .route("/kyuyo/databases", get(routes::kyuyo::databases))
        .route("/kyuyo/payroll", get(routes::kyuyo::payroll))
        .route("/kyuyo/employees", get(routes::kyuyo::employees))
        .route("/kyuyo/sync", post(routes::kyuyo::sync))
        .route("/kyuyo/synced-months", get(routes::kyuyo::synced_months))
        .route(
            "/restraint/summaries",
            axum::routing::put(routes::restraint::put_summaries),
        )
        .route(
            "/restraint/wage-source",
            get(routes::restraint::wage_source),
        )
        .route(
            "/restraint/synced-months",
            get(routes::restraint::synced_months),
        );

    let schema_routes = Router::new()
        .route("/schema/tables", get(routes::schema::list_tables))
        .route("/schema/columns", get(routes::schema::list_columns))
        .route("/schema/sample", get(routes::schema::sample_data));

    let app = Router::new()
        .route("/health", get(routes::health::health))
        .nest("/api", api_routes)
        .nest("/api", schema_routes)
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(Extension(repo))
        .layer(Extension(health_state))
        .layer(Extension(local_store))
        .layer(Extension(cakephp_client))
        .layer(Extension(raw_cfg))
        .layer(Extension(kyuyo_repo))
        .layer(Extension(kyuyo_auth))
        .layer(Extension(kyuyo_limiter))
        .layer(Extension(kyuyo_store))
        .layer(Extension(kintai_store))
        .layer(Extension(kintai_events_repo))
        .layer(Extension(kintai_version_repo))
        .layer(Extension(kintai_pg_store))
        .layer(Extension(kintai_send_target))
        .layer(Extension(kosoku_params))
        .layer(Extension(restraint_store));

    let addr = config.addr();
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!("Listening on {addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
            tracing::info!("Shutdown signal received");
        })
        .await?;

    Ok(())
}
