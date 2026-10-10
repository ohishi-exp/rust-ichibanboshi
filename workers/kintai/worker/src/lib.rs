//! 勤怠 Worker (Refs ohishi-exp/rust-ichibanboshi#322)。社内 MariaDB (打刻・デジタコの生行) の読み出しを
//! Workers VPC (TCP) → 既存 Tunnel → MariaDB で行う。プロトコルは自作の kintai-mysql (平文・mysql_native_password)。
//! Supabase (勤怠スキーマ `kintai.*`) は Hyperdrive (`KINTAI_HYPERDRIVE`) + alc-worker-db で読む。
//!
//! 口:
//! - 到達の確認用の `POST /probe` (接続 → handshake → 認証 → `SET SESSION max_statement_time=60` →
//!   `SELECT 1, VERSION(), @@character_set_connection, CURRENT_USER()` → `COM_QUIT`)。1 リクエスト 1 接続。
//! - Supabase を読むだけの `GET /api/kintai/{day-summaries,shift-overlaps,shift-days,change-log,wage-range}`
//!   ([`reads`]。Cloud Run 版と同じ応答・同じ 400/502/503。テナントは `KINTAI_TENANT_ID` の設定 pin)。
//! - 社内 MariaDB を直接読む `GET /api/kintai/{events,rest-diff,reading-dates,tail-gap-probe}`
//!   (オンプレ版と同じ応答・同じ 400/502/503。検査・SQL の引数・行 → JSON・応答は kintai-logic の `mariadb_reads`)。
//!   `/probe` と同じ接続・認証・`SET SESSION max_statement_time=60` の上で 1 本のクエリを流す (1 リクエスト 1 接続)。
//! - 同じく社内 MariaDB を読む `GET /api/kintai/day-events`・`GET /api/dtako/worktime` (オンプレ版と同じ応答・同じ
//!   400/502/503。検査・SQL の引数・応答は kintai-logic の `dtako_reads`、畳み方は共有 crate kintai-dtako)。
//!   day-events のリンクの base URL は `[vars]` の `KINTAI_RYOHI_BASE_URL`・`KINTAI_DTAKO_BASE_URL` (空 = そのリンクを省く)。
//! - 同じく社内 MariaDB を読む `GET /api/kintai/{kosoku-daily,version,timecard/drivers,timecard/events}` (オンプレ版と
//!   同じ応答・同じ 400/502/503。検査・SQL の引数・応答は kintai-logic の `kosoku_reads`、応答を組む部分は共有 crate
//!   kintai-kosoku)。kosoku-daily と version は 1 接続で遡り起点の 2 本 → 本体 (→ フェリー) を流す。
//!   version の etag の版は build.rs が焼く `KINTAI_WORKER_OUTPUT_SHA` (オンプレ版の `KINTAI_OUTPUT_SHA` とは別の値)。
//!
//! 到達面: fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。
//! **認可なし (ユーザー決定 2026-10-10、一番星と同じ)。** 関門は呼び手の側 (relay の共有 secret、kyuyo-mcp の OAuth)。
//! 資格情報は Secrets Store の binding で読み、呼び手の cookie・Authorization は受け取らない。
//! 社内 MariaDB へは SELECT だけ (SET SESSION はこの接続の打ち切り時間で、データは書かない)。Supabase も読むだけ。

mod conn;
mod probe;
mod reads;
mod tcp;
mod transport;

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use kintai_logic::common::{mariadb_fail, mariadb_unconfigured, Fail};
use kintai_logic::dtako_reads::{DtakoRead, Links, DTAKO_BASE_URL_VAR, RYOHI_BASE_URL_VAR};
use kintai_logic::kosoku_reads::{
    anchors_from, ferry_or_empty, head_sqls, parse_kosoku_daily, parse_timecard_drivers,
    parse_timecard_events, parse_version, timecard_fail, version_respond, version_sql,
    DailyRequest, HeadAnchors, KosokuRead, MAX_ALL_DRIVERS_BYTES,
};
use kintai_logic::mariadb_reads::{jst_today, MariadbRead};

use kintai_mysql::response::ResultSet;
use kintai_mysql::retry::{should_retry, RETRY_DELAY_MS};
use worker::{console_error, console_log, event, Context, Date, Delay, Env, Request, Response};

use conn::Session;
use probe::{
    log_line, parse_creds, reply_for, reply_for_route, route, Creds, Failure, ProbeOk, Reply,
    Route, Stage,
};

/// TCP を開くまでの上限。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// handshake と認証の上限 (それぞれ)。
const LOGIN_TIMEOUT: Duration = Duration::from_secs(10);
/// クエリの上限。サーバー側の max_statement_time (60 秒) より少し長くする。
const QUERY_TIMEOUT: Duration = Duration::from_secs(65);

/// 社内 MariaDB の convoy 対策 (オンプレ版 src/kintai_repo.rs と同じ)。
const SET_STATEMENT_TIME: &str = "SET SESSION max_statement_time=60";
const PROBE_SQL: &str = "SELECT 1, VERSION(), @@character_set_connection, CURRENT_USER()";

/// version の etag に畳む「応答を形づくるコード」の版 (build.rs が workers/kintai の 4 crate の src を畳んで焼く)。
const OUTPUT_SHA: &str = env!("KINTAI_WORKER_OUTPUT_SHA");

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    let route = route(req.method().as_ref(), &req.path());
    if let Route::Read(read) = route {
        let url = req.url()?;
        let outcome = reads::serve(read, url.query().unwrap_or(""), &env);
        return run_read(read.as_str(), outcome).await;
    }
    if let Route::Mariadb(read) = route {
        let url = req.url()?;
        let outcome = mariadb_read(read, url.query().unwrap_or(""), &env);
        return run_read(read.as_str(), outcome).await;
    }
    if let Route::Dtako(read) = route {
        let url = req.url()?;
        let outcome = dtako_read(read, url.query().unwrap_or(""), &env);
        return run_read(read.as_str(), outcome).await;
    }
    if let Route::Kosoku(read) = route {
        let url = req.url()?;
        let outcome = kosoku_read(read, url.query().unwrap_or(""), &env);
        return run_bytes(read.as_str(), outcome).await;
    }
    // reply_for_route で弾かれなかったのは POST /probe だけ
    let reply = match reply_for_route(route) {
        Some(reply) => reply,
        None => run_probe(&env).await,
    };
    respond(reply)
}

fn respond(reply: Reply) -> worker::Result<Response> {
    let headers = worker::Headers::new();
    headers.set("content-type", "application/json")?;
    Ok(Response::ok(reply.body)?
        .with_status(reply.status)
        .with_headers(headers))
}

/// 読む口 (Supabase・MariaDB)。成功は JSON (200)、失敗は平文 (元の axum の `(StatusCode, String)` と同じ text/plain)。
async fn run_read(
    name: &str,
    outcome: impl Future<Output = Result<serde_json::Value, Fail>>,
) -> worker::Result<Response> {
    let started = Date::now().as_millis();
    let outcome = outcome.await;
    let ms = Date::now().as_millis().saturating_sub(started);
    match outcome {
        Ok(body) => {
            console_log!("kintai {name}: ok ({ms} ms)");
            Response::from_json(&body)
        }
        Err(f) => {
            console_error!("kintai {name}: {} ({ms} ms) {}", f.status, f.body);
            let headers = worker::Headers::new();
            headers.set("content-type", "text/plain; charset=utf-8")?;
            Ok(Response::ok(f.body)?
                .with_status(f.status)
                .with_headers(headers))
        }
    }
}

async fn run_probe(env: &Env) -> Reply {
    let started = Date::now().as_millis();
    let outcome = probe(env, started).await;
    let ms = Date::now().as_millis().saturating_sub(started);
    match &outcome {
        Ok(_) => console_log!("kintai probe: ok ({ms} ms)"),
        Err(f) => console_error!("{}", log_line(f, ms)),
    }
    reply_for(outcome)
}

/// 社内 MariaDB を直接読む口。検査 (400) → 資格情報 (無ければ 503) → 接続・クエリ (失敗は 502) → 応答 (元の handler と同じ順)。
async fn mariadb_read(
    read: MariadbRead,
    query: &str,
    env: &Env,
) -> Result<serde_json::Value, Fail> {
    let req = read.parse(query)?;
    let sql = req.sql_text()?;
    let creds = load_creds(env).await.ok_or_else(mariadb_unconfigured)?;
    let set = run_sql(env, &creds, &sql).await.map_err(db_fail)?;
    req.respond(&set.rows, jst_today(Date::now().as_millis()))
}

/// day-events・dtako/worktime。順は [`mariadb_read`] と同じ (検査 → 資格情報 → 接続・クエリ → 応答)。
async fn dtako_read(read: DtakoRead, query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    let req = read.parse(query)?;
    let sql = req.sql_text()?;
    let creds = load_creds(env).await.ok_or_else(mariadb_unconfigured)?;
    let set = run_sql(env, &creds, &sql).await.map_err(db_fail)?;
    let links = Links {
        ryohi_base_url: var_or_empty(env, RYOHI_BASE_URL_VAR),
        dtako_base_url: var_or_empty(env, DTAKO_BASE_URL_VAR),
    };
    req.respond(&set.rows, &links)
}

/// 接続・クエリの失敗を 502 (`MariaDB query failed: <段>:<種別>`) に。
fn db_fail(f: Failure) -> Fail {
    let kind = format!("{}:{}", f.stage.as_str(), f.kind);
    mariadb_fail(&kind)
}

/// 本文のバイト列と、あれば `ETag` ヘッダの値。
struct Bytes {
    body: Vec<u8>,
    etag: Option<String>,
}

impl Bytes {
    fn json(value: &serde_json::Value) -> Self {
        Self {
            body: serde_json::to_vec(value).unwrap_or_default(),
            etag: None,
        }
    }
}

/// [`run_read`] のバイト列版 (kosoku-daily の全員版は直列化済みの本文を返すため)。成功は `application/json`。
async fn run_bytes(
    name: &str,
    outcome: impl Future<Output = Result<Bytes, Fail>>,
) -> worker::Result<Response> {
    let started = Date::now().as_millis();
    let outcome = outcome.await;
    let ms = Date::now().as_millis().saturating_sub(started);
    let (status, content_type, body, etag) = match outcome {
        Ok(b) => {
            console_log!("kintai {name}: ok ({ms} ms, {} bytes)", b.body.len());
            (200, "application/json", b.body, b.etag)
        }
        Err(f) => {
            console_error!("kintai {name}: {} ({ms} ms) {}", f.status, f.body);
            (
                f.status,
                "text/plain; charset=utf-8",
                f.body.into_bytes(),
                None,
            )
        }
    };
    let headers = worker::Headers::new();
    headers.set("content-type", content_type)?;
    if let Some(etag) = etag {
        headers.set("etag", &etag)?;
    }
    Ok(Response::from_bytes(body)?
        .with_status(status)
        .with_headers(headers))
}

/// kosoku-daily・version・timecard/drivers・timecard/events。順は元の handler と同じ
/// (検査 (400) → 資格情報 (503) → 接続・クエリ (502) → 応答)。timecard の 2 本は元と同じく読みの失敗を全部 502
/// (`kintai events read failed: …`) にする (資格情報が無いときも)。
async fn kosoku_read(read: KosokuRead, query: &str, env: &Env) -> Result<Bytes, Fail> {
    match read {
        KosokuRead::KosokuDaily => {
            let req = parse_kosoku_daily(query)?;
            let creds = load_creds(env).await.ok_or_else(mariadb_unconfigured)?;
            let mut session = open(env, &creds).await.map_err(db_fail)?;
            let out = kosoku_daily(&mut session, &req).await;
            session.quit().await;
            out
        }
        KosokuRead::Version => {
            let month = parse_version(query)?;
            let creds = load_creds(env).await.ok_or_else(mariadb_unconfigured)?;
            let mut session = open(env, &creds).await.map_err(db_fail)?;
            let out = version(&mut session, &month).await;
            session.quit().await;
            out
        }
        KosokuRead::TimecardDrivers => {
            let req = parse_timecard_drivers(query)?;
            let creds = load_creds(env)
                .await
                .ok_or_else(|| timecard_fail(mariadb_unconfigured()))?;
            let started = Date::now().as_millis();
            let set = run_sql(env, &creds, &req.sql()?).await;
            let set = set.map_err(|f| timecard_fail(db_fail(f)))?;
            let elapsed = Date::now().as_millis().saturating_sub(started);
            Ok(Bytes::json(&req.respond(&set.rows, elapsed)?))
        }
        KosokuRead::TimecardEvents => {
            let req = parse_timecard_events(query)?;
            let creds = load_creds(env)
                .await
                .ok_or_else(|| timecard_fail(mariadb_unconfigured()))?;
            let started = Date::now().as_millis();
            let set = run_sql(env, &creds, &req.sql()?).await;
            let set = set.map_err(|f| timecard_fail(db_fail(f)))?;
            let elapsed = Date::now().as_millis().saturating_sub(started);
            Ok(Bytes::json(&req.respond(&set.rows, elapsed)?))
        }
    }
}

/// 遡り起点 (`HEAD_RUN_ENDS_SQL` → `HEAD_PUNCHES_SQL`)。
async fn read_anchors(session: &mut Session, month: &str) -> Result<HeadAnchors, Fail> {
    let [runs_sql, punches_sql] = head_sqls(month)?;
    let runs = query(session, &runs_sql).await.map_err(db_fail)?;
    let punches = query(session, &punches_sql).await.map_err(db_fail)?;
    anchors_from(month, &runs.rows, &punches.rows)
}

/// kosoku-daily の 4 本 (起点 2 本 → 生イベント → フェリー)。フェリーの失敗は元と同じく控除 0 で続ける (502 にしない)。
/// 全員版は乗務員 1 人ぶんずつ直列化して書く (応答全体の JSON の木を持たない)。
async fn kosoku_daily(session: &mut Session, req: &DailyRequest) -> Result<Bytes, Fail> {
    let anchors = read_anchors(session, &req.month).await?;
    let events = query(session, &req.events_sql(&anchors)?).await;
    let events = events.map_err(db_fail)?;
    let ferry = match query(session, &req.ferry_sql()?).await {
        Ok(set) => ferry_or_empty(&set.rows),
        Err(f) => {
            console_log!(
                "kintai kosoku-daily: ferry failed ({}:{}), ferry_minus stays 0",
                f.stage.as_str(),
                f.kind
            );
            Vec::new()
        }
    };
    match req.driver {
        Some(driver) => {
            let (body, days) = req.respond_single(driver, &events.rows, &ferry)?;
            console_log!("kintai kosoku-daily: driver {driver}, {days} days");
            Ok(Bytes::json(&body))
        }
        None => {
            let (body, drivers) =
                req.write_all(&anchors, events.rows, ferry, MAX_ALL_DRIVERS_BYTES)?;
            console_log!("kintai kosoku-daily: {drivers} drivers");
            Ok(Bytes { body, etag: None })
        }
    }
}

/// version の 3 本 (起点 2 本 → `VERSION_SQL`)。etag は本文と `ETag` ヘッダの両方に。
async fn version(session: &mut Session, month: &str) -> Result<Bytes, Fail> {
    let anchors = read_anchors(session, month).await?;
    let set = query(session, &version_sql(month, &anchors)?).await;
    let set = set.map_err(db_fail)?;
    let (body, etag) = version_respond(month, &set.rows, OUTPUT_SHA)?;
    Ok(Bytes {
        body: serde_json::to_vec(&body).unwrap_or_default(),
        etag: Some(etag),
    })
}

/// `[vars]` の文字列。無ければ空 (= リンクを省く。オンプレ版の設定の既定と同じ)。
fn var_or_empty(env: &Env, name: &str) -> String {
    env.var(name).map(|v| v.to_string()).unwrap_or_default()
}

async fn probe(env: &Env, started: u64) -> Result<ProbeOk, Failure> {
    let creds = load_creds(env)
        .await
        .ok_or(Failure::new(Stage::Secret, "missing"))?;
    let set = run_sql(env, &creds, PROBE_SQL).await?;

    let unexpected = || Failure::new(Stage::Query, "unexpected_result");
    if set.text(0, 0) != Some("1") {
        return Err(unexpected());
    }
    let version = set.text(0, 1).ok_or_else(unexpected)?.to_string();
    let charset = set.text(0, 2).ok_or_else(unexpected)?.to_string();
    let current_user = set.text(0, 3).ok_or_else(unexpected)?;
    Ok(ProbeOk {
        ok: true,
        version,
        charset,
        user_matches: probe::user_matches(current_user, &creds.user),
        elapsed_ms: Date::now().as_millis().saturating_sub(started),
    })
}

/// 1 接続を開いて `sql` を 1 本流し、COM_QUIT で閉じる。
async fn run_sql(env: &Env, creds: &Creds, sql: &str) -> Result<ResultSet, Failure> {
    let mut session = open(env, creds).await?;
    let result = query(&mut session, sql).await;
    session.quit().await;
    result
}

/// 1 接続を開く (接続 → handshake → 認証 → 打ち切り時間の設定)。この上で [`query`] を何本でも流せる
/// (kosoku-daily は 4 本、version は 3 本)。閉じるのは呼び手 (`Session::quit`)。
///
/// 認証パケットを送る前 (connect・handshake の段) の失敗だけ、新しい接続で `kintai_mysql::retry` の上限まで
/// やり直す (間を空けない再接続で handshake の前に閉じられることがあるため)。認証以降の失敗はそのまま返す。
async fn open(env: &Env, creds: &Creds) -> Result<Session, Failure> {
    let mut retries = 0;
    loop {
        match open_once(env, creds).await {
            Err(f) if f.stage.phase().is_some_and(|p| should_retry(p, retries)) => {
                retries += 1;
                console_log!(
                    "kintai mariadb: retry {retries} after {}:{}",
                    f.stage.as_str(),
                    f.kind
                );
                Delay::from(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
            result => return result,
        }
    }
}

/// 1 回ぶん: 接続 → handshake → 認証 → 打ち切り時間の設定。途中で失敗したら COM_QUIT を送って閉じる。
async fn open_once(env: &Env, creds: &Creds) -> Result<Session, Failure> {
    let socket = step(Stage::Connect, CONNECT_TIMEOUT, async {
        transport::open(env)
            .await
            .map_err(|_| "transport".to_string())
    })
    .await?;
    let mut session = Session::new(socket);
    match login(&mut session, creds).await {
        Ok(()) => Ok(session),
        Err(f) => {
            session.quit().await;
            Err(f)
        }
    }
}

/// handshake → 認証 → 打ち切り時間の設定。
async fn login(session: &mut Session, creds: &Creds) -> Result<(), Failure> {
    let (seq, hs) = step(Stage::Handshake, LOGIN_TIMEOUT, session.handshake()).await?;
    step(
        Stage::Auth,
        LOGIN_TIMEOUT,
        session.authenticate(seq, &hs, creds),
    )
    .await?;
    query(session, SET_STATEMENT_TIME).await.map(|_| ())
}

/// 開いた接続で `sql` を 1 本流す (1 本ごとに [`QUERY_TIMEOUT`] で打ち切る)。
async fn query(session: &mut Session, sql: &str) -> Result<ResultSet, Failure> {
    step(Stage::Query, QUERY_TIMEOUT, session.query(sql)).await
}

/// 1 段を `limit` で打ち切って走らせ、失敗に段を付ける。
async fn step<T>(
    stage: Stage,
    limit: Duration,
    fut: impl Future<Output = Result<T, String>>,
) -> Result<T, Failure> {
    match select(pin!(fut), pin!(Delay::from(limit))).await {
        Either::Left((result, _)) => result.map_err(|kind| Failure::new(stage, kind)),
        Either::Right(_) => Err(Failure::new(stage, "timeout")),
    }
}

/// 資格情報を読んで検証する。binding が無い・secret が未投入・JSON が不正は `None` (中身はどこにも出さない)。
async fn load_creds(env: &Env) -> Option<Creds> {
    let store = env.secret_store("KINTAI_MARIADB").ok()?;
    let json = store.get().await.ok().flatten()?;
    parse_creds(&json)
}
