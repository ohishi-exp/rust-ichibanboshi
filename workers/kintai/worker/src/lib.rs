//! 勤怠 Worker (Refs ohishi-exp/rust-ichibanboshi#322)。社内 MariaDB (打刻・デジタコの生行) の読み出しを
//! Workers VPC (TCP) → 既存 Tunnel → MariaDB で行う。プロトコルは自作の kintai-mysql (平文・mysql_native_password)。
//! Supabase (勤怠スキーマ `kintai.*`) は Hyperdrive (`KINTAI_HYPERDRIVE`) + alc-worker-db で読む。
//!
//! 口:
//! - 到達の確認用の `POST /probe` (接続 → handshake → 認証 → `SET SESSION max_statement_time=60` →
//!   `SELECT 1, VERSION(), @@character_set_connection, CURRENT_USER()` → `COM_QUIT`)。1 リクエスト 1 接続。
//! - Supabase を読むだけの `GET /api/kintai/{day-summaries,shift-overlaps,shift-days,change-log,wage-range}`
//!   ([`reads`]。Cloud Run 版と同じ応答・同じ 400/502/503。テナントは `KINTAI_TENANT_ID` の設定 pin)。
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
use worker::{console_error, console_log, event, Context, Date, Delay, Env, Request, Response};

use conn::Session;
use probe::{
    log_line, parse_creds, reply_for, reply_for_route, route, Creds, Failure, ProbeOk, Read, Reply,
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

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    let route = route(req.method().as_ref(), &req.path());
    if let Route::Read(read) = route {
        let url = req.url()?;
        return run_read(read, url.query().unwrap_or(""), &env).await;
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

/// Supabase を読む口。成功は JSON (200)、失敗は平文 (元の axum の `(StatusCode, String)` と同じ text/plain)。
async fn run_read(read: Read, query: &str, env: &Env) -> worker::Result<Response> {
    let started = Date::now().as_millis();
    let outcome = reads::serve(read, query, env).await;
    let ms = Date::now().as_millis().saturating_sub(started);
    let name = read.as_str();
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

async fn probe(env: &Env, started: u64) -> Result<ProbeOk, Failure> {
    let creds = load_creds(env)
        .await
        .ok_or(Failure::new(Stage::Secret, "missing"))?;
    let socket = step(Stage::Connect, CONNECT_TIMEOUT, async {
        transport::open(env)
            .await
            .map_err(|_| "transport".to_string())
    })
    .await?;
    let mut session = Session::new(socket);
    let result = talk(&mut session, &creds).await;
    session.quit().await;
    let set = result?;

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

/// handshake → 認証 → 打ち切り時間の設定 → probe のクエリ。
async fn talk(
    session: &mut Session,
    creds: &Creds,
) -> Result<kintai_mysql::response::ResultSet, Failure> {
    let (seq, hs) = step(Stage::Handshake, LOGIN_TIMEOUT, session.handshake()).await?;
    step(
        Stage::Auth,
        LOGIN_TIMEOUT,
        session.authenticate(seq, &hs, creds),
    )
    .await?;
    step(Stage::Query, QUERY_TIMEOUT, async {
        session.query(SET_STATEMENT_TIME).await?;
        session.query(PROBE_SQL).await
    })
    .await
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
