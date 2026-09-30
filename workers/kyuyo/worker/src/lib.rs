//! 給与大臣 Worker の PoC (Refs ohishi-exp/rust-ichibanboshi#322): wasm32 の Worker から tiberius で
//! 給与大臣の SQL Server に TDS を張れることを確かめる `POST /probe` だけを持つ。
//!
//! 到達面: fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。同一アカウントで
//! binding を宣言した worker は誰でも叩けるが、効果は給与大臣 SQL Server への 1 回のログインと
//! `SELECT 1` だけで、データは返さない。認可は後続の実 API で auth-worker 経由に入れる。
//!
//! 1 回の流れ: 資格情報 (Secrets Store `KYUYO_SQL`) を読む → `KYUYO_VPC` から TCP を開く →
//! tiberius でログイン (社内 LAN 区間は平文 TDS、`EncryptionLevel::NotSupported`) → `SELECT 1`。
//! 成功は 200 `{"ok":true}`、失敗は 502 `{"ok":false,"stage":"secret|connect|login|query"}`。
//! 応答にもログにもエラーの生文言・ホスト・ポート・ユーザー名を出さない (ログは stage と所要ミリ秒だけ)。

mod tcp;
mod transport;

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use serde::Deserialize;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use worker::{
    console_error, console_log, event, Context, Date, Delay, Env, Method, Request, Response,
};

/// TCP を開いてからログインが終わるまでの上限。応答しない相手で fetch を握り続けない。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// `SELECT 1` の上限。
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// どこで失敗したか。応答とログに出すのはこの名前だけ。
#[derive(Clone, Copy)]
enum Stage {
    Secret,
    Connect,
    Login,
    Query,
}

impl Stage {
    fn as_str(self) -> &'static str {
        match self {
            Stage::Secret => "secret",
            Stage::Connect => "connect",
            Stage::Login => "login",
            Stage::Query => "query",
        }
    }
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    if req.path() != "/probe" {
        return reply(404, r#"{"ok":false}"#);
    }
    if req.method() != Method::Post {
        return reply(405, r#"{"ok":false}"#);
    }
    let started = Date::now().as_millis();
    let result = probe(&env).await;
    let ms = Date::now().as_millis().saturating_sub(started);
    match result {
        Ok(()) => {
            console_log!("kyuyo probe: ok ({ms} ms)");
            reply(200, r#"{"ok":true}"#)
        }
        Err(stage) => {
            let stage = stage.as_str();
            console_error!("kyuyo probe: failed at {stage} ({ms} ms)");
            reply(502, &format!(r#"{{"ok":false,"stage":"{stage}"}}"#))
        }
    }
}

fn reply(status: u16, body: &str) -> worker::Result<Response> {
    let headers = worker::Headers::new();
    headers.set("content-type", "application/json")?;
    Ok(Response::ok(body)?
        .with_status(status)
        .with_headers(headers))
}

/// ログインして `SELECT 1` を 1 本流す。エラーの中身は捨てて stage だけ返す。
async fn probe(env: &Env) -> Result<(), Stage> {
    let creds = load_creds(env).await.ok_or(Stage::Secret)?;

    let mut config = Config::new();
    config.authentication(AuthMethod::sql_server(&creds.user, &creds.pass));
    config.encryption(EncryptionLevel::NotSupported);
    config.database("master");

    let mut client = timeout(CONNECT_TIMEOUT, async {
        let stream = transport::open(env).await.map_err(|_| Stage::Connect)?;
        Client::connect(config, stream)
            .await
            .map_err(|_| Stage::Login)
    })
    .await
    .ok_or(Stage::Connect)??;

    let one = timeout(QUERY_TIMEOUT, async {
        let row = client
            .simple_query("SELECT 1")
            .await
            .ok()?
            .into_row()
            .await
            .ok()??;
        row.get::<i32, _>(0)
    })
    .await
    .flatten();
    let _ = client.close().await;
    match one {
        Some(1) => Ok(()),
        _ => Err(Stage::Query),
    }
}

/// Secrets Store `KYUYO_SQL` の JSON。両キー必須の非空文字列。
#[derive(Deserialize)]
struct Creds {
    user: String,
    pass: String,
}

/// 資格情報を読む。JSON が読めない・キー欠け・空は `None` (中身はどこにも出さない)。
async fn load_creds(env: &Env) -> Option<Creds> {
    // ローカル検証 (wrangler dev) だけ: Secrets Store が使えないので var LOCAL_KYUYO_SQL_JSON で代える。
    // 本番の vars には置かない (scripts/check-exposure.sh が検査する)
    let json = match text(env, "LOCAL_KYUYO_SQL_JSON") {
        Some(json) => json,
        None => env.secret_store("KYUYO_SQL").ok()?.get().await.ok()??,
    };
    let creds: Creds = serde_json::from_str(&json).ok()?;
    (!creds.user.is_empty() && !creds.pass.is_empty()).then_some(creds)
}

/// secret / var の文字列 (空は無いものとして扱う)。`.dev.vars` はローカルでは var として見える。
pub(crate) fn text(env: &Env, name: &str) -> Option<String> {
    env.secret(name)
        .map(|s| s.to_string())
        .or_else(|_| env.var(name).map(|v| v.to_string()))
        .ok()
        .filter(|s| !s.is_empty())
}

/// `fut` を `limit` で打ち切る。時間切れは `None`。
async fn timeout<T>(limit: Duration, fut: impl Future<Output = T>) -> Option<T> {
    match select(pin!(fut), pin!(Delay::from(limit))).await {
        Either::Left((v, _)) => Some(v),
        Either::Right(_) => None,
    }
}
