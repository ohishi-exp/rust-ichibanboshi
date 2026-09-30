//! `POST /probe`: 給与大臣 SQL Server にログインして `SELECT 1` を 1 本流す。DO `KyuyoState` のロックの中からだけ呼ぶ。
//!
//! 1 回の流れ: 資格情報 (Secrets Store `KYUYO_SQL`) を読む → `KYUYO_VPC` から TCP を開く →
//! tiberius でログイン (社内 LAN 区間は平文 TDS、`EncryptionLevel::NotSupported`) → `SELECT 1`。
//! 成功は 200 `{"ok":true}`、失敗は 502 `{"ok":false,"stage":"secret|connect|login|query"}`。
//! 応答にもログにもエラーの生文言・ホスト・ポート・ユーザー名を出さない (ログは stage・失敗の種類 `ErrKind`・所要ミリ秒だけ)。

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use kyuyo_logic::{log_line, parse_creds, reply_for_probe, Creds, ErrKind, Reply, Stage};
use tiberius::error::Error as TdsError;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use worker::{console_error, console_log, Date, Delay, Env};

use crate::{text, transport};

/// TCP を開いてからログインが終わるまでの上限。応答しない相手で fetch を握り続けない。
const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);
/// `SELECT 1` の上限。
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// probe を 1 回走らせ、結果をログに 1 行出して応答を返す。
pub(crate) async fn run(env: &Env) -> Reply {
    let started = Date::now().as_millis();
    let outcome = probe(env).await;
    let ms = Date::now().as_millis().saturating_sub(started);
    match outcome {
        Ok(()) => console_log!("kyuyo probe: ok ({ms} ms)"),
        Err((stage, ref kind)) => console_error!("{}", log_line(stage, kind, ms)),
    }
    reply_for_probe(outcome.map_err(|(stage, _)| stage))
}

/// tiberius のエラーを失敗の種類に写す。`message` や表示文言は読まない (種類・番号・状態だけ)。
fn kind_of(e: &TdsError) -> ErrKind {
    match e {
        TdsError::Io { kind, .. } => ErrKind::Io(format!("{kind:?}")),
        TdsError::Server(t) => ErrKind::Server {
            code: t.code(),
            class: t.class(),
            state: t.state(),
        },
        TdsError::Protocol(_) => ErrKind::Protocol,
        TdsError::Encoding(_) => ErrKind::Encoding,
        TdsError::Tls(_) => ErrKind::Tls,
        TdsError::Routing { .. } => ErrKind::Routing,
        _ => ErrKind::Other,
    }
}

/// ログインして `SELECT 1` を 1 本流す。エラーの中身は捨てて stage と失敗の種類だけ返す。
async fn probe(env: &Env) -> Result<(), (Stage, ErrKind)> {
    let creds = load_creds(env)
        .await
        .map_err(|stage| (stage, ErrKind::Other))?;

    let mut config = Config::new();
    config.authentication(AuthMethod::sql_server(&creds.user, &creds.pass));
    config.encryption(EncryptionLevel::NotSupported);
    config.database("master");

    let mut client = timeout(CONNECT_TIMEOUT, async {
        let stream = transport::open(env)
            .await
            .map_err(|_| (Stage::Connect, ErrKind::Transport))?;
        Client::connect(config, stream)
            .await
            .map_err(|e| (Stage::Login, kind_of(&e)))
    })
    .await
    .ok_or((Stage::Connect, ErrKind::Timeout))??;

    let one: Option<Result<Option<i32>, ErrKind>> = timeout(QUERY_TIMEOUT, async {
        let row = client
            .simple_query("SELECT 1")
            .await
            .map_err(|e| kind_of(&e))?
            .into_row()
            .await
            .map_err(|e| kind_of(&e))?;
        Ok(row.and_then(|r| r.get::<i32, _>(0)))
    })
    .await;
    let _ = client.close().await;
    match one {
        None => Err((Stage::Query, ErrKind::Timeout)),
        Some(Err(kind)) => Err((Stage::Query, kind)),
        Some(Ok(Some(1))) => Ok(()),
        Some(Ok(_)) => Err((Stage::Query, ErrKind::Other)),
    }
}

/// 資格情報を読んで検証する (`kyuyo_logic::parse_creds`)。読めない・キー欠け・空は `Stage::Secret` (中身はどこにも出さない)。
async fn load_creds(env: &Env) -> Result<Creds, Stage> {
    // ローカル検証 (wrangler dev) だけ: Secrets Store が使えないので var LOCAL_KYUYO_SQL_JSON で代える。
    // 本番の vars には置かない (scripts/check-exposure.sh が検査する)
    let json = match text(env, "LOCAL_KYUYO_SQL_JSON") {
        Some(json) => json,
        None => env
            .secret_store("KYUYO_SQL")
            .map_err(|_| Stage::Secret)?
            .get()
            .await
            .map_err(|_| Stage::Secret)?
            .ok_or(Stage::Secret)?,
    };
    parse_creds(&json)
}

/// `fut` を `limit` で打ち切る。時間切れは `None`。
async fn timeout<T>(limit: Duration, fut: impl Future<Output = T>) -> Option<T> {
    match select(pin!(fut), pin!(Delay::from(limit))).await {
        Either::Left((v, _)) => Some(v),
        Either::Right(_) => None,
    }
}
