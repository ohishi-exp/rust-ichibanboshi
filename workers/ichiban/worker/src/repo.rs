//! 一番星 SQL Server への接続 (資格情報・TCP・ログイン)。workers/kyuyo の `worker/src/repo.rs` の `connect` 周りを写している。
//! 1 リクエスト = 1 接続。bb8 は wasm32 で使えないので pool は持たない。
//! 失敗は `DbError` (stage と種類だけ) で返し、エラーの本文・ホスト・ユーザー名はどこにも出さない。

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use tiberius::error::Error as TdsError;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio_util::compat::Compat;
use worker::{Delay, Env, Socket};

use crate::probe_logic::{parse_creds, Creds, DbError, ErrKind, Stage};
use crate::{text, transport};

/// TCP を開いてからログインが終わるまでの上限。応答しない相手で fetch を握り続けない。
pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

pub(crate) type Conn = Client<Compat<Socket>>;

/// tiberius のエラーを失敗の種類に写す。`message` や表示文言は読まない (種類・番号・状態だけ)。
pub(crate) fn kind_of(e: &TdsError) -> ErrKind {
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

fn fail(stage: Stage, kind: ErrKind) -> DbError {
    DbError { stage, kind }
}

/// 資格情報を読み、TCP を開いてログインする (database `CAPE#01`、平文 TDS)。
pub(crate) async fn connect(env: &Env) -> Result<Conn, DbError> {
    let creds = load_creds(env)
        .await
        .map_err(|stage| fail(stage, ErrKind::Other))?;

    let mut config = Config::new();
    config.authentication(AuthMethod::sql_server(&creds.user, &creds.pass));
    // 社内 LAN 区間の平文 TDS (オンプレ版と同じ)
    config.encryption(EncryptionLevel::NotSupported);
    config.database("CAPE#01");
    // port / instance_name は呼ばない: ポートは VPC Service 側で固定 (SQL Browser の UDP は Worker から出せない)

    timeout(CONNECT_TIMEOUT, async {
        let stream = transport::open(env)
            .await
            .map_err(|_| fail(Stage::Connect, ErrKind::Transport))?;
        Client::connect(config, stream)
            .await
            .map_err(|e| fail(Stage::Login, kind_of(&e)))
    })
    .await
    .ok_or(fail(Stage::Connect, ErrKind::Timeout))?
}

/// 資格情報を読んで検証する。読めない・キー欠け・空は `Stage::Secret` (中身はどこにも出さない)。
async fn load_creds(env: &Env) -> Result<Creds, Stage> {
    // ローカル検証 (wrangler dev) だけ: Secrets Store が使えないので var LOCAL_ICHIBAN_SQL_JSON で代える。
    // 本番の vars には置かない (scripts/check-exposure.sh が検査する)
    let json = match text(env, "LOCAL_ICHIBAN_SQL_JSON") {
        Some(json) => json,
        None => env
            .secret_store("ICHIBAN_SQL")
            .map_err(|_| Stage::Secret)?
            .get()
            .await
            .map_err(|_| Stage::Secret)?
            .ok_or(Stage::Secret)?,
    };
    parse_creds(&json)
}

/// `fut` を `limit` で打ち切る。時間切れは `None`。
pub(crate) async fn timeout<T>(limit: Duration, fut: impl Future<Output = T>) -> Option<T> {
    match select(pin!(fut), pin!(Delay::from(limit))).await {
        Either::Left((v, _)) => Some(v),
        Either::Right(_) => None,
    }
}
