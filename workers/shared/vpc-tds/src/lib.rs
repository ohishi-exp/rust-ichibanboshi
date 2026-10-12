//! Workers VPC の binding から SQL Server への TCP を開き、tiberius でログインする (1 リクエスト = 1 接続)。
//! workers/ichiban と workers/kyuyo に写されていた transport・tcp・connect を 1 本にした (Refs ohishi-exp/rust-ichibanboshi#322)。
//! binding 名・名目アドレス・database 名・secret 名・ローカル検証の var 名は呼び手が [`Target`] で渡す。
//! bb8 は wasm32 で使えないので pool は持たない。
//! 失敗は `DbError` (stage と種類だけ) で返し、エラーの本文・ホスト・ユーザー名はどこにも出さない。
//! workers/kintai の `tcp.rs` も同じ写しだが、相手が MariaDB で tiberius を引き込みたくないのでここへは畳まない。

mod tcp;

use std::future::Future;
use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use tiberius::error::Error as TdsError;
use tiberius::{AuthMethod, Client, Config, EncryptionLevel};
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use worker::{Delay, Env, Socket};

use tcp::TcpPort;
pub use vpc_tds_logic::{parse_creds, Creds, DbError, ErrKind, Stage};

/// TCP を開いてからログインが終わるまでの上限。応答しない相手で fetch を握り続けない。
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(20);

pub type Conn = Client<Compat<Socket>>;

/// 繋ぎ先。どれも呼び手の wrangler.toml / Secrets Store に合わせる。
pub struct Target {
    /// Workers VPC の binding (`[[vpc_services]]`)
    pub binding: &'static str,
    /// VPC Service 型は宛先の host:port を Service 側で固定するので、`connect()` に渡すアドレスは名目の値
    /// (この文字列は使われない。社内のアドレスはコードに書かない)
    pub nominal_addr: &'static str,
    /// ローカル検証 (wrangler dev) で VPC を迂回して直接繋ぐ `host:port` の var。本番の vars には置かない
    pub local_addr_var: &'static str,
    /// ログイン後の database
    pub database: &'static str,
    /// 資格情報 JSON `{"user":…,"pass":…}` の Secrets Store binding
    pub secret: &'static str,
    /// ローカル検証で Secrets Store の代わりに資格情報 JSON を渡す var。本番の vars には置かない
    pub local_creds_var: &'static str,
}

/// tiberius のエラーを失敗の種類に写す。`message` や表示文言は読まない (種類・番号・状態だけ)。
pub fn kind_of(e: &TdsError) -> ErrKind {
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

/// 資格情報を読み、TCP を開いてログインする (平文 TDS)。
pub async fn connect(env: &Env, target: &Target) -> Result<Conn, DbError> {
    let creds = load_creds(env, target)
        .await
        .map_err(|stage| fail(stage, ErrKind::Other))?;

    let mut config = Config::new();
    config.authentication(AuthMethod::sql_server(&creds.user, &creds.pass));
    // 社内 LAN 区間の平文 TDS (SQL Server 2008 R2 は新しい TLS を話せない。オンプレ版と同じ)
    config.encryption(EncryptionLevel::NotSupported);
    config.database(target.database);
    // port / instance_name は呼ばない: ポートは VPC Service 側で固定 (SQL Browser の UDP は Worker から出せない)

    timeout(CONNECT_TIMEOUT, async {
        let stream = open(env, target)
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
async fn load_creds(env: &Env, target: &Target) -> Result<Creds, Stage> {
    // ローカル検証 (wrangler dev) だけ: Secrets Store が使えないので var で代える (各 Worker の check-exposure.sh が検査する)
    let json = match text(env, target.local_creds_var) {
        Some(json) => json,
        None => env
            .secret_store(target.secret)
            .map_err(|_| Stage::Secret)?
            .get()
            .await
            .map_err(|_| Stage::Secret)?
            .ok_or(Stage::Secret)?,
    };
    parse_creds(&json)
}

/// SQL Server への socket を開き、tiberius に渡せる形 (futures の AsyncRead/Write) で返す。
/// 拒否された接続は最初の write ではなくここで表に出す。エラーの文言は呼び出し側で捨てる。
async fn open(env: &Env, target: &Target) -> worker::Result<Compat<Socket>> {
    let socket = match text(env, target.local_addr_var) {
        Some(addr) => {
            let (host, port) = vpc_tds_logic::split_host_port(&addr)
                .ok_or_else(|| worker::Error::from("local addr is not host:port"))?;
            Socket::builder().connect(host, port)?
        }
        None => {
            let vpc: TcpPort = env.get_binding(target.binding)?;
            Socket::from(
                vpc.connect(target.nominal_addr)
                    .map_err(|e| worker::Error::from(format!("vpc connect: {e:?}")))?,
            )
        }
    };
    socket.opened().await?;
    Ok(socket.compat())
}

/// `fut` を `limit` で打ち切る。時間切れは `None`。
pub async fn timeout<T>(limit: Duration, fut: impl Future<Output = T>) -> Option<T> {
    match select(pin!(fut), pin!(Delay::from(limit))).await {
        Either::Left((v, _)) => Some(v),
        Either::Right(_) => None,
    }
}

/// secret / var の文字列 (空は無いものとして扱う)。`.dev.vars` はローカルでは var として見える。
pub fn text(env: &Env, name: &str) -> Option<String> {
    env.secret(name)
        .map(|s| s.to_string())
        .or_else(|_| env.var(name).map(|v| v.to_string()))
        .ok()
        .filter(|s| !s.is_empty())
}
