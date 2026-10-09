//! 一番星 SQL Server への TCP を開き、tiberius に渡せる形 (futures の AsyncRead/Write) で返す。
//!
//! ohishi-exp/smb-watch の `workers/smb-ingest/worker/src/transport.rs` の socket の取り方を写している:
//! 本番は Workers VPC の binding (`ICHIBAN_VPC`) の `connect()`、ローカル検証は var `LOCAL_SQL_ADDR`
//! があるときだけ `Socket::builder().connect` で直接繋ぐ。

use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};
use worker::{Env, Socket};

use crate::tcp::TcpPort;
use crate::text;

/// VPC Service 型は宛先の host:port を Service 側で固定するので、`connect()` に渡すアドレスは名目の値
/// (この文字列は使われない。社内のアドレスはコードに書かない)。
const VPC_NOMINAL_ADDR: &str = "ichiban-sql:1433";

/// SQL Server への socket を開く。拒否された接続は最初の write ではなくここで表に出す。
/// エラーの文言は呼び出し側で捨てる (宛先を応答にもログにも出さない)。
pub(crate) async fn open(env: &Env) -> worker::Result<Compat<Socket>> {
    let socket = match text(env, "LOCAL_SQL_ADDR") {
        // ローカル検証 (wrangler dev) だけ。本番の vars には置かない (scripts/check-exposure.sh が検査する)
        Some(addr) => {
            let (host, port) = split_host_port(&addr)
                .ok_or_else(|| worker::Error::from("LOCAL_SQL_ADDR is not host:port"))?;
            Socket::builder().connect(host, port)?
        }
        None => {
            let vpc: TcpPort = env.get_binding("ICHIBAN_VPC")?;
            Socket::from(
                vpc.connect(VPC_NOMINAL_ADDR)
                    .map_err(|e| worker::Error::from(format!("vpc connect: {e:?}")))?,
            )
        }
    };
    socket.opened().await?;
    Ok(socket.compat())
}

fn split_host_port(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Some((host.to_string(), port))
}
