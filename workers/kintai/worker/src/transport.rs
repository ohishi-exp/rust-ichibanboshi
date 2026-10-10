//! 社内 MariaDB への TCP を開く。
//!
//! workers/ichiban の `worker/src/transport.rs` の socket の取り方を写している: Workers VPC の binding
//! (`KINTAI_MARIADB_VPC`) の `connect()`。ichiban と違い、ローカルで VPC を迂回する var は持たない
//! (scripts/check-exposure.sh が `LOCAL_` で始まる var を落とす)。tiberius 向けの compat の包みも要らない。

use worker::{Env, Socket};

use crate::tcp::TcpPort;

/// VPC Service 型は宛先の host:port を Service 側で固定するので、`connect()` に渡すアドレスは名目の値
/// (この文字列は使われない。社内のアドレスはコードに書かない)。
const VPC_NOMINAL_ADDR: &str = "kintai-mariadb:3306";

/// MariaDB への socket を開く。拒否された接続は最初の read ではなくここで表に出す。
/// エラーの文言は呼び出し側で捨てる (宛先を応答にもログにも出さない)。
pub(crate) async fn open(env: &Env) -> worker::Result<Socket> {
    let vpc: TcpPort = env.get_binding("KINTAI_MARIADB_VPC")?;
    let socket = Socket::from(
        vpc.connect(VPC_NOMINAL_ADDR)
            .map_err(|e| worker::Error::from(format!("vpc connect: {e:?}")))?,
    );
    socket.opened().await?;
    Ok(socket)
}
