//! Workers VPC の binding から MariaDB への TCP を開き、MySQL プロトコルで話す (1 リクエスト = 1 接続)。
//! workers/kintai の conn・transport・tcp を共有 crate にした (Refs ohishi-exp/rust-ichibanboshi#322)。
//! binding 名と名目アドレスは [`open`] の引数で渡す。ローカルで VPC を迂回する var は持たない
//! (workers/kintai の scripts/check-exposure.sh が `LOCAL_` で始まる var を落とす)。

mod session;
mod tcp;

use worker::{Env, Socket};

use tcp::TcpPort;

pub use session::Session;
pub use vpc_mysql_logic::{parse_creds, Creds};

/// MariaDB への socket を開く。拒否された接続は最初の read ではここで表に出す。
/// `binding` は Workers VPC の binding (`[[vpc_services]]`)。VPC Service 型は宛先の host:port を Service 側で
/// 固定するので、`address` は名目の値 (この文字列は使われない。社内のアドレスはコードに書かない)。
/// エラーの文言は呼び出し側で捨てる (宛先を応答にもログにも出さない)。
pub async fn open(env: &Env, binding: &str, address: &str) -> worker::Result<Socket> {
    let vpc: TcpPort = env.get_binding(binding)?;
    let socket = Socket::from(
        vpc.connect(address)
            .map_err(|e| worker::Error::from(format!("vpc connect: {e:?}")))?,
    );
    socket.opened().await?;
    Ok(socket)
}
