//! 接続の最初のやり取り: サーバーの Initial Handshake (protocol v10) を読み、HandshakeResponse41 を返し、
//! mysql_native_password で認証する。Auth Switch Request は mysql_native_password のときだけ応じる。

use sha1::{Digest, Sha1};

use crate::packet::{utf8, Reader};
use crate::response::{ErrPacket, OkPacket};
use crate::Error;

pub const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
pub const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
pub const CLIENT_SSL: u32 = 0x0000_0800;
pub const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
pub const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
pub const CLIENT_DEPRECATE_EOF: u32 = 0x0100_0000;

/// このクライアントが立てる capability。CLIENT_SSL と CLIENT_DEPRECATE_EOF は立てない。
pub const CLIENT_FLAGS: u32 =
    CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH | CLIENT_CONNECT_WITH_DB;

/// 送る最大パケット長の申告 (16MB)。
pub const MAX_PACKET_SIZE: u32 = 0x0100_0000;

/// utf8mb4_general_ci。
pub const CHARSET_UTF8MB4: u8 = 45;

/// 応じる唯一の認証プラグイン。
pub const NATIVE_PASSWORD: &str = "mysql_native_password";

/// サーバーの Initial Handshake (protocol v10)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitialHandshake {
    pub server_version: String,
    pub connection_id: u32,
    pub capabilities: u32,
    pub charset: u8,
    pub status: u16,
    /// auth-plugin-data の part1 (8 byte) + part2 (末尾の NUL を除く)。mysql_native_password では 20 byte の nonce
    pub auth_plugin_data: Vec<u8>,
    pub auth_plugin_name: String,
}

/// Initial Handshake を読む。接続を断るサーバーは handshake の代わりに ERR を送るので、それは `Error::Server`。
/// CLIENT_FLAGS のうちサーバーが持たないものがあれば `Error::Capability` (欠けたビット)。
pub fn parse_initial_handshake(payload: &[u8]) -> Result<InitialHandshake, Error> {
    if payload.first() == Some(&0xFF) {
        return Err(Error::Server(ErrPacket::parse(payload)?));
    }
    let mut r = Reader::new(payload);
    let protocol = r.u8()?;
    if protocol != 10 {
        return Err(Error::ProtocolVersion(protocol));
    }
    let server_version = utf8(r.null_str()?)?;
    let connection_id = r.u32()?;
    let mut auth_plugin_data = r.bytes(8)?.to_vec();
    r.u8()?; // filler
    let cap_lower = r.u16()?;
    let charset = r.u8()?;
    let status = r.u16()?;
    let cap_upper = r.u16()?;
    let capabilities = u32::from(cap_lower) | (u32::from(cap_upper) << 16);
    let missing = CLIENT_FLAGS & !capabilities;
    if missing != 0 {
        return Err(Error::Capability(missing));
    }
    let auth_data_len = usize::from(r.u8()?);
    // reserved 10 byte (MariaDB は後ろ 4 byte に拡張 capability を置くが使わない)
    r.bytes(10)?;
    // CLIENT_SECURE_CONNECTION を持つ (上で確かめた) ので part2 がある。長さは max(13, len - 8) で末尾が NUL
    let part2 = r.bytes(usize::max(13, auth_data_len.saturating_sub(8)))?;
    auth_plugin_data.extend_from_slice(part2.strip_suffix(&[0]).unwrap_or(part2));
    let auth_plugin_name = utf8(r.null_or_eof_str())?;
    Ok(InitialHandshake {
        server_version,
        connection_id,
        capabilities,
        charset,
        status,
        auth_plugin_data,
        auth_plugin_name,
    })
}

/// mysql_native_password の応答: `SHA1(pw) XOR SHA1(nonce + SHA1(SHA1(pw)))`。空のパスワードは空の応答。
/// nonce は先頭 20 byte だけを使う。
pub fn native_password_scramble(password: &[u8], nonce: &[u8]) -> Vec<u8> {
    if password.is_empty() {
        return Vec::new();
    }
    let nonce = &nonce[..nonce.len().min(20)];
    let stage1 = Sha1::digest(password);
    let stage2 = Sha1::digest(stage1);
    let mut hasher = Sha1::new();
    hasher.update(nonce);
    hasher.update(stage2);
    let stage3 = hasher.finalize();
    stage1
        .iter()
        .zip(stage3.iter())
        .map(|(a, b)| a ^ b)
        .collect()
}

/// HandshakeResponse41 の payload。認証プラグインは常に mysql_native_password を名乗る
/// (ユーザーが別のプラグインなら、サーバーが Auth Switch Request を返してくる)。
/// user / database に NUL を含む・応答が 255 byte を超えるものは `Malformed`。
pub fn handshake_response41(
    user: &str,
    auth_response: &[u8],
    database: &str,
) -> Result<Vec<u8>, Error> {
    if user.contains('\0') || database.contains('\0') {
        return Err(Error::Malformed);
    }
    let auth_len = u8::try_from(auth_response.len()).map_err(|_| Error::Malformed)?;
    let mut out = Vec::with_capacity(64 + user.len() + database.len());
    out.extend_from_slice(&CLIENT_FLAGS.to_le_bytes());
    out.extend_from_slice(&MAX_PACKET_SIZE.to_le_bytes());
    out.push(CHARSET_UTF8MB4);
    out.extend_from_slice(&[0; 23]);
    out.extend_from_slice(user.as_bytes());
    out.push(0);
    // CLIENT_SECURE_CONNECTION: 1 byte の長さ + 応答
    out.push(auth_len);
    out.extend_from_slice(auth_response);
    out.extend_from_slice(database.as_bytes());
    out.push(0);
    out.extend_from_slice(NATIVE_PASSWORD.as_bytes());
    out.push(0);
    Ok(out)
}

/// HandshakeResponse41 (と Auth Switch の応答) に対するサーバーの返事。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthReply {
    /// 認証できた
    Ok(OkPacket),
    /// Auth Switch Request: 別のプラグインで (または同じプラグインを新しい nonce で) やり直せ
    Switch { plugin: String, data: Vec<u8> },
}

/// 認証の返事を読む。ERR は `Error::Server`、それ以外 (0x01 の追加データ等) は `Error::Unexpected`。
pub fn parse_auth_reply(payload: &[u8]) -> Result<AuthReply, Error> {
    match payload.first() {
        Some(0x00) => Ok(AuthReply::Ok(OkPacket::parse(payload)?)),
        Some(0xFF) => Err(Error::Server(ErrPacket::parse(payload)?)),
        // 1 byte だけの 0xFE は旧形式 (old password) への切り替えで、応じない
        Some(0xFE) if payload.len() > 1 => {
            let mut r = Reader::new(&payload[1..]);
            let plugin = utf8(r.null_str()?)?;
            let data = r.rest();
            let data = data.strip_suffix(&[0]).unwrap_or(data).to_vec();
            Ok(AuthReply::Switch { plugin, data })
        }
        Some(&b) => Err(Error::Unexpected(b)),
        None => Err(Error::Truncated),
    }
}

/// Auth Switch Request への応答。mysql_native_password 以外は `Error::AuthPlugin`。
pub fn auth_switch_response(plugin: &str, data: &[u8], password: &[u8]) -> Result<Vec<u8>, Error> {
    if plugin != NATIVE_PASSWORD {
        return Err(Error::AuthPlugin(plugin.to_string()));
    }
    Ok(native_password_scramble(password, data))
}
