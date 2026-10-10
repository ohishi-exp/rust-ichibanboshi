//! MySQL / MariaDB のクライアント側プロトコルの最小限 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! I/O を持たない。呼び手 (勤怠 Worker) が socket から読んだバイト列を [`packet::PacketBuf`] に足し、
//! 取り出したパケットの中身をここの関数で読み書きする。
//!
//! - [`packet`]: パケットの枠 (3 byte 長 + 1 byte seq)、length-encoded integer / string
//! - [`handshake`]: Initial Handshake (protocol v10)、HandshakeResponse41、mysql_native_password、Auth Switch
//! - [`response`]: OK / ERR / EOF、COM_QUERY・COM_QUIT、テキストプロトコルの結果セット
//!
//! 立てる capability は CLIENT_PROTOCOL_41・CLIENT_SECURE_CONNECTION・CLIENT_PLUGIN_AUTH・CLIENT_CONNECT_WITH_DB
//! だけ。CLIENT_SSL は立てない (平文で話す)。CLIENT_DEPRECATE_EOF も立てない (結果セットは EOF で区切られる形に固定)。

pub mod handshake;
pub mod packet;
pub mod response;

pub use response::ErrPacket;

/// コーデックの失敗。`kind()` は識別子 (ユーザー名・ホスト・SQL の本文) を含まない種別の名前だけを返す。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// パケットが途中で終わっている
    Truncated,
    /// 値の形が仕様と合わない (NUL の無い文字列・UTF-8 でない等)
    Malformed,
    /// Initial Handshake の protocol version が 10 でない
    ProtocolVersion(u8),
    /// サーバーが必要な capability (CLIENT_PROTOCOL_41 等) を持たない。値は欠けているビット
    Capability(u32),
    /// mysql_native_password 以外の認証プラグインへの切り替えを求められた
    AuthPlugin(String),
    /// その場面では来ないはずのパケット (先頭 1 byte)
    Unexpected(u8),
    /// サーバーが ERR を返した
    Server(ErrPacket),
    /// 1 パケットに収まらない (16MB 以上の送信)
    TooLarge,
}

impl Error {
    /// 応答とログに出す種別の名前。ERR はエラー番号だけ (メッセージ本文は出さない)。
    pub fn kind(&self) -> String {
        match self {
            Error::Truncated => "truncated".to_string(),
            Error::Malformed => "malformed".to_string(),
            Error::ProtocolVersion(_) => "protocol_version".to_string(),
            Error::Capability(_) => "capability".to_string(),
            Error::AuthPlugin(_) => "auth_plugin".to_string(),
            Error::Unexpected(_) => "unexpected_packet".to_string(),
            Error::Server(e) => format!("server:{}", e.code),
            Error::TooLarge => "too_large".to_string(),
        }
    }
}
