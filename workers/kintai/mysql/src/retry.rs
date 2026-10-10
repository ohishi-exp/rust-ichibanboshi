//! 接続のやり直しの判断 (純粋関数)。
//!
//! 間を空けずに再接続を続けると、handshake を読む前に TCP が閉じられることがある (`handshake:closed`、
//! 20 回連続で 2〜3 回。3 秒空ければ起きない)。**認証パケットを送る前 (connect・handshake の段) の失敗だけ**、
//! 新しい接続で [`MAX_RETRIES`] 回までやり直す。認証以降・クエリ以降の失敗はやり直さない (読み取りだけでも段の線を守る)。

/// 1 本の接続のどの段で失敗したか。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// TCP を開く
    Connect,
    /// サーバーの Initial Handshake を読む (こちらからはまだ何も送っていない)
    Handshake,
    /// HandshakeResponse41 を送って認証する
    Auth,
    /// クエリ
    Query,
}

/// やり直す回数の上限 (最初の 1 回を除く)。
pub const MAX_RETRIES: u32 = 2;

/// やり直す前に空ける時間 (ミリ秒)。
pub const RETRY_DELAY_MS: u64 = 200;

/// `failed_at` で失敗し、既に `retries_done` 回やり直しているとき、もう 1 回やり直すか。
pub fn should_retry(failed_at: Phase, retries_done: u32) -> bool {
    let before_auth = matches!(failed_at, Phase::Connect | Phase::Handshake);
    before_auth && retries_done < MAX_RETRIES
}
