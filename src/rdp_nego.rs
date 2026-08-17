//! RDP の接続開始 (X.224 Connection Request / Confirm, MS-RDPBCGR 2.2.1.1〜2.2.1.2) を
//! バイト列として組み立て・解釈する。**ソケットには触らない。**
//!
//! ここを純粋に保つのは、中継本体 (`bin/rdp_relay.rs`) が I/O だけを持つようにするため。
//! プロトコルの解釈はこのモジュールで完結させ、固定バイト列に対する単体テストで検証する。
//! テストで使っているバイト列は実機 (ohishi-srv 172.18.21.102:3389) から採取したもの。

use core::fmt;

use ironrdp_core::{decode, encode_vec};
use ironrdp_pdu::nego::{
    ConnectionConfirm, ConnectionRequest, FailureCode, RequestFlags, SecurityProtocol,
};
use ironrdp_pdu::x224::X224;

/// TPKT ヘッダ (RFC1006) の長さ。version(1) + reserved(1) + length(2)。
pub const TPKT_HEADER_LEN: usize = 4;

/// TPKT のバージョン。RDP は常に 3。
const TPKT_VERSION: u8 = 3;

#[derive(Debug)]
pub enum RdpNegoError {
    /// 接続要求の組み立てに失敗した。
    Encode(String),
    /// 接続確認の解釈に失敗した。
    Decode(String),
    /// フレームが宣言された長さに足りない。
    ShortFrame { got: usize, need: usize },
    /// TPKT のバージョンが 3 でない = RDP ではない相手。
    BadTpktVersion(u8),
    /// サーバーが接続を拒否した。
    Rejected(FailureCode),
}

impl fmt::Display for RdpNegoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Encode(e) => write!(f, "接続要求を組み立てられない: {e}"),
            Self::Decode(e) => write!(f, "接続確認を解釈できない: {e}"),
            Self::ShortFrame { got, need } => {
                write!(
                    f,
                    "フレームが短い: {got} バイトしか無いが {need} バイト必要"
                )
            }
            Self::BadTpktVersion(v) => write!(f, "TPKT のバージョンが {v} (RDP なら 3)"),
            Self::Rejected(code) => write!(f, "サーバーが接続を拒否した: {code:?}"),
        }
    }
}

impl std::error::Error for RdpNegoError {}

/// TPKT ヘッダからフレーム全体の長さ (ヘッダを含む) を取り出す。
///
/// 呼び出し側はまず [`TPKT_HEADER_LEN`] バイトだけ読み、この関数で残りの長さを知る。
pub fn tpkt_frame_len(header: &[u8]) -> Result<usize, RdpNegoError> {
    if header.len() < TPKT_HEADER_LEN {
        return Err(RdpNegoError::ShortFrame {
            got: header.len(),
            need: TPKT_HEADER_LEN,
        });
    }
    if header[0] != TPKT_VERSION {
        return Err(RdpNegoError::BadTpktVersion(header[0]));
    }
    Ok(usize::from(u16::from_be_bytes([header[2], header[3]])))
}

/// 接続要求を組み立てる。
///
/// `protocol` に何を要求するかで、この後サーバーが選ぶセキュリティ層が決まる。
/// 一番星の RDS は NLA (`HYBRID`) を選ぶ。
pub fn build_connection_request(protocol: SecurityProtocol) -> Result<Vec<u8>, RdpNegoError> {
    let request = ConnectionRequest {
        nego_data: None,
        flags: RequestFlags::empty(),
        protocol,
    };

    encode_vec(&X224(request)).map_err(|e| RdpNegoError::Encode(e.to_string()))
}

/// 接続確認を解釈し、サーバーが選んだセキュリティプロトコルを返す。
///
/// `frame` は TPKT ヘッダを含むフレーム全体を渡す。
pub fn parse_connection_confirm(frame: &[u8]) -> Result<SecurityProtocol, RdpNegoError> {
    let declared = tpkt_frame_len(frame)?;
    if frame.len() < declared {
        return Err(RdpNegoError::ShortFrame {
            got: frame.len(),
            need: declared,
        });
    }

    let confirm = decode::<X224<ConnectionConfirm>>(frame)
        .map_err(|e| RdpNegoError::Decode(e.to_string()))?;

    match confirm.0 {
        ConnectionConfirm::Response { protocol, .. } => Ok(protocol),
        ConnectionConfirm::Failure { code } => Err(RdpNegoError::Rejected(code)),
    }
}

/// 選ばれたプロトコルが TLS の上で喋るものかどうか。
///
/// `HYBRID` (NLA) も `SSL` も TLS を張る。標準 RDP セキュリティだけが張らない。
pub fn needs_tls(protocol: SecurityProtocol) -> bool {
    protocol
        .intersects(SecurityProtocol::SSL | SecurityProtocol::HYBRID | SecurityProtocol::HYBRID_EX)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 実機の ohishi-srv が返した接続確認。選択プロトコルは HYBRID (NLA)。
    const REAL_CONFIRM: &[u8] = &[
        0x03, 0x00, 0x00, 0x13, 0x0e, 0xd0, 0x00, 0x00, 0x12, 0x34, 0x00, 0x02, 0x1f, 0x08, 0x00,
        0x02, 0x00, 0x00, 0x00,
    ];

    #[test]
    fn tpkt_frame_len_reads_declared_length() {
        assert_eq!(tpkt_frame_len(REAL_CONFIRM).unwrap(), 0x13);
    }

    #[test]
    fn tpkt_frame_len_rejects_short_header() {
        let err = tpkt_frame_len(&[0x03, 0x00]).unwrap_err();
        assert!(matches!(err, RdpNegoError::ShortFrame { got: 2, need: 4 }));
        assert!(err.to_string().contains("フレームが短い"));
    }

    #[test]
    fn tpkt_frame_len_rejects_non_rdp_peer() {
        let err = tpkt_frame_len(&[0x16, 0x03, 0x01, 0x00]).unwrap_err();
        assert!(matches!(err, RdpNegoError::BadTpktVersion(0x16)));
        assert!(err.to_string().contains("TPKT"));
    }

    #[test]
    fn connection_request_asks_for_what_we_pass() {
        let protocol = SecurityProtocol::SSL | SecurityProtocol::HYBRID;
        let bytes = build_connection_request(protocol).unwrap();

        // TPKT ヘッダが自分で宣言した長さと実際の長さは一致していなければならない。
        assert_eq!(bytes[0], TPKT_VERSION);
        assert_eq!(tpkt_frame_len(&bytes).unwrap(), bytes.len());

        // 要求プロトコルは RDP_NEG_REQ の末尾 4 バイト (little endian)。
        let requested = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().unwrap());
        assert_eq!(requested, protocol.bits());
    }

    #[test]
    fn parse_connection_confirm_reads_selected_protocol() {
        let protocol = parse_connection_confirm(REAL_CONFIRM).unwrap();
        assert_eq!(protocol, SecurityProtocol::HYBRID);
        assert!(needs_tls(protocol));
    }

    #[test]
    fn parse_connection_confirm_rejects_truncated_frame() {
        let err = parse_connection_confirm(&REAL_CONFIRM[..10]).unwrap_err();
        assert!(matches!(
            err,
            RdpNegoError::ShortFrame {
                got: 10,
                need: 0x13
            }
        ));
    }

    #[test]
    fn parse_connection_confirm_rejects_garbage() {
        // TPKT としては辻褄が合うが中身が X.224 ではないフレーム。
        let frame = [0x03, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff, 0xff];
        let err = parse_connection_confirm(&frame).unwrap_err();
        assert!(matches!(err, RdpNegoError::Decode(_)));
        assert!(err.to_string().contains("接続確認"));
    }

    #[test]
    fn standard_rdp_security_does_not_need_tls() {
        assert!(!needs_tls(SecurityProtocol::empty()));
        assert!(needs_tls(SecurityProtocol::SSL));
        assert!(needs_tls(SecurityProtocol::HYBRID_EX));
    }
}
