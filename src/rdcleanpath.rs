//! RDCleanPath (IronRDP の RDP 拡張) のハンドシェイクを、バイト列として組み立て・解釈する。
//! **ソケットには触らない。**
//!
//! ブラウザは生の TCP を開けず、TLS も持たない。そこで中継が
//! 「TCP を開き、X.224 を代弁し、TLS を張り、サーバー証明書を返す」ところまでを代行する。
//! その往復の型がこれ。ブラウザ側の実装は `ironrdp-web` の `connect_rdcleanpath()`。
//!
//! 往復は 1 回だけで、その後は同じ WebSocket が素の RDP ストリームを運ぶ。

use core::fmt;

use ironrdp_rdcleanpath::{DetectionResult, RDCleanPath, RDCleanPathPdu};

/// 中継がブラウザから受け取った接続要求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayRequest {
    /// 繋ぎ先。`host:port` 形式。
    pub destination: String,
    /// ブラウザが組み立てた X.224 接続要求。中継はこれをそのまま RDS へ流す。
    pub x224_connection_request: Vec<u8>,
    /// 中継自身に対する認証トークン。
    pub proxy_auth: String,
}

#[derive(Debug)]
pub enum RdCleanPathError {
    /// DER として壊れている。
    Malformed(String),
    /// 受け取ったが、中継が扱える種類ではない。
    Unexpected(&'static str),
    /// 応答を組み立てられない。
    Build(String),
}

impl fmt::Display for RdCleanPathError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(e) => write!(f, "RDCleanPath として解釈できない: {e}"),
            Self::Unexpected(what) => write!(f, "中継が扱えない RDCleanPath: {what}"),
            Self::Build(e) => write!(f, "RDCleanPath 応答を組み立てられない: {e}"),
        }
    }
}

impl std::error::Error for RdCleanPathError {}

/// 受信バッファに PDU が 1 つ揃ったか。揃っていれば全体の長さを返す。
///
/// WebSocket のフレーム境界と PDU の境界は一致しないので、呼び出し側は
/// 溜めながらこれを繰り返し呼ぶ。
pub fn detect_pdu(buf: &[u8]) -> Result<Option<usize>, RdCleanPathError> {
    match RDCleanPathPdu::detect(buf) {
        DetectionResult::Detected { total_length, .. } => Ok(Some(total_length)),
        DetectionResult::NotEnoughBytes => Ok(None),
        DetectionResult::Failed => Err(RdCleanPathError::Malformed("detect に失敗".to_owned())),
    }
}

/// 接続要求を解釈する。
pub fn parse_request(der: &[u8]) -> Result<RelayRequest, RdCleanPathError> {
    let pdu =
        RDCleanPathPdu::from_der(der).map_err(|e| RdCleanPathError::Malformed(e.to_string()))?;
    let message = pdu
        .into_enum()
        .map_err(|e| RdCleanPathError::Malformed(e.to_string()))?;

    match message {
        RDCleanPath::Request {
            destination,
            proxy_auth,
            x224_connection_request,
            ..
        } => Ok(RelayRequest {
            destination,
            x224_connection_request: x224_connection_request.as_bytes().to_vec(),
            proxy_auth,
        }),
        // 応答やエラーがブラウザから来ることはない。
        _ => Err(RdCleanPathError::Unexpected("要求ではない")),
    }
}

/// 接続に成功したときの応答を組み立てる。
///
/// `server_cert_chain` は DER の並び。ブラウザはこの先頭から公開鍵を取り出し、
/// NLA (CredSSP) の束縛に使う。**証明書を返さないとブラウザ側は認証に進めない。**
pub fn build_response(
    server_addr: &str,
    x224_connection_response: Vec<u8>,
    server_cert_chain: Vec<Vec<u8>>,
) -> Result<Vec<u8>, RdCleanPathError> {
    RDCleanPathPdu::new_response(
        server_addr.to_owned(),
        x224_connection_response,
        server_cert_chain,
    )
    .and_then(|pdu| pdu.to_der())
    .map_err(|e| RdCleanPathError::Build(e.to_string()))
}

/// RDS が X.224 の段階で接続を蹴ったときの応答を組み立てる。
///
/// ブラウザ側はこの中の接続確認を復号して、失敗理由を利用者に見せる。
pub fn build_negotiation_error(
    x224_connection_response: Vec<u8>,
) -> Result<Vec<u8>, RdCleanPathError> {
    RDCleanPathPdu::new_negotiation_error(x224_connection_response)
        .and_then(|pdu| pdu.to_der())
        .map_err(|e| RdCleanPathError::Build(e.to_string()))
}

/// 中継側の都合で失敗したときの応答を組み立てる。
pub fn build_general_error() -> Result<Vec<u8>, RdCleanPathError> {
    RDCleanPathPdu::new_general_error()
        .to_der()
        .map_err(|e| RdCleanPathError::Build(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ブラウザが送ってくるのと同じ形の要求を作る。
    fn sample_request_der(destination: &str, x224: &[u8]) -> Vec<u8> {
        RDCleanPathPdu::new_request(
            x224.to_vec(),
            destination.to_owned(),
            "token".to_owned(),
            None,
        )
        .unwrap()
        .to_der()
        .unwrap()
    }

    #[test]
    fn detect_reports_not_enough_bytes_then_the_total_length() {
        let der = sample_request_der("172.18.21.102:3389", &[0x03, 0x00]);
        assert_eq!(detect_pdu(&der[..2]).unwrap(), None);
        assert_eq!(detect_pdu(&der).unwrap(), Some(der.len()));
    }

    #[test]
    fn detect_rejects_garbage() {
        let err = detect_pdu(&[0xff, 0xff, 0xff, 0xff]).unwrap_err();
        assert!(matches!(err, RdCleanPathError::Malformed(_)));
        assert!(err.to_string().contains("RDCleanPath"));
    }

    #[test]
    fn parse_request_extracts_destination_and_x224() {
        let x224 = [0x03, 0x00, 0x00, 0x13, 0x0e, 0xe0];
        let der = sample_request_der("172.18.21.102:3389", &x224);

        let request = parse_request(&der).unwrap();

        assert_eq!(request.destination, "172.18.21.102:3389");
        assert_eq!(request.x224_connection_request, x224);
        assert_eq!(request.proxy_auth, "token");
    }

    #[test]
    fn parse_request_rejects_malformed_der() {
        let err = parse_request(&[0x30, 0x80, 0x00]).unwrap_err();
        assert!(matches!(err, RdCleanPathError::Malformed(_)));
    }

    #[test]
    fn parse_request_rejects_a_response() {
        // 中継が返すはずのものが送られてきた場合。
        let der = build_response("1.2.3.4:3389", vec![0x03, 0x00], vec![vec![0xaa]]).unwrap();
        let err = parse_request(&der).unwrap_err();
        assert!(matches!(err, RdCleanPathError::Unexpected("要求ではない")));
    }

    #[test]
    fn response_round_trips_through_der() {
        let x224 = vec![0x03, 0x00, 0x00, 0x13];
        let cert = vec![0x30, 0x82, 0x01];

        let der = build_response("172.18.21.102:3389", x224.clone(), vec![cert.clone()]).unwrap();

        assert_eq!(detect_pdu(&der).unwrap(), Some(der.len()));

        let message = RDCleanPathPdu::from_der(&der).unwrap().into_enum().unwrap();
        match message {
            RDCleanPath::Response {
                x224_connection_response,
                server_cert_chain,
                server_addr,
            } => {
                assert_eq!(x224_connection_response.as_bytes(), x224);
                assert_eq!(server_cert_chain.len(), 1);
                assert_eq!(server_cert_chain[0].as_bytes(), cert);
                assert_eq!(server_addr, "172.18.21.102:3389");
            }
            other => panic!("応答にならなかった: {other:?}"),
        }
    }

    #[test]
    fn negotiation_error_carries_the_x224_refusal() {
        let refusal = vec![0x03, 0x00, 0x00, 0x13, 0x0e, 0xd0];
        let der = build_negotiation_error(refusal.clone()).unwrap();

        let message = RDCleanPathPdu::from_der(&der).unwrap().into_enum().unwrap();
        match message {
            RDCleanPath::NegotiationErr {
                x224_connection_response,
            } => assert_eq!(x224_connection_response, refusal),
            other => panic!("交渉エラーにならなかった: {other:?}"),
        }
    }

    #[test]
    fn general_error_is_a_valid_pdu() {
        let der = build_general_error().unwrap();
        assert_eq!(detect_pdu(&der).unwrap(), Some(der.len()));

        let message = RDCleanPathPdu::from_der(&der).unwrap().into_enum().unwrap();
        assert!(matches!(message, RDCleanPath::GeneralErr(_)));
    }

    #[test]
    fn build_error_is_displayed() {
        let err = RdCleanPathError::Build("too long".to_owned());
        assert!(err.to_string().contains("too long"));
    }
}
