//! RDP 中継 (ブラウザ ↔ RDS) の実体。
//!
//! ねらい: 一番星の RemoteApp をブラウザ内で描画する構成 (IronRDP/WASM) では、
//! ブラウザが生の TCP を開けないため、間に「RDCleanPath を喋り、RDS との TLS を
//! 代わりに張る」中継が必ず要る。この bin がそれになる。
//!
//! 置き場所は `ohishi-data` — 社内 LAN 側にあり RDS (172.18.21.102:3389) へ直接届くため、
//! 経路が 1 段減る。既存の `ichibanboshi` サービスと同じ systemd の型で動かす。
//!
//! いまは `probe` だけ。「Rust から X.224 → TLS を張れるか」を実機に対して確かめる段階で、
//! RDCleanPath と WebSocket はこの上に積む。
//!
//! プロトコルの解釈は `rust_ichibanboshi::rdp_nego` に閉じている。ここは I/O だけを持つ。

use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use ironrdp_pdu::nego::SecurityProtocol;
use rust_ichibanboshi::rdp_nego::{
    build_connection_request, needs_tls, parse_connection_confirm, tpkt_frame_len, TPKT_HEADER_LEN,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(name = "rdp-relay", about = "RDP 中継 (ブラウザ ↔ RDS)")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// RDS に X.224 の接続開始を投げ、TLS まで張れるかを確かめる。
    Probe {
        /// 接続先。例: 172.18.21.102:3389
        #[arg(long)]
        target: String,

        /// 接続と読み取りの制限時間 (秒)。
        #[arg(long, default_value_t = 10)]
        timeout: u64,
    },
}

/// 証明書を検証しない検証器。
///
/// RDS の証明書は自己署名で、Guacamole 側も `ignore-cert` で運用している。
/// **中継は社内 LAN 内で RDS に隣接して動く**前提なので、ここでは検証しない。
/// 経路上に第三者が入りうる構成へ移すなら、この判断はやり直すこと。
#[derive(Debug)]
struct NoCertVerification(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();

    match cli.command {
        Command::Probe { target, timeout } => probe(&target, Duration::from_secs(timeout)).await,
    }
}

async fn probe(target: &str, timeout: Duration) -> Result<(), BoxError> {
    println!("接続先: {target}");

    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(target)).await??;
    println!("  TCP        : 接続した");

    // NLA を要求する。Guacamole の security=nla と同じ要求内容。
    let requested = SecurityProtocol::SSL | SecurityProtocol::HYBRID;
    let request = build_connection_request(requested)?;
    stream.write_all(&request).await?;

    let mut header = [0u8; TPKT_HEADER_LEN];
    tokio::time::timeout(timeout, stream.read_exact(&mut header)).await??;

    let frame_len = tpkt_frame_len(&header)?;
    let mut frame = vec![0u8; frame_len];
    frame[..TPKT_HEADER_LEN].copy_from_slice(&header);
    tokio::time::timeout(timeout, stream.read_exact(&mut frame[TPKT_HEADER_LEN..])).await??;

    let selected = parse_connection_confirm(&frame)?;
    println!("  X.224      : サーバーが選んだプロトコル = {selected:?}");

    if !needs_tls(selected) {
        println!("  TLS        : 不要 (標準 RDP セキュリティ)");
        return Ok(());
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertVerification(provider)))
        .with_no_client_auth();

    // 証明書を検証しないので、ここに渡す名前は何でも通る。
    let server_name = ServerName::try_from("rdp-target")?.to_owned();
    let tls = TlsConnector::from(Arc::new(config))
        .connect(server_name, stream)
        .await?;

    let (_, connection) = tls.get_ref();
    let version = connection
        .protocol_version()
        .map_or_else(|| "不明".to_owned(), |v| format!("{v:?}"));
    let cert_count = connection.peer_certificates().map_or(0, <[_]>::len);

    println!("  TLS        : ハンドシェイク成功 ({version}, 証明書 {cert_count} 枚)");
    println!("中継の核 (X.224 → TLS) は Rust から張れる。");

    Ok(())
}
