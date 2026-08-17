//! RDP 中継 (ブラウザ ↔ RDS) の実体。
//!
//! ねらい: 一番星の RemoteApp をブラウザ内で描画する構成 (IronRDP/WASM) では、
//! ブラウザが生の TCP を開けないため、間に「RDCleanPath を喋り、RDS との TLS を
//! 代わりに張る」中継が必ず要る。この bin がそれになる。
//!
//! 置き場所は `ohishi-data` — 社内 LAN 側にあり RDS (172.18.21.102:3389) へ直接届くため、
//! 経路が 1 段減る。既存の `ichibanboshi` サービスと同じ systemd の型で動かす
//! (`deploy/rdp-relay.service`)。
//!
//! 口は 2 つ。`probe` が「Rust から X.224 → TLS を張れるか」を実機に対して確かめる用、
//! `serve` が本番の中継。
//!
//! ## 誰を通すか
//!
//! ブラウザがどう届くかで 2 通りあり、`--auth` で選ぶ。**どちらも「中継が公開の口を
//! 素通しで持つ」ことはない。**
//!
//! - `cf-access` … 中継自身を公開ホスト名で出し、前段の Cloudflare Access が利用者を
//!   認証する。中継は Access を通ったことを**自分で確かめる**: `/rdp` は
//!   `Cf-Access-Jwt-Assertion` を team の JWKS で検証し、通らなければ 401 で閉じる
//!   (`rust_ichibanboshi::cf_access`)。設定漏れやヘッダ偽装で素通りさせないため
//! - `vpc` … 中継に公開ホスト名を与えず、Workers VPC の binding 経由でだけ届かせる。
//!   利用者の認証は**アプリ側の Worker が自分のセッションで**行い、中継はそこから
//!   来た要求だけを受ける。Access は経路に居ないのでヘッダを要求しない
//!
//! ブラウザの WebSocket はヘッダを足せず、Access の cookie も別オリジンには飛ばない。
//! アプリに埋め込む (別オリジンになる) 構成では `vpc` を使う。
//!
//! `/health` はどちらでも素通し。deploy の疎通確認が localhost から叩くため。
//!
//! プロトコルの解釈は `rust_ichibanboshi::rdp_nego` に閉じている。ここは I/O だけを持つ。

use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse as _;
use axum::routing::get;
use axum::Router;
use clap::{Parser, Subcommand, ValueEnum};
use futures_util::{SinkExt as _, StreamExt as _};
use ironrdp_pdu::nego::SecurityProtocol;
use rust_ichibanboshi::cf_access::CfAccessVerifier;
use rust_ichibanboshi::rdcleanpath::{
    build_general_error, build_negotiation_error, build_response, detect_pdu, parse_request,
    RelayRequest,
};
use rust_ichibanboshi::rdp_nego::{
    build_connection_request, needs_tls, parse_connection_confirm, tpkt_frame_len, TPKT_HEADER_LEN,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;
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

    /// 中継を起動する。ブラウザ (IronRDP/WASM) が WebSocket で繋いでくる。
    Serve {
        /// 待ち受けアドレス。既定は loopback のみで、公開は前段の Cloudflare Tunnel が行う。
        /// tunnel の connector が別ホストに居る構成では loopback だと届かないので、
        /// そこだけ site 側 (systemd の EnvironmentFile) で上書きする。
        #[arg(long, env = "RDP_RELAY_BIND", default_value = "127.0.0.1:3390")]
        bind: String,

        /// 繋いでよい RDS。ブラウザから任意の宛先を指定させないための allowlist。
        #[arg(long)]
        allow: Vec<String>,

        /// 誰を通すか。経路の作りに合わせて選ぶ (モジュール冒頭の説明を参照)。
        #[arg(long, value_enum, default_value_t = AuthMode::CfAccess, env = "RDP_RELAY_AUTH")]
        auth: AuthMode,

        /// Cloudflare Access の team ドメイン。例: example.cloudflareaccess.com
        /// (team 名だけでもよい)。JWKS と期待する iss の出所になる。
        /// `--auth cf-access` のときだけ要る。systemd では EnvironmentFile から渡す。
        #[arg(long, env = "CF_ACCESS_TEAM_DOMAIN")]
        cf_access_team_domain: Option<String>,

        /// Access アプリの AUD タグ。これを固定しないと同じ team の別アプリの
        /// トークンで入られる。`--auth cf-access` のときだけ要る。
        #[arg(long, env = "CF_ACCESS_AUD")]
        cf_access_aud: Option<String>,

        /// RDS への接続と読み取りの制限時間 (秒)。
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
        Command::Serve {
            bind,
            allow,
            auth,
            cf_access_team_domain,
            cf_access_aud,
            timeout,
        } => {
            serve(ServeOptions {
                bind,
                allow,
                auth,
                team_domain: cf_access_team_domain,
                aud: cf_access_aud,
                timeout: Duration::from_secs(timeout),
            })
            .await
        }
    }
}

/// Cloudflare Access が origin へ挿してくるヘッダ。
const ACCESS_JWT_HEADER: &str = "cf-access-jwt-assertion";

/// 中継の入口を誰に開けるか。
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum AuthMode {
    /// 前段の Cloudflare Access を中継自身で検証する (公開ホスト名で出す構成)。
    CfAccess,
    /// Workers VPC の binding 経由でだけ届く前提。認証はアプリ側の Worker が持つ。
    Vpc,
}

/// `serve` の起動引数。
struct ServeOptions {
    bind: String,
    allow: Vec<String>,
    auth: AuthMode,
    team_domain: Option<String>,
    aud: Option<String>,
    timeout: Duration,
}

/// 中継の設定。WebSocket ハンドラへ共有する。
#[derive(Clone)]
struct RelayState {
    /// 繋いでよい RDS の集合。空なら誰にも繋がない (安全側に倒す)。
    allow: Arc<Vec<String>>,
    /// 入口で Access トークンを検証する。`--auth vpc` では持たない
    /// (Access が経路に居ないため。誰を通すかは前段の Worker が決める)。
    verifier: Option<CfAccessVerifier>,
    timeout: Duration,
}

async fn serve(options: ServeOptions) -> Result<(), BoxError> {
    // 何も繋がらないときに「届いていないのか、落ちているのか」を切り分けられないと詰むので、
    // 中継は経過をログに出す。既定は info。
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if options.allow.is_empty() {
        return Err(
            "--allow で繋ぎ先を 1 つ以上指定すること (例: --allow 172.18.21.102:3389)".into(),
        );
    }

    // 設定不足は起動時に鳴らす (`--allow` 未指定を拒否するのと同じ姿勢)。
    // 黙って上がって全部 401 になる方が、切り分けがはるかに難しい。
    let verifier = match options.auth {
        AuthMode::CfAccess => {
            let team = options
                .team_domain
                .ok_or("--auth cf-access には CF_ACCESS_TEAM_DOMAIN が要る")?;
            let aud = options
                .aud
                .ok_or("--auth cf-access には CF_ACCESS_AUD が要る")?;
            let verifier = CfAccessVerifier::new(&team, aud)?;
            tracing::info!("入口: Access を検証する (team {team})");
            Some(verifier)
        }
        AuthMode::Vpc => {
            tracing::info!("入口: Workers VPC 経由のみ (Access は経路外)");
            None
        }
    };

    let state = RelayState {
        allow: Arc::new(options.allow),
        verifier,
        timeout: options.timeout,
    };

    let app = Router::new()
        // deploy の疎通確認が localhost から叩く。Access の検証は掛けない。
        .route("/health", get(|| async { "ok" }))
        .route("/rdp", get(ws_upgrade))
        .with_state(state.clone());

    let listener = tokio::net::TcpListener::bind(&options.bind).await?;
    tracing::info!("中継を開始: ws://{}/rdp", options.bind);
    tracing::info!("繋ぎ先: {}", state.allow.join(", "));

    axum::serve(listener, app).await?;
    Ok(())
}

async fn ws_upgrade(
    ws: WebSocketUpgrade,
    State(state): State<RelayState>,
    headers: HeaderMap,
) -> axum::response::Response {
    tracing::info!("WebSocket の接続要求を受けた");

    // `--auth vpc` では検証器を持たない。Access が経路に居ないので、ここで
    // ヘッダを要求すると誰も通れなくなる。誰を通すかは前段の Worker が決める。
    if let Some(verifier) = &state.verifier {
        // Access を通ったことを中継自身で確かめる。ヘッダの有無だけでは信用しない。
        let Some(token) = headers.get(ACCESS_JWT_HEADER).and_then(|v| v.to_str().ok()) else {
            tracing::warn!("Access トークンの無い要求を拒否");
            return (StatusCode::UNAUTHORIZED, "Cf-Access-Jwt-Assertion が無い").into_response();
        };

        match verifier.verify(token).await {
            Ok(identity) => tracing::info!("Access 検証を通った: {}", identity.subject),
            Err(e) => {
                tracing::warn!("Access 検証で拒否: {e}");
                return (StatusCode::UNAUTHORIZED, "Access トークンが通らない").into_response();
            }
        }
    }

    ws.on_upgrade(move |socket| async move {
        match relay_session(socket, state).await {
            Ok(()) => tracing::info!("中継セッション終了"),
            Err(e) => tracing::warn!("中継セッション異常終了: {e}"),
        }
    })
}

/// 1 本の WebSocket を最後まで面倒みる。
///
/// 前半が RDCleanPath のハンドシェイク (1 往復)、後半は素の RDP を右から左へ流すだけ。
async fn relay_session(mut socket: WebSocket, state: RelayState) -> Result<(), BoxError> {
    // --- 前半: RDCleanPath ---
    let request = read_rdcleanpath_request(&mut socket).await?;
    tracing::info!("RDCleanPath 要求を受理: 宛先 {}", request.destination);

    if !state.allow.iter().any(|a| a == &request.destination) {
        tracing::warn!("許可されていない宛先を拒否: {}", request.destination);
        let _ = socket
            .send(Message::Binary(build_general_error()?.into()))
            .await;
        return Err(format!("許可されていない宛先: {}", request.destination).into());
    }

    let mut stream =
        tokio::time::timeout(state.timeout, TcpStream::connect(&request.destination)).await??;

    // ブラウザが組み立てた X.224 要求をそのまま流す。中継は中身を作り変えない。
    stream.write_all(&request.x224_connection_request).await?;
    let confirm = read_x224_frame(&mut stream, state.timeout).await?;

    // 蹴られたなら、その接続確認をそのまま返す。理由はブラウザ側で解釈させる。
    if parse_connection_confirm(&confirm).is_err() {
        socket
            .send(Message::Binary(build_negotiation_error(confirm)?.into()))
            .await?;
        return Err("RDS が接続を蹴った".into());
    }

    tracing::info!("X.224 交渉が成立、TLS を張る: {}", request.destination);
    let tls = tls_connect(stream).await?;
    let chain = tls
        .get_ref()
        .1
        .peer_certificates()
        .map(|certs| certs.iter().map(|c| c.to_vec()).collect::<Vec<_>>())
        .unwrap_or_default();

    // 証明書を返さないとブラウザは NLA (CredSSP) に進めない。
    let response = build_response(&request.destination, confirm, chain)?;
    socket.send(Message::Binary(response.into())).await?;
    tracing::info!("RDCleanPath ハンドシェイク完了、素の RDP を流し始める");

    // --- 後半: 素の RDP を双方向に流す ---
    pump(socket, tls).await
}

/// WebSocket から RDCleanPath の要求を 1 つ読む。
///
/// WebSocket のフレーム境界と PDU の境界は一致しないので、揃うまで溜める。
async fn read_rdcleanpath_request(socket: &mut WebSocket) -> Result<RelayRequest, BoxError> {
    let mut buf: Vec<u8> = Vec::new();

    while let Some(message) = socket.recv().await {
        match message? {
            Message::Binary(bytes) => buf.extend_from_slice(&bytes),
            Message::Close(_) => return Err("ハンドシェイク前に切断された".into()),
            // テキストや ping は RDCleanPath には現れない。無視して読み続ける。
            _ => continue,
        }

        if let Some(total) = detect_pdu(&buf)? {
            if buf.len() >= total {
                return Ok(parse_request(&buf[..total])?);
            }
        }
    }

    Err("RDCleanPath 要求が来ないまま WebSocket が閉じた".into())
}

/// TPKT の長さに従って X.224 のフレームを 1 つ読む。
async fn read_x224_frame(stream: &mut TcpStream, timeout: Duration) -> Result<Vec<u8>, BoxError> {
    let mut header = [0u8; TPKT_HEADER_LEN];
    tokio::time::timeout(timeout, stream.read_exact(&mut header)).await??;

    let frame_len = tpkt_frame_len(&header)?;
    let mut frame = vec![0u8; frame_len];
    frame[..TPKT_HEADER_LEN].copy_from_slice(&header);
    tokio::time::timeout(timeout, stream.read_exact(&mut frame[TPKT_HEADER_LEN..])).await??;

    Ok(frame)
}

/// ブラウザ ↔ RDS を双方向に流す。片方が閉じたら終わり。
async fn pump(socket: WebSocket, tls: TlsStream<TcpStream>) -> Result<(), BoxError> {
    let (mut ws_tx, mut ws_rx) = socket.split();
    let (mut server_rx, mut server_tx) = tokio::io::split(tls);

    // ブラウザ → RDS
    let to_server = async {
        while let Some(message) = ws_rx.next().await {
            match message? {
                Message::Binary(bytes) => server_tx.write_all(&bytes).await?,
                Message::Close(_) => break,
                _ => continue,
            }
        }
        Ok::<_, BoxError>(())
    };

    // RDS → ブラウザ
    let to_browser = async {
        let mut chunk = vec![0u8; 16 * 1024];
        loop {
            let read = server_rx.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            ws_tx
                .send(Message::Binary(chunk[..read].to_vec().into()))
                .await?;
        }
        Ok::<_, BoxError>(())
    };

    tokio::select! {
        result = to_server => result,
        result = to_browser => result,
    }
}

/// RDS と TLS を張る。証明書は検証しない ([`NoCertVerification`] の判断理由を参照)。
async fn tls_connect(stream: TcpStream) -> Result<TlsStream<TcpStream>, BoxError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(NoCertVerification(provider)))
        .with_no_client_auth();

    // 証明書を検証しないので、ここに渡す名前は何でも通る。
    let server_name = ServerName::try_from("rdp-target")?.to_owned();
    Ok(TlsConnector::from(Arc::new(config))
        .connect(server_name, stream)
        .await?)
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
