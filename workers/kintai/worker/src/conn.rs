//! 1 本の MySQL 接続の上のやり取り (handshake → 認証 → クエリ → COM_QUIT)。
//! パケットの読み書きは kintai-mysql、ここは socket との間の受け渡しだけ。
//! 失敗は種別の名前 (`String`) で返し、サーバーの文言・ユーザー名・宛先は持ち出さない。

use kintai_mysql::handshake::{
    auth_switch_response, handshake_response41, native_password_scramble, parse_auth_reply,
    parse_initial_handshake, AuthReply, InitialHandshake,
};
use kintai_mysql::packet::{frame, Packet, PacketBuf};
use kintai_mysql::response::{
    com_query, com_quit, parse_query_response, QueryResponse, ResultSet, ResultSetReader,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use worker::Socket;

use crate::probe::Creds;

/// Auth Switch に応じる回数の上限 (普通は 1 回。往復し続けるサーバーで止まらないように)。
const MAX_AUTH_SWITCHES: usize = 2;

pub(crate) struct Session {
    socket: Socket,
    buf: PacketBuf,
}

fn codec(e: kintai_mysql::Error) -> String {
    e.kind()
}

fn io(e: std::io::Error) -> String {
    format!("io:{:?}", e.kind())
}

impl Session {
    pub(crate) fn new(socket: Socket) -> Self {
        Self {
            socket,
            buf: PacketBuf::new(),
        }
    }

    /// 論理パケットを 1 つ読む。相手が閉じたら `closed`。
    async fn read(&mut self) -> Result<Packet, String> {
        let mut chunk = [0u8; 4096];
        loop {
            if let Some(packet) = self.buf.take() {
                return Ok(packet);
            }
            let n = self.socket.read(&mut chunk).await.map_err(io)?;
            if n == 0 {
                return Err("closed".to_string());
            }
            self.buf.extend(&chunk[..n]);
        }
    }

    async fn send(&mut self, seq: u8, payload: &[u8]) -> Result<(), String> {
        let bytes = frame(seq, payload).map_err(codec)?;
        self.socket.write_all(&bytes).await.map_err(io)?;
        self.socket.flush().await.map_err(io)
    }

    /// サーバーの Initial Handshake を読む。返り値の seq は応答に使う。
    pub(crate) async fn handshake(&mut self) -> Result<(u8, InitialHandshake), String> {
        let packet = self.read().await?;
        let hs = parse_initial_handshake(&packet.payload).map_err(codec)?;
        Ok((packet.seq, hs))
    }

    /// HandshakeResponse41 を送り、mysql_native_password で認証する。
    pub(crate) async fn authenticate(
        &mut self,
        seq: u8,
        hs: &InitialHandshake,
        creds: &Creds,
    ) -> Result<(), String> {
        let password = creds.password.as_bytes();
        let auth = native_password_scramble(password, &hs.auth_plugin_data);
        let response = handshake_response41(&creds.user, &auth, &creds.database).map_err(codec)?;
        self.send(seq.wrapping_add(1), &response).await?;
        let mut reply = self.read().await?;
        for _ in 0..MAX_AUTH_SWITCHES {
            match parse_auth_reply(&reply.payload).map_err(codec)? {
                AuthReply::Ok(_) => return Ok(()),
                AuthReply::Switch { plugin, data } => {
                    let response = auth_switch_response(&plugin, &data, password).map_err(codec)?;
                    self.send(reply.seq.wrapping_add(1), &response).await?;
                    reply = self.read().await?;
                }
            }
        }
        Err("auth_switch_loop".to_string())
    }

    /// COM_QUERY を 1 本流して結果を読む。結果セットを返さない文 (SET 等) は空の結果。
    pub(crate) async fn query(&mut self, sql: &str) -> Result<ResultSet, String> {
        self.send(0, &com_query(sql)).await?;
        let first = self.read().await?;
        let count = match parse_query_response(&first.payload).map_err(codec)? {
            QueryResponse::Ok(_) => return Ok(ResultSet::default()),
            QueryResponse::ResultSet(count) => count,
        };
        let mut reader = ResultSetReader::new(count).map_err(codec)?;
        loop {
            let packet = self.read().await?;
            if let Some(set) = reader.push(&packet.payload).map_err(codec)? {
                return Ok(set);
            }
        }
    }

    /// COM_QUIT を送って閉じる。失敗しても結果は変わらないので捨てる。
    pub(crate) async fn quit(mut self) {
        let _ = self.send(0, &com_quit()).await;
        let _ = self.socket.close().await;
    }
}
