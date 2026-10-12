//! `vpc-tds` (Workers VPC → SQL Server の TDS 接続) の純粋部分: 失敗の stage と種類・資格情報 JSON の検証・
//! host:port の分解。Worker にも SQL Server にも依存しないので native で `cargo test` できる。
//! workers/ichiban と workers/kyuyo に写されていたものを 1 本にした (Refs ohishi-exp/rust-ichibanboshi#322)。

mod config;

pub use config::{parse_creds, split_host_port, Creds};

/// どこで失敗したか。応答とログに出すのはこの名前だけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    /// 資格情報の JSON が読めない・キー欠け・空
    Secret,
    /// TCP が開けない、または上限時間内にログインまで終わらない
    Connect,
    /// TDS のログインが拒否された
    Login,
    /// クエリが失敗した・時間切れ (`SELECT 1` の probe は 1 が返らないときも)
    Query,
}

impl Stage {
    pub fn as_str(self) -> &'static str {
        match self {
            Stage::Secret => "secret",
            Stage::Connect => "connect",
            Stage::Login => "login",
            Stage::Query => "query",
        }
    }
}

/// 失敗の種類。ログ 1 行に出すのはこの分類だけで、エラーの本文 (ホスト・ポート・ユーザー名を含みうる) は持たない。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrKind {
    /// 上限時間内に終わらない
    Timeout,
    /// VPC binding からの Socket 取得・connect の失敗
    Transport,
    /// I/O エラー。`std::io::ErrorKind` の名前だけ (例 "Other"、"ConnectionReset")
    Io(String),
    /// SQL Server が返したエラー (番号・クラス・状態だけ)
    Server { code: u32, class: u8, state: u8 },
    /// TDS のプロトコル違反
    Protocol,
    /// 文字コードの不一致
    Encoding,
    /// TLS のハンドシェイク失敗
    Tls,
    /// サーバが別アドレスへの接続を要求した
    Routing,
    /// 上のどれでもない
    Other,
}

impl ErrKind {
    /// ログ用の短い名前 (`io:Other`、`server:18456/14/1` など)。
    pub fn label(&self) -> String {
        match self {
            ErrKind::Timeout => "timeout".to_string(),
            ErrKind::Transport => "transport".to_string(),
            ErrKind::Io(name) => format!("io:{name}"),
            ErrKind::Server { code, class, state } => format!("server:{code}/{class}/{state}"),
            ErrKind::Protocol => "protocol".to_string(),
            ErrKind::Encoding => "encoding".to_string(),
            ErrKind::Tls => "tls".to_string(),
            ErrKind::Routing => "routing".to_string(),
            ErrKind::Other => "other".to_string(),
        }
    }
}

/// SQL Server を読む処理の失敗 (stage と種類だけ。本文は持たない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    pub stage: Stage,
    pub kind: ErrKind,
}

impl DbError {
    /// 「DB が開けない」(SQL Server error 4060)。文言でなくエラー番号で判定する。
    pub fn cannot_open(&self) -> bool {
        matches!(self.kind, ErrKind::Server { code: 4060, .. })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stage_names() {
        for (stage, name) in [
            (Stage::Secret, "secret"),
            (Stage::Connect, "connect"),
            (Stage::Login, "login"),
            (Stage::Query, "query"),
        ] {
            assert_eq!(stage.as_str(), name);
        }
    }

    #[test]
    fn err_kind_labels() {
        let server = ErrKind::Server {
            code: 18456,
            class: 14,
            state: 1,
        };
        for (kind, want) in [
            (ErrKind::Timeout, "timeout"),
            (ErrKind::Transport, "transport"),
            (ErrKind::Io("Other".into()), "io:Other"),
            (ErrKind::Io("ConnectionReset".into()), "io:ConnectionReset"),
            (server, "server:18456/14/1"),
            (ErrKind::Protocol, "protocol"),
            (ErrKind::Encoding, "encoding"),
            (ErrKind::Tls, "tls"),
            (ErrKind::Routing, "routing"),
            (ErrKind::Other, "other"),
        ] {
            assert_eq!(kind.label(), want);
        }
    }

    #[test]
    fn cannot_open_by_code() {
        let err = |kind| DbError {
            stage: Stage::Query,
            kind,
        };
        let server = |code| ErrKind::Server {
            code,
            class: 11,
            state: 1,
        };
        assert!(err(server(4060)).cannot_open());
        assert!(!err(server(208)).cannot_open());
        assert!(!err(ErrKind::Io("Other".into())).cannot_open());
    }
}
