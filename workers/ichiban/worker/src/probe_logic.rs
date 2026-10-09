//! `POST /probe` の経路判定・失敗の stage と応答の写像・資格情報 JSON の検証 (純粋ロジック)。
//! workers/kyuyo の `logic/src/lib.rs` (kyuyo-logic) から probe に要る分だけを写している (path 依存は作らない)。
//! 到達確認が済むまで捨てる可能性がある PoC なので共通化しない。

use serde::Deserialize;

/// 経路判定の結果。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Route {
    Probe,
    NotFound,
    MethodNotAllowed,
}

/// `POST /probe` だけが口。path が合って method が違えば 405、それ以外は 404。
pub(crate) fn route(method: &str, path: &str) -> Route {
    match (path, method) {
        ("/probe", "POST") => Route::Probe,
        ("/probe", _) => Route::MethodNotAllowed,
        _ => Route::NotFound,
    }
}

/// どこで失敗したか。応答とログに出すのはこの名前だけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// 資格情報の JSON が読めない・キー欠け・空
    Secret,
    /// TCP が開けない、または上限時間内にログインまで終わらない
    Connect,
    /// TDS のログインが拒否された
    Login,
    /// `SELECT 1` が失敗した・時間切れ・1 が返らない
    Query,
}

impl Stage {
    pub(crate) fn as_str(self) -> &'static str {
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
pub(crate) enum ErrKind {
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
    pub(crate) fn label(&self) -> String {
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

/// 接続の失敗 (stage と種類だけ。本文は持たない)。
#[derive(Debug)]
pub(crate) struct DbError {
    pub(crate) stage: Stage,
    pub(crate) kind: ErrKind,
}

/// probe の失敗ログの 1 行。メッセージ本文を受け取らない (stage・種類・所要ミリ秒だけ)。
pub(crate) fn log_line(stage: Stage, kind: &ErrKind, elapsed_ms: u64) -> String {
    let (stage, kind) = (stage.as_str(), kind.label());
    format!("ichiban probe: failed at {stage} ({elapsed_ms} ms) kind={kind}")
}

/// 応答の status と JSON 本文。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) body: String,
}

/// 経路判定で弾いた行き先の応答。`Route::Probe` は `None` (先へ進む)。
pub(crate) fn reply_for_route(route: &Route) -> Option<Reply> {
    let status = match route {
        Route::Probe => return None,
        Route::NotFound => 404,
        Route::MethodNotAllowed => 405,
    };
    Some(Reply {
        status,
        body: r#"{"ok":false}"#.to_string(),
    })
}

/// probe の結果の応答。成功は 200 `{"ok":true}`、失敗は 502 `{"ok":false,"stage":…}`。
pub(crate) fn reply_for_probe(outcome: Result<(), (Stage, ErrKind)>) -> Reply {
    match outcome {
        Ok(()) => Reply {
            status: 200,
            body: r#"{"ok":true}"#.to_string(),
        },
        Err((stage, kind)) => Reply {
            status: 502,
            body: format!(
                r#"{{"ok":false,"stage":"{}","kind":"{}"}}"#,
                stage.as_str(),
                kind.label()
            ),
        },
    }
}

/// SQL Server 認証の資格情報 (Secrets Store の JSON `{"user":…,"pass":…}`)。
/// `Debug` を持たせない (うっかりログに出さない)。
#[derive(Deserialize)]
pub(crate) struct Creds {
    pub(crate) user: String,
    pub(crate) pass: String,
}

/// 資格情報の JSON を検証する。JSON が読めない・キー欠け・文字列でない・空は `Err(Stage::Secret)`。
pub(crate) fn parse_creds(json: &str) -> Result<Creds, Stage> {
    let creds: Creds = serde_json::from_str(json).map_err(|_| Stage::Secret)?;
    if creds.user.is_empty() || creds.pass.is_empty() {
        return Err(Stage::Secret);
    }
    Ok(creds)
}
