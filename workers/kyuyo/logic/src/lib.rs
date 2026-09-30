//! 給与大臣 Worker (`kyuyo-worker`) の純粋部分: `/probe` の経路判定・失敗の stage と応答の写像・
//! 資格情報 JSON の検証。Worker にも SQL Server にも依存しないので native で `cargo test` できる。

use serde::Deserialize;

/// リクエストの行き先。
#[derive(Debug, PartialEq, Eq)]
pub enum Route {
    /// `POST /probe`
    Probe,
    /// パスが `/probe` 以外 (404)
    NotFound,
    /// `/probe` で POST 以外 (405)
    MethodNotAllowed,
}

/// method と path から行き先を決める。
pub fn route(method: &str, path: &str) -> Route {
    match (path, method) {
        ("/probe", "POST") => Route::Probe,
        ("/probe", _) => Route::MethodNotAllowed,
        _ => Route::NotFound,
    }
}

/// どこで失敗したか。応答とログに出すのはこの名前だけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
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

/// 失敗ログの 1 行。メッセージ本文を受け取らない (stage・種類・所要ミリ秒だけ)。
pub fn log_line(stage: Stage, kind: &ErrKind, elapsed_ms: u64) -> String {
    format!(
        "kyuyo probe: failed at {} ({elapsed_ms} ms) kind={}",
        stage.as_str(),
        kind.label()
    )
}

/// 応答の status と JSON 本文。
#[derive(Debug, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub body: String,
}

/// 経路判定で弾いた行き先の応答。`Route::Probe` は `None` (probe を走らせる)。
pub fn reply_for_route(route: &Route) -> Option<Reply> {
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
pub fn reply_for_probe(outcome: Result<(), Stage>) -> Reply {
    match outcome {
        Ok(()) => Reply {
            status: 200,
            body: r#"{"ok":true}"#.to_string(),
        },
        Err(stage) => Reply {
            status: 502,
            body: format!(r#"{{"ok":false,"stage":"{}"}}"#, stage.as_str()),
        },
    }
}

/// SQL Server 認証の資格情報 (Secrets Store の JSON `{"user":…,"pass":…}`)。
/// `Debug` を持たせない (うっかりログに出さない)。
#[derive(Deserialize)]
pub struct Creds {
    pub user: String,
    pub pass: String,
}

/// 資格情報の JSON を検証する。JSON が読めない・キー欠け・文字列でない・空は `Err(Stage::Secret)`。
pub fn parse_creds(json: &str) -> Result<Creds, Stage> {
    let creds: Creds = serde_json::from_str(json).map_err(|_| Stage::Secret)?;
    if creds.user.is_empty() || creds.pass.is_empty() {
        return Err(Stage::Secret);
    }
    Ok(creds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn route_probe_only_post() {
        assert_eq!(route("POST", "/probe"), Route::Probe);
        assert_eq!(route("GET", "/probe"), Route::MethodNotAllowed);
        assert_eq!(route("PUT", "/probe"), Route::MethodNotAllowed);
        assert_eq!(route("POST", "/x"), Route::NotFound);
        assert_eq!(route("POST", "/probe/"), Route::NotFound);
        assert_eq!(route("GET", "/"), Route::NotFound);
    }

    #[test]
    fn unrouted_replies() {
        assert_eq!(reply_for_route(&Route::Probe), None);
        let r = reply_for_route(&Route::NotFound).unwrap();
        assert_eq!((r.status, r.body.as_str()), (404, r#"{"ok":false}"#));
        let r = reply_for_route(&Route::MethodNotAllowed).unwrap();
        assert_eq!((r.status, r.body.as_str()), (405, r#"{"ok":false}"#));
    }

    #[test]
    fn probe_ok_is_200() {
        let r = reply_for_probe(Ok(()));
        assert_eq!((r.status, r.body.as_str()), (200, r#"{"ok":true}"#));
    }

    #[test]
    fn probe_failures_are_502_with_stage() {
        for (stage, name) in [
            (Stage::Secret, "secret"),
            (Stage::Connect, "connect"),
            (Stage::Login, "login"),
            (Stage::Query, "query"),
        ] {
            let r = reply_for_probe(Err(stage));
            assert_eq!(r.status, 502);
            let v: serde_json::Value = serde_json::from_str(&r.body).unwrap();
            assert_eq!(v, serde_json::json!({"ok": false, "stage": name}));
        }
    }

    #[test]
    fn log_line_per_kind() {
        let cases = [
            (
                Stage::Login,
                ErrKind::Timeout,
                20000,
                "kyuyo probe: failed at login (20000 ms) kind=timeout",
            ),
            (
                Stage::Connect,
                ErrKind::Transport,
                3,
                "kyuyo probe: failed at connect (3 ms) kind=transport",
            ),
            (
                Stage::Login,
                ErrKind::Io("Other".into()),
                5019,
                "kyuyo probe: failed at login (5019 ms) kind=io:Other",
            ),
            (
                Stage::Query,
                ErrKind::Io("ConnectionReset".into()),
                7,
                "kyuyo probe: failed at query (7 ms) kind=io:ConnectionReset",
            ),
            (
                Stage::Login,
                ErrKind::Server {
                    code: 18456,
                    class: 14,
                    state: 1,
                },
                12,
                "kyuyo probe: failed at login (12 ms) kind=server:18456/14/1",
            ),
            (
                Stage::Login,
                ErrKind::Protocol,
                1,
                "kyuyo probe: failed at login (1 ms) kind=protocol",
            ),
            (
                Stage::Login,
                ErrKind::Encoding,
                1,
                "kyuyo probe: failed at login (1 ms) kind=encoding",
            ),
            (
                Stage::Login,
                ErrKind::Tls,
                1,
                "kyuyo probe: failed at login (1 ms) kind=tls",
            ),
            (
                Stage::Login,
                ErrKind::Routing,
                1,
                "kyuyo probe: failed at login (1 ms) kind=routing",
            ),
            (
                Stage::Secret,
                ErrKind::Other,
                0,
                "kyuyo probe: failed at secret (0 ms) kind=other",
            ),
        ];
        for (stage, kind, ms, want) in cases {
            let line = log_line(stage, &kind, ms);
            assert_eq!(line, want);
            assert!(!line.contains('\n'));
        }
    }

    #[test]
    fn log_line_takes_no_message() {
        // 引数は stage・種類・ミリ秒だけ。本文を渡す口が型に無いことを関数ポインタの型で固定する
        let _: fn(Stage, &ErrKind, u64) -> String = log_line;
    }

    #[test]
    fn creds_ok() {
        let c = parse_creds(r#"{"user":"u","pass":"p"}"#).ok().unwrap();
        assert_eq!((c.user.as_str(), c.pass.as_str()), ("u", "p"));
        // 余分なキーは無視する
        assert!(parse_creds(r#"{"user":"u","pass":"p","x":1}"#).is_ok());
    }

    #[test]
    fn creds_rejected() {
        for json in [
            "",
            "not json",
            "[]",
            r#"{"user":"u"}"#,
            r#"{"pass":"p"}"#,
            r#"{"user":"","pass":"p"}"#,
            r#"{"user":"u","pass":""}"#,
            r#"{"user":null,"pass":"p"}"#,
            r#"{"user":1,"pass":"p"}"#,
        ] {
            assert_eq!(parse_creds(json).err(), Some(Stage::Secret), "{json}");
        }
    }
}
