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
