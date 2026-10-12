//! 接続設定の読み: Secrets Store の資格情報 JSON の検証と、ローカル検証用アドレスの分解。

use serde::Deserialize;

use crate::Stage;

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

/// ローカル検証用のアドレス `host:port` を分ける (`[::1]:1433` の角括弧は外す)。
pub fn split_host_port(addr: &str) -> Option<(String, u16)> {
    let (host, port) = addr.rsplit_once(':')?;
    let port = port.parse::<u16>().ok()?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    Some((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn host_port() {
        let ok = split_host_port;
        assert_eq!(ok("127.0.0.1:1433"), Some(("127.0.0.1".into(), 1433)));
        assert_eq!(ok("localhost:14330"), Some(("localhost".into(), 14330)));
        assert_eq!(ok("[::1]:1433"), Some(("::1".into(), 1433)));
        for bad in ["", "localhost", "localhost:", "localhost:x", "h:70000"] {
            assert_eq!(split_host_port(bad), None, "{bad}");
        }
    }
}
