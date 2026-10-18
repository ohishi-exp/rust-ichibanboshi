//! `vpc-mysql` (Workers VPC → MariaDB の MySQL 接続) の純粋部分: 資格情報 JSON の検証。
//! Worker にも MariaDB にも依存しないので native で `cargo test` できる。

use serde::Deserialize;

/// 資格情報 (Secrets Store の JSON `{"user":…,"password":…,"database":…}`)。
/// `Debug` を持たせない (うっかりログに出さない)。
#[derive(Deserialize)]
pub struct Creds {
    pub user: String,
    pub password: String,
    pub database: String,
}

/// 資格情報の JSON を検証する。JSON が読めない・キー欠け・文字列でない・空は `None`。
pub fn parse_creds(json: &str) -> Option<Creds> {
    let creds: Creds = serde_json::from_str(json).ok()?;
    if creds.user.is_empty() || creds.password.is_empty() || creds.database.is_empty() {
        return None;
    }
    Some(creds)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn creds_ok() {
        let c = parse_creds(r#"{"user":"u","password":"p","database":"d"}"#).unwrap();
        assert_eq!(
            (c.user.as_str(), c.password.as_str(), c.database.as_str()),
            ("u", "p", "d")
        );
        // 余分なキーは無視する
        assert!(parse_creds(r#"{"user":"u","password":"p","database":"d","x":1}"#).is_some());
    }

    #[test]
    fn creds_rejected() {
        for json in [
            "",
            "not json",
            "[]",
            r#"{"user":"u","password":"p"}"#,
            r#"{"user":"u","database":"d"}"#,
            r#"{"password":"p","database":"d"}"#,
            r#"{"user":"","password":"p","database":"d"}"#,
            r#"{"user":"u","password":"","database":"d"}"#,
            r#"{"user":"u","password":"p","database":""}"#,
            r#"{"user":null,"password":"p","database":"d"}"#,
            r#"{"user":1,"password":"p","database":"d"}"#,
        ] {
            assert!(parse_creds(json).is_none(), "{json}");
        }
    }
}
