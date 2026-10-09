//! `/kyuyo/*` の認可と、Worker が返す `/kyuyo/*` の応答 (Refs #322)。
//!
//! 認可は auth-worker の `KyuyoAuthEntrypoint.authorize(token)` (Service Binding `AUTH_KYUYO`) が答える。
//! 戻り値は throw しない `{status, body, contentType}` で、200 `{"allowed":true,"email":…}` のときだけ通す。
//! 200 以外は **その status と body をそのまま返す** (401 / 403 / 503 の文言は auth-worker のもの)。
//! 200 でも body から email が取れなければ通さない (fail-closed)。
//!
//! 認可済みの email は Worker → DO の内部ヘッダ [`EMAIL_HEADER`] で渡す。Worker は DO への
//! リクエストを新しく組み立てる (外から来たヘッダを 1 つも写さない) ので、外から同名のヘッダを
//! 送っても DO には届かない。

use serde::Deserialize;

use crate::api::{AccessResponse, ErrorBody, SyncedMonthEntry, SyncedMonthsResponse};
use crate::store_keys::parse_payroll_scope;
use crate::Reply;

/// Worker → DO の内部ヘッダ (認可済みの email)。外から来た同名ヘッダは DO に届かない。
pub const EMAIL_HEADER: &str = "x-kyuyo-authorized-email";

/// `Authorization` ヘッダから Bearer token を取り出す (`"Bearer "` 接頭辞)。無ければ空文字 — auth-worker は空の token を 401 にする (allowlist 未設定なら先に 503)。
pub fn bearer_token(authorization: Option<&str>) -> &str {
    authorization
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

#[derive(Deserialize)]
struct Allowed {
    allowed: bool,
    email: String,
}

/// auth-worker の `authorize` の戻りを読む。通すなら `Ok(email)`、弾くなら `Err(返す応答)`。
pub fn decide(status: u16, body: &str) -> Result<String, Reply> {
    if status != 200 {
        return Err(Reply {
            status,
            body: body.to_string(),
        });
    }
    match serde_json::from_str::<Allowed>(body) {
        Ok(a) if a.allowed && !a.email.is_empty() => Ok(a.email),
        _ => Err(server_error()),
    }
}

/// 認可に届かない・応答が読めない・内部ヘッダが無い (503、auth-worker の `server_error` と同じ body)。
pub fn server_error() -> Reply {
    error_reply(503, "server_error")
}

/// derived store (DO の SQLite) が読めない (500)。文言はオンプレ版の synced-months と同じ。
pub fn store_error() -> Reply {
    error_reply(500, "キャッシュ一覧の読み出しに失敗しました")
}

fn error_reply(status: u16, error: &str) -> Reply {
    let body = ErrorBody {
        error: error.to_string(),
    };
    Reply {
        status,
        body: to_json(&body),
    }
}

/// `GET /kyuyo/access` の 200 応答。
pub fn access_reply(email: &str) -> Reply {
    let body = AccessResponse {
        allowed: true,
        email: email.to_string(),
    };
    Reply {
        status: 200,
        body: to_json(&body),
    }
}

/// `GET /kyuyo/synced-months` の 200 応答。`rows` は `store_keys::PAYROLL_SYNCED_SQL` の
/// (scope, synced_at, row_count)。scope の形が合わない行は捨てる (オンプレ版と同じ)。
pub fn synced_months_reply(rows: Vec<(String, String, i64)>) -> Reply {
    let entries = rows
        .into_iter()
        .filter_map(|(scope, synced_at, row_count)| {
            let (company, month) = parse_payroll_scope(&scope)?;
            Some(SyncedMonthEntry {
                company,
                month,
                synced_at,
                row_count,
            })
        })
        .collect();
    Reply {
        status: 200,
        body: to_json(&SyncedMonthsResponse { entries }),
    }
}

fn to_json<T: serde::Serialize>(v: &T) -> String {
    // 応答型は String / 数値 / bool だけなので serialize は失敗しない
    serde_json::to_string(v).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer() {
        assert_eq!(bearer_token(Some("Bearer abc")), "abc");
        assert_eq!(bearer_token(Some("Bearer ")), "");
        assert_eq!(bearer_token(Some("bearer abc")), "");
        assert_eq!(bearer_token(Some("Basic abc")), "");
        assert_eq!(bearer_token(None), "");
    }

    #[test]
    fn decide_passes_200_with_email() {
        let body = r#"{"allowed":true,"email":"a@example.com"}"#;
        assert_eq!(decide(200, body), Ok("a@example.com".to_string()));
    }

    #[test]
    fn decide_forwards_non_200_as_is() {
        for (status, body) in [
            (401, r#"{"error":"unauthorized"}"#),
            (403, r#"{"error":"forbidden"}"#),
            (503, r#"{"error":"kyuyo_allowlist_unset"}"#),
            (503, r#"{"error":"server_error"}"#),
        ] {
            let r = decide(status, body).unwrap_err();
            assert_eq!((r.status, r.body.as_str()), (status, body));
        }
    }

    #[test]
    fn decide_fails_closed_on_odd_200() {
        for body in [
            "",
            "not json",
            r#"{"allowed":false,"email":"a@example.com"}"#,
            r#"{"allowed":true,"email":""}"#,
            r#"{"allowed":true}"#,
            r#"{"email":"a@example.com"}"#,
        ] {
            let r = decide(200, body).unwrap_err();
            assert_eq!(r, server_error(), "{body}");
        }
    }

    #[test]
    fn error_replies() {
        let r = server_error();
        assert_eq!(
            (r.status, r.body.as_str()),
            (503, r#"{"error":"server_error"}"#)
        );
        let r = store_error();
        assert_eq!(r.status, 500);
        assert_eq!(
            r.body,
            r#"{"error":"キャッシュ一覧の読み出しに失敗しました"}"#
        );
    }

    #[test]
    fn access() {
        let r = access_reply("a@example.com");
        assert_eq!(
            (r.status, r.body.as_str()),
            (200, r#"{"allowed":true,"email":"a@example.com"}"#)
        );
    }

    #[test]
    fn synced_months() {
        let r = synced_months_reply(vec![]);
        assert_eq!((r.status, r.body.as_str()), (200, r#"{"entries":[]}"#));
        let r = synced_months_reply(vec![
            ("payroll:0100:2026-05".into(), "t1".into(), 3),
            ("payroll".into(), "t2".into(), 1),
            ("payroll:0200:2026-06".into(), "t3".into(), 0),
        ]);
        let want = concat!(
            r#"{"entries":[{"company":"0100","month":"2026-05","synced_at":"t1","row_count":3},"#,
            r#"{"company":"0200","month":"2026-06","synced_at":"t3","row_count":0}]}"#
        );
        assert_eq!(r.body, want);
    }

    #[test]
    fn header_is_lowercase() {
        assert_eq!(EMAIL_HEADER, EMAIL_HEADER.to_ascii_lowercase());
    }
}
