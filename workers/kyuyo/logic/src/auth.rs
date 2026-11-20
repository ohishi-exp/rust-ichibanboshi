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
//!
//! 検証器はヘッダ [`AUTH_MODE_HEADER`] で選ぶ (Refs #414): 無し (か空) なら `authorize` (ブラウザ JWT)、
//! `mcp` なら `authorizeMcp` (kyuyo-mcp の binding_jwt)。それ以外の値は 400 で、auth-worker を呼ばない
//! (黙って `authorize` に落とさない)。どちらのメソッドも auth-worker 側で署名を検証するので、
//! ヘッダは検証器を選ぶだけで認可を緩めない。流れは [`authorize_request`]。

use std::future::Future;

use serde::Deserialize;

use crate::api::{
    AccessResponse, ErrorBody, SyncedBonusEntry, SyncedMonthEntry, SyncedMonthsResponse,
};
use crate::store_keys::{parse_bonus_scope, parse_payroll_scope};
use crate::Reply;

/// Worker → DO の内部ヘッダ (認可済みの email)。外から来た同名ヘッダは DO に届かない。
pub const EMAIL_HEADER: &str = "x-kyuyo-authorized-email";

/// `Authorization` ヘッダから Bearer token を取り出す (`"Bearer "` 接頭辞)。無ければ空文字 — auth-worker は空の token を 401 にする (allowlist 未設定なら先に 503)。
pub fn bearer_token(authorization: Option<&str>) -> &str {
    authorization
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("")
}

/// 検証器を選ぶヘッダ (値 `mcp` のときだけ `authorizeMcp`)。
pub const AUTH_MODE_HEADER: &str = "x-kyuyo-auth";

/// auth-worker の `KyuyoAuthEntrypoint` のどのメソッドで token を検証するか。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verifier {
    /// `authorize(token)` — ブラウザ JWT
    Browser,
    /// `authorizeMcp(token)` — kyuyo-mcp の binding_jwt
    Mcp,
}

impl Verifier {
    /// RPC のメソッド名 (JS 側の名前)。
    pub fn method(self) -> &'static str {
        match self {
            Verifier::Browser => "authorize",
            Verifier::Mcp => "authorizeMcp",
        }
    }
}

/// [`AUTH_MODE_HEADER`] の値から検証器を選ぶ。無し・空は `Browser`、`mcp` は `Mcp`、それ以外は 400。
pub fn select_verifier(mode: Option<&str>) -> Result<Verifier, Reply> {
    match mode {
        None | Some("") => Ok(Verifier::Browser),
        Some("mcp") => Ok(Verifier::Mcp),
        Some(_) => Err(error_reply(400, "bad_auth_mode")),
    }
}

/// `/kyuyo/*` の認可の流れ。`mode` は [`AUTH_MODE_HEADER`]、`authorization` は `Authorization` ヘッダの値。
/// `call` は選んだ検証器で auth-worker を 1 回呼び、`(status, body)` を返す (binding が無い・RPC が落ちた・
/// メソッドが無いときは `None` → 503)。検証器が選べなければ `call` を呼ばずに 400。
pub async fn authorize_request<'a, F, Fut>(
    mode: Option<&str>,
    authorization: Option<&'a str>,
    call: F,
) -> Result<String, Reply>
where
    F: FnOnce(Verifier, &'a str) -> Fut,
    Fut: Future<Output = Option<(u16, String)>>,
{
    let verifier = select_verifier(mode)?;
    let token = bearer_token(authorization);
    match call(verifier, token).await {
        Some((status, body)) => decide(status, &body),
        None => Err(server_error()),
    }
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

/// 賞与の sync 済み 1 行 (`store_keys::BONUS_SYNCED_SQL` の scope, synced_at, pay_date, row_count)。
pub type BonusSyncedRow = (String, String, Option<String>, i64);

/// `GET /kyuyo/synced-months` の 200 応答。`rows` は `store_keys::PAYROLL_SYNCED_SQL` の
/// (scope, synced_at, row_count)、`bonus` は [`BonusSyncedRow`]。scope の形が合わない行は捨てる (オンプレ版と同じ)。
pub fn synced_months_reply(rows: Vec<(String, String, i64)>, bonus: Vec<BonusSyncedRow>) -> Reply {
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
    let bonus = bonus
        .into_iter()
        .filter_map(|(scope, synced_at, pay_date, row_count)| {
            let (company, year) = parse_bonus_scope(&scope)?;
            Some(SyncedBonusEntry {
                company,
                year,
                pay_date,
                synced_at,
                row_count,
            })
        })
        .collect();
    Reply {
        status: 200,
        body: to_json(&SyncedMonthsResponse { entries, bonus }),
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
        let r = synced_months_reply(vec![], vec![]);
        assert_eq!(
            (r.status, r.body.as_str()),
            (200, r#"{"entries":[],"bonus":[]}"#)
        );
        let r = synced_months_reply(
            vec![
                ("payroll:0100:2026-05".into(), "t1".into(), 3),
                ("payroll".into(), "t2".into(), 1),
                ("payroll:0200:2026-06".into(), "t3".into(), 0),
            ],
            vec![
                (
                    "bonus:0100:2025".into(),
                    "t4".into(),
                    Some("2025-08-08".into()),
                    2,
                ),
                ("bonus:0100:x".into(), "t5".into(), None, 0),
                ("bonus:0200:2024".into(), "t6".into(), None, 0),
            ],
        );
        let want = concat!(
            r#"{"entries":[{"company":"0100","month":"2026-05","synced_at":"t1","row_count":3},"#,
            r#"{"company":"0200","month":"2026-06","synced_at":"t3","row_count":0}],"#,
            r#""bonus":[{"company":"0100","year":2025,"pay_date":"2025-08-08","synced_at":"t4","row_count":2},"#,
            r#"{"company":"0200","year":2024,"pay_date":null,"synced_at":"t6","row_count":0}]}"#
        );
        assert_eq!(r.body, want);
    }

    #[test]
    fn header_is_lowercase() {
        assert_eq!(EMAIL_HEADER, EMAIL_HEADER.to_ascii_lowercase());
        assert_eq!(AUTH_MODE_HEADER, AUTH_MODE_HEADER.to_ascii_lowercase());
    }

    use std::cell::RefCell;
    use std::future::{ready, Ready};
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    const OK_BODY: &str = r#"{"allowed":true,"email":"a@example.com"}"#;

    /// mock の呼び出しはすぐ返るので 1 回の poll で終わる。
    fn poll_once<F: Future>(fut: F) -> Poll<F::Output> {
        pin!(fut).poll(&mut Context::from_waker(Waker::noop()))
    }

    /// 呼ばれた検証器と token を記録し、`reply` を返す mock の RPC で [`authorize_request`] を回す。
    fn run(
        mode: Option<&str>,
        authorization: Option<&str>,
        reply: Option<(u16, &str)>,
    ) -> (Poll<Result<String, Reply>>, Vec<(Verifier, String)>) {
        let calls = RefCell::new(Vec::new());
        let call = |v: Verifier, token: &str| -> Ready<Option<(u16, String)>> {
            calls.borrow_mut().push((v, token.to_string()));
            ready(reply.map(|(s, b)| (s, b.to_string())))
        };
        let out = poll_once(authorize_request(mode, authorization, call));
        (out, calls.into_inner())
    }

    #[test]
    fn verifier_method_names() {
        assert_eq!(Verifier::Browser.method(), "authorize");
        assert_eq!(Verifier::Mcp.method(), "authorizeMcp");
    }

    #[test]
    fn select_verifier_by_header() {
        assert_eq!(select_verifier(None), Ok(Verifier::Browser));
        assert_eq!(select_verifier(Some("")), Ok(Verifier::Browser));
        assert_eq!(select_verifier(Some("mcp")), Ok(Verifier::Mcp));
        for v in ["MCP", "browser", " mcp", "mcp "] {
            let r = select_verifier(Some(v)).unwrap_err();
            assert_eq!(
                (r.status, r.body.as_str()),
                (400, r#"{"error":"bad_auth_mode"}"#),
                "{v}"
            );
        }
    }

    // ① ヘッダ無し → authorize だけ (authorizeMcp は呼ばれない)
    #[test]
    fn no_header_calls_authorize_only() {
        let (out, calls) = run(None, Some("Bearer t1"), Some((200, OK_BODY)));
        assert_eq!(out, Poll::Ready(Ok("a@example.com".to_string())));
        assert_eq!(calls, vec![(Verifier::Browser, "t1".to_string())]);
    }

    // ② X-Kyuyo-Auth: mcp → authorizeMcp だけ
    #[test]
    fn mcp_header_calls_authorize_mcp_only() {
        let (out, calls) = run(Some("mcp"), Some("Bearer t2"), Some((200, OK_BODY)));
        assert_eq!(out, Poll::Ready(Ok("a@example.com".to_string())));
        assert_eq!(calls, vec![(Verifier::Mcp, "t2".to_string())]);
    }

    // ③ 未知の値 → 400、auth-worker を呼ばない
    #[test]
    fn unknown_header_is_400_without_rpc() {
        let (out, calls) = run(Some("jwt"), Some("Bearer t3"), Some((200, OK_BODY)));
        let want = Reply {
            status: 400,
            body: r#"{"error":"bad_auth_mode"}"#.to_string(),
        };
        assert_eq!(out, Poll::Ready(Err(want)));
        assert!(calls.is_empty());
    }

    // ④ authorizeMcp の 401 / 403 はそのまま返す
    #[test]
    fn mcp_rejection_is_forwarded() {
        for (status, body) in [
            (401, r#"{"error":"unauthorized"}"#),
            (403, r#"{"error":"forbidden"}"#),
        ] {
            let (out, calls) = run(Some("mcp"), Some("Bearer t4"), Some((status, body)));
            let want = Reply {
                status,
                body: body.to_string(),
            };
            assert_eq!(out, Poll::Ready(Err(want)));
            assert_eq!(calls, vec![(Verifier::Mcp, "t4".to_string())]);
        }
    }

    // authorizeMcp が auth-worker にまだ無い (RPC が「メソッドが無い」で落ちる) → 503 (fail-closed)
    #[test]
    fn rpc_failure_is_503() {
        for mode in [None, Some("mcp")] {
            let (out, calls) = run(mode, Some("Bearer t5"), None);
            assert_eq!(out, Poll::Ready(Err(server_error())));
            assert_eq!(calls.len(), 1);
        }
    }

    // token の取り出し方は変えない (Bearer 無しは空文字で auth-worker へ渡す)
    #[test]
    fn token_extraction_unchanged() {
        let (_, calls) = run(Some("mcp"), None, Some((401, "")));
        assert_eq!(calls, vec![(Verifier::Mcp, String::new())]);
    }
}
