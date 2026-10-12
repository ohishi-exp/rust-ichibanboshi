//! 売上 Worker の経路の判定と応答の形 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 口はこの段では `GET /health`・`POST /probe` (D1 に `SELECT 1`) と、読みの口 6 本 (path はオンプレ版と同じ)。
//! 書き込みの口 (recalc・verify・verify-debug・admin) は次の PR で足す。それまで (と、移さない by-person・raw・ack) は 404。
//!
//! 応答の本文:
//! - 成功はオンプレ版と同じ JSON
//! - 400 はオンプレ版と同じ (必須の Query が空 = 本文なし、verify-history は平文、Query が読めない = axum と同じ平文)
//! - 404 / 405 は `{"ok":false}`
//! - D1 までの失敗は `{"ok":false,"stage":…,"kind":…}` だけ (D1 の message は出さない)。binding が無い = 503、D1 の失敗 = 500
//!   (オンプレ版の LocalStore と同じ status)

use serde::Serialize;

/// 経路。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Health,
    Probe,
    Read(ReadRoute),
    NotFound,
    MethodNotAllowed,
}

/// 読みの口 6 本 (どれも GET)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadRoute {
    Daily,
    PersonMonthlyTotals,
    PersonPartnerTotals,
    R2Pending,
    RecalcJobs,
    VerifyHistory,
}

impl ReadRoute {
    /// ログに出す口の名前 (`/api/uriage/` の後ろ)。
    pub fn name(self) -> &'static str {
        match self {
            ReadRoute::Daily => "daily",
            ReadRoute::PersonMonthlyTotals => "person-monthly-totals",
            ReadRoute::PersonPartnerTotals => "person-partner-totals",
            ReadRoute::R2Pending => "r2/pending",
            ReadRoute::RecalcJobs => "recalc-jobs",
            ReadRoute::VerifyHistory => "verify-history",
        }
    }
}

/// method と path から経路を決める。path が口に当たって method が違えば 405、どの口でもなければ 404。
pub fn route(method: &str, path: &str) -> Route {
    let (want, found) = match path {
        "/health" => ("GET", Route::Health),
        "/probe" => ("POST", Route::Probe),
        _ => match path.strip_prefix("/api/uriage/").and_then(read_route) {
            Some(r) => ("GET", Route::Read(r)),
            None => return Route::NotFound,
        },
    };
    if method == want {
        found
    } else {
        Route::MethodNotAllowed
    }
}

fn read_route(rest: &str) -> Option<ReadRoute> {
    Some(match rest {
        "daily" => ReadRoute::Daily,
        "person-monthly-totals" => ReadRoute::PersonMonthlyTotals,
        "person-partner-totals" => ReadRoute::PersonPartnerTotals,
        "r2/pending" => ReadRoute::R2Pending,
        "recalc-jobs" => ReadRoute::RecalcJobs,
        "verify-history" => ReadRoute::VerifyHistory,
        _ => return None,
    })
}

/// 応答 (status・本文・content-type)。本文が空なら content-type は付けない (オンプレ版の StatusCode だけの応答と同じ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reply {
    pub status: u16,
    pub body: String,
    pub content_type: Option<&'static str>,
}

const JSON: &str = "application/json";
const TEXT: &str = "text/plain; charset=utf-8";

#[derive(Serialize)]
struct FailureBody<'a> {
    ok: bool,
    stage: &'a str,
    kind: &'a str,
}

impl Reply {
    /// 200 の JSON。
    pub fn ok(body: String) -> Self {
        Self {
            status: 200,
            body,
            content_type: Some(JSON),
        }
    }

    /// 本文なし (オンプレ版の `Err(StatusCode::BAD_REQUEST)`)。
    pub fn bare(status: u16) -> Self {
        Self {
            status,
            body: String::new(),
            content_type: None,
        }
    }

    /// 平文 (オンプレ版の `(StatusCode, String)` と axum の Query の拒否)。
    pub fn text(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
            content_type: Some(TEXT),
        }
    }

    /// D1 までの失敗。本文は段と種別だけ。
    pub fn failure(status: u16, stage: &str, kind: &str) -> Self {
        let body = FailureBody {
            ok: false,
            stage,
            kind,
        };
        Self::json(status, serde_json::to_string(&body).unwrap_or_default())
    }

    /// 経路で弾く 404 / 405 (`{"ok":false}`)。口に当たれば `None`。
    pub fn for_route(route: Route) -> Option<Self> {
        match route {
            Route::NotFound => Some(Self::json(404, r#"{"ok":false}"#.to_string())),
            Route::MethodNotAllowed => Some(Self::json(405, r#"{"ok":false}"#.to_string())),
            _ => None,
        }
    }

    /// D1 の binding が無い (503)。
    pub fn no_binding() -> Self {
        Self::failure(503, "binding", "missing")
    }

    /// D1 の失敗 (500)。`stage` は `bind`・`query` 等。
    pub fn d1_failed(stage: &str) -> Self {
        Self::failure(500, stage, "failed")
    }

    fn json(status: u16, body: String) -> Self {
        Self {
            status,
            body,
            content_type: Some(JSON),
        }
    }
}

/// `GET /health` の本文。
pub const HEALTH_BODY: &str = r#"{"status":"ok"}"#;

/// `POST /probe` の本文 (この段では D1 だけ。SQL Server・CakePHP・R2 は次の PR で足す)。
pub const PROBE_BODY: &str = r#"{"ok":true,"d1":"ok"}"#;
