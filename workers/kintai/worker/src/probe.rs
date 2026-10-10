//! 経路判定・失敗の段と応答の写像・資格情報 JSON の検証 (純粋ロジック)。
//! workers/ichiban の `worker/src/probe_logic.rs` の形を写している。

use serde::{Deserialize, Serialize};

/// 経路判定の結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// `POST /probe` — 繋いで `SELECT 1, VERSION(), …` を流す
    Probe,
    /// `GET /api/kintai/*` — Supabase (Hyperdrive) を読む 5 本
    Read(Read),
    NotFound,
    MethodNotAllowed,
}

/// Supabase の勤怠スキーマを読む口 (純粋部分は kintai-logic)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Read {
    DaySummaries,
    ShiftOverlaps,
    ShiftDays,
    ChangeLog,
    WageRange,
}

impl Read {
    /// 口の path。元 (Cloud Run 版) と同じ。
    pub(crate) fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/kintai/day-summaries" => Read::DaySummaries,
            "/api/kintai/shift-overlaps" => Read::ShiftOverlaps,
            "/api/kintai/shift-days" => Read::ShiftDays,
            "/api/kintai/change-log" => Read::ChangeLog,
            "/api/kintai/wage-range" => Read::WageRange,
            _ => return None,
        })
    }

    /// ログに出す名前。
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Read::DaySummaries => "day-summaries",
            Read::ShiftOverlaps => "shift-overlaps",
            Read::ShiftDays => "shift-days",
            Read::ChangeLog => "change-log",
            Read::WageRange => "wage-range",
        }
    }
}

/// 口は `POST /probe` と `GET /api/kintai/*` の 5 本。path が合って method が違えば 405、それ以外の path は 404。
pub(crate) fn route(method: &str, path: &str) -> Route {
    if let Some(read) = Read::from_path(path) {
        return if method == "GET" {
            Route::Read(read)
        } else {
            Route::MethodNotAllowed
        };
    }
    match (path, method) {
        ("/probe", "POST") => Route::Probe,
        ("/probe", _) => Route::MethodNotAllowed,
        _ => Route::NotFound,
    }
}

/// どこで失敗したか。応答とログに出すのはこの名前だけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// 資格情報の secret が無い・読めない・キー欠け・空 (503)
    Secret,
    /// TCP が開けない・時間切れ
    Connect,
    /// Initial Handshake が読めない
    Handshake,
    /// 認証が通らない
    Auth,
    /// クエリが失敗した・時間切れ・結果が想定と違う
    Query,
}

impl Stage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Stage::Secret => "secret",
            Stage::Connect => "connect",
            Stage::Handshake => "handshake",
            Stage::Auth => "auth",
            Stage::Query => "query",
        }
    }
}

/// 失敗 (段と種別の名前だけ。サーバーの文言・宛先・ユーザー名は持たない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Failure {
    pub(crate) stage: Stage,
    pub(crate) kind: String,
}

impl Failure {
    pub(crate) fn new(stage: Stage, kind: impl Into<String>) -> Self {
        Self {
            stage,
            kind: kind.into(),
        }
    }
}

/// 応答の status と JSON 本文。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) body: String,
}

/// 経路判定で弾いた行き先の応答。口に当たったら `None` (先へ進む)。
pub(crate) fn reply_for_route(route: Route) -> Option<Reply> {
    let status = match route {
        Route::NotFound => 404,
        Route::MethodNotAllowed => 405,
        Route::Probe | Route::Read(_) => return None,
    };
    Some(Reply {
        status,
        body: r#"{"ok":false}"#.to_string(),
    })
}

/// 資格情報 (Secrets Store の JSON `{"user":…,"password":…,"database":…}`)。
/// `Debug` を持たせない (うっかりログに出さない)。
#[derive(Deserialize)]
pub(crate) struct Creds {
    pub(crate) user: String,
    pub(crate) password: String,
    pub(crate) database: String,
}

/// 資格情報の JSON を検証する。JSON が読めない・キー欠け・文字列でない・空は `None`。
pub(crate) fn parse_creds(json: &str) -> Option<Creds> {
    let creds: Creds = serde_json::from_str(json).ok()?;
    if creds.user.is_empty() || creds.password.is_empty() || creds.database.is_empty() {
        return None;
    }
    Some(creds)
}

/// `CURRENT_USER()` (`user@host`) の `@` より前が資格情報の user と一致するか。
pub(crate) fn user_matches(current_user: &str, user: &str) -> bool {
    current_user
        .split_once('@')
        .map_or(current_user, |(u, _)| u)
        == user
}

/// `POST /probe` の成功の本文。DB のユーザー名そのものは返さない (一致したかの真偽だけ)。
#[derive(Debug, Serialize, PartialEq, Eq)]
pub(crate) struct ProbeOk {
    pub(crate) ok: bool,
    pub(crate) version: String,
    pub(crate) charset: String,
    pub(crate) user_matches: bool,
    pub(crate) elapsed_ms: u64,
}

/// probe の結果の応答。成功は 200、資格情報が無いのは 503、MariaDB までの失敗は 502 `{"ok":false,"stage":…,"kind":…}`。
pub(crate) fn reply_for(outcome: Result<ProbeOk, Failure>) -> Reply {
    match outcome {
        Ok(ok) => Reply {
            status: 200,
            body: serde_json::to_string(&ok).unwrap_or_default(),
        },
        Err(f) => Reply {
            status: if f.stage == Stage::Secret { 503 } else { 502 },
            body: serde_json::json!({"ok": false, "stage": f.stage.as_str(), "kind": f.kind})
                .to_string(),
        },
    }
}

/// 失敗ログの 1 行 (段・種別・所要ミリ秒だけ)。
pub(crate) fn log_line(f: &Failure, elapsed_ms: u64) -> String {
    let (stage, kind) = (f.stage.as_str(), &f.kind);
    format!("kintai probe: failed at {stage} ({elapsed_ms} ms) kind={kind}")
}
