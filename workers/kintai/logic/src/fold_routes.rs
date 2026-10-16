//! 勤怠 Worker の fold の口 (`GET`/`POST /api/kintai/recalc`・`POST /api/kintai/timecard/window`・
//! `GET /api/kintai/stale-months`・診断の `GET /api/kintai/alc-events`) の HTTP の殻の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 段取り (月ゲート → 畳み → 封 → 応答、打刻の置き換え、窓の畳み直し) は共有 crate の `kintai_kosoku::route_flow`
//! (root と同じもの)。ここに在るのは root の殻 (`src/routes/kintai_recalc.rs`・`kintai_timecard.rs` の `receive_window`・
//! `stale_months.rs`) に当たる部分だけ: 経路の判定 (同じ path で method が違う 2 本を含む)・入力の検査 (400 の文言は root と
//! 同じ)・失敗の写し (502 の本文に DB や上流の message を出さない)・時計・fold の設定 (`KosokuParams` は既定値)。

use std::sync::OnceLock;

use chrono::{DateTime, FixedOffset, NaiveDate};
use kintai_kosoku::fold_flow::{FoldClock, FoldCtx};
use kintai_kosoku::kintai_fold;
use kintai_kosoku::kintai_push::TimecardWindow;
use kintai_kosoku::kosoku::KosokuParams;
use kintai_kosoku::route_flow::{RecalcQuery, RecalcRequest};
use serde::Deserialize;

use crate::common::{bad_request, is_valid_month, parse_json, parse_query, Fail};

const RECALC: &str = "/api/kintai/recalc";
const WINDOW: &str = "/api/kintai/timecard/window";
const STALE_MONTHS: &str = "/api/kintai/stale-months";
const ALC_EVENTS: &str = "/api/kintai/alc-events";

/// 4 口。`Recalc` と `TimecardWindow` が書き込みの口 (`X-Kintai-Write-Token` を照合する)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FoldRoute {
    /// `GET /api/kintai/recalc` — 1 行も書かずに畳み直しの結果を返す (認可なし)
    RecalcPreview,
    /// `POST /api/kintai/recalc` — 1 ページぶんを畳み直す (`apply` が無ければ書かない)
    Recalc,
    /// `POST /api/kintai/timecard/window` — 窓ぶんの打刻を置き換えて、変わった乗務員だけ畳み直す
    TimecardWindow,
    /// `GET /api/kintai/stale-months` — 月ごとの stale 乗務員数 (認可なし)
    StaleMonths,
    /// `GET /api/kintai/alc-events` — fold が読む 1 人ぶんの alc の行と、そこから組んだ勤務 (診断。認可なし・書かない)
    AlcEvents,
}

impl FoldRoute {
    /// path が 4 口のどれでもなければ `None`。path が合えば、method も合えば `Some(Some(口))`、違えば `Some(None)` (405)。
    /// **同じ path (`/api/kintai/recalc`) で method が違えば別の口** (root と同じ。GET は読みの口、POST は書き込みの口)。
    pub fn from_request(method: &str, path: &str) -> Option<Option<Self>> {
        let route = match (path, method) {
            (RECALC, "GET") => Self::RecalcPreview,
            (RECALC, "POST") => Self::Recalc,
            (WINDOW, "POST") => Self::TimecardWindow,
            (STALE_MONTHS, "GET") => Self::StaleMonths,
            (ALC_EVENTS, "GET") => Self::AlcEvents,
            (RECALC | WINDOW | STALE_MONTHS | ALC_EVENTS, _) => return Some(None),
            _ => return None,
        };
        Some(Some(route))
    }

    /// 書き込みの口か (認可の照合が要る)。
    pub fn is_write(self) -> bool {
        matches!(self, Self::Recalc | Self::TimecardWindow)
    }

    /// ログに出す名前。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RecalcPreview => "recalc (preview)",
            Self::Recalc => "recalc",
            Self::TimecardWindow => "timecard/window",
            Self::StaleMonths => "stale-months",
            Self::AlcEvents => "alc-events",
        }
    }
}

const BAD_MONTH: &str = "month は YYYY-MM で指定してください";

fn checked(req: RecalcRequest) -> Result<RecalcRequest, Fail> {
    match is_valid_month(&req.month) {
        true => Ok(req),
        false => Err(bad_request(BAD_MONTH)),
    }
}

/// `GET /api/kintai/recalc` のクエリ (root と同じ順: Query → month)。`apply` は持たない (読みの口から書けない)。
pub fn parse_recalc_query(query: &str) -> Result<RecalcRequest, Fail> {
    let q: RecalcQuery = parse_query(query)?;
    checked(q.into_request())
}

/// `POST /api/kintai/recalc` の本文 (root と同じ順: Json の拒否 (415/413/400/422) → month)。
pub fn parse_recalc_body(content_type: Option<&str>, body: &[u8]) -> Result<RecalcRequest, Fail> {
    checked(parse_json(content_type, body)?)
}

/// `POST /api/kintai/timecard/window` の本文 (root と同じ順: Json の拒否 → months が空 → 最初の不正な月)。
pub fn parse_window(content_type: Option<&str>, body: &[u8]) -> Result<TimecardWindow, Fail> {
    let window: TimecardWindow = parse_json(content_type, body)?;
    if window.months.is_empty() {
        return Err(bad_request("months が空です"));
    }
    if let Some(bad) = window.months.iter().find(|m| !is_valid_month(m)) {
        return Err(bad_request(format!("month は YYYY-MM です: {bad}")));
    }
    Ok(window)
}

/// `GET /api/kintai/alc-events` の入力 (`driver`・`from`・`to`。日付は JST の `YYYY-MM-DD`、`to` の日を含む)。
#[derive(Debug, Deserialize)]
struct AlcEventsQuery {
    driver: u64,
    from: String,
    to: String,
}

/// 検査済みの [`AlcEventsQuery`]。`from`・`to` は fold の読みと同じ `[from, to)` の壁時計 (`to` は翌日の 0 時)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlcEventsRequest {
    pub driver: u64,
    pub from: String,
    pub to: String,
}

/// 1 回で読む日数の上限。fold の 1 か月の読み窓 (遡りと翌月 2 日を含む) が入る長さ。
pub const ALC_EVENTS_MAX_DAYS: i64 = 62;

/// `GET /api/kintai/alc-events` のクエリ。Query → 日付 (`YYYY-MM-DD`) → `from <= to` → 日数の上限。
pub fn parse_alc_events_query(query: &str) -> Result<AlcEventsRequest, Fail> {
    let q: AlcEventsQuery = parse_query(query)?;
    let date = |name: &str, s: &str| {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| bad_request(format!("{name} は YYYY-MM-DD で指定してください")))
    };
    let (from, to) = (date("from", &q.from)?, date("to", &q.to)?);
    if from > to {
        return Err(bad_request("from は to 以前にしてください"));
    }
    if (to - from).num_days() >= ALC_EVENTS_MAX_DAYS {
        return Err(bad_request(format!(
            "期間は {ALC_EVENTS_MAX_DAYS} 日までにしてください"
        )));
    }
    let at = |d: NaiveDate| format!("{} 00:00:00", d.format("%Y-%m-%d"));
    Ok(AlcEventsRequest {
        driver: q.driver,
        from: at(from),
        to: at(to + chrono::Duration::days(1)),
    })
}

/// 502 の本文の頭 (root の `KintaiPushError::Db` の `kintai push db failed: …`)。後ろは kit の `kind` だけ。
pub const DB_WHAT: &str = "kintai push db";

/// stale-months の 502 の本文の頭 (root の `kintai.day_summaries stale read failed: …`)。
pub const STALE_DB_WHAT: &str = "kintai.day_summaries stale read";

/// 種別の名前として残してよい語 (`connect:timeout`・`server:1146`・`42P01`・`2026-13` 等)。空白・`.`・`/`・引用符を
/// 含むもの (上流の本文・serde の文言・宛先) は落とす。
fn is_kind_token(s: &str) -> bool {
    let ok = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':');
    !s.is_empty() && s.len() <= 64 && s.chars().all(ok)
}

/// 失敗の本文を種別だけに削る。最初の `": "` より前 (固定の頭) は残し、後ろは種別の語のときだけ残す。
/// 上流 (alc の RPC) の本文の抜粋や serde の文言を、502 の本文と window の `fold_error` に出さないため。
pub fn kind_only(body: &str) -> String {
    match body.split_once(": ") {
        Some((head, tail)) if is_kind_token(tail) => format!("{head}: {tail}"),
        Some((head, _)) => head.to_string(),
        None => body.to_string(),
    }
}

/// fold の読み (alc の RPC・社内 MariaDB) の失敗。root の `KintaiPushError::Read` と同じく **502** で
/// `kintai events read failed: …` (資格情報や binding が無いときも 502。root の `map_push_err` と同じ)。
pub fn read_failed(f: &Fail) -> Fail {
    Fail::new(
        502,
        format!("kintai events read failed: {}", kind_only(&f.body)),
    )
}

/// 段取りの中で月・窓が読めなかった (root の `KintaiPushError::NotConfigured` と同じく 503・文言そのまま)。
/// 口が先に月を検査するので通常は起きない。
pub fn not_configured(msg: &str) -> Fail {
    Fail::new(503, msg)
}

/// `KosokuParams` は既定値 (root の Cloud Run 版も設定ファイルに `[kosoku]` 節が無く既定値で動いている)。
pub fn params() -> &'static KosokuParams {
    static PARAMS: OnceLock<KosokuParams> = OnceLock::new();
    PARAMS.get_or_init(KosokuParams::default)
}

/// 勤怠の版 (`logic_version`)。root の `kintai_fold::logic_version(&params)` と同じ値になる。
pub fn logic_version() -> String {
    kintai_fold::logic_version(params())
}

/// fold の流れに渡す設定と時計。`code_sha` は記録用 (Worker の `KINTAI_WORKER_OUTPUT_SHA`)。
pub fn fold_ctx<'a>(code_sha: &'a str, clock: &'a (dyn FoldClock + Sync)) -> FoldCtx<'a> {
    FoldCtx {
        params: params(),
        code_sha,
        clock,
    }
}

/// UNIX 時刻 (ミリ秒) を JST の時刻に (Worker の時計 `Date.now()` から `FoldClock::now_jst` を作る)。
pub fn jst_from_ms(now_ms: u64) -> DateTime<FixedOffset> {
    let jst = FixedOffset::east_opt(9 * 3600).expect("JST offset is in range");
    DateTime::from_timestamp_millis(now_ms as i64)
        .unwrap_or_default()
        .with_timezone(&jst)
}
