//! 勤怠 Worker が社内 CakePHP (`yhonda-ohishi/nginx`) を Workers VPC の HTTP で中継する 3 本の口の純粋部分
//! (Refs ohishi-exp/rust-ichibanboshi#322): `GET /api/kintai/daily`・`GET /api/kintai/pdf-json`・`POST /api/dtako/autoload`。
//!
//! - URL・multipart・応答の型・autoload の段取りはオンプレ版と共有 ([`crate::cakephp`]・[`crate::dtako_autoload`])
//! - daily・pdf-json の検査・`map_cakephp_err`・`with_source_meta` は root の `src/routes/kintai.rs` の**写し**
//!   (あちらは勤怠の版の glob の中で動かせないため。対応表は `workers/kintai/README.md`。**撤去までは片方を直したらもう片方も直す**)
//! - **daily はキャッシュを持たない** (ユーザー決定 2026-10-10)。毎回 CakePHP を叩くので `source` は常に `live`。`refresh=1` は受けて無視する
//! - 503 の「CakePHP base_url が未設定」は Worker では「VPC の binding が無い」([`NOT_CONFIGURED`])
//! - autoload の待ちの打ち切り (`timeout`) は 502 だが「失敗」ではなく「不明」([`AUTOLOAD_TIMEOUT`])。取り込みは応答より前に走る

use serde::Deserialize;

use crate::cakephp::{parse_json, CakephpError, TimecardDailyResponse};
use crate::common::{bad_request, is_valid_month, mariadb_fail, parse_driver, parse_query, Fail};
use crate::dtako_autoload::{self, AutoloadQuery, AutoloadRequest, MaterialQuery, MAX_ZIP_BYTES};
use kintai_mysql::bind::{expand, Digits, Value};
use kintai_mysql::response::Row;

/// CakePHP への口 (Workers VPC の VPC Service、HTTP)。wrangler.toml のトップレベルにだけ置く。
pub const CAKEPHP_VPC_BINDING: &str = "KINTAI_CAKEPHP_VPC";

/// fetch の URL の origin。**名目** (宛先の host:port は VPC Service の側で決まる)。社内のホスト名は書かない。
pub const VPC_ORIGIN: &str = "http://kintai-cakephp.internal";

/// binding が無いときの 503 の本文 (オンプレ版の「CakePHP base_url が未設定」に当たる)。
pub const NOT_CONFIGURED: &str = "CakePHP の VPC binding (KINTAI_CAKEPHP_VPC) が無い";

/// daily・pdf-json と ③ (resetby-unko-no) の待ちの上限 (秒)。オンプレ版の `[cakephp] timeout_secs` の既定と同じ。
/// autoload (②) は `cakephp::DTAKO_AUTOLOAD_TIMEOUT_SECS` (120 秒)。
pub const TIMEOUT_SECS: u64 = 30;

/// 待ちを打ち切ったときの `CakephpError::RequestFailed` の中身 (種別の名前だけ。宛先は載せない)。
pub const TIMEOUT: &str = "timeout";

/// autoload (②) の待ちを打ち切ったときの 502 の本文。オンプレ版は同じ場面で `nginx への接続に失敗: <reqwest の文言>`。
pub const AUTOLOAD_TIMEOUT: &str = "nginx の応答待ちを打ち切りました (timeout)。取り込みは応答より前に走るので、取り込まれたかどうかは不明です (失敗とは限らない。同じ zip をすぐ送り直さず、データで確かめる)";

/// month が不正なときの 400 の本文 (root の `routes/kintai.rs` と同じ文言)。
pub const MONTH_INVALID: &str = "month は YYYY-MM で指定してください";

/// driver が不正なときの 400 の本文 (同上)。
pub const DRIVER_INVALID: &str = "driver は乗務員CD (数字) で指定してください";

/// 本文が上限を超えたときの 413 の本文 (axum の `DefaultBodyLimit` と同じ文言)。
pub const BODY_TOO_LARGE: &str = "Failed to buffer the request body: length limit exceeded";

/// CakePHP を読む 2 本。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CakephpRead {
    Daily,
    PdfJson,
}

impl CakephpRead {
    /// 口の path。オンプレ版と同じ。
    pub fn from_path(path: &str) -> Option<Self> {
        match path {
            "/api/kintai/daily" => Some(Self::Daily),
            "/api/kintai/pdf-json" => Some(Self::PdfJson),
            _ => None,
        }
    }

    /// ログに出す名前。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Daily => "daily",
            Self::PdfJson => "pdf-json",
        }
    }
}

/// `?month=YYYY-MM&refresh=1` (root の `routes/kintai.rs` の `DailyQuery` の写し)。`refresh` は受けて使わない。
#[derive(Debug, Deserialize)]
pub struct DailyQuery {
    pub month: Option<String>,
    #[serde(default)]
    pub refresh: Option<String>,
}

/// `?month=YYYY-MM&driver=1051` (root の `routes/kintai.rs` の `EventsQuery` の写し)。
#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
    pub view: Option<String>,
}

/// 検査済みの読みの要求。`path` は CakePHP の相対パス。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadRequest {
    pub read: CakephpRead,
    pub path: String,
}

impl CakephpRead {
    /// 検査 (Query → month → driver の順。オンプレ版と同じ)。
    pub fn parse(self, query: &str) -> Result<ReadRequest, Fail> {
        let path = match self {
            Self::Daily => {
                let q: DailyQuery = parse_query(query)?;
                crate::cakephp::daily_json_path(&valid_month(q.month)?)
            }
            Self::PdfJson => {
                let q: EventsQuery = parse_query(query)?;
                let month = valid_month(q.month)?;
                let driver = match q.driver {
                    None => None,
                    Some(raw) => {
                        Some(parse_driver(&raw).ok_or_else(|| bad_request(DRIVER_INVALID))?)
                    }
                };
                crate::cakephp::pdf_json_path(&month, driver)
            }
        };
        Ok(ReadRequest { read: self, path })
    }
}

fn valid_month(month: Option<String>) -> Result<String, Fail> {
    let month = month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request(MONTH_INVALID));
    }
    Ok(month)
}

impl ReadRequest {
    /// CakePHP の応答 (2xx の本文) → この口の応答の本文。daily は `source: "live"` と `synced_at` を足す
    /// (オンプレ版の `with_source_meta` と同じ形。`cache` の分岐は無い)。`now_ms` は UNIX ミリ秒。
    pub fn respond(&self, body: &[u8], now_ms: u64) -> Result<Vec<u8>, Fail> {
        match self.read {
            CakephpRead::Daily => {
                let resp: TimecardDailyResponse = parse_json(body).map_err(map_cakephp_err)?;
                let resp = with_source_meta(resp, "live", &synced_at(now_ms));
                Ok(serde_json::to_vec(&resp).unwrap_or_default())
            }
            CakephpRead::PdfJson => {
                let resp: serde_json::Value = parse_json(body).map_err(map_cakephp_err)?;
                Ok(serde_json::to_vec(&resp).unwrap_or_default())
            }
        }
    }
}

/// 応答へ出どころメタを足す (root の `routes/kintai.rs` の `with_source_meta` の写し)。
pub fn with_source_meta(
    mut resp: TimecardDailyResponse,
    source: &str,
    synced_at: &str,
) -> TimecardDailyResponse {
    resp.extra
        .insert("source".to_string(), serde_json::Value::from(source));
    resp.extra
        .insert("synced_at".to_string(), serde_json::Value::from(synced_at));
    resp
}

/// `synced_at` (オンプレ版の `chrono::Utc::now().to_rfc3339()` と同じ書式。精度はミリ秒)。
pub fn synced_at(now_ms: u64) -> String {
    let ms = i64::try_from(now_ms).unwrap_or(i64::MAX);
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339()
}

/// CakePHP のエラーを status と本文へ写す (root の `routes/kintai.rs` の `map_cakephp_err` の写し。503 の文言だけ
/// [`NOT_CONFIGURED`] に読み替える)。
pub fn map_cakephp_err(e: CakephpError) -> Fail {
    match e {
        CakephpError::NotConfigured => Fail::new(503, NOT_CONFIGURED),
        CakephpError::RequestFailed(m) => Fail::new(502, format!("CakePHP fetch failed: {m}")),
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => Fail::new(502, format!("CakePHP returned {status}: {body_excerpt}")),
        CakephpError::JsonError(m) => Fail::new(502, format!("CakePHP response parse failed: {m}")),
    }
}

/// autoload の検査 (Query (400) → 本文の上限 (413) → `unko_no` → 本文が空 (400))。オンプレ版の axum の extractor の順と同じ。
pub fn parse_autoload(query: &str, body_len: usize) -> Result<AutoloadRequest, Fail> {
    let q: AutoloadQuery = parse_query(query)?;
    if body_len > MAX_ZIP_BYTES {
        return Err(Fail::new(413, BODY_TOO_LARGE));
    }
    dtako_autoload::parse(q, body_len).map_err(bad_request)
}

/// autoload (②) の送信の失敗を status と本文へ。待ちの打ち切りは [`AUTOLOAD_TIMEOUT`]、それ以外はオンプレ版と同じ文言
/// (503 だけ [`NOT_CONFIGURED`])。
pub fn autoload_fail(e: CakephpError) -> Fail {
    if matches!(&e, CakephpError::RequestFailed(m) if m == TIMEOUT) {
        return Fail::new(502, AUTOLOAD_TIMEOUT);
    }
    let (status, body) = dtako_autoload::map_err(e, NOT_CONFIGURED);
    Fail::new(status, body)
}

/// ③ の材料を数える SQL (名前付き引数を展開済み)。運行NO の 2 パターンは数字だけの文字列として埋める。
pub fn material_sql(q: &MaterialQuery) -> Result<String, String> {
    let digits = |s: &str| Digits::new(s).map(Value::Digits).ok_or("bind_digits");
    let params = [
        ("from", Value::DateTime(q.from)),
        ("to", Value::DateTime(q.to)),
        ("v1", digits(&q.v1).map_err(material_fail)?),
        ("v2", digits(&q.v2).map_err(material_fail)?),
    ];
    expand(dtako_autoload::RESET_MATERIAL_SQL, &params).map_err(|e| material_fail(e.kind()))
}

/// 材料の件数 (1 行 1 列の整数)。行が無ければ 0 (オンプレ版の `exec_first` の `None` と同じ)。
pub fn material_count(rows: &[Row]) -> Result<i64, String> {
    let Some(row) = rows.first() else {
        return Ok(0);
    };
    let cell = row.first().and_then(|c| c.as_deref());
    let text = cell.and_then(|b| std::str::from_utf8(b).ok());
    text.and_then(|s| s.parse::<i64>().ok())
        .ok_or_else(|| material_fail("rows:int"))
}

/// 材料を数えられなかったときの文言 (オンプレ版の `KintaiRepoError::QueryFailed` の Display と同じ頭)。
pub fn material_fail(kind: &str) -> String {
    mariadb_fail(kind).body
}
