//! 口が共有する部品 — 失敗の形・Query の読み方・月と乗務員CD の検査・月の境界・テナントの解決。
//!
//! 元 (root の src/) では口ごとに写しを持っていた (`read_tenant_of` が 3 つ + `tenant_of` が 1 つ、
//! `month_date_bounds` と `month_bounds`)。ここではそれぞれ **1 つだけ**置き、5 本がこれを使う。

use chrono::{DateTime, FixedOffset, NaiveDate, TimeZone};
use postgres_types::{ToSql, Type};
use serde::de::DeserializeOwned;
use uuid::Uuid;

/// 読み先のテナントを固定する var (wrangler.toml の `[vars]`)。**`X-Tenant-ID` は読まない。**
pub const TENANT_VAR: &str = "KINTAI_TENANT_ID";

/// Supabase へ繋ぐ Hyperdrive の binding (wrangler.toml のトップレベルにだけ置く)。
pub const HYPERDRIVE_BINDING: &str = "KINTAI_HYPERDRIVE";

/// JST の UTC からのずれ (秒)。
const JST_OFFSET_SECONDS: i32 = 9 * 3600;

/// `query_typed` に渡す引数 1 つ (値と `$n` の型)。
pub type Param<'a> = (&'a (dyn ToSql + Sync), Type);

/// 失敗の応答。本文は平文 (元の axum の `(StatusCode, String)` と同じ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fail {
    pub status: u16,
    pub body: String,
}

impl Fail {
    pub fn new(status: u16, body: impl Into<String>) -> Self {
        Self {
            status,
            body: body.into(),
        }
    }
}

/// 400 (入力不正)。
pub fn bad_request(msg: impl Into<String>) -> Fail {
    Fail::new(400, msg)
}

/// 503 (`KINTAI_HYPERDRIVE` の binding が無い)。元の「[kintai_push] が無効です」に当たる。
pub fn no_db() -> Fail {
    Fail::new(503, "[KINTAI_HYPERDRIVE] が無効です (読み先がありません)")
}

/// 502 (DB の失敗)。`what` は口ごとの元の文言 (`kintai.day_summaries read` 等)、`kind` は
/// alc-worker-db の `kind` (SQLSTATE か固定の語) か接続の段の label。DB の message は載せない。
pub fn db_fail(what: &str, kind: &str) -> Fail {
    Fail::new(502, format!("{what} failed: {kind}"))
}

/// 503 (社内 MariaDB の資格情報 `KINTAI_MARIADB` が無い・読めない)。元の `map_repo_err` の `NotConfigured` と同じ文言。
pub fn mariadb_unconfigured() -> Fail {
    Fail::new(503, "MariaDB 接続設定が未設定")
}

/// 502 (社内 MariaDB までの途中・クエリ・行の読み取りの失敗)。元の `MariaDB query failed: ` の頭 + 種別だけ
/// (`connect:timeout`・`query:server:1146`・`rows:int` 等)。DB の message・接続先は載せない。
pub fn mariadb_fail(kind: &str) -> Fail {
    Fail::new(502, format!("MariaDB query failed: {kind}"))
}

/// クエリ文字列を `T` に読む。axum 0.8 の `Query` と同じ部品・同じ拒否文言
/// (`Failed to deserialize query string: <path>: <理由>`、400)。
pub fn parse_query<T: DeserializeOwned>(query: &str) -> Result<T, Fail> {
    let de = serde_urlencoded::Deserializer::new(form_urlencoded::parse(query.as_bytes()));
    serde_path_to_error::deserialize(de)
        .map_err(|e| bad_request(format!("Failed to deserialize query string: {e}")))
}

/// `YYYY-MM` (年 4 桁・月 01-12) か。写しではなく共有 crate (`kintai_kosoku::window`) のもの — root の
/// `routes/kintai.rs` も同じものを使う。
pub use kintai_kosoku::window::is_valid_month;

/// 乗務員CD のパース。**数字のみ**を受ける (空・非数字・負値・桁溢れは None)。
/// root の `routes/kintai.rs` の `parse_driver` の写し。
pub fn parse_driver(driver: &str) -> Option<u64> {
    if driver.is_empty() || !driver.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    driver.parse::<u64>().ok()
}

/// 対象月の `[月初, 翌月初)` を暦日の対で返す (`DATE` の境界にはそのまま、`TIMESTAMPTZ` の境界には
/// [`jst_midnight`] を通して使う)。`is_valid_month` が通した文字列では常に `Some`。
pub fn month_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next))
}

/// JST の暦日の 00:00 (root の `kintai_push::jst_day_bounds(date).0` と同じ値)。
pub fn jst_midnight(date: NaiveDate) -> DateTime<FixedOffset> {
    let jst = FixedOffset::east_opt(JST_OFFSET_SECONDS).expect("JST offset is in range");
    let midnight = date.and_hms_opt(0, 0, 0).expect("midnight exists");
    jst.from_local_datetime(&midnight)
        .single()
        .expect("JST has no DST gap")
}

/// 読み先のテナント (`KINTAI_TENANT_ID` の設定 pin)。**`X-Tenant-ID` は読まない。**
///
/// 無い・空・UUID として読めない・nil UUID は 503。nil で引くと 0 件が返るだけで、
/// 「設定が無い」と「その月の勤務が無い」を呼び出し側が区別できない (元の Refs #205 の 23)。
pub fn tenant_of(raw: Option<&str>) -> Result<Uuid, Fail> {
    match raw.map(Uuid::parse_str) {
        Some(Ok(t)) if !t.is_nil() => Ok(t),
        _ => Err(Fail::new(
            503,
            "読み先のテナントが決まりません (KINTAI_TENANT_ID を設定してください)",
        )),
    }
}

/// DB に繋ぐ**前**の検査。元の handler と同じ順 (binding が無ければ 503 → テナントが決まらなければ 503) で、
/// どちらかが欠ければ呼び手は connect しない (`KINTAI_TENANT_ID` が空の初期状態で、接続の失敗 (502) が
/// 設定欠落 (503) を隠さないように)。`has_binding` は `KINTAI_HYPERDRIVE` が env に在るか。
pub fn preflight(has_binding: bool, tenant_raw: Option<&str>) -> Result<Uuid, Fail> {
    if !has_binding {
        return Err(no_db());
    }
    tenant_of(tenant_raw)
}

/// 書き込みの口の [`preflight`]。binding が無いときの文言だけ違う (元の `[kintai_push] が無効です (書き先がありません)`)。
pub fn write_preflight(has_binding: bool, tenant_raw: Option<&str>) -> Result<Uuid, Fail> {
    if !has_binding {
        return Err(no_write_db());
    }
    tenant_of(tenant_raw)
}

/// 503 (`KINTAI_HYPERDRIVE` の binding が無い、書き込みの口)。
pub fn no_write_db() -> Fail {
    Fail::new(503, "[KINTAI_HYPERDRIVE] が無効です (書き先がありません)")
}

/// axum の `Json` が本文を読む上限 (`DefaultBodyLimit` の既定 2MB)。超えたら元と同じく 413。
pub const MAX_JSON_BODY_BYTES: usize = 2_097_152;

/// 本文を `T` に読む。axum 0.8 の `Json` と同じ順・同じ status・同じ文言:
/// Content-Type が `application/json` (か `+json`) でない → 415 / 2MB 超 → 413 /
/// JSON として読めない・後ろに余計な文字 → 400 / 型に合わない → 422 (本文の頭 + `: ` + serde の理由)。
pub fn parse_json<T: DeserializeOwned>(content_type: Option<&str>, body: &[u8]) -> Result<T, Fail> {
    if !json_content_type(content_type) {
        return Err(Fail::new(415, UNSUPPORTED_JSON));
    }
    if body.len() > MAX_JSON_BODY_BYTES {
        return Err(Fail::new(413, TOO_LARGE));
    }
    let mut de = serde_json::Deserializer::from_slice(body);
    let value: T = serde_path_to_error::deserialize(&mut de).map_err(json_fail)?;
    de.end()
        .map_err(|e| Fail::new(400, format!("{JSON_SYNTAX}: {e}")))?;
    Ok(value)
}

const UNSUPPORTED_JSON: &str = "Expected request with `Content-Type: application/json`";
const TOO_LARGE: &str = "Failed to buffer the request body: length limit exceeded";
const JSON_SYNTAX: &str = "Failed to parse the request body as JSON";
const JSON_DATA: &str = "Failed to deserialize the JSON body into the target type";

/// serde の失敗を axum と同じく分ける (型に合わない = 422、それ以外 = 400)。
fn json_fail(e: serde_path_to_error::Error<serde_json::Error>) -> Fail {
    match e.inner().classify() {
        serde_json::error::Category::Data => Fail::new(422, format!("{JSON_DATA}: {e}")),
        _ => Fail::new(400, format!("{JSON_SYNTAX}: {e}")),
    }
}

/// axum の `json_content_type` と同じ判定 (mime として読めて `application/json` か `application/*+json`)。
fn json_content_type(content_type: Option<&str>) -> bool {
    let Some(mime) = content_type.and_then(|c| c.parse::<mime::Mime>().ok()) else {
        return false;
    };
    let json = mime.subtype() == "json" || mime.suffix().is_some_and(|s| s == "json");
    mime.type_() == "application" && json
}
