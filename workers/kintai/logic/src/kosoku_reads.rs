//! 社内 MariaDB を読む残りの 4 本 (`GET /api/kintai/{kosoku-daily,version,timecard/drivers,timecard/events}`) の
//! 検査・SQL の引数・応答。
//!
//! 元はオンプレ版 (root の `src/routes/kintai.rs` の `kosoku_daily`・`src/routes/kintai_version.rs` の `version`・
//! `src/routes/kintai_timecard.rs` の `drivers`・`window_events`)。応答を組む部分 (整形・view・乗務員ごとの組み立て・
//! etag の畳み方・ページの切り方・月の検査) は共有 crate `kintai-kosoku` をオンプレ版と同じものを使う (写さない)。
//! ここは handler の部分 (入力の検査の順・400 の文言・どの SQL をどの順に読むか・失敗の写し方) だけで、
//! **元の handler を直したらここも直す** (対応表は `workers/kintai/README.md`)。DB との往復は worker crate が持つ。
//!
//! kosoku-daily と version は 1 接続で SQL を何本か流す (遡り起点の 2 本 → 本体)。窓は起点で決まるので、
//! 本体の SQL は起点を読んでから作る ([`anchors_from`] → [`DailyRequest::events_sql`] / [`version_sql`])。

use std::collections::BTreeMap;
use std::io::Write;

pub use kintai_kosoku::anchors::HeadAnchors;
use kintai_kosoku::kintai_timecard::{
    drivers_json, page_drivers, parse_months, window_bounds, window_events_json,
    DEFAULT_MAX_DRIVERS,
};
use kintai_kosoku::kintai_version::{version_etag, version_ranges};
use kintai_kosoku::kosoku::{split_ferry_by_driver, KosokuParams};
use kintai_kosoku::kosoku_daily::{build_driver, for_each_driver, parse_view, ResponseView};
use kintai_kosoku::sql::{
    ALL_EVENTS_SQL, EVENTS_SQL, FERRY_SQL, HEAD_PUNCHES_SQL, HEAD_RUN_ENDS_SQL,
    TIMECARD_DRIVERS_SQL, TIMECARD_WINDOW_SQL, VERSION_SQL,
};
use kintai_kosoku::window::{
    exact_month_range, month_head_anchors, month_range, parse_dt, read_window,
};
use kintai_mysql::bind::{expand, Value as Bind};
use kintai_mysql::response::Row;
use serde::Deserialize;
use serde_json::Value;

use crate::common::{bad_request, is_valid_month, mariadb_fail, parse_driver, parse_query, Fail};
use crate::mariadb_reads::{DRIVER_MSG, MONTH_MSG};
use crate::mariadb_rows::{
    all_event_row, event_row, ferry_row, head_punch_row, head_run_end_row, rows_to_json,
    timecard_driver_row, version_row,
};

/// timecard の 2 本の 502 の頭 (元の `map_diff_err` が `KintaiDiffError::Read` を `kintai events read failed: <元の失敗>` で返す)。
pub const TIMECARD_READ_FAILED: &str = "kintai events read failed: ";

/// timecard/events の窓が作れない (元の文言。`parse_months` が通した月では起きない)。
pub const MONTHS_INVALID: &str = "months が不正です";

/// kosoku-daily の全乗務員版の応答の上限 (バイト)。実測の最大 (1 か月・full) は約 2.2MB。超えたら途中まで書いた応答を
/// 返さずに 503 ([`TOO_LARGE`]) で止める。
pub const MAX_ALL_DRIVERS_BYTES: usize = 32 * 1024 * 1024;

/// [`MAX_ALL_DRIVERS_BYTES`] を超えたときの 503 の本文 (固定)。
pub const TOO_LARGE: &str = "kosoku-daily の応答が上限を超えました";

/// 口の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KosokuRead {
    KosokuDaily,
    Version,
    TimecardDrivers,
    TimecardEvents,
}

impl KosokuRead {
    /// 口の path。元 (オンプレ版) と同じ。
    pub fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/kintai/kosoku-daily" => KosokuRead::KosokuDaily,
            "/api/kintai/version" => KosokuRead::Version,
            "/api/kintai/timecard/drivers" => KosokuRead::TimecardDrivers,
            "/api/kintai/timecard/events" => KosokuRead::TimecardEvents,
            _ => return None,
        })
    }

    /// ログに出す名前。
    pub fn as_str(self) -> &'static str {
        match self {
            KosokuRead::KosokuDaily => "kosoku-daily",
            KosokuRead::Version => "version",
            KosokuRead::TimecardDrivers => "timecard-drivers",
            KosokuRead::TimecardEvents => "timecard-events",
        }
    }
}

/// 窓の端 (`YYYY-MM-DD HH:MM:SS`) を日時の引数に。
fn at(s: &str) -> Bind {
    Bind::DateTime(parse_dt(s).expect("window bounds are datetimes"))
}

/// 値を埋め込んだ SQL (COM_QUERY に載せる文)。名前の食い違いは 502 (`bind_*`)。
fn sql_text(sql: &str, binds: &[(&'static str, Bind)]) -> Result<String, Fail> {
    expand(sql, binds).map_err(|e| mariadb_fail(e.kind()))
}

/// 月が窓にならない (元の `KintaiRepoError::QueryFailed("bad month: …")` = 502。検査済みの月では起きない)。
fn bad_month() -> Fail {
    mariadb_fail("bad_month")
}

// ── 遡り起点 (kosoku-daily・version が共有) ──

/// 遡り起点の 2 本 (`HEAD_RUN_ENDS_SQL` → `HEAD_PUNCHES_SQL`)。窓は `month_range` (元の `month_anchors` と同じ)。
pub fn head_sqls(month: &str) -> Result<[String; 2], Fail> {
    let (from, to) = month_range(month).ok_or_else(bad_month)?;
    let binds = [("from", at(&from)), ("to", at(&to))];
    Ok([
        sql_text(HEAD_RUN_ENDS_SQL, &binds)?,
        sql_text(HEAD_PUNCHES_SQL, &binds)?,
    ])
}

/// 2 本の結果から乗務員ごとの遡り起点を決める (元の `mariadb_month_head_anchors`)。1 行でも読めなければ 502。
pub fn anchors_from(month: &str, runs: &[Row], punches: &[Row]) -> Result<HeadAnchors, Fail> {
    let (month_start, _) = month_range(month).ok_or_else(bad_month)?;
    let runs = rows_to_json(runs, head_run_end_row)?;
    let punches = rows_to_json(punches, head_punch_row)?;
    Ok(month_head_anchors(&month_start, &runs, &punches))
}

// ── kosoku-daily ──

/// 元の `EventsQuery` と同じ 3 つの欄。
#[derive(Deserialize)]
struct DailyQuery {
    month: Option<String>,
    driver: Option<String>,
    view: Option<String>,
}

/// 検査済みの kosoku-daily の要求。`driver` 省略は全乗務員。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DailyRequest {
    pub month: String,
    pub driver: Option<u64>,
    pub view: ResponseView,
}

/// kosoku-daily のクエリ文字列を検査する。順と 400 の文言は元の handler と同じ:
/// Query として読めること → `is_valid_month` → `driver` (省略は全乗務員、`driver=` の空・数字でないは 400)。
pub fn parse_kosoku_daily(query: &str) -> Result<DailyRequest, Fail> {
    let q: DailyQuery = parse_query(query)?;
    let month = q.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request(MONTH_MSG));
    }
    let driver = match q.driver {
        None => None,
        Some(raw) => Some(parse_driver(&raw).ok_or_else(|| bad_request(DRIVER_MSG))?),
    };
    Ok(DailyRequest {
        month,
        driver,
        view: parse_view(q.view.as_deref()),
    })
}

/// フェリーの行。読めない行が 1 つでもあれば空 (元は `exec` が落ちて warn し、控除 0 で続ける)。
pub fn ferry_or_empty(rows: &[Row]) -> Vec<Value> {
    rows_to_json(rows, ferry_row).unwrap_or_default()
}

impl DailyRequest {
    /// 本体の生イベントの SQL。窓は遡り起点まで下げたもの (単一版はその乗務員の起点、全員版は全員の最小)。
    /// 単一版は `EVENTS_SQL` (7 列)、全員版は `ALL_EVENTS_SQL` (5 列) — 元の `fetch_events_between` /
    /// `fetch_all_events_between` と同じ。
    pub fn events_sql(&self, anchors: &HeadAnchors) -> Result<String, Fail> {
        let (from, to) = read_window(&self.month, anchors, self.driver).ok_or_else(bad_month)?;
        let mut binds = vec![("from", at(&from)), ("to", at(&to))];
        match self.driver {
            Some(d) => {
                binds.push(("driver", Bind::UInt(d)));
                sql_text(EVENTS_SQL, &binds)
            }
            None => sql_text(ALL_EVENTS_SQL, &binds),
        }
    }

    /// フェリーの SQL。窓は対象月ちょうど (`exact_month_range`)、`driver` 省略は NULL (= 全乗務員)。
    pub fn ferry_sql(&self) -> Result<String, Fail> {
        let (from, to) = exact_month_range(&self.month).ok_or_else(bad_month)?;
        let driver = self.driver.map_or(Bind::Null, Bind::UInt);
        sql_text(
            FERRY_SQL,
            &[("from", at(&from)), ("to", at(&to)), ("driver", driver)],
        )
    }

    /// 単一乗務員版の応答 (`driver` ありのとき)。`events` は `EVENTS_SQL` の行、`ferry` は [`ferry_or_empty`] の結果。
    /// 返すのは応答と勤務の数 (ログ用)。
    pub fn respond_single(
        &self,
        driver: u64,
        events: &[Row],
        ferry: &[Value],
    ) -> Result<(Value, usize), Fail> {
        let rows = rows_to_json(events, event_row)?;
        let built = build_driver(
            rows,
            ferry,
            &self.month,
            &KosokuParams::default(),
            self.view,
        );
        let days = built.days.len();
        Ok((built.into_single(&self.month, driver, self.view), days))
    }

    /// 全乗務員版の応答のバイト列 (`driver` 省略のとき)。返すのは本文と乗務員の数 (ログ用)。
    ///
    /// **応答全体の JSON の木を持たない**: 結果セットの生の行を乗務員ごとに束ね、1 人ぶんずつ JSON にして
    /// `for_each_driver` に渡し、1 人ぶんずつ直列化して書く。並び (乗務員CD 昇順)・落とす乗務員 (CD=0、勤務も打刻も
    /// 無い)・形は `kintai_kosoku::kosoku_daily::write_all_drivers` (全行を一度に渡す版) と同じバイト列になる
    /// (テストで固定)。行は 1 つでも読めなければ 502 (元の `exec` と同じ)、本文が `max_bytes` を超えたら 503 ([`TOO_LARGE`])。
    pub fn write_all(
        &self,
        anchors: &HeadAnchors,
        events: Vec<Row>,
        ferry: Vec<Value>,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, usize), Fail> {
        // 乗務員で束ねる。`driver_id` が無い・負の行は全員版の split と同じく落とすが、読めるかは全行で確かめる
        let mut groups: BTreeMap<u64, Vec<Row>> = BTreeMap::new();
        for row in events {
            if let Some(d) = all_event_row(&row)?["driver_id"].as_u64() {
                groups.entry(d).or_default().push(row);
            }
        }
        let mut ferry_by_driver = split_ferry_by_driver(ferry);
        let params = KosokuParams::default();
        let mut out = Capped::new(max_bytes);
        let too_large = |_| Fail::new(503, TOO_LARGE);
        out.write_all(b"{\"drivers\":[").map_err(too_large)?;
        let mut written = Ok(());
        let mut n = 0usize;
        for (driver, raw) in groups {
            let rows = rows_to_json(&raw, all_event_row)?;
            drop(raw);
            let ferry = ferry_by_driver.remove(&driver).unwrap_or_default();
            for_each_driver(
                rows,
                ferry,
                &self.month,
                anchors,
                &params,
                self.view,
                |entry| {
                    if written.is_ok() {
                        written = write_entry(&mut out, &entry, n > 0);
                        n += 1;
                    }
                },
            );
        }
        written.map_err(too_large)?;
        out.write_all(b"],\"month\":").map_err(too_large)?;
        serde_json::to_writer(&mut out, &self.month).map_err(|_| Fail::new(503, TOO_LARGE))?;
        out.write_all(b"}").map_err(too_large)?;
        out.flush().map_err(too_large)?;
        Ok((out.buf, n))
    }
}

/// 2 人目からは前に `,` を付けて 1 要素を書く。
fn write_entry(out: &mut Capped, entry: &Value, comma: bool) -> std::io::Result<()> {
    if comma {
        out.write_all(b",")?;
    }
    serde_json::to_writer(&mut *out, entry).map_err(std::io::Error::from)
}

/// `max` バイトを超える書き込みを断る書き先。
struct Capped {
    buf: Vec<u8>,
    max: usize,
}

impl Capped {
    fn new(max: usize) -> Self {
        Self {
            buf: Vec::new(),
            max,
        }
    }
}

impl Write for Capped {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.buf.len() + b.len() > self.max {
            return Err(std::io::Error::other("too_large"));
        }
        self.buf.extend_from_slice(b);
        Ok(b.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ── version ──

/// 元の `VersionQuery` と同じ 1 つの欄。
#[derive(Deserialize)]
struct VersionQuery {
    month: Option<String>,
}

/// version のクエリ文字列を検査する (Query として読めること → `is_valid_month`)。返すのは月。
pub fn parse_version(query: &str) -> Result<String, Fail> {
    let q: VersionQuery = parse_query(query)?;
    let month = q.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request(MONTH_MSG));
    }
    Ok(month)
}

/// `VERSION_SQL` (範囲は遡り起点で決まる `version_ranges`)。
pub fn version_sql(month: &str, anchors: &HeadAnchors) -> Result<String, Fail> {
    let r = version_ranges(month, anchors).ok_or_else(bad_month)?;
    sql_text(
        VERSION_SQL,
        &[
            ("from", at(&r.from)),
            ("to", at(&r.to)),
            ("mfrom", at(&r.mfrom)),
            ("mto", at(&r.mto)),
            ("efrom", at(&r.efrom)),
        ],
    )
}

/// version の応答 `{month, etag}` と `ETag` ヘッダの値 (同じ文字列)。`build` は勤怠 Worker の版
/// (オンプレ版の `KINTAI_OUTPUT_SHA` とは別の値)。`KosokuParams` は既定値 (オンプレ版も設定ファイルに `[kosoku]` 節が無い)。
pub fn version_respond(month: &str, rows: &[Row], build: &str) -> Result<(Value, String), Fail> {
    let markers = rows_to_json(rows, version_row)?;
    let etag = version_etag(month, build, &KosokuParams::default(), &markers);
    Ok((serde_json::json!({ "month": month, "etag": etag }), etag))
}

// ── timecard/drivers・timecard/events ──

/// timecard の 2 本の 502。元は読みの失敗を全部 `kintai events read failed: <元の失敗>` の 502 にする
/// (資格情報が無いときも 503 ではなく 502)。
pub fn timecard_fail(inner: Fail) -> Fail {
    Fail::new(502, format!("{TIMECARD_READ_FAILED}{}", inner.body))
}

/// 元の `DriversQuery` と同じ 3 つの欄。`max_drivers` は元が `usize` (64 bit) なので `u64` で受けて丸める
/// (wasm32 の `usize` は 32 bit で、そのまま受けると元が通す値を 400 にしてしまう)。
#[derive(Deserialize)]
struct DriversQuery {
    month: Option<String>,
    after_driver_cd: Option<u64>,
    max_drivers: Option<u64>,
}

/// 検査済みの timecard/drivers の要求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriversRequest {
    pub month: String,
    pub after: Option<u64>,
    pub max: usize,
}

/// timecard/drivers のクエリ文字列を検査する (Query として読めること → `is_valid_month`)。
pub fn parse_timecard_drivers(query: &str) -> Result<DriversRequest, Fail> {
    let q: DriversQuery = parse_query(query)?;
    let month = q.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request(MONTH_MSG));
    }
    let max = q.max_drivers.map_or(DEFAULT_MAX_DRIVERS, |m| {
        usize::try_from(m).unwrap_or(usize::MAX)
    });
    Ok(DriversRequest {
        month,
        after: q.after_driver_cd,
        max,
    })
}

impl DriversRequest {
    /// `TIMECARD_DRIVERS_SQL`。窓は対象月ちょうど (元の `drivers_page` と同じ)。
    pub fn sql(&self) -> Result<String, Fail> {
        let (from, to) = exact_month_range(&self.month)
            .ok_or_else(|| bad_request(format!("bad month: {}", self.month)))?;
        sql_text(
            TIMECARD_DRIVERS_SQL,
            &[("from", at(&from)), ("to", at(&to))],
        )
    }

    /// 応答 `{month, drivers, next_after_driver_cd, elapsed_ms}`。`elapsed_ms` は読みにかかった時間 (呼び手が測る)。
    pub fn respond(&self, rows: &[Row], elapsed_ms: u64) -> Result<Value, Fail> {
        let all = rows_to_json(rows, timecard_driver_row).map_err(timecard_fail)?;
        let mut page = page_drivers(all, self.after, self.max);
        page.elapsed_ms = elapsed_ms;
        Ok(drivers_json(&self.month, &page))
    }
}

/// 元の `WindowQuery` と同じ 1 つの欄。
#[derive(Deserialize)]
struct WindowQuery {
    months: Option<String>,
}

/// 検査済みの timecard/events の要求。`from` / `to` は窓ぜんたい `[最初の月初, 最後の翌月初)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowRequest {
    pub months: Vec<String>,
    pub from: String,
    pub to: String,
}

/// timecard/events のクエリ文字列を検査する (Query として読めること → `parse_months` → 窓)。
pub fn parse_timecard_events(query: &str) -> Result<WindowRequest, Fail> {
    let q: WindowQuery = parse_query(query)?;
    let months = parse_months(q.months.as_deref().unwrap_or_default())
        .map_err(|e| bad_request(e.to_string()))?;
    let (from, to) = window_bounds(&months).ok_or_else(|| bad_request(MONTHS_INVALID))?;
    Ok(WindowRequest { months, from, to })
}

impl WindowRequest {
    /// `TIMECARD_WINDOW_SQL` (全乗務員の打刻を窓ぶん)。
    pub fn sql(&self) -> Result<String, Fail> {
        sql_text(
            TIMECARD_WINDOW_SQL,
            &[("from", at(&self.from)), ("to", at(&self.to))],
        )
    }

    /// 応答 `{months, drivers, events, elapsed_ms}`。行は `EVENTS_SQL` と同じ 7 列 (元の `row_to_json`)。
    pub fn respond(&self, rows: &[Row], elapsed_ms: u64) -> Result<Value, Fail> {
        let events = rows_to_json(rows, event_row).map_err(timecard_fail)?;
        Ok(window_events_json(&self.months, events, elapsed_ms))
    }
}
