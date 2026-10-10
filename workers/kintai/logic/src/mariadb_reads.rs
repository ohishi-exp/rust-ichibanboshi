//! 社内 MariaDB を直接読む 4 本の口 (`GET /api/kintai/{events,rest-diff,reading-dates,tail-gap-probe}`) の純粋部分。
//!
//! root の `src/routes/kintai.rs` の `events`・`rest_diff`・`reading_dates`・`tail_gap_probe` の写し
//! (対応表は `workers/kintai/README.md`。**撤去までは片方を直したらもう片方も直す**)。
//! 入力の検査 (順・400 の文言)・窓 (`month_range` / `exact_month_range`)・SQL と名前付き引数・応答の JSON は元と同じ。
//! SQL 文と突合・引き当て・末尾検知の純粋ロジックは共有 crate (`kintai-kosoku`) をそのまま使う (写さない)。
//! DB との往復 (接続・認証・クエリ) は worker crate が持つ。

use chrono::{DateTime, Duration, NaiveDate};
use kintai_kosoku::kintai_reading_dates::{reading_dates, MAX_READING_DATES};
use kintai_kosoku::kintai_rest_diff::{rest_diff, MAX_REST_DIFF};
use kintai_kosoku::kintai_tail_gap_probe::tail_gap_probe;
use kintai_kosoku::sql::{
    ALL_EVENTS_SQL, EVENTS_SQL, OPERATION_READING_DATES_SQL, REST_EVENTS_SQL,
};
use kintai_kosoku::window::{exact_month_range, month_range, parse_dt};
use kintai_mysql::bind::{expand, Value as Bind};
use kintai_mysql::response::Row;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::common::{bad_request, is_valid_month, mariadb_fail, parse_driver, parse_query, Fail};
use crate::mariadb_rows::{all_event_row, event_row, reading_date_row, rest_row, rows_to_json};

/// 元と同じ 400 の文言。
pub const MONTH_MSG: &str = "month は YYYY-MM で指定してください";
pub const DRIVER_MSG: &str = "driver は乗務員CD (数字) で指定してください";

/// JST の UTC からのずれ (時間)。
const JST_OFFSET_HOURS: i64 = 9;

/// 口の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MariadbRead {
    Events,
    RestDiff,
    ReadingDates,
    TailGapProbe,
}

/// 元の `EventsQuery` (axum の `Query`) と同じ 3 つの欄。`view` は読まないが、元と同じく 2 回来たら 400 になるように持つ。
#[derive(Deserialize)]
struct EventsQuery {
    month: Option<String>,
    driver: Option<String>,
    #[serde(rename = "view")]
    _view: Option<String>,
}

/// 検査済みの要求。`from` / `to` は `YYYY-MM-DD HH:MM:SS`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub read: MariadbRead,
    pub month: String,
    pub driver: Option<u64>,
    pub from: String,
    pub to: String,
}

impl MariadbRead {
    /// 口の path。元 (オンプレ版) と同じ。
    pub fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/kintai/events" => MariadbRead::Events,
            "/api/kintai/rest-diff" => MariadbRead::RestDiff,
            "/api/kintai/reading-dates" => MariadbRead::ReadingDates,
            "/api/kintai/tail-gap-probe" => MariadbRead::TailGapProbe,
            _ => return None,
        })
    }

    /// ログに出す名前。
    pub fn as_str(self) -> &'static str {
        match self {
            MariadbRead::Events => "events",
            MariadbRead::RestDiff => "rest-diff",
            MariadbRead::ReadingDates => "reading-dates",
            MariadbRead::TailGapProbe => "tail-gap-probe",
        }
    }

    /// 読む SQL (`kintai_kosoku::sql`)。
    pub fn sql(self) -> &'static str {
        match self {
            MariadbRead::Events => EVENTS_SQL,
            MariadbRead::RestDiff => REST_EVENTS_SQL,
            MariadbRead::ReadingDates => OPERATION_READING_DATES_SQL,
            MariadbRead::TailGapProbe => ALL_EVENTS_SQL,
        }
    }

    /// クエリ文字列を検査する。順と 400 の文言は元の handler と同じ:
    /// - events: `is_valid_month` → `driver` 必須
    /// - rest-diff・reading-dates: `month_range` (翌月 2 日まで) → `driver` 任意 (`driver=` の空は 400)
    /// - tail-gap-probe: `exact_month_range` (翌月初まで) → `driver` 任意
    pub fn parse(self, query: &str) -> Result<Request, Fail> {
        let q: EventsQuery = parse_query(query)?;
        let month = q.month.unwrap_or_default();
        let (window, driver) = match self {
            MariadbRead::Events => {
                if !is_valid_month(&month) {
                    return Err(bad_request(MONTH_MSG));
                }
                let raw = q.driver.unwrap_or_default();
                let driver = parse_driver(&raw).ok_or_else(|| bad_request(DRIVER_MSG))?;
                (month_range(&month), Some(driver))
            }
            MariadbRead::RestDiff | MariadbRead::ReadingDates | MariadbRead::TailGapProbe => {
                let window = if self == MariadbRead::TailGapProbe {
                    exact_month_range(&month)
                } else {
                    month_range(&month)
                };
                let Some(window) = window else {
                    return Err(bad_request(MONTH_MSG));
                };
                let driver = match q.driver {
                    None => None,
                    Some(raw) => Some(parse_driver(&raw).ok_or_else(|| bad_request(DRIVER_MSG))?),
                };
                (Some(window), driver)
            }
        };
        let (from, to) = window.expect("a valid month has a window");
        Ok(Request {
            read: self,
            month,
            driver,
            from,
            to,
        })
    }
}

impl Request {
    /// SQL に渡す名前付き引数。events・rest-diff・reading-dates は `from`・`to`・`driver`
    /// (`driver` 省略は NULL = 全乗務員)、tail-gap-probe は `from`・`to` だけ (乗務員の絞りは Rust 側)。
    pub fn binds(&self) -> Vec<(&'static str, Bind)> {
        let at = |s: &str| Bind::DateTime(parse_dt(s).expect("window bounds are datetimes"));
        let mut out = vec![("from", at(&self.from)), ("to", at(&self.to))];
        if self.read != MariadbRead::TailGapProbe {
            out.push(("driver", self.driver.map_or(Bind::Null, Bind::UInt)));
        }
        out
    }

    /// 値を埋め込んだ SQL (COM_QUERY に載せる文)。名前の食い違いは 502 (`bind_*`)。
    pub fn sql_text(&self) -> Result<String, Fail> {
        expand(self.read.sql(), &self.binds()).map_err(|e| mariadb_fail(e.kind()))
    }

    /// 結果の行から応答の JSON を作る。`today` は JST の今日 (tail-gap-probe だけが使う)。
    pub fn respond(&self, rows: &[Row], today: NaiveDate) -> Result<Value, Fail> {
        let (month, driver, from, to) = (&self.month, self.driver, &self.from, &self.to);
        Ok(match self.read {
            MariadbRead::Events => json!({ "rows": rows_to_json(rows, event_row)? }),
            MariadbRead::RestDiff => {
                let rows = rows_to_json(rows, rest_row)?;
                let diff = rest_diff(&rows, from, to);
                json!({
                    "month": month,
                    "driver": driver,
                    "from": from,
                    "to": to,
                    "mismatch_total": diff.mismatch_total,
                    "total_by_kind": diff.total_by_kind,
                    "total": diff.total,
                    "items": diff.items,
                    "by_driver": diff.by_driver,
                    "scanned_unko": diff.scanned_unko,
                    "skipped_rows": diff.skipped_rows,
                    "max_items": MAX_REST_DIFF,
                })
            }
            MariadbRead::ReadingDates => {
                let rows = rows_to_json(rows, reading_date_row)?;
                let mapped = reading_dates(&rows);
                json!({
                    "month": month,
                    "driver": driver,
                    "from": from,
                    "to": to,
                    "by_reading_date": mapped.by_reading_date,
                    "unknown_reading_date": mapped.unknown_reading_date,
                    "total": mapped.total,
                    "items": mapped.items,
                    "skipped_rows": mapped.skipped_rows,
                    "max_items": MAX_READING_DATES,
                })
            }
            MariadbRead::TailGapProbe => {
                let rows = rows_to_json(rows, all_event_row)?;
                // 窓の末尾 (= 月末) と「進行中の月は today - 1 日」の小さい方 (元と同じ)
                let next = NaiveDate::parse_from_str(&to[..10], "%Y-%m-%d").expect("to is a date");
                let expected = (next - Duration::days(1)).min(today - Duration::days(1));
                let probe = tail_gap_probe(&rows, month, expected, driver);
                json!({
                    "month": probe.month,
                    "driver": driver,
                    "from": from,
                    "to": to,
                    "expected": probe.expected,
                    "threshold_days": probe.threshold_days,
                    "population": probe.population,
                    "over_threshold_total": probe.over_threshold_total,
                    "over_threshold_unpunched_total": probe.over_threshold_unpunched_total,
                    "drivers": probe.drivers,
                })
            }
        })
    }
}

/// UNIX 時刻 (ミリ秒) の JST の暦日。元の `today_jst` (`Utc::now()` を JST へ) に当たり、Worker は `Date.now()` を渡す。
pub fn jst_today(now_ms: u64) -> NaiveDate {
    let utc = DateTime::from_timestamp_millis(now_ms as i64).unwrap_or_default();
    (utc + Duration::hours(JST_OFFSET_HOURS)).date_naive()
}
