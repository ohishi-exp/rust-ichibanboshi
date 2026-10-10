//! 社内 MariaDB を直接読む 2 本の口 (`GET /api/kintai/day-events`・`GET /api/dtako/worktime`) の検査・引数・応答。
//!
//! 元はオンプレ版 (root の `src/routes/dtako_day.rs` の `day_events`・`src/routes/dtako_worktime.rs` の `worktime`)。
//! 純粋部分 (日の窓・運行への畳み方・リンク・層 A の秒数) は共有 crate `kintai-dtako` をオンプレ版と同じものを使う
//! (写さない)。ここは handler の部分 (入力の検査の順・400 の文言・どの SQL を読むか) だけで、
//! **元の handler を直したらここも直す** (対応表は `workers/kintai/README.md`)。
//! DB との往復 (接続・認証・クエリ) は worker crate が持つ。
//!
//! - day-events: `driver` 必須 → `date` 必須 (`YYYY-MM-DD`)。窓は `[date 00:00:00, 翌日 00:00:00)`、`EVENTS_SQL`
//! - worktime: `month` (`exact_month_range` が作れること) → `driver` 任意 (`driver=` の空は 400)。窓は `[月初, 翌月初)`、
//!   `driver` ありは `EVENTS_SQL`・なしは `ALL_EVENTS_SQL`
//!
//! worktime の行は 1 本ずつ JSON にして [`Aggregate::add_row`] に足す (全乗務員の 1 か月 = 約 10 万行の JSON を並べない)。

use chrono::NaiveDate;
use kintai_dtako::day::{body, build_operations, day_range, parse_date, DATE_INVALID};
use kintai_dtako::worktime::{parse_dt, Aggregate};
use kintai_kosoku::sql::{ALL_EVENTS_SQL, EVENTS_SQL};
use kintai_kosoku::window::exact_month_range;
use kintai_mysql::bind::{expand, Value as Bind};
use kintai_mysql::response::Row;
use serde::Deserialize;
use serde_json::Value;

use crate::common::{bad_request, mariadb_fail, parse_driver, parse_query, Fail};
use crate::mariadb_reads::{DRIVER_MSG, MONTH_MSG};
use crate::mariadb_rows::{all_event_row, event_row, rows_to_json};

/// day-events のリンクの base URL を渡す var (wrangler.toml の `[vars]`)。空 = その項目のリンクを省く。
pub const RYOHI_BASE_URL_VAR: &str = "KINTAI_RYOHI_BASE_URL";
pub const DTAKO_BASE_URL_VAR: &str = "KINTAI_DTAKO_BASE_URL";

/// 口の種類。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DtakoRead {
    DayEvents,
    Worktime,
}

/// 元の `DayEventsQuery` と同じ 2 つの欄。
#[derive(Deserialize)]
struct DayEventsQuery {
    driver: Option<String>,
    date: Option<String>,
}

/// 元の `WorktimeQuery` と同じ 2 つの欄。
#[derive(Deserialize)]
struct WorktimeQuery {
    month: Option<String>,
    driver: Option<String>,
}

/// 検査済みの要求。`from` / `to` は `YYYY-MM-DD HH:MM:SS`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    DayEvents {
        driver: u64,
        date: NaiveDate,
        from: String,
        to: String,
    },
    Worktime {
        month: String,
        driver: Option<u64>,
        from: String,
        to: String,
    },
}

/// day-events のリンクの base URL (オンプレ版の `DtakoDayLinksConfig` と同じ意味)。空はその項目を `null` にする。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Links {
    pub ryohi_base_url: String,
    pub dtako_base_url: String,
}

impl DtakoRead {
    /// 口の path。元 (オンプレ版) と同じ。
    pub fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/kintai/day-events" => DtakoRead::DayEvents,
            "/api/dtako/worktime" => DtakoRead::Worktime,
            _ => return None,
        })
    }

    /// ログに出す名前。
    pub fn as_str(self) -> &'static str {
        match self {
            DtakoRead::DayEvents => "day-events",
            DtakoRead::Worktime => "dtako-worktime",
        }
    }

    /// クエリ文字列を検査する。順と 400 の文言は元の handler と同じ。
    pub fn parse(self, query: &str) -> Result<Request, Fail> {
        match self {
            DtakoRead::DayEvents => {
                let q: DayEventsQuery = parse_query(query)?;
                let raw = q.driver.unwrap_or_default();
                let driver = parse_driver(&raw).ok_or_else(|| bad_request(DRIVER_MSG))?;
                let date = q
                    .date
                    .as_deref()
                    .and_then(parse_date)
                    .ok_or_else(|| bad_request(DATE_INVALID))?;
                let (from, to) = day_range(date);
                Ok(Request::DayEvents {
                    driver,
                    date,
                    from,
                    to,
                })
            }
            DtakoRead::Worktime => {
                let q: WorktimeQuery = parse_query(query)?;
                let month = q.month.unwrap_or_default();
                let (from, to) = exact_month_range(&month).ok_or_else(|| bad_request(MONTH_MSG))?;
                let driver = match q.driver {
                    None => None,
                    Some(raw) => Some(parse_driver(&raw).ok_or_else(|| bad_request(DRIVER_MSG))?),
                };
                Ok(Request::Worktime {
                    month,
                    driver,
                    from,
                    to,
                })
            }
        }
    }
}

impl Request {
    fn window(&self) -> (&str, &str) {
        match self {
            Request::DayEvents { from, to, .. } | Request::Worktime { from, to, .. } => (from, to),
        }
    }

    /// 乗務員で絞るか (絞る = `EVENTS_SQL`、絞らない = `ALL_EVENTS_SQL`)。
    fn driver(&self) -> Option<u64> {
        match self {
            Request::DayEvents { driver, .. } => Some(*driver),
            Request::Worktime { driver, .. } => *driver,
        }
    }

    /// 読む SQL (`kintai_kosoku::sql`)。元の repo の `fetch_events_between` / `fetch_all_events_between` と同じ。
    pub fn sql(&self) -> &'static str {
        match self.driver() {
            Some(_) => EVENTS_SQL,
            None => ALL_EVENTS_SQL,
        }
    }

    /// SQL に渡す名前付き引数。`EVENTS_SQL` は `from`・`to`・`driver`、`ALL_EVENTS_SQL` は `from`・`to` だけ。
    pub fn binds(&self) -> Vec<(&'static str, Bind)> {
        let (from, to) = self.window();
        let at = |s: &str| Bind::DateTime(parse_dt(s).expect("window bounds are datetimes"));
        let mut out = vec![("from", at(from)), ("to", at(to))];
        if let Some(d) = self.driver() {
            out.push(("driver", Bind::UInt(d)));
        }
        out
    }

    /// 値を埋め込んだ SQL (COM_QUERY に載せる文)。名前の食い違いは 502 (`bind_*`)。
    pub fn sql_text(&self) -> Result<String, Fail> {
        expand(self.sql(), &self.binds()).map_err(|e| mariadb_fail(e.kind()))
    }

    /// 結果の行から応答の JSON を作る。行 → JSON は元の repo と同じ (`EVENTS_SQL` = 7 列、`ALL_EVENTS_SQL` = 5 列)。
    /// 1 行でも読めなければ全体が 502 (元の `exec` が 1 行の失敗で全体を落とすのと同じ)。
    pub fn respond(&self, rows: &[Row], links: &Links) -> Result<Value, Fail> {
        let to_json = match self.driver() {
            Some(_) => event_row,
            None => all_event_row,
        };
        match self {
            Request::DayEvents { driver, date, .. } => {
                let rows = rows_to_json(rows, to_json)?;
                let ops = build_operations(&rows, &links.ryohi_base_url, &links.dtako_base_url);
                Ok(body(*driver, *date, ops, rows))
            }
            Request::Worktime {
                month,
                driver,
                from,
                to,
            } => {
                // `exact_month_range` が作った書式なので必ず読める
                let win_from = parse_dt(from).expect("exact_month_range の from");
                let win_to = parse_dt(to).expect("exact_month_range の to");
                let mut agg = Aggregate::default();
                for row in rows {
                    agg.add_row(&to_json(row)?, win_from, win_to);
                }
                Ok(agg.to_json(month, *driver, from, to))
            }
        }
    }
}
