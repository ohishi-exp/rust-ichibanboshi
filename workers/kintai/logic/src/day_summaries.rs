//! `GET /api/kintai/day-summaries?month=YYYY-MM[&driver=1051]` — 畳んだ日別サマリ (`kintai.day_summaries`) の読み出し。
//!
//! **root の `src/routes/kintai_day_summaries.rs` の写し** (Refs #322。撤去までは片方を直したらもう片方も直す)。
//! 読むだけで、突合が使うキー構成 (`乗務員CD|暦日|開始時刻`) と列名をそのまま返す。
//! データが 0 件の月は **404 ではなく 200 + 空の `summaries`**。

use chrono::NaiveDate;
use postgres_types::Type;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{
    bad_request, is_valid_month, month_bounds, parse_driver, parse_query, Fail, Param,
};

/// 502 の本文の頭 (元と同じ)。
pub const DB_WHAT: &str = "kintai.day_summaries read";

/// 突合スクリプトがそのまま使えるよう、オンプレ基準ファイルと同じ 12 列を書く。
pub const SELECT_SQL: &str = r#"
SELECT driver_cd,
       to_char(date, 'YYYY-MM-DD') AS date,
       to_char(shift_start_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS shift_start_at,
       shift_source,
       restraint_minutes,
       working_minutes,
       break_minutes,
       rest_minus_minutes,
       statutory_minutes,
       within_statutory_overtime_minutes,
       overtime_minutes,
       legal_holiday_minutes,
       night_minutes,
       overtime_night_minutes,
       legal_holiday_night_minutes
  FROM kintai.day_summaries
 WHERE tenant_id = $1
   AND date >= $2 AND date < $3
   AND ($4::bigint IS NULL OR driver_cd = $4)
 ORDER BY driver_cd, date, shift_start_at
"#;

/// 11 個の分数の列 (応答のキーと同じ名前・同じ並び)。
pub const MINUTE_COLUMNS: [&str; 11] = [
    "restraint_minutes",
    "working_minutes",
    "break_minutes",
    "rest_minus_minutes",
    "statutory_minutes",
    "within_statutory_overtime_minutes",
    "overtime_minutes",
    "legal_holiday_minutes",
    "night_minutes",
    "overtime_night_minutes",
    "legal_holiday_night_minutes",
];

/// `?month=YYYY-MM[&driver=1051]`。`month` は必須、`driver` は任意 (省略時は全乗務員)。
#[derive(Debug, Default, Deserialize)]
pub struct DaySummariesQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
}

/// 検査済みの入力。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub month: String,
    pub driver: Option<i64>,
}

/// クエリ文字列を読んで検査する (元の handler の 400 と同じ条件・同じ文言)。
pub fn parse(query: &str) -> Result<Request, Fail> {
    validate(parse_query(query)?)
}

pub fn validate(params: DaySummariesQuery) -> Result<Request, Fail> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(i64::try_from(d).map_err(|e| {
                bad_request(format!("driver は乗務員CD (数字) で指定してください: {e}"))
            })?),
            None => return Err(bad_request("driver は乗務員CD (数字) で指定してください")),
        },
    };
    Ok(Request { month, driver })
}

/// `SELECT_SQL` の引数。`$1` = テナント (UUID の pin)、`$2`/`$3` = 月の `[月初, 翌月初)` (DATE)、
/// `$4` = 乗務員CD (INT8、NULL で全乗務員)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binds {
    pub tenant: Uuid,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub driver: Option<i64>,
}

impl Binds {
    pub fn new(tenant: Uuid, req: &Request) -> Self {
        let (from, to) = month_bounds(&req.month).expect("month validated by is_valid_month");
        Self {
            tenant,
            from,
            to,
            driver: req.driver,
        }
    }

    pub fn params(&self) -> Vec<Param<'_>> {
        vec![
            (&self.tenant, Type::UUID),
            (&self.from, Type::DATE),
            (&self.to, Type::DATE),
            (&self.driver, Type::INT8),
        ]
    }
}

/// `SELECT_SQL` の 1 行 (owned)。`minutes` は [`MINUTE_COLUMNS`] の順。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub driver_cd: i64,
    pub date: String,
    pub shift_start_at: String,
    pub shift_source: String,
    pub minutes: [i32; 11],
}

/// 応答 `{"month", "rows", "summaries": {"乗務員CD|暦日|開始時刻": {...}}}`。
pub fn respond(month: &str, rows: &[Row]) -> serde_json::Value {
    let mut summaries = serde_json::Map::with_capacity(rows.len());
    for r in rows {
        let key = format!("{}|{}|{}", r.driver_cd, r.date, r.shift_start_at);
        let mut value = serde_json::Map::with_capacity(MINUTE_COLUMNS.len() + 1);
        value.insert("shift_source".into(), r.shift_source.clone().into());
        for (name, minutes) in MINUTE_COLUMNS.iter().zip(r.minutes) {
            value.insert((*name).into(), minutes.into());
        }
        summaries.insert(key, value.into());
    }
    let count = summaries.len();
    serde_json::json!({
        "month": month,
        "rows": count,
        "summaries": summaries,
    })
}
