//! `GET /api/kintai/shift-days?month=YYYY-MM&driver=<乗務員CD>` — 乗務員 1 人・1 か月ぶんの勤務ごとの始業・終業・
//! 日別サマリ・実働でない区間・暦日の按分。
//!
//! **root の `src/routes/shift_days.rs` の写し** (Refs #322。撤去までは片方を直したらもう片方も直す)。
//! `kintai.shifts` / `day_summaries` / `day_parts` の保存値を勤務 1 本 = 1 要素に束ねて返す。読むだけ・計算しない。
//! 始業 (JST) がその月に入る勤務を始業の昇順で返す。データが 0 件なら **200 + 空の `items`**。

use chrono::{DateTime, FixedOffset};
use postgres_types::Type;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{
    bad_request, is_valid_month, jst_midnight, month_bounds, parse_driver, parse_query, Fail, Param,
};

/// 502 の本文の頭 (元と同じ)。
pub const DB_WHAT: &str = "kintai shift-days read";

/// `kintai.shifts` を起点に、日別サマリ (と同じ行の `non_working`) を LEFT JOIN、暦日の按分を勤務ごとに束ねる。
pub const SELECT_SQL: &str = r#"
SELECT to_char(s.start_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS start_at,
       to_char(s.end_at   AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS end_at,
       s.shift_source,
       CASE WHEN d.shift_start_at IS NULL THEN NULL ELSE jsonb_build_object(
           'restraint_minutes', d.restraint_minutes,
           'working_minutes', d.working_minutes,
           'break_minutes', d.break_minutes,
           'rest_minus_minutes', d.rest_minus_minutes,
           'statutory_minutes', d.statutory_minutes,
           'within_statutory_overtime_minutes', d.within_statutory_overtime_minutes,
           'overtime_minutes', d.overtime_minutes,
           'legal_holiday_minutes', d.legal_holiday_minutes,
           'night_minutes', d.night_minutes,
           'overtime_night_minutes', d.overtime_night_minutes,
           'legal_holiday_night_minutes', d.legal_holiday_night_minutes
       ) END AS summary,
       d.non_working,
       COALESCE((
           SELECT jsonb_agg(jsonb_build_object(
                      'date', to_char(p.date, 'YYYY-MM-DD'),
                      'restraint_minutes', p.restraint_minutes,
                      'working_minutes', p.working_minutes,
                      'night_minutes', p.night_minutes
                  ) ORDER BY p.date)
             FROM kintai.day_parts p
            WHERE p.tenant_id = s.tenant_id
              AND p.driver_cd = s.driver_cd
              AND p.shift_start_at = s.start_at
       ), '[]'::jsonb) AS parts
  FROM kintai.shifts s
  LEFT JOIN kintai.day_summaries d
    ON d.tenant_id = s.tenant_id
   AND d.driver_cd = s.driver_cd
   AND d.shift_start_at = s.start_at
 WHERE s.tenant_id = $1
   AND s.driver_cd = $2
   AND s.start_at >= $3 AND s.start_at < $4
 ORDER BY s.start_at
"#;

/// `?month=YYYY-MM&driver=<乗務員CD>`。**どちらも必須。**
#[derive(Debug, Default, Deserialize)]
pub struct ShiftDaysQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
}

/// 乗務員CD (数字のみ)。無い・空・非数字・桁溢れは `None`。
pub fn parse_driver_cd(raw: Option<&str>) -> Option<i64> {
    i64::try_from(parse_driver(raw?)?).ok()
}

/// 検査済みの入力。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub month: String,
    pub driver_cd: i64,
}

/// クエリ文字列を読んで検査する (元の handler の 400 と同じ条件・同じ文言)。
pub fn parse(query: &str) -> Result<Request, Fail> {
    validate(parse_query(query)?)
}

pub fn validate(params: ShiftDaysQuery) -> Result<Request, Fail> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let Some(driver_cd) = parse_driver_cd(params.driver.as_deref()) else {
        return Err(bad_request("driver は乗務員CD (数字) で指定してください"));
    };
    Ok(Request { month, driver_cd })
}

/// `SELECT_SQL` の引数。`$1` = テナント (UUID の pin)、`$2` = 乗務員CD (INT8)、
/// `$3`/`$4` = 月の `[月初, 翌月初)` の JST 00:00 (TIMESTAMPTZ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binds {
    pub tenant: Uuid,
    pub driver_cd: i64,
    pub from: DateTime<FixedOffset>,
    pub to: DateTime<FixedOffset>,
}

impl Binds {
    pub fn new(tenant: Uuid, req: &Request) -> Self {
        let (first, next) = month_bounds(&req.month).expect("month validated by is_valid_month");
        Self {
            tenant,
            driver_cd: req.driver_cd,
            from: jst_midnight(first),
            to: jst_midnight(next),
        }
    }

    pub fn params(&self) -> Vec<Param<'_>> {
        vec![
            (&self.tenant, Type::UUID),
            (&self.driver_cd, Type::INT8),
            (&self.from, Type::TIMESTAMPTZ),
            (&self.to, Type::TIMESTAMPTZ),
        ]
    }
}

/// `SELECT_SQL` の 1 行 (owned)。jsonb の列は `serde_json::Value` のまま。
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub start_at: String,
    pub end_at: String,
    pub shift_source: String,
    pub summary: Option<serde_json::Value>,
    pub non_working: Option<serde_json::Value>,
    pub parts: serde_json::Value,
}

/// 応答 `{"month", "driver_cd", "items": [{"start_at", "end_at", "shift_source", "summary", "non_working", "parts"}]}`。
pub fn respond(req: &Request, rows: Vec<Row>) -> serde_json::Value {
    let items: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "start_at": r.start_at,
                "end_at": r.end_at,
                "shift_source": r.shift_source,
                "summary": r.summary,
                "non_working": r.non_working,
                "parts": r.parts,
            })
        })
        .collect();
    serde_json::json!({
        "month": req.month,
        "driver_cd": req.driver_cd,
        "items": items,
    })
}
