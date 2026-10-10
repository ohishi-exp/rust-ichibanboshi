//! `GET /api/kintai/shift-overlaps?month=YYYY-MM` — 同じ乗務員の勤務の時間帯が重なっている (かぶり) 組。
//!
//! **root の `src/routes/shift_overlaps.rs` の写し** (Refs #322。撤去までは片方を直したらもう片方も直す)。
//! 判定は `kintai.shifts` の保存値の比較だけ。組は、後から始まる `b` の開始 (JST) が対象月に入るものだけ返す
//! (`a` は前月に始まってよい。勤務の長さに上限を置かない)。データが 0 件の月は **200 + 空の `items`**。

use chrono::{DateTime, FixedOffset};
use postgres_types::Type;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{
    bad_request, is_valid_month, jst_midnight, month_bounds, parse_query, Fail, Param,
};

/// 502 の本文の頭 (元と同じ)。
pub const DB_WHAT: &str = "kintai.shifts read";

/// 同じ乗務員の 2 本 `a` / `b` の自己結合 (開区間で重なる組)。
pub const SELECT_SQL: &str = r#"
SELECT a.driver_cd,
       to_char(a.start_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS a_start,
       to_char(a.end_at   AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS a_end,
       to_char(b.start_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS b_start,
       to_char(b.end_at   AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS b_end
  FROM kintai.shifts a
  JOIN kintai.shifts b
    ON b.tenant_id = a.tenant_id
   AND b.driver_cd = a.driver_cd
   AND a.start_at < b.start_at
   AND b.start_at < a.end_at
 WHERE a.tenant_id = $1
   AND a.end_at > $2 AND a.start_at < $3
   AND b.start_at >= $2 AND b.start_at < $3
 ORDER BY a.driver_cd, b.start_at, a.start_at
"#;

/// `?month=YYYY-MM` (必須)。乗務員指定は受けない。
#[derive(Debug, Default, Deserialize)]
pub struct ShiftOverlapsQuery {
    pub month: Option<String>,
}

/// 検査済みの入力。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub month: String,
}

/// クエリ文字列を読んで検査する (元の handler の 400 と同じ条件・同じ文言)。
pub fn parse(query: &str) -> Result<Request, Fail> {
    validate(parse_query(query)?)
}

pub fn validate(params: ShiftOverlapsQuery) -> Result<Request, Fail> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    Ok(Request { month })
}

/// `SELECT_SQL` の引数。`$1` = テナント (UUID の pin)、`$2`/`$3` = 月の `[月初, 翌月初)` の JST 00:00 (TIMESTAMPTZ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binds {
    pub tenant: Uuid,
    pub from: DateTime<FixedOffset>,
    pub to: DateTime<FixedOffset>,
}

impl Binds {
    pub fn new(tenant: Uuid, req: &Request) -> Self {
        let (first, next) = month_bounds(&req.month).expect("month validated by is_valid_month");
        Self {
            tenant,
            from: jst_midnight(first),
            to: jst_midnight(next),
        }
    }

    pub fn params(&self) -> Vec<Param<'_>> {
        vec![
            (&self.tenant, Type::UUID),
            (&self.from, Type::TIMESTAMPTZ),
            (&self.to, Type::TIMESTAMPTZ),
        ]
    }
}

/// `SELECT_SQL` の 1 行 (owned)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub driver_cd: i64,
    pub a_start: String,
    pub a_end: String,
    pub b_start: String,
    pub b_end: String,
}

/// 応答 `{"month", "items": [{"driver_cd", "a_start", "a_end", "b_start", "b_end"}]}`。
pub fn respond(month: &str, rows: &[Row]) -> serde_json::Value {
    let items: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "driver_cd": r.driver_cd,
                "a_start": r.a_start,
                "a_end": r.a_end,
                "b_start": r.b_start,
                "b_end": r.b_end,
            })
        })
        .collect();
    serde_json::json!({
        "month": month,
        "items": items,
    })
}
