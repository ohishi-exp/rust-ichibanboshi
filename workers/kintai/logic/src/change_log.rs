//! `GET /api/kintai/change-log?driver=<乗務員CD>&from=YYYY-MM-DD&to=YYYY-MM-DD` — 取り込み後に打刻が直された記録
//! (`kintai.event_changes`)。
//!
//! **root の `src/routes/change_log.rs` の写し** (Refs #322。撤去までは片方を直したらもう片方も直す)。
//! `driver` は任意 (省略で全乗務員)。`from` / `to` は必須で両端を含む。最大 400 日。
//! `recording_since` はこのテナントの最古の `recorded_at` (無ければ null)。

use chrono::NaiveDate;
use postgres_types::Type;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{bad_request, parse_query, Fail, Param};

/// 502 の本文の頭 (元と同じ)。
pub const DB_WHAT: &str = "kintai.event_changes read";

/// 1 回に読める期間の上限 (両端を含む日数)。
pub const MAX_CHANGE_LOG_DAYS: i64 = 400;

/// 期間 (両端を含む) の記録。`$4` が NULL なら全乗務員。
pub const SELECT_SQL: &str = r#"
SELECT driver_cd,
       to_char(date, 'YYYY-MM-DD') AS date,
       to_char(recorded_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS recorded_at,
       before, after
  FROM kintai.event_changes
 WHERE tenant_id = $1 AND date >= $2 AND date <= $3
   AND ($4::int8 IS NULL OR driver_cd = $4)
 ORDER BY date, driver_cd, recorded_at
"#;

/// このテナントの最古の `recorded_at` (1 行・1 列。行が無ければ NULL)。
pub const SINCE_SQL: &str = r#"
SELECT to_char(min(recorded_at) AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
  FROM kintai.event_changes
 WHERE tenant_id = $1
"#;

/// `driver` は数値として読む (元と同じく、数字でなければ Query の段で 400)。
#[derive(Debug, Default, Deserialize)]
pub struct ChangeLogQuery {
    pub driver: Option<i64>,
    pub from: Option<String>,
    pub to: Option<String>,
}

/// 検査済みの入力。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub driver: Option<i64>,
    pub from: NaiveDate,
    pub to: NaiveDate,
}

/// クエリ文字列を読んで検査する (元の handler の 400 と同じ条件・同じ文言)。
pub fn parse(query: &str) -> Result<Request, Fail> {
    let q: ChangeLogQuery = parse_query(query)?;
    let (from, to) = parse_range(&q)?;
    Ok(Request {
        driver: q.driver,
        from,
        to,
    })
}

/// `from` / `to` を検査して日付の対に。
pub fn parse_range(q: &ChangeLogQuery) -> Result<(NaiveDate, NaiveDate), Fail> {
    let day = |s: &Option<String>| {
        s.as_deref()
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
    };
    let (Some(from), Some(to)) = (day(&q.from), day(&q.to)) else {
        return Err(bad_request("from / to は YYYY-MM-DD で指定してください"));
    };
    if from > to {
        return Err(bad_request("from は to 以前にしてください"));
    }
    if (to - from).num_days() >= MAX_CHANGE_LOG_DAYS {
        return Err(bad_request("期間は 400 日までです"));
    }
    Ok((from, to))
}

/// `SELECT_SQL` の引数。`$1` = テナント (UUID の pin)、`$2`/`$3` = 期間の両端 (DATE)、`$4` = 乗務員CD (INT8、NULL で全員)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binds {
    pub tenant: Uuid,
    pub from: NaiveDate,
    pub to: NaiveDate,
    pub driver: Option<i64>,
}

impl Binds {
    pub fn new(tenant: Uuid, req: &Request) -> Self {
        Self {
            tenant,
            from: req.from,
            to: req.to,
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

    /// `SINCE_SQL` の引数。`$1` = テナント (UUID の pin) だけ。
    pub fn since_params(&self) -> Vec<Param<'_>> {
        vec![(&self.tenant, Type::UUID)]
    }
}

/// `SELECT_SQL` の 1 行 (owned)。jsonb の列は `serde_json::Value` のまま。
#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub driver_cd: i64,
    pub date: String,
    pub recorded_at: String,
    pub before: Option<serde_json::Value>,
    pub after: Option<serde_json::Value>,
}

/// 応答 `{"driver", "from", "to", "recording_since", "changes": [...]}`。
pub fn respond(req: &Request, since: Option<String>, rows: Vec<Row>) -> serde_json::Value {
    let changes: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "driver_cd": r.driver_cd,
                "date": r.date,
                "recorded_at": r.recorded_at,
                "before": r.before,
                "after": r.after,
            })
        })
        .collect();
    serde_json::json!({
        "driver": req.driver,
        "from": req.from.to_string(),
        "to": req.to.to_string(),
        "recording_since": since,
        "changes": changes,
    })
}
