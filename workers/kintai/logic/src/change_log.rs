//! `GET /api/kintai/change-log?driver=<乗務員CD>&from=YYYY-MM-DD&to=YYYY-MM-DD` — 取り込み後に打刻が直された記録
//! (`kintai.event_changes`)。
//!
//! root の `src/routes/change_log.rs` (読み) と `src/change_log.rs` (書き) の SQL・検査・行の組み立ては**ここが正本**で、
//! root はここを path 依存で使う (Refs #322。写さない)。root に残るのは sqlx の bind と handler だけ。
//! `driver` は任意 (省略で全乗務員)。`from` / `to` は必須で両端を含む。最大 400 日。
//! `recording_since` はこのテナントの最古の `recorded_at` (無ければ null)。

use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime};
use kintai_kosoku::kintai_push::{day_signature, DriverPlan, PushEvent, DATETIME_FORMAT};
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

// ── 書き込み (打刻の置き換えの直前に残す変更履歴、Refs ohishi-exp/nuxt-dtako-admin#1133) ─────────────
//
// root の `src/change_log.rs` から移した純粋部分 (Refs #322)。旧 events の読み (`OLD_EVENTS_SQL`) と記録の書き
// (`INSERT_CHANGES_SQL`) は、置き換え (`kintai_kosoku::kintai_push::DELETE_DAYS_SQL`) と**同じ transaction の中で
// DELETE の前に**流す。bind は root (sqlx) と勤怠 Worker がそれぞれ持つ。
//
// - **初回取り込み (旧が無い日) は記録しない** — 「取り込み後の変更」ではない
// - 旧があり新が無い日 (Deleted) は `after` = NULL で記録する
// - 比較は署名 (`day_signature`) と同じ正規化で行う (並び順の違いを差と数えない)

/// `kintai.event_changes` の 1 行ぶん。
#[derive(Debug, Clone, PartialEq)]
pub struct DayChange {
    pub driver_cd: i64,
    pub date: NaiveDate,
    /// 旧 events の配列。
    pub before: Option<serde_json::Value>,
    /// 新 events の配列。日ごと消えたら `None`。
    pub after: Option<serde_json::Value>,
}

/// events の配列を JSON に。並びは署名と同じ `occurred_at, state, source`。
pub fn events_json(events: &[PushEvent]) -> serde_json::Value {
    let mut sorted: Vec<&PushEvent> = events.iter().collect();
    sorted.sort_by(|a, b| {
        (a.occurred_at, &a.state, &a.source).cmp(&(b.occurred_at, &b.state, &b.source))
    });
    let items = sorted.iter().map(|e| {
        serde_json::json!({
            "occurred_at": e.occurred_at.format(DATETIME_FORMAT).to_string(),
            "state": e.state,
            "source": e.source,
            "unko_no": e.unko_no,
        })
    });
    serde_json::Value::Array(items.collect())
}

/// 旧 events (置き換える日すべてぶん) と置き換えの計画から、記録する行を作る。
///
/// DB を見ない。旧が無い日 (初回取り込み) と、署名が一致する日は返さない。
pub fn build_changes(before: &[PushEvent], plans: &BTreeMap<i64, DriverPlan>) -> Vec<DayChange> {
    let mut old: BTreeMap<(i64, NaiveDate), Vec<PushEvent>> = BTreeMap::new();
    for ev in before {
        old.entry((ev.driver_cd, ev.date()))
            .or_default()
            .push(ev.clone());
    }
    let mut out = Vec::new();
    for (&driver_cd, plan) in plans {
        for (&date, new) in &plan.changed {
            let Some(prev) = old.get(&(driver_cd, date)) else {
                continue; // 初回取り込み
            };
            if day_signature(prev) == day_signature(new) {
                continue;
            }
            out.push(DayChange {
                driver_cd,
                date,
                before: Some(events_json(prev)),
                after: (!new.is_empty()).then(|| events_json(new)),
            });
        }
        for &date in &plan.deleted {
            if let Some(prev) = old.get(&(driver_cd, date)) {
                out.push(DayChange {
                    driver_cd,
                    date,
                    before: Some(events_json(prev)),
                    after: None,
                });
            }
        }
    }
    out
}

/// 置き換える日の旧 events を **1 文で**。条件は `DELETE_DAYS_SQL` と同じ
/// (消す行 = 読む行)。時刻は JST の壁時計 (`timestamp`) で返す。
pub const OLD_EVENTS_SQL: &str = r#"
SELECT e.driver_cd,
       (e.occurred_at AT TIME ZONE 'Asia/Tokyo') AS at,
       e.state, e.source, e.unko_no
  FROM kintai.kintai_events e
  JOIN unnest($2::int8[], $3::timestamptz[], $4::timestamptz[]) AS d(driver_cd, from_ts, to_ts)
    ON e.driver_cd = d.driver_cd
   AND e.occurred_at >= d.from_ts
   AND e.occurred_at < d.to_ts
 WHERE e.tenant_id = $1
   AND e.source = ANY($5)
"#;

/// 記録を **1 文で**。`recorded_at` は既定の `now()` (= このトランザクションの時刻)。
pub const INSERT_CHANGES_SQL: &str = r#"
INSERT INTO kintai.event_changes (tenant_id, driver_cd, date, before, after)
SELECT $1, d.driver_cd, d.date, d.before, d.after
  FROM unnest($2::int8[], $3::date[], $4::jsonb[], $5::jsonb[])
       AS d(driver_cd, date, before, after)
"#;

/// `OLD_EVENTS_SQL` の 1 行 (`at` は JST の壁時計) を、`build_changes` に渡す形に。`raw` は比べないので NULL。
pub fn old_event(
    driver_cd: i64,
    at: NaiveDateTime,
    state: String,
    source: String,
    unko_no: Option<String>,
) -> PushEvent {
    PushEvent {
        driver_cd,
        occurred_at: at,
        state,
        source,
        unko_no,
        raw: serde_json::Value::Null,
    }
}

/// `INSERT_CHANGES_SQL` の `$2`〜`$5` (int8[]・date[]・jsonb[]・jsonb[]。jsonb の NULL は SQL の NULL)。
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ChangeColumns {
    pub driver_cd: Vec<i64>,
    pub date: Vec<NaiveDate>,
    pub before: Vec<Option<serde_json::Value>>,
    pub after: Vec<Option<serde_json::Value>>,
}

/// 記録する行を 1 文ぶんの束に (`build_changes` が空なら空 = 書かない)。
pub fn change_columns(changes: &[DayChange]) -> ChangeColumns {
    ChangeColumns {
        driver_cd: changes.iter().map(|c| c.driver_cd).collect(),
        date: changes.iter().map(|c| c.date).collect(),
        before: changes.iter().map(|c| c.before.clone()).collect(),
        after: changes.iter().map(|c| c.after.clone()).collect(),
    }
}
