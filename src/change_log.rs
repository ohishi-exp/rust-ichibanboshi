//! 打刻を GCP へ送るときの変更記録 (Refs ohishi-exp/nuxt-dtako-admin#1133)。
//!
//! [`crate::kintai_push::KintaiPgStore::replace_window`] は署名の変わった日を
//! DELETE → INSERT で丸ごと置き換える。前の値がどこにも残らないので、**置き換える
//! 直前に同じトランザクションの中で**旧 events を読み、旧と新が違う日だけ
//! `kintai.event_changes` へ前後を残す ([`record_changes`])。
//!
//! - **初回取り込み (旧が無い日) は記録しない** — 「取り込み後の変更」ではない
//! - 旧があり新が無い日 (Deleted) は `after` = NULL で記録する
//! - 比較は署名 ([`day_signature`]) と同じ正規化で行う (並び順の違いを差と数えない)
//!
//! ## ファイル名は `change_log.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` に入ると `logic_version` が変わり、deploy で
//! 全乗務員が stale になる。ここは勤怠の値を形づくらない (記録を残すだけ) ので
//! glob の外が正しい分類。
//!
//! ## 往復の回数
//!
//! 旧 events の読みも記録の書きも **1 文ずつ** (`unnest`)。1 日 1 往復にすると
//! 置き換え本体と同じく 524 を踏む (`replace_window` の docs)。

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};

use crate::kintai_push::{day_signature, DriverPlan, PushEvent, DATETIME_FORMAT, PUSHED_SOURCES};

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

/// 置き換えの**直前に**呼ぶ。旧 events を読み、変わった日の前後を記録する。
///
/// `days` は `replace_window` が DELETE に渡す (乗務員, 日の始まり, 日の終わり) の配列そのもの。
/// 戻り値は記録した行数。
pub async fn record_changes(
    conn: &mut sqlx::PgConnection,
    tenant_id: uuid::Uuid,
    plans: &BTreeMap<i64, DriverPlan>,
    days: (&[i64], &[DateTime<FixedOffset>], &[DateTime<FixedOffset>]),
) -> Result<usize, sqlx::Error> {
    use sqlx::Row;
    let rows = sqlx::query(OLD_EVENTS_SQL)
        .bind(tenant_id)
        .bind(days.0)
        .bind(days.1)
        .bind(days.2)
        .bind(&PUSHED_SOURCES[..])
        .fetch_all(&mut *conn)
        .await?;
    let before: Vec<PushEvent> = rows
        .iter()
        .map(|r| PushEvent {
            driver_cd: r.get("driver_cd"),
            occurred_at: r.get::<NaiveDateTime, _>("at"),
            state: r.get("state"),
            source: r.get("source"),
            unko_no: r.get("unko_no"),
            raw: serde_json::Value::Null,
        })
        .collect();
    let changes = build_changes(&before, plans);
    if changes.is_empty() {
        return Ok(0);
    }
    let drivers: Vec<i64> = changes.iter().map(|c| c.driver_cd).collect();
    let dates: Vec<NaiveDate> = changes.iter().map(|c| c.date).collect();
    let befores: Vec<Option<serde_json::Value>> =
        changes.iter().map(|c| c.before.clone()).collect();
    let afters: Vec<Option<serde_json::Value>> = changes.iter().map(|c| c.after.clone()).collect();
    sqlx::query(INSERT_CHANGES_SQL)
        .bind(tenant_id)
        .bind(&drivers)
        .bind(&dates)
        .bind(&befores)
        .bind(&afters)
        .execute(&mut *conn)
        .await?;
    Ok(changes.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(driver: i64, at: &str, state: &str) -> PushEvent {
        PushEvent {
            driver_cd: driver,
            occurred_at: NaiveDateTime::parse_from_str(at, DATETIME_FORMAT).unwrap(),
            state: state.to_string(),
            source: "timecard".to_string(),
            unko_no: None,
            raw: serde_json::Value::Null,
        }
    }

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    fn plan(changed: &[(&str, Vec<PushEvent>)], deleted: &[&str]) -> DriverPlan {
        DriverPlan {
            changed: changed.iter().map(|(k, v)| (d(k), v.clone())).collect(),
            deleted: deleted.iter().copied().map(d).collect(),
        }
    }

    #[test]
    fn a_corrected_punch_records_before_and_after() {
        let old = vec![ev(1194, "2026-02-06 08:00:00", "始業")];
        let new = vec![ev(1194, "2026-02-06 07:30:00", "始業")];
        let plans = BTreeMap::from([(1194, plan(&[("2026-02-06", new)], &[]))]);
        let got = build_changes(&old, &plans);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].driver_cd, 1194);
        assert_eq!(got[0].date, d("2026-02-06"));
        let before = got[0].before.as_ref().unwrap();
        assert_eq!(before[0]["occurred_at"], "2026-02-06 08:00:00");
        assert_eq!(before[0]["state"], "始業");
        assert_eq!(before[0]["source"], "timecard");
        assert_eq!(before[0]["unko_no"], serde_json::Value::Null);
        assert_eq!(
            got[0].after.as_ref().unwrap()[0]["occurred_at"],
            "2026-02-06 07:30:00"
        );
    }

    #[test]
    fn a_first_import_is_not_a_change() {
        let new = vec![ev(1194, "2026-02-06 08:00:00", "始業")];
        let plans = BTreeMap::from([(1194, plan(&[("2026-02-06", new)], &["2026-02-07"]))]);
        assert!(build_changes(&[], &plans).is_empty());
    }

    #[test]
    fn the_same_events_in_another_order_are_not_a_change() {
        let a = ev(1, "2026-02-06 08:00:00", "始業");
        let b = ev(1, "2026-02-06 17:00:00", "終業");
        let plans = BTreeMap::from([(1, plan(&[("2026-02-06", vec![b.clone(), a.clone()])], &[]))]);
        assert!(build_changes(&[a, b], &plans).is_empty());
    }

    #[test]
    fn a_deleted_day_records_a_null_after() {
        let old = vec![ev(7, "2026-02-06 08:00:00", "始業")];
        let plans = BTreeMap::from([(7, plan(&[], &["2026-02-06"]))]);
        let got = build_changes(&old, &plans);
        assert_eq!(got.len(), 1);
        assert!(got[0].before.is_some());
        assert_eq!(got[0].after, None);
    }

    #[test]
    fn an_emptied_changed_day_also_has_a_null_after() {
        let old = vec![ev(7, "2026-02-06 08:00:00", "始業")];
        let plans = BTreeMap::from([(7, plan(&[("2026-02-06", vec![])], &[]))]);
        assert_eq!(build_changes(&old, &plans)[0].after, None);
    }

    #[test]
    fn other_drivers_old_events_are_not_mixed_in() {
        let old = vec![ev(2, "2026-02-06 08:00:00", "始業")];
        let new = vec![ev(1, "2026-02-06 07:30:00", "始業")];
        let plans = BTreeMap::from([(1, plan(&[("2026-02-06", new)], &[]))]);
        assert!(build_changes(&old, &plans).is_empty());
    }

    #[test]
    fn events_json_is_sorted_like_the_signature() {
        let late = ev(1, "2026-02-06 17:00:00", "終業");
        let early = ev(1, "2026-02-06 08:00:00", "始業");
        let got = events_json(&[late, early]);
        assert_eq!(got[0]["state"], "始業");
        assert_eq!(got[1]["state"], "終業");
    }
}
