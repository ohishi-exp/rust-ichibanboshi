//! 打刻を GCP へ送るときの変更記録 (Refs ohishi-exp/nuxt-dtako-admin#1133)。
//!
//! [`crate::kintai_push::KintaiPgStore::replace_window`] は署名の変わった日を
//! DELETE → INSERT で丸ごと置き換える。前の値がどこにも残らないので、**置き換える
//! 直前に同じトランザクションの中で**旧 events を読み、旧と新が違う日だけ
//! `kintai.event_changes` へ前後を残す ([`record_changes`])。
//!
//! 記録する行の組み立て (`build_changes`)・SQL・bind の束は勤怠 Worker と共有する `kintai-logic`
//! (`workers/kintai/logic/src/change_log.rs`、Refs #322) にある。ここは sqlx の bind だけ。
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

use chrono::{DateTime, FixedOffset, NaiveDateTime};

pub use kintai_logic::change_log::{
    build_changes, change_columns, events_json, old_event, DayChange, INSERT_CHANGES_SQL,
    OLD_EVENTS_SQL,
};

use crate::kintai_push::{DriverPlan, PushEvent, PUSHED_SOURCES};

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
        .map(|r| {
            old_event(
                r.get("driver_cd"),
                r.get::<NaiveDateTime, _>("at"),
                r.get("state"),
                r.get("source"),
                r.get("unko_no"),
            )
        })
        .collect();
    let changes = build_changes(&before, plans);
    if changes.is_empty() {
        return Ok(0);
    }
    let cols = change_columns(&changes);
    sqlx::query(INSERT_CHANGES_SQL)
        .bind(tenant_id)
        .bind(&cols.driver_cd)
        .bind(&cols.date)
        .bind(&cols.before)
        .bind(&cols.after)
        .execute(&mut *conn)
        .await?;
    Ok(changes.len())
}
