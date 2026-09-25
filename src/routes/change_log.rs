//! 取り込み後に打刻が直された記録 (`kintai.event_changes`) を返す読み出し口
//! (Refs ohishi-exp/nuxt-dtako-admin#1133)。
//!
//! 記録は打刻の push が置き換えの瞬間に残す ([`crate::change_log`])。ここは読むだけ。
//!
//! `GET /api/kintai/change-log?driver=<乗務員CD>&from=YYYY-MM-DD&to=YYYY-MM-DD`
//!
//! - `driver` は任意 (省略で全乗務員)。`from` / `to` は必須で両端を含む。最大 400 日
//! - `recording_since` はこのテナントの最古の `recorded_at` (無ければ null)。
//!   記録は仕組みを入れた日からなので、画面が「この日より前は記録なし」と言うため
//!
//! ## ファイル名は `change_log.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` に入ると `logic_version` が変わる
//! (`shift_overlaps.rs` / `unko_gaps.rs` と同じ分類)。
//!
//! ## テナント
//!
//! [`read_tenant_of`] は `unko_gaps::read_tenant_of` と同じ形 (設定の pin で決め、
//! `X-Tenant-ID` は読まない)。この HTTP server 自身は認可を持たない — 他の
//! `/api/kintai/*` と同じ router に載り、前段の網で守られる。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;

use crate::kintai_push::KintaiPgStore;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// 1 回に読める期間の上限 (両端を含む日数)。
pub const MAX_CHANGE_LOG_DAYS: i64 = 400;

#[derive(Debug, Default, Deserialize)]
pub struct ChangeLogQuery {
    pub driver: Option<i64>,
    pub from: Option<String>,
    pub to: Option<String>,
}

fn bad_request(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

/// `unko_gaps::store` と同じ文言で 503。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 読み先のテナント。`unko_gaps::read_tenant_of` と同じ形 — **どちらも無ければ 503**。
fn read_tenant_of(read: ReadTenant, pin: uuid::Uuid) -> Result<uuid::Uuid, (StatusCode, String)> {
    if let Some(t) = read.0 {
        if !t.is_nil() {
            return Ok(t);
        }
    }
    if !pin.is_nil() {
        return Ok(pin);
    }
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        "読み先のテナントが決まりません ([kintai_events] tenant_id を設定してください)".to_string(),
    ))
}

/// `from` / `to` を検査して日付の対に。
fn parse_range(q: &ChangeLogQuery) -> Result<(NaiveDate, NaiveDate), (StatusCode, String)> {
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

/// 期間 (両端を含む) の記録。`$4` が NULL なら全乗務員。
const SELECT_SQL: &str = r#"
SELECT driver_cd,
       to_char(date, 'YYYY-MM-DD') AS date,
       to_char(recorded_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS recorded_at,
       before, after
  FROM kintai.event_changes
 WHERE tenant_id = $1 AND date >= $2 AND date <= $3
   AND ($4::int8 IS NULL OR driver_cd = $4)
 ORDER BY date, driver_cd, recorded_at
"#;

const SINCE_SQL: &str = r#"
SELECT to_char(min(recorded_at) AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
  FROM kintai.event_changes
 WHERE tenant_id = $1
"#;

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.event_changes read failed: {e}"),
    )
}

fn row_to_item(r: &sqlx::postgres::PgRow) -> Result<serde_json::Value, (StatusCode, String)> {
    use sqlx::Row;
    Ok(serde_json::json!({
        "driver_cd": r.try_get::<i64, _>("driver_cd").map_err(db_err)?,
        "date": r.try_get::<String, _>("date").map_err(db_err)?,
        "recorded_at": r.try_get::<String, _>("recorded_at").map_err(db_err)?,
        "before": r.try_get::<Option<serde_json::Value>, _>("before").map_err(db_err)?,
        "after": r.try_get::<Option<serde_json::Value>, _>("after").map_err(db_err)?,
    }))
}

/// GET /api/kintai/change-log — 期間内の変更記録 (モジュール docs)。**書かない**。
pub async fn change_log(
    Query(q): Query<ChangeLogQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let (from, to) = parse_range(&q)?;
    let store = store(&pg)?;
    let tenant = read_tenant_of(read_tenant, store.tenant_id())?;
    let rows = sqlx::query(SELECT_SQL)
        .bind(tenant)
        .bind(from)
        .bind(to)
        .bind(q.driver)
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;
    let changes = rows
        .iter()
        .map(row_to_item)
        .collect::<Result<Vec<_>, _>>()?;
    let since: Option<String> = sqlx::query_scalar(SINCE_SQL)
        .bind(tenant)
        .fetch_one(store.pool())
        .await
        .map_err(db_err)?;
    let n = changes.len();
    tracing::info!(n, "kintai change-log read");
    Ok(Json(serde_json::json!({
        "driver": q.driver,
        "from": from.to_string(),
        "to": to.to_string(),
        "recording_since": since,
        "changes": changes,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    fn q(from: Option<&str>, to: Option<&str>) -> ChangeLogQuery {
        ChangeLogQuery {
            driver: None,
            from: from.map(str::to_string),
            to: to.map(str::to_string),
        }
    }

    #[test]
    fn read_tenant_wins_over_the_write_pin() {
        assert_eq!(
            read_tenant_of(ReadTenant(Some(uuid(1))), uuid(2)),
            Ok(uuid(1))
        );
    }

    #[test]
    fn without_a_read_tenant_the_write_pin_is_used() {
        assert_eq!(read_tenant_of(ReadTenant(None), uuid(2)), Ok(uuid(2)));
        assert_eq!(
            read_tenant_of(ReadTenant(Some(uuid::Uuid::nil())), uuid(2)),
            Ok(uuid(2))
        );
    }

    #[test]
    fn no_tenant_at_all_is_service_unavailable() {
        let (status, msg) = read_tenant_of(ReadTenant(None), uuid::Uuid::nil())
            .expect_err("must fail without any tenant");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_events"), "{msg}");
    }

    #[test]
    fn the_range_is_checked() {
        let ok = parse_range(&q(Some("2026-02-01"), Some("2027-03-07")));
        assert!(ok.is_ok(), "400 日ちょうどは通す");
        for (from, to, want) in [
            (None, Some("2026-02-01"), "YYYY-MM-DD"),
            (Some("2026-02-01"), Some("2026/02/02"), "YYYY-MM-DD"),
            (Some("2026-02-02"), Some("2026-02-01"), "以前"),
            (Some("2026-02-01"), Some("2027-03-08"), "400"),
        ] {
            let (status, msg) = parse_range(&q(from, to)).expect_err("must reject");
            assert_eq!(status, StatusCode::BAD_REQUEST);
            assert!(msg.contains(want), "{msg}");
        }
    }

    #[tokio::test]
    async fn the_handler_fails_closed_without_a_store() {
        let query = Query(q(Some("2026-02-01"), Some("2026-02-28")));
        let (status, msg) = change_log(query, Extension(None), Extension(ReadTenant(None)))
            .await
            .expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }
}
