//! 畳んだ結果 (`kintai.day_summaries`) の読み出し口 (Refs #205 の 18)。
//!
//! `kintai.shifts` / `day_summaries` / `day_parts` の 3 表は 2026-07-31 に本番へ
//! 入ったが、**読み出す口が 1 つも無かった**。この口が無いと、オンプレの基準値
//! (`/api/kintai/kosoku-daily`) との突合が総数レベルでしかできず、差がどの乗務員の
//! どの日で消えているのかが分からない。この口はその突合を機械でできるようにする
//! ところまでが目的で、142 行差の原因究明そのもの (#205-19) はここでは行わない。
//!
//! **読むだけ。1 行も書かない。** `POST` は無い。
//!
//! ## 応答の形はオンプレ基準 JSON に合わせる
//!
//! キーは `乗務員CD|暦日|開始時刻` (開始時刻は JST の壁時計 `YYYY-MM-DD HH:MM:SS`)。
//! 列名は [`crate::kintai_fold::DaySummaryRow`] (= `kintai.day_summaries` の列) と
//! オンプレ基準ファイルとで**もともと一致している**ので、名前の写し替えはしていない。
//!
//! ## 認可 — CF Access Service Token (edge)
//!
//! `/kintai/kosoku-daily` と同じ判断 (`routes::kintai` モジュール docs 参照)。
//! **応答に含まれるのは拘束・実働・深夜などの分数だけで、金額を含まない** ため、
//! `/kyuyo/*` の in-service gate は要らない。将来ここに金額を足すことになったら、
//! その時点で `/kyuyo/*` と同じ in-service gate へ移すこと。
//!
//! ## まだ外から叩けない
//!
//! この口は auth-worker (`ippoan/auth-worker`) の `/ichibanboshi-proxy` allowlist に
//! **まだ登録されていない** (path + method 完全一致の allowlist で、別 repo・別タスク)。
//! 本番露出を広げる判断はこの PR と分けているため、登録は別タスク。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::kintai_push::KintaiPgStore;
use crate::routes::kintai::{is_valid_month, parse_driver};
use crate::routes::kintai_timecard::DynKintaiPgStore;

/// `?month=YYYY-MM[&driver=1051]`。`month` は必須、`driver` は任意
/// (省略時は全乗務員)。範囲指定は受けない — 1 か月のみ。
#[derive(Debug, Deserialize)]
pub struct DaySummariesQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
}

/// `[kintai_push]` が無効な instance では挿さらない。既存の
/// `routes::kintai_timecard` と同じ文言で 503 にする。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 対象月の `[月初, 翌月初)` を `DATE` の境界として返す。
///
/// `is_valid_month` が通した文字列 (年 4 桁・月 01-12) では常に `Some` になる —
/// 呼び出し側はそれを前提に `expect` してよい ([`crate::kintai_fold`] の
/// `month_date_bounds` と同じ形。読むだけのこのファイルからは書き込み側を
/// import できないため、同じロジックをここに持つ)。
fn month_date_bounds(month: &str) -> Option<(chrono::NaiveDate, chrono::NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = chrono::NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next))
}

/// 突合スクリプトがそのまま使えるよう、オンプレ基準ファイルと同じ 12 列を書く
/// (モジュール docs のとおり列名は一致済み)。
const SELECT_SQL: &str = r#"
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

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.day_summaries read failed: {e}"),
    )
}

fn row_to_entry(
    r: &sqlx::postgres::PgRow,
) -> Result<(String, serde_json::Value), (StatusCode, String)> {
    use sqlx::Row;
    let driver_cd = r.try_get::<i64, _>("driver_cd").map_err(db_err)?;
    let date = r.try_get::<String, _>("date").map_err(db_err)?;
    let shift_start_at = r.try_get::<String, _>("shift_start_at").map_err(db_err)?;
    let key = format!("{driver_cd}|{date}|{shift_start_at}");
    let get_i32 = |k: &str| r.try_get::<i32, _>(k).map_err(db_err);
    let value = serde_json::json!({
        "shift_source": r.try_get::<String, _>("shift_source").map_err(db_err)?,
        "restraint_minutes": get_i32("restraint_minutes")?,
        "working_minutes": get_i32("working_minutes")?,
        "break_minutes": get_i32("break_minutes")?,
        "rest_minus_minutes": get_i32("rest_minus_minutes")?,
        "statutory_minutes": get_i32("statutory_minutes")?,
        "within_statutory_overtime_minutes": get_i32("within_statutory_overtime_minutes")?,
        "overtime_minutes": get_i32("overtime_minutes")?,
        "legal_holiday_minutes": get_i32("legal_holiday_minutes")?,
        "night_minutes": get_i32("night_minutes")?,
        "overtime_night_minutes": get_i32("overtime_night_minutes")?,
        "legal_holiday_night_minutes": get_i32("legal_holiday_night_minutes")?,
    });
    Ok((key, value))
}

/// GET /api/kintai/day-summaries?month=YYYY-MM[&driver=1051] — 畳んだ日別サマリの
/// 読み出し (Refs #205 の 18)。
///
/// `kintai.day_summaries` を読むだけで、突合が使うキー構成
/// (`乗務員CD|暦日|開始時刻`) と列名をそのまま返す。データが 0 件の月は
/// **404 ではなく 200 + 空の `summaries`** — 空と「この口が無い」を混ぜない。
pub async fn day_summaries(
    Query(params): Query<DaySummariesQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(i64::try_from(d).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    format!("driver は乗務員CD (数字) で指定してください: {e}"),
                )
            })?),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "driver は乗務員CD (数字) で指定してください".to_string(),
                ))
            }
        },
    };
    let store = store(&pg)?;
    let (from, to) = month_date_bounds(&month).expect("month validated by is_valid_month");
    let rows = sqlx::query(SELECT_SQL)
        .bind(store.tenant_id())
        .bind(from)
        .bind(to)
        .bind(driver)
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;
    let mut summaries = serde_json::Map::with_capacity(rows.len());
    for r in &rows {
        let (key, value) = row_to_entry(r)?;
        summaries.insert(key, value);
    }
    let count = summaries.len();
    tracing::info!(month = %month, rows = count, "kintai day-summaries read");
    Ok(Json(serde_json::json!({
        "month": month,
        "rows": count,
        "summaries": summaries,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_bounds_within_year() {
        assert_eq!(
            month_date_bounds("2026-06"),
            Some((
                chrono::NaiveDate::from_ymd_opt(2026, 6, 1).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2026, 7, 1).unwrap(),
            ))
        );
    }

    #[test]
    fn month_bounds_rolls_over_year() {
        assert_eq!(
            month_date_bounds("2026-12"),
            Some((
                chrono::NaiveDate::from_ymd_opt(2026, 12, 1).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap(),
            ))
        );
    }

    #[test]
    fn store_missing_is_service_unavailable() {
        let pg: DynKintaiPgStore = None;
        let (status, msg) = store(&pg).expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"));
    }
}
