//! 乗務員 1 人・1 か月ぶんの**勤務ごとの始業・終業・日別サマリ・実働でない区間・
//! 暦日の按分**を返す読み出し口 (Refs ohishi-exp/nuxt-dtako-admin#1133)。
//!
//! fold が一緒に書く 3 表 — `kintai.shifts` / `kintai.day_summaries` /
//! `kintai.day_parts` — の保存値を、勤務 1 本 = 1 要素に束ねて返す。
//! **読むだけ。計算はしない。1 行も書かない。** 金額は含まない。
//!
//! ## 対象
//!
//! - `month` (必須) と `driver` (必須)。**全員ぶんを 1 回で返す口にしない**
//! - **始業 (JST) がその月に入る勤務**を、始業の昇順で返す
//!   (`start_at >= 月初 JST AND start_at < 翌月初 JST`)
//! - ★ **前月末に始業して当月へまたぐ勤務は、前月の応答に出る。** 当月の暦日に
//!   掛かる分を漏らさず知りたい呼び手は、前月も読むこと
//!
//! ## 応答
//!
//! ```json
//! { "month": "2026-04", "driver_cd": 9001,
//!   "items": [ {
//!     "start_at": "2026-04-03 22:10:00", "end_at": "2026-04-04 09:05:00",
//!     "shift_source": "timecard",
//!     "summary": { "restraint_minutes": 655, "working_minutes": 595, "break_minutes": 60,
//!                  "rest_minus_minutes": 0, "statutory_minutes": 450,
//!                  "within_statutory_overtime_minutes": 30, "overtime_minutes": 115,
//!                  "legal_holiday_minutes": 0, "night_minutes": 350,
//!                  "overtime_night_minutes": 0, "legal_holiday_night_minutes": 0 },
//!     "non_working": [ { "start": "2026-04-04 02:00:00", "end": "2026-04-04 03:00:00",
//!                        "kind": "break_event" } ],
//!     "parts": [ { "date": "2026-04-03", "restraint_minutes": 110,
//!                  "working_minutes": 110, "night_minutes": 110 } ]
//!   } ] }
//! ```
//!
//! - 時刻は JST の `YYYY-MM-DD HH24:MI:SS` (空白区切り。`day-summaries` と同じ表記)
//! - `summary` は `kintai.day_summaries` の同じ勤務の行 (`shift_start_at` で突き合わせる)
//!   の 11 個の分数。**行が無ければ `null`**
//! - `non_working` は同じ行の `non_working` 列を**そのまま** — 始業〜終業のうち実働に
//!   数えなかった区間 (`[start, end)`、分の格子、始まりの昇順、互いに重ならない)。
//!   長さの和 = (`end_at` − `start_at`) − `summary.working_minutes`
//!   - `kind`: `rest` = 勤務の中に残った休息 / `break_event` = デジタコの休憩イベント
//!     (**実際の時刻**) / `lunch_window` = 運行に出ていない勤務の昼休憩の窓 12:00-13:00 /
//!     `off_hours` = 昼の窓に掛からない勤務のまん中に置く 1 時間
//!   - ★ **`lunch_window` と `off_hours` は勤怠の規則による推定で、実際に休んだ時刻では
//!     ない。** 1 つの勤務に出る休憩の種別は `rest` 以外に 1 つだけ
//!   - `[]` = 区間なし / **`null` = この列が出来る前に畳んだ行** (畳み直すと入る)。
//!     `summary` が `null` の勤務も `null`
//! - `parts` は `kintai.day_parts` のその勤務の行を**表に在るまま、暦日の昇順**で。
//!   **0 行のことも 1 行以上のことも在る** — 行数から勤務が何暦日にまたがるかを
//!   決めないこと (それは `start_at` / `end_at` の日付で分かる)
//! - ★ `parts[].night_minutes` は表の値そのまま = **所定内・法定内残業ぶんの深夜だけ**。
//!   時間外の深夜・法定休日の深夜は暦日ごとには保存されていない (勤務の合計は
//!   `summary` の `overtime_night_minutes` / `legal_holiday_night_minutes` に在る)。
//!   `parts[].working_minutes` は区分に依らない実働
//! - データが 0 件なら **200 + 空の `items`**
//!
//! ## ファイル名は `shift_days.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` に入ると `logic_version` の指紋が動き、全乗務員・
//! 全月が stale になる (`shift_overlaps.rs` のモジュール doc と同じ分類)。
//!
//! ## ★ auth-worker の allowlist に載る
//!
//! この口は ippoan/auth-worker の `ichibanboshi-proxy` の allowlist に載る。
//! 載せてよい根拠は、受け口が設定 pin でテナントを決め `X-Tenant-ID` を読まないこと、
//! 返すのが時刻と分数だけで金額を含まないこと。**`X-Tenant-ID` でテナントを決めるように
//! 変えたら allowlist から外すこと。**
//!
//! ## 認可 — テナントは設定 pin
//!
//! `shift_overlaps.rs` の `store` / `read_tenant_of` / `month_bounds` をそのまま呼ぶ
//! (写しを持たない)。`[kintai_push]` が無効なら 503、DB の失敗は 502。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::routes::kintai::{is_valid_month, parse_driver};
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};
use crate::routes::shift_overlaps::{month_bounds, read_tenant_of, store};

/// `?month=YYYY-MM&driver=<乗務員CD>`。**どちらも必須。**
#[derive(Debug, Default, Deserialize)]
pub struct ShiftDaysQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
}

/// 乗務員CD (数字のみ)。無い・空・非数字・桁溢れは `None`。
fn parse_driver_cd(raw: Option<&str>) -> Option<i64> {
    i64::try_from(parse_driver(raw?)?).ok()
}

/// `kintai.shifts` を起点に、日別サマリ (と同じ行の `non_working`) を LEFT JOIN、
/// 暦日の按分を勤務ごとに束ねる。
/// モジュール docs の「対象」「応答」参照。
const SELECT_SQL: &str = r#"
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

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai shift-days read failed: {e}"),
    )
}

fn row_to_item(r: &sqlx::postgres::PgRow) -> Result<serde_json::Value, (StatusCode, String)> {
    use sqlx::Row;
    Ok(serde_json::json!({
        "start_at": r.try_get::<String, _>("start_at").map_err(db_err)?,
        "end_at": r.try_get::<String, _>("end_at").map_err(db_err)?,
        "shift_source": r.try_get::<String, _>("shift_source").map_err(db_err)?,
        "summary": r.try_get::<Option<serde_json::Value>, _>("summary").map_err(db_err)?,
        "non_working": r.try_get::<Option<serde_json::Value>, _>("non_working").map_err(db_err)?,
        "parts": r.try_get::<serde_json::Value, _>("parts").map_err(db_err)?,
    }))
}

/// GET /api/kintai/shift-days?month=YYYY-MM&driver=<乗務員CD> — 勤務ごとの始業・終業・
/// 日別サマリ・実働でない区間・暦日の按分 (モジュール docs) を返す。データが 0 件なら **200 + 空の `items`**。
pub async fn shift_days(
    Query(params): Query<ShiftDaysQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let Some(driver_cd) = parse_driver_cd(params.driver.as_deref()) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "driver は乗務員CD (数字) で指定してください".to_string(),
        ));
    };
    let store = store(&pg)?;
    let tenant = read_tenant_of(read_tenant, store.tenant_id())?;
    let (from, to) = month_bounds(&month).expect("month validated by is_valid_month");
    let rows = sqlx::query(SELECT_SQL)
        .bind(tenant)
        .bind(driver_cd)
        .bind(from)
        .bind(to)
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;
    let items = rows
        .iter()
        .map(row_to_item)
        .collect::<Result<Vec<_>, _>>()?;
    let n = items.len();
    tracing::info!(month = %month, driver_cd, n, "kintai shift-days read");
    Ok(Json(serde_json::json!({
        "month": month,
        "driver_cd": driver_cd,
        "items": items,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(month: Option<&str>, driver: Option<&str>) -> Query<ShiftDaysQuery> {
        Query(ShiftDaysQuery {
            month: month.map(str::to_string),
            driver: driver.map(str::to_string),
        })
    }

    #[test]
    fn a_numeric_driver_is_accepted() {
        assert_eq!(parse_driver_cd(Some("9001")), Some(9001));
        assert_eq!(parse_driver_cd(Some("0")), Some(0));
    }

    #[test]
    fn a_missing_or_non_numeric_driver_is_rejected() {
        // 末尾 2 つは桁溢れ (u64 に入らない / u64 には入るが BIGINT に入らない)
        for bad in [
            None,
            Some(""),
            Some("abc"),
            Some("-1"),
            Some("90 01"),
            Some("99999999999999999999"),
            Some("9999999999999999999"),
        ] {
            assert_eq!(parse_driver_cd(bad), None, "{bad:?}");
        }
    }

    #[tokio::test]
    async fn a_malformed_month_is_bad_request() {
        for bad in [None, Some(""), Some("2026-4"), Some("2026-13")] {
            let q = query(bad, Some("9001"));
            let (status, msg) = shift_days(q, Extension(None), Extension(ReadTenant(None)))
                .await
                .expect_err("must reject a bad month");
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}");
            assert!(msg.contains("month"), "{msg}");
        }
    }

    #[tokio::test]
    async fn a_missing_or_malformed_driver_is_bad_request() {
        for bad in [None, Some(""), Some("abc"), Some("-1")] {
            let q = query(Some("2026-04"), bad);
            let (status, msg) = shift_days(q, Extension(None), Extension(ReadTenant(None)))
                .await
                .expect_err("must reject a bad driver");
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}");
            assert!(msg.contains("driver"), "{msg}");
        }
    }

    #[tokio::test]
    async fn the_handler_fails_closed_without_a_store() {
        let q = query(Some("2026-04"), Some("9001"));
        let (status, msg) = shift_days(q, Extension(None), Extension(ReadTenant(None)))
            .await
            .expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }
}
