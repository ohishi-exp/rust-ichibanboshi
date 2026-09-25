//! 同じ乗務員の勤務の時間帯が重なっている (かぶり) 組を返す読み出し口
//! (Refs ohishi-exp/nuxt-dtako-admin#1123)。
//!
//! 最低賃金の検証タブの条件 3 (今は `day_parts.rs` が返す「乗務員 × 暦日の拘束の
//! 合計 > 1440」) は、8:00〜18:00 と 10:00〜20:00 のように **合計が 24h を超えない
//! かぶり**を見逃す。ここはその置き換え用に、`kintai.shifts` (`start_at` / `end_at`、
//! `CHECK(end_at > start_at)`) の保存値だけを見て**開区間で重なる 2 本の組**を返す。
//!
//! **判定は保存値の比較だけ。** フェリー按分・打ち切り・新しい式は一切入れない
//! (フェリー按分は暦日集計の拘束を減らすだけで `shifts.end_at` は書き換えないため、
//! この 2 列は信用できる — ユーザー確定)。
//!
//! ## 組の条件
//!
//! - 同じ `tenant_id`・同じ `driver_cd` の 2 本 `a` / `b` (`a.start_at < b.start_at`
//!   で順序を固定し、逆順の重複や自分自身との組を避ける)
//! - `a.start_at < b.start_at AND b.start_at < a.end_at` (開区間。`a.end_at = b.start_at`
//!   の「接しているだけ」は重なりではない — `kosoku.rs` の `overlaps()` と同じ意味)
//! - **組は、後から始まる `b` の開始 (JST) が対象月に入るものだけ**返す
//!   (`b.start_at >= 月初 JST AND b.start_at < 翌月初 JST`)。`a` は前月に始まってよい —
//!   `a` の候補は `a.end_at > 月初 (JST) AND a.start_at < 翌月初 (JST)` で拾う
//!   (★ 勤務の長さに上限を置かない。「月初 − N 日」のような下限を置くと、終業が
//!   欠けて長くなった異常な勤務ほど見落とす)
//!
//! **読むだけ。1 行も書かない。**
//!
//! ## ファイル名は `shift_overlaps.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` はディレクトリ + ファイル名前方一致
//! (`("src","kosoku")` / `("src","kintai")` / `("src/routes","kintai")`) で
//! `logic_version` の指紋を作る。ここに入ると 1 バイトの変更でも全乗務員・全月が
//! stale になる (`day_parts.rs` / `stale_months.rs` と同じ分類)。
//!
//! ## ★ auth-worker の allowlist に載る
//!
//! この口は ippoan/auth-worker の `ichibanboshi-proxy` の allowlist に載る。
//! 載せてよい根拠は、受け口が設定 pin でテナントを決め `X-Tenant-ID` を読まないこと、
//! 返すのが時刻の組だけで金額を含まないこと。**`X-Tenant-ID` でテナントを決めるように
//! 変えたら allowlist から外すこと。**
//!
//! ## 認可 — テナントは設定 pin
//!
//! [`read_tenant_of`] は `day_parts::read_tenant_of` と同じ形 (向こうを pub にすると
//! `logic_version` が動くため写しを持つ)。正は `[kintai_events] tenant_id`、無ければ
//! `[kintai_push] tenant_id`、どちらも無ければ 503。`[kintai_push]` が無効 (オンプレ)
//! なら 503。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::Deserialize;

use crate::kintai_push::{jst_day_bounds, KintaiPgStore};
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// `?month=YYYY-MM` (必須)。乗務員指定は受けない。
#[derive(Debug, Default, Deserialize)]
pub struct ShiftOverlapsQuery {
    pub month: Option<String>,
}

/// `[kintai_push]` が無効な instance では挿さらない。`day_parts::store` と同じ文言で
/// 503 にする。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 読み先のテナント。`day_parts::read_tenant_of` と同じ形 (モジュール docs の
/// 「認可」参照。**`X-Tenant-ID` は読まない**) — **どちらも無ければ 503**。
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

/// 対象月の `[月初, 翌月初)` を JST の `TIMESTAMPTZ` 対で返す。`kintai_timecard::month_bounds`
/// と同じ形 (向こうは private かつ `src/routes/kintai*` は触らない対象なので写しを持つ)。
fn month_bounds(month: &str) -> Option<(DateTime<FixedOffset>, DateTime<FixedOffset>)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((jst_day_bounds(first).0, jst_day_bounds(next).0))
}

/// 同じ乗務員の 2 本 `a` / `b` の自己結合。モジュール docs の「組の条件」参照。
const SELECT_SQL: &str = r#"
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

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.shifts read failed: {e}"),
    )
}

fn row_to_item(r: &sqlx::postgres::PgRow) -> Result<serde_json::Value, (StatusCode, String)> {
    use sqlx::Row;
    Ok(serde_json::json!({
        "driver_cd": r.try_get::<i64, _>("driver_cd").map_err(db_err)?,
        "a_start": r.try_get::<String, _>("a_start").map_err(db_err)?,
        "a_end": r.try_get::<String, _>("a_end").map_err(db_err)?,
        "b_start": r.try_get::<String, _>("b_start").map_err(db_err)?,
        "b_end": r.try_get::<String, _>("b_end").map_err(db_err)?,
    }))
}

/// GET /api/kintai/shift-overlaps?month=YYYY-MM — 同じ乗務員の勤務が重なっている組
/// (モジュール docs) を返す。データが 0 件の月は **200 + 空の `items`**。
pub async fn shift_overlaps(
    Query(params): Query<ShiftOverlapsQuery>,
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
    let store = store(&pg)?;
    let tenant = read_tenant_of(read_tenant, store.tenant_id())?;
    let (from, to) = month_bounds(&month).expect("month validated by is_valid_month");
    let rows = sqlx::query(SELECT_SQL)
        .bind(tenant)
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
    tracing::info!(month = %month, n, "kintai shift-overlaps read");
    Ok(Json(serde_json::json!({
        "month": month,
        "items": items,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).unwrap()
    }

    #[test]
    fn month_bounds_within_year() {
        assert_eq!(
            month_bounds("2026-06"),
            Some((
                jst_day_bounds(ymd(2026, 6, 1)).0,
                jst_day_bounds(ymd(2026, 7, 1)).0
            ))
        );
    }

    #[test]
    fn month_bounds_rolls_over_year() {
        assert_eq!(
            month_bounds("2026-12"),
            Some((
                jst_day_bounds(ymd(2026, 12, 1)).0,
                jst_day_bounds(ymd(2027, 1, 1)).0
            ))
        );
    }

    fn uuid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
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

    #[tokio::test]
    async fn a_malformed_month_is_bad_request() {
        for bad in [None, Some(""), Some("2026-6"), Some("2026-13")] {
            let q = ShiftOverlapsQuery {
                month: bad.map(str::to_string),
            };
            let (status, msg) =
                shift_overlaps(Query(q), Extension(None), Extension(ReadTenant(None)))
                    .await
                    .expect_err("must reject a bad month");
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}");
            assert!(msg.contains("month"), "{msg}");
        }
    }

    #[tokio::test]
    async fn the_handler_fails_closed_without_a_store() {
        let q = ShiftOverlapsQuery {
            month: Some("2026-06".to_string()),
        };
        let (status, msg) = shift_overlaps(Query(q), Extension(None), Extension(ReadTenant(None)))
            .await
            .expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }
}
