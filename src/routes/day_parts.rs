//! 暦日ビュー (`kintai.day_parts`) を 乗務員 × 暦日 で足して返す読み出し口
//! (Refs ohishi-exp/nuxt-dtako-admin#1123)。
//!
//! `kintai.day_parts` は勤務を 0 時で切って暦日に配った行 (1 行 ≤ 1440 の CHECK 付き、
//! `migrations/001_kintai_schema.sql`)。最低賃金の検証タブの条件 3 (同じ乗務員の同じ日に
//! 勤務が 2 本重なっていないか) は、**乗務員 × 暦日で `SUM` しただけ**の値が 1440 を
//! 超えるかで見る。これまで day_parts は書く口 (`kintai_fold` の `INSERT_DAY_PARTS_SQL`)
//! しか無く、読む口が 1 本も無かった。
//!
//! **読むだけ。1 行も書かない。計算は `SUM` だけ** — 按分・打ち切り・その他の式は
//! 入れない (定義はユーザー確定)。1440 を超えたかの判定も呼ぶ側に任せる。
//!
//! ## ファイル名は `day_parts.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` はディレクトリ + ファイル名前方一致
//! (`("src","kosoku")` / `("src","kintai")` / `("src/routes","kintai")`) で
//! `logic_version` の指紋を作る。ここに入ると 1 バイトの変更でも全乗務員・全月が
//! stale になる。パスに `kintai` が入っても glob が見るのはファイル名だけなので
//! `/api/kintai/day-parts` は問題ない (`stale_months.rs` / `unko_gaps.rs` と同じ分類)。
//!
//! ## ★ auth-worker の allowlist に載っている
//!
//! この口は ippoan/auth-worker の `ichibanboshi-proxy` の allowlist に載っている。
//! 載せてよい根拠は、受け口が設定 pin でテナントを決め `X-Tenant-ID` を読まないこと、
//! 返すのが分数だけで金額を含まないこと。**`X-Tenant-ID` でテナントを決めるように
//! 変えたら allowlist から外すこと。** 金額を足すなら `/kyuyo/*` と同じ in-service
//! gate へ移すこと (`kintai_day_summaries.rs` のモジュール doc と同じ線引き)。
//!
//! ## 認可 — テナントは設定 pin
//!
//! [`read_tenant_of`] は `kintai_day_summaries::read_tenant_of` と同じ形
//! (`stale_months.rs` と同じく写しを持つ — 向こうを pub にすると `logic_version` が
//! 動くため)。正は `[kintai_events] tenant_id`、無ければ `[kintai_push] tenant_id`、
//! どちらも無ければ 503。`[kintai_push]` が無効 (オンプレ) なら 503。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDate;
use serde::Deserialize;

use crate::kintai_push::KintaiPgStore;
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// `?month=YYYY-MM` (必須)。範囲指定・乗務員指定は受けない。
#[derive(Debug, Default, Deserialize)]
pub struct DayPartsQuery {
    pub month: Option<String>,
}

/// `[kintai_push]` が無効な instance では挿さらない。`kintai_day_summaries::store`
/// と同じ文言で 503 にする。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 読み先のテナント。`kintai_day_summaries::read_tenant_of` と同じ形
/// (モジュール docs の「認可」参照。**`X-Tenant-ID` は読まない**) — **どちらも無ければ 503**。
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

/// 対象月の `[月初, 翌月初)`。`kintai_day_summaries::month_date_bounds` と同じ形
/// (向こうは private で、pub にすると `logic_version` が動くため写しを持つ)。
fn month_date_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next))
}

/// 乗務員 × 暦日 の `SUM(restraint_minutes)`。**これ以外の計算はしない。**
const SELECT_SQL: &str = r#"
SELECT driver_cd,
       to_char(date, 'YYYY-MM-DD') AS date,
       SUM(restraint_minutes)::bigint AS restraint_minutes
  FROM kintai.day_parts
 WHERE tenant_id = $1
   AND date >= $2 AND date < $3
 GROUP BY driver_cd, date
 ORDER BY driver_cd, date
"#;

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.day_parts read failed: {e}"),
    )
}

fn row_to_item(r: &sqlx::postgres::PgRow) -> Result<serde_json::Value, (StatusCode, String)> {
    use sqlx::Row;
    Ok(serde_json::json!({
        "driver_cd": r.try_get::<i64, _>("driver_cd").map_err(db_err)?,
        "date": r.try_get::<String, _>("date").map_err(db_err)?,
        "restraint_minutes": r.try_get::<i64, _>("restraint_minutes").map_err(db_err)?,
    }))
}

/// GET /api/kintai/day-parts?month=YYYY-MM — `kintai.day_parts` を 乗務員 × 暦日 で
/// `SUM` して返す。データが 0 件の月は **200 + 空の `items`**。
pub async fn day_parts(
    Query(params): Query<DayPartsQuery>,
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
    let (from, to) = month_date_bounds(&month).expect("month validated by is_valid_month");
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
    tracing::info!(month = %month, n, "kintai day-parts read");
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
            month_date_bounds("2026-06"),
            Some((ymd(2026, 6, 1), ymd(2026, 7, 1)))
        );
    }

    #[test]
    fn month_bounds_rolls_over_year() {
        assert_eq!(
            month_date_bounds("2026-12"),
            Some((ymd(2026, 12, 1), ymd(2027, 1, 1)))
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
            let q = DayPartsQuery {
                month: bad.map(str::to_string),
            };
            let (status, msg) = day_parts(Query(q), Extension(None), Extension(ReadTenant(None)))
                .await
                .expect_err("must reject a bad month");
            assert_eq!(status, StatusCode::BAD_REQUEST, "{bad:?}");
            assert!(msg.contains("month"), "{msg}");
        }
    }

    #[tokio::test]
    async fn the_handler_fails_closed_without_a_store() {
        let q = DayPartsQuery {
            month: Some("2026-06".to_string()),
        };
        let (status, msg) = day_parts(Query(q), Extension(None), Extension(ReadTenant(None)))
            .await
            .expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }
}
