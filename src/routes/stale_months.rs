//! 月別 stale (畳み直しが要るか) を返す軽い口 (Refs #620 の 1)。
//!
//! `/restraint-wage` の月タブに「この月は畳み直しが要るか」を出したい。ところが本物の
//! 突合 (`GET /restraint-api/kintai/diff?month=`) は約 50 秒かかり、12 か月ぶんで
//! 10 分になるので丸には使えない。#620 で親が調査済みのとおり **50 秒の正体は
//! `with_unko_diff_sink` / `with_unsplit_sink` (alc の etags 掃引) と fold であって、
//! `stale` はそこに相乗りしているだけ** — [`crate::kintai_fold::stale_state`] は
//! Postgres 1 往復のみで alc も R2 も etags も触らない。切り離せるので、この口は
//! 既存の `STALE_STATE_SQL` 相当を月単位の `GROUP BY` に書き換えるだけで組む
//! (**新しい計算は無い**)。
//!
//! ## ファイル名は `stale_months.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` はディレクトリ + ファイル名前方一致
//! (`("src","kosoku")` / `("src","kintai")` / `("src/routes","kintai")`) で
//! `logic_version` の指紋を作る。ここに入ると 1 バイトの変更でも全乗務員・全月が
//! stale になり、収束に全月ぶんの `run_kintai_recalc` が要る (高くつく)。パスに
//! `kintai` が入っても glob が見るのはファイル名だけなので `/api/kintai/stale-months`
//! は問題ない (`dtako_day.rs` と同じ分類)。
//!
//! ## 1 クエリで月別に返す
//!
//! 月の数だけ往復すると 12 か月で 12 往復になり、この口を作る意味が消える。
//! `generate_series` で対象範囲の月を並べ、`kintai.day_summaries` を月単位で
//! `GROUP BY` した集計を `LEFT JOIN` する — データが 1 行も無い月も
//! `stale_drivers: 0` で必ず出る (月タブが穴無く埋まる)。
//!
//! ## 対象範囲の既定
//!
//! `?from=YYYY-MM&to=YYYY-MM` (どちらも省略可、両端を含む)。
//!
//! - `to` 省略時は当月 (JST)
//! - `from` 省略時は `to` から遡って [`DEFAULT_WINDOW_MONTHS`] (= 12) か月
//! - 範囲が [`MAX_WINDOW_MONTHS`] (= 36) か月を超えると 400 (乱用防止。
//!   `kintai_recalc::MAX_MAX_FOLD_DRIVERS` と同じ考え方)
//!
//! **既定値は応答にも載る** (`from` / `to` / `default_window_months`)。
//!
//! ## 認可 — 読む先は `kintai_day_summaries.rs` と同じテーブル、同じテナント解決
//!
//! この口が読むのは [`crate::routes::kintai_day_summaries`] と同じ
//! `kintai.day_summaries`。**`X-Tenant-ID` は読まない** — テナントは
//! `ReadTenant` (= `[kintai_events] tenant_id` の設定 pin) が正で、無ければ
//! `[kintai_push] tenant_id` の pin に落ちる。`kintai_recalc.rs` の `store_for`
//! (ヘッダ由来) を使わないのは、本番 GCP が `[kintai_push] tenant_id` を設定しない
//! 運用 (受け口が `X-Tenant-ID` で名乗る) で、`store.tenant_id()` を直接 bind すると
//! 本番で常に 0 件になっていた事故 (Refs #205 の 23) と同じ形になるため。
//! それ以外 (CF Access Service Token が edge / `[kintai_push]` 無効なら 503) は
//! 既存の kintai 系の口と同じ。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::{Datelike, NaiveDate};
use serde::Deserialize;

use crate::kintai_fold::logic_version;
use crate::kintai_push::{KintaiPgStore, JST_OFFSET_SECONDS};
use crate::kosoku::KosokuParams;
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// 範囲省略時の窓の広さ (月数)。
pub const DEFAULT_WINDOW_MONTHS: i32 = 12;

/// 範囲の上限 (月数)。乱用防止 (`kintai_recalc::MAX_MAX_FOLD_DRIVERS` と同じ考え方)。
pub const MAX_WINDOW_MONTHS: i32 = 36;

/// `?from=YYYY-MM&to=YYYY-MM`。どちらも省略可、両端を含む。
#[derive(Debug, Default, Deserialize)]
pub struct StaleMonthsQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    /// テストだけが直接埋める「今日」。HTTP の query からは絶対に上書きできない
    /// (`RecalcRequest::today` と同じ形、Refs #286-1)。
    #[serde(skip)]
    pub today: Option<NaiveDate>,
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.into())
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
/// (モジュール docs の「認可」参照) — **どちらも無ければ 503**。
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

/// "YYYY-MM" を月初の `NaiveDate` に。
fn month_start(month: &str) -> Option<NaiveDate> {
    if !is_valid_month(month) {
        return None;
    }
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    NaiveDate::from_ymd_opt(year, mm, 1)
}

/// 月初から `delta` か月ずらした月初 (`delta` は負数も可)。
fn add_months(d: NaiveDate, delta: i32) -> NaiveDate {
    let total = d.year() * 12 + d.month0() as i32 + delta;
    let year = total.div_euclid(12);
    let month0 = total.rem_euclid(12) as u32;
    NaiveDate::from_ymd_opt(year, month0 + 1, 1).expect("normalized month is always valid")
}

/// `[from, to]` (両端含む) を `[lo, hi)` の `DATE` 境界に解決する。
fn resolve_range(
    q: &StaleMonthsQuery,
    today: NaiveDate,
) -> Result<(NaiveDate, NaiveDate), (StatusCode, String)> {
    let to_month = match &q.to {
        Some(s) => month_start(s).ok_or_else(|| bad_request("to は YYYY-MM で指定してください"))?,
        None => {
            NaiveDate::from_ymd_opt(today.year(), today.month(), 1).expect("today is a valid date")
        }
    };
    let from_month = match &q.from {
        Some(s) => {
            month_start(s).ok_or_else(|| bad_request("from は YYYY-MM で指定してください"))?
        }
        None => add_months(to_month, -(DEFAULT_WINDOW_MONTHS - 1)),
    };
    if from_month > to_month {
        return Err(bad_request("from は to 以前にしてください"));
    }
    let span = (to_month.year() - from_month.year()) * 12
        + (to_month.month0() as i32 - from_month.month0() as i32)
        + 1;
    // span はメッセージに含めない (1 行に収めて CLAUDE.md の折り返し罠を避けるため)
    if span > MAX_WINDOW_MONTHS {
        return Err(bad_request(format!("月範囲 上限{MAX_WINDOW_MONTHS}")));
    }
    Ok((from_month, add_months(to_month, 1)))
}

/// JST の「今日」。テストは [`StaleMonthsQuery::today`] で固定値を渡す。
fn jst_today() -> NaiveDate {
    let jst = chrono::FixedOffset::east_opt(JST_OFFSET_SECONDS).expect("JST offset is in range");
    chrono::Utc::now().with_timezone(&jst).date_naive()
}

/// [`crate::kintai_fold::stale_state`] の月単位版。式は 1 文字も変えず、
/// `GROUP BY date_trunc('month', date)` に割っただけ (モジュール docs 参照)。
/// `months` の CTE が対象範囲の月を並べるので、データの無い月も 1 往復のまま返る。
const STALE_MONTHS_SQL: &str = r#"
WITH months AS (
    SELECT generate_series($2::date, $3::date - interval '1 month', interval '1 month')::date AS month
), agg AS (
    SELECT date_trunc('month', date)::date AS month,
           count(DISTINCT driver_cd) FILTER (WHERE logic_version <> $4) AS stale_drivers
      FROM kintai.day_summaries
     WHERE tenant_id = $1 AND date >= $2 AND date < $3
     GROUP BY date_trunc('month', date)
)
SELECT to_char(m.month, 'YYYY-MM') AS month,
       coalesce(a.stale_drivers, 0)::bigint AS stale_drivers
  FROM months m
  LEFT JOIN agg a ON a.month = m.month
 ORDER BY m.month
"#;

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.day_summaries stale read failed: {e}"),
    )
}

/// GET /api/kintai/stale-months?from=YYYY-MM&to=YYYY-MM — 月ごとの stale 乗務員数を
/// 1 往復で返す (Refs #620 の 1)。**畳み直しは起こさない** — 全量再計算は別の口
/// (`POST /api/kintai/recalc`) の仕事で、ここは「要るかどうか」だけを答える。
pub async fn stale_months(
    Query(q): Query<StaleMonthsQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
    Extension(params): Extension<std::sync::Arc<KosokuParams>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let today = q.today.unwrap_or_else(jst_today);
    let (lo, hi) = resolve_range(&q, today)?;
    let store = store(&pg)?;
    let tenant = read_tenant_of(read_tenant, store.tenant_id())?;
    let version = logic_version(&params);

    use sqlx::Row;
    let rows = sqlx::query(STALE_MONTHS_SQL)
        .bind(tenant)
        .bind(lo)
        .bind(hi)
        .bind(&version)
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;
    let months: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "month": r.get::<String, _>("month"),
                "stale_drivers": r.get::<i64, _>("stale_drivers"),
            })
        })
        .collect();

    let n = months.len();
    tracing::info!(n, "kintai stale-months read");
    Ok(Json(serde_json::json!({
        "logic_version": version,
        "from": lo.format("%Y-%m").to_string(),
        "to": add_months(hi, -1).format("%Y-%m").to_string(),
        "default_window_months": DEFAULT_WINDOW_MONTHS,
        "months": months,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ym(y: i32, m: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, 1).unwrap()
    }

    #[test]
    fn month_start_rejects_bad_shapes() {
        assert_eq!(month_start("2026-06"), Some(ym(2026, 6)));
        for bad in ["2026-6", "", "2026-13", "2026/06"] {
            assert_eq!(month_start(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn add_months_rolls_forward_and_backward_across_years() {
        assert_eq!(add_months(ym(2026, 6), 1), ym(2026, 7));
        assert_eq!(add_months(ym(2026, 12), 1), ym(2027, 1));
        assert_eq!(add_months(ym(2026, 1), -1), ym(2025, 12));
        assert_eq!(add_months(ym(2026, 6), -11), ym(2025, 7));
    }

    /// **既定 = 当月から遡って 12 か月ぶん (両端含む)。**
    #[test]
    fn default_range_is_twelve_months_ending_this_month() {
        let q = StaleMonthsQuery::default();
        let (lo, hi) = resolve_range(&q, ym(2026, 6)).unwrap();
        assert_eq!(lo, ym(2025, 7));
        assert_eq!(hi, ym(2026, 7));
    }

    #[test]
    fn to_only_shifts_the_default_twelve_month_window() {
        let q = StaleMonthsQuery {
            to: Some("2026-03".to_string()),
            ..Default::default()
        };
        let (lo, hi) = resolve_range(&q, ym(2026, 6)).unwrap();
        assert_eq!(lo, ym(2025, 4));
        assert_eq!(hi, ym(2026, 4));
    }

    #[test]
    fn explicit_from_and_to_are_both_honored() {
        let q = StaleMonthsQuery {
            from: Some("2026-01".to_string()),
            to: Some("2026-03".to_string()),
            ..Default::default()
        };
        let (lo, hi) = resolve_range(&q, ym(2026, 6)).unwrap();
        assert_eq!(lo, ym(2026, 1));
        assert_eq!(hi, ym(2026, 4));
    }

    #[test]
    fn a_single_month_range_is_allowed() {
        let q = StaleMonthsQuery {
            from: Some("2026-03".to_string()),
            to: Some("2026-03".to_string()),
            ..Default::default()
        };
        let (lo, hi) = resolve_range(&q, ym(2026, 6)).unwrap();
        assert_eq!(lo, ym(2026, 3));
        assert_eq!(hi, ym(2026, 4));
    }

    #[test]
    fn from_after_to_is_bad_request() {
        let q = StaleMonthsQuery {
            from: Some("2026-06".to_string()),
            to: Some("2026-01".to_string()),
            ..Default::default()
        };
        let (status, msg) = resolve_range(&q, ym(2026, 6)).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("from"), "{msg}");
    }

    #[test]
    fn a_malformed_from_is_bad_request() {
        let q = StaleMonthsQuery {
            from: Some("nope".to_string()),
            ..Default::default()
        };
        let (status, msg) = resolve_range(&q, ym(2026, 6)).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("from"), "{msg}");
    }

    #[test]
    fn a_malformed_to_is_bad_request() {
        let q = StaleMonthsQuery {
            to: Some("nope".to_string()),
            ..Default::default()
        };
        let (status, msg) = resolve_range(&q, ym(2026, 6)).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("to"), "{msg}");
    }

    /// 37 か月は上限 (36) を超える。
    #[test]
    fn a_range_wider_than_the_cap_is_bad_request() {
        let q = StaleMonthsQuery {
            from: Some("2023-01".to_string()),
            to: Some("2026-01".to_string()),
            ..Default::default()
        };
        let (status, msg) = resolve_range(&q, ym(2026, 6)).unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("36"), "{msg}");
    }

    /// 上限ちょうど (36 か月) は通る。
    #[test]
    fn a_range_exactly_at_the_cap_is_allowed() {
        let q = StaleMonthsQuery {
            from: Some("2023-02".to_string()),
            to: Some("2026-01".to_string()),
            ..Default::default()
        };
        assert!(resolve_range(&q, ym(2026, 6)).is_ok());
    }

    fn uuid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    #[test]
    fn read_tenant_wins_over_the_write_pin() {
        assert_eq!(
            read_tenant_of(ReadTenant(Some(uuid(1))), uuid::Uuid::nil()),
            Ok(uuid(1))
        );
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
    fn store_missing_is_service_unavailable() {
        let pg: DynKintaiPgStore = None;
        let (status, msg) = store(&pg).expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }

    #[tokio::test]
    async fn the_handler_fails_closed_without_a_store() {
        let (status, _msg) = stale_months(
            Query(StaleMonthsQuery::default()),
            Extension(None),
            Extension(ReadTenant(None)),
            Extension(std::sync::Arc::new(KosokuParams::default())),
        )
        .await
        .expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// `today` を注入していない既定経路も一応通ることを確認する
    /// (`jst_today` を含む分岐のコンパイル・実行が壊れていないか)。
    #[test]
    fn jst_today_returns_a_date() {
        let d = jst_today();
        assert!(d.year() >= 2026);
    }
}
