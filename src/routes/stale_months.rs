//! 月別 stale (畳み直しが要るか) を返す軽い口 (Refs #620 の 1)。
//!
//! `/restraint-wage` の月タブに「この月は畳み直しが要るか」を出したい。ところが本物の
//! 突合 (`GET /restraint-api/kintai/diff?month=`) は約 50 秒かかり、12 か月ぶんで
//! 10 分になるので丸には使えない。#620 で親が調査済みのとおり **50 秒の正体は
//! 月ゲートの etags 掃引 (運行の突合・`unsplit`) と fold であって、
//! `stale` はそこに相乗りしているだけ** — [`crate::kintai_fold::stale_state`] は
//! Postgres 1 往復のみで alc も R2 も etags も触らない。切り離せるので、この口は
//! 既存の `STALE_STATE_SQL` 相当を月単位の `GROUP BY` に書き換えるだけで組む
//! (**新しい計算は無い**)。
//!
//! ## ファイル名は `stale_months.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` はディレクトリ + ファイル名前方一致
//! (`("src","kosoku")` / `("src","kintai")` / `("src/routes","kintai")`) で
//! `/api/kintai/version` の etag を作る。ここに入ると 1 バイトの変更でも relay の上流キャッシュが
//! 全月無効になる。勤怠の版 (`logic_version`) は手で上げる `LOGIC_BASE`
//! なのでファイル名では動かない (Refs #322)。パスに
//! `kintai` が入っても glob が見るのはファイル名だけなので `/api/kintai/stale-months`
//! は問題ない (`dtako_day.rs` と同じ分類)。
//!
//! ## 1 クエリで月別に返す
//!
//! 月の数だけ往復すると 12 か月で 12 往復になり、この口を作る意味が消える。
//! `generate_series` で対象範囲の月を並べ、`kintai.day_summaries` を月単位で
//! `GROUP BY` した集計を `LEFT JOIN` する — データが 1 行も無い月も 1 往復のまま
//! `{ "stale_drivers": 0, "total_drivers": 0 }` で必ず出る (月タブが穴無く埋まる)。
//!
//! **`stale_drivers: 0` だけでは「収束済み」と「まだ 1 度も畳んでいない」を
//! 区別できない** (「応答に無い = 0」と同じ落とし穴)。そこで `total_drivers`
//! (版を問わない distinct driver_cd 数) を添える —
//! `total_drivers == 0` はデータが無い月、`total_drivers > 0 && stale_drivers == 0`
//! が本当の収束済み。
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
use chrono::NaiveDate;
use serde::Deserialize;

use crate::kintai_fold::logic_version;
use crate::kintai_push::KintaiPgStore;
use crate::kosoku::KosokuParams;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

// 範囲の解決・SQL・応答の組み立ては共有 crate (勤怠 Worker と同じもの、Refs #322)。ここは sqlx と HTTP の殻だけ
use kintai_logic::stale_months::{resolve_range, response, StaleMonthRow};
pub use kintai_logic::stale_months::{DEFAULT_WINDOW_MONTHS, MAX_WINDOW_MONTHS, STALE_MONTHS_SQL};

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
    let now_ms = || chrono::Utc::now().timestamp_millis() as u64;
    let today = q
        .today
        .unwrap_or_else(|| kintai_logic::mariadb_reads::jst_today(now_ms()));
    let (lo, hi) = resolve_range(q.from.as_deref(), q.to.as_deref(), today)
        .map_err(|m| (StatusCode::BAD_REQUEST, m))?;
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
    let rows: Vec<StaleMonthRow> = rows
        .iter()
        .map(|r| StaleMonthRow {
            month: r.get::<String, _>("month"),
            stale_drivers: r.get::<i64, _>("stale_drivers"),
            total_drivers: r.get::<i64, _>("total_drivers"),
        })
        .collect();

    let n = rows.len();
    tracing::info!(n, "kintai stale-months read");
    Ok(Json(response(&version, lo, hi, &rows)))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
