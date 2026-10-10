//! 賃金確定値の月次スナップショットの HTTP + SQL (Refs #291、
//! ohishi-exp/nuxt-dtako-admin#677)。判断は [`crate::wage_snapshot`] に置き、
//! ここは「受ける・引く・書く」だけを持つ。
//!
//! - `POST /api/kintai/wage-snapshot` — 画面が確定させた 1 か月ぶんを置き換え保存
//! - `GET  /api/kintai/wage-range` — 期間の月別 + 合計 + カバレッジを 1 往復で返す
//!
//! ## なぜ `/api/kintai/*` なのか (金額を返すのに)
//!
//! [`crate::routes::kintai_day_summaries`] のモジュール docs は、金額を足すなら
//! in-service gate を持つ側へ移すよう指示していた。一度オンプレ版の給与の口に置いたが、
//! **本番で 503 になった** (2026-08-05。給与の口はその後 Worker へ移り、オンプレ版は撤去):
//!
//! | | Supabase 接続 (`[kintai_push]`) | in-service gate |
//! |---|---|---|
//! | オンプレ (ohishi-data) | 無い | あった |
//! | GCP Cloud Run (`/api/kintai/*` の宛先) | ある | 無い |
//!
//! この表が読み書きする `kintai.wage_snapshot` は Supabase にあり、そこへ繋がるのは
//! GCP のインスタンスだけ。**Supabase の接続情報を ohishi-data (local) には置かない**
//! 方針なので (auth-worker 1 箇所に資格情報を集約する設計)、口は GCP 側に置くしかない。
//!
//! ## 代わりに何が守っているか
//!
//! GCP の Cloud Run は `--no-allow-unauthenticated` で、到達できるのは auth-worker の
//! `/ichibanboshi-proxy` が OIDC を mint した呼び出しだけ。その手前で
//! `dtako-scraper-relay` の `restraint-api` が auth-worker JWT + 閲覧者 email で
//! 認可している。**edge の CF Access だけに寄りかかってはいない。**
//!
//! in-service gate をここに掛けるには GCP 側に introspect と
//! allowlist の設定を配る必要があり、それは「資格情報を増やさない」方針と衝突する。
//!
//! ## テナントは設定 pin (`X-Tenant-ID` を読まない)
//!
//! `kintai_day_summaries` / `stale_months` と同じ [`ReadTenant`]。ヘッダでテナントを
//! 選べる口にすると、auth-worker の `/ichibanboshi-proxy` allowlist (shared secret で
//! 通る) 経由で他テナントを引けてしまう。**ヘッダ由来に変えるなら同じ PR で
//! allowlist から外すこと。**
//!
//! ## 保存は「置き換え」
//!
//! 同じ `(tenant, comp_id, ym, restraint_source)` は 1 トランザクションで
//! DELETE → INSERT する。UPSERT にすると、**その月から消えた乗務員の行が残る** —
//! 退職者が期間集計にいつまでも出続けることになる。
//!
//! ## 同じ内容なら書かない
//!
//! 画面は月タブを行き来するたびに保存を投げる。内容が前回と同じなら
//! `skipped_unchanged: true` を返して DB に触らない (`computed_at` も動かさない —
//! 動かすと「いつ計算した値か」が読めなくなる)。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::kintai_push::KintaiPgStore;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};
use crate::wage_snapshot::{
    add_months, validate_snapshot, SnapshotRequest, ValidSnapshot, WageSnapshotRow,
};
// SQL・検査・判定・応答・bind の束は勤怠 Worker と共有する (`kintai-logic`、Refs #322。写さない)。
// ここは sqlx の bind と transaction だけ
use kintai_logic::wage_range::{to_fetched, FetchedRow, RangeRow};
pub use kintai_logic::wage_range::{RangeQuery, SELECT_RANGE_SQL};
use kintai_logic::wage_write::{saved_response, unchanged_response, wage_columns};
pub use kintai_logic::wage_write::{DELETE_MONTH_SQL, INSERT_ROWS_SQL};

/// `[kintai_push]` が無効な instance では挿さらない。`kintai_day_summaries::store`
/// と同じ文言で 503 にする。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 読み書き先のテナント。`stale_months::read_tenant_of` と同じ形 — **どちらも無ければ 503**。
fn tenant_of(read: ReadTenant, pin: uuid::Uuid) -> Result<uuid::Uuid, (StatusCode, String)> {
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

fn bad_request(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.into())
}

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.wage_snapshot access failed: {e}"),
    )
}

/// SELECT の 1 行 → 保存の行 + その月の版 (型変換は `kintai_logic::wage_range::to_fetched`)。
fn to_range_row(r: &sqlx::postgres::PgRow) -> RangeRow {
    RangeRow {
        ym: r.get::<String, _>("ym"),
        row: WageSnapshotRow {
            driver_cd: r.get::<i64, _>("driver_cd"),
            driver_name: r.get::<String, _>("driver_name"),
            company: r.get("company"),
            branch_name: r.get("branch_name"),
            branch_code: r.get("branch_code"),
            job_name: r.get("job_name"),
            pay_kubun: r.get("pay_kubun"),
            hourly_rate: r.get("hourly_rate"),
            calc_base: r.get("calc_base"),
            calc_overtime: r.get("calc_overtime"),
            calc_total: r.get("calc_total"),
            paid_base: r.get("paid_base"),
            paid_overtime: r.get("paid_overtime"),
            working_minutes: r.get("working_minutes"),
            restraint_missing: r.get::<bool, _>("restraint_missing"),
        },
        salary_item_sha: r.get("salary_item_sha"),
        payroll_synced_at: r.get::<Option<DateTime<Utc>>, _>("payroll_synced_at"),
        wage_logic_version: r.get("wage_logic_version"),
        timecard_kosoku: r.get("timecard_kosoku"),
        computed_at: r.get::<Option<DateTime<Utc>>, _>("computed_at"),
    }
}

/// `payroll_synced_at` (RFC3339 文字列) を `TIMESTAMPTZ` に渡せる形へ
/// (検査は `kintai_logic::wage_write::parse_synced_at`。400 の文言も同じ)。
fn parse_synced_at(s: Option<&String>) -> Result<Option<DateTime<Utc>>, (StatusCode, String)> {
    kintai_logic::wage_write::parse_synced_at(s).map_err(|f| bad_request(f.body))
}

/// POST /api/kintai/wage-snapshot — 1 か月ぶんを置き換え保存する。
pub async fn put_wage_snapshot(
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
    Json(req): Json<SnapshotRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let valid = validate_snapshot(req).map_err(bad_request)?;
    let synced_at = parse_synced_at(valid.masters.payroll_synced_at.as_ref())?;
    let store = store(&pg)?;
    let tenant = tenant_of(read_tenant, store.tenant_id())?;

    // 既存と同じなら書かない (月タブを行き来するたびに書き込まないため)
    let existing = sqlx::query(SELECT_RANGE_SQL)
        .bind(tenant)
        .bind(&valid.comp_id)
        .bind(&valid.restraint_source)
        .bind(valid.ym)
        .bind(add_months(valid.ym, 1))
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;
    let fetched: Vec<FetchedRow> = existing
        .iter()
        .map(|r| to_fetched(to_range_row(r)))
        .collect();
    if let Some(skipped) = unchanged_response(&fetched, &valid) {
        return Ok(Json(skipped));
    }

    let saved = write_month(store, tenant, &valid, synced_at).await?;
    tracing::info!(saved, month = %valid.ym, "wage snapshot saved");
    Ok(Json(saved_response(saved, &valid)))
}

/// DELETE → INSERT を 1 トランザクションで。戻り値は入れた行数。
async fn write_month(
    store: &KintaiPgStore,
    tenant: uuid::Uuid,
    valid: &ValidSnapshot,
    synced_at: Option<DateTime<Utc>>,
) -> Result<usize, (StatusCode, String)> {
    let rows = &valid.rows;
    let mut tx = store.pool().begin().await.map_err(db_err)?;
    // BYPASSRLS の kintai_writer では不要だが、RLS の効くロールで動かしても
    // 同じ結果になるように必ず名乗る (`kintai_push` と同じ)
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    sqlx::query(DELETE_MONTH_SQL)
        .bind(tenant)
        .bind(&valid.comp_id)
        .bind(valid.ym)
        .bind(&valid.restraint_source)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    if !rows.is_empty() {
        let c = wage_columns(rows);
        sqlx::query(INSERT_ROWS_SQL)
            .bind(tenant)
            .bind(&valid.comp_id)
            .bind(valid.ym)
            .bind(&valid.restraint_source)
            .bind(valid.masters.salary_item_sha.as_deref())
            .bind(synced_at)
            .bind(&valid.wage_logic_version)
            .bind(valid.timecard_kosoku.as_deref())
            .bind(&c.driver_cd)
            .bind(&c.driver_name)
            .bind(&c.company)
            .bind(&c.branch_name)
            .bind(&c.branch_code)
            .bind(&c.job_name)
            .bind(&c.pay_kubun)
            .bind(&c.hourly_rate)
            .bind(&c.calc_base)
            .bind(&c.calc_overtime)
            .bind(&c.calc_total)
            .bind(&c.paid_base)
            .bind(&c.paid_overtime)
            .bind(&c.working_minutes)
            .bind(&c.restraint_missing)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }
    tx.commit().await.map_err(db_err)?;
    Ok(rows.len())
}

/// GET /api/kintai/wage-range — 期間の月別 + 合計 + カバレッジ
/// (検査・詰め直し・合算・応答は `kintai_logic::wage_range`)。
pub async fn wage_range(
    Query(q): Query<RangeQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let req = kintai_logic::wage_range::validate(q).map_err(|f| bad_request(f.body))?;
    let store = store(&pg)?;
    let tenant = tenant_of(read_tenant, store.tenant_id())?;

    let lo = req.months[0];
    let hi = add_months(*req.months.last().expect("months is not empty"), 1);
    let fetched = sqlx::query(SELECT_RANGE_SQL)
        .bind(tenant)
        .bind(&req.comp)
        .bind(&req.source)
        .bind(lo)
        .bind(hi)
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;

    let body = kintai_logic::wage_range::respond(&req, fetched.iter().map(to_range_row).collect());
    let rows = body["rows"].as_array().map_or(0, Vec::len);
    tracing::info!(months = req.months.len(), rows, "wage range read");
    Ok(Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_synced_at_accepts_rfc3339_and_rejects_garbage() {
        assert!(parse_synced_at(None).unwrap().is_none());
        assert!(parse_synced_at(Some(&"2026-02-03T09:12:00Z".to_string()))
            .unwrap()
            .is_some());
        let err = parse_synced_at(Some(&"2026/02/03".to_string())).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn tenant_prefers_read_pin_then_store_pin() {
        let read = uuid::Uuid::from_u128(1);
        let pin = uuid::Uuid::from_u128(2);
        assert_eq!(tenant_of(ReadTenant(Some(read)), pin).unwrap(), read);
        assert_eq!(tenant_of(ReadTenant(None), pin).unwrap(), pin);
        assert_eq!(
            tenant_of(ReadTenant(Some(uuid::Uuid::nil())), pin).unwrap(),
            pin
        );
        let err = tenant_of(ReadTenant(None), uuid::Uuid::nil()).unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn store_is_unavailable_without_kintai_push() {
        let err = store(&None).unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }
}
