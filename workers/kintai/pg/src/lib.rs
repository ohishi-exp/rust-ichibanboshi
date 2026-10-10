//! 勤怠 Worker の Supabase (勤怠スキーマ `kintai.*`) への書き込み (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の Cloud Run 版 (sqlx) の `KintaiPgStore::stored_day_signatures`・`replace_window`・`apply_timecard_batch`
//! (`src/kintai_push.rs`)・`change_log::record_changes`・`routes::wage_snapshot::put_wage_snapshot` と同じことを、
//! alc-worker-db の `PgClient::tenant_tx` の中で型付きの名前なしの文 (`query_typed` / `execute_typed`) だけで行う。
//! SQL・計画・bind の束は共有 crate (`kintai_kosoku::kintai_push`・`kintai_logic::{change_log, wage_write}`) のもので、
//! ここは `$n` に型を付けて流すだけ。書いた後の表の中身が root の経路と同じことは `tests/root_parity.rs` (実 DB)。
//!
//! - **1 回 = 1 transaction**: `BEGIN` → kit の `SET_TENANT` (`app.current_tenant_id` と `search_path`) →
//!   `statement_timeout` → 本文 → `COMMIT`。途中で失敗したら何も書かれない (root と同じ全か無か)
//! - `statement_timeout` は root の接続ごとの `SET statement_timeout` (既定 300 秒) を transaction の中の
//!   `set_config(.., true)` で再現する (Hyperdrive の接続は使い回されるので、接続ごとの SET は使わない)
//! - 全 SQL の `$1` はテナントの UUID (`tenant_id = $1`)。接続ロールは行単位の権限を素通りするので、これが
//!   他テナントに触れない唯一の担保。SQL は全部 `kintai.` で修飾済み (kit は `search_path = alc_api` にする)
//! - 型なしの `ANY($5)` は SQL を書き換えず、`Type::TEXT_ARRAY` を渡して型を決める

use std::collections::BTreeMap;

use alc_worker_db::{PgClient, TenantTx, TxOutput};
use chrono::{DateTime, FixedOffset, NaiveDate, Utc};
use kintai_kosoku::kintai_push::{
    delete_days, event_columns, plan_received_batch, DriverPlan, TimecardBatch,
    TimecardBatchResult, DELETE_DAYS_SQL, INSERT_EVENTS_SQL, PUSHED_SOURCES, STORED_SIGNATURES_SQL,
};
use kintai_logic::change_log::{
    build_changes, change_columns, old_event, INSERT_CHANGES_SQL, OLD_EVENTS_SQL,
};
use kintai_logic::wage_range::{to_fetched, FetchedRow, RangeRow, SELECT_RANGE_SQL};
use kintai_logic::wage_snapshot::{add_months, ValidSnapshot, WageSnapshotRow};
use kintai_logic::wage_write::{
    saved_response, unchanged_response, wage_columns, DELETE_MONTH_SQL, INSERT_ROWS_SQL,
};
use tokio_postgres::types::Type;
use tokio_postgres::{Error, Row};
use uuid::Uuid;

/// root の `[kintai_push] statement_timeout_secs` の既定 (300 秒) をミリ秒の文字列で。
pub const STATEMENT_TIMEOUT_MS: &str = "300000";

/// transaction の中だけ効く打ち切り時間 (`is_local = true`)。
pub const SET_STATEMENT_TIMEOUT: &str = "SELECT set_config('statement_timeout', $1, true)";

/// transaction の外へ持ち出す値の印 (kit の外の型に付けるための包み)。
struct Out<T>(T);

impl<T: Send + 'static> TxOutput for Out<T> {}

/// `ANY($n)` に渡す `source` の集合 (`text[]`)。
fn sources() -> Vec<String> {
    PUSHED_SOURCES.iter().map(|s| s.to_string()).collect()
}

async fn set_timeout(tx: &TenantTx<'_>) -> Result<(), Error> {
    tx.query_typed(
        SET_STATEMENT_TIMEOUT,
        &[(&STATEMENT_TIMEOUT_MS, Type::TEXT)],
    )
    .await
    .map(|_| ())
}

/// Postgres 側の (暦日, 署名)。root の `KintaiPgStore::stored_day_signatures` と同じ SQL・同じ値。
pub async fn stored_day_signatures(
    pg: &mut PgClient,
    tenant: Uuid,
    driver_cd: i64,
    from: DateTime<FixedOffset>,
    to: DateTime<FixedOffset>,
) -> Result<BTreeMap<NaiveDate, String>, Error> {
    let out = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                set_timeout(tx).await?;
                let src = sources();
                let rows = tx
                    .query_typed(
                        STORED_SIGNATURES_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&driver_cd, Type::INT8),
                            (&from, Type::TIMESTAMPTZ),
                            (&to, Type::TIMESTAMPTZ),
                            (&src, Type::TEXT_ARRAY),
                        ],
                    )
                    .await?;
                let sig = |r: &Row| -> Result<(NaiveDate, String), Error> {
                    Ok((r.try_get("d")?, r.try_get("sig")?))
                };
                rows.iter().map(sig).collect::<Result<_, _>>().map(Out)
            })
        })
        .await?;
    Ok(out.0)
}

/// 計画した日を delete-then-insert する。**1 transaction** で、消す前に旧 events を読んで変更履歴を残す
/// (root の `replace_window` + `change_log::record_changes` と同じ順)。戻り値は記録した変更の行数。
/// 置き換える日が無ければ DB に触らない。
pub async fn replace_window(
    pg: &mut PgClient,
    tenant: Uuid,
    plans: BTreeMap<i64, DriverPlan>,
) -> Result<usize, Error> {
    let days = delete_days(&plans);
    if days.is_empty() {
        return Ok(0);
    }
    let out = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                set_timeout(tx).await?;
                let src = sources();
                let day_params = [
                    (
                        &tenant as &(dyn tokio_postgres::types::ToSql + Sync),
                        Type::UUID,
                    ),
                    (&days.driver_cd, Type::INT8_ARRAY),
                    (&days.from, Type::TIMESTAMPTZ_ARRAY),
                    (&days.to, Type::TIMESTAMPTZ_ARRAY),
                    (&src, Type::TEXT_ARRAY),
                ];
                // 消す前に旧 events を読み、変わった日の前後を残す
                let old = tx.query_typed(OLD_EVENTS_SQL, &day_params).await?;
                let old_row = |r: &Row| -> Result<_, Error> {
                    Ok(old_event(
                        r.try_get("driver_cd")?,
                        r.try_get("at")?,
                        r.try_get("state")?,
                        r.try_get("source")?,
                        r.try_get("unko_no")?,
                    ))
                };
                let before = old.iter().map(old_row).collect::<Result<Vec<_>, _>>()?;
                let changes = build_changes(&before, &plans);
                if !changes.is_empty() {
                    let c = change_columns(&changes);
                    tx.execute_typed(
                        INSERT_CHANGES_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&c.driver_cd, Type::INT8_ARRAY),
                            (&c.date, Type::DATE_ARRAY),
                            (&c.before, Type::JSONB_ARRAY),
                            (&c.after, Type::JSONB_ARRAY),
                        ],
                    )
                    .await?;
                }
                tx.execute_typed(DELETE_DAYS_SQL, &day_params).await?;
                for c in event_columns(&plans) {
                    tx.execute_typed(
                        INSERT_EVENTS_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&c.driver_cd, Type::INT8_ARRAY),
                            (&c.occurred_at, Type::TIMESTAMPTZ_ARRAY),
                            (&c.state, Type::TEXT_ARRAY),
                            (&c.source, Type::TEXT_ARRAY),
                            (&c.unko_no, Type::TEXT_ARRAY),
                            (&c.raw, Type::JSONB_ARRAY),
                        ],
                    )
                    .await?;
                }
                Ok(Out(changes.len()))
            })
        })
        .await?;
    Ok(out.0)
}

/// [`apply_timecard_batch`] の失敗。
#[derive(Debug)]
pub enum ApplyError {
    /// 月が読めない (root の `KintaiPushError::NotConfigured` = 503。口は先に月を検査するので通常は起きない)
    BadMonth(String),
    /// DB の失敗 (502)
    Db(Error),
}

/// 受け取った 1 乗務員ぶんを反映する (root の `apply_timecard_batch` と同じ計画・同じ書き込み・同じ応答)。
pub async fn apply_timecard_batch(
    pg: &mut PgClient,
    tenant: Uuid,
    batch: &TimecardBatch,
) -> Result<TimecardBatchResult, ApplyError> {
    let (plans, result) = plan_received_batch(batch).map_err(ApplyError::BadMonth)?;
    replace_window(pg, tenant, plans)
        .await
        .map_err(ApplyError::Db)?;
    Ok(result)
}

/// `SELECT_RANGE_SQL` の 1 行 (Worker の `GET /api/kintai/wage-range` と保存の「前回と同じか」が共有する)。
pub fn range_row(r: &Row) -> Result<RangeRow, Error> {
    Ok(RangeRow {
        ym: r.try_get("ym")?,
        row: WageSnapshotRow {
            driver_cd: r.try_get("driver_cd")?,
            driver_name: r.try_get("driver_name")?,
            company: r.try_get("company")?,
            branch_name: r.try_get("branch_name")?,
            branch_code: r.try_get("branch_code")?,
            job_name: r.try_get("job_name")?,
            pay_kubun: r.try_get("pay_kubun")?,
            hourly_rate: r.try_get("hourly_rate")?,
            calc_base: r.try_get("calc_base")?,
            calc_overtime: r.try_get("calc_overtime")?,
            calc_total: r.try_get("calc_total")?,
            paid_base: r.try_get("paid_base")?,
            paid_overtime: r.try_get("paid_overtime")?,
            working_minutes: r.try_get("working_minutes")?,
            restraint_missing: r.try_get("restraint_missing")?,
        },
        salary_item_sha: r.try_get("salary_item_sha")?,
        payroll_synced_at: r.try_get("payroll_synced_at")?,
        wage_logic_version: r.try_get("wage_logic_version")?,
        timecard_kosoku: r.try_get("timecard_kosoku")?,
        computed_at: r.try_get("computed_at")?,
    })
}

/// 賃金スナップショットの 1 か月ぶんを置き換え保存する (root の `put_wage_snapshot` の DB 以降と同じ判定・書き込み・応答)。
/// 前回と同じなら書かずに `skipped_unchanged: true` を返す。読みと書きを **1 transaction** で行う。
pub async fn put_wage_snapshot(
    pg: &mut PgClient,
    tenant: Uuid,
    valid: ValidSnapshot,
    synced_at: Option<DateTime<Utc>>,
) -> Result<serde_json::Value, Error> {
    let out = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                set_timeout(tx).await?;
                let next = add_months(valid.ym, 1);
                let existing = tx
                    .query_typed(
                        SELECT_RANGE_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&valid.comp_id, Type::TEXT),
                            (&valid.restraint_source, Type::TEXT),
                            (&valid.ym, Type::DATE),
                            (&next, Type::DATE),
                        ],
                    )
                    .await?;
                let fetched: Vec<FetchedRow> = existing
                    .iter()
                    .map(range_row)
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .map(to_fetched)
                    .collect();
                if let Some(skipped) = unchanged_response(&fetched, &valid) {
                    return Ok(Out(skipped));
                }
                tx.execute_typed(
                    DELETE_MONTH_SQL,
                    &[
                        (&tenant, Type::UUID),
                        (&valid.comp_id, Type::TEXT),
                        (&valid.ym, Type::DATE),
                        (&valid.restraint_source, Type::TEXT),
                    ],
                )
                .await?;
                if !valid.rows.is_empty() {
                    let c = wage_columns(&valid.rows);
                    tx.execute_typed(
                        INSERT_ROWS_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&valid.comp_id, Type::TEXT),
                            (&valid.ym, Type::DATE),
                            (&valid.restraint_source, Type::TEXT),
                            (&valid.masters.salary_item_sha, Type::TEXT),
                            (&synced_at, Type::TIMESTAMPTZ),
                            (&valid.wage_logic_version, Type::TEXT),
                            (&valid.timecard_kosoku, Type::TEXT),
                            (&c.driver_cd, Type::INT8_ARRAY),
                            (&c.driver_name, Type::TEXT_ARRAY),
                            (&c.company, Type::TEXT_ARRAY),
                            (&c.branch_name, Type::TEXT_ARRAY),
                            (&c.branch_code, Type::INT4_ARRAY),
                            (&c.job_name, Type::TEXT_ARRAY),
                            (&c.pay_kubun, Type::INT2_ARRAY),
                            (&c.hourly_rate, Type::INT4_ARRAY),
                            (&c.calc_base, Type::INT4_ARRAY),
                            (&c.calc_overtime, Type::INT4_ARRAY),
                            (&c.calc_total, Type::INT4_ARRAY),
                            (&c.paid_base, Type::INT4_ARRAY),
                            (&c.paid_overtime, Type::INT4_ARRAY),
                            (&c.working_minutes, Type::INT4_ARRAY),
                            (&c.restraint_missing, Type::BOOL_ARRAY),
                        ],
                    )
                    .await?;
                }
                Ok(Out(saved_response(valid.rows.len(), &valid)))
            })
        })
        .await?;
    Ok(out.0)
}
