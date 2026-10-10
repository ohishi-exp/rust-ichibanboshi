//! Supabase (勤怠スキーマ `kintai.*`) を Hyperdrive (`KINTAI_HYPERDRIVE`) 経由で読む 5 本の口。
//!
//! 入力の検査・SQL・`$n` の型・応答の組み立ては kintai-logic (root の src/ の写し)。ここは DB との往復だけ:
//! 検査 (400) → binding (無ければ 503、繋がらなければ 502) → テナント (`KINTAI_TENANT_ID`、無ければ 503) →
//! `PgClient::tenant_tx` の中で `query_typed` → 行を owned な型に詰め直す → 応答 (200)。順は元の handler と同じ。
//!
//! - **テナントは設定 pin。`X-Tenant-ID` は読まない。** 接続ロールは BYPASSRLS なので、SQL の `WHERE tenant_id = $1`
//!   (`$1` = pin の UUID) が他テナントを見せない唯一の担保 (kintai-logic の `tests/common.rs` で確かめている)
//! - 名前付き prepared statement は使わない (`query_typed` / `query_typed_one` だけ。Hyperdrive では接続が切れる)
//! - 502 の本文は元の文言の頭 + kit の `kind` (SQLSTATE か固定の語)。DB の message・接続先は出さない

use alc_worker_db::hyperdrive;
use alc_worker_db::{kind, PgClient, TxOutput};
use kintai_logic::common::{db_fail, no_db, tenant_of, Fail, HYPERDRIVE_BINDING, TENANT_VAR};
use kintai_logic::wage_snapshot::WageSnapshotRow;
use kintai_logic::{change_log, day_summaries, shift_days, shift_overlaps, wage_range};
use tokio_postgres::{Error as PgError, Row};
use uuid::Uuid;
use worker::Env;

use crate::probe::Read;

/// transaction の外へ持ち出す値の印 (kintai-logic の行の型は kit の外なので、この crate の包みに付ける)。
struct Out<T>(T);

impl<T: Send + 'static> TxOutput for Out<T> {}

/// 口ごとに読んで応答の JSON を返す。
pub(crate) async fn serve(read: Read, query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    match read {
        Read::DaySummaries => day_summaries(query, env).await,
        Read::ShiftOverlaps => shift_overlaps(query, env).await,
        Read::ShiftDays => shift_days(query, env).await,
        Read::ChangeLog => change_log(query, env).await,
        Read::WageRange => wage_range(query, env).await,
    }
}

/// binding から繋ぎ、テナントを決める。binding が無い = 503 / 在るのに繋がらない = 502 / テナントが無い = 503。
async fn open(env: &Env, what: &str) -> Result<(PgClient, Uuid), Fail> {
    let pg = match hyperdrive::connect(env, HYPERDRIVE_BINDING).await {
        Ok(Some(pg)) => pg,
        Ok(None) => return Err(no_db()),
        // ConnectError の Display は binding 名・段・kind だけ (接続文字列・宛先を含まない)
        Err(e) => return Err(db_fail(what, &e.to_string())),
    };
    let raw = env.var(TENANT_VAR).ok().map(|v| v.to_string());
    let tenant = tenant_of(raw.as_deref())?;
    Ok((pg, tenant))
}

async fn day_summaries(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use day_summaries::{parse, respond, Binds, DB_WHAT, MINUTE_COLUMNS, SELECT_SQL};
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    let binds = Binds::new(tenant, &req);
    let rows = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(SELECT_SQL, &binds.params()).await?;
                let to_row = |r: &Row| -> Result<day_summaries::Row, PgError> {
                    let mut minutes = [0; 11];
                    for (m, name) in minutes.iter_mut().zip(MINUTE_COLUMNS) {
                        *m = r.try_get(name)?;
                    }
                    Ok(day_summaries::Row {
                        driver_cd: r.try_get("driver_cd")?,
                        date: r.try_get("date")?,
                        shift_start_at: r.try_get("shift_start_at")?,
                        shift_source: r.try_get("shift_source")?,
                        minutes,
                    })
                };
                rows.iter()
                    .map(to_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Out)
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    Ok(respond(&req.month, &rows.0))
}

async fn shift_overlaps(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use shift_overlaps::{parse, respond, Binds, DB_WHAT, SELECT_SQL};
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    let binds = Binds::new(tenant, &req);
    let rows = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(SELECT_SQL, &binds.params()).await?;
                let to_row = |r: &Row| -> Result<shift_overlaps::Row, PgError> {
                    Ok(shift_overlaps::Row {
                        driver_cd: r.try_get("driver_cd")?,
                        a_start: r.try_get("a_start")?,
                        a_end: r.try_get("a_end")?,
                        b_start: r.try_get("b_start")?,
                        b_end: r.try_get("b_end")?,
                    })
                };
                rows.iter()
                    .map(to_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Out)
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    Ok(respond(&req.month, &rows.0))
}

async fn shift_days(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use shift_days::{parse, respond, Binds, DB_WHAT, SELECT_SQL};
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    let binds = Binds::new(tenant, &req);
    let rows = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(SELECT_SQL, &binds.params()).await?;
                let to_row = |r: &Row| -> Result<shift_days::Row, PgError> {
                    Ok(shift_days::Row {
                        start_at: r.try_get("start_at")?,
                        end_at: r.try_get("end_at")?,
                        shift_source: r.try_get("shift_source")?,
                        summary: r.try_get("summary")?,
                        non_working: r.try_get("non_working")?,
                        parts: r.try_get("parts")?,
                    })
                };
                rows.iter()
                    .map(to_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Out)
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    Ok(respond(&req, rows.0))
}

async fn change_log(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use change_log::{parse, respond, Binds, DB_WHAT, SELECT_SQL, SINCE_SQL};
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    let binds = Binds::new(tenant, &req);
    let out = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(SELECT_SQL, &binds.params()).await?;
                let to_row = |r: &Row| -> Result<change_log::Row, PgError> {
                    Ok(change_log::Row {
                        driver_cd: r.try_get("driver_cd")?,
                        date: r.try_get("date")?,
                        recorded_at: r.try_get("recorded_at")?,
                        before: r.try_get("before")?,
                        after: r.try_get("after")?,
                    })
                };
                let changes = rows.iter().map(to_row).collect::<Result<Vec<_>, _>>()?;
                let since_row = tx.query_typed_one(SINCE_SQL, &binds.since_params()).await?;
                let since: Option<String> = since_row.try_get(0)?;
                Ok(Out((changes, since)))
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    let (changes, since) = out.0;
    Ok(respond(&req, since, changes))
}

async fn wage_range(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use wage_range::{parse, respond, Binds, RangeRow, DB_WHAT, SELECT_RANGE_SQL};
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    let binds = Binds::new(tenant, &req);
    let rows = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(SELECT_RANGE_SQL, &binds.params()).await?;
                let to_row = |r: &Row| -> Result<RangeRow, PgError> {
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
                };
                rows.iter()
                    .map(to_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Out)
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    Ok(respond(&req, rows.0))
}
