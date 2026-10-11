//! Supabase (勤怠スキーマ `kintai.*`) を Hyperdrive (`KINTAI_HYPERDRIVE`) 経由で読む 6 本の口 (`timecard/signatures` を含む)。
//!
//! 入力の検査・SQL・`$n` の型・応答の組み立ては kintai-logic (root の src/ の写し)。ここは DB との往復だけ:
//! 検査 (400) → binding の有無 (無ければ 503) → テナント (`KINTAI_TENANT_ID`、無ければ 503) → connect (失敗は 502) →
//! `PgClient::tenant_tx` の中で `query_typed` → 行を owned な型に詰め直す → 応答 (200)。順は元の handler と同じ。
//!
//! - **テナントは設定 pin。`X-Tenant-ID` は読まない。** 接続ロールは BYPASSRLS なので、SQL の `WHERE tenant_id = $1`
//!   (`$1` = pin の UUID) が他テナントを見せない唯一の担保 (kintai-logic の `tests/common.rs` で確かめている)
//! - 名前付き prepared statement は使わない (`query_typed` / `query_typed_one` だけ。Hyperdrive では接続が切れる)
//! - 502 の本文は元の文言の頭 + kit の `kind` (SQLSTATE か固定の語)。DB の message・接続先は出さない

use alc_worker_db::hyperdrive;
use alc_worker_db::{kind, PgClient, TxOutput};
use kintai_logic::common::{
    db_fail, no_db, no_write_db, preflight, write_preflight, Fail, HYPERDRIVE_BINDING, TENANT_VAR,
};
use kintai_logic::{
    change_log, day_summaries, shift_days, shift_overlaps, timecard_write, unko_gaps, wage_range,
};
use tokio_postgres::{Error as PgError, Row};
use uuid::Uuid;
use wasm_bindgen::JsValue;
use worker::js_sys::Reflect;
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
        Read::Signatures => signatures(query, env).await,
        Read::UnkoGaps => unko_gaps(query, env).await,
    }
}

/// binding の有無 → テナント → connect の順 (元の handler と同じく、設定の欠落は DB に繋ぐ前に 503 で決める)。
/// binding が無い = 503 / テナントが無い = 503 (どちらも connect しない) / 在るのに繋がらない = 502。
async fn open(env: &Env, what: &str) -> Result<(PgClient, Uuid), Fail> {
    open_db(env, what, false).await
}

/// [`open`] の本体。`write` は書き込みの口 (と元が書き先の store を使っていた `timecard/signatures`) で、binding が
/// 無いときの 503 の文言だけが違う (元の `[kintai_push] が無効です (書き先がありません)` に当たる)。
pub(crate) async fn open_db(env: &Env, what: &str, write: bool) -> Result<(PgClient, Uuid), Fail> {
    // kit の hyperdrive::connect と同じ判定 (undefined のときだけ「無い」。読めなければ「在る」とし、connect の Err に任せる)
    let has_binding =
        Reflect::get(env, &JsValue::from(HYPERDRIVE_BINDING)).map_or(true, |v| !v.is_undefined());
    let raw = env.var(TENANT_VAR).ok().map(|v| v.to_string());
    let tenant = match write {
        true => write_preflight(has_binding, raw.as_deref())?,
        false => preflight(has_binding, raw.as_deref())?,
    };
    let pg = match hyperdrive::connect(env, HYPERDRIVE_BINDING).await {
        Ok(Some(pg)) => pg,
        Ok(None) if write => return Err(no_write_db()),
        Ok(None) => return Err(no_db()),
        // ConnectError の Display は binding 名・段・kind だけ (接続文字列・宛先を含まない)
        Err(e) => return Err(db_fail(what, &e.to_string())),
    };
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
    use wage_range::{parse, respond, Binds, DB_WHAT, SELECT_RANGE_SQL};
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    let binds = Binds::new(tenant, &req);
    let rows = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx.query_typed(SELECT_RANGE_SQL, &binds.params()).await?;
                rows.iter()
                    .map(kintai_pg::range_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Out)
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    Ok(respond(&req, rows.0))
}

/// `GET /api/kintai/timecard/signatures` (元は書き先の store を使っていたので、binding が無いときは書き込みの口と同じ 503)。
/// 検査 (400) → binding (503) → テナント (503) → connect (502) → `STORED_SIGNATURES_SQL` → 応答。
async fn signatures(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use timecard_write::{parse_signatures, signatures_respond, DB_WHAT};
    let req = parse_signatures(query)?;
    let (mut pg, tenant) = open_db(env, DB_WHAT, true).await?;
    let sigs = kintai_pg::stored_day_signatures(&mut pg, tenant, req.driver_cd, req.from, req.to)
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    Ok(signatures_respond(&req, &sigs))
}

/// `GET /api/kintai/unko-gaps` (root の `src/routes/unko_gaps.rs` と同じ応答。`elapsed_ms` は付けない)。
/// 検査 (400) → binding (503) → テナント (503) → connect (502) → `MONTH_OPERATIONS_SQL` (502) → auth-worker の RPC (`KintaiAlcEntrypoint.dtakoEtags`) で
/// alc の etags (binding が無い 503・404 は `gcp_etags_available: false`・他の失敗は 502) → 応答。順は root と同じ。
/// RPC は JS の値の await なので transaction の外で打つ。
async fn unko_gaps(query: &str, env: &Env) -> Result<serde_json::Value, Fail> {
    use unko_gaps::{
        broken_month, etags_search, parse, read_etags, respond, Binds, Onprem, Window, DB_WHAT,
        MONTH_OPERATIONS_SQL,
    };
    let req = parse(query)?;
    let (mut pg, tenant) = open(env, DB_WHAT).await?;
    // parse が通した月では常に作れる (作れなければ root と同じ 400)
    let window = Window::of(&req.month)?;
    let binds = Binds::new(tenant, &window);
    let rows = pg
        .tenant_tx(tenant, move |tx| {
            Box::pin(async move {
                let rows = tx
                    .query_typed(MONTH_OPERATIONS_SQL, &binds.params())
                    .await?;
                let to_row = |r: &Row| -> Result<(i64, String), PgError> {
                    Ok((r.try_get("driver_cd")?, r.try_get("unko_no")?))
                };
                rows.iter()
                    .map(to_row)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Out)
            })
        })
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))?;
    let onprem = Onprem::from_rows(rows.0.iter().map(|(d, u)| (*d, u.as_str())));
    let search = etags_search(&req.month).ok_or_else(|| broken_month(&req.month))?;
    let gcp = read_etags(&crate::alc::fetch_etags(env, &search).await?)?;
    Ok(respond(
        &req.month,
        &window,
        req.driver_cd,
        &onprem,
        gcp.as_ref(),
    ))
}
