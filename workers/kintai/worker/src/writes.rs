//! Supabase (勤怠スキーマ `kintai.*`) に書く 2 本の口 (Refs #322)。
//!
//! - `POST /api/kintai/timecard` — 打刻の差分の日だけを反映する (元は Cloud Run 版の `kintai_timecard::receive`)
//! - `POST /api/kintai/wage-snapshot` — 賃金確定値の 1 か月ぶんを置き換え保存する (元は `wage_snapshot::put_wage_snapshot`)
//! - `POST /api/dtako/autoload` — csvdata.zip を社内 CakePHP の取り込み口へ中継する (認可の後は [`crate::cakephp`])
//!
//! 検査の順は **認可 → 入力 → テナント → DB**:
//! `X-Kintai-Write-Token` を Secrets Store の `KINTAI_WRITE_TOKEN` と照合 (無い・違う = 403、binding が無い・読めない = 503) →
//! 本文 (axum の `Json` と同じ 415 / 413 / 400 / 422 と、元の 400) → binding (503) → テナント (`KINTAI_TENANT_ID`、503) →
//! connect (502) → `kintai_pg` (1 transaction) → 応答 (200、元と同じ JSON)。
//!
//! - 入力の検査・応答・認可の判定は kintai-logic (`timecard_write`・`wage_write`・`write_auth`)、DB との往復は kintai-pg
//! - **テナントは設定 pin。`X-Tenant-ID` は読まない** (元の timecard は `X-Tenant-ID` を読み pin と食い違えば 403・
//!   無ければ 400 だった。Worker では pin だけで決める)
//! - 502 の本文は元の文言の頭 + kit の `kind`。DB の message・接続先は出さない

use alc_worker_db::kind;
use kintai_logic::common::{db_fail, Fail};
use kintai_logic::write_auth::{authorize, WRITE_TOKEN_BINDING, WRITE_TOKEN_HEADER};
use kintai_logic::{timecard_write, wage_write};
use kintai_pg::ApplyError;
use worker::{Env, Request};

use crate::probe::Write;
use crate::reads::open_db;

/// 口ごとに書いて応答の JSON を返す。
pub(crate) async fn serve(
    write: Write,
    req: &mut Request,
    env: &Env,
) -> Result<serde_json::Value, Fail> {
    let token = req.headers().get(WRITE_TOKEN_HEADER).ok().flatten();
    authorize(load_token(env).await.as_deref(), token.as_deref())?;
    match write {
        Write::Timecard => {
            let (content_type, body) = read_body(req).await?;
            timecard(content_type.as_deref(), &body, env).await
        }
        Write::WageSnapshot => {
            let (content_type, body) = read_body(req).await?;
            wage_snapshot(content_type.as_deref(), &body, env).await
        }
        // 本文は zip (JSON ではない)。入力の検査と段取りは CakePHP の中継の側
        Write::DtakoAutoload => crate::cakephp::autoload(req, env).await,
    }
}

/// 本文の Content-Type とバイト列。読めない (途中で切れた等) のは axum の `Failed to buffer the request body` (400) と同じ扱い。
async fn read_body(req: &mut Request) -> Result<(Option<String>, Vec<u8>), Fail> {
    let content_type = req.headers().get("content-type").ok().flatten();
    let body = req
        .bytes()
        .await
        .map_err(|_| Fail::new(400, "Failed to buffer the request body"))?;
    Ok((content_type, body))
}

/// 共有 secret を読む。binding が無い・secret が未投入は `None` (中身はどこにも出さない)。
pub(crate) async fn load_token(env: &Env) -> Option<String> {
    let store = env.secret_store(WRITE_TOKEN_BINDING).ok()?;
    store.get().await.ok().flatten()
}

async fn timecard(
    content_type: Option<&str>,
    body: &[u8],
    env: &Env,
) -> Result<serde_json::Value, Fail> {
    use timecard_write::{batch_respond, parse_batch, DB_WHAT};
    let batch = parse_batch(content_type, body)?;
    let (mut pg, tenant) = open_db(env, DB_WHAT, true).await?;
    let result = kintai_pg::apply_timecard_batch(&mut pg, tenant, &batch)
        .await
        .map_err(|e| match e {
            // 元の `KintaiPushError::NotConfigured` (503)。月は先に検査済みなので通常は起きない
            ApplyError::BadMonth(m) => Fail::new(503, m),
            ApplyError::Db(e) => db_fail(DB_WHAT, &kind(&e)),
        })?;
    Ok(batch_respond(&result))
}

async fn wage_snapshot(
    content_type: Option<&str>,
    body: &[u8],
    env: &Env,
) -> Result<serde_json::Value, Fail> {
    use wage_write::{parse_snapshot, DB_WHAT};
    let (valid, synced_at) = parse_snapshot(content_type, body)?;
    let (mut pg, tenant) = open_db(env, DB_WHAT, true).await?;
    kintai_pg::put_wage_snapshot(&mut pg, tenant, valid, synced_at)
        .await
        .map_err(|e| db_fail(DB_WHAT, &kind(&e)))
}
