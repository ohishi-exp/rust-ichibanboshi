//! `POST /api/uriage/recalc?month=YYYY-MM&eigyosho_id=N` (オンプレ版 root の `src/routes/uriage.rs` の `recalc` と同じ形)。
//!
//! 検査の順は **認可 → 入力 → binding → I/O**:
//! token (`X-Uriage-Write-Token`、無い・違う = 403、secret が読めない = 503) → Query (400) →
//! binding (D1 = 503 `binding:missing`、R2・CakePHP の VPC・SQL Server の VPC = 503 `binding:<名前>`) →
//! editable months (失敗 = 502 `cakephp:<種類>`) → 月ごとに masters (月末日) → job 数 (> 12 = 422。SQL Server・D1 に触らない) →
//! job ごとに (直列) SQL Server 1 本 → 集計 → D1 の 1 batch → R2 の put → `r2_synced`。
//!
//! job の中の失敗はオンプレ版と同じく 200 の応答の `jobs[].status = "failed"` (error は `stage:kind` だけ) と recalc_jobs の failed。
//! TDS の接続は 1 リクエスト 1 本を job 間で使い回し、接続・クエリが失敗した後の job は張り直す。
//! R2 の put の失敗は failed にしない (status は computed のまま。次の recalc で置き直す)。
//! 純粋部分 (段取り・文・判定) は uriage-logic の `recalc`。

use uriage_logic::api::{RecalcJobResult, RecalcQuery};
use uriage_logic::cakephp::CAKEPHP_VPC_BINDING;
use uriage_logic::d1::{Stmt, D1_BINDING};
use uriage_logic::raw::gzip;
use uriage_logic::recalc::{
    check_jobs, month_range, outside_editable, plan_month, put_needed, response, target_months,
    timestamp, Bucket, Job, Step, BUMON_EMPTY, R2_BINDING, RAW_CONTENT_TYPE,
};
use uriage_logic::routes::Reply;
use uriage_logic::write_auth::{authorize, WRITE_TOKEN_BINDING, WRITE_TOKEN_HEADER};
use vpc_tds::Conn;
use worker::{
    console_error, console_log, Bucket as R2, D1Database, Date, Env, Fetcher, HttpMetadata, Request,
};

use crate::cakephp;
use crate::sql;
use crate::{parse_query, prepared, query_string, run};

/// 書き込みの口の認可 (recalc・verify・admin 共通)。token が無い・違う = 403、secret が読めない = 503。
pub(crate) async fn authorize_request(req: &Request, env: &Env) -> Result<(), Reply> {
    let token = req.headers().get(WRITE_TOKEN_HEADER).ok().flatten();
    authorize(load_token(env).await.as_deref(), token.as_deref())
}

pub(crate) async fn serve(req: &Request, env: &Env) -> Reply {
    if let Err(reply) = authorize_request(req, env).await {
        return reply;
    }
    let q: RecalcQuery = match parse_query(&query_string(req)) {
        Ok(q) => q,
        Err(reply) => return reply,
    };
    let io = match bindings(env) {
        Ok(io) => io,
        Err(reply) => return reply,
    };
    recalc(env, &io, &q).await
}

/// 共有 secret を読む。binding が無い・secret が未投入は `None` (中身はどこにも出さない)。
async fn load_token(env: &Env) -> Option<String> {
    let store = env.secret_store(WRITE_TOKEN_BINDING).ok()?;
    store.get().await.ok().flatten()
}

struct Io {
    db: D1Database,
    r2: R2,
    cakephp: Fetcher,
}

fn bindings(env: &Env) -> Result<Io, Reply> {
    let db = env.d1(D1_BINDING).map_err(|_| Reply::no_binding())?;
    let r2 = env
        .bucket(R2_BINDING)
        .map_err(|_| Reply::missing(R2_BINDING))?;
    let missing = || Reply::missing(CAKEPHP_VPC_BINDING);
    let cakephp = cakephp::fetcher(env).ok_or_else(missing)?;
    sql::require(env)?;
    Ok(Io { db, r2, cakephp })
}

async fn recalc(env: &Env, io: &Io, q: &RecalcQuery) -> Reply {
    let editable = match cakephp::editable_months(&io.cakephp).await {
        Ok(e) => e,
        Err(e) => return Reply::upstream("cakephp", &e.label()),
    };
    if outside_editable(q, &editable) {
        // 入力可能月 ≠ 取得可能月 (オンプレ版と同じく走らせる)
        console_log!("uriage recalc: month is outside the editable months");
    }
    let months = target_months(q, &editable);
    let calculated_at = timestamp(Date::now().as_millis());

    let mut steps = Vec::new();
    for month in &months {
        match month_range(month) {
            Err(failed) => steps.push(Step::Done(*failed)),
            Ok((from, to, date)) => {
                // masters を「月末日」基準で取得 (人事異動等は月跨ぎでは想定外)
                let masters = cakephp::masters(&io.cakephp, &date).await;
                steps.extend(plan_month(month, &from, &to, masters, q.eigyosho_id));
            }
        }
    }
    if let Err(reply) = check_jobs(&steps) {
        return reply;
    }

    let mut conn: Option<Conn> = None;
    let mut jobs = Vec::with_capacity(steps.len());
    for step in steps {
        jobs.push(match step {
            Step::Done(result) => result,
            Step::Run(job) => run_job(env, io, &mut conn, &job, &calculated_at).await,
        });
    }
    if let Some(c) = conn {
        let _ = c.close().await;
    }
    let count = editable.editable_months_count;
    Reply::ok(response(months, count, calculated_at, jobs))
}

/// 1 job (month × 営業所)。
async fn run_job(
    env: &Env,
    io: &Io,
    conn: &mut Option<Conn>,
    job: &Job,
    at: &str,
) -> RecalcJobResult {
    // マスタ未設定 (旧 CakePHP) は compute を走らせない (オンプレ版と同じ防御)
    if job.bumon_empty() {
        return fail(io, job, BUMON_EMPTY, at).await;
    }
    if conn.is_none() {
        match sql::connect(env).await {
            Ok(c) => *conn = Some(c),
            Err(e) => return fail(io, job, &format!("sqlserver:{}", sql::label(&e)), at).await,
        }
    }
    let rows = match (job.sql(), conn.as_mut()) {
        // 部門が英数字に絞って空 = 0 行 (接続の後に見る。オンプレ版と同じ順)
        (None, _) => Vec::new(),
        (Some(s), Some(c)) => match sql::uriage_rows(c, &s, &job.from, &job.to).await {
            Ok(rows) => rows,
            Err(e) => {
                // 失敗した接続は捨てて、次の job で張り直す
                if let Some(c) = conn.take() {
                    let _ = c.close().await;
                }
                return fail(io, job, &format!("sqlserver:{}", sql::label(&e)), at).await;
            }
        },
        (Some(_), None) => return fail(io, job, "sqlserver:connect:other", at).await,
    };

    let bucket = job.bucket(&rows, at);
    let existing = match run(&io.db, &job.existing_statement()).await {
        Ok(rows) => rows,
        Err(_) => return fail(io, job, "d1:select", at).await,
    };
    let put = put_needed(&existing, &bucket.fingerprint);
    if batch(&io.db, &bucket.statements).await.is_err() {
        return fail(io, job, "d1:batch", at).await;
    }

    let mut result = job.computed();
    result.row_count = bucket.row_count;
    result.daily_count_cal = bucket.daily_count_cal;
    result.daily_count_nocal = bucket.daily_count_nocal;
    result.fingerprint = Some(bucket.fingerprint.clone());
    if !put {
        // 同じ中身が同期済み (R2 の物はそのまま)
        result.raw_path = Some(bucket.key);
        return result;
    }
    let (month, eid) = (&job.month, job.eigyosho_id);
    match put_raw(&io.r2, &bucket, at).await {
        Ok(()) => {
            result.raw_path = Some(bucket.key.clone());
            let synced = job.synced_statement(&timestamp(Date::now().as_millis()));
            if write(&io.db, &synced).await.is_err() {
                // computed のまま (次の recalc で置き直す)
                console_error!("uriage recalc {month} {eid}: d1:synced failed");
            }
        }
        // computed のまま (次の recalc で置き直す)。raw_path は応答に載せない
        Err(stage) => console_error!("uriage recalc {month} {eid}: r2:{stage} failed"),
    }
    result
}

/// job の失敗: ログ 1 行・recalc_jobs に failed (記録の失敗は無視。オンプレ版と同じ)・応答の failed。
async fn fail(io: &Io, job: &Job, error: &str, at: &str) -> RecalcJobResult {
    let (month, eid) = (&job.month, job.eigyosho_id);
    console_error!("uriage recalc {month} {eid}: {error}");
    let _ = write(&io.db, &job.failed_statement(error, at)).await;
    job.failed(error)
}

/// raw を gzip して R2 に置く (customMetadata に fingerprint と computed_at)。失敗は段の名前だけ。
async fn put_raw(r2: &R2, bucket: &Bucket, at: &str) -> Result<(), &'static str> {
    let body = gzip(&bucket.ndjson).map_err(|_| "gzip")?;
    let meta = HttpMetadata {
        content_type: Some(RAW_CONTENT_TYPE.to_string()),
        ..Default::default()
    };
    r2.put(bucket.key.clone(), body)
        .http_metadata(meta)
        .custom_metadata(bucket.metadata(at))
        .execute()
        .await
        .map_err(|_| "put")?;
    Ok(())
}

/// 書き込みの 1 文。
pub(crate) async fn write(db: &D1Database, s: &Stmt) -> worker::Result<()> {
    prepared(db, s)?.run().await?;
    Ok(())
}

/// 文の列を 1 batch (原子的) で流す。
async fn batch(db: &D1Database, stmts: &[Stmt]) -> worker::Result<()> {
    let prepared = stmts
        .iter()
        .map(|s| prepared(db, s))
        .collect::<worker::Result<Vec<_>>>()?;
    db.batch(prepared).await?;
    Ok(())
}
