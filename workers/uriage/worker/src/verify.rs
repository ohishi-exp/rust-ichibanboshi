//! `POST /api/uriage/verify?id=N&date=YYYY-MM-DD&cal=true|false` と `GET /api/uriage/verify-debug?…`
//! (オンプレ版 root の `src/routes/uriage.rs` の `verify`・`verify_debug` と同じ形)。
//!
//! 検査の順は **認可 → 入力 → binding → I/O**:
//! - verify: token (`X-Uriage-Write-Token`。D1 の verify_jobs に書くため。無い・違う = 403、secret が読めない = 503) →
//!   Query・date (400) → binding (D1 = 503 `binding:missing`、CakePHP・SQL Server の VPC = 503 `binding:<名前>`) →
//!   masters (失敗 = 502 `cakephp:<種類>`) → 営業所 (無い = 404) → bumon 空は検証せず skipped (200。verify_jobs に ok = 1) →
//!   print-json (502) → SQL Server 1 本 (単日。接続 = 503、クエリ = 500 `sqlserver:<段>:<種類>`) → 集計・差分 → verify_jobs の UPSERT
//!   (失敗は握りつぶす。応答は返す)
//! - verify-debug: 書かない・認可なし (Service Binding 専用で外から届かない)。D1 の binding も要らない。bumon が空でも止めない
//!
//! 純粋部分 (入力の検査・文・集計・応答) は uriage-logic の `verify`。

use std::collections::HashMap;

use uriage_logic::api::VerifyQuery;
use uriage_logic::cakephp::{office_lookup, OfficeMasters, CAKEPHP_VPC_BINDING};
use uriage_logic::compute::{PersonAccum, UriageRow};
use uriage_logic::d1::D1_BINDING;
use uriage_logic::recalc::timestamp;
use uriage_logic::routes::Reply;
use uriage_logic::verify::{
    check_date, debug, office_missing, parse_php_sum, rows_sql, skipped, sum_unreadable, verified,
};
use worker::{console_error, Date, Env, Fetcher, Request};

use crate::cakephp;
use crate::recalc::{authorize_request, write};
use crate::{parse_query, query_string, sql};

pub(crate) async fn serve(req: &Request, env: &Env) -> Reply {
    if let Err(reply) = authorize_request(req, env).await {
        return reply;
    }
    let q = match query(req) {
        Ok(q) => q,
        Err(reply) => return reply,
    };
    let db = match env.d1(D1_BINDING) {
        Ok(db) => db,
        Err(_) => return Reply::no_binding(),
    };
    let vpc = match upstream_bindings(env) {
        Ok(vpc) => vpc,
        Err(reply) => return reply,
    };
    let started = Date::now().as_millis();
    let (reply, stmt) = match run(env, &vpc, &q, true).await {
        Ok(None) => {
            let at = timestamp(Date::now().as_millis());
            skipped(&q, elapsed(started), &at)
        }
        Ok(Some(f)) => {
            let at = timestamp(Date::now().as_millis());
            verified(&q, &f.office, f.php, &f.rows, elapsed(started), &at)
        }
        Err(reply) => return reply,
    };
    // 記録の失敗は致命的でない (verify の結果は呼び手に返す。オンプレ版と同じ)
    if write(&db, &stmt).await.is_err() {
        console_error!("uriage verify {} {}: d1:upsert failed", q.id, q.date);
    }
    reply
}

pub(crate) async fn serve_debug(req: &Request, env: &Env) -> Reply {
    let q = match query(req) {
        Ok(q) => q,
        Err(reply) => return reply,
    };
    let vpc = match upstream_bindings(env) {
        Ok(vpc) => vpc,
        Err(reply) => return reply,
    };
    match run(env, &vpc, &q, false).await {
        Ok(Some(f)) => debug(&q, &f.office, f.php, &f.rows),
        // skip_empty = false なので来ない
        Ok(None) => Reply::bare(500),
        Err(reply) => reply,
    }
}

fn query(req: &Request) -> Result<VerifyQuery, Reply> {
    let q: VerifyQuery = parse_query(&query_string(req))?;
    check_date(&q.date)?;
    Ok(q)
}

/// CakePHP の VPC と SQL Server の binding (ローカル検証の `LOCAL_SQL_ADDR` は VPC を迂回するので binding を問わない)。
fn upstream_bindings(env: &Env) -> Result<Fetcher, Reply> {
    let vpc = cakephp::fetcher(env).ok_or_else(|| Reply::missing(CAKEPHP_VPC_BINDING))?;
    sql::require(env)?;
    Ok(vpc)
}

/// 営業所の masters・PHP の `$sum`・SQL Server の行。
struct Fetched {
    office: OfficeMasters,
    php: HashMap<String, PersonAccum>,
    rows: Vec<UriageRow>,
}

/// masters → 営業所 → (verify だけ。bumon 空なら `None` = skipped) → print-json → SQL Server。
async fn run(
    env: &Env,
    vpc: &Fetcher,
    q: &VerifyQuery,
    skip_empty: bool,
) -> Result<Option<Fetched>, Reply> {
    let masters = cakephp::masters(vpc, &q.date)
        .await
        .map_err(|e| Reply::upstream("cakephp", &e.label()))?;
    let office = office_lookup(&masters, q.id).ok_or_else(|| office_missing(q.id))?;
    if skip_empty && office.bumon.is_empty() {
        return Ok(None);
    }
    let printed = cakephp::print_json(vpc, q.id, &q.date, q.cal)
        .await
        .map_err(|e| Reply::upstream("cakephp", &e.label()))?;
    let php = parse_php_sum(&printed.sum).map_err(|_| sum_unreadable())?;

    // 接続の後に、部門が英数字に絞って空 (= 0 行) を見る (オンプレ版と同じ順)
    let mut conn = sql::connect(env)
        .await
        .map_err(|e| Reply::failure(503, "sqlserver", &sql::label(&e)))?;
    let rows = match rows_sql(office) {
        None => Ok(Vec::new()),
        Some(s) => sql::uriage_rows(&mut conn, &s, &q.date, &q.date).await,
    };
    let _ = conn.close().await;
    let rows = rows.map_err(|e| Reply::failure(500, "sqlserver", &sql::label(&e)))?;
    Ok(Some(Fetched {
        office: office.clone(),
        php,
        rows,
    }))
}

fn elapsed(started: u64) -> u64 {
    Date::now().as_millis().saturating_sub(started)
}
