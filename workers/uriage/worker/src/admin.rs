//! `POST /api/uriage/admin/delete?month=YYYY-MM&eigyosho_id=N` と `POST /api/uriage/admin/rebuild`
//! (オンプレ版 root の `src/routes/uriage.rs` の `admin_delete`・`admin_rebuild` と同じ形。どちらも token 必須)。
//!
//! 検査の順は **認可 → 入力 → binding → I/O**: token (403 / 503) → Query (400。delete の month が空は本文なしの 400) →
//! D1 binding (503) → D1 の 1 batch (失敗 = 500 `batch:failed`)。
//! delete は 3 文 (日次・取引先別・recalc_jobs。verify_jobs は消さない)、rebuild は **DROP せず** 4 表を DELETE する (VIEW は残す)。
//! 文と応答は uriage-logic の `admin`。

use uriage_logic::admin::{
    delete_response, delete_statements, rebuild_response, rebuild_statements,
};
use uriage_logic::api::AdminDeleteQuery;
use uriage_logic::d1::{Stmt, D1_BINDING};
use uriage_logic::recalc::timestamp;
use uriage_logic::routes::Reply;
use worker::{D1Database, Date, Env, Request};

use crate::recalc::authorize_request;
use crate::{parse_query, prepared, query_string};

pub(crate) async fn delete(req: &Request, env: &Env) -> Reply {
    if let Err(reply) = authorize_request(req, env).await {
        return reply;
    }
    let q: AdminDeleteQuery = match parse_query(&query_string(req)) {
        Ok(q) => q,
        Err(reply) => return reply,
    };
    let stmts = match delete_statements(&q) {
        Ok(s) => s,
        Err(reply) => return reply,
    };
    let db = match env.d1(D1_BINDING) {
        Ok(db) => db,
        Err(_) => return Reply::no_binding(),
    };
    match changes(&db, &stmts).await {
        Ok(changes) => delete_response(q, &changes),
        Err(_) => Reply::d1_failed("batch"),
    }
}

pub(crate) async fn rebuild(req: &Request, env: &Env) -> Reply {
    if let Err(reply) = authorize_request(req, env).await {
        return reply;
    }
    let db = match env.d1(D1_BINDING) {
        Ok(db) => db,
        Err(_) => return Reply::no_binding(),
    };
    match changes(&db, &rebuild_statements()).await {
        Ok(_) => rebuild_response(timestamp(Date::now().as_millis())),
        Err(_) => Reply::d1_failed("batch"),
    }
}

/// 文の列を 1 batch (原子的) で流し、文ごとの変更行数 (同じ順) を返す。meta が読めなければ失敗。
async fn changes(db: &D1Database, stmts: &[Stmt]) -> worker::Result<Vec<usize>> {
    let prepared = stmts
        .iter()
        .map(|s| prepared(db, s))
        .collect::<worker::Result<Vec<_>>>()?;
    let results = db.batch(prepared).await?;
    results
        .iter()
        .map(|r| {
            r.meta()?
                .and_then(|m| m.changes)
                .ok_or_else(|| worker::Error::RustError("meta".to_string()))
        })
        .collect()
}
