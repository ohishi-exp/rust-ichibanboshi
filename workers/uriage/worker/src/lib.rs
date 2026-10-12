//! 売上 Worker `ichibanboshi-uriage` (Refs ohishi-exp/rust-ichibanboshi#322)。オンプレ版 rust の担当者別売上の口
//! (`/api/uriage/*`) を移す先:
//!
//! - `GET /health` — D1 (binding `URIAGE_DB`) に `SELECT 1`
//! - `POST /probe` — D1 に `SELECT 1` → SQL Server にログインして `SELECT 1` → CakePHP の editable-months の GET。最初に落ちた段で返す
//! - `GET /api/uriage/{daily,person-monthly-totals,person-partner-totals,r2/pending,recalc-jobs,verify-history}` —
//!   path・Query・応答の JSON はオンプレ版と同じ。SQL・検査・行の読み取り・応答の組み立ては uriage-logic の `d1`
//! - `POST /api/uriage/recalc` — 書き込みの口 ([`recalc`])。token `X-Uriage-Write-Token` 必須
//!
//! verify・verify-debug・admin は次の PR。それまで 404。
//!
//! 到達面: fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。読みの口は認可なし (一番星 Worker・
//! 勤怠 Worker の読みの口と同じ。関門は呼び手の側)。書き込みの口は Worker が共有 secret を照合する。
//! 失敗: D1 の binding が無い = 503、D1 の失敗 = 500 (オンプレ版の LocalStore と同じ status)、SQL Server・CakePHP = 502。
//! 本文は段と種別だけ (D1・SQL Server・CakePHP の message は応答にもログにも出さない)。

mod cakephp;
mod recalc;
mod sql;

use serde::de::DeserializeOwned;
use uriage_logic::cakephp::CAKEPHP_VPC_BINDING;
use uriage_logic::d1::{ping_ok, respond, statements, Bind, Read, Stmt, D1_BINDING, PING_SQL};
use uriage_logic::routes::{route, ReadRoute, Reply, Route, HEALTH_BODY, PROBE_BODY};
use wasm_bindgen::JsValue;
use worker::{
    console_error, console_log, event, Context, D1Database, D1PreparedStatement, Date, Env,
    Request, Response,
};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    let route = route(req.method().as_ref(), &req.path());
    if let Some(reply) = Reply::for_route(route) {
        return respond_with(reply);
    }
    let started = Date::now().as_millis();
    let (name, reply) = match route {
        Route::Health => ("health", ping(&env, HEALTH_BODY).await),
        Route::Probe => ("probe", probe(&env).await),
        Route::Read(r) => {
            // オンプレ版 (axum の Query) と同じく、クエリ文字列の全体を読む
            let query = req.url()?.query().unwrap_or_default().to_string();
            (r.name(), read(&env, r, &query).await)
        }
        Route::Recalc => ("recalc", recalc::serve(&req, &env).await),
        // 経路判定 (`Reply::for_route`) で先に返している
        Route::NotFound | Route::MethodNotAllowed => ("-", Reply::bare(404)),
    };
    let ms = Date::now().as_millis().saturating_sub(started);
    let status = reply.status;
    if status >= 500 {
        console_error!("uriage {name}: {status} {} ({ms} ms)", reply.body);
    } else {
        console_log!("uriage {name}: {status} ({ms} ms)");
    }
    respond_with(reply)
}

fn respond_with(reply: Reply) -> worker::Result<Response> {
    let headers = worker::Headers::new();
    if let Some(ct) = reply.content_type {
        headers.set("content-type", ct)?;
    }
    Ok(Response::ok(reply.body)?
        .with_status(reply.status)
        .with_headers(headers))
}

/// D1 に `SELECT 1` を流して 1 が返れば `ok_body`。
async fn ping(env: &Env, ok_body: &str) -> Reply {
    let rows = match binding(env) {
        Ok(db) => {
            run(
                &db,
                &Stmt {
                    sql: PING_SQL,
                    binds: vec![],
                },
            )
            .await
        }
        Err(reply) => return reply,
    };
    match rows {
        Ok(rows) if ping_ok(&rows) => Reply::ok(ok_body.to_string()),
        Ok(_) => Reply::failure(500, "rows", "malformed"),
        Err(reply) => reply,
    }
}

/// 到達の切り分け: D1 → SQL Server → CakePHP の順に試し、最初に落ちた段を返す。
async fn probe(env: &Env) -> Reply {
    let d1 = ping(env, PROBE_BODY).await;
    if d1.status != 200 {
        return d1;
    }
    let local = vpc_tds::text(env, sql::TARGET.local_addr_var).is_some();
    if !local && cakephp::binding(env, sql::TARGET.binding).is_none() {
        return Reply::missing(sql::TARGET.binding);
    }
    if let Err(e) = sql::ping(env).await {
        return Reply::upstream("sqlserver", &sql::label(&e));
    }
    let Some(vpc) = cakephp::fetcher(env) else {
        return Reply::missing(CAKEPHP_VPC_BINDING);
    };
    if let Err(e) = cakephp::editable_months(&vpc).await {
        return Reply::upstream("cakephp", &e.label());
    }
    Reply::ok(PROBE_BODY.to_string())
}

/// 検査の順: Query → 入力 (400) → binding (503) → D1 (500)。
async fn read(env: &Env, r: ReadRoute, query: &str) -> Reply {
    let read = match read_of(r, query) {
        Ok(read) => read,
        Err(reply) => return reply,
    };
    let stmts = match statements(&read) {
        Ok(s) => s,
        Err(reply) => return reply,
    };
    let db = match binding(env) {
        Ok(db) => db,
        Err(reply) => return reply,
    };
    let mut results = Vec::with_capacity(stmts.len());
    for s in &stmts {
        match run(&db, s).await {
            Ok(rows) => results.push(rows),
            Err(reply) => return reply,
        }
    }
    match respond(read, results) {
        Ok(body) => Reply::ok(body),
        Err(reply) => reply,
    }
}

fn read_of(r: ReadRoute, query: &str) -> Result<Read, Reply> {
    Ok(match r {
        ReadRoute::Daily => Read::Daily(parse_query(query)?),
        ReadRoute::PersonMonthlyTotals => Read::PersonMonthlyTotals(parse_query(query)?),
        ReadRoute::PersonPartnerTotals => Read::PersonPartnerTotals(parse_query(query)?),
        ReadRoute::R2Pending => Read::R2Pending(parse_query(query)?),
        ReadRoute::RecalcJobs => Read::RecalcJobs(parse_query(query)?),
        ReadRoute::VerifyHistory => Read::VerifyHistory(parse_query(query)?),
    })
}

/// axum の `Query` と同じ読み方と 400 の本文 (serde_urlencoded + serde_path_to_error)。
pub(crate) fn parse_query<T: DeserializeOwned>(query: &str) -> Result<T, Reply> {
    let de = serde_urlencoded::Deserializer::new(form_urlencoded::parse(query.as_bytes()));
    serde_path_to_error::deserialize(de)
        .map_err(|e| Reply::text(400, format!("Failed to deserialize query string: {e}")))
}

fn binding(env: &Env) -> Result<D1Database, Reply> {
    env.d1(D1_BINDING).map_err(|_| Reply::no_binding())
}

/// 1 文を bind まで済ませる。
pub(crate) fn prepared(db: &D1Database, s: &Stmt) -> worker::Result<D1PreparedStatement> {
    let values: Vec<JsValue> = s.binds.iter().map(js_value).collect();
    db.prepare(s.sql).bind(&values)
}

/// 1 文を流して行を列の並びの配列で受ける (オンプレ版の `r.get(i)` と同じ並びで読む)。
pub(crate) async fn run(db: &D1Database, s: &Stmt) -> Result<Vec<Vec<serde_json::Value>>, Reply> {
    let stmt = prepared(db, s).map_err(|_| Reply::d1_failed("bind"))?;
    stmt.raw::<serde_json::Value>()
        .await
        .map_err(|_| Reply::d1_failed("query"))
}

fn js_value(b: &Bind) -> JsValue {
    match b {
        Bind::Text(s) => JsValue::from_str(s),
        // D1 は BigInt を受けない。値は営業所 id と cal の 0 / 1 だけ
        Bind::Int(i) => JsValue::from_f64(*i as f64),
    }
}
