//! 拘束サマリ (restraint) の 3 口を D1 (binding `KINTAI_RESTRAINT_DB`) で回す (Refs #322)。
//! オンプレ版 (root の `src/routes/restraint.rs`・`src/restraint_store.rs`、SQLite) から移した。
//!
//! - `PUT /api/restraint/summaries` — relay がサマリの写しを push する (載った乗務員だけ upsert。1 回の `batch` = 1 transaction)
//! - `GET /api/restraint/wage-source` — 当月 + 前月 × theearth + timecard の素材 (8 文を 1 回の `batch`)
//! - `GET /api/restraint/synced-months` — comp の push 済み (source, 月) の一覧
//!
//! 検査・SQL・bind・応答は kintai-logic の `restraint` (オンプレ版と同じもの)、D1 の文の束・結果の読み取り・D1 だけの失敗は
//! `restraint_d1`。ここは D1 との往復と応答の形だけ。
//!
//! 検査の順: PUT は **認可 → 本文 → 検査 → binding → 書き込み**、GET は **Query → 検査 → binding → 読み**。
//! 失敗の本文はオンプレ版と同じ形にする: Query・本文が読めない (axum の extractor の拒否) は平文、それ以外
//! (400 の検査・403・503・502) は `{"error": "…"}`。

use kintai_logic::common::{parse_json, parse_query, Fail};
use kintai_logic::restraint::{
    parse_synced_months, parse_wage_source, validate_push, Bind, BrokenSummary, ErrorBody,
    PushBody, SyncedMonthsQuery, WageSourceQuery,
};
use kintai_logic::restraint_d1::{
    check_push_size, d1_fail, no_d1, push_statements, synced_at_from_millis, synced_from_results,
    synced_statement, wage_source_from_results, wage_source_statements, Stmt, D1_BINDING,
};
use kintai_logic::write_auth::{authorize, WRITE_TOKEN_HEADER};
use wasm_bindgen::JsValue;
use worker::{
    console_error, console_log, D1Database, D1PreparedStatement, Date, Env, Request, Response,
};

use crate::writes::load_token;

/// restraint の 3 口。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Restraint {
    PutSummaries,
    WageSource,
    SyncedMonths,
}

impl Restraint {
    /// 口の path。元 (オンプレ版) と同じ。
    pub(crate) fn from_path(path: &str) -> Option<Self> {
        Some(match path {
            "/api/restraint/summaries" => Restraint::PutSummaries,
            "/api/restraint/wage-source" => Restraint::WageSource,
            "/api/restraint/synced-months" => Restraint::SyncedMonths,
            _ => return None,
        })
    }

    /// 受ける method (元と同じ。違えば 405)。
    pub(crate) fn method(self) -> &'static str {
        match self {
            Restraint::PutSummaries => "PUT",
            Restraint::WageSource | Restraint::SyncedMonths => "GET",
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Restraint::PutSummaries => "restraint/summaries",
            Restraint::WageSource => "restraint/wage-source",
            Restraint::SyncedMonths => "restraint/synced-months",
        }
    }
}

/// 失敗の本文の形。
enum Out {
    /// axum の extractor (Query・Json) の拒否と同じ平文
    Plain(Fail),
    /// オンプレ版の `{"error": "…"}`
    Json(Fail),
}

pub(crate) async fn serve(r: Restraint, req: &mut Request, env: &Env) -> worker::Result<Response> {
    let started = Date::now().as_millis();
    let outcome = match r {
        Restraint::PutSummaries => put_summaries(req, env).await,
        Restraint::WageSource => wage_source(&query_of(req)?, env).await,
        Restraint::SyncedMonths => synced_months(&query_of(req)?, env).await,
    };
    let ms = Date::now().as_millis().saturating_sub(started);
    let name = r.as_str();
    match outcome {
        Ok(body) => {
            console_log!("kintai {name}: ok ({ms} ms)");
            let headers = worker::Headers::new();
            headers.set("content-type", "application/json")?;
            Ok(Response::ok(body)?.with_headers(headers))
        }
        Err(Out::Json(f)) => {
            console_error!("kintai {name}: {} ({ms} ms) {}", f.status, f.body);
            Ok(Response::from_json(&ErrorBody::of(f.clone()))?.with_status(f.status))
        }
        Err(Out::Plain(f)) => {
            console_error!("kintai {name}: {} ({ms} ms) {}", f.status, f.body);
            let headers = worker::Headers::new();
            headers.set("content-type", "text/plain; charset=utf-8")?;
            Ok(Response::ok(f.body)?
                .with_status(f.status)
                .with_headers(headers))
        }
    }
}

fn query_of(req: &Request) -> worker::Result<String> {
    Ok(req.url()?.query().unwrap_or("").to_string())
}

fn binding(env: &Env) -> Result<D1Database, Out> {
    env.d1(D1_BINDING).map_err(|_| Out::Json(no_d1()))
}

/// 認可 (書き込みの口と同じ共有 secret) → 本文 → 検査 → binding → 1 回の batch。
async fn put_summaries(req: &mut Request, env: &Env) -> Result<String, Out> {
    let token = req.headers().get(WRITE_TOKEN_HEADER).ok().flatten();
    authorize(load_token(env).await.as_deref(), token.as_deref()).map_err(Out::Json)?;
    let content_type = req.headers().get("content-type").ok().flatten();
    let body = req
        .bytes()
        .await
        .map_err(|_| Out::Plain(Fail::new(400, "Failed to buffer the request body")))?;
    let push: PushBody = parse_json(content_type.as_deref(), &body).map_err(Out::Plain)?;
    let valid = validate_push(push).map_err(Out::Json)?;
    check_push_size(&valid).map_err(Out::Json)?;
    let db = binding(env)?;
    let synced_at = synced_at_from_millis(Date::now().as_millis() as i64);
    let stmts = prepare(&db, push_statements(&valid, &synced_at))?;
    db.batch(stmts)
        .await
        .map_err(|_| Out::Json(d1_fail("batch")))?;
    let saved = valid.entries.len();
    console_log!(
        "kintai restraint/summaries: {} {} {} saved {saved}",
        valid.comp_id,
        valid.source,
        valid.month
    );
    json_body(&valid.response(synced_at))
}

/// Query → 検査 → binding → 8 文を 1 回の batch。
async fn wage_source(query: &str, env: &Env) -> Result<String, Out> {
    let q: WageSourceQuery = parse_query(query).map_err(Out::Plain)?;
    let req = parse_wage_source(q).map_err(Out::Json)?;
    let db = binding(env)?;
    let stmts = prepare(&db, wage_source_statements(&req))?;
    let results = db
        .batch(stmts)
        .await
        .map_err(|_| Out::Json(d1_fail("batch")))?;
    let rows = results
        .iter()
        .map(|r| r.results::<serde_json::Value>())
        .collect::<worker::Result<Vec<_>>>()
        .map_err(|_| Out::Json(d1_fail("rows")))?;
    let (res, broken) = wage_source_from_results(req, rows).map_err(Out::Json)?;
    log_broken(&broken);
    json_body(&res)
}

/// Query → 検査 → binding → 1 文。
async fn synced_months(query: &str, env: &Env) -> Result<String, Out> {
    let q: SyncedMonthsQuery = parse_query(query).map_err(Out::Plain)?;
    let comp = parse_synced_months(q).map_err(Out::Json)?;
    let db = binding(env)?;
    let stmt = prepare(&db, vec![synced_statement(&comp)])?
        .pop()
        .ok_or_else(|| Out::Json(d1_fail("prepare")))?;
    let result = stmt.all().await.map_err(|_| Out::Json(d1_fail("query")))?;
    let rows = result
        .results::<serde_json::Value>()
        .map_err(|_| Out::Json(d1_fail("rows")))?;
    let res = synced_from_results(&comp, rows).map_err(Out::Json)?;
    json_body(&res)
}

/// 応答の型をそのまま直列化する (キーの順はオンプレ版の axum の `Json` と同じ構造体の順。`serde_json::Value` を
/// 経由すると名前順に並び替わる)。
fn json_body<T: serde::Serialize>(v: &T) -> Result<String, Out> {
    serde_json::to_string(v).map_err(|_| Out::Json(d1_fail("json")))
}

/// 文の束を D1 の prepared statement に (bind は `?NNN` の順)。
fn prepare(db: &D1Database, stmts: Vec<Stmt>) -> Result<Vec<D1PreparedStatement>, Out> {
    stmts
        .into_iter()
        .map(|s| {
            let values: Vec<JsValue> = s.binds.iter().map(js_value).collect();
            db.prepare(s.sql)
                .bind(&values)
                .map_err(|_| Out::Json(d1_fail("bind")))
        })
        .collect()
}

fn js_value(b: &Bind) -> JsValue {
    match b {
        Bind::Text(s) => JsValue::from_str(s),
        // D1 は BigInt を受けない。値は no_data の 0 / 1 だけ
        Bind::Int(i) => JsValue::from_f64(*i as f64),
        Bind::Null => JsValue::null(),
    }
}

/// 壊れた summary_json (push 側で検証済みなので実際には起きない) は行単位で落として残りを返す。乗務員CD と理由だけをログに。
fn log_broken(broken: &[BrokenSummary]) {
    for b in broken {
        console_error!(
            "kintai restraint: summary_json broken driver_cd={} {}",
            b.driver_cd,
            b.error
        );
    }
}
