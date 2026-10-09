//! 一番星 Worker (Refs ohishi-exp/rust-ichibanboshi#322)。一番星 (CAPE#01) の読み出しを
//! Workers VPC (TCP) → 既存 Tunnel → 一番星 SQL Server で行う。
//!
//! 口: 管理画面が使う GET 6 本 (`/health`・`/api/employees`・`/api/vehicles`・`/api/sales/departments`・
//! `/api/sales/vehicle-daily`・`/api/costs/vehicle-daily`。path・クエリ・応答はオンプレ版と同じ) と、
//! 到達の切り分け用の `POST /probe`。本体は [`routes`]。
//!
//! 到達面: fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。
//! **認可なし (ユーザー決定 2026-10-09)。** Service Binding 専用で、関門は呼び手 (管理画面の proxy) の requireAuth と
//! path allowlist。同じアカウントで Worker を deploy できる者は binding で読める — 承知のうえ。

mod probe_logic;
mod repo;
mod routes;
mod rows;
mod tcp;
mod transport;

use probe_logic::{reply_for_route, route, Reply};
use worker::{event, Context, Env, Request, Response};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    let route = route(req.method().as_ref(), &req.path());
    if let Some(reply) = reply_for_route(route) {
        return respond(reply);
    }
    // オンプレ版 (axum の Query) と同じく、クエリ文字列の全体を serde_urlencoded で読む
    let query = req.url()?.query().unwrap_or_default().to_string();
    respond(routes::run(&env, route, &query).await)
}

fn respond(reply: Reply) -> worker::Result<Response> {
    let headers = worker::Headers::new();
    // 本文なし (400) は content-type を付けない (オンプレ版の StatusCode だけの応答と同じ)
    if !reply.body.is_empty() {
        headers.set("content-type", "application/json")?;
    }
    Ok(Response::ok(reply.body)?
        .with_status(reply.status)
        .with_headers(headers))
}

/// secret / var の文字列 (空は無いものとして扱う)。`.dev.vars` はローカルでは var として見える。
pub(crate) fn text(env: &Env, name: &str) -> Option<String> {
    env.secret(name)
        .map(|s| s.to_string())
        .or_else(|_| env.var(name).map(|v| v.to_string()))
        .ok()
        .filter(|s| !s.is_empty())
}
