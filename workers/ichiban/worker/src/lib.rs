//! 一番星 PoC Worker (Refs ohishi-exp/rust-ichibanboshi#322)。一番星 (CAPE#01) の読み出しを Worker へ移す前の到達確認:
//! Workers VPC (TCP) → 既存 Tunnel → 一番星 SQL Server に TDS でログインできるか。
//!
//! 到達面: fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。口は `POST /probe` の 1 本だけ。
//! 認可なし (効果は一番星 SQL Server への 1 回のログインと `SELECT 1` だけで、データは返さない)。

mod probe;
mod probe_logic;
mod repo;
mod tcp;
mod transport;

use probe_logic::{reply_for_route, route, Reply};
use worker::{event, Context, Env, Request, Response};

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    let route = route(req.method().as_ref(), &req.path());
    if let Some(reply) = reply_for_route(&route) {
        return respond(reply);
    }
    respond(probe::run(&env).await)
}

fn respond(reply: Reply) -> worker::Result<Response> {
    let headers = worker::Headers::new();
    headers.set("content-type", "application/json")?;
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
