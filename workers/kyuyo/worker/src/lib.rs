//! 給与大臣 Worker (Refs ohishi-exp/rust-ichibanboshi#322)。給与大臣 (`/api/kyuyo/*`) をオンプレから移す先。
//!
//! 到達面: fetch は Service Binding からだけ届く (route・workers.dev・preview 無し)。
//!
//! fetch は **全リクエストを DO `KyuyoState` (`idFromName("kyuyo")` の 1 インスタンス) へ転送する**。
//! SQL Server を開く処理は DO のロックの中だけ ([`state`])。
//!
//! - `POST /probe` — 認可なし (効果は給与大臣 SQL Server への 1 回のログインと `SELECT 1` だけで、データは返さない。
//!   到達は binding 専用)。DO のロックの中で走る ([`probe`])
//! - `/kyuyo/*` — 転送の前に `Authorization: Bearer <token>` を auth-worker の `KyuyoAuthEntrypoint.authorize`
//!   (binding `AUTH_KYUYO`) に渡す。200 以外はその status と body をそのまま返す。200 なら email を内部ヘッダで
//!   DO へ添える (DO へのリクエストは新しく組み立てるので、外から来た同名ヘッダは捨てられる)。
//!   認可を差し替えるローカル専用の分岐は置かない (ローカル検証はスタブの auth worker を binding で繋ぐ)

mod auth;
mod probe;
mod state;
mod tcp;
mod transport;

use kyuyo_logic::auth::{bearer_token, decide, server_error};
use kyuyo_logic::{reply_for_route, route, Reply, Route};
use worker::{console_error, event, Context, Env, Request, Response};

pub use crate::state::KyuyoState;

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> worker::Result<Response> {
    let route = route(req.method().as_ref(), &req.path());
    if let Some(reply) = reply_for_route(&route) {
        return respond(reply);
    }
    let email = match route {
        Route::Kyuyo(_) => {
            let header = req.headers().get("authorization")?;
            let token = bearer_token(header.as_deref());
            let result = match auth::authorize(&env, token).await {
                Ok(result) => result,
                Err(_) => {
                    console_error!("kyuyo auth: rpc failed");
                    return respond(server_error());
                }
            };
            match decide(result.status, &result.body) {
                Ok(email) => Some(email),
                Err(reply) => return respond(reply),
            }
        }
        _ => None,
    };
    state::forward(&env, &req, email.as_deref()).await
}

pub(crate) fn respond(reply: Reply) -> worker::Result<Response> {
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
