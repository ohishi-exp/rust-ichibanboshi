//! auth-worker の named entrypoint `KyuyoAuthEntrypoint` (Service Binding `AUTH_KYUYO`) への RPC。
//!
//! `authorize(token)` は失敗を throw せず `{status, body, contentType}` で返す (auth-worker
//! `src/kyuyo-auth-entrypoint.ts`)。allowlist・origin は auth-worker 側で固定で、ここから渡せるのは token だけ。
//! 戻りの読み方 (200 だけ通す・fail-closed) は `kyuyo_logic::auth::decide`。
//! 呼び方は ohishi-exp/smb-watch の `workers/smb-ingest/worker/src/ingest.rs` と同じ形。

use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use worker::Env;

const AUTH_BINDING: &str = "AUTH_KYUYO";

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(extends = js_sys::Object)]
    type KyuyoAuthRpc;

    #[wasm_bindgen(method, catch)]
    fn authorize(this: &KyuyoAuthRpc, token: &str) -> Result<js_sys::Promise, JsValue>;
}

/// RPC の戻り (`AlcRpcResult`)。
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RpcResult {
    pub status: u16,
    pub body: String,
}

/// `AUTH_KYUYO.authorize(token)` を呼ぶ。binding が無い・RPC 自体が落ちたときだけ `Err`。
/// エラーの中身は呼び出し側で捨てる (token を含みうる文言をどこにも出さない)。
pub(crate) async fn authorize(env: &Env, token: &str) -> worker::Result<RpcResult> {
    let rpc = env.service(AUTH_BINDING)?.into_rpc::<KyuyoAuthRpc>();
    let call = rpc.authorize(token).map_err(js_error)?;
    let value = JsFuture::from(call).await.map_err(js_error)?;
    Ok(serde_wasm_bindgen::from_value(value)?)
}

fn js_error(_: JsValue) -> worker::Error {
    worker::Error::from("auth rpc failed")
}
