//! auth-worker の勤怠 Worker 専用の named entrypoint `KintaiAlcEntrypoint` (Service Binding `KINTAI_ALC_RPC`) の
//! `dtakoEtags(search)` で、alc (rust-alc-api) の `GET /api/dtako/events/etags` を読む (unko-gaps の GCP 側。Refs #322)。
//!
//! 渡すのは query 文字列 (`date_from=…&date_to=…`、kintai-logic の `etags_search`) だけ。path・method (GET)・tenant は
//! auth-worker 側で固定される (汎用の転送口 `InternalEntrypoint` は path の allowlist だけで tenant が呼び手任せなので使わない)。
//! `dtakoEtags` は失敗を throw せず `{status, body, contentType}` で返す。戻りの読み方 (404 だけ「口なし」・他の非 2xx は 502) は
//! `kintai_logic::unko_gaps::read_etags`。呼び方は workers/kyuyo の `worker/src/auth.rs` と同じ形。

use kintai_logic::common::Fail;
use kintai_logic::unko_gaps::{alc_rpc_failed, no_alc_rpc, RpcResult, ALC_RPC_BINDING};
use wasm_bindgen::prelude::*;
use worker::js_sys::{self, Reflect};
use worker::wasm_bindgen_futures::JsFuture;
use worker::Env;

#[wasm_bindgen]
extern "C" {
    #[wasm_bindgen(extends = js_sys::Object)]
    type KintaiAlcRpc;

    #[wasm_bindgen(method, catch, js_name = dtakoEtags)]
    fn dtako_etags(this: &KintaiAlcRpc, search: &str) -> Result<js_sys::Promise, JsValue>;
}

/// `KINTAI_ALC_RPC.dtakoEtags(search)` を呼ぶ。binding が無い = 503、RPC 自体が落ちた・戻りが読めない = 502。
/// エラーの中身は捨てる (固定の文言だけを返す)。
pub(crate) async fn fetch_etags(env: &Env, search: &str) -> Result<RpcResult, Fail> {
    let has_binding =
        Reflect::get(env, &JsValue::from(ALC_RPC_BINDING)).map_or(true, |v| !v.is_undefined());
    if !has_binding {
        return Err(no_alc_rpc());
    }
    let rpc = env
        .service(ALC_RPC_BINDING)
        .map_err(|_| alc_rpc_failed())?
        .into_rpc::<KintaiAlcRpc>();
    let call = rpc.dtako_etags(search).map_err(|_| alc_rpc_failed())?;
    let value = JsFuture::from(call).await.map_err(|_| alc_rpc_failed())?;
    serde_wasm_bindgen::from_value(value).map_err(|_| alc_rpc_failed())
}
