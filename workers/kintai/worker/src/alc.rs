//! auth-worker の勤怠 Worker 専用の named entrypoint `KintaiAlcEntrypoint` (Service Binding `KINTAI_ALC_RPC`) で alc
//! (rust-alc-api) を読む (Refs #322):
//!
//! - `dtakoEtags(search)` — `GET /api/dtako/events/etags` (unko-gaps の GCP 側と、fold の読みの dtako の指紋)
//! - `dtakoEvents(search)` — `GET /api/dtako/events` (fold の読みの全乗務員の生イベント)
//!
//! 渡すのは query 文字列 (kintai-logic の `etags_search`・`events_search`) だけ。path・method (GET)・tenant は auth-worker 側で
//! 固定される (汎用の転送口 `InternalEntrypoint` は path の allowlist だけで tenant が呼び手任せなので使わない)。どちらも失敗を
//! throw せず `{status, body, contentType}` で返す。戻りの読み方は kintai-logic (unko-gaps は `unko_gaps::read_etags`、
//! fold の読みは `fold_events`)。呼び方は workers/kyuyo の `worker/src/auth.rs` と同じ形。

use kintai_logic::common::Fail;
use kintai_logic::fold_events::AlcRpc;
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

    #[wasm_bindgen(method, catch, js_name = dtakoEvents)]
    fn dtako_events(this: &KintaiAlcRpc, search: &str) -> Result<js_sys::Promise, JsValue>;
}

/// どちらのメソッドを呼ぶか。
#[derive(Clone, Copy)]
enum Method {
    Etags,
    /// fold の読みの口 (次の PR) までは誰も呼ばない
    #[allow(dead_code)]
    Events,
}

/// `KINTAI_ALC_RPC.<method>(search)` を呼ぶ。binding が無い = 503、RPC 自体が落ちた・戻りが読めない = 502。
/// エラーの中身は捨てる (固定の文言だけを返す)。
async fn call(env: &Env, method: Method, search: &str) -> Result<RpcResult, Fail> {
    let has_binding =
        Reflect::get(env, &JsValue::from(ALC_RPC_BINDING)).map_or(true, |v| !v.is_undefined());
    if !has_binding {
        return Err(no_alc_rpc());
    }
    let rpc = env
        .service(ALC_RPC_BINDING)
        .map_err(|_| alc_rpc_failed())?
        .into_rpc::<KintaiAlcRpc>();
    let call = match method {
        Method::Etags => rpc.dtako_etags(search),
        Method::Events => rpc.dtako_events(search),
    };
    let value = JsFuture::from(call.map_err(|_| alc_rpc_failed())?)
        .await
        .map_err(|_| alc_rpc_failed())?;
    serde_wasm_bindgen::from_value(value).map_err(|_| alc_rpc_failed())
}

/// `KINTAI_ALC_RPC.dtakoEtags(search)` (unko-gaps)。
pub(crate) async fn fetch_etags(env: &Env, search: &str) -> Result<RpcResult, Fail> {
    call(env, Method::Etags, search).await
}

/// fold の読み (`kintai_logic::fold_events::WorkerFoldEvents`) に渡す RPC。
/// 口 (recalc・window・stale-months) は次の PR で足すので、今は誰も作らない。
#[allow(dead_code)]
pub(crate) struct EnvAlc<'a>(pub &'a Env);

impl AlcRpc for EnvAlc<'_> {
    async fn dtako_events(&self, search: &str) -> Result<RpcResult, Fail> {
        call(self.0, Method::Events, search).await
    }

    async fn dtako_etags(&self, search: &str) -> Result<RpcResult, Fail> {
        call(self.0, Method::Etags, search).await
    }
}
