//! auth-worker の勤怠 Worker 専用の named entrypoint `KintaiAlcEntrypoint` (Service Binding `KINTAI_ALC_RPC`) で alc
//! (rust-alc-api) を読む (Refs #322):
//!
//! - `dtakoEtags(search)` — `GET /api/dtako/events/etags` (unko-gaps の GCP 側と、fold の読みの dtako の指紋)
//! - `dtakoEvents(search)` — `GET /api/dtako/events` (fold の読みの全乗務員の生イベント)
//!
//! 渡すのは query 文字列 (kintai-logic の `etags_search`・`events_search`) と、既定以外のテナントのときだけ第 2 引数の
//! `tenantId` (auth-worker の許可リストの中から選ぶ。外は 403・未設定は 503)。既定のテナントでは渡さない (auth-worker の既定の
//! KV 固定のまま。auth-worker 側が未対応でも今と同じ動作)。path・method (GET) は auth-worker 側で固定される (汎用の転送口 `InternalEntrypoint` は path の allowlist だけで tenant が呼び手任せなので使わない)。どちらも失敗を
//! throw せず `{status, body, contentType}` で返す。戻りの読み方は kintai-logic (unko-gaps は `unko_gaps::read_etags`、
//! fold の読みは `fold_events`)。呼び方は workers/kyuyo の `worker/src/auth.rs` と同じ形。

use kintai_logic::common::{Fail, Tenancy};
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

    // 既定以外のテナント (第 2 引数の tenantId を付ける)
    #[wasm_bindgen(method, catch, js_name = dtakoEtags)]
    fn dtako_etags_for(
        this: &KintaiAlcRpc,
        search: &str,
        tenant_id: &str,
    ) -> Result<js_sys::Promise, JsValue>;

    #[wasm_bindgen(method, catch, js_name = dtakoEvents)]
    fn dtako_events_for(
        this: &KintaiAlcRpc,
        search: &str,
        tenant_id: &str,
    ) -> Result<js_sys::Promise, JsValue>;
}

/// どちらのメソッドを呼ぶか。
#[derive(Clone, Copy)]
enum Method {
    Etags,
    Events,
}

/// `KINTAI_ALC_RPC.<method>(search[, tenantId])` を呼ぶ (`tenant_id` は既定以外のテナントのときだけ `Some`)。
/// binding が無い = 503、RPC 自体が落ちた・戻りが読めない = 502。エラーの中身は捨てる (固定の文言だけを返す)。
async fn call(
    env: &Env,
    method: Method,
    search: &str,
    tenant_id: Option<&str>,
) -> Result<RpcResult, Fail> {
    let has_binding =
        Reflect::get(env, &JsValue::from(ALC_RPC_BINDING)).map_or(true, |v| !v.is_undefined());
    if !has_binding {
        return Err(no_alc_rpc());
    }
    let rpc = env
        .service(ALC_RPC_BINDING)
        .map_err(|_| alc_rpc_failed())?
        .into_rpc::<KintaiAlcRpc>();
    let call = match (method, tenant_id) {
        (Method::Etags, None) => rpc.dtako_etags(search),
        (Method::Events, None) => rpc.dtako_events(search),
        (Method::Etags, Some(t)) => rpc.dtako_etags_for(search, t),
        (Method::Events, Some(t)) => rpc.dtako_events_for(search, t),
    };
    let value = JsFuture::from(call.map_err(|_| alc_rpc_failed())?)
        .await
        .map_err(|_| alc_rpc_failed())?;
    serde_wasm_bindgen::from_value(value).map_err(|_| alc_rpc_failed())
}

/// `KINTAI_ALC_RPC.dtakoEtags(search[, tenantId])` (unko-gaps)。
pub(crate) async fn fetch_etags(
    env: &Env,
    search: &str,
    tenancy: &Tenancy,
) -> Result<RpcResult, Fail> {
    let tenant_id = tenancy.alc_tenant_id();
    call(env, Method::Etags, search, tenant_id.as_deref()).await
}

/// fold の読み (`kintai_logic::fold_events::WorkerFoldEvents`) に渡す RPC (recalc・timecard/window の口が使う)。
/// `tenant_id` は既定以外のテナントのときだけ `Some` ([`Tenancy::alc_tenant_id`])。
pub(crate) struct EnvAlc<'a> {
    pub env: &'a Env,
    pub tenant_id: Option<String>,
}

impl<'a> EnvAlc<'a> {
    pub(crate) fn new(env: &'a Env, tenancy: &Tenancy) -> Self {
        Self {
            env,
            tenant_id: tenancy.alc_tenant_id(),
        }
    }
}

impl AlcRpc for EnvAlc<'_> {
    async fn dtako_events(&self, search: &str) -> Result<RpcResult, Fail> {
        call(self.env, Method::Events, search, self.tenant_id.as_deref()).await
    }

    async fn dtako_etags(&self, search: &str) -> Result<RpcResult, Fail> {
        call(self.env, Method::Etags, search, self.tenant_id.as_deref()).await
    }
}
