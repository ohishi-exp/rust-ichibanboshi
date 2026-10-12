//! 社内 CakePHP (`/uriage-jyuchu-display/*`) を Workers VPC の VPC Service (HTTP、binding `URIAGE_CAKEPHP_VPC`) で GET する
//! (オンプレ版の reqwest の代わり。Refs ohishi-exp/rust-ichibanboshi#322)。
//! workers/kintai/worker/src/cakephp.rs の `fetcher`・`send` の写し (GET だけ。multipart の分岐は写さない)。
//!
//! ★ **fetch は `redirect: manual`。** Workers の fetch は既定で 3xx を追う。3xx は成功にしない (`Status` = 502 / job の failed)。
//! 失敗は種類だけ (`CakephpError`)。宛先・本文はどこにも出さない。

use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use serde::de::DeserializeOwned;
use uriage_logic::cakephp::{
    join, masters_path, read_json, CakephpError, EditableMonthsResponse, MastersResponse,
    CAKEPHP_VPC_BINDING, EDITABLE_MONTHS_PATH, TIMEOUT_SECS, VPC_ORIGIN,
};
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::{js_sys, Delay, Env, Fetcher, Method, RequestInit, RequestRedirect};

/// CakePHP への口。binding が無ければ `None`。VPC Service の binding の JS 側の型名は公開されていないので、
/// 型名は見ずに値があるかだけで受ける (`fetch(url, init)` を持つ)。
pub(crate) fn fetcher(env: &Env) -> Option<Fetcher> {
    let value = binding(env, CAKEPHP_VPC_BINDING)?;
    Some(value.unchecked_into())
}

/// env の binding (型を問わない)。無ければ `None`。
pub(crate) fn binding(env: &Env, name: &str) -> Option<JsValue> {
    let value = js_sys::Reflect::get(env.as_ref(), &JsValue::from_str(name)).ok()?;
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(value)
}

/// 1 回の GET。**3xx を追わない。** 打ち切ったら `Timeout`、届かなければ `Request`。本文が読めなければ空。
async fn send(vpc: &Fetcher, path: &str) -> Result<(u16, Vec<u8>), CakephpError> {
    let mut init = RequestInit::new();
    init.with_method(Method::Get)
        .with_redirect(RequestRedirect::Manual);
    let url = join(VPC_ORIGIN, path);
    let exchange = async {
        let mut res = vpc
            .fetch(url, Some(init))
            .await
            .map_err(|_| CakephpError::Request)?;
        let body = res.bytes().await.unwrap_or_default();
        Ok((res.status_code(), body))
    };
    let limit = Delay::from(Duration::from_secs(TIMEOUT_SECS));
    match select(pin!(exchange), pin!(limit)).await {
        Either::Left((result, _)) => result,
        Either::Right(_) => Err(CakephpError::Timeout),
    }
}

async fn get<T: DeserializeOwned>(vpc: &Fetcher, path: &str) -> Result<T, CakephpError> {
    let (status, body) = send(vpc, path).await?;
    read_json(status, &body)
}

/// `/uriage-jyuchu-display/editable-months`
pub(crate) async fn editable_months(vpc: &Fetcher) -> Result<EditableMonthsResponse, CakephpError> {
    get(vpc, EDITABLE_MONTHS_PATH).await
}

/// `/uriage-jyuchu-display/masters-json?date=YYYY-MM-DD` (月末日)
pub(crate) async fn masters(vpc: &Fetcher, date: &str) -> Result<MastersResponse, CakephpError> {
    get(vpc, &masters_path(date)).await
}
