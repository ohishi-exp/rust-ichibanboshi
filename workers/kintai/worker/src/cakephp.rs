//! 社内 CakePHP (`yhonda-ohishi/nginx`) への中継 3 本 (Refs #322)。Workers VPC の VPC Service (HTTP、binding
//! `KINTAI_CAKEPHP_VPC`) で直接叩く (オンプレ版の reqwest の代わり)。
//!
//! - `GET /api/kintai/daily?month=` — 認可なし。毎回 CakePHP の daily-json を中継する (キャッシュなし、`source` は常に `live`)
//! - `GET /api/kintai/pdf-json?month=[&driver=]` — 認可なし。`recalc=0` 固定で中継
//! - `POST /api/dtako/autoload?unko_no=&file_name=&preview=&reset_timecard=` — **書き込みの口** (`preview=true` も)。
//!   順は **認可 → 入力 → binding → 段取り** (② autoload → ② が 2xx なら ① MariaDB で材料を数える → 1 件以上なら ③ reset)
//!
//! ★ **CakePHP への fetch はすべて `redirect: manual`。** Workers の fetch は既定で 3xx を追う。追うと 307 の先が 200 で
//! 返り、オンプレ版では止まる ③ が走る (オンプレ版も POST 専用の client で追わない)。3xx はそのまま受けて location を返す。
//!
//! 検査・応答・失敗の写像は kintai-logic の `cakephp_relay`、URL・multipart・段取りは `cakephp`・`dtako_autoload`
//! (オンプレ版と共有)。ここは fetch・打ち切り・MariaDB の往復だけ。

use std::pin::pin;
use std::time::Duration;

use futures_util::future::{select, Either};
use kintai_logic::cakephp::{
    autoload_multipart, autoload_response, is_success, join, reset_multipart, reset_timecard_path,
    status_error, CakephpError, DtakoAutoloadResponse, Multipart, ResetTimecardResponse,
    AUTOLOAD_PATH, DTAKO_AUTOLOAD_TIMEOUT_SECS,
};
use kintai_logic::cakephp_relay::{
    autoload_fail, map_cakephp_err, material_count, material_fail, material_sql, parse_autoload,
    ReadRequest, CAKEPHP_VPC_BINDING, TIMEOUT, TIMEOUT_SECS, VPC_ORIGIN,
};
use kintai_logic::common::{mariadb_unconfigured, Fail};
use kintai_logic::dtako_autoload::{run, AutoloadIo, MaterialQuery};
use worker::wasm_bindgen::{JsCast, JsValue};
use worker::{js_sys, Date, Delay, Env, Fetcher, Method, Request, RequestInit, RequestRedirect};

use crate::Bytes;

/// CakePHP への口。binding が無ければ `None`。VPC Service の binding の JS 側の型名は公開されていないので、
/// 型名は見ずに値があるかだけで受ける (`fetch(url, init)` を持つ)。
fn fetcher(env: &Env) -> Option<Fetcher> {
    let value = js_sys::Reflect::get(env.as_ref(), &JsValue::from_str(CAKEPHP_VPC_BINDING)).ok()?;
    if value.is_undefined() || value.is_null() {
        return None;
    }
    Some(value.unchecked_into())
}

/// CakePHP の応答 (status・Location・本文)。
struct Reply {
    status: u16,
    location: Option<String>,
    body: Vec<u8>,
}

/// 1 回の fetch。**3xx を追わない。** `limit` で打ち切ったら `RequestFailed("timeout")`、届かなければ
/// `RequestFailed("fetch")` (宛先・文言は載せない)。本文が読めなければ空 (オンプレ版の `unwrap_or_default` と同じ)。
async fn send(
    vpc: &Fetcher,
    method: Method,
    path: &str,
    form: Option<Multipart>,
    limit: Duration,
) -> Result<Reply, CakephpError> {
    let mut init = RequestInit::new();
    init.with_method(method)
        .with_redirect(RequestRedirect::Manual);
    if let Some(form) = form {
        let headers = worker::Headers::new();
        let ok = headers.set("content-type", &form.content_type);
        ok.map_err(|_| CakephpError::RequestFailed("headers".into()))?;
        let body = js_sys::Uint8Array::from(form.body.as_slice());
        init.with_headers(headers).with_body(Some(body.into()));
    }
    let url = join(VPC_ORIGIN, path);
    let exchange = async {
        let mut res = vpc.fetch(url, Some(init)).await.map_err(|_| "fetch")?;
        let location = res.headers().get("location").ok().flatten();
        let body = res.bytes().await.unwrap_or_default();
        Ok::<_, &str>(Reply {
            status: res.status_code(),
            location,
            body,
        })
    };
    match select(pin!(exchange), pin!(Delay::from(limit))).await {
        Either::Left((result, _)) => result.map_err(|k| CakephpError::RequestFailed(k.into())),
        Either::Right(_) => Err(CakephpError::RequestFailed(TIMEOUT.into())),
    }
}

/// daily・pdf-json。検査 (400) → binding (503) → fetch (502) → 応答。
pub(crate) async fn read(req: ReadRequest, env: &Env) -> Result<Bytes, Fail> {
    let vpc = fetcher(env).ok_or_else(|| map_cakephp_err(CakephpError::NotConfigured))?;
    let limit = Duration::from_secs(TIMEOUT_SECS);
    let reply = send(&vpc, Method::Get, &req.path, None, limit).await;
    let reply = reply.map_err(map_cakephp_err)?;
    if !is_success(reply.status) {
        let text = String::from_utf8_lossy(&reply.body);
        return Err(map_cakephp_err(status_error(reply.status, &text)));
    }
    let body = req.respond(&reply.body, Date::now().as_millis())?;
    Ok(Bytes { body, etag: None })
}

/// autoload。認可 (403 / 503、[`crate::writes::serve`] が先に済ませる) → 入力 (400 / 413) → 段取り (binding が無ければ
/// preview は `configured: false`、実行は 503)。② の送信の失敗は 502 (待ちの打ち切りは「不明」の文言)。
pub(crate) async fn autoload(req: &mut Request, env: &Env) -> Result<serde_json::Value, Fail> {
    let query = req.url().map(|u| u.query().unwrap_or("").to_string());
    let query = query.unwrap_or_default();
    let body = req
        .bytes()
        .await
        .map_err(|_| Fail::new(400, "Failed to buffer the request body"))?;
    let request = parse_autoload(&query, body.len())?;
    let io = WorkerIo {
        vpc: fetcher(env),
        env,
        body,
    };
    run(&io, &request).await.map_err(autoload_fail)
}

/// ② ③ は VPC の HTTP、① は社内 MariaDB (他の口と同じ接続・資格情報)。
struct WorkerIo<'a> {
    vpc: Option<Fetcher>,
    env: &'a Env,
    body: Vec<u8>,
}

impl AutoloadIo for WorkerIo<'_> {
    fn configured(&self) -> bool {
        self.vpc.is_some()
    }

    async fn autoload(&self, file_name: &str) -> Result<DtakoAutoloadResponse, CakephpError> {
        let vpc = self.vpc.as_ref().ok_or(CakephpError::NotConfigured)?;
        let form = autoload_multipart(&boundary(), file_name, &self.body);
        let limit = Duration::from_secs(DTAKO_AUTOLOAD_TIMEOUT_SECS);
        let reply = send(vpc, Method::Post, AUTOLOAD_PATH, Some(form), limit).await?;
        let text = String::from_utf8_lossy(&reply.body);
        Ok(autoload_response(reply.status, &text, reply.location))
    }

    async fn count_material(&self, q: &MaterialQuery) -> Result<i64, String> {
        let sql = material_sql(q)?;
        let creds = crate::load_creds(self.env).await;
        let creds = creds.ok_or_else(|| mariadb_unconfigured().body)?;
        let set = crate::run_sql(self.env, &creds, &sql).await;
        let set = set.map_err(|f| material_fail(&format!("{}:{}", f.stage.as_str(), f.kind)))?;
        material_count(&set.rows)
    }

    async fn reset(&self, unko_no: &str) -> Result<ResetTimecardResponse, CakephpError> {
        let vpc = self.vpc.as_ref().ok_or(CakephpError::NotConfigured)?;
        let form = reset_multipart(&boundary());
        let limit = Duration::from_secs(TIMEOUT_SECS);
        let path = reset_timecard_path(unko_no);
        let reply = send(vpc, Method::Post, &path, Some(form), limit).await?;
        Ok(ResetTimecardResponse {
            status: reply.status,
            location: reply.location,
        })
    }
}

/// multipart の境界 (オンプレ版と同じ形・長さ)。本文と衝突しなければよいので `crypto.getRandomValues` までは使わない。
fn boundary() -> String {
    let word = || {
        let hi = (js_sys::Math::random() * 4_294_967_296.0) as u64;
        let lo = (js_sys::Math::random() * 4_294_967_296.0) as u64;
        (hi << 32) | lo
    };
    kintai_logic::cakephp::boundary([word(), word(), word(), word()])
}
