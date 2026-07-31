//! 打刻の受け口 (Refs #205 の 04b)。**GCP 側の instance が使う**。
//!
//! GCP には社内 MariaDB が無いので打刻が読めず、`shifts_from_timecard` が空になる。
//! 02 のモジュール docs が「#205 の 04 / 05 が埋める穴」と書いていたものを、
//! 穴の定義どおりに埋める — **オンプレが読んで、ここへ渡す**。
//!
//! | | どちら側 | |
//! |---|---|---|
//! | `GET /api/kintai/timecard/signatures` | GCP | 持っている (乗務員, 暦日) の署名 |
//! | `POST /api/kintai/timecard` | GCP | 差分の日だけを反映する |
//! | `GET /api/kintai/timecard/drivers` | オンプレ | 対象月の乗務員を 1 ページ |
//! | `POST /api/kintai/timecard/diff` | オンプレ | 署名を受け取り、差分を返す |
//!
//! ## 運ぶのは relay。オンプレは外へ出ない
//!
//! **relay が起動する側なので、オンプレから折り返さない。** relay が署名を GCP から
//! 引いてオンプレへ渡し、返ってきた差分を GCP へ渡す。オンプレは request / response
//! だけで、送り先 URL も相手の資格情報も持たない。順序は [`crate::kintai_diff`]。
//!
//! ## 全量を送らない
//!
//! 署名を突き合わせて**違う日と消えた日だけ**を運ぶ。1 か月・全乗務員の打刻は数万行
//! あるので、毎回全部送ると転送も書き込みも無駄になる。突き合わせは Rust 側
//! ([`crate::kintai_push::plan_batch`]) で、relay は署名を計算しない — 2 実装になると
//! 式のずれが「毎回全日が違う」に化ける。署名の作り方は [`crate::kintai_push`]。
//!
//! ## 認可は網層が持つ
//!
//! オンプレは Cloudflare Access Service Token (Tunnel の手前)、GCP は Cloud Run IAM。
//! アプリ層で認証を持たないのはこのリポジトリ全体の方針で、`/api/kintai/*` の
//! 既存ルートと同じ。
//!
//! ## テナントは `X-Tenant-ID` で名乗る
//!
//! 書き込み先の `tenant_id` は**リクエストから**取る。relay が KV
//! (`dtako-relay-config` の `dtako_accounts`) に持っている値がテナントの正で、
//! alc へも同じヘッダで渡している — 設定に写すと同じ値を 2 か所で保つことになる。
//!
//! `[kintai_push] tenant_id` は書いてあれば **pin** として働き、ヘッダと食い違えば
//! 403。単一テナントの instance を設定で固定したいときに使う。pin もヘッダも
//! 無ければ 400 — 書き先が決まらないまま受け取らない。
//!
//! ## 書き先が無ければ 503
//!
//! `[kintai_push] enabled = false` の instance (= オンプレの既定) では
//! `KintaiPgStore` が挿さらないので、両方 503 で fail-closed。「受け取ったが
//! どこにも書いていない」を作らない。

use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::kintai_diff::{diff_month, drivers_page, KintaiDiffError, DEFAULT_MAX_DRIVERS};
use crate::kintai_push::{
    apply_timecard_batch, apply_timecard_window, jst_day_bounds, KintaiPgStore, KintaiPushError,
    TimecardBatch, TimecardWindow,
};
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::is_valid_month;

/// `[kintai_push]` が無効な instance では挿さらない。
pub type DynKintaiPgStore = Option<std::sync::Arc<KintaiPgStore>>;

/// `[kintai_events] tenant_id` — **生イベントの読み先が名乗るテナント**。
///
/// 空 (= MariaDB 直読みの形。テナントの概念が無い) なら `None`。
///
/// 書き先は `X-Tenant-ID` が決めるのに対し、**読み先は設定で固定されている**
/// ([`crate::kintai_http_repo`] が全リクエストで同じ値を送る)。畳むときは両方を
/// 使うので、割れていると「A の打刻を B のデジタコで畳んで A に書く」が成立する。
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadTenant(pub Option<uuid::Uuid>);

/// `?month=YYYY-MM&driver_cd=1130`。どちらも必須。
#[derive(Debug, Deserialize)]
pub struct SignaturesQuery {
    pub month: Option<String>,
    pub driver_cd: Option<i64>,
}

fn bad_request(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

/// 書き先が無い / DB が落ちている、を分けて返す。
pub(crate) fn map_push_err(e: KintaiPushError) -> (StatusCode, String) {
    match e {
        // 宣言していない instance に投げられた = 呼び出し側の経路違い
        KintaiPushError::NotConfigured(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
        other => {
            // **無言の 502 を作らない** (Refs #205 の 20)。2026-07-31 に
            // `kintai.fold_gate` の GRANT 漏れで `POST /api/kintai/recalc` が
            // 502 を返し続けたとき、ここが何も出さないせいで「コンテナが落ちて
            // いる」ようにしか見えず、切り分けに丸 1 タスクかかった
            tracing::warn!(error = %other, "kintai push failed (502)");
            (StatusCode::BAD_GATEWAY, other.to_string())
        }
    }
}

fn store(store: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    store.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// relay が alc へ渡しているのと同じヘッダ名 ([`crate::kintai_http_repo`] の送信側)。
const TENANT_HEADER: &str = "X-Tenant-ID";

/// **書き先のテナントを決める。** ヘッダが正、設定の `tenant_id` は pin。
///
/// 決まらなければ書かない — 既定のテナントへ落とすと、名乗り忘れた relay の打刻が
/// 静かに別テナントの `kintai.*` に積まれる。
fn tenant_of(headers: &HeaderMap, pin: uuid::Uuid) -> Result<uuid::Uuid, (StatusCode, String)> {
    let raw = headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .trim();
    if raw.is_empty() {
        // pin だけで動かす構成 (単一テナントの instance) は許す
        return match pin.is_nil() {
            true => Err(bad_request("X-Tenant-ID が必要です")),
            false => Ok(pin),
        };
    }
    let tenant = uuid::Uuid::parse_str(raw)
        .map_err(|_| bad_request("X-Tenant-ID は alc_api.tenants.id の UUID です"))?;
    if tenant.is_nil() {
        return Err(bad_request("X-Tenant-ID が nil UUID です"));
    }
    if !pin.is_nil() && tenant != pin {
        // 設定で固定した instance に別テナントを名乗られた = 経路違い
        return Err((
            StatusCode::FORBIDDEN,
            "X-Tenant-ID が [kintai_push] tenant_id と一致しません".to_string(),
        ));
    }
    Ok(tenant)
}

/// 書き先を解決して、そのテナント向けの store を返す。
pub(crate) fn store_for(
    pg: &DynKintaiPgStore,
    headers: &HeaderMap,
) -> Result<KintaiPgStore, (StatusCode, String)> {
    let st = store(pg)?;
    Ok(st.for_tenant(tenant_of(headers, st.tenant_id())?))
}

/// **読みと書きが同じテナントか。** 畳むときだけ効く。
///
/// 読み (生イベント) は設定で固定、書き (畳んだ 3 表) はリクエストが名乗る。
/// 割れたまま畳むと、別テナントのデジタコで組んだ勤務をこちらのテナントへ
/// 書き込む — RLS も指紋も「別テナントの入力で作った」ことを教えてくれない。
///
/// 読み先がテナントを名乗らない形 (MariaDB 直読み) では検査しない。社内 DB に
/// テナントの概念が無く、突き合わせる相手が存在しない。
pub(crate) fn assert_same_tenant(
    read: ReadTenant,
    write: uuid::Uuid,
) -> Result<(), (StatusCode, String)> {
    match read.0 {
        Some(t) if t != write => Err((
            StatusCode::FORBIDDEN,
            "X-Tenant-ID が [kintai_events] tenant_id と一致しません (畳めません)".to_string(),
        )),
        _ => Ok(()),
    }
}

/// 月の JST 境界を `TIMESTAMPTZ` の対で返す。
fn month_bounds(
    month: &str,
) -> Option<(
    chrono::DateTime<chrono::FixedOffset>,
    chrono::DateTime<chrono::FixedOffset>,
)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = chrono::NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((jst_day_bounds(first).0, jst_day_bounds(next).0))
}

/// `?month=YYYY-MM&after_driver_cd=&max_drivers=`。
#[derive(Debug, Deserialize)]
pub struct DriversQuery {
    pub month: Option<String>,
    /// 続きから回す位置。前回の応答の `next_after_driver_cd` を渡す。
    #[serde(default)]
    pub after_driver_cd: Option<u64>,
    /// 1 回で返す乗務員数。Tunnel の 30 秒に収める。
    #[serde(default)]
    pub max_drivers: Option<usize>,
}

/// `POST /api/kintai/timecard/diff` の本体。
#[derive(Debug, Deserialize)]
pub struct DiffRequest {
    pub month: String,
    /// **相手 (GCP) が持っている署名。** relay が
    /// `GET /api/kintai/timecard/signatures` から集めて渡す。
    /// キーが対象の乗務員CD で、値が `{暦日: 署名}`。
    #[serde(default)]
    pub remote:
        std::collections::BTreeMap<u64, std::collections::BTreeMap<chrono::NaiveDate, String>>,
}

/// 呼び方が悪い (400) と、読み先が落ちている (502) を分ける。
fn map_diff_err(e: KintaiDiffError) -> (StatusCode, String) {
    match e {
        KintaiDiffError::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
        other => (StatusCode::BAD_GATEWAY, other.to_string()),
    }
}

/// GET /api/kintai/timecard/drivers?month=&after_driver_cd=&max_drivers=
/// — 対象月の乗務員を 1 ページ返す。
///
/// **乗務員数で区切る。** この口は Cloudflare Tunnel (30 秒上限) を通って叩かれる
/// ので、全乗務員を 1 回で返すと必ず 502 になる。応答の `next_after_driver_cd` が
/// `null` になるまで relay が呼び直す。
pub async fn drivers(
    Query(params): Query<DriversQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let page = drivers_page(
        &repo,
        &month,
        params.after_driver_cd,
        params.max_drivers.unwrap_or(DEFAULT_MAX_DRIVERS),
    )
    .await
    .map_err(map_diff_err)?;
    Ok(Json(serde_json::json!({
        "month": month,
        "drivers": page.drivers,
        "next_after_driver_cd": page.next_after_driver_cd,
        "elapsed_ms": page.elapsed_ms,
    })))
}

/// POST /api/kintai/timecard/diff — 相手の署名と突き合わせて**渡すべき差分を返す**。
///
/// **何も書かないし、どこへも送らない。** relay が受け取った `batches` をそのまま
/// GCP の `POST /api/kintai/timecard` へ渡す。オンプレから外へ出る経路を作らない
/// ため、送信はこちらの責務にしない ([`crate::kintai_diff`] のモジュール docs)。
pub async fn diff(
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Json(req): Json<DiffRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&req.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let report = diff_month(&repo, &req.month, &req.remote)
        .await
        .map_err(map_diff_err)?;
    // マクロは 1 行に収める (CLAUDE.md)
    let (n, c, d) = (report.drivers, report.days_changed, report.days_deleted);
    tracing::info!(n, c, d, "timecard diffed");
    if report.has_unexpected() {
        tracing::warn!(n, "timecard had odd rows");
    }
    Ok(Json(serde_json::json!(report)))
}

/// GET /api/kintai/timecard/signatures?month=&driver_cd= — 相手が持っている日別署名。
pub async fn signatures(
    headers: HeaderMap,
    Query(params): Query<SignaturesQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let driver_cd = params
        .driver_cd
        .ok_or_else(|| bad_request("driver_cd は必須です"))?;
    let (from, to) = month_bounds(&month).ok_or_else(|| bad_request("month が不正です"))?;
    let st = store_for(&pg, &headers)?;
    let sigs = st
        .stored_day_signatures(driver_cd, from, to)
        .await
        .map_err(map_push_err)?;
    Ok(Json(serde_json::json!({
        "month": month,
        "driver_cd": driver_cd,
        "signatures": sigs,
    })))
}

/// `?months=2026-06,2026-07`。送り主が覆う月をそのまま名乗る。
#[derive(Debug, Deserialize)]
pub struct WindowQuery {
    pub months: Option<String>,
}

/// GET /api/kintai/timecard/events?months=YYYY-MM,YYYY-MM
/// → **窓ぶんの打刻を全乗務員まとめて**返す (オンプレ)。
///
/// 乗務員でも日でも刻まない。実測では、乗務員ごとに 1 往復していたレグが 94 名で
/// 33.6 秒だったのに対し、同じ月の全打刻の転送は 1.3 秒だった — 費用は往復の回数。
///
/// **何も書かない。** 冪等なので呼び直しは安全。
pub async fn window_events(
    Query(params): Query<WindowQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let months = parse_months(params.months.as_deref().unwrap_or_default())?;
    let started = std::time::Instant::now();
    let (from, to) = window_bounds(&months).ok_or_else(|| bad_request("months が不正です"))?;
    let events = repo
        .fetch_timecard_window(&from, &to)
        .await
        .map_err(|e| map_diff_err(KintaiDiffError::Read(e)))?;
    let drivers: Vec<u64> = events
        .iter()
        .filter_map(|r| r.get("driver_id").and_then(|v| v.as_u64()))
        .collect::<std::collections::BTreeSet<u64>>()
        .into_iter()
        .collect();
    let n = events.len();
    tracing::info!(n, "timecard window read");
    Ok(Json(serde_json::json!({
        "months": months,
        "drivers": drivers,
        "events": events,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })))
}

/// `months=YYYY-MM,YYYY-MM` を検証して返す。**重複は潰し、昇順に揃える。**
fn parse_months(raw: &str) -> Result<Vec<String>, (StatusCode, String)> {
    let months: std::collections::BTreeSet<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if months.is_empty() {
        return Err(bad_request(
            "months は YYYY-MM をカンマ区切りで指定してください",
        ));
    }
    if let Some(bad) = months.iter().find(|m| !is_valid_month(m)) {
        return Err(bad_request(&format!("month は YYYY-MM です: {bad}")));
    }
    Ok(months.into_iter().collect())
}

/// 窓ぜんたいの `[最初の月初, 最後の翌月初)` を MariaDB 用の文字列で返す。
///
/// 月が飛んでいても 1 クエリで読む — 隙間ぶんが混ざっても受け側が窓の外として
/// 落とすので、往復を増やすより安い。
fn window_bounds(months: &[String]) -> Option<(String, String)> {
    let first = crate::kintai_repo::exact_month_range(months.first()?)?;
    let last = crate::kintai_repo::exact_month_range(months.last()?)?;
    Some((first.0, last.1))
}

/// POST /api/kintai/timecard/window — **窓ぶんをまるごと**受けて、変わった日だけ書き、
/// 変わった乗務員だけ**畳み直す** (Refs #205 の 06)。
///
/// 突き合わせはここ。送り主に署名を引かせない ([`apply_timecard_window`])。
///
/// ## 畳むところまでを 1 往復に束ねる
///
/// 「運んだが畳んでいない」状態を作らない。読み出しは計算しないので (#205 の決定 2)、
/// 畳んだ値が古いままだと**遅いのではなく静かに間違う** — リスク欄の筆頭がこれ。
///
/// **対象は窓で変わった乗務員だけ。** 打刻はほとんど戻らないので定常時はほぼ 0 人で、
/// auth-worker proxy (Cloudflare) の 100 秒に収まる。差分ゼロなら読み先も叩かない。
///
/// ## 全量再計算はここでやらない
///
/// deploy や TOML 変更で**全乗務員が stale** になったときは、窓 1 往復では終わらない。
/// 応答の `stale.drivers` がそれを示すので、呼び出し側はページングする
/// `POST /api/kintai/recalc` ([`crate::routes::kintai_recalc`]) に回す。
///
/// ## 畳めなくても 200
///
/// 打刻を書いたあとに fold が落ちても**リクエスト全体は成功**で、応答の
/// `fold_error` に理由が載る ([`fold_into`])。全体を 5xx にすると relay が
/// 「窓ごと失敗」と読んで同じ窓を再送し続けるため。テナントの不一致だけは
/// **書く前**に 403 で断つので、この形にならない。
pub async fn receive_window(
    headers: HeaderMap,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
    Json(window): Json<TimecardWindow>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if window.months.is_empty() {
        return Err(bad_request("months が空です"));
    }
    if let Some(bad) = window.months.iter().find(|m| !is_valid_month(m)) {
        return Err(bad_request(&format!("month は YYYY-MM です: {bad}")));
    }
    let st = store_for(&pg, &headers)?;
    // **畳むなら読みと書きのテナントが同じであることを先に確かめる。** 打刻を
    // 書いてしまってから 403 にすると、書いた分だけが残って畳まれない
    if window.fold {
        assert_same_tenant(read_tenant, st.tenant_id())?;
    }
    let result = apply_timecard_window(&st, &window)
        .await
        .map_err(map_push_err)?;
    // マクロは 1 行に収める (CLAUDE.md)
    let (w, d, m) = (result.days_written, result.days_deleted, result.misplaced);
    tracing::info!(w, d, "timecard window applied");
    if result.has_unexpected() {
        tracing::warn!(m, "timecard window had odd rows");
    }
    let mut body = serde_json::json!(result);
    if window.fold {
        fold_into(&repo, &st, &params, &window, &result, &mut body).await;
    }
    Ok(Json(body))
}

/// 窓の apply 後に畳み直し、結果を応答へ**書き足す**。
///
/// ## fold の失敗で apply の成功を覆い隠さない
///
/// **`Result` を返さない。** ここへ来た時点で打刻は既にコミット済みなので、
/// fold が落ちたからとリクエスト全体を 5xx にすると、relay は「窓ごと失敗」と読んで
/// **同じ窓を再送し続ける** — 打刻は毎回冪等に書き直され、fold は毎回同じ理由で
/// 落ちる。実際に起き得る形で、`[kintai_events]` が未設定の instance では
/// 読み先が `NotConfigured` を返す。
///
/// 代わりに 200 のまま `fold_error` を応答に載せる。#205 のリスク欄の「push した
/// だけで計算していない状態を作らない」とは矛盾しない — **畳めなかったことが
/// 応答に明示される**のが、その状態を黙って作らないということ。
///
/// `fold` と `stale` は独立に試す。畳めたのに stale の集計だけ落ちたときに、
/// 何を書いたかの報告まで失わないため。
///
/// ## dry-run の窓
///
/// `apply = false` で回すので 1 行も書かない。ただし dry-run では打刻も書いて
/// いないので、報告は「**保存済みの打刻で畳んだら**」であって「いま届いた窓を
/// 反映したら」ではない ([`crate::kintai_fold::recalc_drivers`])。
async fn fold_into(
    repo: &DynKintaiEventsRepo,
    st: &KintaiPgStore,
    params: &crate::kosoku::KosokuParams,
    window: &TimecardWindow,
    result: &crate::kintai_push::TimecardWindowResult,
    body: &mut serde_json::Value,
) {
    match fold_window(repo, st, params, window, result).await {
        Ok(folded) => {
            // マクロは 1 行に収める (CLAUDE.md)
            let (n, wr) = (folded.drivers, folded.drivers_written);
            tracing::info!(n, wr, "timecard window folded");
            body["fold"] = serde_json::json!(folded);
        }
        Err((code, msg)) => {
            // 打刻は書けている。畳めなかったことを応答と log の両方で loud に
            tracing::error!(status = code.as_u16(), "timecard window fold failed");
            body["fold_error"] = serde_json::json!(msg);
            body["fold_error_status"] = serde_json::json!(code.as_u16());
        }
    }
    match crate::kintai_fold::stale_state(st, &window.months, params).await {
        Ok(stale) => {
            if stale.drivers > 0 {
                tracing::warn!(s = stale.drivers, "fold is stale — run recalc");
            }
            body["stale"] = serde_json::json!(stale);
        }
        Err(e) => {
            tracing::error!("timecard window stale check failed");
            body["stale_error"] = serde_json::json!(e.to_string());
        }
    }
}

/// 窓ぶんを畳んで [`FoldReport`] を返す。
///
/// [`FoldReport`]: crate::kintai_fold::FoldReport
async fn fold_window(
    repo: &DynKintaiEventsRepo,
    st: &KintaiPgStore,
    params: &crate::kosoku::KosokuParams,
    window: &TimecardWindow,
    result: &crate::kintai_push::TimecardWindowResult,
) -> Result<crate::kintai_fold::FoldReport, (StatusCode, String)> {
    let drivers: Vec<u64> = result
        .drivers_changed
        .iter()
        .filter(|d| **d > 0)
        .map(|d| *d as u64)
        .collect();
    let apply = !window.dry_run;
    // 上流 warnings を握り潰さない — R2 の分割遅れの最中に畳むと、欠けた入力を
    // 「最新」として保存する。tracing だけでは呼び出し側から見えない
    let (folded, warnings) = crate::kintai_http_repo::with_warning_sink(async {
        let mut report = crate::kintai_fold::FoldReport::default();
        for month in &window.months {
            let r = crate::kintai_fold::recalc_drivers(repo, st, params, month, &drivers, apply)
                .await?;
            report = merge_fold(report, r);
        }
        Ok::<_, KintaiPushError>(report)
    })
    .await;
    let mut folded = folded.map_err(map_push_err)?;
    folded.warnings = warnings;
    Ok(folded)
}

/// 月ごとの [`FoldReport`] を足し合わせる。窓は複数月を覆う。
///
/// [`FoldReport`]: crate::kintai_fold::FoldReport
fn merge_fold(
    mut acc: crate::kintai_fold::FoldReport,
    r: crate::kintai_fold::FoldReport,
) -> crate::kintai_fold::FoldReport {
    acc.drivers += r.drivers;
    acc.drivers_written += r.drivers_written;
    acc.drivers_unchanged += r.drivers_unchanged;
    acc.shifts += r.shifts;
    acc.day_summaries += r.day_summaries;
    acc.day_parts += r.day_parts;
    acc.skipped.extend(r.skipped);
    acc.elapsed_ms += r.elapsed_ms;
    acc.dry_run = r.dry_run;
    acc.logic_version = r.logic_version;
    acc.calculated_at = r.calculated_at;
    acc
}

/// POST /api/kintai/timecard — 差分の日だけを反映する。
pub async fn receive(
    headers: HeaderMap,
    Extension(pg): Extension<DynKintaiPgStore>,
    Json(batch): Json<TimecardBatch>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&batch.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let st = store_for(&pg, &headers)?;
    let result = apply_timecard_batch(&st, &batch)
        .await
        .map_err(map_push_err)?;
    // 件数は先に出す (tracing の引数は購読者が居ないと評価されない)。
    // **マクロは 1 行に収める** — 折り返すと llvm-cov の行カバレッジに乗らず
    // 100% gate が落ちる (CLAUDE.md)
    let (w, d, m) = (result.days_written, result.days_deleted, result.misplaced);
    tracing::info!(w, d, "timecard applied");
    if result.has_unexpected() {
        tracing::warn!(m, "timecard had odd rows");
    }
    Ok(Json(serde_json::json!(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(month: Option<&str>, driver: Option<i64>) -> Query<SignaturesQuery> {
        Query(SignaturesQuery {
            month: month.map(str::to_string),
            driver_cd: driver,
        })
    }

    fn batch(month: &str) -> TimecardBatch {
        TimecardBatch {
            month: month.to_string(),
            driver_cd: 1130,
            days: Default::default(),
            delete_dates: Vec::new(),
        }
    }

    /// `X-Tenant-ID: <uuid>` の付いたヘッダ。
    fn hdr(tenant: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        if !tenant.is_empty() {
            h.insert(TENANT_HEADER, tenant.parse().unwrap());
        }
        h
    }

    fn uuid(s: &str) -> uuid::Uuid {
        uuid::Uuid::parse_str(s).unwrap()
    }

    const T1: &str = "11111111-2222-3333-4444-555555555555";
    const T2: &str = "99999999-8888-7777-6666-555555555555";

    /// **書き先が無い instance では 503。** オンプレの既定がこれ。
    /// 200 で空を返すと「受け取ったがどこにも書いていない」が作れてしまう。
    #[tokio::test]
    async fn both_routes_fail_closed_without_a_store() {
        let (code, msg) = signatures(hdr(T1), q(Some("2026-07"), Some(1130)), Extension(None))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");

        let (code, _) = receive(hdr(T1), Extension(None), Json(batch("2026-07")))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// 壊れたパラメータは**書き先を見に行く前に** 400 で落とす。
    #[tokio::test]
    async fn parameters_are_validated_before_the_store() {
        for m in [None, Some(""), Some("2026-7"), Some("nope")] {
            let (code, _) = signatures(hdr(T1), q(m, Some(1130)), Extension(None))
                .await
                .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST, "month={m:?}");
        }
        // driver_cd 省略
        let (code, msg) = signatures(hdr(T1), q(Some("2026-07"), None), Extension(None))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(msg.contains("driver_cd"), "{msg}");

        let (code, _) = receive(hdr(T1), Extension(None), Json(batch("nope")))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn month_bounds_is_jst_and_wraps_the_year() {
        let (a, b) = month_bounds("2026-12").unwrap();
        assert_eq!(a.to_rfc3339(), "2026-12-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2027-01-01T00:00:00+09:00");
        let (a, b) = month_bounds("2026-07").unwrap();
        assert_eq!(a.to_rfc3339(), "2026-07-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2026-08-01T00:00:00+09:00");
        assert!(month_bounds("2026-13").is_none());
        assert!(month_bounds("nope").is_none());
        assert!(month_bounds("20xx-07").is_none());
    }

    /// 壊れた月は**読みに行く前に** 400 で落とす (両方の口で)。
    #[tokio::test]
    async fn the_month_is_validated_before_the_repo() {
        let repo: DynKintaiEventsRepo =
            std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo);
        for m in [None, Some(""), Some("2026-7"), Some("nope")] {
            let (code, _) = drivers(
                Query(DriversQuery {
                    month: m.map(str::to_string),
                    after_driver_cd: None,
                    max_drivers: None,
                }),
                Extension(repo.clone()),
            )
            .await
            .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST, "month={m:?}");
        }

        let (code, _) = diff(
            Extension(repo),
            Json(DiffRequest {
                month: "nope".to_string(),
                remote: Default::default(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    /// 呼び方が悪い (400) と、読み先が落ちている (502) を分ける。
    #[test]
    fn diff_errors_separate_the_caller_from_the_source() {
        let (code, _) = map_diff_err(KintaiDiffError::BadRequest("bad month".to_string()));
        assert_eq!(code, StatusCode::BAD_REQUEST);
        let (code, _) = map_diff_err(KintaiDiffError::Read(
            crate::kintai_repo::KintaiRepoError::NotConfigured,
        ));
        assert_eq!(code, StatusCode::BAD_GATEWAY);
    }

    /// **ヘッダが正。** relay が KV から持ってくる値がそのまま書き先になる。
    #[test]
    fn the_request_names_the_tenant() {
        assert_eq!(tenant_of(&hdr(T1), uuid::Uuid::nil()).unwrap(), uuid(T1));
        // 前後の空白は relay 側の整形ゆれなので受ける
        let mut h = HeaderMap::new();
        h.insert(TENANT_HEADER, format!("  {T1}  ").parse().unwrap());
        assert_eq!(tenant_of(&h, uuid::Uuid::nil()).unwrap(), uuid(T1));
    }

    /// **名乗りが無く pin も無ければ 400。** 既定のテナントへ落とすと、
    /// 名乗り忘れた打刻が静かに別テナントへ積まれる。
    #[test]
    fn an_unnamed_tenant_is_refused_when_nothing_pins_it() {
        let (code, msg) = tenant_of(&hdr(""), uuid::Uuid::nil()).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(msg.contains("X-Tenant-ID"), "{msg}");

        // 空白だけ / nil UUID も「名乗っていない」扱い
        let mut h = HeaderMap::new();
        h.insert(TENANT_HEADER, "   ".parse().unwrap());
        let (code, _) = tenant_of(&h, uuid::Uuid::nil()).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);

        let (code, _) = tenant_of(&hdr(&uuid::Uuid::nil().to_string()), uuid(T1)).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);

        // UUID でなければ打ち間違い
        let (code, _) = tenant_of(&hdr("ichibanboshi"), uuid::Uuid::nil()).unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    /// `[kintai_push] tenant_id` は **pin**。ヘッダが無ければ代わりに使い、
    /// 食い違えば 403 — 設定で固定した instance は他テナントを受けない。
    #[test]
    fn the_configured_tenant_pins_the_header() {
        assert_eq!(tenant_of(&hdr(""), uuid(T1)).unwrap(), uuid(T1));
        assert_eq!(tenant_of(&hdr(T1), uuid(T1)).unwrap(), uuid(T1));

        let (code, msg) = tenant_of(&hdr(T2), uuid(T1)).unwrap_err();
        assert_eq!(code, StatusCode::FORBIDDEN);
        assert!(msg.contains("tenant_id"), "{msg}");
    }

    /// テナントは**書き先を確かめてから**見る (503 が 400 に化けない)。
    #[tokio::test]
    async fn the_missing_store_is_reported_before_the_tenant() {
        let (code, _) = receive(hdr(""), Extension(None), Json(batch("2026-07")))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// **読みと書きが別テナントなら畳まない** (Refs #205 の 06)。
    ///
    /// 生イベントは `[kintai_events] tenant_id` で固定、打刻は `X-Tenant-ID` が
    /// 名乗る。割れたまま畳むと、別テナントのデジタコで組んだ勤務がこちらの
    /// `kintai.shifts` に入る — RLS も指紋もそれを教えてくれない。
    #[test]
    fn folding_needs_the_read_and_write_tenants_to_agree() {
        assert!(assert_same_tenant(ReadTenant(Some(uuid(T1))), uuid(T1)).is_ok());

        let (code, msg) = assert_same_tenant(ReadTenant(Some(uuid(T2))), uuid(T1)).unwrap_err();
        assert_eq!(code, StatusCode::FORBIDDEN);
        assert!(msg.contains("kintai_events"), "{msg}");

        // 読み先がテナントを名乗らない形 (MariaDB 直読み) は突き合わせる相手が無い
        assert!(assert_same_tenant(ReadTenant(None), uuid(T1)).is_ok());
        assert!(assert_same_tenant(ReadTenant::default(), uuid(T1)).is_ok());
    }

    /// 窓ぜんたいを覆う月が壊れていれば、**書き先を見に行く前に** 400。
    #[tokio::test]
    async fn the_window_months_are_validated_first() {
        let repo: DynKintaiEventsRepo =
            std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo);
        let params = std::sync::Arc::new(crate::kosoku::KosokuParams::default());
        for months in [vec![], vec!["nope".to_string()]] {
            let (code, _) = receive_window(
                hdr(T1),
                Extension(None),
                Extension(repo.clone()),
                Extension(params.clone()),
                Extension(ReadTenant(None)),
                Json(TimecardWindow {
                    months,
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST);
        }
    }

    /// 月ごとの報告を足し合わせる。窓は複数月を覆う。
    #[test]
    fn fold_reports_add_up_across_months() {
        let june = crate::kintai_fold::FoldReport {
            drivers: 1,
            drivers_written: 1,
            shifts: 3,
            day_summaries: 3,
            day_parts: 4,
            logic_version: "old".to_string(),
            ..Default::default()
        };
        let july = crate::kintai_fold::FoldReport {
            drivers: 2,
            drivers_unchanged: 2,
            shifts: 1,
            day_summaries: 1,
            day_parts: 1,
            dry_run: true,
            logic_version: "new".to_string(),
            calculated_at: "t".to_string(),
            skipped: vec![crate::kintai_fold::SkipReason::DegenerateShift {
                start: "a".to_string(),
                end: "a".to_string(),
            }],
            ..Default::default()
        };
        let sum = merge_fold(june, july);
        assert_eq!(sum.drivers, 3);
        assert_eq!(sum.drivers_written, 1);
        assert_eq!(sum.drivers_unchanged, 2);
        assert_eq!(sum.shifts, 4);
        assert_eq!(sum.day_summaries, 4);
        assert_eq!(sum.day_parts, 5);
        assert_eq!(sum.skipped.len(), 1, "落とした行は畳まずに残す");
        // 版と時刻は月で変わらないので後勝ちでよい
        assert_eq!(sum.logic_version, "new");
        assert_eq!(sum.calculated_at, "t");
        assert!(sum.dry_run);
    }

    /// **既定は「畳む」。** 送り主が黙っていたら畳む側に倒す — 運んだのに
    /// 畳んでいない状態が #205 のリスク欄の筆頭。
    #[test]
    fn the_window_folds_unless_told_otherwise() {
        let w: TimecardWindow = serde_json::from_value(serde_json::json!({
            "months": ["2026-07"],
        }))
        .expect("deserialize");
        assert!(w.fold, "fold を書いていない窓も畳む");
        assert!(!w.dry_run);

        let off: TimecardWindow = serde_json::from_value(serde_json::json!({
            "months": ["2026-07"], "fold": false,
        }))
        .expect("deserialize");
        assert!(!off.fold);
    }

    /// 宣言していない (503) と、繋がらない (502) を分ける。
    #[test]
    fn errors_separate_declaration_from_failure() {
        let (code, msg) = map_push_err(KintaiPushError::NotConfigured("nope".to_string()));
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(msg, "nope");

        let (code, _) = map_push_err(KintaiPushError::Db(sqlx::Error::RowNotFound));
        assert_eq!(code, StatusCode::BAD_GATEWAY);
    }
}
