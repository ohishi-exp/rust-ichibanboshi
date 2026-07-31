//! 全量再計算の口 (Refs #205 の 06 の HTTP 版)。**GCP 側の instance が使う**。
//!
//! 窓の受け口 ([`crate::routes::kintai_timecard::receive_window`]) が畳み直すのは
//! **その窓で打刻が変わった乗務員だけ**で、定常時はほぼ 0 人。畳んだ結果が古くなる
//! 経路はもう 1 つあり、そちらはこの口が受け持つ:
//!
//! | 古くなる原因 | 誰が直すか |
//! |---|---|
//! | 打刻が増えた / 直った | 窓の受け口 (変わった乗務員だけ) |
//! | **`kosoku.rs` を deploy した** | **この口** (全乗務員が stale) |
//! | **TOML の閾値・丸め方を変えた** | **この口** (同上) |
//!
//! 後者は `logic_version` が変わるので**全単位が一斉に stale になる**。窓 1 往復で
//! 畳み直そうとすると必ず時間切れになるため、口を分けて `after_driver_cd` で
//! ページングする。stale かどうかは窓の応答の `stale.drivers` で分かる。
//!
//! ## 1 ページの乗務員数を小さく採る
//!
//! `GET /api/kintai/timecard/drivers` の 50 ([`crate::kintai_diff::DEFAULT_MAX_DRIVERS`])
//! より小さいのは、**1 乗務員あたりの費用が違う**から。あちらは乗務員CD を数える
//! 1 クエリで 1 ページぶんが返るのに対し、こちらは乗務員ごとに生イベントの読み
//! (alc への 1 往復 = R2 GET が何十回) と書き込みトランザクションを 1 本ずつ払う。
//!
//! **[`DEFAULT_MAX_FOLD_DRIVERS`] は未実測。** 呼び直せば続きから進むだけなので、
//! 時間切れしても壊れない側に倒してある (#226 が実測して上げたのと同じ手順を
//! 踏めばよい)。
//!
//! ## GET は絶対に書かない
//!
//! `GET` は `apply` を持たない。「読むだけのつもりが全乗務員を書き直していた」を
//! 作れないようにするため、書ける口は `POST` だけにしてある。auth-worker の
//! `/ichibanboshi-proxy` は **path + method の完全一致 allowlist** なので、
//! 口を足したら `nuxt-dtako-admin` 側の登録も要る。

use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::kintai_fold::{recalc_driver_page, recalc_drivers, stale_state, FoldReport};
use crate::kintai_push::KintaiPgStore;
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{assert_same_tenant, map_push_err, store_for};
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// 1 回で畳み直す乗務員数の既定。モジュール docs のとおり**未実測**。
pub const DEFAULT_MAX_FOLD_DRIVERS: usize = 20;

/// `max_drivers` の上限。呼び出し側が大きな値を入れて proxy を殺すのを防ぐ。
pub const MAX_MAX_FOLD_DRIVERS: usize = 50;

/// `?month=&after_driver_cd=&max_drivers=&stale_only=`。
#[derive(Debug, Default, Deserialize)]
pub struct RecalcQuery {
    pub month: Option<String>,
    /// 続きから回す位置。前回の応答の `next_after_driver_cd` を渡す。
    #[serde(default)]
    pub after_driver_cd: Option<i64>,
    #[serde(default)]
    pub max_drivers: Option<usize>,
    /// 現行の `logic_version` の行を 1 つも持たない乗務員だけに絞る。
    ///
    /// **既定 `false`。** 勤務が 1 本も立たない乗務員は保存行を作れないので
    /// `true` では毎回この網に残る (収束しない) — 全量を回すときは既定のまま使う。
    #[serde(default)]
    pub stale_only: bool,
}

/// `POST /api/kintai/recalc` の本体。`GET` の query と同じ項目 + `apply`。
#[derive(Debug, Default, Deserialize)]
pub struct RecalcRequest {
    pub month: String,
    #[serde(default)]
    pub after_driver_cd: Option<i64>,
    #[serde(default)]
    pub max_drivers: Option<usize>,
    #[serde(default)]
    pub stale_only: bool,
    /// **`true` で初めて書く。** 既定は 1 行も書かない。
    #[serde(default)]
    pub apply: bool,
}

fn bad_request(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

/// GET /api/kintai/recalc — **1 行も書かずに**、いま畳み直すと何が変わるかを返す。
pub async fn preview(
    headers: HeaderMap,
    Query(q): Query<RecalcQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let req = RecalcRequest {
        month: q.month.unwrap_or_default(),
        after_driver_cd: q.after_driver_cd,
        max_drivers: q.max_drivers,
        stale_only: q.stale_only,
        apply: false,
    };
    run(headers, pg, repo, params, read_tenant, req).await
}

/// POST /api/kintai/recalc — 1 ページぶんを畳み直す。`apply` が無ければ書かない。
pub async fn recalc(
    headers: HeaderMap,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
    Json(req): Json<RecalcRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    run(headers, pg, repo, params, read_tenant, req).await
}

async fn run(
    headers: HeaderMap,
    pg: DynKintaiPgStore,
    repo: DynKintaiEventsRepo,
    params: std::sync::Arc<crate::kosoku::KosokuParams>,
    read_tenant: ReadTenant,
    req: RecalcRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&req.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let started = std::time::Instant::now();
    let st = store_for(&pg, &headers)?;
    // 読みと書きが別テナントのまま畳まない (窓の受け口と同じ検査)
    assert_same_tenant(read_tenant, st.tenant_id())?;
    let max = req
        .max_drivers
        .unwrap_or(DEFAULT_MAX_FOLD_DRIVERS)
        .clamp(1, MAX_MAX_FOLD_DRIVERS);
    let page = recalc_driver_page(
        &st,
        &req.month,
        &params,
        req.after_driver_cd,
        req.stale_only,
        max,
    )
    .await
    .map_err(map_push_err)?;
    let (folded, stale) = fold_page(&repo, &st, &params, &req, &page).await?;
    // 回りきったかは「1 ページに満たなかったか」で決める。次を空で返すより往復が 1 回減る
    let next = match page.len() < max {
        true => None,
        false => page.last().copied(),
    };
    // マクロは 1 行に収める (CLAUDE.md)
    let (n, w) = (folded.drivers, folded.drivers_written);
    tracing::info!(n, w, "kintai recalc page done");
    Ok(Json(serde_json::json!({
        "month": req.month,
        "apply": req.apply,
        "drivers": page,
        "next_after_driver_cd": next,
        "fold": folded,
        "stale": stale,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })))
}

/// 1 ページぶんを畳み、`stale` を数える。
async fn fold_page(
    repo: &DynKintaiEventsRepo,
    st: &KintaiPgStore,
    params: &crate::kosoku::KosokuParams,
    req: &RecalcRequest,
    page: &[i64],
) -> Result<(FoldReport, crate::kintai_fold::StaleReport), (StatusCode, String)> {
    let drivers: Vec<u64> = page.iter().filter(|d| **d > 0).map(|d| *d as u64).collect();
    // 上流 warnings を握り潰さない ([`crate::kintai_http_repo::with_warning_sink`])
    let (folded, warnings) = crate::kintai_http_repo::with_warning_sink(recalc_drivers(
        repo, st, params, &req.month, &drivers, req.apply,
    ))
    .await;
    let mut folded = folded.map_err(map_push_err)?;
    folded.warnings = warnings;
    let months = [req.month.clone()];
    let stale = stale_state(st, &months, params)
        .await
        .map_err(map_push_err)?;
    Ok((folded, stale))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr() -> HeaderMap {
        HeaderMap::new()
    }

    fn repo() -> DynKintaiEventsRepo {
        std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo)
    }

    fn params() -> std::sync::Arc<crate::kosoku::KosokuParams> {
        std::sync::Arc::new(crate::kosoku::KosokuParams::default())
    }

    /// 壊れた月は**書き先を見に行く前に** 400 で落とす。
    #[tokio::test]
    async fn the_month_is_validated_before_the_store() {
        for m in ["", "2026-7", "nope"] {
            let (code, _) = recalc(
                hdr(),
                Extension(None),
                Extension(repo()),
                Extension(params()),
                Extension(ReadTenant(None)),
                Json(RecalcRequest {
                    month: m.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST, "month={m:?}");
        }
    }

    /// **書き先が無い instance では 503。** オンプレの既定がこれ。
    #[tokio::test]
    async fn it_fails_closed_without_a_store() {
        let (code, msg) = recalc(
            hdr(),
            Extension(None),
            Extension(repo()),
            Extension(params()),
            Extension(ReadTenant(None)),
            Json(RecalcRequest {
                month: "2026-07".to_string(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }

    /// **GET は `apply` を持たない。** 読むだけの口から書けないようにしてある。
    #[tokio::test]
    async fn the_get_route_cannot_apply() {
        let (code, _) = preview(
            hdr(),
            Query(RecalcQuery {
                month: Some("2026-07".to_string()),
                ..Default::default()
            }),
            Extension(None),
            Extension(repo()),
            Extension(params()),
            Extension(ReadTenant(None)),
        )
        .await
        .unwrap_err();
        // 書き先が無いので 503 まで進む = month は通っている
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        // GET の query には apply が無い (コンパイル時に保証されている)
        assert!(!RecalcRequest::default().apply);
    }

    #[test]
    fn the_page_size_is_clamped() {
        assert_eq!(DEFAULT_MAX_FOLD_DRIVERS.clamp(1, MAX_MAX_FOLD_DRIVERS), 20);
        assert_eq!(0_usize.clamp(1, MAX_MAX_FOLD_DRIVERS), 1);
        assert_eq!(999_usize.clamp(1, MAX_MAX_FOLD_DRIVERS), 50);
    }
}
