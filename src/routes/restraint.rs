//! 拘束サマリの push 受け口 + wage-report 素材の一括配信 (Refs #106 Phase 3)。
//!
//! 消費者・生産者はどちらも nuxt-dtako-admin の dtako-scraper-relay (Cloudflare
//! Durable Object):
//!
//! - `PUT /api/restraint/summaries` — relay が theearth scrape / 勤怠取り込み /
//!   resummarize の後にサマリの写しを push する
//! - `GET /api/restraint/wage-source` — relay の `handleWageReport` が当月+前月 ×
//!   theearth+timecard の素材を **1 fetch** で引く (従来の R2 GET 約300本の置換)
//!
//! ## 認可 — CF Access Service Token (edge)
//!
//! `/kintai/daily` と同じ扱い。**データの ACL で選んでいる**: サマリは分・日数・
//! 氏名・所属のみで**金額を含まない**。消費者が Worker DO なのでブラウザ JWT を
//! 持てない。金額を足すことになったら給与大臣 Worker (`workers/kyuyo/`、認可は
//! auth-worker の `KyuyoAuthEntrypoint`) 側へ置くこと。
//!
//! ## 純粋部分は共有 crate (Refs #322)
//!
//! 検査と 400 の文言・前月・応答の型と組み立て・summary_json の読み取りは `kintai-logic` の
//! `restraint` (勤怠 Worker の D1 版と同じもの)。ここは axum・store の呼び出し・logging と、
//! オンプレ版だけの 503 / 500 の文言を持つ。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::{Extension, Json};
use kintai_logic::common::Fail;
use kintai_logic::restraint::{
    self, ErrorBody, PushBody, PushResponse, RestraintSyncedResponse, SyncedMonthsQuery,
    WageSourceMonth, WageSourceQuery, WageSourceResponse,
};

use crate::restraint_store::{DynRestraintStore, RestraintStoreError};

type ApiError = (StatusCode, Json<ErrorBody>);

fn err(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(ErrorBody {
            error: message.into(),
        }),
    )
}

/// 共有 crate の検査の失敗 (400) を応答へ。
fn fail(f: Fail) -> ApiError {
    let status = StatusCode::from_u16(f.status).expect("logic の status は正しい");
    (status, Json(ErrorBody::of(f)))
}

fn map_store_err(e: RestraintStoreError) -> ApiError {
    match &e {
        RestraintStoreError::OpenFailed(m) => {
            tracing::error!("restraint store unavailable: {m}");
            err(
                StatusCode::SERVICE_UNAVAILABLE,
                "拘束サマリ store が利用できません ([restraint] sqlite_path を確認してください)",
            )
        }
        _ => {
            tracing::error!("restraint store error: {e}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "拘束サマリ store の読み書きに失敗しました",
            )
        }
    }
}

// ══════════════════════════════════════════════════════════════
// PUT /api/restraint/summaries
// ══════════════════════════════════════════════════════════════

/// サマリ写しの upsert。**載っている乗務員だけ**を上書きする (replace-all では
/// ない) — relay は取り込みの乗務員CD 範囲ごとに push するため。body が大きく
/// なる月は relay 側が分割して複数回 PUT して良い (冪等)。
pub async fn put_summaries(
    Extension(store): Extension<DynRestraintStore>,
    Json(body): Json<PushBody>,
) -> Result<Json<PushResponse>, ApiError> {
    let valid = restraint::validate_push(body).map_err(fail)?;
    let synced_at = restraint::format_synced_at(chrono::Utc::now());
    store
        .upsert(
            &valid.comp_id,
            &valid.source,
            &valid.month,
            &valid.entries,
            &synced_at,
        )
        .await
        .map_err(map_store_err)?;
    let res = valid.response(synced_at);
    // 件数は先に出す — tracing マクロを複数行にすると購読者不在時に未到達 region が
    // 残り coverage_100 が落ちる (kintai.rs と同じ罠)
    let (comp_id, source, month, saved) = (&valid.comp_id, &valid.source, &valid.month, res.saved);
    tracing::info!(comp_id = %comp_id, source = %source, month = %month, saved, "restraint summaries pushed");
    Ok(Json(res))
}

// ══════════════════════════════════════════════════════════════
// GET /api/restraint/synced-months?comp=
// ══════════════════════════════════════════════════════════════

/// comp の push 済み (source, 月) 一覧 (Refs nuxt-dtako-admin#460)。消費側
/// (nuxt-dtako-admin の月タブ) が「高速表示可 (同期済み)」バッジと未同期時の
/// バックフィル案内を出すためのメタデータのみ。
pub async fn synced_months(
    Extension(store): Extension<DynRestraintStore>,
    Query(params): Query<SyncedMonthsQuery>,
) -> Result<Json<RestraintSyncedResponse>, ApiError> {
    let comp = restraint::parse_synced_months(params).map_err(fail)?;
    let rows = store.synced(&comp).await.map_err(map_store_err)?;
    Ok(Json(restraint::synced_response(rows)))
}

// ══════════════════════════════════════════════════════════════
// GET /api/restraint/wage-source?comp=&month=
// ══════════════════════════════════════════════════════════════

async fn read_month(
    store: &DynRestraintStore,
    comp_id: &str,
    source: &str,
    ym: &str,
) -> Result<WageSourceMonth, ApiError> {
    let month = store
        .month(comp_id, source, ym)
        .await
        .map_err(map_store_err)?;
    let (out, broken) = restraint::month_source(month);
    // push 側で検証済みなので実際には起きない — 起きたら行単位で落として残りは返す
    for b in broken {
        tracing::warn!(driver_cd = %b.driver_cd, "restraint summary_json broken: {}", b.error);
    }
    Ok(out)
}

/// wage-report の素材一括配信 — 当月+前月 (週40h の月初跨ぎ週用) × 両 source を
/// 1 応答で返す。
pub async fn wage_source(
    Extension(store): Extension<DynRestraintStore>,
    Query(params): Query<WageSourceQuery>,
) -> Result<Json<WageSourceResponse>, ApiError> {
    let req = restraint::parse_wage_source(params).map_err(fail)?;
    let [a, b, c, d] = req.reads();
    let comp = &req.comp_id;
    let months = [
        read_month(&store, comp, a.0, &a.1).await?,
        read_month(&store, comp, b.0, &b.1).await?,
        read_month(&store, comp, c.0, &c.1).await?,
        read_month(&store, comp, d.0, &d.1).await?,
    ];
    Ok(Json(req.respond(months)))
}
