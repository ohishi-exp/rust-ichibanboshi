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
//! 持てない。金額を足すことになったら `/kyuyo/*` の in-service gate へ移すこと。
//!
//! ## サマリ JSON は解釈しない
//!
//! entries の `summary` は relay のサマリ (RestraintDriverSummary /
//! TimecardDriverSummary) を `serde_json::Value` のまま保存・返却する — 行の形は
//! relay (TS) 側が golden テストで固定しており、本サービスが追従する必要はない。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};

use crate::restraint_store::{
    DynRestraintStore, RestraintEntry, RestraintStoreError, RestraintSyncedRow,
};
use crate::routes::kintai::is_valid_month;

/// エラーレスポンス本文 (kyuyo と同形)。
#[derive(Serialize, Debug)]
pub struct ErrorBody {
    pub error: String,
}

type ApiError = (StatusCode, Json<ErrorBody>);

fn err(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(ErrorBody {
            error: message.into(),
        }),
    )
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

const SOURCES: [&str; 2] = ["theearth", "timecard"];

/// comp_id の書式検証 (dtako テナントの会社ID。パス・SQL には bind でしか使わない
/// ので緩めで良いが、明らかなゴミは弾く)。
fn is_valid_comp(comp_id: &str) -> bool {
    !comp_id.is_empty()
        && comp_id.len() <= 64
        && comp_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// 'YYYY-MM' の前月。`is_valid_month` 通過済み前提。
fn prev_month(ym: &str) -> String {
    let year: i32 = ym[..4].parse().expect("validated year");
    let month: u32 = ym[5..].parse().expect("validated month");
    if month == 1 {
        format!("{}-12", year - 1)
    } else {
        format!("{year}-{:02}", month - 1)
    }
}

// ══════════════════════════════════════════════════════════════
// PUT /api/restraint/summaries
// ══════════════════════════════════════════════════════════════

/// push される 1 乗務員分。
#[derive(Deserialize, Debug)]
pub struct PushEntry {
    pub driver_cd: String,
    /// 「該当データがありません」マーカー (Refs nuxt-dtako-admin#241)。
    #[serde(default)]
    pub no_data: bool,
    /// サマリ JSON verbatim (no_data の時は省略可)。
    #[serde(default)]
    pub summary: Option<serde_json::Value>,
    #[serde(default)]
    pub fetched_at: Option<String>,
    #[serde(default)]
    pub last_verified_at: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PushBody {
    pub comp_id: String,
    /// 'theearth' | 'timecard'
    pub source: String,
    /// 'YYYY-MM'
    pub month: String,
    pub entries: Vec<PushEntry>,
}

#[derive(Serialize, Debug)]
pub struct PushResponse {
    pub saved: usize,
    pub synced_at: String,
}

/// サマリ写しの upsert。**載っている乗務員だけ**を上書きする (replace-all では
/// ない) — relay は取り込みの乗務員CD 範囲ごとに push するため。body が大きく
/// なる月は relay 側が分割して複数回 PUT して良い (冪等)。
pub async fn put_summaries(
    Extension(store): Extension<DynRestraintStore>,
    Json(body): Json<PushBody>,
) -> Result<Json<PushResponse>, ApiError> {
    if !is_valid_comp(&body.comp_id) {
        return Err(err(StatusCode::BAD_REQUEST, "comp_id が不正です"));
    }
    if !SOURCES.contains(&body.source.as_str()) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "source は theearth / timecard のいずれかで指定してください",
        ));
    }
    if !is_valid_month(&body.month) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください",
        ));
    }
    let mut entries: Vec<RestraintEntry> = Vec::with_capacity(body.entries.len());
    for e in &body.entries {
        if e.driver_cd.is_empty() {
            return Err(err(
                StatusCode::BAD_REQUEST,
                "driver_cd が空の entry があります",
            ));
        }
        if !e.no_data && e.summary.is_none() {
            return Err(err(
                StatusCode::BAD_REQUEST,
                format!(
                    "driver_cd={} は no_data でないのに summary がありません",
                    e.driver_cd
                ),
            ));
        }
        entries.push(RestraintEntry {
            driver_cd: e.driver_cd.clone(),
            no_data: e.no_data,
            // 検証済みの Value を verbatim 文字列化して保存 (取り出し時は素通し)
            summary_json: e.summary.as_ref().map(|v| v.to_string()),
            fetched_at: e.fetched_at.clone(),
            last_verified_at: e.last_verified_at.clone(),
        });
    }
    let synced_at = chrono::Utc::now().to_rfc3339();
    store
        .upsert(
            &body.comp_id,
            &body.source,
            &body.month,
            &entries,
            &synced_at,
        )
        .await
        .map_err(map_store_err)?;
    // 件数は先に出す — tracing マクロを複数行にすると購読者不在時に未到達 region が
    // 残り coverage_100 が落ちる (kintai.rs と同じ罠)
    let saved = entries.len();
    tracing::info!(comp_id = %body.comp_id, source = %body.source, month = %body.month, saved, "restraint summaries pushed");
    Ok(Json(PushResponse { saved, synced_at }))
}

// ══════════════════════════════════════════════════════════════
// GET /api/restraint/synced-months?comp=
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct SyncedMonthsQuery {
    pub comp: String,
}

#[derive(Serialize, Debug)]
pub struct RestraintSyncedEntry {
    /// 'theearth' | 'timecard'
    pub source: String,
    pub month: String,
    pub synced_at: String,
    pub row_count: i64,
}

#[derive(Serialize, Debug)]
pub struct RestraintSyncedResponse {
    pub entries: Vec<RestraintSyncedEntry>,
}

/// comp の push 済み (source, 月) 一覧 (Refs nuxt-dtako-admin#460)。消費側
/// (nuxt-dtako-admin の月タブ) が「高速表示可 (同期済み)」バッジと未同期時の
/// バックフィル案内を出すためのメタデータのみ。
pub async fn synced_months(
    Extension(store): Extension<DynRestraintStore>,
    Query(params): Query<SyncedMonthsQuery>,
) -> Result<Json<RestraintSyncedResponse>, ApiError> {
    if !is_valid_comp(&params.comp) {
        return Err(err(StatusCode::BAD_REQUEST, "comp が不正です"));
    }
    let rows: Vec<RestraintSyncedRow> = store.synced(&params.comp).await.map_err(map_store_err)?;
    Ok(Json(RestraintSyncedResponse {
        entries: rows
            .into_iter()
            .map(|r| RestraintSyncedEntry {
                source: r.source,
                month: r.month,
                synced_at: r.synced_at,
                row_count: r.row_count,
            })
            .collect(),
    }))
}

// ══════════════════════════════════════════════════════════════
// GET /api/restraint/wage-source?comp=&month=
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct WageSourceQuery {
    pub comp: String,
    pub month: String,
}

/// 1 ヶ月 × 1 source 分の素材 (relay の loadMonthSummaries の置換素材)。
#[derive(Serialize, Debug)]
pub struct WageSourceMonth {
    /// [{driver_cd, summary, fetched_at, last_verified_at}] (driver_cd 昇順)。
    pub summaries: Vec<WageSourceSummary>,
    pub no_data_drivers: Vec<String>,
    /// この (comp, source, month) が最後に push を受けた時刻。未 push は null —
    /// relay はその時だけ R2 フォールバックする。
    pub synced_at: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct WageSourceSummary {
    pub driver_cd: String,
    /// サマリ JSON verbatim (relay 側の型で解釈する)。
    pub summary: serde_json::Value,
    pub fetched_at: Option<String>,
    pub last_verified_at: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct WageSourceResponse {
    pub comp_id: String,
    pub month: String,
    pub prev_month: String,
    pub current_theearth: WageSourceMonth,
    pub current_timecard: WageSourceMonth,
    pub prev_theearth: WageSourceMonth,
    pub prev_timecard: WageSourceMonth,
}

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
    let mut summaries = Vec::new();
    let mut no_data_drivers = Vec::new();
    for e in month.entries {
        if e.no_data {
            no_data_drivers.push(e.driver_cd);
            continue;
        }
        let Some(json) = e.summary_json else { continue };
        match serde_json::from_str::<serde_json::Value>(&json) {
            Ok(summary) => summaries.push(WageSourceSummary {
                driver_cd: e.driver_cd,
                summary,
                fetched_at: e.fetched_at,
                last_verified_at: e.last_verified_at,
            }),
            Err(parse_err) => {
                // push 側で検証済みなので実際には起きない — 起きたら行単位で落として
                // 残りは返す (1 名の破損で月全体を殺さない)
                tracing::warn!(driver_cd = %e.driver_cd, "restraint summary_json broken: {parse_err}");
            }
        }
    }
    Ok(WageSourceMonth {
        summaries,
        no_data_drivers,
        synced_at: month.synced_at,
    })
}

/// wage-report の素材一括配信 — 当月+前月 (週40h の月初跨ぎ週用) × 両 source を
/// 1 応答で返す。
pub async fn wage_source(
    Extension(store): Extension<DynRestraintStore>,
    Query(params): Query<WageSourceQuery>,
) -> Result<Json<WageSourceResponse>, ApiError> {
    if !is_valid_comp(&params.comp) {
        return Err(err(StatusCode::BAD_REQUEST, "comp が不正です"));
    }
    if !is_valid_month(&params.month) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください",
        ));
    }
    let prev = prev_month(&params.month);
    let current_theearth = read_month(&store, &params.comp, "theearth", &params.month).await?;
    let current_timecard = read_month(&store, &params.comp, "timecard", &params.month).await?;
    let prev_theearth = read_month(&store, &params.comp, "theearth", &prev).await?;
    let prev_timecard = read_month(&store, &params.comp, "timecard", &prev).await?;
    Ok(Json(WageSourceResponse {
        comp_id: params.comp,
        month: params.month,
        prev_month: prev,
        current_theearth,
        current_timecard,
        prev_theearth,
        prev_timecard,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prev_month_handles_year_boundary() {
        assert_eq!(prev_month("2026-06"), "2026-05");
        assert_eq!(prev_month("2026-01"), "2025-12");
        assert_eq!(prev_month("2026-10"), "2026-09");
    }

    #[test]
    fn comp_validation() {
        assert!(is_valid_comp("27324455"));
        assert!(is_valid_comp("comp-1_a"));
        assert!(!is_valid_comp(""));
        assert!(!is_valid_comp("a/b"));
        assert!(!is_valid_comp(&"x".repeat(65)));
    }
}
