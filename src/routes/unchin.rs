//! 得意先・傭車先別 運賃リストの基礎データエンドポイント (Refs #57)
//!
//! `運転日報明細` から得意先 (`partner_type=customer`) または傭車先
//! (`partner_type=subcontractor`) 別の運賃候補行を抽出する。
//!
//! 同じ品目・金額・取引先の場合に積地・卸地ペアをまとめる集約処理は本 endpoint では
//! 行わず、raw 行をそのまま返す。集約・バージョン管理は消費側 (nuxt-ichibanboshi) が
//! 行う設計 (`docs/plan-unchin-rate-list.md` 参照)。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::repo::{DynRepo, RepoError};
use crate::routes::sales::ApiResponse;

fn map_repo_err(e: RepoError) -> StatusCode {
    match &e {
        RepoError::PoolError => StatusCode::SERVICE_UNAVAILABLE,
        RepoError::QueryError(msg) => {
            tracing::error!("Query error: {msg}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

// ══════════════════════════════════════════════════════════════
// Raw 中間構造体 (DB 層 → ロジック層 の橋渡し)
// ══════════════════════════════════════════════════════════════

/// `運転日報明細` 1 行の生データ（得意先 or 傭車先、いずれか一方の側面）。
#[derive(Debug, Clone)]
pub struct RawUnchinRow {
    /// 取引先コード（`得意先C`+`-`+`得意先H`、または `傭車先C`+`-`+`傭車先H`）。
    /// `H` は固定値ではなく変動するため、複合キーで一意化する（#57 実機調査確定）。
    pub partner_code: String,
    pub partner_name: String,
    /// 品名C。
    pub item_code: String,
    /// 品名N。
    pub item_name: String,
    /// 運賃額。customer 側は `金額+割増+実費`、subcontractor 側は
    /// `傭車金額+傭車割増+傭車実費`（#57 確定式。`金額` は既に税抜のため値引は無視）。
    pub fare: i64,
    /// 発地N（積地、自由入力の生文字列）。
    pub origin: String,
    /// 着地N（卸地、自由入力の生文字列）。
    pub dest: String,
    pub sale_date: NaiveDateTime,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct UnchinCandidateRow {
    pub partner_code: String,
    pub partner_name: String,
    pub item_code: String,
    pub item_name: String,
    pub fare: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: String,
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct UnchinQuery {
    /// 売上年月日 下限 (YYYY-MM-DD、含む)
    pub from: Option<String>,
    /// 売上年月日 上限 (YYYY-MM-DD、含まない)
    pub to: Option<String>,
    /// `"customer"`（得意先、default）| `"subcontractor"`（傭車先）
    pub partner_type: Option<String>,
    /// 取得上限件数 (1..=20000、default 5000)
    pub limit: Option<i32>,
}

// ══════════════════════════════════════════════════════════════
// ロジック層 (純粋関数 — テスト可能)
// ══════════════════════════════════════════════════════════════

/// `partner_type` パラメータを正規化する。`"subcontractor"` 以外は
/// `"customer"` にフォールバックする（`surcharge_kind_filter` と同じ緩い方針）。
pub fn normalize_partner_type(partner_type: &str) -> &'static str {
    match partner_type {
        "subcontractor" => "subcontractor",
        _ => "customer",
    }
}

/// Raw 行リストをレスポンス行に変換する（日付整形のみ）。
pub fn build_unchin_rows(raw: &[RawUnchinRow]) -> Vec<UnchinCandidateRow> {
    raw.iter()
        .map(|r| UnchinCandidateRow {
            partner_code: r.partner_code.clone(),
            partner_name: r.partner_name.clone(),
            item_code: r.item_code.clone(),
            item_name: r.item_name.clone(),
            fare: r.fare,
            origin: r.origin.clone(),
            dest: r.dest.clone(),
            sale_date: r.sale_date.format("%Y-%m-%d").to_string(),
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════
// ハンドラ (薄い — param 解析 → repo → build → JSON)
// ══════════════════════════════════════════════════════════════

/// GET /api/unchin/candidates?from=&to=&partner_type=customer|subcontractor&limit=
pub async fn unchin_candidates(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinCandidateRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let partner_type = normalize_partner_type(params.partner_type.as_deref().unwrap_or(""));
    let limit = params.limit.unwrap_or(5000).clamp(1, 20000);

    let raw = repo
        .unchin_candidates(&from, &to, partner_type, limit)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 + {}",
            if partner_type == "subcontractor" {
                "傭車先ﾏｽﾀ"
            } else {
                "得意先ﾏｽﾀ"
            }
        ),
        data: build_unchin_rows(&raw),
    }))
}
