//! 燃料サーチャージ請求の基礎データエンドポイント (Refs ohishi-exp/rust-ichibanboshi#12)
//!
//! 調査 #12 で確定した「`運転日報明細` の単一行から完了条件の全項目が揃う」結論に基づき、
//! 請求のみ行 (`請求K`='1') または通常運送行 (`請求K`='0') を
//! 得意先 / 積地県 / 卸地県 / 車種 / 売上年月日 / 運賃 / 請求日(入金予定) に展開して返す。
//!
//! サーチャージ計算本体に必要な残マスタ (燃費 km/L / 県庁間距離 / 週次軽油価格 /
//! 対象得意先フラグ) は外部データ or 新規構築のため本エンドポイントの scope 外。
//! まずは「請求対象行を県・車種付きで取り出す」基礎部分のみを提供する。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::repo::{DynRepo, RepoError};
use crate::routes::sales::{calc_next_month, ApiResponse};

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

/// `運転日報明細` 1 行 + マスタ join の生データ。
/// 県名は `地域ﾏｽﾀ.地域N` の生値 (正規化前) を保持し、ロジック層で県へ正規化する。
#[derive(Debug, Clone)]
pub struct RawSurchargeRow {
    pub request_kind: String,
    pub customer_code: String,
    pub customer_name: String,
    pub origin_area_name: String,
    pub dest_area_name: String,
    pub vehicle_code: String,
    pub vehicle_name: String,
    pub sale_date: NaiveDateTime,
    pub fare: i64,
    /// `入金予定日` (請求日)。NULL の行があり得るため Option。
    pub billing_date: Option<NaiveDateTime>,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

/// 車種ﾏｽﾀ 1 件 (燃費マスタの車種ドロップダウン用)。
#[derive(Serialize, Debug, PartialEq)]
pub struct VehicleOption {
    pub vehicle_code: String,
    pub vehicle_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct SurchargeRow {
    /// 請求区分 (1=請求のみ / 0=通常運送 / 2=非請求)
    pub request_kind: String,
    pub customer_code: String,
    pub customer_name: String,
    /// 積地県 (正規化済。未マップは "?")
    pub origin_prefecture: String,
    /// 卸地県 (正規化済。未マップは "?")
    pub dest_prefecture: String,
    pub vehicle_code: String,
    pub vehicle_name: String,
    pub sale_date: String,
    pub fare: i64,
    /// 請求日 (入金予定日)。NULL 行は null。
    pub billing_date: Option<String>,
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct SurchargeQuery {
    /// 売上年月の下限 (YYYY-MM、含む)
    pub from: Option<String>,
    /// 売上年月の上限 (YYYY-MM、含む)
    pub to: Option<String>,
    /// 請求区分の絞り込み: billing_only (default) | transport | all
    pub kind: Option<String>,
    /// 取得上限件数 (1..=10000、default 2000)
    pub limit: Option<i32>,
}

// ══════════════════════════════════════════════════════════════
// ロジック層 (純粋関数 — テスト可能)
// ══════════════════════════════════════════════════════════════

/// `地域ﾏｽﾀ.地域N` の先頭を都道府県に正規化する。
///
/// 調査 #12: `地域N` の先頭は必ず都道府県名。`北海道` のみ 4 文字、他は最初の
/// `県`/`府`/`都` まで。未マップ (空文字) は `"?"`。
/// `京都府` のように `都` を内包する `府` 県を誤らないよう `県`→`府`→`都` の順で判定する
/// (prototype SELECT の CASE WHEN 優先順位に一致)。
pub fn normalize_prefecture(area_name: &str) -> String {
    let s = area_name.trim();
    if s.is_empty() {
        return "?".to_string();
    }
    if s.starts_with("北海道") {
        return "北海道".to_string();
    }
    for suffix in ['県', '府', '都'] {
        if let Some(idx) = s.find(suffix) {
            let end = idx + suffix.len_utf8();
            return s[..end].to_string();
        }
    }
    s.to_string()
}

/// `kind` パラメータ → `請求K` の SQL WHERE フラグメント。
/// 未知の値は billing_only と同義 (= 請求のみ行) にフォールバックする。
pub fn surcharge_kind_filter(kind: &str) -> &'static str {
    match kind {
        "transport" => "AND t.[請求K] = '0'",
        "all" => "",
        _ => "AND t.[請求K] = '1'",
    }
}

/// `kind` パラメータ → source_table 表示用ラベル。
pub fn surcharge_kind_label(kind: &str) -> &'static str {
    match kind {
        "transport" => "通常運送 (請求K=0)",
        "all" => "全請求区分",
        _ => "請求のみ (請求K=1)",
    }
}

/// Raw 行リストをレスポンス行に変換 (県正規化・日付整形)。
pub fn build_surcharge_rows(raw: &[RawSurchargeRow]) -> Vec<SurchargeRow> {
    raw.iter()
        .map(|r| SurchargeRow {
            request_kind: r.request_kind.clone(),
            customer_code: r.customer_code.clone(),
            customer_name: r.customer_name.clone(),
            origin_prefecture: normalize_prefecture(&r.origin_area_name),
            dest_prefecture: normalize_prefecture(&r.dest_area_name),
            vehicle_code: r.vehicle_code.clone(),
            vehicle_name: r.vehicle_name.clone(),
            sale_date: r.sale_date.format("%Y-%m-%d").to_string(),
            fare: r.fare,
            billing_date: r.billing_date.map(|d| d.format("%Y-%m-%d").to_string()),
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════
// ハンドラ (薄い — param 解析 → repo → build → JSON)
// ══════════════════════════════════════════════════════════════

/// GET /api/vehicles — 車種ﾏｽﾀ (車種C, 車種N) の一覧。
/// 燃料サーチャージ請求側 (nuxt-ichibanboshi-seikyu) の燃費マスタ編集 UI が
/// 車種ドロップダウンの選択肢として取得する (車種C キーで燃費を登録)。
pub async fn vehicles(
    Extension(repo): Extension<DynRepo>,
) -> Result<Json<ApiResponse<Vec<VehicleOption>>>, StatusCode> {
    let rows = repo.vehicles().await.map_err(map_repo_err)?;
    Ok(Json(ApiResponse {
        source_table: "車種ﾏｽﾀ".to_string(),
        data: rows
            .into_iter()
            .map(|(code, name)| VehicleOption {
                vehicle_code: code,
                vehicle_name: name,
            })
            .collect(),
    }))
}

/// GET /api/surcharge/base
pub async fn surcharge_base(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<SurchargeQuery>,
) -> Result<Json<ApiResponse<Vec<SurchargeRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2025-04".to_string());
    let to = params.to.unwrap_or_else(|| "2026-03".to_string());
    let from_date = format!("{}-01", from);

    // 売上年月日 < (to の翌月初日) で上限を半開区間にする
    let parts: Vec<&str> = to.split('-').collect();
    let ty: i32 = parts.first().and_then(|s| s.parse().ok()).unwrap_or(2026);
    let tm: i32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(3);
    let (ny, nm) = calc_next_month(ty, tm);
    let to_date = format!("{}-{:02}-01", ny, nm);

    let kind = params.kind.unwrap_or_else(|| "billing_only".to_string());
    let kind_filter = surcharge_kind_filter(&kind);
    let limit = params.limit.unwrap_or(2000).clamp(1, 10000);

    let raw = repo
        .surcharge_base(&from_date, &to_date, kind_filter, limit)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 + 得意先ﾏｽﾀ + 車種ﾏｽﾀ + 地域ﾏｽﾀ [{}]",
            surcharge_kind_label(&kind)
        ),
        data: build_surcharge_rows(&raw),
    }))
}
