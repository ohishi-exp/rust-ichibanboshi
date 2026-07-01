//! 得意先・傭車先別 運賃リストの基礎データエンドポイント (Refs #57)
//!
//! `運転日報明細` から得意先 (`partner_type=customer`) または傭車先
//! (`partner_type=subcontractor`) 別の運賃候補行・合計金額を抽出する。
//!
//! 同じ品目・金額・取引先の場合に積地・卸地ペアをまとめる集約処理は本 endpoint では
//! 行わず、raw 行をそのまま返す (`/candidates`)。集約・バージョン管理は消費側
//! (nuxt-ichibanboshi) が行う設計 (`docs/plan-unchin-rate-list.md` 参照)。
//!
//! `請求K` フィルタ (`kind`) は 2 択: `with_billing_only` (請求＋請求のみ,
//! K IN ('0','1')) / `with_non_billing` (請求＋非請求, K IN ('0','2')、default)。
//! 後者は本リポジトリの月計一致条件 (`請求K IN ('0','2')`) と同じ組み合わせ。
//! 「運行記録簿用」「事故請求コード」等の社内向けダミー得意先は単独の `請求K=2`
//! 行として記録されているが、`(0,2)` には含まれてしまうため値が大きい場合は
//! ユーザー側で目視除外が必要 (実機確認、#57)。

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
    /// `得意先ﾏｽﾀ`/`傭車先ﾏｽﾀ` の `部門C`（**自社側**の受注部門/営業所コード。
    /// 得意先・傭車先自身の拠点とは別軸、#92 follow-up 実機調査確定）。
    pub bumon_code: String,
    /// `部門ﾏｽﾀ.部門N`（自社営業所名）。未設定・未マッチ時は空文字。
    pub bumon_name: String,
}

/// 取引先ごとの合計金額 (`/summary` 用)。
#[derive(Debug, Clone)]
pub struct RawUnchinSummaryRow {
    pub partner_code: String,
    pub partner_name: String,
    pub total: i64,
    /// `RawUnchinRow::bumon_code` と同義（自社側の受注部門/営業所コード）。
    pub bumon_code: String,
    pub bumon_name: String,
}

/// 傭車先ごとの 売上(得意先請求)/支払 両建て合計 (`/subcontractor-net` 用)。
/// 「同一運行内の両建て」— 名寄せではなく、運転日報明細の同一行にある
/// 得意先側金額と傭車先側金額を突き合わせる (2026-07-01 user 確認)。
#[derive(Debug, Clone)]
pub struct RawUnchinSubcontractorNetRow {
    pub partner_code: String,
    pub partner_name: String,
    /// その傭車先が使われた運行の得意先請求合計 (`金額+割増+実費`、#57 確定式)。
    pub total_sales: i64,
    /// その傭車先への支払合計 (`傭車金額+傭車割増+傭車実費`)。
    pub total_payment: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

/// 特定の傭車先 (`傭車先C`+`傭車先H`) の運行 1 件分の両建て明細
/// (`/subcontractor-net-detail` 用、`/subcontractor-net` のドリルダウン)。
#[derive(Debug, Clone)]
pub struct RawUnchinSubcontractorNetDetailRow {
    pub item_code: String,
    pub item_name: String,
    /// その行の得意先N (同一運行の売上先)。
    pub customer_name: String,
    /// その行の得意先側金額 (`金額+割増+実費`)。
    pub sales: i64,
    /// その行の傭車先側金額 (`傭車金額+傭車割増+傭車実費`)。
    pub payment: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: NaiveDateTime,
    pub bumon_code: String,
    pub bumon_name: String,
}

/// 得意先ごとの 売上/傭車支払 両建て合計 (`/customer-net` 用)。
/// 「同一運行内の両建て」を得意先軸で見たもの (2026-07-01 user 確認:
/// 「傭車先じゃなくて得意先にグラフ直して」)。自社便のみの得意先は
/// `total_payment` が 0 になり `diff` = `total_sales` そのものになる。
#[derive(Debug, Clone)]
pub struct RawUnchinCustomerNetRow {
    pub partner_code: String,
    pub partner_name: String,
    /// その得意先への請求合計 (`金額+割増+実費`、#57 確定式)。
    pub total_sales: i64,
    /// その得意先の運行のうち傭車を使った分の支払合計 (`傭車金額+傭車割増+傭車実費`)。
    pub total_payment: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

/// 特定の得意先 (`得意先C`+`得意先H`) の運行 1 件分の両建て明細
/// (`/customer-net-detail` 用、`/customer-net` のドリルダウン)。
#[derive(Debug, Clone)]
pub struct RawUnchinCustomerNetDetailRow {
    pub item_code: String,
    pub item_name: String,
    /// その行の傭車先N (自社便なら空文字、傭車先C='000000' がマスタに存在しないため)。
    pub subcontractor_name: String,
    /// その行の得意先側金額 (`金額+割増+実費`)。
    pub sales: i64,
    /// その行の傭車先側金額 (`傭車金額+傭車割増+傭車実費`、自社便なら 0)。
    pub payment: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: NaiveDateTime,
    pub bumon_code: String,
    pub bumon_name: String,
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
    /// 自社側の受注部門コード (`得意先ﾏｽﾀ`/`傭車先ﾏｽﾀ`.`部門C`、#92 follow-up)。
    pub bumon_code: String,
    /// 自社側の受注部門名 (`部門ﾏｽﾀ`.`部門N`)。未マッチ時は空文字。
    pub bumon_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct UnchinSummaryRow {
    pub partner_code: String,
    pub partner_name: String,
    pub total: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct UnchinSubcontractorNetRow {
    pub partner_code: String,
    pub partner_name: String,
    pub total_sales: i64,
    pub total_payment: i64,
    /// 差額 = total_sales - total_payment。
    pub diff: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct UnchinSubcontractorNetDetailRow {
    pub item_code: String,
    pub item_name: String,
    pub customer_name: String,
    pub sales: i64,
    pub payment: i64,
    /// 差額 = sales - payment (行単位)。
    pub diff: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: String,
    pub bumon_code: String,
    pub bumon_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct UnchinCustomerNetRow {
    pub partner_code: String,
    pub partner_name: String,
    pub total_sales: i64,
    pub total_payment: i64,
    /// 差額 = total_sales - total_payment。
    pub diff: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct UnchinCustomerNetDetailRow {
    pub item_code: String,
    pub item_name: String,
    pub subcontractor_name: String,
    pub sales: i64,
    pub payment: i64,
    /// 差額 = sales - payment (行単位)。
    pub diff: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: String,
    pub bumon_code: String,
    pub bumon_name: String,
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
    /// `"with_billing_only"`(請求＋請求のみ, K IN (0,1)) |
    /// `"with_non_billing"`(請求＋非請求, K IN (0,2)、default)
    pub kind: Option<String>,
}

/// `/subcontractor-net` 用 query。`partner_type` は無い (常に傭車先起点)。
#[derive(Deserialize)]
pub struct UnchinSubcontractorNetQuery {
    /// 売上年月日 下限 (YYYY-MM-DD、含む)
    pub from: Option<String>,
    /// 売上年月日 上限 (YYYY-MM-DD、含まない)
    pub to: Option<String>,
    /// `"with_billing_only"`(請求＋請求のみ, K IN (0,1)) |
    /// `"with_non_billing"`(請求＋非請求, K IN (0,2)、default)
    pub kind: Option<String>,
}

/// `/subcontractor-net-detail` 用 query。`code`/`h` で対象傭車先 (`傭車先C`+`傭車先H`)
/// を一意に指定する (`/subcontractor-net` の `partner_code` = `"{code}-{h}"` を分割)。
#[derive(Deserialize)]
pub struct UnchinSubcontractorNetDetailQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub kind: Option<String>,
    /// 傭車先C
    pub code: String,
    /// 傭車先H
    pub h: String,
}

/// `/customer-net` 用 query。`partner_type` は無い (常に得意先起点)。
#[derive(Deserialize)]
pub struct UnchinCustomerNetQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub kind: Option<String>,
}

/// `/customer-net-detail` 用 query。`code`/`h` で対象得意先 (`得意先C`+`得意先H`)
/// を一意に指定する。
#[derive(Deserialize)]
pub struct UnchinCustomerNetDetailQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub kind: Option<String>,
    /// 得意先C
    pub code: String,
    /// 得意先H
    pub h: String,
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

/// `kind` パラメータ → `請求K` の SQL WHERE フラグメント。
/// `"with_billing_only"` (請求＋請求のみ, K IN ('0','1')) 以外は
/// `"with_non_billing"` (請求＋非請求, K IN ('0','2')、default) にフォールバックする。
/// 後者は本リポジトリの月計一致条件 (`請求K IN ('0','2')`) と同じ組み合わせ。
pub fn unchin_kind_filter(kind: &str) -> &'static str {
    match kind {
        "with_billing_only" => "AND t.[請求K] IN ('0', '1')",
        _ => "AND t.[請求K] IN ('0', '2')",
    }
}

/// `kind` パラメータ → source_table 表示用ラベル。
pub fn unchin_kind_label(kind: &str) -> &'static str {
    match kind {
        "with_billing_only" => "請求＋請求のみ (請求K IN (0,1))",
        _ => "請求＋非請求 (請求K IN (0,2))",
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
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}

/// Raw 合計行リストをレスポンス行に変換する。
pub fn build_unchin_summary_rows(raw: &[RawUnchinSummaryRow]) -> Vec<UnchinSummaryRow> {
    raw.iter()
        .map(|r| UnchinSummaryRow {
            partner_code: r.partner_code.clone(),
            partner_name: r.partner_name.clone(),
            total: r.total,
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}

/// Raw 傭車先ネット行リストをレスポンス行に変換する (差額計算含む)。
pub fn build_unchin_subcontractor_net_rows(
    raw: &[RawUnchinSubcontractorNetRow],
) -> Vec<UnchinSubcontractorNetRow> {
    raw.iter()
        .map(|r| UnchinSubcontractorNetRow {
            partner_code: r.partner_code.clone(),
            partner_name: r.partner_name.clone(),
            total_sales: r.total_sales,
            total_payment: r.total_payment,
            diff: r.total_sales - r.total_payment,
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}

/// Raw 傭車先ネット明細行リストをレスポンス行に変換する (行単位の差額計算含む)。
pub fn build_unchin_subcontractor_net_detail_rows(
    raw: &[RawUnchinSubcontractorNetDetailRow],
) -> Vec<UnchinSubcontractorNetDetailRow> {
    raw.iter()
        .map(|r| UnchinSubcontractorNetDetailRow {
            item_code: r.item_code.clone(),
            item_name: r.item_name.clone(),
            customer_name: r.customer_name.clone(),
            sales: r.sales,
            payment: r.payment,
            diff: r.sales - r.payment,
            origin: r.origin.clone(),
            dest: r.dest.clone(),
            sale_date: r.sale_date.format("%Y-%m-%d").to_string(),
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}

/// Raw 得意先ネット行リストをレスポンス行に変換する (差額計算含む)。
pub fn build_unchin_customer_net_rows(
    raw: &[RawUnchinCustomerNetRow],
) -> Vec<UnchinCustomerNetRow> {
    raw.iter()
        .map(|r| UnchinCustomerNetRow {
            partner_code: r.partner_code.clone(),
            partner_name: r.partner_name.clone(),
            total_sales: r.total_sales,
            total_payment: r.total_payment,
            diff: r.total_sales - r.total_payment,
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}

/// Raw 得意先ネット明細行リストをレスポンス行に変換する (行単位の差額計算含む)。
pub fn build_unchin_customer_net_detail_rows(
    raw: &[RawUnchinCustomerNetDetailRow],
) -> Vec<UnchinCustomerNetDetailRow> {
    raw.iter()
        .map(|r| UnchinCustomerNetDetailRow {
            item_code: r.item_code.clone(),
            item_name: r.item_name.clone(),
            subcontractor_name: r.subcontractor_name.clone(),
            sales: r.sales,
            payment: r.payment,
            diff: r.sales - r.payment,
            origin: r.origin.clone(),
            dest: r.dest.clone(),
            sale_date: r.sale_date.format("%Y-%m-%d").to_string(),
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════
// ハンドラ (薄い — param 解析 → repo → build → JSON)
// ══════════════════════════════════════════════════════════════

/// GET /api/unchin/candidates?from=&to=&partner_type=customer|subcontractor&kind=
pub async fn unchin_candidates(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinCandidateRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let partner_type = normalize_partner_type(params.partner_type.as_deref().unwrap_or(""));
    let kind = params.kind.unwrap_or_default();
    let kind_filter = unchin_kind_filter(&kind);

    let raw = repo
        .unchin_candidates(&from, &to, partner_type, kind_filter)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 + {} [{}]",
            if partner_type == "subcontractor" {
                "傭車先ﾏｽﾀ"
            } else {
                "得意先ﾏｽﾀ"
            },
            unchin_kind_label(&kind)
        ),
        data: build_unchin_rows(&raw),
    }))
}

/// GET /api/unchin/summary?from=&to=&partner_type=customer|subcontractor&kind=
///
/// 得意先 (or 傭車先) ごとの合計金額のみを SQL 側で `GROUP BY` + `SUM` して返す。
/// raw 行 TOP-N 方式と違い、結果は取引先数で決まるため一部の取引先だけが行数を
/// 食い潰して他が表示されなくなる問題が起きない（#57 実害の根本対策）。
pub async fn unchin_summary(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinSummaryRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let partner_type = normalize_partner_type(params.partner_type.as_deref().unwrap_or(""));
    let kind = params.kind.unwrap_or_default();
    let kind_filter = unchin_kind_filter(&kind);

    let raw = repo
        .unchin_summary(&from, &to, partner_type, kind_filter)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 + {} [{}]",
            if partner_type == "subcontractor" {
                "傭車先ﾏｽﾀ"
            } else {
                "得意先ﾏｽﾀ"
            },
            unchin_kind_label(&kind)
        ),
        data: build_unchin_summary_rows(&raw),
    }))
}

/// GET /api/unchin/subcontractor-net?from=&to=&kind=
///
/// 傭車先ごとに、その傭車先が使われた運行の得意先請求合計 (`total_sales`) と
/// その傭車先への支払合計 (`total_payment`) を同一行から突き合わせ、
/// 差額 (`diff = total_sales - total_payment`) を返す
/// (2026-07-01 user 確認「同一運行内の両建て」— 傭車先名と得意先名の名寄せではない)。
pub async fn unchin_subcontractor_net(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinSubcontractorNetQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinSubcontractorNetRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let kind = params.kind.unwrap_or_default();
    let kind_filter = unchin_kind_filter(&kind);

    let raw = repo
        .unchin_subcontractor_net(&from, &to, kind_filter)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 (傭車先ﾏｽﾀ + 得意先側金額の両建て) [{}]",
            unchin_kind_label(&kind)
        ),
        data: build_unchin_subcontractor_net_rows(&raw),
    }))
}

/// GET /api/unchin/subcontractor-net-detail?from=&to=&kind=&code=&h=
///
/// `/subcontractor-net` の特定の傭車先 (`code`=傭車先C, `h`=傭車先H) について、
/// 運行 (運転日報明細の行) 単位で得意先請求 (`sales`) と傭車支払 (`payment`)、
/// 行単位の差額を返す (「同一運行内の両建て」のドリルダウン)。
pub async fn unchin_subcontractor_net_detail(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinSubcontractorNetDetailQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinSubcontractorNetDetailRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let kind = params.kind.unwrap_or_default();
    let kind_filter = unchin_kind_filter(&kind);

    let raw = repo
        .unchin_subcontractor_net_detail(&from, &to, &params.code, &params.h, kind_filter)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 (傭車先C={}, 傭車先H={} の両建て明細) [{}]",
            params.code,
            params.h,
            unchin_kind_label(&kind)
        ),
        data: build_unchin_subcontractor_net_detail_rows(&raw),
    }))
}

/// GET /api/unchin/customer-net?from=&to=&kind=
///
/// 得意先ごとに、請求合計 (`total_sales`) とその運行で傭車を使った分の支払合計
/// (`total_payment`) を同一行から突き合わせ、差額 (`diff = total_sales -
/// total_payment`、粗利に相当) を返す
/// (2026-07-01 user 確認「傭車先じゃなくて得意先にグラフ直して」——
/// `/subcontractor-net` を得意先軸で見たもの)。
pub async fn unchin_customer_net(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinCustomerNetQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinCustomerNetRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let kind = params.kind.unwrap_or_default();
    let kind_filter = unchin_kind_filter(&kind);

    let raw = repo
        .unchin_customer_net(&from, &to, kind_filter)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 (得意先ﾏｽﾀ + 傭車先側金額の両建て) [{}]",
            unchin_kind_label(&kind)
        ),
        data: build_unchin_customer_net_rows(&raw),
    }))
}

/// GET /api/unchin/customer-net-detail?from=&to=&kind=&code=&h=
///
/// `/customer-net` の特定の得意先 (`code`=得意先C, `h`=得意先H) について、
/// 運行 (運転日報明細の行) 単位で請求 (`sales`) と傭車支払 (`payment`)、
/// 行単位の差額を返す。自社便の行は `subcontractor_name` が空文字になる。
pub async fn unchin_customer_net_detail(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<UnchinCustomerNetDetailQuery>,
) -> Result<Json<ApiResponse<Vec<UnchinCustomerNetDetailRow>>>, StatusCode> {
    let from = params.from.unwrap_or_else(|| "2024-01-01".to_string());
    let to = params.to.unwrap_or_else(|| "2999-12-31".to_string());
    let kind = params.kind.unwrap_or_default();
    let kind_filter = unchin_kind_filter(&kind);

    let raw = repo
        .unchin_customer_net_detail(&from, &to, &params.code, &params.h, kind_filter)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: format!(
            "運転日報明細 (得意先C={}, 得意先H={} の両建て明細) [{}]",
            params.code,
            params.h,
            unchin_kind_label(&kind)
        ),
        data: build_unchin_customer_net_detail_rows(&raw),
    }))
}
