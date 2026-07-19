//! 車番×期間の伝票明細 API (Refs ohishi-exp/nuxt-dtako-admin#330)。
//!
//! nuxt-dtako-admin の運行収支分析 (一番星売上 × デジタコ実績) が、運行詳細で
//! 選択した区間の車番+運行日から伝票候補を検索するために使う。
//!
//! 積地・卸地は 2 系統のデータを両方返す (#12 実機調査):
//! - `origin_area_name`/`dest_area_name`: `発地域C`/`着地域C` → `地域ﾏｽﾀ.地域N`。
//!   マスタ由来で **市区町村レベルまで届く** (例 `001401`=神奈川県横浜市)。
//!   `surcharge_base` は請求書地図のため県レベルまで丸める (`normalize_prefecture`)
//!   が、突合精度を優先するここでは**丸めず生値を返す**。dtako 側の市町村名との
//!   一次的な突合キーはこちらを想定
//! - `origin`/`dest`: `発地N`/`着地N` (自由入力の生文字列)。`docs/plan-unchin-rate-list.md`
//!   (#57 実機調査) で粒度不揃い (市町村名/県+市/施設名混在)・空文字率 3 割弱と
//!   判明済みだが、施設名等マスタに無い detail を持つ場合があるため補助信号として残す
//!   (`unchin.rs` と同型の判断)。突合方式 (NFKC正規化・部分一致等) は消費側
//!   (nuxt-dtako-admin) の責務とする。
//!
//! 金額は月計一致ルール (CLAUDE.md) に従い `税抜金額+税抜割増+税抜実費-値引`
//! (自車) / `税抜傭車金額+税抜傭車割増+税抜傭車実費-傭車値引` (傭車) を使う。
//! `金額` 列は使わない。傭車判定は `傭車先C='000000'` (自車) / それ以外 (傭車)。
//!
//! 品名 (`品名C`/`品名N`) と数量・単価・単位も返す (nuxt-dtako-admin#330 実データ検証で、
//! 同一日でも複数明細で単価が異なることがあり突合精度の判断材料に必要と判明)。
//! いずれも `INFORMATION_SCHEMA.COLUMNS` (`/api/schema/columns?table=運転日報明細`) で
//! 実在確認済み: `数量`/`単価` は `decimal` (NOT NULL)、`単位` は `varchar` (nullable)。
//!
//! `vehicle` は任意化されている (#79)。nuxt-dtako-admin#330 PR5「類似運行検索」が
//! 積地・卸地ペア/得意先だけで車輌を横断して検索する必要があるため、`customer`
//! (得意先C、完全一致) / `origin`/`dest` (地域名、部分一致) の絞り込みを追加した。
//! `vehicle`/`customer`/`origin`/`dest` は **最低 1 つ必須** (全件スキャン防止、
//! SQL Server/Tunnel の負荷対策)。

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

/// `運転日報明細` 1 行の生データ。自車/傭車の金額は両方保持し、`傭車先C` に応じて
/// ロジック層 (`build_vehicle_daily_rows`) がどちらを使うか決める
/// (`uriage.rs::is_yosha` と同じ判定式、行ごとに片方だけが非ゼロになる想定)。
#[derive(Debug, Clone)]
pub struct RawVehicleDailyRow {
    pub sale_date: NaiveDateTime,
    /// `車輌C` (車番。dtako 側の raw_data.車輌CD と突合するキー)。
    pub vehicle_number: String,
    /// `得意先C`。複合キー (`得意先C`+`得意先H`) ではなく単独 (`surcharge_base` と
    /// 同じ簡略化、表示名の解決用途で金額計算には影響しない)。
    pub customer_code: String,
    pub customer_name: String,
    /// `発地域C` → `地域ﾏｽﾀ.地域N` (未丸め、市区町村まで届きうる)。
    pub origin_area_name: String,
    /// `着地域C` → `地域ﾏｽﾀ.地域N` (未丸め)。
    pub dest_area_name: String,
    /// `発地N` (積地、自由入力の生文字列)。
    pub origin: String,
    /// `着地N` (卸地、自由入力の生文字列)。
    pub dest: String,
    /// `傭車先C`。`"000000"` (6 桁ゼロ) なら自車、それ以外は傭車。
    pub subcontractor_code: String,
    /// 自車側 `税抜金額+税抜割増+税抜実費-値引`。
    pub self_amount: i64,
    /// 傭車側 `税抜傭車金額+税抜傭車割増+税抜傭車実費-傭車値引`。
    pub subcontract_amount: i64,
    /// `品名C`。
    pub item_code: String,
    /// `品名N`。同一日でも複数明細で品名・単価が異なりうる (nuxt-dtako-admin#330 実データ検証)。
    pub item_name: String,
    /// `数量` (decimal)。
    pub quantity: f64,
    /// `単価` (decimal、端数を持ちうるため f64 で保持)。
    pub unit_price: f64,
    /// `単位` (例: `個`/`t`)。
    pub unit: String,
    /// 行 ID = `管理年月日`(yyyymmdd) + '-' + `管理C`。`surcharge.rs`/`uriage.rs` と
    /// 同じ安定キー (値カラムに依存しないため編集されても不変)。
    pub row_id: String,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct VehicleDailyRow {
    pub sale_date: String,
    pub vehicle_number: String,
    pub customer_code: String,
    pub customer_name: String,
    pub origin_area_name: String,
    pub dest_area_name: String,
    pub origin: String,
    pub dest: String,
    /// `傭車先C != "000000"`。
    pub is_subcontracted: bool,
    /// 月計一致ルール適用済みの金額 (`self_amount` / `subcontract_amount` を
    /// `is_subcontracted` で選択)。
    pub amount: i64,
    pub item_code: String,
    pub item_name: String,
    pub quantity: f64,
    pub unit_price: f64,
    pub unit: String,
    pub row_id: String,
}

/// Raw 行リストをレスポンス行に変換 (自車/傭車どちらの金額を使うか判定・日付整形)。
pub fn build_vehicle_daily_rows(raw: &[RawVehicleDailyRow]) -> Vec<VehicleDailyRow> {
    raw.iter()
        .map(|r| {
            let is_subcontracted = r.subcontractor_code != "000000";
            VehicleDailyRow {
                sale_date: r.sale_date.format("%Y-%m-%d").to_string(),
                vehicle_number: r.vehicle_number.clone(),
                customer_code: r.customer_code.clone(),
                customer_name: r.customer_name.clone(),
                origin_area_name: r.origin_area_name.clone(),
                dest_area_name: r.dest_area_name.clone(),
                origin: r.origin.clone(),
                dest: r.dest.clone(),
                is_subcontracted,
                amount: if is_subcontracted {
                    r.subcontract_amount
                } else {
                    r.self_amount
                },
                item_code: r.item_code.clone(),
                item_name: r.item_name.clone(),
                quantity: r.quantity,
                unit_price: r.unit_price,
                unit: r.unit.clone(),
                row_id: r.row_id.clone(),
            }
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct VehicleDailyQuery {
    /// 売上年月日の下限 (YYYY-MM-DD、含む)。
    pub from: String,
    /// 売上年月日の上限 (YYYY-MM-DD、**含まない**。他 endpoint と同じ半開区間)。
    pub to: String,
    /// `車輌C` (車番、完全一致)。
    pub vehicle: Option<String>,
    /// `得意先C` (完全一致)。
    pub customer: Option<String>,
    /// 積地 (`origin_area_name`/`origin` のいずれかに部分一致)。
    pub origin: Option<String>,
    /// 卸地 (`dest_area_name`/`dest` のいずれかに部分一致)。
    pub dest: Option<String>,
    /// 取得上限件数 (1..=5000、default 500)。
    pub limit: Option<i32>,
}

/// クエリ値の前後空白を trim し、空文字なら絞り込みなし (`None`) 扱いにする。
fn normalize_filter(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|v| !v.is_empty())
}

// ══════════════════════════════════════════════════════════════
// ハンドラ
// ══════════════════════════════════════════════════════════════

/// GET /api/sales/vehicle-daily?from=&to=&vehicle=&customer=&origin=&dest=&limit=
///
/// `vehicle`/`customer`/`origin`/`dest` は最低 1 つ必須 (#79)。日付レンジのみでの
/// 全件スキャンは SQL Server/Tunnel への負荷が大きいため 400 で拒否する。
pub async fn vehicle_daily(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<VehicleDailyQuery>,
) -> Result<Json<ApiResponse<Vec<VehicleDailyRow>>>, StatusCode> {
    let vehicle = normalize_filter(&params.vehicle);
    let customer = normalize_filter(&params.customer);
    let origin = normalize_filter(&params.origin);
    let dest = normalize_filter(&params.dest);

    if vehicle.is_none() && customer.is_none() && origin.is_none() && dest.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit = params.limit.unwrap_or(500).clamp(1, 5000);

    let raw = repo
        .vehicle_daily(
            &params.from,
            &params.to,
            vehicle,
            customer,
            origin,
            dest,
            limit,
        )
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: "運転日報明細 + 得意先ﾏｽﾀ + 地域ﾏｽﾀ".to_string(),
        data: build_vehicle_daily_rows(&raw),
    }))
}
