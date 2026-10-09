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
//!
//! SQL 文・Raw 行と応答行の型・絞り込みの判定・行の組み立ては Worker と共有するため
//! `ichiban_logic` (`workers/ichiban/logic`、Refs #322) にある。ここはハンドラだけ。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use ichiban_logic::api::VEHICLE_DAILY_SOURCE;
use ichiban_logic::vehicle_daily::{build_vehicle_daily_rows, VehicleDailyQuery, VehicleDailyRow};

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
// ハンドラ
// ══════════════════════════════════════════════════════════════

/// GET /api/sales/vehicle-daily?from=&to=&vehicle=&driver=&customer=&origin=&dest=&limit=
///
/// `vehicle`/`driver`/`customer`/`origin`/`dest` は最低 1 つ必須 (#79)。日付レンジのみでの
/// 全件スキャンは SQL Server/Tunnel への負荷が大きいため 400 で拒否する。
pub async fn vehicle_daily(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<VehicleDailyQuery>,
) -> Result<Json<ApiResponse<Vec<VehicleDailyRow>>>, StatusCode> {
    let f = params.filters().ok_or(StatusCode::BAD_REQUEST)?;

    let raw = repo
        .vehicle_daily(
            &params.from,
            &params.to,
            f.vehicle,
            f.driver,
            f.customer,
            f.origin,
            f.dest,
            f.limit,
        )
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: VEHICLE_DAILY_SOURCE.to_string(),
        data: build_vehicle_daily_rows(&raw),
    }))
}
