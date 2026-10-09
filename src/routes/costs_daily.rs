//! 車番×期間の経費明細 API (Refs ohishi-exp/nuxt-dtako-admin#760)。
//!
//! nuxt-dtako-admin の運行手当タブが **粗利 = 売上 − 手当 − 経費** を出すために使う。
//! 売上は `vehicle_daily` (`/api/sales/vehicle-daily`)、手当は nuxt 側が持っているが、
//! 経費を読む口がこれまで無かった。
//!
//! 作りは `vehicle_daily.rs` と同型 (#302/#303/#304 で 3 回触って固まった型):
//! マスタはスカラサブクエリ (`TOP 1`)、絞り込みは `(@Pn IS NULL OR ...)` の固定
//! パラメータ数、`vehicle`/`driver`/`kind` は最低 1 つ必須 (全件スキャン防止)。
//!
//! ## 金額は `税抜金額` を使う (`金額` は使わない)
//!
//! `vehicle_daily` の売上が税抜で揃っている以上、引く側の経費も税抜でなければ
//! 粗利がずれる。`金額` は実費の税処理 (内税/外税/非課税) で消費税の含み方が
//! 行ごとに違う (CLAUDE.md の月計一致ルールと同じ理由)。
//!
//! ## `is_fixed` (`固定経費K`) を返す理由
//!
//! 保険料・リース料のような月極めの固定費は 1 行にまとまって載る。運行単位の粗利へ
//! 素直に足すと、その行が当たった 1 運行だけが赤くなる。オーナー決定で **運行に直接
//! 紐づかない経費は走行距離の比で按分**することになっており、消費側はその**固定費と
//! 変動費を分ける材料**としてこの区分を使う。よって**絞らずそのまま返す**
//! (`vehicle_daily` の `request_kind` と同じ方針)。
//!
//! ## `remarks` / `vendor_*` / `entered_date` を返す理由 (#760-11)
//!
//! 粗利タブで直課経費が突出する乗務員が居ても、経費C/種別/金額だけでは
//! **何の修理か・どこに払ったか**が読めない。`経費明細` の `備考` (varchar 64) と
//! `未払先C`/`未払先H` (名前は `未払先ﾏｽﾀ.未払先N`)、`入力年月日` をそのまま足す。
//! 既存フィールドの意味・順序は変えない (JSON は末尾への追加のみ)。
//!
//! SQL 文・Raw 行と応答行の型・絞り込みの判定・行の組み立ては Worker と共有するため
//! `ichiban_logic` (`workers/ichiban/logic`、Refs #322) にある。ここはハンドラだけ。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use ichiban_logic::api::COSTS_DAILY_SOURCE;
use ichiban_logic::costs_daily::{build_costs_daily_rows, CostsDailyQuery, CostsDailyRow};

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

/// GET /api/costs/vehicle-daily?from=&to=&vehicle=&driver=&kind=&limit=
///
/// `vehicle`/`driver`/`kind` は最低 1 つ必須。日付レンジのみでの全件スキャンは
/// SQL Server/Tunnel への負荷が大きいため 400 で拒否する (`vehicle_daily` と同じ)。
pub async fn costs_daily(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<CostsDailyQuery>,
) -> Result<Json<ApiResponse<Vec<CostsDailyRow>>>, StatusCode> {
    let f = params.filters().ok_or(StatusCode::BAD_REQUEST)?;

    let raw = repo
        .costs_daily(
            &params.from,
            &params.to,
            f.vehicle,
            f.driver,
            f.kind,
            f.limit,
        )
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: COSTS_DAILY_SOURCE.to_string(),
        data: build_costs_daily_rows(&raw),
    }))
}
