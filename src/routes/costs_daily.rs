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

/// `経費明細` 1 行の生データ。区分の文字列 → bool の解釈はロジック層
/// (`build_costs_daily_rows`) が行い、DB 層は生値のまま運ぶ。
#[derive(Debug, Clone)]
pub struct RawCostsDailyRow {
    /// `運行年月日`。**`入力年月日` でも `計上年月日` でもない** — 運行単位の粗利に
    /// 足すので、走った日で並べる必要がある。
    pub operation_date: NaiveDateTime,
    /// `車輌C` (車番)。
    pub vehicle_number: String,
    /// `車輌H` (車番の枝番)。**`車輌C` だけでは車輌を一意に指せない**
    /// (#302 と同じ理由。帳票も `0040 01` のように 2 つ並べて印字している)。
    pub vehicle_branch: String,
    /// `運転手C` (乗務員CD)。
    pub driver_code: String,
    /// `経費C`。
    pub cost_code: String,
    /// `経費C` → `経費ﾏｽﾀ.経費N` (表示名)。`TOP 1` のスカラサブクエリで引く
    /// (LEFT JOIN だと明細が N 重に返る)。`経費ﾏｽﾀ` の主キーは `経費種別C` + `経費C`
    /// の複合だが、**引き当ては `経費C` 単独**。実機の 51 行で `経費C` は一意で、
    /// 複合にすると明細側の `経費種別C` がずれている行で名前だけ空になる。
    pub cost_name: String,
    /// `経費種別C` (`"01"`〜`"15"`)。燃料 `"01"` / 通行料 `"04"` 等。
    pub cost_kind: String,
    /// `経費種別C` → `経費種別ﾏｽﾀ.経費種別N` (表示名)。同じく `TOP 1`。
    pub cost_kind_name: String,
    /// `数量` (給油量 L 等)。
    pub quantity: f64,
    /// `単価` (端数を持ちうるため f64)。
    pub unit_price: f64,
    /// `税抜金額`。**`金額` は使わない** (module doc 参照)。
    pub amount: i64,
    /// `軽油引取税`。軽油は本体が非課税でこの税だけ別立てになるため、燃料費の実額を
    /// 出すには `amount` と足す必要がある。
    pub diesel_tax: i64,
    /// `KM` (給油時等の走行距離計)。
    pub km: f64,
    /// `固定経費K` の生値。`"1"` なら固定経費。
    pub fixed_cost_flag: String,
    /// 行 ID = `管理年月日`(yyyymmdd) + '-' + `管理C`。`vehicle_daily` と同じ安定キー
    /// (値カラムに依存しないため編集されても不変)。
    pub row_id: String,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug, PartialEq)]
pub struct CostsDailyRow {
    pub operation_date: String,
    pub vehicle_number: String,
    /// `車輌H` (車番の枝番)。`vehicle_number` と対で使う。
    pub vehicle_branch: String,
    pub driver_code: String,
    pub cost_code: String,
    pub cost_name: String,
    /// `経費種別C` (`"01"`〜`"15"`)。**そのまま返す** — 燃料/通行料/修繕をどう分けるかは
    /// 消費側の判断で、ここで絞ると呼び出し側から見えなくなる。
    pub cost_kind: String,
    pub cost_kind_name: String,
    pub quantity: f64,
    pub unit_price: f64,
    /// `税抜金額` (`金額` は使わない)。
    pub amount: i64,
    /// `軽油引取税` (`amount` には含まれない別立ての税)。
    pub diesel_tax: i64,
    pub km: f64,
    /// `固定経費K == "1"`。月極めの固定費を走行距離の比で按分するための材料。
    pub is_fixed: bool,
    pub row_id: String,
}

/// Raw 行リストをレスポンス行に変換 (日付整形・`固定経費K` の bool 化)。
pub fn build_costs_daily_rows(raw: &[RawCostsDailyRow]) -> Vec<CostsDailyRow> {
    raw.iter()
        .map(|r| CostsDailyRow {
            operation_date: r.operation_date.format("%Y-%m-%d").to_string(),
            vehicle_number: r.vehicle_number.clone(),
            vehicle_branch: r.vehicle_branch.clone(),
            driver_code: r.driver_code.clone(),
            cost_code: r.cost_code.clone(),
            cost_name: r.cost_name.clone(),
            cost_kind: r.cost_kind.clone(),
            cost_kind_name: r.cost_kind_name.clone(),
            quantity: r.quantity,
            unit_price: r.unit_price,
            amount: r.amount,
            diesel_tax: r.diesel_tax,
            km: r.km,
            // 空文字 (ISNULL の既定値) も `"0"` も変動費として扱う。
            is_fixed: r.fixed_cost_flag == "1",
            row_id: r.row_id.clone(),
        })
        .collect()
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct CostsDailyQuery {
    /// 運行年月日の下限 (YYYY-MM-DD、含む)。
    pub from: String,
    /// 運行年月日の上限 (YYYY-MM-DD、**含まない**。他 endpoint と同じ半開区間)。
    pub to: String,
    /// `車輌C` (車番、完全一致)。
    pub vehicle: Option<String>,
    /// `運転手C` (乗務員CD、完全一致)。
    pub driver: Option<String>,
    /// `経費種別C` (完全一致。燃料 `"01"` / 通行料 `"04"` 等)。
    pub kind: Option<String>,
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

/// GET /api/costs/vehicle-daily?from=&to=&vehicle=&driver=&kind=&limit=
///
/// `vehicle`/`driver`/`kind` は最低 1 つ必須。日付レンジのみでの全件スキャンは
/// SQL Server/Tunnel への負荷が大きいため 400 で拒否する (`vehicle_daily` と同じ)。
pub async fn costs_daily(
    Extension(repo): Extension<DynRepo>,
    Query(params): Query<CostsDailyQuery>,
) -> Result<Json<ApiResponse<Vec<CostsDailyRow>>>, StatusCode> {
    let vehicle = normalize_filter(&params.vehicle);
    let driver = normalize_filter(&params.driver);
    let kind = normalize_filter(&params.kind);

    if vehicle.is_none() && driver.is_none() && kind.is_none() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let limit = params.limit.unwrap_or(500).clamp(1, 5000);

    let raw = repo
        .costs_daily(&params.from, &params.to, vehicle, driver, kind, limit)
        .await
        .map_err(map_repo_err)?;

    Ok(Json(ApiResponse {
        source_table: "経費明細 + 経費ﾏｽﾀ + 経費種別ﾏｽﾀ".to_string(),
        data: build_costs_daily_rows(&raw),
    }))
}
