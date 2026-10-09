//! 一番星 (CAPE#01) の読み出し 6 本 (`/health`・`/api/employees`・`/api/vehicles`・
//! `/api/sales/departments`・`/api/sales/vehicle-daily`・`/api/costs/vehicle-daily`) の純粋部分
//! (Refs ohishi-exp/rust-ichibanboshi#322)。SQL Server にも Worker にも依存しない。
//!
//! オンプレ版 (repo ルートの package) と Worker (`workers/ichiban/worker`) が同じ定義を使う —
//! 並走期間に応答を比べるので、SQL 文・応答の型・絞り込みの判定はここ 1 か所に置く。
//! `tiberius::Row` から `Raw*Row` を詰める関数は両側に別々に持つ (列の並びは [`sql`] の各定数と 1 対 1)。
//!
//! - [`sql`] — 6 本の SQL 文
//! - [`api`] — 応答の型 (社員・車種・部門と、一覧の包み) と `source_table` の値
//! - [`vehicle_daily`] — `/api/sales/vehicle-daily` の Query・Raw 行・応答行・組み立て
//! - [`costs_daily`] — `/api/costs/vehicle-daily` の Query・Raw 行・応答行・組み立て

pub mod api;
pub mod costs_daily;
pub mod sql;
pub mod vehicle_daily;

/// クエリ値の前後空白を trim し、空文字なら絞り込みなし (`None`) 扱いにする。
pub fn normalize_filter(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|v| !v.is_empty())
}

/// 取得上限件数 (1..=5000、既定 500)。vehicle-daily と costs-daily で同じ。
pub fn clamp_limit(limit: Option<i32>) -> i32 {
    limit.unwrap_or(500).clamp(1, 5000)
}
