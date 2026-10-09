//! `/api/costs/vehicle-daily` (車番×期間の経費明細、Refs ohishi-exp/nuxt-dtako-admin#760) の純粋部分。
//! 経緯と `税抜金額` を使う理由・`is_fixed` を返す理由はオンプレ版の `src/routes/costs_daily.rs` の module doc。
//!
//! `vehicle`/`driver`/`kind` は最低 1 つ必須 (全件スキャン防止)。判定は [`CostsDailyQuery::filters`]。

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{clamp_limit, normalize_filter};

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
    /// (LEFT JOIN だと明細が N 重に返る)。引き当ては **`経費種別C` + `経費C` の複合**
    /// (これが `経費ﾏｽﾀ` の主キー)。`経費C` は実測でマスタ 51 行に対し一意なので単独でも
    /// 当たるが、それだと将来 `経費C` が別種別で再利用されたとき `amount` は正しいまま
    /// **`cost_name` だけ静かにすり替わる**。
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
    /// `備考` (varchar 64)。何の修理か等の自由記述。NULL は DB 層の `ISNULL` で空文字。
    pub remarks: String,
    /// `未払先C` (支払先コード)。
    pub vendor_code: String,
    /// `未払先H` (支払先コードの枝番)。`車輌C`/`車輌H` と同じく **単独では一意に指せない**
    /// 前提で、`未払先ﾏｽﾀ` は `未払先C` + `未払先H` の複合で引く。
    pub vendor_branch: String,
    /// `未払先C` + `未払先H` → `未払先ﾏｽﾀ.未払先N` (表示名)。`経費N` と同じく
    /// `TOP 1` のスカラサブクエリで引く (LEFT JOIN だと明細が N 重に返る)。
    /// 引けなければ空文字。
    pub vendor_name: String,
    /// `入力年月日`。NULL は `None` (ロジック層で空文字にする)。
    pub entered_date: Option<NaiveDateTime>,
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
    /// `備考`。NULL は空文字。
    pub remarks: String,
    /// `未払先C`。
    pub vendor_code: String,
    /// `未払先H`。`vendor_code` と対で使う。
    pub vendor_branch: String,
    /// `未払先ﾏｽﾀ.未払先N`。引けなければ空文字。
    pub vendor_name: String,
    /// `入力年月日` を `YYYY-MM-DD` に整形。NULL は空文字。
    pub entered_date: String,
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
            remarks: r.remarks.clone(),
            vendor_code: r.vendor_code.clone(),
            vendor_branch: r.vendor_branch.clone(),
            vendor_name: r.vendor_name.clone(),
            // NULL (None) は空文字。消費側が「未入力」と「日付」を同じ型で受けられる。
            entered_date: r
                .entered_date
                .map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default(),
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

/// 絞り込みを正規化した結果 (空白だけの値は絞り込みなし)。
#[derive(Debug, PartialEq, Eq)]
pub struct CostsDailyFilters<'a> {
    pub vehicle: Option<&'a str>,
    pub driver: Option<&'a str>,
    pub kind: Option<&'a str>,
    /// 1..=5000 に丸めた取得上限件数 (既定 500)。
    pub limit: i32,
}

impl CostsDailyQuery {
    /// 絞り込みを正規化する。`vehicle`/`driver`/`kind` が 1 つも無ければ `None` (= 400。
    /// 日付レンジのみでの全件スキャンは SQL Server/Tunnel への負荷が大きい)。
    pub fn filters(&self) -> Option<CostsDailyFilters<'_>> {
        let f = CostsDailyFilters {
            vehicle: normalize_filter(&self.vehicle),
            driver: normalize_filter(&self.driver),
            kind: normalize_filter(&self.kind),
            limit: clamp_limit(self.limit),
        };
        if f.vehicle.is_none() && f.driver.is_none() && f.kind.is_none() {
            return None;
        }
        Some(f)
    }
}
