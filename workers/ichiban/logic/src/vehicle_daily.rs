//! `/api/sales/vehicle-daily` (車番×期間の伝票明細、Refs ohishi-exp/nuxt-dtako-admin#330) の純粋部分。
//! 経緯と積地・卸地の 2 系統・金額の月計一致ルールの説明はオンプレ版の `src/routes/vehicle_daily.rs` の module doc。
//!
//! `vehicle`/`driver`/`customer`/`origin`/`dest` は最低 1 つ必須 (#79、全件スキャン防止)。
//! 判定は [`VehicleDailyQuery::filters`]。

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::{clamp_limit, normalize_filter};

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
    /// `車輌H` (車番の枝番)。**`車輌C` だけでは車輌を一意に指せない** — 帳票は
    /// `0040 01` のように 2 つ並べて印字しており、同じ `車輌C` に別の枝番が存在する
    /// (ohishi-exp/nuxt-dtako-admin#741 の突合で判明)。
    pub vehicle_branch: String,
    /// `運転手C` (乗務員CD)。**車番ではなく乗務員で明細を引くための鍵。**
    pub driver_code: String,
    /// `運転手C` → `社員ﾏｽﾀ.社員N` (表示名)。
    ///
    /// **`運転日報明細` の `乗務員N` 列は使わない。** 自由入力で、実データではほぼ空
    /// (2026-07 の帯広5台を実機で引くと全行が空文字だった)。`得意先C`→`得意先N`、
    /// `地域C`→`地域N` と同じく**マスタを引く**のが正しい。
    /// `社員ﾏｽﾀ` は同一 `社員C` の複数行があり得るので `TOP 1`
    /// (`uriage`/`surcharge` のスカラサブクエリと同じ扱い)。
    ///
    /// **突合には使わない — `driver_code` を使う。** 表示専用。
    pub driver_name: String,
    /// `請求K` (請求区分)。`"0"` 通常運送 / `"1"` 請求のみ / `"2"` 非請求。
    pub request_kind: String,
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
    /// `車輌H` (車番の枝番)。`vehicle_number` と対で使う。
    pub vehicle_branch: String,
    /// `運転手C` (乗務員CD)。
    pub driver_code: String,
    /// `運転手C` → `社員ﾏｽﾀ.社員N` (表示名。突合には使わない — `driver_code` を使う)。
    pub driver_name: String,
    /// `請求K` (請求区分)。**そのまま返す** — 意味づけ (どれを収支に入れるか) は
    /// 消費側の判断で、ここで絞ると呼び出し側から見えなくなる。
    ///
    /// | 値 | 意味 | 2026-07 実測 (全社 4,510 件) |
    /// |---|---|---|
    /// | `"0"` | 通常運送 (請求あり) | 3,546 件 |
    /// | `"1"` | **請求のみ** (運送を伴わない請求行) | 124 件 |
    /// | `"2"` | **非請求** (車輌収支用の按分行) | 840 件 |
    ///
    /// **`"1"` と `"2"` は同じ荷の表裏になりうる。** 実例 (2026-07、中継):
    ///
    /// ```text
    /// 請求K=1  07-16 車1318  釧路 → ユナイテッド牧場  12.5t  ¥43,750  ← 通しの請求
    /// 請求K=2  07-16 車1318  釧路 → 駒場             12.5t  ¥21,750  ┐ 実際に走った
    /// 請求K=2  07-17 車0040  駒場 → ユナイテッド牧場  12.5t  ¥22,000  ┘ 和が通しと一致
    /// ```
    ///
    /// **車輌収支に両方足すと二重計上になる。** `請求K=1` は走っていない行なので、
    /// 車輌ごとの収支や運行との突合からは外すのが正しい (得意先ﾏｽﾀにも
    /// `車輌収支用(非請求)` という得意先が実在する)。
    pub request_kind: String,
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
                vehicle_branch: r.vehicle_branch.clone(),
                driver_code: r.driver_code.clone(),
                driver_name: r.driver_name.clone(),
                request_kind: r.request_kind.clone(),
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
    /// `運転手C` (乗務員CD、完全一致)。
    ///
    /// **車番では引けない日があるため足した** (ohishi-exp/nuxt-dtako-admin#741)。
    /// 同じ乗務員の売上が日によって別の車番 (デジタコを積んでいない車輌等) に
    /// 載ることがあり、車番で引くとその日の明細がまるごと見えない。
    pub driver: Option<String>,
    /// `得意先C` (完全一致)。
    pub customer: Option<String>,
    /// 積地 (`origin_area_name`/`origin` のいずれかに部分一致)。
    pub origin: Option<String>,
    /// 卸地 (`dest_area_name`/`dest` のいずれかに部分一致)。
    pub dest: Option<String>,
    /// 取得上限件数 (1..=5000、default 500)。
    pub limit: Option<i32>,
}

/// 絞り込みを正規化した結果 (空白だけの値は絞り込みなし)。
#[derive(Debug, PartialEq, Eq)]
pub struct VehicleDailyFilters<'a> {
    pub vehicle: Option<&'a str>,
    pub driver: Option<&'a str>,
    pub customer: Option<&'a str>,
    pub origin: Option<&'a str>,
    pub dest: Option<&'a str>,
    /// 1..=5000 に丸めた取得上限件数 (既定 500)。
    pub limit: i32,
}

impl VehicleDailyQuery {
    /// 絞り込みを正規化する。`vehicle`/`driver`/`customer`/`origin`/`dest` が 1 つも無ければ
    /// `None` (= 400。日付レンジのみでの全件スキャンは SQL Server/Tunnel への負荷が大きい)。
    pub fn filters(&self) -> Option<VehicleDailyFilters<'_>> {
        let f = VehicleDailyFilters {
            vehicle: normalize_filter(&self.vehicle),
            driver: normalize_filter(&self.driver),
            customer: normalize_filter(&self.customer),
            origin: normalize_filter(&self.origin),
            dest: normalize_filter(&self.dest),
            limit: clamp_limit(self.limit),
        };
        if f.vehicle.is_none()
            && f.driver.is_none()
            && f.customer.is_none()
            && f.origin.is_none()
            && f.dest.is_none()
        {
            return None;
        }
        Some(f)
    }
}
