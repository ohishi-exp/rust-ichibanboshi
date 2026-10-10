//! `/api/surcharge/base` の SQL・Raw 型・応答型・Query・組み立て (Refs #322)。
//!
//! オンプレ版 `src/routes/surcharge.rs` の `surcharge_base` と `src/repo.rs` の `surcharge_base` を写した。
//! SQL 文・既定値・応答の JSON は同じ。運賃 (fare) は #12 の確定式 `金額 + 割増 + 実費` をそのまま使う
//! (税抜カラムには書き換えない。オンプレ版と同値であることがこの口の要件)。

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

use crate::period::calc_next_month;

/// `source_table` の前半。後ろに `[{kind のラベル}]` を付ける。
const SURCHARGE_SOURCE_PREFIX: &str = "運転日報明細 + 得意先ﾏｽﾀ + 車種ﾏｽﾀ + 地域ﾏｽﾀ";

/// SELECT 本体 (`SELECT TOP {n} ` の後ろ、`kind` の絞り込みの前まで)。@P1 from, @P2 to (半開区間)。
/// 列: 0 請求K, 1 得意先C, 2 得意先N, 3 発地域N, 4 着地域N, 5 車種C, 6 車種N, 7 売上年月日, 8 運賃,
/// 9 入金予定日, 10 傭車先C, 11 品名C, 12 品名N, 13 車輌C, 14 燃料サーチャージ, 15 行 ID, 16 入力担当C, 17 入力者N。
/// マスタはスカラサブクエリ (TOP 1) で引き、明細 1 行 = 出力 1 行を保つ (LEFT JOIN のファンアウト防止)。
const SURCHARGE_SQL_BODY: &str = "t.[請求K], t.[得意先C], \
     ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c WHERE c.[得意先C] = t.[得意先C]), ''), \
     ISNULL((SELECT TOP 1 o.[地域N] FROM [地域ﾏｽﾀ] o WHERE o.[地域C] = t.[発地域C]), ''), \
     ISNULL((SELECT TOP 1 d.[地域N] FROM [地域ﾏｽﾀ] d WHERE d.[地域C] = t.[着地域C]), ''), \
     t.[車種C], \
     ISNULL((SELECT TOP 1 v.[車種N] FROM [車種ﾏｽﾀ] v WHERE v.[車種C] = t.[車種C]), ''), \
     t.[売上年月日], \
     ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
     t.[入金予定日], \
     ISNULL(t.[傭車先C], ''), \
     ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
     ISNULL(t.[車輌C], ''), \
     ISNULL((SELECT SUM(\
       CASE WHEN za.[割増C1] = '19' THEN ISNULL(za.[割増金額1], 0) ELSE 0 END \
     + CASE WHEN za.[割増C2] = '19' THEN ISNULL(za.[割増金額2], 0) ELSE 0 END \
     + CASE WHEN za.[割増C3] = '19' THEN ISNULL(za.[割増金額3], 0) ELSE 0 END) \
       FROM [運転日報割増明細] za \
       WHERE za.[管理年月日] = t.[管理年月日] \
         AND za.[管理C] = t.[管理C] \
         AND za.[自車傭車K] = '0'), 0), \
     CONCAT(CONVERT(varchar(8), t.[管理年月日], 112), '-', t.[管理C]), \
     ISNULL(t.[入力担当C], ''), \
     ISNULL((SELECT TOP 1 s.[社員N] FROM [社員ﾏｽﾀ] s WHERE s.[社員C] = t.[入力担当C]), '') \
     FROM [運転日報明細] t \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 ";

/// 取得上限件数の既定値と範囲。
const LIMIT_DEFAULT: i32 = 2000;
const LIMIT_MIN: i32 = 1;
const LIMIT_MAX: i32 = 10000;

/// `GET /api/surcharge/base` の SQL。`kind_filter` は [`surcharge_kind_filter`] の戻り値 (`'static`) だけを
/// 受け付け、生の SQL 片は渡せない。`TOP n` は clamp した整数だけを連結する。
pub fn surcharge_sql(kind_filter: &'static str, limit: i32) -> String {
    let top = limit.clamp(LIMIT_MIN, LIMIT_MAX);
    format!(
        "SELECT TOP {top} {SURCHARGE_SQL_BODY}{kind_filter} \
         ORDER BY t.[入金予定日], t.[得意先C], t.[売上年月日]"
    )
}

/// `運転日報明細` 1 行 + マスタ join の生データ。
/// 県名は `地域ﾏｽﾀ.地域N` の生値 (正規化前) を保持し、[`build_surcharge_rows`] で県へ正規化する。
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
    /// `傭車先C`。'000000' (6 桁ゼロ) なら自車、それ以外は傭車。
    pub subcontractor_code: String,
    /// `品名C` (例: 9003=消費税調整 / 9998=端数調整)。
    pub item_code: String,
    pub item_name: String,
    /// `車輌C` (車番)。車種C とは別の具体的な車輌番号。
    pub vehicle_number: String,
    /// 燃料サーチャージ額 (円)。`割増C='19'` 枠のみ。`fare` には含めず分離して保持する。
    pub fuel_surcharge: i64,
    /// 行 ID = `管理年月日`(yyyymmdd) + '-' + `管理C`。
    pub row_id: String,
    pub input_staff_code: String,
    pub input_staff_name: String,
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
    pub subcontractor_code: String,
    pub item_code: String,
    pub item_name: String,
    pub vehicle_number: String,
    pub fuel_surcharge: i64,
    pub row_id: String,
    pub input_staff_code: String,
    pub input_staff_name: String,
}

#[derive(Deserialize, Debug, Default)]
pub struct SurchargeQuery {
    /// 売上年月の下限 (YYYY-MM、含む。既定 2025-04)
    pub from: Option<String>,
    /// 売上年月の上限 (YYYY-MM、含む。既定 2026-03)
    pub to: Option<String>,
    /// 請求区分の絞り込み: billing_only (既定) | transport | all
    pub kind: Option<String>,
    /// 取得上限件数 (1..=10000、既定 2000)
    pub limit: Option<i32>,
}

/// クエリから決まる値 (オンプレ版のハンドラ冒頭と同じ既定値)。
#[derive(Debug, Clone, PartialEq)]
pub struct SurchargeParams {
    /// "YYYY-MM-01" (@P1)
    pub from_date: String,
    /// `to` の翌月初日 (@P2。半開区間)
    pub to_date: String,
    pub kind_filter: &'static str,
    pub limit: i32,
    pub source_table: String,
}

impl SurchargeQuery {
    pub fn params(&self) -> SurchargeParams {
        let from = self.from.as_deref().unwrap_or("2025-04");
        let to = self.to.as_deref().unwrap_or("2026-03");
        // 売上年月日 < (to の翌月初日) で上限を半開区間にする。読めない値は 2026 年 3 月に落とす
        let mut parts = to.split('-');
        let ty: i32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(2026);
        let tm: i32 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(3);
        let (ny, nm) = calc_next_month(ty, tm);
        let kind = self.kind.as_deref().unwrap_or("billing_only");
        SurchargeParams {
            from_date: format!("{from}-01"),
            to_date: format!("{ny}-{nm:02}-01"),
            kind_filter: surcharge_kind_filter(kind),
            limit: self
                .limit
                .unwrap_or(LIMIT_DEFAULT)
                .clamp(LIMIT_MIN, LIMIT_MAX),
            source_table: format!("{SURCHARGE_SOURCE_PREFIX} [{}]", surcharge_kind_label(kind)),
        }
    }
}

/// `地域ﾏｽﾀ.地域N` の先頭を都道府県に正規化する。
///
/// `北海道` のみ 4 文字、他は最初の `県`/`府`/`都` まで。未マップ (空文字) は `"?"`。
/// `京都府` のように `都` を内包する `府` を誤らないよう `県`→`府`→`都` の順で判定する。
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

/// `kind` パラメータ → `請求K` の SQL WHERE 断片。未知の値は billing_only と同義。
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

/// Raw 行リストを応答行に変換 (県正規化・日付整形)。
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
            subcontractor_code: r.subcontractor_code.clone(),
            item_code: r.item_code.clone(),
            item_name: r.item_name.clone(),
            vehicle_number: r.vehicle_number.clone(),
            fuel_surcharge: r.fuel_surcharge,
            row_id: r.row_id.clone(),
            input_staff_code: r.input_staff_code.clone(),
            input_staff_name: r.input_staff_name.clone(),
        })
        .collect()
}
