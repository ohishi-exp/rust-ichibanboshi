//! `/api/unchin/candidates`・`/api/unchin/summary`・`/api/unchin/customer-net`・`/api/unchin/customer-net-detail`
//! (得意先・傭車先別 運賃リストの基礎データ、Refs #57) の SQL・Raw 型・応答型・Query・組み立て。
//! オンプレ版 `src/routes/unchin.rs` と `src/repo.rs` の `unchin_*` から写した (Refs #322)。
//! 経緯 (`金額+割増+実費` を使う理由・C+H の複合キー・自社便の除外) はオンプレ版の doc とコメント。
//!
//! オンプレ版は `請求K` の絞り込みを生の SQL 片 (`&str`) で repo に渡していた。ここでは
//! [`UnchinKind::filter`] / [`PartnerType`] の `&'static str` だけを連結する (結果の SQL 文字列はオンプレ版と同じ)。
//! 値 (from / to / code / h) はすべて `@P1..` でバインドする。
//!
//! `/subcontractor-net` と `-detail` の 2 本は呼び手が無いので移していない。

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

/// `from` が無いときの下限 (オンプレ版と同じ)。
pub const DEFAULT_FROM: &str = "2024-01-01";
/// `to` が無いときの上限 (オンプレ版と同じ)。
pub const DEFAULT_TO: &str = "2999-12-31";

// ══════════════════════════════════════════════════════════════
// 切り替え (enum → &'static str)
// ══════════════════════════════════════════════════════════════

/// `partner_type`。`"subcontractor"` 以外は得意先 (オンプレ版 `normalize_partner_type` と同じ緩い方針)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartnerType {
    Customer,
    Subcontractor,
}

impl PartnerType {
    pub fn parse(s: &str) -> Self {
        match s {
            "subcontractor" => Self::Subcontractor,
            _ => Self::Customer,
        }
    }

    /// オンプレ版 `normalize_partner_type` の戻り値。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Customer => "customer",
            Self::Subcontractor => "subcontractor",
        }
    }

    /// `source_table` に出すマスタ名。
    pub fn master(self) -> &'static str {
        match self {
            Self::Customer => "得意先ﾏｽﾀ",
            Self::Subcontractor => "傭車先ﾏｽﾀ",
        }
    }
}

/// `kind` (`請求K` の組み合わせ)。`"with_billing_only"` 以外は `with_non_billing` (月計一致条件と同じ `(0,2)`)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnchinKind {
    WithBillingOnly,
    WithNonBilling,
}

impl UnchinKind {
    pub fn parse(s: &str) -> Self {
        match s {
            "with_billing_only" => Self::WithBillingOnly,
            _ => Self::WithNonBilling,
        }
    }

    /// `請求K` の WHERE 片 (alias `t.` 前提。オンプレ版 `unchin_kind_filter` と同じ)。
    pub fn filter(self) -> &'static str {
        match self {
            Self::WithBillingOnly => "AND t.[請求K] IN ('0', '1')",
            Self::WithNonBilling => "AND t.[請求K] IN ('0', '2')",
        }
    }

    /// `source_table` に出すラベル (オンプレ版 `unchin_kind_label` と同じ)。
    pub fn label(self) -> &'static str {
        match self {
            Self::WithBillingOnly => "請求＋請求のみ (請求K IN (0,1))",
            Self::WithNonBilling => "請求＋非請求 (請求K IN (0,2))",
        }
    }
}

// ══════════════════════════════════════════════════════════════
// Query パラメータ
// ══════════════════════════════════════════════════════════════

/// `/candidates`・`/summary` の query。全部省略可 (既定値で 200)。
#[derive(Deserialize, Debug, Default)]
pub struct UnchinQuery {
    /// 売上年月日 下限 (YYYY-MM-DD、含む)
    pub from: Option<String>,
    /// 売上年月日 上限 (YYYY-MM-DD、含まない)
    pub to: Option<String>,
    /// `"customer"` (default) | `"subcontractor"`
    pub partner_type: Option<String>,
    /// `"with_billing_only"` | `"with_non_billing"` (default)
    pub kind: Option<String>,
}

impl UnchinQuery {
    pub fn range(&self) -> (&str, &str) {
        range(&self.from, &self.to)
    }

    pub fn partner_type(&self) -> PartnerType {
        PartnerType::parse(self.partner_type.as_deref().unwrap_or(""))
    }

    pub fn kind(&self) -> UnchinKind {
        kind(&self.kind)
    }
}

/// `/customer-net` の query。`partner_type` は無い (常に得意先起点)。
#[derive(Deserialize, Debug, Default)]
pub struct UnchinCustomerNetQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub kind: Option<String>,
}

impl UnchinCustomerNetQuery {
    pub fn range(&self) -> (&str, &str) {
        range(&self.from, &self.to)
    }

    pub fn kind(&self) -> UnchinKind {
        kind(&self.kind)
    }
}

/// `/customer-net-detail` の query。`code` (得意先C) と `h` (得意先H) は必須 (欠落はオンプレ版と同じ 400)。
#[derive(Deserialize, Debug)]
pub struct UnchinCustomerNetDetailQuery {
    pub from: Option<String>,
    pub to: Option<String>,
    pub kind: Option<String>,
    pub code: String,
    pub h: String,
}

impl UnchinCustomerNetDetailQuery {
    pub fn range(&self) -> (&str, &str) {
        range(&self.from, &self.to)
    }

    pub fn kind(&self) -> UnchinKind {
        kind(&self.kind)
    }
}

/// 期間の既定値 (空文字は既定値にせずそのまま使う。オンプレ版の `unwrap_or_else` と同じ)。
fn range<'a>(from: &'a Option<String>, to: &'a Option<String>) -> (&'a str, &'a str) {
    let from = from.as_deref().unwrap_or(DEFAULT_FROM);
    (from, to.as_deref().unwrap_or(DEFAULT_TO))
}

fn kind(kind: &Option<String>) -> UnchinKind {
    UnchinKind::parse(kind.as_deref().unwrap_or(""))
}

// ══════════════════════════════════════════════════════════════
// source_table
// ══════════════════════════════════════════════════════════════

/// `/candidates`・`/summary` の `source_table`。
pub fn partner_source(pt: PartnerType, kind: UnchinKind) -> String {
    ["運転日報明細 + ", pt.master(), " [", kind.label(), "]"].concat()
}

/// `/customer-net` の `source_table`。
pub fn customer_net_source(kind: UnchinKind) -> String {
    [CUSTOMER_NET_SOURCE_HEAD, kind.label(), "]"].concat()
}

/// `/customer-net-detail` の `source_table` (code / h はクエリの値をそのまま入れる。オンプレ版と同じ)。
pub fn customer_net_detail_source(code: &str, h: &str, kind: UnchinKind) -> String {
    let parts = [
        "運転日報明細 (得意先C=",
        code,
        ", 得意先H=",
        h,
        " の両建て明細) [",
    ];
    [parts.concat().as_str(), kind.label(), "]"].concat()
}

const CUSTOMER_NET_SOURCE_HEAD: &str = "運転日報明細 (得意先ﾏｽﾀ + 傭車先側金額の両建て) [";

// ══════════════════════════════════════════════════════════════
// SQL (オンプレ版 src/repo.rs の unchin_* と同じ文字列)
// ══════════════════════════════════════════════════════════════

// 各 SQL は「`請求K` の片の前」と「後」の 2 つの定数で持ち、間に [`UnchinKind::filter`] を挟む。
// バインドは candidates / summary / customer-net が @P1 from, @P2 to。customer-net-detail は加えて @P3 code, @P4 h。

const CANDIDATES_CUSTOMER_HEAD: &str = "SELECT \
     CONCAT(t.[得意先C], '-', t.[得意先H]), \
     ISNULL(m.[得意先N], ''), \
     ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
     ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
     ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
     t.[売上年月日], \
     ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], ''), \
     CONCAT(ISNULL(t.[車輌C], ''), '-', ISNULL(t.[車輌H], '')) \
     FROM [運転日報明細] t \
     OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
       WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
     LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
       AND t.[品名C] NOT IN ('9003', '9998') ";
const CANDIDATES_CUSTOMER_TAIL: &str = "ORDER BY t.[得意先C], t.[得意先H], t.[品名C], t.[金額]";

const CANDIDATES_SUBCONTRACTOR_HEAD: &str = "SELECT \
     CONCAT(t.[傭車先C], '-', t.[傭車先H]), \
     ISNULL(m.[傭車先N], ''), \
     ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
     ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0), \
     ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
     t.[売上年月日], \
     ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], ''), \
     CONCAT(ISNULL(t.[車輌C], ''), '-', ISNULL(t.[車輌H], '')) \
     FROM [運転日報明細] t \
     OUTER APPLY (SELECT TOP 1 c.[傭車先N], c.[部門C] FROM [傭車先ﾏｽﾀ] c \
       WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) m \
     LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
       AND t.[品名C] NOT IN ('9003', '9998') \
       AND ISNULL(t.[傭車先C], '000000') != '000000' ";
const CANDIDATES_SUBCONTRACTOR_TAIL: &str =
    "ORDER BY t.[傭車先C], t.[傭車先H], t.[品名C], t.[金額]";

const SUMMARY_CUSTOMER_HEAD: &str = "SELECT t.[得意先C], t.[得意先H], \
     ISNULL(m.[得意先N], ''), \
     SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)), \
     ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
     FROM [運転日報明細] t \
     OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
       WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
     LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
       AND t.[品名C] NOT IN ('9003', '9998') ";
const SUMMARY_CUSTOMER_TAIL: &str =
    "GROUP BY t.[得意先C], t.[得意先H], m.[得意先N], m.[部門C], bm.[部門N] \
     ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) DESC";

const SUMMARY_SUBCONTRACTOR_HEAD: &str = "SELECT t.[傭車先C], t.[傭車先H], \
     ISNULL(m.[傭車先N], ''), \
     SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)), \
     ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
     FROM [運転日報明細] t \
     OUTER APPLY (SELECT TOP 1 c.[傭車先N], c.[部門C] FROM [傭車先ﾏｽﾀ] c \
       WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) m \
     LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
       AND t.[品名C] NOT IN ('9003', '9998') \
       AND ISNULL(t.[傭車先C], '000000') != '000000' ";
const SUMMARY_SUBCONTRACTOR_TAIL: &str = "GROUP BY t.[傭車先C], t.[傭車先H], m.[傭車先N], m.[部門C], bm.[部門N] \
     ORDER BY SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC";

const CUSTOMER_NET_HEAD: &str = "SELECT t.[得意先C], t.[得意先H], \
     ISNULL(m.[得意先N], ''), \
     SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)), \
     SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)), \
     ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
     FROM [運転日報明細] t \
     OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
       WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
     LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
       AND t.[品名C] NOT IN ('9003', '9998') \
       AND ISNULL(t.[傭車先C], '000000') != '000000' ";
const CUSTOMER_NET_TAIL: &str =
    "GROUP BY t.[得意先C], t.[得意先H], m.[得意先N], m.[部門C], bm.[部門N] \
     ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) \
       - SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC";

const CUSTOMER_NET_DETAIL_HEAD: &str = "SELECT \
     ISNULL(t.[品名C], ''), ISNULL(t.[品名N], ''), \
     ISNULL(sm.[傭車先N], ''), \
     ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0), \
     ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0), \
     ISNULL(t.[発地N], ''), ISNULL(t.[着地N], ''), \
     t.[売上年月日], \
     ISNULL(cm.[部門C], ''), ISNULL(bm.[部門N], '') \
     FROM [運転日報明細] t \
     OUTER APPLY (SELECT TOP 1 c.[傭車先N] FROM [傭車先ﾏｽﾀ] c \
       WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) sm \
     OUTER APPLY (SELECT TOP 1 c.[部門C] FROM [得意先ﾏｽﾀ] c \
       WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) cm \
     LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = cm.[部門C] \
     WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
       AND t.[品名C] NOT IN ('9003', '9998') \
       AND t.[得意先C] = @P3 AND t.[得意先H] = @P4 \
       AND ISNULL(t.[傭車先C], '000000') != '000000' ";
const CUSTOMER_NET_DETAIL_TAIL: &str = "ORDER BY t.[売上年月日] DESC";

/// `head` + `請求K` の片 + `tail`。
fn with_kind(head: &str, kind: UnchinKind, tail: &str) -> String {
    [head, kind.filter(), " ", tail].concat()
}

/// `/candidates` の SQL (@P1 from, @P2 to)。列は [`RawUnchinRow`] の並び。
pub fn candidates_sql(pt: PartnerType, kind: UnchinKind) -> String {
    match pt {
        PartnerType::Customer => {
            with_kind(CANDIDATES_CUSTOMER_HEAD, kind, CANDIDATES_CUSTOMER_TAIL)
        }
        PartnerType::Subcontractor => with_kind(
            CANDIDATES_SUBCONTRACTOR_HEAD,
            kind,
            CANDIDATES_SUBCONTRACTOR_TAIL,
        ),
    }
}

/// `/summary` の SQL (@P1 from, @P2 to)。列は 0 C, 1 H, 2 名称, 3 合計, 4 部門C, 5 部門N。
pub fn summary_sql(pt: PartnerType, kind: UnchinKind) -> String {
    match pt {
        PartnerType::Customer => with_kind(SUMMARY_CUSTOMER_HEAD, kind, SUMMARY_CUSTOMER_TAIL),
        PartnerType::Subcontractor => {
            with_kind(SUMMARY_SUBCONTRACTOR_HEAD, kind, SUMMARY_SUBCONTRACTOR_TAIL)
        }
    }
}

/// `/customer-net` の SQL (@P1 from, @P2 to)。列は 0 C, 1 H, 2 得意先N, 3 請求, 4 支払, 5 部門C, 6 部門N。
pub fn customer_net_sql(kind: UnchinKind) -> String {
    with_kind(CUSTOMER_NET_HEAD, kind, CUSTOMER_NET_TAIL)
}

/// `/customer-net-detail` の SQL (@P1 from, @P2 to, @P3 code, @P4 h)。列は [`RawUnchinCustomerNetDetailRow`] の並び。
pub fn customer_net_detail_sql(kind: UnchinKind) -> String {
    with_kind(CUSTOMER_NET_DETAIL_HEAD, kind, CUSTOMER_NET_DETAIL_TAIL)
}

// ══════════════════════════════════════════════════════════════
// Raw 中間構造体 (DB 層 → ロジック層 の橋渡し)
// ══════════════════════════════════════════════════════════════

/// `運転日報明細` 1 行の生データ (得意先 or 傭車先、いずれか一方の側面)。
#[derive(Debug, Clone)]
pub struct RawUnchinRow {
    /// 取引先コード (`C`+`-`+`H`。`H` は変動するため複合キーで一意化する)。
    pub partner_code: String,
    pub partner_name: String,
    pub item_code: String,
    pub item_name: String,
    /// 運賃額。customer は `金額+割増+実費`、subcontractor は `傭車金額+傭車割増+傭車実費` (#57 確定式)。
    pub fare: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: NaiveDateTime,
    /// 自社側の受注部門コード (`得意先ﾏｽﾀ`/`傭車先ﾏｽﾀ`.`部門C`)。
    pub bumon_code: String,
    pub bumon_name: String,
    /// 車輌C+`-`+車輌H。
    pub vehicle_code: String,
}

/// 取引先ごとの合計金額 (`/summary`)。`partner_code` は `C-H`。
#[derive(Debug, Clone)]
pub struct RawUnchinSummaryRow {
    pub partner_code: String,
    pub partner_name: String,
    pub total: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

/// 得意先ごとの 売上/傭車支払 両建て合計 (`/customer-net`)。自社便は SQL 側で除外済み。
#[derive(Debug, Clone)]
pub struct RawUnchinCustomerNetRow {
    pub partner_code: String,
    pub partner_name: String,
    pub total_sales: i64,
    pub total_payment: i64,
    pub bumon_code: String,
    pub bumon_name: String,
}

/// 特定の得意先の運行 1 件分の両建て明細 (`/customer-net-detail`)。
#[derive(Debug, Clone)]
pub struct RawUnchinCustomerNetDetailRow {
    pub item_code: String,
    pub item_name: String,
    pub subcontractor_name: String,
    pub sales: i64,
    pub payment: i64,
    pub origin: String,
    pub dest: String,
    pub sale_date: NaiveDateTime,
    pub bumon_code: String,
    pub bumon_name: String,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体 (フィールドの並びはオンプレ版と同じ)
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
    pub bumon_code: String,
    pub bumon_name: String,
    pub vehicle_code: String,
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
// 組み立て
// ══════════════════════════════════════════════════════════════

fn ymd(d: &NaiveDateTime) -> String {
    d.format("%Y-%m-%d").to_string()
}

/// Raw 行 → 応答行 (日付整形のみ)。
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
            sale_date: ymd(&r.sale_date),
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
            vehicle_code: r.vehicle_code.clone(),
        })
        .collect()
}

/// Raw 合計行 → 応答行。
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

/// Raw 得意先ネット行 → 応答行 (差額計算含む)。
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

/// Raw 得意先ネット明細行 → 応答行 (行単位の差額計算含む)。
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
            sale_date: ymd(&r.sale_date),
            bumon_code: r.bumon_code.clone(),
            bumon_name: r.bumon_name.clone(),
        })
        .collect()
}
