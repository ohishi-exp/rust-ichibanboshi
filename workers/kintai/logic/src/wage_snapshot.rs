//! 賃金確定値の月次スナップショットの純ロジック (Refs #291、
//! ohishi-exp/nuxt-dtako-admin#677)。
//!
//! **root の `src/wage_snapshot.rs` の写し** (Refs #322。撤去までは片方を直したらもう片方も直す —
//! `workers/kintai/README.md` の対応表)。Worker が使うのは読み出し (`GET /api/kintai/wage-range`) の
//! [`resolve_months`]・[`aggregate_range`]・[`normalize_ts`] 等だけだが、保存側の検証
//! ([`validate_snapshot`]) も写しの差分を作らないため丸ごと持つ (テストも 39 本そのまま)。
//!
//! HTTP と SQL は [`crate::wage_range`] (元は root の `src/routes/wage_snapshot.rs`)。ここには **DB も時計も要らない**
//! 判断だけを置く — 受け取った payload の検証・期間の解決・期間合計の組み立て・
//! 鮮度 (stale) の判定。この分割は `kosoku.rs` (ロジック) と `routes/kintai.rs`
//! (HTTP) と同じ形で、100% カバレッジ gate に載せられるのはこちら側。
//!
//! ## ファイル名を `kintai` / `kosoku` で始めない
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` はディレクトリ + ファイル名前方一致で
//! 勤怠の `logic_version` の指紋を作る。ここに入ると 1 バイトの変更で全乗務員・
//! 全月が stale になり、収束に全月ぶんの `run_kintai_recalc` が要る。賃金の計算は
//! **relay 側 (TypeScript)** にあって勤怠の畳み直しとは無関係なので、指紋を汚さない
//! 名前にしている (`stale_months.rs` と同じ判断)。
//!
//! ## 合算の規則 — 足してはいけないものを足さない
//!
//! 「応答に無い = 0」と同じ落とし穴が金額にもある。0 円として足すと期間合計が
//! 過小になり、**支払いが理論値を大きく下回っているように見える**。
//!
//! - 欠測 (`restraint_missing`) の月は足さない・集計月数に数えない
//! - 単価未設定 (`calc_total` が NULL) の月も同じ
//! - **給与が揃っていない月は月ごと集計から外す** (「-」で出すのではなく、そもそも
//!   期間集計に載せない — ユーザー決定 2026-08-05)。給与DB を取り込んで保存し直せば
//!   その月が入る
//! - その乗務員だけ給与明細に無い月も外し、その人の集計月数を減らす
//!
//! 差 (`paid - calc`) はここでも DB でも持たない。期間の 給与合計 − 計算合計 を
//! 画面が引く (単月表の `minWageCompareRow` と定義を 1 箇所に保つ)。

use std::collections::BTreeMap;

use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};

/// 期間の上限 (月数)。画面の `MONTH_RANGE_MAX` と揃える。
pub const MAX_RANGE_MONTHS: i32 = 24;

/// 1 回の保存で受け付ける行数の上限。112 名 × 会社数を見込んで広めに取る
/// (超えるのは呼び出し側の組み立てミスなので、黙って切らず 400 にする)。
pub const MAX_SNAPSHOT_ROWS: usize = 4000;

/// 拘束時間ソース。DDL の CHECK と同じ 2 値。
pub const RESTRAINT_SOURCES: [&str; 2] = ["gcp", "current"];

/// 当月の拘束を オンプレ `kosoku-daily` から組めたか。DDL の CHECK と同じ 3 値で、
/// 画面 (`GET /restraint-api/wage-report` の `timecard_kosoku`) の語彙をそのまま使う
/// (Refs ohishi-exp/nuxt-dtako-admin#986 / #980)。
///
/// **`None` (未指定) はここに入れない** — `None` は「見ていない」で、`"yes"`
/// (揃っていた) とは別の事実。`restraint_source: "gcp"` は `kosoku-daily` を
/// 取りに行かないので、その経路では `None` が正しい値になる。
///
/// **`"no"` (取れなかった) と `"unreadable"` (読めなかった) を畳まない** — 前者は
/// 読み直せば入り、後者は上流の応答の形が変わっている。処方が逆になる。
pub const TIMECARD_KOSOKU_STATES: [&str; 3] = ["yes", "no", "unreadable"];

/// 乗務員 1 人 × 1 か月の確定値。保存 (POST の payload) と読み出し (SELECT の 1 行) で
/// 同じ形を使う — 片方だけ列が増える事故を防ぐ。
///
/// 金額は円。**NULL の意味が 0 と違う**ので `Option` を潰さない
/// (`calc_*` の NULL = 単価未設定、`paid_*` の NULL = 給与明細にこの人が無い)。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct WageSnapshotRow {
    pub driver_cd: i64,
    #[serde(default)]
    pub driver_name: String,
    #[serde(default)]
    pub company: Option<String>,
    #[serde(default)]
    pub branch_name: Option<String>,
    #[serde(default)]
    pub branch_code: Option<i32>,
    #[serde(default)]
    pub job_name: Option<String>,
    #[serde(default)]
    pub pay_kubun: Option<i16>,
    #[serde(default)]
    pub hourly_rate: Option<i32>,
    #[serde(default)]
    pub calc_base: Option<i32>,
    #[serde(default)]
    pub calc_overtime: Option<i32>,
    #[serde(default)]
    pub calc_total: Option<i32>,
    #[serde(default)]
    pub paid_base: Option<i32>,
    #[serde(default)]
    pub paid_overtime: Option<i32>,
    #[serde(default)]
    pub working_minutes: Option<i32>,
    #[serde(default)]
    pub restraint_missing: bool,
}

/// その (会社, 月, ソース) の保存に付いていた版。鮮度判定の材料。
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct MonthMasters {
    #[serde(default)]
    pub salary_item_sha: Option<String>,
    /// 突合した給与明細の同期時刻 (RFC3339 文字列のまま扱う — 比較は等値のみ)。
    #[serde(default)]
    pub payroll_synced_at: Option<String>,
}

// **最低賃金マスタの版 (`min_wage_sha`) は持たない** (2026-08-05 に廃止)。
//
// 保存している 9 数値 (計算 3 / 給与 2 + 実働) は単価マスタ・拘束時間・支給項目区分で
// 決まり、**最低賃金は 1 円も動かさない** (割れているかの判定に使うだけ)。影響しない
// ものを鮮度メタに入れたせいで、画面側では「最低賃金カードを開かないと版が付かない」
// という UI の折りたたみ状態への依存が生まれていた。
//
// 表の `min_wage_sha` 列は 006 で作ってしまった (適用済み migration は改変しない) ので
// 残るが、**常に NULL を書く**。将来ここが計算に効く設計になったら再利用する。

/// 保存要求 (`POST /api/kintai/wage-snapshot` の body)。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotRequest {
    pub comp_id: String,
    pub month: String,
    pub restraint_source: String,
    /// 当月の拘束の土台が取れていたか ([`TIMECARD_KOSOKU_STATES`] のいずれか)。
    ///
    /// **`masters` ではなくここに置く** — `masters` は「マスタの内容ハッシュ」の
    /// 並びで、これは土台の**取得可否**なので層が違う。
    ///
    /// 省略 (`None`) は「見ていない」。**既存クライアント (この列を送らない画面) が
    /// 壊れないよう `#[serde(default)]`** で受け、`"yes"` には化けさせない。
    #[serde(default)]
    pub timecard_kosoku: Option<String>,
    pub wage_logic_version: String,
    #[serde(default)]
    pub masters: MonthMasters,
    #[serde(default)]
    pub rows: Vec<WageSnapshotRow>,
}

/// 検証済みの保存要求。`month` は月初の `DATE` に解決済み。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidSnapshot {
    pub comp_id: String,
    pub ym: NaiveDate,
    pub restraint_source: String,
    /// [`SnapshotRequest::timecard_kosoku`] と同じ。検証済みなので
    /// [`TIMECARD_KOSOKU_STATES`] のいずれか、または `None`。
    pub timecard_kosoku: Option<String>,
    pub wage_logic_version: String,
    pub masters: MonthMasters,
    pub rows: Vec<WageSnapshotRow>,
}

/// "YYYY-MM" を月初の `NaiveDate` に。形が違えば `None`。
pub fn month_start(month: &str) -> Option<NaiveDate> {
    if month.len() != 7 || month.as_bytes().get(4) != Some(&b'-') {
        return None;
    }
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    if !(1..=12).contains(&mm) {
        return None;
    }
    NaiveDate::from_ymd_opt(year, mm, 1)
}

/// 月初から `delta` か月ずらした月初 (`delta` は負数も可)。
pub fn add_months(d: NaiveDate, delta: i32) -> NaiveDate {
    let total = d.year() * 12 + d.month0() as i32 + delta;
    let year = total.div_euclid(12);
    let month0 = total.rem_euclid(12) as u32;
    NaiveDate::from_ymd_opt(year, month0 + 1, 1).expect("normalized month is always valid")
}

/// `NaiveDate` (月初) を "YYYY-MM" に。
pub fn ym_label(d: NaiveDate) -> String {
    format!("{:04}-{:02}", d.year(), d.month())
}

/// RFC3339 の時刻を UTC の RFC3339 に正規化する。形が違えば `None`。
///
/// 保存は `TIMESTAMPTZ` を経由して `to_rfc3339()` で戻ってくる (`+00:00` 形) が、
/// 画面が送ってくるのは `Z` 形のこともある。**同じ時刻が別の文字列になると鮮度判定が
/// 常に「動いた」になる**ので、保存側も比較側もここを通す。
pub fn normalize_ts(s: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|t| t.with_timezone(&chrono::Utc).to_rfc3339())
}

/// 2 つの行集合が (順序を問わず) 同じか。保存前の「内容が同じなら書かない」判定用。
///
/// 乗務員CD で並べ直してから比べる — SELECT は `ORDER BY` で揃うが、画面が送る順は
/// 表示順 (会社 → 職員区分 → 営業所) なので一致しない。
pub fn rows_equal(a: &[WageSnapshotRow], b: &[WageSnapshotRow]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut x: Vec<&WageSnapshotRow> = a.iter().collect();
    let mut y: Vec<&WageSnapshotRow> = b.iter().collect();
    x.sort_by_key(|r| r.driver_cd);
    y.sort_by_key(|r| r.driver_cd);
    x == y
}

/// 保存要求の検証。**黙って切り詰めない** — 形が違えば理由つきで弾く。
pub fn validate_snapshot(req: SnapshotRequest) -> Result<ValidSnapshot, String> {
    if req.comp_id.trim().is_empty() {
        return Err("comp_id は必須です".to_string());
    }
    let ym = month_start(&req.month).ok_or("month は YYYY-MM で指定してください")?;
    if !RESTRAINT_SOURCES.contains(&req.restraint_source.as_str()) {
        return Err("restraint_source は gcp / current のいずれかです".to_string());
    }
    // 既知の 3 値でなければ弾く (**黙って `None` に倒さない**)。理由は 4 つ:
    //
    // 1. **`None` は既に「見ていない」という意味を持っている。** そこへ「未知の値が
    //    来た」を混ぜると 1 つの値に意味が 2 つ乗り、後から読む人が区別できない。
    // 2. 取れなかった土台の上で組んだ数字を「見ていない」と記録するのは、
    //    **この issue が塞ごうとしている穴そのものと同じ形** — 健全に見えるのに
    //    数字だけ違う保存物がもう 1 つ増える。この repo の流儀は loud fail で、
    //    `parse_synced_at` も形の違う時刻を NULL にせず 400 にしている。
    // 3. 同じ層・同じ性格の `restraint_source` が [`RESTRAINT_SOURCES`] で
    //    strict に検証しているので、流儀を揃える。
    // 4. DDL 側にも同じ CHECK を置いた (007)。ここで弾かないと DB エラーが 502 で出る。
    //
    // 前方互換の心配は要らない — PR の順が「上流が先」なので、画面が新しい値を
    // 送り始める時点で上流は必ずその値を知っている。
    if let Some(v) = &req.timecard_kosoku {
        if !TIMECARD_KOSOKU_STATES.contains(&v.as_str()) {
            return Err("timecard_kosoku は yes / no / unreadable のいずれかです".to_string());
        }
    }
    if req.wage_logic_version.trim().is_empty() {
        return Err("wage_logic_version は必須です".to_string());
    }
    if req.rows.len() > MAX_SNAPSHOT_ROWS {
        return Err(format!("rows 上限{MAX_SNAPSHOT_ROWS}"));
    }
    let mut seen = std::collections::HashSet::new();
    for row in &req.rows {
        if !seen.insert(row.driver_cd) {
            return Err(format!("乗務員CD {} が重複しています", row.driver_cd));
        }
    }
    // 時刻は保存前に正規化しておく — 比較 (`skipped_unchanged`・鮮度判定) が
    // 表記揺れで壊れないように、DB から戻る形と同じ文字列にしてから持つ
    let payroll_synced_at = match &req.masters.payroll_synced_at {
        Some(v) => {
            Some(normalize_ts(v).ok_or("masters.payroll_synced_at は RFC3339 で指定してください")?)
        }
        None => None,
    };
    Ok(ValidSnapshot {
        comp_id: req.comp_id,
        ym,
        restraint_source: req.restraint_source,
        timecard_kosoku: req.timecard_kosoku,
        wage_logic_version: req.wage_logic_version,
        masters: MonthMasters {
            payroll_synced_at,
            ..req.masters
        },
        rows: req.rows,
    })
}

/// `[from, to]` (両端含む) を月初の一覧に解決する。上限は [`MAX_RANGE_MONTHS`]。
pub fn resolve_months(from: &str, to: &str) -> Result<Vec<NaiveDate>, String> {
    let lo = month_start(from).ok_or("from は YYYY-MM で指定してください")?;
    let hi = month_start(to).ok_or("to は YYYY-MM で指定してください")?;
    if lo > hi {
        return Err("from は to 以前にしてください".to_string());
    }
    let span = (hi.year() - lo.year()) * 12 + (hi.month0() as i32 - lo.month0() as i32) + 1;
    if span > MAX_RANGE_MONTHS {
        return Err(format!("月範囲 上限{MAX_RANGE_MONTHS}"));
    }
    Ok((0..span).map(|i| add_months(lo, i)).collect())
}

/// 期間集計の入力 1 か月ぶん (DB から読んだ行 + その月の版)。
#[derive(Debug, Clone, Default)]
pub struct MonthBucket {
    pub rows: Vec<WageSnapshotRow>,
    pub masters: MonthMasters,
    /// 保存時に付いていた [`ValidSnapshot::timecard_kosoku`]。読み出しでそのまま返す。
    pub timecard_kosoku: Option<String>,
    pub wage_logic_version: Option<String>,
    pub computed_at: Option<String>,
}

/// 画面が渡してくる「今の版」。**渡されなかった項目は判定しない** (`None`)。
///
/// 単価マスタ・支給項目区分は R2 にあり Postgres からは引けないので、突き合わせる
/// 現行値は呼び出し側 (画面) が渡す。単価だけは行ごとに違うため、ここでは扱わず
/// 保存済みの `hourly_rate` を応答に載せて画面が突き合わせる (乗務員 × 月の粒度)。
#[derive(Debug, Clone, Default)]
pub struct CurrentVersions {
    pub salary_item_sha: Option<String>,
    pub wage_logic_version: Option<String>,
    pub payroll_synced_at: Option<String>,
}

impl CurrentVersions {
    /// 1 つも渡されていない = 鮮度を判定しない。
    pub fn is_empty(&self) -> bool {
        self.salary_item_sha.is_none()
            && self.wage_logic_version.is_none()
            && self.payroll_synced_at.is_none()
    }
}

/// 保存された版と今の版を突き合わせ、動いた項目を並べる。
///
/// **渡されていない項目は「変わっていない」ではなく「判定しない」**。片方だけ渡して
/// 全月 stale になるより、判定材料が無いことを黙って無視する方が害が小さい
/// (画面は判定できた項目だけを根拠に再計算を促す)。
pub fn stale_reasons(
    saved: &MonthMasters,
    saved_logic: Option<&str>,
    current: &CurrentVersions,
) -> Vec<String> {
    let mut out = Vec::new();
    let differs = |cur: &Option<String>, sav: &Option<String>| -> bool {
        matches!(cur, Some(c) if sav.as_deref() != Some(c.as_str()))
    };
    if differs(&current.salary_item_sha, &saved.salary_item_sha) {
        out.push("salary_item".to_string());
    }
    if differs(&current.payroll_synced_at, &saved.payroll_synced_at) {
        out.push("payroll".to_string());
    }
    if let Some(cur) = &current.wage_logic_version {
        if saved_logic != Some(cur.as_str()) {
            out.push("wage_logic_version".to_string());
        }
    }
    out
}

/// その月が「給与を取り込んでいない」か。
///
/// **判定は保存された金額だけで行う** — 全行の `paid_base` が NULL なら未取込。
/// **0 円と NULL は別物**で、全員が本当に 0 円の月は無いので、全行 NULL は取り込み漏れ。
///
/// ## `payroll_synced_at` を判定に混ぜない (2026-08-05 の修正)
///
/// 当初は「同期時刻が無ければ未取込」も条件に入れていたが、**本番で全月が集計から
/// 消えた**。同期時刻は画面が突合に使った給与明細から拾う鮮度メタで、
/// 給与額そのものが入っていても取れないことがある (画面のキャッシュに古い形の
/// 明細が残っている等)。
///
/// **鮮度メタの欠落は「古いかもしれない」であって「データが無い」ではない。**
/// 混ぜると、金額が入っているのに月ごと集計から外れる — 実際の支払い不足を
/// 見落とす方向の誤りなので、判定は金額の有無だけに寄せる。
pub fn month_payroll_missing(bucket: &MonthBucket) -> bool {
    bucket.rows.iter().all(|r| r.paid_base.is_none())
}

/// 月ごとの状態 (画面のカバレッジバー)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MonthCoverage {
    pub ym: String,
    pub saved: bool,
    pub drivers: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub computed_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stale: Option<bool>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub stale_reason: Vec<String>,
    /// 集計から外した理由 (`"payroll_missing"`)。入っている月は合計に寄与しない。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub excluded: Option<String>,
    /// 保存時に拘束の土台が取れていたか ([`TIMECARD_KOSOKU_STATES`])。
    ///
    /// **無ければ列ごと出さない。** 出すと「見ていない」と「揃っていた」が混ざる —
    /// 画面はこの値が在るときだけ注記を出す (nuxt-dtako-admin#989 と同じ読み方)。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timecard_kosoku: Option<String>,
}

/// 乗務員 × 月の金額 (`by_month`)。差は入れない (画面が引く)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MonthAmounts {
    pub calc_base: Option<i32>,
    pub calc_overtime: Option<i32>,
    pub calc_total: Option<i32>,
    pub paid_base: Option<i32>,
    pub paid_overtime: Option<i32>,
    /// その月に適用した基礎単価。画面が今のマスタと突き合わせて行単位の
    /// 「要再計算」を出すために返す。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hourly_rate: Option<i32>,
    /// その月の実労働時間 (分)。行合計とは別に月ごとで返す — 画面が
    /// 月セルの内訳 (単価 × 実働 でその金額になったのか) を出せるようにするため。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub working_minutes: Option<i32>,
}

/// 期間集計の 1 行 (1 乗務員)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AggregatedDriver {
    pub driver_cd: i64,
    pub driver_name: String,
    pub company: Option<String>,
    pub branch_name: Option<String>,
    pub branch_code: Option<i32>,
    pub job_name: Option<String>,
    pub pay_kubun: Option<i16>,
    /// 合計に寄与した月数。**期間の月数と一致するとは限らない** (入社・退職・欠測)。
    pub months_counted: usize,
    /// 月は集計対象なのにこの人だけ欠けた月 (欠測・単価未設定・給与に無い)。
    /// 月ごと外れた月は `months` のカバレッジで分かるのでここには入れない。
    pub months_missing: Vec<String>,
    pub by_month: BTreeMap<String, MonthAmounts>,
    pub calc_base: i64,
    pub calc_overtime: i64,
    pub calc_total: i64,
    pub paid_base: i64,
    pub paid_overtime: i64,
    pub working_minutes: i64,
}

/// 期間集計の結果。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RangeAggregate {
    pub months: Vec<MonthCoverage>,
    pub rows: Vec<AggregatedDriver>,
}

/// 属性は「期間内で最後に見た月」のものを採る。退職者は行に残す (ユーザー決定
/// 2026-08-05) ので、最後に在籍した月の所属・氏名で並ぶ。
fn overwrite_attrs(dst: &mut AggregatedDriver, src: &WageSnapshotRow) {
    dst.driver_name = src.driver_name.clone();
    dst.company = src.company.clone();
    dst.branch_name = src.branch_name.clone();
    dst.branch_code = src.branch_code;
    dst.job_name = src.job_name.clone();
    dst.pay_kubun = src.pay_kubun;
}

fn empty_driver(row: &WageSnapshotRow) -> AggregatedDriver {
    AggregatedDriver {
        driver_cd: row.driver_cd,
        driver_name: row.driver_name.clone(),
        company: row.company.clone(),
        branch_name: row.branch_name.clone(),
        branch_code: row.branch_code,
        job_name: row.job_name.clone(),
        pay_kubun: row.pay_kubun,
        months_counted: 0,
        months_missing: Vec::new(),
        by_month: BTreeMap::new(),
        calc_base: 0,
        calc_overtime: 0,
        calc_total: 0,
        paid_base: 0,
        paid_overtime: 0,
        working_minutes: 0,
    }
}

/// その行をその月の合計に入れてよいか。入れられない理由があれば `false`。
///
/// - 欠測 (拘束ソースにこの人のこの月が無い) — 0 分ではないので判定も金額も出さない
/// - 単価未設定 (`calc_total` が NULL) — 計算側が出ないので差も出せない
/// - 給与明細にこの人が無い (`paid_base` が NULL) — 0 円で足すと支払い不足に化ける
pub fn row_counts(row: &WageSnapshotRow) -> bool {
    !row.restraint_missing && row.calc_total.is_some() && row.paid_base.is_some()
}

/// 期間合計を組む。
///
/// `buckets` は**期間の全月**を昇順で渡す (データが無い月も空の `None` で渡す) —
/// 「応答に無い = 0」を作らないため、カバレッジは月の数だけ必ず返す。
pub fn aggregate_range(
    months: &[NaiveDate],
    buckets: &[Option<MonthBucket>],
    current: &CurrentVersions,
) -> RangeAggregate {
    let mut coverage = Vec::with_capacity(months.len());
    let mut drivers: BTreeMap<i64, AggregatedDriver> = BTreeMap::new();

    for (ym, bucket) in months.iter().zip(buckets.iter()) {
        let label = ym_label(*ym);
        let Some(bucket) = bucket else {
            coverage.push(MonthCoverage {
                ym: label,
                saved: false,
                drivers: 0,
                computed_at: None,
                stale: None,
                stale_reason: Vec::new(),
                excluded: None,
                // 未保存の月は「見ていない」ですらない (保存物が無い)。None のまま出さない
                timecard_kosoku: None,
            });
            continue;
        };
        let reasons = stale_reasons(
            &bucket.masters,
            bucket.wage_logic_version.as_deref(),
            current,
        );
        let stale = if current.is_empty() {
            None
        } else {
            Some(!reasons.is_empty())
        };
        if month_payroll_missing(bucket) {
            coverage.push(MonthCoverage {
                ym: label,
                saved: true,
                drivers: 0,
                computed_at: bucket.computed_at.clone(),
                stale,
                stale_reason: reasons,
                excluded: Some("payroll_missing".to_string()),
                timecard_kosoku: bucket.timecard_kosoku.clone(),
            });
            continue;
        }
        let mut counted = 0usize;
        for row in &bucket.rows {
            let entry = drivers
                .entry(row.driver_cd)
                .or_insert_with(|| empty_driver(row));
            overwrite_attrs(entry, row);
            if !row_counts(row) {
                entry.months_missing.push(label.clone());
                continue;
            }
            entry.by_month.insert(
                label.clone(),
                MonthAmounts {
                    calc_base: row.calc_base,
                    calc_overtime: row.calc_overtime,
                    calc_total: row.calc_total,
                    paid_base: row.paid_base,
                    paid_overtime: row.paid_overtime,
                    hourly_rate: row.hourly_rate,
                    working_minutes: row.working_minutes,
                },
            );
            entry.months_counted += 1;
            entry.calc_base += i64::from(row.calc_base.unwrap_or(0));
            entry.calc_overtime += i64::from(row.calc_overtime.unwrap_or(0));
            entry.calc_total += i64::from(row.calc_total.unwrap_or(0));
            entry.paid_base += i64::from(row.paid_base.unwrap_or(0));
            entry.paid_overtime += i64::from(row.paid_overtime.unwrap_or(0));
            entry.working_minutes += i64::from(row.working_minutes.unwrap_or(0));
            counted += 1;
        }
        coverage.push(MonthCoverage {
            ym: label,
            saved: true,
            drivers: counted,
            computed_at: bucket.computed_at.clone(),
            stale,
            stale_reason: reasons,
            excluded: None,
            timecard_kosoku: bucket.timecard_kosoku.clone(),
        });
    }

    RangeAggregate {
        months: coverage,
        // 1 か月も合計に寄与しなかった人は出さない (全部 0 の行は読み手を惑わす)
        rows: drivers
            .into_values()
            .filter(|d| d.months_counted > 0)
            .collect(),
    }
}
