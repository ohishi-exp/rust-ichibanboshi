//! 賃金確定値の月次スナップショットの純ロジック (Refs #291、
//! ohishi-exp/nuxt-dtako-admin#677)。
//!
//! HTTP と SQL は [`crate::routes::wage_snapshot`]。ここには **DB も時計も要らない**
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
    #[serde(default)]
    pub min_wage_sha: Option<String>,
    /// 突合した給与明細の同期時刻 (RFC3339 文字列のまま扱う — 比較は等値のみ)。
    #[serde(default)]
    pub payroll_synced_at: Option<String>,
}

/// 保存要求 (`POST /api/kintai/wage-snapshot` の body)。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SnapshotRequest {
    pub comp_id: String,
    pub month: String,
    pub restraint_source: String,
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
    pub min_wage_sha: Option<String>,
    pub wage_logic_version: Option<String>,
    pub payroll_synced_at: Option<String>,
}

impl CurrentVersions {
    /// 1 つも渡されていない = 鮮度を判定しない。
    pub fn is_empty(&self) -> bool {
        self.salary_item_sha.is_none()
            && self.min_wage_sha.is_none()
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
    if differs(&current.min_wage_sha, &saved.min_wage_sha) {
        out.push("min_wage".to_string());
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
/// 同期時刻が無い、または保存された全行の `paid_base` が NULL なら未取込とみなす。
/// **0 円と NULL は別物** — 全員が本当に 0 円の月は無いので、全行 NULL は取り込み漏れ。
pub fn month_payroll_missing(bucket: &MonthBucket) -> bool {
    bucket.masters.payroll_synced_at.is_none() || bucket.rows.iter().all(|r| r.paid_base.is_none())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ym(y: i32, m: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, 1).unwrap()
    }

    fn row(driver_cd: i64) -> WageSnapshotRow {
        WageSnapshotRow {
            driver_cd,
            driver_name: "山田".to_string(),
            company: Some("0200".to_string()),
            branch_name: Some("本社".to_string()),
            branch_code: Some(210),
            job_name: Some("乗務員".to_string()),
            pay_kubun: Some(1),
            hourly_rate: Some(1420),
            calc_base: Some(200_000),
            calc_overtime: Some(80_000),
            calc_total: Some(280_000),
            paid_base: Some(198_000),
            paid_overtime: Some(78_000),
            working_minutes: Some(11_820),
            restraint_missing: false,
        }
    }

    fn bucket(rows: Vec<WageSnapshotRow>) -> MonthBucket {
        MonthBucket {
            rows,
            masters: MonthMasters {
                salary_item_sha: Some("item-1".to_string()),
                min_wage_sha: Some("mw-1".to_string()),
                payroll_synced_at: Some("2026-02-03T09:12:00Z".to_string()),
            },
            wage_logic_version: Some("wage-1".to_string()),
            computed_at: Some("2026-08-05T01:20:00Z".to_string()),
        }
    }

    fn req() -> SnapshotRequest {
        SnapshotRequest {
            comp_id: "comp".to_string(),
            month: "2026-01".to_string(),
            restraint_source: "gcp".to_string(),
            wage_logic_version: "wage-1".to_string(),
            masters: MonthMasters::default(),
            rows: vec![row(1035)],
        }
    }

    #[test]
    fn month_start_rejects_bad_shapes() {
        assert_eq!(month_start("2026-06"), Some(ym(2026, 6)));
        for bad in ["2026-6", "", "2026-13", "2026/06", "2026-00", "2026-0x"] {
            assert_eq!(month_start(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn add_months_rolls_across_years() {
        assert_eq!(add_months(ym(2026, 12), 1), ym(2027, 1));
        assert_eq!(add_months(ym(2026, 1), -1), ym(2025, 12));
        assert_eq!(ym_label(ym(2026, 3)), "2026-03");
    }

    #[test]
    fn validate_accepts_a_well_formed_request() {
        let v = validate_snapshot(req()).unwrap();
        assert_eq!(v.ym, ym(2026, 1));
        assert_eq!(v.rows.len(), 1);
        assert_eq!(v.comp_id, "comp");
        assert_eq!(v.restraint_source, "gcp");
        assert_eq!(v.wage_logic_version, "wage-1");
        assert_eq!(v.masters, MonthMasters::default());
    }

    #[test]
    fn validate_rejects_empty_comp_id() {
        let bad = SnapshotRequest {
            comp_id: "  ".to_string(),
            ..req()
        };
        assert!(validate_snapshot(bad).unwrap_err().contains("comp_id"));
    }

    #[test]
    fn validate_rejects_bad_month() {
        let bad = SnapshotRequest {
            month: "2026-13".to_string(),
            ..req()
        };
        assert!(validate_snapshot(bad).unwrap_err().contains("month"));
    }

    /// DDL の CHECK と同じ 2 値。ここで弾かないと DB エラーが 502 になって出る。
    #[test]
    fn validate_rejects_unknown_restraint_source() {
        let bad = SnapshotRequest {
            restraint_source: "supabase".to_string(),
            ..req()
        };
        assert!(validate_snapshot(bad)
            .unwrap_err()
            .contains("restraint_source"));
    }

    #[test]
    fn validate_rejects_empty_logic_version() {
        let bad = SnapshotRequest {
            wage_logic_version: " ".to_string(),
            ..req()
        };
        assert!(validate_snapshot(bad)
            .unwrap_err()
            .contains("wage_logic_version"));
    }

    #[test]
    fn validate_rejects_too_many_rows() {
        let bad = SnapshotRequest {
            rows: (0..=MAX_SNAPSHOT_ROWS as i64).map(row).collect(),
            ..req()
        };
        assert!(validate_snapshot(bad).unwrap_err().contains("rows"));
    }

    /// 重複した乗務員CD は主キー衝突になる前に弾く (どちらが正か決められない)。
    #[test]
    fn validate_rejects_duplicate_driver_cd() {
        let bad = SnapshotRequest {
            rows: vec![row(1035), row(1035)],
            ..req()
        };
        assert!(validate_snapshot(bad).unwrap_err().contains("1035"));
    }

    #[test]
    fn validate_normalizes_the_payroll_sync_time() {
        let req = SnapshotRequest {
            masters: MonthMasters {
                payroll_synced_at: Some("2026-02-03T18:12:00+09:00".to_string()),
                ..Default::default()
            },
            ..req()
        };
        let v = validate_snapshot(req).unwrap();
        assert_eq!(
            v.masters.payroll_synced_at.as_deref(),
            Some("2026-02-03T09:12:00+00:00")
        );
    }

    /// 形が違う時刻を黙って NULL にしない — NULL は「給与未取込」の意味になり、
    /// その月が期間集計から丸ごと消える。
    #[test]
    fn validate_rejects_a_malformed_payroll_sync_time() {
        let req = SnapshotRequest {
            masters: MonthMasters {
                payroll_synced_at: Some("2026/02/03".to_string()),
                ..Default::default()
            },
            ..req()
        };
        assert!(validate_snapshot(req)
            .unwrap_err()
            .contains("payroll_synced_at"));
    }

    #[test]
    fn normalize_ts_folds_offsets_to_utc() {
        assert_eq!(
            normalize_ts("2026-02-03T09:12:00Z").as_deref(),
            Some("2026-02-03T09:12:00+00:00")
        );
        assert_eq!(normalize_ts("nope"), None);
    }

    /// 送る順 (表示順) と DB の順 (乗務員CD順) が違っても、内容が同じなら同じと見る。
    #[test]
    fn rows_equal_ignores_order_but_not_content() {
        let a = vec![row(1035), row(2042)];
        let b = vec![row(2042), row(1035)];
        assert!(rows_equal(&a, &b));
        assert!(!rows_equal(&a, &[row(1035)]));
        let changed = vec![
            row(1035),
            WageSnapshotRow {
                paid_base: Some(1),
                ..row(2042)
            },
        ];
        assert!(!rows_equal(&a, &changed));
    }

    #[test]
    fn resolve_months_lists_both_ends() {
        let months = resolve_months("2026-01", "2026-03").unwrap();
        assert_eq!(months, vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)]);
    }

    #[test]
    fn resolve_months_rejects_bad_shapes_and_order_and_span() {
        assert!(resolve_months("2026-1", "2026-03")
            .unwrap_err()
            .contains("from"));
        assert!(resolve_months("2026-01", "").unwrap_err().contains("to"));
        assert!(resolve_months("2026-03", "2026-01")
            .unwrap_err()
            .contains("以前"));
        assert!(resolve_months("2024-01", "2026-03")
            .unwrap_err()
            .contains("上限"));
        // 上限ちょうどは通る
        assert_eq!(resolve_months("2025-01", "2026-12").unwrap().len(), 24);
    }

    #[test]
    fn stale_reasons_lists_only_what_moved() {
        let saved = bucket(vec![]).masters;
        let current = CurrentVersions {
            salary_item_sha: Some("item-2".to_string()),
            min_wage_sha: Some("mw-1".to_string()),
            wage_logic_version: Some("wage-1".to_string()),
            payroll_synced_at: Some("2026-02-03T09:12:00Z".to_string()),
        };
        assert_eq!(
            stale_reasons(&saved, Some("wage-1"), &current),
            vec!["salary_item".to_string()]
        );
    }

    #[test]
    fn stale_reasons_catches_payroll_and_logic_version() {
        let saved = bucket(vec![]).masters;
        let current = CurrentVersions {
            payroll_synced_at: Some("2026-03-01T00:00:00Z".to_string()),
            wage_logic_version: Some("wage-2".to_string()),
            ..Default::default()
        };
        assert_eq!(
            stale_reasons(&saved, Some("wage-1"), &current),
            vec!["payroll".to_string(), "wage_logic_version".to_string()]
        );
    }

    /// 渡されていない項目は「変わっていない」ではなく**判定しない**。
    #[test]
    fn stale_reasons_ignores_versions_the_caller_did_not_send() {
        let saved = bucket(vec![]).masters;
        assert!(stale_reasons(&saved, Some("wage-1"), &CurrentVersions::default()).is_empty());
        assert!(CurrentVersions::default().is_empty());
    }

    /// 保存側に版が無い (古い保存) 場合も、今の版が来ていれば動いたと見る。
    #[test]
    fn stale_reasons_treats_missing_saved_version_as_moved() {
        let current = CurrentVersions {
            min_wage_sha: Some("mw-1".to_string()),
            wage_logic_version: Some("wage-1".to_string()),
            ..Default::default()
        };
        let reasons = stale_reasons(&MonthMasters::default(), None, &current);
        assert_eq!(
            reasons,
            vec!["min_wage".to_string(), "wage_logic_version".to_string()]
        );
    }

    #[test]
    fn payroll_missing_when_no_sync_time_or_all_rows_null() {
        let mut b = bucket(vec![row(1)]);
        assert!(!month_payroll_missing(&b));

        b.masters.payroll_synced_at = None;
        assert!(month_payroll_missing(&b));

        let mut b = bucket(vec![WageSnapshotRow {
            paid_base: None,
            ..row(1)
        }]);
        assert!(month_payroll_missing(&b));
        b.rows.push(row(2));
        assert!(!month_payroll_missing(&b));
    }

    #[test]
    fn row_counts_rejects_missing_restraint_rate_or_payroll() {
        assert!(row_counts(&row(1)));
        assert!(!row_counts(&WageSnapshotRow {
            restraint_missing: true,
            ..row(1)
        }));
        assert!(!row_counts(&WageSnapshotRow {
            calc_total: None,
            ..row(1)
        }));
        assert!(!row_counts(&WageSnapshotRow {
            paid_base: None,
            ..row(1)
        }));
    }

    #[test]
    fn aggregate_sums_saved_months_and_keeps_month_cells() {
        let months = vec![ym(2026, 1), ym(2026, 2)];
        let buckets = vec![Some(bucket(vec![row(1035)])), Some(bucket(vec![row(1035)]))];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

        assert_eq!(agg.rows.len(), 1);
        let d = &agg.rows[0];
        assert_eq!(d.months_counted, 2);
        assert_eq!(d.calc_total, 560_000);
        assert_eq!(d.paid_base, 396_000);
        assert_eq!(d.by_month.len(), 2);
        assert_eq!(d.by_month["2026-01"].hourly_rate, Some(1420));
        assert!(d.months_missing.is_empty());
        assert!(agg.months.iter().all(|m| m.saved && m.excluded.is_none()));
        // 版を渡していないので鮮度は判定しない
        assert!(agg.months.iter().all(|m| m.stale.is_none()));
    }

    /// 保存が無い月は `saved: false` で必ず並ぶ (「応答に無い = 0」を作らない)。
    #[test]
    fn aggregate_lists_unsaved_months_without_dropping_them() {
        let months = vec![ym(2026, 1), ym(2026, 2)];
        let buckets = vec![Some(bucket(vec![row(1035)])), None];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

        assert_eq!(agg.months.len(), 2);
        assert!(!agg.months[1].saved);
        assert_eq!(agg.months[1].ym, "2026-02");
        assert_eq!(agg.rows[0].months_counted, 1);
        // 月ごと外れた月は months_missing に入れない (カバレッジで分かる)
        assert!(agg.rows[0].months_missing.is_empty());
    }

    /// 給与未取込の月は**そもそも集計に出さない** (ユーザー決定 2026-08-05)。
    #[test]
    fn aggregate_excludes_months_without_payroll() {
        let months = vec![ym(2026, 1), ym(2026, 2)];
        let mut no_payroll = bucket(vec![row(1035)]);
        no_payroll.masters.payroll_synced_at = None;
        let buckets = vec![Some(bucket(vec![row(1035)])), Some(no_payroll)];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

        assert_eq!(agg.months[1].excluded.as_deref(), Some("payroll_missing"));
        assert_eq!(agg.months[1].drivers, 0);
        assert_eq!(agg.rows[0].months_counted, 1);
        assert_eq!(agg.rows[0].calc_total, 280_000);
    }

    /// 欠測・単価未設定・その人だけ給与に無い月は、その人の集計だけから外れる。
    #[test]
    fn aggregate_skips_rows_that_cannot_be_counted() {
        let months = vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)];
        let buckets = vec![
            Some(bucket(vec![
                row(1035),
                WageSnapshotRow {
                    restraint_missing: true,
                    ..row(2042)
                },
            ])),
            Some(bucket(vec![
                row(1035),
                WageSnapshotRow {
                    calc_total: None,
                    ..row(2042)
                },
            ])),
            Some(bucket(vec![
                row(1035),
                WageSnapshotRow {
                    paid_base: None,
                    ..row(2042)
                },
            ])),
        ];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

        // 2042 は 3 か月とも数えられないので行ごと出ない
        assert_eq!(agg.rows.len(), 1);
        assert_eq!(agg.rows[0].driver_cd, 1035);
        assert_eq!(agg.rows[0].months_counted, 3);
        // 月の drivers は「合計に寄与した人数」
        assert!(agg.months.iter().all(|m| m.drivers == 1));
    }

    /// 期間の途中で欠けた人は行に残り、集計月数だけが減る (退職者の扱い)。
    #[test]
    fn aggregate_keeps_partial_drivers_with_missing_months() {
        let months = vec![ym(2026, 1), ym(2026, 2)];
        let buckets = vec![
            Some(bucket(vec![row(1035), row(2042)])),
            Some(bucket(vec![
                row(1035),
                WageSnapshotRow {
                    restraint_missing: true,
                    ..row(2042)
                },
            ])),
        ];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());

        let d = agg.rows.iter().find(|d| d.driver_cd == 2042).unwrap();
        assert_eq!(d.months_counted, 1);
        assert_eq!(d.months_missing, vec!["2026-02".to_string()]);
        assert_eq!(d.by_month.len(), 1);
    }

    /// 属性は期間内で最後に見た月のものを採る (退職者は最後の所属で並ぶ)。
    #[test]
    fn aggregate_takes_attributes_from_the_latest_month() {
        let months = vec![ym(2026, 1), ym(2026, 2)];
        let buckets = vec![
            Some(bucket(vec![row(1035)])),
            Some(bucket(vec![WageSnapshotRow {
                branch_name: Some("大阪".to_string()),
                branch_code: Some(310),
                ..row(1035)
            }])),
        ];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());
        assert_eq!(agg.rows[0].branch_name.as_deref(), Some("大阪"));
        assert_eq!(agg.rows[0].branch_code, Some(310));
    }

    /// **月ごとの差の合計 == 期間合計から出した差** (画面が横に並べる値と右端の値が合う)。
    #[test]
    fn monthly_diffs_sum_to_the_range_diff() {
        let months = vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)];
        let buckets = vec![
            Some(bucket(vec![row(1035)])),
            Some(bucket(vec![WageSnapshotRow {
                paid_base: Some(210_000),
                paid_overtime: Some(90_000),
                ..row(1035)
            }])),
            Some(bucket(vec![WageSnapshotRow {
                calc_base: Some(190_000),
                calc_total: Some(265_000),
                ..row(1035)
            }])),
        ];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());
        let d = &agg.rows[0];

        let monthly_diff: i64 = d
            .by_month
            .values()
            .map(|m| {
                i64::from(m.paid_base.unwrap_or(0) + m.paid_overtime.unwrap_or(0))
                    - i64::from(m.calc_total.unwrap_or(0))
            })
            .sum();
        let range_diff = d.paid_base + d.paid_overtime - d.calc_total;
        assert_eq!(monthly_diff, range_diff);
    }

    /// 3 段の縦計 (基本給 + 残業代 = 合計) は期間合計でも成り立つ
    /// (ohishi-exp/nuxt-dtako-admin#673 と同じ不変則)。
    #[test]
    fn base_plus_overtime_equals_total_in_range_sum() {
        let months = vec![ym(2026, 1), ym(2026, 2)];
        let buckets = vec![Some(bucket(vec![row(1035)])), Some(bucket(vec![row(1035)]))];
        let agg = aggregate_range(&months, &buckets, &CurrentVersions::default());
        let d = &agg.rows[0];
        assert_eq!(d.calc_base + d.calc_overtime, d.calc_total);
    }

    #[test]
    fn aggregate_marks_stale_months_when_versions_move() {
        let months = vec![ym(2026, 1)];
        let buckets = vec![Some(bucket(vec![row(1035)]))];
        let current = CurrentVersions {
            salary_item_sha: Some("item-2".to_string()),
            ..Default::default()
        };
        let agg = aggregate_range(&months, &buckets, &current);
        assert_eq!(agg.months[0].stale, Some(true));
        assert_eq!(agg.months[0].stale_reason, vec!["salary_item".to_string()]);
        assert_eq!(
            agg.months[0].computed_at.as_deref(),
            Some("2026-08-05T01:20:00Z")
        );
    }

    #[test]
    fn aggregate_marks_fresh_months_when_versions_match() {
        let months = vec![ym(2026, 1)];
        let buckets = vec![Some(bucket(vec![row(1035)]))];
        let current = CurrentVersions {
            salary_item_sha: Some("item-1".to_string()),
            min_wage_sha: Some("mw-1".to_string()),
            wage_logic_version: Some("wage-1".to_string()),
            payroll_synced_at: Some("2026-02-03T09:12:00Z".to_string()),
        };
        let agg = aggregate_range(&months, &buckets, &current);
        assert_eq!(agg.months[0].stale, Some(false));
        assert!(agg.months[0].stale_reason.is_empty());
    }
}
