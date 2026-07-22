//! 給与大臣 (OHKEN) 読み取りの純粋ロジック (Refs #82)。
//!
//! スキーマの根拠は `docs/kyuyo-daijin-schema.md` (#81 実機調査):
//! - DB 命名 `KYDATA{会社4桁}_{年度3桁}C`、年度起点は 12 月分 (12 月給与は翌年度 DB)
//! - `KYUYO` 1 行 = 社員 (`SHAIN`) × 支給回インデックス (`MONTH`)。月の特定は
//!   固定式でなく `CHINGINKIKANST` (賃金期間開始) の範囲照合で行う (月内複数支給や
//!   欠月でインデックスがずれるため)
//! - 支給/控除項目: `MONEY00..79` ↔ `KOUMOKU.TAIKEIKOUNO = 体系(2桁) + (18+列番号)(3桁)`
//! - 支給合計等は `SHUKEI1` の計算済み列 (`SOSHIKYU{NN}` 等、NN = MONTH) を使う

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde::Serialize;

/// 給与比較 API の対象会社 (#81 で確定した現行 4 社。0500/0900 は廃業済み DB のみ)。
pub const ALLOWED_COMPANIES: [&str; 4] = ["0100", "0200", "0300", "0400"];

/// `KYUYO.MONEY00..79` の列数。
pub const MONEY_COLUMNS: usize = 80;

/// `SHUKEI1` の支給回インデックス上限 (列 `SOSHIKYU00`..`SOSHIKYU21`)。
pub const MAX_MONTH_INDEX: i32 = 21;

/// "YYYY-MM" を (年, 月) にパースする。
pub fn parse_month(s: &str) -> Option<(i32, u32)> {
    let (y, m) = s.split_once('-')?;
    if y.len() != 4 || m.len() != 2 {
        return None;
    }
    let year: i32 = y.parse().ok()?;
    let month: u32 = m.parse().ok()?;
    if !(1990..=2999).contains(&year) || !(1..=12).contains(&month) {
        return None;
    }
    Some((year, month))
}

/// 支給対象月 (賃金期間の月) が属する年度 3 桁を返す。
///
/// 年度 = 西暦 - 1900。ただし **12 月分は翌年度 DB に入る** (#81: `_126C` の
/// `MONTH=0` = 2025年12月分) ため 12 月は +1 する。
pub fn nendo_for_month(year: i32, month: u32) -> i32 {
    if month == 12 {
        year - 1900 + 1
    } else {
        year - 1900
    }
}

/// 年度 DB 名を組み立てる。`company` は [`ALLOWED_COMPANIES`] 検証済み前提。
pub fn kydata_db_name(company: &str, nendo: i32) -> String {
    format!("KYDATA{company}_{nendo}C")
}

/// `KYDATA{会社4桁}_{年度3桁}C` 形式の DB 名を (会社, 年度) に分解する。
pub fn parse_kydata_db_name(name: &str) -> Option<(String, i32)> {
    let rest = name.strip_prefix("KYDATA")?;
    let (company, nendo_part) = rest.split_once('_')?;
    let nendo_digits = nendo_part.strip_suffix('C')?;
    if company.len() != 4 || !company.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    if nendo_digits.len() != 3 || !nendo_digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some((company.to_string(), nendo_digits.parse().ok()?))
}

/// 賃金期間 (対象月) の範囲を "YYYY-MM-DD" の半開区間で返す。
pub fn month_period(year: i32, month: u32) -> (String, String) {
    let from = format!("{year:04}-{month:02}-01");
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    (from, format!("{ny:04}-{nm:02}-01"))
}

/// `KOUMOKU.TAIKEIKOUNO` (体系 2 桁 + 項目番号 3 桁) を組み立てる。
/// `money_index` は `MONEY{N}` の列番号 (0..=79)、項目番号は `18 + N`。
pub fn taikeikouno(taikei: i32, money_index: usize) -> String {
    format!("{:02}{:03}", taikei.clamp(0, 99), 18 + money_index)
}

/// `SHAIN1.CODE` (前ゼロ + 末尾スペース埋め) から dtako 側と突合するキーを作る。
/// trim + 前ゼロ除去。全部ゼロなら "0"。
pub fn employee_code_key(code: &str) -> String {
    let trimmed = code.trim();
    let stripped = trimmed.trim_start_matches('0');
    if stripped.is_empty() {
        if trimmed.is_empty() {
            String::new()
        } else {
            "0".to_string()
        }
    } else {
        stripped.to_string()
    }
}

/// allowlist 設定 (カンマ区切り相当の配列) を正規化する: trim + 小文字化、空要素除去。
pub fn normalize_emails(raw: &[String]) -> Vec<String> {
    raw.iter()
        .map(|e| e.trim().to_lowercase())
        .filter(|e| !e.is_empty())
        .collect()
}

/// email が allowlist に含まれるか (大文字小文字を無視した完全一致)。
pub fn email_allowed(allowed: &[String], email: &str) -> bool {
    let target = email.trim().to_lowercase();
    !target.is_empty() && allowed.iter().any(|e| e == &target)
}

// ══════════════════════════════════════════════════════════════
// DB 層 → ロジック層の生データ
// ══════════════════════════════════════════════════════════════

/// `KYUYO` + `SHAIN1` + `SHOZOKU` を JOIN した 1 行。
#[derive(Debug, Clone)]
pub struct RawKyuyoRow {
    /// `KYUYO.SHAIN` (社員内部コード)。
    pub shain: i32,
    /// `KYUYO.[MONTH]` (年度内の支給回インデックス、0 起点)。
    pub month_index: i32,
    /// `SHIKYUBI` (支給日、"YYYY-MM-DD")。
    pub pay_date: String,
    /// `CHINGINKIKANST` (賃金期間開始、"YYYY-MM-DD")。
    pub period_start: String,
    /// `CHINGINKIKANEN` (賃金期間終了、"YYYY-MM-DD")。
    pub period_end: String,
    /// `SHAIN1.CODE` (社員番号、trim 済み)。
    pub employee_code: String,
    /// `SHAIN1.NAME` (氏名、trim 済み)。
    pub employee_name: String,
    /// `SHAIN1.TAIKYU` (0=在籍中)。
    pub taikyu: i32,
    /// `SHOZOKU.SNAME` (所属表示名、trim 済み)。
    pub department: String,
    /// `SHOZOKU.TAIKEI` (給与体系コード、`TAIKEIKOUNO` の先頭 2 桁)。
    pub taikei: i32,
    /// `MONEY00..79` (円、[`MONEY_COLUMNS`] 個)。
    pub money: Vec<i64>,
}

/// `SHUKEI1` の 1 社員 × 1 支給回の計算済み集計。
#[derive(Debug, Clone)]
pub struct RawShukeiRow {
    pub shain: i32,
    pub month_index: i32,
    /// `SOSHIKYU{NN}` (総支給額)。
    pub soshikyu: i64,
    /// `KAZEI{NN}` (課税支給合計)。
    pub kazei: i64,
    /// `HOKEN{NN}` (社会保険料)。
    pub hoken: i64,
    /// `ZEI{NN}` (税金)。
    pub zei: i64,
    /// `SHOKOUJO{NN}` (諸控除)。
    pub shokoujo: i64,
}

// ══════════════════════════════════════════════════════════════
// レスポンス構造体
// ══════════════════════════════════════════════════════════════

/// `SHUKEI1` 由来の計算済み合計。控除合計・差引は #81 の式で導出する。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct PayrollTotals {
    /// 総支給額。
    pub soshikyu: i64,
    /// 課税支給合計。
    pub kazei: i64,
    /// 社会保険料 (控除)。
    pub hoken: i64,
    /// 税金 (控除)。
    pub zei: i64,
    /// 諸控除。
    pub shokoujo: i64,
    /// 控除合計 = hoken + zei + shokoujo。
    pub deduction_total: i64,
    /// 差引支給 = soshikyu - 控除合計。
    pub net_pay: i64,
}

/// 社員 × 支給回の給与 1 行。
#[derive(Debug, Clone, Serialize)]
pub struct PayrollRow {
    /// 社員番号 (`SHAIN1.CODE` trim 済み、前ゼロは残す)。
    pub employee_code: String,
    /// 前ゼロ除去済みの突合キー (dtako 側の乗務員 CD と数値同値で突合)。
    pub employee_code_key: String,
    pub employee_name: String,
    pub department: String,
    /// 給与体系コード (`SHOZOKU.TAIKEI`)。
    pub taikei: i32,
    /// 年度 DB 内の支給回インデックス (`KYUYO.[MONTH]`)。
    pub month_index: i32,
    pub pay_date: String,
    pub period_start: String,
    pub period_end: String,
    /// `SHAIN1.TAIKYU != 0` (退職済み)。
    pub retired: bool,
    /// 支給/控除項目名 → 金額 (円)。0 円の項目は含めない。同名項目は合算。
    pub amounts: BTreeMap<String, i64>,
    /// `SHUKEI1` の計算済み合計。該当行が無い場合は null (warning を併記)。
    pub totals: Option<PayrollTotals>,
}

/// `KYUYO` 生行 + 項目マスタ + `SHUKEI1` 集計から給与行を組み立てる。
///
/// - `koumoku`: `TAIKEIKOUNO` (trim 済み) → 項目名 (trim 済み)
/// - 項目名が引けない非ゼロ金額は `MONEY{NN}` キーで返し warning を出す
/// - 同名項目は合算する (給与比較の SalaryCsvRow と同じ規則)
pub fn build_payroll_rows(
    raw: &[RawKyuyoRow],
    koumoku: &HashMap<String, String>,
    shukei: &[RawShukeiRow],
) -> (Vec<PayrollRow>, Vec<String>) {
    let shukei_by_key: HashMap<(i32, i32), &RawShukeiRow> = shukei
        .iter()
        .map(|s| ((s.shain, s.month_index), s))
        .collect();

    let mut warnings: BTreeSet<String> = BTreeSet::new();
    let mut rows: Vec<PayrollRow> = raw
        .iter()
        .map(|r| {
            let mut amounts: BTreeMap<String, i64> = BTreeMap::new();
            for (n, amount) in r.money.iter().enumerate() {
                if *amount == 0 {
                    continue;
                }
                let key = taikeikouno(r.taikei, n);
                let name = match koumoku.get(&key) {
                    Some(name) if !name.is_empty() => name.clone(),
                    _ => {
                        warnings.insert(format!(
                            "項目マスタ未解決: TAIKEIKOUNO={key} (体系{}, MONEY{n:02})",
                            r.taikei
                        ));
                        format!("MONEY{n:02}")
                    }
                };
                *amounts.entry(name).or_insert(0) += amount;
            }

            let totals = match shukei_by_key.get(&(r.shain, r.month_index)) {
                Some(s) => {
                    let deduction_total = s.hoken + s.zei + s.shokoujo;
                    Some(PayrollTotals {
                        soshikyu: s.soshikyu,
                        kazei: s.kazei,
                        hoken: s.hoken,
                        zei: s.zei,
                        shokoujo: s.shokoujo,
                        deduction_total,
                        net_pay: s.soshikyu - deduction_total,
                    })
                }
                None => {
                    warnings.insert(format!(
                        "SHUKEI1 に SHAIN={} MONTH={} の集計行がありません",
                        r.shain, r.month_index
                    ));
                    None
                }
            };

            PayrollRow {
                employee_code: r.employee_code.clone(),
                employee_code_key: employee_code_key(&r.employee_code),
                employee_name: r.employee_name.clone(),
                department: r.department.clone(),
                taikei: r.taikei,
                month_index: r.month_index,
                pay_date: r.pay_date.clone(),
                period_start: r.period_start.clone(),
                period_end: r.period_end.clone(),
                retired: r.taikyu != 0,
                amounts,
                totals,
            }
        })
        .collect();

    // 社員番号の数値順 (突合キー) → 同値なら原文順で安定ソート
    rows.sort_by(|a, b| {
        let an = a.employee_code_key.parse::<u64>().ok();
        let bn = b.employee_code_key.parse::<u64>().ok();
        an.cmp(&bn)
            .then_with(|| a.employee_code.cmp(&b.employee_code))
            .then_with(|| a.month_index.cmp(&b.month_index))
    });

    (rows, warnings.into_iter().collect())
}

// ══════════════════════════════════════════════════════════════
// companies (DB 一覧 → 会社×年度の整理)
// ══════════════════════════════════════════════════════════════

/// 会社 1 社ぶんの年度 DB サマリ。
#[derive(Debug, Clone, Serialize)]
pub struct CompanyInfo {
    /// 会社コード 4 桁 (例 "0100")。
    pub company: String,
    /// `KYCOMSTD.SELDATA` 由来の会社名 (取れなければ空文字)。
    pub name: String,
    /// アクセス可能な年度 (西暦、昇順)。年度 DB の主対象年 = 1900 + 年度3桁。
    pub years: Vec<i32>,
}

/// `sys.databases` の (DB 名, HAS_DBACCESS) 一覧を会社別サマリに整理する。
///
/// - 対象は [`ALLOWED_COMPANIES`] のみ (廃業済み 0500/0900 は除外)
/// - `HAS_DBACCESS != 1` の DB はアクセス不可 (restore 由来の権限抜け等) として
///   years に含めず warning を出す — 500 にしない (#82 受け入れ条件)
pub fn build_companies(
    databases: &[(String, Option<i32>)],
    names: &HashMap<String, String>,
) -> (Vec<CompanyInfo>, Vec<String>) {
    let mut warnings: Vec<String> = Vec::new();
    let mut by_company: BTreeMap<String, Vec<i32>> = BTreeMap::new();

    for (db_name, has_access) in databases {
        let Some((company, nendo)) = parse_kydata_db_name(db_name) else {
            continue;
        };
        if !ALLOWED_COMPANIES.contains(&company.as_str()) {
            continue;
        }
        if *has_access != Some(1) {
            warnings.push(format!(
                "{db_name} にアクセスできません (データ復旧で作られた DB は権限の再付与が必要です)"
            ));
            continue;
        }
        by_company.entry(company).or_default().push(1900 + nendo);
    }

    let companies = by_company
        .into_iter()
        .map(|(company, mut years)| {
            years.sort_unstable();
            CompanyInfo {
                name: names.get(&company).cloned().unwrap_or_default(),
                company,
                years,
            }
        })
        .collect();

    (companies, warnings)
}
