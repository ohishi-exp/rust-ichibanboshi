//! 給与大臣 (OHKEN) 読み取りの純粋ロジック (Refs #82)。
//!
//! スキーマの根拠は `docs/kyuyo-daijin-schema.md` (#81 実機調査):
//! - DB 命名 `KYDATA{会社4桁}_{年度3桁}C`、年度起点は 12 月分 (12 月給与は翌年度 DB)
//! - `KYUYO` 1 行 = 社員 (`SHAIN`) × 支給回インデックス (`MONTH`)。月の特定は
//!   固定式でなく `CHINGINKIKANST` (賃金期間開始) の範囲照合で行う (月内複数支給や
//!   欠月でインデックスがずれるため)
//! - 支給/控除項目: `MONEY00..79` ↔ `KOUMOKU.TAIKEIKOUNO = 体系(2桁) + (18+列番号)(3桁)`
//! - 支給合計等は `SHUKEI1` の計算済み列 (`SOSHIKYU{NN}` 等、NN = MONTH) を使う

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

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

/// `KYCOMSTD.SELDATA.KCODE` を DB 名由来の会社コード (4 桁ゼロ埋め、[`parse_kydata_db_name`]
/// が返す表記) に正規化する。KCODE が数値列でゼロ埋めが失われている場合に突合キーを
/// 一致させるための防御 (#86)。数字以外や 4 桁超はそのまま (trim のみ) 返す。
pub fn normalize_company_code(code: &str) -> String {
    let trimmed = code.trim();
    if trimmed.len() < 4 && !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit()) {
        format!("{trimmed:0>4}")
    } else {
        trimmed.to_string()
    }
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

/// `KOUMOKU` (支給/控除項目マスタ) の 1 行。
///
/// **支給/控除は `KAZEI` (課税区分) で分ける** — `KUBUN` (1〜5) は支給項目と控除項目が
/// 同じ値に混在しており機械判定できないことを実データで確認した (#93、#81 の懸念どおり)。
/// `GENGAKU` は**バケツ分けには使わない**が、符号には使う (下記)。
#[derive(Debug, Clone)]
pub struct RawKoumokuRow {
    /// 「体系(2桁) + 項目番号(3桁)」の合成キー (trim 済み)。
    pub taikeikouno: String,
    /// 項目名 (trim 済み)。
    pub name: String,
    /// 課税区分。1/2 = 支給 (2 は通勤手当の非課税枠)、0 = 控除。
    pub kazei: i32,
    /// 1 = 明細に出る単価項目 (残業単価・基本単価 等)。支給でも控除でもない。
    pub meisai: i32,
    /// 1 = 減額項目 (「時間修正控除」「遅早控除」等)。支給側に居ても**マイナス**で効く。
    ///
    /// `MONEY` には絶対値が正で入っているが `SHUKEI1.SOSHIKYU` は差し引き済みなので、
    /// そのまま足すと差額の 2 倍ぶん過大になる (0200社 2026-06 の 8 名で実測、Refs #87)。
    /// 控除側へ移すのは誤り — `deduction_total` は `HOKEN+ZEI+SHOKOUJO` が正で、
    /// 減額項目はそこに含まれない。
    pub gengaku: i32,
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
    /// **支給**項目名 → 金額 (円)。`KOUMOKU.KAZEI IN (1,2)`。0 円は含めない。同名は合算。
    /// この合計が `totals.soshikyu` と一致する (#93 で 0100 社 52 名で実証)。
    pub payments: BTreeMap<String, i64>,
    /// **控除**項目名 → 金額 (円)。`KOUMOKU.KAZEI = 0` かつ単価項目でないもの。
    pub deductions: BTreeMap<String, i64>,
    /// 基本単価 (日額、円)。`MEISAI=1` の「基本単価」項目。DB は ×100 の固定小数。
    pub base_rate: Option<f64>,
    /// 残業単価 (時給、円)。`MEISAI=1` の「残業単価」項目。
    pub overtime_rate: Option<f64>,
    /// `SHUKEI1` の計算済み合計。該当行が無い場合は null (warning を併記)。
    pub totals: Option<PayrollTotals>,
}

/// 単価項目 (`MEISAI=1`) の DB 値は ×100 の固定小数 (`1318.00` 円が `131800`)。
const RATE_SCALE: f64 = 100.0;

/// `KYUYO` 生行 + 項目マスタ + `SHUKEI1` 集計から給与行を組み立てる。
///
/// - `koumoku`: `TAIKEIKOUNO` (trim 済み) → 項目マスタ行
/// - **支給/控除は `KAZEI` で分ける** (`1,2` = 支給 / `0` = 控除)。`KUBUN` は両者が
///   混在しており使えない (#93 実証)。項目別の区分が要るのは、消費側の給与比較が
///   項目ごとに 5 区分 (割増基礎 × 最低賃金) を割り当てるため — 合計値だけでは足りない
/// - **`MEISAI=1` は単価項目**で支給でも控除でもない。基本単価/残業単価として抽出し、
///   `payments`/`deductions` のどちらにも入れない (入れると合計が合わなくなる)
/// - **`GENGAKU=1` は減額項目**。支給側に居ても符号を反転して足す (`SOSHIKYU` が
///   差し引き済みのため。控除側へ移すのは誤り — Refs [`RawKoumokuRow::gengaku`])
/// - 項目名が引けない非ゼロ金額は `MONEY{NN}` キーで**支給側**に入れ warning を出す
///   (区分不明を控除に倒すと支給合計が過少になり、突合で気付けなくなるため)
/// - 同名項目は合算する (給与比較の SalaryCsvRow と同じ規則)
/// - 最後に `payments` 合計と `SHUKEI1.SOSHIKYU` を突き合わせ、ズレたら warning を出す
pub fn build_payroll_rows(
    raw: &[RawKyuyoRow],
    koumoku: &HashMap<String, RawKoumokuRow>,
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
            let mut payments: BTreeMap<String, i64> = BTreeMap::new();
            let mut deductions: BTreeMap<String, i64> = BTreeMap::new();
            let mut base_rate: Option<f64> = None;
            let mut overtime_rate: Option<f64> = None;
            for (n, amount) in r.money.iter().enumerate() {
                if *amount == 0 {
                    continue;
                }
                let key = taikeikouno(r.taikei, n);
                let Some(item) = koumoku.get(&key).filter(|i| !i.name.is_empty()) else {
                    warnings.insert(format!(
                        "項目マスタ未解決: TAIKEIKOUNO={key} (体系{}, MONEY{n:02})",
                        r.taikei
                    ));
                    *payments.entry(format!("MONEY{n:02}")).or_insert(0) += amount;
                    continue;
                };
                if item.meisai == 1 {
                    // 単価項目。名前で基本単価/残業単価だけを拾う (通勤単価・休日単価は
                    // 消費側が使わないので落とす — 必要になったら足す)
                    let rate = *amount as f64 / RATE_SCALE;
                    match item.name.as_str() {
                        "基本単価" => base_rate = Some(rate),
                        "残業単価" => overtime_rate = Some(rate),
                        _ => {}
                    }
                    continue;
                }
                let bucket = if item.kazei == 0 {
                    &mut deductions
                } else {
                    &mut payments
                };
                // 支給側の減額項目 (GENGAKU=1) は符号を反転する。SOSHIKYU は既に
                // 差し引き済みなので、正のまま足すと差額の 2 倍ぶん過大になる
                let signed = if item.gengaku == 1 && item.kazei != 0 {
                    -amount
                } else {
                    *amount
                };
                *bucket.entry(item.name.clone()).or_insert(0) += signed;
            }

            let totals = match shukei_by_key.get(&(r.shain, r.month_index)) {
                Some(s) => {
                    // 支給合計の自己突合 (Refs #87)。項目別の区分 (KAZEI/MEISAI) が
                    // 壊れても payments が静かに欠けるだけで応答は 200 のまま返るため、
                    // SHUKEI1 の集計値と突き合わせて食い違いを warning に出す
                    let payments_total: i64 = payments.values().sum();
                    if payments_total != s.soshikyu {
                        warnings.insert(format!(
                            "支給合計が SHUKEI1 と不一致: SHAIN={} MONTH={} payments={payments_total} SOSHIKYU={}",
                            r.shain, r.month_index, s.soshikyu
                        ));
                    }
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
                payments,
                deductions,
                base_rate,
                overtime_rate,
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
// employees (社員の識別情報のみ — 金額を持たない)
// ══════════════════════════════════════════════════════════════

/// 社員マスタ (`SHAIN1` × `SHOZOKU`) の生行。
///
/// **給与明細 (`KYUYO`) は読まない** — 所属は `SHAIN1.SHOZOKU` が持っており
/// (docs/kyuyo-daijin-schema.md の社員マスタ節)、社員一覧を得るのに支給実績を
/// 経由する必要が無い。金額列 (`MONEY00..79`) は SELECT にも現れない。
#[derive(Debug, Clone)]
pub struct RawEmployeeRow {
    /// `SHAIN1.CODE` (社員番号、trim 済み)。
    pub employee_code: String,
    /// `SHAIN1.NAME` (氏名、trim 済み)。
    pub employee_name: String,
    /// `SHAIN1.TAIKYU` (0=在籍中)。
    pub taikyu: i32,
    /// `SHOZOKU.SNAME` (所属表示名、trim 済み)。
    pub department: String,
    /// `SHOZOKU.INCODE` (所属コード)。並べ替えの基準に使う (給与大臣が持つ所属の順序)。
    pub department_code: i32,
    /// `SHOZOKU.NAME1` (営業所名、trim 済み)。`SNAME` から切り出す必要が無くなる。
    pub branch_name: String,
    /// `SHOZOKU.NAME2` (職種名、trim 済み)。
    pub job_name: String,
    /// `SHOZOKU.TAIKEI` (給与体系コード)。
    pub taikei: i32,
}

/// 社員 1 名の識別情報 (金額なし)。
///
/// 消費者は ohishi-exp/nuxt-dtako-admin の社員マスタ (D1、Refs #367) —
/// 給与明細 CSV をブラウザに貼らずに社員マスタを作るための供給元。
/// **支給額・控除額は一切含めない**(「金額はブラウザから出さない」方針)。
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct EmployeeRow {
    /// 社員番号 (`SHAIN1.CODE` trim 済み、前ゼロは残す)。
    pub employee_code: String,
    /// 前ゼロ除去済みの突合キー。
    pub employee_code_key: String,
    pub employee_name: String,
    /// 所属 (`SHOZOKU.SNAME`)。
    pub department: String,
    /// 所属コード (`SHOZOKU.INCODE`)。**並べ替えの基準**に使う — 表示名の文字順では
    /// なく給与大臣が持つ所属の順序で並べたいため (Refs nuxt-dtako-admin#409)。
    pub department_code: i32,
    /// 営業所名 (`SHOZOKU.NAME1`)。拠点はこれをそのまま使えるので、表示名からの
    /// 切り出し (全角スペース揺れの正規化・前方一致) が要らなくなる。
    pub branch_name: String,
    /// 職種名 (`SHOZOKU.NAME2`)。
    pub job_name: String,
    /// 給与体系コード (`SHOZOKU.TAIKEI`)。
    pub taikei: i32,
    /// `SHAIN1.TAIKYU != 0` (退職済み)。
    pub retired: bool,
}

/// 社員マスタの生行を応答形に整える。
///
/// `SHAIN1` は 1 社員 1 行なので畳み込みは不要 — 突合キーの生成と並べ替えだけ。
/// **同じ社員番号が複数行あった場合は先勝ち**で 1 件に落とす (再利用された
/// 社員番号や DEL フラグの運用揺れで重複しても、消費側の社員マスタが
/// (会社, 給与コード) を主キーにしているため 2 件返しても意味が無い)。
/// 並びは社員番号の数値順 → 原文順 ([`build_payroll_rows`] と同じ)。
/// 退職者 (`TAIKYU != 0`) も `retired: true` で含める — 過去月の突合に要る。
pub fn build_employee_rows(raw: &[RawEmployeeRow]) -> Vec<EmployeeRow> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut rows: Vec<EmployeeRow> = Vec::new();
    for r in raw {
        let key = employee_code_key(&r.employee_code);
        if !seen.insert(key.clone()) {
            continue;
        }
        rows.push(EmployeeRow {
            employee_code: r.employee_code.clone(),
            employee_code_key: key,
            employee_name: r.employee_name.clone(),
            department: r.department.clone(),
            department_code: r.department_code,
            branch_name: r.branch_name.clone(),
            job_name: r.job_name.clone(),
            taikei: r.taikei,
            retired: r.taikyu != 0,
        });
    }

    rows.sort_by(|a, b| {
        let an = a.employee_code_key.parse::<u64>().ok();
        let bn = b.employee_code_key.parse::<u64>().ok();
        an.cmp(&bn)
            .then_with(|| a.employee_code.cmp(&b.employee_code))
    });
    rows
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
