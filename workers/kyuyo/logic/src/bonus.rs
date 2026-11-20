//! 賞与 (`SHOYO`) の読み取りの純粋ロジック (Refs #414)。
//!
//! 根拠は `GET /kyuyo/schema` で本番の `KYDATA0100_125C` を読んだ結果 (件数とメタデータだけ):
//! - 賞与は `SHOYO` に入る。1 行 = 社員 (`SHAIN`) × 支給回 (`KAI`)。支給日は `SHIKYUBI`
//!   (8 月・12 月の 2 回。どちらも支給した暦年の年度 DB に入る)
//! - 支給/控除項目は `MONEY00..49` で、`MONEY{n}` が項目番号 `098 + n` に対応する
//!   (`KOUMOKU` の `01098` = 賞与、`01100` = 就労調整金、`01108..` = 社会保険・所得税)。
//!   月例給与 (`KYUYO.MONEY00..79` = 018〜097) と同じ規則 ([`split_money`]) で支給/控除に分ける
//! - 合計は `SHUKEI1` の支給回インデックス (本番では 13 / 14) にある。インデックスと `KAI` の対応は
//!   決め打ちせず、**社員 × 支給日** (`SHUKEI1.SHIKYUBI{NN}` = `SHOYO.SHIKYUBI`) で突き合わせる

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::payroll::{employee_code_key, split_money, MoneySplit, PayrollTotals, RawKoumokuRow};

/// `SHOYO.MONEY00..49` の列数。
pub const BONUS_MONEY_COLUMNS: usize = 50;

/// `SHOYO.MONEY00` の項目番号 (098)。
pub const BONUS_FIRST_ITEM: usize = 98;

/// `SHOYO` + `SHAIN1` + `SHOZOKU` を JOIN した 1 行。
#[derive(Debug, Clone)]
pub struct RawBonusRow {
    /// `SHOYO.SHAIN` (社員内部コード)。
    pub shain: i32,
    /// `SHOYO.KAI` (支給回)。
    pub kai: i32,
    /// `SHOYO.SHIKYUBI` (支給日、"YYYY-MM-DD")。
    pub pay_date: String,
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
    /// `MONEY00..49` (円、[`BONUS_MONEY_COLUMNS`] 個)。
    pub money: Vec<i64>,
}

/// `SHUKEI1` の 1 社員 × 1 支給回の集計と、その回の支給日。
#[derive(Debug, Clone)]
pub struct RawDatedShukeiRow {
    pub shain: i32,
    /// 支給回インデックス (`NN`)。
    pub month_index: i32,
    /// `SHIKYUBI{NN}` ("YYYY-MM-DD"、空なら未使用の回)。
    pub pay_date: String,
    pub soshikyu: i64,
    pub kazei: i64,
    pub hoken: i64,
    pub zei: i64,
    pub shokoujo: i64,
}

/// 社員 × 賞与の支給回の 1 行。キーは月例給与の `PayrollRow` に揃える。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BonusRow {
    /// 社員番号 (`SHAIN1.CODE` trim 済み、前ゼロは残す)。
    pub employee_code: String,
    /// 前ゼロ除去済みの突合キー (dtako 側の乗務員 CD と数値同値で突合)。
    pub employee_code_key: String,
    pub employee_name: String,
    pub department: String,
    /// 給与体系コード (`SHOZOKU.TAIKEI`)。
    pub taikei: i32,
    /// `SHOYO.KAI` (支給回)。
    pub kai: i32,
    /// 突き合わせた `SHUKEI1` の支給回インデックス。合わなければ null。
    pub month_index: Option<i32>,
    pub pay_date: String,
    /// `SHAIN1.TAIKYU != 0` (退職済み)。
    pub retired: bool,
    /// **支給**項目名 → 金額 (円)。`KOUMOKU.KAZEI IN (1,2)`。0 円は含めない。同名は合算。
    pub payments: std::collections::BTreeMap<String, i64>,
    /// **控除**項目名 → 金額 (円)。`KOUMOKU.KAZEI = 0` かつ単価項目でないもの。
    pub deductions: std::collections::BTreeMap<String, i64>,
    /// `SHUKEI1` の計算済み合計。突き合わせられなければ null (warning を併記)。
    pub totals: Option<PayrollTotals>,
}

/// 賞与の生行 + 項目マスタ + `SHUKEI1` (支給日つき) から賞与の行を組み立てる。
///
/// 支給/控除の分け方は月例給与と同じ (`KAZEI`・`MEISAI`・`GENGAKU`、[`split_money`])。単価項目は捨てる。
/// `SHUKEI1` とは社員 × 支給日で突き合わせ、`payments` 合計と `SOSHIKYU` が食い違えば warning を出す。
/// 並びは支給日 → 社員番号の数値順。
pub fn build_bonus_rows(
    raw: &[RawBonusRow],
    koumoku: &HashMap<String, RawKoumokuRow>,
    shukei: &[RawDatedShukeiRow],
) -> (Vec<BonusRow>, Vec<String>) {
    let mut by_key: HashMap<(i32, &str), Vec<&RawDatedShukeiRow>> = HashMap::new();
    for s in shukei.iter().filter(|s| !s.pay_date.is_empty()) {
        by_key.entry((s.shain, &s.pay_date)).or_default().push(s);
    }

    let mut warnings: BTreeSet<String> = BTreeSet::new();
    let mut rows: Vec<BonusRow> = raw
        .iter()
        .map(|r| {
            let MoneySplit {
                payments,
                deductions,
                ..
            } = split_money(r.taikei, &r.money, BONUS_FIRST_ITEM, koumoku, &mut warnings);
            let (shain, date) = (r.shain, r.pay_date.as_str());
            let (month_index, totals) = match by_key.get(&(shain, date)).map(Vec::as_slice) {
                Some([s]) => {
                    let paid: i64 = payments.values().sum();
                    if paid != s.soshikyu {
                        let soshikyu = s.soshikyu;
                        let msg = format!("賞与の支給合計が SHUKEI1 と不一致: SHAIN={shain} 支給日={date} payments={paid} SOSHIKYU={soshikyu}");
                        warnings.insert(msg);
                    }
                    let t = PayrollTotals::from_shukei(s.soshikyu, s.kazei, s.hoken, s.zei, s.shokoujo);
                    (Some(s.month_index), Some(t))
                }
                Some(_) => {
                    let msg = format!("SHUKEI1 に SHAIN={shain} 支給日={date} の回が複数あります");
                    warnings.insert(msg);
                    (None, None)
                }
                None => {
                    let msg = format!("SHUKEI1 に SHAIN={shain} 支給日={date} の集計がありません");
                    warnings.insert(msg);
                    (None, None)
                }
            };
            BonusRow {
                employee_code: r.employee_code.clone(),
                employee_code_key: employee_code_key(&r.employee_code),
                employee_name: r.employee_name.clone(),
                department: r.department.clone(),
                taikei: r.taikei,
                kai: r.kai,
                month_index,
                pay_date: r.pay_date.clone(),
                retired: r.taikyu != 0,
                payments,
                deductions,
                totals,
            }
        })
        .collect();

    rows.sort_by(|a, b| {
        let an = a.employee_code_key.parse::<u64>().ok();
        let bn = b.employee_code_key.parse::<u64>().ok();
        a.pay_date
            .cmp(&b.pay_date)
            .then_with(|| an.cmp(&bn))
            .then_with(|| a.employee_code.cmp(&b.employee_code))
    });

    (rows, warnings.into_iter().collect())
}

/// 暦年 `year` の支給日の半開区間 ("YYYY-01-01", "YYYY+1-01-01")。
pub fn year_period(year: i32) -> (String, String) {
    let from = format!("{year:04}-01-01");
    let to = format!("{:04}-01-01", year + 1);
    (from, to)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(no: &str, name: &str, kazei: i32, meisai: i32, gengaku: i32) -> RawKoumokuRow {
        RawKoumokuRow {
            taikeikouno: no.to_string(),
            name: name.to_string(),
            kazei,
            meisai,
            gengaku,
        }
    }

    /// 仮の項目マスタ (体系 01)。名前は本番と同じ項目名、金額はダミー。
    fn koumoku() -> HashMap<String, RawKoumokuRow> {
        [
            item("01098", "賞与", 1, 0, 0),
            item("01100", "就労調整金", 1, 0, 0),
            item("01101", "減額", 1, 0, 1),
            item("01102", "単価", 1, 1, 0),
            item("01108", "健康保険", 0, 0, 0),
            item("01113", "所得税", 0, 0, 0),
        ]
        .into_iter()
        .map(|r| (r.taikeikouno.clone(), r))
        .collect()
    }

    fn raw(shain: i32, code: &str, pay_date: &str, money: &[(usize, i64)]) -> RawBonusRow {
        let mut m = vec![0; BONUS_MONEY_COLUMNS];
        for (n, v) in money {
            m[*n] = *v;
        }
        RawBonusRow {
            shain,
            kai: 1,
            pay_date: pay_date.to_string(),
            employee_code: code.to_string(),
            employee_name: "試験 太郎".to_string(),
            taikyu: 0,
            department: "本社 乗務員".to_string(),
            taikei: 1,
            money: m,
        }
    }

    fn shukei(shain: i32, idx: i32, pay_date: &str, soshikyu: i64) -> RawDatedShukeiRow {
        RawDatedShukeiRow {
            shain,
            month_index: idx,
            pay_date: pay_date.to_string(),
            soshikyu,
            kazei: soshikyu,
            hoken: 300,
            zei: 200,
            shokoujo: 0,
        }
    }

    #[test]
    fn splits_items_and_matches_shukei_by_date() {
        // MONEY00 = 098 賞与 / MONEY02 = 100 就労調整金 / MONEY03 = 101 減額 / MONEY04 = 102 単価 /
        // MONEY10 = 108 健康保険 / MONEY15 = 113 所得税
        let money = [
            (0, 10_000),
            (2, 1_500),
            (3, 500),
            (4, 999),
            (10, 300),
            (15, 200),
        ];
        let raw = vec![raw(7, "0012", "2025-08-12", &money)];
        let shukei = vec![
            shukei(7, 13, "2025-08-12", 11_000),
            shukei(7, 14, "2025-12-29", 1),
            shukei(7, 15, "", 0),
        ];
        let (rows, warnings) = build_bonus_rows(&raw, &koumoku(), &shukei);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(rows.len(), 1);
        let r = &rows[0];
        assert_eq!(r.employee_code, "0012");
        assert_eq!(r.employee_code_key, "12");
        assert_eq!((r.kai, r.month_index), (1, Some(13)));
        assert_eq!(r.pay_date, "2025-08-12");
        assert!(!r.retired);
        let want: Vec<(&str, i64)> = vec![("就労調整金", 1_500), ("減額", -500), ("賞与", 10_000)];
        let got: Vec<(&str, i64)> = r.payments.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        assert_eq!(got, want);
        let got: Vec<(&str, i64)> = r.deductions.iter().map(|(k, v)| (k.as_str(), *v)).collect();
        assert_eq!(got, vec![("健康保険", 300), ("所得税", 200)]);
        assert_eq!(
            r.totals,
            Some(PayrollTotals::from_shukei(11_000, 11_000, 300, 200, 0))
        );
        let t = r.totals.as_ref().unwrap();
        assert_eq!((t.deduction_total, t.net_pay), (500, 10_500));
    }

    #[test]
    fn warns_on_mismatch_missing_and_ambiguous() {
        let raw = vec![
            raw(1, "0003", "2025-12-29", &[(0, 100)]),
            raw(2, "0002", "2025-08-12", &[(0, 100)]),
            raw(3, "0001", "2025-08-12", &[(0, 100), (49, 5)]),
            raw(4, "X", "2025-08-12", &[]),
        ];
        let mut retired = raw.clone();
        retired[0].taikyu = 1;
        let shukei = vec![
            shukei(1, 14, "2025-12-29", 90),
            shukei(3, 13, "2025-08-12", 105),
            shukei(4, 13, "2025-08-12", 0),
            shukei(4, 14, "2025-08-12", 0),
        ];
        let (rows, warnings) = build_bonus_rows(&retired, &koumoku(), &shukei);
        // 支給日 → 社員番号の数値順 (数値でない番号は前)
        let order: Vec<&str> = rows.iter().map(|r| r.employee_code.as_str()).collect();
        assert_eq!(order, vec!["X", "0001", "0002", "0003"]);
        assert!(rows[3].retired);
        assert_eq!(rows[3].totals.as_ref().unwrap().soshikyu, 90);
        // 未解決の項目は MONEY{nn} で支給側へ
        assert_eq!(rows[1].payments.get("MONEY49"), Some(&5));
        assert_eq!(rows[1].month_index, Some(13));
        assert_eq!(
            (rows[0].month_index, rows[0].totals.is_none()),
            (None, true)
        );
        assert_eq!(
            (rows[2].month_index, rows[2].totals.is_none()),
            (None, true)
        );
        assert_eq!(
            warnings,
            vec![
                "SHUKEI1 に SHAIN=2 支給日=2025-08-12 の集計がありません".to_string(),
                "SHUKEI1 に SHAIN=4 支給日=2025-08-12 の回が複数あります".to_string(),
                "賞与の支給合計が SHUKEI1 と不一致: SHAIN=1 支給日=2025-12-29 payments=100 SOSHIKYU=90".to_string(),
                "項目マスタ未解決: TAIKEIKOUNO=01147 (体系1, MONEY49)".to_string(),
            ]
        );
    }

    #[test]
    fn same_code_sorts_by_original_text() {
        let raw = vec![
            raw(1, "012", "2025-08-12", &[]),
            raw(2, "0012", "2025-08-12", &[]),
        ];
        let (rows, _) = build_bonus_rows(&raw, &koumoku(), &[]);
        let order: Vec<&str> = rows.iter().map(|r| r.employee_code.as_str()).collect();
        assert_eq!(order, vec!["0012", "012"]);
    }

    #[test]
    fn row_json_round_trips() {
        let (rows, _) = build_bonus_rows(&[raw(1, "1", "2025-08-12", &[])], &koumoku(), &[]);
        let json = serde_json::to_string(&rows[0]).unwrap();
        let back: BonusRow = serde_json::from_str(&json).unwrap();
        assert_eq!(back.employee_code, "1");
        let want = concat!(
            r#"{"employee_code":"1","employee_code_key":"1","employee_name":"試験 太郎","#,
            r#""department":"本社 乗務員","taikei":1,"kai":1,"month_index":null,"#,
            r#""pay_date":"2025-08-12","retired":false,"payments":{},"deductions":{},"totals":null}"#
        );
        assert_eq!(json, want);
        assert!(format!("{back:?}").contains("BonusRow"));
    }

    #[test]
    fn period_is_the_calendar_year() {
        let want = ("2025-01-01".to_string(), "2026-01-01".to_string());
        assert_eq!(year_period(2025), want);
    }
}
