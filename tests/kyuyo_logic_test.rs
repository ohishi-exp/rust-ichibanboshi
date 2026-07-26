//! kyuyo::logic 純粋関数のテスト (Refs #82)。
//! スキーマ根拠は docs/kyuyo-daijin-schema.md (#81 実機調査)。

use std::collections::HashMap;

use rust_ichibanboshi::kyuyo::logic::{
    build_companies, build_employee_rows, build_payroll_rows, email_allowed, employee_code_key,
    kintai_taikeikouno, kydata_db_name, month_period, nendo_for_month, normalize_company_code,
    normalize_emails, normalize_hire_date, normalize_retire_date, parse_kydata_db_name,
    parse_month, taikeikouno, RawEmployeeRow, RawKoumokuRow, RawKyuyoRow, RawShukeiRow,
    ALLOWED_COMPANIES, KINDATA_COLUMNS, MONEY_COLUMNS,
};

// ══════════════════════════════════════════════════════════════
// 月・年度・DB 名
// ══════════════════════════════════════════════════════════════

#[test]
fn test_parse_month() {
    assert_eq!(parse_month("2026-06"), Some((2026, 6)));
    assert_eq!(parse_month("2025-12"), Some((2025, 12)));
    assert_eq!(parse_month("1990-01"), Some((1990, 1)));
    // 不正形式
    assert_eq!(parse_month(""), None);
    assert_eq!(parse_month("2026"), None);
    assert_eq!(parse_month("2026-6"), None); // 月は 2 桁必須
    assert_eq!(parse_month("26-06"), None); // 年は 4 桁必須
    assert_eq!(parse_month("2026-13"), None);
    assert_eq!(parse_month("2026-00"), None);
    assert_eq!(parse_month("1989-06"), None); // 範囲外
    assert_eq!(parse_month("3000-01"), None);
    assert_eq!(parse_month("abcd-ef"), None);
    assert_eq!(parse_month("2026-06-01"), None); // 日付まで付いている
}

#[test]
fn test_nendo_for_month() {
    // #81: _126C の MONTH=0 は 2025年12月分 → 12 月は翌年度 DB
    assert_eq!(nendo_for_month(2026, 6), 126);
    assert_eq!(nendo_for_month(2025, 12), 126);
    assert_eq!(nendo_for_month(2026, 1), 126);
    assert_eq!(nendo_for_month(2026, 11), 126);
    assert_eq!(nendo_for_month(2026, 12), 127);
    assert_eq!(nendo_for_month(2012, 1), 112);
}

#[test]
fn test_kydata_db_name() {
    assert_eq!(kydata_db_name("0100", 126), "KYDATA0100_126C");
    assert_eq!(kydata_db_name("0400", 116), "KYDATA0400_116C");
}

#[test]
fn test_parse_kydata_db_name() {
    assert_eq!(
        parse_kydata_db_name("KYDATA0100_126C"),
        Some(("0100".to_string(), 126))
    );
    assert_eq!(
        parse_kydata_db_name("KYDATA0900_116C"),
        Some(("0900".to_string(), 116))
    );
    // 不正形式
    assert_eq!(parse_kydata_db_name("KYCOMSTD"), None);
    assert_eq!(parse_kydata_db_name("KYDATA100_126C"), None); // 会社 3 桁
    assert_eq!(parse_kydata_db_name("KYDATA0100_126"), None); // C 無し
    assert_eq!(parse_kydata_db_name("KYDATA0100_12C"), None); // 年度 2 桁
    assert_eq!(parse_kydata_db_name("KYDATA01A0_126C"), None); // 会社に非数字
    assert_eq!(parse_kydata_db_name("KYDATA0100126C"), None); // 区切り無し
    assert_eq!(parse_kydata_db_name("OTHER0100_126C"), None);
    assert_eq!(parse_kydata_db_name("KYDATA0100_1a6C"), None); // 年度に非数字
}

#[test]
fn test_normalize_company_code() {
    // #86: KCODE が数値列でゼロ埋めが失われるケースを 4 桁に正規化
    assert_eq!(normalize_company_code("100"), "0100");
    assert_eq!(normalize_company_code(" 100 "), "0100"); // trim してから判定
    assert_eq!(normalize_company_code("1"), "0001");
    // 既に 4 桁ならそのまま
    assert_eq!(normalize_company_code("0100"), "0100");
    // 数字以外・4 桁超・空は trim のみ
    assert_eq!(normalize_company_code("10A"), "10A");
    assert_eq!(normalize_company_code("12345"), "12345");
    assert_eq!(normalize_company_code(""), "");
    assert_eq!(normalize_company_code("   "), "");
}

#[test]
fn test_month_period() {
    assert_eq!(
        month_period(2026, 6),
        ("2026-06-01".to_string(), "2026-07-01".to_string())
    );
    // 12 月は年跨ぎ
    assert_eq!(
        month_period(2025, 12),
        ("2025-12-01".to_string(), "2026-01-01".to_string())
    );
}

// ══════════════════════════════════════════════════════════════
// 項目マッピング・突合キー・allowlist
// ══════════════════════════════════════════════════════════════

#[test]
fn test_taikeikouno() {
    // #81 実データ検証: 体系1 MONEY00 → 01018 (基本給)、MONEY10 → 01028 (家畜運搬手当)
    assert_eq!(taikeikouno(1, 0), "01018");
    assert_eq!(taikeikouno(1, 10), "01028");
    assert_eq!(taikeikouno(2, 79), "02097");
    // clamp (体系は 2 桁に収める)
    assert_eq!(taikeikouno(-1, 0), "00018");
    assert_eq!(taikeikouno(100, 0), "99018");
}

#[test]
fn test_employee_code_key() {
    // #81: CODE は前ゼロ + 末尾スペース埋め (例 "0941    ")
    assert_eq!(employee_code_key("0941    "), "941");
    assert_eq!(employee_code_key("1771"), "1771");
    assert_eq!(employee_code_key("  007 "), "7");
    assert_eq!(employee_code_key("0000"), "0");
    assert_eq!(employee_code_key(""), "");
    assert_eq!(employee_code_key("   "), "");
}

#[test]
fn test_normalize_and_allow_emails() {
    let raw = vec![
        " Keiri@Example.com ".to_string(),
        "".to_string(),
        "  ".to_string(),
        "boss@example.com".to_string(),
    ];
    let allowed = normalize_emails(&raw);
    assert_eq!(allowed, vec!["keiri@example.com", "boss@example.com"]);

    assert!(email_allowed(&allowed, "keiri@example.com"));
    assert!(email_allowed(&allowed, "KEIRI@EXAMPLE.COM "));
    assert!(!email_allowed(&allowed, "other@example.com"));
    assert!(!email_allowed(&allowed, ""));
    assert!(!email_allowed(&[], "keiri@example.com"));
}

// ══════════════════════════════════════════════════════════════
// build_payroll_rows
// ══════════════════════════════════════════════════════════════

fn raw_row(shain: i32, code: &str, taikei: i32, money: &[(usize, i64)]) -> RawKyuyoRow {
    raw_row_with_kindata(shain, code, taikei, money, &[])
}

/// 勤怠 (`KINDATA*`) つきの生行 (Refs #103)。値は**生の ×100 固定小数**で渡す。
fn raw_row_with_kindata(
    shain: i32,
    code: &str,
    taikei: i32,
    money: &[(usize, i64)],
    kindata: &[(usize, i64)],
) -> RawKyuyoRow {
    let mut m = vec![0i64; MONEY_COLUMNS];
    for (idx, v) in money {
        m[*idx] = *v;
    }
    let mut k = vec![0i64; KINDATA_COLUMNS];
    for (idx, v) in kindata {
        k[*idx] = *v;
    }
    RawKyuyoRow {
        shain,
        month_index: 5,
        pay_date: "2026-06-15".to_string(),
        period_start: "2026-05-01".to_string(),
        period_end: "2026-05-31".to_string(),
        employee_code: code.to_string(),
        employee_name: format!("社員{shain}"),
        taikyu: 0,
        department: "本社　乗務員".to_string(),
        taikei,
        money: m,
        kindata: k,
    }
}

/// 項目マスタ 1 件。`kazei` 1/2 = 支給・0 = 控除、`meisai` 1 = 単価項目 (Refs #93)。
fn koumoku_row(key: &str, name: &str, kazei: i32, meisai: i32) -> (String, RawKoumokuRow) {
    koumoku_row_gengaku(key, name, kazei, meisai, 0)
}

/// 減額項目 (`gengaku` = 1) を含めて組み立てる版 (Refs #87)。
fn koumoku_row_gengaku(
    key: &str,
    name: &str,
    kazei: i32,
    meisai: i32,
    gengaku: i32,
) -> (String, RawKoumokuRow) {
    (
        key.to_string(),
        RawKoumokuRow {
            taikeikouno: key.to_string(),
            name: name.to_string(),
            kazei,
            meisai,
            gengaku,
        },
    )
}

fn koumoku_taikei1() -> HashMap<String, RawKoumokuRow> {
    // #81 実データ検証の 4 項目 (体系 1、いずれも支給)
    HashMap::from([
        koumoku_row("01018", "基本給", 1, 0),
        koumoku_row("01022", "住宅手当", 1, 0),
        koumoku_row("01024", "無事故手当", 1, 0),
        koumoku_row("01028", "家畜運搬手当", 1, 0),
    ])
}

#[test]
fn test_build_payroll_rows_maps_items_and_totals() {
    // #81 の実データ検証値 (SHAIN=4, MONTH=5, 2026年6月分)
    let raw = vec![raw_row(
        4,
        "1771    ",
        1,
        &[(0, 83_418), (4, 9_000), (6, 27_000), (10, 52_000)],
    )];
    let shukei = vec![RawShukeiRow {
        shain: 4,
        month_index: 5,
        soshikyu: 404_045,
        kazei: 300_000,
        hoken: 56_398,
        zei: 7_830,
        shokoujo: 30_500,
    }];

    let (rows, warnings) = build_payroll_rows(&raw, &koumoku_taikei1(), &shukei);
    // この fixture は #81 で名寄せ検証した 4 項目だけの部分集合で、実際の行には
    // 残業手当などがまだ載る。よって支給合計の自己突合 (Refs #87) は不一致側に立つ
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("支給合計が SHUKEI1 と不一致"));
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row.employee_code, "1771    ");
    assert_eq!(row.employee_code_key, "1771");
    assert_eq!(row.employee_name, "社員4");
    assert_eq!(row.department, "本社　乗務員");
    assert_eq!(row.taikei, 1);
    assert_eq!(row.month_index, 5);
    assert_eq!(row.pay_date, "2026-06-15");
    assert_eq!(row.period_start, "2026-05-01");
    assert_eq!(row.period_end, "2026-05-31");
    assert!(!row.retired);

    assert_eq!(row.payments.len(), 4);
    assert_eq!(row.payments["基本給"], 83_418);
    assert_eq!(row.payments["住宅手当"], 9_000);
    assert_eq!(row.payments["無事故手当"], 27_000);
    assert_eq!(row.payments["家畜運搬手当"], 52_000);
    assert!(row.deductions.is_empty());

    // #81 の恒等式: 控除合計 94,728 / 差引 309,317
    let totals = row.totals.as_ref().expect("totals");
    assert_eq!(totals.soshikyu, 404_045);
    assert_eq!(totals.deduction_total, 94_728);
    assert_eq!(totals.net_pay, 309_317);
    assert_eq!(totals.kazei, 300_000);
    assert_eq!(totals.hoken, 56_398);
    assert_eq!(totals.zei, 7_830);
    assert_eq!(totals.shokoujo, 30_500);
}

#[test]
fn test_build_payroll_rows_merges_same_item_name() {
    // 同名項目は合算 (SalaryCsvRow と同じ規則)。体系 1 の 01018/01022 を同名にする
    let koumoku = HashMap::from([
        koumoku_row("01018", "調整手当", 1, 0),
        koumoku_row("01022", "調整手当", 1, 0),
    ]);
    let raw = vec![raw_row(4, "0941  ", 1, &[(0, 1_000), (4, 234)])];
    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].payments.len(), 1);
    assert_eq!(rows[0].payments["調整手当"], 1_234);
    // SHUKEI1 欠落 warning も同時に立つ
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("SHUKEI1"));
    assert!(rows[0].totals.is_none());
}

#[test]
fn test_build_payroll_rows_unmapped_item_falls_back_with_warning() {
    // 項目マスタに無い列 / 名前が空の列は MONEY{NN} キー + warning
    let koumoku = HashMap::from([koumoku_row("01019", "", 1, 0)]);
    let raw = vec![raw_row(4, "1", 1, &[(1, 500), (7, 300)])];
    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &[]);
    // 区分不明は**支給側**へ入れる (控除に倒すと支給合計が過少になり突合で気付けない)
    assert_eq!(rows[0].payments["MONEY01"], 500);
    assert_eq!(rows[0].payments["MONEY07"], 300);
    assert!(rows[0].deductions.is_empty());
    // 未解決 2 件 + SHUKEI1 欠落 1 件
    assert_eq!(warnings.len(), 3);
    assert!(warnings.iter().any(|w| w.contains("01019")));
    assert!(warnings.iter().any(|w| w.contains("01025")));
}

#[test]
fn test_build_payroll_rows_zero_amounts_excluded_and_retired_flag() {
    let mut raw = raw_row(9, "0002", 1, &[]);
    raw.taikyu = 1;
    let (rows, warnings) = build_payroll_rows(
        &[raw],
        &koumoku_taikei1(),
        &[RawShukeiRow {
            shain: 9,
            month_index: 5,
            soshikyu: 0,
            kazei: 0,
            hoken: 0,
            zei: 0,
            shokoujo: 0,
        }],
    );
    assert!(warnings.is_empty());
    assert!(rows[0].payments.is_empty()); // 全項目 0 円 → 支給も控除も空
    assert!(rows[0].deductions.is_empty());
    assert!(rows[0].retired);
    assert_eq!(rows[0].totals.as_ref().unwrap().net_pay, 0);
}

#[test]
fn test_build_payroll_rows_payments_matching_soshikyu_has_no_warning() {
    // 支給項目が揃っていれば payments 合計 = SOSHIKYU で warning は立たない。
    // 控除 (kazei=0) と単価 (meisai=1) は支給合計に足さないことも同時に確かめる
    let koumoku = HashMap::from([
        koumoku_row("01018", "基本給", 1, 0),
        koumoku_row("01022", "通勤手当", 2, 0),
        koumoku_row("01024", "所得税", 0, 0),
        koumoku_row("01026", "残業単価", 1, 1),
    ]);
    let raw = vec![raw_row(
        4,
        "1771",
        1,
        &[(0, 200_000), (4, 8_000), (6, 7_830), (8, 131_800)],
    )];
    let shukei = vec![RawShukeiRow {
        shain: 4,
        month_index: 5,
        soshikyu: 208_000,
        kazei: 200_000,
        hoken: 0,
        zei: 7_830,
        shokoujo: 0,
    }];

    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &shukei);
    assert!(warnings.is_empty(), "warnings: {warnings:?}");
    assert_eq!(rows[0].payments.values().sum::<i64>(), 208_000);
    assert_eq!(rows[0].deductions["所得税"], 7_830);
    assert_eq!(rows[0].overtime_rate, Some(1_318.0));
}

#[test]
fn test_build_payroll_rows_gengaku_item_is_subtracted_from_payments() {
    // 減額項目 (GENGAKU=1) は MONEY に絶対値が正で入るが SOSHIKYU は差し引き済み。
    // 素直に足すと差額の 2 倍 (ここでは 4,492) ぶん過大になる — 0200社 2026-06 の
    // 8 名で実測した形 (Refs #87)
    let koumoku = HashMap::from([
        koumoku_row("01018", "基本給", 1, 0),
        koumoku_row_gengaku("01022", "時間修正控除", 1, 0, 1),
    ]);
    let raw = vec![raw_row(4, "1771", 1, &[(0, 200_000), (4, 2_246)])];
    let shukei = vec![RawShukeiRow {
        shain: 4,
        month_index: 5,
        soshikyu: 197_754,
        kazei: 197_754,
        hoken: 0,
        zei: 0,
        shokoujo: 0,
    }];

    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &shukei);
    assert!(warnings.is_empty(), "warnings: {warnings:?}");
    assert_eq!(rows[0].payments["時間修正控除"], -2_246);
    assert_eq!(rows[0].payments.values().sum::<i64>(), 197_754);
    // 控除側へ移すのは誤り — deduction_total は HOKEN+ZEI+SHOKOUJO が正
    assert!(rows[0].deductions.is_empty());
    assert_eq!(rows[0].totals.as_ref().unwrap().deduction_total, 0);
}

#[test]
fn test_build_payroll_rows_gengaku_on_deduction_side_keeps_sign() {
    // 控除側 (kazei=0) の GENGAKU は符号を触らない。SOSHIKYU 突合の対象外で、
    // 反転すると控除額が負になるだけ
    let koumoku = HashMap::from([koumoku_row_gengaku("01018", "その他控除", 0, 0, 1)]);
    let raw = vec![raw_row(4, "1771", 1, &[(0, 5_000)])];
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].deductions["その他控除"], 5_000);
}

#[test]
fn test_build_payroll_rows_warns_when_kazei_flags_are_all_zero() {
    // 本番で起きた退行の再現 (Refs #87): KOUMOKU.KAZEI/MEISAI が CAST 漏れで全件 0 に
    // なると、支給項目も単価も deductions に落ち payments が空のまま 200 で返る
    let koumoku = HashMap::from([
        koumoku_row("01018", "基本給", 0, 0),
        koumoku_row("01026", "残業単価", 0, 0),
    ]);
    let raw = vec![raw_row(4, "1771", 1, &[(0, 200_000), (8, 131_800)])];
    let shukei = vec![RawShukeiRow {
        shain: 4,
        month_index: 5,
        soshikyu: 200_000,
        kazei: 200_000,
        hoken: 0,
        zei: 0,
        shokoujo: 0,
    }];

    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &shukei);
    assert!(rows[0].payments.is_empty());
    assert_eq!(rows[0].overtime_rate, None);
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("支給合計が SHUKEI1 と不一致"));
    assert!(warnings[0].contains("payments=0"));
    assert!(warnings[0].contains("SOSHIKYU=200000"));
}

#[test]
fn test_build_payroll_rows_sorted_by_code_key_numeric() {
    let raw = vec![
        raw_row(3, "0100", 1, &[(0, 1)]),
        raw_row(1, "0002", 1, &[(0, 1)]),
        raw_row(2, "0030", 1, &[(0, 1)]),
    ];
    let (rows, _) = build_payroll_rows(&raw, &koumoku_taikei1(), &[]);
    let keys: Vec<&str> = rows.iter().map(|r| r.employee_code_key.as_str()).collect();
    assert_eq!(keys, vec!["2", "30", "100"]);
}

#[test]
fn test_build_payroll_rows_sort_tiebreakers() {
    // タイブレーク: 数値キー同値 → 原文 code 順、同一 code (月内複数支給) → month_index 順
    let mut r1 = raw_row(1, "0100", 1, &[(0, 1)]);
    r1.month_index = 6;
    let r2 = raw_row(1, "0100", 1, &[(0, 2)]); // month_index = 5
    let r3 = raw_row(2, "100", 1, &[(0, 3)]); // 数値キーは同じ "100"、原文が異なる
    let (rows, _) = build_payroll_rows(&[r1, r2, r3], &koumoku_taikei1(), &[]);
    let order: Vec<(String, i32)> = rows
        .iter()
        .map(|r| (r.employee_code.clone(), r.month_index))
        .collect();
    assert_eq!(
        order,
        vec![
            ("0100".to_string(), 5),
            ("0100".to_string(), 6),
            ("100".to_string(), 5),
        ]
    );
}

#[test]
fn test_build_payroll_rows_empty() {
    let (rows, warnings) = build_payroll_rows(&[], &HashMap::new(), &[]);
    assert!(rows.is_empty());
    assert!(warnings.is_empty());
}

// ══════════════════════════════════════════════════════════════
// build_companies
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_companies_groups_and_warns() {
    let databases = vec![
        ("KYDATA0100_125C".to_string(), Some(1)),
        ("KYDATA0100_126C".to_string(), Some(1)),
        // restore 由来の権限抜け → warning、years に含めない
        ("KYDATA0200_126C".to_string(), Some(0)),
        // HAS_DBACCESS NULL も不可扱い
        ("KYDATA0300_126C".to_string(), None),
        // 廃業済み会社は対象外 (warning も出さない)
        ("KYDATA0500_123C".to_string(), Some(1)),
        ("KYDATA0900_116C".to_string(), Some(1)),
        // KYDATA 形式でない DB は無視
        ("KYCOMSTD".to_string(), Some(1)),
    ];
    let names = HashMap::from([("0100".to_string(), "有限会社 大石運輸".to_string())]);

    let (companies, warnings) = build_companies(&databases, &names);

    assert_eq!(companies.len(), 1);
    assert_eq!(companies[0].company, "0100");
    assert_eq!(companies[0].name, "有限会社 大石運輸");
    assert_eq!(companies[0].years, vec![2025, 2026]);

    assert_eq!(warnings.len(), 2);
    assert!(warnings[0].contains("KYDATA0200_126C"));
    assert!(warnings[1].contains("KYDATA0300_126C"));
}

#[test]
fn test_build_companies_empty() {
    let (companies, warnings) = build_companies(&[], &HashMap::new());
    assert!(companies.is_empty());
    assert!(warnings.is_empty());
}

#[test]
fn test_allowed_companies_constant() {
    // #81 で確定した現行 4 社
    assert_eq!(ALLOWED_COMPANIES, ["0100", "0200", "0300", "0400"]);
}

#[test]
fn test_derived_impls_are_exercised() {
    // derive (Debug/Clone/PartialEq) も coverage 対象になるため明示的に実行する
    let raw = raw_row(1, "0001", 1, &[(0, 1)]);
    let shukei = RawShukeiRow {
        shain: 1,
        month_index: 5,
        soshikyu: 1,
        kazei: 0,
        hoken: 0,
        zei: 0,
        shokoujo: 0,
    };
    let (rows, _) = build_payroll_rows(
        std::slice::from_ref(&raw),
        &koumoku_taikei1(),
        std::slice::from_ref(&shukei),
    );
    let totals = rows[0].totals.clone().unwrap();
    assert_eq!(totals, totals.clone()); // PartialEq
    assert!(format!("{raw:?}").contains("RawKyuyoRow"));
    assert!(format!("{shukei:?}").contains("RawShukeiRow"));
    assert!(format!("{:?}", rows[0]).contains("PayrollRow"));
    let (companies, _) =
        build_companies(&[("KYDATA0100_126C".to_string(), Some(1))], &HashMap::new());
    assert!(format!("{:?}", companies[0]).contains("CompanyInfo"));
}

// ══════════════════════════════════════════════════════════════
// build_employee_rows (社員マスタ、Refs ohishi-exp/nuxt-dtako-admin#367)
// ══════════════════════════════════════════════════════════════

fn raw_employee(
    code: &str,
    name: &str,
    dept: &str,
    taikei: i32,
    taikyu: i32,
    kkubun: i32,
) -> RawEmployeeRow {
    // SHOZOKU は営業所名 (NAME1) と職種名 (NAME2) を別に持ち、SNAME はその結合
    let mut parts = dept.split('　');
    RawEmployeeRow {
        employee_code: code.to_string(),
        employee_name: name.to_string(),
        taikyu,
        department: dept.to_string(),
        taikei,
        department_code: 14,
        branch_name: parts.next().unwrap_or("").to_string(),
        job_name: parts.next().unwrap_or("").to_string(),
        kkubun,
        hire_date: "2020-04-01".to_string(),
        // 在籍中は NULL でなくセンチネル `1970-01-02` が入る (実データ)
        retire_date: "1970-01-02".to_string(),
        taikbn: 0,
    }
}

#[test]
fn build_employee_rows_maps_and_sorts_by_numeric_code() {
    let rows = build_employee_rows(&[
        raw_employee("1771", "鈴木　花子", "本社　事務", 2, 0, 1),
        raw_employee("0941", "山田　太郎", "本社　乗務員", 1, 0, 2),
    ]);
    assert_eq!(
        rows.iter()
            .map(|r| r.employee_code.as_str())
            .collect::<Vec<_>>(),
        vec!["0941", "1771"]
    );
    assert_eq!(rows[0].employee_code_key, "941");
    assert_eq!(rows[0].department, "本社　乗務員");
    assert_eq!(rows[0].taikei, 1);
    assert_eq!(rows[0].kkubun, 2);
}

#[test]
fn build_employee_rows_exposes_shozoku_code_and_split_names() {
    // 消費側 (nuxt-dtako-admin#409) は拠点を SNAME から切り出していたが、SHOZOKU は
    // 営業所名と職種名を別に持つ。並べ替えの基準に使う所属コードも合わせて返す
    let rows = build_employee_rows(&[raw_employee("1771", "鈴木　花子", "本社　乗務員", 1, 0, 2)]);
    assert_eq!(rows[0].department_code, 14);
    assert_eq!(rows[0].branch_name, "本社");
    assert_eq!(rows[0].job_name, "乗務員");
    // SNAME は従来どおり結合済みの表示名
    assert_eq!(rows[0].department, "本社　乗務員");
}

#[test]
fn build_employee_rows_exposes_hire_date_and_nulls_the_retire_sentinel() {
    // 在籍日数を出す消費者 (nuxt-dtako-admin#445) 向け。入社日は SHAIN1 ではなく
    // SHAIN2.DAYNYU にある
    let rows = build_employee_rows(&[raw_employee("0941", "山田　太郎", "本社　乗務員", 1, 0, 2)]);
    assert_eq!(rows[0].hire_date.as_deref(), Some("2020-04-01"));
    // 在籍中の DAYTAI は 1970-01-02 (未設定センチネル) なので None に倒す —
    // そのまま返すと消費側が「1970 年に退職した人」として扱ってしまう
    assert_eq!(rows[0].retire_date, None);
    assert_eq!(rows[0].taikbn, 0);
}

#[test]
fn build_employee_rows_keeps_real_retire_date() {
    let mut raw = raw_employee("1771", "鈴木　花子", "本社　事務", 2, 1, 1);
    raw.retire_date = "2026-01-26".to_string();
    raw.taikbn = 2;
    let rows = build_employee_rows(&[raw]);
    assert_eq!(rows[0].retire_date.as_deref(), Some("2026-01-26"));
    assert_eq!(rows[0].taikbn, 2);
    assert!(rows[0].retired);
}

#[test]
fn normalize_retire_date_drops_sentinel_and_blank() {
    assert_eq!(normalize_retire_date("1970-01-02"), None);
    assert_eq!(normalize_retire_date(""), None);
    assert_eq!(normalize_retire_date("   "), None);
    assert_eq!(
        normalize_retire_date(" 2025-12-19 ").as_deref(),
        Some("2025-12-19")
    );
}

#[test]
fn normalize_hire_date_keeps_1970_02_because_it_is_not_a_sentinel_here() {
    // センチネルは**退社日にだけ**ある概念。入社日で同じ値を潰すと、
    // 実在の日付を静かに落とすことになる
    assert_eq!(
        normalize_hire_date("1970-01-02").as_deref(),
        Some("1970-01-02")
    );
    assert_eq!(normalize_hire_date(""), None);
    assert_eq!(normalize_hire_date("  "), None);
    assert_eq!(
        normalize_hire_date(" 2003-06-01 ").as_deref(),
        Some("2003-06-01")
    );
}

#[test]
fn build_employee_rows_flags_retired() {
    let rows = build_employee_rows(&[raw_employee("1", "退職　太郎", "本社", 1, 1, 2)]);
    assert!(rows[0].retired);
}

#[test]
fn build_employee_rows_dedupes_same_code_first_wins() {
    let rows = build_employee_rows(&[
        raw_employee("0007", "先勝ち", "本社", 1, 0, 2),
        raw_employee("7", "後", "支社", 2, 0, 1),
    ]);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].employee_name, "先勝ち");
}

/// 給与区分 (`SHAIN3.KKUBUN`) は所属の体系 (`SHOZOKU.TAIKEI`) とは**独立した軸**で、
/// TAIKEI から推定してはいけない (Refs #101 の実機調査)。
#[test]
fn build_employee_rows_keeps_kkubun_independent_from_taikei() {
    let rows = build_employee_rows(&[
        // 同じ TAIKEI=1 (乗務員) でも月給/日給/時給が混在する
        raw_employee("1", "乗務　月給", "本社　乗務員", 1, 0, 1),
        raw_employee("2", "乗務　日給", "本社　乗務員", 1, 0, 2),
        raw_employee("3", "乗務　時給", "本社　乗務員", 1, 0, 3),
        // 逆に TAIKEI=2 (事務) にも時給者がいる
        raw_employee("4", "事務　時給", "本社　事務", 2, 0, 3),
    ]);
    assert_eq!(
        rows.iter()
            .map(|r| (r.taikei, r.kkubun))
            .collect::<Vec<_>>(),
        vec![(1, 1), (1, 2), (1, 3), (2, 3)]
    );
}

/// `KKUBUN` が NULL / 未設定の社員は 0 のまま返す — 消費側が「不明」として
/// 安全側 (単価なし) に倒せるようにするため、勝手に既定区分へ寄せない。
#[test]
fn build_employee_rows_keeps_unset_kkubun_as_zero() {
    let rows = build_employee_rows(&[raw_employee("1", "区分なし", "本社", 1, 0, 0)]);
    assert_eq!(rows[0].kkubun, 0);
}

#[test]
fn build_employee_rows_empty_input() {
    assert!(build_employee_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// 支給/控除の分離と単価の抽出 (Refs #93)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_payroll_rows_splits_payments_and_deductions_by_kazei() {
    // KUBUN では支給/控除を分けられない (実データで混在) ため KAZEI で判定する。
    // KAZEI 1/2 = 支給 (2 は通勤手当の非課税枠)、0 = 控除。
    let koumoku = HashMap::from([
        koumoku_row("01018", "基本給", 1, 0),
        koumoku_row("01021", "通勤手当", 2, 0),
        koumoku_row("01066", "健康保険料", 0, 0),
        koumoku_row("01070", "所得税", 0, 0),
    ]);
    let raw = vec![raw_row(
        4,
        "1771",
        1,
        &[(0, 83_418), (3, 4_100), (48, 12_000), (52, 3_500)],
    )];
    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &[]);
    let row = &rows[0];

    assert_eq!(row.payments.len(), 2);
    assert_eq!(row.payments["基本給"], 83_418);
    assert_eq!(row.payments["通勤手当"], 4_100); // KAZEI=2 も支給側
    assert_eq!(row.deductions.len(), 2);
    assert_eq!(row.deductions["健康保険料"], 12_000);
    assert_eq!(row.deductions["所得税"], 3_500);
    // SHUKEI1 欠落 warning のみ (項目は全て解決している)
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains("SHUKEI1"));
}

#[test]
fn test_build_payroll_rows_extracts_rates_from_meisai_items() {
    // 単価は MEISAI=1 の項目として MONEY 枠内にある (新規テーブル不要)。
    // DB 値は ×100 の固定小数。実データ: 残業単価 MONEY60 = 131800 → 1318.00 円で、
    // 残業時間 59.00h × 1318.00 = 77,762 円が残業手当 (MONEY24) と一致する。
    let koumoku = HashMap::from([
        koumoku_row("01042", "残業手当", 1, 0),
        koumoku_row("01078", "残業単価", 0, 1),
        koumoku_row("01079", "通勤単価", 0, 1),
        koumoku_row("01080", "基本単価", 0, 1),
    ]);
    let raw = vec![raw_row(
        4,
        "1771",
        1,
        &[(24, 77_762), (60, 131_800), (61, 5_000), (62, 367_900)],
    )];
    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &[]);
    let row = &rows[0];

    assert_eq!(row.overtime_rate, Some(1_318.0));
    assert_eq!(row.base_rate, Some(3_679.0));
    // 単価項目は支給にも控除にも入れない (入れると合計が合わなくなる)。
    // 通勤単価・休日単価は消費側が使わないので落とす
    assert_eq!(row.payments.len(), 1);
    assert_eq!(row.payments["残業手当"], 77_762);
    assert!(row.deductions.is_empty());
    assert_eq!(warnings.len(), 1); // SHUKEI1 欠落のみ

    // 実データの整合: 残業時間 × 残業単価 = 残業手当
    let overtime_pay = (59.0_f64 * row.overtime_rate.unwrap()).round() as i64;
    assert_eq!(overtime_pay, row.payments["残業手当"]);
}

#[test]
fn test_build_payroll_rows_rates_absent_when_no_meisai_items() {
    let (rows, _) = build_payroll_rows(
        &[raw_row(4, "1771", 1, &[(0, 1_000)])],
        &koumoku_taikei1(),
        &[],
    );
    assert_eq!(rows[0].base_rate, None);
    assert_eq!(rows[0].overtime_rate, None);
}

// ══════════════════════════════════════════════════════════════
// 勤怠 (KINDATA*) — Refs #103
// ══════════════════════════════════════════════════════════════

#[test]
fn test_kintai_taikeikouno_maps_to_item_numbers_1_to_17() {
    // 勤怠は項目番号 001〜017 (MONEY の 018〜097 とは別の帯)
    assert_eq!(kintai_taikeikouno(1, 0), "01001");
    assert_eq!(kintai_taikeikouno(1, 16), "01017");
    assert_eq!(kintai_taikeikouno(2, 4), "02005");
    // 体系コードは 2 桁に丸める (MONEY 側と同じ規則)
    assert_eq!(kintai_taikeikouno(0, 0), "00001");
    assert_eq!(kintai_taikeikouno(-5, 0), "00001");
    assert_eq!(kintai_taikeikouno(1234, 0), "99001");
    // MONEY 側と衝突しない (018 未満と 018 以上)
    assert_eq!(taikeikouno(1, 0), "01018");
}

#[test]
fn test_build_payroll_rows_attendance_divides_raw_by_100() {
    // 実データ (0100 社): 残業時間 raw 5900 = 59.00h、有休日数 raw 150 = 1.5 日
    let raw = vec![raw_row_with_kindata(
        1,
        "1771",
        1,
        &[(0, 83_418)],
        &[(0, 2_100), (4, 5_900), (6, 150)],
    )];
    let koumoku = HashMap::from([
        koumoku_row("01018", "基本給", 1, 0),
        koumoku_row("01001", "出勤日数", 0, 0),
        koumoku_row("01005", "残業時間", 0, 0),
        koumoku_row("01007", "有休日数", 0, 0),
    ]);
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    let a = &rows[0].attendance;
    assert_eq!(a.get("出勤日数"), Some(&21.0));
    assert_eq!(a.get("残業時間"), Some(&59.0));
    // 半休 (前休・後休) が 0.5 で表現できることの確認
    assert_eq!(a.get("有休日数"), Some(&1.5));
}

#[test]
fn test_build_payroll_rows_attendance_scales_counts_too() {
    // **回数系も ×100**。遅刻回数 raw 800 は 8 回 — 割らないと 800 回になる
    let raw = vec![raw_row_with_kindata(
        1,
        "1771",
        2,
        &[],
        &[(8, 800), (9, 100)],
    )];
    let koumoku = HashMap::from([
        koumoku_row("02009", "遅刻回数", 0, 0),
        koumoku_row("02010", "早退回数", 0, 0),
    ]);
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].attendance.get("遅刻回数"), Some(&8.0));
    assert_eq!(rows[0].attendance.get("早退回数"), Some(&1.0));
}

#[test]
fn test_build_payroll_rows_attendance_keeps_sub_hour_precision() {
    // 実データ (0200/0400 社): raw 13750 = 137.50h、raw 7497 = 74.97h
    let raw = vec![raw_row_with_kindata(1, "1771", 1, &[], &[(4, 13_750)])];
    let koumoku = HashMap::from([koumoku_row("01005", "残業時間", 0, 0)]);
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].attendance.get("残業時間"), Some(&137.5));

    let raw = vec![raw_row_with_kindata(1, "1771", 1, &[], &[(4, 7_497)])];
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].attendance.get("残業時間"), Some(&74.97));
}

#[test]
fn test_build_payroll_rows_attendance_is_separate_from_payments() {
    // 勤怠は payments/deductions のどちらにも混ざらない (合計が狂わない)
    let raw = vec![raw_row_with_kindata(
        1,
        "1771",
        1,
        &[(0, 83_418)],
        &[(0, 2_100)],
    )];
    let koumoku = HashMap::from([
        koumoku_row("01018", "基本給", 1, 0),
        koumoku_row("01001", "出勤日数", 0, 0),
    ]);
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].payments.get("基本給"), Some(&83_418));
    assert!(!rows[0].payments.contains_key("出勤日数"));
    assert!(!rows[0].deductions.contains_key("出勤日数"));
    assert_eq!(rows[0].attendance.len(), 1);
}

#[test]
fn test_build_payroll_rows_attendance_skips_zero_without_warning() {
    // 体系によって未定義の項目番号があるので、0 を拾うと warning が全社で毎回出る
    let raw = vec![raw_row_with_kindata(1, "1771", 1, &[], &[])];
    let (rows, warnings) = build_payroll_rows(&raw, &HashMap::new(), &[]);
    assert!(rows[0].attendance.is_empty());
    assert!(warnings.iter().all(|w| !w.contains("勤怠")));
}

#[test]
fn test_build_payroll_rows_attendance_warns_on_unresolved_item() {
    // 名前が引けない非ゼロ値は捨てるが warning は出す (金額側は支給へ倒すが、
    // 勤怠は名前が無いと単位すら分からないので合算しない)
    let raw = vec![raw_row_with_kindata(1, "1771", 1, &[], &[(3, 2_100)])];
    let (rows, warnings) = build_payroll_rows(&raw, &HashMap::new(), &[]);
    assert!(rows[0].attendance.is_empty());
    assert!(warnings.iter().any(|w| w.contains("勤怠の項目マスタ未解決")
        && w.contains("01004")
        && w.contains("KINDATA0300")));
}

#[test]
fn test_build_payroll_rows_attendance_ignores_empty_item_name() {
    let raw = vec![raw_row_with_kindata(1, "1771", 1, &[], &[(0, 2_100)])];
    let koumoku = HashMap::from([koumoku_row("01001", "", 0, 0)]);
    let (rows, warnings) = build_payroll_rows(&raw, &koumoku, &[]);
    assert!(rows[0].attendance.is_empty());
    assert!(warnings
        .iter()
        .any(|w| w.contains("勤怠の項目マスタ未解決")));
}

#[test]
fn test_build_payroll_rows_attendance_sums_same_name() {
    // 同名項目は合算する (payments と同じ規則)
    let raw = vec![raw_row_with_kindata(
        1,
        "1771",
        1,
        &[],
        &[(0, 2_100), (1, 50)],
    )];
    let koumoku = HashMap::from([
        koumoku_row("01001", "出勤日数", 0, 0),
        koumoku_row("01002", "出勤日数", 0, 0),
    ]);
    let (rows, _) = build_payroll_rows(&raw, &koumoku, &[]);
    assert_eq!(rows[0].attendance.get("出勤日数"), Some(&21.5));
}
