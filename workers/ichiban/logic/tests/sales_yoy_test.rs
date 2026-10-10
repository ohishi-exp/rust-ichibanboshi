//! `/api/sales/customer-yoy`・`/api/sales/customer-yoy-by-dept` の純粋部分
//! (オンプレ版の tests/sales_logic_test.rs の calc_yoy_entries・split_and_sort_yoy・customer-yoy-by-dept の分から移し、
//! 既定値・SQL・同じ値どうしの並び・JSON の形を足した)。

use std::collections::HashMap;

use ichiban_logic::api::Department;
use ichiban_logic::sales_yoy::*;

fn yoy(code: &str, cur: i64, prev: i64, pct: f64) -> CustomerYoy {
    CustomerYoy {
        customer_code: code.into(),
        customer_name: code.into(),
        current_total: cur,
        prev_total: prev,
        diff: cur - prev,
        yoy_percent: pct,
    }
}

fn yoy_dept(dept: &str, code: &str, cur: i64, prev: i64, pct: f64) -> CustomerYoyWithDept {
    CustomerYoyWithDept {
        department_code: dept.into(),
        department_name: format!("部門{dept}"),
        customer_code: code.into(),
        customer_name: code.into(),
        current_total: cur,
        prev_total: prev,
        diff: cur - prev,
        yoy_percent: pct,
    }
}

fn total(code: &str, name: &str, total: i64) -> RawCustomerTotalRow {
    RawCustomerTotalRow {
        customer_code: code.into(),
        customer_name: name.into(),
        total,
    }
}

fn dept_row(dc: &str, dn: &str, cc: &str, cn: &str, total: i64) -> RawCustomerDeptRow {
    RawCustomerDeptRow {
        department_code: dc.into(),
        department_name: dn.into(),
        customer_code: cc.into(),
        customer_name: cn.into(),
        total,
    }
}

fn codes(v: &[CustomerYoy]) -> Vec<&str> {
    v.iter().map(|e| e.customer_code.as_str()).collect()
}

fn dept_keys(v: &[CustomerYoyWithDept]) -> Vec<(&str, &str)> {
    v.iter()
        .map(|e| (e.department_code.as_str(), e.customer_code.as_str()))
        .collect()
}

// ══════════════════════════════════════════════════════════════
// SQL
// ══════════════════════════════════════════════════════════════

#[test]
fn test_customer_yoy_sql_is_onprem_string() {
    assert_eq!(
        CUSTOMER_YOY_SQL,
        "SELECT m.[得意先C], ISNULL(c.[得意先N], ''), SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) FROM [得意先別月計] m LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 GROUP BY m.[得意先C], c.[得意先N]"
    );
}

const BY_DEPT_BASE: &str = "SELECT t.[受注部門], ISNULL(d.[部門N], ''), t.[得意先C], ISNULL(c.[得意先N], ''), SUM(ISNULL(t.[税抜金額],0) + ISNULL(t.[税抜割増],0) + ISNULL(t.[税抜実費],0) - ISNULL(t.[値引],0)) + SUM(ISNULL(t.[税抜傭車金額],0) + ISNULL(t.[税抜傭車割増],0) + ISNULL(t.[税抜傭車実費],0) - ISNULL(t.[傭車値引],0)) FROM [運転日報明細] t LEFT JOIN [部門ﾏｽﾀ] d ON t.[受注部門] = d.[部門C] LEFT JOIN [得意先ﾏｽﾀ] c ON t.[得意先C] = c.[得意先C] WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 AND t.[請求K] IN ('0','2')";
const BY_DEPT_GROUP: &str = " GROUP BY t.[受注部門], d.[部門N], t.[得意先C], c.[得意先N]";

#[test]
fn test_customer_yoy_by_dept_sql_without_department() {
    // オンプレ版: format!("{}{}", base_select, group_order)
    assert_eq!(
        customer_yoy_by_dept_sql(false),
        format!("{BY_DEPT_BASE}{BY_DEPT_GROUP}")
    );
}

#[test]
fn test_customer_yoy_by_dept_sql_with_department() {
    // オンプレ版: format!("{} AND t.[受注部門] = @P3 {}", base_select, group_order)
    let sql = customer_yoy_by_dept_sql(true);
    assert_eq!(
        sql,
        format!("{BY_DEPT_BASE} AND t.[受注部門] = @P3 {BY_DEPT_GROUP}")
    );
    assert!(!sql.contains("@P4"));
}

// ══════════════════════════════════════════════════════════════
// Query の既定値
// ══════════════════════════════════════════════════════════════

#[test]
fn test_customer_yoy_query_defaults() {
    let p = CustomerYoyQuery::default().period();
    assert_eq!(
        p,
        YoyPeriod {
            from_date: "2025-04-01".into(),
            to_date: "2026-03-01".into(),
            prev_from: "2024-04-01".into(),
            prev_to: "2025-03-01".into(),
            limit: 10,
            months: 12,
            min_prev: 480_000,
        }
    );
}

#[test]
fn test_customer_yoy_query_explicit_and_limit_capped() {
    let q = CustomerYoyQuery {
        from: Some("2026-01".into()),
        to: Some("2026-03".into()),
        limit: Some(999),
        min_prev: Some(5),
    };
    let p = q.period();
    assert_eq!(p.from_date, "2026-01-01");
    assert_eq!(p.to_date, "2026-03-01");
    assert_eq!(p.prev_from, "2025-01-01");
    assert_eq!(p.prev_to, "2025-03-01");
    assert_eq!(p.limit, 50);
    assert_eq!(p.months, 3);
    assert_eq!(p.min_prev, 5);
}

#[test]
fn test_customer_yoy_query_limit_zero_kept() {
    let q = CustomerYoyQuery {
        limit: Some(0),
        ..Default::default()
    };
    assert_eq!(q.period().limit, 0);
}

#[test]
fn test_customer_yoy_by_dept_query_defaults_and_department() {
    let q = CustomerYoyByDeptQuery::default();
    assert_eq!(q.period(), CustomerYoyQuery::default().period());
    assert_eq!(q.department(), None);

    let q = CustomerYoyByDeptQuery {
        from: Some("2026-04".into()),
        to: Some("2026-04".into()),
        limit: Some(3),
        min_prev: None,
        department_code: Some("  01 ".into()),
    };
    let p = q.period();
    assert_eq!(p.months, 1);
    assert_eq!(p.min_prev, 40_000);
    assert_eq!(p.limit, 3);
    assert_eq!(q.department().as_deref(), Some("01"));

    let blank = CustomerYoyByDeptQuery {
        department_code: Some("   ".into()),
        ..Default::default()
    };
    assert_eq!(blank.department(), None);
}

// ══════════════════════════════════════════════════════════════
// rows_to_code_total_map / rows_to_dept_customer_map
// ══════════════════════════════════════════════════════════════

#[test]
fn test_rows_to_code_total_map_last_row_wins() {
    let map = rows_to_code_total_map(&[
        total("A", "旧名", 1),
        total("B", "顧客B", 2),
        total("A", "顧客A", 3),
    ]);
    assert_eq!(map.len(), 2);
    assert_eq!(map["A"], ("顧客A".to_string(), 3));
    assert_eq!(map["B"], ("顧客B".to_string(), 2));
}

#[test]
fn test_rows_to_dept_customer_map_basic() {
    let rows = vec![
        dept_row("01", "本社", "A", "顧客A", 1_000_000),
        dept_row("02", "大阪", "B", "顧客B", 500_000),
    ];
    let map = rows_to_dept_customer_map(&rows);
    assert_eq!(map.len(), 2);
    let v = map.get(&("01".into(), "A".into())).cloned().unwrap();
    assert_eq!(v.0, "本社");
    assert_eq!(v.1, "顧客A");
    assert_eq!(v.2, 1_000_000);
}

#[test]
fn test_rows_to_dept_customer_map_empty() {
    assert!(rows_to_dept_customer_map(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// calc_yoy_entries + split_and_sort_yoy
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_yoy_entries_basic() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("A".into(), ("顧客A".into(), 1_200_000));
    cur.insert("B".into(), ("顧客B".into(), 800_000));

    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("A".into(), ("顧客A".into(), 1_000_000));
    prev.insert("B".into(), ("顧客B".into(), 1_000_000));

    let entries = calc_yoy_entries(&cur, &prev, 100_000);
    assert_eq!(codes(&entries), ["A", "B"]); // 得意先コード順

    assert_eq!(entries[0].yoy_percent, 20.0);
    assert_eq!(entries[0].diff, 200_000);
    assert_eq!(entries[1].yoy_percent, -20.0);
}

#[test]
fn test_calc_yoy_entries_rounds_to_one_decimal() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("A".into(), ("A".into(), 1_001));
    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("A".into(), ("A".into(), 3_000));
    let entries = calc_yoy_entries(&cur, &prev, 0);
    // -1999/3000 = -66.633..% → -66.6
    assert_eq!(entries[0].yoy_percent, -66.6);
}

#[test]
fn test_calc_yoy_entries_min_prev_filter() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("A".into(), ("A".into(), 500_000));
    cur.insert("B".into(), ("B".into(), 100_000));

    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("A".into(), ("A".into(), 400_000));
    prev.insert("B".into(), ("B".into(), 30_000)); // min_prev 未満

    let entries = calc_yoy_entries(&cur, &prev, 40_000);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].customer_code, "A");
}

#[test]
fn test_calc_yoy_entries_no_prev_data() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("NEW".into(), ("新規".into(), 500_000));
    let prev: CodeTotalMap = HashMap::new();

    // prev_total=0 < min_prev=1 → 除外
    assert!(calc_yoy_entries(&cur, &prev, 1).is_empty());

    // min_prev=0 なら残り、前年 0 で inf
    let entries = calc_yoy_entries(&cur, &prev, 0);
    assert_eq!(entries.len(), 1);
    assert!(entries[0].yoy_percent.is_infinite());
}

#[test]
fn test_calc_yoy_entries_only_in_prev() {
    let cur: CodeTotalMap = HashMap::new();
    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("OLD".into(), ("旧顧客".into(), 500_000));

    let entries = calc_yoy_entries(&cur, &prev, 100_000);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].customer_name, "旧顧客");
    assert_eq!(entries[0].current_total, 0);
    assert_eq!(entries[0].yoy_percent, -100.0);
}

#[test]
fn test_calc_yoy_entries_prefers_current_name() {
    let mut cur: CodeTotalMap = HashMap::new();
    cur.insert("A".into(), ("新名".into(), 10));
    let mut prev: CodeTotalMap = HashMap::new();
    prev.insert("A".into(), ("旧名".into(), 10));
    let entries = calc_yoy_entries(&cur, &prev, 0);
    assert_eq!(entries[0].customer_name, "新名");
}

#[test]
fn test_split_and_sort_yoy() {
    let entries = vec![
        yoy("A", 120, 100, 20.0),
        yoy("B", 80, 100, -20.0),
        yoy("C", 50, 200, -75.0),
        yoy("D", 150, 50, 200.0),
    ];

    let (pos, neg) = split_and_sort_yoy(entries, 10);

    // positive: 前年売上降順
    assert_eq!(codes(&pos), ["A", "D"]);
    // negative: YoY%昇順
    assert_eq!(codes(&neg), ["C", "B"]);
}

#[test]
fn test_split_and_sort_yoy_with_limit() {
    let entries = vec![
        yoy("A", 200, 100, 100.0),
        yoy("B", 150, 100, 50.0),
        yoy("C", 130, 100, 30.0),
        yoy("X", 10, 100, -90.0),
        yoy("Y", 20, 100, -80.0),
    ];

    let (pos, neg) = split_and_sort_yoy(entries, 2);
    assert_eq!(pos.len(), 2); // limit で切られる
    assert_eq!(codes(&neg), ["X", "Y"]);

    let (pos, neg) = split_and_sort_yoy(vec![yoy("A", 2, 1, 100.0)], 0);
    assert!(pos.is_empty() && neg.is_empty());
}

#[test]
fn test_split_and_sort_yoy_zero_and_nan_excluded() {
    let entries = vec![yoy("X", 100, 100, 0.0), yoy("N", 0, 0, f64::NAN)];
    let (pos, neg) = split_and_sort_yoy(entries, 10);
    assert!(pos.is_empty()); // 0% と NaN は positive でも negative でもない
    assert!(neg.is_empty());
}

#[test]
fn test_split_and_sort_yoy_ties_by_customer_code() {
    // positive: 前年売上が同じなら得意先コード昇順 (入力の順に依らない)
    let (pos, _) = split_and_sort_yoy(
        vec![
            yoy("C", 200, 100, 100.0),
            yoy("A", 150, 100, 50.0),
            yoy("B", 300, 100, 200.0),
            yoy("Z", 900, 500, 80.0),
        ],
        10,
    );
    assert_eq!(codes(&pos), ["Z", "A", "B", "C"]);

    // negative: 率 → 前年売上の降順 → 得意先コード昇順
    let (_, neg) = split_and_sort_yoy(
        vec![
            yoy("B", 50, 100, -50.0),
            yoy("A", 50, 100, -50.0),
            yoy("C", 100, 200, -50.0),
            yoy("D", 0, 10, -100.0),
        ],
        10,
    );
    assert_eq!(codes(&neg), ["D", "C", "A", "B"]);
}

// ══════════════════════════════════════════════════════════════
// customer-yoy-by-dept: calc_yoy_with_dept_entries
// ══════════════════════════════════════════════════════════════

fn make_dept_map(entries: &[(&str, &str, &str, &str, i64)]) -> DeptCustomerTotalMap {
    let mut map = HashMap::new();
    for (dc, dn, cc, cn, total) in entries {
        map.insert(
            ((*dc).to_string(), (*cc).to_string()),
            ((*dn).to_string(), (*cn).to_string(), *total),
        );
    }
    map
}

#[test]
fn test_calc_yoy_with_dept_entries_growth() {
    let cur = make_dept_map(&[("01", "本社", "A", "顧客A", 1_200_000)]);
    let prev = make_dept_map(&[("01", "本社", "A", "顧客A", 1_000_000)]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.department_code, "01");
    assert_eq!(e.department_name, "本社");
    assert_eq!(e.customer_code, "A");
    assert_eq!(e.current_total, 1_200_000);
    assert_eq!(e.prev_total, 1_000_000);
    assert_eq!(e.diff, 200_000);
    assert!((e.yoy_percent - 20.0).abs() < 1e-6);
}

#[test]
fn test_calc_yoy_with_dept_entries_filter_min_prev() {
    let cur = make_dept_map(&[("01", "本社", "A", "顧客A", 100)]);
    let prev = make_dept_map(&[("01", "本社", "A", "顧客A", 100)]);
    // min_prev=1000 → filtered out
    assert!(calc_yoy_with_dept_entries(&cur, &prev, 1000).is_empty());
}

#[test]
fn test_calc_yoy_with_dept_entries_prev_only_uses_prev_names() {
    let cur = HashMap::new();
    let prev = make_dept_map(&[("01", "本社", "A", "顧客A", 500_000)]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].department_name, "本社");
    assert_eq!(entries[0].customer_name, "顧客A");
    assert_eq!(entries[0].current_total, 0);
    assert_eq!(entries[0].prev_total, 500_000);
    assert_eq!(entries[0].diff, -500_000);
}

#[test]
fn test_calc_yoy_with_dept_entries_prefers_current_names() {
    let cur = make_dept_map(&[("01", "新部門", "A", "新名", 10)]);
    let prev = make_dept_map(&[("01", "旧部門", "A", "旧名", 10)]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(entries[0].department_name, "新部門");
    assert_eq!(entries[0].customer_name, "新名");
}

#[test]
fn test_calc_yoy_with_dept_entries_distinct_by_dept() {
    // 同じ顧客コードでも営業所が違えば別エントリ ((部門, 得意先) 順)
    let cur = make_dept_map(&[
        ("02", "大阪", "A", "顧客A", 2_000),
        ("01", "本社", "A", "顧客A", 1_000),
    ]);
    let prev = make_dept_map(&[
        ("01", "本社", "A", "顧客A", 800),
        ("02", "大阪", "A", "顧客A", 3_000),
    ]);
    let entries = calc_yoy_with_dept_entries(&cur, &prev, 0);
    assert_eq!(dept_keys(&entries), [("01", "A"), ("02", "A")]);
}

// ══════════════════════════════════════════════════════════════
// split_and_sort_yoy_with_dept
// ══════════════════════════════════════════════════════════════

#[test]
fn test_split_and_sort_yoy_with_dept_basic() {
    let entries = vec![
        yoy_dept("01", "A", 120, 100, 20.0),
        yoy_dept("01", "B", 80, 100, -20.0),
        yoy_dept("02", "C", 100, 100, 0.0),
    ];
    let (pos, neg) = split_and_sort_yoy_with_dept(entries, 10);
    assert_eq!(dept_keys(&pos), [("01", "A")]);
    assert_eq!(dept_keys(&neg), [("01", "B")]);
}

#[test]
fn test_split_and_sort_yoy_with_dept_limit() {
    let entries: Vec<CustomerYoyWithDept> = (0..20)
        .map(|i| yoy_dept("01", &format!("{:03}", i), 100 + i, 100, i as f64))
        .collect();
    let (pos, neg) = split_and_sort_yoy_with_dept(entries, 5);
    assert_eq!(pos.len(), 5);
    assert!(neg.is_empty());
}

#[test]
fn test_split_and_sort_yoy_with_dept_neg_sort_by_percent() {
    let entries = vec![
        yoy_dept("01", "B", 90, 100, -10.0),
        yoy_dept("01", "A", 0, 100, -100.0),
    ];
    let (_, neg) = split_and_sort_yoy_with_dept(entries, 10);
    // 最も減少率が大きいものが先頭
    assert_eq!(dept_keys(&neg), [("01", "A"), ("01", "B")]);
}

#[test]
fn test_split_and_sort_yoy_with_dept_zero_and_nan_excluded() {
    let entries = vec![
        yoy_dept("01", "X", 100, 100, 0.0),
        yoy_dept("01", "N", 0, 0, f64::NAN),
    ];
    let (pos, neg) = split_and_sort_yoy_with_dept(entries, 10);
    assert!(pos.is_empty()); // 0% と NaN は positive でも negative でもない
    assert!(neg.is_empty());
}

#[test]
fn test_split_and_sort_yoy_with_dept_ties() {
    // 同じ値どうしは得意先コード → 部門コードの昇順
    let (pos, _) = split_and_sort_yoy_with_dept(
        vec![
            yoy_dept("02", "A", 150, 100, 50.0),
            yoy_dept("01", "B", 150, 100, 50.0),
            yoy_dept("01", "A", 150, 100, 50.0),
            yoy_dept("09", "Z", 600, 500, 20.0),
        ],
        10,
    );
    assert_eq!(
        dept_keys(&pos),
        [("09", "Z"), ("01", "A"), ("02", "A"), ("01", "B")]
    );

    let (_, neg) = split_and_sort_yoy_with_dept(
        vec![
            yoy_dept("02", "A", 50, 100, -50.0),
            yoy_dept("01", "B", 50, 100, -50.0),
            yoy_dept("01", "A", 50, 100, -50.0),
            yoy_dept("03", "C", 100, 200, -50.0),
            yoy_dept("03", "N", 0, 0, f64::NAN),
        ],
        10,
    );
    assert_eq!(
        dept_keys(&neg),
        [("03", "C"), ("01", "A"), ("02", "A"), ("01", "B")]
    );
}

// ══════════════════════════════════════════════════════════════
// build_customer_yoy / build_customer_yoy_by_dept と JSON の形
// ══════════════════════════════════════════════════════════════

fn period(limit: usize, min_prev: i64) -> YoyPeriod {
    YoyPeriod {
        from_date: "2026-01-01".into(),
        to_date: "2026-03-01".into(),
        prev_from: "2025-01-01".into(),
        prev_to: "2025-03-01".into(),
        limit,
        months: 3,
        min_prev,
    }
}

#[test]
fn test_build_customer_yoy() {
    let cur = [total("A", "顧客A", 150), total("B", "顧客B", 50)];
    let prev = [
        total("A", "顧客A", 100),
        total("B", "顧客B", 100),
        total("C", "顧客C", 5),
    ];
    let r = build_customer_yoy(&period(10, 10), &cur, &prev);
    assert_eq!(codes(&r.positive), ["A"]);
    assert_eq!(codes(&r.negative), ["B"]);
    assert_eq!(r.min_prev, 10);
    assert_eq!(r.months, 3);

    let json = serde_json::to_string(&r).unwrap();
    assert_eq!(
        json,
        r#"{"positive":[{"customer_code":"A","customer_name":"顧客A","current_total":150,"prev_total":100,"diff":50,"yoy_percent":50.0}],"negative":[{"customer_code":"B","customer_name":"顧客B","current_total":50,"prev_total":100,"diff":-50,"yoy_percent":-50.0}],"min_prev":10,"months":3}"#
    );
}

#[test]
fn test_build_customer_yoy_inf_serializes_as_null() {
    let r = build_customer_yoy(&period(10, 0), &[total("N", "新規", 10)], &[]);
    let json = serde_json::to_string(&r).unwrap();
    assert!(json.contains(r#""yoy_percent":null"#));
}

#[test]
fn test_build_customer_yoy_by_dept() {
    let cur = [
        dept_row("01", "本社", "A", "顧客A", 120),
        dept_row("02", "大阪", "B", "顧客B", 10),
    ];
    let prev = [
        dept_row("01", "本社", "A", "顧客A", 100),
        dept_row("02", "大阪", "B", "顧客B", 100),
    ];
    let departments = vec![Department {
        department_code: "01".into(),
        department_name: "本社".into(),
    }];
    let r = build_customer_yoy_by_dept(&period(1, 0), Some("01".into()), &cur, &prev, departments);
    assert_eq!(dept_keys(&r.positive), [("01", "A")]);
    assert_eq!(dept_keys(&r.negative), [("02", "B")]);

    let json = serde_json::to_string(&r).unwrap();
    assert_eq!(
        json,
        r#"{"positive":[{"department_code":"01","department_name":"本社","customer_code":"A","customer_name":"顧客A","current_total":120,"prev_total":100,"diff":20,"yoy_percent":20.0}],"negative":[{"department_code":"02","department_name":"大阪","customer_code":"B","customer_name":"顧客B","current_total":10,"prev_total":100,"diff":-90,"yoy_percent":-90.0}],"months":3,"min_prev":0,"department_code":"01","departments":[{"department_code":"01","department_name":"本社"}]}"#
    );

    let none = build_customer_yoy_by_dept(&period(10, 0), None, &[], &[], vec![]);
    assert_eq!(
        serde_json::to_string(&none).unwrap(),
        r#"{"positive":[],"negative":[],"months":3,"min_prev":0,"department_code":null,"departments":[]}"#
    );
}
