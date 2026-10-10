//! `/api/sales/daily`・`/api/sales/customer-trend`・`/api/sales/customer-detail` の純粋部分。
//! build_* と mode_label はオンプレ版の tests/sales_logic_test.rs から移し、SQL の組み立て・Query の既定値・
//! 400 の判定を足した。SQL の期待値はオンプレ版 `src/repo.rs` の文字列リテラル (`\` の継続行を畳んだもの) を写している。

use chrono::{NaiveDate, NaiveDateTime};
use ichiban_logic::sales_daily::*;

fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

// オンプレ版 src/repo.rs の daily の format 文字列 (当期・前年)。`{} {}` に請求区分・除外部門の条件が入る。
const ON_PREM_DAILY: &str = "SELECT [売上年月日], SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0)), SUM(ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0)), SUM(ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0)), COUNT(*) FROM [運転日報明細] WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 {} {} GROUP BY [売上年月日] ORDER BY [売上年月日]";
const ON_PREM_DAILY_PREV: &str = "SELECT [売上年月日], SUM(ISNULL([税抜金額],0)+ISNULL([税抜割増],0)+ISNULL([税抜実費],0)-ISNULL([値引],0)), SUM(ISNULL([税抜傭車金額],0)+ISNULL([税抜傭車割増],0)+ISNULL([税抜傭車実費],0)-ISNULL([傭車値引],0)), SUM(ISNULL([金額],0)+ISNULL([割増],0)+ISNULL([実費],0)-ISNULL([値引],0)), SUM(ISNULL([傭車金額],0)+ISNULL([傭車割増],0)+ISNULL([傭車実費],0)-ISNULL([傭車値引],0)) FROM [運転日報明細] WHERE [売上年月日] >= @P1 AND [売上年月日] < @P2 {} {} GROUP BY [売上年月日] ORDER BY [売上年月日]";
const ON_PREM_TREND_TOP: &str = "SELECT TOP {} m.[得意先C], ISNULL(c.[得意先N], '') FROM [得意先別月計] m LEFT JOIN [得意先ﾏｽﾀ] c ON m.[得意先C] = c.[得意先C] AND m.[得意先H] = c.[得意先H] WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 GROUP BY m.[得意先C], c.[得意先N] ORDER BY SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) DESC";

const BILLING: &str = "AND [請求K] IN ('0', '1')";
const NON_BILLING: &str = "AND [請求K] IN ('0', '2')";
const DEPT: &str = "AND [受注部門] NOT IN (SELECT [部門C] FROM [部門ﾏｽﾀ] WHERE [部門N] LIKE @P3)";

fn fill(template: &str, a: &str, b: &str) -> String {
    template.replacen("{}", a, 1).replacen("{}", b, 1)
}

fn daily_query(month: Option<&str>, mode: Option<&str>, exclude: Option<&str>) -> DailyQuery {
    DailyQuery {
        month: month.map(str::to_string),
        mode: mode.map(str::to_string),
        exclude_dept: exclude.map(str::to_string),
    }
}

// ══════════════════════════════════════════════════════════════
// build_daily_sales
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_daily_sales() {
    // 2025-04-01 は火曜日
    let current = vec![
        RawDailyRow {
            date: dt(2025, 4, 1),
            own_sales: 100,
            charter_sales: 50,
            own_sales_raw: 110,
            charter_sales_raw: 55,
            transport_count: 10,
        },
        RawDailyRow {
            date: dt(2025, 4, 2),
            own_sales: 200,
            charter_sales: 80,
            own_sales_raw: 220,
            charter_sales_raw: 88,
            transport_count: 15,
        },
    ];
    let prev = vec![RawDailyPrevRow {
        date: dt(2024, 4, 1),
        own_sales: 90,
        charter_sales: 40,
        own_sales_raw: 95,
        charter_sales_raw: 42,
    }];

    let result = build_daily_sales(&current, &prev);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].date, "2025-04-01");
    assert_eq!(result[0].weekday, "火");
    assert_eq!(result[0].total_sales, 150);
    assert_eq!(result[0].total_sales_raw, 165);
    assert_eq!(result[0].prev_year_own, 90);
    assert_eq!(result[0].prev_year_charter, 40);
    assert_eq!(result[0].prev_year_total, 130);
    assert_eq!(result[0].prev_year_own_raw, 95);
    assert_eq!(result[0].prev_year_charter_raw, 42);
    assert_eq!(result[0].prev_year_total_raw, 137);

    // 2日は前年データなし
    assert_eq!(result[1].prev_year_total, 0);
    assert_eq!(result[1].prev_year_total_raw, 0);
    assert_eq!(result[1].transport_count, 15);
}

#[test]
fn test_build_daily_sales_empty() {
    assert!(build_daily_sales(&[], &[]).is_empty());
}

#[test]
fn test_build_daily_sales_sunday() {
    // 2025-04-06 は日曜日
    let current = vec![RawDailyRow {
        date: dt(2025, 4, 6),
        own_sales: 0,
        charter_sales: 0,
        own_sales_raw: 0,
        charter_sales_raw: 0,
        transport_count: 0,
    }];
    let result = build_daily_sales(&current, &[]);
    assert_eq!(result[0].weekday, "日");
}

#[test]
fn test_daily_sales_json_field_order() {
    // オンプレ版の DailySales と同じフィールド名・順
    let current = vec![RawDailyRow {
        date: dt(2026, 3, 7),
        own_sales: 1,
        charter_sales: 2,
        own_sales_raw: 3,
        charter_sales_raw: 4,
        transport_count: 5,
    }];
    let json = serde_json::to_string(&build_daily_sales(&current, &[])).unwrap();
    assert_eq!(
        json,
        r#"[{"date":"2026-03-07","weekday":"土","own_sales":1,"charter_sales":2,"total_sales":3,"own_sales_raw":3,"charter_sales_raw":4,"total_sales_raw":7,"transport_count":5,"prev_year_own":0,"prev_year_charter":0,"prev_year_total":0,"prev_year_own_raw":0,"prev_year_charter_raw":0,"prev_year_total_raw":0}]"#
    );
}

// ══════════════════════════════════════════════════════════════
// mode_label / DailyMode
// ══════════════════════════════════════════════════════════════

#[test]
fn test_mode_label() {
    assert_eq!(mode_label("billing"), "請求+請求のみ");
    assert_eq!(mode_label("non_billing"), "請求+非請求");
    assert_eq!(mode_label("all"), "全て");
    assert_eq!(mode_label("unknown"), "全て");
}

#[test]
fn test_daily_mode_billing_filter() {
    assert_eq!(DailyMode::from_param("billing").billing_filter(), BILLING);
    assert_eq!(
        DailyMode::from_param("non_billing").billing_filter(),
        NON_BILLING
    );
    assert_eq!(DailyMode::from_param("all").billing_filter(), "");
    assert_eq!(DailyMode::from_param("x").billing_filter(), "");
}

#[test]
fn test_dept_filter() {
    assert_eq!(dept_filter(true), DEPT);
    assert_eq!(dept_filter(false), "");
}

// ══════════════════════════════════════════════════════════════
// daily_sql (オンプレ版と同じ文字列)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_daily_sql_matches_on_prem() {
    for (mode, billing) in [
        (DailyMode::All, ""),
        (DailyMode::Billing, BILLING),
        (DailyMode::NonBilling, NON_BILLING),
    ] {
        for (exclude, dept) in [(false, ""), (true, DEPT)] {
            assert_eq!(
                daily_sql(false, mode, exclude),
                fill(ON_PREM_DAILY, billing, dept)
            );
            assert_eq!(
                daily_sql(true, mode, exclude),
                fill(ON_PREM_DAILY_PREV, billing, dept)
            );
        }
    }
}

// ══════════════════════════════════════════════════════════════
// DailyQuery::plan
// ══════════════════════════════════════════════════════════════

#[test]
fn test_daily_plan_defaults() {
    let p = DailyQuery::default().plan().unwrap();
    assert_eq!(p.from, "2026-03-01");
    assert_eq!(p.to, "2026-04-01");
    assert_eq!(p.prev_from, "2025-03-01");
    assert_eq!(p.prev_to, "2025-04-01");
    assert_eq!(p.current_sql, fill(ON_PREM_DAILY, "", ""));
    assert_eq!(p.prev_sql, fill(ON_PREM_DAILY_PREV, "", ""));
    assert_eq!(p.exclude_pattern, None);
    assert_eq!(p.source_table, "運転日報明細 [全て]");
}

#[test]
fn test_daily_plan_december_and_exclude() {
    let p = daily_query(Some("2025-12"), Some("billing"), Some("本社"))
        .plan()
        .unwrap();
    assert_eq!(p.from, "2025-12-01");
    assert_eq!(p.to, "2026-01-01");
    assert_eq!(p.prev_from, "2024-12-01");
    assert_eq!(p.prev_to, "2025-01-01");
    assert_eq!(p.current_sql, fill(ON_PREM_DAILY, BILLING, DEPT));
    assert_eq!(p.prev_sql, fill(ON_PREM_DAILY_PREV, BILLING, DEPT));
    assert_eq!(p.exclude_pattern.as_deref(), Some("%本社%"));
    assert_eq!(p.source_table, "運転日報明細 [請求+請求のみ, 本社除く]");
}

#[test]
fn test_daily_plan_empty_exclude_keeps_filter_but_not_label() {
    // オンプレ版: exclude_dept=Some("") は SQL に除外条件 (LIKE '%%') が入り、source_table には出ない
    let p = daily_query(Some("2026-01"), Some("non_billing"), Some(""))
        .plan()
        .unwrap();
    assert_eq!(p.current_sql, fill(ON_PREM_DAILY, NON_BILLING, DEPT));
    assert_eq!(p.exclude_pattern.as_deref(), Some("%%"));
    assert_eq!(p.source_table, "運転日報明細 [請求+非請求]");
    // 1 月の前年は前年 1 月〜2 月
    assert_eq!(p.prev_from, "2025-01-01");
    assert_eq!(p.prev_to, "2025-02-01");
}

#[test]
fn test_daily_plan_unreadable_numbers_fall_back() {
    // 年・月が数字でなければ 2026・3。from はそのまま bind する (オンプレ版と同じ)
    let p = daily_query(Some("abc-xy"), None, None).plan().unwrap();
    assert_eq!(p.from, "abc-xy-01");
    assert_eq!(p.to, "2026-04-01");
    assert_eq!(p.prev_from, "2025-03-01");
    // 3 つ目以降の区切りは見ない (parts[1] だけ)
    let p = daily_query(Some("2026-05-15"), None, None).plan().unwrap();
    assert_eq!(p.to, "2026-06-01");
}

#[test]
fn test_daily_plan_month_without_dash_is_rejected() {
    // オンプレ版は parts[1] で panic する → None (400)
    assert!(daily_query(Some("202603"), None, None).plan().is_none());
    assert!(daily_query(Some(""), None, None).plan().is_none());
}

// ══════════════════════════════════════════════════════════════
// build_customer_trend
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_customer_trend() {
    let top = vec![
        ("A".to_string(), "顧客A".to_string()),
        ("B".to_string(), "顧客B".to_string()),
    ];
    let monthly = vec![
        RawCustomerMonthlyRow {
            customer_code: "A".into(),
            year_month: dt(2025, 4, 1),
            total: 1000,
        },
        RawCustomerMonthlyRow {
            customer_code: "B".into(),
            year_month: dt(2025, 4, 1),
            total: 800,
        },
        RawCustomerMonthlyRow {
            customer_code: "C".into(),
            year_month: dt(2025, 4, 1),
            total: 500,
        },
        RawCustomerMonthlyRow {
            customer_code: "A".into(),
            year_month: dt(2025, 5, 1),
            total: 700,
        },
        RawCustomerMonthlyRow {
            customer_code: "B".into(),
            year_month: dt(2025, 5, 1),
            total: 900,
        },
    ];

    let result = build_customer_trend(&top, &monthly);

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].customer_code, "A");
    assert_eq!(result[0].customer_name, "顧客A");
    assert_eq!(result[0].months.len(), 2);
    assert_eq!(result[0].months[0].year_month, "2025-04");
    assert_eq!(result[0].months[0].total_sales, 1000);
    assert_eq!(result[0].months[0].rank, 1); // A=1000 > B=800
    assert_eq!(result[0].months[1].rank, 2); // A=700 < B=900

    assert_eq!(result[1].customer_code, "B");
    assert_eq!(result[1].months[0].rank, 2);
    assert_eq!(result[1].months[1].rank, 1);
}

#[test]
fn test_build_customer_trend_empty_top() {
    let result = build_customer_trend(&[], &[]);
    assert!(result.is_empty());
}

#[test]
fn test_build_customer_trend_missing_month() {
    let top = vec![("A".to_string(), "顧客A".to_string())];
    let monthly = vec![
        RawCustomerMonthlyRow {
            customer_code: "A".into(),
            year_month: dt(2025, 4, 1),
            total: 1000,
        },
        // 5月はBのみ
        RawCustomerMonthlyRow {
            customer_code: "B".into(),
            year_month: dt(2025, 5, 1),
            total: 500,
        },
    ];

    let result = build_customer_trend(&top, &monthly);
    assert_eq!(result[0].months[1].total_sales, 0); // A は5月データなし
    assert_eq!(result[0].months[1].rank, 0);
}

#[test]
fn test_customer_trend_json_field_order() {
    let top = vec![("A".to_string(), "顧客A".to_string())];
    let monthly = vec![RawCustomerMonthlyRow {
        customer_code: "A".into(),
        year_month: dt(2025, 4, 1),
        total: 10,
    }];
    let json = serde_json::to_string(&build_customer_trend(&top, &monthly)).unwrap();
    assert_eq!(
        json,
        r#"[{"customer_code":"A","customer_name":"顧客A","months":[{"year_month":"2025-04","total_sales":10,"rank":1}]}]"#
    );
}

// ══════════════════════════════════════════════════════════════
// CustomerTrendQuery::plan と SQL
// ══════════════════════════════════════════════════════════════

#[test]
fn test_customer_trend_plan_defaults() {
    let p = CustomerTrendQuery::default().plan().unwrap();
    assert_eq!(p.from, "2025-04-01");
    assert_eq!(p.to, "2026-03-01");
    assert_eq!(p.top_sql, ON_PREM_TREND_TOP.replacen("{}", "20", 1));
}

#[test]
fn test_customer_trend_plan_clamps_to_50() {
    let q = CustomerTrendQuery {
        from: Some("2024-01".into()),
        to: Some("2024-06".into()),
        limit: Some(999),
    };
    let p = q.plan().unwrap();
    assert_eq!(p.from, "2024-01-01");
    assert_eq!(p.to, "2024-06-01");
    assert_eq!(p.top_sql, ON_PREM_TREND_TOP.replacen("{}", "50", 1));
    let q = CustomerTrendQuery {
        limit: Some(1),
        ..Default::default()
    };
    assert_eq!(q.plan().unwrap().top_sql, customer_trend_top_sql(1));
}

#[test]
fn test_customer_trend_plan_rejects_negative_limit() {
    // オンプレ版は TOP に負数が入って SQL Server のエラー (500) → None (400)
    for limit in [-1, i32::MIN] {
        let q = CustomerTrendQuery {
            limit: Some(limit),
            ..Default::default()
        };
        assert!(q.plan().is_none(), "limit={limit}");
    }
}

#[test]
fn test_customer_trend_plan_zero_limit_is_top_0() {
    // オンプレ版は TOP 0 で 200・空 (TOP が空なので 2 本目は流さない)。Worker も同じ SQL を流す
    let q = CustomerTrendQuery {
        limit: Some(0),
        ..Default::default()
    };
    let p = q.plan().unwrap();
    assert_eq!(p.top_sql, ON_PREM_TREND_TOP.replacen("{}", "0", 1));
    assert!(build_customer_trend(&[], &[]).is_empty());
}

#[test]
fn test_customer_trend_monthly_sql_matches_on_prem() {
    assert_eq!(
        CUSTOMER_TREND_MONTHLY_SQL,
        "SELECT m.[得意先C], m.[年月度], SUM(ISNULL(m.[自車売上], 0)) + SUM(ISNULL(m.[傭車売上], 0)) as total FROM [得意先別月計] m WHERE m.[年月度] >= @P1 AND m.[年月度] <= @P2 GROUP BY m.[得意先C], m.[年月度] ORDER BY m.[年月度], total DESC"
    );
}

// ══════════════════════════════════════════════════════════════
// build_customer_detail と SQL
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_customer_detail() {
    let raw = vec![
        RawCustomerDetailRow {
            year_month: dt(2025, 4, 1),
            own_sales: 100,
            charter_sales: 50,
            transport_count: 10,
        },
        RawCustomerDetailRow {
            year_month: dt(2025, 5, 1),
            own_sales: 200,
            charter_sales: 80,
            transport_count: 15,
        },
    ];
    let result = build_customer_detail(&raw);
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].year_month, "2025-04");
    assert_eq!(result[0].total_sales, 150);
    assert_eq!(result[0].transport_count, 10);
    assert_eq!(result[1].total_sales, 280);
}

#[test]
fn test_build_customer_detail_empty() {
    assert!(build_customer_detail(&[]).is_empty());
}

#[test]
fn test_customer_detail_response_json_field_order() {
    let body = CustomerDetailResponse {
        customer_code: "000001".into(),
        customer_name: "顧客".into(),
        months: build_customer_detail(&[RawCustomerDetailRow {
            year_month: dt(2025, 4, 1),
            own_sales: 1,
            charter_sales: 2,
            transport_count: 3,
        }]),
    };
    assert_eq!(
        serde_json::to_string(&body).unwrap(),
        r#"{"customer_code":"000001","customer_name":"顧客","months":[{"year_month":"2025-04","own_sales":1,"charter_sales":2,"total_sales":3,"transport_count":3}]}"#
    );
}

#[test]
fn test_customer_detail_sql_matches_on_prem() {
    assert_eq!(
        CUSTOMER_DETAIL_NAME_SQL,
        "SELECT TOP 1 ISNULL(c.[得意先N], '') FROM [得意先ﾏｽﾀ] c WHERE c.[得意先C] = @P1"
    );
    assert_eq!(
        CUSTOMER_DETAIL_MONTHS_SQL,
        "SELECT m.[年月度], SUM(ISNULL(m.[自車売上], 0)), SUM(ISNULL(m.[傭車売上], 0)), SUM(ISNULL(m.[輸送回数], 0)) FROM [得意先別月計] m WHERE m.[得意先C] = @P1 GROUP BY m.[年月度] ORDER BY m.[年月度]"
    );
    assert_eq!(CUSTOMER_SOURCE, "得意先別月計 + 得意先ﾏｽﾀ");
}
