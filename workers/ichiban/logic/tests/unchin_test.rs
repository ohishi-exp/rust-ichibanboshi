//! `/api/unchin/{candidates,summary,customer-net,customer-net-detail}` の純粋部分
//! (オンプレ版の tests/unchin_test.rs の純関数の分から移し、SQL・source_table・Query の既定値を足した)。

use chrono::{NaiveDate, NaiveDateTime};
use ichiban_logic::unchin::{
    build_unchin_customer_net_detail_rows, build_unchin_customer_net_rows, build_unchin_rows,
    build_unchin_summary_rows, candidates_sql, customer_net_detail_source, customer_net_detail_sql,
    customer_net_source, customer_net_sql, partner_source, summary_sql, PartnerType,
    RawUnchinCustomerNetDetailRow, RawUnchinCustomerNetRow, RawUnchinRow, RawUnchinSummaryRow,
    UnchinCustomerNetDetailQuery, UnchinCustomerNetQuery, UnchinKind, UnchinQuery,
};

fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn ks(s: &str) -> UnchinKind {
    UnchinKind::parse(s)
}

// ══════════════════════════════════════════════════════════════
// PartnerType (オンプレ版 normalize_partner_type)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_normalize_partner_type_subcontractor() {
    assert_eq!(
        PartnerType::parse("subcontractor").as_str(),
        "subcontractor"
    );
    assert_eq!(PartnerType::parse("subcontractor").master(), "傭車先ﾏｽﾀ");
}

#[test]
fn test_normalize_partner_type_customer_and_fallback() {
    assert_eq!(PartnerType::parse("customer").as_str(), "customer");
    // 未知の値・空文字は customer にフォールバック
    assert_eq!(PartnerType::parse("").as_str(), "customer");
    assert_eq!(PartnerType::parse("xxx").as_str(), "customer");
    assert_eq!(PartnerType::parse("xxx").master(), "得意先ﾏｽﾀ");
}

// ══════════════════════════════════════════════════════════════
// UnchinKind (オンプレ版 unchin_kind_filter / unchin_kind_label)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_unchin_kind_filter() {
    assert_eq!(
        ks("with_billing_only").filter(),
        "AND t.[請求K] IN ('0', '1')"
    );
    // 未知の値・default は with_non_billing (請求K IN (0,2)) にフォールバック
    assert_eq!(
        ks("with_non_billing").filter(),
        "AND t.[請求K] IN ('0', '2')"
    );
    assert_eq!(ks("").filter(), "AND t.[請求K] IN ('0', '2')");
    assert_eq!(ks("xxx").filter(), "AND t.[請求K] IN ('0', '2')");
}

#[test]
fn test_unchin_kind_label() {
    assert_eq!(
        ks("with_billing_only").label(),
        "請求＋請求のみ (請求K IN (0,1))"
    );
    assert_eq!(
        ks("with_non_billing").label(),
        "請求＋非請求 (請求K IN (0,2))"
    );
    assert_eq!(ks("xxx").label(), "請求＋非請求 (請求K IN (0,2))");
}

// ══════════════════════════════════════════════════════════════
// Query の既定値 (オンプレ版のハンドラの unwrap_or_else / unwrap_or_default)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_unchin_query_defaults() {
    let q = UnchinQuery::default();
    assert_eq!(q.range(), ("2024-01-01", "2999-12-31"));
    assert_eq!(q.partner_type(), PartnerType::Customer);
    assert_eq!(q.kind(), UnchinKind::WithNonBilling);
}

#[test]
fn test_unchin_query_values() {
    let q = UnchinQuery {
        from: Some("2026-06-01".into()),
        to: Some("2026-07-01".into()),
        partner_type: Some("subcontractor".into()),
        kind: Some("with_billing_only".into()),
    };
    assert_eq!(q.range(), ("2026-06-01", "2026-07-01"));
    assert_eq!(q.partner_type(), PartnerType::Subcontractor);
    assert_eq!(q.kind(), UnchinKind::WithBillingOnly);
}

#[test]
fn test_unchin_query_empty_from_is_kept() {
    // `from=` (空文字) は既定値に落とさずそのまま (オンプレ版と同じ)
    let q = UnchinQuery {
        from: Some("".into()),
        ..Default::default()
    };
    assert_eq!(q.range(), ("", "2999-12-31"));
}

#[test]
fn test_customer_net_query() {
    let q = UnchinCustomerNetQuery::default();
    assert_eq!(q.range(), ("2024-01-01", "2999-12-31"));
    assert_eq!(q.kind(), UnchinKind::WithNonBilling);
    let q = UnchinCustomerNetQuery {
        from: Some("2026-01-01".into()),
        to: None,
        kind: Some("with_billing_only".into()),
    };
    assert_eq!(q.range(), ("2026-01-01", "2999-12-31"));
    assert_eq!(q.kind(), UnchinKind::WithBillingOnly);
}

#[test]
fn test_customer_net_detail_query() {
    let q = UnchinCustomerNetDetailQuery {
        from: None,
        to: Some("2026-02-01".into()),
        kind: None,
        code: "034760".into(),
        h: "015".into(),
    };
    assert_eq!(q.range(), ("2024-01-01", "2026-02-01"));
    assert_eq!(q.kind(), UnchinKind::WithNonBilling);
}

// ══════════════════════════════════════════════════════════════
// source_table (オンプレ版のハンドラの format! と同じ文字列)
// ══════════════════════════════════════════════════════════════

#[test]
fn test_partner_source() {
    assert_eq!(
        partner_source(PartnerType::Customer, UnchinKind::WithNonBilling),
        "運転日報明細 + 得意先ﾏｽﾀ [請求＋非請求 (請求K IN (0,2))]"
    );
    assert_eq!(
        partner_source(PartnerType::Subcontractor, UnchinKind::WithBillingOnly),
        "運転日報明細 + 傭車先ﾏｽﾀ [請求＋請求のみ (請求K IN (0,1))]"
    );
}

#[test]
fn test_customer_net_source() {
    assert_eq!(
        customer_net_source(UnchinKind::WithBillingOnly),
        "運転日報明細 (得意先ﾏｽﾀ + 傭車先側金額の両建て) [請求＋請求のみ (請求K IN (0,1))]"
    );
}

#[test]
fn test_customer_net_detail_source() {
    assert_eq!(
        customer_net_detail_source("034760", "015", UnchinKind::WithNonBilling),
        "運転日報明細 (得意先C=034760, 得意先H=015 の両建て明細) [請求＋非請求 (請求K IN (0,2))]"
    );
}

// ══════════════════════════════════════════════════════════════
// SQL (オンプレ版 src/repo.rs の format! をそのまま写した期待値と一致させる)
// ══════════════════════════════════════════════════════════════

const KINDS: [UnchinKind; 2] = [UnchinKind::WithBillingOnly, UnchinKind::WithNonBilling];

#[test]
fn test_candidates_sql_matches_onprem() {
    for k in KINDS {
        let customer = format!(
            "SELECT \
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
               AND t.[品名C] NOT IN ('9003', '9998') \
               {} \
             ORDER BY t.[得意先C], t.[得意先H], t.[品名C], t.[金額]",
            k.filter()
        );
        assert_eq!(candidates_sql(PartnerType::Customer, k), customer);

        let sub = format!(
            "SELECT \
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
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             ORDER BY t.[傭車先C], t.[傭車先H], t.[品名C], t.[金額]",
            k.filter()
        );
        assert_eq!(candidates_sql(PartnerType::Subcontractor, k), sub);
    }
}

#[test]
fn test_summary_sql_matches_onprem() {
    for k in KINDS {
        let sub = format!(
            "SELECT t.[傭車先C], t.[傭車先H], \
             ISNULL(m.[傭車先N], ''), \
             SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)), \
             ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
             FROM [運転日報明細] t \
             OUTER APPLY (SELECT TOP 1 c.[傭車先N], c.[部門C] FROM [傭車先ﾏｽﾀ] c \
               WHERE c.[傭車先C] = t.[傭車先C] AND c.[傭車先H] = t.[傭車先H]) m \
             LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND t.[品名C] NOT IN ('9003', '9998') \
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             GROUP BY t.[傭車先C], t.[傭車先H], m.[傭車先N], m.[部門C], bm.[部門N] \
             ORDER BY SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC",
            k.filter()
        );
        assert_eq!(summary_sql(PartnerType::Subcontractor, k), sub);

        let customer = format!(
            "SELECT t.[得意先C], t.[得意先H], \
             ISNULL(m.[得意先N], ''), \
             SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)), \
             ISNULL(m.[部門C], ''), ISNULL(bm.[部門N], '') \
             FROM [運転日報明細] t \
             OUTER APPLY (SELECT TOP 1 c.[得意先N], c.[部門C] FROM [得意先ﾏｽﾀ] c \
               WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]) m \
             LEFT JOIN [部門ﾏｽﾀ] bm ON bm.[部門C] = m.[部門C] \
             WHERE t.[売上年月日] >= @P1 AND t.[売上年月日] < @P2 \
               AND t.[品名C] NOT IN ('9003', '9998') \
               {} \
             GROUP BY t.[得意先C], t.[得意先H], m.[得意先N], m.[部門C], bm.[部門N] \
             ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) DESC",
            k.filter()
        );
        assert_eq!(summary_sql(PartnerType::Customer, k), customer);
    }
}

#[test]
fn test_customer_net_sql_matches_onprem() {
    for k in KINDS {
        let expected = format!(
            "SELECT t.[得意先C], t.[得意先H], \
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
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             GROUP BY t.[得意先C], t.[得意先H], m.[得意先N], m.[部門C], bm.[部門N] \
             ORDER BY SUM(ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)) \
               - SUM(ISNULL(t.[傭車金額], 0) + ISNULL(t.[傭車割増], 0) + ISNULL(t.[傭車実費], 0)) DESC",
            k.filter()
        );
        assert_eq!(customer_net_sql(k), expected);
    }
}

#[test]
fn test_customer_net_detail_sql_matches_onprem() {
    for k in KINDS {
        let expected = format!(
            "SELECT \
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
               AND ISNULL(t.[傭車先C], '000000') != '000000' \
               {} \
             ORDER BY t.[売上年月日] DESC",
            k.filter()
        );
        assert_eq!(customer_net_detail_sql(k), expected);
    }
}

// ══════════════════════════════════════════════════════════════
// build_unchin_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_rows_normal_and_edges() {
    let raw = vec![
        RawUnchinRow {
            partner_code: "034760-015".into(),
            partner_name: "全農物流㈱　九州支店".into(),
            item_code: "6301".into(),
            item_name: "フレコン".into(),
            fare: 30_000,
            origin: "釧路".into(),
            dest: "八代".into(),
            sale_date: dt(2026, 6, 20),
            bumon_code: "010".into(),
            bumon_name: "本社".into(),
            vehicle_code: "0272-01".into(),
        },
        // エッジ: 空品名コード・空積地・空車輌 (車輌C/H が未設定の行)
        RawUnchinRow {
            partner_code: "034760-015".into(),
            partner_name: "全農物流㈱　九州支店".into(),
            item_code: "0000".into(),
            item_name: "".into(),
            fare: 140_000,
            origin: "".into(),
            dest: "福岡県北九州市".into(),
            sale_date: dt(2026, 6, 19),
            bumon_code: "".into(),
            bumon_name: "".into(),
            vehicle_code: "-".into(),
        },
    ];

    let rows = build_unchin_rows(&raw);
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    assert_eq!(first.partner_code, "034760-015");
    assert_eq!(first.partner_name, "全農物流㈱　九州支店");
    assert_eq!(first.item_code, "6301");
    assert_eq!(first.item_name, "フレコン");
    assert_eq!(first.fare, 30_000);
    assert_eq!(first.origin, "釧路");
    assert_eq!(first.dest, "八代");
    assert_eq!(first.sale_date, "2026-06-20");
    assert_eq!(first.bumon_code, "010");
    assert_eq!(first.bumon_name, "本社");
    assert_eq!(first.vehicle_code, "0272-01");

    let second = &rows[1];
    assert_eq!(second.item_code, "0000");
    assert_eq!(second.item_name, "");
    assert_eq!(second.origin, "");
    assert_eq!(second.dest, "福岡県北九州市");
    assert_eq!(second.bumon_code, "");
    assert_eq!(second.bumon_name, "");
    assert_eq!(second.vehicle_code, "-");
}

#[test]
fn test_build_unchin_rows_empty() {
    assert!(build_unchin_rows(&[]).is_empty());
}

#[test]
fn test_build_unchin_rows_json_field_order() {
    // フィールドの並びはオンプレ版と同じ (sha256 比較の前提)
    let raw = vec![RawUnchinRow {
        partner_code: "1-2".into(),
        partner_name: "n".into(),
        item_code: "i".into(),
        item_name: "in".into(),
        fare: 5,
        origin: "o".into(),
        dest: "d".into(),
        sale_date: dt(2026, 1, 2),
        bumon_code: "b".into(),
        bumon_name: "bn".into(),
        vehicle_code: "v".into(),
    }];
    let json = serde_json::to_string(&build_unchin_rows(&raw)).unwrap();
    assert_eq!(
        json,
        r#"[{"partner_code":"1-2","partner_name":"n","item_code":"i","item_name":"in","fare":5,"origin":"o","dest":"d","sale_date":"2026-01-02","bumon_code":"b","bumon_name":"bn","vehicle_code":"v"}]"#
    );
}

// ══════════════════════════════════════════════════════════════
// build_unchin_summary_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_summary_rows() {
    let raw = vec![RawUnchinSummaryRow {
        partner_code: "034760-015".into(),
        partner_name: "全農物流㈱　九州支店".into(),
        total: 170_000,
        bumon_code: "010".into(),
        bumon_name: "本社".into(),
    }];
    let rows = build_unchin_summary_rows(&raw);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].partner_code, "034760-015");
    assert_eq!(rows[0].total, 170_000);
    assert_eq!(rows[0].bumon_code, "010");
    assert_eq!(rows[0].bumon_name, "本社");
    let json = serde_json::to_string(&rows).unwrap();
    assert_eq!(
        json,
        r#"[{"partner_code":"034760-015","partner_name":"全農物流㈱　九州支店","total":170000,"bumon_code":"010","bumon_name":"本社"}]"#
    );
}

#[test]
fn test_build_unchin_summary_rows_empty() {
    assert!(build_unchin_summary_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// build_unchin_customer_net_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_customer_net_rows_positive_diff() {
    let raw = vec![RawUnchinCustomerNetRow {
        partner_code: "034760-015".into(),
        partner_name: "全農物流㈱　九州支店".into(),
        total_sales: 170_000,
        total_payment: 120_000,
        bumon_code: "010".into(),
        bumon_name: "本社".into(),
    }];
    let rows = build_unchin_customer_net_rows(&raw);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].partner_code, "034760-015");
    assert_eq!(rows[0].partner_name, "全農物流㈱　九州支店");
    assert_eq!(rows[0].total_sales, 170_000);
    assert_eq!(rows[0].total_payment, 120_000);
    assert_eq!(rows[0].diff, 50_000);
    assert_eq!(rows[0].bumon_code, "010");
    assert_eq!(rows[0].bumon_name, "本社");
    let json = serde_json::to_string(&rows).unwrap();
    assert_eq!(
        json,
        r#"[{"partner_code":"034760-015","partner_name":"全農物流㈱　九州支店","total_sales":170000,"total_payment":120000,"diff":50000,"bumon_code":"010","bumon_name":"本社"}]"#
    );
}

#[test]
fn test_build_unchin_customer_net_rows_zero_payment_diff_equals_sales() {
    // total_payment=0 (SQL 側は自社便を除外済みだが、純粋関数は入力をそのまま計算する)
    let raw = vec![RawUnchinCustomerNetRow {
        partner_code: "099999-000".into(),
        partner_name: "ゼロ支払テスト".into(),
        total_sales: 50_000,
        total_payment: 0,
        bumon_code: "".into(),
        bumon_name: "".into(),
    }];
    let rows = build_unchin_customer_net_rows(&raw);
    assert_eq!(rows[0].diff, 50_000);
}

#[test]
fn test_build_unchin_customer_net_rows_empty() {
    assert!(build_unchin_customer_net_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// build_unchin_customer_net_detail_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_customer_net_detail_rows_with_subcontractor() {
    let raw = vec![RawUnchinCustomerNetDetailRow {
        item_code: "6301".into(),
        item_name: "フレコン".into(),
        subcontractor_name: "㈱九州運輸".into(),
        sales: 30_000,
        payment: 22_000,
        origin: "釧路".into(),
        dest: "八代".into(),
        sale_date: dt(2026, 6, 20),
        bumon_code: "010".into(),
        bumon_name: "本社".into(),
    }];
    let rows = build_unchin_customer_net_detail_rows(&raw);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item_code, "6301");
    assert_eq!(rows[0].subcontractor_name, "㈱九州運輸");
    assert_eq!(rows[0].sales, 30_000);
    assert_eq!(rows[0].payment, 22_000);
    assert_eq!(rows[0].diff, 8_000);
    assert_eq!(rows[0].sale_date, "2026-06-20");
    let json = serde_json::to_string(&rows).unwrap();
    assert_eq!(
        json,
        r#"[{"item_code":"6301","item_name":"フレコン","subcontractor_name":"㈱九州運輸","sales":30000,"payment":22000,"diff":8000,"origin":"釧路","dest":"八代","sale_date":"2026-06-20","bumon_code":"010","bumon_name":"本社"}]"#
    );
}

#[test]
fn test_build_unchin_customer_net_detail_rows_zero_payment() {
    // payment=0 のエッジケース (SQL 側は自社便を除外済みだが、純粋関数は入力をそのまま計算する)
    let raw = vec![RawUnchinCustomerNetDetailRow {
        item_code: "0000".into(),
        item_name: "".into(),
        subcontractor_name: "".into(),
        sales: 15_000,
        payment: 0,
        origin: "".into(),
        dest: "".into(),
        sale_date: dt(2026, 1, 5),
        bumon_code: "".into(),
        bumon_name: "".into(),
    }];
    let rows = build_unchin_customer_net_detail_rows(&raw);
    assert_eq!(rows[0].diff, 15_000);
}

#[test]
fn test_build_unchin_customer_net_detail_rows_empty() {
    assert!(build_unchin_customer_net_detail_rows(&[]).is_empty());
}
