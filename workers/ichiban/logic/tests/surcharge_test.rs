//! `/api/surcharge/base` の純粋部分 (オンプレ版の tests/surcharge_test.rs から移した)。
//! ハンドラ (axum) のテストは Worker には無いので、Query → 値の決定 (`params`) と SQL の組み立てで置き換えた。

use chrono::{NaiveDate, NaiveDateTime};
use ichiban_logic::surcharge::{
    build_surcharge_rows, normalize_prefecture, surcharge_kind_filter, surcharge_kind_label,
    surcharge_sql, RawSurchargeRow, SurchargeQuery,
};

fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: normalize_prefecture
// ══════════════════════════════════════════════════════════════

#[test]
fn test_normalize_prefecture_empty_is_unmapped() {
    assert_eq!(normalize_prefecture(""), "?");
    assert_eq!(normalize_prefecture("   "), "?"); // trim 後に空
}

#[test]
fn test_normalize_prefecture_hokkaido() {
    assert_eq!(normalize_prefecture("北海道札幌市中央区"), "北海道");
    assert_eq!(normalize_prefecture("北海道"), "北海道");
}

#[test]
fn test_normalize_prefecture_ken() {
    assert_eq!(normalize_prefecture("長崎県"), "長崎県");
    assert_eq!(normalize_prefecture("神奈川県横浜市"), "神奈川県");
    assert_eq!(normalize_prefecture("福岡県北九州市"), "福岡県");
}

#[test]
fn test_normalize_prefecture_fu() {
    // 京都府: 「都」を内包するが「府」優先で正しく京都府になる
    assert_eq!(normalize_prefecture("京都府京都市"), "京都府");
    assert_eq!(normalize_prefecture("大阪府"), "大阪府");
}

#[test]
fn test_normalize_prefecture_to() {
    assert_eq!(normalize_prefecture("東京都千代田区"), "東京都");
}

#[test]
fn test_normalize_prefecture_no_suffix() {
    // 県/府/都 のいずれも含まない場合はそのまま返す (防御的)
    assert_eq!(normalize_prefecture("不明地域"), "不明地域");
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: surcharge_kind_filter / surcharge_kind_label
// ══════════════════════════════════════════════════════════════

#[test]
fn test_surcharge_kind_filter() {
    assert_eq!(surcharge_kind_filter("billing_only"), "AND t.[請求K] = '1'");
    assert_eq!(surcharge_kind_filter("transport"), "AND t.[請求K] = '0'");
    assert_eq!(surcharge_kind_filter("all"), "");
    // 未知の値は billing_only と同義にフォールバック
    assert_eq!(surcharge_kind_filter("xxx"), "AND t.[請求K] = '1'");
}

#[test]
fn test_surcharge_kind_label() {
    assert_eq!(surcharge_kind_label("billing_only"), "請求のみ (請求K=1)");
    assert_eq!(surcharge_kind_label("transport"), "通常運送 (請求K=0)");
    assert_eq!(surcharge_kind_label("all"), "全請求区分");
    assert_eq!(surcharge_kind_label("xxx"), "請求のみ (請求K=1)");
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_surcharge_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_surcharge_rows_normal_and_edges() {
    let raw = vec![
        RawSurchargeRow {
            request_kind: "1".into(),
            customer_code: "000001".into(),
            customer_name: "㈱田浦畜産".into(),
            origin_area_name: "長崎県".into(),
            dest_area_name: "福岡県".into(),
            vehicle_code: "04".into(),
            vehicle_name: "大型幌".into(),
            sale_date: dt(2026, 6, 21),
            fare: 65_000,
            billing_date: Some(dt(2026, 7, 31)),
            subcontractor_code: "000000".into(),
            item_code: "".into(),
            item_name: "".into(),
            vehicle_number: "8504".into(),
            fuel_surcharge: 4_020,
            row_id: "20260621-1001".into(),
            input_staff_code: "0012".into(),
            input_staff_name: "西田　和恵".into(),
        },
        RawSurchargeRow {
            request_kind: "1".into(),
            customer_code: "000002".into(),
            customer_name: "㈱谷川商事".into(),
            origin_area_name: "".into(),
            dest_area_name: "".into(),
            vehicle_code: "00".into(),
            vehicle_name: "".into(),
            sale_date: dt(2026, 6, 20),
            fare: 840_000,
            billing_date: None,
            subcontractor_code: "001234".into(),
            item_code: "9003".into(),
            item_name: "消費税調整".into(),
            vehicle_number: "9481".into(),
            fuel_surcharge: 0,
            row_id: "20260620-1002".into(),
            input_staff_code: "".into(),
            input_staff_name: "".into(),
        },
    ];

    let rows = build_surcharge_rows(&raw);
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    assert_eq!(first.request_kind, "1");
    assert_eq!(first.customer_code, "000001");
    assert_eq!(first.customer_name, "㈱田浦畜産");
    assert_eq!(first.origin_prefecture, "長崎県");
    assert_eq!(first.dest_prefecture, "福岡県");
    assert_eq!(first.vehicle_code, "04");
    assert_eq!(first.vehicle_name, "大型幌");
    assert_eq!(first.sale_date, "2026-06-21");
    assert_eq!(first.fare, 65_000);
    assert_eq!(first.billing_date, Some("2026-07-31".to_string()));
    assert_eq!(first.subcontractor_code, "000000"); // 自車
    assert_eq!(first.fuel_surcharge, 4_020); // 割増C=19 分は fare と分離して保持
    assert_eq!(first.row_id, "20260621-1001"); // 行 ID = 管理年月日+管理C
    assert_eq!(first.input_staff_code, "0012"); // 入力担当C (入力者 絞り込み用)
    assert_eq!(first.input_staff_name, "西田　和恵"); // 社員ﾏｽﾀ.社員N (Refs #29)

    // エッジ: 未マップ地域 → "?"、入金予定日 NULL → None
    let second = &rows[1];
    assert_eq!(second.origin_prefecture, "?");
    assert_eq!(second.dest_prefecture, "?");
    assert_eq!(second.vehicle_name, "");
    assert_eq!(second.billing_date, None);
    assert_eq!(second.subcontractor_code, "001234"); // 傭車
    assert_eq!(second.fuel_surcharge, 0); // 燃料SC無し行は 0
    assert_eq!(second.row_id, "20260620-1002");
    assert_eq!(second.input_staff_code, ""); // 入力担当C 空欄行は空文字
    assert_eq!(second.input_staff_name, ""); // 未マップは空文字
}

#[test]
fn test_build_surcharge_rows_empty() {
    assert!(build_surcharge_rows(&[]).is_empty());
}

#[test]
fn test_surcharge_row_json_field_order() {
    // 応答の JSON はオンプレ版と同じフィールド名・並び (親が sha256 で比べる)
    let raw = vec![RawSurchargeRow {
        request_kind: "1".into(),
        customer_code: "c".into(),
        customer_name: "n".into(),
        origin_area_name: "".into(),
        dest_area_name: "".into(),
        vehicle_code: "v".into(),
        vehicle_name: "vn".into(),
        sale_date: dt(2026, 1, 2),
        fare: 7,
        billing_date: None,
        subcontractor_code: "000000".into(),
        item_code: "i".into(),
        item_name: "in".into(),
        vehicle_number: "1".into(),
        fuel_surcharge: 3,
        row_id: "r".into(),
        input_staff_code: "s".into(),
        input_staff_name: "sn".into(),
    }];
    let json = serde_json::to_string(&build_surcharge_rows(&raw)).unwrap();
    assert_eq!(
        json,
        "[{\"request_kind\":\"1\",\"customer_code\":\"c\",\"customer_name\":\"n\",\
         \"origin_prefecture\":\"?\",\"dest_prefecture\":\"?\",\"vehicle_code\":\"v\",\
         \"vehicle_name\":\"vn\",\"sale_date\":\"2026-01-02\",\"fare\":7,\"billing_date\":null,\
         \"subcontractor_code\":\"000000\",\"item_code\":\"i\",\"item_name\":\"in\",\
         \"vehicle_number\":\"1\",\"fuel_surcharge\":3,\"row_id\":\"r\",\
         \"input_staff_code\":\"s\",\"input_staff_name\":\"sn\"}]"
    );
}

// ══════════════════════════════════════════════════════════════
// Query → 値の決定 (オンプレ版ハンドラ冒頭の既定値・clamp・翌月計算)
// ══════════════════════════════════════════════════════════════

fn query(
    from: Option<&str>,
    to: Option<&str>,
    kind: Option<&str>,
    limit: Option<i32>,
) -> SurchargeQuery {
    SurchargeQuery {
        from: from.map(str::to_string),
        to: to.map(str::to_string),
        kind: kind.map(str::to_string),
        limit,
    }
}

#[test]
fn test_params_defaults() {
    let p = SurchargeQuery::default().params();
    assert_eq!(p.from_date, "2025-04-01");
    assert_eq!(p.to_date, "2026-04-01"); // 既定 to=2026-03 の翌月初日
    assert_eq!(p.kind_filter, "AND t.[請求K] = '1'");
    assert_eq!(p.limit, 2000);
    assert_eq!(
        p.source_table,
        "運転日報明細 + 得意先ﾏｽﾀ + 車種ﾏｽﾀ + 地域ﾏｽﾀ [請求のみ (請求K=1)]"
    );
}

#[test]
fn test_params_all_given() {
    let p = query(
        Some("2026-04"),
        Some("2026-06"),
        Some("transport"),
        Some(500),
    )
    .params();
    assert_eq!(p.from_date, "2026-04-01");
    assert_eq!(p.to_date, "2026-07-01");
    assert_eq!(p.kind_filter, "AND t.[請求K] = '0'");
    assert_eq!(p.limit, 500);
    assert_eq!(
        p.source_table,
        "運転日報明細 + 得意先ﾏｽﾀ + 車種ﾏｽﾀ + 地域ﾏｽﾀ [通常運送 (請求K=0)]"
    );
}

#[test]
fn test_params_year_rollover_and_kind_all() {
    let p = query(Some("2026-12"), Some("2026-12"), Some("all"), None).params();
    assert_eq!(p.from_date, "2026-12-01");
    assert_eq!(p.to_date, "2027-01-01"); // calc_next_month の年跨ぎ
    assert_eq!(p.kind_filter, "");
    assert_eq!(
        p.source_table,
        "運転日報明細 + 得意先ﾏｽﾀ + 車種ﾏｽﾀ + 地域ﾏｽﾀ [全請求区分]"
    );
}

#[test]
fn test_params_malformed_to_falls_back() {
    // to に月が無い / parse 不能 → 2026 年 3 月のフォールバック (オンプレ版 unwrap_or と同じ)
    assert_eq!(
        query(None, Some("xxxx"), None, None).params().to_date,
        "2026-04-01"
    );
    assert_eq!(
        query(None, Some("2026-xx"), None, None).params().to_date,
        "2026-04-01"
    );
    assert_eq!(
        query(None, Some("2024-05"), None, None).params().to_date,
        "2024-06-01"
    );
}

#[test]
fn test_params_limit_is_clamped() {
    assert_eq!(query(None, None, None, Some(0)).params().limit, 1);
    assert_eq!(query(None, None, None, Some(-5)).params().limit, 1);
    assert_eq!(query(None, None, None, Some(99_999)).params().limit, 10_000);
}

// ══════════════════════════════════════════════════════════════
// SQL の組み立て
// ══════════════════════════════════════════════════════════════

#[test]
fn test_surcharge_sql_top_and_kind() {
    let sql = surcharge_sql(surcharge_kind_filter("billing_only"), 2000);
    assert!(sql.starts_with("SELECT TOP 2000 t.[請求K], t.[得意先C], "));
    assert!(sql.contains(
        "< @P2 AND t.[請求K] = '1' ORDER BY t.[入金予定日], t.[得意先C], t.[売上年月日]"
    ));
    // 運賃は金額 + 割増 + 実費 のまま (税抜カラムに書き換えない)
    assert!(sql.contains("ISNULL(t.[金額], 0) + ISNULL(t.[割増], 0) + ISNULL(t.[実費], 0)"));
    assert!(sql.contains("FROM [運転日報明細] t WHERE t.[売上年月日] >= @P1"));
}

#[test]
fn test_surcharge_sql_all_has_no_kind_clause() {
    let sql = surcharge_sql(surcharge_kind_filter("all"), 10);
    assert!(sql.starts_with("SELECT TOP 10 "));
    assert!(!sql.contains("[請求K] = '"));
    assert!(sql.contains("< @P2  ORDER BY"));
}

#[test]
fn test_surcharge_sql_top_is_clamped() {
    assert!(surcharge_sql("", 0).starts_with("SELECT TOP 1 "));
    assert!(surcharge_sql("", 50_000).starts_with("SELECT TOP 10000 "));
}
