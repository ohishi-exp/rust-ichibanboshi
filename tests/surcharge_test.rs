mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::surcharge::{
    build_surcharge_rows, normalize_prefecture, surcharge_kind_filter, surcharge_kind_label,
    RawSurchargeRow,
};
use tower::ServiceExt;

use common::dt;

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

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/surcharge/base
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_surcharge_base_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/surcharge/base")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_surcharge_base_ok_with_all_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/surcharge/base?from=2026-04&to=2026-06&kind=transport&limit=500")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_surcharge_base_ok_kind_all_and_year_rollover() {
    // to=YYYY-12 で calc_next_month の年跨ぎ分岐を踏む
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/surcharge/base?from=2026-12&to=2026-12&kind=all")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_surcharge_base_ok_malformed_to_falls_back() {
    // to に月が無い / parse 不能 → unwrap_or のフォールバックを踏む
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/surcharge/base?to=xxxx&limit=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_surcharge_base_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/surcharge/base")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_surcharge_base_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/surcharge/base")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/vehicles (車種ﾏｽﾀ)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vehicles_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/vehicles")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
