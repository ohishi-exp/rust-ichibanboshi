mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::unchin::{
    build_unchin_customer_net_detail_rows, build_unchin_customer_net_rows, build_unchin_rows,
    build_unchin_subcontractor_net_detail_rows, build_unchin_subcontractor_net_rows,
    build_unchin_summary_rows, normalize_partner_type, unchin_kind_filter, unchin_kind_label,
    RawUnchinCustomerNetDetailRow, RawUnchinCustomerNetRow, RawUnchinRow,
    RawUnchinSubcontractorNetDetailRow, RawUnchinSubcontractorNetRow, RawUnchinSummaryRow,
};
use tower::ServiceExt;

use common::dt;

// ══════════════════════════════════════════════════════════════
// 純粋関数: normalize_partner_type
// ══════════════════════════════════════════════════════════════

#[test]
fn test_normalize_partner_type_subcontractor() {
    assert_eq!(normalize_partner_type("subcontractor"), "subcontractor");
}

#[test]
fn test_normalize_partner_type_customer_and_fallback() {
    assert_eq!(normalize_partner_type("customer"), "customer");
    // 未知の値・空文字は customer にフォールバック
    assert_eq!(normalize_partner_type(""), "customer");
    assert_eq!(normalize_partner_type("xxx"), "customer");
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: unchin_kind_filter / unchin_kind_label
// ══════════════════════════════════════════════════════════════

#[test]
fn test_unchin_kind_filter() {
    assert_eq!(
        unchin_kind_filter("with_billing_only"),
        "AND t.[請求K] IN ('0', '1')"
    );
    // 未知の値・default は with_non_billing (請求K IN (0,2)) にフォールバック
    assert_eq!(
        unchin_kind_filter("with_non_billing"),
        "AND t.[請求K] IN ('0', '2')"
    );
    assert_eq!(unchin_kind_filter(""), "AND t.[請求K] IN ('0', '2')");
    assert_eq!(unchin_kind_filter("xxx"), "AND t.[請求K] IN ('0', '2')");
}

#[test]
fn test_unchin_kind_label() {
    assert_eq!(
        unchin_kind_label("with_billing_only"),
        "請求＋請求のみ (請求K IN (0,1))"
    );
    assert_eq!(
        unchin_kind_label("with_non_billing"),
        "請求＋非請求 (請求K IN (0,2))"
    );
    assert_eq!(unchin_kind_label("xxx"), "請求＋非請求 (請求K IN (0,2))");
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_unchin_rows
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
        },
        // エッジ: 空品名コード・空積地
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

    let second = &rows[1];
    assert_eq!(second.item_code, "0000");
    assert_eq!(second.item_name, "");
    assert_eq!(second.origin, "");
    assert_eq!(second.dest, "福岡県北九州市");
    assert_eq!(second.bumon_code, "");
    assert_eq!(second.bumon_name, "");
}

#[test]
fn test_build_unchin_rows_empty() {
    assert!(build_unchin_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_unchin_summary_rows
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
}

#[test]
fn test_build_unchin_summary_rows_empty() {
    assert!(build_unchin_summary_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_unchin_subcontractor_net_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_subcontractor_net_rows_positive_diff() {
    // 得意先請求合計 > 傭車先支払合計 (通常ケース、儲けが出ている)
    let raw = vec![RawUnchinSubcontractorNetRow {
        partner_code: "021970-000".into(),
        partner_name: "㈱九州運輸".into(),
        total_sales: 40_000,
        total_payment: 28_000,
        bumon_code: "012".into(),
        bumon_name: "佐賀".into(),
    }];
    let rows = build_unchin_subcontractor_net_rows(&raw);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].partner_code, "021970-000");
    assert_eq!(rows[0].partner_name, "㈱九州運輸");
    assert_eq!(rows[0].total_sales, 40_000);
    assert_eq!(rows[0].total_payment, 28_000);
    assert_eq!(rows[0].diff, 12_000);
    assert_eq!(rows[0].bumon_code, "012");
    assert_eq!(rows[0].bumon_name, "佐賀");
}

#[test]
fn test_build_unchin_subcontractor_net_rows_negative_diff() {
    // 傭車先支払合計 > 得意先請求合計 (逆ざや、負の差額もそのまま出す)
    let raw = vec![RawUnchinSubcontractorNetRow {
        partner_code: "099999-000".into(),
        partner_name: "逆ざやテスト運輸".into(),
        total_sales: 10_000,
        total_payment: 15_000,
        bumon_code: "".into(),
        bumon_name: "".into(),
    }];
    let rows = build_unchin_subcontractor_net_rows(&raw);
    assert_eq!(rows[0].diff, -5_000);
}

#[test]
fn test_build_unchin_subcontractor_net_rows_empty() {
    assert!(build_unchin_subcontractor_net_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_unchin_subcontractor_net_detail_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_unchin_subcontractor_net_detail_rows_positive_diff() {
    let raw = vec![RawUnchinSubcontractorNetDetailRow {
        item_code: "6301".into(),
        item_name: "フレコン".into(),
        customer_name: "㈱九州テスト物産".into(),
        sales: 28_000,
        payment: 20_000,
        origin: "鳥栖".into(),
        dest: "大石運輸  本社".into(),
        sale_date: dt(2026, 6, 18),
        bumon_code: "012".into(),
        bumon_name: "佐賀".into(),
    }];
    let rows = build_unchin_subcontractor_net_detail_rows(&raw);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].item_code, "6301");
    assert_eq!(rows[0].item_name, "フレコン");
    assert_eq!(rows[0].customer_name, "㈱九州テスト物産");
    assert_eq!(rows[0].sales, 28_000);
    assert_eq!(rows[0].payment, 20_000);
    assert_eq!(rows[0].diff, 8_000);
    assert_eq!(rows[0].origin, "鳥栖");
    assert_eq!(rows[0].dest, "大石運輸  本社");
    assert_eq!(rows[0].sale_date, "2026-06-18");
    assert_eq!(rows[0].bumon_code, "012");
    assert_eq!(rows[0].bumon_name, "佐賀");
}

#[test]
fn test_build_unchin_subcontractor_net_detail_rows_negative_diff() {
    let raw = vec![RawUnchinSubcontractorNetDetailRow {
        item_code: "0000".into(),
        item_name: "".into(),
        customer_name: "".into(),
        sales: 5_000,
        payment: 9_000,
        origin: "".into(),
        dest: "".into(),
        sale_date: dt(2026, 1, 1),
        bumon_code: "".into(),
        bumon_name: "".into(),
    }];
    let rows = build_unchin_subcontractor_net_detail_rows(&raw);
    assert_eq!(rows[0].diff, -4_000);
}

#[test]
fn test_build_unchin_subcontractor_net_detail_rows_empty() {
    assert!(build_unchin_subcontractor_net_detail_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_unchin_customer_net_rows
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
}

#[test]
fn test_build_unchin_customer_net_rows_own_fleet_only_diff_equals_sales() {
    // 自社便のみの得意先は total_payment=0 → diff = total_sales
    let raw = vec![RawUnchinCustomerNetRow {
        partner_code: "099999-000".into(),
        partner_name: "自社便のみテスト".into(),
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
// 純粋関数: build_unchin_customer_net_detail_rows
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
}

#[test]
fn test_build_unchin_customer_net_detail_rows_own_fleet_empty_subcontractor_name() {
    // 自社便の行は 傭車先C='000000' がマスタに存在しないため subcontractor_name が空文字
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
    assert_eq!(rows[0].subcontractor_name, "");
    assert_eq!(rows[0].diff, 15_000);
}

#[test]
fn test_build_unchin_customer_net_detail_rows_empty() {
    assert!(build_unchin_customer_net_detail_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/candidates
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_candidates_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_ok_customer_with_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates?from=2026-04-01&to=2026-07-01&partner_type=customer&kind=with_billing_only")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_ok_subcontractor() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates?partner_type=subcontractor&kind=with_non_billing")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_ok_unknown_partner_type_and_kind_fall_back() {
    // partner_type / kind が不正な値 → fallback して 200 を返す
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates?partner_type=xxx&kind=xxx")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_candidates_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_candidates_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/candidates")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/summary
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_summary_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_summary_ok_subcontractor_with_kind() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/summary?partner_type=subcontractor&kind=with_billing_only&from=2026-01-01&to=2027-01-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_summary_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_summary_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/summary")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/subcontractor-net
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_subcontractor_net_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_ok_with_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net?from=2026-04-01&to=2026-07-01&kind=with_billing_only")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/subcontractor-net-detail
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_subcontractor_net_detail_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net-detail?code=021970&h=000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_detail_ok_with_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net-detail?from=2026-04-01&to=2026-07-01&kind=with_billing_only&code=021970&h=000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_detail_missing_code_returns_400() {
    // code/h は必須 query param (Deserialize が失敗 → axum が 400 Bad Request を返す)
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net-detail")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_detail_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net-detail?code=021970&h=000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_subcontractor_net_detail_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/subcontractor-net-detail?code=021970&h=000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/customer-net
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_customer_net_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_customer_net_ok_with_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri(
                    "/api/unchin/customer-net?from=2026-04-01&to=2026-07-01&kind=with_billing_only",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_customer_net_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_customer_net_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/unchin/customer-net-detail
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_unchin_customer_net_detail_ok_defaults() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net-detail?code=034760&h=015")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_customer_net_detail_ok_with_params() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net-detail?from=2026-04-01&to=2026-07-01&kind=with_billing_only&code=034760&h=015")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_unchin_customer_net_detail_missing_code_returns_400() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net-detail")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_unchin_customer_net_detail_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net-detail?code=034760&h=015")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_unchin_customer_net_detail_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/unchin/customer-net-detail?code=034760&h=015")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
