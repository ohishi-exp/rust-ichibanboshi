mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::costs_daily::{build_costs_daily_rows, RawCostsDailyRow};
use tower::ServiceExt;

use common::dt;

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_costs_daily_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_costs_daily_rows_variable_and_fixed() {
    let raw = vec![
        // 変動費 (燃料)。軽油引取税は 税抜金額 に含まれない別立ての税なので独立に返る。
        RawCostsDailyRow {
            operation_date: dt(2026, 6, 21),
            vehicle_number: "8504".into(),
            vehicle_branch: "01".into(),
            driver_code: "1656".into(),
            cost_code: "0101".into(),
            cost_name: "軽油".into(),
            cost_kind: "01".into(),
            cost_kind_name: "燃料費".into(),
            quantity: 150.5,
            unit_price: 128.5, // 単価は decimal で端数を持ちうる
            amount: 19_339,
            diesel_tax: 4_830,
            km: 12_345.6,
            fixed_cost_flag: "0".into(),
            row_id: "20260621-2001".into(),
        },
        // 固定経費 (固定経費K="1")。月極めなので乗務員が紐付かず、数量/単価/KM も 0。
        RawCostsDailyRow {
            operation_date: dt(2026, 6, 1),
            vehicle_number: "8504".into(),
            vehicle_branch: "01".into(),
            driver_code: "".into(),
            cost_code: "0901".into(),
            cost_name: "自動車保険料".into(),
            cost_kind: "09".into(),
            cost_kind_name: "保険料".into(),
            quantity: 0.0,
            unit_price: 0.0,
            amount: 45_000,
            diesel_tax: 0,
            km: 0.0,
            fixed_cost_flag: "1".into(),
            row_id: "20260601-2003".into(),
        },
        // 区分も名前も空 (ISNULL の既定値) のエッジ。空文字は固定経費ではない。
        RawCostsDailyRow {
            operation_date: dt(2026, 6, 22),
            vehicle_number: "9012".into(),
            vehicle_branch: "".into(),
            driver_code: "1656".into(),
            cost_code: "".into(),
            cost_name: "".into(),
            cost_kind: "".into(),
            cost_kind_name: "".into(),
            quantity: 0.0,
            unit_price: 0.0,
            amount: 0,
            diesel_tax: 0,
            km: 0.0,
            fixed_cost_flag: "".into(),
            row_id: "20260622-2004".into(),
        },
    ];

    let rows = build_costs_daily_rows(&raw);
    assert_eq!(rows.len(), 3);

    let fuel = &rows[0];
    assert_eq!(fuel.operation_date, "2026-06-21");
    assert_eq!(fuel.vehicle_number, "8504");
    // 車番の枝番 (#302: 車輌C だけでは車輌を一意に指せない)
    assert_eq!(fuel.vehicle_branch, "01");
    assert_eq!(fuel.driver_code, "1656");
    assert_eq!(fuel.cost_code, "0101");
    assert_eq!(fuel.cost_name, "軽油");
    assert_eq!(fuel.cost_kind, "01");
    assert_eq!(fuel.cost_kind_name, "燃料費");
    assert_eq!(fuel.quantity, 150.5);
    assert_eq!(fuel.unit_price, 128.5);
    // 税抜金額 (金額 は使わない。vehicle_daily の売上が税抜で揃っているため)
    assert_eq!(fuel.amount, 19_339);
    assert_eq!(fuel.diesel_tax, 4_830);
    assert_eq!(fuel.km, 12_345.6);
    assert!(!fuel.is_fixed);
    assert_eq!(fuel.row_id, "20260621-2001");

    let fixed = &rows[1];
    assert_eq!(fixed.operation_date, "2026-06-01");
    // 固定経費K="1" → is_fixed。按分するか外すかは消費側の判断
    assert!(fixed.is_fixed);
    assert_eq!(fixed.cost_kind, "09");
    assert_eq!(fixed.cost_kind_name, "保険料");
    assert_eq!(fixed.amount, 45_000);
    assert_eq!(fixed.driver_code, "");
    assert_eq!(fixed.quantity, 0.0);
    assert_eq!(fixed.unit_price, 0.0);
    assert_eq!(fixed.km, 0.0);
    assert_eq!(fixed.diesel_tax, 0);

    let blank = &rows[2];
    // 空文字の 固定経費K は変動費扱い ("1" 以外は全て false)
    assert!(!blank.is_fixed);
    assert_eq!(blank.vehicle_branch, "");
    assert_eq!(blank.cost_code, "");
    assert_eq!(blank.cost_name, "");
    assert_eq!(blank.cost_kind, "");
    assert_eq!(blank.cost_kind_name, "");
    assert_eq!(blank.amount, 0);
    assert_eq!(blank.row_id, "20260622-2004");
}

#[test]
fn test_build_costs_daily_rows_empty() {
    assert!(build_costs_daily_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/costs/vehicle-daily
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_costs_daily_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    assert_eq!(json["source_table"], "経費明細 + 経費ﾏｽﾀ + 経費種別ﾏｽﾀ");
    // 車輌 8504 は 燃料 / 通行料 / 固定経費 の 3 行 (mock フィクスチャ)
    assert_eq!(json["data"].as_array().unwrap().len(), 3);
}

/// 消費側 (運行手当タブ) が種別で内訳を分けるため、**燃料と通行料が種別付きで
/// 揃って返る**こと。片方でも落ちると粗利の内訳が出せない。
#[tokio::test]
async fn test_costs_daily_returns_fuel_and_toll_kinds() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let kinds: Vec<&str> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["cost_kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"01")); // 燃料
    assert!(kinds.contains(&"04")); // 通行料
                                    // 固定経費は is_fixed=true で 1 行だけ (按分の判断材料)
    let fixed: Vec<&serde_json::Value> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|r| r["is_fixed"].as_bool().unwrap())
        .collect();
    assert_eq!(fixed.len(), 1);
    assert_eq!(fixed[0]["cost_kind"], "09");
}

#[tokio::test]
async fn test_costs_daily_kind_filter() {
    // kind だけの絞り込み (vehicle/driver 無し) でも 200。燃料は 2 車輌にまたがる
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&kind=01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let data = json["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
}

#[tokio::test]
async fn test_costs_daily_driver_only_searches_across_vehicles() {
    // 同じ乗務員の経費が日によって別の車番に載る (#741 と同じ形) ため、
    // **車番ではなく乗務員CD で横断して引けること**
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&driver=1656")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let json = body_json(res).await;
    let data = json["data"].as_array().unwrap();
    // 乗務員 1656 は 燃料(8504) / 通行料(8504) / 燃料(9012) の 3 行。
    // 固定経費は乗務員が紐付かない (driver_code="") ので落ちる
    assert_eq!(data.len(), 3);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
}

#[tokio::test]
async fn test_costs_daily_ok_with_limit() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_costs_daily_no_filters_is_bad_request() {
    // vehicle/driver/kind が 1 つも無い → 全件スキャン防止のチェックで 400
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_costs_daily_blank_filters_is_bad_request() {
    // 全パラメータはあるが空文字/空白のみ → trim().is_empty() で無視され結局 0 件、400
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri(
                    "/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01\
                     &vehicle=%20&driver=&kind=",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_costs_daily_driver_only_is_ok() {
    // 必須チェックの 2 つ目の枝: vehicle が無くても driver があれば通る
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&driver=1656")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_costs_daily_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_costs_daily_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/costs/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}

async fn body_json(res: axum::response::Response) -> serde_json::Value {
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&body).unwrap()
}
