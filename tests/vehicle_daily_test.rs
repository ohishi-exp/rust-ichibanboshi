mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::vehicle_daily::{build_vehicle_daily_rows, RawVehicleDailyRow};
use tower::ServiceExt;

use common::dt;

// ══════════════════════════════════════════════════════════════
// 純粋関数: build_vehicle_daily_rows
// ══════════════════════════════════════════════════════════════

#[test]
fn test_build_vehicle_daily_rows_self_and_subcontract() {
    let raw = vec![
        // 自車 (傭車先C='000000') → self_amount を使う。origin_area_name は #12 実機調査の
        // 例 (神奈川県横浜市、市区町村レベル)。
        RawVehicleDailyRow {
            sale_date: dt(2026, 6, 21),
            vehicle_number: "8504".into(),
            customer_code: "000001".into(),
            customer_name: "㈱田浦畜産".into(),
            origin_area_name: "長崎県".into(),
            dest_area_name: "神奈川県横浜市".into(),
            origin: "釧路".into(),
            dest: "福岡県北九州市".into(),
            subcontractor_code: "000000".into(),
            self_amount: 65_000,
            subcontract_amount: 999_999, // 自車なので使われないはず
            item_code: "0001".into(),
            item_name: "冷凍食品".into(),
            quantity: 10.5,
            unit_price: 6190.47, // 単価は decimal で端数を持ちうる (実データ検証で確認済み)
            unit: "個".into(),
            row_id: "20260621-1001".into(),
            vehicle_branch: "01".into(),
            driver_code: "1656".into(),
            driver_name: "西島 健太".into(),
            request_kind: "0".into(),
        },
        // 傭車 (傭車先C!='000000') → subcontract_amount を使う。品名/数量/単価/単位も未入力のエッジ。
        RawVehicleDailyRow {
            sale_date: dt(2026, 6, 20),
            vehicle_number: "8504".into(),
            customer_code: "000002".into(),
            customer_name: "".into(),
            origin_area_name: "".into(),
            dest_area_name: "".into(),
            origin: "".into(),
            dest: "".into(),
            subcontractor_code: "001234".into(),
            self_amount: 999_999, // 傭車なので使われないはず
            subcontract_amount: 40_000,
            item_code: "".into(),
            item_name: "".into(),
            quantity: 0.0,
            unit_price: 0.0,
            unit: "".into(),
            row_id: "20260620-1002".into(),
            // 枝番・乗務員CD が空のエッジ (実データにも空行がある)。
            vehicle_branch: "".into(),
            driver_code: "".into(),
            driver_name: "".into(),
            // 請求区分が空のエッジ (ISNULL の既定値)。
            request_kind: "".into(),
        },
    ];

    let rows = build_vehicle_daily_rows(&raw);
    assert_eq!(rows.len(), 2);

    let first = &rows[0];
    assert_eq!(first.sale_date, "2026-06-21");
    assert_eq!(first.vehicle_number, "8504");
    assert_eq!(first.customer_code, "000001");
    assert_eq!(first.customer_name, "㈱田浦畜産");
    assert_eq!(first.origin_area_name, "長崎県");
    assert_eq!(first.dest_area_name, "神奈川県横浜市");
    assert_eq!(first.origin, "釧路");
    assert_eq!(first.dest, "福岡県北九州市");
    assert!(!first.is_subcontracted);
    assert_eq!(first.amount, 65_000);
    assert_eq!(first.item_code, "0001");
    assert_eq!(first.item_name, "冷凍食品");
    assert_eq!(first.quantity, 10.5);
    assert_eq!(first.unit_price, 6190.47);
    assert_eq!(first.unit, "個");
    assert_eq!(first.row_id, "20260621-1001");
    // 車番の枝番と乗務員CD (#741: 車番だけでは車輌も乗務員も一意に指せない)
    assert_eq!(first.vehicle_branch, "01");
    assert_eq!(first.driver_code, "1656");
    // `社員ﾏｽﾀ.社員N` 由来の表示名 (`運転日報明細.乗務員N` は自由入力でほぼ空)。
    assert_eq!(first.driver_name, "西島 健太");
    // 請求区分はそのまま返す (どれを収支に入れるかは消費側の判断)
    assert_eq!(first.request_kind, "0");

    let second = &rows[1];
    assert_eq!(second.sale_date, "2026-06-20");
    assert!(second.is_subcontracted);
    assert_eq!(second.vehicle_branch, "");
    assert_eq!(second.driver_code, "");
    assert_eq!(second.driver_name, "");
    assert_eq!(second.request_kind, "");
    assert_eq!(second.amount, 40_000);
    // 積地・卸地・得意先名は空文字のまま passthrough (surcharge_base 同様に県正規化しない)
    assert_eq!(second.origin_area_name, "");
    assert_eq!(second.dest_area_name, "");
    assert_eq!(second.origin, "");
    assert_eq!(second.dest, "");
    assert_eq!(second.customer_name, "");
    // 品名/数量/単価/単位が未入力の明細は 0/空文字で passthrough (ISNULLの既定値と一致)
    assert_eq!(second.item_code, "");
    assert_eq!(second.item_name, "");
    assert_eq!(second.quantity, 0.0);
    assert_eq!(second.unit_price, 0.0);
    assert_eq!(second.unit, "");
}

#[test]
fn test_build_vehicle_daily_rows_empty() {
    assert!(build_vehicle_daily_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/sales/vehicle-daily
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_vehicle_daily_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

/// **請求区分をそのまま返す** (2026-08-22)。`請求K=1` は「請求のみ」= 運送を伴わない
/// 請求行で、車輌収支に足すと二重計上になる。実データ (2026-07) では中継の通し請求
/// `釧路 → ユナイテッド牧場 ¥43,750` (請求K=1) と、実際に走った 2 本
/// `釧路 → 駒場 ¥21,750` + `駒場 → ユナイテッド牧場 ¥22,000` (どちらも請求K=2) が
/// 同じ荷の表裏になっていた。**絞り込みはせず、消費側が判断できるように出す。**
#[tokio::test]
async fn test_vehicle_daily_returns_request_kind() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&customer=000001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let kinds: Vec<&str> = json["data"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["request_kind"].as_str().unwrap())
        .collect();
    // 得意先 000001 の 2 行 = 通常運送 (請求K=0) と 請求のみ (請求K=1)。どちらも落ちない。
    assert_eq!(kinds, vec!["0", "1"]);
}

#[tokio::test]
async fn test_vehicle_daily_ok_with_limit() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_vehicle_daily_no_filters_is_bad_request() {
    // vehicle/customer/origin/dest が 1 つも無い → ハンドラ側の絞り込み必須チェックで 400
    // (#79: vehicle は任意化されたため、以前と違い axum の Query extractor では拒否されない)
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_vehicle_daily_blank_filters_is_bad_request() {
    // 全パラメータはあるが空文字/空白のみ → trim().is_empty() で無視され結局 0 件、400
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri(
                    "/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01\
                     &vehicle=%20&driver=%20&customer=&origin=&dest=",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_vehicle_daily_customer_only_searches_across_vehicles() {
    // #79 の主目的: vehicle を指定せず customer だけで車輌を横断して検索できること
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&customer=000001")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let data = json["data"].as_array().unwrap();
    // customer=000001 は車輌 8504 と 9012 の両方に存在する (mock フィクスチャ)
    assert_eq!(data.len(), 2);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
}

#[tokio::test]
async fn test_vehicle_daily_driver_only_searches_across_vehicles() {
    // #741 の主目的: 同じ乗務員の売上が日によって別の車番に載る (デジタコを積んで
    // いない車輌等) ため、**車番ではなく乗務員CD で横断して引けること**。
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&driver=1656")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let data = json["data"].as_array().unwrap();
    // 乗務員 1656 は車輌 8504 と 9012 の両方で走っている (mock フィクスチャ)
    assert_eq!(data.len(), 2);
    let vehicles: std::collections::HashSet<_> = data
        .iter()
        .map(|r| r["vehicle_number"].as_str().unwrap())
        .collect();
    assert!(vehicles.contains("8504"));
    assert!(vehicles.contains("9012"));
    assert_eq!(data[0]["driver_code"], "1656");
}

#[tokio::test]
async fn test_vehicle_daily_origin_partial_match() {
    // origin は地域ﾏｽﾀ由来 (origin_area_name) と自由入力 (origin) のいずれかへの部分一致
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&origin=%E9%95%B7%E5%B4%8E")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let data = json["data"].as_array().unwrap();
    // "長崎" は "長崎県" (row1) と "長崎県佐世保市" (row3) の両方に部分一致する
    assert_eq!(data.len(), 2);
}

#[tokio::test]
async fn test_vehicle_daily_pool_error() {
    let app = common::build_app(common::error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_vehicle_daily_query_error() {
    let app = common::build_app(common::query_error_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/sales/vehicle-daily?from=2026-06-01&to=2026-07-01&vehicle=8504")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
}
