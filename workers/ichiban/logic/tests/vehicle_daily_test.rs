//! `/api/sales/vehicle-daily` の純粋部分 (オンプレ版の tests/vehicle_daily_test.rs から移した)。

use chrono::{NaiveDate, NaiveDateTime};
use ichiban_logic::vehicle_daily::{
    build_vehicle_daily_rows, RawVehicleDailyRow, VehicleDailyFilters, VehicleDailyQuery,
};

fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

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
// Query: 絞り込みの正規化・limit の丸め・400 判定
// ══════════════════════════════════════════════════════════════

fn query(json: &str) -> VehicleDailyQuery {
    serde_json::from_str(json).unwrap()
}

#[test]
fn test_filters_none_when_no_filter() {
    // 日付レンジだけ → 400 (全件スキャン防止)
    let q = query(r#"{"from":"2026-06-01","to":"2026-07-01"}"#);
    assert_eq!(q.from, "2026-06-01");
    assert_eq!(q.to, "2026-07-01");
    assert_eq!(q.filters(), None);
}

#[test]
fn test_filters_none_when_all_blank() {
    // 空白だけの値は絞り込みなし扱い → 400
    let q = query(
        r#"{"from":"a","to":"b","vehicle":" ","driver":"","customer":"  ","origin":"\t","dest":""}"#,
    );
    assert_eq!(q.filters(), None);
}

#[test]
fn test_filters_each_alone_is_enough() {
    for key in ["vehicle", "driver", "customer", "origin", "dest"] {
        let q = query(&format!(r#"{{"from":"a","to":"b","{key}":" x "}}"#));
        let f = q.filters().expect(key);
        let got = [f.vehicle, f.driver, f.customer, f.origin, f.dest];
        // trim されて 1 つだけ入る
        assert_eq!(got.iter().filter(|v| **v == Some("x")).count(), 1, "{key}");
        assert_eq!(got.iter().filter(|v| v.is_none()).count(), 4, "{key}");
        assert_eq!(f.limit, 500);
    }
}

#[test]
fn test_filters_limit_clamped() {
    let f = |limit: &str| {
        let q = query(&format!(
            r#"{{"from":"a","to":"b","vehicle":"8504","limit":{limit}}}"#
        ));
        q.filters().unwrap().limit
    };
    assert_eq!(f("0"), 1);
    assert_eq!(f("-3"), 1);
    assert_eq!(f("20"), 20);
    assert_eq!(f("5000"), 5000);
    assert_eq!(f("5001"), 5000);
}

#[test]
fn test_filters_all_fields() {
    let q = query(
        r#"{"from":"a","to":"b","vehicle":"8504","driver":"1656","customer":"000001","origin":"釧路","dest":"横浜","limit":100}"#,
    );
    assert_eq!(
        q.filters(),
        Some(VehicleDailyFilters {
            vehicle: Some("8504"),
            driver: Some("1656"),
            customer: Some("000001"),
            origin: Some("釧路"),
            dest: Some("横浜"),
            limit: 100,
        })
    );
}

#[test]
fn test_response_row_json_field_order() {
    // オンプレ版と Worker の応答を sha256 で比べるので、フィールド名と順序を固定する
    let raw = RawVehicleDailyRow {
        sale_date: dt(2026, 6, 21),
        vehicle_number: "8504".into(),
        customer_code: "000001".into(),
        customer_name: "c".into(),
        origin_area_name: "oa".into(),
        dest_area_name: "da".into(),
        origin: "o".into(),
        dest: "d".into(),
        subcontractor_code: "000000".into(),
        self_amount: 1,
        subcontract_amount: 2,
        item_code: "ic".into(),
        item_name: "in".into(),
        quantity: 1.5,
        unit_price: 2.0,
        unit: "u".into(),
        row_id: "r".into(),
        vehicle_branch: "01".into(),
        driver_code: "1656".into(),
        driver_name: "n".into(),
        request_kind: "0".into(),
    };
    let json = serde_json::to_string(&build_vehicle_daily_rows(&[raw])).unwrap();
    assert_eq!(
        json,
        r#"[{"sale_date":"2026-06-21","vehicle_number":"8504","customer_code":"000001","customer_name":"c","origin_area_name":"oa","dest_area_name":"da","origin":"o","dest":"d","is_subcontracted":false,"amount":1,"item_code":"ic","item_name":"in","quantity":1.5,"unit_price":2.0,"unit":"u","row_id":"r","vehicle_branch":"01","driver_code":"1656","driver_name":"n","request_kind":"0"}]"#
    );
}
