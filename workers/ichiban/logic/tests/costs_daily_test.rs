//! `/api/costs/vehicle-daily` の純粋部分 (オンプレ版の tests/costs_daily_test.rs から移した)。

use chrono::{NaiveDate, NaiveDateTime};
use ichiban_logic::costs_daily::{
    build_costs_daily_rows, CostsDailyFilters, CostsDailyQuery, RawCostsDailyRow,
};

fn dt(y: i32, m: u32, d: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

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
            remarks: "".into(),
            vendor_code: "".into(),
            vendor_branch: "".into(),
            vendor_name: "".into(),
            entered_date: None,
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
            remarks: "".into(),
            vendor_code: "".into(),
            vendor_branch: "".into(),
            vendor_name: "".into(),
            entered_date: None,
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
            remarks: "".into(),
            vendor_code: "".into(),
            vendor_branch: "".into(),
            vendor_name: "".into(),
            entered_date: None,
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

/// #760-11: 備考 / 未払先 / 入力日 の詰め替え。粗利タブで突出した直課経費が
/// 「何の修理か・どこに払ったか」を読めるようにする追加フィールド。
#[test]
fn test_build_costs_daily_rows_remarks_vendor_entered_date() {
    let raw = vec![
        // 一般修理費。備考・未払先名・入力日が全部入っている行。
        RawCostsDailyRow {
            operation_date: dt(2026, 7, 13),
            vehicle_number: "1420".into(),
            vehicle_branch: "00".into(),
            driver_code: "1234".into(),
            cost_code: "0631".into(),
            cost_name: "一般修理費".into(),
            cost_kind: "02".into(),
            cost_kind_name: "修繕費".into(),
            quantity: 1.0,
            unit_price: 206_060.0,
            amount: 206_060,
            diesel_tax: 0,
            km: 0.0,
            fixed_cost_flag: "0".into(),
            row_id: "20260713-3001".into(),
            remarks: "ミッション載せ替え".into(),
            vendor_code: "001122".into(),
            vendor_branch: "01".into(),
            vendor_name: "○○自動車整備".into(),
            entered_date: Some(dt(2026, 7, 15)),
        },
        // 備考 NULL (DB 層の ISNULL で空文字)、未払先ﾏｽﾀ に無い (名前 空)、入力日 NULL。
        RawCostsDailyRow {
            operation_date: dt(2026, 7, 13),
            vehicle_number: "1420".into(),
            vehicle_branch: "00".into(),
            driver_code: "1234".into(),
            cost_code: "0631".into(),
            cost_name: "一般修理費".into(),
            cost_kind: "02".into(),
            cost_kind_name: "修繕費".into(),
            quantity: 1.0,
            unit_price: 12_000.0,
            amount: 12_000,
            diesel_tax: 0,
            km: 0.0,
            fixed_cost_flag: "0".into(),
            row_id: "20260713-3002".into(),
            remarks: "".into(),
            vendor_code: "999999".into(),
            vendor_branch: "00".into(),
            vendor_name: "".into(),
            entered_date: None,
        },
    ];

    let rows = build_costs_daily_rows(&raw);
    assert_eq!(rows.len(), 2);

    let full = &rows[0];
    assert_eq!(full.remarks, "ミッション載せ替え");
    assert_eq!(full.vendor_code, "001122");
    assert_eq!(full.vendor_branch, "01");
    assert_eq!(full.vendor_name, "○○自動車整備");
    // 入力年月日 は YYYY-MM-DD (運行年月日 と同じ整形)
    assert_eq!(full.entered_date, "2026-07-15");
    // 既存フィールドは不変
    assert_eq!(full.operation_date, "2026-07-13");
    assert_eq!(full.amount, 206_060);
    assert_eq!(full.row_id, "20260713-3001");

    let blank = &rows[1];
    // 備考 NULL → 空文字
    assert_eq!(blank.remarks, "");
    // 未払先C/H はコードのまま返り、マスタで引けない名前だけ空
    assert_eq!(blank.vendor_code, "999999");
    assert_eq!(blank.vendor_branch, "00");
    assert_eq!(blank.vendor_name, "");
    // 入力日 NULL → 空文字 (None のまま返さない)
    assert_eq!(blank.entered_date, "");
    assert_eq!(blank.amount, 12_000);
}

#[test]
fn test_build_costs_daily_rows_empty() {
    assert!(build_costs_daily_rows(&[]).is_empty());
}

// ══════════════════════════════════════════════════════════════
// Query: 絞り込みの正規化・limit の丸め・400 判定
// ══════════════════════════════════════════════════════════════

fn query(json: &str) -> CostsDailyQuery {
    serde_json::from_str(json).unwrap()
}

#[test]
fn test_filters_none_when_no_filter() {
    let q = query(r#"{"from":"2026-06-01","to":"2026-07-01"}"#);
    assert_eq!(q.from, "2026-06-01");
    assert_eq!(q.to, "2026-07-01");
    assert_eq!(q.filters(), None);
}

#[test]
fn test_filters_none_when_all_blank() {
    let q = query(r#"{"from":"a","to":"b","vehicle":" ","driver":"","kind":"  "}"#);
    assert_eq!(q.filters(), None);
}

#[test]
fn test_filters_each_alone_is_enough() {
    for key in ["vehicle", "driver", "kind"] {
        let q = query(&format!(r#"{{"from":"a","to":"b","{key}":" x "}}"#));
        let f = q.filters().expect(key);
        let got = [f.vehicle, f.driver, f.kind];
        assert_eq!(got.iter().filter(|v| **v == Some("x")).count(), 1, "{key}");
        assert_eq!(got.iter().filter(|v| v.is_none()).count(), 2, "{key}");
        assert_eq!(f.limit, 500);
    }
}

#[test]
fn test_filters_all_fields_and_limit() {
    let q =
        query(r#"{"from":"a","to":"b","vehicle":"8504","driver":"1656","kind":"01","limit":9999}"#);
    assert_eq!(
        q.filters(),
        Some(CostsDailyFilters {
            vehicle: Some("8504"),
            driver: Some("1656"),
            kind: Some("01"),
            limit: 5000,
        })
    );
    let q = query(r#"{"from":"a","to":"b","kind":"01","limit":0}"#);
    assert_eq!(q.filters().unwrap().limit, 1);
}

#[test]
fn test_response_row_json_field_order() {
    // オンプレ版と Worker の応答を sha256 で比べるので、フィールド名と順序を固定する
    let raw = RawCostsDailyRow {
        operation_date: dt(2026, 7, 13),
        vehicle_number: "1420".into(),
        vehicle_branch: "00".into(),
        driver_code: "1234".into(),
        cost_code: "0631".into(),
        cost_name: "cn".into(),
        cost_kind: "02".into(),
        cost_kind_name: "kn".into(),
        quantity: 1.0,
        unit_price: 2.5,
        amount: 3,
        diesel_tax: 4,
        km: 5.5,
        fixed_cost_flag: "1".into(),
        row_id: "r".into(),
        remarks: "rm".into(),
        vendor_code: "vc".into(),
        vendor_branch: "vb".into(),
        vendor_name: "vn".into(),
        entered_date: Some(dt(2026, 7, 15)),
    };
    let json = serde_json::to_string(&build_costs_daily_rows(&[raw])).unwrap();
    assert_eq!(
        json,
        r#"[{"operation_date":"2026-07-13","vehicle_number":"1420","vehicle_branch":"00","driver_code":"1234","cost_code":"0631","cost_name":"cn","cost_kind":"02","cost_kind_name":"kn","quantity":1.0,"unit_price":2.5,"amount":3,"diesel_tax":4,"km":5.5,"is_fixed":true,"row_id":"r","remarks":"rm","vendor_code":"vc","vendor_branch":"vb","vendor_name":"vn","entered_date":"2026-07-15"}]"#
    );
}
