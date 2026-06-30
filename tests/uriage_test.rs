//! `routes::uriage::compute_person_sum` characterization test。
//!
//! `yhonda-ohishi/nginx` の PHP `ComputePersonSumTest.php` (issue #762 PR6 で
//! 整備された 17 ケース golden + 全分岐網羅) を 1:1 で写経したもの。golden
//! 値は PHP 側 PR #764 で確定済み。本 Rust 実装が PHP と 1 円単位で一致する
//! ことを保証する。
//!
//! 末尾には HTTP endpoint (`POST /api/uriage/by-person`) の統合テストも置く。

mod common;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use rust_ichibanboshi::routes::uriage::{compute_person_sum, PersonAccum, UriageRow};
use rust_ichibanboshi::sqlite::LocalStore;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tower::ServiceExt;

/// PHP `testRowData` の Rust 表現 (横横=1, 入力担当C=1499, 稼動部門="010", ...)。
fn base_row() -> UriageRow {
    UriageRow {
        yokoyoko: 1,
        seikyu_k: 0,
        biko2: String::new(),
        nyuryoku_tanto_c: 1499,
        kado_bumon: "010".to_string(),
        kingaku: 10000,
        nebiki: 0,
        warimashi: 0,
        jippi: 0,
        yosha_kingaku: 9000,
        yosha_nebiki: 0,
        yosha_warimashi: 0,
        yosha_jippi: 0,
        shain_r: "青井健".to_string(),
        yoshasaki_c: "021970".to_string(),
    }
}

/// `print()` の `$id=="test"` 経路と同一の 17 件フィクスチャ + マスタ。
fn fixture17() -> (
    Vec<UriageRow>,
    HashMap<i32, String>,
    HashMap<String, String>,
) {
    let mut rows: Vec<UriageRow> = Vec::new();
    let mut iii: i64 = 0;
    let mut push = |overrides: UriageRow, iii: &mut i64| {
        let mut r = overrides;
        r.kingaku = 10000 + *iii;
        *iii += 1;
        rows.push(r);
    };

    // 0: 普通傭車
    push(base_row(), &mut iii);
    // 1: 諸富→傭車 (稼動部門=021)
    push(
        UriageRow {
            kado_bumon: "021".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 2: 受注諸富→本社 (受注部門=021、稼動部門 base "010"、担当青井 1499)
    //    受注部門は本 module の関心外なので省略
    push(base_row(), &mut iii);
    // 3: 受注佐賀→本社 ＋ 備考2=売上 (請求K=2)
    push(
        UriageRow {
            biko2: "売上".to_string(),
            seikyu_k: 2,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 4: 受注佐賀→本社 ＋ 備考2=表示 (請求K=2)
    push(
        UriageRow {
            biko2: "表示".to_string(),
            seikyu_k: 2,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 5: 受注佐賀→本社 ＋ 備考2=請求のみ (請求K=1) → skip
    push(
        UriageRow {
            biko2: "請求のみ".to_string(),
            seikyu_k: 1,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 6: 受注佐賀→本社 ＋ 備考2=売上 山﨑智 (請求K=2)
    push(
        UriageRow {
            biko2: "売上\u{3000}山﨑智".to_string(),
            seikyu_k: 2,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 7: 受注佐賀→本社 ＋ 備考2=表示のみ (請求K=1) → tanto 未設定 (PHP の `!請求K == 1` 罠)
    push(
        UriageRow {
            biko2: "表示のみ".to_string(),
            seikyu_k: 1,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 8: 稼動部門=013、備考2=山﨑 (請求K=0、B5 経路、cal=false で skip)
    push(
        UriageRow {
            kado_bumon: "013".to_string(),
            biko2: "山﨑".to_string(),
            seikyu_k: 0,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 9: 稼動部門=013、備考2=売上 山﨑 (請求K=0)
    push(
        UriageRow {
            kado_bumon: "013".to_string(),
            biko2: "売上\u{3000}山﨑".to_string(),
            seikyu_k: 0,
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 10: 入力担当C=1180 (マスタ外)、備考2=空 → B6 で社員R="坂本"
    push(
        UriageRow {
            nyuryoku_tanto_c: 1180,
            shain_r: "坂本".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 11: 入力担当C=1180、備考2=青井 → B3 で青井に加算
    push(
        UriageRow {
            nyuryoku_tanto_c: 1180,
            biko2: "青井".to_string(),
            shain_r: "坂本".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 12: 入力担当C=1112、備考2=売上 山﨑→青井 (請求K=0、B2 経路)
    push(
        UriageRow {
            nyuryoku_tanto_c: 1112,
            biko2: "売上\u{3000}山﨑→青井".to_string(),
            shain_r: "前川".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 13: 入力担当C=1112、備考2=売上 青井 (請求K=2、B3 経路)
    push(
        UriageRow {
            nyuryoku_tanto_c: 1112,
            biko2: "売上\u{3000}青井".to_string(),
            seikyu_k: 2,
            shain_r: "前川".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 14: 稼動部門=013、入力担当C=1112、備考2=売上 山﨑智→山﨑 (B1: Fr=山﨑智 がマスタ)
    push(
        UriageRow {
            kado_bumon: "013".to_string(),
            nyuryoku_tanto_c: 1112,
            biko2: "売上\u{3000}山﨑智→山﨑".to_string(),
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 15: 稼動部門=013、入力担当C=1112、備考2=売上 山﨑→大石 (B2: To=大石 がマスタ)
    push(
        UriageRow {
            kado_bumon: "013".to_string(),
            nyuryoku_tanto_c: 1112,
            biko2: "売上\u{3000}山﨑→大石".to_string(),
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );
    // 16: 稼動部門=013、入力担当C=1112、備考2=売上 大石 (B3 で大石)
    push(
        UriageRow {
            kado_bumon: "013".to_string(),
            nyuryoku_tanto_c: 1112,
            biko2: "売上\u{3000}大石".to_string(),
            shain_r: "青井".to_string(),
            ..base_row()
        },
        &mut iii,
    );

    let _ = iii; // suppress unused

    let persons: HashMap<i32, String> = [
        (1132, "大石"),
        (1621, "松岡"),
        (1542, "楠本"),
        (1499, "青井"),
        (1364, "山﨑智"),
        (1120, "松岡"),
        (1475, "大石"),
        (1065, "松岡"),
        (1605, "瀬戸口"),
        (1680, "山口"),
        (1469, "石川"),
        (1476, "児玉"),
        (1698, "山﨑智"),
    ]
    .into_iter()
    .map(|(k, v)| (k, v.to_string()))
    .collect();

    let other: HashMap<String, String> = [
        ("031", "帯広営業所"),
        ("025", "大阪営業所"),
        ("014", "北九州営業所"),
        ("013", "佐賀営業所"),
        ("032", "宮崎営業所"),
        ("020", "帯広営業所"),
        ("015", "宮崎営業所"),
        ("033", "広島営業所"),
        ("016", "広島営業所"),
        ("021", "諸富営業所"),
        ("012", "佐賀営業所"),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();

    (rows, persons, other)
}

/// `nonZero($sum)` 相当 (件数 0 の担当を除外)。
fn non_zero(sum: &HashMap<String, PersonAccum>) -> HashMap<String, PersonAccum> {
    sum.iter()
        .filter(|(_, v)| v.kingaku != 0 || v.yosha_kingaku != 0 || v.kensuu != 0)
        .map(|(k, v)| (k.clone(), *v))
        .collect()
}

/// PHP `testGolden17CasesCalTrue` の golden 値一致。
#[test]
fn golden_17_cases_cal_true() {
    let (rows, persons, other) = fixture17();
    let result = compute_person_sum(&rows, &persons, &other, true);
    let nz = non_zero(&result.sum);

    let expected: HashMap<String, PersonAccum> = [
        (
            "大石",
            PersonAccum {
                yosha_kingaku: 20031,
                kingaku: 20031,
                kensuu: 2,
            },
        ),
        (
            "青井",
            PersonAccum {
                yosha_kingaku: 90059,
                kingaku: 90059,
                kensuu: 9,
            },
        ),
        (
            "山﨑智",
            PersonAccum {
                yosha_kingaku: 20020,
                kingaku: 20020,
                kensuu: 2,
            },
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    assert_eq!(nz, expected);
}

/// PHP `testGolden17CasesCalFalse` の golden 値一致。
#[test]
fn golden_17_cases_cal_false() {
    let (rows, persons, other) = fixture17();
    let result = compute_person_sum(&rows, &persons, &other, false);
    let nz = non_zero(&result.sum);

    let expected: HashMap<String, PersonAccum> = [
        (
            "青井",
            PersonAccum {
                yosha_kingaku: 60041,
                kingaku: 60041,
                kensuu: 6,
            },
        ),
        (
            "山﨑智",
            PersonAccum {
                yosha_kingaku: 10006,
                kingaku: 10006,
                kensuu: 1,
            },
        ),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect();

    assert_eq!(nz, expected);
}

/// PHP `testAllPersonsInitializedToZero` — マスタに登録された全担当が 0 初期化されること。
#[test]
fn all_persons_initialized_to_zero() {
    let (rows, persons, other) = fixture17();
    let result = compute_person_sum(&rows, &persons, &other, true);
    for name in persons.values() {
        assert!(
            result.sum.contains_key(name),
            "expected {name} to be present in sum",
        );
    }
}

/// PHP `testAllBranchesReached` — 17 件で到達できない 2 分岐 (横横!=1 / B3-表示のみ) を含む
/// 追加行を入れて全 14 分岐網羅。
#[test]
fn all_branches_reached() {
    let (mut rows, persons, other) = fixture17();

    // 横横 != 1 (傭車金額が金額と独立) + 入力担当一致 (B5)
    rows.push(UriageRow {
        yokoyoko: 0,
        kingaku: 5000,
        nebiki: 100,
        warimashi: 200,
        jippi: 50,
        yosha_kingaku: 3000,
        yosha_nebiki: 10,
        yosha_warimashi: 20,
        yosha_jippi: 5,
        shain_r: "青井".to_string(),
        ..base_row()
    });

    // B3-表示のみ (備考2=担当名 だが 請求K==2 で売上なし → 集計せず)
    rows.push(UriageRow {
        yokoyoko: 0,
        seikyu_k: 2,
        biko2: "青井".to_string(),
        nyuryoku_tanto_c: 9999,
        kingaku: 8000,
        yosha_kingaku: 6000,
        shain_r: "無関係".to_string(),
        ..base_row()
    });

    let result = compute_person_sum(&rows, &persons, &other, true);

    // B5 (横横!=1) の 5000+100+200+50=5350 / 3000+10+20+5=3035 が青井に加算される。
    // B3-表示のみ (allow_seikyu false) は積算しないので両方とも sum に影響しない。
    let aoi = result.sum.get("青井").expect("青井 in sum");
    assert_eq!(aoi.kingaku, 95409); // 90059 + 5350
    assert_eq!(aoi.yosha_kingaku, 93094); // 90059 + 3035
    assert_eq!(aoi.kensuu, 10); // 9 + 1
}

// ══════════════════════════════════════════════════════════════
// HTTP endpoint test: POST /api/uriage/by-person
// ══════════════════════════════════════════════════════════════

/// `Json` body helper
fn json_body(v: Value) -> Body {
    Body::from(serde_json::to_vec(&v).unwrap())
}

/// POST した response から JSON を取り出す
async fn post_by_person(app: axum::Router, body: Value) -> (StatusCode, Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/uriage/by-person")
                .header("content-type", "application/json")
                .body(json_body(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1024 * 1024).await.unwrap();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, v)
}

#[tokio::test]
async fn by_person_ok_with_mock_rows() {
    // MockRepo.uriage_rows は 2 行返す:
    //  - 入力担当C=1499 (= 青井) / 横横=0 / 金額=50000 / 値引=0 / 割増=1000 / 実費=500
    //                                    / 傭車金額=30000 / 傭車値引=0 / 傭車割増=600 / 傭車実費=200
    //  - 入力担当C=9999 (マスタ外) → B6 で表示のみ、$sum に影響しない
    let app = common::build_app(common::mock_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "2026-06-30",
        "bumon": ["010", "011", "030"],
        "persons": {
            "1499": "青井",
            "1364": "山﨑智"
        },
        "other": {
            "021": "諸富営業所",
            "013": "佐賀営業所"
        },
        "cal": true
    });

    let (status, v) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::OK);

    // メタ情報
    assert_eq!(v["source_table"], "運転日報明細 + 社員ﾏｽﾀ");
    assert_eq!(v["row_count"], 2);
    assert_eq!(v["cal"], true);

    // 青井に B5 経路で加算: 50000 + 0 + 1000 + 500 = 51500
    //                       傭車: 30000 + 0 + 600 + 200 = 30800
    assert_eq!(v["sum"]["青井"]["金額"], 51500);
    assert_eq!(v["sum"]["青井"]["傭車金額"], 30800);
    assert_eq!(v["sum"]["青井"]["件数"], 1);

    // 山﨑智 はマスタ初期化のみで 0 のまま
    assert_eq!(v["sum"]["山﨑智"]["金額"], 0);
    assert_eq!(v["sum"]["山﨑智"]["件数"], 0);
}

#[tokio::test]
async fn by_person_cal_false_skips_other_eigyosho() {
    // MockRepo の行は 稼動部門="010" (other に無い) なので、cal=false でも skip されない。
    // 確認: cal=false でも 青井 に加算される。
    let app = common::build_app(common::mock_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "2026-06-30",
        "bumon": ["010"],
        "persons": { "1499": "青井" },
        "other": { "021": "諸富営業所" },
        "cal": false
    });

    let (status, v) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["sum"]["青井"]["金額"], 51500);
    assert_eq!(v["sum"]["青井"]["件数"], 1);
}

#[tokio::test]
async fn by_person_default_cal_is_true() {
    // body から `cal` を省略すると default_cal() で true になる
    let app = common::build_app(common::mock_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "2026-06-30",
        "bumon": ["010"],
        "persons": { "1499": "青井" },
        "other": {}
    });

    let (status, v) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["cal"], true);
}

#[tokio::test]
async fn by_person_rejects_empty_bumon() {
    let app = common::build_app(common::mock_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "2026-06-30",
        "bumon": [],
        "persons": {},
        "other": {}
    });

    let (status, _) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn by_person_rejects_empty_from() {
    let app = common::build_app(common::mock_repo());
    let body = json!({
        "from": "",
        "to": "2026-06-30",
        "bumon": ["010"],
        "persons": {},
        "other": {}
    });

    let (status, _) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn by_person_rejects_empty_to() {
    let app = common::build_app(common::mock_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "",
        "bumon": ["010"],
        "persons": {},
        "other": {}
    });

    let (status, _) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn by_person_pool_error_returns_503() {
    let app = common::build_app(common::error_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "2026-06-30",
        "bumon": ["010"],
        "persons": { "1499": "青井" },
        "other": {}
    });

    let (status, _) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn by_person_query_error_returns_500() {
    let app = common::build_app(common::query_error_repo());
    let body = json!({
        "from": "2026-06-01",
        "to": "2026-06-30",
        "bumon": ["010"],
        "persons": { "1499": "青井" },
        "other": {}
    });

    let (status, _) = post_by_person(app, body).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn by_person_rejects_malformed_json() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/uriage/by-person")
                .header("content-type", "application/json")
                .body(Body::from("{not json"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

// ══════════════════════════════════════════════════════════════
// HTTP endpoint test: POST /api/uriage/recalc (Phase 2 PR-C1)
// ══════════════════════════════════════════════════════════════

async fn post_recalc(app: axum::Router, body: Value) -> (StatusCode, Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/uriage/recalc")
                .header("content-type", "application/json")
                .body(json_body(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = to_bytes(res.into_body(), 1024 * 1024).await.unwrap();
    let v = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, v)
}

fn recalc_body(month: &str, eigyosho_id: i64, cal: bool) -> Value {
    json!({
        "month": month,
        "from": format!("{month}-01"),
        "to":   format!("{month}-30"),
        "eigyosho_id": eigyosho_id,
        "bumon": ["010"],
        "persons": { "1499": "青井" },
        "other":   {},
        "cal": cal
    })
}

#[tokio::test]
async fn recalc_persists_summary_into_sqlite() {
    let store = common::local_store();
    let app = common::build_app_with_store(common::mock_repo(), store.clone());

    let body = recalc_body("2026-06", 1, true);
    let (status, v) = post_recalc(app, body).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["month"], "2026-06");
    assert_eq!(v["eigyosho_id"], 1);
    assert_eq!(v["cal"], true);
    assert_eq!(v["persisted_count"], 1); // 青井のみ非ゼロ
    assert_eq!(v["row_count"], 2); // MockRepo の uriage_rows 2 行
    assert_eq!(v["sum"]["青井"]["金額"], 51500);

    // SQLite に persist された行を直接確認
    let rows = store
        .get_person_monthly("2026-06", 1, true)
        .await
        .expect("get_person_monthly");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].person_name, "青井");
    assert_eq!(rows[0].kingaku, 51500);
    assert_eq!(rows[0].yosha_kingaku, 30800);
    assert_eq!(rows[0].kensuu, 1);
    assert!(v["calculated_at"].as_str().unwrap().contains('T'));
}

#[tokio::test]
async fn recalc_second_run_overwrites_bucket() {
    let store = common::local_store();
    let app = common::build_app_with_store(common::mock_repo(), store.clone());
    let body = recalc_body("2026-06", 1, true);

    // 1 回目
    let (status, _) = post_recalc(app.clone(), body.clone()).await;
    assert_eq!(status, StatusCode::OK);
    let first_count = store
        .get_person_monthly("2026-06", 1, true)
        .await
        .unwrap()
        .len();
    assert_eq!(first_count, 1);

    // 2 回目: 同じ bucket、同じ内容 → 上書きされても行数 / 値は同一
    let (status, _) = post_recalc(app, body).await;
    assert_eq!(status, StatusCode::OK);
    let rows = store.get_person_monthly("2026-06", 1, true).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].kingaku, 51500);
}

#[tokio::test]
async fn recalc_isolates_buckets() {
    let store = common::local_store();
    let app = common::build_app_with_store(common::mock_repo(), store.clone());

    // (2026-06, 1, true) を入れる
    let (s, _) = post_recalc(app.clone(), recalc_body("2026-06", 1, true)).await;
    assert_eq!(s, StatusCode::OK);
    // 別営業所
    let (s, _) = post_recalc(app.clone(), recalc_body("2026-06", 2, true)).await;
    assert_eq!(s, StatusCode::OK);
    // 別 cal
    let (s, _) = post_recalc(app.clone(), recalc_body("2026-06", 1, false)).await;
    assert_eq!(s, StatusCode::OK);
    // 別月
    let (s, _) = post_recalc(app, recalc_body("2026-07", 1, true)).await;
    assert_eq!(s, StatusCode::OK);

    // 4 つの独立 bucket に persist される
    for (m, e, c) in [
        ("2026-06", 1, true),
        ("2026-06", 2, true),
        ("2026-06", 1, false),
        ("2026-07", 1, true),
    ] {
        let rows = store.get_person_monthly(m, e, c).await.unwrap();
        assert_eq!(
            rows.len(),
            1,
            "bucket (m={}, e={}, c={}) should have 1 row",
            m,
            e,
            c
        );
    }
}

#[tokio::test]
async fn recalc_rejects_missing_required_fields() {
    let app = common::build_app(common::mock_repo());

    // month 欠落
    let body = json!({
        "from": "2026-06-01", "to": "2026-06-30",
        "eigyosho_id": 1, "bumon": ["010"],
        "persons": {}, "other": {}
    });
    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/uriage/recalc")
                .header("content-type", "application/json")
                .body(json_body(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::UNPROCESSABLE_ENTITY); // axum json deserialize fail

    // 空 from
    let body = json!({
        "month": "2026-06", "from": "", "to": "2026-06-30",
        "eigyosho_id": 1, "bumon": ["010"],
        "persons": {}, "other": {}
    });
    let (status, _) = post_recalc(app.clone(), body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // 空 bumon
    let body = json!({
        "month": "2026-06", "from": "2026-06-01", "to": "2026-06-30",
        "eigyosho_id": 1, "bumon": [],
        "persons": {}, "other": {}
    });
    let (status, _) = post_recalc(app, body).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn recalc_pool_error_returns_503() {
    let app = common::build_app(common::error_repo());
    let (status, _) = post_recalc(app, recalc_body("2026-06", 1, true)).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn recalc_query_error_returns_500() {
    let app = common::build_app(common::query_error_repo());
    let (status, _) = post_recalc(app, recalc_body("2026-06", 1, true)).await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn recalc_local_store_independent_instance() {
    // build_app は新規 in-memory store を毎回作る → store は独立。
    // 2 つの app は別の store を持つので互いの persist が見えない。
    let store_a = Arc::new(LocalStore::open(":memory:").unwrap());
    let store_b = Arc::new(LocalStore::open(":memory:").unwrap());
    let app_a = common::build_app_with_store(common::mock_repo(), store_a.clone());
    let app_b = common::build_app_with_store(common::mock_repo(), store_b.clone());

    let (s, _) = post_recalc(app_a, recalc_body("2026-06", 1, true)).await;
    assert_eq!(s, StatusCode::OK);

    // store_a には入っているが store_b は空
    assert_eq!(
        store_a
            .get_person_monthly("2026-06", 1, true)
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(store_b
        .get_person_monthly("2026-06", 1, true)
        .await
        .unwrap()
        .is_empty());

    let _ = app_b; // suppress unused
}
