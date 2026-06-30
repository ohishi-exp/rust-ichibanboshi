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
// HTTP endpoint test: POST /api/uriage/recalc (Phase 2 PR-C2 / D)
//   CakePHP pull + editable_months チェック + raw NDJSON.gz 出力 + fingerprint 記録
// ══════════════════════════════════════════════════════════════

use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::config::RawConfig;
use wiremock::matchers::{method as wm_method, path as wm_path};
use wiremock::{Mock, MockServer, ResponseTemplate};

async fn post_recalc_path(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri(uri)
                .body(Body::empty())
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

/// 標準的な editable_months + masters を返す wiremock server を立てる。
/// `editable_months` と `master_offices` を caller から指定可能。
async fn start_cakephp_mock(
    editable_months: Vec<&str>,
    master_offices: Value, // {"1": {display_name, persons, other, bumon}, ...}
) -> MockServer {
    let server = MockServer::start().await;

    let em_resp = json!({
        "operation_month": "2026-07",
        "editable_months_count": editable_months.len(),
        "editable_months": editable_months,
    });
    Mock::given(wm_method("GET"))
        .and(wm_path("/uriage-jyuchu-display/editable-months"))
        .respond_with(ResponseTemplate::new(200).set_body_json(em_resp))
        .mount(&server)
        .await;

    let masters_resp = json!({
        "date": "2026-06-30",
        "offices": master_offices,
    });
    Mock::given(wm_method("GET"))
        .and(wm_path("/uriage-jyuchu-display/masters-json"))
        .respond_with(ResponseTemplate::new(200).set_body_json(masters_resp))
        .mount(&server)
        .await;

    server
}

fn standard_offices() -> Value {
    json!({
        "1": {
            "display_name": "本社",
            "persons": { "1499": "青井" },
            "other": {},
            "bumon": ["010"]
        },
        "9": {
            "display_name": "宮崎",
            "persons": { "2000": "田中" },
            "other": {},
            "bumon": ["015"]
        }
    })
}

fn build_app_with_cakephp(
    repo: rust_ichibanboshi::repo::DynRepo,
    store: rust_ichibanboshi::sqlite::DynLocalStore,
    cakephp_base: String,
    raw_cfg: Arc<RawConfig>,
) -> axum::Router {
    let cakephp = Arc::new(CakephpClient::new(cakephp_base, 30).unwrap());
    common::build_app_full(repo, store, cakephp, raw_cfg)
}

#[tokio::test]
async fn recalc_503_when_cakephp_not_configured() {
    // base_url 空 → /recalc は 503
    let app = common::build_app(common::mock_repo());
    let (status, _) = post_recalc_path(app, "/api/uriage/recalc?month=2026-06").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn recalc_422_when_month_not_editable() {
    let server = start_cakephp_mock(vec!["2026-06", "2026-07"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        common::local_store(),
        server.uri(),
        raw,
    );
    // 2026-05 は editable_months に無い → 422
    let (status, v) = post_recalc_path(app, "/api/uriage/recalc?month=2026-05").await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    // body は plain text (axum (StatusCode, String) tuple)
    assert!(v == Value::Null || v.is_string() || v.is_object());
}

#[tokio::test]
async fn recalc_single_month_single_office_persists_and_records_fingerprint() {
    let server = start_cakephp_mock(vec!["2026-06", "2026-07"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    let (status, v) = post_recalc_path(app, "/api/uriage/recalc?month=2026-06&eigyosho_id=1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(v["months"][0], "2026-06");
    assert_eq!(v["editable_months_count"], 2);
    assert_eq!(v["jobs"].as_array().unwrap().len(), 1);
    let job = &v["jobs"][0];
    assert_eq!(job["month"], "2026-06");
    assert_eq!(job["eigyosho_id"], 1);
    assert_eq!(job["status"], "computed");
    assert_eq!(job["row_count"], 2); // MockRepo の uriage_rows 2 行
    assert_eq!(job["persisted_count_cal"], 1); // 青井のみ非ゼロ
    assert_eq!(job["persisted_count_nocal"], 1);
    assert!(job["fingerprint"]
        .as_str()
        .unwrap()
        .chars()
        .all(|c| c.is_ascii_hexdigit()));
    let raw_path = job["raw_path"].as_str().unwrap().to_string();
    assert!(raw_path.ends_with("eigyosho-1.ndjson.gz"));
    assert!(std::path::Path::new(&raw_path).exists());

    // SQLite に cal=true / cal=false 両方 persist
    let rows_cal = store.get_person_monthly("2026-06", 1, true).await.unwrap();
    let rows_nocal = store.get_person_monthly("2026-06", 1, false).await.unwrap();
    assert_eq!(rows_cal.len(), 1);
    assert_eq!(rows_nocal.len(), 1);

    // recalc_jobs に記録
    let rec = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
    assert_eq!(rec.status, "computed");
    assert_eq!(
        rec.fingerprint_after.as_deref(),
        job["fingerprint"].as_str()
    );
    assert_eq!(rec.raw_path.as_deref(), Some(raw_path.as_str()));

    // 後始末
    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn recalc_no_month_processes_all_editable_all_offices() {
    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    let (status, v) = post_recalc_path(app, "/api/uriage/recalc").await;
    assert_eq!(status, StatusCode::OK);
    let jobs = v["jobs"].as_array().unwrap();
    // 1 ヶ月 × 2 営業所 = 2 jobs
    assert_eq!(jobs.len(), 2);
    let ids: Vec<i64> = jobs
        .iter()
        .map(|j| j["eigyosho_id"].as_i64().unwrap())
        .collect();
    assert!(ids.contains(&1));
    assert!(ids.contains(&9));
    // 全 job が "computed"
    for j in jobs {
        assert_eq!(j["status"], "computed");
    }

    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn recalc_eigyosho_id_not_in_masters_returns_skipped() {
    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        common::local_store(),
        server.uri(),
        raw.clone(),
    );

    // eigyosho_id=99 は masters に居ない → skipped
    let (status, v) =
        post_recalc_path(app, "/api/uriage/recalc?month=2026-06&eigyosho_id=99").await;
    assert_eq!(status, StatusCode::OK);
    let job = &v["jobs"][0];
    assert_eq!(job["status"], "skipped");
    assert_eq!(job["eigyosho_id"], 99);
    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn recalc_sqlserver_error_records_failed() {
    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::error_repo(), // pool error
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    let (status, v) = post_recalc_path(app, "/api/uriage/recalc?month=2026-06&eigyosho_id=1").await;
    // 全体 HTTP は 200 (個別 job が failed)
    assert_eq!(status, StatusCode::OK);
    let job = &v["jobs"][0];
    assert_eq!(job["status"], "failed");
    assert!(job["error"].as_str().unwrap().contains("sqlserver"));
    // recalc_jobs にも failed が記録される
    let rec = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
    assert_eq!(rec.status, "failed");
    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn recalc_same_fingerprint_keeps_r2_synced_at() {
    // 1) recalc → r2_synced 記録 → 同 fingerprint で recalc 再実行 → r2_synced_at 維持
    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    let (s1, _) = post_recalc_path(
        app.clone(),
        "/api/uriage/recalc?month=2026-06&eigyosho_id=1",
    )
    .await;
    assert_eq!(s1, StatusCode::OK);

    // r2_synced を明示記録
    store
        .record_r2_synced("2026-06", 1, "manual-sync-ts")
        .await
        .unwrap();
    let rec1 = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
    assert_eq!(rec1.status, "r2_synced");
    assert_eq!(rec1.r2_synced_at.as_deref(), Some("manual-sync-ts"));

    // 2 回目 recalc (raw rows 不変 → fingerprint も同じ)
    let (s2, _) = post_recalc_path(app, "/api/uriage/recalc?month=2026-06&eigyosho_id=1").await;
    assert_eq!(s2, StatusCode::OK);
    let rec2 = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
    // fingerprint が同じなので r2_synced_at は維持される (= 再送不要)
    assert_eq!(rec2.r2_synced_at.as_deref(), Some("manual-sync-ts"));
    assert_eq!(rec2.fingerprint_after, rec1.fingerprint_after);

    let _ = std::fs::remove_dir_all(&raw.dir);
}

// ══════════════════════════════════════════════════════════════
// HTTP endpoint test: R2 sync endpoints (Phase 2 PR-D)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn r2_pending_empty_when_nothing_computed() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/uriage/r2/pending")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), 1024 * 1024).await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["count"], 0);
    assert_eq!(v["items"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn r2_pending_lists_after_recalc() {
    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    // recalc 実行 → /r2/pending に 1 件出る
    let (s, _) = post_recalc_path(
        app.clone(),
        "/api/uriage/recalc?month=2026-06&eigyosho_id=1",
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/uriage/r2/pending")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), 1024 * 1024).await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["count"], 1);
    assert_eq!(v["items"][0]["month"], "2026-06");
    assert_eq!(v["items"][0]["eigyosho_id"], 1);

    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn raw_get_returns_gz_bytes_after_recalc() {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    // recalc 実行
    let (s, _) = post_recalc_path(
        app.clone(),
        "/api/uriage/recalc?month=2026-06&eigyosho_id=1",
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // raw_get で bytes を取得
    let res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/uriage/raw/2026-06/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let ct = res.headers().get("content-type").unwrap();
    assert_eq!(ct, "application/gzip");
    let cd = res.headers().get("content-disposition").unwrap();
    assert!(cd
        .to_str()
        .unwrap()
        .contains("2026-06-eigyosho-1.ndjson.gz"));

    let bytes = to_bytes(res.into_body(), 4 * 1024 * 1024).await.unwrap();
    assert!(!bytes.is_empty());
    // gunzip して NDJSON 行が含まれるか
    let mut dec = GzDecoder::new(&bytes[..]);
    let mut s = String::new();
    dec.read_to_string(&mut s).unwrap();
    assert!(s.contains("\"yokoyoko\""));
    assert!(s.lines().count() >= 1);

    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn raw_get_404_when_no_recalc() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/uriage/raw/2026-06/1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn raw_ack_marks_synced_and_removes_from_pending() {
    let server = start_cakephp_mock(vec!["2026-06"], standard_offices()).await;
    let raw = common::temp_raw_dir();
    let store = common::local_store();
    let app = build_app_with_cakephp(
        common::mock_repo(),
        store.clone(),
        server.uri(),
        raw.clone(),
    );

    // recalc → /r2/pending に 1 件 → ack → /r2/pending 空
    let (s, _) = post_recalc_path(
        app.clone(),
        "/api/uriage/recalc?month=2026-06&eigyosho_id=1",
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    let res = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/uriage/raw/2026-06/1/ack")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);
    let bytes = to_bytes(res.into_body(), 1024 * 1024).await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["month"], "2026-06");
    assert_eq!(v["eigyosho_id"], 1);
    assert!(v["synced_at"].as_str().unwrap().contains('T'));

    // /r2/pending は空に
    let res2 = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/api/uriage/r2/pending")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let bytes2 = to_bytes(res2.into_body(), 1024 * 1024).await.unwrap();
    let v2: Value = serde_json::from_slice(&bytes2).unwrap();
    assert_eq!(v2["count"], 0);

    let _ = std::fs::remove_dir_all(&raw.dir);
}

#[tokio::test]
async fn raw_ack_404_when_no_computed_job() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/uriage/raw/2026-06/1/ack")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::NOT_FOUND);
}
