//! `routes::uriage::compute_person_sum` characterization test。
//!
//! `yhonda-ohishi/nginx` の PHP `ComputePersonSumTest.php` (issue #762 PR6 で
//! 整備された 17 ケース golden + 全分岐網羅) を 1:1 で写経したもの。golden
//! 値は PHP 側 PR #764 で確定済み。本 Rust 実装が PHP と 1 円単位で一致する
//! ことを保証する。

use rust_ichibanboshi::routes::uriage::{compute_person_sum, PersonAccum, UriageRow};
use std::collections::HashMap;

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
