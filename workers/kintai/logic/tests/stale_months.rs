//! `kintai_logic::stale_months` (月別 stale の範囲・SQL・応答) の検査 (Refs ohishi-exp/rust-ichibanboshi#322)。
//! 範囲の検査は root の `src/routes/stale_months.rs` の unit test から移した。SQL は移す前 (e5aba7d) の本文の sha256 で縛る。

use chrono::NaiveDate;
use kintai_logic::stale_months::{
    resolve_range, response, StaleMonthRow, DEFAULT_WINDOW_MONTHS, MAX_WINDOW_MONTHS,
    STALE_MONTHS_SQL,
};
use serde_json::json;

fn ym(y: i32, m: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, 1).unwrap()
}

fn today() -> NaiveDate {
    NaiveDate::from_ymd_opt(2026, 6, 17).unwrap()
}

#[test]
fn the_sql_text_is_byte_identical_to_the_root_one() {
    use sha2::Digest;
    let sha = format!("{:x}", sha2::Sha256::digest(STALE_MONTHS_SQL.as_bytes()));
    assert_eq!(
        sha,
        "8418fc5d3c95ddf7580a8deacd5db44e7aeafac541780857d1d5539d19f280b5"
    );
    assert!(STALE_MONTHS_SQL.contains("tenant_id = $1"));
}

/// **既定 = 当月から遡って 12 か月ぶん (両端含む)。**
#[test]
fn default_range_is_twelve_months_ending_this_month() {
    assert_eq!(DEFAULT_WINDOW_MONTHS, 12);
    assert_eq!(
        resolve_range(None, None, today()),
        Ok((ym(2025, 7), ym(2026, 7)))
    );
}

#[test]
fn to_only_shifts_the_default_twelve_month_window() {
    assert_eq!(
        resolve_range(None, Some("2026-03"), today()),
        Ok((ym(2025, 4), ym(2026, 4)))
    );
}

#[test]
fn explicit_from_and_to_are_both_honored() {
    assert_eq!(
        resolve_range(Some("2026-01"), Some("2026-03"), today()),
        Ok((ym(2026, 1), ym(2026, 4)))
    );
    assert_eq!(
        resolve_range(Some("2026-03"), Some("2026-03"), today()),
        Ok((ym(2026, 3), ym(2026, 4)))
    );
    assert_eq!(
        resolve_range(Some("2025-12"), Some("2025-12"), today()),
        Ok((ym(2025, 12), ym(2026, 1))),
        "年をまたぐ翌月初"
    );
}

#[test]
fn bad_shapes_and_order_are_rejected_with_the_root_messages() {
    let t = today();
    assert_eq!(
        resolve_range(Some("2026-06"), Some("2026-01"), t),
        Err("from は to 以前にしてください".to_string())
    );
    // `month_start` は符号付きの年・月も読むので、root と同じく `is_valid_month` を先に通す
    for bad in [
        "nope", "2026-6", "", "2026-13", "2026/06", "+026-06", "2026-+6",
    ] {
        assert_eq!(
            resolve_range(Some(bad), None, t),
            Err("from は YYYY-MM で指定してください".to_string()),
            "{bad:?}"
        );
        assert_eq!(
            resolve_range(None, Some(bad), t),
            Err("to は YYYY-MM で指定してください".to_string()),
            "{bad:?}"
        );
    }
}

/// 上限ちょうど (36 か月) は通り、37 か月は 400。
#[test]
fn the_range_is_capped() {
    assert_eq!(MAX_WINDOW_MONTHS, 36);
    assert!(resolve_range(Some("2023-02"), Some("2026-01"), today()).is_ok());
    assert_eq!(
        resolve_range(Some("2023-01"), Some("2026-01"), today()),
        Err("月範囲 上限36".to_string())
    );
}

#[test]
fn the_response_has_the_root_shape() {
    let rows = [
        StaleMonthRow {
            month: "2026-05".into(),
            stale_drivers: 2,
            total_drivers: 9,
        },
        StaleMonthRow {
            month: "2026-06".into(),
            stale_drivers: 0,
            total_drivers: 0,
        },
    ];
    assert_eq!(
        response("v1", ym(2026, 5), ym(2026, 7), &rows),
        json!({
            "logic_version": "v1",
            "from": "2026-05",
            "to": "2026-06",
            "default_window_months": 12,
            "months": [
                {"month": "2026-05", "stale_drivers": 2, "total_drivers": 9},
                {"month": "2026-06", "stale_drivers": 0, "total_drivers": 0},
            ],
        })
    );
}
