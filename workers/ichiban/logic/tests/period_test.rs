//! 期間の計算。オンプレ版 `tests/sales_logic_test.rs` の calc_* のテストを写し、
//! 読めない入力で既定値に落ちるところ (オンプレ版と同じ挙動) を足している。

use ichiban_logic::period::{calc_months, calc_next_month, calc_prev_period};

// ══════════════════════════════════════════════════════════════
// calc_prev_period
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_prev_period_standard() {
    let (from, to) = calc_prev_period("2025-04", "2026-03");
    assert_eq!(from, "2024-04-01");
    assert_eq!(to, "2025-03-01");
}

#[test]
fn test_calc_prev_period_single_month() {
    let (from, to) = calc_prev_period("2025-01", "2025-01");
    assert_eq!(from, "2024-01-01");
    assert_eq!(to, "2024-01-01");
}

#[test]
fn test_calc_prev_period_calendar_year() {
    let (from, to) = calc_prev_period("2026-01", "2026-12");
    assert_eq!(from, "2025-01-01");
    assert_eq!(to, "2025-12-01");
}

#[test]
fn test_calc_prev_period_unreadable_falls_back() {
    // 年が数字でなければ 2024 / 2025 から 1 引く。月が無ければ 04 / 03
    let (from, to) = calc_prev_period("", "abc");
    assert_eq!(from, "2023-04-01");
    assert_eq!(to, "2024-03-01");
    // 月は検証せずそのまま写す
    let (from, to) = calc_prev_period("2025-x", "2026-13");
    assert_eq!(from, "2024-x-01");
    assert_eq!(to, "2025-13-01");
}

// ══════════════════════════════════════════════════════════════
// calc_next_month
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_next_month_normal() {
    assert_eq!(calc_next_month(2025, 3), (2025, 4));
    assert_eq!(calc_next_month(2025, 11), (2025, 12));
}

#[test]
fn test_calc_next_month_december() {
    assert_eq!(calc_next_month(2025, 12), (2026, 1));
}

#[test]
fn test_calc_next_month_over_twelve() {
    // 12 より大きい月も翌年 1 月 (オンプレ版と同じ `m >= 12`)
    assert_eq!(calc_next_month(2025, 13), (2026, 1));
}

// ══════════════════════════════════════════════════════════════
// calc_months
// ══════════════════════════════════════════════════════════════

#[test]
fn test_calc_months_full_year() {
    assert_eq!(calc_months("2025-04", "2026-03"), 12);
}

#[test]
fn test_calc_months_single() {
    assert_eq!(calc_months("2025-04", "2025-04"), 1);
}

#[test]
fn test_calc_months_half_year() {
    assert_eq!(calc_months("2025-01", "2025-06"), 6);
}

#[test]
fn test_calc_months_minimum_one() {
    // 逆転しても最低1
    assert_eq!(calc_months("2026-03", "2025-04"), 1);
}

#[test]
fn test_calc_months_unreadable_falls_back() {
    // 読めなければ 2025-04 .. 2026-03 として数える
    assert_eq!(calc_months("", "x-y"), 12);
    assert_eq!(calc_months("2025", "2025"), 1);
}
