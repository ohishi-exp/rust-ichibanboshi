//! 期間の計算 (sales の複数の口と surcharge/base で共用)。オンプレ版 `src/routes/sales.rs` の
//! `calc_prev_period`・`calc_next_month`・`calc_months` を同じ挙動のまま写した (Refs #322)。
//! 読めない入力は panic せず、オンプレ版と同じ既定値 (`unwrap_or`) に落ちる。

/// 月文字列 "YYYY-MM" から前年同期間の日付文字列 ("YYYY-MM-01") を計算する。
/// 年が数字でなければ from は 2024、to は 2025 として 1 引く。月が無ければ from は "04"、to は "03"。
/// 月は検証せずそのまま写す (オンプレ版と同じ)。
pub fn calc_prev_period(from: &str, to: &str) -> (String, String) {
    let from_y = from
        .split('-')
        .next()
        .unwrap_or("2024")
        .parse::<i32>()
        .unwrap_or(2024);
    let from_m = from.split('-').nth(1).unwrap_or("04");
    let to_y = to
        .split('-')
        .next()
        .unwrap_or("2025")
        .parse::<i32>()
        .unwrap_or(2025);
    let to_m = to.split('-').nth(1).unwrap_or("03");
    let prev_from = format!("{}-{}-01", from_y - 1, from_m);
    let prev_to = format!("{}-{}-01", to_y - 1, to_m);
    (prev_from, prev_to)
}

/// 翌月 (年, 月)。12 以上は翌年 1 月。
pub fn calc_next_month(y: i32, m: i32) -> (i32, i32) {
    if m >= 12 {
        (y + 1, 1)
    } else {
        (y, m + 1)
    }
}

/// "YYYY-MM" の from..=to の月数 (最低 1)。読めない年は from 2025・to 2026、月は from 4・to 3 に落ちる。
pub fn calc_months(from: &str, to: &str) -> i64 {
    let from_parts: Vec<&str> = from.split('-').collect();
    let to_parts: Vec<&str> = to.split('-').collect();
    let from_y = from_parts[0].parse::<i32>().unwrap_or(2025);
    let from_m = from_parts
        .get(1)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(4);
    let to_y = to_parts[0].parse::<i32>().unwrap_or(2026);
    let to_m = to_parts
        .get(1)
        .and_then(|s| s.parse::<i32>().ok())
        .unwrap_or(3);
    ((to_y - from_y) * 12 + (to_m - from_m) + 1).max(1) as i64
}
