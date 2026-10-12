//! 月 (`YYYY-MM`) の扱い — 月初・月末と月の日数 (root の `src/routes/uriage.rs` から移した。Refs ohishi-exp/rust-ichibanboshi#322)。
//! 時計は使わない (`chrono` の clock feature に依存しない)。

/// `month` (YYYY-MM) → 月初/月末 日付 (YYYY-MM-DD) を返す。
/// SQL Server の `運行年月日 >=` と `<=` の inclusive 区間に使う。
pub fn month_to_range(month: &str) -> Option<(String, String, String)> {
    // YYYY-MM 形式チェック
    if month.len() != 7 || month.as_bytes()[4] != b'-' {
        return None;
    }
    let year: i32 = month[..4].parse().ok()?;
    let m: u32 = month[5..].parse().ok()?;
    if !(1..=12).contains(&m) {
        return None;
    }
    // chrono で月末日を計算
    let first = chrono::NaiveDate::from_ymd_opt(year, m, 1)?;
    // 月末 = 翌月初の前日
    let next = if m == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, m + 1, 1)?
    };
    let last = next.pred_opt()?;
    // masters fetch 用に「月末日」を date として返す (CakePHP `/masters-json?date=` 用)
    Some((
        first.format("%Y-%m-%d").to_string(),
        last.format("%Y-%m-%d").to_string(),
        last.format("%Y-%m-%d").to_string(),
    ))
}

/// YYYY-MM → その月の日数 (28-31)。形式不正なら `None`。
pub fn days_in_month(month: &str) -> Option<u32> {
    use chrono::Datelike;
    if month.len() != 7 || month.as_bytes()[4] != b'-' {
        return None;
    }
    let year: i32 = month[..4].parse().ok()?;
    let m: u32 = month[5..].parse().ok()?;
    if !(1..=12).contains(&m) {
        return None;
    }
    let next = if m == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, m + 1, 1)?
    };
    let last = next.pred_opt()?;
    Some(last.day())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn month_to_range_jan() {
        let (from, to, date) = month_to_range("2026-01").unwrap();
        assert_eq!(from, "2026-01-01");
        assert_eq!(to, "2026-01-31");
        assert_eq!(date, "2026-01-31");
    }

    #[test]
    fn month_to_range_feb_handles_leap() {
        // 2024 はうるう年
        let (from, to, date) = month_to_range("2024-02").unwrap();
        assert_eq!(from, "2024-02-01");
        assert_eq!(to, "2024-02-29");
        assert_eq!(date, "2024-02-29");
        // 2026 は平年
        let (from, to, _) = month_to_range("2026-02").unwrap();
        assert_eq!(from, "2026-02-01");
        assert_eq!(to, "2026-02-28");
    }

    #[test]
    fn month_to_range_dec_year_rollover() {
        let (from, to, _) = month_to_range("2026-12").unwrap();
        assert_eq!(from, "2026-12-01");
        assert_eq!(to, "2026-12-31");
    }

    #[test]
    fn month_to_range_invalid_format_returns_none() {
        assert!(month_to_range("2026-1").is_none()); // 1 桁月
        assert!(month_to_range("26-01").is_none()); // 短い年
        assert!(month_to_range("2026/01").is_none()); // 区切り違い
        assert!(month_to_range("2026-13").is_none()); // 月範囲外
        assert!(month_to_range("2026-00").is_none()); // 0 月
        assert!(month_to_range("").is_none());
        assert!(month_to_range("abcd-ef").is_none());
    }
}
