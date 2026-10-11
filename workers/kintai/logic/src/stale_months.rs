//! `GET /api/kintai/stale-months?from=YYYY-MM&to=YYYY-MM` — 月ごとの stale 乗務員数の範囲・SQL・応答
//! (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/routes/stale_months.rs` から移した。root (Cloud Run 版、sqlx) と勤怠 Worker が同じものを使う。
//! I/O (SQL を流す・テナントの解決) は呼び手。月の計算は [`crate::wage_snapshot`] の `month_start` / `add_months`
//! (月の形の検査は root と同じ `is_valid_month` を先に通す — `month_start` は `+026-06` のような符号付きも読むため)。

use chrono::{Datelike, NaiveDate};
use kintai_kosoku::window::is_valid_month;

use crate::wage_snapshot::{add_months, month_start};

/// 範囲省略時の窓の広さ (月数)。
pub const DEFAULT_WINDOW_MONTHS: i32 = 12;

/// 範囲の上限 (月数)。乱用防止 (`MAX_MAX_FOLD_DRIVERS` と同じ考え方)。
pub const MAX_WINDOW_MONTHS: i32 = 36;

/// root の `kintai_fold::stale_state` (`kintai_kosoku::kintai_fold::STALE_STATE_SQL`) の月単位版。式は 1 文字も変えず、
/// `GROUP BY date_trunc('month', date)` に割っただけ (root の `src/routes/stale_months.rs` のモジュール docs 参照)。
/// `months` の CTE が対象範囲の月を並べるので、データの無い月も 1 往復のまま返る。
///
/// **`total_drivers` を添えるのは「stale_drivers: 0 (収束済み)」と「その月は
/// まだ 1 度も畳まれていない (データが無い)」を区別するため** — `total_drivers`
/// (版を問わない distinct driver_cd 数) が 0 の月は後者。「応答に無い = 0」と
/// 混同しないための追加列で、月の数だけ往復する形には戻さない。
pub const STALE_MONTHS_SQL: &str = r#"
WITH months AS (
    SELECT generate_series($2::date, $3::date - interval '1 month', interval '1 month')::date AS month
), agg AS (
    SELECT date_trunc('month', date)::date AS month,
           count(DISTINCT driver_cd) FILTER (WHERE logic_version <> $4) AS stale_drivers,
           count(DISTINCT driver_cd) AS total_drivers
      FROM kintai.day_summaries
     WHERE tenant_id = $1 AND date >= $2 AND date < $3
     GROUP BY date_trunc('month', date)
)
SELECT to_char(m.month, 'YYYY-MM') AS month,
       coalesce(a.stale_drivers, 0)::bigint AS stale_drivers,
       coalesce(a.total_drivers, 0)::bigint AS total_drivers
  FROM months m
  LEFT JOIN agg a ON a.month = m.month
 ORDER BY m.month
"#;

/// `"YYYY-MM"` を月初に (`is_valid_month` を満たさなければ `None`)。
fn month_of(s: &str) -> Option<NaiveDate> {
    is_valid_month(s).then(|| month_start(s)).flatten()
}

/// `[from, to]` (両端含む、どちらも省略可) を `[lo, hi)` の `DATE` 境界に解決する。`Err` は 400 の本文。
///
/// `to` 省略は `today` の月、`from` 省略は `to` から遡って [`DEFAULT_WINDOW_MONTHS`] か月。
/// [`MAX_WINDOW_MONTHS`] か月を超えると `Err`。
pub fn resolve_range(
    from: Option<&str>,
    to: Option<&str>,
    today: NaiveDate,
) -> Result<(NaiveDate, NaiveDate), String> {
    let to_month = match to {
        Some(s) => month_of(s).ok_or("to は YYYY-MM で指定してください")?,
        None => today.with_day(1).expect("day 1 always exists"),
    };
    let from_month = match from {
        Some(s) => month_of(s).ok_or("from は YYYY-MM で指定してください")?,
        None => add_months(to_month, -(DEFAULT_WINDOW_MONTHS - 1)),
    };
    if from_month > to_month {
        return Err("from は to 以前にしてください".to_string());
    }
    let span = (to_month.year() - from_month.year()) * 12
        + (to_month.month0() as i32 - from_month.month0() as i32)
        + 1;
    // span はメッセージに含めない (1 行に収めて CLAUDE.md の折り返し罠を避けるため)
    if span > MAX_WINDOW_MONTHS {
        return Err(format!("月範囲 上限{MAX_WINDOW_MONTHS}"));
    }
    Ok((from_month, add_months(to_month, 1)))
}

/// [`STALE_MONTHS_SQL`] の 1 行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaleMonthRow {
    pub month: String,
    pub stale_drivers: i64,
    /// 0 は「まだ畳まれていない (データが無い)」の意味 (`stale_drivers: 0` 単体だと収束済みと区別できない)。
    pub total_drivers: i64,
}

/// 応答の本文 (`[lo, hi)` は [`resolve_range`] の戻り)。
pub fn response(
    logic_version: &str,
    lo: NaiveDate,
    hi: NaiveDate,
    rows: &[StaleMonthRow],
) -> serde_json::Value {
    let months: Vec<serde_json::Value> = rows
        .iter()
        .map(|r| {
            serde_json::json!({
                "month": r.month,
                "stale_drivers": r.stale_drivers,
                "total_drivers": r.total_drivers,
            })
        })
        .collect();
    serde_json::json!({
        "logic_version": logic_version,
        "from": lo.format("%Y-%m").to_string(),
        "to": add_months(hi, -1).format("%Y-%m").to_string(),
        "default_window_months": DEFAULT_WINDOW_MONTHS,
        "months": months,
    })
}
