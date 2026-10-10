//! `GET /api/kintai/timecard/drivers`・`GET /api/kintai/timecard/events` の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版の `kintai_diff.rs` (`drivers_page`) と `routes/kintai_timecard.rs` (`window_events` の
//! `parse_months`・`window_bounds`) から I/O を除いた部分を移したもの。往復の順と「オンプレは外へ出ない」は
//! オンプレ版の `kintai_diff` のモジュール docs を参照。

use std::collections::BTreeSet;

use serde_json::{json, Value};

use crate::window::{exact_month_range, is_valid_month};

/// 1 回の呼び出しで返す乗務員数の既定。
///
/// 経緯 — かつての根拠「1 乗務員あたり 0.2 秒」は本番で成立していなかった。
/// 2026-07-30 の初回 dry-run では 10 人ぶんの `POST /api/kintai/timecard/diff` が
/// Cloudflare の 524 (100 秒) を超え、1 人なら通った。原因は乗務員ごとの読み出しが
/// `dtako_events` と `dtako_cars` まで引いていたこと (#225) — 押し出さない行だった。
///
/// 打刻 2 表に絞ったあとの実測 (2026-07-31、2026-06 = 94 名):
///
/// | 1 回の人数 | 結果 |
/// |---|---|
/// | 10 | 通る (524 が消えた) |
/// | 50 | 通る |
///
/// **50 は実測済みなので既定に上げる。** 94 名なら 2 回で終わる。
pub const DEFAULT_MAX_DRIVERS: usize = 50;

/// `max_drivers` の上限。呼び出し側が大きな値を入れて Tunnel を殺すのを防ぐ。
///
/// **100 は未実測。** 50 が通ったこと・上限に当たっても 524 で落ちるだけで
/// **1 件も書かれない** (この経路は読むだけ、呼び直せば同じ状態に収束する) ことから、
/// 現在の頭数 (94 名) が 1 回で終わる値まで開ける。踏んだら下げればよい。
pub const MAX_MAX_DRIVERS: usize = 100;

/// `GET /api/kintai/timecard/drivers` の 1 ページ。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct DriversPage {
    pub drivers: Vec<u64>,
    /// 続きの位置。`None` なら回りきった。
    pub next_after_driver_cd: Option<u64>,
    /// 洗い出しにかかった時間 (ms)。ページごとに毎回払う費用なので出す。読みを測る呼び手が入れる
    pub elapsed_ms: u64,
}

/// 対象月に打刻がある乗務員 (昇順、`all`) から、`after` の次の `max` 人 (1〜[`MAX_MAX_DRIVERS`] に丸める) を
/// 1 ページにする。続きがあれば `next_after_driver_cd` = このページの最後。`elapsed_ms` は 0 (呼び手が入れる)。
pub fn page_drivers(all: Vec<u64>, after: Option<u64>, max: usize) -> DriversPage {
    let max = max.clamp(1, MAX_MAX_DRIVERS);
    let rest: Vec<u64> = match after {
        Some(a) => all.into_iter().filter(|d| *d > a).collect(),
        None => all,
    };
    let next = rest.get(max).map(|_| rest[max - 1]);
    DriversPage {
        drivers: rest.into_iter().take(max).collect(),
        next_after_driver_cd: next,
        elapsed_ms: 0,
    }
}

/// `timecard/drivers` の応答 `{month, drivers, next_after_driver_cd, elapsed_ms}`。
pub fn drivers_json(month: &str, page: &DriversPage) -> Value {
    json!({
        "month": month,
        "drivers": page.drivers,
        "next_after_driver_cd": page.next_after_driver_cd,
        "elapsed_ms": page.elapsed_ms,
    })
}

/// `months=` が読めない (400)。本文は [`std::fmt::Display`] (オンプレ版の文言そのまま)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonthsError {
    /// 空 (カンマと空白だけも)
    Empty,
    /// `YYYY-MM` でない月 (最初の 1 つ)
    Bad(String),
}

impl std::fmt::Display for MonthsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Empty => f.write_str("months は YYYY-MM をカンマ区切りで指定してください"),
            Self::Bad(m) => write!(f, "month は YYYY-MM です: {m}"),
        }
    }
}

/// `months=YYYY-MM,YYYY-MM` を検証して返す。**重複は潰し、昇順に揃える。**
pub fn parse_months(raw: &str) -> Result<Vec<String>, MonthsError> {
    let months: BTreeSet<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if months.is_empty() {
        return Err(MonthsError::Empty);
    }
    if let Some(bad) = months.iter().find(|m| !is_valid_month(m)) {
        return Err(MonthsError::Bad(bad.clone()));
    }
    Ok(months.into_iter().collect())
}

/// 窓ぜんたいの `[最初の月初, 最後の翌月初)` を MariaDB 用の文字列で返す。
///
/// 月が飛んでいても 1 クエリで読む — 隙間ぶんが混ざっても受け側が窓の外として
/// 落とすので、往復を増やすより安い。
pub fn window_bounds(months: &[String]) -> Option<(String, String)> {
    let first = exact_month_range(months.first()?)?;
    let last = exact_month_range(months.last()?)?;
    Some((first.0, last.1))
}

/// `timecard/events` の応答 `{months, drivers, events, elapsed_ms}`。`drivers` は `events` の `driver_id` の昇順・重複なし
/// (数でない行は数えない)。
pub fn window_events_json(months: &[String], events: Vec<Value>, elapsed_ms: u64) -> Value {
    let drivers: Vec<u64> = events
        .iter()
        .filter_map(|r| r.get("driver_id").and_then(|v| v.as_u64()))
        .collect::<BTreeSet<u64>>()
        .into_iter()
        .collect();
    json!({
        "months": months,
        "drivers": drivers,
        "events": events,
        "elapsed_ms": elapsed_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_follows_after_and_names_the_next() {
        let all = vec![1100, 1200, 1300, 1400];
        let first = page_drivers(all.clone(), None, 2);
        assert_eq!(first.drivers, vec![1100, 1200]);
        assert_eq!(first.next_after_driver_cd, Some(1200));
        let second = page_drivers(all.clone(), Some(1200), 2);
        assert_eq!(second.drivers, vec![1300, 1400]);
        // 回りきったら null
        assert_eq!(second.next_after_driver_cd, None);
        assert_eq!(page_drivers(all, None, 10).next_after_driver_cd, None);
    }

    #[test]
    fn max_is_clamped() {
        let all: Vec<u64> = (1..=200).collect();
        // 0 は 1 人に
        assert_eq!(page_drivers(all.clone(), None, 0).drivers, vec![1]);
        // 上限を超えたら MAX_MAX_DRIVERS で区切る
        let page = page_drivers(all, None, 9_999);
        assert_eq!(page.drivers.len(), MAX_MAX_DRIVERS);
        assert_eq!(page.next_after_driver_cd, Some(MAX_MAX_DRIVERS as u64));
    }

    #[test]
    fn the_drivers_response_keeps_null_for_the_end() {
        let page = DriversPage {
            drivers: vec![1018],
            next_after_driver_cd: None,
            elapsed_ms: 12,
        };
        let v = drivers_json("2026-07", &page);
        assert_eq!(
            v.to_string(),
            r#"{"drivers":[1018],"elapsed_ms":12,"month":"2026-07","next_after_driver_cd":null}"#
        );
    }

    #[test]
    fn months_are_deduped_and_sorted() {
        let got = parse_months(" 2026-07,2026-06,,2026-07 ").unwrap();
        assert_eq!(got, vec!["2026-06".to_string(), "2026-07".to_string()]);
    }

    #[test]
    fn bad_months_are_named() {
        assert_eq!(parse_months(""), Err(MonthsError::Empty));
        assert_eq!(parse_months(" , "), Err(MonthsError::Empty));
        let e = parse_months("2026-06,nope").unwrap_err();
        assert_eq!(e, MonthsError::Bad("nope".to_string()));
        assert_eq!(e.to_string(), "month は YYYY-MM です: nope");
        assert_eq!(
            MonthsError::Empty.to_string(),
            "months は YYYY-MM をカンマ区切りで指定してください"
        );
    }

    #[test]
    fn the_window_covers_first_to_last() {
        let months = vec!["2026-06".to_string(), "2026-08".to_string()];
        assert_eq!(
            window_bounds(&months),
            Some((
                "2026-06-01 00:00:00".to_string(),
                "2026-09-01 00:00:00".to_string()
            ))
        );
        assert_eq!(window_bounds(&[]), None);
        assert_eq!(window_bounds(&["nope".to_string()]), None);
    }

    #[test]
    fn window_events_name_their_drivers() {
        let events = vec![
            json!({"driver_id": 1200, "state": "始業"}),
            json!({"driver_id": 1018, "state": "始業"}),
            json!({"driver_id": 1200, "state": "終業"}),
            json!({"driver_id": "x", "state": "終業"}),
        ];
        let v = window_events_json(&["2026-07".to_string()], events, 5);
        assert_eq!(v["drivers"], json!([1018, 1200]));
        assert_eq!(v["events"].as_array().unwrap().len(), 4);
        assert_eq!(v["months"], json!(["2026-07"]));
        assert_eq!(v["elapsed_ms"], 5);
    }
}
