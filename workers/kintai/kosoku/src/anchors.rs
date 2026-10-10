//! 遡って読んだ行を乗務員ごとの窓へ切り戻す (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版の `kintai_fold` から中身を変えずに移したもの。

use crate::window::{month_range, parse_dt};

/// 乗務員CD → 読みの遡り起点 (`YYYY-MM-DD HH:MM:SS`)。
/// `kintai_repo::KintaiEventsApi::fetch_month_head_anchors` の戻り値。
pub type HeadAnchors = std::collections::BTreeMap<u64, String>;

/// 乗務員ごとに `[起点 or 月初, to)` へ切り戻す。**切り戻した後に 0 行の乗務員は
/// 落とす** — 広げた窓のせいで現れただけの乗務員に単位を立てない (今までの母集団を
/// 保つ)。
///
/// 述語は HTTP 実装の読み (`kintai_http_repo` の `in_window`) と同じ
/// [`crate::window::window_holds`]。起点の無い乗務員は、月初から読んだ
/// ときと同じ行の集合に戻る。時刻が読めない行は落とさない (読み先が窓で絞って
/// 返したものなので、今までも入っていた)。
pub fn clip_to_anchors(
    by_driver: Vec<(u64, Vec<serde_json::Value>)>,
    month: &str,
    anchors: &HeadAnchors,
) -> Vec<(u64, Vec<serde_json::Value>)> {
    let Some((month_start, to)) = month_range(month) else {
        return by_driver;
    };
    let (Some(month_start), Some(to)) = (parse_dt(&month_start), parse_dt(&to)) else {
        return by_driver;
    };
    let at = |r: &serde_json::Value, k: &str| r.get(k).and_then(|v| v.as_str()).and_then(parse_dt);
    by_driver
        .into_iter()
        .filter_map(|(cd, rows)| {
            let from = anchors
                .get(&cd)
                .and_then(|a| parse_dt(a))
                .unwrap_or(month_start);
            let rows: Vec<serde_json::Value> = rows
                .into_iter()
                .filter(|r| match at(r, "datetime") {
                    Some(start) => {
                        let end = at(r, "end_datetime");
                        crate::window::window_holds(start, end, from, to)
                    }
                    None => true,
                })
                .collect();
            (!rows.is_empty()).then_some((cd, rows))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(at: &str, end: Option<&str>) -> serde_json::Value {
        match end {
            Some(end) => json!({"datetime": at, "end_datetime": end}),
            None => json!({"datetime": at}),
        }
    }

    /// 起点のある乗務員は起点から、無い乗務員は月初から。0 行になった乗務員は落とす。
    #[test]
    fn clips_each_driver_to_its_anchor_or_the_month_start() {
        let anchors: HeadAnchors = [(1, "2026-03-31 21:00:00".to_string())]
            .into_iter()
            .collect();
        let by_driver = vec![
            (
                1,
                vec![
                    row("2026-03-31 21:30:00", None),
                    row("2026-03-31 20:00:00", None),
                ],
            ),
            (2, vec![row("2026-03-31 21:30:00", None)]),
            (
                3,
                vec![
                    row("2026-03-31 23:00:00", Some("2026-04-01 01:00:00")),
                    json!({"state": "時刻なし"}),
                ],
            ),
        ];
        let got = clip_to_anchors(by_driver, "2026-04", &anchors);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], (1, vec![row("2026-03-31 21:30:00", None)]));
        assert_eq!(got[1].0, 3);
        assert_eq!(
            got[1].1.len(),
            2,
            "月初に終わる区間と時刻の読めない行は残す"
        );
    }

    #[test]
    fn a_bad_month_returns_the_rows_untouched() {
        let by_driver = vec![(1, vec![row("2000-01-01 00:00:00", None)])];
        let got = clip_to_anchors(by_driver.clone(), "bad", &HeadAnchors::new());
        assert_eq!(got, by_driver);
    }
}
