//! **末尾検知 (tail gap) が鳴らしている乗務員を、乗務員別に名指しする** (Refs #205)。
//!
//! **直す口ではなく、見える口。** 月ゲートの封の条件・閾値・warning 文言には
//! 一切触れない — [`crate::kintai_http_repo::InputCoverage`] が実際に見ている量
//! (最後の運行開始日 → 窓末尾までの空き) を、オンプレの実データで乗務員ごとに
//! 並べるだけ。
//!
//! ## これは alc の tail gap 警告と**同じ量ではない**
//!
//! 本物の警告 ([`crate::kintai_http_repo::missing_input_warnings`]) は GCP 側で
//! **alc の etags** (R2 の CSV の有無) から測っている。この口が読むのは
//! **オンプレの MariaDB 直読み** (`dtako_events`) — 経路も母集団の取り方も別物。
//! `/health` の `backends.kintai_events` が `mariadb` のとき、オンプレは alc を
//! 呼べない ([`crate::kintai_reading_dates`] のモジュール docs と同じ理由)。
//! **代理として使うのは構わないが、値が 1:1 では一致しない。**
//!
//! ## 母集団はミラーする — 「その月に運行が始まった乗務員」だけ
//!
//! [`crate::kintai_http_repo::InputCoverage`] の母集団の絞り方 (Refs #205 の 37)
//! と同じ理由で、**この月に `dtako_events` の `運行開始` を 1 件も持たない乗務員は
//! 対象に入れない。** 入れると「そもそも稼働していない」全員が「末尾が欠けた」に
//! 化ける (本番実測でこの絞りが無かったとき 73 名が 37 日超で鳴った実例がある)。
//!
//! **この絞りだけでは「月の途中で退職・長期休暇に入った」形は排除できない** —
//! その乗務員は月の前半に運行を持つので母集団には入り、末尾の空きは大きいまま
//! 鳴り続ける。**空き期間に打刻 ([`crate::kintai_repo`] の `time_card_dstate`) が
//! あったかどうか**が、「運行記録だけが欠けている」(本物) と
//! 「その期間は働いていない」(データの問題ではない) を分ける材料になる ——
//! かどうかを実データで確かめるのがこの口の目的。
//!
//! ## 閾値は複製しない
//!
//! [`crate::kintai_http_repo::MAX_TAIL_GAP_DAYS`] をそのまま読む。値をここに
//! 複製すると、片方だけ変えたときに気付けずに drift する。

use std::collections::HashMap;

use chrono::NaiveDate;
use serde::Serialize;

use crate::kintai_http_repo::MAX_TAIL_GAP_DAYS;

/// 運行の開始を示す `dtako_events` の `イベント名`。
/// alc の `unko_no_start_date` に相当する量を、オンプレの実イベントから直接取る。
const START_EVENT_STATE: &str = "運行開始";

/// 打刻 2 表 (`time_card_dstate` / `time_card_dtako`) の `source`。
/// [`crate::kintai_repo`] の `EVENTS_SQL` / `ALL_EVENTS_SQL` が付ける値と同じ。
const TIMECARD_SOURCE: &str = "timecard";

/// `dtako_events` の `source` 名。同じく `ALL_EVENTS_SQL` と揃える。
const DTAKO_EVENTS_SOURCE: &str = "dtako_events";

/// 乗務員 1 人ぶんの末尾検知の実測値。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct DriverTailGap {
    pub driver_cd: u64,
    /// この乗務員の `運行開始` の最終日 (月の窓の中)。
    pub last_start_date: String,
    /// `expected` からの空き日数。alc の `tail_gap` が見ている量と同じ定義。
    pub gap_days: i64,
    /// `MAX_TAIL_GAP_DAYS` を超えているか (= いま alc なら鳴る側かどうかの目安)。
    pub over_threshold: bool,
    /// **空き期間 (`last_start_date` の翌日 〜 `expected`) に打刻があったか。**
    /// `false` なら「その期間は働いていない」と読める候補、`true` なら
    /// 「打刻はあるのに運行記録が無い」= 本物の欠けの候補。
    pub punched_in_gap: bool,
}

/// 実測ぜんたい。
#[derive(Debug, Clone, Serialize)]
pub struct TailGapProbe {
    pub month: String,
    /// 末尾がここまで届いていてほしい日 (進行中の月は `today - 1 日` に切り下げ)。
    pub expected: String,
    /// このとき参照した [`MAX_TAIL_GAP_DAYS`] の値 (複製ではなくミラー)。
    pub threshold_days: i64,
    /// 母集団 (その月に運行が始まった乗務員) の人数。
    pub population: usize,
    /// 閾値を超えた人数。alc の warning 文言の `n名` に相当する量 (別経路の値)。
    pub over_threshold_total: usize,
    /// 閾値を超え、かつ空き期間に打刻も無かった人数
    /// (「働いていない」で説明がつく候補の数)。
    pub over_threshold_unpunched_total: usize,
    /// **名指し。** 母集団全員 (空き日数の降順)。閾値を超えた人だけ知りたいときは
    /// `over_threshold` で絞る。
    pub drivers: Vec<DriverTailGap>,
}

/// `row[field]` の先頭 10 桁 (`YYYY-MM-DD`) を日付として読む。
fn row_date(row: &serde_json::Value, field: &str) -> Option<NaiveDate> {
    let s = row.get(field)?.as_str()?;
    NaiveDate::parse_from_str(s.get(..10)?, "%Y-%m-%d").ok()
}

/// `driver_id` を読む。0 以下は乗務員CD ではないので捨てる
/// ([`crate::kintai_repo`] の `timecard_driver_cds` と同じ理由)。
fn row_driver(row: &serde_json::Value) -> Option<u64> {
    row.get("driver_id")
        .and_then(|v| v.as_u64())
        .filter(|d| *d > 0)
}

/// `rows` (乗務員別または全乗務員の `fetch_all_events_between` / `fetch_events_between`
/// の生行) から、末尾検知を乗務員ごとに組み立てる。
///
/// `expected` は呼び出し側が決める (窓の末尾 or 進行中の月なら `today - 1日`) —
/// 「いま」に依存する判断はここに持ち込まない (`reading_dates` / `rest_diff` と同じ
/// 「純粋関数に外から日付を渡す」形)。
///
/// `driver_filter` を渡すと、その乗務員 1 人だけに絞る (母集団の判定もその 1 人で行う —
/// 全体の中で何位かではなく、その人が対象かどうかを見る用途)。
pub fn tail_gap_probe(
    rows: &[serde_json::Value],
    month: &str,
    expected: NaiveDate,
    driver_filter: Option<u64>,
) -> TailGapProbe {
    let mut last_start: HashMap<u64, NaiveDate> = HashMap::new();
    let mut punch_dates: HashMap<u64, Vec<NaiveDate>> = HashMap::new();

    for row in rows {
        let Some(driver) = row_driver(row) else {
            continue;
        };
        if driver_filter.is_some_and(|want| want != driver) {
            continue;
        }
        let source = row.get("source").and_then(|v| v.as_str()).unwrap_or("");
        let state = row.get("state").and_then(|v| v.as_str()).unwrap_or("");
        if source == DTAKO_EVENTS_SOURCE && state == START_EVENT_STATE {
            if let Some(d) = row_date(row, "datetime") {
                last_start
                    .entry(driver)
                    .and_modify(|l: &mut NaiveDate| *l = (*l).max(d))
                    .or_insert(d);
            }
        } else if source == TIMECARD_SOURCE {
            if let Some(d) = row_date(row, "datetime") {
                punch_dates.entry(driver).or_default().push(d);
            }
        }
    }

    let mut drivers: Vec<DriverTailGap> = last_start
        .into_iter()
        .map(|(driver_cd, last)| {
            let gap_days = (expected - last).num_days();
            let punched_in_gap = punch_dates
                .get(&driver_cd)
                .is_some_and(|dates| dates.iter().any(|d| *d > last && *d <= expected));
            DriverTailGap {
                driver_cd,
                last_start_date: last.to_string(),
                gap_days,
                over_threshold: gap_days > MAX_TAIL_GAP_DAYS,
                punched_in_gap,
            }
        })
        .collect();
    // 空き日数の降順、同点は乗務員CD 昇順 (「一番怪しい人」から読める並び)
    drivers.sort_by(|a, b| {
        b.gap_days
            .cmp(&a.gap_days)
            .then(a.driver_cd.cmp(&b.driver_cd))
    });

    let over_threshold_total = drivers.iter().filter(|d| d.over_threshold).count();
    let over_threshold_unpunched_total = drivers
        .iter()
        .filter(|d| d.over_threshold && !d.punched_in_gap)
        .count();

    TailGapProbe {
        month: month.to_string(),
        expected: expected.to_string(),
        threshold_days: MAX_TAIL_GAP_DAYS,
        population: drivers.len(),
        over_threshold_total,
        over_threshold_unpunched_total,
        drivers,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn start(driver: u64, date: &str) -> serde_json::Value {
        json!({
            "datetime": format!("{date} 08:00:00"),
            "driver_id": driver,
            "source": "dtako_events",
            "state": "運行開始",
        })
    }

    fn punch(driver: u64, date: &str) -> serde_json::Value {
        json!({
            "datetime": format!("{date} 07:45:00"),
            "driver_id": driver,
            "source": "timecard",
            "state": "始業",
        })
    }

    fn other_dtako_event(driver: u64, date: &str, state: &str) -> serde_json::Value {
        json!({
            "datetime": format!("{date} 09:00:00"),
            "driver_id": driver,
            "source": "dtako_events",
            "state": state,
        })
    }

    fn d(s: &str) -> NaiveDate {
        NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
    }

    /// 空きが閾値以内なら鳴らない側 (over_threshold = false)。
    #[test]
    fn a_short_gap_is_not_over_threshold() {
        let rows = vec![start(1078, "2026-06-25")];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-30"), None);
        assert_eq!(probe.population, 1);
        let it = &probe.drivers[0];
        assert_eq!(it.driver_cd, 1078);
        assert_eq!(it.last_start_date, "2026-06-25");
        assert_eq!(it.gap_days, 5);
        assert!(!it.over_threshold);
        assert!(!it.punched_in_gap);
        assert_eq!(probe.over_threshold_total, 0);
        assert_eq!(probe.over_threshold_unpunched_total, 0);
    }

    /// **閾値超え・打刻も無い** = 「働いていない」で説明がつく候補。
    #[test]
    fn a_long_unpunched_gap_is_flagged_without_a_punch() {
        let rows = vec![start(1517, "2026-06-05")];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        let it = &probe.drivers[0];
        assert_eq!(it.gap_days, 24);
        assert!(it.over_threshold);
        assert!(!it.punched_in_gap);
        assert_eq!(probe.over_threshold_total, 1);
        assert_eq!(probe.over_threshold_unpunched_total, 1);
    }

    /// **閾値超え・空き期間に打刻あり** = 「打刻はあるのに運行記録が無い」候補
    /// (本物の欠けの疑い)。`over_threshold_unpunched_total` には数えない。
    #[test]
    fn a_long_gap_with_a_punch_is_flagged_but_not_counted_as_unpunched() {
        let rows = vec![start(1688, "2026-06-05"), punch(1688, "2026-06-20")];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        let it = &probe.drivers[0];
        assert!(it.over_threshold);
        assert!(it.punched_in_gap);
        assert_eq!(probe.over_threshold_total, 1);
        assert_eq!(probe.over_threshold_unpunched_total, 0);
    }

    /// 打刻が空き期間の**外** (last_start_date 以前) にしか無ければ数えない —
    /// 「その空きに打刻があったか」であって「月にいつか打刻したか」ではない。
    #[test]
    fn a_punch_before_the_gap_does_not_count() {
        let rows = vec![start(1445, "2026-06-10"), punch(1445, "2026-06-10")];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        assert!(!probe.drivers[0].punched_in_gap);
    }

    /// **母集団の絞り**: この月に `運行開始` を 1 件も持たない乗務員 (打刻だけ
    /// ある、または他の `dtako_events` 種別しか無い) は対象外
    /// (Refs #205 の 37 と同じ絞り方をミラー)。
    #[test]
    fn drivers_without_a_start_event_are_excluded_from_the_population() {
        let rows = vec![
            punch(9999, "2026-06-10"),
            other_dtako_event(9998, "2026-06-10", "休息"),
        ];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        assert_eq!(probe.population, 0);
        assert!(probe.drivers.is_empty());
    }

    /// `driver_id` が 0 以下の行は捨てる (乗務員CD ではない)。
    #[test]
    fn a_zero_driver_id_is_ignored() {
        let rows = vec![start(0, "2026-06-10")];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        assert_eq!(probe.population, 0);
    }

    /// 同じ乗務員の複数運行は最大日を取る。
    #[test]
    fn the_latest_start_date_wins() {
        let rows = vec![
            start(1078, "2026-06-05"),
            start(1078, "2026-06-20"),
            start(1078, "2026-06-11"),
        ];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        assert_eq!(probe.drivers[0].last_start_date, "2026-06-20");
    }

    /// `driver_filter` はその乗務員だけに絞る。
    #[test]
    fn a_driver_filter_narrows_to_one_person() {
        let rows = vec![start(1078, "2026-06-05"), start(1517, "2026-06-05")];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), Some(1517));
        assert_eq!(probe.population, 1);
        assert_eq!(probe.drivers[0].driver_cd, 1517);
    }

    /// 並びは空き日数の降順、同点は乗務員CD 昇順。
    #[test]
    fn drivers_are_sorted_by_gap_then_driver_cd() {
        let rows = vec![
            start(1002, "2026-06-20"),
            start(1001, "2026-06-20"),
            start(1500, "2026-06-01"),
        ];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        let ids: Vec<u64> = probe.drivers.iter().map(|d| d.driver_cd).collect();
        assert_eq!(ids, vec![1500, 1001, 1002]);
    }

    /// `datetime` が読めない (欠損 / 壊れた形式) 行は無視する。読めないものを
    /// 「開始日」として使わない (推測で埋めない)。
    #[test]
    fn an_unparseable_datetime_is_ignored() {
        let rows = vec![json!({
            "datetime": "not-a-date",
            "driver_id": 1078,
            "source": "dtako_events",
            "state": "運行開始",
        })];
        let probe = tail_gap_probe(&rows, "2026-06", d("2026-06-29"), None);
        assert_eq!(probe.population, 0);
    }

    /// 応答に閾値をそのまま載せる (複製ではなくミラー)。
    #[test]
    fn the_threshold_mirrors_the_gate_constant() {
        let probe = tail_gap_probe(&[], "2026-06", d("2026-06-29"), None);
        assert_eq!(probe.threshold_days, MAX_TAIL_GAP_DAYS);
        assert_eq!(probe.population, 0);
        assert_eq!(probe.month, "2026-06");
        assert_eq!(probe.expected, "2026-06-29");
    }
}
