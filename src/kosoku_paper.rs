//! 紙のタイムカード表 (社内 CakePHP `TimeCardKosokuController`) の日別拘束の**再現**
//! (Refs ohishi-exp/nuxt-dtako-admin#501)。
//!
//! ## なぜ再現するのか
//!
//! 突合の残差が ±数分まで縮まった段階で、残る差は**秒の落とし方**だった。紙は打刻・
//! イベントの秒を保持したまま**区分ごとに**切り捨てて日計へ足すため、区分の切れ目が
//! 多い日に ±1 分が堆積する (正負両方向に出る)。[`crate::kosoku`] の勤務単位の計算に
//! この丸めを持ち込むと拘束の意味が変わってしまうので、**紙の値そのものを別に計算**し、
//! こちらの日別値との差 (`paper_drift_by_date`) を突合の説明 (cause `rounding`) に使う。
//!
//! ## 紙の丸めの正体 (2026-07-29、実データ 6 名で全日一致を確認)
//!
//! - **TC_DC**: 打刻 (`始業`/`終業`) と `time_card_dtako` (`運行開始`/`運行終了`/`休息`)
//!   を時刻順に並べ、**隣接する対**ごとに `date_diff()` の `h*60+i` = **経過秒切り捨て**
//!   で日計へ足す (`_make_tc_to_tc`)
//! - **デジタコ**: `dtako_events` (積み/降し/休憩/運転/その他/待機) の同日イベントは
//!   `区間時間` 列 = **端点をそれぞれ分に切り捨てた差**。連続するイベントでは端点が
//!   telescoping してこちらの区間計算と一致する — ずれるのは連鎖の切れ目だけ
//! - **日跨ぎ**は深夜 0 時で割り、前半は経過秒切り捨て・後半は終端の時刻 (分) を使う
//! - 昼休は運行を挟まない対だけ、窓 (12:00-13:00) との重なりを引く。`運行開始 → 始業`
//!   は `minus_unko` として **TC_DC が値を持つ日だけ**引かれる
//!
//! ## 含めないもの
//!
//! - **同日フェリー控除** (`_make_kosoku_time` の減算)。あれは紙側の二重控除で、突合は
//!   cause `ferry` として実額 ([`crate::kosoku::ferry_minus_by_date`]) で別に説明する。
//!   ここに入れると説明が二重になる
//! - `TimeCardKosokuExp` (除外休息の再加算) と `chng_state=99` の除外 — 読める列に無く、
//!   実データ 6 名 (1021/1071/1634/1714/1729/1194、2026-03 全日) では発生しなかった。
//!   発生すれば残差が unknown に残るので、そのとき次の型として扱う

use std::collections::BTreeMap;

use chrono::{Duration, NaiveDateTime, Timelike};

use crate::kosoku::{parse_events, DaySummary, Event};

/// 紙のデジタコ type が数えるイベント名 (`_make_kosoku_time` の `イベント名 in`)。
/// 道路種別 (一般道/高速道/専用道など) は運転の中に入れ子で記録されるため含まれない。
const DIGI_STATES: [&str; 6] = ["積み", "降し", "休憩", "運転", "その他", "待機"];

/// `date_diff()->h*60 + ->i` 相当: 経過秒を切り捨てた分。**日の成分は落とす** —
/// 紙は「昨日の 0 時からの差」で終端の時刻を取り出すとき、意図的に日を無視している。
fn elapsed_min_drop_days(a: NaiveDateTime, b: NaiveDateTime) -> i64 {
    let s = (b - a).num_seconds();
    (s % 86_400) / 60
}

/// 経過秒切り捨て (日も含む)。対の窓 (d<1 等) を通った後の加算に使う。
fn elapsed_min(a: NaiveDateTime, b: NaiveDateTime) -> i64 {
    (b - a).num_seconds() / 60
}

/// 区間時間の再現: 端点をそれぞれ分に切り捨てた差。
fn endpoint_floor_min(a: NaiveDateTime, b: NaiveDateTime) -> i64 {
    let fa = a - Duration::seconds(a.second() as i64);
    let fb = b - Duration::seconds(b.second() as i64);
    (fb - fa).num_seconds() / 60
}

/// その日の翌日 0 時。
fn midnight_after(dt: NaiveDateTime) -> NaiveDateTime {
    (dt + Duration::days(1))
        .date()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

/// PHP `DateInterval` の `d` / `h` 成分 (絶対値)。対の窓 (`d < 2 && h < 14` 等) の判定用。
fn interval_d_h(a: NaiveDateTime, b: NaiveDateTime) -> (i64, i64) {
    let s = (b - a).num_seconds().abs();
    (s / 86_400, (s % 86_400) / 3_600)
}

/// TC_DC の時系列 1 行。`st` は紙の `_make_tc_dc_events` が見る状態名。
struct TcRow {
    at: NaiveDateTime,
    st: String,
}

/// 打刻 + `time_card_dtako` を紙と同じ (時刻, 状態名) 順に並べる。
///
/// `time_card_dtako` の `休息` は開始 (state 20) と終了 (21) が同じ名前で来るので、
/// `dtako_events` の休息区間の端と突き合わせて `休息開始` / `休息終了` に読み替える。
/// どちらとも突き合わないものはそのまま残す — 対の判定には使われないが、
/// **隣接を切る**役は果たす (紙の stream にも居るため)。
fn tc_stream(events: &[Event], month: &str) -> Vec<TcRow> {
    let rest_starts: Vec<NaiveDateTime> = events
        .iter()
        .filter(|e| e.source == "dtako_events" && e.state == "休息")
        .map(|e| e.start)
        .collect();
    let rest_ends: Vec<NaiveDateTime> = events
        .iter()
        .filter(|e| e.source == "dtako_events" && e.state == "休息")
        .filter_map(|e| e.end)
        .collect();
    let mut out: Vec<TcRow> = Vec::new();
    for e in events {
        if !e.start.format("%Y-%m").to_string().eq(month) {
            continue;
        }
        let st = match e.source.as_str() {
            "timecard" => e.state.clone(),
            "dtako" => {
                if e.state == "休息" {
                    let is_start = rest_starts.contains(&e.start);
                    let is_end = rest_ends.contains(&e.start);
                    // 連続する休息 (前の終了 = 次の開始) は開始扱い — 紙の state 20 側
                    if is_start {
                        "休息開始".to_string()
                    } else if is_end {
                        "休息終了".to_string()
                    } else {
                        e.state.clone()
                    }
                } else {
                    e.state.clone()
                }
            }
            _ => continue,
        };
        out.push(TcRow { at: e.start, st });
    }
    out.sort_by(|a, b| (a.at, a.st.as_str()).cmp(&(b.at, b.st.as_str())));
    out
}

/// TC_DC type の日別 (`日 → 分`) と、`運行開始 → 始業` の減算 (`_make_minus_unko_day`)。
fn tc_dc_daily(stream: &[TcRow]) -> (BTreeMap<u32, i64>, BTreeMap<u32, i64>) {
    use chrono::Datelike;
    let mut day_ar: BTreeMap<u32, i64> = BTreeMap::new();
    let mut minus_unko: BTreeMap<u32, i64> = BTreeMap::new();
    for (k, tc) in stream.iter().enumerate() {
        let Some(next) = stream.get(k + 1) else {
            continue;
        };
        let (t, nt) = (tc.at, next.at);
        match (tc.st.as_str(), next.st.as_str()) {
            ("運行開始", "始業") => {
                if t.date() == nt.date() {
                    // 上書き (紙も代入) — 同じ日に 2 度あれば後の値が残る
                    minus_unko.insert(t.day(), elapsed_min(t, nt));
                }
            }
            ("始業", "運行開始") => {
                if nt <= t {
                    continue;
                }
                let (d, h) = interval_d_h(t, nt);
                if d < 2 && h < 14 && t.date() == nt.date() {
                    *day_ar.entry(nt.day()).or_default() += elapsed_min(t, nt);
                }
            }
            ("始業", "終業") => {
                let (d, _) = interval_d_h(t, nt);
                if d < 1 {
                    if t.date() == nt.date() {
                        *day_ar.entry(nt.day()).or_default() += elapsed_min(t, nt);
                    } else {
                        *day_ar.entry(t.day()).or_default() += elapsed_min(t, midnight_after(t));
                        *day_ar.entry(nt.day()).or_default() +=
                            i64::from(nt.hour() * 60 + nt.minute());
                    }
                    // 昼休: 運行を挟まない対だけがここへ来る (間に運行があれば隣接しない)
                    let noon = t.date().and_hms_opt(12, 0, 0).unwrap();
                    let one = t.date().and_hms_opt(13, 0, 0).unwrap();
                    if t < noon {
                        if nt > one {
                            *day_ar.entry(nt.day()).or_default() -= 60;
                        } else if nt > noon {
                            // 紙は `->i` (分の成分) だけ見る — 窓の中なので h は 0
                            *day_ar.entry(nt.day()).or_default() -=
                                elapsed_min_drop_days(noon, nt) % 60;
                        }
                    }
                }
            }
            ("運行終了", "終業") | ("休息開始", "終業") => {
                let (d, h) = interval_d_h(t, nt);
                if d < 2 && h < 14 && t.date() == nt.date() {
                    *day_ar.entry(nt.day()).or_default() += elapsed_min(t, nt);
                }
            }
            ("運行終了", "運行開始") => {
                let (d, h) = interval_d_h(t, nt);
                if d < 1 && h < 12 {
                    // 同日の縛りが無い — 深夜を跨いでも丸ごと運行開始の日へ乗る
                    *day_ar.entry(nt.day()).or_default() += elapsed_min(t, nt);
                }
            }
            _ => {}
        }
    }
    (day_ar, minus_unko)
}

/// デジタコ type の日別 (`日 → 分`)。同日イベントは `区間時間` (端点床)、日跨ぎは
/// 深夜 0 時で割る。**フェリー控除は含めない** (モジュール docs 参照)。
fn digitaco_daily(events: &[Event], month: &str) -> BTreeMap<u32, i64> {
    use chrono::Datelike;
    let mut day_ar: BTreeMap<u32, i64> = BTreeMap::new();
    let in_month = |dt: NaiveDateTime| dt.format("%Y-%m").to_string() == month;
    for e in events {
        if e.source != "dtako_events" || !DIGI_STATES.contains(&e.state.as_str()) {
            continue;
        }
        let Some(end) = e.end else { continue };
        if e.start.date() == end.date() {
            if in_month(e.start) && in_month(end) {
                *day_ar.entry(e.start.day()).or_default() += endpoint_floor_min(e.start, end);
            }
        } else {
            if in_month(e.start) {
                *day_ar.entry(e.start.day()).or_default() +=
                    elapsed_min(e.start, midnight_after(e.start));
            }
            if in_month(end) {
                // 紙は「開始日時の前日 0 時からの差」の h*60+i — 日を落とすので
                // 終端の時刻 (分) がそのまま乗る
                *day_ar.entry(end.day()).or_default() += i64::from(end.hour() * 60 + end.minute());
            }
        }
    }
    day_ar
}

/// 紙の日別拘束 (`YYYY-MM-DD → 分`) を再現する。値を持つ日だけ返す。
///
/// 入力は [`crate::kosoku::daily_summary`] と同じ 1 乗務員ぶんの生行。
pub fn paper_daily_minutes(rows: &[serde_json::Value], month: &str) -> BTreeMap<String, i64> {
    let events = parse_events(rows);
    let stream = tc_stream(&events, month);
    let (tc_dc, minus_unko) = tc_dc_daily(&stream);
    let digi = digitaco_daily(&events, month);
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    let days: std::collections::BTreeSet<u32> = tc_dc.keys().chain(digi.keys()).copied().collect();
    for day in days {
        // minus_unko は **TC_DC が値を持つ日だけ**効く (紙は null の日に着地しない —
        // nuxt-dtako-admin#517 と同じ規則)
        let tc = tc_dc
            .get(&day)
            .map(|v| v - minus_unko.get(&day).copied().unwrap_or(0));
        let total = tc.unwrap_or(0) + digi.get(&day).copied().unwrap_or(0);
        out.insert(format!("{month}-{day:02}"), total);
    }
    out
}

/// こちらの日別値 (暦日按分後) と紙の再現値の差 (`ours − paper`) を日別に返す。
/// **両方に値がある日だけ**、差が 0 でない日だけ載せる — 突合 (relay) が cause
/// `rounding` の実額として使う。正 = こちらが大きい (紙が小さく数えている)。
pub fn paper_drift_by_date(
    days: &[DaySummary],
    paper: &BTreeMap<String, i64>,
) -> BTreeMap<String, i64> {
    let mut ours: BTreeMap<String, i64> = BTreeMap::new();
    for d in days {
        // relay の kosokuPartsByDate と同じ読み方: 日跨ぎ勤務は parts、単日は本体
        if d.parts.is_empty() {
            *ours.entry(d.date.clone()).or_default() += d.restraint_minutes;
        } else {
            for p in &d.parts {
                *ours.entry(p.date.clone()).or_default() += p.restraint_minutes;
            }
        }
    }
    ours.into_iter()
        .filter_map(|(date, o)| {
            let p = paper.get(&date)?;
            (o != *p).then_some((date, o - p))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kosoku::{daily_summary, KosokuParams};
    use serde_json::json;

    fn tc(datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "source": "timecard", "state": state})
    }

    fn dtako(datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "source": "dtako", "state": state})
    }

    fn ev(start: &str, end: &str, state: &str) -> serde_json::Value {
        json!({"datetime": start, "end_datetime": end, "source": "dtako_events", "state": state})
    }

    /// 乗務員 1021 鈴木 / 2026-03-04 の実データそのまま。紙の値 449 =
    /// TC_DC 83 (運行終了 08:12:53 → 運行開始 09:36:06) − minus_unko 20
    /// (運行開始 07:09:16 → 始業 07:29:32) + デジタコ 386 (区間時間 = 端点床の合算)。
    /// 本番の pdf-json と一致することを確認済み。
    fn suzuki_0304() -> Vec<serde_json::Value> {
        vec![
            dtako("2026-03-04 07:09:16", "運行開始"),
            ev("2026-03-04 07:09:16", "2026-03-04 07:26:23", "運転"),
            ev("2026-03-04 07:09:16", "2026-03-04 08:12:53", "一般道空車"),
            ev("2026-03-04 07:26:23", "2026-03-04 08:12:53", "休憩"),
            tc("2026-03-04 07:29:32", "始業"),
            dtako("2026-03-04 08:12:53", "運行終了"),
            dtako("2026-03-04 09:36:06", "運行開始"),
            ev("2026-03-04 09:36:06", "2026-03-04 10:11:45", "一般道空車"),
            ev("2026-03-04 09:36:06", "2026-03-04 10:36:24", "運転"),
            ev("2026-03-04 10:11:45", "2026-03-04 10:20:04", "専用道"),
            ev("2026-03-04 10:20:04", "2026-03-04 13:38:43", "高速道"),
            dtako("2026-03-04 10:36:24", "休息"),
            ev("2026-03-04 10:36:24", "2026-03-04 13:43:58", "休息"),
            ev("2026-03-04 13:38:43", "2026-03-04 15:19:08", "一般道空車"),
            dtako("2026-03-04 13:43:58", "休息"),
            ev("2026-03-04 13:43:58", "2026-03-04 13:45:10", "運転"),
            ev("2026-03-04 13:45:10", "2026-03-04 14:03:24", "休憩"),
            ev("2026-03-04 14:03:24", "2026-03-04 15:19:08", "積み"),
            ev("2026-03-04 15:19:08", "2026-03-04 16:44:04", "休憩"),
            ev("2026-03-04 15:19:08", "2026-03-04 16:53:32", "一般道実車"),
            ev("2026-03-04 16:44:04", "2026-03-04 17:14:52", "運転"),
            ev("2026-03-04 16:53:32", "2026-03-04 16:58:45", "専用道"),
            ev("2026-03-04 16:58:45", "2026-03-05 06:00:11", "高速道"),
            ev("2026-03-04 17:14:52", "2026-03-04 17:25:00", "休憩"),
            ev("2026-03-04 17:25:00", "2026-03-04 18:06:21", "運転"),
            dtako("2026-03-04 18:06:21", "休息"),
            ev("2026-03-04 18:06:21", "2026-03-05 03:09:09", "休息"),
        ]
    }

    #[test]
    fn reproduces_the_paper_value_for_suzuki_2026_03_04() {
        let paper = paper_daily_minutes(&suzuki_0304(), "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&449));
    }

    #[test]
    fn drift_is_ours_minus_paper_only_when_both_have_values() {
        // 運行の無い打刻の対: こちらは昼休を休憩として数える (拘束 479) が、
        // 紙は拘束から引く (419)。drift はこの構造差も含んだ ours − paper = 60 —
        // だからこそ突合 (relay) は rounding を**最後の候補**として試す
        let rows = vec![
            tc("2026-03-04 09:00:30", "始業"),
            tc("2026-03-04 17:00:20", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days[0].restraint_minutes, 479);
        let paper = paper_daily_minutes(&rows, "2026-03");
        let drift = paper_drift_by_date(&days, &paper);
        assert_eq!(drift.get("2026-03-04"), Some(&60));
        // 値の無い日は載らない
        assert!(!drift.contains_key("2026-03-05"));
    }

    #[test]
    fn drift_is_omitted_when_ours_and_paper_agree() {
        // 秒の無い綺麗な対は両者一致 — 0 の drift は載せない
        let rows = vec![
            tc("2026-03-04 14:00:00", "始業"),
            tc("2026-03-04 20:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert!(paper_drift_by_date(&days, &paper).is_empty());
    }

    #[test]
    fn interval_time_is_endpoint_floor_not_elapsed_floor() {
        // 13:43:58 → 13:45:10 は経過 1 分 12 秒だが、紙の区間時間は端点床で 2 分
        let rows = vec![ev("2026-03-04 13:43:58", "2026-03-04 13:45:10", "運転")];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&2));
    }

    #[test]
    fn road_type_and_rest_events_are_not_counted() {
        // 道路種別は運転の中の入れ子、休息は紙も拘束から外す
        let rows = vec![
            ev("2026-03-04 10:20:04", "2026-03-04 13:38:43", "高速道"),
            ev("2026-03-04 10:36:24", "2026-03-04 13:43:58", "休息"),
        ];
        assert!(paper_daily_minutes(&rows, "2026-03").is_empty());
    }

    #[test]
    fn a_day_crossing_event_splits_at_midnight_with_truncation() {
        // 前半: 22:10:30 → 0:00 = 1 時間 49 分 30 秒 → 109 分 (経過秒切り捨て)。
        // 後半: 終端 06:00:11 の時刻から 360 分
        let rows = vec![ev("2026-03-04 22:10:30", "2026-03-05 06:00:11", "運転")];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&109));
        assert_eq!(paper.get("2026-03-05"), Some(&360));
    }

    #[test]
    fn punch_pair_truncates_elapsed_seconds() {
        // 13:05:30 → 21:05:20 = 経過 7:59:50 → 479 分 (端点床なら 480)。
        // 始業が 12:00 より後なので昼休控除は掛からない
        let rows = vec![
            tc("2026-03-04 13:05:30", "始業"),
            tc("2026-03-04 21:05:20", "終業"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&479));
    }

    #[test]
    fn a_full_lunch_hour_is_deducted_from_a_punch_pair() {
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            tc("2026-03-04 17:00:00", "終業"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&480));
    }

    #[test]
    fn a_partial_lunch_deducts_only_the_overlap() {
        // 1714 井上 03-04 の形: 終業が窓の中に落ちる対は重なりだけ引かれる
        let rows = vec![
            tc("2026-03-04 07:18:00", "始業"),
            tc("2026-03-04 12:20:30", "終業"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        // 経過 302 分 − 窓の重なり 20 分
        assert_eq!(paper.get("2026-03-04"), Some(&282));
    }

    #[test]
    fn a_day_crossing_punch_pair_splits_at_midnight() {
        // 前半 22:10:30 → 0:00 = 109 分、後半は終業 00:20:40 の時刻から 20 分
        let rows = vec![
            tc("2026-03-04 22:10:30", "始業"),
            tc("2026-03-05 00:20:40", "終業"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&109));
        assert_eq!(paper.get("2026-03-05"), Some(&20));
    }

    #[test]
    fn a_run_gap_lands_wholly_on_the_next_runs_day_even_across_midnight() {
        let rows = vec![
            dtako("2026-03-04 23:50:00", "運行終了"),
            dtako("2026-03-05 00:30:30", "運行開始"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert!(!paper.contains_key("2026-03-04"));
        assert_eq!(paper.get("2026-03-05"), Some(&40));
    }

    #[test]
    fn minus_unko_applies_only_when_tc_dc_has_a_value() {
        // 運行開始 → 始業 だけの日: minus_unko は検出されるが TC_DC が null なので
        // 着地しない (nuxt-dtako-admin#517 と同じ規則)。デジタコ側はそのまま
        let rows = vec![
            dtako("2026-03-04 07:09:16", "運行開始"),
            ev("2026-03-04 07:09:16", "2026-03-04 08:12:53", "運転"),
            tc("2026-03-04 07:29:32", "始業"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&63));
    }

    #[test]
    fn rest_rows_split_adjacency_between_run_end_and_run_start() {
        // 運行終了と次の運行開始の間に休息 (time_card_dtako) が挟まると対にならない —
        // 紙が継ぎ目を数えないのはこの形 (cause run-gap)
        let rows = vec![
            dtako("2026-03-04 08:12:53", "運行終了"),
            dtako("2026-03-04 08:30:00", "休息"),
            ev("2026-03-04 08:30:00", "2026-03-04 09:20:00", "休息"),
            dtako("2026-03-04 09:20:00", "休息"),
            dtako("2026-03-04 09:36:06", "運行開始"),
        ];
        assert!(paper_daily_minutes(&rows, "2026-03").is_empty());
    }

    #[test]
    fn punch_in_to_run_start_counts_the_head_wait() {
        // 始業 → 同日の運行開始。紙は TC_DC で出発待ちを数える (10 分 30 秒 → 10 分)
        let rows = vec![
            tc("2026-03-04 07:30:00", "始業"),
            dtako("2026-03-04 07:40:30", "運行開始"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&10));
    }

    #[test]
    fn a_punch_pair_a_full_day_apart_adds_nothing() {
        // 紙の対の窓は d < 1 — 24 時間以上離れた対は数えない
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            tc("2026-03-05 09:00:00", "終業"),
        ];
        assert!(paper_daily_minutes(&rows, "2026-03").is_empty());
    }

    #[test]
    fn drift_reads_parts_for_a_day_crossing_shift() {
        // 日跨ぎ勤務はこちらの値を parts (暦日按分) で読む。紙も深夜で割るので、
        // 秒の無い綺麗な対なら両日とも一致して drift は空
        let rows = vec![
            tc("2026-03-04 20:00:00", "始業"),
            tc("2026-03-05 04:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert!(!days[0].parts.is_empty());
        let paper = paper_daily_minutes(&rows, "2026-03");
        assert_eq!(paper.get("2026-03-04"), Some(&240));
        assert_eq!(paper.get("2026-03-05"), Some(&240));
        assert!(paper_drift_by_date(&days, &paper).is_empty());
    }

    #[test]
    fn run_end_to_punch_out_counts_the_tail() {
        // 運行終了 → 同日の終業。紙は TC_DC で尻尾を数える
        let rows = vec![
            dtako("2026-03-04 16:23:03", "運行終了"),
            tc("2026-03-04 16:40:59", "終業"),
        ];
        let paper = paper_daily_minutes(&rows, "2026-03");
        // 17 分 56 秒 → 17 分
        assert_eq!(paper.get("2026-03-04"), Some(&17));
    }

    #[test]
    fn a_simultaneous_run_start_after_punch_in_adds_nothing() {
        // 同時刻の対は紙もスキップする (重複計上しない)
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            dtako("2026-03-04 08:00:00", "運行開始"),
        ];
        assert!(paper_daily_minutes(&rows, "2026-03").is_empty());
    }

    #[test]
    fn an_unmatched_rest_row_still_splits_adjacency() {
        // dtako_events 側に対応する休息区間が無い休息行は名前のまま残し、
        // 運行終了 → 運行開始 の隣接を切る
        let rows = vec![
            dtako("2026-03-04 08:12:53", "運行終了"),
            dtako("2026-03-04 08:30:00", "休息"),
            dtako("2026-03-04 09:36:06", "運行開始"),
        ];
        assert!(paper_daily_minutes(&rows, "2026-03").is_empty());
    }

    #[test]
    fn events_outside_the_month_are_ignored() {
        let rows = vec![
            ev("2026-02-28 10:00:00", "2026-02-28 11:00:00", "運転"),
            tc("2026-02-28 09:00:00", "始業"),
            tc("2026-02-28 12:00:00", "終業"),
        ];
        assert!(paper_daily_minutes(&rows, "2026-03").is_empty());
    }
}
