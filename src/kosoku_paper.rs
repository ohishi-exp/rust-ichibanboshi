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

use crate::kosoku::{
    floor_min, midnight_after, parse_events, restraint_spans_and_diagnostics, shift_cover,
    subtract_intervals, DaySummary, Event, DIGI_STATES,
};

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

/// 紙が日計から引く `運行開始 → 始業` (`_make_minus_unko_day`) を日別に返す
/// (`YYYY-MM-DD → 分`、Refs #182 / nuxt-dtako-admin#546)。
///
/// 打刻より先に運行を始めた日、紙はその頭を**引く**のでこちらより小さくなる。
/// [`paper_daily_minutes`] の中では既に効いているが、突合の cause 候補としては
/// **実額が要る** — これが無いと `ours-outside` などと組み合わせられず、複合の日が
/// drift (cause `rounding`) の受け皿へ落ちる (実測 1729 / 2026-01-09 の −15 =
/// ours-outside 6 + minus_unko 9)。
///
/// 紙と同じく **TC_DC が値を持つ日だけ**返す — 紙が着地しない日は引かれないので、
/// 実額として出すと過剰説明になる。
pub fn minus_unko_by_date(rows: &[serde_json::Value], month: &str) -> BTreeMap<String, i64> {
    let events = parse_events(rows);
    let stream = tc_stream(&events, month);
    let (tc_dc, minus_unko) = tc_dc_daily(&stream);
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    for (day, minutes) in minus_unko {
        if minutes != 0 && tc_dc.contains_key(&day) {
            out.insert(format!("{month}-{day:02}"), minutes);
        }
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

/// 紙が**勤務の外で**数えている分 (`YYYY-MM-DD → 分`、Refs #182 フォローアップ)。
///
/// 紙は打刻に縛られず、デジタコのイベントと隣接対を数え続ける。こちらの勤務が
/// 覆っていない時間に紙の計上が残る形は 2 つ:
///
/// - **終業後もイベントが続く**: 状態の切り忘れ (実測 1069 前田 2026-01-05: 終業
///   17:17 の後も夜通し続く 16 時間の「積み」。紙は深夜 0 時まで 403 分、翌朝も
///   始業 07:36 まで 456 分を数える) や、終業打刻後の構内ミニ運行
///   (1018 金原 2026-03-03: 18〜27 秒の運行 2 本 = digi 1 分)
/// - **運行の継ぎ目が勤務の外にある**: 上の構内運行の間の 運行終了 → 運行開始
///   (1018 の 5 分)。隣接判定は紙と同じ stream ([`tc_stream`]) — 間に打刻や休息の
///   行が挟まれば紙も数えないので、こちらも数えない
///
/// 「外」は [`crate::kosoku::shift_cover`] (勤務 span + 始業前の運行の頭 — 頭は
/// cause `run-head` の領分で、ここに混ぜると二重説明になる)。突合 (relay) が
/// cause `paper-outside` の実額として使う (**紙が大きくなる向き**、`run-head` と同じ)。
///
/// 日跨ぎの外は暦日で切って各日へ足す。紙の深夜割り (経過床) との差は ±1 分で、
/// 突合の許容誤差 (2 分) の中。
pub fn paper_outside_by_date(rows: &[serde_json::Value], month: &str) -> BTreeMap<String, i64> {
    let events = parse_events(rows);
    let cover = shift_cover(&events);
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    let add_span = |out: &mut BTreeMap<String, i64>, s: NaiveDateTime, e: NaiveDateTime| {
        let mut cur = s;
        while cur < e {
            let bound = midnight_after(cur).min(e);
            *out.entry(cur.format("%Y-%m-%d").to_string()).or_default() +=
                (bound - cur).num_seconds() / 60;
            cur = bound;
        }
    };
    // デジタコ type: DIGI イベントのうち勤務が覆っていない部分 (端点床)。
    // 勤務の中に落ちる部分は、イベント重複の二重計上 (下) のために別に集める
    let mut digi_spans: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    let mut inside_sum: BTreeMap<String, i64> = BTreeMap::new();
    let mut inside_portions: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    for e in &events {
        if e.source != "dtako_events" || !DIGI_STATES.contains(&e.state.as_str()) {
            continue;
        }
        let Some(end) = e.end else { continue };
        let span = (floor_min(e.start), floor_min(end));
        if span.1 <= span.0 {
            continue;
        }
        digi_spans.push(span);
        for (s, x) in subtract_intervals(&[span], &cover) {
            add_span(&mut out, s, x);
        }
        for c in &cover {
            let (s, x) = (span.0.max(c.0), span.1.min(c.1));
            if x > s {
                add_span(&mut inside_sum, s, x);
                inside_portions.push((s, x));
            }
        }
    }
    let digi_spans = crate::kosoku::merge_intervals(digi_spans);
    // 紙の**イベント重複の二重計上**: 運行 NO が並走する日 (乗り換え・二重記録) は
    // DIGI イベント同士が重なる。紙はイベントごとに区間時間を足すので重なりが
    // 二重に積まれる — こちらは union で 1 回だけ数える (実測 1740 / 2026-05-09:
    // 並走 2 本の重なり 104 分)。勤務の外は上の加算がイベントごと (= 重なり込み) に
    // 数えているので、**勤務の中の重なりだけ**を足す
    let mut inside_union: BTreeMap<String, i64> = BTreeMap::new();
    for (s, x) in crate::kosoku::merge_intervals(inside_portions) {
        add_span(&mut inside_union, s, x);
    }
    for (date, total) in &inside_sum {
        let extra = total - inside_union.get(date).copied().unwrap_or(0);
        if extra > 0 {
            *out.entry(date.clone()).or_default() += extra;
        }
    }
    // TC_DC type: 隣接する 運行終了 → 運行開始 の対 (窓 d < 1 && h < 12) の勤務外の部分
    let stream = tc_stream(&events, month);
    for w in stream.windows(2) {
        let (t, nt) = (&w[0], &w[1]);
        if t.st != "運行終了" || nt.st != "運行開始" || nt.at <= t.at {
            continue;
        }
        let (d, h) = interval_d_h(t.at, nt.at);
        if !(d < 1 && h < 12) {
            continue;
        }
        let span = (floor_min(t.at), floor_min(nt.at));
        if span.1 <= span.0 {
            continue;
        }
        // 紙はこの対を**丸ごと運行開始の日**に載せる ([`tc_dc_daily`] の
        // 「同日の縛りが無い」)。暦日で割ると深夜を跨ぐ対の説明が前日と当日に
        // 散って一致しなくなる (実測 1634 / 2026-01-04: 583 分の対が 202+380 に
        // 割れていた) — 外に残った分をまとめて運行開始の日へ
        let mut gap_outside = 0i64;
        for (s, x) in subtract_intervals(&[span], &cover) {
            gap_outside += (x - s).num_seconds() / 60;
        }
        if gap_outside > 0 {
            *out
                .entry(nt.at.format("%Y-%m-%d").to_string())
                .or_default() += gap_outside;
        }
    }
    // 紙の**二重計上** (`time_card_dtako` の運行行が欠けた日、Refs #182 フォローアップ)。
    // 運行開始/終了の行が無いと 始業 → 終業 が隣接したままになり、紙は打刻の対
    // (TC_DC) と同じ時間のデジタコイベント (digi) を**両方**数える — 対とイベントの
    // 重なりがそのまま紙の水増しになる (実測 1108 福留 2026-01-13: 対 357 +
    // digi 357 = 715 で、こちらの 358 のほぼ 2 倍)。重なり分を「紙だけが数える分」
    // として足す
    for w in stream.windows(2) {
        let (t, nt) = (&w[0], &w[1]);
        if t.st != "始業" || nt.st != "終業" || nt.at <= t.at {
            continue;
        }
        let (d, _) = interval_d_h(t.at, nt.at);
        if d >= 1 {
            continue;
        }
        let pair = (floor_min(t.at), floor_min(nt.at));
        if pair.1 <= pair.0 {
            continue;
        }
        let mut added = 0i64;
        for digi in &digi_spans {
            let (s, x) = (pair.0.max(digi.0), pair.1.min(digi.1));
            if x > s {
                added += (x - s).num_seconds() / 60;
                add_span(&mut out, s, x);
            }
        }
        if added == 0 {
            continue;
        }
        // 紙は対から昼休を引く ([`tc_dc_daily`] と同じ窓) — 二重計上の水増しは
        // その分だけ小さい (実測 1078 / 2026-04-04: 対 646 + digi − 昼休 60)。
        // この成分が負になることはない — 引くのは対に足した分まで
        let noon = t.at.date().and_hms_opt(12, 0, 0).expect("12:00 は常に有効");
        let one = t.at.date().and_hms_opt(13, 0, 0).expect("13:00 は常に有効");
        let ded = if t.at < noon && nt.at > one {
            60
        } else if t.at < noon && nt.at > noon {
            elapsed_min_drop_days(noon, nt.at) % 60
        } else {
            0
        };
        *out
            .entry(nt.at.format("%Y-%m-%d").to_string())
            .or_default() -= ded.min(added);
    }
    // 昼休の控除で 0 に落ちた日は載せない。行データの端 (翌月頭の margin) は
    // 勤務が組めず全部が「外」に見えるので、対象月の日だけ返す
    out.retain(|date, m| *m != 0 && date.starts_with(month));
    out
}

/// **こちらだけが数える時間** (`YYYY-MM-DD → 分`、[`paper_outside_by_date`] の鏡像、
/// Refs #182 フォローアップ)。
///
/// こちらの拘束のうち、**紙の計上材料に覆われていない**部分。紙の材料は
/// デジタコの DIGI イベントと、tc_stream の隣接対 (窓つき) — それに無い時間は
/// 紙のどの type にも積まれない。実データの形は 2 つ:
///
/// - **アイドリングだけの休息明け勤務**: 車中泊のアイドリング (DIGI に無い) しか
///   イベントが無い区間を、こちらは運行の証拠として勤務に数える
///   (実測 1442 / 2026-05-27: 05:52→18:55 の 782 分)
/// - **紙の対の窓から落ちた打刻の対**: d ≥ 1 の対などは紙がどこにも数えない
///
/// 突合 (relay) が cause `ours-outside` の実額に使う (**紙が小さくなる向き** =
/// run-gap などと同じ符号)。運行の継ぎ目・日跨ぎの頭尻尾は個別 cause が実額を
/// 持っているので、二重説明を避けるため覆いに足して除く
/// ([`restraint_spans_and_diagnostics`] の診断区間)。
pub fn ours_outside_by_date(rows: &[serde_json::Value], month: &str) -> BTreeMap<String, i64> {
    let events = parse_events(rows);
    let (restraint, diags) = restraint_spans_and_diagnostics(&events);
    // 紙の計上材料: DIGI イベント + tc_stream の隣接対 (tc_dc_daily と同じ窓)
    let mut material: Vec<(NaiveDateTime, NaiveDateTime)> = diags;
    for e in &events {
        if e.source != "dtako_events" || !DIGI_STATES.contains(&e.state.as_str()) {
            continue;
        }
        let Some(end) = e.end else { continue };
        let span = (floor_min(e.start), floor_min(end));
        if span.1 > span.0 {
            material.push(span);
        }
    }
    let stream = tc_stream(&events, month);
    for w in stream.windows(2) {
        let (t, nt) = (&w[0], &w[1]);
        if nt.at <= t.at {
            continue;
        }
        let (d, h) = interval_d_h(t.at, nt.at);
        let counted = match (t.st.as_str(), nt.st.as_str()) {
            ("始業", "運行開始") => d < 2 && h < 14 && t.at.date() == nt.at.date(),
            ("始業", "終業") => d < 1,
            ("運行終了", "終業") | ("休息開始", "終業") => {
                d < 2 && h < 14 && t.at.date() == nt.at.date()
            }
            ("運行終了", "運行開始") => d < 1 && h < 12,
            _ => false,
        };
        if !counted {
            continue;
        }
        let span = (floor_min(t.at), floor_min(nt.at));
        if span.1 > span.0 {
            material.push(span);
        }
    }
    let material = crate::kosoku::merge_intervals(material);
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    for span in &restraint {
        for (s, x) in subtract_intervals(&[*span], &material) {
            let mut cur = s;
            while cur < x {
                let bound = midnight_after(cur).min(x);
                *out.entry(cur.format("%Y-%m-%d").to_string()).or_default() +=
                    (bound - cur).num_seconds() / 60;
                cur = bound;
            }
        }
    }
    out.retain(|date, _| date.starts_with(month));
    out
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
    fn paper_rounding_mode_zeroes_the_drift_for_a_mixed_day() {
        // 打刻の頭・尻尾 (経過床) とデジタコの連鎖 (端点床) が混ざる日 — 勤務単位の
        // 丸めでは ±1 が cause `rounding` として残った形。既定 (紙丸め、Refs #182)
        // では公式値がそのまま紙と一致して drift が消える
        let rows = vec![
            tc("2026-06-02 08:00:20", "始業"),
            dtako("2026-06-02 08:10:40", "運行開始"),
            ev("2026-06-02 08:10:40", "2026-06-02 09:20:10", "運転"),
            ev("2026-06-02 09:20:10", "2026-06-02 10:30:50", "積み"),
            dtako("2026-06-02 10:30:50", "運行終了"),
            tc("2026-06-02 10:40:30", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        let paper = paper_daily_minutes(&rows, "2026-06");
        assert_eq!(paper.get("2026-06-02"), Some(&159));
        assert!(paper_drift_by_date(&days, &paper).is_empty());
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
        // 実額として出すのも TC_DC が値を持つ日だけ — 引かれていない分を突合の
        // cause 候補に出すと過剰説明になる
        assert!(minus_unko_by_date(&rows, "2026-03").is_empty());
    }

    #[test]
    fn minus_unko_by_date_reports_the_run_head_the_paper_subtracts() {
        // 1729 石坂 2026-01-09 の形。運行開始 → 始業 の 9 分 23 秒を紙は日計から
        // 引く。TC_DC は 運行終了 → 終業 で値を持つので着地する
        let rows = vec![
            dtako("2026-03-04 05:50:04", "運行開始"),
            tc("2026-03-04 05:59:27", "始業"),
            ev("2026-03-04 06:05:45", "2026-03-04 18:43:06", "運転"),
            dtako("2026-03-04 18:43:06", "運行終了"),
            tc("2026-03-04 18:54:02", "終業"),
        ];
        assert_eq!(
            minus_unko_by_date(&rows, "2026-03").get("2026-03-04"),
            Some(&9)
        );
        // 紙の値もその分だけ小さい: デジタコ 758 (06:05 → 18:43 の端点床) +
        // 尻尾 10 (運行終了 → 終業) − 9。実測 1729 / 2026-01-09 の nginx 値と同じ
        assert_eq!(
            paper_daily_minutes(&rows, "2026-03").get("2026-03-04"),
            Some(&759)
        );
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
    fn paper_outside_counts_events_and_gaps_after_the_closing_punch() {
        // 1018 金原 2026-03-03 の形: 終業打刻の後の構内ミニ運行 (Refs #182)。
        // digi 1 分 + 運行の継ぎ目 5 分。最初の 運行終了 → 運行開始 は間に終業打刻が
        // 挟まるので紙も対にしない (数えない)
        let rows = vec![
            tc("2026-03-03 09:23:49", "始業"),
            dtako("2026-03-03 09:39:52", "運行開始"),
            ev("2026-03-03 09:39:52", "2026-03-03 20:06:44", "運転"),
            dtako("2026-03-03 20:06:44", "運行終了"),
            tc("2026-03-03 20:25:27", "終業"),
            dtako("2026-03-03 20:36:57", "運行開始"),
            ev("2026-03-03 20:36:57", "2026-03-03 20:37:15", "運転"),
            dtako("2026-03-03 20:37:15", "運行終了"),
            dtako("2026-03-03 20:42:29", "運行開始"),
            ev("2026-03-03 20:42:29", "2026-03-03 20:42:56", "一般道空車"),
            dtako("2026-03-03 20:42:56", "運行終了"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        assert_eq!(outside.get("2026-03-03"), Some(&6));
        assert_eq!(outside.len(), 1);
    }

    #[test]
    fn paper_outside_splits_an_overnight_event_at_midnight() {
        // 1069 前田 2026-01-05 の形: 終業の後も夜通し続く「積み」(状態の切り忘れ)。
        // 紙は 0 時まで前日に、0 時から翌朝の始業までを翌日に数える (Refs #182)
        let rows = vec![
            tc("2026-03-04 07:31:00", "始業"),
            ev("2026-03-04 16:05:20", "2026-03-05 08:07:09", "積み"),
            tc("2026-03-04 17:17:34", "終業"),
            tc("2026-03-05 07:36:09", "始業"),
            tc("2026-03-05 10:06:03", "終業"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 17:17 → 24:00 の 403 分 + 対の二重計上 (運行行が無いので 始業 → 終業 が
        // 隣接のまま。積みとの重なり 72 − 昼休 60 = 12)。
        // 翌日は 0:00 → 始業 07:36 の 456 分 + 対の二重 31 (07:36 → 08:07)
        assert_eq!(outside.get("2026-03-04"), Some(&415));
        assert_eq!(outside.get("2026-03-05"), Some(&487));
    }

    #[test]
    fn paper_outside_excludes_the_run_head_before_the_punch_in() {
        // 始業前の運行の頭は cause `run-head` の領分 — 外に数えると二重説明になる。
        // 翌月頭の margin 行 (勤務が組めず全部が外に見える) も対象月ではないので返さない
        let rows = vec![
            dtako("2026-03-04 07:09:00", "運行開始"),
            ev("2026-03-04 07:09:00", "2026-03-04 08:00:00", "運転"),
            tc("2026-03-04 07:29:00", "始業"),
            tc("2026-03-04 17:00:00", "終業"),
            ev("2026-04-01 09:00:00", "2026-04-01 10:00:00", "運転"),
        ];
        assert!(paper_outside_by_date(&rows, "2026-03").is_empty());
    }

    #[test]
    fn a_midnight_crossing_gap_pair_lands_wholly_on_the_run_start_day() {
        // 1634 中山 2026-01-04 の形 (Refs #182)。紙は 運行終了 → 運行開始 の対を
        // **丸ごと運行開始の日**に載せる ([`tc_dc_daily`] の「同日の縛りが無い」)。
        // 暦日で割ると説明が前日と当日に散って一致しない
        let rows = vec![
            ev("2026-03-02 10:00:00", "2026-03-03 10:00:00", "休息"),
            dtako("2026-03-03 10:00:00", "運行開始"),
            ev("2026-03-03 10:00:00", "2026-03-03 20:14:00", "運転"),
            dtako("2026-03-03 20:14:00", "運行終了"),
            dtako("2026-03-04 06:00:00", "運行開始"),
            ev("2026-03-04 06:00:00", "2026-03-04 20:00:00", "運転"),
            dtako("2026-03-04 20:00:00", "運行終了"),
            ev("2026-03-04 20:00:00", "2026-03-05 07:00:00", "休息"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 20:14 → 翌 06:00 の 586 分が丸ごと 03-04 へ (03-03 には載らない)
        assert_eq!(outside.get("2026-03-04"), Some(&586));
        assert!(!outside.contains_key("2026-03-03"));
    }

    #[test]
    fn paper_outside_skips_sub_minute_spans_and_wide_gaps() {
        // 端点床で 0 分になる区間・対と、窓 (d < 1 && h < 12) の外の継ぎ目は数えない
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            tc("2026-03-04 12:00:00", "終業"),
            ev("2026-03-04 20:36:10", "2026-03-04 20:36:50", "運転"),
            dtako("2026-03-04 20:40:10", "運行終了"),
            dtako("2026-03-04 20:40:50", "運行開始"),
            dtako("2026-03-05 06:00:00", "運行終了"),
            dtako("2026-03-05 19:00:00", "運行開始"),
        ];
        assert!(paper_outside_by_date(&rows, "2026-03").is_empty());
    }

    #[test]
    fn paper_outside_counts_a_long_gap_between_split_shifts() {
        // 1674 前田 2026-05-13 の形 (Refs #182)。8 時間以上の継ぎ目で勤務が割れると
        // その空きはこちらの拘束に無いが、紙の対の窓は h < 12 なので数える。
        // 覆いを**分割後の勤務**で取らないとここが漏れる
        let rows = vec![
            ev("2026-03-03 20:00:00", "2026-03-04 05:00:00", "休息"),
            dtako("2026-03-04 05:00:00", "運行開始"),
            ev("2026-03-04 05:00:00", "2026-03-04 08:28:00", "運転"),
            dtako("2026-03-04 08:28:00", "運行終了"),
            dtako("2026-03-04 19:56:00", "運行開始"),
            ev("2026-03-04 19:56:00", "2026-03-05 06:00:00", "運転"),
            dtako("2026-03-05 06:00:00", "運行終了"),
            ev("2026-03-05 06:00:00", "2026-03-05 20:00:00", "休息"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 08:28 → 19:56 の 688 分 (勤務は 25 時間 → 継ぎ目で割れて空きが外に出る)
        assert_eq!(outside.get("2026-03-04"), Some(&688));
    }

    #[test]
    fn paper_outside_counts_the_punch_pair_double_when_run_rows_are_missing() {
        // 1108 福留 2026-01-13 の形 (Refs #182)。time_card_dtako の運行開始行が
        // 無いと 始業 → 終業 が隣接したままになり、紙は対とデジタコを両方数える
        let rows = vec![
            tc("2026-03-04 04:40:37", "始業"),
            ev("2026-03-04 04:40:37", "2026-03-04 10:38:03", "運転"),
            tc("2026-03-04 10:38:03", "終業"),
            dtako("2026-03-04 10:38:03", "運行終了"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 対 (04:40〜10:38) とデジタコの重なり 358 分 (昼の窓には掛からない)。
        // 同時刻の運行終了は 終業 の後に並ぶので隣接を壊さない (実データと同じ)
        assert_eq!(outside.get("2026-03-04"), Some(&358));
        // 拘束は対とデジタコに覆われている — こちらだけの時間は無い
        assert!(ours_outside_by_date(&rows, "2026-03").is_empty());
    }

    #[test]
    fn the_pair_double_subtracts_the_paper_lunch_deduction() {
        // 1078 立山 2026-04-04 の形: 対が昼の窓を跨ぐと紙は 60 分引くので、
        // 二重計上の水増しもその分だけ小さい
        let rows = vec![
            tc("2026-03-04 09:08:00", "始業"),
            ev("2026-03-04 09:08:00", "2026-03-04 19:54:00", "運転"),
            tc("2026-03-04 19:54:00", "終業"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 対 646 − 昼休 60
        assert_eq!(outside.get("2026-03-04"), Some(&586));
    }

    #[test]
    fn the_pair_double_deducts_only_the_window_overlap_when_it_ends_inside() {
        // 終業が窓の中に落ちる対は重なり分だけ引かれる (紙の `->i` の再現)
        let rows = vec![
            tc("2026-03-04 09:00:00", "始業"),
            ev("2026-03-04 09:00:00", "2026-03-04 12:20:00", "運転"),
            tc("2026-03-04 12:20:00", "終業"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 対 200 − 窓の重なり 20
        assert_eq!(outside.get("2026-03-04"), Some(&180));
    }

    #[test]
    fn the_pair_double_skips_pairs_without_digitaco_and_full_day_pairs() {
        // digi の無い対 (added 0) と 24 時間以上離れた対 (d >= 1) は二重にならない
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            tc("2026-03-04 12:00:00", "終業"),
            tc("2026-03-06 08:00:00", "始業"),
            ev("2026-03-06 08:00:00", "2026-03-06 09:00:00", "運転"),
            tc("2026-03-07 09:00:00", "終業"),
        ];
        assert!(paper_outside_by_date(&rows, "2026-03").is_empty());
    }

    #[test]
    fn paper_outside_counts_overlapping_digitaco_events_twice_like_the_paper() {
        // 1740 佐藤 2026-05-09 の形 (Refs #182): 運行 NO が並走すると DIGI イベントが
        // 重なる。紙はイベントごとに足すので重なりが二重に積まれる — 勤務の中の
        // 重なり分を「紙だけが数える分」として足す
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            dtako("2026-03-04 08:00:00", "運行開始"),
            ev("2026-03-04 08:00:00", "2026-03-04 12:00:00", "運転"),
            ev("2026-03-04 11:00:00", "2026-03-04 13:00:00", "運転"),
            dtako("2026-03-04 13:00:00", "運行終了"),
            tc("2026-03-04 13:00:00", "終業"),
        ];
        let outside = paper_outside_by_date(&rows, "2026-03");
        // 11:00 → 12:00 の重なり 60 分だけ (運行行があるので対の二重は無い)
        assert_eq!(outside.get("2026-03-04"), Some(&60));
    }

    #[test]
    fn ours_outside_counts_the_span_paper_has_no_material_for() {
        // 1442 廣々 2026-05-27 の形 (Refs #182)。休息明けから次の始業までの勤務に
        // アイドリング (DIGI に無い) しかイベントが無い区間 — こちらは運行の証拠と
        // して拘束に数えるが、紙にはどの type の材料も無い
        let rows = vec![
            ev("2026-03-03 20:00:00", "2026-03-04 06:00:00", "休息"),
            ev("2026-03-04 06:00:00", "2026-03-04 16:30:00", "アイドリング"),
            dtako("2026-03-04 17:00:00", "運行開始"),
            ev("2026-03-04 17:00:00", "2026-03-04 17:30:00", "運転"),
            tc("2026-03-04 18:00:00", "始業"),
            tc("2026-03-04 23:00:00", "終業"),
        ];
        let ours = ours_outside_by_date(&rows, "2026-03");
        // 休息明け 06:00 → 運行開始 17:00 の 660 分 (運転 17:00〜17:30 は紙の材料、
        // 17:30 → 18:00 は 始業 との対にならず紙に無いが勤務にもならない…
        // 勤務は 06:00 → 18:00 で、材料は 運転 30 分だけ)
        assert_eq!(ours.get("2026-03-04"), Some(&690));
    }

    #[test]
    fn ours_outside_is_empty_when_paper_has_material_for_everything() {
        // 普通の運行日: 打刻の対とデジタコが勤務を覆う。勤務の中の継ぎ目
        // (運行終了 → 運行開始、覆いの中) も紙の材料なので外に出ない
        let rows = vec![
            tc("2026-03-04 08:00:00", "始業"),
            dtako("2026-03-04 08:10:00", "運行開始"),
            ev("2026-03-04 08:10:00", "2026-03-04 12:00:00", "運転"),
            dtako("2026-03-04 12:00:00", "運行終了"),
            dtako("2026-03-04 12:30:00", "運行開始"),
            ev("2026-03-04 12:30:00", "2026-03-04 16:00:00", "運転"),
            dtako("2026-03-04 16:00:00", "運行終了"),
            tc("2026-03-04 16:30:00", "終業"),
        ];
        assert!(ours_outside_by_date(&rows, "2026-03").is_empty());
        // 継ぎ目は覆いの中 — 対の外の write は起きない (gap_outside 0 の分岐)
        assert!(paper_outside_by_date(&rows, "2026-03").is_empty());
    }

    #[test]
    fn degenerate_pairs_and_swallowed_shifts_add_nothing() {
        // 同時刻打刻 (分床で 0 の対) と、休息が勤務を丸ごと覆う形 (拘束は勤務の
        // まま残す) の両方で、外の写像は静かに空になる
        let rows = vec![
            tc("2026-03-04 08:00:10", "始業"),
            ev("2026-03-04 08:00:00", "2026-03-04 17:00:00", "休息"),
            ev("2026-03-04 08:00:20", "2026-03-04 08:00:40", "運転"),
            tc("2026-03-04 08:00:30", "終業"),
        ];
        assert!(paper_outside_by_date(&rows, "2026-03").is_empty());
        assert!(ours_outside_by_date(&rows, "2026-03").is_empty());
        // 休息が勤務を丸ごと覆う形 — 拘束は勤務のまま残す ([`summarize`] と同じ)
        let rows = vec![
            tc("2026-03-05 08:00:00", "始業"),
            ev("2026-03-05 08:00:00", "2026-03-05 17:00:00", "休息"),
            tc("2026-03-05 17:00:00", "終業"),
        ];
        assert!(ours_outside_by_date(&rows, "2026-03").is_empty());
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
