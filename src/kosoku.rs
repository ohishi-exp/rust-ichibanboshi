//! 拘束時間の日別サマリ — 純粋ロジック (拘束時間の打刻基準化 Phase 2、Refs #118)。
//!
//! `/api/kintai/events` が返す**解釈しない生イベント列**を受け取り、日別サマリへ畳む。
//! **DB も HTTP も触らない** — 入力は `Vec<serde_json::Value>`、出力は `Vec<DaySummary>`
//! だけなので、実データを使わずにテストできる。
//!
//! ## 規則 (#118 で確定、2026-07-27)
//!
//! ### 就業時間 (始業・終業) の決め方
//!
//! 1. **打刻があればタイムカードを使う** — 実測で打刻は運行を完全に包んでいた
//!    (乗務員 1018 / 2026-06 の全 23 日で、始業打刻 → 運行開始が中央 +7 分、
//!    運行終了 → 終業打刻が中央 +3 分、逆転ゼロ)
//! 2. **打刻が無ければ休息イベント** — 休息の終了 = 始業、休息の開始 = 終業
//!
//! **運行の開始・終了では切らない。同日の運行の継ぎ目は「作業」**であって休憩ではない
//! (拘束にも実働にも残す)。ただし継ぎ目に終業打刻があればそこで勤務が切れ、次の始業まで
//! は休息になる — 打刻優先の規則がそのまま効く。実測 (96 名 / 2026-06) では同日の継ぎ目
//! 95 箇所の内訳が「終業打刻あり 27 / 何もない 68 / 休息イベントあり 0」で、この 2 通りで
//! 全件を処理できる。 実測では運行の継ぎ目が 4〜112 分 (中央 8 分)
//! しかなく、勤務の切れ目ではない (荷を降ろして次の伝票を積んで出るだけ)。運行で
//! 切ると乗務員 1119 の 2026-06 は 28 勤務になるが、切らないと 1 か月が 1 勤務
//! (拘束 767 時間) になる。休息で切ると 26 勤務・拘束 16,394 分で、現行の
//! 出勤 26 日・16,278 分とほぼ一致した。
//!
//! 打刻と休息は補完関係にある (長距離で日跨ぎの乗務員は打刻の機会が無い代わりに
//! 休息が記録され、日帰りの乗務員は打刻が揃う代わりに休息が発生しない)。
//!
//! ### 時間区分
//!
//! | 区分 | 範囲 | 割増 |
//! |---|---|---|
//! | 所定内 | 実働 0〜7.5h | 1.00 |
//! | 法定内残業 | 7.5〜8h | 1.00 |
//! | 法定時間外 | 8h 超 | 1.25 |
//! | 法定休日労働 | 日曜の実働すべて | 1.35 |
//!
//! **法定休日 = 日曜日。** 祝日は割増に使わない (所定休日であり固有の割増を持たない)。
//! 賃金側の実測でも法定休日は日曜の労働時間とちょうど一致していた (685 乗務員月で
//! 例外ゼロ)。祝日はタイムカード表示用に別途 API 化する (#119)。
//!
//! ### 深夜 (22:00〜05:00)
//!
//! **所定内/時間外 × 平日/法定休日**の直積で持つ。法定外休日は持たない — 固有の
//! 割増が無い (通常労働 + 週 40 時間超で 1.25) ため構造的に不要。
//!
//! - [`DaySummary::night_minutes`] … 平日の所定内・法定内残業に重なる深夜。どちらも
//!   基礎が 1.00 なので深夜の 0.25 を上乗せするだけで足り、区別する必要がない
//! - [`DaySummary::overtime_night_minutes`] … 平日の法定時間外に重なる深夜 (1.25+0.25)
//! - [`DaySummary::legal_holiday_night_minutes`] … 法定休日に重なる深夜 (1.35+0.25)
//!
//! **深夜と時間外深夜は排他。** 同じ 1 分が両方に入ることはない。
//!
//! ### 休憩
//!
//! 閾値 (既定 10 分) 以上の `休憩` イベントだけを数える。**拘束からは外さない** —
//! 拘束 = 終業 − 始業、実働 = 拘束 − 休憩。
//!
//! ### 24 時間を超える拘束
//!
//! たまに日付を跨いで 24 時間以上続く運行がある (実測では最長 38.1 時間)。**例外的な
//! 外れ値**であり、24 時間超の拘束は改善基準告示に照らして明確な違反なので、正確に
//! 積み上げる意味がない。**24 時間で打ち切り** [`DaySummary::over_24h`] を立てて
//! 遵守チェックに回す。
//!
//! ## 月境界
//!
//! **勤務は始業日で当月に振り分ける。** 月初の勤務の始業は、前月末に始まって月初に
//! 終わる休息の**終わり**で決まる。読み出し側 ([`crate::kintai_repo`] の `EVENTS_SQL`) は
//! 「期間内に始まる区間」に加えて「期間内に終わる区間」も拾うので、この休息は範囲に入る。
//! 拾い漏らすと毎月 1 日目の勤務が静かに欠ける。

use std::collections::BTreeMap;

use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Weekday};
use serde::Serialize;

/// 生イベントの日時書式 (`kintai_repo` が `DATE_FORMAT` で文字列化したもの)。
const FMT: &str = "%Y-%m-%d %H:%M:%S";

/// 深夜の開始時 (この時以降)。
const NIGHT_FROM_HOUR: u32 = 22;
/// 深夜の終了時 (この時未満)。
const NIGHT_TO_HOUR: u32 = 5;

/// 1 勤務の拘束の上限 (分)。これを超えたら**打ち切る**。
///
/// たまに日付を跨いで 24 時間以上続く運行がある (実測では最長 2288 分 = 38.1 時間、
/// 2026-04 の乗務員 1442)。**例外的な外れ値**であり、24 時間を超える拘束は
/// 改善基準告示に照らして明確な違反なので、正確に積み上げる意味がない。
/// 24 時間で切って [`DaySummary::over_24h`] を立て、遵守チェックに回す。
const MAX_RESTRAINT_MINUTES: i64 = 24 * 60;

/// 日別サマリの計算パラメータ。
#[derive(Debug, Clone, Copy)]
pub struct KosokuParams {
    /// 休憩として数える最小の長さ (分)。これ未満の `休憩` イベントは無視する。
    pub break_threshold_minutes: i64,
    /// 所定労働時間 (分)。既定 450 = 7.5 時間。
    pub prescribed_minutes: i64,
    /// 法定労働時間 (分)。既定 480 = 8 時間。`prescribed_minutes` との差が法定内残業。
    pub legal_minutes: i64,
}

impl Default for KosokuParams {
    fn default() -> Self {
        Self {
            break_threshold_minutes: 10,
            prescribed_minutes: 450,
            legal_minutes: 480,
        }
    }
}

/// 生行から必要な列だけ取り出したイベント。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub start: NaiveDateTime,
    /// 区間イベント (`dtako_events` 由来) だけ持つ。
    pub end: Option<NaiveDateTime>,
    pub source: String,
    pub state: String,
}

/// 勤務 1 回。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shift {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    /// 始業・終業をどちらの規則で決めたか。
    pub source: ShiftSource,
}

/// 勤務の境界をどこから取ったか。応答に載せて、後から差分の理由を追えるようにする。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ShiftSource {
    /// 打刻 (`始業` / `終業`)
    Timecard,
    /// 休息イベント (休息の終了 → 次の休息の開始)
    Rest,
}

/// 勤務を構成した打刻 1 つ (Refs #128)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Punch {
    /// 打刻時刻 (`YYYY-MM-DD HH:MM:SS`)。**秒を落とさない** — 打刻カードの元の値。
    pub at: String,
    /// `始業` / `終業`。
    pub state: String,
}

/// 日別サマリ 1 行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaySummary {
    /// 始業日 (`YYYY-MM-DD`)。日跨ぎ勤務もここに寄せる。
    pub date: String,
    pub start: String,
    pub end: String,
    pub source: ShiftSource,
    /// この勤務の中にあった**打刻そのもの** (時刻順、Refs #128)。
    ///
    /// `start` / `end` は勤務としての解釈 (分に丸め、24 時間で打ち切り) が入っているが、
    /// **こちらは生の打刻**。社内タイムカード表は打刻を日ごとに並べただけのもので、
    /// 勤務という単位を持たないため、同じ表を作るには元の時刻が要る。
    ///
    /// - **打ち切り前の区間から拾う** — 24 時間で切ると、切った先にある終業打刻が
    ///   落ちる (実測: 乗務員 1194 の 2026-04-01 始業 → 2026-04-03 16:47 終業)
    /// - 休息イベント由来の勤務 (`source: rest`) は空
    pub punches: Vec<Punch>,
    /// 始業日が日曜か。
    pub is_legal_holiday: bool,
    /// 拘束が 24 時間を超えたため打ち切ったか。**改善基準告示に照らして違反**であり、
    /// 遵守チェックで拾うべき行。`true` のとき `restraint_minutes` は 1440 で頭打ち。
    pub over_24h: bool,

    /// 拘束 = 終業 − 始業。
    pub restraint_minutes: i64,
    /// 閾値以上の休憩の合計。
    pub break_minutes: i64,
    /// 実働 = 拘束 − 休憩。
    pub working_minutes: i64,

    /// 所定内 (0〜所定)。法定休日は 0。
    pub statutory_minutes: i64,
    /// 法定内残業 (所定〜法定、割増 1.0)。法定休日は 0。
    pub within_statutory_overtime_minutes: i64,
    /// 法定時間外 (法定超、割増 1.25)。法定休日は 0。
    pub overtime_minutes: i64,
    /// 法定休日労働 (割増 1.35)。平日は 0。
    pub legal_holiday_minutes: i64,

    /// 平日の所定内・法定内残業に重なる深夜。
    pub night_minutes: i64,
    /// 平日の法定時間外に重なる深夜。
    pub overtime_night_minutes: i64,
    /// 法定休日に重なる深夜。
    pub legal_holiday_night_minutes: i64,
}

/// 生行 (`serde_json::Value`) をイベントへ。**壊れた行は黙って捨てる** —
/// 上流が列を足しても落ちないようにするため、必要な列が揃わない行だけ落とす。
pub fn parse_events(rows: &[serde_json::Value]) -> Vec<Event> {
    let mut out = Vec::new();
    for row in rows {
        let (Some(dt), Some(source), Some(state)) = (
            row.get("datetime").and_then(|v| v.as_str()),
            row.get("source").and_then(|v| v.as_str()),
            row.get("state").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let Ok(start) = NaiveDateTime::parse_from_str(dt, FMT) else {
            continue;
        };
        let end = row
            .get("end_datetime")
            .and_then(|v| v.as_str())
            .and_then(|s| NaiveDateTime::parse_from_str(s, FMT).ok());
        out.push(Event {
            start,
            end,
            source: source.to_string(),
            state: state.to_string(),
        });
    }
    out.sort_by_key(|e| e.start);
    out
}

/// 秒を切り捨てて分に丸める。拘束・実働は分単位で出すので、境界も分に揃える。
fn floor_min(dt: NaiveDateTime) -> NaiveDateTime {
    // 秒とナノ秒を 0 にするだけなので、範囲外にはならない
    dt - Duration::seconds(dt.second() as i64) - Duration::nanoseconds(dt.nanosecond() as i64)
}

/// 深夜帯 (22:00〜05:00) か。
fn is_night(t: NaiveDateTime) -> bool {
    let h = t.hour();
    !(NIGHT_TO_HOUR..NIGHT_FROM_HOUR).contains(&h)
}

/// 打刻から勤務を組む。`始業` の後に来る最初の `終業` と対にする。
///
/// 終業が見つからない始業 (月末で切れた等) は捨てる — 終業が無ければ拘束が出せない。
fn shifts_from_timecard(events: &[Event]) -> Vec<Shift> {
    let mut out = Vec::new();
    let mut pending: Option<NaiveDateTime> = None;
    for e in events.iter().filter(|e| e.source == "timecard") {
        match e.state.as_str() {
            "始業" => pending = Some(e.start),
            "終業" => {
                if let Some(start) = pending.take() {
                    if e.start > start {
                        out.push(Shift {
                            start,
                            end: e.start,
                            source: ShiftSource::Timecard,
                        });
                    }
                }
            }
            _ => {}
        }
    }
    out
}

/// 休息イベントから勤務を組む。**休息の終了 = 始業、次の休息の開始 = 終業。**
///
/// 区間を持つ `dtako_events` 由来の `休息` だけを使う (`dtako` 側は開始・終了が
/// 別行の点イベントで、対にする根拠が無い)。
fn shifts_from_rest(events: &[Event]) -> Vec<Shift> {
    let mut rests: Vec<(NaiveDateTime, NaiveDateTime)> = events
        .iter()
        .filter(|e| e.state == "休息" && e.source == "dtako_events")
        .filter_map(|e| e.end.map(|end| (e.start, end)))
        .filter(|(s, e)| e > s)
        .collect();
    rests.sort();
    rests
        .windows(2)
        .filter(|w| w[1].0 > w[0].1)
        .map(|w| Shift {
            start: w[0].1,
            end: w[1].0,
            source: ShiftSource::Rest,
        })
        .collect()
}

/// 区間 `[from, to]` に入る打刻 (`始業` / `終業`) を時刻順に拾う (Refs #128)。
///
/// **両端を含む** — 勤務の始業・終業そのものを落とさないため。比較は**分に丸めてから**
/// 行う: 勤務の端は秒を切り捨ててあるので、`16:47:04` の終業打刻を `16:47:00` の
/// 終業と比べると範囲外になって落ちる。
fn punches_in(events: &[Event], from: NaiveDateTime, to: NaiveDateTime) -> Vec<Punch> {
    events
        .iter()
        .filter(|e| e.source == "timecard" && (e.state == "始業" || e.state == "終業"))
        .filter(|e| floor_min(e.start) >= from && floor_min(e.start) <= to)
        .map(|e| Punch {
            at: e.start.format(FMT).to_string(),
            state: e.state.clone(),
        })
        .collect()
}

/// 2 つの区間が重なるか。
fn overlaps(a: (NaiveDateTime, NaiveDateTime), b: (NaiveDateTime, NaiveDateTime)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// 打刻由来と休息由来を統合する。**打刻を優先し、打刻の勤務と重なる休息由来は捨てる。**
///
/// 打刻と休息は実測では補完関係にあり重ならないが、両方ある乗務員が現れても
/// 二重計上しないようにしておく。
fn merge_shifts(timecard: Vec<Shift>, rest: Vec<Shift>) -> Vec<Shift> {
    let mut out = timecard;
    for r in rest {
        if !out
            .iter()
            .any(|t| overlaps((t.start, t.end), (r.start, r.end)))
        {
            out.push(r);
        }
    }
    out.sort_by_key(|e| e.start);
    out
}

/// 勤務に重なる休憩区間を、閾値でふるって勤務内へ切り詰めて返す。
///
/// 閾値の判定は**切り詰める前の長さ**で行う — 勤務の端に半分だけ掛かった 30 分の
/// 休憩を「10 分未満だから無視」にしないため。
fn breaks_in(
    events: &[Event],
    shift: &Shift,
    threshold: i64,
) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let mut out: Vec<(NaiveDateTime, NaiveDateTime)> = events
        .iter()
        .filter(|e| e.state == "休憩")
        .filter_map(|e| e.end.map(|end| (floor_min(e.start), floor_min(end))))
        .filter(|(s, e)| (*e - *s).num_minutes() >= threshold)
        .map(|(s, e)| (s.max(shift.start), e.min(shift.end)))
        .filter(|(s, e)| e > s)
        .collect();
    out.sort();
    // 重なった休憩をまとめる (重複して引かないため)
    let mut merged: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    for (s, e) in out {
        match merged.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    merged
}

/// 拘束区間から休憩を差し引いて、実働区間の列を返す。
fn working_intervals(
    shift: &Shift,
    breaks: &[(NaiveDateTime, NaiveDateTime)],
) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let mut out = Vec::new();
    let mut cur = shift.start;
    for (s, e) in breaks {
        if *s > cur {
            out.push((cur, *s));
        }
        cur = cur.max(*e);
    }
    if shift.end > cur {
        out.push((cur, shift.end));
    }
    out
}

/// 勤務 1 回を日別サマリへ畳む。
///
/// 実働区間を**時刻順に 1 分ずつ**歩いて、経過実働で所定内 / 法定内残業 / 法定時間外に
/// 振り分けながら、その分が深夜帯かを見る。1 勤務は最長でも数千分なので素直に回す。
fn summarize(shift: &Shift, events: &[Event], p: &KosokuParams) -> DaySummary {
    // **打刻は打ち切り前の区間から拾う** — 24 時間で切ると、切った先にある終業打刻
    // (実測: 乗務員 1194 の 2026-04-03 16:47) が落ちる。拘束の値は打ち切ったままでよいが、
    // 打刻カードとして出す側にはその時刻が要る (Refs #128)
    let punches = punches_in(events, shift.start, shift.end);
    // 24 時間を超える拘束はここで打ち切る (法令違反なので積み上げる意味がない)
    let over_24h = (shift.end - shift.start).num_minutes() > MAX_RESTRAINT_MINUTES;
    let shift = &Shift {
        start: shift.start,
        end: if over_24h {
            shift.start + Duration::minutes(MAX_RESTRAINT_MINUTES)
        } else {
            shift.end
        },
        source: shift.source,
    };
    let breaks = breaks_in(events, shift, p.break_threshold_minutes);
    let break_minutes: i64 = breaks.iter().map(|(s, e)| (*e - *s).num_minutes()).sum();
    let intervals = working_intervals(shift, &breaks);
    let is_legal_holiday = shift.start.weekday() == Weekday::Sun;

    let mut elapsed = 0i64;
    let (mut statutory, mut within, mut overtime, mut holiday) = (0i64, 0i64, 0i64, 0i64);
    let (mut night, mut ot_night, mut hol_night) = (0i64, 0i64, 0i64);

    for (s, e) in &intervals {
        let mut t = *s;
        while t < *e {
            let n = is_night(t);
            if is_legal_holiday {
                holiday += 1;
                if n {
                    hol_night += 1;
                }
            } else if elapsed < p.prescribed_minutes {
                statutory += 1;
                if n {
                    night += 1;
                }
            } else if elapsed < p.legal_minutes {
                within += 1;
                // 法定内残業は割増 1.0 なので、深夜は所定内と同じ 0.25 上乗せで足りる
                if n {
                    night += 1;
                }
            } else {
                overtime += 1;
                if n {
                    ot_night += 1;
                }
            }
            elapsed += 1;
            t += Duration::minutes(1);
        }
    }

    DaySummary {
        date: shift.start.format("%Y-%m-%d").to_string(),
        start: shift.start.format(FMT).to_string(),
        end: shift.end.format(FMT).to_string(),
        source: shift.source,
        punches,
        is_legal_holiday,
        over_24h,
        restraint_minutes: (shift.end - shift.start).num_minutes(),
        break_minutes,
        working_minutes: elapsed,
        statutory_minutes: statutory,
        within_statutory_overtime_minutes: within,
        overtime_minutes: overtime,
        legal_holiday_minutes: holiday,
        night_minutes: night,
        overtime_night_minutes: ot_night,
        legal_holiday_night_minutes: hol_night,
    }
}

/// 全乗務員ぶんの生イベント列を**乗務員ごとに分ける** (Refs #125)。
///
/// [`daily_summary`] は乗務員を知らない — 1 人ぶんのイベント列を前提に休息で勤務を
/// 切るので、混ざったまま渡すと他人の休息で勤務が切れる。**畳む前にここで分ける。**
///
/// - 乗務員CD 昇順で返す (行の並びは入力のまま維持する)
/// - **`driver_id` が無い / 数でない / 負の行は捨てる。** 誰のものか決められない行を
///   どこかの乗務員に混ぜると、その人の拘束が静かに伸びる
pub fn split_by_driver(rows: Vec<serde_json::Value>) -> Vec<(u64, Vec<serde_json::Value>)> {
    let mut by_driver: BTreeMap<u64, Vec<serde_json::Value>> = BTreeMap::new();
    for row in rows {
        let Some(driver) = row.get("driver_id").and_then(|v| v.as_u64()) else {
            continue;
        };
        by_driver.entry(driver).or_default().push(row);
    }
    by_driver.into_iter().collect()
}

/// 生イベント列 → 対象月の日別サマリ。
///
/// `month` (`YYYY-MM`) は**始業日**で絞る。前後にはみ出した勤務は落ちる/入る:
/// 前月に始業した勤務は当月に出さず、当月末に始業して翌月に終業する勤務は
/// 拘束を丸ごと当月に載せる。
pub fn daily_summary(rows: &[serde_json::Value], month: &str, p: &KosokuParams) -> Vec<DaySummary> {
    let events = parse_events(rows);
    let shifts = merge_shifts(shifts_from_timecard(&events), shifts_from_rest(&events));
    shifts
        .iter()
        .map(|s| Shift {
            start: floor_min(s.start),
            end: floor_min(s.end),
            source: s.source,
        })
        .filter(|s| s.end > s.start)
        .map(|s| summarize(&s, &events, p))
        .filter(|d| d.date.starts_with(month))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, FMT).unwrap()
    }

    fn tc(datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "end_datetime": null, "source": "timecard", "state": state})
    }

    fn dtako(datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "end_datetime": null, "source": "dtako", "state": state})
    }

    fn ev(start: &str, end: &str, state: &str) -> serde_json::Value {
        json!({"datetime": start, "end_datetime": end, "source": "dtako_events", "state": state})
    }

    #[test]
    fn parse_skips_broken_rows() {
        let rows = vec![
            tc("2026-06-02 09:25:00", "始業"),
            json!({"datetime": "not a date", "source": "timecard", "state": "始業"}),
            json!({"datetime": null, "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 09:25:00", "source": null, "state": "始業"}),
            json!({"datetime": "2026-06-02 09:25:00", "source": "timecard", "state": null}),
            // end_datetime が壊れていても行自体は生きる (end だけ None)
            ev("2026-06-02 12:00:00", "bogus", "休憩"),
        ];
        let out = parse_events(&rows);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].state, "始業");
        assert!(out[1].end.is_none());
    }

    #[test]
    fn parse_sorts_by_datetime() {
        let rows = vec![
            tc("2026-06-02 19:39:00", "終業"),
            tc("2026-06-02 09:25:00", "始業"),
        ];
        let out = parse_events(&rows);
        assert_eq!(out[0].state, "始業");
    }

    #[test]
    fn floor_min_drops_seconds() {
        assert_eq!(
            floor_min(dt("2026-06-02 09:25:41")),
            dt("2026-06-02 09:25:00")
        );
    }

    #[test]
    fn night_window_is_22_to_5() {
        assert!(is_night(dt("2026-06-02 22:00:00")));
        assert!(is_night(dt("2026-06-02 23:59:00")));
        assert!(is_night(dt("2026-06-02 04:59:00")));
        assert!(!is_night(dt("2026-06-02 05:00:00")));
        assert!(!is_night(dt("2026-06-02 21:59:00")));
    }

    // --- 打刻のみ (1018 型: 日帰り、休息イベントなし) ---

    #[test]
    fn timecard_only_day_shift() {
        // 2026-06-02 は火曜。09:25〜19:39 = 614 分、休憩 96 分 → 実働 518 分
        let rows = vec![
            tc("2026-06-02 09:25:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:36:00", "休憩"),
            tc("2026-06-02 19:39:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        let d = &d[0];
        assert_eq!(d.source, ShiftSource::Timecard);
        assert!(!d.is_legal_holiday);
        assert_eq!(d.restraint_minutes, 614);
        assert_eq!(d.break_minutes, 96);
        assert_eq!(d.working_minutes, 518);
        // 450 所定内 + 30 法定内残業 + 38 法定時間外
        assert_eq!(d.statutory_minutes, 450);
        assert_eq!(d.within_statutory_overtime_minutes, 30);
        assert_eq!(d.overtime_minutes, 38);
        assert_eq!(d.legal_holiday_minutes, 0);
        // 昼勤なので深夜は無い
        assert_eq!(d.night_minutes, 0);
        assert_eq!(d.overtime_night_minutes, 0);
    }

    #[test]
    fn working_equals_statutory_plus_overtime() {
        let rows = vec![
            tc("2026-06-02 09:25:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:36:00", "休憩"),
            tc("2026-06-02 19:39:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(
            d.working_minutes,
            d.statutory_minutes
                + d.within_statutory_overtime_minutes
                + d.overtime_minutes
                + d.legal_holiday_minutes
        );
        assert_eq!(d.restraint_minutes, d.working_minutes + d.break_minutes);
    }

    #[test]
    fn short_shift_has_no_overtime() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"), // 360 分
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.statutory_minutes, 360);
        assert_eq!(d.within_statutory_overtime_minutes, 0);
        assert_eq!(d.overtime_minutes, 0);
    }

    #[test]
    fn timecard_without_close_is_dropped() {
        let rows = vec![tc("2026-06-02 09:25:00", "始業")];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn timecard_close_before_open_is_dropped() {
        let rows = vec![
            tc("2026-06-02 19:39:00", "始業"),
            // 始業より前の終業 (壊れた打刻) — parse で並べ替わるので終業が先に来る
            tc("2026-06-02 09:25:00", "終業"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn timecard_ignores_other_states() {
        let rows = vec![
            json!({"datetime": "2026-06-02 08:00:00", "end_datetime": null,
                   "source": "timecard", "state": "その他"}),
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ];
        assert_eq!(
            daily_summary(&rows, "2026-06", &KosokuParams::default()).len(),
            1
        );
    }

    // --- 休息のみ (1119 型: 長距離、打刻なし) ---

    #[test]
    fn rest_only_shift_spans_midnight() {
        // 休息 6/1 16:19→6/2 04:42、次の休息 6/2 16:18→6/3 06:01
        // → 勤務は 6/2 04:42〜16:18 = 696 分 (実測値と同じ)
        let rows = vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].source, ShiftSource::Rest);
        assert_eq!(d[0].start, "2026-06-02 04:42:00");
        assert_eq!(d[0].end, "2026-06-02 16:18:00");
        assert_eq!(d[0].restraint_minutes, 696);
        // 04:42〜05:00 の 18 分が深夜
        assert_eq!(d[0].night_minutes, 18);
    }

    #[test]
    fn rest_needs_two_events_to_make_a_shift() {
        let rows = vec![ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息")];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn rest_without_end_is_ignored() {
        let rows = vec![
            json!({"datetime": "2026-06-01 16:19:00", "end_datetime": null,
                   "source": "dtako_events", "state": "休息"}),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn point_rest_from_dtako_is_ignored() {
        // dtako 側の休息は点イベント — 対にする根拠が無いので使わない
        let rows = vec![
            json!({"datetime": "2026-06-01 16:19:00", "end_datetime": "2026-06-02 04:42:00",
                   "source": "dtako", "state": "休息"}),
            json!({"datetime": "2026-06-02 16:18:00", "end_datetime": "2026-06-03 06:01:00",
                   "source": "dtako", "state": "休息"}),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn zero_length_rest_is_ignored() {
        let rows = vec![
            ev("2026-06-01 16:19:00", "2026-06-01 16:19:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn overlapping_rests_make_no_shift() {
        // 次の休息の開始が前の休息の終了より前 = 勤務時間が無い
        let rows = vec![
            ev("2026-06-01 16:00:00", "2026-06-02 10:00:00", "休息"),
            ev("2026-06-02 08:00:00", "2026-06-02 20:00:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    // --- 打刻優先 ---

    #[test]
    fn timecard_wins_over_overlapping_rest() {
        let rows = vec![
            ev("2026-06-01 20:00:00", "2026-06-02 08:00:00", "休息"),
            tc("2026-06-02 09:25:00", "始業"),
            tc("2026-06-02 19:39:00", "終業"),
            ev("2026-06-02 22:00:00", "2026-06-03 08:00:00", "休息"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        // 休息由来 (08:00〜22:00) は打刻由来 (09:25〜19:39) と重なるので捨てる
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].source, ShiftSource::Timecard);
        assert_eq!(d[0].start, "2026-06-02 09:25:00");
    }

    #[test]
    fn rest_shift_kept_when_not_overlapping_timecard() {
        // 6/2 は打刻、6/5〜6/6 は休息だけ — 両方残る
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
            ev("2026-06-04 20:00:00", "2026-06-05 06:00:00", "休息"),
            ev("2026-06-05 18:00:00", "2026-06-06 06:00:00", "休息"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].source, ShiftSource::Timecard);
        assert_eq!(d[1].source, ShiftSource::Rest);
        assert_eq!(d[1].date, "2026-06-05");
    }

    // --- 法定休日 (日曜) ---

    #[test]
    fn sunday_is_legal_holiday_and_has_no_overtime_split() {
        // 2026-06-07 は日曜
        let rows = vec![
            tc("2026-06-07 08:00:00", "始業"),
            tc("2026-06-07 20:00:00", "終業"), // 720 分
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.is_legal_holiday);
        assert_eq!(d.legal_holiday_minutes, 720);
        assert_eq!(d.statutory_minutes, 0);
        assert_eq!(d.within_statutory_overtime_minutes, 0);
        assert_eq!(d.overtime_minutes, 0);
    }

    #[test]
    fn sunday_night_goes_to_legal_holiday_night() {
        // 日曜 21:00〜翌 02:00 → 深夜は 22:00〜02:00 の 240 分
        let rows = vec![
            tc("2026-06-07 21:00:00", "始業"),
            tc("2026-06-08 02:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.is_legal_holiday);
        assert_eq!(d.legal_holiday_night_minutes, 240);
        assert_eq!(d.night_minutes, 0);
        assert_eq!(d.overtime_night_minutes, 0);
    }

    // --- 深夜の振り分け ---

    #[test]
    fn night_splits_between_statutory_and_overtime() {
        // 月曜 18:00〜翌 06:00 = 720 分、休憩なし
        //   実働 0〜450 分  = 18:00〜01:30 … うち深夜 22:00〜01:30 = 210 分 → night
        //   実働 450〜480   = 01:30〜02:00 … 全部深夜 30 分         → night (法定内残業)
        //   実働 480〜720   = 02:00〜06:00 … うち深夜 02:00〜05:00 = 180 分 → overtime_night
        let rows = vec![
            tc("2026-06-08 18:00:00", "始業"),
            tc("2026-06-09 06:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.working_minutes, 720);
        assert_eq!(d.night_minutes, 240);
        assert_eq!(d.overtime_night_minutes, 180);
        assert_eq!(d.legal_holiday_night_minutes, 0);
        // 深夜と時間外深夜は排他 — 合計が実際の深夜帯 (22:00〜05:00 = 420 分) に一致
        assert_eq!(d.night_minutes + d.overtime_night_minutes, 420);
    }

    // --- 休憩 ---

    #[test]
    fn breaks_below_threshold_are_ignored() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 10:09:00", "休憩"), // 9 分
            ev("2026-06-02 12:00:00", "2026-06-02 12:10:00", "休憩"), // 10 分
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 10);
    }

    #[test]
    fn break_threshold_is_configurable() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 10:09:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let p = KosokuParams {
            break_threshold_minutes: 5,
            ..KosokuParams::default()
        };
        assert_eq!(daily_summary(&rows, "2026-06", &p)[0].break_minutes, 9);
    }

    #[test]
    fn overlapping_breaks_are_counted_once() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:00:00", "休憩"),
            ev("2026-06-02 12:30:00", "2026-06-02 13:30:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 90);
        assert_eq!(d.working_minutes, 540 - 90);
    }

    #[test]
    fn break_outside_shift_is_clipped_away() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 07:00:00", "2026-06-02 08:00:00", "休憩"), // 勤務前
            ev("2026-06-02 19:00:00", "2026-06-02 20:00:00", "休憩"), // 勤務後
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
        assert_eq!(d.working_minutes, 540);
    }

    #[test]
    fn break_straddling_shift_start_is_clipped_not_dropped() {
        // 08:45〜09:15 の 30 分休憩。勤務内は 09:00〜09:15 の 15 分だけ
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 08:45:00", "2026-06-02 09:15:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 15);
    }

    #[test]
    fn break_without_end_is_ignored() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            json!({"datetime": "2026-06-02 12:00:00", "end_datetime": null,
                   "source": "dtako_events", "state": "休憩"}),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        assert_eq!(
            daily_summary(&rows, "2026-06", &KosokuParams::default())[0].break_minutes,
            0
        );
    }

    // --- 同日の運行の継ぎ目 ---

    #[test]
    fn same_day_run_joint_without_punch_is_work_not_break() {
        // 乗務員 1029 / 2026-06-23 と同じ形 — 継ぎ目 5 分、間に打刻なし (2 本目は
        // 同じ乗務員の 06-24 で観測した 1 分の継ぎ目を同日に寄せたもの)。
        // 継ぎ目は勤務の中に残り、休憩にも入らない (作業として拘束・実働に計上)
        let rows = vec![
            tc("2026-06-23 00:04:00", "始業"),
            dtako("2026-06-23 00:18:00", "運行終了"),
            dtako("2026-06-23 00:23:00", "運行開始"),
            dtako("2026-06-23 02:15:00", "運行終了"),
            dtako("2026-06-23 02:16:00", "運行開始"),
            tc("2026-06-23 15:03:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].restraint_minutes, 899);
        assert_eq!(d[0].break_minutes, 0);
        assert_eq!(d[0].working_minutes, 899);
    }

    #[test]
    fn same_day_run_joint_with_close_punch_splits_into_two_shifts() {
        // 乗務員 1026 / 2026-06-02 と同じ形 — 継ぎ目 14:07→23:26 に終業打刻がある。
        // 勤務が切れ、間の時間は拘束に入らない (休息)
        let rows = vec![
            tc("2026-06-02 01:28:00", "始業"),
            dtako("2026-06-02 14:07:00", "運行終了"),
            tc("2026-06-02 14:14:00", "終業"),
            tc("2026-06-02 23:32:00", "始業"),
            dtako("2026-06-02 23:26:00", "運行開始"),
            tc("2026-06-03 15:13:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].restraint_minutes, 766);
        assert_eq!(d[1].restraint_minutes, 941);
        // 14:14〜23:32 の 558 分はどちらの拘束にも入らない
        assert_eq!(d[0].end, "2026-06-02 14:14:00");
        assert_eq!(d[1].start, "2026-06-02 23:32:00");
    }

    // --- 24 時間超の打ち切り ---

    #[test]
    fn restraint_over_24h_is_capped_and_flagged() {
        // 実測の外れ値と同じ長さ (2288 分 = 38.1 時間) を与える
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        assert_eq!(d.restraint_minutes, 1440);
        assert_eq!(d.end, "2026-06-03 06:00:00");
        // 打ち切った後も内訳の合計は実働に一致する
        assert_eq!(
            d.working_minutes,
            d.statutory_minutes
                + d.within_statutory_overtime_minutes
                + d.overtime_minutes
                + d.legal_holiday_minutes
        );
    }

    #[test]
    fn exactly_24h_is_not_flagged() {
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 06:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(!d.over_24h);
        assert_eq!(d.restraint_minutes, 1440);
    }

    #[test]
    fn breaks_after_the_cut_are_not_counted() {
        // 打ち切り後 (翌日 10:00) の休憩は勤務外なので引かない
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            ev("2026-06-03 10:00:00", "2026-06-03 11:00:00", "休憩"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        assert_eq!(d.break_minutes, 0);
        assert_eq!(d.working_minutes, 1440);
    }

    // --- 勤務を構成した打刻 (Refs #128) ---

    #[test]
    fn punches_keep_the_raw_stamps() {
        // 秒を落とさない (打刻カードの元の値)
        let rows = vec![
            tc("2026-06-02 09:25:37", "始業"),
            tc("2026-06-02 19:39:04", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(
            d.punches,
            vec![
                Punch { at: "2026-06-02 09:25:37".into(), state: "始業".into() },
                Punch { at: "2026-06-02 19:39:04".into(), state: "終業".into() },
            ]
        );
        // 勤務としての解釈 (分に丸め) は start / end のまま
        assert_eq!(d.start, "2026-06-02 09:25:00");
    }

    #[test]
    fn punches_survive_the_24h_cap() {
        // 乗務員 1194 / 2026-04 と同じ形 — 打刻が運行 1 本 (2 晩) を挟み 43 時間になる。
        // 拘束は 24 時間で打ち切るが、**切った先にある終業打刻を落とさない**
        let rows = vec![
            tc("2026-06-01 21:31:32", "始業"),
            tc("2026-06-03 16:47:04", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        assert_eq!(d.restraint_minutes, 1440);
        // end は打ち切り後 (実在しない時刻)
        assert_eq!(d.end, "2026-06-02 21:31:00");
        // 打刻は実際の終業まで残る
        assert_eq!(d.punches.len(), 2);
        assert_eq!(d.punches[1].at, "2026-06-03 16:47:04");
    }

    #[test]
    fn punches_are_empty_for_rest_shifts() {
        let rows = vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.source, ShiftSource::Rest);
        assert!(d.punches.is_empty());
    }

    #[test]
    fn punches_exclude_other_shifts_and_non_punch_events() {
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            dtako("2026-06-02 06:30:00", "運行開始"),
            tc("2026-06-02 15:00:00", "終業"),
            // 次の勤務の打刻は混ぜない
            tc("2026-06-02 20:00:00", "始業"),
            tc("2026-06-03 05:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].punches.len(), 2);
        assert_eq!(days[0].punches[1].at, "2026-06-02 15:00:00");
        assert_eq!(days[1].punches[0].at, "2026-06-02 20:00:00");
    }

    #[test]
    fn punch_traits_are_wired() {
        let p = Punch { at: "2026-06-02 09:25:37".into(), state: "始業".into() };
        assert_eq!(p.clone(), p);
        assert!(format!("{p:?}").contains("始業"));
    }

    // --- 乗務員ごとの分割 (Refs #125) ---

    fn tc_of(driver: i64, datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "end_datetime": null, "driver_id": driver,
               "source": "timecard", "state": state})
    }

    #[test]
    fn split_by_driver_groups_and_sorts() {
        let rows = vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1018, "2026-06-02 09:25:00", "始業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
            tc_of(1018, "2026-06-02 19:39:00", "終業"),
        ];
        let split = split_by_driver(rows);
        assert_eq!(split.len(), 2);
        // 乗務員CD 昇順
        assert_eq!(split[0].0, 1018);
        assert_eq!(split[1].0, 1119);
        // 行の並びは入力のまま
        assert_eq!(split[0].1.len(), 2);
        assert_eq!(split[0].1[0]["datetime"], "2026-06-02 09:25:00");
        assert_eq!(split[1].1[0]["datetime"], "2026-06-02 06:00:00");
    }

    #[test]
    fn split_by_driver_drops_rows_without_a_driver() {
        // 誰のものか決められない行を混ぜると、その人の拘束が静かに伸びる
        let rows = vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            json!({"datetime": "2026-06-02 07:00:00", "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 08:00:00", "driver_id": null,
                   "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 09:00:00", "driver_id": "1119",
                   "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 10:00:00", "driver_id": -1,
                   "source": "timecard", "state": "始業"}),
        ];
        let split = split_by_driver(rows);
        assert_eq!(split.len(), 1);
        assert_eq!(split[0].0, 1119);
        assert_eq!(split[0].1.len(), 1);
    }

    #[test]
    fn split_by_driver_keeps_shifts_apart() {
        // 混ざったまま畳むと、他人の休息で勤務が切れる
        let rows = vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1018, "2026-06-02 07:00:00", "始業"),
            tc_of(1018, "2026-06-02 12:00:00", "終業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
        ];
        let p = KosokuParams::default();
        // 分けずに畳むと、後から来た始業が前の始業を上書きし、1119 の 06:00 が消えて
        // 1018 の 07:00〜12:00 だけが 1 勤務として残る (1119 の拘束が丸ごと落ちる)
        let mixed = daily_summary(&rows.clone(), "2026-06", &p);
        assert_eq!(mixed.len(), 1);
        assert_eq!(mixed[0].restraint_minutes, 300);
        // 分ければそれぞれの拘束が出る
        let split = split_by_driver(rows);
        let d1018 = daily_summary(&split[0].1, "2026-06", &p);
        let d1119 = daily_summary(&split[1].1, "2026-06", &p);
        assert_eq!(d1018[0].restraint_minutes, 300);
        assert_eq!(d1119[0].restraint_minutes, 720);
    }

    #[test]
    fn split_by_driver_on_empty_input() {
        assert!(split_by_driver(Vec::new()).is_empty());
    }

    // --- 月境界 ---

    #[test]
    fn shift_is_filed_under_its_start_date() {
        // 6/30 22:00 始業 → 7/1 08:00 終業。丸ごと 6 月に載る
        let rows = vec![
            tc("2026-06-30 22:00:00", "始業"),
            tc("2026-07-01 08:00:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].date, "2026-06-30");
        assert_eq!(d[0].restraint_minutes, 600);
        // 7 月として集計すると出てこない
        assert!(daily_summary(&rows, "2026-07", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn previous_month_shift_is_excluded() {
        let rows = vec![
            tc("2026-05-31 09:00:00", "始業"),
            tc("2026-05-31 18:00:00", "終業"),
            tc("2026-06-01 09:00:00", "始業"),
            tc("2026-06-01 18:00:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].date, "2026-06-01");
    }

    #[test]
    fn seconds_are_floored_on_shift_bounds() {
        let rows = vec![
            tc("2026-06-02 09:25:41", "始業"),
            tc("2026-06-02 19:39:59", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.start, "2026-06-02 09:25:00");
        assert_eq!(d.end, "2026-06-02 19:39:00");
        assert_eq!(d.restraint_minutes, 614);
    }

    #[test]
    fn sub_minute_shift_is_dropped() {
        let rows = vec![
            tc("2026-06-02 09:25:10", "始業"),
            tc("2026-06-02 09:25:50", "終業"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn empty_input_is_empty_output() {
        assert!(daily_summary(&[], "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn default_params_are_7h30_and_8h() {
        let p = KosokuParams::default();
        assert_eq!(p.break_threshold_minutes, 10);
        assert_eq!(p.prescribed_minutes, 450);
        assert_eq!(p.legal_minutes, 480);
    }

    #[test]
    fn shift_source_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&ShiftSource::Timecard).unwrap(),
            "\"timecard\""
        );
        assert_eq!(
            serde_json::to_string(&ShiftSource::Rest).unwrap(),
            "\"rest\""
        );
    }

    #[test]
    fn event_and_shift_are_debug_and_comparable() {
        // 派生 trait を使う経路を 1 本通しておく (Debug / PartialEq / Clone)
        let e = Event {
            start: dt("2026-06-02 09:00:00"),
            end: None,
            source: "timecard".into(),
            state: "始業".into(),
        };
        assert_eq!(e.clone(), e);
        assert!(format!("{e:?}").contains("始業"));
        let s = Shift {
            start: dt("2026-06-02 09:00:00"),
            end: dt("2026-06-02 18:00:00"),
            source: ShiftSource::Timecard,
        };
        assert_eq!(s.clone(), s);
        assert!(format!("{s:?}").contains("Timecard"));
        let p = KosokuParams::default();
        assert!(format!("{:?}", p.clone()).contains("450"));
        let d = &daily_summary(
            &[
                tc("2026-06-02 09:00:00", "始業"),
                tc("2026-06-02 18:00:00", "終業"),
            ],
            "2026-06",
            &p,
        )[0];
        assert_eq!(d.clone(), *d);
        assert!(format!("{d:?}").contains("2026-06-02"));
    }
}
