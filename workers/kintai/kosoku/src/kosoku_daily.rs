//! `GET /api/kintai/kosoku-daily` の「取ってきた行から応答を組む」部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版の `routes/kintai.rs` (`kosoku_daily` / `kosoku_daily_all`) から I/O を除いた全部を移したもの。
//! 読み (遡り起点・生イベント・フェリー) は呼び手が持ち、ここは行を受けて JSON を返すだけ。
//!
//! - 単一乗務員版と全乗務員版は [`build_driver`] (1 乗務員ぶんを組む) を共有する。
//!   形の違いは外側だけ — 単一版は [`DriverDaily::into_single`]、全員版は 1 要素ずつ [`for_each_driver`]
//! - 全員版は**乗務員ごとに 1 要素を渡す**ので、応答全体の木を持たずに書き出せる ([`write_all_drivers`])。
//!   オンプレ版は集めて `{month, drivers}` を作る。どちらも同じバイト列になる

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::anchors::{clip_to_anchors, HeadAnchors};
use crate::kosoku::{
    apply_ferry_minus, daily_summary, drop_duplicate_rows, ferry_minus_by_date, month_punches,
    split_by_driver, split_ferry_by_driver, DayPart, DaySummary, KosokuParams, Punch, ShiftSource,
};
use crate::kosoku_paper::{
    gap_midnight_by_date, minus_unko_by_date, ours_outside_by_date, paper_daily_minutes,
    paper_drift_by_date, paper_outside_by_date,
};

/// 突合用に日別を絞る (Refs #157)。
///
/// 全項目だと 1 日 516 B・19 キーあり、2026-05 の全乗務員で **1.71 MB**。突合
/// (`timecard-compare` / `get_timecard_diff`) が使うのは**日付・拘束・フェリー控除**と、
/// 暦日按分のための `parts` の日付・拘束だけで、残り 15 キーは受け取って捨てられていた。
/// 絞ると **108 KB (16 分の 1)**。
///
/// この経路は社内から Cloudflare Tunnel を通って出ていくので、応答サイズがそのまま
/// 応答時間になる (実測: DB 0.48 秒 / rust 0.46 秒 なのにブラウザで 14〜57 秒)。
///
/// **キー名は元のまま**にする。短縮すると消費側 (relay / kyuyo-mcp) のパーサを
/// 2 通り持つことになり、削れるのは数 % しかない。
fn compare_days(days: &[DaySummary]) -> Vec<Value> {
    days.iter()
        .map(|d| {
            let parts: Vec<Value> = d
                .parts
                .iter()
                .map(|p| {
                    let mut o = json!({
                        "date": p.date,
                        "restraint_minutes": p.restraint_minutes,
                    });
                    if p.run_gap_minutes != 0 {
                        o["run_gap_minutes"] = json!(p.run_gap_minutes);
                    }
                    if p.punch_tail_minutes != 0 {
                        o["punch_tail_minutes"] = json!(p.punch_tail_minutes);
                    }
                    if p.punch_head_minutes != 0 {
                        o["punch_head_minutes"] = json!(p.punch_head_minutes);
                    }
                    if p.run_head_minutes != 0 {
                        o["run_head_minutes"] = json!(p.run_head_minutes);
                    }
                    if p.lunch_overlap_minutes != 0 {
                        o["lunch_overlap_minutes"] = json!(p.lunch_overlap_minutes);
                    }
                    // 日跨ぎ勤務のフェリー控除は**内訳側が正** — 突合 (relay の
                    // kosokuPartsByDate) は parts がある勤務を parts だけで暦日合算
                    // するので、ここに載せないと控除が丸ごと落ちて unknown になる
                    // (実測 ある乗務員: 単日勤務の 03-08 だけ ferry が付き、日跨ぎの
                    // 03-05/06/15/22/29 は 71〜75 分がそのまま残差になっていた)
                    if p.ferry_minus_minutes != 0 {
                        o["ferry_minus_minutes"] = json!(p.ferry_minus_minutes);
                    }
                    o
                })
                .collect();
            let mut o = json!({
                "date": d.date,
                "restraint_minutes": d.restraint_minutes,
            });
            // **0 は載せない** (Refs #157)。フェリー控除がある日は月に数十日しか無いのに
            // `"ferry_minus_minutes":0,` が全日に付くと 3,128 日で約 75 KB (応答の 29%)
            // を食う。消費側は欠けを 0 として読む
            if d.ferry_minus_minutes != 0 {
                o["ferry_minus_minutes"] = json!(d.ferry_minus_minutes);
            }
            // 休息控除も同じ扱い。拘束からは既に外してあるので突合の値は動かないが、
            // 「この日は休息を何分外したか」が無いと残差の説明が付かない
            if d.rest_minus_minutes != 0 {
                o["rest_minus_minutes"] = json!(d.rest_minus_minutes);
            }
            // 運行の継ぎ目 (cause "run-gap" の実額) も 0 は載せない
            if d.run_gap_minutes != 0 {
                o["run_gap_minutes"] = json!(d.run_gap_minutes);
            }
            // 日跨ぎ終業の尻尾 (cause "punch-tail" の実額) も同じ規則
            if d.punch_tail_minutes != 0 {
                o["punch_tail_minutes"] = json!(d.punch_tail_minutes);
            }
            // 日跨ぎ始業の頭 (cause "punch-head" の実額) も同じ規則
            if d.punch_head_minutes != 0 {
                o["punch_head_minutes"] = json!(d.punch_head_minutes);
            }
            // 始業前の運行の頭 (cause "run-head" の実額、紙が大きくなる向き) も同じ規則
            if d.run_head_minutes != 0 {
                o["run_head_minutes"] = json!(d.run_head_minutes);
            }
            // 昼休の窓との重なり (cause "lunch" の実額) も同じ規則
            if d.lunch_overlap_minutes != 0 {
                o["lunch_overlap_minutes"] = json!(d.lunch_overlap_minutes);
            }
            // 1 日で終わる勤務は内訳が本体と同じなので載せない (元の応答と同じ規則)
            if !parts.is_empty() {
                o["parts"] = Value::Array(parts);
            }
            o
        })
        .collect()
}

/// 画面のタイムカード表用に日別を絞る (Refs #164)。
///
/// [`compare_days`] (突合用) と同じ発想の**画面経路**版。全項目だと全乗務員で
/// 月 ~1.7 MB あり、それが社内から Cloudflare Tunnel を通って毎回出ていく
/// (方針は「圧縮より先にデータを減らす」— #156 revert 時のユーザー決定)。
///
/// 消費側は 2 つ — nuxt-dtako-admin front の `app/utils/kosoku-daily.ts`
/// (`toKosokuDay`) と relay の `workers/dtako-scraper-relay/src/kosoku-daily.ts`
/// (`parseKosokuDaily`)。残す/落とすはどちらの実コードにも合わせてある:
///
/// - **常に残す**: `date` / `start` / `end` — 消費側はどれかが欠けた日を捨てる
/// - **既定と違うときだけ載せる**: `source` は `rest` のみ (消費側は `=== 'rest'`
///   判定)、`is_legal_holiday` / `over_24h` は `true` のみ (`=== true` 判定)、
///   分数は非 0 のみ (欠けは 0 に落ちる)
/// - `punches` (勤務の中の打刻 = 表の出勤/退社列の原本) と `parts` (暦日按分)
///   は**空でなければ**残す。part 側も `date` + 非 0 分数だけ
/// - **落とす**: `rest_minus_minutes` (compare の診断専用で画面は読まない)
///
/// **キー名は元のまま** (compare_days と同じ理由 — 消費側のパーサを 2 通りに
/// しない)。
fn timecard_days(days: &[DaySummary]) -> Vec<Value> {
    days.iter()
        .map(|d| {
            let mut o = json!({
                "date": d.date,
                "start": d.start,
                "end": d.end,
            });
            // 消費側は `=== 'rest'` で見るので、既定の `timecard` は書かない
            if d.source == ShiftSource::Rest {
                o["source"] = json!(d.source);
            }
            // `=== true` 判定なので false は書かない
            if d.is_legal_holiday {
                o["is_legal_holiday"] = json!(true);
            }
            if d.over_24h {
                o["over_24h"] = json!(true);
            }
            // **0 は載せない** (Refs #157 と同じ規則)。日別 13 個の分数の過半は 0 で、
            // 消費側は欠けを 0 として読む
            for (key, v) in [
                ("restraint_minutes", d.restraint_minutes),
                ("break_minutes", d.break_minutes),
                ("working_minutes", d.working_minutes),
                ("statutory_minutes", d.statutory_minutes),
                (
                    "within_statutory_overtime_minutes",
                    d.within_statutory_overtime_minutes,
                ),
                ("overtime_minutes", d.overtime_minutes),
                ("legal_holiday_minutes", d.legal_holiday_minutes),
                ("night_minutes", d.night_minutes),
                ("overtime_night_minutes", d.overtime_night_minutes),
                ("legal_holiday_night_minutes", d.legal_holiday_night_minutes),
                ("ferry_minus_minutes", d.ferry_minus_minutes),
            ] {
                if v != 0 {
                    o[key] = json!(v);
                }
            }
            // 休息由来の勤務は空 — 空配列を全日ぶら下げない
            if !d.punches.is_empty() {
                o["punches"] = json!(d.punches);
            }
            // 1 日で終わる勤務は空 (元の応答と同じ規則)
            if !d.parts.is_empty() {
                o["parts"] = Value::Array(timecard_parts(&d.parts));
            }
            o
        })
        .collect()
}

/// [`timecard_days`] の暦日按分 — `date` + 非 0 分数だけ (Refs #164)。
fn timecard_parts(parts: &[DayPart]) -> Vec<Value> {
    parts
        .iter()
        .map(|p| {
            let mut o = json!({ "date": p.date });
            for (key, v) in [
                ("restraint_minutes", p.restraint_minutes),
                ("working_minutes", p.working_minutes),
                ("overtime_minutes", p.overtime_minutes),
                ("legal_holiday_minutes", p.legal_holiday_minutes),
                ("night_minutes", p.night_minutes),
                ("overtime_night_minutes", p.overtime_night_minutes),
                ("legal_holiday_night_minutes", p.legal_holiday_night_minutes),
                ("ferry_minus_minutes", p.ferry_minus_minutes),
            ] {
                if v != 0 {
                    o[key] = json!(v);
                }
            }
            o
        })
        .collect()
}

/// 応答の絞り方。**未知の値は [`Full`](ResponseView::Full)** — 綴り間違いで黙って
/// 情報が減らないように、従来どおり全項目へ倒す (壊さない方に倒す)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseView {
    /// 従来どおり全項目。
    Full,
    /// 突合に要る項目だけ (Refs #157)。
    Compare,
    /// 画面のタイムカード表に要る項目だけ (Refs #164)。
    Timecard,
}

/// `view=compare` / `view=timecard`。省略・未知の値は [`ResponseView::Full`]。
pub fn parse_view(view: Option<&str>) -> ResponseView {
    match view {
        Some("compare") => ResponseView::Compare,
        Some("timecard") => ResponseView::Timecard,
        _ => ResponseView::Full,
    }
}

/// 1 乗務員ぶんを組んだ結果。外側の形 (単一版 / 全員版の 1 要素) は [`DriverDaily::into_single`] /
/// [`DriverDaily::into_entry`] が決める。
#[derive(Debug, Clone, PartialEq)]
pub struct DriverDaily {
    /// 日別サマリ (フェリー控除を貼った後)。
    pub days: Vec<DaySummary>,
    /// 月全打刻 — 勤務と切り離して返す (対になる終業が無い始業も表に出すため、#137)。
    pub punches: Vec<Punch>,
    /// 全列同一の行の暦日ごとの件数 (紙は二重計上する)。
    pub duplicate_rows: BTreeMap<String, i64>,
    /// 紙の再現値との日別の差 (cause `rounding` の実額)。compare だけ。
    paper_drift: BTreeMap<String, i64>,
    /// フェリー控除の日別マップそのもの。
    ferry_map: BTreeMap<String, i64>,
    /// 紙が勤務の外で数えている分 (cause `paper-outside`)。compare だけ。
    paper_outside: BTreeMap<String, i64>,
    /// こちらだけが数える時間 (cause `ours-outside`)。compare だけ。
    ours_outside: BTreeMap<String, i64>,
    /// 紙が引く 運行開始 → 始業 (cause `minus-unko`)。compare だけ。
    minus_unko: BTreeMap<String, i64>,
    /// 深夜を跨ぐ継ぎ目の暦日配分の差 (cause `gap-midnight`)。compare だけ。
    gap_midnight: BTreeMap<String, i64>,
}

/// 1 乗務員ぶんの行 (`rows`) とフェリーの行 (`ferry`) から日別を組む。
///
/// - 紙の再現は**重複除去の前**の行で計算する — 紙は重複行を二重計上するので、除去後の行では
///   再現にならない (実測 ある乗務員 2026-04-04: 二重登録の運行 11 分を紙は数え、除去後の再現では
///   drift 0 になって差が unknown に残った)。突合 (`view=compare`) のときだけ計算する
/// - フェリー控除は**拘束の計算には入れない** — 突合で差の原因を説明するためだけ (Refs #146)。
///   フェリーが読めなかったときは呼び手が空を渡す (控除 0 で続ける。付帯情報のために本体を落とさない)
pub fn build_driver(
    rows: Vec<Value>,
    ferry: &[Value],
    month: &str,
    params: &KosokuParams,
    view: ResponseView,
) -> DriverDaily {
    let compare = view == ResponseView::Compare;
    let by_date = |f: fn(&[Value], &str) -> BTreeMap<String, i64>| {
        if compare {
            f(&rows, month)
        } else {
            BTreeMap::new()
        }
    };
    let paper = compare.then(|| paper_daily_minutes(&rows, month));
    let paper_outside = by_date(paper_outside_by_date);
    let ours_outside = by_date(ours_outside_by_date);
    let minus_unko = by_date(minus_unko_by_date);
    let gap_midnight = by_date(gap_midnight_by_date);
    // 取り込みが 2 回走ると全列同一の行が入る。**紙は二重計上する**ので件数を返す
    let (rows, duplicate_rows) = drop_duplicate_rows(rows);
    let mut days = daily_summary(&rows, month, params);
    // 日別マップも持ち回る — 前月に始業した勤務だけが覆う日の控除は勤務に貼れないため、
    // 突合はマップを優先して読む (実測 ある乗務員 2026-05-01: 出庫 04-30 の運行のフェリー 76 分)
    let ferry_map = ferry_minus_by_date(ferry);
    apply_ferry_minus(&mut days, &ferry_map);
    let paper_drift = paper
        .map(|p| paper_drift_by_date(&days, &p))
        .unwrap_or_default();
    let punches = month_punches(&rows, month);
    DriverDaily {
        days,
        punches,
        duplicate_rows,
        paper_drift,
        ferry_map,
        paper_outside,
        ours_outside,
        minus_unko,
        gap_midnight,
    }
}

impl DriverDaily {
    /// 勤務も打刻も無い (全員版はこの乗務員を落とす — 退職者・内勤で応答を膨らませない)。
    pub fn is_empty(&self) -> bool {
        self.days.is_empty() && self.punches.is_empty()
    }

    /// 突合の付帯項目。無い方が普通なので、空でないときだけ載せる (フェリー控除と同じ規則)。
    fn put_compare_extras(&self, o: &mut Value) {
        for (key, map) in [
            ("duplicate_rows", &self.duplicate_rows),
            ("paper_drift_by_date", &self.paper_drift),
            ("ferry_minus_by_date", &self.ferry_map),
            ("paper_outside_by_date", &self.paper_outside),
            ("ours_outside_by_date", &self.ours_outside),
            ("minus_unko_by_date", &self.minus_unko),
            ("gap_midnight_by_date", &self.gap_midnight),
        ] {
            if !map.is_empty() {
                o[key] = json!(map);
            }
        }
    }

    /// 単一乗務員版の応答 (`driver=N`)。
    ///
    /// | view | 形 |
    /// |---|---|
    /// | full | `{month, driver, days, duplicate_rows, punches}` (`duplicate_rows` は空でも載せる) |
    /// | compare | `{month, driver, days}` + 突合の付帯項目 (空でないものだけ) |
    /// | timecard | `{month, driver, days}` — 画面は月全打刻も重複の診断も読まない (Refs #164) |
    pub fn into_single(self, month: &str, driver: u64, view: ResponseView) -> Value {
        match view {
            ResponseView::Compare => {
                // 突合は打刻を見ない
                let mut o =
                    json!({ "month": month, "driver": driver, "days": compare_days(&self.days) });
                self.put_compare_extras(&mut o);
                o
            }
            ResponseView::Timecard => {
                json!({ "month": month, "driver": driver, "days": timecard_days(&self.days) })
            }
            ResponseView::Full => json!({
                "month": month,
                "driver": driver,
                "days": self.days,
                "duplicate_rows": self.duplicate_rows,
                "punches": self.punches,
            }),
        }
    }

    /// 全乗務員版の 1 要素。
    ///
    /// | view | 形 |
    /// |---|---|
    /// | full | `{driver, days, punches}` + `duplicate_rows` (空でなければ) |
    /// | compare | `{driver, days}` + 突合の付帯項目 (空でないものだけ) |
    /// | timecard | `{driver, days}` |
    pub fn into_entry(self, driver: u64, view: ResponseView) -> Value {
        match view {
            ResponseView::Timecard => {
                json!({ "driver": driver, "days": timecard_days(&self.days) })
            }
            ResponseView::Compare => {
                let mut o = json!({ "driver": driver, "days": compare_days(&self.days) });
                self.put_compare_extras(&mut o);
                o
            }
            ResponseView::Full => {
                let mut o = json!({ "driver": driver, "days": self.days, "punches": self.punches });
                if !self.duplicate_rows.is_empty() {
                    o["duplicate_rows"] = json!(self.duplicate_rows);
                }
                o
            }
        }
    }
}

/// 全乗務員版 (`driver` 省略、Refs #125)。乗務員CD 昇順に 1 要素ずつ `each` へ渡し、渡した数を返す。
///
/// - `rows` は全乗務員を窓 (遡り起点の最小から) で 1 回読んだもの。乗務員ごとの起点へ切り戻してから
///   畳む (fold と同じ、`clip_to_anchors`)。`ferry` は全乗務員のフェリーの行 (読めなかったら空)
/// - [`daily_summary`] は乗務員を知らないので**先に分けてから**乗務員ごとに呼ぶ。混ぜたまま渡すと
///   他人の休息で勤務が切れる
/// - 乗務員CD=0 は打刻の紐付かないデジタコ運行 (構内移動・回送・乗務員未確定等) で実在の従業員ではない
///   (Refs #284)。単一乗務員版は診断用途を残すため対象外 — ここは全員版だけの絞り込み
/// - **勤務も打刻も無い乗務員は落とす** ([`DriverDaily::is_empty`])
pub fn for_each_driver(
    rows: Vec<Value>,
    ferry: Vec<Value>,
    month: &str,
    anchors: &HeadAnchors,
    params: &KosokuParams,
    view: ResponseView,
    mut each: impl FnMut(Value),
) -> usize {
    let ferry_by_driver = split_ferry_by_driver(ferry);
    let mut n = 0;
    for (driver, rows) in clip_to_anchors(split_by_driver(rows), month, anchors) {
        if driver == 0 {
            continue;
        }
        let ferry = ferry_by_driver.get(&driver).map_or(&[][..], Vec::as_slice);
        let built = build_driver(rows, ferry, month, params, view);
        if built.is_empty() {
            continue;
        }
        each(built.into_entry(driver, view));
        n += 1;
    }
    n
}

/// 全乗務員版の応答を `out` へ書き出す。返すのは乗務員の数。
///
/// 中身は `{"month": month, "drivers": [<for_each_driver の要素>…]}` を serde_json で直列化したものと
/// **同じバイト列** (キーは serde_json の `Map` と同じ昇順 = `drivers`・`month`)。応答全体の木を持たずに、
/// 乗務員 1 人ぶんずつ直列化して書く (勤務 Worker のメモリのため)。書き込みの失敗はそのまま返す。
pub fn write_all_drivers(
    out: &mut impl std::io::Write,
    rows: Vec<Value>,
    ferry: Vec<Value>,
    month: &str,
    anchors: &HeadAnchors,
    params: &KosokuParams,
    view: ResponseView,
) -> std::io::Result<usize> {
    out.write_all(b"{\"drivers\":[")?;
    let mut result = Ok(());
    let mut written = 0usize;
    let n = for_each_driver(rows, ferry, month, anchors, params, view, |entry| {
        // 最初の失敗で止める (以後の要素は書かない)。2 人目からは前に `,` を付ける
        if result.is_ok() {
            result = write_entry(out, &entry, written > 0);
            written += 1;
        }
    });
    result?;
    out.write_all(b"],\"month\":")?;
    serde_json::to_writer(&mut *out, month)?;
    out.write_all(b"}")?;
    Ok(n)
}

fn write_entry(out: &mut impl std::io::Write, entry: &Value, comma: bool) -> std::io::Result<()> {
    if comma {
        out.write_all(b",")?;
    }
    serde_json::to_writer(&mut *out, entry)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tc_of(driver: i64, datetime: &str, state: &str) -> Value {
        json!({"datetime": datetime, "end_datetime": null, "driver_id": driver,
               "source": "timecard", "state": state})
    }

    fn ev_of(driver: i64, start: &str, end: &str, state: &str) -> Value {
        json!({"datetime": start, "end_datetime": end, "driver_id": driver,
               "source": "dtako_events", "state": state})
    }

    fn ferry_row(start: &str, end: &str, driver: u64) -> Value {
        json!({"start_datetime": start, "end_datetime": end, "driver_id": driver})
    }

    fn part(n: i64) -> DayPart {
        DayPart {
            date: "2026-06-03".to_string(),
            restraint_minutes: n,
            working_minutes: n,
            overtime_minutes: n,
            legal_holiday_minutes: n,
            night_minutes: n,
            overtime_night_minutes: n,
            legal_holiday_night_minutes: n,
            ferry_minus_minutes: n,
            run_gap_minutes: n,
            punch_tail_minutes: n,
            punch_head_minutes: n,
            run_head_minutes: n,
            lunch_overlap_minutes: n,
        }
    }

    /// 分数が全部 `n`。`n` が 0 でなければ「0 は載せない」の全部の分岐を通る。
    fn day(n: i64, source: ShiftSource, flags: bool) -> DaySummary {
        DaySummary {
            date: "2026-06-02".to_string(),
            start: "2026-06-02 06:00".to_string(),
            end: "2026-06-03 06:00".to_string(),
            source,
            punches: if flags {
                vec![Punch {
                    at: "2026-06-02 06:00:12".to_string(),
                    state: "始業".to_string(),
                }]
            } else {
                vec![]
            },
            parts: if flags {
                vec![part(n), part(0)]
            } else {
                vec![]
            },
            is_legal_holiday: flags,
            over_24h: flags,
            restraint_minutes: n,
            break_minutes: n,
            working_minutes: n,
            rest_minus_minutes: n,
            statutory_minutes: n,
            within_statutory_overtime_minutes: n,
            overtime_minutes: n,
            legal_holiday_minutes: n,
            night_minutes: n,
            overtime_night_minutes: n,
            legal_holiday_night_minutes: n,
            ferry_minus_minutes: n,
            run_gap_minutes: n,
            punch_tail_minutes: n,
            punch_head_minutes: n,
            run_head_minutes: n,
            lunch_overlap_minutes: n,
            non_working: vec![],
        }
    }

    fn built(days: Vec<DaySummary>) -> DriverDaily {
        DriverDaily {
            days,
            punches: vec![],
            duplicate_rows: BTreeMap::new(),
            paper_drift: BTreeMap::new(),
            ferry_map: BTreeMap::new(),
            paper_outside: BTreeMap::new(),
            ours_outside: BTreeMap::new(),
            minus_unko: BTreeMap::new(),
            gap_midnight: BTreeMap::new(),
        }
    }

    fn keys(v: &Value) -> Vec<&str> {
        v.as_object().unwrap().keys().map(String::as_str).collect()
    }

    #[test]
    fn view_parsing() {
        assert_eq!(parse_view(None), ResponseView::Full);
        assert_eq!(parse_view(Some("compare")), ResponseView::Compare);
        assert_eq!(parse_view(Some("timecard")), ResponseView::Timecard);
        // 未知の値は全項目へ倒す (壊さない方に倒す)
        assert_eq!(parse_view(Some("full")), ResponseView::Full);
        assert_eq!(parse_view(Some("")), ResponseView::Full);
    }

    #[test]
    fn compare_days_puts_only_non_zero_minutes() {
        let full = compare_days(&[day(7, ShiftSource::Timecard, true)]);
        let d = &full[0];
        for k in [
            "ferry_minus_minutes",
            "rest_minus_minutes",
            "run_gap_minutes",
            "punch_tail_minutes",
            "punch_head_minutes",
            "run_head_minutes",
            "lunch_overlap_minutes",
        ] {
            assert_eq!(d[k], 7, "{k}");
        }
        // 突合は打刻・実働・深夜を読まない
        assert!(d.get("punches").is_none() && d.get("working_minutes").is_none());
        let p = &d["parts"][0];
        assert_eq!(p["restraint_minutes"], 7);
        assert_eq!(p["ferry_minus_minutes"], 7);
        assert_eq!(p["lunch_overlap_minutes"], 7);
        assert!(p.get("working_minutes").is_none());
        // 0 の内訳は date と拘束だけ
        assert_eq!(keys(&d["parts"][1]), vec!["date", "restraint_minutes"]);

        let zero = compare_days(&[day(0, ShiftSource::Timecard, false)]);
        assert_eq!(keys(&zero[0]), vec!["date", "restraint_minutes"]);
    }

    #[test]
    fn timecard_days_puts_only_what_the_screen_reads() {
        let full = timecard_days(&[day(5, ShiftSource::Rest, true)]);
        let d = &full[0];
        assert_eq!(d["source"], "rest");
        assert_eq!(d["is_legal_holiday"], true);
        assert_eq!(d["over_24h"], true);
        assert_eq!(d["within_statutory_overtime_minutes"], 5);
        assert_eq!(d["punches"][0]["at"], "2026-06-02 06:00:12");
        assert_eq!(d["parts"][0]["legal_holiday_night_minutes"], 5);
        assert_eq!(keys(&d["parts"][1]), vec!["date"]);
        // 画面は休息控除を読まない
        assert!(d.get("rest_minus_minutes").is_none());

        let zero = timecard_days(&[day(0, ShiftSource::Timecard, false)]);
        assert_eq!(keys(&zero[0]), vec!["date", "end", "start"]);
    }

    #[test]
    fn single_and_entry_shapes_per_view() {
        let mk = || built(vec![day(0, ShiftSource::Timecard, false)]);
        assert_eq!(
            keys(&mk().into_single("2026-06", 1442, ResponseView::Full)),
            vec!["days", "driver", "duplicate_rows", "month", "punches"]
        );
        assert_eq!(
            keys(&mk().into_single("2026-06", 1442, ResponseView::Compare)),
            vec!["days", "driver", "month"]
        );
        assert_eq!(
            keys(&mk().into_single("2026-06", 1442, ResponseView::Timecard)),
            vec!["days", "driver", "month"]
        );
        assert_eq!(
            keys(&mk().into_entry(1442, ResponseView::Full)),
            vec!["days", "driver", "punches"]
        );
        assert_eq!(
            keys(&mk().into_entry(1442, ResponseView::Compare)),
            vec!["days", "driver"]
        );
        assert_eq!(
            keys(&mk().into_entry(1442, ResponseView::Timecard)),
            vec!["days", "driver"]
        );
    }

    #[test]
    fn compare_extras_are_put_only_when_non_empty() {
        let one: BTreeMap<String, i64> = [("2026-06-02".to_string(), 3)].into_iter().collect();
        let mut b = built(vec![]);
        b.duplicate_rows = one.clone();
        b.paper_drift = one.clone();
        b.ferry_map = one.clone();
        b.paper_outside = one.clone();
        b.ours_outside = one.clone();
        b.minus_unko = one.clone();
        b.gap_midnight = one;
        let single = b.clone().into_single("2026-06", 1, ResponseView::Compare);
        let entry = b.clone().into_entry(1, ResponseView::Compare);
        for k in [
            "duplicate_rows",
            "paper_drift_by_date",
            "ferry_minus_by_date",
            "paper_outside_by_date",
            "ours_outside_by_date",
            "minus_unko_by_date",
            "gap_midnight_by_date",
        ] {
            assert_eq!(single[k]["2026-06-02"], 3, "{k}");
            assert_eq!(entry[k]["2026-06-02"], 3, "{k}");
        }
        // 全員版の full は重複の診断だけ (空でなければ)
        let full = b.into_entry(1, ResponseView::Full);
        assert_eq!(full["duplicate_rows"]["2026-06-02"], 3);
        assert!(full.get("ferry_minus_by_date").is_none());
    }

    #[test]
    fn build_driver_folds_one_driver() {
        let rows = vec![
            tc_of(1442, "2026-06-02 06:00:00", "始業"),
            ev_of(1442, "2026-06-02 10:00:00", "2026-06-02 11:00:00", "休憩"),
            ev_of(1442, "2026-06-02 10:00:00", "2026-06-02 11:00:00", "休憩"),
            tc_of(1442, "2026-06-02 20:00:00", "終業"),
        ];
        let ferry = [ferry_row(
            "2026-06-02 12:00:00",
            "2026-06-02 12:30:00",
            1442,
        )];
        let p = KosokuParams::default();
        let b = build_driver(rows.clone(), &ferry, "2026-06", &p, ResponseView::Compare);
        assert_eq!(b.days.len(), 1);
        // 重複は落として 1 回だけ引く。件数は暦日ごと
        assert_eq!(b.days[0].break_minutes, 60);
        assert_eq!(b.duplicate_rows["2026-06-02"], 1);
        // フェリーは貼るが拘束には入れない
        assert_eq!(b.days[0].ferry_minus_minutes, 30);
        assert_eq!(b.days[0].restraint_minutes, 840);
        assert_eq!(b.ferry_map["2026-06-02"], 30);
        assert_eq!(b.punches.len(), 2);
        assert!(!b.is_empty());

        // 突合でなければ紙の再現は計算しない
        let full = build_driver(rows, &[], "2026-06", &p, ResponseView::Full);
        assert!(full.paper_drift.is_empty() && full.paper_outside.is_empty());
        assert_eq!(full.days[0].ferry_minus_minutes, 0);
        assert!(build_driver(vec![], &[], "2026-06", &p, ResponseView::Full).is_empty());
    }

    fn all_rows() -> Vec<Value> {
        vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1018, "2026-06-02 09:25:00", "始業"),
            tc_of(1018, "2026-06-02 19:39:00", "終業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
            // 乗務員CD=0 は従業員ではない
            tc_of(0, "2026-06-02 07:00:00", "始業"),
            tc_of(0, "2026-06-02 17:00:00", "終業"),
            // 勤務も打刻も無い乗務員は落とす
            json!({"datetime": "2026-06-02 08:00:00", "end_datetime": null, "driver_id": 1500,
                   "source": "dtako", "state": "運行開始"}),
        ]
    }

    #[test]
    fn for_each_driver_splits_and_drops() {
        let ferry = vec![
            ferry_row("2026-06-02 10:00:00", "2026-06-02 11:00:00", 1119),
            ferry_row("2026-06-02 10:00:00", "2026-06-02 10:30:00", 1018),
        ];
        let mut got = vec![];
        let n = for_each_driver(
            all_rows(),
            ferry,
            "2026-06",
            &HeadAnchors::new(),
            &KosokuParams::default(),
            ResponseView::Full,
            |v| got.push(v),
        );
        assert_eq!(n, 2);
        // 乗務員CD 昇順、乗務員ごとに畳む (混ぜると 06:00〜19:39 の 1 勤務になる)
        assert_eq!(got[0]["driver"], 1018);
        assert_eq!(got[0]["days"][0]["restraint_minutes"], 614);
        assert_eq!(got[0]["days"][0]["ferry_minus_minutes"], 30);
        assert_eq!(got[1]["driver"], 1119);
        assert_eq!(got[1]["days"][0]["restraint_minutes"], 720);
        assert_eq!(got[1]["days"][0]["ferry_minus_minutes"], 60);
    }

    #[test]
    fn written_bytes_equal_the_collected_response() {
        let p = KosokuParams::default();
        let anchors = HeadAnchors::new();
        for view in [
            ResponseView::Full,
            ResponseView::Compare,
            ResponseView::Timecard,
        ] {
            let mut drivers = vec![];
            for_each_driver(all_rows(), vec![], "2026-06", &anchors, &p, view, |v| {
                drivers.push(v)
            });
            let want = json!({ "month": "2026-06", "drivers": drivers }).to_string();
            let mut out = Vec::new();
            let n = write_all_drivers(&mut out, all_rows(), vec![], "2026-06", &anchors, &p, view)
                .unwrap();
            assert_eq!(n, 2);
            assert_eq!(String::from_utf8(out).unwrap(), want);
        }
        // 0 人でも同じ
        let mut out = Vec::new();
        let view = ResponseView::Full;
        write_all_drivers(&mut out, vec![], vec![], "2026-06", &anchors, &p, view).unwrap();
        assert_eq!(out, br#"{"drivers":[],"month":"2026-06"}"#);
    }

    /// `limit` バイトまで受けて、超えたら失敗する書き先。
    struct Limited {
        buf: Vec<u8>,
        limit: usize,
    }

    impl std::io::Write for Limited {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            if self.buf.len() + b.len() > self.limit {
                return Err(std::io::Error::other("full"));
            }
            self.buf.extend_from_slice(b);
            Ok(b.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn a_write_failure_is_returned_and_stops_writing() {
        let p = KosokuParams::default();
        let anchors = HeadAnchors::new();
        let view = ResponseView::Full;
        // 頭・1 人目の途中・2 人目・閉じの各所で落ちても Err を返す
        let whole = {
            let mut out = Vec::new();
            write_all_drivers(&mut out, all_rows(), vec![], "2026-06", &anchors, &p, view).unwrap();
            out.len()
        };
        for limit in [0, 20, whole / 2 + 1, whole - 12, whole - 1] {
            let mut out = Limited { buf: vec![], limit };
            let r = write_all_drivers(&mut out, all_rows(), vec![], "2026-06", &anchors, &p, view);
            assert!(r.is_err(), "limit={limit}");
            assert!(out.buf.len() <= limit);
            std::io::Write::flush(&mut out).unwrap();
        }
    }
}
