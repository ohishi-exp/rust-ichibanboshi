//! #205-30: 共通キー 3,229 件のうち **値が違う 65 行**の究明 (2026-06)。
//!
//! オンプレ経路と GCP 経路は**同じ `daily_summary`** を通る。これは実測で確かめた —
//! オンプレの生イベント (乗務員 1021 / 2026-06 の 578 行) を現行コードで畳むと
//! **基準 34 行と 1 行残らず一致**する (`tests/kintai_205_30_replay_test.rs`)。
//! よって差は入力だけ。原因は**入力の source ごとに違う** 2 つだった。
//!
//! ## 1. 打刻が月境界で欠ける (`shift_source` 反転 28 行)
//!
//! **push と fold の窓が違う。**
//!
//! - `kintai_push::push_month` は `exact_month_range` = **`[月初, 翌月初)`** で書く
//! - `kintai_fold` は `month_range` = **`[月初, 翌月 2 日)`** で読む
//!
//! ずれの 1 日ぶん (`07-01`) は **7 月の push でしか書かれない**。7 月がまだ
//! push されていなければ、6 月の fold は**月を跨いだ勤務の終業打刻を見られない**。
//!
//! 実データの 1021 はまさにこれで、`始業 06-21 06:12:34` の相手は
//! **`終業 07-01 14:54:29`**。オンプレは MariaDB を `month_range` で直読みするので
//! 見えるが、GCP は Postgres に無いので見えない。
//!
//! 見える側 = 1 本の 10.4 日の勤務 → `split_long_shift` が休息で割り直し、
//! **欠片は元の `source: timecard` を引き継ぐ**ので全部 `timecard`。
//! 見えない側 = 対の無い始業として `next_rest_start` で閉じ、翌日以降は
//! `shifts_from_rest` が組むので `rest`。**割れ目が同じ休息なので 11 列は 1 分も
//! 動かず、ラベルだけ反転する。**
//!
//! 初日だけ 20 分ずれるのも必然で、閉じる側は始業打刻から始まり中の休息が
//! `rest_minus` に出る / 割る側は休息の後から始まる (実データの「余分 1 行」)。
//!
//! **反転 28 行は 6 名に出ていて、6 名とも「月内に対の無い始業があり、反転行が
//! 06-30 まで連続」**という同じ形だった。
//!
//! ## 2. `dtako_events` が両経路で違う (休憩差 32 行 / 拘束暴発 4 行)
//!
//! `dtako_events` (デジタコの区間イベント) だけはオンプレが MariaDB の
//! `dtako_events` 表、GCP が alc/R2 の `KUDGIVT.csv` と**別の置き場から読む**
//! (`src/kintai_http_repo.rs` のモジュール docs の対応表)。拘束は休憩を一切見ない
//! ので、**`restraint` 同値で `break` だけ違う = 休憩イベントそのものの差**。

use rust_ichibanboshi::kosoku::{daily_summary, DaySummary, KosokuParams, ShiftSource};
use serde_json::{json, Value};

const DRIVER: u64 = 9030;
const MONTH: &str = "2026-06";

fn tc(at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "timecard", "state": state})
}

/// `time_card_dtako` 由来の点イベント (運行開始 / 運行終了)。
fn dtako(at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "dtako", "state": state})
}

/// `dtako_events` 由来の区間イベント。
fn span(start: &str, end: &str, state: &str) -> Value {
    json!({"datetime": start, "end_datetime": end, "driver_id": DRIVER, "source": "dtako_events", "state": state})
}

/// 1 日ぶんの運行 (05:44 出庫 → 17:00 帰庫、昼休憩 60 分)。
fn workday(date: &str) -> Vec<Value> {
    vec![
        dtako(&format!("{date} 05:44:00"), "運行開始"),
        span(
            &format!("{date} 12:00:00"),
            &format!("{date} 13:00:00"),
            "休憩",
        ),
        dtako(&format!("{date} 17:00:00"), "運行終了"),
    ]
}

/// 夜の休息 (`date` の 17:00 → 翌 `next` の 05:44)。
fn night_rest(date: &str, next: &str) -> Value {
    span(
        &format!("{date} 17:00:00"),
        &format!("{next} 05:44:00"),
        "休息",
    )
}

const DAYS: [&str; 5] = [
    "2026-06-26",
    "2026-06-27",
    "2026-06-28",
    "2026-06-29",
    "2026-06-30",
];

/// 実データの 1021 と同じ形。月末に**対の無い始業**があり、その相手の終業は
/// **翌月 1 日**にある。始業の直前には 20 分の休息が終わっている。
fn base_rows() -> Vec<Value> {
    let mut rows = vec![
        tc("2026-06-26 06:12:34", "始業"),
        // 始業を跨いで終わる休息。**開始が始業より前**なので `next_rest_start` は
        // 拾わない (勤務を閉じない) が、`rest_spans` には残る
        span("2026-06-25 17:00:00", "2026-06-26 06:32:00", "休息"),
        dtako("2026-06-26 06:32:00", "運行開始"),
        span("2026-06-26 12:00:00", "2026-06-26 13:00:00", "休憩"),
        dtako("2026-06-26 17:00:00", "運行終了"),
    ];
    for date in &DAYS[1..] {
        rows.extend(workday(date));
    }
    for w in DAYS.windows(2) {
        rows.push(night_rest(w[0], w[1]));
    }
    // 月末の夜の休息 (これが無いと閉じる手がかりが消えて勤務ごと落ちる)
    rows.push(night_rest("2026-06-30", "2026-07-01"));
    rows
}

/// オンプレ = MariaDB を `month_range` (`[月初, 翌月 2 日)`) で直読みするので、
/// **翌月 1 日の終業打刻が見える**。
fn rows_with_next_month_punch_out() -> Vec<Value> {
    let mut rows = base_rows();
    rows.push(tc("2026-07-01 14:54:29", "終業"));
    rows
}

/// GCP = Postgres 経由。`kintai_push` は `exact_month_range` (`[月初, 翌月初)`) で
/// 書くので、**7 月がまだ push されていなければ 07-01 の終業打刻は無い**。
fn rows_without_next_month_punch_out() -> Vec<Value> {
    base_rows()
}

fn summarize(rows: &[Value]) -> Vec<DaySummary> {
    daily_summary(rows, MONTH, &KosokuParams::default())
}

/// 11 列 (day_summaries が持つ分列) を突合用に取り出す。
fn minutes(d: &DaySummary) -> Vec<i64> {
    vec![
        d.restraint_minutes,
        d.working_minutes,
        d.break_minutes,
        d.rest_minus_minutes,
        d.statutory_minutes,
        d.within_statutory_overtime_minutes,
        d.overtime_minutes,
        d.legal_holiday_minutes,
        d.night_minutes,
        d.overtime_night_minutes,
        d.legal_holiday_night_minutes,
    ]
}

#[test]
fn a_punch_out_in_the_next_month_flips_shift_source_without_moving_any_minute() {
    let with_punch = summarize(&rows_with_next_month_punch_out());
    let without_punch = summarize(&rows_without_next_month_punch_out());

    assert_eq!(
        with_punch.len(),
        DAYS.len(),
        "終業あり: 割り直して 1 日 1 勤務"
    );
    assert_eq!(without_punch.len(), DAYS.len(), "終業なし: 1 日 1 勤務");

    // 終業が見える側 = 1 本の >24h 勤務を割り直したので、全部が打刻由来のまま
    for d in &with_punch {
        assert_eq!(
            d.source,
            ShiftSource::Timecard,
            "{} も打刻由来を引き継ぐ",
            d.date
        );
    }
    // 終業が見えない側 = 対の無い始業は最初の休息で閉じ、翌日以降は休息由来
    assert_eq!(without_punch[0].source, ShiftSource::Timecard);
    for d in &without_punch[1..] {
        assert_eq!(d.source, ShiftSource::Rest, "{} が休息由来になる", d.date);
    }

    // **初日だけ 20 分ずれる** — 割る側は休息の後から始まり、閉じる側は始業打刻から
    // 始まって中の休息が rest_minus に出る (実データの「余分 1 行」1021|06-21)
    assert_eq!(with_punch[0].start, "2026-06-26 06:32:00");
    assert_eq!(without_punch[0].start, "2026-06-26 06:12:00");
    assert_eq!(with_punch[0].rest_minus_minutes, 0);
    assert_eq!(without_punch[0].rest_minus_minutes, 20);
    // 初日も拘束・実働・休憩は動かない
    assert_eq!(
        with_punch[0].restraint_minutes,
        without_punch[0].restraint_minutes
    );
    assert_eq!(
        with_punch[0].working_minutes,
        without_punch[0].working_minutes
    );
    assert_eq!(with_punch[0].break_minutes, without_punch[0].break_minutes);

    // **2 日目以降は 11 列が 1 つも動かない** — 実データの反転 28 行がこの形
    for (a, b) in with_punch[1..].iter().zip(without_punch[1..].iter()) {
        assert_eq!(a.date, b.date);
        assert_eq!(a.start, b.start, "{} の始業", a.date);
        assert_eq!(minutes(a), minutes(b), "{} の 11 列", a.date);
    }
}

#[test]
fn one_extra_break_event_moves_break_and_working_only() {
    let base = rows_with_next_month_punch_out();
    let mut extra = base.clone();
    // 片方の経路にだけ在る 25 分の休憩 (他の休憩と重ならない位置)
    extra.push(span("2026-06-28 15:00:00", "2026-06-28 15:25:00", "休憩"));

    let a = summarize(&base);
    let b = summarize(&extra);
    assert_eq!(a.len(), b.len());
    let (a, b) = (
        a.iter().find(|d| d.date == "2026-06-28").unwrap(),
        b.iter().find(|d| d.date == "2026-06-28").unwrap(),
    );
    assert_eq!(a.restraint_minutes, b.restraint_minutes, "拘束は動かない");
    assert_eq!(b.break_minutes - a.break_minutes, 25);
    assert_eq!(a.working_minutes - b.working_minutes, 25);
}

#[test]
fn losing_the_break_events_zeroes_break_instead_of_adding_lunch() {
    let full = rows_with_next_month_punch_out();
    // R2 の反映漏れで `dtako_events` の休憩が落ちた形。休息まで落とすと勤務が
    // 1 本も組めなくなる (#205-19 の形) ので、ここは休憩だけを落として比べる
    let stripped: Vec<Value> = full
        .iter()
        .filter(|r| r["state"] != "休憩")
        .cloned()
        .collect();
    let a = summarize(&full);
    let b = summarize(&stripped);
    let a0 = a.iter().find(|d| d.date == "2026-06-28").unwrap();
    let b0 = b.iter().find(|d| d.date == "2026-06-28").unwrap();
    assert_eq!(a0.break_minutes, 60, "休憩イベントの 60 分");
    assert_eq!(
        b0.break_minutes, 0,
        "運行開始/運行終了 が残っているので has_operation は true のまま = 昼休憩は入らない"
    );
}
