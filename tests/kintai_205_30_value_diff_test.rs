//! #205-30: 共通キー 3,229 件のうち **値が違う 65 行**の究明 (2026-06)。
//!
//! オンプレ経路 (MariaDB 直読み) と GCP 経路 (R2/Postgres) は**同じ
//! `daily_summary`** を通る。値が割れるのは入力が割れているからで、割れうる
//! 入力は 1 つしかない — **`source = "dtako_events"` (デジタコの区間イベント。
//! 休憩・休息・運転…)**。打刻 (`timecard`) と確定イベント (`dtako`) は同じ
//! MariaDB から push された同じ行だが、`dtako_events` だけはオンプレが MariaDB の
//! `dtako_events` 表、GCP が alc/R2 の `KUDGIVT.csv` と**別の置き場から読む**
//! (`src/kintai_http_repo.rs` のモジュール docs の対応表)。
//!
//! ここでは 65 行に現れた 3 つの形を、合成イベントで**入力差 → 出力差**として
//! 再現する。どれも `kosoku.rs` の不具合ではなく、入力の欠け/増えがそのまま出る。
//!
//! 1. `missing_rest_flips_shift_source_without_moving_any_minute`
//!    — 休息 (`dtako_events` の `休息`) が欠けると、対の無い始業が閉じられず
//!    **1 本の >24h 勤務**になり、`split_long_shift` / `split_by_run_gaps` が
//!    暦日ごとに割り直す。割れた欠片は元の `source: timecard` を引き継ぐので、
//!    **11 列すべて同値のまま `shift_source` だけ `timecard` → `rest` が反転**する。
//!    実データの (b) 28 行 (乗務員 1021 だけで 10 行) と (c) 20 分ずれ 1 行の形
//! 2. `one_extra_break_event_moves_break_and_working_only`
//!    — 休憩イベントが 1 件増減すると `break` と `working` だけが相殺で動き、
//!    `restraint` は動かない。実データの (a) 32 行の形
//! 3. `losing_every_dtako_events_row_zeroes_break_instead_of_adding_lunch`
//!    — `dtako_events` の休憩が欠けても `dtako` の 運行開始/運行終了 が残っていれば
//!    `has_operation` は true のままなので、**昼休憩は入らず休憩が 0 になる**。
//!    実データで GCP 側の `break_minutes` が 0 になった行 (1130 / 1195 / 1676)
//!    が「昼休憩 60 分」ではなく 0 だった理由

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

/// 1 日ぶんの運行 (運行開始 05:44 → 運行終了 17:00、昼休憩 60 分)。
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
    "2026-06-21",
    "2026-06-22",
    "2026-06-23",
    "2026-06-24",
    "2026-06-25",
];

/// 両変種に共通の土台。**対になる終業が無い始業が 1 本だけ**あり (実データの
/// 乗務員 1021 は月の最後の打刻が 06-21 06:12:34 の始業だった)、その直前に
/// 20 分の休息が終わっている。
fn base_rows() -> Vec<Value> {
    let mut rows = vec![
        tc("2026-06-21 06:12:34", "始業"),
        // 始業を跨いで終わる休息。**開始が始業より前**なので `next_rest_start` は
        // 拾わない (勤務を閉じない) が、`rest_spans` には残る
        span("2026-06-21 06:12:00", "2026-06-21 06:32:00", "休息"),
        // 初日は休息明けの 06:32 から走り出す
        dtako("2026-06-21 06:32:00", "運行開始"),
        span("2026-06-21 12:00:00", "2026-06-21 13:00:00", "休憩"),
        dtako("2026-06-21 17:00:00", "運行終了"),
        // 月末側で必ず 1 本は休息がある (これが無いと勤務そのものが落ちる)
        night_rest("2026-06-25", "2026-06-26"),
    ];
    for date in &DAYS[1..] {
        rows.extend(workday(date));
    }
    rows
}

/// GCP 側の入力: 夜の休息が**揃っている**。
fn rows_with_night_rests() -> Vec<Value> {
    let mut rows = base_rows();
    for w in DAYS.windows(2) {
        rows.push(night_rest(w[0], w[1]));
    }
    rows
}

/// オンプレ側の入力: 途中の夜の休息が**欠けている** (`dtako_events` の反映漏れ)。
fn rows_without_night_rests() -> Vec<Value> {
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
fn missing_rest_flips_shift_source_without_moving_any_minute() {
    let with_rest = summarize(&rows_with_night_rests());
    let without_rest = summarize(&rows_without_night_rests());

    assert_eq!(with_rest.len(), DAYS.len(), "休息あり: 1 日 1 勤務");
    assert_eq!(
        without_rest.len(),
        DAYS.len(),
        "休息なし: 割り直して同じ本数"
    );

    // 休息あり = 始業は最初の休息で閉じ、残りは休息由来 (実データの GCP 側)
    assert_eq!(with_rest[0].source, ShiftSource::Timecard);
    for d in &with_rest[1..] {
        assert_eq!(d.source, ShiftSource::Rest, "{} が休息由来になる", d.date);
    }
    // 休息なし = 1 本の >24h 勤務を割り直したので、全部が打刻由来のまま
    for d in &without_rest {
        assert_eq!(
            d.source,
            ShiftSource::Timecard,
            "{} も打刻由来を引き継ぐ",
            d.date
        );
    }

    // **初日だけ 20 分ずれる** — 割らないと始業打刻から始まり中の休息が
    // rest_minus に出る / 割ると休息の後から始まる (実データの (c) 1021 06-21)
    assert_eq!(with_rest[0].start, "2026-06-21 06:12:00");
    assert_eq!(without_rest[0].start, "2026-06-21 06:32:00");
    assert_eq!(with_rest[0].rest_minus_minutes, 20);
    assert_eq!(without_rest[0].rest_minus_minutes, 0);

    // 初日は rest_minus (= 上の 20 分) だけが違い、他の 10 列は同値
    assert_eq!(
        with_rest[0].restraint_minutes,
        without_rest[0].restraint_minutes
    );
    assert_eq!(
        with_rest[0].working_minutes,
        without_rest[0].working_minutes
    );
    assert_eq!(with_rest[0].break_minutes, without_rest[0].break_minutes);

    // **2 日目以降は 11 列が 1 つも動かない** — 実データの (b) 28 行
    // (1021 の 06-22〜06-30 は shift_source だけの差) がこの形
    for (a, b) in with_rest[1..].iter().zip(without_rest[1..].iter()) {
        assert_eq!(a.date, b.date);
        assert_eq!(a.start, b.start, "{} の始業", a.date);
        assert_eq!(minutes(a), minutes(b), "{} の 11 列", a.date);
    }
}

#[test]
fn one_extra_break_event_moves_break_and_working_only() {
    let base = rows_with_night_rests();
    let mut extra = base.clone();
    // 片方の経路にだけ在る 25 分の休憩 (他の休憩と重ならない位置)
    extra.push(span("2026-06-23 15:00:00", "2026-06-23 15:25:00", "休憩"));

    let a = summarize(&base);
    let b = summarize(&extra);
    assert_eq!(a.len(), b.len());
    let (a, b) = (
        a.iter().find(|d| d.date == "2026-06-23").unwrap(),
        b.iter().find(|d| d.date == "2026-06-23").unwrap(),
    );
    assert_eq!(a.restraint_minutes, b.restraint_minutes, "拘束は動かない");
    assert_eq!(b.break_minutes - a.break_minutes, 25);
    assert_eq!(a.working_minutes - b.working_minutes, 25);
}

#[test]
fn losing_every_dtako_events_row_zeroes_break_instead_of_adding_lunch() {
    let full = rows_with_night_rests();
    // R2 の反映漏れで `dtako_events` の休憩が落ちた形。休息まで落とすと勤務が
    // 1 本も組めなくなる (#205-19 の形) ので、ここは休憩だけを落として比べる
    let stripped: Vec<Value> = full
        .iter()
        .filter(|r| r["state"] != "休憩")
        .cloned()
        .collect();
    let a = summarize(&full);
    let b = summarize(&stripped);
    let a0 = a.iter().find(|d| d.date == "2026-06-21").unwrap();
    let b0 = b.iter().find(|d| d.date == "2026-06-21").unwrap();
    assert_eq!(a0.break_minutes, 60, "休憩イベントの 60 分");
    assert_eq!(
        b0.break_minutes, 0,
        "運行開始/運行終了 が残っているので has_operation は true のまま = 昼休憩は入らない"
    );
}
