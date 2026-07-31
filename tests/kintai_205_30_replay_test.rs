//! #205-30: オンプレの**生イベントを現行コードで畳み直す** replay harness。
//!
//! これで確かめたのは 2 つ (乗務員 1021 / 2026-06、生イベント 578 行):
//!
//! 1. **基準は現行コードで再現できる。** `/api/kintai/kosoku-daily` が返した基準 34 行と
//!    **1 行残らず一致**した。オンプレのサービスと手元のコードで `logic_version` が
//!    割れている可能性は、これで潰れている (#205-19 が 1078 / 1071 / 1049 で
//!    やったのと同じ確認)
//! 2. **GCP 側も同じ 1 本の入力から再現できる。** `source` が `timecard` / `dtako`
//!    (= push が運ぶ 2 つ、`kintai_push::PUSHED_SOURCES`) の行のうち
//!    **`2026-07-01` 以降を落とす**と、GCP の 34 行と**1 行残らず一致**した。
//!    落とす境界が `exact_month_range` の上端そのもの = **6 月の fold が
//!    「7 月の push がまだ走っていない Postgres」を読んだ状態**
//!
//! つまり 1021 の 12 行差は `dtako_events` ではなく **打刻の月境界の欠け**だった。
//!
//! ## 回し方 (生イベントは本番の読み取り専用の口から取る。CI では skip される)
//!
//! ```text
//! REPLAY_EVENTS=<events.json> REPLAY_EXPECT=<day_summaries.json> \
//!   REPLAY_DRIVER=1021 cargo test --test kintai_205_30_replay_test -- --nocapture
//! ```
//!
//! - `REPLAY_EVENTS` … `GET /api/kintai/events?month=&driver=` の応答
//!   (`{month, driver, rows}`)
//! - `REPLAY_EXPECT` … 突合先。`baseline_full.py` が吐く
//!   `{"<CD>|<暦日>|<開始時刻>": {shift_source, …11 列}}` の形
//! - `REPLAY_CUT_PUSHED_FROM` … 指定すると `timecard` / `dtako` の行のうち
//!   この日時以降を落とす (GCP 側の再現用)
//!
//! **`REPLAY_EVENTS` か `REPLAY_EXPECT` が無ければ丸ごと skip する。**
//! 生イベントを固定で置くと 143KB が増え、しかも本番データなのでリポジトリに置かない。

use std::collections::BTreeMap;

use rust_ichibanboshi::kosoku::{daily_summary, drop_duplicate_rows, DaySummary, KosokuParams};

/// `day_summaries` が持つ 11 列。`baseline_full.py` の `MINUTE_COLS` と同じ順。
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

const COLS: [&str; 11] = [
    "restraint_minutes",
    "working_minutes",
    "break_minutes",
    "rest_minus_minutes",
    "statutory_minutes",
    "within_statutory_overtime_minutes",
    "overtime_minutes",
    "legal_holiday_minutes",
    "night_minutes",
    "overtime_night_minutes",
    "legal_holiday_night_minutes",
];

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

/// 期待値の 1 行を `(shift_source, 11 列)` に落とす。
fn expected_row(v: &serde_json::Value) -> (String, Vec<i64>) {
    (
        v["shift_source"].as_str().unwrap_or_default().to_string(),
        COLS.iter().map(|c| v[*c].as_i64().unwrap_or(0)).collect(),
    )
}

#[test]
fn onprem_raw_events_replay_to_the_expected_day_summaries() {
    let (Some(events), Some(expect)) = (env("REPLAY_EVENTS"), env("REPLAY_EXPECT")) else {
        eprintln!("REPLAY_EVENTS / REPLAY_EXPECT が無いので skip");
        return;
    };
    let raw: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&events).expect("read REPLAY_EVENTS"))
            .expect("REPLAY_EVENTS は JSON");
    let month = raw["month"].as_str().expect("month").to_string();
    let driver = env("REPLAY_DRIVER").unwrap_or_else(|| {
        raw["driver"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_default()
    });

    let cut = env("REPLAY_CUT_PUSHED_FROM");
    let rows: Vec<serde_json::Value> = raw["rows"]
        .as_array()
        .expect("rows")
        .iter()
        // `kosoku_daily_all` が読む全乗務員版の行の形へ落とす
        // (`kintai_repo::all_row_to_json` — `unko_no` / `vehicle` はキーごと出さない)
        .map(|r| {
            serde_json::json!({
                "datetime": r["datetime"],
                "end_datetime": r["end_datetime"],
                "driver_id": r["driver_id"],
                "source": r["source"],
                "state": r["state"],
            })
        })
        // push が運ぶ source だけを境界で切る (GCP 側の再現)
        .filter(|r| {
            let Some(from) = cut.as_deref() else {
                return true;
            };
            let pushed = matches!(r["source"].as_str(), Some("timecard") | Some("dtako"));
            !(pushed && r["datetime"].as_str().unwrap_or_default() >= from)
        })
        .collect();

    let (rows, _dropped) = drop_duplicate_rows(rows);
    let days = daily_summary(&rows, &month, &KosokuParams::default());

    let expect: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(&std::fs::read_to_string(&expect).expect("read REPLAY_EXPECT"))
            .expect("REPLAY_EXPECT は JSON");
    // 期待値は全乗務員ぶん来るので、対象の乗務員だけに絞る
    let expect: BTreeMap<String, (String, Vec<i64>)> = expect
        .iter()
        .filter(|(k, _)| k.split('|').next() == Some(driver.as_str()))
        .map(|(k, v)| (k.clone(), expected_row(v)))
        .collect();

    let got: BTreeMap<String, (String, Vec<i64>)> = days
        .iter()
        .map(|d| {
            (
                format!("{driver}|{}|{}", d.date, d.start),
                (
                    serde_json::to_value(d.source)
                        .expect("source")
                        .as_str()
                        .expect("str")
                        .to_string(),
                    minutes(d),
                ),
            )
        })
        .collect();

    eprintln!(
        "replay: rows={} 畳んだ行={} 期待={} cut={:?}",
        rows.len(),
        got.len(),
        expect.len(),
        cut
    );
    assert!(!expect.is_empty(), "期待値に乗務員 {driver} の行が無い");
    assert_eq!(got, expect, "畳み直した日別サマリが期待値と違う");
}
