//! #205-19: 142 行差 (2026-06、乗務員CD 0 を除く 132 名で オンプレ 3,372 行 /
//! GCP 3,230 行) の究明。**乗務員 1078 の実データで再現し、原因の形を特定した。**
//!
//! ## 手順
//!
//! `http://ohishi-data:3100/api/kintai/events?month=2026-06&driver=1078` (オンプレ、
//! 読み取り専用・単一乗務員) の生イベント 535 行を [`daily_summary`] に通すと、
//! **オンプレ基準 (`onprem_day_summaries_2026-06.json`) の 26 行と 1 行残らずバイト
//! 単位で一致した** (harness の正しさをまず実測で確認、乗務員 1071 / 1049 でも同様)。
//!
//! ## 反証の追加 (#227 の射程を広げて確認)
//!
//! `source = "dtako"` (打刻由来の確定 運行開始/運行終了/休息、点イベント) を**丸ごと
//! 落としても 26 行のまま値も不変**だった。[`tests/kintai_fold_227_diff_test.rs`] は
//! 「休息 1 件だけ」で run_head_minutes 以外は不変と示したが、今回は乗務員 1078 の
//! 月内の `dtako` 該当 59 行 (休息 51 / 運行開始 4 / 運行終了 4) を丸ごと落としても
//! 同じ結論だった。理由はコード自体に書いてある — [`rest_spans`] は
//! `dtako_events` 由来の休息だけを使う (`dtako` 側は開始・終了が別行の点イベントで
//! 対にする根拠が無い、[`kosoku::rest_spans`] のコメント参照)。
//!
//! ## 見つかった形: **dtako_events が無いと、その区間の「打刻の無い勤務」は
//! 消えるだけで代わりが立たない**
//!
//! 実データで乗務員 1078 の `dtako_events` を月末 (2026-06-24 以降) だけ落とすと、
//! **その日以降の 7 行がそっくり消え、それより前の 19 行は 1 バイトも変わらない**。
//! 消えた 7 行は全部 `shift_source = "rest"` (打刻の無い勤務)。乗務員 1071 / 1049
//! でも同じ実験で「対象区間の rest 系の行が丸ごと消える」形が再現した。
//!
//! 理由は 2 箇所:
//!
//! 1. [`shifts_from_rest`] (休息由来の勤務) は前後を `dtako_events` の休息区間
//!    ([`rest_spans`]) で挟んで組む。次の休息が無ければ `(None, None) => continue`
//!    (line 712 あたり) で**そのまま捨てる** — 打刻が無い区間はこれ以外に
//!    閉じる手段が無い
//! 2. [`shifts_from_timecard`] の**末尾の未対始業** (次の始業も終業も来ないまま
//!    月末に達した勤務) も `next_rest_start` (同じく `dtako_events` の休息限定)
//!    で閉じる。無ければ**その勤務も丸ごと落ちる**
//!
//! **どちらも `kosoku.rs` の不具合ではない** — 閉じる証拠が無い勤務を捏造せず
//! 捨てるのは意図通りの安全側の判断 (`fold_days` の「1 行のために全部を失わない」
//! と同じ思想)。**GCP の `dtako_events` は alc/R2 から直接引く**(push を経由しない)
//! ので、**R2 の反映がまだ追いついていない期間があると、その期間に打刻を持たない
//! 勤務が黙って抜け落ちる** — 保存される 3 表にもエラーにも出ない。
//!
//! ## 142 行差との整合性
//!
//! 特定の乗務員に偏在する (91 名中 8 名) のは、たまたま「その反映漏れの期間に
//! 打刻の無い勤務があった」乗務員だけが影響を受けるため — 全乗務員に一律には
//! 出ない。1078 (7 行) は月末に rest 系の勤務が集中しており、この形とよく合う。
//!
//! **どの日から `dtako_events` が反映漏れだったか (R2 の実際の遅れ幅) は、この
//! テストの範囲では特定できない** — 3 乗務員それぞれで「n 行消える cutoff」が
//! 微妙にずれる (1078: 06-24 以降で 7 行、1071: 06-29 以降で 4 行、1049: 06-28
//! 以降で 3 行) ため、月ゲート撃ち直しの前後比較か alc 側のログでないと
//! 「いつ」は確定できない。
//!
//! ## 結論 / 次の一手
//!
//! - `src/kosoku.rs` / `src/kintai_fold.rs` に**修正すべきロジック不具合は無い**
//!   と判断する — 閉じられない勤務を捨てる挙動は意図通り
//! - 根本原因は **alc/R2 の反映遅延中に GCP 側の fold が走った** ことで、
//!   これは #205-13 / #205-16 / #205-17 が扱っている「月ゲート・fold 中の
//!   warnings」と同じ問題系。**月ゲートが正しく効けば、データが追いついた後の
//!   再 fold で自然に解消する**はず — 月ゲートが実際にこの trailing gap 型の
//!   欠落も warnings で検知できているかは要確認 ([質問] で申し送り)

use rust_ichibanboshi::kosoku::{daily_summary, DaySummary, KosokuParams};
use serde_json::{json, Value};

const DRIVER: u64 = 9019;
const MONTH: &str = "2026-06";

fn tc(at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "timecard", "state": state, "unko_no": null})
}

fn rest_span(start: &str, end: &str) -> Value {
    json!({"datetime": start, "end_datetime": end, "driver_id": DRIVER, "source": "dtako_events", "state": "休息", "unko_no": null})
}

/// 早い時期の完全に対になった打刻勤務 (control、影響を受けない)。
fn paired_timecard_shift() -> Vec<Value> {
    vec![tc("2026-06-05 08:00:00", "始業"), tc("2026-06-05 17:00:00", "終業")]
}

/// 月末の「打刻の無い勤務」(休息 2 本に挟まれるだけの rest 系)。
fn tail_rest_shift() -> Vec<Value> {
    vec![
        rest_span("2026-06-25 20:00:00", "2026-06-26 05:00:00"),
        rest_span("2026-06-26 20:00:00", "2026-06-27 05:00:00"),
    ]
}

/// 月末の「対の無い始業」(次の休息で閉じるしかない勤務)。
fn tail_unpaired_start_shift() -> Vec<Value> {
    vec![
        tc("2026-06-29 06:00:00", "始業"),
        rest_span("2026-06-29 20:00:00", "2026-06-30 05:00:00"),
    ]
}

fn find<'a>(days: &'a [DaySummary], date: &str) -> Option<&'a DaySummary> {
    days.iter().find(|d| d.date == date)
}

/// **control**: 何も欠けていなければ 3 勤務とも揃う。
#[test]
fn all_three_shapes_present_when_dtako_events_is_complete() {
    let mut rows = paired_timecard_shift();
    rows.extend(tail_rest_shift());
    rows.extend(tail_unpaired_start_shift());

    let days = daily_summary(&rows, MONTH, &KosokuParams::default());
    assert_eq!(days.len(), 3, "{days:#?}");
    assert!(find(&days, "2026-06-05").is_some(), "対になった打刻勤務");
    assert!(find(&days, "2026-06-26").is_some(), "休息に挟まれた rest 勤務");
    assert!(find(&days, "2026-06-29").is_some(), "対の無い始業");
}

/// **本題**: 月末の休息区間 (`dtako_events`) だけが欠けると (R2 の反映遅延を
/// 模す)、対になった打刻勤務は無傷のまま、**rest 系と対の無い始業だけが
/// エラーも警告も無く消える** — 実データ (乗務員 1078/1071/1049, 2026-06) で
/// 見つかった形そのもの。
#[test]
fn tail_rest_gap_silently_drops_the_unclosable_shifts() {
    let mut rows = paired_timecard_shift();
    // tail_rest_shift() の 2 本目の休息と tail_unpaired_start_shift() の休息を
    // 落とす — 「06-26 以降の dtako_events がまだ届いていない」を模す
    rows.push(rest_span("2026-06-25 20:00:00", "2026-06-26 05:00:00"));
    rows.push(tc("2026-06-29 06:00:00", "始業"));
    // ここより後の dtako_events (2 本の休息) が欠けている

    let days = daily_summary(&rows, MONTH, &KosokuParams::default());
    assert_eq!(
        days.len(),
        1,
        "残るのは対になった打刻勤務だけのはず: {days:#?}"
    );
    assert!(find(&days, "2026-06-05").is_some(), "打刻勤務は無傷");
    assert!(
        find(&days, "2026-06-26").is_none(),
        "閉じる休息が無い rest 勤務は黙って消える"
    );
    assert!(
        find(&days, "2026-06-29").is_none(),
        "閉じる休息が無い対の無い始業も黙って消える"
    );
}
