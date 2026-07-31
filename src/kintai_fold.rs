//! `kosoku.rs` の出力を `kintai` スキーマへ畳んで保存する (Refs #205 実装計画 05)。
//!
//! **`kosoku.rs` を 1 行も変えない。** 畳む処理を写すと 2 実装になり、改修のたびに
//! 両方を直すことになる (#205 のリスク欄)。ここがやるのは
//! [`crate::kosoku::daily_summary`] の戻り値を 3 表の行に**写すだけ**。
//!
//! | 表 | 1 行の単位 |
//! |---|---|
//! | `kintai.shifts` | 勤務 1 本 |
//! | `kintai.day_summaries` | 勤務 1 本 (002 で PK に `shift_start_at` を足した) |
//! | `kintai.day_parts` | 勤務 × 乗った暦日 |
//!
//! ## 読み出し経路と同じ手順で計算する
//!
//! `/api/kintai/kosoku-daily` の全乗務員経路 (`routes/kintai.rs` の
//! `kosoku_daily_all`) と**同じ順序**で呼ぶ — 違う手順で畳むと、画面と保存値が
//! 食い違ったときに原因が追えない。
//!
//! 1. `fetch_all_events_between` (対象月を**全乗務員まとめて 1 回**読む)
//! 2. `split_by_driver` (`daily_summary` は乗務員を知らない純粋関数なので先に分ける)
//! 3. `drop_duplicate_rows` (取り込みが 2 回走った重複を落とす)
//! 4. `daily_summary`
//!
//! **1 と 2 を「乗務員ごとに 1 回読む」に置き換えない。** GCP の HTTP 実装
//! ([`crate::kintai_http_repo`]) では 1 乗務員 1 往復が `rust-alc-api` への
//! 1 往復 (裏で R2 の GET 群) になる。95 名 × 2 か月で約 190 往復、しかも乗務員の
//! 列挙で全量をもう 1 周読んでいた。読みは月 1 回に畳み、分けるのは in-process でやる。
//!
//! **フェリー控除 (`apply_ferry_minus`) は呼ばない。** あれが埋めるのは
//! `ferry_minus_minutes` だけで、これは 3 表のどこにも列が無い (紙との差を説明する
//! ためだけの値で、拘束にも実働にも入っていない)。呼んでも保存値は 1 つも変わらず、
//! 乗務員ごとに 1 往復増えるだけになる。
//!
//! ## 指紋 — 何が変わったら再計算するか
//!
//! ```text
//! logic_version = sha256(KINTAI_OUTPUT_SHA + "|" + KosokuParams)[..16]
//! fingerprint   = sha256(
//!     logic_version               // コードと設定を畳んだ 16 桁 hex
//!   + "|" + 乗務員CD + 対象月
//!   + "|" + その乗務員の月の生行 (正規化して並べたもの)
//! )
//! ```
//!
//! - **`KosokuParams` を材料に入れるのが必須。** `restraint_rounding` などは TOML で
//!   再ビルド無しに変えられて出力を変える。入れないと切り替えても全単位が stale に
//!   ならず、古い集計が永久にスキップされる (#205 のリスク欄)
//! - **`KINTAI_OUTPUT_SHA` は手で上げる版番号ではない。** `kosoku.rs` を 1 バイト
//!   直せば必ず変わるので「上げ忘れ」が原理的に存在しない (`build.rs`)
//! - 材料の生行は**行まるごと**を畳む。`daily_summary` が読むのは 4 列だけだが、
//!   `drop_duplicate_rows` は行の完全一致で重複を判定するので、列が増えれば
//!   結果が変わり得る。**広い方に倒す** — 余分に再計算するのは安全側で、
//!   取りこぼしだけが危ない
//!
//! ### 材料の行は**全乗務員版の形**
//!
//! 全乗務員版の行は `unko_no` / `vehicle` を**キーごと持たない**
//! (`kintai_repo::all_row_to_json` / `kintai_http_repo::event_to_all_json`)。
//! 単一乗務員版 (`/api/kintai/events`) は持つので、**同じ打刻でも行 JSON が経路で
//! 違い、指紋も違う値になる。**
//!
//! 畳んだ値そのものは変わらない。[`crate::kosoku::parse_events`] が読むのは
//! `datetime` / `end_datetime` / `source` / `state` の 4 キーだけで、
//! [`crate::kosoku::Event`] に運行NO の場所が無い。効くのは
//! [`crate::kosoku::drop_duplicate_rows`] の判定材料としてだけ —
//! 「同時刻・同イベントで運行NO だけ違う 2 行」(実測 1526 の `…011` / `…012`) が、
//! 全乗務員版では区別が付かず 1 行に潰れる。潰れても拘束・実働・深夜は動かない
//! (休憩も休息も `merge_intervals` / `windows(2)` で畳むので重複に強い)。
//!
//! **だから fold は `driver` 指定の有無にかかわらず常に全乗務員版で読む。**
//! 経路によって形を変えると、同じ (乗務員, 月) の指紋が読み方で割れ、
//! 単一指定の再計算と月次バッチが**互いに相手の書いた行を stale と見て毎回書き直す**。
//! 指紋を跨いで比べてよいのは「同じ読み方をした指紋」だけ。
//!
//! ## `logic_version` は保存行に焼く「コード + 設定」の印
//!
//! `KINTAI_OUTPUT_SHA` **単体ではない** ([`logic_version`])。単体にすると TOML の
//! 閾値・丸め方を変えたときに保存行の `logic_version` が変わらず、`SELECT DISTINCT
//! logic_version` では設定変更由来の stale が捕まらない — 指紋には入っているので
//! 再計算そのものは走るが、「走らせるべきか」を**読むだけで判る**手段が消える。
//!
//! 指紋は乗務員ごと・月ごとに違うので、stale かどうかを聞くには全単位を数える
//! ことになる。`logic_version` は全単位で同じ値なので 1 クエリで済む
//! ([`stale_state`])。`CHAR(16)` のまま収まるよう先頭 16 桁で切る。
//!
//! ## 単位は (乗務員, 月)
//!
//! issue は「指紋が変わった (乗務員, 日) だけ再計算」と書いているが、**勤務は日を
//! 跨ぐので日を単独では計算できない** (issue 自身も「再計算の対象は差分日 ± 2 日」と
//! している)。月単位のバッチではそれを最後まで広げた形 = (乗務員, 月) が素直で、
//! 差分日 ± 2 日を必ず含む。指紋は行ごとに持たせるので、
//! **書き込みは「その乗務員の月が変わったときだけ」**に絞られる。

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use sha2::{Digest, Sha256};

use crate::kintai_push::{
    jst_day_bounds, KintaiPgStore, KintaiPushError, DATETIME_FORMAT, PUSHED_SOURCES,
};
use crate::kintai_repo::{exact_month_range, month_range, DynKintaiEventsRepo};
use crate::kosoku::{daily_summary, drop_duplicate_rows, DaySummary, KosokuParams, ShiftSource};

/// `build.rs` が焼き込む「出力に効くコード」の内容ハッシュ (16 桁 hex)。
///
/// これ**単体は保存しない**。TOML で変えられる [`KosokuParams`] を畳んだ
/// [`logic_version`] が保存行に入る値。
pub fn output_sha() -> &'static str {
    env!("KINTAI_OUTPUT_SHA")
}

/// 保存行に焼く「コード + 設定」の印 (16 桁 hex)。
///
/// `shifts.logic_version` / `day_summaries.logic_version` (`CHAR(16)`) にそのまま
/// 入り、[`fingerprint`] の材料の先頭でもある。**両方が同じ 1 つの定義から来る**ので、
/// 「指紋は変わったのに `logic_version` は据え置き」が作れない。
///
/// 材料を `KINTAI_OUTPUT_SHA` 単体にしない理由はモジュール docs 参照。
pub fn logic_version(params: &KosokuParams) -> String {
    let mut h = Sha256::new();
    h.update(output_sha().as_bytes());
    h.update(b"|");
    h.update(format!("{params:?}").as_bytes());
    format!("{:x}", h.finalize())[..LOGIC_VERSION_LEN].to_string()
}

/// `logic_version` の桁数。DDL の `CHAR(16)` に合わせる。
const LOGIC_VERSION_LEN: usize = 16;

/// `shifts` 1 行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShiftRow {
    pub driver_cd: i64,
    pub start_at: NaiveDateTime,
    pub end_at: NaiveDateTime,
    pub shift_source: &'static str,
}

/// `day_summaries` 1 行。列は 002 適用後の形。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaySummaryRow {
    pub driver_cd: i64,
    pub date: NaiveDate,
    pub shift_start_at: NaiveDateTime,
    pub shift_source: &'static str,
    pub restraint_minutes: i64,
    pub working_minutes: i64,
    pub break_minutes: i64,
    pub rest_minus_minutes: i64,
    pub statutory_minutes: i64,
    pub within_statutory_overtime_minutes: i64,
    pub overtime_minutes: i64,
    pub legal_holiday_minutes: i64,
    pub night_minutes: i64,
    pub overtime_night_minutes: i64,
    pub legal_holiday_night_minutes: i64,
}

/// `day_parts` 1 行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayPartRow {
    pub driver_cd: i64,
    pub shift_start_at: NaiveDateTime,
    pub date: NaiveDate,
    pub restraint_minutes: i64,
    pub working_minutes: i64,
    pub night_minutes: i64,
}

/// 1 乗務員 1 か月ぶんの畳んだ結果。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FoldUnit {
    pub driver_cd: i64,
    pub shifts: Vec<ShiftRow>,
    pub day_summaries: Vec<DaySummaryRow>,
    pub day_parts: Vec<DayPartRow>,
    /// 写せなかった勤務の理由。**黙って落とさない**。
    pub skipped: Vec<SkipReason>,
}

impl FoldUnit {
    /// 3 表のどれにも行が立たなかったか。
    ///
    /// 対象月に畳める勤務が 1 本も無い乗務員がこれになる。打刻も休息も無い月、
    /// 退職して以降の月、勤務が全部 [`SkipReason`] で落ちた月。
    pub fn is_empty(&self) -> bool {
        self.shifts.is_empty() && self.day_summaries.is_empty() && self.day_parts.is_empty()
    }
}

/// 3 表に写せなかったもの。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub enum SkipReason {
    /// `start >= end` に潰れた勤務。`CHECK (end_at > start_at)` を満たせない。
    ///
    /// 24 時間超の勤務を休息や運行の継ぎ目で切り直すとき、末尾の断片だけが
    /// 「1 分未満」の検査を通らずに残る経路がある (`split_long_shift` /
    /// `split_by_run_gaps` の末尾)。分に丸めると始点と終点が同じ分に落ちる。
    DegenerateShift { start: String, end: String },
    /// 勤務の始業日より**前**の暦日を指す `day_parts`。
    ///
    /// `run_head` (直前の運行開始 → 始業、最大 8 時間前) だけが乗った暦日で起きる。
    /// `CHECK (date >= (shift_start_at AT TIME ZONE 'Asia/Tokyo')::date)` を満たせない。
    /// この行は拘束も実働も深夜も 0 なので、落としても保存値は変わらない。
    PartBeforeShift { shift_start: String, date: String },
}

impl SkipReason {
    /// **設計が処理を決めてある既知のデータ形か。**
    ///
    /// 既知の形は「想定外」に数えない ([`FoldReport::has_unexpected`]) —
    /// 実データに常時 1 件混ざる零長勤務 (2026-06 の乗務員 1518) で毎回
    /// 非 0 終了すると、本当に想定外が来たときの合図が埋もれる。
    /// **数えないだけで表示は消さない** (`main.rs` の `print_fold` /
    /// 応答の `skipped`) — 件数が急に増えたら人が気付ける形は残す。
    ///
    /// `match` を網羅で書くのは、variant を足した人にここでの分類を必ず
    /// 迫るため。既定で「既知」に落ちると、新しい壊れ方が黙って消える。
    pub fn is_known(&self) -> bool {
        match self {
            SkipReason::DegenerateShift { .. } | SkipReason::PartBeforeShift { .. } => true,
        }
    }
}

fn shift_source_str(s: ShiftSource) -> &'static str {
    match s {
        ShiftSource::Timecard => "timecard",
        ShiftSource::Rest => "rest",
    }
}

fn parse_dt(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, DATETIME_FORMAT).ok()
}

fn parse_date(s: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
}

/// [`DaySummary`] の並びを 3 表の行へ写す。
///
/// **DB の CHECK 制約を満たせない行はここで落とす。** 送ってしまうとその乗務員の
/// 月が丸ごと巻き戻るので、1 行のために全部を失わない。
pub fn fold_days(driver_cd: i64, days: &[DaySummary]) -> FoldUnit {
    let mut unit = FoldUnit {
        driver_cd,
        ..Default::default()
    };
    for d in days {
        let (Some(start_at), Some(end_at), Some(date)) =
            (parse_dt(&d.start), parse_dt(&d.end), parse_date(&d.date))
        else {
            continue;
        };
        if end_at <= start_at {
            unit.skipped.push(SkipReason::DegenerateShift {
                start: d.start.clone(),
                end: d.end.clone(),
            });
            continue;
        }
        let shift_source = shift_source_str(d.source);
        unit.shifts.push(ShiftRow {
            driver_cd,
            start_at,
            end_at,
            shift_source,
        });
        unit.day_summaries.push(DaySummaryRow {
            driver_cd,
            date,
            shift_start_at: start_at,
            shift_source,
            restraint_minutes: d.restraint_minutes,
            working_minutes: d.working_minutes,
            break_minutes: d.break_minutes,
            rest_minus_minutes: d.rest_minus_minutes,
            statutory_minutes: d.statutory_minutes,
            within_statutory_overtime_minutes: d.within_statutory_overtime_minutes,
            overtime_minutes: d.overtime_minutes,
            legal_holiday_minutes: d.legal_holiday_minutes,
            night_minutes: d.night_minutes,
            overtime_night_minutes: d.overtime_night_minutes,
            legal_holiday_night_minutes: d.legal_holiday_night_minutes,
        });
        for p in &d.parts {
            let Some(part_date) = parse_date(&p.date) else {
                continue;
            };
            // 拘束も実働も深夜も 0 の暦日は保存しない。`run_head` や
            // `lunch_overlap` だけが乗った日がこれで、3 表に列が無いので
            // 保存しても何も分からない
            if p.restraint_minutes == 0 && p.working_minutes == 0 && p.night_minutes == 0 {
                continue;
            }
            if part_date < start_at.date() {
                unit.skipped.push(SkipReason::PartBeforeShift {
                    shift_start: d.start.clone(),
                    date: p.date.clone(),
                });
                continue;
            }
            unit.day_parts.push(DayPartRow {
                driver_cd,
                shift_start_at: start_at,
                date: part_date,
                restraint_minutes: p.restraint_minutes,
                working_minutes: p.working_minutes,
                night_minutes: p.night_minutes,
            });
        }
    }
    unit
}

/// 指紋。材料はモジュール docs のとおり。
pub fn fingerprint(
    driver_cd: i64,
    month: &str,
    params: &KosokuParams,
    rows: &[serde_json::Value],
) -> String {
    let mut lines: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
    lines.sort();
    let mut h = Sha256::new();
    h.update(logic_version(params).as_bytes());
    h.update(b"|");
    h.update(format!("{driver_cd}|{month}").as_bytes());
    h.update(b"|");
    h.update(lines.join("\n").as_bytes());
    format!("{:x}", h.finalize())
}

/// 1 乗務員 1 か月を畳む。読み出し経路と同じ手順 (モジュール docs 参照)。
pub fn fold_driver_month(
    driver_cd: i64,
    month: &str,
    params: &KosokuParams,
    rows: Vec<serde_json::Value>,
) -> (FoldUnit, String) {
    let fp = fingerprint(driver_cd, month, params, &rows);
    let (rows, _duplicates) = drop_duplicate_rows(rows);
    let days = daily_summary(&rows, month, params);
    (fold_days(driver_cd, &days), fp)
}

// ── 保存 ──────────────────────────────────────────────────────────────────

/// 保存済みの姿。[`FoldUnit`] と突き合わせて「書く必要があるか」を決める。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredState {
    /// その乗務員の月に載っている `fingerprint` の集合。
    pub fingerprints: Vec<String>,
    pub shifts: i64,
    pub day_summaries: i64,
    pub day_parts: i64,
}

impl StoredState {
    /// 3 表に 1 行も載っていないか。指紋も当然 1 つも無い。
    pub fn is_empty(&self) -> bool {
        self.fingerprints.is_empty()
            && self.shifts == 0
            && self.day_summaries == 0
            && self.day_parts == 0
    }

    /// 書かなくてよいか。
    ///
    /// 指紋が 1 種類でそれが今回の指紋と同じ、**かつ** 3 表の行数が今回と同じとき
    /// だけスキップする。行数まで見るのは、前回が途中で落ちて一部だけ書けている
    /// 状態を「同じ指紋だから」で見逃さないため。
    ///
    /// **空 = 空は current。** 畳める勤務が 1 本も無い乗務員は書くものが無いので
    /// 指紋も載らず、指紋の一致だけで判定すると毎回 stale になる。毎回
    /// `drivers_written` に乗って [`FoldReport::wrote_anything`] が誤検知し、
    /// `sync` が「何か書いた」と報告し続ける。
    pub fn is_current(&self, unit: &FoldUnit, fp: &str) -> bool {
        if self.is_empty() && unit.is_empty() {
            return true;
        }
        self.fingerprints.len() == 1
            && self.fingerprints[0] == fp
            && self.shifts == unit.shifts.len() as i64
            && self.day_summaries == unit.day_summaries.len() as i64
            && self.day_parts == unit.day_parts.len() as i64
    }
}

/// 再計算 1 回の集計。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct FoldReport {
    pub drivers: usize,
    pub drivers_written: usize,
    pub drivers_unchanged: usize,
    pub shifts: usize,
    pub day_summaries: usize,
    pub day_parts: usize,
    pub skipped: Vec<SkipReason>,
    /// **`true` なら件数は計画であって実績ではない** (1 行も書いていない)。
    ///
    /// [`TimecardWindowResult::dry_run`] と同じ理由で応答に出す — 無いと
    /// dry-run の `drivers_written` を書けたものと読み違える。
    ///
    /// [`TimecardWindowResult::dry_run`]: crate::kintai_push::TimecardWindowResult::dry_run
    pub dry_run: bool,
    /// この再計算が使った [`logic_version`]。
    ///
    /// **応答に載せるのがリスク欄の筆頭への対応。** 読み出しは計算しないので、
    /// 畳んだ値が古いままだと遅いのではなく静かに間違う — どの版で畳んだ値かを
    /// 呼び出し側が読めるようにする。
    pub logic_version: String,
    /// 畳んだ時刻 (JST, RFC 3339)。`logic_version` と対で「いつの計算か」を示す。
    pub calculated_at: String,
    /// 生イベントを読む途中で**上流が返した warnings**。
    ///
    /// R2 の分割遅れ (`NoSuchKey`) の最中に畳むと、欠けた入力を指紋付きで
    /// 「最新」として保存してしまう。指紋は入力から作るので、次に運行が揃えば
    /// 指紋が変わって畳み直されるが、**その間は静かに少ない拘束を返す**。
    /// tracing に落とすだけでは呼び出し側から見えないのでここまで運ぶ。
    pub warnings: Vec<String>,
    /// 畳むのにかかった時間 (ms)。
    ///
    /// 窓の受け口はこれを proxy の 100 秒に収める必要があるので、実測値を出す。
    /// 1 ページの乗務員数を決めるのもこの値 (`kintai_recalc` のモジュール docs)。
    pub elapsed_ms: u64,
}

impl FoldReport {
    pub fn wrote_anything(&self) -> bool {
        self.drivers_written > 0
    }

    /// 想定外があったか (呼び出し側が非 0 終了するのに使う)。
    ///
    /// [`SkipReason::is_known`] が真の skip は数えない — 落としたこと自体は
    /// `skipped` に残るので、表示と応答からは消えない。上流 warnings は
    /// 引き続き数える (欠けた入力で畳んだかもしれないシグナルなので)。
    pub fn has_unexpected(&self) -> bool {
        self.skipped.iter().any(|s| !s.is_known()) || !self.warnings.is_empty()
    }
}

/// 保存済みの `logic_version` の姿 (実装計画 06 の stale 検知)。
///
/// **`SELECT DISTINCT logic_version` 1 発で済ませる。** 指紋は乗務員ごと・月ごとに
/// 違うので、指紋で stale を数えると全単位を畳み直すのと同じ費用になる。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct StaleReport {
    /// いま走っているコードと設定の [`logic_version`]。
    pub logic_version: String,
    /// **これが 0 でなければ全量再計算が要る** (`POST /api/kintai/recalc`)。
    /// 対象期間に 1 行でも古い版の `day_summaries` を持つ乗務員の数。
    pub drivers: usize,
    /// 対象期間の `day_summaries` に載っている版の一覧。
    /// 現行版だけなら長さ 1、空なら 1 行も畳んでいない。
    pub versions: Vec<String>,
}

const STORED_STATE_SQL: &str = r#"
SELECT (SELECT coalesce(array_agg(DISTINCT fingerprint), '{}')
          FROM kintai.shifts
         WHERE tenant_id = $1 AND driver_cd = $2 AND date_start >= $3 AND date_start < $4) AS fps,
       (SELECT count(*) FROM kintai.shifts
         WHERE tenant_id = $1 AND driver_cd = $2 AND date_start >= $3 AND date_start < $4) AS n_shifts,
       (SELECT count(*) FROM kintai.day_summaries
         WHERE tenant_id = $1 AND driver_cd = $2 AND date >= $3 AND date < $4) AS n_days,
       (SELECT count(*) FROM kintai.day_parts p
          JOIN kintai.shifts s ON s.tenant_id = p.tenant_id AND s.driver_cd = p.driver_cd
                              AND s.start_at = p.shift_start_at
         WHERE p.tenant_id = $1 AND p.driver_cd = $2
           AND s.date_start >= $3 AND s.date_start < $4) AS n_parts
"#;

/// [`STORED_STATE_SQL`] の**複数乗務員版**。式は 1 文字も変えない。
///
/// 単数版は乗務員 1 人につき 1 往復で、全量再計算では**往復回数がそのまま時間**に
/// なる (1 ページ 50 人なら 50 往復、137 名なら約 137 往復)。しかも Pg 読みは
/// `[kintai_push]` の pool を共有していて `max_connections=1` — 完全に直列で、
/// 窓の受け口が書いている間はその 1 本を待つ。#231 (10,157 往復を `unnest` で
/// 畳んだ) と [`crate::kintai_push::STORED_WINDOW_SIGNATURES_SQL`] (乗務員ごとの
/// `GET /signatures` 94 名 33.6 秒を 1 発に畳んだ) と同じ型の話で、**費用は
/// 往復回数で転送量ではない**。
///
/// 単数版の `driver_cd = $2` を `driver_cd = q.driver_cd` に読み替え、乗務員を
/// `unnest($2::int8[])` から供給するだけ。**乗務員は等値・日付は範囲比較のまま**
/// なので索引の使われ方も単数版と同じ (`::date = ANY(...)` にすると索引が効かない)。
/// 保存が 1 件も無い乗務員も `unnest` 側の行として残るので、単数版が
/// `fetch_one` で必ず 1 行返すのと**同じく全乗務員ぶんの行が返る**。
///
/// **式を写し間違えると「中身は同じなのに毎回全乗務員が stale」**になり、
/// 静かに毎回全書き直しになる。2 つが同じであることはテストで縛る
/// (`the_states_sql_matches_the_single_driver_one` と、実 Postgres で単数版と
/// 複数版の結果が一致することを確かめる `kintai_fold_pg_test.rs` の口)。
const STORED_STATES_SQL: &str = r#"
SELECT q.driver_cd,
       (SELECT coalesce(array_agg(DISTINCT fingerprint), '{}')
          FROM kintai.shifts
         WHERE tenant_id = $1 AND driver_cd = q.driver_cd AND date_start >= $3 AND date_start < $4) AS fps,
       (SELECT count(*) FROM kintai.shifts
         WHERE tenant_id = $1 AND driver_cd = q.driver_cd AND date_start >= $3 AND date_start < $4) AS n_shifts,
       (SELECT count(*) FROM kintai.day_summaries
         WHERE tenant_id = $1 AND driver_cd = q.driver_cd AND date >= $3 AND date < $4) AS n_days,
       (SELECT count(*) FROM kintai.day_parts p
          JOIN kintai.shifts s ON s.tenant_id = p.tenant_id AND s.driver_cd = p.driver_cd
                              AND s.start_at = p.shift_start_at
         WHERE p.tenant_id = $1 AND p.driver_cd = q.driver_cd
           AND s.date_start >= $3 AND s.date_start < $4) AS n_parts
  FROM unnest($2::int8[]) AS q(driver_cd)
"#;

/// 期間に載っている `logic_version` と、古い版を 1 行でも持つ乗務員数。
///
/// `day_summaries` だけを見る — 3 表は同じトランザクションで同じ版を書くので、
/// どれか 1 表で足りる。読み出しの主経路がここなので、索引が温まっている方を選ぶ。
const STALE_STATE_SQL: &str = r#"
SELECT (SELECT coalesce(array_agg(DISTINCT logic_version), '{}')
          FROM kintai.day_summaries
         WHERE tenant_id = $1 AND date >= $2 AND date < $3) AS versions,
       (SELECT count(*) FROM (
           SELECT driver_cd
             FROM kintai.day_summaries
            WHERE tenant_id = $1 AND date >= $2 AND date < $3
              AND logic_version <> $4
            GROUP BY driver_cd) t) AS stale_drivers
"#;

/// 全量再計算の対象乗務員を 1 ページ (`driver_cd` の keyset ページング)。
///
/// **母集団は Postgres の distinct driver_cd と `extra` (呼び出し側の `unnest`) の和。**
///
/// 昔は Postgres の中だけで決めていた — 生イベントの読み先 (alc / MariaDB) に
/// 聞くと、乗務員を数えるためだけに月ぶんの `dtako_events` を JSON にして捨てる
/// ことになる (`kintai_diff::drivers_page` が同じ理由で `UNION` 1 本に絞ったのと
/// 同じ話)、という考え方自体は正しい。**が、それだと打刻ゼロ・休息由来だけの
/// 乗務員が母集団から永久に漏れる** — `kintai_events` は打刻由来の行しか持たず、
/// `day_summaries` は最低 1 回畳まれていないと現れない。1 回も畳んだことが無い
/// rest-only 乗務員はどちらの表にも 1 行も無い (#205-12、2026-06 実測で 42 名)。
///
/// 呼び出し元 ([`crate::routes::kintai_recalc::run`]) は母集団を決める前に
/// [`fold_month`] を 1 回読んでいる (ページの畳みそのものに要るので、ここで
/// もう 1 回生イベントを読み直す必要が無い)。その `split_by_driver` の
/// キー集合を `extra` として渡せば、月ぶんの読みを増やさずに埋まる。
const RECALC_DRIVER_PAGE_SQL: &str = r#"
WITH pop AS (
    SELECT DISTINCT driver_cd FROM kintai.kintai_events
     WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
    UNION
    SELECT DISTINCT driver_cd FROM kintai.day_summaries
     WHERE tenant_id = $1 AND date >= $4 AND date < $5
    UNION
    SELECT driver_cd FROM unnest($10::int8[]) AS e(driver_cd)
)
SELECT driver_cd FROM pop
 WHERE driver_cd > $6 AND driver_cd > 0
   AND (NOT $7 OR NOT EXISTS (
         SELECT 1 FROM kintai.day_summaries s
          WHERE s.tenant_id = $1 AND s.driver_cd = pop.driver_cd
            AND s.date >= $4 AND s.date < $5
            AND s.logic_version = $8))
 ORDER BY driver_cd
 LIMIT $9
"#;

/// `shifts` を消すと `day_summaries` / `day_parts` は FK の CASCADE で消える。
const DELETE_SHIFTS_SQL: &str = r#"
DELETE FROM kintai.shifts
 WHERE tenant_id = $1 AND driver_cd = $2 AND date_start >= $3 AND date_start < $4
"#;

/// 入れる勤務を **1 文で**。列ごとの配列を `unnest` で行に開く。
///
/// `fingerprint` / `logic_version` は単位ぜんたいで 1 つなので配列にしない。
const INSERT_SHIFTS_SQL: &str = r#"
INSERT INTO kintai.shifts
       (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
SELECT $1, d.driver_cd, d.start_at, d.end_at, d.shift_source, $6, $7
  FROM unnest($2::int8[], $3::timestamptz[], $4::timestamptz[], $5::text[])
       AS d(driver_cd, start_at, end_at, shift_source)
"#;

/// 入れる日別サマリを **1 文で**。分の列は 11 本あるが全部 `int4` の配列。
const INSERT_DAY_SUMMARIES_SQL: &str = r#"
INSERT INTO kintai.day_summaries
       (tenant_id, driver_cd, date, shift_start_at, shift_source,
        restraint_minutes, working_minutes, break_minutes, rest_minus_minutes,
        statutory_minutes, within_statutory_overtime_minutes, overtime_minutes,
        legal_holiday_minutes, night_minutes, overtime_night_minutes,
        legal_holiday_night_minutes, fingerprint, logic_version)
SELECT $1, d.driver_cd, d.date, d.shift_start_at, d.shift_source,
       d.restraint_minutes, d.working_minutes, d.break_minutes, d.rest_minus_minutes,
       d.statutory_minutes, d.within_statutory_overtime_minutes, d.overtime_minutes,
       d.legal_holiday_minutes, d.night_minutes, d.overtime_night_minutes,
       d.legal_holiday_night_minutes, $17, $18
  FROM unnest($2::int8[], $3::date[], $4::timestamptz[], $5::text[],
              $6::int4[], $7::int4[], $8::int4[], $9::int4[],
              $10::int4[], $11::int4[], $12::int4[],
              $13::int4[], $14::int4[], $15::int4[], $16::int4[])
       AS d(driver_cd, date, shift_start_at, shift_source,
            restraint_minutes, working_minutes, break_minutes, rest_minus_minutes,
            statutory_minutes, within_statutory_overtime_minutes, overtime_minutes,
            legal_holiday_minutes, night_minutes, overtime_night_minutes,
            legal_holiday_night_minutes)
"#;

/// 入れる暦日ビューを **1 文で**。
const INSERT_DAY_PARTS_SQL: &str = r#"
INSERT INTO kintai.day_parts
       (tenant_id, driver_cd, shift_start_at, date,
        restraint_minutes, working_minutes, night_minutes)
SELECT $1, d.driver_cd, d.shift_start_at, d.date,
       d.restraint_minutes, d.working_minutes, d.night_minutes
  FROM unnest($2::int8[], $3::timestamptz[], $4::date[],
              $5::int4[], $6::int4[], $7::int4[])
       AS d(driver_cd, shift_start_at, date,
            restraint_minutes, working_minutes, night_minutes)
"#;

/// JST の壁時計を `TIMESTAMPTZ` へ。
fn tz(dt: NaiveDateTime) -> DateTime<FixedOffset> {
    use chrono::TimeZone;
    FixedOffset::east_opt(crate::kintai_push::JST_OFFSET_SECONDS)
        .expect("JST offset is in range")
        .from_local_datetime(&dt)
        .single()
        .expect("JST has no DST gap")
}

/// 対象月の `[月初, 翌月初)` を `DATE` の境界として返す。
fn month_date_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next))
}

/// 保存済みの姿を読む。
pub async fn stored_state(
    store: &KintaiPgStore,
    driver_cd: i64,
    month: &str,
) -> Result<StoredState, KintaiPushError> {
    use sqlx::Row;
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let row = sqlx::query(STORED_STATE_SQL)
        .bind(store.tenant_id())
        .bind(driver_cd)
        .bind(m0)
        .bind(m1)
        .fetch_one(store.pool())
        .await?;
    Ok(StoredState {
        fingerprints: row.get::<Vec<String>, _>("fps"),
        shifts: row.get::<i64, _>("n_shifts"),
        day_summaries: row.get::<i64, _>("n_days"),
        day_parts: row.get::<i64, _>("n_parts"),
    })
}

/// [`stored_state`] の**複数乗務員版** — ページの全乗務員を 1 往復で読む。
///
/// 返すのは `driver_cd → StoredState`。**保存が 1 件も無い乗務員も
/// [`StoredState::is_empty`] な値で必ず入る** ([`STORED_STATES_SQL`] の
/// `unnest` 側の行が残るため)。呼び出し側は「鍵が無い = 保存が無い」と
/// 「鍵が無い = 読めていない」を混同せずに済む。
///
/// `drivers` が空なら**1 往復もしない**。
pub async fn stored_states(
    store: &KintaiPgStore,
    drivers: &[i64],
    month: &str,
) -> Result<std::collections::BTreeMap<i64, StoredState>, KintaiPushError> {
    use sqlx::Row;
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    if drivers.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }
    let rows = sqlx::query(STORED_STATES_SQL)
        .bind(store.tenant_id())
        .bind(drivers)
        .bind(m0)
        .bind(m1)
        .fetch_all(store.pool())
        .await?;
    let mut out = std::collections::BTreeMap::new();
    for row in rows {
        out.insert(
            row.get::<i64, _>("driver_cd"),
            StoredState {
                fingerprints: row.get::<Vec<String>, _>("fps"),
                shifts: row.get::<i64, _>("n_shifts"),
                day_summaries: row.get::<i64, _>("n_days"),
                day_parts: row.get::<i64, _>("n_parts"),
            },
        );
    }
    Ok(out)
}

/// 期間に載っている `logic_version` を数える (stale 検知)。
///
/// 窓の受け口が応答に載せるのはこれ。**畳み直しは起こさない** — 全量再計算は
/// 費用が違う別の口 (`POST /api/kintai/recalc`) の仕事で、ここは「要るかどうか」
/// だけを 1 クエリで答える。
pub async fn stale_state(
    store: &KintaiPgStore,
    months: &[String],
    params: &KosokuParams,
) -> Result<StaleReport, KintaiPushError> {
    use sqlx::Row;
    let version = logic_version(params);
    let (lo, hi) = months_span(months)?;
    let row = sqlx::query(STALE_STATE_SQL)
        .bind(store.tenant_id())
        .bind(lo)
        .bind(hi)
        .bind(&version)
        .fetch_one(store.pool())
        .await?;
    Ok(StaleReport {
        logic_version: version,
        drivers: row.get::<i64, _>("stale_drivers") as usize,
        versions: row.get::<Vec<String>, _>("versions"),
    })
}

/// 全量再計算の対象乗務員を 1 ページ返す。`after` は前ページの最後の乗務員CD。
///
/// `extra` は Postgres の外から足す母集団 (rest-only 乗務員の穴埋め、
/// [`RECALC_DRIVER_PAGE_SQL`] docs 参照)。`driver_cd <= 0` は呼び出し側が
/// 混ぜていても SQL 側で落とす ([`crate::kintai_repo`] の「0 以下は捨てる」と同じ規則)。
///
/// `stale_only` は「対象月に**現行版の行を 1 つも持たない**乗務員」に絞る。
/// 勤務が 1 本も立たない乗務員 (打刻はあるが全部落ちた月) は保存行を作れないので
/// **毎回この網に残る** — 収束しないので、全量を回す用途では `false` で使う。
pub async fn recalc_driver_page(
    store: &KintaiPgStore,
    month: &str,
    params: &KosokuParams,
    extra: &std::collections::BTreeSet<i64>,
    after: Option<i64>,
    stale_only: bool,
    limit: usize,
) -> Result<Vec<i64>, KintaiPushError> {
    use sqlx::Row;
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let extra: Vec<i64> = extra.iter().copied().collect();
    let rows = sqlx::query(RECALC_DRIVER_PAGE_SQL)
        .bind(store.tenant_id())
        .bind(jst_day_bounds(m0).0)
        .bind(jst_day_bounds(m1).0)
        .bind(m0)
        .bind(m1)
        .bind(after.unwrap_or(i64::MIN))
        .bind(stale_only)
        .bind(logic_version(params))
        .bind(limit as i64)
        .bind(&extra)
        .fetch_all(store.pool())
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<i64, _>("driver_cd"))
        .collect())
}

/// 月の並びぜんたいを覆う `[最初の月初, 最後の翌月初)`。
///
/// 月が飛んでいても 1 クエリで数える — 隙間ぶんの乗務員が混ざっても
/// 「stale が多めに出る」だけで、取りこぼす側には倒れない。
fn months_span(months: &[String]) -> Result<(NaiveDate, NaiveDate), KintaiPushError> {
    let mut lo: Option<NaiveDate> = None;
    let mut hi: Option<NaiveDate> = None;
    for m in months {
        let (a, b) = month_date_bounds(m)
            .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {m}")))?;
        lo = Some(lo.map_or(a, |c: NaiveDate| c.min(a)));
        hi = Some(hi.map_or(b, |c: NaiveDate| c.max(b)));
    }
    match (lo, hi) {
        (Some(a), Some(b)) => Ok((a, b)),
        _ => Err(KintaiPushError::NotConfigured(
            "months が空です".to_string(),
        )),
    }
}

/// 1 乗務員 1 か月ぶんを置き換える。**1 トランザクション**。
///
/// `shifts` を先に消してから入れ直す — `day_summaries` / `day_parts` は
/// `shifts` への FK を `ON DELETE CASCADE` で持つので、消し忘れが起きない。
///
/// ## 行ごとに往復しない
///
/// 1 行 1 INSERT で回すと、初回 fold (2 か月・95 名で `shifts` 約 1,900 +
/// `day_summaries` 数千 + `day_parts`) が数千往復になり、push 側が #231 で潰した
/// 「10,157 往復 → Cloudflare の 524 (100 秒)」と同型になる。
/// [`crate::kintai_push::KintaiPgStore::replace_window`] と同じく `unnest` で
/// **DELETE 1 文 + 表ごとに INSERT 数文**に畳む。
///
/// 刻み幅は push と同じ [`crate::kintai_push::INSERT_CHUNK`]。刻んでも同じ
/// トランザクションの中に居るので、全か無かは変わらない。
pub async fn write_unit(
    store: &KintaiPgStore,
    month: &str,
    unit: &FoldUnit,
    fingerprint: &str,
    params: &KosokuParams,
) -> Result<(), KintaiPushError> {
    use crate::kintai_push::INSERT_CHUNK;

    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let tenant = store.tenant_id();
    let version = logic_version(params);
    let mut tx = store.pool().begin().await?;
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await?;
    sqlx::query(DELETE_SHIFTS_SQL)
        .bind(tenant)
        .bind(unit.driver_cd)
        .bind(m0)
        .bind(m1)
        .execute(&mut *tx)
        .await?;

    for chunk in unit.shifts.chunks(INSERT_CHUNK) {
        let driver: Vec<i64> = chunk.iter().map(|s| s.driver_cd).collect();
        let start: Vec<DateTime<FixedOffset>> = chunk.iter().map(|s| tz(s.start_at)).collect();
        let end: Vec<DateTime<FixedOffset>> = chunk.iter().map(|s| tz(s.end_at)).collect();
        let source: Vec<&str> = chunk.iter().map(|s| s.shift_source).collect();
        sqlx::query(INSERT_SHIFTS_SQL)
            .bind(tenant)
            .bind(&driver)
            .bind(&start)
            .bind(&end)
            .bind(&source)
            .bind(fingerprint)
            .bind(version.as_str())
            .execute(&mut *tx)
            .await?;
    }

    for chunk in unit.day_summaries.chunks(INSERT_CHUNK) {
        let driver: Vec<i64> = chunk.iter().map(|d| d.driver_cd).collect();
        let date: Vec<NaiveDate> = chunk.iter().map(|d| d.date).collect();
        let start: Vec<DateTime<FixedOffset>> =
            chunk.iter().map(|d| tz(d.shift_start_at)).collect();
        let source: Vec<&str> = chunk.iter().map(|d| d.shift_source).collect();
        let col = |f: fn(&DaySummaryRow) -> i64| -> Vec<i32> {
            chunk.iter().map(|d| f(d) as i32).collect()
        };
        sqlx::query(INSERT_DAY_SUMMARIES_SQL)
            .bind(tenant)
            .bind(&driver)
            .bind(&date)
            .bind(&start)
            .bind(&source)
            .bind(col(|d| d.restraint_minutes))
            .bind(col(|d| d.working_minutes))
            .bind(col(|d| d.break_minutes))
            .bind(col(|d| d.rest_minus_minutes))
            .bind(col(|d| d.statutory_minutes))
            .bind(col(|d| d.within_statutory_overtime_minutes))
            .bind(col(|d| d.overtime_minutes))
            .bind(col(|d| d.legal_holiday_minutes))
            .bind(col(|d| d.night_minutes))
            .bind(col(|d| d.overtime_night_minutes))
            .bind(col(|d| d.legal_holiday_night_minutes))
            .bind(fingerprint)
            .bind(version.as_str())
            .execute(&mut *tx)
            .await?;
    }

    for chunk in unit.day_parts.chunks(INSERT_CHUNK) {
        let driver: Vec<i64> = chunk.iter().map(|p| p.driver_cd).collect();
        let start: Vec<DateTime<FixedOffset>> =
            chunk.iter().map(|p| tz(p.shift_start_at)).collect();
        let date: Vec<NaiveDate> = chunk.iter().map(|p| p.date).collect();
        let restraint: Vec<i32> = chunk.iter().map(|p| p.restraint_minutes as i32).collect();
        let working: Vec<i32> = chunk.iter().map(|p| p.working_minutes as i32).collect();
        let night: Vec<i32> = chunk.iter().map(|p| p.night_minutes as i32).collect();
        sqlx::query(INSERT_DAY_PARTS_SQL)
            .bind(tenant)
            .bind(&driver)
            .bind(&start)
            .bind(&date)
            .bind(&restraint)
            .bind(&working)
            .bind(&night)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

/// JST の今日。[`push_window_gap_warning`] が「はみ出した先がもう過ぎた日か」を
/// 見るためだけに使う。
fn today_jst() -> NaiveDate {
    let jst = FixedOffset::east_opt(9 * 3600).expect("JST offset is in range");
    chrono::Utc::now().with_timezone(&jst).date_naive()
}

/// **fold が読む窓のうち push が書かない 1 日**に打刻が 1 行も無ければ警告を返す
/// (Refs #205 の 30)。
///
/// | | 関数 | 窓 |
/// |---|---|---|
/// | 書く ([`crate::kintai_push::push_month`]) | `exact_month_range` | `[月初, 翌月初)` |
/// | 読む ([`fold_month`]) | [`month_range`] | `[月初, 翌月 2 日)` |
///
/// **ずれの 1 日 (`翌月 1 日`) は翌月の push でしか書かれない。** 翌月がまだ
/// push されていなければ、6 月の fold は**月を跨いだ勤務の終業打刻を読めないまま
/// 畳む**。窓が違うこと自体は意図どおり (日跨ぎ勤務の終業を拾うため) だが、
/// **はみ出した先が push されているという前提はどこにも保証が無かった。**
///
/// 実測 (2026-06 / 乗務員 1021): `始業 06-21 06:12:34` の相手は
/// `終業 07-01 14:54:29`。7 月が未 push で 6 月の fold から見えず、勤務が休息で
/// 閉じて `shift_source` が 6 名 28 行ぶん反転していた。**値は 11 列とも同じだった
/// ので突合するまで誰も気付けない** — だから鳴らす。
///
/// **窓は揃えない。** `exact_month_range` を選んだ理由 (翌月頭を「その日の一部しか
/// 見ていない状態」で署名すると毎回書き直しになる) は
/// `crate::kintai_push::push_month` の docs にあり、正当なので触らない。
///
/// - **翌月初がまだ過ぎていない月では黙る** — 当月を畳むときは、はみ出した先が
///   未来なので打刻が無いのが正常。過ぎた日にだけ鳴らす (`spill < today`)
/// - 判定に使うのは**既に読んだ行だけ**。往復は増やさない
/// - 見るのは [`PUSHED_SOURCES`] (`timecard` / `dtako`) だけ。`dtako_events` は
///   push を通らず alc から直に来るので、在っても push の証拠にならない
fn push_window_gap_warning(
    month: &str,
    rows: &[serde_json::Value],
    today: NaiveDate,
) -> Option<String> {
    let (_, spill_from) = exact_month_range(month)?;
    let spill = NaiveDate::parse_from_str(spill_from.get(..10)?, "%Y-%m-%d").ok()?;
    if spill >= today {
        return None;
    }
    let pushed = |r: &serde_json::Value| {
        let src = r.get("source").and_then(|v| v.as_str()).unwrap_or_default();
        let at = r
            .get("datetime")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        PUSHED_SOURCES.contains(&src) && at >= spill_from.as_str()
    };
    if rows.iter().any(pushed) {
        return None;
    }
    Some(format!("push 窓ずれ: {month} の fold が読む {spill} に打刻が 0 行 — 翌月が未 push なら月跨ぎ勤務の終業打刻が欠ける"))
}

/// 対象月を**生イベント 1 回読み**で乗務員ごとに畳む。返すのは乗務員CD 昇順。
///
/// 期間は [`month_range`] = `[月初, 翌月 2 日)`。読み出し経路と同じで、日跨ぎ勤務の
/// 終業打刻を拾うために翌月へはみ出す (push の `exact_month_range` とは違う —
/// あちらは「その日の全部を見た上で署名する」必要があるため)。
///
/// 読みは [`KintaiEventsApi::fetch_all_events_between`] **1 回だけ**。乗務員で分ける
/// のは [`crate::kosoku::split_by_driver`] で、これは in-process の純粋関数なので
/// 往復が増えない。`driver` を指定してもこの読み方を変えない — 理由は
/// モジュール docs の「材料の行は全乗務員版の形」。
///
/// **打刻 2 表だけの読み ([`crate::kintai_push::read_driver_events`]) を使わない。**
/// あれは #225 で push / diff 用に絞ったもので `dtako_events` が落ちている。
/// [`daily_summary`] は休息イベントで勤務を切るので、渡すと休息由来の勤務が
/// 丸ごと消える (2026-07-31 の回帰、#234)。全乗務員版は 3 表を `UNION ALL` した
/// ものなので、この穴が構造的に開かない。
///
/// **`driver` 指定でその乗務員の行が 1 つも無くても単位を 1 つ返す。** 空の
/// [`FoldUnit`] を返すことで、呼び出し側が「保存済みの行が余っている」と気付いて
/// 消せる。ここで黙って 0 件にすると、退職などで打刻が消えた乗務員の古い行が残る。
/// (`driver` 未指定のとき同じことが起きないのは、そもそも行が無い乗務員を
/// 列挙できないため — こちらは別途 #205 の宿題)
///
/// [`KintaiEventsApi::fetch_all_events_between`]: crate::kintai_repo::KintaiEventsApi::fetch_all_events_between
pub async fn fold_month(
    repo: &DynKintaiEventsRepo,
    params: &KosokuParams,
    month: &str,
    driver: Option<u64>,
) -> Result<Vec<(u64, FoldUnit, String)>, KintaiPushError> {
    let (from, to) = month_range(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let rows = repo.fetch_all_events_between(&from, &to).await?;
    tracing::debug!("fold {month}: read {} rows in 1 fetch_all", rows.len());
    // はみ出した先の 1 日が push されているかを確かめる (Refs #205 の 30)
    if let Some(w) = push_window_gap_warning(month, &rows, today_jst()) {
        tracing::warn!("{w}");
        crate::kintai_http_repo::record_warning(&w);
    }
    let mut units: Vec<(u64, FoldUnit, String)> = crate::kosoku::split_by_driver(rows)
        .into_iter()
        .filter(|(cd, _)| driver.is_none_or(|want| want == *cd))
        .map(|(cd, rows)| {
            let (unit, fp) = fold_driver_month(cd as i64, month, params, rows);
            (cd, unit, fp)
        })
        .collect();
    if let Some(cd) = driver {
        if units.is_empty() {
            let (unit, fp) = fold_driver_month(cd as i64, month, params, Vec::new());
            units.push((cd, unit, fp));
        }
    }
    Ok(units)
}

// ── 13: 月ゲート ─────────────────────────────────────────────────────────

/// 前回 fold したときの入力の指紋 (Refs #205 実装計画 13、`kintai.fold_gate`)。
struct FoldGateRow {
    dtako_digest: String,
    punch_digest: String,
    logic_version: String,
}

const FOLD_GATE_SELECT_SQL: &str = r#"
SELECT dtako_digest, punch_digest, logic_version
  FROM kintai.fold_gate
 WHERE tenant_id = $1 AND month = $2
"#;

/// 書いてよいのは「月まるごとを 1 呼び出しで完結させた」ときだけ。
/// [`recalc_month`] (`driver` 省略) と、それを満たした
/// [`crate::routes::kintai_recalc::run`] の 1 ページが両方これに当たる —
/// 判定条件はそれぞれの docs 参照。
const FOLD_GATE_UPSERT_SQL: &str = r#"
INSERT INTO kintai.fold_gate (tenant_id, month, dtako_digest, punch_digest, logic_version, folded_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (tenant_id, month) DO UPDATE
   SET dtako_digest = EXCLUDED.dtako_digest,
       punch_digest = EXCLUDED.punch_digest,
       logic_version = EXCLUDED.logic_version,
       folded_at = EXCLUDED.folded_at
"#;

async fn read_fold_gate(
    store: &KintaiPgStore,
    month: &str,
) -> Result<Option<FoldGateRow>, KintaiPushError> {
    use sqlx::Row;
    let row = sqlx::query(FOLD_GATE_SELECT_SQL)
        .bind(store.tenant_id())
        .bind(month)
        .fetch_optional(store.pool())
        .await?;
    Ok(row.map(|r| FoldGateRow {
        dtako_digest: r.get::<String, _>("dtako_digest"),
        punch_digest: r.get::<String, _>("punch_digest"),
        logic_version: r.get::<String, _>("logic_version"),
    }))
}

/// gate の実書き込み (実装計画 13)。**呼ぶのは [`write_fold_gate_best_effort`]
/// だけ** — 失敗を持ち上げてよい呼び出し元が 1 つも無いため private にしてある
/// (Refs #205 の 20)。
///
/// [`recalc_month`] 経由 (CLI の `Recalc` / `Sync`) は
/// [`crate::kintai_http_repo::warnings_seen`] で「収集器の中で warnings ゼロ」が
/// 確認できた回だけ書く (Refs #205-17)。`routes::kintai_recalc::run` の 5 条件の
/// 1 つ (`folded.warnings.is_empty()`) と同じ理由 — R2 の分割遅れ中に欠けた入力を
/// 「最新」と刻まないため。
async fn write_fold_gate(
    store: &KintaiPgStore,
    month: &str,
    dtako_digest: &str,
    punch_digest: &str,
    logic_version: &str,
) -> Result<(), KintaiPushError> {
    sqlx::query(FOLD_GATE_UPSERT_SQL)
        .bind(store.tenant_id())
        .bind(month)
        .bind(dtako_digest)
        .bind(punch_digest)
        .bind(logic_version)
        .execute(store.pool())
        .await?;
    Ok(())
}

/// [`write_fold_gate`] の「書けなくても呼び出し元の口を落とさない」版
/// (Refs #205 の 20)。
///
/// 書き込みは**畳み終わった後**に走る。ここで `Err` を持ち上げると、再計算も保存も
/// 全部成功したリクエストが最適化の書き込み 1 本だけを理由に 502 (`map_push_err`)
/// になる — 呼び出し側から見ると「再計算が失敗した」と区別が付かない。書けなければ
/// gate が立たないだけで、次回また全量読みに落ちる**安全側**なので、loud に warn
/// して先へ進む。無言にはしない。
pub(crate) async fn write_fold_gate_best_effort(
    store: &KintaiPgStore,
    month: &str,
    dtako_digest: &str,
    punch_digest: &str,
    logic_version: &str,
) {
    let w = write_fold_gate(store, month, dtako_digest, punch_digest, logic_version).await;
    if let Err(e) = w {
        tracing::warn!(month = %month, error = %e, "kintai month-gate write failed");
    }
}

/// いまの月の入力から指紋を作る。**`Ok(None)` は「判定できない」の意味** —
/// alc に `GET /api/dtako/events/etags` の口が無い環境、上流エラー、そして
/// **打刻側 (Pg) の読み取り失敗**がこれになる。どれも呼び出し側は安全側
/// (従来どおり全量読み) に倒す。**loud** — 呼び出し元の応答を握り潰さないよう、
/// ここで tracing::warn しておく。
///
/// **Pg 側の失敗を `Err` で持ち上げない** (Refs #205 の 20)。月ゲートは最適化で
/// あって、失敗しても「省けなかった」以上の意味を持たない。持ち上げると
/// [`crate::routes::kintai_recalc::run`] の `map_push_err` が **502** に変えて
/// 口ぜんたいを落とす — 2026-07-31 に実害。`kintai.fold_gate` の GRANT 漏れ
/// (migrations/005) で `permission denied` になり、`POST /api/kintai/recalc` が
/// **ログ 1 行も出さずに** 502 を返し続けた。Pg が本当に死んでいるなら後続の
/// [`store_units`] が正しくエラーを返すので、ここで倒しても失敗は隠れない。
///
/// 打刻側 ([`KintaiPgStore::stored_month_punch_digest`]) の窓は [`fold_month`] が
/// 実際に読む [`month_range`] と**同じ**にする。窓がずれると、fold の入力が
/// 変わっているのに月ゲートだけ古い窓を見て「変わっていない」と誤判定しうる。
async fn compute_month_digests(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    month: &str,
) -> Result<Option<(String, String)>, KintaiPushError> {
    let dtako_digest = match repo.fetch_dtako_month_digest(month).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            tracing::info!(month = %month, "kintai month-gate unavailable (no alc etags endpoint)");
            return Ok(None);
        }
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate dtako digest failed");
            return Ok(None);
        }
    };
    let (from_s, to_s) = month_range(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let bad_month = || KintaiPushError::NotConfigured(format!("bad month: {month}"));
    let from = tz(parse_dt(&from_s).ok_or_else(bad_month)?);
    let to = tz(parse_dt(&to_s).ok_or_else(bad_month)?);
    // 運行の突合 (Refs #205 の 37)。両側とも既に読んでいるものだけで組む —
    // GCP 側は直前の etags、オンプレ側は押し込み済みの kintai_events
    measure_unko_diff(store, month, from, to).await;
    let punch_digest = match store.stored_month_punch_digest(from, to).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate punch digest failed");
            return Ok(None);
        }
    };
    Ok(Some((dtako_digest, punch_digest)))
}

/// **オンプレと GCP の運行を突き合わせて記録する** (Refs #205 の 37)。
///
/// GCP 側で畳んだ 2026-06 が オンプレ基準より 143 行少ない件は「乗務員ごとに最後の
/// 1 本の `運行NO` がまるごと GCP 側の運行一覧に無い」形だと分かっている。**どの
/// 運行が欠けているかを名指し**できるようにするのがここ — 結果は task local に
/// 積まれ、[`crate::routes::kintai_recalc`] が `unko_diff` として応答に載せる。
///
/// **判定には一切入らない。** GCP 側の一覧は直前の `fetch_dtako_month_digest` が
/// 引いた etags をそのまま読むだけで、月ゲートの指紋 (`digest_from_pairs`) には
/// 触れない。突合そのものが失敗しても loud に warn して先へ進む — 最適化でも
/// 判定でもない観測なので、ここで口を落とす理由が無い ([`compute_month_digests`]
/// の docs と同じ考え方)。
async fn measure_unko_diff(
    store: &KintaiPgStore,
    month: &str,
    from: DateTime<FixedOffset>,
    to: DateTime<FixedOffset>,
) {
    let Some(gcp) = crate::kintai_http_repo::collected_etag_unko_nos() else {
        return;
    };
    let rows = match store.stored_month_operations(from, to).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai unko diff read failed");
            return;
        }
    };
    let onprem: Vec<crate::kintai_http_repo::OnpremOperation> = rows
        .into_iter()
        .map(|(driver_cd, unko_no, first_date, last_date)| {
            crate::kintai_http_repo::OnpremOperation {
                driver_cd,
                unko_no,
                first_date,
                last_date,
            }
        })
        .collect();
    let diff = crate::kintai_http_repo::record_unko_diff(&onprem, &gcp);
    let (n, r) = (diff.total, diff.gcp_only);
    tracing::info!(n, r, "kintai unko diff");
}

/// 保存済みの指紋がいまの指紋と一致するか (= 読み・畳みを省いてよいか)。
async fn month_gate_hit(
    store: &KintaiPgStore,
    month: &str,
    version: &str,
    dtako_digest: &str,
    punch_digest: &str,
) -> Result<bool, KintaiPushError> {
    let stored = read_fold_gate(store, month).await?;
    Ok(stored.is_some_and(|g| {
        g.logic_version == version
            && g.dtako_digest == dtako_digest
            && g.punch_digest == punch_digest
    }))
}

/// 月ゲートの判定結果 (実装計画 13)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonthGate {
    /// 一致した — 呼び出し側はこの [`FoldReport`] をそのまま返してよい。
    /// [`fold_month`] の読みを 1 バイトも払っていない。
    Hit(FoldReport),
    /// 一致しなかった (gate が未確立な場合も含む) — 通常どおり読み・畳みへ進む。
    ///
    /// **digest は「判定した瞬間の値」。** 月まるごとを完結させたときに書き戻すのは
    /// 必ずこの値のまま — fold の後に作り直さない。処理中に入力が増えても、
    /// 古い digest を書く方が安全側 (次回また miss して読み直すだけ)。新しい
    /// digest を書くと、増えた分が畳まれないまま「最新」として取り残される
    /// (静かに間違う側の事故)。
    Miss {
        dtako_digest: String,
        punch_digest: String,
        logic_version: String,
    },
    /// 判定できない (alc に口が無い / 上流エラー)。gate を諦めて常に読みに進む。
    Unavailable,
}

/// 月ゲートを判定する (実装計画 13)。**読むだけで書かない** — 書くかどうかは
/// 呼び出し側が「月まるごとを 1 呼び出しで完結させたか」を見て決める
/// ([`recalc_month`] / [`crate::routes::kintai_recalc::run`] の docs 参照)。
///
/// **`kintai.fold_gate` が読めなくても `Err` にしない** ([`MonthGate::Unavailable`]
/// に倒す、Refs #205 の 20)。理由は [`compute_month_digests`] の docs と同じ —
/// 最適化の失敗で口ぜんたいを 502 にしない。
pub async fn month_gate_report(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    apply: bool,
) -> Result<MonthGate, KintaiPushError> {
    let Some((dtako_digest, punch_digest)) = compute_month_digests(repo, store, month).await?
    else {
        return Ok(MonthGate::Unavailable);
    };
    let version = logic_version(params);
    let hit = match month_gate_hit(store, month, &version, &dtako_digest, &punch_digest).await {
        Ok(hit) => hit,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate read failed");
            return Ok(MonthGate::Unavailable);
        }
    };
    if hit {
        tracing::info!(month = %month, "kintai month-gate skip");
        return Ok(MonthGate::Hit(new_report(params, apply)));
    }
    Ok(MonthGate::Miss {
        dtako_digest,
        punch_digest,
        logic_version: version,
    })
}

/// 対象月を再計算して保存する (実装計画 05)。
///
/// 畳むのは [`fold_month`] (生イベントの読みは月 1 回)。ここがやるのは
/// 保存済みの姿との突き合わせと書き込みだけ。
///
/// ## 月ゲート (実装計画 13、`driver` 省略時だけ)
///
/// `driver` を指定しない呼び出しは**月まるごとを 1 回で完結させる**経路の 1 つ
/// (CLI の `Recalc` / `Sync`、systemd timer が呼ぶのはこちら。もう 1 つは
/// [`crate::routes::kintai_recalc::run`] が全ページ回りきったとき)。前回 fold
/// 時の指紋といまの指紋が一致すれば、[`fold_month`] の読み (R2 GET が運行数ぶん)
/// もその後の乗務員ごとの `stored_state` 突き合わせも**まるごと省く**。
///
/// **[`recalc_drivers`] はこの gate を読むだけで書かない。** ページングされた
/// 再計算の 1 ページだけが書いてしまうと、1 ページ目の乗務員だけ処理した時点で
/// gate が「この月は最新」になり、未処理のページの乗務員が古い fingerprint の
/// まま取り残される — #225 / #234 と同型の「静かに一部が消える」事故になる。
///
/// **gate を書くのは [`crate::kintai_http_repo::warnings_seen`] が `Some(false)`
/// (収集器の中・warnings ゼロ) のときだけ** (Refs #205-17)。呼び出し元 (`main.rs`)
/// が `with_warning_sink` で包んでいる前提だが、包まれていない (`None`) 場合は
/// 「無い」と「分からない」を混同しないよう書かない — 次回また全量読みに落ちる
/// だけで安全側。
pub async fn recalc_month(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    driver: Option<u64>,
    apply: bool,
) -> Result<FoldReport, KintaiPushError> {
    if driver.is_none() {
        match month_gate_report(repo, store, params, month, apply).await? {
            MonthGate::Hit(report) => return Ok(report),
            MonthGate::Miss {
                dtako_digest,
                punch_digest,
                logic_version,
            } => {
                let units = fold_month(repo, params, month, driver).await?;
                let report = store_units(store, params, month, units, apply).await?;
                // warnings が確認できた回 (Some(false)) だけ刻む。None (収集器の外) や
                // Some(true) では書かない — 迷ったら書かない側 (Refs #205-17)
                if apply && crate::kintai_http_repo::warnings_seen() == Some(false) {
                    write_fold_gate_best_effort(
                        store,
                        month,
                        &dtako_digest,
                        &punch_digest,
                        &logic_version,
                    )
                    .await;
                }
                return Ok(report);
            }
            MonthGate::Unavailable => {}
        }
    }
    let units = fold_month(repo, params, month, driver).await?;
    store_units(store, params, month, units, apply).await
}

/// **名指しした乗務員だけ**を再計算する (Refs #205 の 06 の HTTP 版)。
///
/// 窓の受け口が apply 後に呼ぶのはこちら。全乗務員を洗い出さないのが要点で、
/// 打刻の窓で変わった乗務員は定常時ほぼ 0 人なので、auth-worker proxy
/// (Cloudflare) の 100 秒上限に必ず収まる。
///
/// **`drivers` が空なら 1 往復もしない。** 差分ゼロの窓で読み先を叩くと、何も
/// 変わっていないのに月ぶんの生イベントを毎回引くことになる。
///
/// 読み方は [`fold_month`] と同じ**全乗務員版 1 回読み**。名指ししたのが 1 人でも
/// 変えない — 経路によって行の形が変わると、同じ (乗務員, 月) の指紋が読み方で
/// 割れ、窓の畳み直しと月次バッチが互いに相手の書いた行を stale と見て毎回
/// 書き直し合う (モジュール docs の「材料の行は全乗務員版の形」)。
///
/// ## `apply = false` は「保存済みの入力で畳んだら」の報告
///
/// 生イベントは**読み先から読み直す**ので、dry-run の窓と束ねたときの報告は
/// 「いま届いた打刻を反映したら」ではなく「**すでに保存されている打刻で畳んだら**」に
/// なる (dry-run の窓は `kintai_events` を書かないため)。届いた打刻を織り込んだ
/// 結果を見たいなら、先に窓を apply してから畳む。
pub async fn recalc_drivers(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    drivers: &[u64],
    apply: bool,
) -> Result<FoldReport, KintaiPushError> {
    if drivers.is_empty() {
        return Ok(new_report(params, apply));
    }
    // 月ゲート (実装計画 13) — **読むだけで書かない** (理由は recalc_month docs)。
    // ここで一致すれば、名指しした乗務員が全員すでに最新の指紋で保存済みという
    // ことなので、fold_month の全量読みごと省いて "unchanged" 扱いで返す
    if let MonthGate::Hit(_) = month_gate_report(repo, store, params, month, apply).await? {
        tracing::info!(month = %month, n = drivers.len(), "kintai month-gate skip");
        let mut report = new_report(params, apply);
        report.drivers = drivers.len();
        report.drivers_unchanged = drivers.len();
        return Ok(report);
    }
    let all = fold_month(repo, params, month, None).await?;
    recalc_drivers_from_units(store, params, month, drivers, all, apply).await
}

/// [`recalc_drivers`] の続き — **生イベントの読みを呼び出し側から受け取る**。
///
/// 呼び出し元が [`fold_month`] を既に済ませている場合 (`kintai_recalc` が母集団の
/// 決定 (#205-12) と畳みで同じ 1 回読みを使い回すとき) はこちらを直接呼ぶ。ここで
/// もう一度 [`fold_month`] を呼ぶと 1 ページの費用 (全量読み 25〜55 秒) が単純に
/// 倍になる。
pub async fn recalc_drivers_from_units(
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    drivers: &[u64],
    all: Vec<(u64, FoldUnit, String)>,
    apply: bool,
) -> Result<FoldReport, KintaiPushError> {
    if drivers.is_empty() {
        return Ok(new_report(params, apply));
    }
    let want: std::collections::BTreeSet<u64> = drivers.iter().copied().collect();
    let mut units: Vec<(u64, FoldUnit, String)> = all
        .into_iter()
        .filter(|(cd, _, _)| want.contains(cd))
        .collect();
    // **行が 1 つも無い乗務員にも空の単位を立てる。** 打刻が丸ごと消された乗務員は
    // ここで拾わないと、保存済みの古い勤務が消えずに残る ([`fold_month`] の
    // `driver` 指定と同じ扱い)
    let seen: std::collections::BTreeSet<u64> = units.iter().map(|(cd, _, _)| *cd).collect();
    for cd in want.difference(&seen).copied() {
        let (unit, fp) = fold_driver_month(cd as i64, month, params, Vec::new());
        units.push((cd, unit, fp));
    }
    units.sort_by_key(|(cd, _, _)| *cd);
    store_units(store, params, month, units, apply).await
}

/// 畳んだ単位を保存済みの姿と突き合わせ、変わったものだけ書く。
///
/// [`recalc_month`] と [`recalc_drivers`] の共通部分。**読み方だけが違って
/// 書き方は同じ**であることをここで担保する。
///
/// ## 保存済みの姿は 1 往復でまとめて読む (Refs #205 の 25)
///
/// 乗務員ごとに [`stored_state`] を呼ぶと**乗務員の数だけ往復**する。Pg 読みは
/// 窓の受け口と pool (`max_connections=1`) を共有しているので完全に直列で、
/// 137 名なら 137 回ぶんの待ちがそのまま全量再計算の時間に乗る。[`stored_states`]
/// で先に全員ぶんを引き、突き合わせは Rust の中で回す — 判定そのもの
/// ([`StoredState::is_current`]) は 1 行も変えない。
async fn store_units(
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    units: Vec<(u64, FoldUnit, String)>,
    apply: bool,
) -> Result<FoldReport, KintaiPushError> {
    let started = std::time::Instant::now();
    let mut report = new_report(params, apply);
    let cds: Vec<i64> = units.iter().map(|(cd, _, _)| *cd as i64).collect();
    let stored_all = stored_states(store, &cds, month).await?;
    for (driver_cd, unit, fp) in units {
        report.drivers += 1;
        report.skipped.extend(unit.skipped.iter().cloned());

        // 引けなかった乗務員 (`unnest` が行を残すので実際には来ない) は「保存が
        // 無い」ではなく **stale 側に倒す** — 書き直すだけなら壊れないが、逆に
        // 倒すと畳んだ結果が静かに落ちる
        let stored = stored_all.get(&(driver_cd as i64));
        if stored.is_some_and(|s| s.is_current(&unit, &fp)) {
            report.drivers_unchanged += 1;
            continue;
        }
        report.drivers_written += 1;
        report.shifts += unit.shifts.len();
        report.day_summaries += unit.day_summaries.len();
        report.day_parts += unit.day_parts.len();
        if apply {
            write_unit(store, month, &unit, &fp, params).await?;
        }
    }
    report.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(report)
}

/// 空の [`FoldReport`]。**版と計算時刻は 1 行も書かなくても載せる** — 呼び出し側が
/// 「どの版で畳んだ結果か」を必ず読めるようにするため (#205 のリスク欄の筆頭)。
fn new_report(params: &KosokuParams, apply: bool) -> FoldReport {
    FoldReport {
        dry_run: !apply,
        logic_version: logic_version(params),
        calculated_at: now_jst(),
        ..Default::default()
    }
}

/// 畳んだ時刻 (JST, RFC 3339)。
fn now_jst() -> String {
    use chrono::TimeZone;
    let jst = FixedOffset::east_opt(crate::kintai_push::JST_OFFSET_SECONDS)
        .expect("JST offset is in range");
    jst.from_utc_datetime(&chrono::Utc::now().naive_utc())
        .to_rfc3339()
}

// ── 06: push と再計算を束ねる ──────────────────────────────────────────────

/// [`sync_month`] の集計。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncReport {
    pub push: crate::kintai_push::PushReport,
    pub fold: FoldReport,
}

impl SyncReport {
    /// 想定外があったか (呼び出し側が非 0 終了するのに使う)。
    pub fn has_unexpected(&self) -> bool {
        self.push.has_unexpected() || self.fold.has_unexpected()
    }
}

/// 打刻の push と再計算を 1 回で回す (実装計画 06)。
///
/// **push が 1 日でも書いたら再計算を必ず走らせる。** 「push しただけで計算して
/// いない」状態を作らないため — 読み出しは計算しないので、畳んだ値が古いままだと
/// 遅いのではなく**静かに間違う** (#205 のリスク欄の筆頭)。
///
/// 逆に差分が無ければ再計算も指紋で弾かれるので、実質何もせずに終わる。
/// 再計算を呼ぶこと自体は常に行う — push が差分ゼロでも、`kosoku.rs` や TOML の
/// 変更で指紋が変わっている可能性があるため。
pub async fn sync_month(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    opts: &crate::kintai_push::PushOptions,
) -> Result<SyncReport, KintaiPushError> {
    let push = crate::kintai_push::push_month(repo, store, opts).await?;
    let fold = recalc_month(repo, store, params, &opts.month, opts.driver, opts.apply).await?;
    Ok(SyncReport { push, fold })
}

/// `day_parts` の暦日境界。テストと診断で使う。
pub fn day_bounds(date: NaiveDate) -> (DateTime<FixedOffset>, DateTime<FixedOffset>) {
    jst_day_bounds(date)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kosoku::DayPart;

    fn day(date: &str, start: &str, end: &str) -> DaySummary {
        DaySummary {
            date: date.to_string(),
            start: start.to_string(),
            end: end.to_string(),
            source: ShiftSource::Timecard,
            punches: Vec::new(),
            parts: Vec::new(),
            is_legal_holiday: false,
            over_24h: false,
            restraint_minutes: 600,
            break_minutes: 60,
            working_minutes: 540,
            rest_minus_minutes: 0,
            statutory_minutes: 450,
            within_statutory_overtime_minutes: 30,
            overtime_minutes: 60,
            legal_holiday_minutes: 0,
            night_minutes: 0,
            overtime_night_minutes: 0,
            legal_holiday_night_minutes: 0,
            ferry_minus_minutes: 0,
            run_gap_minutes: 0,
            punch_tail_minutes: 0,
            punch_head_minutes: 0,
            run_head_minutes: 0,
            lunch_overlap_minutes: 0,
        }
    }

    fn part(date: &str, restraint: i64, working: i64, night: i64) -> DayPart {
        let mut p = DayPart {
            date: date.to_string(),
            restraint_minutes: restraint,
            working_minutes: working,
            overtime_minutes: 0,
            legal_holiday_minutes: 0,
            night_minutes: night,
            overtime_night_minutes: 0,
            legal_holiday_night_minutes: 0,
            ferry_minus_minutes: 0,
            run_gap_minutes: 0,
            punch_tail_minutes: 0,
            punch_head_minutes: 0,
            run_head_minutes: 0,
            lunch_overlap_minutes: 0,
        };
        p.run_head_minutes = 0;
        p
    }

    #[test]
    fn fold_maps_every_minutes_column() {
        let unit = fold_days(
            1130,
            &[day(
                "2026-07-01",
                "2026-07-01 08:00:00",
                "2026-07-01 18:00:00",
            )],
        );
        assert_eq!(unit.shifts.len(), 1);
        assert_eq!(unit.day_summaries.len(), 1);
        let s = &unit.shifts[0];
        assert_eq!(s.driver_cd, 1130);
        assert_eq!(s.shift_source, "timecard");
        assert_eq!(s.start_at, parse_dt("2026-07-01 08:00:00").unwrap());
        let d = &unit.day_summaries[0];
        assert_eq!(d.date, parse_date("2026-07-01").unwrap());
        assert_eq!(d.shift_start_at, s.start_at, "day_summaries は勤務に紐づく");
        assert_eq!(d.restraint_minutes, 600);
        assert_eq!(d.working_minutes, 540);
        assert_eq!(d.break_minutes, 60);
        assert_eq!(d.statutory_minutes, 450);
        assert_eq!(d.within_statutory_overtime_minutes, 30);
        assert_eq!(d.overtime_minutes, 60);
    }

    #[test]
    fn fold_keeps_two_shifts_on_the_same_date() {
        // 実測 (1726 / 2026-03-14) は 1 日が 4 勤務。002 で PK に勤務を足したので
        // 同じ date の行が並んでよい
        let unit = fold_days(
            1726,
            &[
                day("2026-07-01", "2026-07-01 01:00:00", "2026-07-01 01:16:00"),
                day("2026-07-01", "2026-07-01 05:00:00", "2026-07-01 06:22:00"),
            ],
        );
        assert_eq!(unit.day_summaries.len(), 2);
        assert_ne!(
            unit.day_summaries[0].shift_start_at,
            unit.day_summaries[1].shift_start_at
        );
        assert_eq!(unit.day_summaries[0].date, unit.day_summaries[1].date);
    }

    #[test]
    fn fold_drops_shifts_that_collapse_to_a_point() {
        // CHECK (end_at > start_at) を満たせない。1 本のために月を落とさない
        let unit = fold_days(
            1,
            &[day(
                "2026-07-01",
                "2026-07-01 17:00:00",
                "2026-07-01 17:00:00",
            )],
        );
        assert!(unit.shifts.is_empty());
        assert!(unit.day_summaries.is_empty());
        assert_eq!(unit.skipped.len(), 1);
        assert!(matches!(
            unit.skipped[0],
            SkipReason::DegenerateShift { .. }
        ));
    }

    #[test]
    fn fold_drops_parts_before_the_shift_start() {
        // run_head は始業の最大 8 時間前まで遡るので、前日の DayPart ができうる。
        // CHECK (date >= shift の JST 日付) を満たせない
        let mut d = day("2026-07-02", "2026-07-02 00:30:00", "2026-07-02 18:00:00");
        d.parts = vec![part("2026-07-01", 5, 0, 0), part("2026-07-02", 100, 90, 10)];
        let unit = fold_days(1, &[d]);
        assert_eq!(unit.day_parts.len(), 1);
        assert_eq!(unit.day_parts[0].date, parse_date("2026-07-02").unwrap());
        assert_eq!(unit.skipped.len(), 1);
        assert!(matches!(
            unit.skipped[0],
            SkipReason::PartBeforeShift { .. }
        ));
    }

    #[test]
    fn fold_maps_rest_derived_shifts() {
        // 休息イベントで境界を決めた勤務。DDL の CHECK は 'timecard' / 'rest' の 2 値
        let mut d = day("2026-07-01", "2026-07-01 08:00:00", "2026-07-01 18:00:00");
        d.source = ShiftSource::Rest;
        let unit = fold_days(1, &[d]);
        assert_eq!(unit.shifts[0].shift_source, "rest");
        assert_eq!(unit.day_summaries[0].shift_source, "rest");
    }

    #[test]
    fn fold_skips_parts_with_an_unparsable_date() {
        let mut d = day("2026-07-01", "2026-07-01 08:00:00", "2026-07-02 18:00:00");
        d.parts = vec![part("nope", 100, 90, 0), part("2026-07-02", 100, 90, 0)];
        let unit = fold_days(1, &[d]);
        assert_eq!(unit.day_parts.len(), 1);
    }

    #[test]
    fn fold_drops_all_zero_parts() {
        // run_head / lunch_overlap だけが乗った暦日。3 表に列が無いので保存しない
        let mut d = day("2026-07-02", "2026-07-02 08:00:00", "2026-07-03 09:00:00");
        d.parts = vec![
            part("2026-07-02", 0, 0, 0),
            part("2026-07-03", 540, 500, 60),
        ];
        let unit = fold_days(1, &[d]);
        assert_eq!(unit.day_parts.len(), 1);
        assert!(unit.skipped.is_empty(), "0 の日は「落とした」に数えない");
    }

    #[test]
    fn fold_keeps_day_parts_within_1440() {
        let mut d = day("2026-07-01", "2026-07-01 22:00:00", "2026-07-03 13:00:00");
        d.parts = vec![
            part("2026-07-01", 120, 120, 60),
            part("2026-07-02", 1440, 1200, 300),
            part("2026-07-03", 780, 700, 0),
        ];
        let unit = fold_days(1, &[d]);
        assert_eq!(unit.day_parts.len(), 3);
        for p in &unit.day_parts {
            assert!(p.restraint_minutes <= 1440, "{p:?}");
            assert!(p.working_minutes <= 1440, "{p:?}");
        }
    }

    #[test]
    fn fold_skips_rows_with_unparsable_timestamps() {
        let mut d = day("nope", "2026-07-01 08:00:00", "2026-07-01 18:00:00");
        d.date = "nope".to_string();
        assert!(fold_days(1, &[d]).shifts.is_empty());
    }

    fn rows() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"datetime": "2026-07-01 08:00:00", "source": "timecard", "state": "始業"}),
            serde_json::json!({"datetime": "2026-07-01 18:00:00", "source": "timecard", "state": "終業"}),
        ]
    }

    #[test]
    fn fingerprint_is_order_independent_and_hex() {
        let p = KosokuParams::default();
        let a = fingerprint(1, "2026-07", &p, &rows());
        let mut reversed = rows();
        reversed.reverse();
        assert_eq!(a, fingerprint(1, "2026-07", &p, &reversed));
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn fingerprint_changes_with_every_ingredient() {
        let p = KosokuParams::default();
        let base = fingerprint(1, "2026-07", &p, &rows());
        assert_ne!(base, fingerprint(2, "2026-07", &p, &rows()), "乗務員");
        assert_ne!(base, fingerprint(1, "2026-08", &p, &rows()), "月");

        // TOML で再ビルド無しに変えられる設定 — 入れ忘れると古い集計が永久に残る
        let rounded = KosokuParams {
            restraint_rounding: crate::kosoku::RestraintRounding::TruncateElapsed,
            ..p
        };
        assert_ne!(base, fingerprint(1, "2026-07", &rounded, &rows()), "丸め方");
        let threshold = KosokuParams {
            break_threshold_minutes: 11,
            ..p
        };
        assert_ne!(base, fingerprint(1, "2026-07", &threshold, &rows()), "閾値");
        let prescribed = KosokuParams {
            prescribed_minutes: 451,
            ..p
        };
        assert_ne!(
            base,
            fingerprint(1, "2026-07", &prescribed, &rows()),
            "所定"
        );
        let legal = KosokuParams {
            legal_minutes: 481,
            ..p
        };
        assert_ne!(base, fingerprint(1, "2026-07", &legal, &rows()), "法定");

        let mut more = rows();
        more.push(serde_json::json!({"datetime": "2026-07-02 08:00:00", "source": "timecard", "state": "始業"}));
        assert_ne!(base, fingerprint(1, "2026-07", &p, &more), "生行");
    }

    #[test]
    fn fingerprint_folds_the_build_hash() {
        // build.rs が焼く 16 桁 hex。kosoku.rs を 1 バイト直せば必ず変わる
        assert_eq!(output_sha().len(), 16);
        assert!(output_sha().chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// `logic_version` は `CHAR(16)` に収まる 16 桁 hex。
    #[test]
    fn the_logic_version_fits_the_column() {
        let v = logic_version(&KosokuParams::default());
        assert_eq!(v.len(), LOGIC_VERSION_LEN);
        assert!(v.chars().all(|c| c.is_ascii_hexdigit()), "{v}");
    }

    /// **TOML の閾値・丸め方を変えると `logic_version` が変わる。**
    ///
    /// #205 のテスト計画「`restraint_rounding` を切り替えると全単位が stale になる」
    /// の本体。`KINTAI_OUTPUT_SHA` 単体だと変わらず、`SELECT DISTINCT logic_version`
    /// で設定変更由来の stale が捕まえられない。
    #[test]
    fn the_logic_version_changes_with_the_toml_settings() {
        let base = KosokuParams::default();
        let v = logic_version(&base);
        // 出力コードのハッシュは同じまま — 変わっているのは設定だけ
        for (name, changed) in [
            (
                "丸め方",
                KosokuParams {
                    restraint_rounding: crate::kosoku::RestraintRounding::TruncateElapsed,
                    ..base
                },
            ),
            (
                "休憩の閾値",
                KosokuParams {
                    break_threshold_minutes: 11,
                    ..base
                },
            ),
            (
                "所定",
                KosokuParams {
                    prescribed_minutes: 451,
                    ..base
                },
            ),
            (
                "法定",
                KosokuParams {
                    legal_minutes: 481,
                    ..base
                },
            ),
        ] {
            assert_ne!(v, logic_version(&changed), "{name}");
            assert_eq!(logic_version(&changed).len(), LOGIC_VERSION_LEN, "{name}");
        }
    }

    /// 指紋と `logic_version` は**同じ 1 つの定義**から来る。
    ///
    /// 別々に組むと「指紋は変わったのに `logic_version` は据え置き」が作れてしまい、
    /// 保存行の版だけが古いまま残る。
    #[test]
    fn the_fingerprint_is_built_on_the_logic_version() {
        let base = KosokuParams::default();
        let changed = KosokuParams {
            restraint_rounding: crate::kosoku::RestraintRounding::TruncateElapsed,
            ..base
        };
        assert_ne!(logic_version(&base), logic_version(&changed));
        assert_ne!(
            fingerprint(1, "2026-07", &base, &rows()),
            fingerprint(1, "2026-07", &changed, &rows()),
        );
    }

    /// 月が飛んでいても端から端まで 1 本の期間にする。
    #[test]
    fn months_span_covers_the_ends() {
        let span = months_span(&["2026-07".to_string(), "2026-05".to_string()]).unwrap();
        assert_eq!(
            span,
            (
                NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()
            )
        );
        let one = months_span(&["2026-12".to_string()]).unwrap();
        assert_eq!(one.1, NaiveDate::from_ymd_opt(2027, 1, 1).unwrap());
        assert!(months_span(&[]).is_err(), "空は書き先が決まらない");
        assert!(months_span(&["nope".to_string()]).is_err());
    }

    /// **dry-run の件数を実績と読み違えない。**
    #[test]
    fn the_report_says_whether_it_wrote() {
        let dry = FoldReport {
            dry_run: true,
            ..Default::default()
        };
        assert!(dry.dry_run);
        assert!(!dry.has_unexpected());

        // 上流 warnings があれば「想定外」に数える — 欠けた入力で畳んでいる
        let warned = FoldReport {
            warnings: vec!["NoSuchKey".to_string()],
            ..Default::default()
        };
        assert!(warned.has_unexpected());

        // 既知の skip (is_known) は「想定外」に数えない (Refs #205 の 09) —
        // 2026-06 の乗務員 1518 のような零長勤務が毎月実在し、数えると CLI が
        // 毎回 exit 3 になって本当の想定外のシグナルが埋もれる
        let skipped = FoldReport {
            skipped: vec![SkipReason::DegenerateShift {
                start: "a".to_string(),
                end: "a".to_string(),
            }],
            ..Default::default()
        };
        assert!(!skipped.has_unexpected());

        // 既知の skip だけでも、上流 warnings が乗れば「想定外」— 2 つの条件は独立
        let skipped_with_warning = FoldReport {
            skipped: vec![SkipReason::DegenerateShift {
                start: "a".to_string(),
                end: "a".to_string(),
            }],
            warnings: vec!["NoSuchKey".to_string()],
            ..Default::default()
        };
        assert!(
            skipped_with_warning.has_unexpected(),
            "known skip だけを免除しても warnings は免除しない"
        );
    }

    #[test]
    fn now_jst_is_a_jst_timestamp() {
        let s = now_jst();
        assert!(s.ends_with("+09:00"), "{s}");
        assert!(chrono::DateTime::parse_from_rfc3339(&s).is_ok(), "{s}");
    }

    /// 複数乗務員版の SQL は単数版と**式が 1 文字も違わない**
    /// (`crate::kintai_push` の窓署名 SQL と同じ縛り)。写し間違えると
    /// 「中身は同じなのに毎回全乗務員が stale」になり、静かに毎回全書き直しになる。
    #[test]
    fn the_states_sql_matches_the_single_driver_one() {
        let normalise = |s: &str| {
            s.replace("driver_cd = q.driver_cd", "driver_cd = $2")
                .replace(
                    "SELECT q.driver_cd,\n       (SELECT coalesce",
                    "SELECT (SELECT coalesce",
                )
                .replace("\n  FROM unnest($2::int8[]) AS q(driver_cd)", "")
        };
        assert_eq!(
            normalise(STORED_STATES_SQL).replace([' ', '\n'], ""),
            STORED_STATE_SQL.replace([' ', '\n'], "")
        );
    }

    #[test]
    fn stored_state_is_current_only_on_an_exact_match() {
        let unit = fold_days(
            1,
            &[day(
                "2026-07-01",
                "2026-07-01 08:00:00",
                "2026-07-01 18:00:00",
            )],
        );
        let good = StoredState {
            fingerprints: vec!["fp".to_string()],
            shifts: 1,
            day_summaries: 1,
            day_parts: 0,
        };
        assert!(good.is_current(&unit, "fp"));
        assert!(!good.is_current(&unit, "other"), "指紋が違う");

        // 前回が途中で落ちて一部だけ書けている状態を見逃さない
        let partial = StoredState {
            shifts: 1,
            day_summaries: 0,
            ..good.clone()
        };
        assert!(!partial.is_current(&unit, "fp"));

        // 版が混ざっている (前回の書き込みが途中で止まった) なら書き直す
        let mixed = StoredState {
            fingerprints: vec!["fp".to_string(), "old".to_string()],
            ..good.clone()
        };
        assert!(!mixed.is_current(&unit, "fp"));

        let empty = StoredState {
            fingerprints: vec![],
            shifts: 0,
            day_summaries: 0,
            day_parts: 0,
        };
        assert!(!empty.is_current(&unit, "fp"), "1 行も無いなら書く");
    }

    #[test]
    fn stored_state_treats_empty_against_empty_as_current() {
        // 畳める勤務が 1 本も無い乗務員。書くものが無いので指紋も載らず、
        // stale 扱いにすると毎回 drivers_written に乗る
        let empty_unit = fold_days(1, &[]);
        assert!(empty_unit.is_empty());
        let empty = StoredState {
            fingerprints: vec![],
            shifts: 0,
            day_summaries: 0,
            day_parts: 0,
        };
        assert!(empty.is_empty());
        assert!(empty.is_current(&empty_unit, "fp"));
        // 指紋が何であっても当たる — 突き合わせる行がそもそも無い
        assert!(empty.is_current(&empty_unit, "other"));

        // 前回書いた行が残っているなら、今回が空でも消しに行く
        let stored = StoredState {
            fingerprints: vec!["fp".to_string()],
            shifts: 1,
            day_summaries: 1,
            day_parts: 0,
        };
        assert!(
            !stored.is_current(&empty_unit, "fp"),
            "空にするのも書き込み"
        );
    }

    #[test]
    fn month_date_bounds_wraps_the_year() {
        assert_eq!(
            month_date_bounds("2026-12"),
            Some((
                NaiveDate::from_ymd_opt(2026, 12, 1).unwrap(),
                NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()
            ))
        );
        assert_eq!(
            month_date_bounds("2026-07"),
            Some((
                NaiveDate::from_ymd_opt(2026, 7, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()
            ))
        );
        assert_eq!(month_date_bounds("nope"), None);
        assert_eq!(month_date_bounds("2026-13"), None);
    }

    #[test]
    fn fold_driver_month_runs_the_read_path_pipeline() {
        let p = KosokuParams::default();
        let (unit, fp) = fold_driver_month(1130, "2026-07", &p, rows());
        assert_eq!(unit.shifts.len(), 1, "始業/終業の対から勤務が 1 本");
        assert_eq!(unit.day_summaries[0].restraint_minutes, 600);
        assert_eq!(fp.len(), 64);
    }

    #[test]
    fn fold_driver_month_drops_duplicate_rows_like_the_read_path() {
        let p = KosokuParams::default();
        let mut dup = rows();
        dup.extend(rows());
        let (unit, _) = fold_driver_month(1130, "2026-07", &p, dup);
        assert_eq!(unit.shifts.len(), 1, "重複しても勤務は 1 本");
    }

    #[test]
    fn report_knows_when_it_wrote() {
        let mut r = FoldReport::default();
        assert!(!r.wrote_anything());
        r.drivers_written = 1;
        assert!(r.wrote_anything());
    }

    /// 既知の skip は**報告に残るが非 0 終了にしない**。
    ///
    /// 2026-06 の乗務員 1518 (06-17 19:09、start = end) のような零長勤務は毎月
    /// 実在する。数えると CLI が毎回 exit 3 になり、本当の想定外が埋もれる。
    #[test]
    fn known_skips_are_reported_but_not_unexpected() {
        let degenerate = day("2026-06-17", "2026-06-17 19:09:00", "2026-06-17 19:09:00");
        let mut early_part = day("2026-07-02", "2026-07-02 00:30:00", "2026-07-02 18:00:00");
        early_part.parts = vec![part("2026-07-01", 5, 0, 0)];
        let unit = fold_days(1518, &[degenerate, early_part]);

        let report = FoldReport {
            skipped: unit.skipped.clone(),
            ..Default::default()
        };
        assert_eq!(report.skipped.len(), 2, "落としたことは報告に残す");
        assert!(matches!(
            report.skipped[0],
            SkipReason::DegenerateShift { .. }
        ));
        assert!(matches!(
            report.skipped[1],
            SkipReason::PartBeforeShift { .. }
        ));
        assert!(!report.has_unexpected(), "既知の形は想定外に数えない");

        let sync = SyncReport {
            push: crate::kintai_push::PushReport::default(),
            fold: report,
        };
        assert!(!sync.has_unexpected(), "sync も同じ");
    }

    /// 上流由来の想定外は引き続き数える (`sync` は push の判定をそのまま含む)。
    #[test]
    fn sync_still_counts_rejected_rows_and_unknown_states() {
        let skipped = fold_days(
            1,
            &[day(
                "2026-06-17",
                "2026-06-17 19:09:00",
                "2026-06-17 19:09:00",
            )],
        )
        .skipped;
        let fold = FoldReport {
            skipped,
            ..Default::default()
        };
        assert!(!fold.has_unexpected());

        let mut push = crate::kintai_push::PushReport::default();
        push.rejected
            .insert(crate::kintai_push::RejectReason::NoDriver, 1);
        let with_rejected = SyncReport {
            push: push.clone(),
            fold: fold.clone(),
        };
        assert!(with_rejected.has_unexpected(), "読み飛ばした行は想定外");

        let mut push = crate::kintai_push::PushReport::default();
        push.unknown_states.insert("知らない".to_string());
        let with_unknown_state = SyncReport { push, fold };
        assert!(
            with_unknown_state.has_unexpected(),
            "CHECK に無い state は想定外"
        );
    }

    #[test]
    fn day_bounds_is_jst_midnight() {
        let (a, b) = day_bounds(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
        assert_eq!(a.to_rfc3339(), "2026-07-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2026-07-02T00:00:00+09:00");
    }

    // ── push 窓ずれの検知 (Refs #205 の 30) ──────────────────────────────

    fn ev(source: &str, at: &str) -> serde_json::Value {
        serde_json::json!({"datetime": at, "source": source, "state": "始業"})
    }

    fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
    }

    #[test]
    fn push_window_gap_warns_when_the_spill_day_has_no_punch() {
        // 2026-06 の fold は 07-01 まで読むが、push が書くのは 06-30 まで。
        // 07-01 に打刻が 1 行も無い = 7 月が未 push の疑い (実測の 1021 の形)
        let rows = vec![ev("timecard", "2026-06-21 06:12:34")];
        let w = push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15));
        assert!(
            w.is_some_and(|w| w.contains("2026-07-01")),
            "07-01 を名指しする"
        );
    }

    #[test]
    fn push_window_gap_is_quiet_when_the_spill_day_has_a_punch() {
        // 7 月が push 済み = はみ出した先に打刻が在る
        let rows = vec![
            ev("timecard", "2026-06-21 06:12:34"),
            ev("timecard", "2026-07-01 14:54:29"),
        ];
        assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15)).is_none());
    }

    #[test]
    fn push_window_gap_is_quiet_while_the_spill_day_is_still_ahead() {
        // 当月を畳んでいる最中。はみ出した先が未来なので打刻が無いのが正常
        let rows = vec![ev("timecard", "2026-06-21 06:12:34")];
        assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 6, 25)).is_none());
        // 翌月初そのものの日もまだ鳴らさない (その日はまだ進行中)
        assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 1)).is_none());
    }

    #[test]
    fn push_window_gap_ignores_dtako_events_in_the_spill_day() {
        // `dtako_events` は push を通らず alc から直に来るので push の証拠にならない
        let rows = vec![
            ev("timecard", "2026-06-21 06:12:34"),
            ev("dtako_events", "2026-07-01 09:00:00"),
        ];
        assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15)).is_some());
    }

    #[test]
    fn push_window_gap_counts_the_other_pushed_source() {
        // `dtako` (運行開始/終了) も push が運ぶので、在れば push 済みの証拠になる
        let rows = vec![ev("dtako", "2026-07-01 09:00:00")];
        assert!(push_window_gap_warning("2026-06", &rows, ymd(2026, 7, 15)).is_none());
    }

    #[test]
    fn push_window_gap_returns_none_for_a_bad_month() {
        assert!(push_window_gap_warning("nope", &[], ymd(2026, 7, 15)).is_none());
    }

    #[test]
    fn today_jst_is_a_real_date() {
        // 呼べること自体の確認 (JST offset の expect が外れないこと)
        assert!(today_jst() > ymd(2020, 1, 1));
    }
}
