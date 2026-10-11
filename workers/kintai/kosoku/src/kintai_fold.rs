//! 畳んだ結果 (`kintai.shifts` / `day_summaries` / `day_parts`) と月ゲートの書き込みの純粋部分
//! (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/kintai_fold.rs` から移した。**DB も I/O も持たない** — 3 表の行の型・`DaySummary` から行への
//! 写し・保存済みの姿の判定・SQL 定数・bind に渡す「列ごとの Vec の束」まで。bind と transaction は root (sqlx) と
//! 勤怠 Worker がそれぞれ持つ。指紋・`logic_version`・1 乗務員 1 か月の畳み・報告の型もここ (版の材料
//! `output_sha` と時計は呼び出し側が渡す。`env!` は root の `build.rs` 側に残る)。
//! 束の中身と SQL の文字列は移す前と同じ (`tests/pg_write_snapshot.rs` が基点の値で縛る)。

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use sha2::{Digest, Sha256};

use crate::anchors::HeadAnchors;
use crate::kintai_push::{month_date_bounds, INSERT_CHUNK, JST_OFFSET_SECONDS};
use crate::kosoku::{
    daily_summary, drop_duplicate_rows, DaySummary, KosokuParams, NonWorking, ShiftSource,
};
use crate::window::parse_dt;

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
    /// 実働でない区間 (009 の `non_working`)。区間なしは空 = `[]` を書く (NULL は書かない)。
    pub non_working: Vec<NonWorking>,
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
    /// 既知の形は「想定外」に数えない (root の `FoldReport::has_unexpected`) —
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
            non_working: d.non_working.clone(),
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

// ── 指紋・`logic_version`・畳み・報告 (Refs #322) ─────────────────────────────
//
// root の `src/kintai_fold.rs` から移した。版の材料 (`KINTAI_OUTPUT_SHA`) は root の `build.rs` が焼くので、
// ここは `output_sha` を引数で受ける (`env!` を持たない)。時計 (`now`) も引数で受ける。

/// `logic_version` の桁数。DDL の `CHAR(16)` に合わせる。
pub const LOGIC_VERSION_LEN: usize = 16;

/// 保存行に焼く「コード + 設定」の印 (16 桁 hex) = `sha256(output_sha | KosokuParams の Debug)` の先頭 16 桁。
///
/// `shifts.logic_version` / `day_summaries.logic_version` (`CHAR(16)`) にそのまま入り、[`fingerprint`] の
/// 材料の先頭でもある。**両方が同じ 1 つの定義から来る**ので「指紋は変わったのに版は据え置き」が作れない。
/// `output_sha` 単体にしないのは、TOML で再ビルド無しに変えられる閾値・丸め方も出力を変えるため。
pub fn logic_version(params: &KosokuParams, output_sha: &str) -> String {
    let mut h = Sha256::new();
    h.update(output_sha.as_bytes());
    h.update(b"|");
    h.update(format!("{params:?}").as_bytes());
    format!("{:x}", h.finalize())[..LOGIC_VERSION_LEN].to_string()
}

/// 指紋 = `sha256(logic_version | 乗務員CD|対象月 | 生行を正規化して並べたもの)`。行の順には依らない。
pub fn fingerprint(
    driver_cd: i64,
    month: &str,
    params: &KosokuParams,
    rows: &[serde_json::Value],
    output_sha: &str,
) -> String {
    let mut lines: Vec<String> = rows.iter().map(|r| r.to_string()).collect();
    lines.sort();
    let mut h = Sha256::new();
    h.update(logic_version(params, output_sha).as_bytes());
    h.update(b"|");
    h.update(format!("{driver_cd}|{month}").as_bytes());
    h.update(b"|");
    h.update(lines.join("\n").as_bytes());
    format!("{:x}", h.finalize())
}

/// 1 乗務員 1 か月を畳む。読み出し経路と同じ手順 (重複を落としてから `daily_summary`)。
pub fn fold_driver_month(
    driver_cd: i64,
    month: &str,
    params: &KosokuParams,
    rows: Vec<serde_json::Value>,
    output_sha: &str,
) -> (FoldUnit, String) {
    let fp = fingerprint(driver_cd, month, params, &rows, output_sha);
    let (rows, _duplicates) = drop_duplicate_rows(rows);
    let days = daily_summary(&rows, month, params);
    (fold_days(driver_cd, &days), fp)
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
    ///
    /// **診断専用 (tail gap) の警告も含む** — 月ゲートの封を止めるかどうかは
    /// この `Vec` の空/非空では判定しない (Refs #205-51、root の
    /// `kintai_http_repo::warnings_seen` 参照)。降格であって削除ではない
    /// ので、鳴っていることはここから今までどおり読める。
    pub warnings: Vec<String>,
    /// 畳むのにかかった時間 (ms)。
    ///
    /// 窓の受け口はこれを proxy の 100 秒に収める必要があるので、実測値を出す。
    /// 1 ページの乗務員数を決めるのもこの値 (root の `kintai_recalc` のモジュール docs)。
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

/// 空の [`FoldReport`]。**版と計算時刻は 1 行も書かなくても載せる** — 呼び出し側が
/// 「どの版で畳んだ結果か」を必ず読めるようにするため (#205 のリスク欄の筆頭)。
pub fn new_report(
    params: &KosokuParams,
    apply: bool,
    output_sha: &str,
    now: DateTime<FixedOffset>,
) -> FoldReport {
    FoldReport {
        dry_run: !apply,
        logic_version: logic_version(params, output_sha),
        calculated_at: now.to_rfc3339(),
        ..Default::default()
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

/// 月ゲートの判定結果 (実装計画 13)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MonthGate {
    /// 一致した — 呼び出し側はこの [`FoldReport`] をそのまま返してよい。
    /// 月の生イベントの読みを 1 バイトも払っていない。
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

/// 突合に入れる運行を**乗務員ごとの窓**に絞る (Refs ohishi-exp/nuxt-dtako-admin#1123)。
///
/// 取得は全員の始端 (`from_global`) から 1 回だが、fold の入力は乗務員ごとに
/// `[起点 or 月初, to)` へ切り戻している ([`crate::anchors::clip_to_anchors`])。突合も同じにしないと、
/// 1 人の起点で**起点の無い他の乗務員の前月末の運行**まで数え、そこに GCP 側の欠けが
/// あると fold が読みもしない運行で「dtako 入力欠け」が立ち当月の封を止める。
///
/// 材料が暦日 (`last_date`) しか持たないので、判定は日単位 — 運行の最後の記録が
/// 窓の始端の日以降なら入れる。起点の無い乗務員は始端が月初 0:00 なので正確、
/// 起点のある乗務員は**起点の日のうち起点より前**に終わった運行まで入る (広い側)。
pub fn operations_in_driver_windows(
    rows: Vec<(i64, String, NaiveDate, NaiveDate)>,
    month: &str,
    anchors: &HeadAnchors,
) -> Vec<(i64, String, NaiveDate, NaiveDate)> {
    let Some((first, _)) = month_date_bounds(month) else {
        return rows;
    };
    let start = |cd: i64| {
        let anchor = u64::try_from(cd).ok().and_then(|d| anchors.get(&d));
        anchor.and_then(|a| parse_dt(a)).map_or(first, |a| a.date())
    };
    rows.into_iter()
        .filter(|(cd, _, _, last)| *last >= start(*cd))
        .collect()
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
    /// `drivers_written` に乗って root の `FoldReport::wrote_anything` が誤検知し、
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

pub const STORED_STATE_SQL: &str = r#"
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
pub const STORED_STATES_SQL: &str = r#"
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
pub const STALE_STATE_SQL: &str = r#"
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

/// `shifts` を消すと `day_summaries` / `day_parts` は FK の CASCADE で消える。
pub const DELETE_SHIFTS_SQL: &str = r#"
DELETE FROM kintai.shifts
 WHERE tenant_id = $1 AND driver_cd = $2 AND date_start >= $3 AND date_start < $4
"#;

/// 入れる勤務を **1 文で**。列ごとの配列を `unnest` で行に開く。
///
/// `fingerprint` / `logic_version` は単位ぜんたいで 1 つなので配列にしない。
pub const INSERT_SHIFTS_SQL: &str = r#"
INSERT INTO kintai.shifts
       (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
SELECT $1, d.driver_cd, d.start_at, d.end_at, d.shift_source, $6, $7
  FROM unnest($2::int8[], $3::timestamptz[], $4::timestamptz[], $5::text[])
       AS d(driver_cd, start_at, end_at, shift_source)
"#;

/// 入れる日別サマリを **1 文で**。分の列は 11 本あるが全部 `int4` の配列。
/// `non_working` は行ごとに JSON の配列 1 つ (`jsonb[]` の要素 1 つ = 1 行ぶん)。
pub const INSERT_DAY_SUMMARIES_SQL: &str = r#"
INSERT INTO kintai.day_summaries
       (tenant_id, driver_cd, date, shift_start_at, shift_source,
        restraint_minutes, working_minutes, break_minutes, rest_minus_minutes,
        statutory_minutes, within_statutory_overtime_minutes, overtime_minutes,
        legal_holiday_minutes, night_minutes, overtime_night_minutes,
        legal_holiday_night_minutes, non_working, fingerprint, logic_version)
SELECT $1, d.driver_cd, d.date, d.shift_start_at, d.shift_source,
       d.restraint_minutes, d.working_minutes, d.break_minutes, d.rest_minus_minutes,
       d.statutory_minutes, d.within_statutory_overtime_minutes, d.overtime_minutes,
       d.legal_holiday_minutes, d.night_minutes, d.overtime_night_minutes,
       d.legal_holiday_night_minutes, d.non_working, $18, $19
  FROM unnest($2::int8[], $3::date[], $4::timestamptz[], $5::text[],
              $6::int4[], $7::int4[], $8::int4[], $9::int4[],
              $10::int4[], $11::int4[], $12::int4[],
              $13::int4[], $14::int4[], $15::int4[], $16::int4[], $17::jsonb[])
       AS d(driver_cd, date, shift_start_at, shift_source,
            restraint_minutes, working_minutes, break_minutes, rest_minus_minutes,
            statutory_minutes, within_statutory_overtime_minutes, overtime_minutes,
            legal_holiday_minutes, night_minutes, overtime_night_minutes,
            legal_holiday_night_minutes, non_working)
"#;

/// 入れる暦日ビューを **1 文で**。
pub const INSERT_DAY_PARTS_SQL: &str = r#"
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

// ── 13: 月ゲート ─────────────────────────────────────────────────────────

pub const FOLD_GATE_SELECT_SQL: &str = r#"
SELECT dtako_digest, punch_digest, logic_version
  FROM kintai.fold_gate
 WHERE tenant_id = $1 AND month = $2
"#;

/// 書いてよいのは「月まるごとを 1 呼び出しで完結させた」ときだけ。
/// root の `recalc_month` (`driver` 省略) と、それを満たした
/// root の `routes::kintai_recalc::run` の 1 ページが両方これに当たる —
/// 判定条件はそれぞれの docs 参照。
pub const FOLD_GATE_UPSERT_SQL: &str = r#"
INSERT INTO kintai.fold_gate (tenant_id, month, dtako_digest, punch_digest, logic_version, folded_at)
VALUES ($1, $2, $3, $4, $5, now())
ON CONFLICT (tenant_id, month) DO UPDATE
   SET dtako_digest = EXCLUDED.dtako_digest,
       punch_digest = EXCLUDED.punch_digest,
       logic_version = EXCLUDED.logic_version,
       folded_at = EXCLUDED.folded_at
"#;

// ── bind の束 (Refs #322) ────────────────────────────────────────────────────

/// JST の壁時計を `TIMESTAMPTZ` へ。
pub fn tz(dt: NaiveDateTime) -> DateTime<FixedOffset> {
    FixedOffset::east_opt(JST_OFFSET_SECONDS)
        .expect("JST offset is in range")
        .from_local_datetime(&dt)
        .single()
        .expect("JST has no DST gap")
}

/// [`INSERT_SHIFTS_SQL`] の `$2`〜`$5` (int8[]・timestamptz[]・timestamptz[]・text[])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShiftColumns {
    pub driver_cd: Vec<i64>,
    pub start_at: Vec<DateTime<FixedOffset>>,
    pub end_at: Vec<DateTime<FixedOffset>>,
    pub shift_source: Vec<String>,
}

/// [`INSERT_DAY_SUMMARIES_SQL`] の `$2`〜`$17`。分の 11 列は int4[] (`minutes` の並びは SQL の `$6`〜`$16` の順)、
/// `non_working` は行ごとに JSON の配列 1 つ (jsonb[] の要素 1 つ = 1 行ぶん)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaySummaryColumns {
    pub driver_cd: Vec<i64>,
    pub date: Vec<NaiveDate>,
    pub shift_start_at: Vec<DateTime<FixedOffset>>,
    pub shift_source: Vec<String>,
    pub minutes: [Vec<i32>; 11],
    pub non_working: Vec<serde_json::Value>,
}

/// [`INSERT_DAY_PARTS_SQL`] の `$2`〜`$7` (int8[]・timestamptz[]・date[]・int4[] × 3)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayPartColumns {
    pub driver_cd: Vec<i64>,
    pub shift_start_at: Vec<DateTime<FixedOffset>>,
    pub date: Vec<NaiveDate>,
    pub restraint_minutes: Vec<i32>,
    pub working_minutes: Vec<i32>,
    pub night_minutes: Vec<i32>,
}

/// `shifts` を [`INSERT_CHUNK`] 行ごとの束に (1 束 = INSERT 1 文)。
pub fn shift_columns(unit: &FoldUnit) -> Vec<ShiftColumns> {
    shift_columns_by(unit, INSERT_CHUNK)
}

fn shift_columns_by(unit: &FoldUnit, chunk_rows: usize) -> Vec<ShiftColumns> {
    unit.shifts
        .chunks(chunk_rows)
        .map(|chunk| ShiftColumns {
            driver_cd: chunk.iter().map(|s| s.driver_cd).collect(),
            start_at: chunk.iter().map(|s| tz(s.start_at)).collect(),
            end_at: chunk.iter().map(|s| tz(s.end_at)).collect(),
            shift_source: chunk.iter().map(|s| s.shift_source.to_string()).collect(),
        })
        .collect()
}

/// `day_summaries` を [`INSERT_CHUNK`] 行ごとの束に。分の列は元と同じく `as i32`。
pub fn day_summary_columns(unit: &FoldUnit) -> Vec<DaySummaryColumns> {
    day_summary_columns_by(unit, INSERT_CHUNK)
}

fn day_summary_columns_by(unit: &FoldUnit, chunk_rows: usize) -> Vec<DaySummaryColumns> {
    unit.day_summaries
        .chunks(chunk_rows)
        .map(|chunk| {
            let col = |f: fn(&DaySummaryRow) -> i64| -> Vec<i32> {
                chunk.iter().map(|d| f(d) as i32).collect()
            };
            DaySummaryColumns {
                driver_cd: chunk.iter().map(|d| d.driver_cd).collect(),
                date: chunk.iter().map(|d| d.date).collect(),
                shift_start_at: chunk.iter().map(|d| tz(d.shift_start_at)).collect(),
                shift_source: chunk.iter().map(|d| d.shift_source.to_string()).collect(),
                minutes: [
                    col(|d| d.restraint_minutes),
                    col(|d| d.working_minutes),
                    col(|d| d.break_minutes),
                    col(|d| d.rest_minus_minutes),
                    col(|d| d.statutory_minutes),
                    col(|d| d.within_statutory_overtime_minutes),
                    col(|d| d.overtime_minutes),
                    col(|d| d.legal_holiday_minutes),
                    col(|d| d.night_minutes),
                    col(|d| d.overtime_night_minutes),
                    col(|d| d.legal_holiday_night_minutes),
                ],
                non_working: chunk
                    .iter()
                    .map(|d| non_working_json(&d.non_working))
                    .collect(),
            }
        })
        .collect()
}

/// `non_working` の 1 行ぶん (区間なしは `[]`。NULL は書かない)。
pub fn non_working_json(spans: &[NonWorking]) -> serde_json::Value {
    serde_json::to_value(spans).expect("NonWorking は文字列と enum だけ")
}

/// `day_parts` を [`INSERT_CHUNK`] 行ごとの束に。
pub fn day_part_columns(unit: &FoldUnit) -> Vec<DayPartColumns> {
    day_part_columns_by(unit, INSERT_CHUNK)
}

fn day_part_columns_by(unit: &FoldUnit, chunk_rows: usize) -> Vec<DayPartColumns> {
    unit.day_parts
        .chunks(chunk_rows)
        .map(|chunk| DayPartColumns {
            driver_cd: chunk.iter().map(|p| p.driver_cd).collect(),
            shift_start_at: chunk.iter().map(|p| tz(p.shift_start_at)).collect(),
            date: chunk.iter().map(|p| p.date).collect(),
            restraint_minutes: chunk.iter().map(|p| p.restraint_minutes as i32).collect(),
            working_minutes: chunk.iter().map(|p| p.working_minutes as i32).collect(),
            night_minutes: chunk.iter().map(|p| p.night_minutes as i32).collect(),
        })
        .collect()
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
            non_working: Vec::new(),
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
    fn every_skip_reason_is_known() {
        assert!(SkipReason::DegenerateShift {
            start: String::new(),
            end: String::new()
        }
        .is_known());
        assert!(SkipReason::PartBeforeShift {
            shift_start: String::new(),
            date: String::new()
        }
        .is_known());
    }

    #[test]
    fn the_bind_columns_are_chunked_per_table() {
        let mut d = day("2026-07-01", "2026-07-01 22:00:00", "2026-07-02 08:00:00");
        d.parts = vec![
            part("2026-07-01", 120, 120, 60),
            part("2026-07-02", 480, 420, 300),
        ];
        let unit = fold_days(
            1,
            &[
                d,
                day("2026-07-03", "2026-07-03 08:00:00", "2026-07-03 17:00:00"),
            ],
        );
        assert_eq!(shift_columns(&unit).len(), 1);
        assert_eq!(day_summary_columns(&unit).len(), 1);
        assert_eq!(day_part_columns(&unit).len(), 1);
        let shifts = shift_columns_by(&unit, 1);
        assert_eq!(shifts.len(), 2);
        assert_eq!(
            shifts[0].start_at[0].to_rfc3339(),
            "2026-07-01T22:00:00+09:00"
        );
        let days = day_summary_columns_by(&unit, 1);
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].minutes[0], vec![600]);
        assert_eq!(days[0].non_working, vec![serde_json::json!([])]);
        let parts = day_part_columns_by(&unit, 1);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[1].night_minutes, vec![300]);
        assert!(shift_columns(&FoldUnit::default()).is_empty());
    }

    // ── 指紋・logic_version・畳み・報告 (root から移した) ──

    /// 版の材料の代わりに使う固定値 (root では `build.rs` が焼く 16 桁 hex)。
    const SHA: &str = "0123456789abcdef";

    fn rows() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"datetime": "2026-07-01 08:00:00", "source": "timecard", "state": "始業"}),
            serde_json::json!({"datetime": "2026-07-01 18:00:00", "source": "timecard", "state": "終業"}),
        ]
    }

    fn ymd(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("valid date")
    }

    /// **移す前の root の式で出る値**に縛る (`sha256("0123456789abcdef|" + KosokuParams::default() の Debug)`
    /// の先頭 16 桁と、その版で 2 行を畳んだ指紋。どちらも移す前に root の式を python で再計算して得た値)。
    /// これが動いたら保存行の版と指紋が全部変わる — 移しただけで値が変わってはいけない。
    #[test]
    fn the_values_are_the_ones_the_root_computed_before_the_move() {
        let p = KosokuParams::default();
        assert_eq!(logic_version(&p, SHA), "a6ae550be16b63c3");
        assert_eq!(
            fingerprint(1, "2026-07", &p, &rows(), SHA),
            "9cb376c435693319a74f7d6bed7aaf710244ef448356efa970a3501192ac7180"
        );
    }

    #[test]
    fn the_logic_version_folds_the_output_sha() {
        let p = KosokuParams::default();
        assert_ne!(
            logic_version(&p, SHA),
            logic_version(&p, "fedcba9876543210")
        );
    }

    #[test]
    fn fingerprint_is_order_independent_and_hex() {
        let p = KosokuParams::default();
        let a = fingerprint(1, "2026-07", &p, &rows(), SHA);
        let mut reversed = rows();
        reversed.reverse();
        assert_eq!(a, fingerprint(1, "2026-07", &p, &reversed, SHA));
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn fingerprint_changes_with_every_ingredient() {
        let p = KosokuParams::default();
        let fp = |cd: i64, m: &str, p: &KosokuParams, r: &[serde_json::Value]| {
            fingerprint(cd, m, p, r, SHA)
        };
        let base = fp(1, "2026-07", &p, &rows());
        assert_ne!(base, fp(2, "2026-07", &p, &rows()), "乗務員");
        assert_ne!(base, fp(1, "2026-08", &p, &rows()), "月");
        assert_ne!(base, fingerprint(1, "2026-07", &p, &rows(), "x"), "版");

        // TOML で再ビルド無しに変えられる設定 — 入れ忘れると古い集計が永久に残る
        let rounded = KosokuParams {
            restraint_rounding: crate::kosoku::RestraintRounding::TruncateElapsed,
            ..p
        };
        assert_ne!(base, fp(1, "2026-07", &rounded, &rows()), "丸め方");
        let threshold = KosokuParams {
            break_threshold_minutes: 11,
            ..p
        };
        assert_ne!(base, fp(1, "2026-07", &threshold, &rows()), "閾値");
        let prescribed = KosokuParams {
            prescribed_minutes: 451,
            ..p
        };
        assert_ne!(base, fp(1, "2026-07", &prescribed, &rows()), "所定");
        let legal = KosokuParams {
            legal_minutes: 481,
            ..p
        };
        assert_ne!(base, fp(1, "2026-07", &legal, &rows()), "法定");

        let mut more = rows();
        more.push(serde_json::json!({"datetime": "2026-07-02 08:00:00", "source": "timecard", "state": "始業"}));
        assert_ne!(base, fp(1, "2026-07", &p, &more), "生行");
    }

    /// `logic_version` は `CHAR(16)` に収まる 16 桁 hex。
    #[test]
    fn the_logic_version_fits_the_column() {
        let v = logic_version(&KosokuParams::default(), SHA);
        assert_eq!(v.len(), LOGIC_VERSION_LEN);
        assert!(v.chars().all(|c| c.is_ascii_hexdigit()), "{v}");
    }

    /// **TOML の閾値・丸め方を変えると `logic_version` が変わる。**
    ///
    /// #205 のテスト計画「`restraint_rounding` を切り替えると全単位が stale になる」
    /// の本体。版の材料単体だと変わらず、`SELECT DISTINCT logic_version`
    /// で設定変更由来の stale が捕まえられない。
    #[test]
    fn the_logic_version_changes_with_the_toml_settings() {
        let base = KosokuParams::default();
        let v = logic_version(&base, SHA);
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
            assert_ne!(v, logic_version(&changed, SHA), "{name}");
            assert_eq!(
                logic_version(&changed, SHA).len(),
                LOGIC_VERSION_LEN,
                "{name}"
            );
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
        assert_ne!(logic_version(&base, SHA), logic_version(&changed, SHA));
        assert_ne!(
            fingerprint(1, "2026-07", &base, &rows(), SHA),
            fingerprint(1, "2026-07", &changed, &rows(), SHA),
        );
    }

    #[test]
    fn fold_driver_month_runs_the_read_path_pipeline() {
        let p = KosokuParams::default();
        let (unit, fp) = fold_driver_month(1130, "2026-07", &p, rows(), SHA);
        assert_eq!(unit.shifts.len(), 1, "始業/終業の対から勤務が 1 本");
        assert_eq!(unit.day_summaries[0].restraint_minutes, 600);
        assert_eq!(fp, fingerprint(1130, "2026-07", &p, &rows(), SHA));
    }

    #[test]
    fn fold_driver_month_drops_duplicate_rows_like_the_read_path() {
        let p = KosokuParams::default();
        let mut dup = rows();
        dup.extend(rows());
        let (unit, _) = fold_driver_month(1130, "2026-07", &p, dup, SHA);
        assert_eq!(unit.shifts.len(), 1, "重複しても勤務は 1 本");
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

        // 既知の skip (is_known) は「想定外」に数えない (Refs #205 の 09)
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
            warnings: vec!["NoSuchKey".to_string()],
            ..skipped
        };
        assert!(skipped_with_warning.has_unexpected());
    }

    #[test]
    fn report_knows_when_it_wrote() {
        let mut r = FoldReport::default();
        assert!(!r.wrote_anything());
        r.drivers_written = 1;
        assert!(r.wrote_anything());
    }

    /// 空の報告にも版と時刻が載る (時計は呼び出し側が渡す)。
    #[test]
    fn a_new_report_carries_the_version_and_the_time() {
        let p = KosokuParams::default();
        let now = tz(ymd(2026, 7, 1).and_hms_opt(9, 30, 0).unwrap());
        let r = new_report(&p, false, SHA, now);
        assert!(r.dry_run);
        assert_eq!(r.logic_version, logic_version(&p, SHA));
        assert_eq!(r.calculated_at, "2026-07-01T09:30:00+09:00");
        assert!(!new_report(&p, true, SHA, now).dry_run);
        let gate = MonthGate::Hit(r.clone());
        assert_ne!(gate, MonthGate::Unavailable);
        assert_eq!(StaleReport::default().drivers, 0);
    }

    /// 突合は乗務員ごとの窓 (Refs ohishi-exp/nuxt-dtako-admin#1123)。起点の無い乗務員の
    /// 前月末の運行は出ず、起点のある乗務員の起点以降の前月の運行は出る。
    #[test]
    fn unko_diff_counts_operations_in_each_drivers_window() {
        let op = |cd: i64, u: &str, f: NaiveDate, l: NaiveDate| (cd, u.to_string(), f, l);
        let rows = vec![
            // 1731 (起点 2/21 05:00) の閉じ忘れ運行 — 出る
            op(1731, "A", ymd(2026, 2, 21), ymd(2026, 3, 2)),
            // 1731 の起点より前の日に終わった運行 — 出ない
            op(1731, "B", ymd(2026, 2, 18), ymd(2026, 2, 20)),
            // 起点の無い 1130 の前月末の運行 — 出ない (from_global では読まれるが)
            op(1130, "C", ymd(2026, 2, 25), ymd(2026, 2, 27)),
            // 1130 の月初をまたぐ運行 (最後の記録が当月) — 今までどおり出る
            op(1130, "D", ymd(2026, 2, 28), ymd(2026, 3, 1)),
            op(1130, "E", ymd(2026, 3, 10), ymd(2026, 3, 10)),
        ];
        let anchors: HeadAnchors = [(1731, "2026-02-21 05:00:00".to_string())]
            .into_iter()
            .collect();
        let got: Vec<String> = operations_in_driver_windows(rows.clone(), "2026-03", &anchors)
            .into_iter()
            .map(|(_, u, _, _)| u)
            .collect();
        assert_eq!(got, vec!["A", "D", "E"]);
        // 起点が無ければ全員月初から (前月末だけの運行は落ちる)
        let none = operations_in_driver_windows(rows.clone(), "2026-03", &HeadAnchors::new());
        let none: Vec<&str> = none.iter().map(|(_, u, _, _)| u.as_str()).collect();
        assert_eq!(none, vec!["A", "D", "E"]);
        // 月が壊れていれば絞らない
        assert_eq!(
            operations_in_driver_windows(rows.clone(), "x", &anchors),
            rows
        );
    }
}
