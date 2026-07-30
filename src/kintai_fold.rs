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
//! `/api/kintai/kosoku-daily` の全乗務員経路 (`routes/kintai.rs`) と**同じ順序**で
//! 呼ぶ — 違う手順で畳むと、画面と保存値が食い違ったときに原因が追えない。
//!
//! 1. `drop_duplicate_rows` (取り込みが 2 回走った重複を落とす)
//! 2. `daily_summary`
//!
//! **フェリー控除 (`apply_ferry_minus`) は呼ばない。** あれが埋めるのは
//! `ferry_minus_minutes` だけで、これは 3 表のどこにも列が無い (紙との差を説明する
//! ためだけの値で、拘束にも実働にも入っていない)。呼んでも保存値は 1 つも変わらず、
//! 乗務員ごとに 1 往復増えるだけになる。
//!
//! ## 指紋 — 何が変わったら再計算するか
//!
//! ```text
//! fingerprint = sha256(
//!     KINTAI_OUTPUT_SHA        // build.rs が焼く「出力に効くコード」の内容ハッシュ
//!   + "|" + KosokuParams        // 再ビルド無しで変わる TOML の閾値・丸め方
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
//! ## 単位は (乗務員, 月)
//!
//! issue は「指紋が変わった (乗務員, 日) だけ再計算」と書いているが、**勤務は日を
//! 跨ぐので日を単独では計算できない** (issue 自身も「再計算の対象は差分日 ± 2 日」と
//! している)。月単位のバッチではそれを最後まで広げた形 = (乗務員, 月) が素直で、
//! 差分日 ± 2 日を必ず含む。指紋は行ごとに持たせるので、
//! **書き込みは「その乗務員の月が変わったときだけ」**に絞られる。

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime};
use sha2::{Digest, Sha256};

use crate::kintai_push::{jst_day_bounds, KintaiPgStore, KintaiPushError, DATETIME_FORMAT};
use crate::kintai_repo::{month_range, DynKintaiEventsRepo};
use crate::kosoku::{daily_summary, drop_duplicate_rows, DaySummary, KosokuParams, ShiftSource};

/// `build.rs` が焼き込む「出力に効くコード」の内容ハッシュ (16 桁 hex)。
/// `shifts.logic_version` / `day_summaries.logic_version` (`CHAR(16)`) にそのまま入る。
pub fn logic_version() -> &'static str {
    env!("KINTAI_OUTPUT_SHA")
}

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

/// 3 表に写せなかったもの。
#[derive(Debug, Clone, PartialEq, Eq)]
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
    h.update(logic_version().as_bytes());
    h.update(b"|");
    h.update(format!("{params:?}").as_bytes());
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
    /// 書かなくてよいか。
    ///
    /// 指紋が 1 種類でそれが今回の指紋と同じ、**かつ** 3 表の行数が今回と同じとき
    /// だけスキップする。行数まで見るのは、前回が途中で落ちて一部だけ書けている
    /// 状態を「同じ指紋だから」で見逃さないため。
    pub fn is_current(&self, unit: &FoldUnit, fp: &str) -> bool {
        self.fingerprints.len() == 1
            && self.fingerprints[0] == fp
            && self.shifts == unit.shifts.len() as i64
            && self.day_summaries == unit.day_summaries.len() as i64
            && self.day_parts == unit.day_parts.len() as i64
    }
}

/// 再計算 1 回の集計。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FoldReport {
    pub drivers: usize,
    pub drivers_written: usize,
    pub drivers_unchanged: usize,
    pub shifts: usize,
    pub day_summaries: usize,
    pub day_parts: usize,
    pub skipped: Vec<SkipReason>,
}

impl FoldReport {
    pub fn wrote_anything(&self) -> bool {
        self.drivers_written > 0
    }
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

/// `shifts` を消すと `day_summaries` / `day_parts` は FK の CASCADE で消える。
const DELETE_SHIFTS_SQL: &str = r#"
DELETE FROM kintai.shifts
 WHERE tenant_id = $1 AND driver_cd = $2 AND date_start >= $3 AND date_start < $4
"#;

const INSERT_SHIFT_SQL: &str = r#"
INSERT INTO kintai.shifts
       (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"#;

const INSERT_DAY_SUMMARY_SQL: &str = r#"
INSERT INTO kintai.day_summaries
       (tenant_id, driver_cd, date, shift_start_at, shift_source,
        restraint_minutes, working_minutes, break_minutes, rest_minus_minutes,
        statutory_minutes, within_statutory_overtime_minutes, overtime_minutes,
        legal_holiday_minutes, night_minutes, overtime_night_minutes,
        legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
"#;

const INSERT_DAY_PART_SQL: &str = r#"
INSERT INTO kintai.day_parts
       (tenant_id, driver_cd, shift_start_at, date,
        restraint_minutes, working_minutes, night_minutes)
VALUES ($1, $2, $3, $4, $5, $6, $7)
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

/// 1 乗務員 1 か月ぶんを置き換える。**1 トランザクション**。
///
/// `shifts` を先に消してから入れ直す — `day_summaries` / `day_parts` は
/// `shifts` への FK を `ON DELETE CASCADE` で持つので、消し忘れが起きない。
pub async fn write_unit(
    store: &KintaiPgStore,
    month: &str,
    unit: &FoldUnit,
    fingerprint: &str,
) -> Result<(), KintaiPushError> {
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let tenant = store.tenant_id();
    let version = logic_version();
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
    for s in &unit.shifts {
        sqlx::query(INSERT_SHIFT_SQL)
            .bind(tenant)
            .bind(s.driver_cd)
            .bind(tz(s.start_at))
            .bind(tz(s.end_at))
            .bind(s.shift_source)
            .bind(fingerprint)
            .bind(version)
            .execute(&mut *tx)
            .await?;
    }
    for d in &unit.day_summaries {
        sqlx::query(INSERT_DAY_SUMMARY_SQL)
            .bind(tenant)
            .bind(d.driver_cd)
            .bind(d.date)
            .bind(tz(d.shift_start_at))
            .bind(d.shift_source)
            .bind(d.restraint_minutes as i32)
            .bind(d.working_minutes as i32)
            .bind(d.break_minutes as i32)
            .bind(d.rest_minus_minutes as i32)
            .bind(d.statutory_minutes as i32)
            .bind(d.within_statutory_overtime_minutes as i32)
            .bind(d.overtime_minutes as i32)
            .bind(d.legal_holiday_minutes as i32)
            .bind(d.night_minutes as i32)
            .bind(d.overtime_night_minutes as i32)
            .bind(d.legal_holiday_night_minutes as i32)
            .bind(fingerprint)
            .bind(version)
            .execute(&mut *tx)
            .await?;
    }
    for p in &unit.day_parts {
        sqlx::query(INSERT_DAY_PART_SQL)
            .bind(tenant)
            .bind(p.driver_cd)
            .bind(tz(p.shift_start_at))
            .bind(p.date)
            .bind(p.restraint_minutes as i32)
            .bind(p.working_minutes as i32)
            .bind(p.night_minutes as i32)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(())
}

/// 対象月を再計算して保存する (実装計画 05)。
///
/// 期間は [`month_range`] = `[月初, 翌月 2 日)`。読み出し経路と同じで、日跨ぎ勤務の
/// 終業打刻を拾うために翌月へはみ出す (push の `exact_month_range` とは違う —
/// あちらは「その日の全部を見た上で署名する」必要があるため)。
pub async fn recalc_month(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    driver: Option<u64>,
    apply: bool,
) -> Result<FoldReport, KintaiPushError> {
    let (from, to) = month_range(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let drivers: Vec<u64> = match driver {
        Some(d) => vec![d],
        None => crate::kosoku::split_by_driver(repo.fetch_all_events_between(&from, &to).await?)
            .into_iter()
            .map(|(d, _)| d)
            .collect(),
    };

    let mut report = FoldReport::default();
    for driver_cd in drivers {
        let rows = crate::kintai_push::read_driver_events(repo, driver_cd, &from, &to).await?;
        let (unit, fp) = fold_driver_month(driver_cd as i64, month, params, rows);
        report.drivers += 1;
        report.skipped.extend(unit.skipped.iter().cloned());

        let stored = stored_state(store, driver_cd as i64, month).await?;
        if stored.is_current(&unit, &fp) {
            report.drivers_unchanged += 1;
            continue;
        }
        report.drivers_written += 1;
        report.shifts += unit.shifts.len();
        report.day_summaries += unit.day_summaries.len();
        report.day_parts += unit.day_parts.len();
        if apply {
            write_unit(store, month, &unit, &fp).await?;
        }
    }
    Ok(report)
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
        self.push.has_unexpected() || !self.fold.skipped.is_empty()
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
        assert_eq!(logic_version().len(), 16);
        assert!(logic_version().chars().all(|c| c.is_ascii_hexdigit()));
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

    #[test]
    fn day_bounds_is_jst_midnight() {
        let (a, b) = day_bounds(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
        assert_eq!(a.to_rfc3339(), "2026-07-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2026-07-02T00:00:00+09:00");
    }
}
