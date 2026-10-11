//! fold の I/O を含む流れ — 月ゲート・運行の突合・畳み・保存済みとの突き合わせ (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/kintai_fold.rs` から移した。root (Cloud Run 版、sqlx + alc の HTTP) と勤怠 Worker (Hyperdrive) が
//! **同じ流れ**を使うため、I/O は 2 つの trait 越しに受け取る:
//!
//! | trait | 中身 | root の実装 |
//! |---|---|---|
//! | [`FoldEventsIo`] | 生イベントの読み 3 本 (遡り起点・全乗務員の行・dtako の指紋) | `dyn KintaiEventsApi` |
//! | [`FoldStoreIo`] | Supabase の勤怠スキーマ (`kintai.*`) の読み書き | `KintaiPgStore` (sqlx) |
//!
//! 形は `kintai-logic` の `AutoloadIo` と同じ (`async fn` を trait に直に書く。Send は要らない — Worker は単一スレッド)。
//!
//! ## warnings は値で持ち回る
//!
//! root はかつて warning・`unsplit`・etags を tokio の task_local で集めていた。Worker には tokio が無いので、
//! 呼び出し側が [`FoldSink`] を作って `&mut` で渡す。**規則は task_local の頃と同じ**:
//!
//! - 同じ文面は 1 回だけ・上限 [`MAX_COLLECTED_WARNINGS`] 件 ([`Warnings::push`])。上限は渡された束ぜんたいに効く
//! - 封を止めるかは warning ごとに生成箇所で決める ([`Warning::blocks_seal`])。診断専用 (tail gap・読み窓の遡り) は
//!   表示に出るが封は止めない ([`Warnings::blocks_seal`])
//! - 運行の突合 ([`crate::unko_diff`]) は [`FoldSink::measuring_unko_diff`] で作った束を渡したときだけ測る
//!   (task_local の頃の `with_unko_diff_sink` の内側に当たる。外では Postgres を 2 本余計に読まない)
//!
//! 時計は [`FoldClock`] で受け取る (wasm32 では `std::time::Instant` も `Utc::now` も使えない)。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::{Deserialize, Serialize};

use crate::anchors::{clip_to_anchors, HeadAnchors};
use crate::kintai_fold::{
    fold_driver_month, logic_version, new_report, operations_in_driver_windows, tz, FoldReport,
    FoldUnit, MonthGate, StoredState,
};
use crate::kintai_push::{month_date_bounds, PUSHED_SOURCES};
use crate::kosoku::{split_by_driver, KosokuParams};
use crate::unko_diff::{unko_diff_with_warnings, OnpremOperation, UnkoDiff, UnkoDiffAux};
use crate::window::{exact_month_range, month_range, parse_dt, read_window};

// ── warnings ────────────────────────────────────────────────────────────────

/// 1 回の呼び出しで持ち帰る warnings の上限。
///
/// 上流は運行ごとに 1 本出すので、R2 の分割がまるごと遅れていると月ぶんの
/// 運行数 (実測 1,100 件超) が並ぶ。応答を膨らませても読めないので頭だけ。
pub const MAX_COLLECTED_WARNINGS: usize = 20;

/// warning 1 本 (Refs #205-51)。
///
/// **`blocks_seal` は生成箇所で決める** — 呼び出し側が文面を見て後から分類しない。
/// 文字列の前方一致にすると、メッセージ文言を変えた瞬間に静かに壊れるため。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Warning {
    pub text: String,
    pub blocks_seal: bool,
}

impl Warning {
    /// 封を止める warning (入力が欠けている疑い)。
    pub fn blocking(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            blocks_seal: true,
        }
    }

    /// 表示には出るが封は止めない warning (入力カバレッジの診断)。
    pub fn diagnostic(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            blocks_seal: false,
        }
    }
}

/// 集めた warnings。**同じ文面は 1 回だけ・上限 [`MAX_COLLECTED_WARNINGS`] 件。**
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Warnings {
    items: Vec<Warning>,
}

impl Warnings {
    /// 足す。同じ文面が既にあるか上限に達していれば捨てる。
    pub fn push(&mut self, w: Warning) {
        if self.items.len() < MAX_COLLECTED_WARNINGS && !self.items.iter().any(|c| c.text == w.text)
        {
            self.items.push(w);
        }
    }

    /// 並びのまま 1 本ずつ [`Self::push`] する。
    pub fn extend(&mut self, ws: impl IntoIterator<Item = Warning>) {
        for w in ws {
            self.push(w);
        }
    }

    /// 表示用の文面 (診断専用も含む)。
    pub fn texts(&self) -> Vec<String> {
        self.items.iter().map(|c| c.text.clone()).collect()
    }

    /// **封を止める** warning を集めたか (Refs #205-51)。診断専用だけなら `false`。
    pub fn blocks_seal(&self) -> bool {
        self.items.iter().any(|c| c.blocks_seal)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn items(&self) -> &[Warning] {
        &self.items
    }
}

/// `has_kudgivt = FALSE` (=読み取り側 3 クエリの母集団からも欠け検知からも同時に
/// 消えている運行) の 1 件。Refs #205 の 32 で alc (#205-36) が足す前方互換フィールド。
///
/// **判定には使わない — `run_kintai_recalc` の応答まで素通しするだけ。**
/// #205 の 142 行差の仮説 (`has_kudgivt = FALSE` が原因) を検証するための実データ
/// 突き合わせ材料で、判定ロジックへの組み込みは実データを見てから別途決める。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UnsplitOperation {
    pub unko_no: String,
    pub driver_cd: String,
    pub reading_date: String,
}

/// 1 回の流れで集めたもの (task_local の頃の 3 つの sink を 1 つの値にしたもの)。
#[derive(Debug, Clone, Default)]
pub struct FoldSink {
    pub warnings: Warnings,
    /// 最後に読んだ etags の `unsplit` (上書き)。読んでいなければ空。
    pub unsplit: Vec<UnsplitOperation>,
    pub unsplit_total: usize,
    /// 運行の突合の結果。[`Self::measuring_unko_diff`] で作った束で月ゲートを判定したときだけ埋まる。
    pub unko_diff: UnkoDiff,
    measure_unko_diff: bool,
}

impl FoldSink {
    /// 運行の突合も測る束 (`POST /api/kintai/recalc` の月ゲート)。
    pub fn measuring_unko_diff() -> Self {
        Self {
            measure_unko_diff: true,
            ..Self::default()
        }
    }
}

// ── 読み (生イベント) ───────────────────────────────────────────────────────

/// alc の etags から控えたもの。突合 (Refs #205 の 37・39) と応答の素通し (`unsplit`) に使う。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EtagsRead {
    pub unsplit: Vec<UnsplitOperation>,
    pub unsplit_total: usize,
    /// etags の `unko_no` の集合 (GCP 側の運行一覧)。
    pub unko_nos: HashSet<String>,
    /// etags の `unko_no` → `driver_cds`。alc が返さない環境では空。
    pub driver_cds: HashMap<String, Vec<String>>,
}

/// dtako 側の月ゲート材料の読み。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DtakoMonthRead {
    /// 月ゲートの dtako 指紋。**`None` は「口が無い」** (判定できない)。
    pub digest: Option<String>,
    /// etags を読めたときだけ `Some`。
    pub etags: Option<EtagsRead>,
    /// 読みの途中で立った warnings (並びのまま)。
    pub warnings: Vec<Warning>,
}

impl DtakoMonthRead {
    /// 指紋だけ (etags も warnings も持たない読み先)。
    pub fn digest_only(digest: Option<String>) -> Self {
        Self {
            digest,
            ..Self::default()
        }
    }
}

/// 生イベントの読み 3 本。
#[allow(async_fn_in_trait)]
pub trait FoldEventsIo {
    type Error: std::fmt::Display;

    /// 月初をまたぐ運行・勤務の始まり (乗務員CD → 起点)。`[month_start, to)`。
    async fn month_head_anchors(
        &self,
        month_start: &str,
        to: &str,
    ) -> Result<HeadAnchors, Self::Error>;

    /// `[from, to)` の全乗務員の生イベント (全乗務員版の形) と、読みの途中で上流が返した warnings。
    async fn all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<(Vec<serde_json::Value>, Vec<Warning>), Self::Error>;

    /// 月ゲートの dtako 指紋 (始端を `since` まで下げる) と etags の控え・warnings。
    async fn dtako_month_read_since(
        &self,
        month: &str,
        since: Option<NaiveDate>,
    ) -> Result<DtakoMonthRead, Self::Error>;
}

// ── 読み書き (Supabase の勤怠スキーマ) ──────────────────────────────────────

/// 前回 fold したときの入力の指紋 (Refs #205 実装計画 13、`kintai.fold_gate`)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoldGateRow {
    pub dtako_digest: String,
    pub punch_digest: String,
    pub logic_version: String,
}

/// Supabase の勤怠スキーマ (`kintai.*`) の読み書き。SQL は `crate::kintai_fold` / `crate::kintai_push` の定数。
#[allow(async_fn_in_trait)]
pub trait FoldStoreIo {
    type Error: std::fmt::Display;

    /// ページの全乗務員の保存済みの姿 (`STORED_STATES_SQL`)。保存が無い乗務員も空の値で入る。`drivers` が空なら空。
    async fn stored_states(
        &self,
        drivers: &[i64],
        month: &str,
    ) -> Result<BTreeMap<i64, StoredState>, Self::Error>;

    /// 1 乗務員 1 か月ぶんを 1 transaction で置き換える。
    async fn write_unit(
        &self,
        month: &str,
        unit: &FoldUnit,
        fingerprint: &str,
        logic_version: &str,
    ) -> Result<(), Self::Error>;

    /// `FOLD_GATE_SELECT_SQL`。
    async fn read_fold_gate(&self, month: &str) -> Result<Option<FoldGateRow>, Self::Error>;

    /// `FOLD_GATE_UPSERT_SQL`。
    async fn write_fold_gate(&self, month: &str, gate: &FoldGateRow) -> Result<(), Self::Error>;

    /// 打刻側の月ゲート材料 (`MONTH_PUNCH_DIGEST_SQL`)。
    async fn month_punch_digest(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<String, Self::Error>;

    /// 押し込み済みの運行一覧 (`MONTH_OPERATIONS_SQL`): `(乗務員CD, unko_no, 最初の暦日, 最後の暦日)`。
    async fn month_operations(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<Vec<(i64, String, NaiveDate, NaiveDate)>, Self::Error>;

    /// `unko_no` 付きの行を一度でも持った乗務員CD (root の `OPERATION_DRIVER_CDS_SQL`)。
    async fn operation_driver_cds(&self) -> Result<HashSet<i64>, Self::Error>;
}

/// 時計。
pub trait FoldClock {
    /// 今の時刻 (JST)。
    fn now_jst(&self) -> DateTime<FixedOffset>;
    /// 単調増加のミリ秒 (経過時間を測るためだけに使う。起点は問わない)。
    fn monotonic_ms(&self) -> u64;
}

/// 流れに渡す設定と時計。
pub struct FoldCtx<'a> {
    pub params: &'a KosokuParams,
    /// 勤怠の版 (root は `build.rs` の `KINTAI_OUTPUT_SHA`)。
    pub output_sha: &'a str,
    /// `Sync` を求めるのは root (axum) の future を Send に保つため (Worker の時計は状態を持たないので満たす)。
    pub clock: &'a (dyn FoldClock + Sync),
}

impl FoldCtx<'_> {
    fn logic_version(&self) -> String {
        logic_version(self.params, self.output_sha)
    }

    fn new_report(&self, apply: bool) -> FoldReport {
        new_report(self.params, apply, self.output_sha, self.clock.now_jst())
    }
}

/// 月が `YYYY-MM` として読めない。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadMonth(pub String);

impl std::fmt::Display for BadMonth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "bad month: {}", self.0)
    }
}

fn bad_month(month: &str) -> BadMonth {
    BadMonth(month.to_string())
}

/// 月が `YYYY-MM` として読めるか (読みの窓 `[月初, 翌月 2 日)` を返す)。
fn month_bounds(month: &str) -> Result<(String, String), BadMonth> {
    month_range(month).ok_or_else(|| bad_month(month))
}

// ── 畳み ────────────────────────────────────────────────────────────────────

/// **fold が読む窓のうち push が書かない 1 日**に打刻が 1 行も無ければ警告を返す
/// (Refs #205 の 30)。
///
/// | | 関数 | 窓 |
/// |---|---|---|
/// | 書く (root の `kintai_push::push_month`) | `exact_month_range` | `[月初, 翌月初)` |
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
/// 見ていない状態」で署名すると毎回書き直しになる) は root の `kintai_push::push_month`
/// の docs にあり、正当なので触らない。
///
/// - **翌月初がまだ過ぎていない月では黙る** — 当月を畳むときは、はみ出した先が
///   未来なので打刻が無いのが正常。過ぎた日にだけ鳴らす (`spill < today`)
/// - 判定に使うのは**既に読んだ行だけ**。往復は増やさない
/// - 見るのは [`PUSHED_SOURCES`] (`timecard` / `dtako`) だけ。`dtako_events` は
///   push を通らず alc から直に来るので、在っても push の証拠にならない
pub fn push_window_gap_warning(
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

/// 遡った乗務員を 1 行ずつ warnings に出す (封は止めない — 診断)。
///
/// 呼ぶのは月が読めた後だけ (月の初日が引けないことは無い)。起点が読めない行は 0 日前と書く。
fn lookback_warnings(month: &str, anchors: &HeadAnchors, warnings: &mut Warnings) {
    let first = month_date_bounds(month).map(|(f, _)| f);
    for (cd, at) in anchors {
        let days = parse_dt(at)
            .zip(first)
            .map_or(0, |(a, f)| (f - a.date()).num_days());
        let w = format!("読み窓の遡り: {month} の {cd} を {days} 日前 ({at}) から読む");
        tracing::info!("{w}");
        warnings.push(Warning::diagnostic(w));
    }
}

/// 対象月の遡り起点を引く。月が壊れていれば読む前に落とす。
pub async fn month_anchors<R>(repo: &R, month: &str) -> Result<HeadAnchors, R::Error>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
{
    let (from, to) = month_bounds(month)?;
    repo.month_head_anchors(&from, &to).await
}

/// 対象月を**生イベント 1 回読み**で乗務員ごとに畳む。返すのは乗務員CD 昇順。
///
/// 期間は [`month_range`] = `[月初, 翌月 2 日)`。始端は月初をまたぐ運行・勤務の開始まで遡る
/// ([`read_window`] / [`clip_to_anchors`]、Refs ohishi-exp/nuxt-dtako-admin#1123)。
/// 読みは [`FoldEventsIo::all_events_between`] **1 回だけ**。乗務員で分けるのは
/// [`split_by_driver`] (in-process)。`driver` を指定してもこの読み方を変えない
/// (root の `kintai_fold` のモジュール docs の「材料の行は全乗務員版の形」)。
///
/// **`driver` 指定でその乗務員の行が 1 つも無くても単位を 1 つ返す** — 空の [`FoldUnit`] で、
/// 呼び出し側が「保存済みの行が余っている」と気付いて消せる。
///
/// `today` は [`push_window_gap_warning`] の基準日。**`None` なら時計の今日** — 固定日を渡せるのはテストだけ
/// (Refs #286-1)。
pub async fn fold_month<R, E>(
    repo: &R,
    ctx: &FoldCtx<'_>,
    month: &str,
    driver: Option<u64>,
    today: Option<NaiveDate>,
    warnings: &mut Warnings,
) -> Result<Vec<(u64, FoldUnit, String)>, E>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    E: From<R::Error> + From<BadMonth>,
{
    month_bounds(month)?;
    let anchors = month_anchors(repo, month).await?;
    fold_month_with_anchors(repo, ctx, month, driver, today, &anchors, warnings).await
}

/// [`fold_month`] の本体 — 遡り起点を呼び出し側から受け取る。[`recalc_month`] が月ゲートと
/// 畳みに**同じ起点**を渡すための口。
pub async fn fold_month_with_anchors<R, E>(
    repo: &R,
    ctx: &FoldCtx<'_>,
    month: &str,
    driver: Option<u64>,
    today: Option<NaiveDate>,
    anchors: &HeadAnchors,
    warnings: &mut Warnings,
) -> Result<Vec<(u64, FoldUnit, String)>, E>
where
    R: FoldEventsIo + ?Sized,
    E: From<R::Error> + From<BadMonth>,
{
    month_bounds(month)?;
    let (from, to) = read_window(month, anchors, None).ok_or_else(|| bad_month(month))?;
    let (rows, upstream) = repo.all_events_between(&from, &to).await?;
    warnings.extend(upstream);
    tracing::debug!("fold {month}: read {} rows in 1 fetch_all", rows.len());
    // はみ出した先の 1 日が push されているかを確かめる (Refs #205 の 30)
    let today = today.unwrap_or_else(|| ctx.clock.now_jst().date_naive());
    if let Some(w) = push_window_gap_warning(month, &rows, today) {
        tracing::warn!("{w}");
        warnings.push(Warning::blocking(w));
    }
    lookback_warnings(month, anchors, warnings);
    let by_driver = clip_to_anchors(split_by_driver(rows), month, anchors);
    let mut units: Vec<(u64, FoldUnit, String)> = by_driver
        .into_iter()
        .filter(|(cd, _)| driver.is_none_or(|want| want == *cd))
        .map(|(cd, rows)| {
            let (unit, fp) = fold_driver_month(cd as i64, month, ctx.params, rows, ctx.output_sha);
            (cd, unit, fp)
        })
        .collect();
    if let Some(cd) = driver {
        if units.is_empty() {
            let (unit, fp) =
                fold_driver_month(cd as i64, month, ctx.params, Vec::new(), ctx.output_sha);
            units.push((cd, unit, fp));
        }
    }
    Ok(units)
}

// ── 月ゲート (実装計画 13) ──────────────────────────────────────────────────

/// gate の書き込み。**書けなくても呼び出し元の口を落とさない** (Refs #205 の 20)。
///
/// 書き込みは**畳み終わった後**に走る。ここで `Err` を持ち上げると、再計算も保存も
/// 全部成功したリクエストが最適化の書き込み 1 本だけを理由に 502 になる。書けなければ
/// gate が立たないだけで、次回また全量読みに落ちる**安全側**なので、loud に warn して先へ進む。
pub async fn write_fold_gate_best_effort<S>(store: &S, month: &str, gate: &FoldGateRow)
where
    S: FoldStoreIo + ?Sized,
{
    if let Err(e) = store.write_fold_gate(month, gate).await {
        tracing::warn!(month = %month, error = %e, "kintai month-gate write failed");
    }
}

/// いまの月の入力から指紋を作る。**`Ok(None)` は「判定できない」の意味** — alc に etags の口が
/// 無い環境、上流エラー、**打刻側 (Pg) の読み取り失敗**がこれになる。どれも呼び出し側は
/// 安全側 (従来どおり全量読み) に倒す。**Pg 側の失敗を `Err` で持ち上げない** (Refs #205 の 20 —
/// `kintai.fold_gate` の GRANT 漏れで recalc の口がログ 1 行も出さずに 502 を返し続けた実害)。
///
/// 窓は [`fold_month_with_anchors`] が実際に読む窓 ([`read_window`]) と**同じ**にする (起点がある月は
/// 始端を下げる、Refs ohishi-exp/nuxt-dtako-admin#1123)。etags は**ここで 1 回だけ**引き、
/// 運行の突合もその値を使う。
async fn compute_month_digests<R, S>(
    repo: &R,
    store: &S,
    month: &str,
    anchors: &HeadAnchors,
    sink: &mut FoldSink,
) -> Result<Option<(String, String)>, S::Error>
where
    R: FoldEventsIo + ?Sized,
    S: FoldStoreIo + ?Sized,
    S::Error: From<BadMonth>,
{
    let (from_s, to_s) = read_window(month, anchors, None).ok_or_else(|| bad_month(month))?;
    let from = tz(parse_dt(&from_s).ok_or_else(|| bad_month(month))?);
    let to = tz(parse_dt(&to_s).ok_or_else(|| bad_month(month))?);
    let since = (!anchors.is_empty()).then(|| from.date_naive());
    let read = match repo.dtako_month_read_since(month, since).await {
        Ok(read) => read,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate dtako digest failed");
            return Ok(None);
        }
    };
    sink.warnings.extend(read.warnings);
    let etags = read.etags.map(|e| {
        sink.unsplit = e.unsplit;
        sink.unsplit_total = e.unsplit_total;
        (e.unko_nos, e.driver_cds)
    });
    let Some(dtako_digest) = read.digest else {
        tracing::info!(month = %month, "kintai month-gate unavailable (no alc etags endpoint)");
        return Ok(None);
    };
    // 運行の突合 (Refs #205 の 37)。両側とも既に読んでいるものだけで組む —
    // GCP 側は直前の etags、オンプレ側は押し込み済みの kintai_events
    if let (true, Some((gcp, gcp_drivers))) = (sink.measure_unko_diff, etags) {
        measure_unko_diff(store, month, (from, to), anchors, gcp, gcp_drivers, sink).await;
    }
    let punch_digest = match store.month_punch_digest(from, to).await {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate punch digest failed");
            return Ok(None);
        }
    };
    Ok(Some((dtako_digest, punch_digest)))
}

/// **オンプレと GCP の運行を突き合わせて記録する** (Refs #205 の 37)。**判定には入らない観測**だが、
/// 突合の warning は封を止める ([`unko_diff_with_warnings`])。読めなければ loud に warn して先へ進む。
async fn measure_unko_diff<S>(
    store: &S,
    month: &str,
    (from, to): (DateTime<FixedOffset>, DateTime<FixedOffset>),
    anchors: &HeadAnchors,
    gcp: HashSet<String>,
    gcp_drivers: HashMap<String, Vec<String>>,
    sink: &mut FoldSink,
) where
    S: FoldStoreIo + ?Sized,
{
    let rows = match store.month_operations(from, to).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai unko diff read failed");
            return;
        }
    };
    let onprem: Vec<OnpremOperation> = operations_in_driver_windows(rows, month, anchors)
        .into_iter()
        .map(
            |(driver_cd, unko_no, first_date, last_date)| OnpremOperation {
                driver_cd,
                unko_no,
                first_date,
                last_date,
            },
        )
        .collect();
    // 逆方向を「対象月に始まった運行だけ」でも数えるために月を渡す (Refs #205 の 37)
    let ym = month_ym(month);
    // 逆方向を乗務員別に割るための材料 (Refs #205 の 39)。**どちらも欠けてよい**
    let aux = UnkoDiffAux {
        gcp_drivers,
        onprem_ever: onprem_ever_driver_cds(store, month).await,
    };
    let (diff, warnings) = unko_diff_with_warnings(&onprem, &gcp, &aux, ym);
    sink.warnings.extend(warnings);
    let (n, r) = (diff.total, diff.gcp_only_in_month);
    tracing::info!(n, r, "kintai unko diff");
    let s = &diff.gcp_only_driver_split;
    let (a, b) = (
        s.never_onprem_ops,
        s.other_month_only_ops + s.also_in_month_ops,
    );
    tracing::info!(a, b, "kintai unko diff gcp-only split");
    sink.unko_diff = diff;
}

/// **`unko_no` 付きの行を一度でも持った乗務員CD** (Refs #205 の 39)。引けなければ空で先へ進む。
async fn onprem_ever_driver_cds<S>(store: &S, month: &str) -> HashSet<i64>
where
    S: FoldStoreIo + ?Sized,
{
    match store.operation_driver_cds().await {
        Ok(set) => set,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai unko diff ever-drivers failed");
            HashSet::new()
        }
    }
}

/// `YYYY-MM` を `(年, 月)` へ。壊れていれば `None` (月で絞る観測を諦めるだけ)。
fn month_ym(month: &str) -> Option<(i32, u32)> {
    let y = month.get(..4)?.parse().ok()?;
    let m = month.get(5..7)?.parse().ok()?;
    Some((y, m))
}

/// 月ゲートを判定する (実装計画 13)。**読むだけで書かない** — 書くかどうかは呼び出し側が
/// 「月まるごとを 1 呼び出しで完結させたか」を見て決める。
///
/// **`kintai.fold_gate` が読めなくても `Err` にしない** ([`MonthGate::Unavailable`] に倒す、Refs #205 の 20)。
pub async fn month_gate_report<R, S>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    month: &str,
    apply: bool,
    sink: &mut FoldSink,
) -> Result<MonthGate, S::Error>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    S: FoldStoreIo + ?Sized,
    S::Error: From<BadMonth>,
{
    month_bounds(month)?;
    // 起点が引けなければ判定しない (安全側) — 読めない理由は畳みの側が口で返す
    let anchors = match month_anchors(repo, month).await {
        Ok(a) => a,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate anchors failed");
            return Ok(MonthGate::Unavailable);
        }
    };
    month_gate_report_with_anchors(repo, store, ctx, month, apply, &anchors, sink).await
}

/// [`month_gate_report`] の本体 — 遡り起点を呼び出し側から受け取る ([`fold_month_with_anchors`] と同じ値を渡すため)。
pub async fn month_gate_report_with_anchors<R, S>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    month: &str,
    apply: bool,
    anchors: &HeadAnchors,
    sink: &mut FoldSink,
) -> Result<MonthGate, S::Error>
where
    R: FoldEventsIo + ?Sized,
    S: FoldStoreIo + ?Sized,
    S::Error: From<BadMonth>,
{
    let digests = compute_month_digests(repo, store, month, anchors, sink).await?;
    let Some((dtako_digest, punch_digest)) = digests else {
        return Ok(MonthGate::Unavailable);
    };
    let version = ctx.logic_version();
    let stored = match store.read_fold_gate(month).await {
        Ok(stored) => stored,
        Err(e) => {
            tracing::warn!(month = %month, error = %e, "kintai month-gate read failed");
            return Ok(MonthGate::Unavailable);
        }
    };
    let hit = stored.is_some_and(|g| {
        g.logic_version == version
            && g.dtako_digest == dtako_digest
            && g.punch_digest == punch_digest
    });
    if hit {
        tracing::info!(month = %month, "kintai month-gate skip");
        return Ok(MonthGate::Hit(ctx.new_report(apply)));
    }
    Ok(MonthGate::Miss {
        dtako_digest,
        punch_digest,
        logic_version: version,
    })
}

// ── 再計算 ──────────────────────────────────────────────────────────────────

/// 対象月を再計算して保存する (実装計画 05)。
///
/// ## 月ゲート (実装計画 13、`driver` 省略時だけ)
///
/// `driver` を指定しない呼び出しは**月まるごとを 1 回で完結させる**経路 (root の CLI の `Recalc` / `Sync`)。
/// 前回 fold 時の指紋といまの指紋が一致すれば読みも突き合わせも**まるごと省く**。
///
/// **gate を書くのは `sink.warnings` に封を止める warning が無いときだけ** (Refs #205-17 / #205-51)。
/// 束ぜんたい (gate の判定・畳みの読み・呼び出し側がそれより前に積んだもの) を見る。
/// **tail gap のような診断専用は数えない** — HTTP 経路 (root の `routes::kintai_recalc::run`) と同じ基準。
///
/// `today` は [`fold_month`] にそのまま渡す (`None` = 時計の今日。固定できるのはテストだけ)。
#[allow(clippy::too_many_arguments)]
pub async fn recalc_month<R, S>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    month: &str,
    driver: Option<u64>,
    apply: bool,
    today: Option<NaiveDate>,
    sink: &mut FoldSink,
) -> Result<FoldReport, S::Error>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    S: FoldStoreIo + ?Sized,
    S::Error: From<R::Error> + From<BadMonth>,
{
    month_bounds(month)?;
    // 遡り起点は 1 回だけ引き、月ゲートと畳みに同じ値を渡す (Refs
    // ohishi-exp/nuxt-dtako-admin#1123)。ずれると封の材料と畳んだ入力の窓が割れる
    let anchors = month_anchors(repo, month).await?;
    if driver.is_none() {
        match month_gate_report_with_anchors(repo, store, ctx, month, apply, &anchors, sink).await?
        {
            MonthGate::Hit(report) => return Ok(report),
            MonthGate::Miss {
                dtako_digest,
                punch_digest,
                logic_version,
            } => {
                let units = fold_month_with_anchors::<R, S::Error>(
                    repo,
                    ctx,
                    month,
                    driver,
                    today,
                    &anchors,
                    &mut sink.warnings,
                )
                .await?;
                let report = store_units(store, ctx, month, units, apply).await?;
                // 封を止める warning が無い回だけ刻む — 迷ったら書かない側 (Refs #205-17)
                if apply && !sink.warnings.blocks_seal() {
                    let gate = FoldGateRow {
                        dtako_digest,
                        punch_digest,
                        logic_version,
                    };
                    write_fold_gate_best_effort(store, month, &gate).await;
                }
                return Ok(report);
            }
            MonthGate::Unavailable => {}
        }
    }
    let units = fold_month_with_anchors::<R, S::Error>(
        repo,
        ctx,
        month,
        driver,
        today,
        &anchors,
        &mut sink.warnings,
    )
    .await?;
    store_units(store, ctx, month, units, apply).await
}

/// **名指しした乗務員だけ**を再計算する (Refs #205 の 06 の HTTP 版)。窓の受け口が apply 後に呼ぶ。
///
/// **`drivers` が空なら 1 往復もしない。** 月ゲートは**読むだけで書かない** (ページの一部だけで
/// 「この月は最新」と刻むと未処理の乗務員が取り残される)。読み方は [`fold_month`] と同じ全乗務員版 1 回読み。
pub async fn recalc_drivers<R, S>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    month: &str,
    drivers: &[u64],
    apply: bool,
    sink: &mut FoldSink,
) -> Result<FoldReport, S::Error>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    S: FoldStoreIo + ?Sized,
    S::Error: From<R::Error> + From<BadMonth>,
{
    if drivers.is_empty() {
        return Ok(ctx.new_report(apply));
    }
    month_bounds(month)?;
    let anchors = month_anchors(repo, month).await?;
    let gate =
        month_gate_report_with_anchors(repo, store, ctx, month, apply, &anchors, sink).await?;
    if let MonthGate::Hit(_) = gate {
        tracing::info!(month = %month, n = drivers.len(), "kintai month-gate skip");
        let mut report = ctx.new_report(apply);
        report.drivers = drivers.len();
        report.drivers_unchanged = drivers.len();
        return Ok(report);
    }
    // 時計の今日 — この経路は月ゲートを書かないので push 窓ずれ警告の有無が判定を左右しない
    let all = fold_month_with_anchors::<R, S::Error>(
        repo,
        ctx,
        month,
        None,
        None,
        &anchors,
        &mut sink.warnings,
    )
    .await?;
    recalc_drivers_from_units(store, ctx, month, drivers, all, apply).await
}

/// [`recalc_drivers`] の続き — **生イベントの読みを呼び出し側から受け取る** (同じ 1 回読みを使い回すとき)。
pub async fn recalc_drivers_from_units<S>(
    store: &S,
    ctx: &FoldCtx<'_>,
    month: &str,
    drivers: &[u64],
    all: Vec<(u64, FoldUnit, String)>,
    apply: bool,
) -> Result<FoldReport, S::Error>
where
    S: FoldStoreIo + ?Sized,
{
    if drivers.is_empty() {
        return Ok(ctx.new_report(apply));
    }
    let want: BTreeSet<u64> = drivers.iter().copied().collect();
    let mut units: Vec<(u64, FoldUnit, String)> = all
        .into_iter()
        .filter(|(cd, _, _)| want.contains(cd))
        .collect();
    // **行が 1 つも無い乗務員にも空の単位を立てる。** 打刻が丸ごと消された乗務員は
    // ここで拾わないと、保存済みの古い勤務が消えずに残る
    let seen: BTreeSet<u64> = units.iter().map(|(cd, _, _)| *cd).collect();
    for cd in want.difference(&seen).copied() {
        let (unit, fp) =
            fold_driver_month(cd as i64, month, ctx.params, Vec::new(), ctx.output_sha);
        units.push((cd, unit, fp));
    }
    units.sort_by_key(|(cd, _, _)| *cd);
    store_units(store, ctx, month, units, apply).await
}

/// 畳んだ単位を保存済みの姿と突き合わせ、変わったものだけ書く。保存済みの姿は 1 往復でまとめて読む (Refs #205 の 25)。
async fn store_units<S>(
    store: &S,
    ctx: &FoldCtx<'_>,
    month: &str,
    units: Vec<(u64, FoldUnit, String)>,
    apply: bool,
) -> Result<FoldReport, S::Error>
where
    S: FoldStoreIo + ?Sized,
{
    let started = ctx.clock.monotonic_ms();
    let mut report = ctx.new_report(apply);
    let version = ctx.logic_version();
    let cds: Vec<i64> = units.iter().map(|(cd, _, _)| *cd as i64).collect();
    let stored_all = store.stored_states(&cds, month).await?;
    for (driver_cd, unit, fp) in units {
        report.drivers += 1;
        report.skipped.extend(unit.skipped.iter().cloned());
        // 引けなかった乗務員は「保存が無い」ではなく **stale 側に倒す**
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
            store.write_unit(month, &unit, &fp, &version).await?;
        }
    }
    report.elapsed_ms = ctx.clock.monotonic_ms().saturating_sub(started);
    Ok(report)
}
