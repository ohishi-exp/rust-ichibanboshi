//! 勤怠の口の段取り — 全量再計算の 1 ページ (`/api/kintai/recalc`)・窓の受け口の畳み直し
//! (`POST /api/kintai/timecard/window`) と打刻の置き換え (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/routes/kintai_recalc.rs` (`run`)・`src/routes/kintai_timecard.rs` (`fold_into` / `fold_window` /
//! `merge_fold`)・`src/kintai_push.rs` (`apply_timecard_window`) から移した。root (Cloud Run 版) と勤怠 Worker が
//! **同じ段取り**を使うため、I/O は [`crate::fold_flow`] の trait 越しに受け取る。ここに残らないのは HTTP の殻
//! (月の 400・書き先とテナントの解決・エラーの status への写し) だけ。
//!
//! 全量再計算の費用・月ゲートの 5 条件・運用 (`LOGIC_BASE` を上げた後の 2 段構え) は root の
//! `src/routes/kintai_recalc.rs` のモジュール docs。

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::Deserialize;

use crate::fold_flow::{
    fold_month_with_anchors, month_anchors, month_gate_report_with_anchors, recalc_drivers,
    recalc_drivers_from_units, write_fold_gate_best_effort, BadMonth, FoldClock, FoldCtx,
    FoldEventsIo, FoldGateRow, FoldSink, FoldStoreIo, Warnings,
};
use crate::kintai_fold::{FoldReport, MonthGate, StaleReport};
use crate::kintai_push::{
    jst_day_bounds, plan_window, window_spans, DriverPlan, TimecardWindow, TimecardWindowResult,
};

// ── 全量再計算 (`/api/kintai/recalc`) ───────────────────────────────────────

/// 1 回で畳み直す乗務員数の既定。
///
/// **2026-07-31 の実測 (2026-06 = 94 名) に基づき 20 → 100 へ引き上げ。**
/// 費用の主成分は乗務員数に依存しない全量読みなので、20 のままだと 94 名を畳むのに
/// 全量読みを 5 回払っていた。100 なら 1 回で終わる。
pub const DEFAULT_MAX_FOLD_DRIVERS: usize = 100;

/// `max_drivers` の上限。呼び出し側が大きな値を入れて proxy を殺すのを防ぐ。
///
/// 既定の 1.5 倍。**150 は未実測** — 100 名ぶんの fold (実測の線形外挿で
/// 15〜23 秒) より上は測っていない。踏んだら下げる。
pub const MAX_MAX_FOLD_DRIVERS: usize = 150;

/// `?month=&after_driver_cd=&max_drivers=&stale_only=` (`GET`)。
#[derive(Debug, Default, Deserialize)]
pub struct RecalcQuery {
    pub month: Option<String>,
    /// 続きから回す位置。前回の応答の `next_after_driver_cd` を渡す。
    #[serde(default)]
    pub after_driver_cd: Option<i64>,
    #[serde(default)]
    pub max_drivers: Option<usize>,
    /// 現行の `logic_version` の行を 1 つも持たない乗務員だけに絞る。
    ///
    /// **既定 `false`。** 勤務が 1 本も立たない乗務員は保存行を作れないので
    /// `true` では毎回この網に残る (収束しない) — 全量を回すときは既定のまま使う。
    #[serde(default)]
    pub stale_only: bool,
}

/// `POST /api/kintai/recalc` の本体。`GET` の query と同じ項目 + `apply`。
#[derive(Debug, Default, Deserialize)]
pub struct RecalcRequest {
    pub month: String,
    #[serde(default)]
    pub after_driver_cd: Option<i64>,
    #[serde(default)]
    pub max_drivers: Option<usize>,
    #[serde(default)]
    pub stale_only: bool,
    /// **`true` で初めて書く。** 既定は 1 行も書かない。
    #[serde(default)]
    pub apply: bool,
    /// [`crate::fold_flow::fold_month`] に渡す「今日」。**`#[serde(skip)]` — HTTP の
    /// JSON body からは絶対に上書きできない** (常に `None` = 実時計)。テストだけが
    /// Rust の構造体リテラルで直接 `Some` を渡せる (Refs #286-1)。
    #[serde(skip)]
    pub today: Option<NaiveDate>,
}

impl RecalcQuery {
    /// `GET` は `apply` を持たない — 読むだけの口から書けないようにしてある。
    pub fn into_request(self) -> RecalcRequest {
        RecalcRequest {
            month: self.month.unwrap_or_default(),
            after_driver_cd: self.after_driver_cd,
            max_drivers: self.max_drivers,
            stale_only: self.stale_only,
            apply: false,
            today: None,
        }
    }
}

/// 1 ページの乗務員数 (省略は [`DEFAULT_MAX_FOLD_DRIVERS`]、`1..=`[`MAX_MAX_FOLD_DRIVERS`] に丸める)。
pub fn page_size(max_drivers: Option<usize>) -> usize {
    max_drivers
        .unwrap_or(DEFAULT_MAX_FOLD_DRIVERS)
        .clamp(1, MAX_MAX_FOLD_DRIVERS)
}

/// 月 1 つぶんの stale ([`FoldStoreIo::stale_state`])。
async fn stale_of<S>(
    store: &S,
    ctx: &FoldCtx<'_>,
    months: &[String],
) -> Result<StaleReport, S::Error>
where
    S: FoldStoreIo + ?Sized,
{
    store.stale_state(months, &ctx.logic_version()).await
}

/// 応答の 1 ページ。月ゲートが刺さった回もそうでない回も同じキーで返す。
fn page_json(
    req: &RecalcRequest,
    drivers: &[i64],
    next: Option<i64>,
    fold: &FoldReport,
    stale: &StaleReport,
    elapsed_ms: u64,
    sink: &FoldSink,
) -> serde_json::Value {
    let d = &sink.unko_diff;
    serde_json::json!({
        "month": req.month,
        "apply": req.apply,
        "drivers": drivers,
        "next_after_driver_cd": next,
        "fold": fold,
        "stale": stale,
        "elapsed_ms": elapsed_ms,
        "unsplit": sink.unsplit,
        "unsplit_total": sink.unsplit_total,
        "unko_diff": d.items,
        "unko_diff_total": d.total,
        "unko_diff_gcp_only": d.gcp_only,
        "unko_diff_gcp_only_in_month": d.gcp_only_in_month,
        "unko_diff_gcp_only_by_month": d.gcp_only_by_month,
        "unko_diff_gcp_only_in_month_by_day": d.gcp_only_in_month_by_day,
        "unko_diff_gcp_only_in_month_by_driver": d.gcp_only_in_month_by_driver,
        "unko_diff_gcp_only_in_month_unknown_driver": d.gcp_only_in_month_unknown_driver,
        "unko_diff_gcp_only_driver_split": d.gcp_only_driver_split,
        "unko_diff_gcp_only_sample": d.gcp_only_sample,
        "unko_diff_onprem_sample": d.onprem_sample,
        "unko_diff_onprem_shape": d.onprem_shape,
        "unko_diff_gcp_shape": d.gcp_shape,
        "unko_diff_trials": d.trials,
    })
}

/// 全量再計算の 1 ページ (`GET` / `POST /api/kintai/recalc` の本体)。応答の JSON を返す。
///
/// 呼び出し側 (HTTP の殻) が済ませておくこと: 月の検査 (400)・書き先の解決・読みと書きのテナントの一致。
///
/// 段取り: 遡り起点 (1 回だけ) → 月ゲート (運行の突合も測る束) → 刺さったら stale だけ返す →
/// 生イベントの 1 回読み → 母集団の 1 ページ → 畳み → stale → **月まるごとを完結させたときだけ** gate を刻む。
///
/// gate を刻む 5 条件: `apply` かつ `after_driver_cd` 無し (1 ページ目から) かつ次のページが無い (回りきった) かつ
/// `stale_only` でない (母集団を狭めていない) かつ封を止める warning が無い (gate 段・畳み段の束のどちらにも)。
/// 書く digest は冒頭で判定した値のまま (fold の後に作り直さない — [`MonthGate::Miss`] の docs)。
pub async fn recalc_page<R, S>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    req: &RecalcRequest,
) -> Result<serde_json::Value, S::Error>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    S: FoldStoreIo + ?Sized,
    S::Error: From<R::Error> + From<BadMonth>,
{
    let started = ctx.clock.monotonic_ms();
    let elapsed = || ctx.clock.monotonic_ms().saturating_sub(started);
    // 読みの遡り起点は**ここで 1 回だけ**引き、月ゲートと畳みに同じ値を渡す — 別々に引くと封の材料と
    // 畳んだ入力の窓が割れうる (Refs ohishi-exp/nuxt-dtako-admin#1123)
    let anchors = month_anchors(repo, &req.month).await?;
    // 月ゲートの判定にも束を渡す (Refs #205 の 21)。指紋を取る読みが立てる warning を捨てると、欠けたままの月に
    // 「最新」の封をしてしまう。同じ束に `unsplit` と運行の突合 (この口だけが測る) も積まれる
    let mut gate_sink = FoldSink::measuring_unko_diff();
    let gate = month_gate_report_with_anchors(
        repo,
        store,
        ctx,
        &req.month,
        req.apply,
        &anchors,
        &mut gate_sink,
    )
    .await?;
    let months = [req.month.clone()];
    if let MonthGate::Hit(mut report) = gate {
        let stale = stale_of(store, ctx, &months).await?;
        let (n, w) = (report.drivers, report.drivers_written);
        tracing::info!(n, w, "kintai recalc page done (gate)");
        // 読みを省いた回でも入力の欠けは応答に出す (指紋が同じ = 入力が増えていないだけで、欠けが埋まったわけではない)
        report.warnings = gate_sink.warnings.texts();
        return Ok(page_json(
            req,
            &[],
            None,
            &report,
            &stale,
            elapsed(),
            &gate_sink,
        ));
    }

    let max = page_size(req.max_drivers);
    // 生イベントは月 1 回だけ読み、母集団の決定 (#205-12) と畳みの両方に使い回す
    let mut fold_warnings = Warnings::default();
    let all_units = fold_month_with_anchors::<R, S::Error>(
        repo,
        ctx,
        &req.month,
        None,
        req.today,
        &anchors,
        &mut fold_warnings,
    )
    .await?;
    // 2 つの束は別々に上限が効く (畳みの束の後ろに月ゲートの束を足す)。封を止めるかは 2 つの束の `||`
    let mut warnings = fold_warnings.texts();
    warnings.extend(gate_sink.warnings.texts());
    let has_blocking_warning = fold_warnings.blocks_seal() || gate_sink.warnings.blocks_seal();
    // rest-only 乗務員 (Postgres に 1 行も無い) を母集団に足すための集合 (`RECALC_DRIVER_PAGE_SQL` の docs)
    let extra: BTreeSet<i64> = all_units
        .iter()
        .map(|(cd, _, _)| *cd as i64)
        .filter(|cd| *cd > 0)
        .collect();
    let version = ctx.logic_version();
    let page = store
        .recalc_driver_page(
            &req.month,
            &version,
            &extra,
            req.after_driver_cd,
            req.stale_only,
            max,
        )
        .await?;
    // 回りきったかは「1 ページに満たなかったか」で決める。次を空で返すより往復が 1 回減る
    let next = match page.len() < max {
        true => None,
        false => page.last().copied(),
    };
    let drivers: Vec<u64> = page.iter().filter(|d| **d > 0).map(|d| *d as u64).collect();
    let mut folded =
        recalc_drivers_from_units(store, ctx, &req.month, &drivers, all_units, req.apply).await?;
    folded.warnings = warnings;
    let stale = stale_of(store, ctx, &months).await?;

    if let MonthGate::Miss {
        dtako_digest,
        punch_digest,
        logic_version,
    } = gate
    {
        let completed_the_whole_month = req.apply
            && req.after_driver_cd.is_none()
            && next.is_none()
            && !req.stale_only
            && !has_blocking_warning;
        if completed_the_whole_month {
            // 書けなくてもこの応答は落とさない (Refs #205 の 20)
            let gate = FoldGateRow {
                dtako_digest,
                punch_digest,
                logic_version,
            };
            write_fold_gate_best_effort(store, &req.month, &gate).await;
        }
    }

    let (n, w) = (folded.drivers, folded.drivers_written);
    tracing::info!(n, w, "kintai recalc page done");
    Ok(page_json(
        req,
        &page,
        next,
        &folded,
        &stale,
        elapsed(),
        &gate_sink,
    ))
}

// ── 窓の受け口 (`POST /api/kintai/timecard/window`) ─────────────────────────

/// 打刻の置き換えの I/O (`kintai.kintai_events`)。root の実装は `KintaiPgStore` (sqlx)。
#[allow(async_fn_in_trait)]
pub trait TimecardWindowIo {
    type Error: std::fmt::Display;

    /// 名乗った乗務員の `[from, to)` の日別署名 (`STORED_WINDOW_SIGNATURES_SQL`)。
    async fn stored_window_signatures(
        &self,
        drivers: &[i64],
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<BTreeMap<i64, BTreeMap<NaiveDate, String>>, Self::Error>;

    /// 計画どおりに日を丸ごと置き換える (窓ぜんたいで 1 transaction)。
    async fn replace_window(&self, plans: &BTreeMap<i64, DriverPlan>) -> Result<(), Self::Error>;
}

/// 窓の月が読めない ([`window_spans`] の文面、`months が空です` 等)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadWindow(pub String);

/// 窓ぶんを受けて、**変わった日だけ**を反映する。
///
/// 突き合わせはここ — 送り主に署名を引かせない。**送り主を信用しない** — 窓の外の日と、名乗っていない
/// 乗務員の行は落として数える ([`plan_window`])。dry-run なら書かない。
pub async fn apply_timecard_window<S>(
    store: &S,
    window: &TimecardWindow,
    clock: &(dyn FoldClock + Sync),
) -> Result<TimecardWindowResult, S::Error>
where
    S: TimecardWindowIo + ?Sized,
    S::Error: From<BadWindow>,
{
    let started = clock.monotonic_ms();
    let (spans, lo, hi) = window_spans(&window.months).map_err(BadWindow)?;
    let declared: BTreeSet<i64> = window.drivers.iter().copied().collect();
    let drivers: Vec<i64> = declared.iter().copied().collect();
    let remote = store
        .stored_window_signatures(&drivers, jst_day_bounds(lo).0, jst_day_bounds(hi).0)
        .await?;
    let (plans, mut result) = plan_window(&spans, &declared, &window.events, &remote);
    result.dry_run = window.dry_run;
    if !window.dry_run {
        // **乗務員ごとに往復しない。** 窓ぜんたいで 1 トランザクション
        store.replace_window(&plans).await?;
    }
    result.elapsed_ms = clock.monotonic_ms().saturating_sub(started);
    Ok(result)
}

/// 月ごとの [`FoldReport`] を足し合わせる。窓は複数月を覆う。
pub fn merge_fold(mut acc: FoldReport, r: FoldReport) -> FoldReport {
    acc.drivers += r.drivers;
    acc.drivers_written += r.drivers_written;
    acc.drivers_unchanged += r.drivers_unchanged;
    acc.shifts += r.shifts;
    acc.day_summaries += r.day_summaries;
    acc.day_parts += r.day_parts;
    acc.skipped.extend(r.skipped);
    acc.elapsed_ms += r.elapsed_ms;
    acc.dry_run = r.dry_run;
    acc.logic_version = r.logic_version;
    acc.calculated_at = r.calculated_at;
    acc
}

/// 窓で変わった乗務員だけを月ごとに畳み直す ([`recalc_drivers`])。
///
/// 上流 warnings を握り潰さない — R2 の分割遅れの最中に畳むと、欠けた入力を「最新」として保存する。
/// 束は月をまたいで 1 つ (上限・重複排除は窓ぜんたいに効く)。
pub async fn fold_window<R, S>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    window: &TimecardWindow,
    result: &TimecardWindowResult,
) -> Result<FoldReport, S::Error>
where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    S: FoldStoreIo + ?Sized,
    S::Error: From<R::Error> + From<BadMonth>,
{
    let drivers: Vec<u64> = result
        .drivers_changed
        .iter()
        .filter(|d| **d > 0)
        .map(|d| *d as u64)
        .collect();
    let apply = !window.dry_run;
    let mut sink = FoldSink::default();
    let mut folded = FoldReport::default();
    for month in &window.months {
        let r = recalc_drivers(repo, store, ctx, month, &drivers, apply, &mut sink).await?;
        folded = merge_fold(folded, r);
    }
    folded.warnings = sink.warnings.texts();
    Ok(folded)
}

/// 窓の apply 後に畳み直し、結果を応答 `body` へ**書き足す**。
///
/// **`Result` を返さない。** ここへ来た時点で打刻は既にコミット済みなので、fold が落ちたからとリクエスト
/// 全体を 5xx にすると、relay は「窓ごと失敗」と読んで同じ窓を再送し続ける。代わりに 200 のまま
/// `fold_error` / `fold_error_status` を載せる (`status_of` が失敗を status と文面に写す — HTTP の殻の流儀)。
/// `fold` と `stale` は独立に試す (畳めたのに stale の集計だけ落ちても、何を書いたかの報告を失わない)。
pub async fn fold_into<R, S, F>(
    repo: &R,
    store: &S,
    ctx: &FoldCtx<'_>,
    window: &TimecardWindow,
    result: &TimecardWindowResult,
    body: &mut serde_json::Value,
    status_of: F,
) where
    R: FoldEventsIo + ?Sized,
    R::Error: From<BadMonth>,
    S: FoldStoreIo + ?Sized,
    S::Error: From<R::Error> + From<BadMonth>,
    F: FnOnce(S::Error) -> (u16, String),
{
    match fold_window(repo, store, ctx, window, result).await {
        Ok(folded) => {
            let (n, wr) = (folded.drivers, folded.drivers_written);
            tracing::info!(n, wr, "timecard window folded");
            body["fold"] = serde_json::json!(folded);
        }
        Err(e) => {
            let (code, msg) = status_of(e);
            // 打刻は書けている。畳めなかったことを応答と log の両方で loud に
            tracing::error!(status = code, "timecard window fold failed");
            body["fold_error"] = serde_json::json!(msg);
            body["fold_error_status"] = serde_json::json!(code);
        }
    }
    match stale_of(store, ctx, &window.months).await {
        Ok(stale) => {
            if stale.drivers > 0 {
                tracing::warn!(s = stale.drivers, "fold is stale — run recalc");
            }
            body["stale"] = serde_json::json!(stale);
        }
        Err(e) => {
            tracing::error!("timecard window stale check failed");
            body["stale_error"] = serde_json::json!(e.to_string());
        }
    }
}
