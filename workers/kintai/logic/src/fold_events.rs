//! 勤怠 Worker の fold の読み 3 本 (`kintai_kosoku::fold_flow::FoldEventsIo`) の実装 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 本番の Cloud Run 版 (root の `HttpKintaiEventsRepo` が alc を reqwest で読み、fallback の `PgKintaiEventsRepo` = relay が送った
//! 打刻の写し `kintai.kintai_events` を合流する) と**同じ読み方**を、往復だけ差し替えて作る。合流・ページング・指紋・入力の
//! 欠けの検知は共有 crate の `kintai_kosoku::alc_events`、写しの SQL・行の形は `kintai_kosoku::pushed_events` (root と同じもの):
//!
//! | 読み | root (Cloud Run 版) | 勤怠 Worker |
//! |---|---|---|
//! | 遡り起点 | fallback (写し) の `HEAD_RUN_ENDS_SQL` → `HEAD_PUNCHES_SQL` | 同じ 2 本 ([`PushedEventsRead`]) |
//! | 全乗務員の行 | alc のページング → fallback (写し) の `ALL_EVENTS_SQL` を合流 | alc は auth-worker の `dtakoEvents` ([`AlcRpc`])、写しは同じ SQL |
//! | dtako の指紋 | alc の etags | auth-worker の `dtakoEtags` |
//!
//! **社内 MariaDB は読まない** (オンプレ版の経路。写しには `休息` の行と最後の送り以降の打刻が無いので、MariaDB を読むと
//! Cloud Run 版と畳みの入力がずれる)。往復は 2 つの小さな trait で受け取る (worker crate が auth-worker の binding で、kintai-pg の
//! `FoldStore` が Supabase の接続で実装する。テストは fake)。
//! **非 2xx の扱いは root の fold と同じ**: 生イベントは 404 を含めて非 2xx は全部 502、etags は 404 だけ「口なし」
//! (`digest: None`) でそれ以外の非 2xx と parse 失敗は 502 ([`crate::unko_gaps::read_etags_body`])。
//! `dtakoEvents` / `dtakoEtags` に渡すのは query の鍵 (`date_from`・`date_to`・`page_size`・`after_driver_cd`) だけで、
//! path・method (GET)・tenant は auth-worker 側で固定される。

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveDateTime};
use kintai_kosoku::alc_events::{
    event_to_json, fetch_all_events, fetch_dtako_month_read, merge_all_rows,
    overlapping_operations, AlcEventsError, AlcEventsGet, EventsPage, RawEvent, UpstreamAll,
    UpstreamEtags,
};
use kintai_kosoku::anchors::HeadAnchors;
use kintai_kosoku::fold_flow::{BadMonth, DtakoMonthRead, FoldEventsIo, Warning};
use kintai_kosoku::kintai_push::jst_at;
use kintai_kosoku::kosoku::{alc_open_shift_starts, driver_shifts};
use kintai_kosoku::window::{month_head_anchors, HeadPunch};

use crate::common::Fail;
use crate::fold_routes::AlcEventsRequest;
use crate::unko_gaps::{etags_search_between, read_etags_body, RpcResult};

/// auth-worker の `KintaiAlcEntrypoint` の 2 メソッド。どちらも失敗を throw せず `{status, body}` で返す
/// (binding が無い・RPC 自体が落ちたときだけ `Err`)。
#[allow(async_fn_in_trait)]
pub trait AlcRpc {
    /// `dtakoEvents(search)` (alc の `GET /api/dtako/events`)。
    async fn dtako_events(&self, search: &str) -> Result<RpcResult, Fail>;
    /// `dtakoEtags(search)` (alc の `GET /api/dtako/events/etags`)。
    async fn dtako_etags(&self, search: &str) -> Result<RpcResult, Fail>;
}

/// `[from, to)` の `TIMESTAMPTZ` (JST の壁時計を [`pushed_window`] で変換したもの)。
pub type Span = (DateTime<FixedOffset>, DateTime<FixedOffset>);

/// relay が送った打刻の写し (`kintai.kintai_events`) の読み 3 本。SQL と行の形は `kintai_kosoku::pushed_events`
/// (root の `PgKintaiEventsRepo` と同じ)。失敗は 502 で、本文に DB の message・接続先を載せない。
#[allow(async_fn_in_trait)]
pub trait PushedEventsRead {
    /// `ALL_EVENTS_SQL` (source は `PUSHED_SOURCES`) の行を 5 キーの JSON で。
    async fn pushed_all_events(&self, span: Span) -> Result<Vec<serde_json::Value>, Fail>;
    /// `HEAD_RUN_ENDS_SQL` の (乗務員CD, 運行NO)。
    async fn pushed_head_run_ends(&self, span: Span) -> Result<Vec<(u64, String)>, Fail>;
    /// `HEAD_PUNCHES_SQL` の月初時点の打刻の姿。
    async fn pushed_head_punches(&self, span: Span) -> Result<Vec<HeadPunch>, Fail>;
}

/// 勤怠 Worker の fold の読み。`today` は月ゲートの末尾検知の切り下げに使う (JST の日付。呼び手が時計から作る)。
pub struct WorkerFoldEvents<'a, A, P> {
    pub alc: &'a A,
    pub pushed: &'a P,
    pub today: NaiveDate,
}

/// `[from, to)` の JST の壁時計を `TIMESTAMPTZ` に (root の `PgKintaiEventsRepo` と同じ `jst_at`。読めなければ 502)。
pub fn pushed_window(from: &str, to: &str) -> Result<Span, Fail> {
    let at = |s: &str| jst_at(s).map_err(|e| Fail::new(502, e.0));
    Ok((at(from)?, at(to)?))
}

/// `dtakoEvents` に渡す query 文字列 (鍵は [`EventsPage::params`] の順。値は form-urlencode する)。
pub fn events_search(page: &EventsPage) -> String {
    form_urlencoded::Serializer::new(String::new())
        .extend_pairs(page.params())
        .finish()
}

/// `dtakoEvents` の戻りを読む。**404 を含めて非 2xx は全部 502** (root の `get` と同じ — 生イベントの口が無いのは
/// 「判定できない」ではなく読めない)。
pub fn read_events(res: &RpcResult) -> Result<UpstreamAll, Fail> {
    if !(200..300).contains(&res.status) {
        let (status, excerpt): (u16, String) = (res.status, res.body.chars().take(200).collect());
        let msg = format!("alc dtako-events status {status}: {excerpt}");
        return Err(Fail::new(502, msg));
    }
    let parse = |e: serde_json::Error| Fail::new(502, format!("alc dtako-events parse: {e}"));
    serde_json::from_str(&res.body).map_err(parse)
}

/// 共有 crate の流れが期間・月・ページングで落ちたとき (502。文面は root の `KintaiRepoError::QueryFailed` と同じ)。
impl From<AlcEventsError> for Fail {
    fn from(e: AlcEventsError) -> Self {
        Fail::new(502, e.to_string())
    }
}

/// fold の流れが月を読めなかったとき (502。文面は `bad month: …`)。
impl From<BadMonth> for Fail {
    fn from(e: BadMonth) -> Self {
        Fail::new(502, e.to_string())
    }
}

/// [`WorkerFoldEvents::alc_head_anchors`] の遡りの暴走止め (日)。どこまで遡るかはデータで決める (#317 と同じく
/// 日数では決めない) が、勤務が切れないまま何十日も続くデータで読みを止めないための上限。実データでは 1〜3 日で止まる。
pub const ALC_HEAD_LOOKBACK_MAX_DAYS: i64 = 31;

impl<A: AlcRpc, P: PushedEventsRead> WorkerFoldEvents<'_, A, P> {
    /// 打刻を持たない乗務員 (行が全部 alc) の遡り起点 = **月初の時点で開いている勤務の始まり**
    /// ([`alc_open_shift_starts`]、ユーザー決定 2026-10-16)。写しの起点 (#317) は運行・打刻の写しからしか作らない
    /// ので、写しの無い会社はここで作る。
    ///
    /// 月初の前 1 日から月初の後 1 日までを読んで勤務を組み、月初をまたぐ勤務の始まりを起点にする。最初の勤務が読んだ
    /// 範囲の端から始まる乗務員がいれば、勤務 (日締めの起点) はもっと前から続いているかもしれないので 1 日ずつ遡って読み直す
    /// ([`ALC_HEAD_LOOKBACK_MAX_DAYS`] まで)。読みは写しの行も合わせるので、打刻のある乗務員 (大石) は対象にならない。
    async fn alc_head_anchors(
        &self,
        month_start: NaiveDateTime,
    ) -> Result<std::collections::BTreeMap<u64, NaiveDateTime>, Fail> {
        let fmt = |t: NaiveDateTime| t.format("%Y-%m-%d %H:%M:%S").to_string();
        let to = fmt(month_start + Duration::days(1));
        let mut days = 1;
        loop {
            let from = month_start - Duration::days(days);
            let (rows, _) = FoldEventsIo::all_events_between(self, &fmt(from), &to).await?;
            let (open, at_edge) = alc_open_shift_starts(rows, month_start, from);
            if days >= ALC_HEAD_LOOKBACK_MAX_DAYS || !at_edge {
                return Ok(open);
            }
            days += 1;
        }
    }
}

/// 運行 1 本ぶんの行の要約 (診断の口の `operations[]`)。
fn operation_summary(unko_no: &str, events: &[&RawEvent]) -> serde_json::Value {
    let fmt = |t: NaiveDateTime| t.format("%Y-%m-%d %H:%M:%S").to_string();
    let mut states: std::collections::BTreeMap<String, usize> = Default::default();
    for e in events {
        *states
            .entry(e.state.clone().unwrap_or_default())
            .or_default() += 1;
    }
    let first = events.iter().map(|e| e.start).min().map(fmt);
    let last = events
        .iter()
        .map(|e| e.end.unwrap_or(e.start))
        .max()
        .map(fmt);
    serde_json::json!({
        "unko_no": unko_no,
        "vehicle": events.first().and_then(|e| e.vehicle.clone()),
        "first": first,
        "last": last,
        "events": events.len(),
        "states": states,
    })
}

/// `GET /api/kintai/alc-events` (診断。書かない): fold と同じ読み ([`fetch_all_events`] + 写しの借りる行) の
/// **1 人ぶん**と、その行から fold と同じ組み方で作った勤務 ([`driver_shifts`])。
///
/// 応答: `driver`・`from`・`to` (`[from, to)`)・`alc_only`・`shifts[]` (`start`・`end`・`minutes`)・
/// `operations[]` (運行NO ごとの最初と最後の時刻・行数・`イベント名` ごとの行数)・`events[]` (alc の行。運行NO・車輌つき)・
/// `borrowed[]` (写しから借りた打刻・運行の確定イベント)・`warnings[]` (上流の警告)。
pub async fn driver_alc_events<A: AlcRpc, P: PushedEventsRead>(
    io: &WorkerFoldEvents<'_, A, P>,
    req: &AlcEventsRequest,
) -> Result<serde_json::Value, Fail> {
    let fmt = |t: NaiveDateTime| t.format("%Y-%m-%d %H:%M:%S").to_string();
    let (events, warnings) = fetch_all_events(io, &req.from, &req.to).await?;
    let driver = req.driver as i64;
    let mine: Vec<RawEvent> = events
        .into_iter()
        .filter(|e| e.driver_id == Some(driver))
        .collect();
    let mut warnings = warnings;
    warnings.extend(overlapping_operations(&mine));
    let borrowed: Vec<serde_json::Value> = io
        .pushed
        .pushed_all_events(pushed_window(&req.from, &req.to)?)
        .await?
        .into_iter()
        .filter(|r| r["driver_id"].as_i64() == Some(driver))
        .collect();
    let rows = merge_all_rows(mine.clone(), Some(borrowed.clone()));
    let (alc_only, shifts) = driver_shifts(&rows);
    let mut by_op: std::collections::BTreeMap<String, Vec<&RawEvent>> = Default::default();
    for e in &mine {
        by_op
            .entry(e.unko_no.clone().unwrap_or_default())
            .or_default()
            .push(e);
    }
    let operations: Vec<_> = by_op
        .iter()
        .map(|(u, evs)| operation_summary(u, evs))
        .collect();
    let shifts: Vec<_> = shifts
        .into_iter()
        .map(|(s, e)| serde_json::json!({"start": fmt(s), "end": fmt(e), "minutes": (e - s).num_minutes()}))
        .collect();
    Ok(serde_json::json!({
        "driver": req.driver,
        "from": req.from,
        "to": req.to,
        "alc_only": alc_only,
        "shifts": shifts,
        "operations": operations,
        "events": mine.into_iter().map(event_to_json).collect::<Vec<_>>(),
        "borrowed": borrowed,
        "warnings": warnings.into_iter().map(|w| w.text).collect::<Vec<_>>(),
    }))
}

impl<A: AlcRpc, P> AlcEventsGet for WorkerFoldEvents<'_, A, P> {
    type Error = Fail;

    async fn events_page(&self, page: &EventsPage) -> Result<UpstreamAll, Fail> {
        read_events(&self.alc.dtako_events(&events_search(page)).await?)
    }

    async fn etags(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Option<UpstreamEtags>, Fail> {
        let search = etags_search_between(date_from, date_to);
        read_etags_body(&self.alc.dtako_etags(&search).await?)
    }
}

impl<A: AlcRpc, P: PushedEventsRead> FoldEventsIo for WorkerFoldEvents<'_, A, P> {
    type Error = Fail;

    /// 遡り起点。写しの 2 本 (規則は root と同じ `month_head_anchors`) に、**写しを持たない乗務員の起点を alc の行から**
    /// 足す ([`WorkerFoldEvents::alc_head_anchors`])。両方あれば早いほう。
    async fn month_head_anchors(&self, month_start: &str, to: &str) -> Result<HeadAnchors, Fail> {
        let span = pushed_window(month_start, to)?;
        let runs = self.pushed.pushed_head_run_ends(span).await?;
        let punches = self.pushed.pushed_head_punches(span).await?;
        let mut anchors = month_head_anchors(month_start, &runs, &punches);
        for (cd, at) in self.alc_head_anchors(span.0.naive_local()).await? {
            let at = at.format("%Y-%m-%d %H:%M:%S").to_string();
            let slot = anchors.entry(cd).or_insert_with(|| at.clone());
            if at < *slot {
                *slot = at;
            }
        }
        Ok(anchors)
    }

    /// alc のページング → 写しの全員行 (root と同じ順)。写しからは alc に口が無い source だけを借りる。
    async fn all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<(Vec<serde_json::Value>, Vec<Warning>), Fail> {
        let (events, mut warnings) = fetch_all_events(self, from, to).await?;
        // 2 台の運行が同じ時間に重なっていたらエラーとして出す (勤務と実働が正しく出ない)
        warnings.extend(overlapping_operations(&events));
        let borrowed = self
            .pushed
            .pushed_all_events(pushed_window(from, to)?)
            .await?;
        Ok((merge_all_rows(events, Some(borrowed)), warnings))
    }

    async fn dtako_month_read_since(
        &self,
        month: &str,
        since: Option<NaiveDate>,
    ) -> Result<DtakoMonthRead, Fail> {
        fetch_dtako_month_read(self, month, since, self.today).await
    }
}
