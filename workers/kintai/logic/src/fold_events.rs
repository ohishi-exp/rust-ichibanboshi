//! 勤怠 Worker の fold の読み 3 本 (`kintai_kosoku::fold_flow::FoldEventsIo`) の実装 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `HttpKintaiEventsRepo` (alc を reqwest で読み、MariaDB の fallback を合流する) と**同じ読み方**を、往復だけ
//! 差し替えて作る。合流・ページング・指紋・入力の欠けの検知は共有 crate の `kintai_kosoku::alc_events` (root と同じもの):
//!
//! | 読み | root | 勤怠 Worker |
//! |---|---|---|
//! | 遡り起点 | fallback (MariaDB) の `HEAD_RUN_ENDS_SQL` → `HEAD_PUNCHES_SQL` | 同じ 2 本を 1 接続で ([`MariadbRun`]) |
//! | 全乗務員の行 | alc のページング → fallback の `ALL_EVENTS_SQL` を合流 | alc は auth-worker の `dtakoEvents` ([`AlcRpc`])、MariaDB は同じ SQL |
//! | dtako の指紋 | alc の etags | auth-worker の `dtakoEtags` |
//!
//! 往復は 2 つの小さな trait で受け取る (worker crate が env の binding と MariaDB の接続で実装する。テストは fake)。
//! **非 2xx の扱いは root の fold と同じ**: 生イベントは 404 を含めて非 2xx は全部 502、etags は 404 だけ「口なし」
//! (`digest: None`) でそれ以外の非 2xx と parse 失敗は 502 ([`crate::unko_gaps::read_etags_body`])。
//! `dtakoEvents` / `dtakoEtags` に渡すのは query の鍵 (`date_from`・`date_to`・`page_size`・`after_driver_cd`) だけで、
//! path・method (GET)・tenant は auth-worker 側で固定される。

use chrono::NaiveDate;
use kintai_kosoku::alc_events::{
    fetch_all_events, fetch_dtako_month_read, merge_all_rows, AlcEventsError, AlcEventsGet,
    EventsPage, UpstreamAll, UpstreamEtags,
};
use kintai_kosoku::anchors::HeadAnchors;
use kintai_kosoku::fold_flow::{BadMonth, DtakoMonthRead, FoldEventsIo, Warning};
use kintai_mysql::response::Row;

use crate::common::Fail;
use crate::kosoku_reads::{all_events_sql, anchors_between, head_sqls_between};
use crate::mariadb_rows::{all_event_row, rows_to_json};
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

/// 社内 MariaDB に 1 接続を開いて SQL を並びのまま流し、結果の行を同じ並びで返す (失敗は 502)。
#[allow(async_fn_in_trait)]
pub trait MariadbRun {
    async fn run(&self, sqls: &[String]) -> Result<Vec<Vec<Row>>, Fail>;
}

/// 勤怠 Worker の fold の読み。`today` は月ゲートの末尾検知の切り下げに使う (JST の日付。呼び手が時計から作る)。
pub struct WorkerFoldEvents<'a, A, M> {
    pub alc: &'a A,
    pub mariadb: &'a M,
    pub today: NaiveDate,
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

impl<A: AlcRpc, M> AlcEventsGet for WorkerFoldEvents<'_, A, M> {
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

impl<A: AlcRpc, M: MariadbRun> FoldEventsIo for WorkerFoldEvents<'_, A, M> {
    type Error = Fail;

    /// 遡り起点は MariaDB だけ (alc に口が無い。root も fallback へ委譲する)。
    async fn month_head_anchors(&self, month_start: &str, to: &str) -> Result<HeadAnchors, Fail> {
        let sqls = head_sqls_between(month_start, to)?;
        let sets = self.mariadb.run(&sqls).await?;
        let [runs, punches] = <[Vec<Row>; 2]>::try_from(sets).map_err(|_| shape())?;
        anchors_between(month_start, &runs, &punches)
    }

    /// alc のページング → MariaDB の全員行 (root と同じ順)。MariaDB からは alc に口が無い source だけを借りる。
    async fn all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<(Vec<serde_json::Value>, Vec<Warning>), Fail> {
        let (events, warnings) = fetch_all_events(self, from, to).await?;
        let sets = self.mariadb.run(&[all_events_sql(from, to)?]).await?;
        let [rows] = <[Vec<Row>; 1]>::try_from(sets).map_err(|_| shape())?;
        let borrowed = rows_to_json(&rows, all_event_row)?;
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

/// [`MariadbRun`] が流した本数と違う数の結果を返した (実装の誤り。502)。
fn shape() -> Fail {
    crate::common::mariadb_fail("rows:sets")
}
