//! alc (`rust-alc-api`) の `GET /api/dtako/events` (生イベント) と `GET /api/dtako/events/etags` (月ゲートの材料) を
//! 読んだあとの純粋部分と、読みの流れ (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/kintai_http_repo.rs` から移した。root (Cloud Run 版、alc へ reqwest) と勤怠 Worker (auth-worker の
//! `KintaiAlcEntrypoint` の RPC) が**同じものを使う** (写さない)。往復そのものだけを [`AlcEventsGet`] で受け取る:
//!
//! | ここ | 中身 |
//! |---|---|
//! | 応答の型 | [`UpstreamAll`]・[`UpstreamSingle`]・[`UpstreamEtags`] (要る部分だけ deserialize する) |
//! | 生 CSV 行 → 戻り値 | [`EventCols`]・[`row_to_event`]・[`collect`] (`運行NO` で重複排除)・[`event_to_all_json`] |
//! | 合流 | [`merge_all_rows`] (上流に口が無い source だけを MariaDB 等から借りる、[`is_borrowed_source`]) |
//! | 期間の分割 | [`date_chunks`]・[`query_dates`]・[`month_etags_bounds`] |
//! | 読みの流れ | [`fetch_all_events`] (31 日 chunk × keyset ページング)・[`fetch_dtako_month_read`] (etags → 指紋・warnings) |
//! | 入力の欠けの検知 | [`InputCoverage`]・[`missing_input_warnings`] |
//! | 月ゲートの指紋 | [`digest_from_pairs`] |
//!
//! 時計は引数で受ける (wasm32 では `Utc::now` を使わない)。
//!
//! ## 上流は生 CSV 行を返す — 写すのはこちらの責務
//!
//! 上流は**畳まない**。R2 の per-運行 `KUDGIVT.csv` を `{headers, rows}` のまま返す (勤怠計算は一番星固有で
//! マルチテナント基盤に置かないという #205 の決定 3)。MariaDB の `dtako_events` ブランチとの列の対応は
//! root の `src/kintai_http_repo.rs` の module docs。
//!
//! ## 期間の写し方
//!
//! 読みの期間は `[from, to)` の `YYYY-MM-DD HH:MM:SS`、上流は**日付の閉区間** `date_from..=date_to`。粒度が違うので
//! 上流には広めに投げ ([`query_dates`])、**返ってきた行を `EVENTS_SQL` と同じ 2 条件で絞り直す** (`in_window`)。
//!
//! - 上流の期間上限が単一乗務員 366 日 / 全乗務員 31 日なので、超える期間は [`date_chunks`] で分割して複数回叩く
//!   (`month_range` は 32〜34 日になるため全乗務員版では必ず分割が要る)
//! - 全乗務員版は `page_size` / `after_driver_cd` の keyset ページングを回しきる
//! - 同じ運行が複数の chunk / 複数の乗務員グループに現れるので `運行NO` で重複排除する。
//!   **CSV の中の重複行は落とさない** — 取り込みが 2 回走った重複は `kosoku.rs` 側が扱う話で、ここで消すと
//!   MariaDB 経路と値が割れる

use std::collections::{HashMap, HashSet};

use chrono::{NaiveDate, NaiveDateTime};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::fold_flow::{DtakoMonthRead, EtagsRead, UnsplitOperation, Warning};
use crate::window::{tail_expected, unko_no_start_date, window_holds, MAX_TAIL_GAP_DAYS};

/// 上流の生イベントの endpoint path (root が叩く。勤怠 Worker は auth-worker の `dtakoEvents` 越しで、path は渡さない)。
pub const EVENTS_PATH: &str = "/api/dtako/events";

/// 上流の月ゲート用 etags endpoint (Refs #205 実装計画 13)。R2 の LIST だけで `{unko_no, etag}` を返す — CSV は読まない。
/// alc に口が無い環境 (404) では [`AlcEventsGet::etags`] が `Ok(None)` を返し、月ゲートを諦めて全量読みへ degrade する。
pub const ETAGS_PATH: &str = "/api/dtako/events/etags";

/// 上流の期間上限 (単一乗務員、閉区間の日数)。
pub const MAX_RANGE_DAYS_SINGLE: i64 = 366;

/// 上流の期間上限 (全乗務員)。
pub const MAX_RANGE_DAYS_ALL: i64 = 31;

/// 全乗務員版の 1 ページあたり乗務員数。上流の上限 (50) に合わせて往復を減らす。
pub const PAGE_SIZE: i64 = 50;

/// ページングの打ち切り。無限ループを黙って回さず error にする。
pub const MAX_PAGES: usize = 200;

/// 上流に投げる期間を何日前から始めるか。
///
/// `EVENTS_SQL` の 4 番目のブランチ (期間内に終わる区間、開始は期間より前) を拾うために要る。区間の長さで一番長いのは
/// 休息 (実測 1,123 分 ≒ 19 時間) なので 2 日あれば足りる — #205 の再計算が「差分日 ± 2 日」を対象にしているのと同じ幅。
pub const LOOKBACK_DAYS: i64 = 2;

/// CSV の日時形式 (`2026/02/24 14:40:56`)。
const CSV_DATETIME_FORMATS: [&str; 2] = ["%Y/%m/%d %H:%M:%S", "%Y/%m/%d %k:%M:%S"];

/// 戻り値の日時形式 (`DATE_FORMAT(..., '%Y-%m-%d %H:%i:%s')` と同じ)。
const OUT_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// この経路が名乗る `source`。MariaDB の 3 / 4 番目のブランチと同じ値。
pub const SOURCE_DTAKO_EVENTS: &str = "dtako_events";

// ── 上流の応答 (要る部分だけ deserialize する) ──────────────────────────────

/// 1 運行分の生 CSV。`headers` を運行ごとに持つ形をそのまま受ける。
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamOperation {
    #[serde(default)]
    pub unko_no: String,
    #[serde(default)]
    pub headers: Vec<String>,
    #[serde(default)]
    pub rows: Vec<Vec<String>>,
}

/// 単一乗務員版の応答。
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamSingle {
    #[serde(default)]
    pub operations: Vec<UpstreamOperation>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamDriverGroup {
    #[serde(default)]
    pub operations: Vec<UpstreamOperation>,
}

/// 全乗務員版の応答 1 ページ。
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamAll {
    #[serde(default)]
    pub drivers: Vec<UpstreamDriverGroup>,
    #[serde(default)]
    pub next_after_driver_cd: Option<String>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// `GET /api/dtako/events/etags` の 1 件。`etag` が無い (R2 に無い / upload 未完了) 運行は `None` のまま持ち出す —
/// digest 計算が空文字と区別できるようにする。
///
/// `driver_cds` は Refs #205 の 32 で足した前方互換フィールド。alc が返さない環境では常に空 — その場合
/// [`InputCoverage`] は全 item を 1 グループとして扱う。**複数形なのは 1 運行に複数の乗務員 (主 / 副運転) が付き得るため**
/// (`list_operations_for_drivers` の `DISTINCT ON (driver_id, unko_no)`)。この場合その運行の開始日は `driver_cds`
/// 全員の `last` に寄与させる (親の判断、2026-07-31)。
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamEtagItem {
    pub unko_no: String,
    #[serde(default)]
    pub etag: Option<String>,
    #[serde(default)]
    pub driver_cds: Vec<String>,
}

/// `GET /api/dtako/events/etags` の応答。
#[derive(Debug, Clone, Deserialize)]
pub struct UpstreamEtags {
    #[serde(default)]
    pub items: Vec<UpstreamEtagItem>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub unsplit: Vec<UnsplitOperation>,
    #[serde(default)]
    pub unsplit_total: usize,
}

/// digest の材料 1 件 `(unko_no, etag, driver_cds)`。
pub type EtagPair = (String, Option<String>, Vec<String>);

// ── 列の解決 ──────────────────────────────────────────────────────────────

/// 1 運行の `headers` から要る列の位置を引く。
///
/// `KUDGIVT.csv` は upload 時点のデジタコ出力に依存し、列の並びも有無も運行ごとに違い得る。だから毎運行ごとに引き直す。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventCols {
    unko_no: Option<usize>,
    driver_cd: Option<usize>,
    start_at: usize,
    end_at: Option<usize>,
    event_name: usize,
    vehicle: Option<usize>,
}

fn find_col(headers: &[String], name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim() == name)
}

impl EventCols {
    /// 必須列 (`開始日時` / `イベント名`) が無ければ `None`。
    ///
    /// `driver_cd` は `対象乗務員CD` を優先し、無ければ `乗務員CD1` に落とす (どちらも無ければ `None` = `driver_id` が
    /// `null` になる)。`乗務員CD1` は運行の主運転者で全行同じ値になるため、優先すると 2 名乗務で取り違える。
    pub fn resolve(headers: &[String]) -> Option<Self> {
        Some(Self {
            unko_no: find_col(headers, "運行NO"),
            driver_cd: find_col(headers, "対象乗務員CD").or_else(|| find_col(headers, "乗務員CD1")),
            start_at: find_col(headers, "開始日時")?,
            end_at: find_col(headers, "終了日時"),
            event_name: find_col(headers, "イベント名")?,
            vehicle: find_col(headers, "車輌名"),
        })
    }
}

fn field(row: &[String], idx: usize) -> Option<&str> {
    row.get(idx).map(|s| s.trim()).filter(|s| !s.is_empty())
}

fn opt_field(row: &[String], idx: Option<usize>) -> Option<&str> {
    idx.and_then(|i| field(row, i))
}

fn parse_csv_datetime(s: &str) -> Option<NaiveDateTime> {
    CSV_DATETIME_FORMATS
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(s, f).ok())
}

// ── 生 CSV 行 → 戻り値 ─────────────────────────────────────────────────────

/// MariaDB の `dtako_events` ブランチ 1 行に相当する中間表現。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawEvent {
    pub start: NaiveDateTime,
    pub end: Option<NaiveDateTime>,
    pub driver_id: Option<i64>,
    /// `イベント名`。空欄は `null` にする (MariaDB の `NULL` と同じ扱い)。
    pub state: Option<String>,
    pub unko_no: Option<String>,
    pub vehicle: Option<String>,
}

/// CSV 1 行を写す。`開始日時` が読めない行は捨てる (MariaDB では NOT NULL の列で、上流の parser も同じ行を落としている)。
pub fn row_to_event(cols: &EventCols, row: &[String], op_unko_no: &str) -> Option<RawEvent> {
    let start = parse_csv_datetime(field(row, cols.start_at)?)?;
    let unko_no = opt_field(row, cols.unko_no)
        .map(str::to_string)
        .or_else(|| Some(op_unko_no.to_string()).filter(|s| !s.is_empty()));
    Some(RawEvent {
        start,
        end: opt_field(row, cols.end_at).and_then(parse_csv_datetime),
        driver_id: opt_field(row, cols.driver_cd).and_then(|s| s.parse::<i64>().ok()),
        state: field(row, cols.event_name).map(str::to_string),
        unko_no,
        vehicle: opt_field(row, cols.vehicle).map(str::to_string),
    })
}

/// `EVENTS_SQL` の 3 / 4 番目のブランチと同じ絞り込み (期間内に始まる区間 + 期間内に終わる区間で開始が期間より前)。
fn in_window(ev: &RawEvent, from: NaiveDateTime, to: NaiveDateTime) -> bool {
    window_holds(ev.start, ev.end, from, to)
}

fn fmt_dt(dt: NaiveDateTime) -> String {
    dt.format(OUT_DATETIME_FORMAT).to_string()
}

/// 単一乗務員版の 1 行 (`row_to_json` と同じキー構成)。
pub fn event_to_json(ev: RawEvent) -> serde_json::Value {
    serde_json::json!({
        "datetime": fmt_dt(ev.start),
        "end_datetime": ev.end.map(fmt_dt),
        "driver_id": ev.driver_id,
        "source": SOURCE_DTAKO_EVENTS,
        "state": ev.state,
        "unko_no": ev.unko_no,
        "vehicle": ev.vehicle,
    })
}

/// 全乗務員版の 1 行。`vehicle` を**キーごと出さない** — `all_row_to_json` が「読んでいない列は `null` に
/// しない」としているのに揃える。`unko_no` は出す — alc だけの会社は運行の区切りに使う (`kosoku::run_bounds`)。
pub fn event_to_all_json(ev: RawEvent) -> serde_json::Value {
    serde_json::json!({
        "datetime": fmt_dt(ev.start),
        "end_datetime": ev.end.map(fmt_dt),
        "driver_id": ev.driver_id,
        "source": SOURCE_DTAKO_EVENTS,
        "state": ev.state,
        "unko_no": ev.unko_no,
    })
}

/// `ORDER BY datetime, source` 相当。
pub fn sort_rows(rows: &mut [serde_json::Value]) {
    rows.sort_by_key(key_dt_source);
}

/// `ORDER BY driver_id, datetime, source` 相当 (`driver_id` の NULL は先頭)。
pub fn sort_rows_by_driver(rows: &mut [serde_json::Value]) {
    rows.sort_by(|a, b| {
        (a["driver_id"].as_i64(), key_dt_source(a))
            .cmp(&(b["driver_id"].as_i64(), key_dt_source(b)))
    });
}

fn key_dt_source(v: &serde_json::Value) -> (String, String) {
    let s = |k: &str| v[k].as_str().unwrap_or_default().to_string();
    (s("datetime"), s("source"))
}

/// 上流に口が無い読み出しの委譲先 (MariaDB 等) から借りるのは**上流に口が無い source だけ** (打刻と運行の確定イベント)。
/// `dtako_events` を混ぜると alc の行と二重になる。
pub fn is_borrowed_source(v: &serde_json::Value) -> bool {
    v["source"].as_str() != Some(SOURCE_DTAKO_EVENTS)
}

/// 全乗務員版の合流: alc の行 ([`event_to_all_json`]) に委譲先の行のうち借りる source だけを足し、
/// `ORDER BY driver_id, datetime, source` に並べる。`borrowed` が `None` は委譲先が無い形態。
pub fn merge_all_rows(
    events: Vec<RawEvent>,
    borrowed: Option<Vec<serde_json::Value>>,
) -> Vec<serde_json::Value> {
    let mut rows: Vec<serde_json::Value> = events.into_iter().map(event_to_all_json).collect();
    if let Some(b) = borrowed {
        rows.extend(b.into_iter().filter(is_borrowed_source));
    }
    sort_rows_by_driver(&mut rows);
    rows
}

/// 運行の時間帯を決める行 (作業と運行の境界)。休息・休憩・エンジン ON・道路種別などは運行の外へはみ出すことがある
/// (車輌を乗り換えた後も前の車輌の休息が開いたまま等) ので使わない。
const OPERATION_SPAN_STATES: [&str; 8] = [
    "運転",
    "積み",
    "降し",
    "待機",
    "その他",
    "フェリー",
    "運行開始",
    "運行終了",
];

/// **同じ乗務員の 2 つの運行が時間で重なっている**ものを、「重複」の warning にする。重なった時間帯と分数を出す
/// (ユーザー決定 2026-10-17「2 台かぶりはわかるように出してくれたらいい」「重なりは重複で警告だせ」)。
///
/// 乗務員CD の付け間違いのほか、車輌の乗り換えでも起きる。勤務は 2 本の行を 1 本の時間の流れとして組むので、
/// 重なった時間の勤務と実働は theearth と合わないことがある。実測:
///
/// - 北海 乗務員 15 / 2026-06-03: 車輌 245 (03:07 出庫) と 226 (03:26 出庫〜16:55 帰庫) が重なり、theearth より
///   実働が 482 分少なかった
/// - 北海 乗務員 16 / 2026-09-18: 5506 を 16:46 に休息で止めたまま (運行終了 21:16) 237 で 17:24 から走った (1 人乗務の
///   乗り換え)。実働は theearth と一致し、拘束は theearth が 134 分多い
///
/// 運行の時間帯は [`OPERATION_SPAN_STATES`] の行の最初の始まりから最後の終わりまで。封は止めない (入力の欠けではない)。
pub fn overlapping_operations(events: &[RawEvent]) -> Vec<Warning> {
    type Span = (NaiveDateTime, NaiveDateTime, Option<String>);
    let mut ops: std::collections::BTreeMap<(i64, String), Span> = Default::default();
    for e in events {
        let (Some(driver), Some(unko_no), Some(state)) = (e.driver_id, &e.unko_no, &e.state) else {
            continue;
        };
        if !OPERATION_SPAN_STATES.contains(&state.as_str()) {
            continue;
        }
        let end = e.end.unwrap_or(e.start);
        let slot =
            ops.entry((driver, unko_no.clone()))
                .or_insert((e.start, end, e.vehicle.clone()));
        slot.0 = slot.0.min(e.start);
        slot.1 = slot.1.max(end);
    }
    let mut by_driver: std::collections::BTreeMap<i64, Vec<(String, Span)>> = Default::default();
    for ((driver, unko_no), span) in ops {
        by_driver.entry(driver).or_default().push((unko_no, span));
    }
    let fmt = |t: NaiveDateTime| t.format(OUT_DATETIME_FORMAT).to_string();
    let label = |unko_no: &str, (s, e, v): &Span| {
        let vehicle = v.as_deref().unwrap_or("車輌不明");
        format!("{unko_no} ({vehicle}、{}〜{})", fmt(*s), fmt(*e))
    };
    let mut out = Vec::new();
    for (driver, mut list) in by_driver {
        list.sort_by_key(|(u, (s, _, _))| (*s, u.clone()));
        for (i, (ua, a)) in list.iter().enumerate() {
            for (ub, b) in list.iter().skip(i + 1).take_while(|(_, b)| b.0 < a.1) {
                tracing::warn!(driver, a = %ua, b = %ub, "2 台の運行の重複");
                let (from, to) = (b.0, a.1.min(b.1));
                let minutes = (to - from).num_minutes();
                out.push(Warning::diagnostic(format!(
                    "重複: 乗務員 {driver} の 2 台の運行が {}〜{} ({minutes} 分) 重なっています — {} と {}。重なった時間の勤務と実働は theearth と合わないことがあります",
                    fmt(from),
                    fmt(to),
                    label(ua, a),
                    label(ub, b)
                )));
            }
        }
    }
    out
}

/// 上流から取った運行を写して `[from, to)` で絞る。`運行NO` で重複排除する (`seen` は chunk・ページをまたいで持ち回る)。
pub fn collect(
    ops: Vec<UpstreamOperation>,
    seen: &mut HashSet<String>,
    window: (NaiveDateTime, NaiveDateTime),
    out: &mut Vec<RawEvent>,
) {
    for op in ops {
        if !op.unko_no.is_empty() && !seen.insert(op.unko_no.clone()) {
            continue;
        }
        let Some(cols) = EventCols::resolve(&op.headers) else {
            tracing::warn!(unko_no = %op.unko_no, "KUDGIVT に開始日時/イベント名が無い");
            continue;
        };
        for row in &op.rows {
            if let Some(ev) = row_to_event(&cols, row, &op.unko_no) {
                if in_window(&ev, window.0, window.1) {
                    out.push(ev);
                }
            }
        }
    }
}

/// 上流の warnings を握り潰さない (R2 の分割遅れで一部の運行が欠けたことが分かる)。
///
/// `tracing` は運用ログ、戻り値は**呼び出し側の応答**への持ち出し (どれも封を止める)。**ここでは束ねない** —
/// 重複排除と上限は呼び出し側の束 ([`crate::fold_flow::Warnings`]) が束ぜんたいに掛ける。
pub fn upstream_warnings(warnings: &[String]) -> Vec<Warning> {
    warnings
        .iter()
        .map(|w| {
            tracing::warn!(warning = %w, "alc dtako-events warning");
            Warning::blocking(w.as_str())
        })
        .collect()
}

// ── 期間の分割 ────────────────────────────────────────────────────────────

/// 月ゲート用の etags 取得範囲 `[月初, 翌月初]` (両端含む NaiveDate)。
///
/// **`month_range` が読む窓と同じものを覆う。** あちらは `[月初 00:00, 翌月2日 00:00)` = 「暦月 + 翌月 1 日ぶん」を
/// 読むので、月ゲートの digest がこの範囲を漏れなく覆っていないと、月末に日跨ぎする勤務の変化を見逃して
/// 「変わっていない」と誤判定しうる。閉区間なので上端は「翌月**初日**」。
pub fn month_etags_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next_first = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next_first))
}

/// 日付の閉区間 `from..=to` を `max_days` 日以内の閉区間へ割る。`from > to` なら空。
pub fn date_chunks(from: NaiveDate, to: NaiveDate, max_days: i64) -> Vec<(NaiveDate, NaiveDate)> {
    let mut out = Vec::new();
    let mut start = from;
    while start <= to {
        let end = (start + chrono::Duration::days(max_days - 1)).min(to);
        out.push((start, end));
        start = end + chrono::Duration::days(1);
    }
    out
}

/// 読みの `[from, to)` を、上流に投げる日付の閉区間へ ([`LOOKBACK_DAYS`] 遡り、上端は排他)。
pub fn query_dates(from: NaiveDateTime, to: NaiveDateTime) -> (NaiveDate, NaiveDate) {
    (
        from.date() - chrono::Duration::days(LOOKBACK_DAYS),
        (to - chrono::Duration::seconds(1)).date(),
    )
}

/// 読みの `[from, to)` (`YYYY-MM-DD HH:MM:SS`) を読む。
pub fn parse_window(
    from: &str,
    to: &str,
) -> Result<(NaiveDateTime, NaiveDateTime), AlcEventsError> {
    let p = |s: &str| {
        NaiveDateTime::parse_from_str(s, OUT_DATETIME_FORMAT).map_err(|e| {
            AlcEventsError::BadRange {
                value: s.to_string(),
                reason: e.to_string(),
            }
        })
    };
    Ok((p(from)?, p(to)?))
}

// ── 読みの流れ ────────────────────────────────────────────────────────────

/// 往復の外で起きる失敗 (文面は root が `KintaiRepoError::QueryFailed` に載せていたもののまま)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AlcEventsError {
    /// 読みの期間が `YYYY-MM-DD HH:MM:SS` として読めない。
    BadRange { value: String, reason: String },
    /// 上流が `next_after_driver_cd` を返し続けた ([`MAX_PAGES`])。
    PagingDidNotTerminate,
    /// 月が `YYYY-MM` として読めない。
    BadMonth(String),
}

impl std::fmt::Display for AlcEventsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadRange { value, reason } => write!(f, "bad range {value:?}: {reason}"),
            Self::PagingDidNotTerminate => f.write_str("alc dtako-events paging did not terminate"),
            Self::BadMonth(month) => write!(f, "bad month: {month}"),
        }
    }
}

/// 全乗務員版の 1 ページの要求。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventsPage {
    pub date_from: NaiveDate,
    pub date_to: NaiveDate,
    pub after_driver_cd: Option<String>,
}

impl EventsPage {
    /// query の鍵と値 (この順で載せる。`after_driver_cd` は 2 ページ目から)。
    pub fn params(&self) -> Vec<(&'static str, String)> {
        let mut params = vec![
            ("date_from", self.date_from.to_string()),
            ("date_to", self.date_to.to_string()),
            ("page_size", PAGE_SIZE.to_string()),
        ];
        if let Some(a) = &self.after_driver_cd {
            params.push(("after_driver_cd", a.clone()));
        }
        params
    }
}

/// alc への往復 2 種。root は reqwest、勤怠 Worker は auth-worker の `KintaiAlcEntrypoint` の RPC で実装する。
///
/// **非 2xx の扱いは実装の側で揃える**: 生イベントは 404 を含めて非 2xx は全部 `Err`、etags は 404 だけ `Ok(None)`
/// (口が無い = 月ゲートを諦める) でそれ以外の非 2xx と parse 失敗は `Err` (口はあるはずなのに読めなかった)。
#[allow(async_fn_in_trait)]
pub trait AlcEventsGet {
    type Error: From<AlcEventsError>;

    /// 全乗務員版の 1 ページ。
    async fn events_page(&self, page: &EventsPage) -> Result<UpstreamAll, Self::Error>;

    /// etags (`date_from..=date_to`)。`Ok(None)` は口が無い (404)。
    async fn etags(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Option<UpstreamEtags>, Self::Error>;
}

/// 全乗務員の `[from, to)`。[`MAX_RANGE_DAYS_ALL`] 日の chunk ごとに keyset ページングを回しきり、上流の warnings も
/// 並びのまま返す。期間が読めなければ往復の前に落とす。
pub async fn fetch_all_events<G: AlcEventsGet>(
    g: &G,
    from: &str,
    to: &str,
) -> Result<(Vec<RawEvent>, Vec<Warning>), G::Error> {
    let window = parse_window(from, to)?;
    let (qf, qt) = query_dates(window.0, window.1);
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    for (cf, ct) in date_chunks(qf, qt, MAX_RANGE_DAYS_ALL) {
        let mut page = EventsPage {
            date_from: cf,
            date_to: ct,
            after_driver_cd: None,
        };
        for n in 0..=MAX_PAGES {
            if n == MAX_PAGES {
                return Err(AlcEventsError::PagingDidNotTerminate.into());
            }
            let resp = g.events_page(&page).await?;
            warnings.extend(upstream_warnings(&resp.warnings));
            for group in resp.drivers {
                collect(group.operations, &mut seen, window, &mut out);
            }
            match resp.next_after_driver_cd {
                Some(next) => page.after_driver_cd = Some(next),
                None => break,
            }
        }
    }
    Ok((out, warnings))
}

/// etags の応答を digest の材料・突合と応答の素通しに使う控え・上流の warnings に分ける。
pub fn etags_read(parsed: UpstreamEtags) -> (Vec<EtagPair>, EtagsRead, Vec<Warning>) {
    let warnings = upstream_warnings(&parsed.warnings);
    let pairs: Vec<EtagPair> = parsed
        .items
        .into_iter()
        .map(|it| (it.unko_no, it.etag, it.driver_cds))
        .collect();
    // 突合 (Refs #205 の 37) 用に GCP 側の運行一覧を控える。digest の材料は変えない。
    // 乗務員CD も一緒に控えるのは逆方向の内訳を割るため (Refs #205 の 39)
    let unko_nos = pairs.iter().map(|(u, _, _)| u.clone()).collect();
    let driver_cds: HashMap<String, Vec<String>> = pairs
        .iter()
        .map(|(u, _, ds)| (u.clone(), ds.clone()))
        .collect();
    let etags = EtagsRead {
        unsplit: parsed.unsplit,
        unsplit_total: parsed.unsplit_total,
        unko_nos,
        driver_cds,
    };
    (pairs, etags, warnings)
}

/// 月ゲートの dtako 指紋に、etags の控え (`unsplit`・運行一覧) と warnings を添えて返す。始端は `since` まで下げる
/// (fold が月初より前から読む月は、その分の運行の CSV も封の材料に入れる。Refs ohishi-exp/nuxt-dtako-admin#1123)。
///
/// 末尾検知の母集団 ([`InputCoverage`] の `window_start`) は**月初のまま** — 前月に始まった運行を数えないのは
/// そちらの docs の決定。etags の口が無ければ (404) 指紋は `None` (「欠けている」ではなく「判定できない」)。
pub async fn fetch_dtako_month_read<G: AlcEventsGet>(
    g: &G,
    month: &str,
    since: Option<NaiveDate>,
    today: NaiveDate,
) -> Result<DtakoMonthRead, G::Error> {
    let (first, to) =
        month_etags_bounds(month).ok_or_else(|| AlcEventsError::BadMonth(month.to_string()))?;
    let from = since.map_or(first, |d| d.min(first));
    let Some(parsed) = g.etags(from, to).await? else {
        return Ok(DtakoMonthRead::digest_only(None));
    };
    let (p, etags, mut warnings) = etags_read(parsed);
    // 入力の欠けは alc からは見えない (索引に無い運行は warnings に出ない) ので、引いてきた一覧の形から自分で
    // 見つけて warnings へ足す (Refs #205 の 21)
    let cov = InputCoverage::measure(&p, first, to, today);
    // **警告の有無に関わらず毎回出す** (閾値を後から締めるための実測値)。マクロは 1 行に収める
    let (gap, cover) = (cov.gap_days().unwrap_or(-1), cov.summary());
    tracing::info!(gap, %cover, "kintai dtako input coverage");
    // alc がまだ driver_cds を返さない環境の切り分け用 (Refs #205 の 32)
    if !cov.driver_cd_source {
        tracing::info!("kintai dtako driver_cds 未対応: 月ぜんたい判定にフォールバック");
    }
    for w in missing_input_warnings(&cov) {
        let blocks = w.blocks_seal;
        tracing::warn!(warning = %w.text, blocks, "kintai dtako input gap");
        warnings.push(w);
    }
    Ok(DtakoMonthRead {
        digest: Some(digest_from_pairs(&p)),
        etags: Some(etags),
        warnings,
    })
}

// ── 入力 (dtako_events) の欠けの検知 (Refs #205 の 21) ─────────────────────

/// etags の一覧から測った「入力がどこまで届いているか」(Refs #205 の 21)。
///
/// **警告が立つかどうかに関わらず毎回 `tracing::info!` に出す** — 閾値 ([`MAX_TAIL_GAP_DAYS`]) は実測で決めたが、
/// 母集団は alc の `dtako_operations` を `reading_date` で引いたもので、その分布はこちらから観測できない。
/// その場のログに実測 gap が出ていれば 1 回の追い PR で締められる (親の判断、2026-07-31)。
///
/// ## 末尾検知は乗務員別 (Refs #205 の 32)
///
/// 月ぶん・全乗務員を跨いだ「たった 1 つの `last`」で測ると、1 人でも窓の端まで運行があれば `last` がそこに
/// 張り付き、故障している他の乗務員が埋もれて沈黙した (2026-06 本番、49 名影響)。**粒度を `driver_cd` 別に割り、
/// 乗務員ごとに `gap = expected - last` を出して閾値超えを数える。** `driver_cd` が無い item は空文字キーの
/// 1 グループにまとまり、旧実装と同じ結果になる。
///
/// ## 母集団は「その月に運行が始まった乗務員」だけ (Refs #205 の 37)
///
/// etags は**読み取り日**で引くので、窓の中に「ずっと前に始まった運行」が混ざる。その古い運行しか窓に無い乗務員を
/// 母集団に入れると全員が「末尾が欠けている」になる (本番で `乗務員73名の末尾が37日超`)。**運行が無いのは
/// 「末尾が欠けた」ではなく「そもそも稼働していない」**なので、窓の月に始まった運行を 1 件も持たない乗務員は
/// `per_driver_last` に入れない。「その乗務員の運行が丸ごと全部消えた」形は運行の突合 ([`crate::unko_diff`]) が
/// 名指しで拾う。`no_etag` の検知は母集団に関係なく従来どおり。閾値も乗務員別に合わせ直した (2 → 7 日)。
#[derive(Debug, Clone)]
pub struct InputCoverage {
    /// etags の item 数 (= 窓の中の運行数)。
    items: usize,
    /// `etag` が `null` の item 数 (R2 に CSV が無い)。
    no_etag: usize,
    /// 運行開始日の最小 / 最大 (全乗務員通し、summary 表示用)。1 件も読めなければ `None`。
    first: Option<NaiveDate>,
    last: Option<NaiveDate>,
    /// 末尾がここまで届いていてほしい日 ([`tail_expected`])。
    expected: NaiveDate,
    /// `driver_cds` (無ければ空文字 1 本) → その乗務員の運行開始日の最大値。**窓の月に始まった運行を持つ乗務員だけ**。
    per_driver_last: HashMap<String, NaiveDate>,
    /// `driver_cds` を持つ item が 1 件でもあったか。`false` なら alc がまだ `driver_cds` を返さない環境。
    driver_cd_source: bool,
}

impl InputCoverage {
    /// `pairs` を 1 度だけ走査して測る (応答は実測 1,100 件、O(n) に収める)。
    ///
    /// **1 運行が複数の `driver_cds` を持つ場合、その運行開始日は全員の `last` に寄与する。**
    /// **`window_start` より前に始まった運行は `per_driver_last` に入れない** (母集団の節)。`first` / `last` /
    /// `no_etag` は窓の中ぜんぶを見たままにする。
    pub fn measure(
        pairs: &[EtagPair],
        window_start: NaiveDate,
        window_end: NaiveDate,
        today: NaiveDate,
    ) -> Self {
        let mut no_etag = 0;
        let (mut first, mut last) = (None, None);
        let mut per_driver_last: HashMap<String, NaiveDate> = HashMap::new();
        let mut driver_cd_source = false;
        for (unko_no, etag, driver_cds) in pairs {
            if etag.is_none() {
                no_etag += 1;
            }
            if let Some(d) = unko_no_start_date(unko_no) {
                first = Some(first.map_or(d, |f: NaiveDate| f.min(d)));
                last = Some(last.map_or(d, |l: NaiveDate| l.max(d)));
                driver_cd_source = driver_cd_source || !driver_cds.is_empty();
                // 窓の月より前に始まった運行は末尾検知の母集団に入れない (#205 の 37)
                if d < window_start {
                    continue;
                }
                let keys = match driver_cds.is_empty() {
                    true => vec![String::new()],
                    false => driver_cds.clone(),
                };
                for key in keys {
                    per_driver_last
                        .entry(key)
                        .and_modify(|l: &mut NaiveDate| *l = (*l).max(d))
                        .or_insert(d);
                }
            }
        }
        Self {
            items: pairs.len(),
            no_etag,
            first,
            last,
            expected: tail_expected(window_end, today),
            per_driver_last,
            driver_cd_source,
        }
    }

    /// 末尾の不足日数 (全乗務員通し)。運行開始日が 1 件も読めなければ `None`。ログの `gap` に出す互換値 —
    /// 実際の警告判定は [`Self::tail_gap`] (乗務員別) が行う。
    pub fn gap_days(&self) -> Option<i64> {
        self.last.map(|l| (self.expected - l).num_days())
    }

    /// 閾値 ([`MAX_TAIL_GAP_DAYS`]) を超えた乗務員数と、その中の最大不足日数。1 人も超えていなければ `None`。
    pub fn tail_gap(&self) -> Option<(usize, i64)> {
        let over: Vec<i64> = self
            .per_driver_last
            .values()
            .map(|l| (self.expected - *l).num_days())
            .filter(|g| *g > MAX_TAIL_GAP_DAYS)
            .collect();
        over.iter().max().map(|&max_gap| (over.len(), max_gap))
    }

    /// `driver_cds` を持つ item が 1 件でもあったか。
    pub fn driver_cd_source(&self) -> bool {
        self.driver_cd_source
    }

    /// ログにも警告本文にも載せる 1 行の実測値。**`対象` = 末尾検知の母集団** (その月に運行が始まった乗務員の数)。
    pub fn summary(&self) -> String {
        let f = self
            .first
            .map_or_else(|| "?".to_string(), |d| d.to_string());
        let l = self.last.map_or_else(|| "?".to_string(), |d| d.to_string());
        let (n, z, e) = (self.items, self.no_etag, self.expected);
        let m = self.per_driver_last.len();
        format!("n={n} etag無={z} {f}..{l} 期待={e} 対象={m}名")
    }
}

/// **入力の欠けを etags の一覧から見つける** (Refs #205 の 21)。
///
/// etags は月ゲートのために毎回引いているので、**追加の往復ゼロ・alc の改修ゼロ**で日別のカバー状況が作れる。見るのは:
///
/// 1. `etag` が `None` = alc の DB に運行はあるのに R2 に `KUDGIVT.csv` が無い。閾値の要らない確実な欠け
/// 2. 窓の末尾の空き (#205-19 の「末尾の `dtako_events` が欠けると打刻の無い勤務が黙って消える」形)
///
/// 文面には**実測値 ([`InputCoverage::summary`]) を必ず入れる**。誤検知は**安全側** (封をしない = 全量読みに戻る)。
///
/// **`blocks_seal` は生成した瞬間に決まる。** tail gap だけは封を止めない (Refs #205-51) — 運行開始日を稼働の
/// 代理指標にしている限り、月の途中で稼働が止まった乗務員と本物の入力欠けを区別できず、恒常的に鳴って #199 の
/// 月ゲートが一生封じられないため。**文面も `dtako 入力欠け` を名乗らない** (見ているのは窓の末尾に運行が無いことだけ)。
pub fn missing_input_warnings(cov: &InputCoverage) -> Vec<Warning> {
    let mut out = Vec::new();
    let s = cov.summary();
    if cov.no_etag > 0 {
        let n = cov.no_etag;
        let w = format!("dtako 入力欠け: R2 に CSV の無い運行 {n} 件 ({s})");
        out.push(Warning::blocking(w));
    }
    if cov.last.is_none() {
        let w = format!("dtako 入力欠け: 運行開始日が 1 件も読めない ({s})");
        out.push(Warning::blocking(w));
    } else if let Some((n, g)) = cov.tail_gap() {
        let w = format!("dtako 末尾ギャップ: 乗務員{n}名の末尾が{g}日超 ({s})");
        out.push(Warning::diagnostic(w));
    }
    out
}

/// `(unko_no, etag, driver_cds)` の一覧から月ゲートの dtako 側 digest を作る。
///
/// `etag` が `None` (R2 に無い / upload 未完了) の運行は空文字として畳む — 存在しない扱いにすると upload 中の運行が
/// こっそり digest から抜け落ち、揃った後もゲートが「変わっていない」と誤判定しうる。空文字で織り込めば、揃った瞬間に
/// digest が変わってゲートが必ず外れる (安全側)。`driver_cds` は digest に**含めない** — alc が新たに返し始めた
/// 時点で全月の gate がいっせいに外れるのを避けるため。
pub fn digest_from_pairs(pairs: &[EtagPair]) -> String {
    let mut lines: Vec<String> = pairs
        .iter()
        .map(|(unko_no, etag, _driver_cds)| format!("{unko_no}:{}", etag.as_deref().unwrap_or("")))
        .collect();
    lines.sort();
    let mut h = Sha256::new();
    h.update(lines.join("\n").as_bytes());
    format!("{:x}", h.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, OUT_DATETIME_FORMAT).unwrap()
    }

    fn headers(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn row(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn resolve_prefers_taisho_driver_cd() {
        // 乗務員CD1 は運行の主運転者で全行同じ値。優先すると 2 名乗務で取り違える
        let h = headers(&[
            "運行NO",
            "乗務員CD1",
            "対象乗務員CD",
            "開始日時",
            "イベント名",
        ]);
        let cols = EventCols::resolve(&h).unwrap();
        assert_eq!(cols.driver_cd, Some(2), "対象乗務員CD (index 2) を採る");
    }

    #[test]
    fn resolve_falls_back_to_driver_cd1() {
        let h = headers(&["運行NO", "乗務員CD1", "開始日時", "イベント名"]);
        let cols = EventCols::resolve(&h).unwrap();
        assert_eq!(cols.driver_cd, Some(1));
        assert_eq!(cols.end_at, None);
        assert_eq!(cols.vehicle, None);
    }

    #[test]
    fn resolve_allows_missing_driver_columns() {
        let h = headers(&["開始日時", "イベント名"]);
        let cols = EventCols::resolve(&h).unwrap();
        assert_eq!(cols.driver_cd, None);
        assert_eq!(cols.unko_no, None);
    }

    #[test]
    fn resolve_requires_start_and_event_name() {
        assert!(EventCols::resolve(&headers(&["運行NO", "イベント名"])).is_none());
        assert!(EventCols::resolve(&headers(&["運行NO", "開始日時"])).is_none());
    }

    #[test]
    fn resolve_trims_header_whitespace() {
        let h = headers(&[" 開始日時 ", " イベント名"]);
        assert!(EventCols::resolve(&h).is_some());
    }

    #[test]
    fn row_to_event_maps_every_column() {
        let h = headers(&[
            "運行NO",
            "車輌名",
            "対象乗務員CD",
            "開始日時",
            "終了日時",
            "イベント名",
        ]);
        let cols = EventCols::resolve(&h).unwrap();
        let ev = row_to_event(
            &cols,
            &row(&[
                "2602241025060000000272",
                "帯広100け272",
                "1130",
                "2026/02/24 14:40:56",
                "2026/02/25 09:23:56",
                "休息",
            ]),
            "ignored",
        )
        .unwrap();
        assert_eq!(ev.start, dt("2026-02-24 14:40:56"));
        assert_eq!(ev.end, Some(dt("2026-02-25 09:23:56")));
        assert_eq!(ev.driver_id, Some(1130));
        assert_eq!(ev.state.as_deref(), Some("休息"));
        assert_eq!(ev.unko_no.as_deref(), Some("2602241025060000000272"));
        assert_eq!(ev.vehicle.as_deref(), Some("帯広100け272"));
    }

    #[test]
    fn row_to_event_accepts_space_padded_hour() {
        let cols = EventCols::resolve(&headers(&["開始日時", "イベント名"])).unwrap();
        let ev = row_to_event(&cols, &row(&["2026/03/01  9:05:00", "運転"]), "").unwrap();
        assert_eq!(ev.start, dt("2026-03-01 09:05:00"));
    }

    #[test]
    fn row_to_event_drops_rows_without_usable_start() {
        let cols = EventCols::resolve(&headers(&["開始日時", "イベント名"])).unwrap();
        assert!(row_to_event(&cols, &row(&["", "運転"]), "").is_none());
        assert!(row_to_event(&cols, &row(&["INVALID", "運転"]), "").is_none());
        // 列が足りない短い行も落とす
        assert!(row_to_event(&cols, &row(&[]), "").is_none());
    }

    #[test]
    fn row_to_event_keeps_nulls_and_borrows_unko_no() {
        let h = headers(&[
            "対象乗務員CD",
            "開始日時",
            "終了日時",
            "イベント名",
            "車輌名",
        ]);
        let cols = EventCols::resolve(&h).unwrap();
        // 空欄・壊れた値は 0 や空文字に化かさず null にする
        let ev = row_to_event(
            &cols,
            &row(&["", "2026/03/01 08:00:00", "bad", "", ""]),
            "OP-1",
        )
        .unwrap();
        assert_eq!(ev.driver_id, None);
        assert_eq!(ev.end, None);
        assert_eq!(ev.state, None);
        assert_eq!(ev.vehicle, None);
        // 運行NO 列が無い CSV は運行のメタデータから借りる
        assert_eq!(ev.unko_no.as_deref(), Some("OP-1"));
    }

    #[test]
    fn row_to_event_leaves_unko_no_null_when_unknown() {
        let cols = EventCols::resolve(&headers(&["開始日時", "イベント名"])).unwrap();
        let ev = row_to_event(&cols, &row(&["2026/03/01 08:00:00", "運転"]), "").unwrap();
        assert_eq!(ev.unko_no, None);
    }

    #[test]
    fn row_to_event_ignores_non_numeric_driver_cd() {
        let h = headers(&["乗務員CD1", "開始日時", "イベント名"]);
        let cols = EventCols::resolve(&h).unwrap();
        let ev = row_to_event(&cols, &row(&["DR01", "2026/03/01 08:00:00", "運転"]), "").unwrap();
        assert_eq!(ev.driver_id, None, "数値でない CD は null (0 に化かさない)");
    }

    fn op_ev(driver: i64, unko: &str, start: &str, end: &str, state: &str) -> RawEvent {
        RawEvent {
            start: dt(start),
            end: Some(dt(end)),
            driver_id: Some(driver),
            state: Some(state.into()),
            unko_no: Some(unko.into()),
            vehicle: Some(format!("車{}", &unko[unko.len() - 3..])),
        }
    }

    #[test]
    fn overlapping_operations_of_one_driver_are_a_duplicate() {
        // 乗務員 15 / 2026-06-03 の形: 245 (03:07〜6/6) と 226 (03:26〜16:55) が重なる
        let events = vec![
            op_ev(
                15,
                "OP245",
                "2026-06-03 03:07:33",
                "2026-06-03 03:07:33",
                "運行開始",
            ),
            op_ev(
                15,
                "OP245",
                "2026-06-03 03:07:33",
                "2026-06-06 12:02:12",
                "運転",
            ),
            op_ev(
                15,
                "OP226",
                "2026-06-03 03:26:45",
                "2026-06-03 16:55:46",
                "運転",
            ),
            // 重ならない運行・別の乗務員・運行の時間帯に入れない行
            op_ev(
                15,
                "OP300",
                "2026-06-07 05:00:00",
                "2026-06-07 09:00:00",
                "運転",
            ),
            op_ev(
                16,
                "OP226",
                "2026-06-03 03:26:45",
                "2026-06-03 16:55:46",
                "運転",
            ),
            op_ev(
                15,
                "OP301",
                "2026-06-06 11:00:00",
                "2026-06-07 08:00:00",
                "休息",
            ),
        ];
        let got = overlapping_operations(&events);
        assert_eq!(got.len(), 1);
        assert!(!got[0].blocks_seal);
        assert_eq!(
            got[0].text,
            "重複: 乗務員 15 の 2 台の運行が 2026-06-03 03:26:45〜2026-06-03 16:55:46 (809 分) 重なっています — OP245 (車245、2026-06-03 03:07:33〜2026-06-06 12:02:12) と OP226 (車226、2026-06-03 03:26:45〜2026-06-03 16:55:46)。重なった時間の勤務と実働は theearth と合わないことがあります"
        );
    }

    #[test]
    fn back_to_back_operations_are_not_an_error() {
        // 運行終了と次の運行開始が同時刻なら重なりではない。乗務員・運行NO・イベント名の無い行は見ない
        let mut events = vec![
            op_ev(
                15,
                "OPA",
                "2026-06-03 03:00:00",
                "2026-06-03 10:00:00",
                "運転",
            ),
            op_ev(
                15,
                "OPB",
                "2026-06-03 10:00:00",
                "2026-06-03 12:00:00",
                "運転",
            ),
        ];
        let mut stray = op_ev(
            15,
            "OPC",
            "2026-06-03 04:00:00",
            "2026-06-03 05:00:00",
            "運転",
        );
        stray.driver_id = None;
        let mut no_unko = op_ev(
            15,
            "OPC",
            "2026-06-03 04:00:00",
            "2026-06-03 05:00:00",
            "運転",
        );
        no_unko.unko_no = None;
        let mut no_state = op_ev(
            15,
            "OPC",
            "2026-06-03 04:00:00",
            "2026-06-03 05:00:00",
            "運転",
        );
        no_state.state = None;
        events.extend([stray, no_unko, no_state]);
        assert!(overlapping_operations(&events).is_empty());
        // 車輌が無くても文面は出る
        let mut a = op_ev(
            1,
            "OPA",
            "2026-06-03 03:00:00",
            "2026-06-03 10:00:00",
            "運転",
        );
        a.vehicle = None;
        let b = op_ev(
            1,
            "OPB",
            "2026-06-03 09:00:00",
            "2026-06-03 12:00:00",
            "運転",
        );
        let got = overlapping_operations(&[a, b]);
        assert!(got[0].text.contains("OPA (車輌不明、"), "{}", got[0].text);
        // 後の運行が先に終わらなければ、重なりは後の運行の始まり〜先の運行の終わり
        let want = "2026-06-03 09:00:00〜2026-06-03 10:00:00 (60 分)";
        let text = &got[0].text;
        assert!(text.contains(want), "{text}");
    }

    fn ev(start: &str, end: Option<&str>) -> RawEvent {
        RawEvent {
            start: dt(start),
            end: end.map(dt),
            driver_id: Some(1),
            state: Some("休息".to_string()),
            unko_no: None,
            vehicle: None,
        }
    }

    #[test]
    fn in_window_matches_the_two_sql_branches() {
        let (f, t) = (dt("2026-07-01 00:00:00"), dt("2026-08-02 00:00:00"));
        // 期間内に始まる
        assert!(in_window(&ev("2026-07-01 00:00:00", None), f, t));
        assert!(in_window(&ev("2026-08-01 23:59:59", None), f, t));
        // 期間内に終わる (開始は期間より前) — 月初の勤務を組むのに要る
        assert!(in_window(
            &ev("2026-06-30 22:00:00", Some("2026-07-01 06:00:00")),
            f,
            t
        ));
        // 外
        assert!(!in_window(&ev("2026-08-02 00:00:00", None), f, t));
        assert!(!in_window(&ev("2026-06-30 22:00:00", None), f, t));
        assert!(!in_window(
            &ev("2026-06-01 00:00:00", Some("2026-06-02 00:00:00")),
            f,
            t
        ));
        // 終了が期間の外
        assert!(!in_window(
            &ev("2026-06-30 22:00:00", Some("2026-08-02 00:00:00")),
            f,
            t
        ));
    }

    #[test]
    fn json_shapes_match_the_mariadb_rows() {
        let e = RawEvent {
            start: dt("2026-06-02 06:00:00"),
            end: Some(dt("2026-06-02 06:20:00")),
            driver_id: Some(1119),
            state: Some("休憩".to_string()),
            unko_no: Some("OP-1".to_string()),
            vehicle: Some("帯広100け272".to_string()),
        };
        let single = event_to_json(e.clone());
        assert_eq!(single["datetime"], "2026-06-02 06:00:00");
        assert_eq!(single["end_datetime"], "2026-06-02 06:20:00");
        assert_eq!(single["driver_id"], 1119);
        assert_eq!(single["source"], "dtako_events");
        assert_eq!(single["state"], "休憩");
        assert_eq!(single["unko_no"], "OP-1");
        assert_eq!(single["vehicle"], "帯広100け272");

        // 全乗務員版は読んでいない列をキーごと出さない (運行NO は運行の区切りに使うので出す)
        let all = event_to_all_json(e);
        assert_eq!(all["unko_no"], "OP-1");
        assert!(all.get("vehicle").is_none());
        assert_eq!(all["state"], "休憩");
    }

    #[test]
    fn json_keeps_nulls() {
        let v = event_to_json(RawEvent {
            start: dt("2026-06-02 06:00:00"),
            end: None,
            driver_id: None,
            state: None,
            unko_no: None,
            vehicle: None,
        });
        assert!(v["end_datetime"].is_null());
        assert!(v["driver_id"].is_null());
        assert!(v["state"].is_null());
        assert!(v["unko_no"].is_null());
        assert!(v["vehicle"].is_null());
    }

    #[test]
    fn sorting_matches_the_sql_order_by() {
        let mut rows = vec![
            serde_json::json!({"datetime": "2026-06-02 08:00:00", "source": "timecard", "driver_id": 2}),
            serde_json::json!({"datetime": "2026-06-02 07:00:00", "source": "dtako_events", "driver_id": 2}),
            serde_json::json!({"datetime": "2026-06-02 08:00:00", "source": "dtako_events", "driver_id": 1}),
        ];
        sort_rows(&mut rows);
        assert_eq!(rows[0]["datetime"], "2026-06-02 07:00:00");
        assert_eq!(rows[1]["source"], "dtako_events", "同時刻は source 順");
        assert_eq!(rows[2]["source"], "timecard");

        sort_rows_by_driver(&mut rows);
        assert_eq!(rows[0]["driver_id"], 1, "全乗務員版は driver_id が先");
        assert_eq!(rows[1]["datetime"], "2026-06-02 07:00:00");
        assert_eq!(rows[2]["datetime"], "2026-06-02 08:00:00");
    }

    #[test]
    fn sorting_puts_null_driver_first() {
        let mut rows = vec![
            serde_json::json!({"datetime": "2026-06-02 08:00:00", "source": "dtako_events", "driver_id": 1}),
            serde_json::json!({"datetime": "2026-06-02 07:00:00", "source": "dtako_events"}),
        ];
        sort_rows_by_driver(&mut rows);
        assert!(rows[0]["driver_id"].is_null());
    }

    #[test]
    fn borrowed_source_excludes_dtako_events() {
        assert!(is_borrowed_source(
            &serde_json::json!({"source": "timecard"})
        ));
        assert!(is_borrowed_source(&serde_json::json!({"source": "dtako"})));
        assert!(is_borrowed_source(&serde_json::json!({})));
        assert!(!is_borrowed_source(
            &serde_json::json!({"source": "dtako_events"})
        ));
    }

    #[test]
    fn date_chunks_splits_at_the_upstream_limit() {
        // 全乗務員版の上限 31 日。month_range は 32〜34 日になるので必ず割れる
        let chunks = date_chunks(d(2026, 6, 29), d(2026, 8, 1), MAX_RANGE_DAYS_ALL);
        assert_eq!(
            chunks,
            vec![
                (d(2026, 6, 29), d(2026, 7, 29)),
                (d(2026, 7, 30), d(2026, 8, 1)),
            ]
        );
        // ちょうど上限なら 1 本
        assert_eq!(date_chunks(d(2026, 7, 1), d(2026, 7, 31), 31).len(), 1);
        // 単一乗務員の上限では 1 か月は割れない
        assert_eq!(
            date_chunks(d(2026, 6, 29), d(2026, 8, 1), MAX_RANGE_DAYS_SINGLE).len(),
            1
        );
        // 逆転は空
        assert!(date_chunks(d(2026, 7, 2), d(2026, 7, 1), 31).is_empty());
    }

    #[test]
    fn month_etags_bounds_covers_month_range_exactly() {
        // month_range("2026-07") は [07-01 00:00, 08-02 00:00) = 7 月 + 翌月 1 日ぶん。
        // 閉区間の date_to は排他境界の 1 日前 = 08-01
        let (from, to) = month_etags_bounds("2026-07").unwrap();
        assert_eq!(from, d(2026, 7, 1));
        assert_eq!(to, d(2026, 8, 1));
    }

    #[test]
    fn month_etags_bounds_rolls_over_the_year() {
        let (from, to) = month_etags_bounds("2026-12").unwrap();
        assert_eq!(from, d(2026, 12, 1));
        assert_eq!(to, d(2027, 1, 1));
    }

    #[test]
    fn month_etags_bounds_rejects_garbage() {
        assert!(month_etags_bounds("").is_none());
        assert!(month_etags_bounds("2026-13").is_none());
        assert!(month_etags_bounds("nope").is_none());
    }

    #[test]
    fn digest_from_pairs_is_order_independent() {
        // 入力の並びが違っても sort してから畳むので同じ digest になる
        let a = digest_from_pairs(&[
            ("U1".to_string(), Some("etag1".to_string()), vec![]),
            ("U2".to_string(), Some("etag2".to_string()), vec![]),
        ]);
        let b = digest_from_pairs(&[
            ("U2".to_string(), Some("etag2".to_string()), vec![]),
            ("U1".to_string(), Some("etag1".to_string()), vec![]),
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn digest_from_pairs_changes_when_an_etag_changes() {
        let before = digest_from_pairs(&[("U1".to_string(), Some("etag1".to_string()), vec![])]);
        let after = digest_from_pairs(&[("U1".to_string(), Some("etag1-new".to_string()), vec![])]);
        assert_ne!(before, after);
    }

    #[test]
    fn digest_from_pairs_treats_missing_etag_as_distinct_from_present() {
        // None (R2 に未着) と Some("") はどちらも空文字として畳まれるが、
        // None と Some(other) は必ず違う digest になる (揃った瞬間に gate が外れる)
        let missing = digest_from_pairs(&[("U1".to_string(), None, vec![])]);
        let present = digest_from_pairs(&[("U1".to_string(), Some("etag1".to_string()), vec![])]);
        assert_ne!(missing, present);
        let empty_string = digest_from_pairs(&[("U1".to_string(), Some(String::new()), vec![])]);
        assert_eq!(missing, empty_string, "None は空文字と同じ畳み方");
    }

    /// digest は `driver_cds` を含めない — alc が新たに返し始めても、既存の gate が
    /// いっせいに外れて無駄な全量読み直しが起きないようにするため (Refs #205 の 32)。
    #[test]
    fn digest_from_pairs_ignores_driver_cds() {
        let without = digest_from_pairs(&[("U1".to_string(), Some("etag1".to_string()), vec![])]);
        let with = digest_from_pairs(&[(
            "U1".to_string(),
            Some("etag1".to_string()),
            vec!["D1".to_string()],
        )]);
        assert_eq!(without, with, "driver_cds の有無で digest が変わらない");
    }

    #[test]
    fn digest_from_pairs_of_empty_input_is_deterministic() {
        assert_eq!(digest_from_pairs(&[]), digest_from_pairs(&[]));
    }

    #[test]
    fn query_dates_widens_backwards_and_excludes_the_upper_bound() {
        let (f, t) = query_dates(dt("2026-07-01 00:00:00"), dt("2026-08-02 00:00:00"));
        // 期間内に終わる区間 (開始は期間より前) を拾うため 2 日遡る
        assert_eq!(f, d(2026, 6, 29));
        // 上端は排他なので 08-02 00:00:00 は 08-01 まで
        assert_eq!(t, d(2026, 8, 1));
    }

    #[test]
    fn parse_window_rejects_garbage() {
        assert!(parse_window("2026-07-01 00:00:00", "2026-08-02 00:00:00").is_ok());
        let err = parse_window("nope", "2026-08-02 00:00:00").unwrap_err();
        assert!(err.to_string().contains("bad range"), "{err}");
        assert!(parse_window("2026-07-01 00:00:00", "nope").is_err());
    }

    #[test]
    fn find_col_trims() {
        assert_eq!(find_col(&headers(&["a", " b "]), "b"), Some(1));
        assert_eq!(find_col(&headers(&["a"]), "z"), None);
    }

    /// **上流 warnings が呼び出し側まで届く** (値で返す、Refs #322)。どれも封を止める。
    #[test]
    fn upstream_warnings_come_back_to_the_caller() {
        let out = upstream_warnings(&[
            "NoSuchKey: 1234/KUDGIVT.csv".to_string(),
            "NoSuchKey: 5678/KUDGIVT.csv".to_string(),
        ]);
        assert_eq!(out.len(), 2);
        assert!(out[0].text.contains("NoSuchKey"), "{out:?}");
        assert!(
            out.iter().all(|w| w.blocks_seal),
            "上流の warning は封を止める"
        );
    }

    /// **ここでは束ねない** — 重複排除と上限は呼び出し側の束が束ぜんたいに掛ける。
    #[test]
    fn upstream_warnings_are_returned_raw() {
        assert_eq!(upstream_warnings(&vec!["same".to_string(); 3]).len(), 3);
    }

    // ── 入力欠けの検知 (Refs #205 の 21) ──────────────────────────────────

    /// `driver_cds` 無し (alc がまだ返さない現行環境相当)。
    fn pair(unko_no: &str, etag: Option<&str>) -> Pair {
        (unko_no.to_string(), etag.map(str::to_string), Vec::new())
    }

    /// `driver_cds` 1 名 (Refs #205 の 32 が alc に足す前方互換フィールド)。
    fn driver_pair(unko_no: &str, etag: Option<&str>, driver_cd: &str) -> Pair {
        crew_pair(unko_no, etag, &[driver_cd])
    }

    /// `driver_cds` 複数名 — 1 運行に主 / 副運転が付くケース (Refs #205 の 32)。
    fn crew_pair(unko_no: &str, etag: Option<&str>, driver_cds: &[&str]) -> Pair {
        (
            unko_no.to_string(),
            etag.map(str::to_string),
            driver_cds.iter().map(|s| s.to_string()).collect(),
        )
    }

    type Pair = (String, Option<String>, Vec<String>);

    /// 母集団の絞り込み (Refs #205 の 37) を効かせない旧来の呼び方。窓の下端を
    /// 十分古い日に置くので、末尾検知そのものの規則だけを見る形になる。
    fn warns(pairs: &[Pair], end: NaiveDate, today: NaiveDate) -> Vec<String> {
        warns_in(pairs, d(2000, 1, 1), end, today)
    }

    /// 窓 `[start, end]` を明示する版 (母集団の確認用)。**文面だけ** — 分類
    /// (`blocks_seal`) を見るテストは [`missing_input_warnings`] を直接呼ぶ。
    fn warns_in(pairs: &[Pair], start: NaiveDate, end: NaiveDate, today: NaiveDate) -> Vec<String> {
        missing_input_warnings(&InputCoverage::measure(pairs, start, end, today))
            .into_iter()
            .map(|w| w.text)
            .collect()
    }

    /// **揃っている月は静か。** 窓の端まで運行開始が届いていれば warning ゼロ。
    #[test]
    fn missing_input_warnings_is_silent_when_the_window_is_covered() {
        let pairs = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26070110000000000023021", Some("e2")),
        ];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert!(w.is_empty(), "窓の端 (07-01) まで在るので静か: {w:?}");
    }

    /// **実測の自然な空き (1 日) では立たない。** 閾値の余裕ぶんの確認。
    #[test]
    fn missing_input_warnings_tolerates_the_measured_natural_tail_gap() {
        let pairs = vec![pair("25123110000000000023021", Some("e1"))];
        let w = warns(&pairs, d(2026, 1, 1), d(2026, 1, 20));
        assert!(w.is_empty(), "年末の 1 日空きは自然: {w:?}");
    }

    /// **#205-19 が実証した形。** 末尾の運行が丸ごと欠けたら立つ。
    /// 文面には実測値 (不足日数と covered の範囲) が入る。
    #[test]
    fn missing_input_warnings_fires_when_the_tail_of_the_month_is_missing() {
        let pairs = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26062310000000000023021", Some("e2")),
        ];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1, "末尾 8 日ぶんの欠け: {w:?}");
        // driver_cd 無しなので全員 1 グループにまとまり、その 1 グループが超過する
        assert!(
            w[0].contains("乗務員1名の末尾が8日超"),
            "不足日数を書く: {w:?}"
        );
        assert!(w[0].contains("2026-06-01..2026-06-23"), "範囲を書く: {w:?}");
        assert!(
            w[0].contains("期待=2026-07-01"),
            "期待した末尾も書く: {w:?}"
        );
        // 実体は入力欠けではなく末尾ギャップ (Refs #c1121-3)。名乗りをここで縛る。
        assert!(w[0].contains("dtako 末尾ギャップ"), "prefix: {w:?}");
        assert!(!w[0].contains("入力欠け"), "入力欠けを名乗らない: {w:?}");
    }

    /// **閾値 7 日の境目を両側から縛る** (Refs #205 の 37)。
    ///
    /// 乗務員別に割った以上、**土日を挟むだけで gap は 3〜4 日**になる — 2 日の
    /// ままでは本番 2026-06 で 113 名中 72 名が鳴り続けた。7 日は「週末 + 1 日」で、
    /// **実際に欠けている 1078 / 1517 / 1688 (gap 8 前後) は拾える**位置。
    #[test]
    fn the_tail_gap_threshold_tolerates_a_weekend_but_catches_the_real_gaps() {
        // 金曜まで走って月曜の朝に測る形 = gap 4 日。業務として正常
        let weekend = vec![driver_pair("26062610000000000023021", Some("e1"), "D1")];
        let w = warns_in(&weekend, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "週末ぶんの空きでは鳴らない: {w:?}");

        // ちょうど 7 日はまだ許容 (閾値は「超えたら」)
        let exactly = vec![driver_pair("26062310000000000023021", Some("e1"), "D1")];
        let w = warns_in(&exactly, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "gap 7 日ちょうどは境界の内側: {w:?}");

        // 1078 の実際の欠け (最終運行 06-24 が丸ごと無く 06-22 で切れる) = gap 8 日
        let real = vec![driver_pair("26062210000000000023021", Some("e1"), "D1")];
        let w = warns_in(&real, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w.len(), 1, "本物の欠けは拾う: {w:?}");
        assert!(w[0].contains("乗務員1名の末尾が8日超"), "{w:?}");
    }

    /// **tail gap だけは封を止めない。他 (no_etag / 読めない) は止める**
    /// (Refs #205-51、台帳の受け入れ条件 #4)。分類は文字列一致ではなく
    /// [`Warning::blocks_seal`] (生成箇所) で見る。
    #[test]
    fn tail_gap_does_not_block_the_seal_but_no_etag_and_unreadable_dates_do() {
        // tail gap 単独: 本物の末尾欠け (gap 8 日) — 診断のみ
        let real = vec![driver_pair("26062210000000000023021", Some("e1"), "D1")];
        let cov = InputCoverage::measure(&real, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        let ws = missing_input_warnings(&cov);
        assert_eq!(ws.len(), 1, "{ws:?}");
        assert!(!ws[0].blocks_seal, "tail gap は診断のみ: {ws:?}");
        assert!(ws[0].text.contains("乗務員1名の末尾が8日超"), "{ws:?}");
        // ★ #c1121-3: この 1 本だけ「dtako 入力欠け」を名乗らない (実体は末尾ギャップ)。
        assert!(ws[0].text.contains("dtako 末尾ギャップ"), "{ws:?}");
        assert!(!ws[0].text.contains("入力欠け"), "{ws:?}");

        // no_etag: 封を止める
        let no_etag = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26070110000000000023021", None),
        ];
        let cov = InputCoverage::measure(&no_etag, d(2000, 1, 1), d(2026, 7, 1), d(2026, 7, 20));
        let ws = missing_input_warnings(&cov);
        assert_eq!(ws.len(), 1, "{ws:?}");
        assert!(ws[0].blocks_seal, "no_etag は封を止める: {ws:?}");
        assert!(ws[0].text.contains("R2 に CSV の無い運行"), "{ws:?}");

        // 運行開始日が 1 件も読めない: 封を止める
        let unreadable = vec![pair("U1", Some("e1"))];
        let cov = InputCoverage::measure(&unreadable, d(2000, 1, 1), d(2026, 7, 1), d(2026, 7, 20));
        let ws = missing_input_warnings(&cov);
        assert_eq!(ws.len(), 1, "{ws:?}");
        assert!(ws[0].blocks_seal, "読めない = 封を止める: {ws:?}");
        assert!(ws[0].text.contains("1 件も読めない"), "{ws:?}");
    }

    /// **進行中の月は末尾に運行が無くて当然。** 期待値を `today - 1` に切り下げる。
    #[test]
    fn missing_input_warnings_clamps_the_expectation_to_yesterday() {
        let pairs = vec![pair("26070310000000000023021", Some("e1"))];
        // 7 月を 07-04 に畳む — 窓の端 (08-01) はまだ来ていない
        let w = warns(&pairs, d(2026, 8, 1), d(2026, 7, 4));
        assert!(w.is_empty(), "当月の末尾の空きは欠けではない: {w:?}");
    }

    /// **#205-32 の本題。** 他の乗務員が窓の端まで在ると、月・全乗務員通しの
    /// `last` はそこに張り付き、1 名だけの末尾欠けが埋もれて沈黙する。
    /// `driver_cd` 無し (現行相当) では鳴らず、`driver_cd` 有り (乗務員別) では鳴る。
    #[test]
    fn missing_input_warnings_catches_a_single_drivers_tail_gap_when_others_cover_the_window() {
        // D1 / D2 は窓の端 (06-30) まで在る。D3 は 06-20 で切れている (10 日不足)。
        let no_driver = vec![
            pair("26063010000000000023021", Some("e1")),
            pair("26063010000000000023022", Some("e2")),
            pair("26062010000000000023023", Some("e3")),
        ];
        let w_old = warns(&no_driver, d(2026, 6, 30), d(2026, 7, 5));
        assert!(
            w_old.is_empty(),
            "driver_cd 無し (現行相当) は沈黙: {w_old:?}"
        );

        let with_driver = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26063010000000000023022", Some("e2"), "D2"),
            driver_pair("26062010000000000023023", Some("e3"), "D3"),
        ];
        let w_new = warns(&with_driver, d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w_new.len(), 1, "{w_new:?}");
        assert!(
            w_new[0].contains("乗務員1名の末尾が10日超"),
            "D3 だけ検出する: {w_new:?}"
        );
    }

    /// 全乗務員が窓の端まで揃っていれば (driver_cd 有りでも) 鳴らない。
    #[test]
    fn missing_input_warnings_is_silent_when_every_driver_covers_the_window() {
        let pairs = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26063010000000000023022", Some("e2"), "D2"),
            // 1 日の空きは実測の自然な揺らぎとして許容範囲内 (MAX_TAIL_GAP_DAYS = 7)
            driver_pair("26062910000000000023023", Some("e3"), "D3"),
        ];
        let w = warns(&pairs, d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "全員揃っているので静か: {w:?}");
    }

    /// 進行中の月の `today - 1 日` への切り下げは、乗務員別に割っても効く。
    #[test]
    fn missing_input_warnings_clamps_the_expectation_to_yesterday_per_driver() {
        // 7 月を対象、today=07-04 → 期待は 07-03 に切り下がる。D1/D2 とも
        // そこまで在るので静か (1 名だけの窓だと偶然揃うのと区別が付かないため 2 名で確認)。
        let pairs = vec![
            driver_pair("26070310000000000023021", Some("e1"), "D1"),
            driver_pair("26070310000000000023022", Some("e2"), "D2"),
        ];
        let w = warns(&pairs, d(2026, 8, 1), d(2026, 7, 4));
        assert!(w.is_empty(), "乗務員別でも当月クランプが効く: {w:?}");
    }

    /// **1 運行に複数の乗務員 (主 / 副運転) が付く場合、その開始日は両方の `last`
    /// に寄与する。** `list_operations_for_drivers` の `DISTINCT ON (driver_id,
    /// unko_no)` により同じ `unko_no` が複数 `driver_cd` に紐づき得るため。
    #[test]
    fn missing_input_warnings_lets_one_operation_cover_multiple_crew_drivers() {
        // D1/D2 は同じ運行 (主/副運転) で窓の端まで在る。D3 は単独で 06-20 止まり。
        let pairs = vec![
            crew_pair("26063010000000000023021", Some("e1"), &["D1", "D2"]),
            driver_pair("26062010000000000023023", Some("e3"), "D3"),
        ];
        let w = warns(&pairs, d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(
            w[0].contains("乗務員1名の末尾が10日超"),
            "D1/D2 は揃っているので D3 だけ: {w:?}"
        );
    }

    /// `InputCoverage::driver_cd_source` は `driver_cds` を持つ item が 1 件でも
    /// あれば `true`。alc がまだ返さない環境と、乗務員別検知が効いている環境の
    /// 切り分けに使う (Refs #205 の 32)。
    #[test]
    fn input_coverage_reports_whether_driver_cds_were_present() {
        let none = InputCoverage::measure(
            &[pair("26063010000000000023021", Some("e1"))],
            d(2026, 6, 1),
            d(2026, 6, 30),
            d(2026, 7, 5),
        );
        assert!(!none.driver_cd_source, "driver_cds 無しはフォールバック");

        let some = InputCoverage::measure(
            &[driver_pair("26063010000000000023021", Some("e1"), "D1")],
            d(2026, 6, 1),
            d(2026, 6, 30),
            d(2026, 7, 5),
        );
        assert!(some.driver_cd_source, "driver_cds 有りは乗務員別判定");
    }

    /// **1 件も日付が読めなければ「判定できない」ではなく警告。** 安全側。
    #[test]
    fn missing_input_warnings_fires_when_no_unko_no_carries_a_date() {
        let pairs = vec![pair("U1", Some("e1")), pair("U2", Some("e2"))];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1);
        assert!(w[0].contains("1 件も読めない"), "{w:?}");
        assert!(
            w[0].contains("n=2 etag無=0 ?..?"),
            "実測値は ? で埋める: {w:?}"
        );
    }

    /// **`etag: null` は閾値の要らない確実な欠け** (R2 に CSV がまだ無い)。
    #[test]
    fn missing_input_warnings_fires_for_operations_without_an_r2_etag() {
        let pairs = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26070110000000000023021", None),
        ];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1, "末尾は埋まっているので etag の 1 本だけ: {w:?}");
        assert!(w[0].contains("R2 に CSV の無い運行 1 件"), "{w:?}");
    }

    /// 空の一覧は「末尾が欠けている」ではなく「1 件も読めない」で立つ。
    #[test]
    fn missing_input_warnings_fires_on_an_empty_list() {
        let w = warns(&[], d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1, "{w:?}");
        assert_eq!(
            InputCoverage::measure(&[], d(2026, 6, 1), d(2026, 7, 1), d(2026, 7, 20)).gap_days(),
            None,
            "gap は測れない"
        );
    }

    /// **#205 の 37 の本題 (その 2)。** 末尾検知の母集団は「その月に運行が始まった
    /// 乗務員」だけ。etags は読み取り日で引くので窓の中に 2 か月前に始まった運行が
    /// 混ざり、それしか無い乗務員を数えると全員が「末尾が欠けている」になる
    /// (本番 2026-06 の `乗務員73名の末尾が37日超`)。
    #[test]
    fn tail_gap_ignores_drivers_without_an_operation_started_in_the_month() {
        // 窓は 2026-06。D1 は月内を端まで走っている。D2 は 04-20 に始まった運行が
        // 6 月に読まれただけで、6 月には 1 本も走っていない
        let pairs = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26042010000000000023022", Some("e2"), "D2"),
        ];
        let w = warns_in(&pairs, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "稼働していない D2 は末尾欠けではない: {w:?}");

        // 母集団を絞らなければ D2 が 71 日の末尾欠けとして鳴っていた (旧挙動)
        let old = warns(&pairs, d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(old.len(), 1, "{old:?}");
        assert!(old[0].contains("乗務員1名の末尾が71日超"), "{old:?}");
    }

    /// 母集団から外れた乗務員が居ても `no_etag` の検知は従来どおり
    /// (R2 に CSV が無いのは閾値も母集団も関係ない確実な欠け)。
    #[test]
    fn tail_gap_population_does_not_touch_the_no_etag_check() {
        let pairs = vec![driver_pair("26042010000000000023022", None, "D2")];
        let w = warns_in(&pairs, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w.len(), 1, "etag 無しの 1 本だけ: {w:?}");
        assert!(w[0].contains("R2 に CSV の無い運行 1 件"), "{w:?}");
    }

    /// summary に末尾検知の母集団 (`対象`) が出る — 過剰発火したときに
    /// 「何名を測っているのか」をログから読めるようにするため (Refs #205 の 37)。
    #[test]
    fn the_summary_reports_the_tail_gap_population() {
        let pairs = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26042010000000000023022", Some("e2"), "D2"),
        ];
        let cov = InputCoverage::measure(&pairs, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        let s = cov.summary();
        assert!(s.contains("n=2"), "{s}");
        assert!(s.contains("対象=1名"), "母集団は D1 だけ: {s}");
    }

    /// `collect`: 必須列の無い運行は (落とさずに) 飛ばし、開始日時が読めない行・窓の外の行は捨て、運行NO の重複は数えない。
    /// 運行NO が空の運行は重複排除の鍵にしない。
    #[test]
    fn collect_skips_broken_operations_rows_and_duplicates() {
        let op = |unko_no: &str, h: &[&str], rows: Vec<Vec<String>>| UpstreamOperation {
            unko_no: unko_no.to_string(),
            headers: headers(h),
            rows,
        };
        let cols = ["開始日時", "イベント名"];
        let ops = vec![
            op("OP-0", &["運行NO"], vec![row(&["OP-0"])]),
            op(
                "OP-1",
                &cols,
                vec![
                    row(&["2026/07/02 08:00:00", "出庫"]),
                    row(&["broken", "出庫"]),
                    row(&["2026/09/01 08:00:00", "窓の外"]),
                ],
            ),
            op("OP-1", &cols, vec![row(&["2026/07/03 08:00:00", "重複"])]),
            op("", &cols, vec![row(&["2026/07/04 08:00:00", "a"])]),
            op("", &cols, vec![row(&["2026/07/05 08:00:00", "b"])]),
        ];
        let (mut seen, mut out) = (HashSet::new(), Vec::new());
        let window = (dt("2026-07-01 00:00:00"), dt("2026-08-02 00:00:00"));
        collect(ops, &mut seen, window, &mut out);
        let states: Vec<&str> = out.iter().map(|e| e.state.as_deref().unwrap()).collect();
        assert_eq!(states, vec!["出庫", "a", "b"]);
        assert!(
            seen.contains("OP-0"),
            "飛ばした運行も既読にする (root と同じ)"
        );
    }
}
