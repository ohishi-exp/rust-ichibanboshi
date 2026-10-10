//! 拘束サマリ (restraint) の 3 口の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! - `PUT /api/restraint/summaries` — relay (nuxt-dtako-admin の dtako-scraper-relay) がサマリの写しを push する
//! - `GET /api/restraint/wage-source` — 当月 + 前月 × theearth + timecard の素材を 1 応答で返す
//! - `GET /api/restraint/synced-months` — comp の push 済み (source, 月) の一覧
//!
//! 検査と 400 の文言・前月・表の定義・SQL の文字列・bind の値の並び・応答の組み立て・summary_json の読み取りを持つ。
//! オンプレ版 (root の `src/routes/restraint.rs`・`src/restraint_store.rs`、rusqlite) と勤怠 Worker (D1) が同じものを使う
//! (写さない)。I/O と logging は呼び手が持つ: 壊れた summary_json は [`BrokenSummary`] として返し、呼び手が warn する。
//!
//! ## SQL は SQLite と D1 で同じ文字列
//!
//! 引数は `?NNN` (どちらも受ける)。D1 の複数文の transaction は `batch` だけで、途中の結果を呼び手に戻せない。
//! だから sync_state の `row_count` は**同じ batch (transaction) の中の副問い合わせ**で数える
//! ([`UPSERT_SYNC_STATE_SQL`])。rusqlite の経路も同じ文を同じ transaction で流す。
//!
//! ## サマリ JSON は解釈しない
//!
//! entries の `summary` は relay のサマリを `serde_json::Value` のまま保存・返却する (行の形は relay 側の golden テストが固定)。

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::common::{bad_request, is_valid_month, Fail};

/// 2 表の定義。正本は `workers/kintai/worker/migrations/0001_restraint.sql` (D1 の migration と同じファイル)。
/// `PRAGMA user_version` は含めない (オンプレ版の init が持つ)。
pub const SCHEMA_SQL: &str = include_str!("../../worker/migrations/0001_restraint.sql");

/// `source` に許す値。
pub const SOURCES: [&str; 2] = ["theearth", "timecard"];

/// comp_id の書式検証 (dtako テナントの会社ID。SQL には bind でしか使わないので緩めで良いが、明らかなゴミは弾く)。
/// `:` を許さないので scope (`comp:source:ym`) の区切りと混ざらない。
pub fn is_valid_comp(comp_id: &str) -> bool {
    !comp_id.is_empty()
        && comp_id.len() <= 64
        && comp_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// 'YYYY-MM' の前月。`is_valid_month` 通過済み前提。
pub fn prev_month(ym: &str) -> String {
    let year: i32 = ym[..4].parse().expect("validated year");
    let month: u32 = ym[5..].parse().expect("validated month");
    if month == 1 {
        format!("{}-12", year - 1)
    } else {
        format!("{year}-{:02}", month - 1)
    }
}

/// sync_state の鍵 `comp:source:ym`。
pub fn scope(comp_id: &str, source: &str, ym: &str) -> String {
    format!("{comp_id}:{source}:{ym}")
}

/// PUT の応答と sync_state に入れる時刻。呼び手が「今」を渡し、書式はここで固定する
/// (RFC3339・ナノ秒 9 桁・`+00:00`。オンプレ版の `Utc::now().to_rfc3339()` の書式。Worker で JS の Date を
/// 文字列にすると `…Z` になってずれるので、ミリ秒から作った `DateTime` をここへ渡す)。
pub fn format_synced_at(now: DateTime<Utc>) -> String {
    now.to_rfc3339_opts(SecondsFormat::Nanos, false)
}

// ── 保存の単位 ──

/// 1 乗務員分の行。`summary_json` は relay のサマリ JSON verbatim (no_data の時は None)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestraintEntry {
    pub driver_cd: String,
    pub no_data: bool,
    pub summary_json: Option<String>,
    pub fetched_at: Option<String>,
    pub last_verified_at: Option<String>,
}

/// sync 済み 1 件 (メタのみ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestraintSyncedRow {
    pub source: String,
    pub month: String,
    pub synced_at: String,
    pub row_count: i64,
}

/// 読み出し結果 1 ヶ月分 (source 単位)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestraintMonth {
    /// driver_cd 昇順 ([`MONTH_ROWS_SQL`] の順)。
    pub entries: Vec<RestraintEntry>,
    /// 最後に push を受けた時刻。一度も受けていなければ None。
    pub synced_at: Option<String>,
}

// ── SQL と bind ──

/// bind する値 1 つ。rusqlite と D1 の両方に写せる 3 種だけ。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    Text(String),
    Int(i64),
    Null,
}

impl Bind {
    fn opt(v: &Option<String>) -> Self {
        v.clone().map_or(Self::Null, Self::Text)
    }
}

/// 乗務員 1 名の upsert (載っている乗務員だけを上書きする。replace-all ではない)。bind は [`summary_binds`]。
pub const UPSERT_SUMMARY_SQL: &str = "INSERT INTO restraint_summary \
(comp_id, source, ym, driver_cd, no_data, summary_json, fetched_at, last_verified_at) \
VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) \
ON CONFLICT (comp_id, source, ym, driver_cd) DO UPDATE SET \
no_data = excluded.no_data, summary_json = excluded.summary_json, \
fetched_at = excluded.fetched_at, last_verified_at = excluded.last_verified_at";

/// (comp, source, ym) の sync_state の upsert。`row_count` は同じ transaction の中で upsert した後の行数を
/// 副問い合わせで数える (D1 の batch は途中の結果を戻せないため)。bind は [`sync_state_binds`]。
pub const UPSERT_SYNC_STATE_SQL: &str = "INSERT INTO restraint_sync_state (scope, synced_at, row_count) \
VALUES (?1, ?2, (SELECT COUNT(*) FROM restraint_summary WHERE comp_id = ?3 AND source = ?4 AND ym = ?5)) \
ON CONFLICT (scope) DO UPDATE SET synced_at = excluded.synced_at, row_count = excluded.row_count";

/// (comp, source, ym) の最後の push の時刻。bind は [`synced_at_binds`]。列は `synced_at`。
pub const SYNCED_AT_SQL: &str = "SELECT synced_at FROM restraint_sync_state WHERE scope = ?1";

/// (comp, source, ym) の全乗務員分 (driver_cd 昇順)。bind は [`month_rows_binds`]。
/// 列は `driver_cd, no_data, summary_json, fetched_at, last_verified_at` (この順)。
pub const MONTH_ROWS_SQL: &str =
    "SELECT driver_cd, no_data, summary_json, fetched_at, last_verified_at \
FROM restraint_summary WHERE comp_id = ?1 AND source = ?2 AND ym = ?3 ORDER BY driver_cd ASC";

/// comp の sync 済み一覧 (scope 昇順)。bind は [`synced_binds`]。列は `scope, synced_at, row_count` (この順)。
/// `LIKE` の `_` は 1 文字の wildcard なので別の comp も当たりうる — [`synced_rows`] が接頭辞で落とす。
pub const SYNCED_SQL: &str = "SELECT scope, synced_at, row_count FROM restraint_sync_state \
WHERE scope LIKE ?1 || '%' ORDER BY scope ASC";

/// [`UPSERT_SUMMARY_SQL`] の `?1..?8`。
pub fn summary_binds(comp_id: &str, source: &str, ym: &str, e: &RestraintEntry) -> [Bind; 8] {
    [
        Bind::Text(comp_id.to_string()),
        Bind::Text(source.to_string()),
        Bind::Text(ym.to_string()),
        Bind::Text(e.driver_cd.clone()),
        Bind::Int(i64::from(e.no_data)),
        Bind::opt(&e.summary_json),
        Bind::opt(&e.fetched_at),
        Bind::opt(&e.last_verified_at),
    ]
}

/// [`UPSERT_SYNC_STATE_SQL`] の `?1..?5`。
pub fn sync_state_binds(comp_id: &str, source: &str, ym: &str, synced_at: &str) -> [Bind; 5] {
    [
        Bind::Text(scope(comp_id, source, ym)),
        Bind::Text(synced_at.to_string()),
        Bind::Text(comp_id.to_string()),
        Bind::Text(source.to_string()),
        Bind::Text(ym.to_string()),
    ]
}

/// [`SYNCED_AT_SQL`] の `?1`。
pub fn synced_at_binds(comp_id: &str, source: &str, ym: &str) -> [Bind; 1] {
    [Bind::Text(scope(comp_id, source, ym))]
}

/// [`MONTH_ROWS_SQL`] の `?1..?3`。
pub fn month_rows_binds(comp_id: &str, source: &str, ym: &str) -> [Bind; 3] {
    [
        Bind::Text(comp_id.to_string()),
        Bind::Text(source.to_string()),
        Bind::Text(ym.to_string()),
    ]
}

/// [`SYNCED_SQL`] の `?1` (`comp:`)。
pub fn synced_binds(comp_id: &str) -> [Bind; 1] {
    [Bind::Text(format!("{comp_id}:"))]
}

/// [`SYNCED_SQL`] の行 `(scope, synced_at, row_count)` を (source, month) に分ける。接頭辞が `comp:` でない行
/// (`LIKE` の wildcard で当たった別の comp) と、`:` で 2 つに割れない行は落とす。
pub fn synced_rows(comp_id: &str, rows: Vec<(String, String, i64)>) -> Vec<RestraintSyncedRow> {
    let prefix = format!("{comp_id}:");
    rows.into_iter()
        .filter_map(|(scope, synced_at, row_count)| {
            let rest = scope.strip_prefix(&prefix)?;
            let (source, month) = rest.split_once(':')?;
            Some(RestraintSyncedRow {
                source: source.to_string(),
                month: month.to_string(),
                synced_at,
                row_count,
            })
        })
        .collect()
}

// ── 失敗の本文 ──

/// 失敗の応答の本文 `{"error": "…"}` (オンプレ版と同じ形)。
#[derive(Serialize, Debug, PartialEq, Eq)]
pub struct ErrorBody {
    pub error: String,
}

impl ErrorBody {
    pub fn of(fail: Fail) -> Self {
        Self { error: fail.body }
    }
}

// ══════════════════════════════════════════════════════════════
// PUT /api/restraint/summaries
// ══════════════════════════════════════════════════════════════

/// push される 1 乗務員分。
#[derive(Deserialize, Debug)]
pub struct PushEntry {
    pub driver_cd: String,
    /// 「該当データがありません」マーカー (Refs nuxt-dtako-admin#241)。
    #[serde(default)]
    pub no_data: bool,
    /// サマリ JSON verbatim (no_data の時は省略可)。
    #[serde(default)]
    pub summary: Option<serde_json::Value>,
    #[serde(default)]
    pub fetched_at: Option<String>,
    #[serde(default)]
    pub last_verified_at: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct PushBody {
    pub comp_id: String,
    /// 'theearth' | 'timecard'
    pub source: String,
    /// 'YYYY-MM'
    pub month: String,
    pub entries: Vec<PushEntry>,
}

#[derive(Serialize, Debug)]
pub struct PushResponse {
    pub saved: usize,
    pub synced_at: String,
}

/// 検査済みの push。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidPush {
    pub comp_id: String,
    pub source: String,
    pub month: String,
    pub entries: Vec<RestraintEntry>,
}

impl ValidPush {
    /// 書けた後の応答。`saved` は載っていた乗務員の数。
    pub fn response(&self, synced_at: String) -> PushResponse {
        PushResponse {
            saved: self.entries.len(),
            synced_at,
        }
    }
}

const NO_SUMMARY: &str = "は no_data でないのに summary がありません";

/// 本文の検査 (順: comp_id → source → month → entries を頭から)。失敗は 400。
pub fn validate_push(body: PushBody) -> Result<ValidPush, Fail> {
    if !is_valid_comp(&body.comp_id) {
        return Err(bad_request("comp_id が不正です"));
    }
    if !SOURCES.contains(&body.source.as_str()) {
        return Err(bad_request(
            "source は theearth / timecard のいずれかで指定してください",
        ));
    }
    if !is_valid_month(&body.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let mut entries = Vec::with_capacity(body.entries.len());
    for e in body.entries {
        if e.driver_cd.is_empty() {
            return Err(bad_request("driver_cd が空の entry があります"));
        }
        if !e.no_data && e.summary.is_none() {
            // format! を 1 行に収める (複数行は llvm-cov の行に乗らず gate が落ちる)
            let msg = format!("driver_cd={} {NO_SUMMARY}", e.driver_cd);
            return Err(bad_request(msg));
        }
        entries.push(RestraintEntry {
            driver_cd: e.driver_cd,
            no_data: e.no_data,
            // 検証済みの Value を verbatim 文字列化して保存 (取り出し時は素通し)
            summary_json: e.summary.as_ref().map(|v| v.to_string()),
            fetched_at: e.fetched_at,
            last_verified_at: e.last_verified_at,
        });
    }
    Ok(ValidPush {
        comp_id: body.comp_id,
        source: body.source,
        month: body.month,
        entries,
    })
}

// ══════════════════════════════════════════════════════════════
// GET /api/restraint/synced-months?comp=
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct SyncedMonthsQuery {
    pub comp: String,
}

#[derive(Serialize, Debug)]
pub struct RestraintSyncedEntry {
    /// 'theearth' | 'timecard'
    pub source: String,
    pub month: String,
    pub synced_at: String,
    pub row_count: i64,
}

#[derive(Serialize, Debug)]
pub struct RestraintSyncedResponse {
    pub entries: Vec<RestraintSyncedEntry>,
}

/// comp の検査 (400)。通れば comp を返す。
pub fn parse_synced_months(q: SyncedMonthsQuery) -> Result<String, Fail> {
    if !is_valid_comp(&q.comp) {
        return Err(bad_request("comp が不正です"));
    }
    Ok(q.comp)
}

/// synced-months の応答 (行の順のまま = scope 昇順)。
pub fn synced_response(rows: Vec<RestraintSyncedRow>) -> RestraintSyncedResponse {
    RestraintSyncedResponse {
        entries: rows
            .into_iter()
            .map(|r| RestraintSyncedEntry {
                source: r.source,
                month: r.month,
                synced_at: r.synced_at,
                row_count: r.row_count,
            })
            .collect(),
    }
}

// ══════════════════════════════════════════════════════════════
// GET /api/restraint/wage-source?comp=&month=
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct WageSourceQuery {
    pub comp: String,
    pub month: String,
}

/// 1 ヶ月 × 1 source 分の素材 (relay の loadMonthSummaries の置換素材)。
#[derive(Serialize, Debug, PartialEq)]
pub struct WageSourceMonth {
    /// [{driver_cd, summary, fetched_at, last_verified_at}] (driver_cd 昇順)。
    pub summaries: Vec<WageSourceSummary>,
    pub no_data_drivers: Vec<String>,
    /// この (comp, source, month) が最後に push を受けた時刻。未 push は null —
    /// relay はその時だけ R2 フォールバックする。
    pub synced_at: Option<String>,
}

#[derive(Serialize, Debug, PartialEq)]
pub struct WageSourceSummary {
    pub driver_cd: String,
    /// サマリ JSON verbatim (relay 側の型で解釈する)。
    pub summary: serde_json::Value,
    pub fetched_at: Option<String>,
    pub last_verified_at: Option<String>,
}

#[derive(Serialize, Debug)]
pub struct WageSourceResponse {
    pub comp_id: String,
    pub month: String,
    pub prev_month: String,
    pub current_theearth: WageSourceMonth,
    pub current_timecard: WageSourceMonth,
    pub prev_theearth: WageSourceMonth,
    pub prev_timecard: WageSourceMonth,
}

/// 検査済みの wage-source。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WageSourceRequest {
    pub comp_id: String,
    pub month: String,
    pub prev_month: String,
}

impl WageSourceRequest {
    /// 読む (source, 月) の 4 組。順は応答の current_theearth・current_timecard・prev_theearth・prev_timecard。
    pub fn reads(&self) -> [(&'static str, String); 4] {
        [
            ("theearth", self.month.clone()),
            ("timecard", self.month.clone()),
            ("theearth", self.prev_month.clone()),
            ("timecard", self.prev_month.clone()),
        ]
    }

    /// [`Self::reads`] の順に読んだ 4 つから応答を組む。
    pub fn respond(self, months: [WageSourceMonth; 4]) -> WageSourceResponse {
        let [current_theearth, current_timecard, prev_theearth, prev_timecard] = months;
        WageSourceResponse {
            comp_id: self.comp_id,
            month: self.month,
            prev_month: self.prev_month,
            current_theearth,
            current_timecard,
            prev_theearth,
            prev_timecard,
        }
    }
}

/// 検査 (順: comp → month)。失敗は 400。
pub fn parse_wage_source(q: WageSourceQuery) -> Result<WageSourceRequest, Fail> {
    if !is_valid_comp(&q.comp) {
        return Err(bad_request("comp が不正です"));
    }
    if !is_valid_month(&q.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let prev_month = prev_month(&q.month);
    Ok(WageSourceRequest {
        comp_id: q.comp,
        month: q.month,
        prev_month,
    })
}

/// summary_json が JSON として読めず落とした行 (呼び手が warn する)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokenSummary {
    pub driver_cd: String,
    pub error: String,
}

/// 1 ヶ月分の行を素材にする。no_data は `no_data_drivers` へ、summary_json が無い行は黙って落とし、
/// 読めない行は [`BrokenSummary`] にして落とす (push 側で検証済みなので実際には起きない — 起きても 1 名の破損で
/// 月全体を殺さない)。
pub fn month_source(month: RestraintMonth) -> (WageSourceMonth, Vec<BrokenSummary>) {
    let mut summaries = Vec::new();
    let mut no_data_drivers = Vec::new();
    let mut broken = Vec::new();
    for e in month.entries {
        if e.no_data {
            no_data_drivers.push(e.driver_cd);
            continue;
        }
        let Some(json) = e.summary_json else { continue };
        match serde_json::from_str::<serde_json::Value>(&json) {
            Ok(summary) => summaries.push(WageSourceSummary {
                driver_cd: e.driver_cd,
                summary,
                fetched_at: e.fetched_at,
                last_verified_at: e.last_verified_at,
            }),
            Err(err) => broken.push(BrokenSummary {
                driver_cd: e.driver_cd,
                error: err.to_string(),
            }),
        }
    }
    let out = WageSourceMonth {
        summaries,
        no_data_drivers,
        synced_at: month.synced_at,
    };
    (out, broken)
}
