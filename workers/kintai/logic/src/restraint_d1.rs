//! 拘束サマリ (restraint) の 3 口を勤怠 Worker の D1 で回すための純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! SQL・bind・検査・応答は [`crate::restraint`] (オンプレ版の rusqlite と同じもの)。ここは D1 だけの部分:
//!
//! - **1 回の `batch` に流す文の束** ([`Stmt`]): PUT は「載った乗務員ごとの upsert + sync_state の upsert」、
//!   wage-source は「(synced_at, 行) × 4 か月分」の 8 文。D1 の `batch` は 1 transaction で、途中の結果を戻せないので
//!   sync_state の row_count は副問い合わせで数える ([`crate::restraint::UPSERT_SYNC_STATE_SQL`])
//! - **結果の行の読み取り**: D1 は行を列名をキーにした object で返す (数は JS の number)。それを `serde_json::Value` で
//!   受けて [`RestraintEntry`]・一覧の行に直す。読めなければ 502 (`rows`)
//! - **D1 だけの失敗**: binding が無い = 503 ([`no_d1`])、D1 の失敗 = 502 で種別だけ ([`d1_fail`]、D1 の message は出さない)、
//!   1 回の PUT の entries の上限 ([`MAX_PUSH_ENTRIES`]、400)
//!
//! native の SQLite で同じ束を流した結果がオンプレ版 (rusqlite) と一致することは `workers/kintai/pg/tests/restraint_d1_parity.rs`。

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::common::{bad_request, Fail};
use crate::restraint::{
    format_synced_at, month_rows_binds, month_source, summary_binds, sync_state_binds,
    synced_at_binds, synced_binds, synced_response, synced_rows, Bind, BrokenSummary,
    RestraintEntry, RestraintMonth, RestraintSyncedResponse, ValidPush, WageSourceRequest,
    WageSourceResponse, MONTH_ROWS_SQL, SYNCED_AT_SQL, SYNCED_SQL, UPSERT_SUMMARY_SQL,
    UPSERT_SYNC_STATE_SQL,
};

/// D1 の binding (wrangler.toml のトップレベルの `[[d1_databases]]`)。
pub const D1_BINDING: &str = "KINTAI_RESTRAINT_DB";

/// 1 回の PUT で受ける entries の上限。D1 の `batch` は文ごとに 1 クエリと数え、Workers Paid の 1 invocation の上限は
/// 1000 クエリ。entries + 1 (sync_state) がその半分に収まるようにする (今の量は 1 か月あたり約 70 名)。
/// 本文の上限 (2MB、axum と同じ) の方が先に効くことが多い。
pub const MAX_PUSH_ENTRIES: usize = 500;

/// 503 (D1 の binding が無い)。オンプレ版の「sqlite_path を確認」とは原因が違うので文言を分ける。
pub fn no_d1() -> Fail {
    Fail::new(
        503,
        "拘束サマリの D1 (KINTAI_RESTRAINT_DB) の binding がありません",
    )
}

/// 502 (D1 の失敗)。`stage` は種別だけ (`batch`・`rows` 等)。D1 の message は載せない。
pub fn d1_fail(stage: &str) -> Fail {
    Fail::new(
        502,
        format!("拘束サマリの D1 の読み書きに失敗しました: {stage}"),
    )
}

/// Worker の「今」(`Date.now()` のミリ秒) を PUT の synced_at の書式へ (JS の Date の文字列 `…Z` にしない)。
/// 範囲外は epoch (実際には起きない)。
pub fn synced_at_from_millis(ms: i64) -> String {
    format_synced_at(DateTime::<Utc>::from_timestamp_millis(ms).unwrap_or_default())
}

/// entries の数の検査 (Worker だけ。オンプレ版には無い)。
pub fn check_push_size(valid: &ValidPush) -> Result<(), Fail> {
    if valid.entries.len() > MAX_PUSH_ENTRIES {
        return Err(bad_request(TOO_MANY));
    }
    Ok(())
}

const TOO_MANY: &str = "entries は 1 回の PUT で 500 件までです (分けて PUT してください)";

/// `batch` に流す 1 文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stmt {
    pub sql: &'static str,
    pub binds: Vec<Bind>,
}

impl Stmt {
    fn new(sql: &'static str, binds: impl Into<Vec<Bind>>) -> Self {
        Self {
            sql,
            binds: binds.into(),
        }
    }
}

/// PUT の束: 載った乗務員ごとの upsert → sync_state の upsert (この順。row_count は upsert の後に数える)。
pub fn push_statements(valid: &ValidPush, synced_at: &str) -> Vec<Stmt> {
    let (c, s, m) = (&valid.comp_id, &valid.source, &valid.month);
    let mut out: Vec<Stmt> = valid
        .entries
        .iter()
        .map(|e| Stmt::new(UPSERT_SUMMARY_SQL, summary_binds(c, s, m, e)))
        .collect();
    out.push(Stmt::new(
        UPSERT_SYNC_STATE_SQL,
        sync_state_binds(c, s, m, synced_at),
    ));
    out
}

/// wage-source の束: [`WageSourceRequest::reads`] の 4 組それぞれに (synced_at, 行) の 2 文 = 8 文。
pub fn wage_source_statements(req: &WageSourceRequest) -> Vec<Stmt> {
    let comp = &req.comp_id;
    req.reads()
        .iter()
        .flat_map(|(source, ym)| {
            [
                Stmt::new(SYNCED_AT_SQL, synced_at_binds(comp, source, ym)),
                Stmt::new(MONTH_ROWS_SQL, month_rows_binds(comp, source, ym)),
            ]
        })
        .collect()
}

/// synced-months の 1 文。
pub fn synced_statement(comp_id: &str) -> Stmt {
    Stmt::new(SYNCED_SQL, synced_binds(comp_id))
}

/// wage-source の束の結果 (文ごとの行の列、8 本) から応答を組む。壊れた summary_json の行は落として返す (呼び手がログに出す)。
pub fn wage_source_from_results(
    req: WageSourceRequest,
    results: Vec<Vec<Value>>,
) -> Result<(WageSourceResponse, Vec<BrokenSummary>), Fail> {
    let results: [Vec<Value>; 8] = results.try_into().map_err(|_| d1_fail("rows"))?;
    let [s0, r0, s1, r1, s2, r2, s3, r3] = results;
    let mut broken = Vec::new();
    let mut month = |synced: Vec<Value>, rows: Vec<Value>| {
        let (out, b) = month_source(month_from_rows(&synced, &rows)?);
        broken.extend(b);
        Ok::<_, Fail>(out)
    };
    let months = [
        month(s0, r0)?,
        month(s1, r1)?,
        month(s2, r2)?,
        month(s3, r3)?,
    ];
    Ok((req.respond(months), broken))
}

/// synced-months の結果の行から応答を組む。
pub fn synced_from_results(
    comp_id: &str,
    rows: Vec<Value>,
) -> Result<RestraintSyncedResponse, Fail> {
    let rows = rows
        .iter()
        .map(|r| {
            Ok((
                text(r, "scope")?,
                text(r, "synced_at")?,
                int(r, "row_count")?,
            ))
        })
        .collect::<Result<Vec<_>, Fail>>()?;
    Ok(synced_response(synced_rows(comp_id, rows)))
}

/// 1 ヶ月分 ([`SYNCED_AT_SQL`] の行 0〜1 本と [`MONTH_ROWS_SQL`] の行)。
fn month_from_rows(synced: &[Value], rows: &[Value]) -> Result<RestraintMonth, Fail> {
    let synced_at = synced.first().map(|r| text(r, "synced_at")).transpose()?;
    let entries = rows
        .iter()
        .map(|r| {
            Ok(RestraintEntry {
                driver_cd: text(r, "driver_cd")?,
                no_data: int(r, "no_data")? != 0,
                summary_json: opt_text(r, "summary_json")?,
                fetched_at: opt_text(r, "fetched_at")?,
                last_verified_at: opt_text(r, "last_verified_at")?,
            })
        })
        .collect::<Result<Vec<_>, Fail>>()?;
    Ok(RestraintMonth { entries, synced_at })
}

fn text(row: &Value, col: &str) -> Result<String, Fail> {
    opt_text(row, col)?.ok_or_else(|| d1_fail("rows"))
}

/// 列が無い・文字列でも NULL でもない = 502。
fn opt_text(row: &Value, col: &str) -> Result<Option<String>, Fail> {
    match row.get(col) {
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(Value::Null) => Ok(None),
        _ => Err(d1_fail("rows")),
    }
}

/// 整数の列。D1 は JS の number で返すので、小数部の無い浮動小数も受ける。
fn int(row: &Value, col: &str) -> Result<i64, Fail> {
    let v = row.get(col).ok_or_else(|| d1_fail("rows"))?;
    v.as_i64()
        .or_else(|| v.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64))
        .ok_or_else(|| d1_fail("rows"))
}
