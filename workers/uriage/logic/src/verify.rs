//! `/api/uriage/verify`・`verify-debug` の純粋部分 — PHP `print-json.sum` の読み方と、PHP と Rust の `$sum` の差分
//! (root の `src/routes/uriage.rs` から移した。Refs ohishi-exp/rust-ichibanboshi#322)。

use serde::Serialize;
use std::collections::HashMap;

use crate::api::{VerifyDebugResponse, VerifyDebugRow, VerifyQuery, VerifyResponse};
use crate::cakephp::OfficeMasters;
use crate::compute::{compute_person_sum, PersonAccum, UriageRow};
use crate::d1::{Bind, Stmt};
use crate::routes::Reply;
use crate::sqlserver::uriage_rows_sql;

/// PHP `$sum` と Rust 集計の差分。
///
/// `php_only`: PHP にあって rust と値が違う、または rust に無い entry
/// `rust_only`: 逆
#[derive(Debug, Serialize)]
pub struct VerifyDiff {
    pub php_only: HashMap<String, PersonAccum>,
    pub rust_only: HashMap<String, PersonAccum>,
}

/// `PrintJsonResponse.sum` (= `serde_json::Value`) → `HashMap<String, PersonAccum>` 変換。
/// 空 object / null は空 map を返す (PHP 側で対象データが無い日)。
///
/// 誤りは応答の本文になる文言だけを返す (status は呼び手が決める。オンプレ版も Worker も 502)。
pub fn parse_php_sum(v: &serde_json::Value) -> Result<HashMap<String, PersonAccum>, String> {
    if v.is_null() {
        return Ok(HashMap::new());
    }
    serde_json::from_value::<HashMap<String, PersonAccum>>(v.clone())
        .map_err(|e| format!("PHP print-json sum parse failed: {e}"))
}

/// shell の jq logic を写経した diff 計算:
/// 1. 両側で「金額/傭車金額/件数 が全て 0」の entry を除外
/// 2. キー集合と各値が一致するか比較し、不一致なら php_only / rust_only に振り分け
/// 3. 完全一致なら `None`
pub fn compute_verify_diff(
    php: &HashMap<String, PersonAccum>,
    rust: &HashMap<String, PersonAccum>,
) -> Option<VerifyDiff> {
    fn is_nonzero(a: &PersonAccum) -> bool {
        a.kingaku != 0 || a.yosha_kingaku != 0 || a.kensuu != 0
    }
    // PHP は `kingaku` / `yosha_kingaku` / `kensuu` の 3 フィールドしか返さないため、
    // diff 比較も main 3 フィールドだけで行う。Rust 側の `kingaku_y0` 等は PHP 互換性に
    // 関係しないため diff から除外する (= y0 値を 0 に潰してから比較する)。
    fn main_only(a: &PersonAccum) -> PersonAccum {
        PersonAccum {
            kingaku: a.kingaku,
            yosha_kingaku: a.yosha_kingaku,
            kensuu: a.kensuu,
            ..Default::default()
        }
    }

    let php_nz: HashMap<String, PersonAccum> = php
        .iter()
        .filter(|(_, v)| is_nonzero(v))
        .map(|(k, v)| (k.clone(), main_only(v)))
        .collect();
    let rust_nz: HashMap<String, PersonAccum> = rust
        .iter()
        .filter(|(_, v)| is_nonzero(v))
        .map(|(k, v)| (k.clone(), main_only(v)))
        .collect();

    let php_only: HashMap<String, PersonAccum> = php_nz
        .iter()
        .filter(|(k, v)| rust_nz.get(*k).copied() != Some(**v))
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let rust_only: HashMap<String, PersonAccum> = rust_nz
        .iter()
        .filter(|(k, v)| php_nz.get(*k).copied() != Some(**v))
        .map(|(k, v)| (k.clone(), *v))
        .collect();

    if php_only.is_empty() && rust_only.is_empty() {
        None
    } else {
        Some(VerifyDiff {
            php_only,
            rust_only,
        })
    }
}

// ── POST /api/uriage/verify・GET /api/uriage/verify-debug の段取り (src/routes/uriage.rs の `verify`・`verify_debug` の写し) ──
// 検査の順 (Worker): token (verify だけ) → Query・date (400) → binding (503) → masters → 営業所 (404) → bumon 空なら skipped →
// print-json → SQL Server 1 本 (単日) → 集計・差分 → verify_jobs の UPSERT (失敗は握りつぶす)。

/// bumon が空の営業所の `skipped_reason` (verify_jobs にも ok = 1 で記録する。無いと r2_pending が永久に埋まらない)。
pub const SKIPPED_NO_BUMON: &str = "no_bumon";

/// `date` が `YYYY-MM-DD` の長さ (10 バイト) の ASCII か。違えば 400 (オンプレ版と同じ平文)。
pub fn check_date(date: &str) -> Result<(), Reply> {
    if date.len() != 10 || !date.is_ascii() {
        let msg = format!("date 形式不正 (YYYY-MM-DD): {date}");
        return Err(Reply::text(400, msg));
    }
    Ok(())
}

/// masters に営業所が無い (404、オンプレ版と同じ平文)。
pub fn office_missing(id: i64) -> Reply {
    Reply::text(404, format!("office_id={id} は masters に無い"))
}

/// print-json の `sum` が読めない (502 `cakephp:sum`)。オンプレ版の本文 (serde の文言) は出さない。
pub fn sum_unreadable() -> Reply {
    Reply::upstream("cakephp", "sum")
}

/// 営業所の SQL Server に流す SQL (`@P1` = `@P2` = 検証日)。部門が英数字に絞って空なら `None` (= 0 行。接続の後に見る)。
pub fn rows_sql(office: &OfficeMasters) -> Option<String> {
    let persons: Vec<i32> = office.persons_as_int_map().into_keys().collect();
    uriage_rows_sql(&office.bumon, &persons)
}

/// verify_jobs の UPSERT (src/sqlite.rs:1118 の `upsert_verify_job` の写し。PR-F で root 側を消す)。
pub const VERIFY_JOB_UPSERT_SQL: &str = "INSERT INTO verify_jobs \
         (unko_date, eigyosho_id, cal, month, ok, skipped_reason, \
          diff_json, row_count, elapsed_ms, ran_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
     ON CONFLICT(unko_date, eigyosho_id, cal) DO UPDATE SET \
         month          = excluded.month, \
         ok             = excluded.ok, \
         skipped_reason = excluded.skipped_reason, \
         diff_json      = excluded.diff_json, \
         row_count      = excluded.row_count, \
         elapsed_ms     = excluded.elapsed_ms, \
         ran_at         = excluded.ran_at";

/// verify_jobs に書く 1 行 (PK = unko_date × eigyosho_id × cal)。
#[derive(Debug)]
pub struct VerifyJob<'a> {
    pub unko_date: &'a str,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub ok: bool,
    pub skipped_reason: Option<&'a str>,
    pub diff_json: Option<&'a str>,
    pub row_count: i64,
    pub elapsed_ms: i64,
    pub ran_at: &'a str,
}

impl VerifyJob<'_> {
    /// UPSERT の文。`month` は `unko_date` の先頭 7 文字 (`YYYY-MM`)。
    pub fn statement(&self) -> Stmt {
        let month = self.unko_date.get(..7).unwrap_or(self.unko_date);
        let text = |s: &str| Bind::Text(s.to_string());
        let opt = |s: Option<&str>| s.map_or(Bind::Null, text);
        let binds = vec![
            text(self.unko_date),
            Bind::Int(self.eigyosho_id),
            Bind::Int(i64::from(self.cal)),
            text(month),
            Bind::Int(i64::from(self.ok)),
            opt(self.skipped_reason),
            opt(self.diff_json),
            Bind::Int(self.row_count),
            Bind::Int(self.elapsed_ms),
            text(self.ran_at),
        ];
        Stmt {
            sql: VERIFY_JOB_UPSERT_SQL,
            binds,
        }
    }
}

/// 200 の JSON。
fn json<T: Serialize>(body: &T) -> Reply {
    // 文字列・整数・bool・map だけの型なので失敗しない
    Reply::ok(serde_json::to_string(body).unwrap_or_default())
}

/// bumon が空の営業所: 検証せず 200 + `skipped_reason`、verify_jobs に `ok = 1` で 1 行。
pub fn skipped(q: &VerifyQuery, elapsed_ms: u64, ran_at: &str) -> (Reply, Stmt) {
    let job = VerifyJob {
        unko_date: &q.date,
        eigyosho_id: q.id,
        cal: q.cal,
        ok: true,
        skipped_reason: Some(SKIPPED_NO_BUMON),
        diff_json: None,
        row_count: 0,
        elapsed_ms: elapsed_ms as i64,
        ran_at,
    };
    let reply = json(&VerifyResponse {
        office_id: q.id,
        date: q.date.clone(),
        cal: q.cal,
        php_sum: HashMap::new(),
        rust_sum: HashMap::new(),
        diff: None,
        ok: true,
        row_count: 0,
        elapsed_ms,
        skipped_reason: Some(SKIPPED_NO_BUMON.to_string()),
    });
    (reply, job.statement())
}

/// 取れた行を集計して PHP の `$sum` と突き合わせ、(応答, verify_jobs の UPSERT) を返す。
pub fn verified(
    q: &VerifyQuery,
    office: &OfficeMasters,
    php_sum: HashMap<String, PersonAccum>,
    rows: &[UriageRow],
    elapsed_ms: u64,
    ran_at: &str,
) -> (Reply, Stmt) {
    let persons = office.persons_as_int_map();
    let rust_sum = compute_person_sum(rows, &persons, &office.other, q.cal).sum;
    let diff = compute_verify_diff(&php_sum, &rust_sum);
    let ok = diff.is_none();
    let diff_json = diff.as_ref().and_then(|d| serde_json::to_string(d).ok());
    let job = VerifyJob {
        unko_date: &q.date,
        eigyosho_id: q.id,
        cal: q.cal,
        ok,
        skipped_reason: None,
        diff_json: diff_json.as_deref(),
        row_count: rows.len() as i64,
        elapsed_ms: elapsed_ms as i64,
        ran_at,
    };
    let reply = json(&VerifyResponse {
        office_id: q.id,
        date: q.date.clone(),
        cal: q.cal,
        php_sum,
        rust_sum,
        diff,
        ok,
        row_count: rows.len(),
        elapsed_ms,
        skipped_reason: None,
    });
    (reply, job.statement())
}

/// verify と同じ集計に、行ごとの判定・bumon・persons・other を添えた応答 (書かない)。bumon が空でも止めない。
pub fn debug(
    q: &VerifyQuery,
    office: &OfficeMasters,
    php_sum: HashMap<String, PersonAccum>,
    rows: &[UriageRow],
) -> Reply {
    let persons = office.persons_as_int_map();
    let result = compute_person_sum(rows, &persons, &office.other, q.cal);
    let diff = compute_verify_diff(&php_sum, &result.sum);
    let rows = rows
        .iter()
        .zip(result.rows.iter())
        .map(|(r, d)| VerifyDebugRow::new(r, d))
        .collect();
    json(&VerifyDebugResponse {
        office_id: q.id,
        date: q.date.clone(),
        cal: q.cal,
        bumon: office.bumon.clone(),
        persons,
        other: office.other.clone(),
        rows,
        php_sum,
        rust_sum: result.sum,
        diff,
    })
}
