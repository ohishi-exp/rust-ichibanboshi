//! `POST /api/uriage/recalc` の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 段取りはオンプレ版 (root の `src/routes/uriage.rs` の `recalc`・`recalc_one`) と同じ: editable months →
//! 月ごとに masters (月末日) → job (month × 営業所) ごとに SQL Server 1 本 → 集計 → 置き場に書く。違いは:
//!
//! - 置き場は D1。bucket (month × 営業所) ごとに 1 batch (原子的): 日次と取引先別の cal=1/0 の DELETE 4 文 +
//!   `INSERT … SELECT … FROM json_each(?1)` 4 文 + recalc_jobs の upsert 1 文 ([`Bucket::statements`])
//! - raw は disk ではなく R2 (key は [`r2_key`]。D1 の `raw_path` にも key を入れる)。fingerprint 不変かつ同期済みなら put を省く
//!   ([`put_needed`])。put が通れば同じリクエストで `r2_synced` にする ([`synced_statement`])
//! - SQL を流す job が [`MAX_JOBS`] を超えたら SQL Server にも D1 にも触らず 422 ([`check_jobs`])
//! - job の失敗の error は `stage:kind` だけ (オンプレ版の生の文言は出さない)。bumon 空・month 不正・営業所なしはオンプレ版と同じ文言

use std::collections::HashMap;

use serde::Serialize;

use crate::api::{RecalcJobResult, RecalcQuery, RecalcResponse};
use crate::cakephp::OfficeMasters;
use crate::cakephp::{office_lookup, CakephpError, EditableMonthsResponse, MastersResponse};
use crate::compute::{
    compute_person_partner_sum_by_day, compute_person_sum_by_day, PartnerAccum, PersonAccum,
    PersonPartnerKey, UriageRow,
};
use crate::d1::{Bind, Stmt};
use crate::period::month_to_range;
use crate::raw::{r2_key, serialize_ndjson_and_fingerprint};
use crate::routes::Reply;
use crate::sqlserver::uriage_rows_sql;

/// raw を置く R2 の binding (wrangler.toml のトップレベルの `[[r2_buckets]]`、bucket `ichibanboshi-uriage`)。
pub const R2_BINDING: &str = "URIAGE_R2";

/// R2 の物の Content-Type (オンプレ版の `GET /api/uriage/raw` と同じ)。
pub const RAW_CONTENT_TYPE: &str = "application/gzip";

/// 1 リクエストで SQL を流す job (month × 営業所) の上限。超えたら 422 (上限は実測で直す)。
pub const MAX_JOBS: usize = 12;

/// 応答の `source_table` (オンプレ版と同じ)。
pub const SOURCE_TABLE: &str =
    "運転日報明細 + CakePHP masters → uriage_person_monthly + recalc_jobs";

/// masters の bumon が空のときの error (オンプレ版と同じ文言)。
pub const BUMON_EMPTY: &str = "masters.bumon is empty (CakePHP 旧仕様?)";

/// 時刻の形式 (オンプレ版の `calculated_at`・`computed_at` と同じ)。
const TIME_FORMAT: &str = "%Y-%m-%dT%H:%M:%SZ";

/// UNIX 時刻 (ms) → `YYYY-MM-DDTHH:MM:SSZ` (UTC)。Worker は `Date::now()` を渡す (chrono の clock は使わない)。
pub fn timestamp(ms: u64) -> String {
    let t = chrono::DateTime::from_timestamp_millis(ms as i64);
    t.map(|t| t.format(TIME_FORMAT).to_string())
        .unwrap_or_default()
}

/// 処理する月 (`month` 指定ならその 1 つ、無ければ editable months 全部)。
pub fn target_months(q: &RecalcQuery, editable: &EditableMonthsResponse) -> Vec<String> {
    match &q.month {
        Some(m) => vec![m.clone()],
        None => editable.editable_months.clone(),
    }
}

/// `month` が CakePHP の編集可能月の外か (外でも recalc は走らせる。ログに残すだけ)。
pub fn outside_editable(q: &RecalcQuery, editable: &EditableMonthsResponse) -> bool {
    let months = &editable.editable_months;
    q.month.as_ref().is_some_and(|m| !months.contains(m))
}

/// SQL を流す 1 job (month × 営業所)。
#[derive(Debug, Clone)]
pub struct Job {
    pub month: String,
    pub eigyosho_id: i64,
    /// 運行年月日の下限・上限 (`YYYY-MM-DD`、含む)
    pub from: String,
    pub to: String,
    pub masters: OfficeMasters,
}

/// 応答の jobs の 1 つになるもの。`Done` は SQL を流さずに決まった結果 (failed / skipped)。
#[derive(Debug)]
pub enum Step {
    Done(RecalcJobResult),
    Run(Job),
}

fn result(month: &str, eigyosho_id: i64, status: &str, error: String) -> RecalcJobResult {
    RecalcJobResult {
        month: month.to_string(),
        eigyosho_id,
        status: status.to_string(),
        error: Some(error),
        ..Default::default()
    }
}

/// 月の範囲 (`(月初, 月末, masters の date)`)。形式が不正なら failed の結果 (オンプレ版と同じ文言)。
pub fn month_range(month: &str) -> Result<(String, String, String), Box<RecalcJobResult>> {
    let msg = || format!("month の形式が不正: {month}");
    month_to_range(month).ok_or_else(|| Box::new(result(month, 0, "failed", msg())))
}

/// 1 か月ぶんの job。masters が取れなければ failed 1 つ、営業所の指定が masters に無ければ skipped 1 つ。
/// 全営業所は id 昇順 (オンプレ版と同じ)。
pub fn plan_month(
    month: &str,
    from: &str,
    to: &str,
    masters: Result<MastersResponse, CakephpError>,
    eigyosho_id: Option<i64>,
) -> Vec<Step> {
    let masters = match masters {
        Ok(m) => m,
        Err(e) => {
            let error = format!("cakephp:{}", e.label());
            return vec![Step::Done(result(month, 0, "failed", error))];
        }
    };
    let job = |id: i64, om: &OfficeMasters| {
        Step::Run(Job {
            month: month.to_string(),
            eigyosho_id: id,
            from: from.to_string(),
            to: to.to_string(),
            masters: om.clone(),
        })
    };
    if let Some(eid) = eigyosho_id {
        let msg = format!("eigyosho_id={eid} が masters に存在しない");
        return vec![match office_lookup(&masters, eid) {
            Some(om) => job(eid, om),
            None => Step::Done(result(month, eid, "skipped", msg)),
        }];
    }
    let mut ids: Vec<(i64, &OfficeMasters)> = masters
        .offices
        .iter()
        .filter_map(|(k, v)| k.parse::<i64>().ok().map(|i| (i, v)))
        .collect();
    ids.sort_by_key(|(id, _)| *id);
    ids.into_iter().map(|(id, om)| job(id, om)).collect()
}

/// SQL を流す job が [`MAX_JOBS`] を超えたら 422 (`jobs:too_many`)。
pub fn check_jobs(steps: &[Step]) -> Result<(), Reply> {
    let n = steps.iter().filter(|s| matches!(s, Step::Run(_))).count();
    if n > MAX_JOBS {
        return Err(Reply::failure(422, "jobs", "too_many"));
    }
    Ok(())
}

impl Job {
    /// 成功の結果の雛形 (`status = "computed"`)。
    pub fn computed(&self) -> RecalcJobResult {
        RecalcJobResult {
            month: self.month.clone(),
            eigyosho_id: self.eigyosho_id,
            status: "computed".to_string(),
            ..Default::default()
        }
    }

    /// 失敗の結果 (`error` は `stage:kind` か、オンプレ版と同じ文言)。
    pub fn failed(&self, error: &str) -> RecalcJobResult {
        result(&self.month, self.eigyosho_id, "failed", error.to_string())
    }

    /// bumon が空なら SQL を流さず failed (オンプレ版と同じ防御)。
    pub fn bumon_empty(&self) -> bool {
        self.masters.bumon.is_empty()
    }

    /// SQL Server に流す SQL (`@P1` = from、`@P2` = to)。部門が英数字に絞って空なら `None` (= 0 行。接続の後に見る)。
    pub fn sql(&self) -> Option<String> {
        let persons: Vec<i32> = self.masters.persons_as_int_map().into_keys().collect();
        uriage_rows_sql(&self.masters.bumon, &persons)
    }

    /// 取れた行から bucket (raw・fingerprint・D1 の文) を組む。
    pub fn bucket(&self, rows: &[UriageRow], calculated_at: &str) -> Bucket {
        Bucket::new(self, rows, calculated_at)
    }

    /// 失敗を recalc_jobs に記録する文。
    pub fn failed_statement(&self, error: &str, now: &str) -> Stmt {
        failed_statement(&self.month, self.eigyosho_id, error, now)
    }

    /// 既存の recalc_jobs の行 (fingerprint_after・status) を読む文。結果は [`put_needed`] に渡す。
    pub fn existing_statement(&self) -> Stmt {
        let binds = vec![Bind::Text(self.month.clone()), Bind::Int(self.eigyosho_id)];
        Stmt {
            sql: EXISTING_SQL,
            binds,
        }
    }

    /// R2 に置けた後に `r2_synced` にする文。
    pub fn synced_statement(&self, synced_at: &str) -> Stmt {
        synced_statement(&self.month, self.eigyosho_id, synced_at)
    }
}

/// 1 bucket の raw・fingerprint・D1 に流す文。
#[derive(Debug)]
pub struct Bucket {
    /// gzip 前の raw ndjson (オンプレ版と同じバイト列)
    pub ndjson: Vec<u8>,
    pub fingerprint: String,
    /// R2 の key (= D1 の `raw_path`)
    pub key: String,
    pub row_count: usize,
    pub daily_count_cal: usize,
    pub daily_count_nocal: usize,
    pub statements: Vec<Stmt>,
}

/// 日次の 1 行 (`json_each` に渡す配列の要素): unko_date, person_name, kingaku, yosha_kingaku, kensuu, *_y0
pub type DailyRow<'a> = (&'a str, &'a str, i64, i64, i64, i64, i64, i64);

/// 取引先別の 1 行: unko_date, person_name, partner_kind, partner_code, partner_name, kingaku, kensuu, *_y0
pub type PartnerRow<'a> = (
    &'a str,
    &'a str,
    &'a str,
    &'a str,
    &'a str,
    i64,
    i64,
    i64,
    i64,
);

/// 日次の行 (`(unko_date, person_name)` の昇順)。0 行は入れない (6 値のどれかが非 0 の行だけ)。
/// src/sqlite.rs:464-478 の写し。PR-F で root 側を消す。
pub fn daily_rows(sums: &HashMap<String, HashMap<String, PersonAccum>>) -> Vec<DailyRow<'_>> {
    let mut out: Vec<DailyRow<'_>> = sums
        .iter()
        .flat_map(|(date, per)| per.iter().map(move |(name, v)| (date, name, v)))
        .filter(|(_, _, v)| {
            v.kensuu != 0
                || v.kingaku != 0
                || v.yosha_kingaku != 0
                || v.kensuu_y0 != 0
                || v.kingaku_y0 != 0
                || v.yosha_kingaku_y0 != 0
        })
        .map(|(d, n, v)| {
            let (k, y, c) = (v.kingaku, v.yosha_kingaku, v.kensuu);
            (
                d.as_str(),
                n.as_str(),
                k,
                y,
                c,
                v.kingaku_y0,
                v.yosha_kingaku_y0,
                v.kensuu_y0,
            )
        })
        .collect();
    out.sort();
    out
}

pub type PartnerSums = HashMap<String, HashMap<PersonPartnerKey, PartnerAccum>>;

/// 取引先別の行 (昇順)。0 行は入れない (4 値のどれかが非 0 の行だけ)。src/sqlite.rs:552-558 の写し。PR-F で root 側を消す。
pub fn partner_rows(sums: &PartnerSums) -> Vec<PartnerRow<'_>> {
    let mut out: Vec<PartnerRow<'_>> = sums
        .iter()
        .flat_map(|(date, per)| per.iter().map(move |(k, v)| (date, k, v)))
        .filter(|(_, _, v)| {
            v.kensuu != 0 || v.kingaku != 0 || v.kensuu_y0 != 0 || v.kingaku_y0 != 0
        })
        .map(|(d, k, v)| {
            let (kind, code) = (k.partner_kind.as_str(), k.partner_code.as_str());
            let (name, pn) = (k.person_name.as_str(), v.partner_name.as_str());
            (
                d.as_str(),
                name,
                kind,
                code,
                pn,
                v.kingaku,
                v.kensuu,
                v.kingaku_y0,
                v.kensuu_y0,
            )
        })
        .collect();
    out.sort();
    out
}

fn json<T: Serialize>(rows: &[T]) -> String {
    // 文字列と整数のタプルだけなので失敗しない
    serde_json::to_string(rows).unwrap_or_default()
}

impl Bucket {
    fn new(job: &Job, rows: &[UriageRow], calculated_at: &str) -> Self {
        let persons = job.masters.persons_as_int_map();
        let other = &job.masters.other;
        // raw fingerprint (cal は raw rows には影響しないので 1 回でよい)
        let (ndjson, fingerprint) = serialize_ndjson_and_fingerprint(rows);
        let key = r2_key(&job.month, job.eigyosho_id);

        let daily_cal = compute_person_sum_by_day(rows, &persons, other, true);
        let daily_nocal = compute_person_sum_by_day(rows, &persons, other, false);
        let partner_cal = compute_person_partner_sum_by_day(rows, &persons, other, true);
        let partner_nocal = compute_person_partner_sum_by_day(rows, &persons, other, false);
        let (d1, d0) = (daily_rows(&daily_cal), daily_rows(&daily_nocal));
        let (p1, p0) = (partner_rows(&partner_cal), partner_rows(&partner_nocal));

        let month = Bind::Text(job.month.clone());
        let eid = Bind::Int(job.eigyosho_id);
        let at = Bind::Text(calculated_at.to_string());
        let delete = |sql: &'static str, cal: i64| Stmt {
            sql,
            binds: vec![month.clone(), eid.clone(), Bind::Int(cal)],
        };
        let insert = |sql: &'static str, rows: String, cal: i64| Stmt {
            sql,
            binds: vec![
                Bind::Text(rows),
                month.clone(),
                eid.clone(),
                Bind::Int(cal),
                at.clone(),
            ],
        };
        let statements = vec![
            delete(DAILY_DELETE_SQL, 1),
            insert(DAILY_INSERT_SQL, json(&d1), 1),
            delete(DAILY_DELETE_SQL, 0),
            insert(DAILY_INSERT_SQL, json(&d0), 0),
            delete(PARTNER_DELETE_SQL, 1),
            insert(PARTNER_INSERT_SQL, json(&p1), 1),
            delete(PARTNER_DELETE_SQL, 0),
            insert(PARTNER_INSERT_SQL, json(&p0), 0),
            computed_statement(
                &job.month,
                job.eigyosho_id,
                &fingerprint,
                &key,
                calculated_at,
            ),
        ];
        Bucket {
            row_count: rows.len(),
            daily_count_cal: d1.len(),
            daily_count_nocal: d0.len(),
            ndjson,
            fingerprint,
            key,
            statements,
        }
    }

    /// R2 の customMetadata (fingerprint と computed_at)。
    pub fn metadata(&self, computed_at: &str) -> HashMap<String, String> {
        HashMap::from([
            ("fingerprint".to_string(), self.fingerprint.clone()),
            ("computed_at".to_string(), computed_at.to_string()),
        ])
    }
}

/// 既存の行 ([`Job::existing_statement`] の結果、D1 の `raw()`) から、R2 に置き直すかを決める。
/// fingerprint 不変かつ `r2_synced` なら置かない (R2 の物は同じ中身のまま)。行が無い・読めないなら置く。
pub fn put_needed(existing: &[Vec<serde_json::Value>], fingerprint: &str) -> bool {
    let Some(row) = existing.first() else {
        return true;
    };
    let fp = row.first().and_then(|v| v.as_str());
    let status = row.get(1).and_then(|v| v.as_str());
    !(fp == Some(fingerprint) && status == Some("r2_synced"))
}

/// 応答の本文 (オンプレ版の `RecalcResponse` と同じ形)。
pub fn response(
    months: Vec<String>,
    editable_months_count: i32,
    calculated_at: String,
    jobs: Vec<RecalcJobResult>,
) -> String {
    let body = RecalcResponse {
        source_table: SOURCE_TABLE.to_string(),
        months,
        editable_months_count,
        calculated_at,
        jobs,
    };
    // 文字列・整数だけの型なので失敗しない
    serde_json::to_string(&body).unwrap_or_default()
}

// ── D1 の文 (書き込み) ──
// オンプレ版の rusqlite の文 (src/sqlite.rs) と同じ意味。行の中身は JSON の配列 1 本 (?1) で渡し、json_each で展開する
// (D1 の 1 文の bind は 100 まで。行ごとに bind を並べると収まらない)。?2 = month、?3 = eigyosho_id、?4 = cal、?5 = calculated_at

/// 日次の bucket × cal の削除 (src/sqlite.rs:450 の写し。PR-F で root 側を消す)。
pub const DAILY_DELETE_SQL: &str =
    "DELETE FROM uriage_person_daily WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3";

/// 日次の投入 (src/sqlite.rs:450 の INSERT を json_each にしたもの。PR-F で root 側を消す)。
pub const DAILY_INSERT_SQL: &str = "INSERT INTO uriage_person_daily \
     (unko_date, month, person_name, eigyosho_id, cal, kingaku, \
      yosha_kingaku, kensuu, kingaku_y0, yosha_kingaku_y0, kensuu_y0, \
      calculated_at) \
     SELECT json_extract(value, '$[0]'), ?2, json_extract(value, '$[1]'), ?3, ?4, \
            json_extract(value, '$[2]'), json_extract(value, '$[3]'), \
            json_extract(value, '$[4]'), json_extract(value, '$[5]'), \
            json_extract(value, '$[6]'), json_extract(value, '$[7]'), ?5 \
     FROM json_each(?1)";

/// 取引先別の bucket × cal の削除 (src/sqlite.rs:540 の写し。PR-F で root 側を消す)。
pub const PARTNER_DELETE_SQL: &str =
    "DELETE FROM uriage_person_partner_daily WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3";

/// 取引先別の投入 (src/sqlite.rs:540 の INSERT を json_each にしたもの。PR-F で root 側を消す)。
pub const PARTNER_INSERT_SQL: &str = "INSERT INTO uriage_person_partner_daily \
     (unko_date, month, person_name, eigyosho_id, cal, partner_kind, \
      partner_code, partner_name, kingaku, kensuu, kingaku_y0, kensuu_y0, \
      calculated_at) \
     SELECT json_extract(value, '$[0]'), ?2, json_extract(value, '$[1]'), ?3, ?4, \
            json_extract(value, '$[2]'), json_extract(value, '$[3]'), \
            json_extract(value, '$[4]'), json_extract(value, '$[5]'), \
            json_extract(value, '$[6]'), json_extract(value, '$[7]'), \
            json_extract(value, '$[8]'), ?5 \
     FROM json_each(?1)";

/// recalc の成功の upsert (src/sqlite.rs:960-990 の写し。PR-F で root 側を消す)。
/// fingerprint 不変なら status・r2_synced_at・fingerprint_changed_at を保つ。変われば computed・NULL・now。
/// オンプレ版 src/sqlite.rs の ON CONFLICT は failed も保持してしまう。Worker では直した (保つのは computed / r2_synced だけ)。
pub const COMPUTED_SQL: &str = "INSERT INTO recalc_jobs \
     (month, eigyosho_id, status, fingerprint_before, fingerprint_after, \
      raw_path, created_at, computed_at, fingerprint_changed_at, \
      r2_synced_at, last_error) \
 VALUES (?1, ?2, 'computed', NULL, ?3, ?4, ?5, ?5, ?5, NULL, NULL) \
 ON CONFLICT(month, eigyosho_id) DO UPDATE SET \
     status = CASE \
         WHEN recalc_jobs.fingerprint_after = excluded.fingerprint_after \
              AND recalc_jobs.status IN ('computed', 'r2_synced') \
              THEN recalc_jobs.status \
         ELSE 'computed' \
     END, \
     fingerprint_before = recalc_jobs.fingerprint_after, \
     fingerprint_after = excluded.fingerprint_after, \
     raw_path = excluded.raw_path, \
     computed_at = excluded.computed_at, \
     fingerprint_changed_at = CASE \
         WHEN recalc_jobs.fingerprint_after = excluded.fingerprint_after \
              THEN recalc_jobs.fingerprint_changed_at \
         ELSE excluded.fingerprint_changed_at \
     END, \
     r2_synced_at = CASE \
         WHEN recalc_jobs.fingerprint_after = excluded.fingerprint_after \
              THEN recalc_jobs.r2_synced_at \
         ELSE NULL \
     END, \
     last_error = NULL";

/// recalc の失敗の記録 (src/sqlite.rs:1002-1030 の写し。PR-F で root 側を消す)。raw の状態は触らない。
pub const FAILED_SQL: &str = "INSERT INTO recalc_jobs \
     (month, eigyosho_id, status, created_at, last_error) \
 VALUES (?1, ?2, 'failed', ?3, ?4) \
 ON CONFLICT(month, eigyosho_id) DO UPDATE SET \
     status = 'failed', \
     last_error = excluded.last_error";

/// R2 に置けた記録 (src/sqlite.rs:1225 の `record_r2_synced` の写し。PR-F で root 側を消す)。
pub const SYNCED_SQL: &str = "UPDATE recalc_jobs SET status = 'r2_synced', r2_synced_at = ?1 \
     WHERE month = ?2 AND eigyosho_id = ?3 AND status = 'computed'";

/// put を省くかの判定に読む既存の行。
pub const EXISTING_SQL: &str =
    "SELECT fingerprint_after, status FROM recalc_jobs WHERE month = ?1 AND eigyosho_id = ?2";

fn text(s: &str) -> Bind {
    Bind::Text(s.to_string())
}

/// 成功の upsert の文 (raw_path は R2 の key)。
pub fn computed_statement(month: &str, eid: i64, fingerprint: &str, key: &str, at: &str) -> Stmt {
    let binds = vec![
        text(month),
        Bind::Int(eid),
        text(fingerprint),
        text(key),
        text(at),
    ];
    Stmt {
        sql: COMPUTED_SQL,
        binds,
    }
}

/// 失敗の記録の文。
pub fn failed_statement(month: &str, eid: i64, error: &str, now: &str) -> Stmt {
    let binds = vec![text(month), Bind::Int(eid), text(now), text(error)];
    Stmt {
        sql: FAILED_SQL,
        binds,
    }
}

/// R2 に置けた記録の文。
pub fn synced_statement(month: &str, eid: i64, synced_at: &str) -> Stmt {
    let binds = vec![text(synced_at), text(month), Bind::Int(eid)];
    Stmt {
        sql: SYNCED_SQL,
        binds,
    }
}
