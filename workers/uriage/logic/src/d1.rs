//! 売上 Worker の D1 (binding `URIAGE_DB`) を読む純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! - **表の定義**: [`SCHEMA_SQL`] (= `worker/migrations/0001_uriage.sql`、D1 の正本)
//! - **読みの口 6 本の文**: SELECT はオンプレ版 (root の `src/sqlite.rs` の `LocalStore`) の rusqlite の文と同じ文字列
//! - **入力の検査** ([`statements`]): 必須の Query が空なら 400 (オンプレ版の handler と同じ判定・同じ本文)
//! - **結果の行の読み取り** ([`respond`]): D1 の `raw()` (列の並びの配列。数は JS の number) を受けて、オンプレ版と同じ
//!   応答の型に詰めて直列化する。読めなければ 500 (`rows:malformed`)
//!
//! 書き込みの文: recalc は [`crate::recalc`]。verify・admin は次の PR で足す。

use serde_json::Value;

use crate::api::{
    annotate_r2_pending, DailyQuery, DailyResponse, PersonDailyRow, PersonMonthlyRow,
    PersonMonthlyTotalsQuery, PersonMonthlyTotalsResponse, PersonPartnerTotalRow,
    PersonPartnerTotalsQuery, PersonPartnerTotalsResponse, R2PendingQuery, R2PendingResponse,
    R2PendingRow, RecalcJob, RecalcJobsQuery, RecalcJobsResponse, VerifyHistoryQuery,
    VerifyHistoryResponse, VerifyJobRow,
};
use crate::compute::PartnerKind;
use crate::routes::Reply;

/// D1 の binding (wrangler.toml のトップレベルの `[[d1_databases]]`)。
pub const D1_BINDING: &str = "URIAGE_DB";

/// D1 の表の定義 (4 表・index・VIEW 3 本)。正本は Worker の migration。
pub const SCHEMA_SQL: &str = include_str!("../../worker/migrations/0001_uriage.sql");

/// `/health`・`/probe` の到達の確認。
pub const PING_SQL: &str = "SELECT 1";

/// `GET /api/uriage/daily` (オンプレ版 `get_person_daily`)。
pub const DAILY_SQL: &str = "SELECT unko_date, month, person_name, eigyosho_id, cal, \
                            kingaku, yosha_kingaku, kensuu, \
                            kingaku_y0, yosha_kingaku_y0, kensuu_y0, calculated_at \
                     FROM uriage_person_daily \
                     WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3 \
                     ORDER BY unko_date ASC, kingaku DESC, person_name ASC";

/// `GET /api/uriage/person-monthly-totals` (オンプレ版 `person_monthly_totals`)。
pub const MONTHLY_TOTALS_SQL: &str = "SELECT month, person_name, 0 AS eigyosho_id, cal, \
                            SUM(kingaku), SUM(yosha_kingaku), SUM(kensuu), \
                            SUM(kingaku_y0), SUM(yosha_kingaku_y0), SUM(kensuu_y0), \
                            MAX(calculated_at) \
                     FROM uriage_person_daily \
                     WHERE month >= ?1 AND month <= ?2 AND cal = ?3 \
                     GROUP BY month, person_name, cal \
                     ORDER BY month ASC, person_name ASC";

/// `GET /api/uriage/person-partner-totals` (オンプレ版 `person_partner_totals`。得意先と傭車先で 1 本ずつ)。
pub const PARTNER_TOTALS_SQL: &str = "SELECT partner_code, partner_name, \
                            SUM(kingaku), SUM(kensuu), SUM(kingaku_y0), SUM(kensuu_y0) \
                     FROM uriage_person_partner_daily \
                     WHERE person_name = ?1 AND month >= ?2 AND month <= ?3 \
                       AND cal = ?4 AND partner_kind = ?5 \
                     GROUP BY partner_code, partner_name \
                     ORDER BY SUM(kingaku) DESC, partner_code ASC";

/// `GET /api/uriage/r2/pending` の期間あり (オンプレ版 `list_r2_pending`)。
pub const R2_PENDING_RANGE_SQL: &str =
    "SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at, \
                            verified_count, verified_ok, verified_ng \
                     FROM r2_pending \
                     WHERE month BETWEEN ?1 AND ?2";

/// `GET /api/uriage/r2/pending` の全件。
pub const R2_PENDING_ALL_SQL: &str =
    "SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at, \
                            verified_count, verified_ok, verified_ng \
                     FROM r2_pending";

/// `GET /api/uriage/recalc-jobs` の期間あり (オンプレ版 `list_recalc_jobs`)。
pub const RECALC_JOBS_RANGE_SQL: &str = "SELECT rj.month, rj.eigyosho_id, rj.status, \
                            rj.fingerprint_before, rj.fingerprint_after, \
                            rj.raw_path, rj.created_at, rj.computed_at, \
                            rj.fingerprint_changed_at, \
                            rj.r2_synced_at, rj.last_error, \
                            COALESCE(vc.verified_count, 0), \
                            COALESCE(vc.verified_ok,    0), \
                            COALESCE(vc.verified_ng,    0) \
                     FROM recalc_jobs rj \
                     LEFT JOIN verify_coverage vc \
                       ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id \
                     WHERE rj.month BETWEEN ?1 AND ?2 \
                     ORDER BY rj.month DESC, rj.eigyosho_id ASC";

/// `GET /api/uriage/recalc-jobs` の全件。
pub const RECALC_JOBS_ALL_SQL: &str = "SELECT rj.month, rj.eigyosho_id, rj.status, \
                            rj.fingerprint_before, rj.fingerprint_after, \
                            rj.raw_path, rj.created_at, rj.computed_at, \
                            rj.fingerprint_changed_at, \
                            rj.r2_synced_at, rj.last_error, \
                            COALESCE(vc.verified_count, 0), \
                            COALESCE(vc.verified_ok,    0), \
                            COALESCE(vc.verified_ng,    0) \
                     FROM recalc_jobs rj \
                     LEFT JOIN verify_coverage vc \
                       ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id \
                     ORDER BY rj.month DESC, rj.eigyosho_id ASC";

/// `GET /api/uriage/verify-history` の営業所あり (オンプレ版 `list_verify_jobs`)。
pub const VERIFY_HISTORY_OFFICE_SQL: &str =
    "SELECT unko_date, eigyosho_id, cal, month, ok, skipped_reason, \
                            diff_json, row_count, elapsed_ms, ran_at \
                     FROM verify_jobs \
                     WHERE unko_date BETWEEN ?1 AND ?2 AND eigyosho_id = ?3 \
                     ORDER BY unko_date ASC, eigyosho_id ASC, cal ASC";

/// `GET /api/uriage/verify-history` の全営業所。
pub const VERIFY_HISTORY_ALL_SQL: &str =
    "SELECT unko_date, eigyosho_id, cal, month, ok, skipped_reason, \
                            diff_json, row_count, elapsed_ms, ran_at \
                     FROM verify_jobs \
                     WHERE unko_date BETWEEN ?1 AND ?2 \
                     ORDER BY unko_date ASC, eigyosho_id ASC, cal ASC";

/// verify-history の 400 の本文 (オンプレ版と同じ)。
const VERIFY_HISTORY_REQUIRED: &str = "from と to は必須 (YYYY-MM-DD)";

/// bind の値 (`?NNN` の順)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    Text(String),
    Int(i64),
}

/// D1 に流す 1 文。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stmt {
    pub sql: &'static str,
    pub binds: Vec<Bind>,
}

impl Stmt {
    fn new(sql: &'static str, binds: Vec<Bind>) -> Self {
        Self { sql, binds }
    }
}

/// 読みの口 6 本の、Query を読んだ後の要求。
#[derive(Debug)]
pub enum Read {
    Daily(DailyQuery),
    PersonMonthlyTotals(PersonMonthlyTotalsQuery),
    PersonPartnerTotals(PersonPartnerTotalsQuery),
    R2Pending(R2PendingQuery),
    RecalcJobs(RecalcJobsQuery),
    VerifyHistory(VerifyHistoryQuery),
}

fn text(s: &str) -> Bind {
    Bind::Text(s.to_string())
}

fn flag(b: bool) -> Bind {
    Bind::Int(i64::from(b))
}

/// 入力の検査 (400) と、流す文 (この順に 1 本ずつ流し、結果を同じ順で [`respond`] に渡す)。
pub fn statements(read: &Read) -> Result<Vec<Stmt>, Reply> {
    Ok(match read {
        Read::Daily(q) => {
            if q.month.is_empty() {
                return Err(Reply::bare(400));
            }
            let binds = vec![text(&q.month), Bind::Int(q.eigyosho_id), flag(q.cal)];
            vec![Stmt::new(DAILY_SQL, binds)]
        }
        Read::PersonMonthlyTotals(q) => {
            if q.from.is_empty() || q.to.is_empty() {
                return Err(Reply::bare(400));
            }
            let binds = vec![text(&q.from), text(&q.to), flag(q.cal)];
            vec![Stmt::new(MONTHLY_TOTALS_SQL, binds)]
        }
        Read::PersonPartnerTotals(q) => {
            if q.person.is_empty() || q.from.is_empty() || q.to.is_empty() {
                return Err(Reply::bare(400));
            }
            [PartnerKind::Customer, PartnerKind::Subcontractor]
                .iter()
                .map(|kind| {
                    let binds = vec![
                        text(&q.person),
                        text(&q.from),
                        text(&q.to),
                        flag(q.cal),
                        text(kind.as_str()),
                    ];
                    Stmt::new(PARTNER_TOTALS_SQL, binds)
                })
                .collect()
        }
        Read::R2Pending(q) => match (&q.from, &q.to) {
            (Some(f), Some(t)) => vec![Stmt::new(R2_PENDING_RANGE_SQL, vec![text(f), text(t)])],
            _ => vec![Stmt::new(R2_PENDING_ALL_SQL, vec![])],
        },
        Read::RecalcJobs(q) => {
            // 空文字は指定なしと同じ (オンプレ版と同じ)
            let from = q.from.as_deref().filter(|s| !s.is_empty());
            let to = q.to.as_deref().filter(|s| !s.is_empty());
            match (from, to) {
                (Some(f), Some(t)) => {
                    vec![Stmt::new(RECALC_JOBS_RANGE_SQL, vec![text(f), text(t)])]
                }
                _ => vec![Stmt::new(RECALC_JOBS_ALL_SQL, vec![])],
            }
        }
        Read::VerifyHistory(q) => {
            if q.from.is_empty() || q.to.is_empty() {
                return Err(Reply::text(400, VERIFY_HISTORY_REQUIRED));
            }
            match q.eigyosho_id {
                Some(id) => {
                    let binds = vec![text(&q.from), text(&q.to), Bind::Int(id)];
                    vec![Stmt::new(VERIFY_HISTORY_OFFICE_SQL, binds)]
                }
                None => {
                    let binds = vec![text(&q.from), text(&q.to)];
                    vec![Stmt::new(VERIFY_HISTORY_ALL_SQL, binds)]
                }
            }
        }
    })
}

/// D1 の `raw()` の 1 行 (列の並び)。
type Row = Vec<Value>;

/// [`statements`] の文ごとの結果 (同じ順) から応答の本文を組む。行が読めなければ 500 (`rows:malformed`)。
pub fn respond(read: Read, results: Vec<Vec<Row>>) -> Result<String, Reply> {
    let mut results = results.into_iter();
    let mut next = || results.next().ok_or_else(malformed);
    let body = match read {
        Read::Daily(q) => serde_json::to_string(&DailyResponse {
            rows: decode(next()?, daily_row)?,
            month: q.month,
            eigyosho_id: q.eigyosho_id,
            cal: q.cal,
        }),
        Read::PersonMonthlyTotals(q) => serde_json::to_string(&PersonMonthlyTotalsResponse {
            rows: decode(next()?, monthly_row)?,
            from: q.from,
            to: q.to,
            cal: q.cal,
        }),
        Read::PersonPartnerTotals(q) => serde_json::to_string(&PersonPartnerTotalsResponse {
            customers: decode(next()?, partner_row)?,
            subcontractors: decode(next()?, partner_row)?,
            person: q.person,
            from: q.from,
            to: q.to,
            cal: q.cal,
        }),
        Read::R2Pending(_) => {
            let items: Vec<_> = decode(next()?, r2_pending_row)?
                .into_iter()
                .map(annotate_r2_pending)
                .collect();
            serde_json::to_string(&R2PendingResponse {
                count: items.len(),
                items,
            })
        }
        Read::RecalcJobs(q) => {
            let jobs = decode(next()?, recalc_job)?;
            serde_json::to_string(&RecalcJobsResponse {
                from: q.from,
                to: q.to,
                count: jobs.len(),
                jobs,
            })
        }
        Read::VerifyHistory(q) => {
            let rows = decode(next()?, verify_job)?;
            serde_json::to_string(&VerifyHistoryResponse {
                from: q.from,
                to: q.to,
                eigyosho_id: q.eigyosho_id,
                count: rows.len(),
                rows,
            })
        }
    };
    // 文字列・整数・bool だけの型なので失敗しない
    Ok(body.unwrap_or_default())
}

/// `/health`・`/probe` の [`PING_SQL`] の結果が 1 か。
pub fn ping_ok(rows: &[Row]) -> bool {
    rows.first()
        .and_then(|r| r.first())
        .and_then(as_int)
        .is_some_and(|v| v == 1)
}

fn malformed() -> Reply {
    Reply::failure(500, "rows", "malformed")
}

fn decode<T>(rows: Vec<Row>, f: fn(&Cols) -> Option<T>) -> Result<Vec<T>, Reply> {
    rows.iter()
        .map(|r| f(&Cols(r)).ok_or_else(malformed))
        .collect()
}

/// 1 行の列を番号で読む (オンプレ版の `r.get(i)` と同じ並び)。
struct Cols<'a>(&'a [Value]);

impl Cols<'_> {
    fn text(&self, i: usize) -> Option<String> {
        self.0.get(i)?.as_str().map(str::to_string)
    }

    /// NULL は `Some(None)`、文字列でも NULL でもない・列が無いは `None`。
    fn opt_text(&self, i: usize) -> Option<Option<String>> {
        match self.0.get(i)? {
            Value::Null => Some(None),
            v => v.as_str().map(|s| Some(s.to_string())),
        }
    }

    fn int(&self, i: usize) -> Option<i64> {
        as_int(self.0.get(i)?)
    }
}

/// 整数の列。D1 は JS の number で返すので、小数部の無い浮動小数も受ける。
fn as_int(v: &Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_f64().filter(|f| f.fract() == 0.0).map(|f| f as i64))
}

fn daily_row(c: &Cols) -> Option<PersonDailyRow> {
    Some(PersonDailyRow {
        unko_date: c.text(0)?,
        month: c.text(1)?,
        person_name: c.text(2)?,
        eigyosho_id: c.int(3)?,
        cal: c.int(4)? != 0,
        kingaku: c.int(5)?,
        yosha_kingaku: c.int(6)?,
        kensuu: c.int(7)?,
        kingaku_y0: c.int(8)?,
        yosha_kingaku_y0: c.int(9)?,
        kensuu_y0: c.int(10)?,
        calculated_at: c.text(11)?,
    })
}

fn monthly_row(c: &Cols) -> Option<PersonMonthlyRow> {
    Some(PersonMonthlyRow {
        month: c.text(0)?,
        person_name: c.text(1)?,
        eigyosho_id: c.int(2)?,
        cal: c.int(3)? != 0,
        kingaku: c.int(4)?,
        yosha_kingaku: c.int(5)?,
        kensuu: c.int(6)?,
        kingaku_y0: c.int(7)?,
        yosha_kingaku_y0: c.int(8)?,
        kensuu_y0: c.int(9)?,
        calculated_at: c.text(10)?,
    })
}

fn partner_row(c: &Cols) -> Option<PersonPartnerTotalRow> {
    Some(PersonPartnerTotalRow {
        partner_code: c.text(0)?,
        partner_name: c.text(1)?,
        kingaku: c.int(2)?,
        kensuu: c.int(3)?,
        kingaku_y0: c.int(4)?,
        kensuu_y0: c.int(5)?,
    })
}

fn r2_pending_row(c: &Cols) -> Option<R2PendingRow> {
    Some(R2PendingRow {
        month: c.text(0)?,
        eigyosho_id: c.int(1)?,
        raw_path: c.text(2)?,
        fingerprint_after: c.text(3)?,
        computed_at: c.text(4)?,
        verified_count: c.int(5)?,
        verified_ok: c.int(6)?,
        verified_ng: c.int(7)?,
    })
}

fn recalc_job(c: &Cols) -> Option<RecalcJob> {
    Some(RecalcJob {
        month: c.text(0)?,
        eigyosho_id: c.int(1)?,
        status: c.text(2)?,
        fingerprint_before: c.opt_text(3)?,
        fingerprint_after: c.opt_text(4)?,
        raw_path: c.opt_text(5)?,
        created_at: c.text(6)?,
        computed_at: c.opt_text(7)?,
        fingerprint_changed_at: c.opt_text(8)?,
        r2_synced_at: c.opt_text(9)?,
        last_error: c.opt_text(10)?,
        verified_count: c.int(11)?,
        verified_ok: c.int(12)?,
        verified_ng: c.int(13)?,
    })
}

fn verify_job(c: &Cols) -> Option<VerifyJobRow> {
    Some(VerifyJobRow {
        unko_date: c.text(0)?,
        eigyosho_id: c.int(1)?,
        cal: c.int(2)?,
        month: c.text(3)?,
        ok: c.int(4)?,
        skipped_reason: c.opt_text(5)?,
        diff_json: c.opt_text(6)?,
        row_count: c.int(7)?,
        elapsed_ms: c.int(8)?,
        ran_at: c.text(9)?,
    })
}
