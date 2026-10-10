//! `GET /api/kintai/wage-range?comp=&from=&to=&source=` (+ 任意の現行版) — 賃金確定値の期間の月別 + 合計 + カバレッジ。
//!
//! **root の `src/routes/wage_snapshot.rs` の `wage_range` (読み出し) の写し** (Refs #322。撤去までは片方を直したら
//! もう片方も直す)。保存 (`POST /api/kintai/wage-snapshot`) は移していない。判断は [`crate::wage_snapshot`]、
//! ここは入力の検査・SQL・行の詰め直し・応答の組み立てだけ。

use std::collections::HashMap;

use chrono::{DateTime, NaiveDate, Utc};
use postgres_types::Type;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{bad_request, parse_query, Fail, Param};
use crate::wage_snapshot::{
    add_months, aggregate_range, normalize_ts, resolve_months, ym_label, CurrentVersions,
    MonthBucket, MonthMasters, WageSnapshotRow, RESTRAINT_SOURCES,
};

/// 502 の本文の頭 (元と同じ)。
pub const DB_WHAT: &str = "kintai.wage_snapshot access";

/// 期間 (または 1 か月) の行を月順・乗務員CD順で引く。
pub const SELECT_RANGE_SQL: &str = r#"
SELECT to_char(ym, 'YYYY-MM') AS ym,
       driver_cd, driver_name, company, branch_name, branch_code, job_name,
       pay_kubun, hourly_rate, calc_base, calc_overtime, calc_total,
       paid_base, paid_overtime, working_minutes, restraint_missing,
       salary_item_sha, payroll_synced_at, wage_logic_version, timecard_kosoku,
       computed_at
  FROM kintai.wage_snapshot
 WHERE tenant_id = $1 AND comp_id = $2 AND restraint_source = $3
   AND ym >= $4 AND ym < $5
 ORDER BY ym, driver_cd
"#;

/// `?comp=&from=&to=&source=` + 任意の現行版 (鮮度判定に使う)。
#[derive(Debug, Default, Deserialize)]
pub struct RangeQuery {
    pub comp: Option<String>,
    pub from: Option<String>,
    pub to: Option<String>,
    pub source: Option<String>,
    pub salary_item_sha: Option<String>,
    pub wage_logic_version: Option<String>,
    pub payroll_synced_at: Option<String>,
}

/// 検査済みの入力。`months` は期間の全月 (月初、昇順、1 つ以上)。
#[derive(Debug, Clone)]
pub struct Request {
    pub comp: String,
    pub source: String,
    pub from: String,
    pub to: String,
    pub months: Vec<NaiveDate>,
    pub current: CurrentVersions,
}

/// クエリ文字列を読んで検査する (元の handler の 400 と同じ条件・同じ文言)。
pub fn parse(query: &str) -> Result<Request, Fail> {
    validate(parse_query(query)?)
}

pub fn validate(q: RangeQuery) -> Result<Request, Fail> {
    let comp = q.comp.as_deref().unwrap_or("").trim().to_string();
    if comp.is_empty() {
        return Err(bad_request("comp は必須です"));
    }
    let source = q.source.as_deref().unwrap_or("gcp").to_string();
    if !RESTRAINT_SOURCES.contains(&source.as_str()) {
        return Err(bad_request("source は gcp / current のいずれかです"));
    }
    let (from, to) = match (&q.from, &q.to) {
        (Some(f), Some(t)) => (f.clone(), t.clone()),
        _ => return Err(bad_request("from / to は YYYY-MM で指定してください")),
    };
    let months = resolve_months(&from, &to).map_err(bad_request)?;
    let current = CurrentVersions {
        salary_item_sha: q.salary_item_sha,
        wage_logic_version: q.wage_logic_version,
        // 画面が送ってくる時刻も保存側と同じ正規化を通す (表記揺れで常に stale になるのを防ぐ)。
        // 形が違う時は判定材料にしない
        payroll_synced_at: q.payroll_synced_at.as_deref().and_then(normalize_ts),
    };
    Ok(Request {
        comp,
        source,
        from,
        to,
        months,
        current,
    })
}

/// `SELECT_RANGE_SQL` の引数。`$1` = テナント (UUID の pin)、`$2` = comp_id (TEXT)、`$3` = restraint_source (TEXT)、
/// `$4`/`$5` = 期間の `[最初の月初, 最後の翌月初)` (DATE)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binds {
    pub tenant: Uuid,
    pub comp: String,
    pub source: String,
    pub lo: NaiveDate,
    pub hi: NaiveDate,
}

impl Binds {
    pub fn new(tenant: Uuid, req: &Request) -> Self {
        let lo = req.months[0];
        let hi = add_months(*req.months.last().expect("months is not empty"), 1);
        Self {
            tenant,
            comp: req.comp.clone(),
            source: req.source.clone(),
            lo,
            hi,
        }
    }

    pub fn params(&self) -> Vec<Param<'_>> {
        vec![
            (&self.tenant, Type::UUID),
            (&self.comp, Type::TEXT),
            (&self.source, Type::TEXT),
            (&self.lo, Type::DATE),
            (&self.hi, Type::DATE),
        ]
    }
}

/// `SELECT_RANGE_SQL` の 1 行 (owned。時刻は DB の `TIMESTAMPTZ` のまま)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeRow {
    pub ym: String,
    pub row: WageSnapshotRow,
    pub salary_item_sha: Option<String>,
    pub payroll_synced_at: Option<DateTime<Utc>>,
    pub wage_logic_version: Option<String>,
    pub timecard_kosoku: Option<String>,
    pub computed_at: Option<DateTime<Utc>>,
}

/// SELECT の 1 行 → 保存の行 + その月の版 (時刻は RFC3339 の文字列に)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchedRow {
    pub ym: String,
    pub row: WageSnapshotRow,
    pub masters: MonthMasters,
    pub timecard_kosoku: Option<String>,
    pub wage_logic_version: Option<String>,
    pub computed_at: Option<String>,
}

/// 元の `to_fetched` の型変換の部分 (`TIMESTAMPTZ` → `to_rfc3339()`)。
pub fn to_fetched(r: RangeRow) -> FetchedRow {
    FetchedRow {
        ym: r.ym,
        row: r.row,
        masters: MonthMasters {
            salary_item_sha: r.salary_item_sha,
            payroll_synced_at: r.payroll_synced_at.map(|t| t.to_rfc3339()),
        },
        timecard_kosoku: r.timecard_kosoku,
        wage_logic_version: r.wage_logic_version,
        computed_at: r.computed_at.map(|t| t.to_rfc3339()),
    }
}

/// 引いた行を**期間の全月**の並びに詰め直す。行が 1 つも無い月は `None` (= 未保存) のまま残す —
/// 「応答に無い = 0」を作らないため。
pub fn to_buckets(
    months: &[NaiveDate],
    fetched: impl Iterator<Item = FetchedRow>,
) -> Vec<Option<MonthBucket>> {
    let mut by_month: HashMap<String, MonthBucket> = HashMap::new();
    for f in fetched {
        let bucket = by_month.entry(f.ym).or_insert_with(|| MonthBucket {
            rows: Vec::new(),
            masters: f.masters.clone(),
            timecard_kosoku: f.timecard_kosoku.clone(),
            wage_logic_version: f.wage_logic_version.clone(),
            computed_at: f.computed_at.clone(),
        });
        bucket.rows.push(f.row);
    }
    months
        .iter()
        .map(|m| by_month.remove(&ym_label(*m)))
        .collect()
}

/// 応答 `{"from", "to", "restraint_source", "months", "rows"}`。
pub fn respond(req: &Request, rows: Vec<RangeRow>) -> serde_json::Value {
    let buckets = to_buckets(&req.months, rows.into_iter().map(to_fetched));
    let agg = aggregate_range(&req.months, &buckets, &req.current);
    serde_json::json!({
        "from": req.from,
        "to": req.to,
        "restraint_source": req.source,
        "months": agg.months,
        "rows": agg.rows,
    })
}
