//! `POST /api/kintai/wage-snapshot` — 賃金確定値の 1 か月ぶんの置き換え保存の純粋部分 (Refs #291、#322)。
//!
//! root の `src/routes/wage_snapshot.rs` の保存側 (`put_wage_snapshot`・`write_month`) から移した: SQL 定数・
//! `payroll_synced_at` の検査・「前回と同じなら書かない」の判定・応答・bind に渡す列ごとの Vec の束。
//! root と勤怠 Worker が同じものを使う (写さない)。bind と transaction はそれぞれが持つ。
//!
//! 保存は「置き換え」: 同じ `(tenant, comp_id, ym, restraint_source)` を 1 transaction で DELETE → INSERT する
//! (UPSERT にすると、その月から消えた乗務員の行が残る)。内容が前回と同じなら `skipped_unchanged: true` を返して
//! DB に触らない (`computed_at` も動かさない)。

use chrono::{DateTime, Utc};

use crate::common::{bad_request, Fail};
use crate::wage_range::FetchedRow;
use crate::wage_snapshot::{rows_equal, ValidSnapshot, WageSnapshotRow};

/// 置き換える月の行を消す。`$1` = テナント (UUID の pin)、`$2` = comp_id、`$3` = ym (DATE)、`$4` = restraint_source。
pub const DELETE_MONTH_SQL: &str = r#"
DELETE FROM kintai.wage_snapshot
 WHERE tenant_id = $1 AND comp_id = $2 AND ym = $3 AND restraint_source = $4
"#;

/// 入れる行を **1 文で**。列ごとの配列を `unnest` で行に開く (`kintai_push` と同じ作法)。
pub const INSERT_ROWS_SQL: &str = r#"
INSERT INTO kintai.wage_snapshot
       (tenant_id, comp_id, ym, restraint_source, driver_cd, driver_name, company,
        branch_name, branch_code, job_name, pay_kubun, hourly_rate,
        calc_base, calc_overtime, calc_total, paid_base, paid_overtime,
        working_minutes, restraint_missing,
        salary_item_sha, min_wage_sha, payroll_synced_at, wage_logic_version,
        timecard_kosoku, computed_at)
SELECT $1, $2, $3, $4, d.driver_cd, d.driver_name, d.company, d.branch_name,
       d.branch_code, d.job_name, d.pay_kubun, d.hourly_rate,
       d.calc_base, d.calc_overtime, d.calc_total, d.paid_base, d.paid_overtime,
       d.working_minutes, d.restraint_missing,
       -- min_wage_sha は常に NULL (2026-08-05 に廃止、`crate::wage_snapshot` の docs 参照)
       -- timecard_kosoku ($8) は会社 × 月 × ソースの属性なので全行に同じ値が入る
       -- (`salary_item_sha` / `wage_logic_version` と同じ持ち方)
       $5, NULL, $6, $7, $8, now()
  FROM unnest($9::int8[], $10::text[], $11::text[], $12::text[], $13::int4[],
              $14::text[], $15::int2[], $16::int4[], $17::int4[], $18::int4[],
              $19::int4[], $20::int4[], $21::int4[], $22::int4[], $23::bool[])
       AS d(driver_cd, driver_name, company, branch_name, branch_code, job_name,
            pay_kubun, hourly_rate, calc_base, calc_overtime, calc_total,
            paid_base, paid_overtime, working_minutes, restraint_missing)
"#;

/// `payroll_synced_at` (RFC3339 文字列) を `TIMESTAMPTZ` に渡せる形へ (`$6`)。
/// 形が違えば 400 — 黙って NULL にすると「給与未取込」に化けて月ごと集計から消える。
pub fn parse_synced_at(s: Option<&String>) -> Result<Option<DateTime<Utc>>, Fail> {
    match s {
        None => Ok(None),
        Some(v) => DateTime::parse_from_rfc3339(v)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|_| bad_request("masters.payroll_synced_at は RFC3339 で指定してください")),
    }
}

/// 既存 (その月の `SELECT_RANGE_SQL` の行) と同じなら、書かずに返す応答。違えば `None` (= 書く)。
///
/// `timecard_kosoku` も比べる。ここに入れないと、**土台の取得可否だけが変わった保存が `skipped_unchanged` で
/// 捨てられる**。
pub fn unchanged_response(
    fetched: &[FetchedRow],
    valid: &ValidSnapshot,
) -> Option<serde_json::Value> {
    let same_versions = fetched.first().is_some_and(|f| {
        f.masters == valid.masters
            && f.timecard_kosoku == valid.timecard_kosoku
            && f.wage_logic_version.as_deref() == Some(&valid.wage_logic_version)
    });
    let prev_rows: Vec<WageSnapshotRow> = fetched.iter().map(|f| f.row.clone()).collect();
    if !(same_versions && rows_equal(&prev_rows, &valid.rows)) {
        return None;
    }
    Some(serde_json::json!({
        "saved": prev_rows.len(),
        "skipped_unchanged": true,
        "computed_at": fetched.first().and_then(|f| f.computed_at.clone()),
        "timecard_kosoku": fetched.first().and_then(|f| f.timecard_kosoku.clone()),
    }))
}

/// 置き換え保存した後の応答。
pub fn saved_response(saved: usize, valid: &ValidSnapshot) -> serde_json::Value {
    serde_json::json!({
        "saved": saved,
        "skipped_unchanged": false,
        "timecard_kosoku": valid.timecard_kosoku,
    })
}

/// `INSERT_ROWS_SQL` の `$9`〜`$23` (列ごとの配列)。行が 0 なら INSERT しない (DELETE だけ)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WageColumns {
    pub driver_cd: Vec<i64>,
    pub driver_name: Vec<String>,
    pub company: Vec<Option<String>>,
    pub branch_name: Vec<Option<String>>,
    pub branch_code: Vec<Option<i32>>,
    pub job_name: Vec<Option<String>>,
    pub pay_kubun: Vec<Option<i16>>,
    pub hourly_rate: Vec<Option<i32>>,
    pub calc_base: Vec<Option<i32>>,
    pub calc_overtime: Vec<Option<i32>>,
    pub calc_total: Vec<Option<i32>>,
    pub paid_base: Vec<Option<i32>>,
    pub paid_overtime: Vec<Option<i32>>,
    pub working_minutes: Vec<Option<i32>>,
    pub restraint_missing: Vec<bool>,
}

/// 保存する行を 1 文ぶんの束に。
pub fn wage_columns(rows: &[WageSnapshotRow]) -> WageColumns {
    WageColumns {
        driver_cd: rows.iter().map(|r| r.driver_cd).collect(),
        driver_name: rows.iter().map(|r| r.driver_name.clone()).collect(),
        company: rows.iter().map(|r| r.company.clone()).collect(),
        branch_name: rows.iter().map(|r| r.branch_name.clone()).collect(),
        branch_code: rows.iter().map(|r| r.branch_code).collect(),
        job_name: rows.iter().map(|r| r.job_name.clone()).collect(),
        pay_kubun: rows.iter().map(|r| r.pay_kubun).collect(),
        hourly_rate: rows.iter().map(|r| r.hourly_rate).collect(),
        calc_base: rows.iter().map(|r| r.calc_base).collect(),
        calc_overtime: rows.iter().map(|r| r.calc_overtime).collect(),
        calc_total: rows.iter().map(|r| r.calc_total).collect(),
        paid_base: rows.iter().map(|r| r.paid_base).collect(),
        paid_overtime: rows.iter().map(|r| r.paid_overtime).collect(),
        working_minutes: rows.iter().map(|r| r.working_minutes).collect(),
        restraint_missing: rows.iter().map(|r| r.restraint_missing).collect(),
    }
}
