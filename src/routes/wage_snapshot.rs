//! 賃金確定値の月次スナップショットの HTTP + SQL (Refs #291、
//! ohishi-exp/nuxt-dtako-admin#677)。判断は [`crate::wage_snapshot`] に置き、
//! ここは「受ける・引く・書く」だけを持つ。
//!
//! - `POST /api/kintai/wage-snapshot` — 画面が確定させた 1 か月ぶんを置き換え保存
//! - `GET  /api/kintai/wage-range` — 期間の月別 + 合計 + カバレッジを 1 往復で返す
//!
//! ## なぜ `/api/kintai/*` なのか (金額を返すのに)
//!
//! [`crate::routes::kintai_day_summaries`] のモジュール docs は「将来ここに金額を足す
//! ことになったら `/kyuyo/*` と同じ in-service gate へ移すこと」と指示している。
//! 一度そのとおり `/kyuyo/*` に置いたが、**本番で 503 になった** (2026-08-05):
//!
//! | | Supabase 接続 (`[kintai_push]`) | `/kyuyo/*` の認可 |
//! |---|---|---|
//! | ohishi-data (`/api/kyuyo/*` の宛先) | 無い | ある |
//! | GCP Cloud Run (`/api/kintai/*` の宛先) | ある | 無い |
//!
//! この表が読み書きする `kintai.wage_snapshot` は Supabase にあり、そこへ繋がるのは
//! GCP のインスタンスだけ。**Supabase の接続情報を ohishi-data (local) には置かない**
//! 方針なので (auth-worker 1 箇所に資格情報を集約する設計)、口は GCP 側に置くしかない。
//!
//! ## 代わりに何が守っているか
//!
//! GCP の Cloud Run は `--no-allow-unauthenticated` で、到達できるのは auth-worker の
//! `/ichibanboshi-proxy` が OIDC を mint した呼び出しだけ。その手前で
//! `dtako-scraper-relay` の `restraint-api` が auth-worker JWT + 閲覧者 email で
//! 認可している。**edge の CF Access だけに寄りかかってはいない。**
//!
//! `/kyuyo/*` と同じ in-service gate をここに掛けるには GCP 側に introspect と
//! allowlist の設定を配る必要があり、それは「資格情報を増やさない」方針と衝突する。
//!
//! ## テナントは設定 pin (`X-Tenant-ID` を読まない)
//!
//! `kintai_day_summaries` / `stale_months` と同じ [`ReadTenant`]。ヘッダでテナントを
//! 選べる口にすると、auth-worker の `/ichibanboshi-proxy` allowlist (shared secret で
//! 通る) 経由で他テナントを引けてしまう。**ヘッダ由来に変えるなら同じ PR で
//! allowlist から外すこと。**
//!
//! ## 保存は「置き換え」
//!
//! 同じ `(tenant, comp_id, ym, restraint_source)` は 1 トランザクションで
//! DELETE → INSERT する。UPSERT にすると、**その月から消えた乗務員の行が残る** —
//! 退職者が期間集計にいつまでも出続けることになる。
//!
//! ## 同じ内容なら書かない
//!
//! 画面は月タブを行き来するたびに保存を投げる。内容が前回と同じなら
//! `skipped_unchanged: true` を返して DB に触らない (`computed_at` も動かさない —
//! 動かすと「いつ計算した値か」が読めなくなる)。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Deserialize;
use sqlx::Row;

use crate::kintai_push::KintaiPgStore;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};
use crate::wage_snapshot::{
    add_months, aggregate_range, normalize_ts, resolve_months, rows_equal, validate_snapshot,
    CurrentVersions, MonthBucket, MonthMasters, SnapshotRequest, ValidSnapshot, WageSnapshotRow,
};

/// `[kintai_push]` が無効な instance では挿さらない。`kintai_day_summaries::store`
/// と同じ文言で 503 にする。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 読み書き先のテナント。`stale_months::read_tenant_of` と同じ形 — **どちらも無ければ 503**。
fn tenant_of(read: ReadTenant, pin: uuid::Uuid) -> Result<uuid::Uuid, (StatusCode, String)> {
    if let Some(t) = read.0 {
        if !t.is_nil() {
            return Ok(t);
        }
    }
    if !pin.is_nil() {
        return Ok(pin);
    }
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        "読み先のテナントが決まりません ([kintai_events] tenant_id を設定してください)".to_string(),
    ))
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.into())
}

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.wage_snapshot access failed: {e}"),
    )
}

const DELETE_MONTH_SQL: &str = r#"
DELETE FROM kintai.wage_snapshot
 WHERE tenant_id = $1 AND comp_id = $2 AND ym = $3 AND restraint_source = $4
"#;

/// 入れる行を **1 文で**。列ごとの配列を `unnest` で行に開く (`kintai_push` と同じ作法)。
const INSERT_ROWS_SQL: &str = r#"
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

/// 期間 (または 1 か月) の行を月順・乗務員CD順で引く。
const SELECT_RANGE_SQL: &str = r#"
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

/// SELECT の 1 行 → 保存の行 + その月の版。
struct FetchedRow {
    ym: String,
    row: WageSnapshotRow,
    masters: MonthMasters,
    timecard_kosoku: Option<String>,
    wage_logic_version: Option<String>,
    computed_at: Option<String>,
}

fn to_fetched(r: &sqlx::postgres::PgRow) -> FetchedRow {
    let synced: Option<DateTime<Utc>> = r.get("payroll_synced_at");
    let computed: Option<DateTime<Utc>> = r.get("computed_at");
    FetchedRow {
        ym: r.get::<String, _>("ym"),
        row: WageSnapshotRow {
            driver_cd: r.get::<i64, _>("driver_cd"),
            driver_name: r.get::<String, _>("driver_name"),
            company: r.get("company"),
            branch_name: r.get("branch_name"),
            branch_code: r.get("branch_code"),
            job_name: r.get("job_name"),
            pay_kubun: r.get("pay_kubun"),
            hourly_rate: r.get("hourly_rate"),
            calc_base: r.get("calc_base"),
            calc_overtime: r.get("calc_overtime"),
            calc_total: r.get("calc_total"),
            paid_base: r.get("paid_base"),
            paid_overtime: r.get("paid_overtime"),
            working_minutes: r.get("working_minutes"),
            restraint_missing: r.get::<bool, _>("restraint_missing"),
        },
        masters: MonthMasters {
            salary_item_sha: r.get("salary_item_sha"),
            payroll_synced_at: synced.map(|t| t.to_rfc3339()),
        },
        timecard_kosoku: r.get("timecard_kosoku"),
        wage_logic_version: r.get("wage_logic_version"),
        computed_at: computed.map(|t| t.to_rfc3339()),
    }
}

/// `payroll_synced_at` (RFC3339 文字列) を `TIMESTAMPTZ` に渡せる形へ。
/// 形が違えば 400 — 黙って NULL にすると「給与未取込」に化けて月ごと集計から消える。
fn parse_synced_at(s: Option<&String>) -> Result<Option<DateTime<Utc>>, (StatusCode, String)> {
    match s {
        None => Ok(None),
        Some(v) => DateTime::parse_from_rfc3339(v)
            .map(|t| Some(t.with_timezone(&Utc)))
            .map_err(|_| bad_request("masters.payroll_synced_at は RFC3339 で指定してください")),
    }
}

/// POST /api/kintai/wage-snapshot — 1 か月ぶんを置き換え保存する。
pub async fn put_wage_snapshot(
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
    Json(req): Json<SnapshotRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let valid = validate_snapshot(req).map_err(bad_request)?;
    let synced_at = parse_synced_at(valid.masters.payroll_synced_at.as_ref())?;
    let store = store(&pg)?;
    let tenant = tenant_of(read_tenant, store.tenant_id())?;

    // 既存と同じなら書かない (月タブを行き来するたびに書き込まないため)
    let existing = sqlx::query(SELECT_RANGE_SQL)
        .bind(tenant)
        .bind(&valid.comp_id)
        .bind(&valid.restraint_source)
        .bind(valid.ym)
        .bind(add_months(valid.ym, 1))
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;
    let fetched: Vec<FetchedRow> = existing.iter().map(to_fetched).collect();
    // timecard_kosoku も比べる。ここに入れないと、**土台の取得可否だけが変わった
    // 保存が `skipped_unchanged` で捨てられる** (この issue と同じ「送っているのに
    // 残らない」型の穴になる)
    let same_versions = fetched.first().is_some_and(|f| {
        f.masters == valid.masters
            && f.timecard_kosoku == valid.timecard_kosoku
            && f.wage_logic_version.as_deref() == Some(&valid.wage_logic_version)
    });
    let prev_rows: Vec<WageSnapshotRow> = fetched.iter().map(|f| f.row.clone()).collect();
    if same_versions && rows_equal(&prev_rows, &valid.rows) {
        return Ok(Json(serde_json::json!({
            "saved": prev_rows.len(),
            "skipped_unchanged": true,
            "computed_at": fetched.first().and_then(|f| f.computed_at.clone()),
            "timecard_kosoku": fetched.first().and_then(|f| f.timecard_kosoku.clone()),
        })));
    }

    let saved = write_month(store, tenant, &valid, synced_at).await?;
    tracing::info!(saved, month = %valid.ym, "wage snapshot saved");
    Ok(Json(serde_json::json!({
        "saved": saved,
        "skipped_unchanged": false,
        "timecard_kosoku": valid.timecard_kosoku,
    })))
}

/// DELETE → INSERT を 1 トランザクションで。戻り値は入れた行数。
async fn write_month(
    store: &KintaiPgStore,
    tenant: uuid::Uuid,
    valid: &ValidSnapshot,
    synced_at: Option<DateTime<Utc>>,
) -> Result<usize, (StatusCode, String)> {
    let rows = &valid.rows;
    let mut tx = store.pool().begin().await.map_err(db_err)?;
    // BYPASSRLS の kintai_writer では不要だが、RLS の効くロールで動かしても
    // 同じ結果になるように必ず名乗る (`kintai_push` と同じ)
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    sqlx::query(DELETE_MONTH_SQL)
        .bind(tenant)
        .bind(&valid.comp_id)
        .bind(valid.ym)
        .bind(&valid.restraint_source)
        .execute(&mut *tx)
        .await
        .map_err(db_err)?;

    if !rows.is_empty() {
        let driver_cd: Vec<i64> = rows.iter().map(|r| r.driver_cd).collect();
        let driver_name: Vec<&str> = rows.iter().map(|r| r.driver_name.as_str()).collect();
        let company: Vec<Option<&str>> = rows.iter().map(|r| r.company.as_deref()).collect();
        let branch_name: Vec<Option<&str>> =
            rows.iter().map(|r| r.branch_name.as_deref()).collect();
        let branch_code: Vec<Option<i32>> = rows.iter().map(|r| r.branch_code).collect();
        let job_name: Vec<Option<&str>> = rows.iter().map(|r| r.job_name.as_deref()).collect();
        let pay_kubun: Vec<Option<i16>> = rows.iter().map(|r| r.pay_kubun).collect();
        let hourly_rate: Vec<Option<i32>> = rows.iter().map(|r| r.hourly_rate).collect();
        let calc_base: Vec<Option<i32>> = rows.iter().map(|r| r.calc_base).collect();
        let calc_overtime: Vec<Option<i32>> = rows.iter().map(|r| r.calc_overtime).collect();
        let calc_total: Vec<Option<i32>> = rows.iter().map(|r| r.calc_total).collect();
        let paid_base: Vec<Option<i32>> = rows.iter().map(|r| r.paid_base).collect();
        let paid_overtime: Vec<Option<i32>> = rows.iter().map(|r| r.paid_overtime).collect();
        let working_minutes: Vec<Option<i32>> = rows.iter().map(|r| r.working_minutes).collect();
        let restraint_missing: Vec<bool> = rows.iter().map(|r| r.restraint_missing).collect();

        sqlx::query(INSERT_ROWS_SQL)
            .bind(tenant)
            .bind(&valid.comp_id)
            .bind(valid.ym)
            .bind(&valid.restraint_source)
            .bind(valid.masters.salary_item_sha.as_deref())
            .bind(synced_at)
            .bind(&valid.wage_logic_version)
            .bind(valid.timecard_kosoku.as_deref())
            .bind(&driver_cd)
            .bind(&driver_name)
            .bind(&company)
            .bind(&branch_name)
            .bind(&branch_code)
            .bind(&job_name)
            .bind(&pay_kubun)
            .bind(&hourly_rate)
            .bind(&calc_base)
            .bind(&calc_overtime)
            .bind(&calc_total)
            .bind(&paid_base)
            .bind(&paid_overtime)
            .bind(&working_minutes)
            .bind(&restraint_missing)
            .execute(&mut *tx)
            .await
            .map_err(db_err)?;
    }
    tx.commit().await.map_err(db_err)?;
    Ok(rows.len())
}

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

/// GET /api/kintai/wage-range — 期間の月別 + 合計 + カバレッジ。
pub async fn wage_range(
    Query(q): Query<RangeQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let comp = q.comp.as_deref().unwrap_or("").trim().to_string();
    if comp.is_empty() {
        return Err(bad_request("comp は必須です"));
    }
    let source = q.source.as_deref().unwrap_or("gcp").to_string();
    if !crate::wage_snapshot::RESTRAINT_SOURCES.contains(&source.as_str()) {
        return Err(bad_request("source は gcp / current のいずれかです"));
    }
    let (from, to) = match (&q.from, &q.to) {
        (Some(f), Some(t)) => (f.clone(), t.clone()),
        _ => return Err(bad_request("from / to は YYYY-MM で指定してください")),
    };
    let months = resolve_months(&from, &to).map_err(bad_request)?;
    let store = store(&pg)?;
    let tenant = tenant_of(read_tenant, store.tenant_id())?;

    let lo = months[0];
    let hi = add_months(*months.last().expect("months is not empty"), 1);
    let fetched = sqlx::query(SELECT_RANGE_SQL)
        .bind(tenant)
        .bind(&comp)
        .bind(&source)
        .bind(lo)
        .bind(hi)
        .fetch_all(store.pool())
        .await
        .map_err(db_err)?;

    let buckets = to_buckets(&months, fetched.iter().map(to_fetched));
    let current = CurrentVersions {
        salary_item_sha: q.salary_item_sha.clone(),
        wage_logic_version: q.wage_logic_version.clone(),
        // 画面が送ってくる時刻も保存側と同じ正規化を通す (表記揺れで常に stale に
        // なるのを防ぐ)。形が違う時は判定材料にしない
        payroll_synced_at: q.payroll_synced_at.as_deref().and_then(normalize_ts),
    };
    let agg = aggregate_range(&months, &buckets, &current);

    tracing::info!(
        months = months.len(),
        rows = agg.rows.len(),
        "wage range read"
    );
    Ok(Json(serde_json::json!({
        "from": from,
        "to": to,
        "restraint_source": source,
        "months": agg.months,
        "rows": agg.rows,
    })))
}

/// 引いた行を**期間の全月**の並びに詰め直す。行が 1 つも無い月は `None`
/// (= 未保存) のまま残す — 「応答に無い = 0」を作らないため。
fn to_buckets(
    months: &[NaiveDate],
    fetched: impl Iterator<Item = FetchedRow>,
) -> Vec<Option<MonthBucket>> {
    let mut by_month: std::collections::HashMap<String, MonthBucket> =
        std::collections::HashMap::new();
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
        .map(|m| by_month.remove(&crate::wage_snapshot::ym_label(*m)))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wage_snapshot::ym_label;

    fn ym(y: i32, m: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, 1).unwrap()
    }

    fn fetched(ym: &str, driver_cd: i64) -> FetchedRow {
        FetchedRow {
            ym: ym.to_string(),
            row: WageSnapshotRow {
                driver_cd,
                driver_name: "山田".to_string(),
                company: None,
                branch_name: None,
                branch_code: None,
                job_name: None,
                pay_kubun: None,
                hourly_rate: None,
                calc_base: Some(1),
                calc_overtime: Some(2),
                calc_total: Some(3),
                paid_base: Some(4),
                paid_overtime: Some(5),
                working_minutes: Some(6),
                restraint_missing: false,
            },
            masters: MonthMasters {
                payroll_synced_at: Some("2026-02-03T09:12:00+00:00".to_string()),
                ..Default::default()
            },
            timecard_kosoku: Some("no".to_string()),
            wage_logic_version: Some("wage-1".to_string()),
            computed_at: Some("2026-08-05T01:20:00+00:00".to_string()),
        }
    }

    #[test]
    fn buckets_keep_every_month_of_the_range() {
        let months = vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)];
        let rows = vec![
            fetched("2026-01", 1),
            fetched("2026-01", 2),
            fetched("2026-03", 3),
        ];
        let buckets = to_buckets(&months, rows.into_iter());

        assert_eq!(buckets.len(), 3);
        assert_eq!(buckets[0].as_ref().unwrap().rows.len(), 2);
        assert!(buckets[1].is_none(), "行の無い月は未保存のまま残す");
        assert_eq!(buckets[2].as_ref().unwrap().rows.len(), 1);
        assert_eq!(
            buckets[0].as_ref().unwrap().wage_logic_version.as_deref(),
            Some("wage-1")
        );
        assert_eq!(
            buckets[0].as_ref().unwrap().timecard_kosoku.as_deref(),
            Some("no"),
            "土台の取得可否は月の属性として bucket に運ぶ"
        );
        assert_eq!(ym_label(months[0]), "2026-01");
    }

    #[test]
    fn parse_synced_at_accepts_rfc3339_and_rejects_garbage() {
        assert!(parse_synced_at(None).unwrap().is_none());
        assert!(parse_synced_at(Some(&"2026-02-03T09:12:00Z".to_string()))
            .unwrap()
            .is_some());
        let err = parse_synced_at(Some(&"2026/02/03".to_string())).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn tenant_prefers_read_pin_then_store_pin() {
        let read = uuid::Uuid::from_u128(1);
        let pin = uuid::Uuid::from_u128(2);
        assert_eq!(tenant_of(ReadTenant(Some(read)), pin).unwrap(), read);
        assert_eq!(tenant_of(ReadTenant(None), pin).unwrap(), pin);
        assert_eq!(
            tenant_of(ReadTenant(Some(uuid::Uuid::nil())), pin).unwrap(),
            pin
        );
        let err = tenant_of(ReadTenant(None), uuid::Uuid::nil()).unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn store_is_unavailable_without_kintai_push() {
        let err = store(&None).unwrap_err();
        assert_eq!(err.0, StatusCode::SERVICE_UNAVAILABLE);
    }
}
