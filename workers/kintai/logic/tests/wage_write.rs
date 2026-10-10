//! `kintai_logic::wage_write` (賃金スナップショットの保存) の単体テスト。元は root の `src/routes/wage_snapshot.rs` の
//! `parse_synced_at_accepts_rfc3339_and_rejects_garbage` と、`put_wage_snapshot` の「前回と同じなら書かない」の判定
//! (元は実 DB のテスト `tests/wage_snapshot_pg_test.rs` だけが通していた)。

use chrono::NaiveDate;
use kintai_logic::wage_range::FetchedRow;
use kintai_logic::wage_snapshot::{MonthMasters, ValidSnapshot, WageSnapshotRow};
use kintai_logic::wage_write::{parse_synced_at, saved_response, unchanged_response, wage_columns};

fn row(driver_cd: i64) -> WageSnapshotRow {
    WageSnapshotRow {
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
    }
}

fn valid(rows: Vec<WageSnapshotRow>) -> ValidSnapshot {
    ValidSnapshot {
        comp_id: "c1".to_string(),
        ym: NaiveDate::from_ymd_opt(2026, 2, 1).unwrap(),
        restraint_source: "gcp".to_string(),
        timecard_kosoku: Some("yes".to_string()),
        wage_logic_version: "wage-1".to_string(),
        masters: MonthMasters {
            salary_item_sha: Some("s".to_string()),
            payroll_synced_at: Some("2026-02-03T09:12:00+00:00".to_string()),
        },
        rows,
    }
}

fn stored(v: &ValidSnapshot, r: WageSnapshotRow) -> FetchedRow {
    FetchedRow {
        ym: "2026-02".to_string(),
        row: r,
        masters: v.masters.clone(),
        timecard_kosoku: v.timecard_kosoku.clone(),
        wage_logic_version: Some(v.wage_logic_version.clone()),
        computed_at: Some("2026-08-05T01:20:00+00:00".to_string()),
    }
}

#[test]
fn parse_synced_at_accepts_rfc3339_and_rejects_garbage() {
    assert!(parse_synced_at(None).unwrap().is_none());
    assert!(parse_synced_at(Some(&"2026-02-03T09:12:00Z".to_string()))
        .unwrap()
        .is_some());
    let err = parse_synced_at(Some(&"2026/02/03".to_string())).unwrap_err();
    assert_eq!(err.status, 400);
    assert_eq!(
        err.body,
        "masters.payroll_synced_at は RFC3339 で指定してください"
    );
}

#[test]
fn the_same_month_is_not_written_again() {
    let v = valid(vec![row(2), row(1)]);
    // 並び順が違っても同じ内容
    let fetched = vec![stored(&v, row(1)), stored(&v, row(2))];
    assert_eq!(
        unchanged_response(&fetched, &v).unwrap(),
        serde_json::json!({
            "saved": 2,
            "skipped_unchanged": true,
            "computed_at": "2026-08-05T01:20:00+00:00",
            "timecard_kosoku": "yes",
        })
    );
}

#[test]
fn any_difference_is_written() {
    let v = valid(vec![row(1)]);
    // 初めての月
    assert!(unchanged_response(&[], &v).is_none());
    // 行が違う
    let mut other = row(1);
    other.calc_total = Some(99);
    assert!(unchanged_response(&[stored(&v, other)], &v).is_none());
    // 土台の取得可否だけが違う
    let mut f = stored(&v, row(1));
    f.timecard_kosoku = Some("no".to_string());
    assert!(unchanged_response(&[f], &v).is_none());
    // 版が違う
    let mut f = stored(&v, row(1));
    f.wage_logic_version = None;
    assert!(unchanged_response(&[f], &v).is_none());
    let mut f = stored(&v, row(1));
    f.masters.salary_item_sha = None;
    assert!(unchanged_response(&[f], &v).is_none());
}

#[test]
fn the_saved_response_carries_the_count_and_the_source_state() {
    let v = valid(vec![row(1)]);
    assert_eq!(
        saved_response(1, &v),
        serde_json::json!({"saved": 1, "skipped_unchanged": false, "timecard_kosoku": "yes"})
    );
}

#[test]
fn the_bind_columns_keep_the_row_order() {
    let c = wage_columns(&[row(2), row(1)]);
    assert_eq!(c.driver_cd, vec![2, 1]);
    assert_eq!(c.restraint_missing, vec![false, false]);
    assert!(wage_columns(&[]).driver_cd.is_empty());
}
