//! `kintai_logic::change_log` の書き込み側 (変更履歴の組み立て・bind の束) の単体テスト。元は root の
//! `src/change_log.rs` の単体テスト (Refs #322 で移した)。

use std::collections::BTreeMap;

use chrono::{NaiveDate, NaiveDateTime};
use kintai_kosoku::kintai_push::{DriverPlan, PushEvent, DATETIME_FORMAT};
use kintai_logic::change_log::{build_changes, change_columns, events_json, old_event};

fn ev(driver: i64, at: &str, state: &str) -> PushEvent {
    PushEvent {
        driver_cd: driver,
        occurred_at: NaiveDateTime::parse_from_str(at, DATETIME_FORMAT).unwrap(),
        state: state.to_string(),
        source: "timecard".to_string(),
        unko_no: None,
        raw: serde_json::Value::Null,
    }
}

fn d(s: &str) -> NaiveDate {
    NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

fn plan(changed: &[(&str, Vec<PushEvent>)], deleted: &[&str]) -> DriverPlan {
    DriverPlan {
        changed: changed.iter().map(|(k, v)| (d(k), v.clone())).collect(),
        deleted: deleted.iter().copied().map(d).collect(),
    }
}

#[test]
fn a_corrected_punch_records_before_and_after() {
    let old = vec![ev(1194, "2026-02-06 08:00:00", "始業")];
    let new = vec![ev(1194, "2026-02-06 07:30:00", "始業")];
    let plans = BTreeMap::from([(1194, plan(&[("2026-02-06", new)], &[]))]);
    let got = build_changes(&old, &plans);
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].driver_cd, 1194);
    assert_eq!(got[0].date, d("2026-02-06"));
    let before = got[0].before.as_ref().unwrap();
    assert_eq!(before[0]["occurred_at"], "2026-02-06 08:00:00");
    assert_eq!(before[0]["state"], "始業");
    assert_eq!(before[0]["source"], "timecard");
    assert_eq!(before[0]["unko_no"], serde_json::Value::Null);
    assert_eq!(
        got[0].after.as_ref().unwrap()[0]["occurred_at"],
        "2026-02-06 07:30:00"
    );
}

#[test]
fn a_first_import_is_not_a_change() {
    let new = vec![ev(1194, "2026-02-06 08:00:00", "始業")];
    let plans = BTreeMap::from([(1194, plan(&[("2026-02-06", new)], &["2026-02-07"]))]);
    assert!(build_changes(&[], &plans).is_empty());
}

#[test]
fn the_same_events_in_another_order_are_not_a_change() {
    let a = ev(1, "2026-02-06 08:00:00", "始業");
    let b = ev(1, "2026-02-06 17:00:00", "終業");
    let plans = BTreeMap::from([(1, plan(&[("2026-02-06", vec![b.clone(), a.clone()])], &[]))]);
    assert!(build_changes(&[a, b], &plans).is_empty());
}

#[test]
fn a_deleted_day_records_a_null_after() {
    let old = vec![ev(7, "2026-02-06 08:00:00", "始業")];
    let plans = BTreeMap::from([(7, plan(&[], &["2026-02-06"]))]);
    let got = build_changes(&old, &plans);
    assert_eq!(got.len(), 1);
    assert!(got[0].before.is_some());
    assert_eq!(got[0].after, None);
}

#[test]
fn an_emptied_changed_day_also_has_a_null_after() {
    let old = vec![ev(7, "2026-02-06 08:00:00", "始業")];
    let plans = BTreeMap::from([(7, plan(&[("2026-02-06", vec![])], &[]))]);
    assert_eq!(build_changes(&old, &plans)[0].after, None);
}

#[test]
fn other_drivers_old_events_are_not_mixed_in() {
    let old = vec![ev(2, "2026-02-06 08:00:00", "始業")];
    let new = vec![ev(1, "2026-02-06 07:30:00", "始業")];
    let plans = BTreeMap::from([(1, plan(&[("2026-02-06", new)], &[]))]);
    assert!(build_changes(&old, &plans).is_empty());
}

#[test]
fn events_json_is_sorted_like_the_signature() {
    let late = ev(1, "2026-02-06 17:00:00", "終業");
    let early = ev(1, "2026-02-06 08:00:00", "始業");
    let got = events_json(&[late, early]);
    assert_eq!(got[0]["state"], "始業");
    assert_eq!(got[1]["state"], "終業");
}

#[test]
fn old_rows_become_events_without_raw() {
    let e = old_event(
        7,
        NaiveDateTime::parse_from_str("2026-02-06 08:00:00", DATETIME_FORMAT).unwrap(),
        "始業".to_string(),
        "timecard".to_string(),
        Some("1".to_string()),
    );
    assert_eq!(e.raw, serde_json::Value::Null);
    assert_eq!(e.date(), d("2026-02-06"));
    assert_eq!(e.unko_no.as_deref(), Some("1"));
}

#[test]
fn the_bind_columns_follow_the_changes() {
    let old = vec![
        ev(7, "2026-02-06 08:00:00", "始業"),
        ev(8, "2026-02-07 08:00:00", "始業"),
    ];
    let plans = BTreeMap::from([
        (7, plan(&[], &["2026-02-06"])),
        (
            8,
            plan(
                &[("2026-02-07", vec![ev(8, "2026-02-07 07:00:00", "始業")])],
                &[],
            ),
        ),
    ]);
    let cols = change_columns(&build_changes(&old, &plans));
    assert_eq!(cols.driver_cd, vec![7, 8]);
    assert_eq!(cols.date, vec![d("2026-02-06"), d("2026-02-07")]);
    assert_eq!(cols.after[0], None);
    assert!(cols.after[1].is_some());
    assert_eq!(change_columns(&[]).driver_cd, Vec::<i64>::new());
}
