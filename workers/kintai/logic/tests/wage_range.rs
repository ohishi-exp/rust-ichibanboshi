//! `kintai_logic::wage_range` の単体テスト。元 (root の `src/routes/wage_snapshot.rs`) の route の 4 本のうち、
//! `buckets_keep_every_month_of_the_range` はここに写した。テナント・store の 2 本は `tests/common.rs` に畳んだ。
//! `parse_synced_at` の 1 本は保存 (`put_wage_snapshot`) 側の関数なので写していない (保存は移さない)。

use chrono::{NaiveDate, TimeZone, Utc};
use kintai_logic::common::Fail;
use kintai_logic::wage_range::{
    parse, respond, to_buckets, to_fetched, Binds, FetchedRow, RangeRow,
};
use kintai_logic::wage_snapshot::{ym_label, MonthMasters, WageSnapshotRow};
use postgres_types::Type;
use uuid::Uuid;

fn ym(y: i32, m: u32) -> NaiveDate {
    NaiveDate::from_ymd_opt(y, m, 1).unwrap()
}

fn wage_row(driver_cd: i64) -> WageSnapshotRow {
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

fn fetched(ym: &str, driver_cd: i64) -> FetchedRow {
    FetchedRow {
        ym: ym.to_string(),
        row: wage_row(driver_cd),
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

/// 元の handler の 400 の条件と文言 (comp → source → from/to → resolve_months の順)。
#[test]
fn input_is_checked_in_the_original_order() {
    for (q, want) in [
        ("from=2026-01&to=2026-02", "comp は必須です"),
        ("comp=%20%20&from=2026-01&to=2026-02", "comp は必須です"),
        (
            "comp=c&source=supabase&from=2026-01&to=2026-02",
            "source は gcp / current のいずれかです",
        ),
        (
            "comp=c&from=2026-01",
            "from / to は YYYY-MM で指定してください",
        ),
        (
            "comp=c&to=2026-01",
            "from / to は YYYY-MM で指定してください",
        ),
        (
            "comp=c&from=2026-1&to=2026-02",
            "from は YYYY-MM で指定してください",
        ),
        (
            "comp=c&from=2026-03&to=2026-02",
            "from は to 以前にしてください",
        ),
        ("comp=c&from=2024-01&to=2026-02", "月範囲 上限24"),
    ] {
        assert_eq!(parse(q).unwrap_err(), Fail::new(400, want), "{q}");
    }
}

#[test]
fn defaults_and_current_versions() {
    let req = parse("comp=%20c1%20&from=2026-01&to=2026-03").unwrap();
    assert_eq!(req.comp, "c1", "comp は前後の空白を落とす");
    assert_eq!(req.source, "gcp", "source の既定は gcp");
    assert_eq!(req.months, vec![ym(2026, 1), ym(2026, 2), ym(2026, 3)]);
    assert!(req.current.is_empty());

    let req = parse(concat!(
        "comp=c&from=2026-01&to=2026-01&source=current&salary_item_sha=s&wage_logic_version=w",
        "&payroll_synced_at=2026-02-03T18:12:00%2B09:00"
    ))
    .unwrap();
    assert_eq!(req.source, "current");
    assert_eq!(req.current.salary_item_sha.as_deref(), Some("s"));
    assert_eq!(req.current.wage_logic_version.as_deref(), Some("w"));
    // 保存側と同じ正規化 (UTC の +00:00 形)
    assert_eq!(
        req.current.payroll_synced_at.as_deref(),
        Some("2026-02-03T09:12:00+00:00")
    );
    // 形が違う時刻は判定材料にしない (400 にはしない)
    let req = parse("comp=c&from=2026-01&to=2026-01&payroll_synced_at=nope").unwrap();
    assert_eq!(req.current.payroll_synced_at, None);
}

#[test]
fn binds_cover_the_whole_range() {
    let b = Binds::new(
        Uuid::from_u128(1),
        &parse("comp=c&from=2026-11&to=2027-01").unwrap(),
    );
    assert_eq!((b.lo, b.hi), (ym(2026, 11), ym(2027, 2)));
    assert_eq!((b.comp.as_str(), b.source.as_str()), ("c", "gcp"));
    let types: Vec<Type> = b.params().into_iter().map(|(_, t)| t).collect();
    assert_eq!(
        types,
        vec![Type::UUID, Type::TEXT, Type::TEXT, Type::DATE, Type::DATE]
    );
}

/// 元の `to_fetched`: `TIMESTAMPTZ` は `to_rfc3339()` (`+00:00` 形) にする。
#[test]
fn fetched_rows_carry_rfc3339_times() {
    let raw = RangeRow {
        ym: "2026-01".into(),
        row: wage_row(1),
        salary_item_sha: Some("item".into()),
        payroll_synced_at: Some(Utc.with_ymd_and_hms(2026, 2, 3, 9, 12, 0).unwrap()),
        wage_logic_version: Some("wage-1".into()),
        timecard_kosoku: None,
        computed_at: Some(Utc.with_ymd_and_hms(2026, 8, 5, 1, 20, 0).unwrap()),
    };
    let f = to_fetched(raw.clone());
    assert_eq!(f.masters.salary_item_sha.as_deref(), Some("item"));
    assert_eq!(
        f.masters.payroll_synced_at.as_deref(),
        Some("2026-02-03T09:12:00+00:00")
    );
    assert_eq!(f.computed_at.as_deref(), Some("2026-08-05T01:20:00+00:00"));
    let none = to_fetched(RangeRow {
        payroll_synced_at: None,
        computed_at: None,
        ..raw
    });
    assert_eq!(
        (none.masters.payroll_synced_at, none.computed_at),
        (None, None)
    );
}

/// 応答の形 `{"from","to","restraint_source","months","rows"}` (中身は `aggregate_range` の結果そのまま)。
#[test]
fn response_shape_matches_the_original() {
    let req = parse("comp=c&from=2026-01&to=2026-02").unwrap();
    let rows = vec![RangeRow {
        ym: "2026-01".into(),
        row: wage_row(1035),
        salary_item_sha: None,
        payroll_synced_at: None,
        wage_logic_version: Some("wage-1".into()),
        timecard_kosoku: Some("yes".into()),
        computed_at: Some(Utc.with_ymd_and_hms(2026, 8, 5, 1, 20, 0).unwrap()),
    }];
    assert_eq!(
        serde_json::to_string(&respond(&req, rows)).unwrap(),
        concat!(
            r#"{"from":"2026-01","months":[{"computed_at":"2026-08-05T01:20:00+00:00","drivers":1,"saved":true,"#,
            r#""timecard_kosoku":"yes","ym":"2026-01"},{"drivers":0,"saved":false,"ym":"2026-02"}],"#,
            r#""restraint_source":"gcp","rows":[{"branch_code":null,"branch_name":null,"by_month":{"2026-01":"#,
            r#"{"calc_base":1,"calc_overtime":2,"calc_total":3,"paid_base":4,"paid_overtime":5,"working_minutes":6}},"#,
            r#""calc_base":1,"calc_overtime":2,"calc_total":3,"company":null,"driver_cd":1035,"driver_name":"山田","#,
            r#""job_name":null,"months_counted":1,"months_missing":[],"paid_base":4,"paid_overtime":5,"pay_kubun":null,"#,
            r#""working_minutes":6}],"to":"2026-02"}"#
        )
    );
}
