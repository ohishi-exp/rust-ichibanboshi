//! 拘束サマリ (restraint) の 3 口の純粋部分の単体テスト (Refs ohishi-exp/rust-ichibanboshi#322)。
//! オンプレ版 (rusqlite) を通した応答の固定は root の `tests/restraint_test.rs`。

use chrono::{TimeZone, Utc};
use kintai_logic::common::{parse_query, Fail};
use kintai_logic::restraint::*;
use serde_json::json;

fn push(comp: &str, source: &str, month: &str, entries: serde_json::Value) -> PushBody {
    serde_json::from_value(
        json!({"comp_id": comp, "source": source, "month": month, "entries": entries}),
    )
    .unwrap()
}

fn bad(f: Fail) -> (u16, String) {
    (f.status, f.body)
}

// ── 検査の部品 ──

#[test]
fn prev_month_handles_year_boundary() {
    assert_eq!(prev_month("2026-06"), "2026-05");
    assert_eq!(prev_month("2026-01"), "2025-12");
    assert_eq!(prev_month("2026-10"), "2026-09");
}

#[test]
fn comp_validation() {
    assert!(is_valid_comp("27324455"));
    assert!(is_valid_comp("comp-1_a"));
    assert!(is_valid_comp(&"x".repeat(64)));
    assert!(!is_valid_comp(""));
    assert!(!is_valid_comp("a/b"));
    assert!(!is_valid_comp("a:b"));
    assert!(!is_valid_comp(&"x".repeat(65)));
}

#[test]
fn synced_at_is_rfc3339_with_nanos_and_offset() {
    let t = Utc.with_ymd_and_hms(2026, 10, 10, 1, 2, 3).unwrap();
    assert_eq!(format_synced_at(t), "2026-10-10T01:02:03.000000000+00:00");
    let t = Utc.timestamp_millis_opt(1_791_594_123_456).unwrap();
    assert!(format_synced_at(t).ends_with(".456000000+00:00"));
}

#[test]
fn scope_joins_with_colons() {
    assert_eq!(
        scope("27324455", "theearth", "2026-06"),
        "27324455:theearth:2026-06"
    );
}

// ── 表の定義と SQL ──

#[test]
fn schema_is_the_migration_file() {
    assert_eq!(
        SCHEMA_SQL,
        include_str!("../../worker/migrations/0001_restraint.sql")
    );
    assert!(SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS restraint_summary ("));
    assert!(SCHEMA_SQL.contains("CREATE TABLE IF NOT EXISTS restraint_sync_state ("));
    assert!(!SCHEMA_SQL.to_lowercase().contains("user_version"));
}

/// SQL の `?N` の最大と bind の数が一致すること (D1 は数が合わないと失敗する)。
fn max_placeholder(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut max = 0;
    for (i, c) in b.iter().enumerate() {
        if *c == b'?' {
            let digits: String = sql[i + 1..]
                .chars()
                .take_while(|c| c.is_ascii_digit())
                .collect();
            max = max.max(digits.parse::<usize>().unwrap());
        }
    }
    max
}

#[test]
fn binds_match_placeholders() {
    let e = RestraintEntry {
        driver_cd: "100".into(),
        no_data: false,
        summary_json: Some("{}".into()),
        fetched_at: None,
        last_verified_at: Some("v".into()),
    };
    assert_eq!(
        max_placeholder(UPSERT_SUMMARY_SQL),
        summary_binds("c", "s", "m", &e).len()
    );
    assert_eq!(
        max_placeholder(UPSERT_SYNC_STATE_SQL),
        sync_state_binds("c", "s", "m", "t").len()
    );
    assert_eq!(
        max_placeholder(SYNCED_AT_SQL),
        synced_at_binds("c", "s", "m").len()
    );
    assert_eq!(
        max_placeholder(MONTH_ROWS_SQL),
        month_rows_binds("c", "s", "m").len()
    );
    assert_eq!(max_placeholder(SYNCED_SQL), synced_binds("c").len());
}

#[test]
fn bind_values_in_order() {
    let e = RestraintEntry {
        driver_cd: "100".into(),
        no_data: true,
        summary_json: None,
        fetched_at: Some("f".into()),
        last_verified_at: None,
    };
    let t = |s: &str| Bind::Text(s.to_string());
    assert_eq!(
        summary_binds("27324455", "theearth", "2026-06", &e),
        [
            t("27324455"),
            t("theearth"),
            t("2026-06"),
            t("100"),
            Bind::Int(1),
            Bind::Null,
            t("f"),
            Bind::Null
        ]
    );
    let e = RestraintEntry {
        no_data: false,
        ..e
    };
    assert_eq!(summary_binds("c", "s", "m", &e)[4], Bind::Int(0));
    assert_eq!(
        sync_state_binds("c", "s", "m", "t0"),
        [t("c:s:m"), t("t0"), t("c"), t("s"), t("m")]
    );
    assert_eq!(synced_at_binds("c", "s", "m"), [t("c:s:m")]);
    assert_eq!(month_rows_binds("c", "s", "m"), [t("c"), t("s"), t("m")]);
    assert_eq!(synced_binds("a_b"), [t("a_b:")]);
}

#[test]
fn synced_rows_drop_other_comps_and_malformed_scopes() {
    let rows = vec![
        ("a_b:theearth:2026-06".to_string(), "t1".to_string(), 3),
        // LIKE 'a_b:%' の '_' に当たった別 comp
        ("axb:timecard:2026-06".to_string(), "t2".to_string(), 1),
        // ':' で 2 つに割れない
        ("a_b:broken".to_string(), "t3".to_string(), 0),
    ];
    assert_eq!(
        synced_rows("a_b", rows),
        vec![RestraintSyncedRow {
            source: "theearth".into(),
            month: "2026-06".into(),
            synced_at: "t1".into(),
            row_count: 3,
        }]
    );
}

// ── PUT ──

#[test]
fn push_is_validated_in_order() {
    let cases = [
        (push("a/b", "venus", "x", json!([])), "comp_id が不正です"),
        (
            push("27324455", "venus", "x", json!([])),
            "source は theearth / timecard のいずれかで指定してください",
        ),
        (
            push("27324455", "theearth", "2026-6", json!([])),
            "month は YYYY-MM で指定してください",
        ),
        (
            push(
                "27324455",
                "theearth",
                "2026-06",
                json!([{"driver_cd": ""}]),
            ),
            "driver_cd が空の entry があります",
        ),
        (
            push(
                "27324455",
                "timecard",
                "2026-06",
                json!([{"driver_cd": "1"}]),
            ),
            "driver_cd=1 は no_data でないのに summary がありません",
        ),
    ];
    for (body, msg) in cases {
        assert_eq!(
            bad(validate_push(body).unwrap_err()),
            (400, msg.to_string())
        );
    }
}

#[test]
fn push_keeps_entries_verbatim() {
    let body = push(
        "27324455",
        "theearth",
        "2026-06",
        json!([
            {"driver_cd": "100", "summary": {"b": 1, "a": [2]}, "fetched_at": "f", "last_verified_at": "v"},
            {"driver_cd": "300", "no_data": true},
        ]),
    );
    let valid = validate_push(body).unwrap();
    assert_eq!(
        valid,
        ValidPush {
            comp_id: "27324455".into(),
            source: "theearth".into(),
            month: "2026-06".into(),
            entries: vec![
                RestraintEntry {
                    driver_cd: "100".into(),
                    no_data: false,
                    summary_json: Some(r#"{"a":[2],"b":1}"#.into()),
                    fetched_at: Some("f".into()),
                    last_verified_at: Some("v".into()),
                },
                RestraintEntry {
                    driver_cd: "300".into(),
                    no_data: true,
                    summary_json: None,
                    fetched_at: None,
                    last_verified_at: None,
                },
            ],
        }
    );
    let res = serde_json::to_value(valid.response("t0".into())).unwrap();
    assert_eq!(res, json!({"saved": 2, "synced_at": "t0"}));
}

#[test]
fn error_body_wraps_the_message() {
    let body = ErrorBody::of(Fail::new(400, "comp が不正です"));
    assert_eq!(
        serde_json::to_value(body).unwrap(),
        json!({"error": "comp が不正です"})
    );
}

// ── synced-months ──

#[test]
fn synced_months_checks_comp_and_lists_rows() {
    let q: SyncedMonthsQuery = parse_query("comp=a%2Fb").unwrap();
    assert_eq!(
        bad(parse_synced_months(q).unwrap_err()),
        (400, "comp が不正です".into())
    );
    let q: SyncedMonthsQuery = parse_query("comp=27324455").unwrap();
    assert_eq!(parse_synced_months(q).unwrap(), "27324455");
    let res = synced_response(vec![RestraintSyncedRow {
        source: "timecard".into(),
        month: "2026-06".into(),
        synced_at: "t".into(),
        row_count: 7,
    }]);
    assert_eq!(
        serde_json::to_string(&res).unwrap(),
        r#"{"entries":[{"source":"timecard","month":"2026-06","synced_at":"t","row_count":7}]}"#
    );
}

// ── wage-source ──

#[test]
fn wage_source_is_validated_in_order() {
    let q: WageSourceQuery = parse_query("comp=&month=junk").unwrap();
    assert_eq!(
        bad(parse_wage_source(q).unwrap_err()),
        (400, "comp が不正です".into())
    );
    let q: WageSourceQuery = parse_query("comp=27324455&month=junk").unwrap();
    assert_eq!(
        bad(parse_wage_source(q).unwrap_err()),
        (400, "month は YYYY-MM で指定してください".into())
    );
}

#[test]
fn wage_source_reads_four_months_and_responds_in_order() {
    let q: WageSourceQuery = parse_query("comp=27324455&month=2026-01").unwrap();
    let req = parse_wage_source(q).unwrap();
    assert_eq!(
        req.reads(),
        [
            ("theearth", "2026-01".to_string()),
            ("timecard", "2026-01".to_string()),
            ("theearth", "2025-12".to_string()),
            ("timecard", "2025-12".to_string()),
        ]
    );
    let m = |cd: &str| WageSourceMonth {
        summaries: vec![],
        no_data_drivers: vec![cd.to_string()],
        synced_at: None,
    };
    let res = req.respond([m("a"), m("b"), m("c"), m("d")]);
    assert_eq!(
        serde_json::to_string(&res).unwrap(),
        concat!(
            r#"{"comp_id":"27324455","month":"2026-01","prev_month":"2025-12","#,
            r#""current_theearth":{"summaries":[],"no_data_drivers":["a"],"synced_at":null},"#,
            r#""current_timecard":{"summaries":[],"no_data_drivers":["b"],"synced_at":null},"#,
            r#""prev_theearth":{"summaries":[],"no_data_drivers":["c"],"synced_at":null},"#,
            r#""prev_timecard":{"summaries":[],"no_data_drivers":["d"],"synced_at":null}}"#
        )
    );
}

#[test]
fn month_source_sorts_rows_into_summaries_no_data_and_broken() {
    let row = |cd: &str, no_data: bool, json: Option<&str>| RestraintEntry {
        driver_cd: cd.into(),
        no_data,
        summary_json: json.map(str::to_string),
        fetched_at: Some(format!("f{cd}")),
        last_verified_at: None,
    };
    let month = RestraintMonth {
        entries: vec![
            row("100", false, Some("not-json")),
            row("150", false, None),
            row("200", false, Some(r#"{"driverCd":"200"}"#)),
            row("300", true, Some(r#"{"ignored":true}"#)),
        ],
        synced_at: Some("t".into()),
    };
    let (out, broken) = month_source(month);
    assert_eq!(
        out,
        WageSourceMonth {
            summaries: vec![WageSourceSummary {
                driver_cd: "200".into(),
                summary: json!({"driverCd": "200"}),
                fetched_at: Some("f200".into()),
                last_verified_at: None,
            }],
            no_data_drivers: vec!["300".into()],
            synced_at: Some("t".into()),
        }
    );
    assert_eq!(broken.len(), 1);
    assert_eq!(broken[0].driver_cd, "100");
    assert!(
        broken[0].error.contains("expected ident"),
        "{}",
        broken[0].error
    );
}

#[test]
fn empty_month_is_empty_with_null_synced_at() {
    let (out, broken) = month_source(RestraintMonth::default());
    assert_eq!(
        serde_json::to_value(out).unwrap(),
        json!({"summaries": [], "no_data_drivers": [], "synced_at": null})
    );
    assert!(broken.is_empty());
}
