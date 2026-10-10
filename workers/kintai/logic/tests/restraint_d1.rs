//! 拘束サマリ (restraint) の D1 用の純粋部分の単体テスト (Refs ohishi-exp/rust-ichibanboshi#322)。
//! 束を native の SQLite で流してオンプレ版 (rusqlite) と比べるのは `workers/kintai/pg/tests/restraint_d1_parity.rs`。

use kintai_logic::common::parse_query;
use kintai_logic::restraint::*;
use kintai_logic::restraint_d1::*;
use serde_json::{json, Value};

fn valid(n: usize) -> ValidPush {
    let entries = (0..n)
        .map(|i| RestraintEntry {
            driver_cd: format!("{i}"),
            no_data: false,
            summary_json: Some("{}".into()),
            fetched_at: None,
            last_verified_at: None,
        })
        .collect();
    ValidPush {
        comp_id: "27324455".into(),
        source: "theearth".into(),
        month: "2026-06".into(),
        entries,
    }
}

fn wage_req(month: &str) -> WageSourceRequest {
    let q: WageSourceQuery = parse_query(&format!("comp=27324455&month={month}")).unwrap();
    parse_wage_source(q).unwrap()
}

#[test]
fn failures_are_fixed_texts() {
    let f = no_d1();
    assert_eq!(f.status, 503);
    assert!(f.body.contains("KINTAI_RESTRAINT_DB"));
    assert!(!f.body.contains("sqlite_path"));
    let f = d1_fail("batch");
    assert_eq!(
        (f.status, f.body.as_str()),
        (502, "拘束サマリの D1 の読み書きに失敗しました: batch")
    );
    assert_eq!(D1_BINDING, "KINTAI_RESTRAINT_DB");
}

#[test]
fn synced_at_from_millis_matches_the_on_prem_format() {
    assert_eq!(
        synced_at_from_millis(1_791_594_123_456),
        "2026-10-10T01:02:03.456000000+00:00"
    );
    assert_eq!(
        synced_at_from_millis(i64::MAX),
        "1970-01-01T00:00:00.000000000+00:00"
    );
}

#[test]
fn push_size_is_capped() {
    assert!(check_push_size(&valid(MAX_PUSH_ENTRIES)).is_ok());
    let f = check_push_size(&valid(MAX_PUSH_ENTRIES + 1)).unwrap_err();
    assert_eq!(f.status, 400);
    assert!(f.body.contains("500 件まで"), "{}", f.body);
    // 1 batch = entries + 1 文。Workers Paid の 1 invocation 1000 クエリに収まる
    const { assert!(MAX_PUSH_ENTRIES < 1000) };
}

#[test]
fn push_statements_upsert_each_entry_then_sync_state() {
    let v = valid(2);
    let stmts = push_statements(&v, "t0");
    assert_eq!(stmts.len(), 3);
    assert_eq!(stmts[0].sql, UPSERT_SUMMARY_SQL);
    assert_eq!(
        stmts[0].binds,
        summary_binds("27324455", "theearth", "2026-06", &v.entries[0]).to_vec()
    );
    assert_eq!(
        stmts[1].binds,
        summary_binds("27324455", "theearth", "2026-06", &v.entries[1]).to_vec()
    );
    assert_eq!(stmts[2].sql, UPSERT_SYNC_STATE_SQL);
    assert_eq!(
        stmts[2].binds,
        sync_state_binds("27324455", "theearth", "2026-06", "t0").to_vec()
    );
    // 空の push でも sync_state は書く (row_count 0 の行ができる。オンプレ版と同じ)
    assert_eq!(push_statements(&valid(0), "t0").len(), 1);
    // bind は 1 文 100 個までの D1 の上限より十分少ない
    assert!(stmts.iter().all(|s| s.binds.len() <= 100));
}

#[test]
fn wage_source_statements_are_eight_in_read_order() {
    let req = wage_req("2026-01");
    let stmts = wage_source_statements(&req);
    assert_eq!(stmts.len(), 8);
    let reads = req.reads();
    for (i, (source, ym)) in reads.iter().enumerate() {
        assert_eq!(stmts[2 * i].sql, SYNCED_AT_SQL);
        assert_eq!(
            stmts[2 * i].binds,
            synced_at_binds("27324455", source, ym).to_vec()
        );
        assert_eq!(stmts[2 * i + 1].sql, MONTH_ROWS_SQL);
        assert_eq!(
            stmts[2 * i + 1].binds,
            month_rows_binds("27324455", source, ym).to_vec()
        );
    }
    let s = synced_statement("a_b");
    assert_eq!((s.sql, s.binds), (SYNCED_SQL, synced_binds("a_b").to_vec()));
}

fn row(cd: &str, no_data: Value, json: Value) -> Value {
    json!({"driver_cd": cd, "no_data": no_data, "summary_json": json, "fetched_at": "f", "last_verified_at": null})
}

#[test]
fn wage_source_from_results_builds_the_response() {
    let results = vec![
        vec![json!({"synced_at": "t1"})],
        vec![
            row("100", json!(0), json!(r#"{"a":1}"#)),
            row("150", json!(0.0), json!("broken")),
            row("200", json!(1.0), Value::Null),
        ],
        vec![],
        vec![],
        vec![],
        vec![],
        vec![json!({"synced_at": "t2"})],
        vec![row("300", json!(1), Value::Null)],
    ];
    let (res, broken) = wage_source_from_results(wage_req("2026-06"), results).unwrap();
    assert_eq!(
        serde_json::to_value(&res).unwrap(),
        json!({
            "comp_id": "27324455", "month": "2026-06", "prev_month": "2026-05",
            "current_theearth": {"summaries": [{"driver_cd": "100", "summary": {"a": 1}, "fetched_at": "f", "last_verified_at": null}],
                                 "no_data_drivers": ["200"], "synced_at": "t1"},
            "current_timecard": {"summaries": [], "no_data_drivers": [], "synced_at": null},
            "prev_theearth": {"summaries": [], "no_data_drivers": [], "synced_at": null},
            "prev_timecard": {"summaries": [], "no_data_drivers": ["300"], "synced_at": "t2"},
        })
    );
    assert_eq!(broken.len(), 1);
    assert_eq!(broken[0].driver_cd, "150");
}

#[test]
fn unreadable_results_are_502_rows() {
    let rows_fail = |results: Vec<Vec<Value>>| {
        let f = wage_source_from_results(wage_req("2026-06"), results).unwrap_err();
        assert_eq!(
            (f.status, f.body),
            (
                502,
                "拘束サマリの D1 の読み書きに失敗しました: rows".to_string()
            )
        );
    };
    // 結果の本数が 8 でない
    rows_fail(vec![vec![]; 7]);
    let with = |i: usize, rows: Vec<Value>| {
        let mut r = vec![vec![]; 8];
        r[i] = rows;
        r
    };
    // synced_at が文字列でない・無い
    rows_fail(with(0, vec![json!({"synced_at": 1})]));
    rows_fail(with(2, vec![json!({})]));
    // driver_cd が NULL・no_data が整数でない・no_data が無い・summary_json が数
    rows_fail(with(
        1,
        vec![row("x", json!(0), Value::Null)
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .map(|(k, v)| {
                if k == "driver_cd" {
                    (k, Value::Null)
                } else {
                    (k, v)
                }
            })
            .collect()],
    ));
    rows_fail(with(3, vec![row("x", json!(0.5), Value::Null)]));
    rows_fail(with(5, vec![row("x", json!("0"), Value::Null)]));
    rows_fail(with(7, vec![json!({"driver_cd": "x"})]));
    rows_fail(with(1, vec![row("x", json!(0), json!(3))]));
}

#[test]
fn synced_from_results_splits_scopes() {
    let rows = vec![
        json!({"scope": "a_b:theearth:2026-06", "synced_at": "t1", "row_count": 3.0}),
        json!({"scope": "axb:timecard:2026-06", "synced_at": "t2", "row_count": 1}),
    ];
    let res = synced_from_results("a_b", rows).unwrap();
    assert_eq!(
        serde_json::to_value(&res).unwrap(),
        json!({"entries": [{"source": "theearth", "month": "2026-06", "synced_at": "t1", "row_count": 3}]})
    );
    for bad in [
        json!({"scope": 1, "synced_at": "t", "row_count": 1}),
        json!({"scope": "a_b:x:y", "synced_at": null, "row_count": 1}),
        json!({"scope": "a_b:x:y", "synced_at": "t"}),
    ] {
        let f = synced_from_results("a_b", vec![bad]).unwrap_err();
        assert_eq!(f.status, 502);
    }
}
