//! `kintai_logic::rest_overrides` (休息の開始の補正、migration 011) の検査 (Refs ohishi-exp/rust-ichibanboshi#322)。

use chrono::NaiveDateTime;
use kintai_logic::rest_overrides::{
    apply_rest_start_overrides, months_to_reopen, override_from_row, override_json,
    parse_list_query, parse_put_body, RestStartOverride, LIST_SQL, MAX_ITEMS, REOPEN_GATE_SQL,
    SOURCE_NET780, UPSERT_SQL,
};
use serde_json::{json, Value};

const JSON: Option<&str> = Some("application/json");
const UNKO: &str = "2607150330460000000233";

fn t(s: &str) -> NaiveDateTime {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
}

fn item(driver: i64, rest: &str, last: &str, unko: &str) -> Value {
    json!({"driver_cd": driver, "rest_start_at": rest, "last_move_at": last, "unko_no": unko})
}

fn put(items: Vec<Value>) -> Result<Vec<RestStartOverride>, (u16, String)> {
    let body = json!({ "items": items }).to_string();
    parse_put_body(JSON, body.as_bytes()).map_err(|f| (f.status, f.body))
}

fn ov(driver: i64, rest: &str, last: &str) -> RestStartOverride {
    RestStartOverride {
        driver_cd: driver,
        rest_start: t(rest),
        last_move: t(last),
        unko_no: UNKO.into(),
    }
}

/// 北海 7/17 乗務員 11 の実測 (最後の動き 16:45:46 / 休息 16:46:09) がそのまま通る。
#[test]
fn put_body_reads_items() {
    let got = put(vec![item(
        11,
        "2026-07-17 16:46:09",
        "2026-07-17 16:45:46",
        UNKO,
    )])
    .unwrap();
    assert_eq!(
        got,
        vec![ov(11, "2026-07-17 16:46:09", "2026-07-17 16:45:46")]
    );
}

/// 件数・乗務員CD・時刻の書式・前後と 1 時間・運行NO の桁は 400。
#[test]
fn put_body_rejects_bad_items() {
    let r = "2026-07-17 16:46:09";
    let cases = [
        (vec![], "items は 1〜200 件にしてください"),
        (
            vec![item(0, r, "2026-07-17 16:45:46", UNKO)],
            "driver_cd が不正です: 0",
        ),
        (
            vec![item(11, "2026/07/17 16:46:09", "2026-07-17 16:45:46", UNKO)],
            "rest_start_at は YYYY-MM-DD HH:MM:SS で指定してください: 2026/07/17 16:46:09",
        ),
        (
            vec![item(11, r, "16:45:46", UNKO)],
            "last_move_at は YYYY-MM-DD HH:MM:SS で指定してください: 16:45:46",
        ),
        (
            vec![item(11, r, r, UNKO)],
            "last_move_at は rest_start_at より前の 1 時間以内にしてください: 2026-07-17 16:46:09",
        ),
        (
            vec![item(11, r, "2026-07-17 15:46:08", UNKO)],
            "last_move_at は rest_start_at より前の 1 時間以内にしてください: 2026-07-17 16:46:09",
        ),
        (
            vec![item(
                11,
                r,
                "2026-07-17 16:45:46",
                "26071503304600000002331",
            )],
            "unko_no は 22 桁の数字です: 26071503304600000002331",
        ),
        (
            vec![item(11, r, "2026-07-17 16:45:46", "260715033046000000023x")],
            "unko_no は 22 桁の数字です: 260715033046000000023x",
        ),
    ];
    for (items, msg) in cases {
        assert_eq!(put(items), Err((400, msg.to_string())), "{msg}");
    }
    let many = vec![item(11, r, "2026-07-17 16:45:46", UNKO); MAX_ITEMS + 1];
    assert_eq!(
        put(many),
        Err((400, "items は 1〜200 件にしてください".into()))
    );
    // ちょうど 1 時間前は通る
    assert!(put(vec![item(11, r, "2026-07-17 15:46:09", UNKO)]).is_ok());
}

/// GET は `[from, to+1)`。日付の書式と前後は 400。
#[test]
fn list_query_reads_the_range() {
    let got = parse_list_query("from=2026-07-01&to=2026-07-31").unwrap();
    assert_eq!(
        (got.from, got.to),
        (t("2026-07-01 00:00:00"), t("2026-08-01 00:00:00"))
    );
    let bad = |q: &str| {
        parse_list_query(q)
            .map(|_| ())
            .map_err(|f| (f.status, f.body))
    };
    assert_eq!(
        bad("from=2026/07/01&to=2026-07-31"),
        Err((400, "from は YYYY-MM-DD で指定してください".into()))
    );
    assert_eq!(
        bad("from=2026-07-01&to=x"),
        Err((400, "to は YYYY-MM-DD で指定してください".into()))
    );
    assert_eq!(
        bad("from=2026-07-02&to=2026-07-01"),
        Err((400, "from は to 以前にしてください".into()))
    );
}

/// 行の読みと JSON。時刻が読めない行は捨てる。
#[test]
fn rows_and_json() {
    let o = override_from_row(
        11,
        "2026-07-17 16:46:09",
        "2026-07-17 16:45:46",
        UNKO.into(),
    )
    .unwrap();
    assert_eq!(o, ov(11, "2026-07-17 16:46:09", "2026-07-17 16:45:46"));
    assert!(override_from_row(11, "x", "2026-07-17 16:45:46", UNKO.into()).is_none());
    assert!(override_from_row(11, "2026-07-17 16:46:09", "x", UNKO.into()).is_none());
    assert_eq!(
        override_json(&o),
        json!({"driver_cd": 11, "rest_start_at": "2026-07-17 16:46:09", "last_move_at": "2026-07-17 16:45:46", "unko_no": UNKO})
    );
    assert!(LIST_SQL.contains("kintai.rest_start_overrides"));
    assert!(UPSERT_SQL.contains("ON CONFLICT (tenant_id, driver_cd, rest_start_at)"));
    assert_eq!(SOURCE_NET780, "net780");
}

fn row(driver: i64, at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": driver, "source": "dtako_events", "state": state})
}

/// 乗務員と開始が一致する `休息` の行だけを差し替える (他の状態・他の乗務員・秒違いはそのまま)。
#[test]
fn apply_replaces_only_matching_rest_rows() {
    let mut rows = vec![
        row(11, "2026-07-17 16:46:09", "休息"),
        row(11, "2026-07-17 16:46:09", "運転"),
        row(12, "2026-07-17 16:46:09", "休息"),
        row(11, "2026-07-17 16:46:10", "休息"),
    ];
    let n = apply_rest_start_overrides(
        &mut rows,
        &[ov(11, "2026-07-17 16:46:09", "2026-07-17 16:45:46")],
    );
    assert_eq!(n, 1);
    let starts: Vec<&str> = rows
        .iter()
        .map(|r| r["datetime"].as_str().unwrap())
        .collect();
    assert_eq!(
        starts,
        [
            "2026-07-17 16:45:46",
            "2026-07-17 16:46:09",
            "2026-07-17 16:46:09",
            "2026-07-17 16:46:10"
        ]
    );
    assert_eq!(apply_rest_start_overrides(&mut rows, &[]), 0);
}

/// 月ゲートを開け直す月: 休息の開始の月と前の月 (重複なし・昇順)。年をまたぐ月も。
#[test]
fn months_to_reopen_are_the_rest_month_and_the_one_before() {
    let items = [
        ov(9, "2026-10-11 08:39:42", "2026-10-11 08:38:54"),
        ov(6, "2026-10-01 01:00:00", "2026-10-01 00:59:00"),
        ov(11, "2026-01-05 10:00:00", "2026-01-05 09:59:00"),
    ];
    assert_eq!(
        months_to_reopen(&items),
        ["2025-12", "2026-01", "2026-09", "2026-10"]
    );
    assert!(months_to_reopen(&[]).is_empty());
    assert!(REOPEN_GATE_SQL.contains("DELETE FROM kintai.fold_gate"));
}
