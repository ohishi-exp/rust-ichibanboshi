//! `kintai_logic::unko_gaps` の単体テスト。`build_gaps`・`drop_crew_suffix`・`onprem_count_for` の 14 本は root の
//! `src/routes/unko_gaps.rs` から移した (root の handler のテスト 5 本は root に残る)。ほかは Worker の口の検査・
//! etags の RPC の引数と戻りの読み方・応答の JSON (root と同じキー・同じ並び。`elapsed_ms` は root が足す)。

use std::collections::{HashMap, HashSet};

use chrono::{FixedOffset, TimeZone};
use kintai_logic::common::Fail;
use kintai_logic::unko_gaps::{
    alc_rpc_failed, build_gaps, check_month, drop_crew_suffix, etags_search, no_alc_rpc,
    onprem_count_for, parse, read_etags, respond, Binds, Onprem, Request, RpcResult, Window,
    ALC_RPC_BINDING, ETAGS_PATH, MAX_UNKO_GAPS_DRIVERS, MAX_UNKO_GAPS_PER_DRIVER, PUSHED_SOURCES,
};
use postgres_types::Type;
use uuid::Uuid;

// ── 入力の検査 ──────────────────────────────────────────────────────────────

#[test]
fn month_and_optional_driver_cd_are_read() {
    assert_eq!(
        parse("month=2026-06").unwrap(),
        Request {
            month: "2026-06".into(),
            driver_cd: None
        }
    );
    assert_eq!(
        parse("month=2026-06&driver_cd=1445").unwrap(),
        Request {
            month: "2026-06".into(),
            driver_cd: Some(1445)
        }
    );
}

#[test]
fn month_is_required_then_must_be_yyyy_mm() {
    for q in ["", "driver_cd=1"] {
        assert_eq!(
            parse(q).unwrap_err(),
            Fail::new(400, "month は必須です (YYYY-MM)"),
            "{q:?}"
        );
    }
    for q in ["month=", "month=nope", "month=2026-13", "month=2026-6"] {
        assert_eq!(
            parse(q).unwrap_err(),
            Fail::new(400, "month は YYYY-MM で指定してください"),
            "{q:?}"
        );
    }
    assert_eq!(check_month(Some("2026-06".into())), Ok("2026-06".into()));
}

#[test]
fn an_unreadable_query_is_axum_s_rejection() {
    let e = parse("month=2026-06&driver_cd=abc").unwrap_err();
    assert_eq!(e.status, 400);
    assert!(
        e.body.starts_with("Failed to deserialize query string: "),
        "{e:?}"
    );
}

// ── 窓と SQL の引数 ─────────────────────────────────────────────────────────

#[test]
fn the_window_is_month_range_in_jst() {
    let jst = FixedOffset::east_opt(9 * 3600).unwrap();
    let w = Window::of("2026-12").unwrap();
    assert_eq!((w.year, w.month_num), (2026, 12));
    assert_eq!(w.from, jst.with_ymd_and_hms(2026, 12, 1, 0, 0, 0).unwrap());
    assert_eq!(
        w.to,
        jst.with_ymd_and_hms(2027, 1, 2, 0, 0, 0).unwrap(),
        "翌月 2 日 (排他)"
    );
    assert_eq!(Window::of("nope"), None);
    assert_eq!(Window::of("2026-xx"), None);
    assert_eq!(Window::of("2026-13"), None);
}

#[test]
fn binds_put_the_tenant_pin_first_and_type_every_param() {
    let tenant = Uuid::from_u128(7);
    let w = Window::of("2026-06").unwrap();
    let b = Binds::new(tenant, &w);
    assert_eq!((b.tenant, b.from, b.to), (tenant, w.from, w.to));
    assert_eq!(b.sources, PUSHED_SOURCES.to_vec());
    let types: Vec<Type> = b.params().into_iter().map(|(_, t)| t).collect();
    assert_eq!(
        types,
        vec![
            Type::UUID,
            Type::TIMESTAMPTZ,
            Type::TIMESTAMPTZ,
            Type::TEXT_ARRAY
        ]
    );
}

#[test]
fn onprem_rows_count_per_driver_and_drop_the_crew_digit() {
    let o = Onprem::from_rows([
        (1445, "26060610055500000023021"),
        (1445, "26060710055500000023022"),
        (1740, "26060610055500000023022"),
    ]);
    assert_eq!(o.in_month, HashMap::from([(1445, 2), (1740, 1)]));
    assert_eq!(
        o.seen,
        HashSet::from([
            "2606061005550000002302".to_string(),
            "2606071005550000002302".to_string()
        ])
    );
}

// ── etags の RPC ────────────────────────────────────────────────────────────

#[test]
fn the_rpc_argument_is_only_the_etags_window() {
    // root の month_etags_bounds と同じ [月初, 翌月初] (閉区間)。path・method・tenant は auth-worker 側で固定
    assert_eq!(
        etags_search("2026-12").as_deref(),
        Some("date_from=2026-12-01&date_to=2027-01-01")
    );
    assert_eq!(
        etags_search("2026-06").as_deref(),
        Some("date_from=2026-06-01&date_to=2026-07-01")
    );
    assert_eq!(etags_search("nope"), None);
    assert_eq!(ETAGS_PATH, "/api/dtako/events/etags");
    assert_eq!(ALC_RPC_BINDING, "KINTAI_ALC_RPC");
}

fn res(status: u16, body: &str) -> RpcResult {
    RpcResult {
        status,
        body: body.to_string(),
    }
}

#[test]
fn not_found_is_no_etags_endpoint() {
    assert_eq!(read_etags(&res(404, "")), Ok(None));
}

#[test]
fn auth_worker_s_own_refusals_are_named_as_502() {
    let unset = r#"{"error":"kintai_alc_tenant_unset"}"#;
    assert_eq!(
        read_etags(&res(503, unset)),
        Err(Fail::new(
            502,
            format!("alc dtako-etags status 503: {unset}")
        ))
    );
    let bad = r#"{"error":"bad_query"}"#;
    assert_eq!(read_etags(&res(400, bad)).unwrap_err().status, 502);
}

#[test]
fn other_non_2xx_are_502_with_the_status_and_an_excerpt() {
    assert_eq!(
        read_etags(&res(403, r#"{"error":"forbidden"}"#)),
        Err(Fail::new(
            502,
            r#"alc dtako-etags status 403: {"error":"forbidden"}"#
        ))
    );
    let long = "x".repeat(300);
    let e = read_etags(&res(500, &long)).unwrap_err();
    assert_eq!(
        e.body,
        format!("alc dtako-etags status 500: {}", "x".repeat(200))
    );
    assert_eq!(e.status, 502);
    assert_eq!(read_etags(&res(302, "")).unwrap_err().status, 502);
    assert_eq!(read_etags(&res(199, "")).unwrap_err().status, 502);
}

#[test]
fn an_unreadable_body_is_502() {
    let e = read_etags(&res(200, "not json")).unwrap_err();
    assert_eq!(e.status, 502);
    assert!(e.body.starts_with("alc dtako-etags parse: "), "{e:?}");
    // 型の違う既知の欄も root と同じく読めない (warnings は文字列の配列)
    let e = read_etags(&res(200, r#"{"items":[],"warnings":[1]}"#)).unwrap_err();
    assert!(e.body.starts_with("alc dtako-etags parse: "), "{e:?}");
}

#[test]
fn items_are_read_with_root_s_defaults_and_later_duplicates_win() {
    let body = r#"{
        "items": [
            {"unko_no": "A", "etag": "e1", "driver_cds": ["1445"]},
            {"unko_no": "B"},
            {"unko_no": "A", "etag": null, "driver_cds": ["1740"]}
        ],
        "warnings": ["w"],
        "unsplit": [{"unko_no": "C", "driver_cd": "1", "reading_date": "2026-06-01"}],
        "unsplit_total": 1,
        "unknown": true
    }"#;
    let got = read_etags(&res(200, body)).unwrap().unwrap();
    assert_eq!(
        got,
        HashMap::from([
            ("A".to_string(), vec!["1740".to_string()]),
            ("B".to_string(), Vec::new()),
        ])
    );
    assert_eq!(read_etags(&res(204, "{}")), Ok(Some(HashMap::new())));
}

#[test]
fn binding_and_rpc_failures_have_fixed_bodies() {
    assert_eq!(no_alc_rpc().status, 503);
    assert!(no_alc_rpc().body.contains("KINTAI_ALC_RPC"));
    assert_eq!(
        alc_rpc_failed(),
        Fail::new(502, "alc dtako-etags request: rpc")
    );
}

// ── 応答 ────────────────────────────────────────────────────────────────────

/// 22 桁の GCP 側 `unko_no`。先頭 6 桁が `YYMMDD` (運行開始日)。
fn u(ymd: &str, seq: u32) -> String {
    format!("{ymd}{seq:016}")
}

fn gcp(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
    pairs
        .iter()
        .map(|(u, ds)| (u.to_string(), ds.iter().map(|s| s.to_string()).collect()))
        .collect()
}

/// root の `serde_json::json!` と同じバイト列 (serde_json は preserve_order 無し = キーは名前順)。
#[test]
fn the_response_has_root_s_keys_in_root_s_order() {
    let w = Window::of("2026-06").unwrap();
    let onprem = Onprem::from_rows([(1445, "26060110000000000000011")]);
    let m = gcp(&[(&u("260610", 1), &["1445"]), (&u("260611", 2), &[])]);
    let got = respond("2026-06", &w, None, &onprem, Some(&m)).to_string();
    let want = concat!(
        r#"{"driver_cd":null,"driver_cds_available":true,"#,
        r#""drivers":[{"driver_cd":"1445","truncated":false,"unko_nos":["2606100000000000000001"]}],"#,
        r#""drivers_truncated":false,"gcp_etags_available":true,"month":"2026-06","#,
        r#""onprem_operations_in_month":null,"unknown_driver_unko_nos":["2606110000000000000002"],"#,
        r#""unknown_driver_unko_nos_truncated":false,"unko_no_digits":22}"#
    );
    assert_eq!(got, want);
}

#[test]
fn without_etags_nothing_is_judged() {
    let w = Window::of("2026-06").unwrap();
    let onprem = Onprem::from_rows([(1445, "26060110000000000000011")]);
    let got = respond("2026-06", &w, Some(1445), &onprem, None);
    assert_eq!(
        got,
        serde_json::json!({
            "month": "2026-06",
            "driver_cd": 1445,
            "onprem_operations_in_month": 1,
            "gcp_etags_available": false,
            "driver_cds_available": false,
            "unko_no_digits": 22,
            "drivers": [],
            "drivers_truncated": false,
            "unknown_driver_unko_nos": [],
            "unknown_driver_unko_nos_truncated": false,
        })
    );
    // etags は引けたが 0 件 = 判定はできて候補が居ない
    let empty = HashMap::new();
    let got = respond("2026-06", &w, None, &onprem, Some(&empty));
    assert_eq!(got["gcp_etags_available"], true);
    assert_eq!(got["driver_cds_available"], false);
}

// ── drop_crew_suffix (root から移した) ──────────────────────────────────────

#[test]
fn drop_crew_suffix_drops_only_the_last_character() {
    assert_eq!(
        drop_crew_suffix("26060610055500000023021"),
        "2606061005550000002302"
    );
    assert_eq!(drop_crew_suffix(""), "");
    assert_eq!(drop_crew_suffix("1"), "1");
}

// ── build_gaps (I/O から切り離した核。root から移した) ─────────────────────

#[test]
fn a_gap_is_attributed_to_its_driver_when_onprem_has_that_month() {
    let seen = HashSet::new(); // オンプレに何も無い = 一致するものが無い
    let mut in_month = HashMap::new();
    in_month.insert(1445, 5); // also_in_month の実測と同じ形 (onprem_in_month > 0)
    let cds = gcp(&[(&u("260610", 1), &["1445"])]);

    let (drivers, dt, unknown, ut) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(!dt && !ut);
    assert!(unknown.is_empty());
    assert_eq!(drivers.len(), 1, "{drivers:?}");
    assert_eq!(drivers[0].driver_cd, "1445");
    assert_eq!(drivers[0].unko_nos, vec![u("260610", 1)]);
}

#[test]
fn a_matched_unko_no_is_not_a_gap() {
    let mut seen = HashSet::new();
    let key = u("260610", 1);
    seen.insert(key.clone()); // オンプレ側に (対象CD 落とし後) 同じ値がある
    let mut in_month = HashMap::new();
    in_month.insert(1445, 1);
    let cds = gcp(&[(&key, &["1445"])]);

    let (drivers, _, unknown, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(drivers.is_empty(), "{drivers:?}");
    assert!(unknown.is_empty());
}

#[test]
fn a_driver_without_onprem_this_month_is_not_a_default_candidate() {
    let seen = HashSet::new();
    let in_month = HashMap::new(); // 9999 は対象月にオンプレの運行が無い
    let cds = gcp(&[(&u("260615", 1), &["9999"])]);

    let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(
        drivers.is_empty(),
        "省略時は also_in_month だけ: {drivers:?}"
    );
}

#[test]
fn an_explicit_driver_cd_bypasses_the_also_in_month_bucket() {
    let seen = HashSet::new();
    let in_month = HashMap::new(); // 9999 は候補ではないが、明示指定なら返す
    let cds = gcp(&[(&u("260615", 1), &["9999"])]);

    let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, Some(9999));
    assert_eq!(drivers.len(), 1, "{drivers:?}");
    assert_eq!(drivers[0].driver_cd, "9999");
}

#[test]
fn onprem_count_is_reported_only_for_an_explicit_driver_cd() {
    let mut in_month = HashMap::new();
    in_month.insert(1445, 5);
    // 指定なしは返さない (候補の絞り込みが効いているので要らない)
    assert_eq!(onprem_count_for(&in_month, None), None);
    assert_eq!(onprem_count_for(&in_month, Some(1445)), Some(5));
    // オンプレ側に 1 件も無い乗務員は 0 (= 照らし合わせる相手が無い)
    assert_eq!(onprem_count_for(&in_month, Some(1590)), Some(0));
}

#[test]
fn an_explicit_driver_cd_that_has_no_gap_returns_empty_not_an_error() {
    let seen = HashSet::new();
    let in_month = HashMap::new();
    let cds = gcp(&[(&u("260615", 1), &["9999"])]);

    let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, Some(1));
    assert!(drivers.is_empty(), "{drivers:?}");
}

#[test]
fn a_gap_outside_the_target_month_is_excluded() {
    let seen = HashSet::new();
    let mut in_month = HashMap::new();
    in_month.insert(1445, 3);
    // 開始日が前月 (etags の窓は読取日で引くので前月以前の運行が混ざりうる —
    // UnkoDiff::gcp_only_in_month の docs と同じ現象)
    let cds = gcp(&[(&u("260531", 1), &["1445"])]);

    let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(drivers.is_empty(), "対象月の外は数えない: {drivers:?}");
}

#[test]
fn an_unparseable_start_date_is_excluded_safely() {
    let seen = HashSet::new();
    let mut in_month = HashMap::new();
    in_month.insert(1445, 1);
    let cds = gcp(&[("not-a-date", &["1445"])]);

    let (drivers, _, unknown, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(drivers.is_empty());
    assert!(
        unknown.is_empty(),
        "判定できない = 安全側で外す。候補にも unknown にも出さない"
    );
}

#[test]
fn driver_cds_empty_falls_into_unknown_driver_not_silently_dropped() {
    let seen = HashSet::new();
    let mut in_month = HashMap::new();
    in_month.insert(1445, 1);
    // alc が driver_cds を返さない環境 (前方互換フィールドの既定 = 空配列)
    let cds = gcp(&[(&u("260610", 1), &[])]);

    let (drivers, _, unknown, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(
        drivers.is_empty(),
        "乗務員が引けないので drivers には出ない"
    );
    assert_eq!(unknown, vec![u("260610", 1)], "空を候補無しに読ませない");
}

#[test]
fn a_two_crew_operation_attributes_the_gap_to_both_drivers() {
    let seen = HashSet::new();
    let mut in_month = HashMap::new();
    in_month.insert(1445, 1);
    in_month.insert(1740, 1);
    let cds = gcp(&[(&u("260610", 1), &["1445", "1740"])]);

    let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    let mut got: Vec<&str> = drivers.iter().map(|d| d.driver_cd.as_str()).collect();
    got.sort_unstable();
    assert_eq!(got, vec!["1445", "1740"]);
}

#[test]
fn driver_count_above_the_cap_is_truncated_and_flagged() {
    let seen = HashSet::new();
    let mut in_month = HashMap::new();
    let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
    for i in 0..(MAX_UNKO_GAPS_DRIVERS + 5) {
        let cd = (2000 + i as i64).to_string();
        in_month.insert(2000 + i as i64, 1);
        pairs.push((u("260610", i as u32), vec![cd]));
    }
    let cds: HashMap<String, Vec<String>> = pairs.into_iter().collect();

    let (drivers, truncated, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(truncated);
    assert_eq!(drivers.len(), MAX_UNKO_GAPS_DRIVERS);
}

#[test]
fn unko_no_count_above_the_cap_is_truncated_and_flagged_per_driver() {
    let seen = HashSet::new();
    let mut in_month = HashMap::new();
    in_month.insert(1445, 1);
    let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
    for i in 0..(MAX_UNKO_GAPS_PER_DRIVER + 5) {
        pairs.push((u("260610", i as u32), vec!["1445".to_string()]));
    }
    let cds: HashMap<String, Vec<String>> = pairs.into_iter().collect();

    let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert_eq!(drivers.len(), 1);
    assert!(drivers[0].truncated);
    assert_eq!(drivers[0].unko_nos.len(), MAX_UNKO_GAPS_PER_DRIVER);
}

#[test]
fn unknown_driver_count_above_the_cap_is_truncated_and_flagged() {
    let seen = HashSet::new();
    let in_month = HashMap::new();
    let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
    for i in 0..(MAX_UNKO_GAPS_PER_DRIVER + 5) {
        pairs.push((u("260610", i as u32), Vec::new()));
    }
    let cds: HashMap<String, Vec<String>> = pairs.into_iter().collect();

    let (_, _, unknown, truncated) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(truncated);
    assert_eq!(unknown.len(), MAX_UNKO_GAPS_PER_DRIVER);
}

#[test]
fn empty_gcp_data_yields_no_drivers_and_no_unknown() {
    let seen = HashSet::new();
    let in_month = HashMap::new();
    let cds = HashMap::new();
    let (drivers, dt, unknown, ut) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
    assert!(drivers.is_empty() && !dt);
    assert!(unknown.is_empty() && !ut);
}
