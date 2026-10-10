//! `POST /api/dtako/autoload` の純粋部分 (`kintai_logic::dtako_autoload`) の単体テスト: クエリの検査・400 の文言・
//! ① ② ③ の段取り・応答・③ の材料を数える SQL と引数。オンプレ版の応答が移す前と同じことは root の
//! `routes::dtako_autoload::tests::autoload_snapshot_matches_the_baseline` が基点 (dd2b9c4) の実物で縛る。

use std::cell::RefCell;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use chrono::NaiveDate;
use kintai_logic::cakephp::{CakephpError, DtakoAutoloadResponse, ResetTimecardResponse};
use kintai_logic::dtako_autoload::{
    map_err, parse, parse_unko_no, run, AutoloadIo, AutoloadQuery, AutoloadRequest, MaterialQuery,
    BODY_EMPTY, MAX_ZIP_BYTES, NOT_CONFIGURED_ONPREM, RESET_MATERIAL_SQL,
    RESET_TIMECARD_STATUS_NOTE, UNKO_NO_INVALID,
};
use kintai_mysql::bind::{expand, names, Digits, Value};
use serde_json::{json, Value as Json};

const U: &str = "26060507533000000042861";

/// fake の I/O は待たずに返すので、1 回 poll すれば終わる。
fn block_on<F: Future>(f: F) -> F::Output {
    let mut f = pin!(f);
    match f.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(v) => v,
        Poll::Pending => panic!("fake の I/O は待たない"),
    }
}

fn query(
    unko_no: Option<&str>,
    file_name: Option<&str>,
    preview: bool,
    reset: bool,
) -> AutoloadQuery {
    AutoloadQuery {
        unko_no: unko_no.map(str::to_string),
        file_name: file_name.map(str::to_string),
        preview,
        reset_timecard: reset,
    }
}

fn req(preview: bool, reset: bool) -> AutoloadRequest {
    parse(query(Some(U), None, preview, reset), 12).unwrap()
}

/// 呼ばれた順を記録する fake。
struct Fake {
    step2: Result<u16, ()>,
    count: Result<i64, String>,
    step3: Result<u16, ()>,
    calls: RefCell<Vec<String>>,
}

fn fake(step2: Result<u16, ()>, count: Result<i64, String>, step3: Result<u16, ()>) -> Fake {
    Fake {
        step2,
        count,
        step3,
        calls: RefCell::new(Vec::new()),
    }
}

impl AutoloadIo for Fake {
    fn configured(&self) -> bool {
        true
    }

    async fn autoload(&self, file_name: &str) -> Result<DtakoAutoloadResponse, CakephpError> {
        self.calls
            .borrow_mut()
            .push(format!("autoload {file_name}"));
        match self.step2 {
            Ok(status) => Ok(DtakoAutoloadResponse {
                status,
                body_excerpt: "ok".into(),
                location: (status == 307).then(|| "/".to_string()),
            }),
            Err(()) => Err(CakephpError::RequestFailed("timeout".into())),
        }
    }

    async fn count_material(&self, q: &MaterialQuery) -> Result<i64, String> {
        self.calls
            .borrow_mut()
            .push(format!("count {} {}", q.v1, q.v2));
        self.count.clone()
    }

    async fn reset(&self, unko_no: &str) -> Result<ResetTimecardResponse, CakephpError> {
        self.calls.borrow_mut().push(format!("reset {unko_no}"));
        match self.step3 {
            Ok(status) => Ok(ResetTimecardResponse {
                status,
                location: None,
            }),
            Err(()) => Err(CakephpError::RequestFailed("timeout".into())),
        }
    }
}

fn calls(f: &Fake) -> Vec<String> {
    f.calls.borrow().clone()
}

// ── 検査 ──

#[test]
fn unko_no_must_be_at_least_12_digits() {
    assert_eq!(parse_unko_no(U), Some(U));
    assert_eq!(parse_unko_no("260605075330"), Some("260605075330"));
    for bad in ["", "26060507533", "2026-06", "2606050753300000004286a"] {
        assert_eq!(parse_unko_no(bad), None, "{bad:?}");
    }
}

#[test]
fn parse_checks_unko_no_then_body_and_defaults_the_file_name() {
    assert_eq!(
        parse(query(None, None, true, false), 3),
        Err(UNKO_NO_INVALID)
    );
    assert_eq!(
        parse(query(Some("2026-06"), None, true, false), 0),
        Err(UNKO_NO_INVALID),
        "unko_no が先"
    );
    assert_eq!(
        parse(query(Some(U), None, false, false), 0),
        Err(BODY_EMPTY)
    );
    let r = parse(query(Some(U), Some("  "), true, true), 5).unwrap();
    assert_eq!(
        r,
        AutoloadRequest {
            unko_no: U.into(),
            file_name: "csvdata.zip".into(),
            size_bytes: 5,
            preview: true,
            reset_timecard: true,
        }
    );
    assert_eq!(
        parse(query(Some(U), Some("x.zip"), false, false), 5)
            .unwrap()
            .file_name,
        "x.zip"
    );
    assert_eq!(MAX_ZIP_BYTES, 20 * 1024 * 1024);
}

#[test]
fn query_defaults_to_no_preview_and_no_reset() {
    let q: AutoloadQuery = serde_json::from_value(json!({"unko_no": U})).unwrap();
    assert!(!q.preview && !q.reset_timecard && q.file_name.is_none());
}

#[test]
fn errors_map_to_502_and_503() {
    assert_eq!(
        map_err(CakephpError::NotConfigured, NOT_CONFIGURED_ONPREM),
        (
            503,
            "CakePHP base_url が未設定 (CAKEPHP_BASE_URL)".to_string()
        )
    );
    assert_eq!(
        map_err(CakephpError::RequestFailed("dns".into()), ""),
        (502, "nginx への接続に失敗: dns".to_string())
    );
    let e = CakephpError::StatusError {
        status: 404,
        body_excerpt: "nf".into(),
    };
    assert_eq!(
        map_err(e, ""),
        (502, "CakePHP returned 404: nf".to_string())
    );
    assert_eq!(
        map_err(CakephpError::JsonError("bad".into()), ""),
        (502, "CakePHP response parse failed: bad".to_string())
    );
}

// ── 段取り ──

#[test]
fn preview_never_sends_and_counts_only_with_reset() {
    let f = fake(Ok(200), Ok(3), Ok(200));
    let v = block_on(run(&f, &req(true, false))).unwrap();
    assert!(calls(&f).is_empty(), "preview は ② も ① も打たない");
    assert_eq!(v["preview"], json!(true));
    assert_eq!(v["configured"], json!(true));
    assert_eq!(v["reset_target_path"], Json::Null);
    assert_eq!(v["target_path"], json!("/dtako-events/autoload"));

    let v = block_on(run(&f, &req(true, true))).unwrap();
    assert_eq!(
        calls(&f),
        ["count 26060507533000000042861 26060507533000000042862"]
    );
    assert_eq!(v["dtako_events_count"], json!(3));
    assert_eq!(
        v["reset_target_path"],
        json!(format!("/time-card-dtako/resetby-unko-no/{U}"))
    );

    let f = fake(Ok(200), Err("MariaDB query failed: boom".into()), Ok(200));
    let v = block_on(run(&f, &req(true, true))).unwrap();
    assert_eq!(v["dtako_events_count"], Json::Null);
    assert_eq!(
        v["dtako_events_count_error"],
        json!("MariaDB query failed: boom")
    );
}

#[test]
fn without_reset_only_step2_runs() {
    let f = fake(Ok(200), Ok(3), Ok(200));
    let v = block_on(run(&f, &req(false, false))).unwrap();
    assert_eq!(calls(&f), ["autoload csvdata.zip"]);
    assert_eq!(v["http_ok"], json!(true));
    assert_eq!(v["reset_attempted"], json!(false));
    assert_eq!(v["reset_note"], Json::Null);
}

#[test]
fn reset_counts_after_step2_then_resets() {
    let f = fake(Ok(200), Ok(2), Ok(200));
    let v = block_on(run(&f, &req(false, true))).unwrap();
    assert_eq!(
        calls(&f),
        [
            "autoload csvdata.zip",
            "count 26060507533000000042861 26060507533000000042862",
            "reset 26060507533000000042861"
        ],
        "② → ① → ③ の順"
    );
    assert_eq!(v["dtako_events_count"], json!(2));
    assert_eq!(v["reset_attempted"], json!(true));
    assert_eq!(v["reset_http_status"], json!(200));
    assert_eq!(v["reset_note"], json!(RESET_TIMECARD_STATUS_NOTE));
}

#[test]
fn reset_failure_is_reported_not_raised() {
    let f = fake(Ok(200), Ok(2), Err(()));
    let v = block_on(run(&f, &req(false, true))).unwrap();
    assert_eq!(v["reset_attempted"], json!(true));
    assert_eq!(v["reset_http_status"], Json::Null);
    assert_eq!(v["reset_error"], json!("CakePHP request failed: timeout"));
}

#[test]
fn no_material_means_no_reset() {
    let f = fake(Ok(200), Ok(0), Ok(200));
    let v = block_on(run(&f, &req(false, true))).unwrap();
    assert!(
        !calls(&f).iter().any(|c| c.starts_with("reset")),
        "材料 0 件で ③ を打たない (#281)"
    );
    assert_eq!(v["reset_skip_reason"], json!("no_dtako_events"));
    assert_eq!(v["dtako_events_count"], json!(0));
}

#[test]
fn a_failed_count_means_no_reset() {
    let f = fake(Ok(200), Err("boom".into()), Ok(200));
    let v = block_on(run(&f, &req(false, true))).unwrap();
    assert!(!calls(&f).iter().any(|c| c.starts_with("reset")));
    assert_eq!(v["reset_skip_reason"], json!("count_failed"));
    assert_eq!(v["reset_error"], json!("boom"));
}

#[test]
fn an_unparseable_start_is_zero_material_without_querying() {
    let r = parse(query(Some("123456789012"), None, false, true), 1).unwrap();
    let f = fake(Ok(200), Ok(5), Ok(200));
    let v = block_on(run(&f, &r)).unwrap();
    assert_eq!(calls(&f), ["autoload csvdata.zip"], "数えにいかない");
    assert_eq!(v["reset_skip_reason"], json!("no_dtako_events"));
}

#[test]
fn a_non_2xx_step2_means_no_count_and_no_reset_and_a_307_carries_the_location() {
    for status in [307, 500] {
        let f = fake(Ok(status), Ok(2), Ok(200));
        let v = block_on(run(&f, &req(false, true))).unwrap();
        assert_eq!(calls(&f), ["autoload csvdata.zip"], "{status}");
        assert_eq!(v["http_ok"], json!(false));
        assert_eq!(v["http_status"], json!(status));
        assert_eq!(v["reset_skip_reason"], json!("step2_failed"));
    }
    let f = fake(Ok(307), Ok(2), Ok(200));
    let v = block_on(run(&f, &req(false, false))).unwrap();
    assert_eq!(v["location"], json!("/"));
}

#[test]
fn a_failed_step2_send_is_an_error_and_never_reaches_step3() {
    let f = fake(Err(()), Ok(2), Ok(200));
    let e = block_on(run(&f, &req(false, true))).unwrap_err();
    assert!(matches!(e, CakephpError::RequestFailed(_)));
    assert_eq!(calls(&f), ["autoload csvdata.zip"]);
}

// ── ③ の材料 ──

#[test]
fn material_query_is_the_window_and_both_crew_variants() {
    let q = MaterialQuery::new("26060608220000000041573").unwrap();
    let day = |d| {
        NaiveDate::from_ymd_opt(2026, 6, d)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
    };
    assert_eq!(
        (q.from, q.to),
        (day(5), day(9)),
        "前日 0 時から 4 日後の 0 時"
    );
    assert_eq!(
        (q.v1.as_str(), q.v2.as_str()),
        ("26060608220000000041571", "26060608220000000041572")
    );
    assert_eq!(
        q.window_strings(),
        (
            "2026-06-05 00:00:00".to_string(),
            "2026-06-09 00:00:00".to_string()
        )
    );
    let q22 = MaterialQuery::new("2606060822000000004157").unwrap();
    assert_eq!(q22.v1, "26060608220000000041571", "22 桁でも動く");
    assert_eq!(MaterialQuery::new("U1"), None);
    assert_eq!(MaterialQuery::new("261306082200"), None, "13 月");
}

#[test]
fn material_sql_counts_rest_start_and_end_on_dtako_events_only() {
    for e in ["'休息'", "'運行開始'", "'運行終了'", "FROM dtako_events"] {
        assert!(RESET_MATERIAL_SQL.contains(e), "{e}");
    }
    assert!(!RESET_MATERIAL_SQL.contains("time_card_dtako"));
    assert!(!RESET_MATERIAL_SQL.contains("'休憩'"));
}

#[test]
fn material_sql_binds_with_kintai_mysql() {
    assert_eq!(
        names(RESET_MATERIAL_SQL).unwrap(),
        ["from", "to", "v1", "v2"]
    );
    let q = MaterialQuery::new(U).unwrap();
    let digits = |s: &str| Value::Digits(Digits::new(s).unwrap());
    let sql = expand(
        RESET_MATERIAL_SQL,
        &[
            ("from", Value::DateTime(q.from)),
            ("to", Value::DateTime(q.to)),
            ("v1", digits(&q.v1)),
            ("v2", digits(&q.v2)),
        ],
    )
    .unwrap();
    assert_eq!(
        sql.matches("IN ('26060507533000000042861', '26060507533000000042862')")
            .count(),
        2
    );
    assert_eq!(sql.matches("'2026-06-04 00:00:00'").count(), 3);
    assert_eq!(sql.matches("'2026-06-08 00:00:00'").count(), 2);
}
