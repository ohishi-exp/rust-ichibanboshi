//! `POST /api/uriage/verify`・`GET /api/uriage/verify-debug`・`POST /api/uriage/admin/{delete,rebuild}` の純粋部分
//! (Refs ohishi-exp/rust-ichibanboshi#322): verify_jobs の UPSERT・delete / rebuild の文を native の SQLite
//! (D1 と同じ SQLite) に流した結果と、応答の形。

use std::collections::HashMap;

use rusqlite::types::Value as SqlValue;
use rusqlite::{params, params_from_iter, Connection};
use serde_json::{json, Value};
use uriage_logic::admin::{
    delete_response, delete_statements, rebuild_response, rebuild_statements, DELETE_DAILY_SQL,
    DELETE_JOBS_SQL, DELETE_PARTNER_SQL, REBUILD_SQL,
};
use uriage_logic::api::{AdminDeleteQuery, VerifyQuery};
use uriage_logic::cakephp::OfficeMasters;
use uriage_logic::compute::{compute_person_sum, PersonAccum, UriageRow};
use uriage_logic::d1::{Bind, Stmt, SCHEMA_SQL};
use uriage_logic::recalc::failed_statement;
use uriage_logic::routes::Reply;
use uriage_logic::verify::{
    check_date, debug, office_missing, rows_sql, skipped, sum_unreadable, verified, VerifyJob,
    SKIPPED_NO_BUMON, VERIFY_JOB_UPSERT_SQL,
};

fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(SCHEMA_SQL).unwrap();
    conn
}

fn sql_binds(s: &Stmt) -> Vec<SqlValue> {
    s.binds
        .iter()
        .map(|b| match b {
            Bind::Text(t) => SqlValue::Text(t.clone()),
            Bind::Int(i) => SqlValue::Integer(*i),
            Bind::Null => SqlValue::Null,
        })
        .collect()
}

/// 1 文を流して変更行数を返す。
fn exec(conn: &Connection, s: &Stmt) -> usize {
    conn.execute(s.sql, params_from_iter(sql_binds(s))).unwrap()
}

fn count(conn: &Connection, table: &str) -> i64 {
    let sql = format!("SELECT COUNT(*) FROM {table}");
    conn.query_row(&sql, [], |r| r.get(0)).unwrap()
}

fn office(bumon: &[&str]) -> OfficeMasters {
    OfficeMasters {
        display_name: "本社".to_string(),
        persons: HashMap::from([("1499".to_string(), "青井".to_string())]),
        other: HashMap::new(),
        bumon: bumon.iter().map(|b| b.to_string()).collect(),
    }
}

fn query(id: i64, date: &str, cal: bool) -> VerifyQuery {
    VerifyQuery {
        id,
        date: date.to_string(),
        cal,
    }
}

/// 入力担当C 1499 (青井) に積算される 1 行 (B5)。
fn row(kingaku: i64) -> UriageRow {
    UriageRow {
        nyuryoku_tanto_c: 1499,
        kingaku,
        yoshasaki_c: "000000".to_string(),
        unko_date: "2026-06-30".to_string(),
        uriage_date: "2026-06-30".to_string(),
        ..Default::default()
    }
}

fn json_of(reply: &Reply) -> Value {
    assert_eq!(reply.status, 200);
    assert_eq!(reply.content_type, Some("application/json"));
    serde_json::from_str(&reply.body).unwrap()
}

fn job_row(conn: &Connection, date: &str) -> Vec<Value> {
    let sql = "SELECT unko_date, eigyosho_id, cal, month, ok, skipped_reason, diff_json, \
               row_count, elapsed_ms, ran_at FROM verify_jobs WHERE unko_date = ?1";
    conn.query_row(sql, [date], |r| {
        let mut out = Vec::new();
        for i in 0..10 {
            out.push(match r.get::<_, SqlValue>(i)? {
                SqlValue::Null => Value::Null,
                SqlValue::Integer(v) => json!(v),
                SqlValue::Text(t) => json!(t),
                _ => unreachable!(),
            });
        }
        Ok(out)
    })
    .unwrap()
}

// ── 入力・失敗の本文 ──

#[test]
fn date_must_be_ten_ascii_bytes() {
    assert_eq!(check_date("2026-06-30"), Ok(()));
    for bad in ["", "2026-6-30", "2026-06-300", "あいう1"] {
        let reply = check_date(bad).unwrap_err();
        assert_eq!(reply.status, 400, "{bad}");
        assert_eq!(reply.body, format!("date 形式不正 (YYYY-MM-DD): {bad}"));
        assert_eq!(reply.content_type, Some("text/plain; charset=utf-8"));
    }
}

#[test]
fn failure_replies_keep_the_onprem_wording_or_stage_and_kind() {
    let missing = office_missing(7);
    assert_eq!(missing.status, 404);
    assert_eq!(missing.body, "office_id=7 は masters に無い");
    let sum = sum_unreadable();
    assert_eq!(sum.status, 502);
    assert_eq!(sum.body, r#"{"ok":false,"stage":"cakephp","kind":"sum"}"#);
}

#[test]
fn rows_sql_follows_the_bumon_list() {
    assert_eq!(rows_sql(&office(&[])), None);
    // 英数字以外しか無ければ 0 行 (接続の後に見る)
    assert_eq!(rows_sql(&office(&["a b", "'; --"])), None);
    let sql = rows_sql(&office(&["010", "011"])).unwrap();
    assert!(sql.contains("'010','011'"));
    assert!(sql.contains("1499"));
}

// ── verify_jobs の UPSERT ──

#[test]
fn skipped_records_ok_with_no_bumon() {
    let conn = db();
    let (reply, stmt) = skipped(&query(3, "2026-06-30", false), 12, "2026-07-01T00:00:00Z");
    let body = json_of(&reply);
    assert_eq!(
        body,
        json!({
            "office_id": 3, "date": "2026-06-30", "cal": false,
            "php_sum": {}, "rust_sum": {}, "diff": null, "ok": true,
            "row_count": 0, "elapsed_ms": 12, "skipped_reason": "no_bumon"
        })
    );
    assert_eq!(SKIPPED_NO_BUMON, "no_bumon");
    assert_eq!(stmt.sql, VERIFY_JOB_UPSERT_SQL);
    assert_eq!(exec(&conn, &stmt), 1);
    assert_eq!(
        job_row(&conn, "2026-06-30"),
        [
            json!("2026-06-30"),
            json!(3),
            json!(0),
            json!("2026-06"),
            json!(1),
            json!("no_bumon"),
            Value::Null,
            json!(0),
            json!(12),
            json!("2026-07-01T00:00:00Z"),
        ]
    );
}

#[test]
fn verified_ok_when_php_and_rust_agree() {
    let conn = db();
    let office = office(&["010"]);
    let q = query(1, "2026-06-30", true);
    let rows = [row(1000), row(500)];
    let rust = compute_person_sum(&rows, &office.persons_as_int_map(), &office.other, true).sum;
    assert_eq!(rust["青井"].kingaku, 1500);

    let (reply, stmt) = verified(&q, &office, rust.clone(), &rows, 5, "2026-07-01T00:00:00Z");
    let body = json_of(&reply);
    assert_eq!(body["ok"], json!(true));
    assert_eq!(body["diff"], Value::Null);
    assert_eq!(body["row_count"], json!(2));
    assert_eq!(body["php_sum"], body["rust_sum"]);
    assert!(body.get("skipped_reason").is_none());

    assert_eq!(exec(&conn, &stmt), 1);
    let saved = job_row(&conn, "2026-06-30");
    assert_eq!(saved[4], json!(1));
    assert_eq!(saved[5], Value::Null);
    assert_eq!(saved[6], Value::Null);
    assert_eq!(saved[7], json!(2));
}

#[test]
fn verified_ng_keeps_the_diff_and_the_upsert_replaces_the_row() {
    let conn = db();
    let office = office(&["010"]);
    let q = query(1, "2026-06-30", true);
    let rows = [row(1000)];

    // 1 回目: PHP と一致 (ok = 1)
    let rust = compute_person_sum(&rows, &office.persons_as_int_map(), &office.other, true).sum;
    let (_, stmt) = verified(&q, &office, rust.clone(), &rows, 1, "t1");
    exec(&conn, &stmt);
    assert_eq!(job_row(&conn, "2026-06-30")[4], json!(1));

    // 2 回目: PHP が 1 円違う (ok = 0。同じ PK の行を置き換える)
    let mut php = rust.clone();
    php.insert(
        "青井".to_string(),
        PersonAccum {
            kingaku: 999,
            ..rust["青井"]
        },
    );
    let (reply, stmt) = verified(&q, &office, php, &rows, 2, "t2");
    let body = json_of(&reply);
    assert_eq!(body["ok"], json!(false));
    assert_eq!(body["diff"]["php_only"]["青井"]["金額"], json!(999));
    assert_eq!(body["diff"]["rust_only"]["青井"]["金額"], json!(1000));
    exec(&conn, &stmt);

    assert_eq!(count(&conn, "verify_jobs"), 1);
    let saved = job_row(&conn, "2026-06-30");
    assert_eq!(saved[4], json!(0));
    assert_eq!(saved[8], json!(2));
    assert_eq!(saved[9], json!("t2"));
    let diff: Value = serde_json::from_str(saved[6].as_str().unwrap()).unwrap();
    assert_eq!(diff, body["diff"]);
}

#[test]
fn verify_job_statement_binds_in_column_order() {
    let job = VerifyJob {
        unko_date: "2026-06-30",
        eigyosho_id: 4,
        cal: true,
        ok: false,
        skipped_reason: None,
        diff_json: Some("{}"),
        row_count: 9,
        elapsed_ms: 8,
        ran_at: "t",
    };
    let text = |s: &str| Bind::Text(s.to_string());
    assert_eq!(
        job.statement().binds,
        vec![
            text("2026-06-30"),
            Bind::Int(4),
            Bind::Int(1),
            text("2026-06"),
            Bind::Int(0),
            Bind::Null,
            text("{}"),
            Bind::Int(9),
            Bind::Int(8),
            text("t"),
        ]
    );
    // 短い日付でも落ちない (呼び手は check_date で弾く)
    let short = VerifyJob {
        unko_date: "2026",
        ..job
    };
    assert_eq!(short.statement().binds[3], text("2026"));
}

// ── verify-debug ──

#[test]
fn debug_returns_rows_with_decisions_and_does_not_need_bumon() {
    // bumon が空でも止めない (verify は skipped にするが、debug は原因調査用)
    let office = office(&[]);
    let q = query(1, "2026-06-30", false);
    let rows = [row(1000), row(500)];
    let reply = debug(&q, &office, HashMap::new(), &rows);
    let body = json_of(&reply);
    assert_eq!(body["office_id"], json!(1));
    assert_eq!(body["cal"], json!(false));
    assert_eq!(body["bumon"], json!([]));
    assert_eq!(body["persons"], json!({"1499": "青井"}));
    assert_eq!(body["other"], json!({}));
    let out = body["rows"].as_array().unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(out[0]["nyuryoku_tanto_c"], json!(1499));
    assert_eq!(out[0]["decision"]["tanto"], json!("青井"));
    assert_eq!(out[1]["kingaku"], json!(500));
    assert_eq!(body["php_sum"], json!({}));
    assert_eq!(body["rust_sum"]["青井"]["金額"], json!(1500));
    // PHP が空で Rust に値がある = rust_only
    assert_eq!(body["diff"]["rust_only"]["青井"]["金額"], json!(1500));
    assert_eq!(body["diff"]["php_only"], json!({}));
}

// ── admin/delete ──

fn seed(conn: &Connection, month: &str, eid: i64) {
    conn.execute(
        "INSERT INTO uriage_person_daily (unko_date, month, person_name, eigyosho_id, cal, \
         kingaku, yosha_kingaku, kensuu, calculated_at) VALUES (?1, ?2, '青井', ?3, 1, 1, 0, 1, 't')",
        params![format!("{month}-01"), month, eid],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO uriage_person_daily (unko_date, month, person_name, eigyosho_id, cal, \
         kingaku, yosha_kingaku, kensuu, calculated_at) VALUES (?1, ?2, '青井', ?3, 0, 1, 0, 1, 't')",
        params![format!("{month}-01"), month, eid],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO uriage_person_partner_daily (unko_date, month, person_name, eigyosho_id, cal, \
         partner_kind, partner_code, partner_name, kingaku, kensuu, calculated_at) \
         VALUES (?1, ?2, '青井', ?3, 1, 'customer', 'c1', '得意先', 1, 1, 't')",
        params![format!("{month}-01"), month, eid],
    )
    .unwrap();
    exec(conn, &failed_statement(month, eid, "e", "t"));
    conn.execute(
        "INSERT INTO verify_jobs (unko_date, eigyosho_id, cal, month, ok, row_count, elapsed_ms, ran_at) \
         VALUES (?1, ?2, 1, ?3, 1, 0, 0, 't')",
        params![format!("{month}-01"), eid, month],
    )
    .unwrap();
}

fn delete_query(month: &str, eid: i64) -> AdminDeleteQuery {
    AdminDeleteQuery {
        month: month.to_string(),
        eigyosho_id: eid,
    }
}

#[test]
fn delete_removes_one_bucket_and_keeps_verify_jobs() {
    let conn = db();
    seed(&conn, "2026-06", 1);
    seed(&conn, "2026-06", 2);
    seed(&conn, "2026-05", 1);

    let stmts = delete_statements(&delete_query("2026-06", 1)).unwrap();
    assert_eq!(
        stmts.iter().map(|s| s.sql).collect::<Vec<_>>(),
        [DELETE_DAILY_SQL, DELETE_PARTNER_SQL, DELETE_JOBS_SQL]
    );
    let changes: Vec<usize> = stmts.iter().map(|s| exec(&conn, s)).collect();
    // 日次は cal=1 と cal=0 の 2 行、recalc_jobs は 1 行
    assert_eq!(changes, [2, 1, 1]);

    let reply = delete_response(delete_query("2026-06", 1), &changes);
    assert_eq!(
        json_of(&reply),
        json!({"month": "2026-06", "eigyosho_id": 1, "daily_deleted": 2, "jobs_deleted": 1})
    );

    // 他の bucket と verify_jobs は残る
    assert_eq!(count(&conn, "uriage_person_daily"), 4);
    assert_eq!(count(&conn, "uriage_person_partner_daily"), 2);
    assert_eq!(count(&conn, "recalc_jobs"), 2);
    assert_eq!(count(&conn, "verify_jobs"), 3);
    // 月次は日次の VIEW なので連動して消える
    let months: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM uriage_person_monthly WHERE month = '2026-06'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert!(months > 0);
}

#[test]
fn delete_without_a_month_is_a_bare_400() {
    let reply = delete_statements(&delete_query("", 1)).unwrap_err();
    assert_eq!(
        (reply.status, reply.body.as_str(), reply.content_type),
        (400, "", None)
    );
}

#[test]
fn delete_response_refuses_a_malformed_batch_result() {
    for bad in [&[][..], &[1, 2][..], &[1, 2, 3, 4][..]] {
        let reply = delete_response(delete_query("2026-06", 1), bad);
        assert_eq!(reply.status, 500, "{bad:?}");
        assert_eq!(
            reply.body,
            r#"{"ok":false,"stage":"rows","kind":"malformed"}"#
        );
    }
}

// ── admin/rebuild ──

#[test]
fn rebuild_empties_the_four_tables_and_keeps_the_views() {
    let conn = db();
    seed(&conn, "2026-06", 1);
    seed(&conn, "2026-05", 2);

    let stmts = rebuild_statements();
    assert_eq!(stmts.iter().map(|s| s.sql).collect::<Vec<_>>(), REBUILD_SQL);
    assert!(stmts.iter().all(|s| s.binds.is_empty()));
    for s in &stmts {
        exec(&conn, s);
    }
    for table in [
        "uriage_person_daily",
        "uriage_person_partner_daily",
        "recalc_jobs",
        "verify_jobs",
    ] {
        assert_eq!(count(&conn, table), 0, "{table}");
    }
    // VIEW は残り、そのまま読める
    for view in ["uriage_person_monthly", "verify_coverage", "r2_pending"] {
        assert_eq!(count(&conn, view), 0, "{view}");
    }
    // DROP していないので、続けて書ける
    seed(&conn, "2026-06", 1);
    assert_eq!(count(&conn, "recalc_jobs"), 1);
}

#[test]
fn rebuild_response_carries_the_timestamp() {
    let reply = rebuild_response("2026-07-01T00:00:00Z".to_string());
    assert_eq!(
        json_of(&reply),
        json!({"rebuilt_at": "2026-07-01T00:00:00Z"})
    );
}
