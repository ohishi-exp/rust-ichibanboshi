//! 売上 Worker の読みの口 6 本: D1 の経路を native の SQLite で流したものに**固定の要求の列**を与え、応答 (status・
//! content-type・本文) が固定した期待値になることを確かめる (Refs ohishi-exp/rust-ichibanboshi#322)。DB は要らない。
//!
//! - 表は D1 の migration (`SCHEMA_SQL` = worker/migrations/0001_uriage.sql)、行は `fixtures/d1_fixture.sql`
//! - 要求の列は `fixtures/d1_requests.txt`、期待値は `fixtures/d1_expected.txt`。期待値は、オンプレ版 (root の axum の handler +
//!   rusqlite の `LocalStore`) に同じ行・同じ要求の列を与えて得た応答と一致していた値 (root は売上の撤去で消えるので、
//!   比べる相手を外して値を固定している。`workers/kintai/pg/tests/restraint_d1_parity.rs` と同じ形)
//! - 経路は Worker (`worker/src/lib.rs`) と同じ順・同じ部品: 経路 (`routes::route`) → Query (axum と同じ読み方) →
//!   検査と文 (`d1::statements`) → 1 文ずつ流して行を D1 の `raw()` と同じ形 (列の並びの配列・数は JS の number) にする →
//!   `d1::respond`。wasm 専用の部分 (D1 の binding・JsValue からの読み取り) だけがここに無い

use rusqlite::types::ValueRef;
use rusqlite::{params_from_iter, Connection};
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use uriage_logic::d1::{
    ping_ok, respond, statements, Bind, Read, Stmt, D1_BINDING, PING_SQL, SCHEMA_SQL,
};
use uriage_logic::routes::{route, ReadRoute, Reply, Route, HEALTH_BODY, PROBE_BODY};

const FIXTURE: &str = include_str!("fixtures/d1_fixture.sql");
const REQUESTS: &str = include_str!("fixtures/d1_requests.txt");
const EXPECTED: &str = include_str!("fixtures/d1_expected.txt");

/// 整数の列を D1 がどの形で渡しても同じ応答になること (JS の number は serde で i64 にも f64 にもなりうる)。
#[derive(Clone, Copy)]
enum Num {
    Int,
    Float,
}

fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(SCHEMA_SQL).unwrap();
    conn.execute_batch(FIXTURE).unwrap();
    conn
}

/// D1 の `raw()` と同じ形 (行ごとに列の並びの配列)。
fn raw(conn: &Connection, s: &Stmt, num: Num) -> Vec<Vec<Value>> {
    let mut st = conn.prepare(s.sql).unwrap();
    let n = st.column_count();
    let binds: Vec<rusqlite::types::Value> = s
        .binds
        .iter()
        .map(|b| match b {
            Bind::Text(t) => rusqlite::types::Value::Text(t.clone()),
            Bind::Int(i) => rusqlite::types::Value::Integer(*i),
        })
        .collect();
    let mut rows = st.query(params_from_iter(binds)).unwrap();
    let mut out = Vec::new();
    while let Some(r) = rows.next().unwrap() {
        let row = (0..n)
            .map(|i| match r.get_ref(i).unwrap() {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(v) => match num {
                    Num::Int => json!(v),
                    Num::Float => json!(v as f64),
                },
                ValueRef::Real(f) => json!(f),
                ValueRef::Text(t) => json!(String::from_utf8(t.to_vec()).unwrap()),
                ValueRef::Blob(_) => unreachable!("blob の列は無い"),
            })
            .collect();
        out.push(row);
    }
    out
}

/// axum の `Query` と同じ読み方 (Worker の `parse_query` と同じ)。
fn parse_query<T: DeserializeOwned>(query: &str) -> Result<T, Reply> {
    let de = serde_urlencoded::Deserializer::new(form_urlencoded::parse(query.as_bytes()));
    serde_path_to_error::deserialize(de)
        .map_err(|e| Reply::text(400, format!("Failed to deserialize query string: {e}")))
}

fn read_of(r: ReadRoute, query: &str) -> Result<Read, Reply> {
    Ok(match r {
        ReadRoute::Daily => Read::Daily(parse_query(query)?),
        ReadRoute::PersonMonthlyTotals => Read::PersonMonthlyTotals(parse_query(query)?),
        ReadRoute::PersonPartnerTotals => Read::PersonPartnerTotals(parse_query(query)?),
        ReadRoute::R2Pending => Read::R2Pending(parse_query(query)?),
        ReadRoute::RecalcJobs => Read::RecalcJobs(parse_query(query)?),
        ReadRoute::VerifyHistory => Read::VerifyHistory(parse_query(query)?),
    })
}

/// Worker の 1 リクエスト (GET) と同じ流れ。
fn serve(conn: &Connection, req: &str, num: Num) -> Reply {
    let (path, query) = req.split_once('?').unwrap_or((req, ""));
    let Route::Read(r) = route("GET", path) else {
        panic!("読みの口ではない: {req}");
    };
    let read = match read_of(r, query) {
        Ok(read) => read,
        Err(reply) => return reply,
    };
    let stmts = match statements(&read) {
        Ok(s) => s,
        Err(reply) => return reply,
    };
    let results = stmts.iter().map(|s| raw(conn, s, num)).collect();
    match respond(read, results) {
        Ok(body) => Reply::ok(body),
        Err(reply) => reply,
    }
}

/// 1 要求 1 行: `要求 \t status \t content-type (無ければ -) \t 本文`。
fn line(req: &str, reply: &Reply) -> String {
    let ct = reply.content_type.unwrap_or("-");
    format!("{req}\t{}\t{ct}\t{}", reply.status, reply.body)
}

fn run_all(num: Num) -> String {
    let conn = db();
    REQUESTS
        .lines()
        .filter(|l| !l.is_empty())
        .map(|req| line(req, &serve(&conn, req, num)) + "\n")
        .collect()
}

#[test]
fn six_reads_match_the_fixed_expectation() {
    let got = run_all(Num::Int);
    if got != EXPECTED {
        for (g, e) in got.lines().zip(EXPECTED.lines()) {
            assert_eq!(g, e);
        }
        assert_eq!(got.lines().count(), EXPECTED.lines().count());
    }
}

#[test]
fn numbers_as_js_floats_give_the_same_replies() {
    assert_eq!(run_all(Num::Float), run_all(Num::Int));
}

#[test]
fn every_read_route_is_in_the_request_list() {
    for name in [
        "daily",
        "person-monthly-totals",
        "person-partner-totals",
        "r2/pending",
        "recalc-jobs",
        "verify-history",
    ] {
        let path = format!("/api/uriage/{name}");
        let Route::Read(r) = route("GET", &path) else {
            panic!("{path}");
        };
        assert_eq!(r.name(), name);
        assert!(REQUESTS
            .lines()
            .any(|l| l.starts_with(&format!("{path}?")) || l == path));
    }
}

#[test]
fn ping_reads_one() {
    let conn = db();
    let stmt = Stmt {
        sql: PING_SQL,
        binds: vec![],
    };
    assert!(ping_ok(&raw(&conn, &stmt, Num::Int)));
    assert!(ping_ok(&raw(&conn, &stmt, Num::Float)));
    assert!(!ping_ok(&[]));
    assert!(!ping_ok(&[vec![]]));
    assert!(!ping_ok(&[vec![json!(2)]]));
    assert!(!ping_ok(&[vec![json!(1.5)]]));
    assert!(!ping_ok(&[vec![json!("1")]]));
    assert_eq!(D1_BINDING, "URIAGE_DB");
}

#[test]
fn routes_and_methods() {
    assert_eq!(route("GET", "/health"), Route::Health);
    assert_eq!(route("POST", "/probe"), Route::Probe);
    assert_eq!(route("POST", "/health"), Route::MethodNotAllowed);
    assert_eq!(route("GET", "/probe"), Route::MethodNotAllowed);
    assert_eq!(route("POST", "/api/uriage/daily"), Route::MethodNotAllowed);
    // 書き込みの口 (次の PR) と移さない口は 404
    for path in [
        "/api/uriage/recalc",
        "/api/uriage/verify",
        "/api/uriage/verify-debug",
        "/api/uriage/admin/delete",
        "/api/uriage/admin/rebuild",
        "/api/uriage/by-person",
        "/api/uriage/raw/2026-05/1",
        "/api/uriage/raw/2026-05/1/ack",
        "/api/uriage/",
        "/api/uriage",
        "/",
        "/api/uriage/daily/",
    ] {
        assert_eq!(route("GET", path), Route::NotFound, "{path}");
        assert_eq!(route("POST", path), Route::NotFound, "{path}");
    }
}

#[test]
fn replies() {
    assert_eq!(Reply::for_route(Route::Health), None);
    assert_eq!(Reply::for_route(Route::Read(ReadRoute::Daily)), None);
    let nf = Reply::for_route(Route::NotFound).unwrap();
    assert_eq!((nf.status, nf.body.as_str()), (404, r#"{"ok":false}"#));
    let na = Reply::for_route(Route::MethodNotAllowed).unwrap();
    assert_eq!((na.status, na.body.as_str()), (405, r#"{"ok":false}"#));
    assert_eq!(na.content_type, Some("application/json"));
    let nb = Reply::no_binding();
    assert_eq!(
        (nb.status, nb.body.as_str()),
        (503, r#"{"ok":false,"stage":"binding","kind":"missing"}"#)
    );
    let q = Reply::d1_failed("query");
    assert_eq!(
        (q.status, q.body.as_str()),
        (500, r#"{"ok":false,"stage":"query","kind":"failed"}"#)
    );
    let bare = Reply::bare(400);
    assert_eq!((bare.body.as_str(), bare.content_type), ("", None));
    assert_eq!(
        serde_json::from_str::<Value>(HEALTH_BODY).unwrap(),
        json!({"status": "ok"})
    );
    assert_eq!(
        serde_json::from_str::<Value>(PROBE_BODY).unwrap(),
        json!({"ok": true, "d1": "ok"})
    );
}

/// D1 の行が想定の形でなければ 500 (`rows:malformed`)。
#[test]
fn malformed_rows_are_500() {
    let bad = Reply::failure(500, "rows", "malformed");
    let read = |req: &str| {
        let (path, query) = req.split_once('?').unwrap();
        let Route::Read(r) = route("GET", path) else {
            panic!()
        };
        read_of(r, query).unwrap()
    };
    let daily = "/api/uriage/daily?month=2026-05&eigyosho_id=1";
    // 結果の数が足りない
    assert_eq!(respond(read(daily), vec![]), Err(bad.clone()));
    let partner = "/api/uriage/person-partner-totals?person=A&from=2026-05&to=2026-05";
    assert_eq!(respond(read(partner), vec![vec![]]), Err(bad.clone()));
    // 文字列の列に数
    let row: Vec<Value> = vec![json!(1); 12];
    assert_eq!(respond(read(daily), vec![vec![row]]), Err(bad.clone()));
    // 整数の列に小数・文字列、列が足りない
    let mut row = vec![json!("x"); 3];
    row.extend([json!(1.5)]);
    assert_eq!(respond(read(daily), vec![vec![row]]), Err(bad.clone()));
    assert_eq!(
        respond(read(daily), vec![vec![vec![json!("2026-05-01")]]]),
        Err(bad.clone())
    );
    // NULL を許す列に数
    let mut job = vec![json!("2026-05"), json!(1), json!("computed")];
    job.push(json!(5));
    let jobs = "/api/uriage/recalc-jobs?";
    assert_eq!(respond(read(jobs), vec![vec![job]]), Err(bad.clone()));
    let short = vec![json!("2026-05"), json!(1), json!("computed")];
    assert_eq!(respond(read(jobs), vec![vec![short]]), Err(bad.clone()));
    for req in [
        "/api/uriage/person-monthly-totals?from=a&to=b",
        "/api/uriage/r2/pending?",
        "/api/uriage/verify-history?from=a&to=b",
    ] {
        assert_eq!(
            respond(read(req), vec![vec![vec![]]]),
            Err(bad.clone()),
            "{req}"
        );
        assert_eq!(respond(read(req), vec![]), Err(bad.clone()), "{req}");
    }
    assert_eq!(respond(read(partner), vec![vec![vec![]]]), Err(bad.clone()));
    assert_eq!(respond(read(partner), vec![]), Err(bad.clone()));
    assert_eq!(respond(read(jobs), vec![]), Err(bad));
}

/// migration は D1 に 2 回当たっても壊れない (`IF NOT EXISTS`)。表 4・VIEW 3。
#[test]
fn schema_is_idempotent() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(SCHEMA_SQL).unwrap();
    conn.execute_batch(SCHEMA_SQL).unwrap();
    let count = |kind: &str| -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = ?1",
            [kind],
            |r| r.get(0),
        )
        .unwrap()
    };
    assert_eq!(count("table"), 4);
    assert_eq!(count("view"), 3);
}
