//! 拘束サマリ (restraint) の 3 口: 勤怠 Worker の D1 の経路を native の SQLite で流したものに**固定の要求の列**を与え、応答
//! (status と本文) が固定した期待値になることを確かめる (Refs #322)。DB は要らない。
//!
//! 期待値は、オンプレ版 (root の axum の handler + rusqlite の `RestraintStore`) に同じ要求の列を与えて一致していた値
//! (本文の `synced_at` は時刻なので `<synced_at>` に伏せる)。ファイル名の `restraint_d1_parity` は、比較の相手を外す前の名前を
//! 残している。
//!
//! D1 の経路は Worker (`worker/src/restraint.rs`) と同じ順・同じ部品で組む: 本文 (`parse_json`) / Query (`parse_query`) →
//! 検査 (`restraint`) → 文の束 (`restraint_d1`) を 1 transaction で流す (D1 の `batch` と同じ) → 行を列名をキーにした object
//! (D1 の結果と同じ形) にして `restraint_d1` が応答を組む。表は D1 の migration (`SCHEMA_SQL` = migrations/0001_restraint.sql)。
//! wasm 専用の部分 (D1 の binding・JsValue への写し) だけがここに無い。

use kintai_logic::common::{parse_json, parse_query, Fail};
use kintai_logic::restraint::{
    parse_synced_months, parse_wage_source, summary_binds, validate_push, Bind, ErrorBody,
    PushBody, RestraintEntry, SyncedMonthsQuery, WageSourceQuery, SCHEMA_SQL, UPSERT_SUMMARY_SQL,
};
use kintai_logic::restraint_d1::{
    check_push_size, push_statements, synced_at_from_millis, synced_from_results, synced_statement,
    wage_source_from_results, wage_source_statements, Stmt,
};
use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{params_from_iter, Connection};
use serde_json::{json, Value};

/// JSON ならその値、平文なら文字列。
fn body_value(bytes: &[u8]) -> Value {
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(bytes).into()))
}

// ── D1 の経路 (native の SQLite) ──

struct D1 {
    conn: Connection,
}

/// Worker の失敗の形 (平文 = axum の extractor の拒否、JSON = `{"error"}`)。
fn plain(f: Fail) -> (u16, String) {
    (f.status, f.body)
}

fn json_fail(f: Fail) -> (u16, String) {
    (f.status, serde_json::to_string(&ErrorBody::of(f)).unwrap())
}

/// Worker と同じく応答の型をそのまま直列化する (キーの順まで比べる)。
fn ok(body: serde_json::Result<String>) -> (u16, String) {
    (200, body.unwrap())
}

fn sql_values(binds: &[Bind]) -> Vec<SqlValue> {
    binds
        .iter()
        .map(|b| match b {
            Bind::Text(s) => SqlValue::Text(s.clone()),
            Bind::Int(i) => SqlValue::Integer(*i),
            Bind::Null => SqlValue::Null,
        })
        .collect()
}

impl D1 {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        Self { conn }
    }

    /// D1 の `batch` (1 transaction で順に流し、文ごとの行を列名をキーにした object で返す)。
    fn batch(&mut self, stmts: &[Stmt]) -> Vec<Vec<Value>> {
        let tx = self.conn.transaction().unwrap();
        let mut out = Vec::new();
        for s in stmts {
            let mut st = tx.prepare(s.sql).unwrap();
            let names: Vec<String> = st.column_names().iter().map(|c| c.to_string()).collect();
            let mut rows = st.query(params_from_iter(sql_values(&s.binds))).unwrap();
            let mut got = Vec::new();
            while let Some(r) = rows.next().unwrap() {
                let mut obj = serde_json::Map::new();
                for (i, name) in names.iter().enumerate() {
                    let v = match r.get_ref(i).unwrap() {
                        ValueRef::Null => Value::Null,
                        // D1 は JS の number で返す (整数でも浮動小数)。その形で渡す
                        ValueRef::Integer(n) => json!(n as f64),
                        ValueRef::Real(f) => json!(f),
                        ValueRef::Text(t) => json!(String::from_utf8(t.to_vec()).unwrap()),
                        ValueRef::Blob(_) => unreachable!("blob の列は無い"),
                    };
                    obj.insert(name.clone(), v);
                }
                got.push(Value::Object(obj));
            }
            out.push(got);
        }
        tx.commit().unwrap();
        out
    }

    fn put(&mut self, body: &str, now_ms: i64) -> (u16, String) {
        let push: PushBody = match parse_json(Some("application/json"), body.as_bytes()) {
            Ok(p) => p,
            Err(f) => return plain(f),
        };
        let valid = match validate_push(push).and_then(|v| check_push_size(&v).map(|_| v)) {
            Ok(v) => v,
            Err(f) => return json_fail(f),
        };
        let synced_at = synced_at_from_millis(now_ms);
        self.batch(&push_statements(&valid, &synced_at));
        ok(serde_json::to_string(&valid.response(synced_at)))
    }

    fn wage_source(&mut self, query: &str) -> (u16, String) {
        let q: WageSourceQuery = match parse_query(query) {
            Ok(q) => q,
            Err(f) => return plain(f),
        };
        let req = match parse_wage_source(q) {
            Ok(r) => r,
            Err(f) => return json_fail(f),
        };
        let results = self.batch(&wage_source_statements(&req));
        let (res, _broken) = wage_source_from_results(req, results).unwrap();
        ok(serde_json::to_string(&res))
    }

    fn synced_months(&mut self, query: &str) -> (u16, String) {
        let q: SyncedMonthsQuery = match parse_query(query) {
            Ok(q) => q,
            Err(f) => return plain(f),
        };
        let comp = match parse_synced_months(q) {
            Ok(c) => c,
            Err(f) => return json_fail(f),
        };
        let rows = self.batch(&[synced_statement(&comp)]).remove(0);
        ok(serde_json::to_string(
            &synced_from_results(&comp, rows).unwrap(),
        ))
    }
}

// ── 比べ方 ──

/// 本文の `"synced_at":"…"` の値を "<synced_at>" に (時刻なので)。キーの順・他の値はそのまま比べる。
fn mask(body: &str) -> String {
    const KEY: &str = "\"synced_at\":\"";
    let mut out = String::new();
    let mut rest = body;
    while let Some(i) = rest.find(KEY) {
        out.push_str(&rest[..i + KEY.len()]);
        rest = &rest[i + KEY.len()..];
        let end = rest.find('"').unwrap();
        out.push_str("<synced_at>");
        rest = &rest[end..];
    }
    out.push_str(rest);
    out
}

/// 応答が status・本文 (synced_at を伏せたバイト列) で期待値と一致し、synced_at の書式がオンプレ版と同じこと。
fn assert_fixed(got: &(u16, String), want: &(u16, &str), what: &str) {
    assert_eq!(got.0, want.0, "{what}: status");
    assert_eq!(mask(&got.1), want.1, "{what}: 本文");
    normalize(&mut body_value(got.1.as_bytes()));
}

/// synced_at は時刻なので "<synced_at>" に (null はそのまま)。書式はオンプレ版と同じであることを先に確かめる。
fn normalize(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for (k, child) in map.iter_mut() {
                if k == "synced_at" {
                    if let Some(s) = child.as_str() {
                        let frac = s.split_once('.').expect("小数部がある").1;
                        assert_eq!(frac.len(), "123456789+00:00".len(), "{s}");
                        assert!(frac.ends_with("+00:00"), "{s}");
                        *child = json!("<synced_at>");
                    }
                } else {
                    normalize(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(normalize),
        _ => {}
    }
}

fn summary_entry(driver_cd: &str, restraint: i64) -> Value {
    json!({
        "driver_cd": driver_cd,
        "summary": {"driverCd": driver_cd, "driverName": format!("乗務員{driver_cd}"),
                    "restraintMinutes": restraint, "days": [{"day": 1, "isRestDay": false}]},
        "fetched_at": "2026-07-01T00-00-00Z",
        "last_verified_at": "2026-07-02T00-00-00Z",
    })
}

fn push(comp: &str, source: &str, month: &str, entries: Value) -> String {
    json!({"comp_id": comp, "source": source, "month": month, "entries": entries}).to_string()
}

enum Step {
    Put(String),
    Get(&'static str),
}

fn steps() -> Vec<Step> {
    use Step::*;
    vec![
        Put(push(
            "27324455",
            "theearth",
            "2026-06",
            json!([summary_entry("100", 600), summary_entry("200", 700)]),
        )),
        // 100 を上書き・300 を no_data で足す (200 は残る → row_count 3)
        Put(push(
            "27324455",
            "theearth",
            "2026-06",
            json!([summary_entry("100", 999), {"driver_cd": "300", "no_data": true}]),
        )),
        Put(push(
            "27324455",
            "timecard",
            "2026-06",
            json!([summary_entry("300", 480)]),
        )),
        Put(push(
            "27324455",
            "theearth",
            "2026-05",
            json!([summary_entry("100", 500)]),
        )),
        Put(push(
            "27324455",
            "timecard",
            "2025-12",
            json!([{"driver_cd": "400", "no_data": true, "summary": {"x": 1}}]),
        )),
        Put(push("27324455", "theearth", "2026-01", json!([]))),
        Put(push(
            "a_b",
            "theearth",
            "2026-06",
            json!([summary_entry("1", 1)]),
        )),
        Put(push(
            "axb",
            "timecard",
            "2026-06",
            json!([summary_entry("2", 2)]),
        )),
        // 400 の検査 (順と文言)
        Put(push("a/b", "venus", "x", json!([]))),
        Put(push("27324455", "venus", "x", json!([]))),
        Put(push("27324455", "theearth", "2026-6", json!([]))),
        Put(push(
            "27324455",
            "theearth",
            "2026-06",
            json!([{"driver_cd": ""}]),
        )),
        Put(push(
            "27324455",
            "theearth",
            "2026-06",
            json!([{"driver_cd": "9"}]),
        )),
        // 本文が読めない (axum の Json の拒否と同じ平文)
        Put("{not json".to_string()),
        Put(json!({"comp_id": "1"}).to_string()),
        Get("/api/restraint/wage-source?comp=27324455&month=2026-06"),
        Get("/api/restraint/wage-source?comp=27324455&month=2026-01"),
        Get("/api/restraint/wage-source?comp=nobody&month=2026-06"),
        Get("/api/restraint/wage-source?comp=&month=2026-06"),
        Get("/api/restraint/wage-source?comp=27324455&month=junk"),
        Get("/api/restraint/wage-source?comp=27324455"),
        Get("/api/restraint/synced-months?comp=27324455"),
        Get("/api/restraint/synced-months?comp=a_b"),
        Get("/api/restraint/synced-months?comp=nobody"),
        Get("/api/restraint/synced-months?comp=a%2Fb"),
        Get("/api/restraint/synced-months"),
    ]
}

/// `steps()` と同じ順の期待値 (status, `synced_at` を伏せた本文)。
fn expected() -> Vec<(u16, &'static str)> {
    let put = |saved: u8| -> (u16, &'static str) {
        match saved {
            0 => (200, r#"{"saved":0,"synced_at":"<synced_at>"}"#),
            1 => (200, r#"{"saved":1,"synced_at":"<synced_at>"}"#),
            _ => (200, r#"{"saved":2,"synced_at":"<synced_at>"}"#),
        }
    };
    vec![
        put(2),
        put(2),
        put(1),
        put(1),
        put(1),
        put(0),
        put(1),
        put(1),
        // 400 の検査 (順と文言)
        (400, r#"{"error":"comp_id が不正です"}"#),
        (
            400,
            r#"{"error":"source は theearth / timecard のいずれかで指定してください"}"#,
        ),
        (400, r#"{"error":"month は YYYY-MM で指定してください"}"#),
        (400, r#"{"error":"driver_cd が空の entry があります"}"#),
        (
            400,
            r#"{"error":"driver_cd=9 は no_data でないのに summary がありません"}"#,
        ),
        // 本文が読めない (axum の Json の拒否と同じ平文)
        (
            400,
            "Failed to parse the request body as JSON: key must be a string at line 1 column 2",
        ),
        (
            422,
            "Failed to deserialize the JSON body into the target type: missing field `source` at line 1 column 15",
        ),
        // wage-source: 200 (100 は上書き後の 999・200 は残る・300 は no_data・前月 05 の 100)
        (
            200,
            r#"{"comp_id":"27324455","month":"2026-06","prev_month":"2026-05","current_theearth":{"summaries":[{"driver_cd":"100","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"100","driverName":"乗務員100","restraintMinutes":999},"fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z"},{"driver_cd":"200","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"200","driverName":"乗務員200","restraintMinutes":700},"fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z"}],"no_data_drivers":["300"],"synced_at":"<synced_at>"},"current_timecard":{"summaries":[{"driver_cd":"300","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"300","driverName":"乗務員300","restraintMinutes":480},"fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z"}],"no_data_drivers":[],"synced_at":"<synced_at>"},"prev_theearth":{"summaries":[{"driver_cd":"100","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"100","driverName":"乗務員100","restraintMinutes":500},"fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z"}],"no_data_drivers":[],"synced_at":"<synced_at>"},"prev_timecard":{"summaries":[],"no_data_drivers":[],"synced_at":null}}"#,
        ),
        (
            200,
            r#"{"comp_id":"27324455","month":"2026-01","prev_month":"2025-12","current_theearth":{"summaries":[],"no_data_drivers":[],"synced_at":"<synced_at>"},"current_timecard":{"summaries":[],"no_data_drivers":[],"synced_at":null},"prev_theearth":{"summaries":[],"no_data_drivers":[],"synced_at":null},"prev_timecard":{"summaries":[],"no_data_drivers":["400"],"synced_at":"<synced_at>"}}"#,
        ),
        (
            200,
            r#"{"comp_id":"nobody","month":"2026-06","prev_month":"2026-05","current_theearth":{"summaries":[],"no_data_drivers":[],"synced_at":null},"current_timecard":{"summaries":[],"no_data_drivers":[],"synced_at":null},"prev_theearth":{"summaries":[],"no_data_drivers":[],"synced_at":null},"prev_timecard":{"summaries":[],"no_data_drivers":[],"synced_at":null}}"#,
        ),
        (400, r#"{"error":"comp が不正です"}"#),
        (400, r#"{"error":"month は YYYY-MM で指定してください"}"#),
        (
            400,
            "Failed to deserialize query string: missing field `month`",
        ),
        // synced-months
        (
            200,
            r#"{"entries":[{"source":"theearth","month":"2026-01","synced_at":"<synced_at>","row_count":0},{"source":"theearth","month":"2026-05","synced_at":"<synced_at>","row_count":1},{"source":"theearth","month":"2026-06","synced_at":"<synced_at>","row_count":3},{"source":"timecard","month":"2025-12","synced_at":"<synced_at>","row_count":1},{"source":"timecard","month":"2026-06","synced_at":"<synced_at>","row_count":1}]}"#,
        ),
        (
            200,
            r#"{"entries":[{"source":"theearth","month":"2026-06","synced_at":"<synced_at>","row_count":1}]}"#,
        ),
        (200, r#"{"entries":[]}"#),
        (400, r#"{"error":"comp が不正です"}"#),
        (
            400,
            "Failed to deserialize query string: missing field `comp`",
        ),
    ]
}

#[test]
fn the_fixed_requests_give_the_fixed_responses() {
    let mut d1 = D1::new();
    let mut now = 1_791_594_123_000_i64;
    let want = expected();
    let steps = steps();
    assert_eq!(steps.len(), want.len());
    for (i, step) in steps.into_iter().enumerate() {
        let got = match &step {
            Step::Put(body) => {
                now += 1;
                d1.put(body, now)
            }
            Step::Get(uri) => {
                let query = uri.split_once('?').map_or("", |(_, q)| q);
                if uri.starts_with("/api/restraint/wage-source") {
                    d1.wage_source(query)
                } else {
                    d1.synced_months(query)
                }
            }
        };
        assert_fixed(&got, &want[i], &format!("要求 {i}"));
    }
}

/// 壊れた summary_json の行 (検査を通らない形で表に入ったもの) を行単位で落として残りを返す。
/// 行だけを入れた (sync_state を書いていない) ので `synced_at` は null。
#[test]
fn broken_rows_are_dropped() {
    let rows = [
        RestraintEntry {
            driver_cd: "100".into(),
            no_data: false,
            summary_json: Some("not-json".into()),
            fetched_at: None,
            last_verified_at: None,
        },
        RestraintEntry {
            driver_cd: "150".into(),
            no_data: false,
            summary_json: None,
            fetched_at: None,
            last_verified_at: None,
        },
        RestraintEntry {
            driver_cd: "200".into(),
            no_data: false,
            summary_json: Some(r#"{"driverCd":"200"}"#.into()),
            fetched_at: Some("f".into()),
            last_verified_at: None,
        },
    ];
    let mut d1 = D1::new();
    let stmts: Vec<Stmt> = rows
        .iter()
        .map(|e| Stmt {
            sql: UPSERT_SUMMARY_SQL,
            binds: summary_binds("27324455", "theearth", "2026-06", e).to_vec(),
        })
        .collect();
    d1.batch(&stmts);
    let got = d1.wage_source("comp=27324455&month=2026-06");
    assert_eq!(got.0, 200);
    assert_eq!(
        got.1,
        r#"{"comp_id":"27324455","month":"2026-06","prev_month":"2026-05","current_theearth":{"summaries":[{"driver_cd":"200","summary":{"driverCd":"200"},"fetched_at":"f","last_verified_at":null}],"no_data_drivers":[],"synced_at":null},"current_timecard":{"summaries":[],"no_data_drivers":[],"synced_at":null},"prev_theearth":{"summaries":[],"no_data_drivers":[],"synced_at":null},"prev_timecard":{"summaries":[],"no_data_drivers":[],"synced_at":null}}"#
    );
}
