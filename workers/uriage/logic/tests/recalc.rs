//! `POST /api/uriage/recalc` の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322): 認可・段取り (月・masters・job の上限)・
//! bucket の文 (0 行の除外・json_each の行の形)・D1 の書き込みの文を native の SQLite (D1 と同じ SQLite) に流した結果。
//! 特に recalc_jobs の upsert の ON CONFLICT が「fingerprint 不変なら status・r2_synced_at・fingerprint_changed_at を保つ」を固定する。

use std::collections::HashMap;

use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{params_from_iter, Connection};
use serde_json::{json, Value};
use uriage_logic::api::RecalcQuery;
use uriage_logic::cakephp::{CakephpError, EditableMonthsResponse, MastersResponse, OfficeMasters};
use uriage_logic::compute::{PartnerAccum, PartnerKind, PersonAccum, PersonPartnerKey, UriageRow};
use uriage_logic::d1::{Bind, Stmt, SCHEMA_SQL};
use uriage_logic::recalc::{
    check_jobs, daily_rows, month_range, outside_editable, partner_rows, plan_month, put_needed,
    response, target_months, timestamp, Job, Step, BUMON_EMPTY, COMPUTED_SQL, DAILY_DELETE_SQL,
    DAILY_INSERT_SQL, MAX_JOBS, PARTNER_DELETE_SQL, PARTNER_INSERT_SQL,
};
use uriage_logic::routes::Reply;
use uriage_logic::write_auth::{authorize, WRITE_TOKEN_BINDING, WRITE_TOKEN_HEADER};

// ── 認可 ──

#[test]
fn recalc_refuses_a_missing_or_wrong_token() {
    let forbidden = Err(Reply::failure(403, "auth", "forbidden"));
    assert_eq!(authorize(Some("s3cret"), None), forbidden);
    assert_eq!(authorize(Some("s3cret"), Some("other")), forbidden);
    assert_eq!(authorize(Some("s3cret"), Some("s3cre")), forbidden);
    assert_eq!(authorize(Some("s3cret"), Some("s3cret")), Ok(()));
}

#[test]
fn recalc_without_a_configured_token_is_503() {
    let unconfigured = Err(Reply::failure(503, "auth", "unconfigured"));
    assert_eq!(authorize(None, Some("x")), unconfigured);
    // 空の secret と空のヘッダーを一致させない
    assert_eq!(authorize(Some(""), Some("")), unconfigured);
    assert_eq!(WRITE_TOKEN_BINDING, "URIAGE_WRITE_TOKEN");
    assert_eq!(WRITE_TOKEN_HEADER, "X-Uriage-Write-Token");
}

// ── 段取り ──

fn editable(months: &[&str]) -> EditableMonthsResponse {
    EditableMonthsResponse {
        operation_month: "2026-07".to_string(),
        editable_months_count: months.len() as i32,
        editable_months: months.iter().map(|m| m.to_string()).collect(),
    }
}

fn office(name: &str, bumon: &[&str]) -> OfficeMasters {
    OfficeMasters {
        display_name: name.to_string(),
        persons: HashMap::from([("1499".to_string(), "青井".to_string())]),
        other: HashMap::new(),
        bumon: bumon.iter().map(|b| b.to_string()).collect(),
    }
}

fn masters(ids: &[&str]) -> MastersResponse {
    MastersResponse {
        date: "2026-06-30".to_string(),
        offices: ids
            .iter()
            .map(|id| (id.to_string(), office(id, &["010"])))
            .collect(),
    }
}

fn query(month: Option<&str>, eigyosho_id: Option<i64>) -> RecalcQuery {
    RecalcQuery {
        month: month.map(str::to_string),
        eigyosho_id,
    }
}

#[test]
fn timestamp_formats_like_the_onprem_clock() {
    assert_eq!(timestamp(0), "1970-01-01T00:00:00Z");
    // 2026-06-30T12:34:56.789Z (ミリ秒は落とす)
    assert_eq!(timestamp(1_782_822_896_789), "2026-06-30T12:34:56Z");
    // chrono の範囲外は空
    assert_eq!(timestamp(i64::MAX as u64), "");
}

#[test]
fn months_come_from_the_query_or_the_editable_months() {
    let e = editable(&["2026-06", "2026-07"]);
    assert_eq!(
        target_months(&query(None, None), &e),
        ["2026-06", "2026-07"]
    );
    assert_eq!(
        target_months(&query(Some("2025-01"), None), &e),
        ["2025-01"]
    );
    // 編集可能月の外でも走らせる (ログに残すだけ)
    assert!(outside_editable(&query(Some("2025-01"), None), &e));
    assert!(!outside_editable(&query(Some("2026-06"), None), &e));
    assert!(!outside_editable(&query(None, None), &e));
}

#[test]
fn a_malformed_month_is_a_failed_job_with_the_onprem_message() {
    let (from, to, date) = month_range("2026-02").unwrap();
    assert_eq!(
        (from.as_str(), to.as_str(), date.as_str()),
        ("2026-02-01", "2026-02-28", "2026-02-28")
    );
    let bad = month_range("2026-13").unwrap_err();
    assert_eq!(
        (bad.month.as_str(), bad.status.as_str()),
        ("2026-13", "failed")
    );
    assert_eq!(bad.error.as_deref(), Some("month の形式が不正: 2026-13"));
}

fn runs(steps: &[Step]) -> Vec<i64> {
    steps
        .iter()
        .filter_map(|s| match s {
            Step::Run(j) => Some(j.eigyosho_id),
            Step::Done(_) => None,
        })
        .collect()
}

#[test]
fn plan_month_lists_offices_in_id_order() {
    let m = masters(&["9", "1", "x", "10"]);
    let steps = plan_month("2026-06", "2026-06-01", "2026-06-30", Ok(m), None);
    // 数でない key は落とす。id 昇順
    assert_eq!(runs(&steps), [1, 9, 10]);
    let Step::Run(job) = &steps[0] else { panic!() };
    assert_eq!(
        (job.month.as_str(), job.from.as_str(), job.to.as_str()),
        ("2026-06", "2026-06-01", "2026-06-30")
    );
}

#[test]
fn plan_month_with_one_office() {
    let steps = plan_month("2026-06", "a", "b", Ok(masters(&["1", "9"])), Some(9));
    assert_eq!(runs(&steps), [9]);
    let steps = plan_month("2026-06", "a", "b", Ok(masters(&["1"])), Some(7));
    let [Step::Done(skipped)] = &steps[..] else {
        panic!()
    };
    assert_eq!(
        (skipped.eigyosho_id, skipped.status.as_str()),
        (7, "skipped")
    );
    assert_eq!(
        skipped.error.as_deref(),
        Some("eigyosho_id=7 が masters に存在しない")
    );
}

#[test]
fn plan_month_without_masters_is_one_failed_job() {
    let steps = plan_month("2026-06", "a", "b", Err(CakephpError::Status(500)), Some(1));
    let [Step::Done(failed)] = &steps[..] else {
        panic!()
    };
    assert_eq!((failed.eigyosho_id, failed.status.as_str()), (0, "failed"));
    assert_eq!(failed.error.as_deref(), Some("cakephp:status:500"));
}

#[test]
fn more_than_max_jobs_is_422() {
    let ids: Vec<String> = (1..=MAX_JOBS).map(|i| i.to_string()).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    let mut steps = plan_month("2026-06", "a", "b", Ok(masters(&ids)), None);
    // Done (failed / skipped) は数えない
    steps.extend(plan_month(
        "2026-07",
        "a",
        "b",
        Err(CakephpError::Json),
        None,
    ));
    assert_eq!(check_jobs(&steps), Ok(()));
    steps.extend(plan_month("2026-07", "a", "b", Ok(masters(&["1"])), None));
    let too_many = check_jobs(&steps).unwrap_err();
    assert_eq!(
        (too_many.status, too_many.body.as_str()),
        (422, r#"{"ok":false,"stage":"jobs","kind":"too_many"}"#)
    );
    assert_eq!(MAX_JOBS, 12);
}

fn job(bumon: &[&str]) -> Job {
    Job {
        month: "2026-06".to_string(),
        eigyosho_id: 1,
        from: "2026-06-01".to_string(),
        to: "2026-06-30".to_string(),
        masters: office("本社", bumon),
    }
}

#[test]
fn job_results_and_sql() {
    let j = job(&["010"]);
    let ok = j.computed();
    assert_eq!(
        (ok.month.as_str(), ok.eigyosho_id, ok.status.as_str()),
        ("2026-06", 1, "computed")
    );
    let failed = j.failed(BUMON_EMPTY);
    assert_eq!(
        failed.error.as_deref(),
        Some("masters.bumon is empty (CakePHP 旧仕様?)")
    );
    assert!(!j.bumon_empty());
    assert!(job(&[]).bumon_empty());
    let sql = j.sql().unwrap();
    assert!(sql.contains("'010'") && sql.contains("1499"));
    // 英数字に絞って空なら SQL を流さない (= 0 行)
    assert_eq!(job(&["0-1"]).sql(), None);
}

// ── bucket の文 ──

fn row(date: &str, kingaku: i64) -> UriageRow {
    UriageRow {
        yokoyoko: 0,
        nyuryoku_tanto_c: 1499,
        kado_bumon: "010".to_string(),
        kingaku,
        yosha_kingaku: 0,
        shain_r: "青井健".to_string(),
        yoshasaki_c: "000000".to_string(),
        unko_date: date.to_string(),
        uriage_date: date.to_string(),
        tokuisaki_key: "TESTCUST-0".to_string(),
        tokuisaki_n: "テスト得意先".to_string(),
        ..UriageRow::default()
    }
}

fn json_rows(s: &Stmt) -> Value {
    let Bind::Text(t) = &s.binds[0] else { panic!() };
    serde_json::from_str(t).unwrap()
}

#[test]
fn bucket_statements_are_one_batch_of_nine() {
    let j = job(&["010"]);
    let rows = [row("2026-06-02", 2000), row("2026-06-01", 1000)];
    let b = j.bucket(&rows, "2026-07-01T00:00:00Z");
    assert_eq!(b.key, "uriage/2026-06/eigyosho-1.ndjson.gz");
    assert_eq!(b.fingerprint.len(), 64);
    assert_eq!(
        (b.row_count, b.daily_count_cal, b.daily_count_nocal),
        (2, 2, 2)
    );
    let sqls: Vec<&str> = b.statements.iter().map(|s| s.sql).collect();
    let want = [
        DAILY_DELETE_SQL,
        DAILY_INSERT_SQL,
        DAILY_DELETE_SQL,
        DAILY_INSERT_SQL,
        PARTNER_DELETE_SQL,
        PARTNER_INSERT_SQL,
        PARTNER_DELETE_SQL,
        PARTNER_INSERT_SQL,
        COMPUTED_SQL,
    ];
    assert_eq!(sqls, want);
    // D1 の 1 文の bind は 100 まで
    assert!(b.statements.iter().all(|s| s.binds.len() <= 5));
    let m = Bind::Text("2026-06".to_string());
    let at = Bind::Text("2026-07-01T00:00:00Z".to_string());
    let del = &b.statements[2];
    assert_eq!(del.binds, [m.clone(), Bind::Int(1), Bind::Int(0)]);
    let ins = &b.statements[1];
    assert_eq!(ins.binds[1..], [m, Bind::Int(1), Bind::Int(1), at]);
    // json_each に渡す行の形 (日次: 日・担当者・金額・傭車金額・件数・*_y0。日の昇順)
    assert_eq!(
        json_rows(ins),
        json!([
            ["2026-06-01", "青井", 1000, 0, 1, 1000, 0, 1],
            ["2026-06-02", "青井", 2000, 0, 1, 2000, 0, 1]
        ])
    );
    // 取引先別: 日・担当者・種別・取引先キー・取引先名・金額・件数・*_y0
    assert_eq!(
        json_rows(&b.statements[5])[0],
        json!([
            "2026-06-01",
            "青井",
            "customer",
            "TESTCUST-0",
            "テスト得意先",
            1000,
            1,
            1000,
            1
        ])
    );
    let meta = b.metadata("2026-07-01T00:00:00Z");
    assert_eq!(meta["fingerprint"], b.fingerprint);
    assert_eq!(meta["computed_at"], "2026-07-01T00:00:00Z");
}

#[test]
fn zero_rows_are_not_inserted() {
    let only =
        |v: PersonAccum| HashMap::from([("d".to_string(), HashMap::from([("n".to_string(), v)]))]);
    let zero = PersonAccum::default();
    assert!(daily_rows(&only(zero)).is_empty());
    // 6 値のどれか 1 つが非 0 なら入れる
    let ones = [
        PersonAccum { kensuu: 1, ..zero },
        PersonAccum { kingaku: 1, ..zero },
        PersonAccum {
            yosha_kingaku: 1,
            ..zero
        },
        PersonAccum {
            kensuu_y0: 1,
            ..zero
        },
        PersonAccum {
            kingaku_y0: 1,
            ..zero
        },
        PersonAccum {
            yosha_kingaku_y0: 1,
            ..zero
        },
    ];
    for v in ones {
        assert_eq!(daily_rows(&only(v)).len(), 1, "{v:?}");
    }

    let key = PersonPartnerKey {
        person_name: "n".to_string(),
        partner_kind: PartnerKind::Subcontractor,
        partner_code: "c".to_string(),
    };
    let only =
        |v: PartnerAccum| HashMap::from([("d".to_string(), HashMap::from([(key.clone(), v)]))]);
    let zero = PartnerAccum::default();
    // 4 値が全部 0 なら入れない (名前だけあっても)
    let named = PartnerAccum {
        partner_name: "x".to_string(),
        ..zero.clone()
    };
    assert!(partner_rows(&only(named)).is_empty());
    let ones = [
        PartnerAccum {
            kensuu: 1,
            ..zero.clone()
        },
        PartnerAccum {
            kingaku: 1,
            ..zero.clone()
        },
        PartnerAccum {
            kensuu_y0: 1,
            ..zero.clone()
        },
        PartnerAccum {
            kingaku_y0: 1,
            ..zero.clone()
        },
    ];
    for v in ones {
        let rows = only(v);
        let got = partner_rows(&rows);
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].2, got[0].3), ("subcontractor", "c"));
    }
}

#[test]
fn put_is_skipped_only_for_the_same_synced_fingerprint() {
    assert!(put_needed(&[], "fp"));
    assert!(!put_needed(&[vec![json!("fp"), json!("r2_synced")]], "fp"));
    assert!(put_needed(&[vec![json!("fp"), json!("computed")]], "fp"));
    assert!(put_needed(&[vec![json!("old"), json!("r2_synced")]], "fp"));
    assert!(put_needed(&[vec![Value::Null, json!("failed")]], "fp"));
    assert!(put_needed(&[vec![]], "fp"));
}

#[test]
fn response_has_the_onprem_shape() {
    let j = job(&["010"]);
    let body = response(
        vec!["2026-06".to_string()],
        2,
        "t".to_string(),
        vec![j.computed()],
    );
    let v: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        v,
        json!({
            "source_table": "運転日報明細 + CakePHP masters → uriage_person_monthly + recalc_jobs",
            "months": ["2026-06"],
            "editable_months_count": 2,
            "calculated_at": "t",
            "jobs": [{
                "month": "2026-06", "eigyosho_id": 1, "status": "computed", "row_count": 0,
                "daily_count_cal": 0, "daily_count_nocal": 0,
                "fingerprint": null, "raw_path": null, "error": null
            }]
        })
    );
}

// ── D1 (SQLite) に流す ──

fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(SCHEMA_SQL).unwrap();
    conn
}

fn binds(s: &Stmt) -> Vec<SqlValue> {
    s.binds
        .iter()
        .map(|b| match b {
            Bind::Text(t) => SqlValue::Text(t.clone()),
            Bind::Int(i) => SqlValue::Integer(*i),
            Bind::Null => SqlValue::Null,
        })
        .collect()
}

/// D1 の batch と同じく、文の列を 1 transaction で流す。
fn batch(conn: &mut Connection, stmts: &[Stmt]) {
    let tx = conn.transaction().unwrap();
    for s in stmts {
        tx.execute(s.sql, params_from_iter(binds(s))).unwrap();
    }
    tx.commit().unwrap();
}

fn select(conn: &Connection, s: &Stmt) -> Vec<Vec<Value>> {
    let mut st = conn.prepare(s.sql).unwrap();
    let n = st.column_count();
    let mut rows = st.query(params_from_iter(binds(s))).unwrap();
    let mut out = Vec::new();
    while let Some(r) = rows.next().unwrap() {
        let row = (0..n)
            .map(|i| match r.get_ref(i).unwrap() {
                ValueRef::Null => Value::Null,
                ValueRef::Integer(v) => json!(v),
                ValueRef::Text(t) => json!(String::from_utf8(t.to_vec()).unwrap()),
                _ => unreachable!(),
            })
            .collect();
        out.push(row);
    }
    out
}

fn query_rows(conn: &Connection, sql: &'static str) -> Vec<Vec<Value>> {
    select(conn, &Stmt { sql, binds: vec![] })
}

/// recalc_jobs の (status, fingerprint_before, fingerprint_after, raw_path, created_at, computed_at,
/// fingerprint_changed_at, r2_synced_at, last_error)
fn job_row(conn: &Connection) -> Value {
    let rows = query_rows(
        conn,
        "SELECT status, fingerprint_before, fingerprint_after, raw_path, created_at, computed_at, \
         fingerprint_changed_at, r2_synced_at, last_error FROM recalc_jobs",
    );
    assert_eq!(rows.len(), 1);
    Value::Array(rows[0].clone())
}

#[test]
fn bucket_batch_replaces_the_rows_of_the_bucket() {
    let mut conn = db();
    let j = job(&["010"]);
    // 別の bucket (営業所 2) の行は消さない
    let other = Job {
        eigyosho_id: 2,
        ..job(&["010"])
    };
    batch(
        &mut conn,
        &other.bucket(&[row("2026-06-01", 5)], "t0").statements,
    );

    batch(
        &mut conn,
        &j.bucket(&[row("2026-06-01", 1000), row("2026-06-02", 2000)], "t1")
            .statements,
    );
    batch(
        &mut conn,
        &j.bucket(&[row("2026-06-03", 3000)], "t2").statements,
    );
    let daily = query_rows(
        &conn,
        "SELECT unko_date, month, person_name, eigyosho_id, cal, kingaku, yosha_kingaku, kensuu, \
         kingaku_y0, yosha_kingaku_y0, kensuu_y0, calculated_at FROM uriage_person_daily \
         ORDER BY eigyosho_id, cal, unko_date",
    );
    assert_eq!(
        Value::Array(daily.into_iter().map(Value::Array).collect()),
        json!([
            [
                "2026-06-03",
                "2026-06",
                "青井",
                1,
                0,
                3000,
                0,
                1,
                3000,
                0,
                1,
                "t2"
            ],
            [
                "2026-06-03",
                "2026-06",
                "青井",
                1,
                1,
                3000,
                0,
                1,
                3000,
                0,
                1,
                "t2"
            ],
            [
                "2026-06-01",
                "2026-06",
                "青井",
                2,
                0,
                5,
                0,
                1,
                5,
                0,
                1,
                "t0"
            ],
            [
                "2026-06-01",
                "2026-06",
                "青井",
                2,
                1,
                5,
                0,
                1,
                5,
                0,
                1,
                "t0"
            ]
        ])
    );
    let partner = query_rows(
        &conn,
        "SELECT unko_date, eigyosho_id, cal, partner_kind, partner_code, partner_name, kingaku, kensuu, \
         kingaku_y0, kensuu_y0, calculated_at FROM uriage_person_partner_daily \
         WHERE eigyosho_id = 1 AND partner_kind = 'customer' ORDER BY cal",
    );
    assert_eq!(
        Value::Array(partner.into_iter().map(Value::Array).collect()),
        json!([
            [
                "2026-06-03",
                1,
                0,
                "customer",
                "TESTCUST-0",
                "テスト得意先",
                3000,
                1,
                3000,
                1,
                "t2"
            ],
            [
                "2026-06-03",
                1,
                1,
                "customer",
                "TESTCUST-0",
                "テスト得意先",
                3000,
                1,
                3000,
                1,
                "t2"
            ]
        ])
    );
    // 行が 0 の bucket は空にする
    batch(&mut conn, &j.bucket(&[], "t3").statements);
    let left = query_rows(
        &conn,
        "SELECT COUNT(*) FROM uriage_person_daily WHERE eigyosho_id = 1",
    );
    assert_eq!(left, [[json!(0)]]);
}

#[test]
fn upsert_keeps_the_sync_state_while_the_fingerprint_is_unchanged() {
    let mut conn = db();
    let j = job(&["010"]);
    let rows = [row("2026-06-01", 1000)];
    let b = j.bucket(&rows, "t1");
    let (fp, key) = (b.fingerprint.clone(), b.key.clone());
    batch(&mut conn, &b.statements);
    assert_eq!(
        job_row(&conn),
        json!(["computed", null, fp, key, "t1", "t1", "t1", null, null])
    );
    assert!(put_needed(&select(&conn, &j.existing_statement()), &fp));

    // put が通った
    batch(&mut conn, &[j.synced_statement("t1s")]);
    assert_eq!(
        job_row(&conn),
        json!(["r2_synced", null, fp, key, "t1", "t1", "t1", "t1s", null])
    );

    // 同じ行で 2 回目: status・r2_synced_at・fingerprint_changed_at を保つ。computed_at は更新
    let existing = select(&conn, &j.existing_statement());
    assert_eq!(existing, [[json!(fp), json!("r2_synced")]]);
    assert!(!put_needed(&existing, &fp));
    batch(&mut conn, &j.bucket(&rows, "t2").statements);
    assert_eq!(
        job_row(&conn),
        json!(["r2_synced", fp, fp, key, "t1", "t2", "t1", "t1s", null])
    );

    // 中身が変わった: computed・r2_synced_at は NULL・fingerprint_changed_at は今
    let changed = j.bucket(&[row("2026-06-01", 1001)], "t3");
    let fp2 = changed.fingerprint.clone();
    assert_ne!(fp, fp2);
    assert!(put_needed(&select(&conn, &j.existing_statement()), &fp2));
    batch(&mut conn, &changed.statements);
    assert_eq!(
        job_row(&conn),
        json!(["computed", fp, fp2, key, "t1", "t3", "t3", null, null])
    );

    // put が失敗したまま同じ行でもう一度: computed のまま (次の recalc で置き直す)
    batch(
        &mut conn,
        &j.bucket(&[row("2026-06-01", 1001)], "t4").statements,
    );
    assert_eq!(
        job_row(&conn),
        json!(["computed", fp2, fp2, key, "t1", "t4", "t3", null, null])
    );
    assert!(put_needed(&select(&conn, &j.existing_statement()), &fp2));
}

#[test]
fn a_success_after_a_failure_with_the_same_fingerprint_is_computed_again() {
    let mut conn = db();
    let j = job(&["010"]);
    let rows = [row("2026-06-01", 1000)];
    let b = j.bucket(&rows, "t1");
    let (fp, key) = (b.fingerprint.clone(), b.key.clone());
    batch(&mut conn, &b.statements);
    batch(&mut conn, &[j.synced_statement("t1s")]);

    // 失敗の記録は status と last_error だけを変える (raw の状態は触らない)
    batch(
        &mut conn,
        &[j.failed_statement("sqlserver:query:timeout", "t2")],
    );
    assert_eq!(
        job_row(&conn),
        json!([
            "failed",
            null,
            fp,
            key,
            "t1",
            "t1",
            "t1",
            "t1s",
            "sqlserver:query:timeout"
        ])
    );
    assert!(put_needed(&select(&conn, &j.existing_statement()), &fp));

    // 同じ fingerprint で成功: オンプレ版は failed を保ってしまうが、Worker は computed に戻す
    batch(&mut conn, &j.bucket(&rows, "t3").statements);
    assert_eq!(
        job_row(&conn),
        json!(["computed", fp, fp, key, "t1", "t3", "t1", "t1s", null])
    );
    // put が通れば r2_synced
    batch(&mut conn, &[j.synced_statement("t3s")]);
    assert_eq!(
        job_row(&conn),
        json!(["r2_synced", fp, fp, key, "t1", "t3", "t1", "t3s", null])
    );
}

#[test]
fn a_first_failure_creates_the_row() {
    let mut conn = db();
    let j = job(&[]);
    batch(&mut conn, &[j.failed_statement(BUMON_EMPTY, "t1")]);
    assert_eq!(
        job_row(&conn),
        json!([
            "failed",
            null,
            null,
            null,
            "t1",
            null,
            null,
            null,
            BUMON_EMPTY
        ])
    );
    // synced は computed のときだけ
    batch(&mut conn, &[j.synced_statement("t2")]);
    assert_eq!(job_row(&conn)[0], json!("failed"));
}
