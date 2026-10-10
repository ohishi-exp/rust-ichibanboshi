//! Supabase への書き込み (変更履歴・賃金スナップショット) の SQL と bind の束が、root から移す前 (a06a4d0) と
//! 同じであることを縛る (Refs #322)。測り方は `workers/kintai/kosoku/tests/pg_write_snapshot.rs` と同じ:
//! 移す前の root の SQL 定数の本文の sha256 と、同じ固定入力 (下の `fx_*`) から**移す前の bind のコードそのまま**で
//! 作った Vec の束の `Debug` の sha256。root と写し (logic) で同じだった読みの SQL (`SELECT_SQL`・`SINCE_SQL`・
//! `SELECT_RANGE_SQL`) も、写しを消して 1 つにしたので同じ値で縛る。

use kintai_kosoku::kintai_push::{DriverPlan, PushEvent};
use kintai_logic::change_log::{
    build_changes, change_columns, INSERT_CHANGES_SQL, OLD_EVENTS_SQL, SELECT_SQL, SINCE_SQL,
};
use kintai_logic::wage_range::SELECT_RANGE_SQL;
use kintai_logic::wage_snapshot::WageSnapshotRow;
use kintai_logic::wage_write::{wage_columns, DELETE_MONTH_SQL, INSERT_ROWS_SQL};

/// (名前, SQL, 基点の sha256)。
const SQLS: [(&str, &str, &str); 7] = [
    (
        "OLD_EVENTS_SQL",
        OLD_EVENTS_SQL,
        "fcf7bf243d77989a1284ac1936d696f820199f80e477d88e8ed26375b1206d59",
    ),
    (
        "INSERT_CHANGES_SQL",
        INSERT_CHANGES_SQL,
        "5d8b17e9d4739ebd16890e8bce6c1cb73c5248732efd1cb80031d697f49f2658",
    ),
    (
        "DELETE_MONTH_SQL",
        DELETE_MONTH_SQL,
        "166dfbdfdfdcb0fb289b77ce979272e154cb798d7317a0f9a35dcbad4b24a9d5",
    ),
    (
        "INSERT_ROWS_SQL",
        INSERT_ROWS_SQL,
        "c4f9fe931afb6cefe3fdff8bbb808c613bbae0370d9005003af8a2e6ee34d8e5",
    ),
    (
        "SELECT_RANGE_SQL",
        SELECT_RANGE_SQL,
        "99986c0aad10b24eb9f60eb256635abe199e1849ea82dd4376ae65e11d31b3c4",
    ),
    (
        "SELECT_SQL",
        SELECT_SQL,
        "74a45da85b576a9900032f84e789bebe51596fa6e3354f8ab7c510b2f12ee179",
    ),
    (
        "SINCE_SQL",
        SINCE_SQL,
        "cd2769a2552823d7b05eed20841873ae181a3f0edb43466171c19e3352530214",
    ),
];

/// どの SQL も `tenant_id = $1` で絞る (INSERT は `$1` を tenant_id の列に入れる)。接続ロールは行単位の権限を
/// 素通りするので、これが他テナントに触れない唯一の担保。
#[test]
fn every_sql_pins_the_tenant_to_dollar_one() {
    for (name, sql, _) in SQLS {
        let pinned = sql.contains("tenant_id = $1")
            || (sql.contains("(tenant_id,") && sql.contains("SELECT $1,"));
        assert!(pinned, "{name} に tenant_id = $1 が無い");
    }
}

/// 表はどれも `kintai.` で修飾する (Worker の接続は `search_path = alc_api` 固定)。
#[test]
fn every_table_is_schema_qualified() {
    for (name, sql, _) in SQLS {
        for kw in ["FROM ", "INTO ", "JOIN "] {
            for (i, _) in sql.match_indices(kw) {
                let rest = &sql[i + kw.len()..];
                assert!(
                    rest.starts_with("kintai.") || rest.starts_with("unnest("),
                    "{name}: {kw}{}",
                    &rest[..rest.len().min(30)]
                );
            }
        }
    }
}

#[test]
fn the_sql_text_is_byte_identical_to_the_base() {
    for (name, sql, base) in SQLS {
        assert_eq!(fx_sha(sql), base, "{name}");
    }
}

#[test]
fn the_bind_columns_are_identical_to_the_base() {
    let c = change_columns(&build_changes(&fx_old_events(), &fx_plans()));
    assert_eq!(
        fx_sha(&format!(
            "{:?}",
            (&c.driver_cd, &c.date, &c.before, &c.after)
        )),
        "6097a52fa7ec1a3af1458b4cb2ea757497fc15ae2737b1ccd64bcf07f0d6c304"
    );
    let w = wage_columns(&fx_wage_rows());
    let s = format!(
        "{:?}",
        (
            (
                &w.driver_cd,
                &w.driver_name,
                &w.company,
                &w.branch_name,
                &w.branch_code,
                &w.job_name,
                &w.pay_kubun,
                &w.hourly_rate
            ),
            (
                &w.calc_base,
                &w.calc_overtime,
                &w.calc_total,
                &w.paid_base,
                &w.paid_overtime,
                &w.working_minutes,
                &w.restraint_missing
            )
        )
    );
    assert_eq!(
        fx_sha(&s),
        "fbe1980b9df8311b218976c978b5f42f14eba018c8d73f6457f55c701c4746bd"
    );
}

// ── 固定入力 (基点の束を測ったときと同じ文字列) ──

fn fx_dt(s: &str) -> chrono::NaiveDateTime {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").unwrap()
}

fn fx_d(s: &str) -> chrono::NaiveDate {
    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap()
}

fn fx_ev(driver: i64, at: &str, state: &str, source: &str, unko: Option<&str>) -> PushEvent {
    PushEvent {
        driver_cd: driver,
        occurred_at: fx_dt(at),
        state: state.to_string(),
        source: source.to_string(),
        unko_no: unko.map(str::to_string),
        raw: serde_json::json!({"driver_id": driver, "datetime": at, "state": state, "source": source, "unko_no": unko, "extra": [1, "二"]}),
    }
}

fn fx_plans() -> std::collections::BTreeMap<i64, DriverPlan> {
    let mut plans = std::collections::BTreeMap::new();
    plans.insert(
        1130,
        DriverPlan {
            changed: std::collections::BTreeMap::from([
                (
                    fx_d("2026-06-01"),
                    vec![
                        fx_ev(1130, "2026-06-01 08:00:00", "始業", "timecard", None),
                        fx_ev(
                            1130,
                            "2026-06-01 09:15:30",
                            "運行開始",
                            "dtako",
                            Some("26060109153000000012341"),
                        ),
                        fx_ev(1130, "2026-06-01 18:02:05", "終業", "timecard", None),
                    ],
                ),
                (fx_d("2026-06-03"), vec![]),
            ]),
            deleted: vec![fx_d("2026-06-02")],
        },
    );
    plans.insert(
        1702,
        DriverPlan {
            changed: std::collections::BTreeMap::from([(
                fx_d("2026-06-30"),
                vec![fx_ev(
                    1702,
                    "2026-06-30 23:59:59",
                    "運行終了",
                    "dtako",
                    Some("2606300000000000009999"),
                )],
            )]),
            deleted: vec![],
        },
    );
    plans
}

fn fx_old_events() -> Vec<PushEvent> {
    vec![
        fx_ev(1130, "2026-06-01 08:30:00", "始業", "timecard", None),
        fx_ev(1130, "2026-06-02 08:00:00", "始業", "timecard", None),
        fx_ev(
            1702,
            "2026-06-30 23:59:59",
            "運行終了",
            "dtako",
            Some("2606300000000000009999"),
        ),
    ]
}

fn fx_wage_rows() -> Vec<WageSnapshotRow> {
    vec![
        WageSnapshotRow {
            driver_cd: 1130,
            driver_name: "山田 太郎".to_string(),
            company: Some("本社".to_string()),
            branch_name: None,
            branch_code: Some(3),
            job_name: Some("乗務".to_string()),
            pay_kubun: Some(2),
            hourly_rate: Some(1250),
            calc_base: Some(200000),
            calc_overtime: Some(31000),
            calc_total: Some(231000),
            paid_base: None,
            paid_overtime: Some(30000),
            working_minutes: Some(10500),
            restraint_missing: false,
        },
        WageSnapshotRow {
            driver_cd: 1702,
            driver_name: String::new(),
            company: None,
            branch_name: Some("釧路".to_string()),
            branch_code: None,
            job_name: None,
            pay_kubun: None,
            hourly_rate: None,
            calc_base: None,
            calc_overtime: None,
            calc_total: None,
            paid_base: None,
            paid_overtime: None,
            working_minutes: None,
            restraint_missing: true,
        },
    ]
}

fn fx_sha(s: &str) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(s.as_bytes()))
}
