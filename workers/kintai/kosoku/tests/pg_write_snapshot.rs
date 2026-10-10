//! Supabase への書き込みの SQL と bind の束が、root から移す前 (a06a4d0) と同じであることを縛る (Refs #322)。
//!
//! 基点の値の測り方: 移す前の root の `src/kintai_push.rs`・`src/kintai_fold.rs` の SQL 定数の本文 (raw string の中身) の
//! sha256 と、同じ固定入力 (下の `fx_*`) から**移す前の bind のコードそのまま**で作った Vec の束の `Debug` の sha256。
//! 移した後の部品 (`delete_days`・`event_columns`・`shift_columns`・`day_summary_columns`・`day_part_columns`) が
//! 同じ値になること = 書き込む内容が変わらないこと。jsonb の列 (`non_working`) は、元が `sqlx::types::Json` で渡していた
//! 値を `serde_json::to_value` した値 (jsonb が持つ値) で比べる。

use chrono::{DateTime, FixedOffset};
use kintai_kosoku::kintai_fold::{
    day_part_columns, day_summary_columns, shift_columns, DayPartRow, DaySummaryRow, FoldUnit,
    ShiftRow, DELETE_SHIFTS_SQL, FOLD_GATE_SELECT_SQL, FOLD_GATE_UPSERT_SQL, INSERT_DAY_PARTS_SQL,
    INSERT_DAY_SUMMARIES_SQL, INSERT_SHIFTS_SQL, STALE_STATE_SQL, STORED_STATES_SQL,
    STORED_STATE_SQL,
};
use kintai_kosoku::kintai_push::{
    delete_days, event_columns, DriverPlan, PushEvent, DELETE_DAYS_SQL, INSERT_EVENTS_SQL,
    MONTH_PUNCH_DIGEST_SQL, STORED_SIGNATURES_SQL, STORED_WINDOW_SIGNATURES_SQL,
};
use kintai_kosoku::kosoku::{NonWorking, NonWorkingKind};

/// (名前, SQL, 基点の sha256)。
const SQLS: [(&str, &str, &str); 14] = [
    (
        "STORED_SIGNATURES_SQL",
        STORED_SIGNATURES_SQL,
        "bd31a4af617cb5c3258fbc7193444a8cdab1030b2b1663710fd652dad1d180c2",
    ),
    (
        "MONTH_PUNCH_DIGEST_SQL",
        MONTH_PUNCH_DIGEST_SQL,
        "bb327cad375cda40c69f30f44ad818bcff5142ff7beece755caed552e35dd567",
    ),
    (
        "STORED_WINDOW_SIGNATURES_SQL",
        STORED_WINDOW_SIGNATURES_SQL,
        "0e19d51a0670ec9280e1acf4281d13fb8b25eb3618e7cbf998955a85a935ac7d",
    ),
    (
        "DELETE_DAYS_SQL",
        DELETE_DAYS_SQL,
        "8092a894f4c9feebce10606beb18acae930d8b11fcc070d43be825eaac17eb21",
    ),
    (
        "INSERT_EVENTS_SQL",
        INSERT_EVENTS_SQL,
        "701b4a95b43fd4047c31aa2d33b6288b806bc368429292b917aa5af070fe8d78",
    ),
    (
        "STORED_STATE_SQL",
        STORED_STATE_SQL,
        "888ab940e8cae8532ca38b7061918b2c72d2d3f4ee256b78ecdcc92f21def995",
    ),
    (
        "STORED_STATES_SQL",
        STORED_STATES_SQL,
        "f06e4e6752c0d142e3af132803e6a10fa84e138ec69f8dcb1f6292d80484d033",
    ),
    (
        "STALE_STATE_SQL",
        STALE_STATE_SQL,
        "80d6d8ac456a1382200f6459438fa110b0f8a4134f33fea3462d6e1b1e5624a8",
    ),
    (
        "DELETE_SHIFTS_SQL",
        DELETE_SHIFTS_SQL,
        "5acb64c2bcc6377ffa50e588b467841f490c06aac57bf508da335fc4a9f8950b",
    ),
    (
        "INSERT_SHIFTS_SQL",
        INSERT_SHIFTS_SQL,
        "8e2aa9c433ef153d0b53f17250734ea3e833463f2f76bb2a6fa571b71440ba22",
    ),
    (
        "INSERT_DAY_SUMMARIES_SQL",
        INSERT_DAY_SUMMARIES_SQL,
        "948b0cc719d44dedc231df9f36d6de11916e47097002481d3c17b55cfb9de79c",
    ),
    (
        "INSERT_DAY_PARTS_SQL",
        INSERT_DAY_PARTS_SQL,
        "0a6fcf08f24afde0a25a7441d63732bd160aa642ae7e09479b41b0475c9ed737",
    ),
    (
        "FOLD_GATE_SELECT_SQL",
        FOLD_GATE_SELECT_SQL,
        "c5645ef6ff0d7bdc678d2a9fec2a6a9e13f72d6bd5edf28f748922e8852d052f",
    ),
    (
        "FOLD_GATE_UPSERT_SQL",
        FOLD_GATE_UPSERT_SQL,
        "006cac45f85ef6468e8bdf778455597b9ca6673e20233df0c069b392063922b5",
    ),
];

/// 書き込み・書き込みの途中の読みの SQL はどれも `tenant_id = $1` で絞る (INSERT は `$1` を tenant_id の列に入れる)。
/// 接続ロールは行単位の権限を素通りするので、これが他テナントに触れない唯一の担保。
#[test]
fn every_sql_pins_the_tenant_to_dollar_one() {
    for (name, sql, _) in SQLS {
        let pinned = sql.contains("tenant_id = $1")
            || (sql.contains("(tenant_id,")
                && (sql.contains("SELECT $1,") || sql.contains("VALUES ($1,")));
        assert!(pinned, "{name} に tenant_id = $1 が無い");
    }
}

/// 表はどれも `kintai.` で修飾する (Worker の接続は `search_path = alc_api` 固定なので、修飾漏れは別の表を引く)。
#[test]
fn every_table_is_schema_qualified() {
    for (name, sql, _) in SQLS {
        for kw in ["FROM ", "INTO ", "JOIN ", "UPDATE "] {
            for (i, _) in sql.match_indices(kw) {
                let rest = &sql[i + kw.len()..];
                assert!(
                    rest.starts_with("kintai.")
                        || rest.starts_with("unnest(")
                        || rest.starts_with("("),
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
    let plans = fx_plans();
    let b = delete_days(&plans);
    assert_eq!(
        fx_sha(&format!("{:?}", (&b.driver_cd, &b.from, &b.to))),
        "3a33aa4599519831ad34a867ed469d301449ea74e40428d3cbc1039d714ab534"
    );
    let ins: String = event_columns(&plans)
        .iter()
        .map(|c| {
            format!(
                "{:?}",
                (
                    &c.driver_cd,
                    &c.occurred_at,
                    &c.state,
                    &c.source,
                    &c.unko_no,
                    &c.raw
                )
            )
        })
        .collect();
    assert_eq!(
        fx_sha(&ins),
        "dd329525f7c1cd09646ab488f044b590d75da10e6d9ddc3788019ce0e0586fe8"
    );

    let unit = fx_unit();
    let s: String = shift_columns(&unit)
        .iter()
        .map(|c| {
            format!(
                "{:?}",
                (&c.driver_cd, &c.start_at, &c.end_at, &c.shift_source)
            )
        })
        .collect();
    assert_eq!(
        fx_sha(&s),
        "c2ce5bce31f14c6de3b4dbd4127cdae29b8ff18500617f30d8bae1acb2336b48"
    );
    let s: String = day_summary_columns(&unit)
        .iter()
        .map(|c| {
            format!(
                "{:?}",
                (
                    &c.driver_cd,
                    &c.date,
                    &c.shift_start_at,
                    &c.shift_source,
                    &c.minutes,
                    &c.non_working
                )
            )
        })
        .collect();
    assert_eq!(
        fx_sha(&s),
        "3d3f4715fb8c60566b4f38d442c63d917ffed419ac5ea669df03285561e2ee50"
    );
    let s: String = day_part_columns(&unit)
        .iter()
        .map(|c| {
            format!(
                "{:?}",
                (
                    &c.driver_cd,
                    &c.shift_start_at,
                    &c.date,
                    &c.restraint_minutes,
                    &c.working_minutes,
                    &c.night_minutes
                )
            )
        })
        .collect();
    assert_eq!(
        fx_sha(&s),
        "a527ab3365d021c18d2bf8c23b69864d838b026227853a5cb14262793a2211c7"
    );
    // 型が付いていること (timestamptz の束は JST の壁時計を +09:00 で渡す)
    let at: &DateTime<FixedOffset> = &event_columns(&plans)[0].occurred_at[0];
    assert_eq!(at.to_rfc3339(), "2026-06-01T08:00:00+09:00");
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

fn fx_unit() -> FoldUnit {
    FoldUnit {
        driver_cd: 1130,
        shifts: vec![
            ShiftRow {
                driver_cd: 1130,
                start_at: fx_dt("2026-06-01 08:00:00"),
                end_at: fx_dt("2026-06-01 18:02:00"),
                shift_source: "timecard",
            },
            ShiftRow {
                driver_cd: 1130,
                start_at: fx_dt("2026-06-02 22:00:00"),
                end_at: fx_dt("2026-06-03 07:00:00"),
                shift_source: "rest",
            },
        ],
        day_summaries: vec![
            DaySummaryRow {
                driver_cd: 1130,
                date: fx_d("2026-06-01"),
                shift_start_at: fx_dt("2026-06-01 08:00:00"),
                shift_source: "timecard",
                restraint_minutes: 602,
                working_minutes: 542,
                break_minutes: 60,
                rest_minus_minutes: 0,
                statutory_minutes: 450,
                within_statutory_overtime_minutes: 30,
                overtime_minutes: 62,
                legal_holiday_minutes: 0,
                night_minutes: 0,
                overtime_night_minutes: 0,
                legal_holiday_night_minutes: 0,
                non_working: vec![NonWorking {
                    start: "2026-06-01 12:00:00".to_string(),
                    end: "2026-06-01 13:00:00".to_string(),
                    kind: NonWorkingKind::LunchWindow,
                }],
            },
            DaySummaryRow {
                driver_cd: 1130,
                date: fx_d("2026-06-02"),
                shift_start_at: fx_dt("2026-06-02 22:00:00"),
                shift_source: "rest",
                restraint_minutes: 540,
                working_minutes: 480,
                break_minutes: 60,
                rest_minus_minutes: 5,
                statutory_minutes: 450,
                within_statutory_overtime_minutes: 30,
                overtime_minutes: 0,
                legal_holiday_minutes: 0,
                night_minutes: 360,
                overtime_night_minutes: 0,
                legal_holiday_night_minutes: 0,
                non_working: vec![],
            },
        ],
        day_parts: vec![
            DayPartRow {
                driver_cd: 1130,
                shift_start_at: fx_dt("2026-06-02 22:00:00"),
                date: fx_d("2026-06-02"),
                restraint_minutes: 120,
                working_minutes: 120,
                night_minutes: 120,
            },
            DayPartRow {
                driver_cd: 1130,
                shift_start_at: fx_dt("2026-06-02 22:00:00"),
                date: fx_d("2026-06-03"),
                restraint_minutes: 420,
                working_minutes: 360,
                night_minutes: 240,
            },
        ],
        skipped: vec![],
    }
}

fn fx_sha(s: &str) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(s.as_bytes()))
}
