//! fold の書き込み側 `FoldStoreIo` の Worker 実装 (`kintai_pg::FoldStore`、tokio-postgres の `tenant_tx` + `query_typed*` /
//! `execute_typed`) と root の `KintaiPgStore` (sqlx) に**同じ入力**を与え、7 メソッドの戻りと書いた後の表の中身が一致する
//! ことを実 Postgres で確かめる (Refs #322)。
//!
//! - 表: `kintai.shifts`・`day_summaries`・`day_parts`・`fold_gate` (テナントと `folded_at` は除く)
//! - 畳んだ結果は golden の固定入力 (`kosoku/tests/golden/fold_input.json`) を `fold_driver_month` に通したもの
//! - テナントは経路ごとに新しい UUID (root = A、Worker = B)。同じ DB・同じ migration
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**失敗する** (skip して緑にしない)。CI は worker-kintai.yml の `pg-parity` job。

mod common;

use std::collections::BTreeMap;
use std::time::Instant;

use alc_worker_db::PgClient;
use chrono::NaiveDate;
use common::{database_url, ensure_schema};
use kintai_kosoku::fold_flow::{FoldGateRow, FoldStoreIo};
use kintai_kosoku::kintai_fold::{fold_driver_month, logic_version, FoldUnit};
use kintai_kosoku::kintai_push::{jst_day_bounds, month_date_bounds};
use kintai_kosoku::kosoku::KosokuParams;
use kintai_pg::FoldStore;
use rust_ichibanboshi::kintai_push::KintaiPgStore;
use serde_json::Value;
use uuid::Uuid;

const MONTH: &str = "2026-06";

/// root の経路 (テナント A) と Worker の経路 (テナント B)。
struct Both {
    pool: sqlx::PgPool,
    root: KintaiPgStore,
    worker: FoldStore,
    a: Uuid,
    b: Uuid,
}

async fn both() -> Both {
    let url = database_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Both {
        root: KintaiPgStore::from_pool(pool.clone(), a),
        pool,
        worker: FoldStore::new(PgClient::new(client), b),
        a,
        b,
    }
}

async fn dump(pool: &sqlx::PgPool, sql: &str, tenant: Uuid) -> Value {
    sqlx::query_scalar::<_, Value>(sql)
        .bind(tenant)
        .fetch_one(pool)
        .await
        .expect("dump")
}

const SHIFTS: &str = r#"
SELECT coalesce(jsonb_agg(to_jsonb(t) - 'tenant_id' ORDER BY driver_cd, start_at), '[]'::jsonb)
  FROM kintai.shifts t WHERE tenant_id = $1
"#;

const DAYS: &str = r#"
SELECT coalesce(jsonb_agg(to_jsonb(t) - 'tenant_id' ORDER BY driver_cd, date), '[]'::jsonb)
  FROM kintai.day_summaries t WHERE tenant_id = $1
"#;

const PARTS: &str = r#"
SELECT coalesce(jsonb_agg(to_jsonb(t) - 'tenant_id' ORDER BY driver_cd, shift_start_at, date), '[]'::jsonb)
  FROM kintai.day_parts t WHERE tenant_id = $1
"#;

const GATES: &str = r#"
SELECT coalesce(jsonb_agg(to_jsonb(t) - 'tenant_id' - 'folded_at' ORDER BY month), '[]'::jsonb)
  FROM kintai.fold_gate t WHERE tenant_id = $1
"#;

/// 3 表の中身 (テナント列を除く) が両経路で同じこと。返すのは Worker 側の (shifts, day_summaries, day_parts)。
async fn same_tables(t: &Both) -> (Value, Value, Value) {
    let mut out = Vec::new();
    for sql in [SHIFTS, DAYS, PARTS] {
        let (ra, wb) = (dump(&t.pool, sql, t.a).await, dump(&t.pool, sql, t.b).await);
        assert_eq!(ra, wb, "{sql}");
        out.push(wb);
    }
    let parts = out.pop().unwrap();
    let days = out.pop().unwrap();
    (out.pop().unwrap(), days, parts)
}

fn len(v: &Value) -> usize {
    v.as_array().expect("array").len()
}

/// golden の固定入力を乗務員ごとに畳む (`skipped` は書かない行なので parity には関係しない)。
fn golden_units() -> Vec<FoldUnit> {
    let v: Value = serde_json::from_str(include_str!("../../kosoku/tests/golden/fold_input.json"))
        .expect("fold_input.json");
    assert_eq!(v["month"], MONTH);
    let p = KosokuParams::default();
    let units: Vec<FoldUnit> = v["drivers"]
        .as_object()
        .expect("drivers")
        .iter()
        .map(|(cd, rows)| {
            let rows = rows.as_array().expect("rows").clone();
            fold_driver_month(cd.parse().expect("cd"), MONTH, &p, rows).0
        })
        .collect();
    assert!(
        units.iter().all(|u| !u.is_empty()),
        "golden の乗務員は全員勤務を持つ"
    );
    units
}

/// 同じ unit を別の乗務員CD に付け替える (行の中の乗務員CD も)。
fn as_driver(u: &FoldUnit, cd: i64) -> FoldUnit {
    let mut u = u.clone();
    u.driver_cd = cd;
    u.shifts.iter_mut().for_each(|s| s.driver_cd = cd);
    u.day_summaries.iter_mut().for_each(|d| d.driver_cd = cd);
    u.day_parts.iter_mut().for_each(|p| p.driver_cd = cd);
    u
}

async fn write_both(t: &Both, u: &FoldUnit, fp: &str, ver: &str) {
    t.root
        .write_unit(MONTH, u, fp, ver)
        .await
        .expect("root write_unit");
    t.worker
        .write_unit(MONTH, u, fp, ver)
        .await
        .expect("worker write_unit");
}

async fn same_states(
    t: &Both,
    drivers: &[i64],
) -> BTreeMap<i64, kintai_kosoku::kintai_fold::StoredState> {
    let r = t
        .root
        .stored_states(drivers, MONTH)
        .await
        .expect("root stored_states");
    let w = t
        .worker
        .stored_states(drivers, MONTH)
        .await
        .expect("worker stored_states");
    assert_eq!(r, w);
    w
}

/// 空 → 初回の書き込み → 上書き (前の行が消える) → 空 unit (全部消える) の各段で、`stored_states` の戻りと 3 表の中身が
/// 両経路で一致すること。壊れた月はどちらも書く前に落ちる。
#[tokio::test(flavor = "multi_thread")]
async fn write_unit_and_stored_states_match_the_root_path() {
    let t = both().await;
    let units = golden_units();
    let ver = logic_version(&KosokuParams::default());
    let (fp1, fp2) = ("1".repeat(64), "2".repeat(64));
    let mut drivers: Vec<i64> = units.iter().map(|u| u.driver_cd).collect();
    drivers.push(999_999); // 保存の無い乗務員 (空の値で入る)

    // 空の drivers は空、保存が無ければ全員 0
    assert!(same_states(&t, &[]).await.is_empty());
    let before = same_states(&t, &drivers).await;
    assert_eq!(before.len(), drivers.len());
    assert!(before.values().all(|s| s.is_empty()));

    // 初回
    for u in &units {
        write_both(&t, u, &fp1, &ver).await;
    }
    let (shifts, days, parts) = same_tables(&t).await;
    let n: usize = units.iter().map(|u| u.shifts.len()).sum();
    assert_eq!(len(&shifts), n);
    assert_eq!(
        len(&days),
        units.iter().map(|u| u.day_summaries.len()).sum::<usize>()
    );
    assert_eq!(
        len(&parts),
        units.iter().map(|u| u.day_parts.len()).sum::<usize>()
    );
    assert!(days.to_string().contains("non_working"));
    let states = same_states(&t, &drivers).await;
    assert_eq!(states[&units[0].driver_cd].fingerprints, vec![fp1.clone()]);
    assert!(states[&999_999].is_empty());

    // 上書き: 先頭の乗務員を勤務 1 本ぶんに縮めて別の指紋で書き直す → 前の行は残らない
    let mut short = units[0].clone();
    let keep = short.shifts[0].start_at;
    short.shifts.truncate(1);
    short.day_summaries.retain(|d| d.shift_start_at == keep);
    short.day_parts.retain(|p| p.shift_start_at == keep);
    assert!(units[0].shifts.len() > 1, "上書きで行が減る形にする");
    write_both(&t, &short, &fp2, &ver).await;
    let (shifts, _, _) = same_tables(&t).await;
    assert_eq!(len(&shifts), n - units[0].shifts.len() + 1);
    let s = &same_states(&t, &drivers).await[&units[0].driver_cd];
    assert_eq!((s.fingerprints.clone(), s.shifts), (vec![fp2.clone()], 1));

    // 空 unit: その乗務員の月の行が全部消える (day_summaries / day_parts は CASCADE)
    write_both(
        &t,
        &FoldUnit {
            driver_cd: units[0].driver_cd,
            ..FoldUnit::default()
        },
        &fp2,
        &ver,
    )
    .await;
    same_tables(&t).await;
    assert!(same_states(&t, &drivers).await[&units[0].driver_cd].is_empty());

    // 壊れた月は書く前に落ちる (どちらも)
    assert!(t
        .root
        .write_unit("2026-13", &units[1], &fp1, &ver)
        .await
        .is_err());
    let e = t
        .worker
        .write_unit("2026-13", &units[1], &fp1, &ver)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), "bad month: 2026-13");
    assert!(t.worker.stored_states(&drivers, "x").await.is_err());
    same_tables(&t).await;
}

/// 月ゲート: 無い月は両方 None、書いた値 (digest は `CHAR(64)` なので 64 文字) がそのまま読め、2 回目の書き込みで上書きされる。表の中身も一致する。
#[tokio::test(flavor = "multi_thread")]
async fn fold_gate_matches_the_root_path() {
    let t = both().await;
    assert_eq!(t.root.read_fold_gate(MONTH).await.unwrap(), None);
    assert_eq!(t.worker.read_fold_gate(MONTH).await.unwrap(), None);
    for (dtako, punch) in [("d1", "p1"), ("d2", "p2")] {
        let gate = FoldGateRow {
            dtako_digest: dtako.repeat(32),
            punch_digest: punch.repeat(32),
            logic_version: logic_version(&KosokuParams::default()),
        };
        t.root.write_fold_gate(MONTH, &gate).await.unwrap();
        t.worker.write_fold_gate(MONTH, &gate).await.unwrap();
        let r = t.root.read_fold_gate(MONTH).await.unwrap();
        assert_eq!(r, t.worker.read_fold_gate(MONTH).await.unwrap());
        assert_eq!(r, Some(gate));
        assert_eq!(
            dump(&t.pool, GATES, t.a).await,
            dump(&t.pool, GATES, t.b).await
        );
    }
    assert_eq!(len(&dump(&t.pool, GATES, t.b).await), 1);
    assert_eq!(t.worker.read_fold_gate("2026-07").await.unwrap(), None);
}

/// 打刻の月ゲート材料・運行一覧・運行を持った乗務員CD が、同じ events を持つ 2 テナントで一致する。
/// 窓の外・`alc_app` (押し込み対象外の source)・`unko_no` 空は落ちる。events が無い月の digest も一致する。
#[tokio::test(flavor = "multi_thread")]
async fn event_reads_match_the_root_path() {
    let t = both().await;
    let (m0, m1) = month_date_bounds(MONTH).unwrap();
    let (from, to) = (jst_day_bounds(m0).0, jst_day_bounds(m1).0);

    // events が無い月 (digest は sha256('') の固定値、一覧は空)
    let r = t.root.month_punch_digest(from, to).await.unwrap();
    assert_eq!(r, t.worker.month_punch_digest(from, to).await.unwrap());
    assert!(t
        .worker
        .month_operations(from, to)
        .await
        .unwrap()
        .is_empty());
    assert!(t.worker.operation_driver_cds().await.unwrap().is_empty());

    let rows: [(i64, &str, &str, &str, Option<&str>); 9] = [
        (1130, "2026-06-01 08:00:00+09", "始業", "timecard", None),
        (
            1130,
            "2026-06-01 09:15:30+09",
            "運行開始",
            "dtako",
            Some("26060109153000000012341"),
        ),
        (
            1130,
            "2026-06-02 01:00:00+09",
            "運行終了",
            "dtako",
            Some("26060109153000000012341"),
        ),
        (1130, "2026-06-02 02:00:00+09", "終業", "timecard", None),
        (
            1140,
            "2026-06-10 07:00:00+09",
            "運行開始",
            "dtako",
            Some("26061007000000000099991"),
        ),
        (
            1150,
            "2026-06-11 07:00:00+09",
            "運行開始",
            "alc_app",
            Some("26061107000000000088881"),
        ),
        (
            1160,
            "2026-06-12 07:00:00+09",
            "運行開始",
            "dtako",
            Some(""),
        ),
        // 窓の外 (前月)。運行一覧と digest からは落ち、乗務員CD の集合には入る
        (
            1170,
            "2026-05-20 07:00:00+09",
            "運行開始",
            "dtako",
            Some("26052007000000000077771"),
        ),
        (1170, "2026-06-20 07:00:00+09", "始業", "timecard", None),
    ];
    for tenant in [t.a, t.b] {
        for (cd, at, state, source, unko) in rows {
            sqlx::query(
                "INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw) \
                 VALUES ($1, $2, $3::timestamptz, $4, $5, $6, '{}'::jsonb)",
            )
            .bind(tenant)
            .bind(cd)
            .bind(at)
            .bind(state)
            .bind(source)
            .bind(unko)
            .execute(&t.pool)
            .await
            .expect("seed events");
        }
    }

    let d = t.root.month_punch_digest(from, to).await.unwrap();
    assert_eq!(d, t.worker.month_punch_digest(from, to).await.unwrap());
    assert_ne!(d, r, "events で digest が動く");

    let ops = t.root.month_operations(from, to).await.unwrap();
    assert_eq!(ops, t.worker.month_operations(from, to).await.unwrap());
    let day = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").unwrap();
    assert_eq!(
        ops,
        vec![
            (
                1130,
                "26060109153000000012341".to_string(),
                day("2026-06-01"),
                day("2026-06-02")
            ),
            (
                1140,
                "26061007000000000099991".to_string(),
                day("2026-06-10"),
                day("2026-06-10")
            ),
        ]
    );

    let cds = t.root.operation_driver_cds().await.unwrap();
    assert_eq!(cds, t.worker.operation_driver_cds().await.unwrap());
    assert_eq!(cds, [1130, 1140, 1170].into_iter().collect());
}

/// 100 乗務員ぶんの write_unit を両経路で回し、表の中身が一致すること (所要時間は stderr に出す。判定はしない)。
#[tokio::test(flavor = "multi_thread")]
async fn hundred_drivers_write_the_same_rows() {
    let t = both().await;
    let golden = golden_units();
    let ver = logic_version(&KosokuParams::default());
    let fp = "3".repeat(64);
    let units: Vec<FoldUnit> = (0..100)
        .map(|i| as_driver(&golden[i % golden.len()], 50_000 + i as i64))
        .collect();
    let started = Instant::now();
    for u in &units {
        t.root.write_unit(MONTH, u, &fp, &ver).await.unwrap();
    }
    let root_ms = started.elapsed().as_millis();
    let started = Instant::now();
    for u in &units {
        t.worker.write_unit(MONTH, u, &fp, &ver).await.unwrap();
    }
    let worker_ms = started.elapsed().as_millis();
    eprintln!("write_unit x100: root {root_ms} ms / worker {worker_ms} ms");
    let (shifts, _, _) = same_tables(&t).await;
    assert_eq!(
        len(&shifts),
        units.iter().map(|u| u.shifts.len()).sum::<usize>()
    );
}
