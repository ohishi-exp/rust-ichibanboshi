//! fold の書き込み側 `FoldStoreIo` の Worker 実装 (`kintai_pg::FoldStore`、tokio-postgres の `tenant_tx` + `query_typed*` /
//! `execute_typed`) の 9 メソッドを実 Postgres で確かめる (Refs #322)。root の `KintaiPgStore` (sqlx) との突き合わせは
//! やめ、**Worker で書いた行を Worker の読みで読み戻して一致すること (往復) と件数**を確かめる。SQL 本文は共有 crate が
//! sha256 で縛っている (kosoku/tests の snapshot)。ファイル名の `fold_store_parity` は、比較の相手を外す前の名前を残している。
//!
//! - 表: `kintai.shifts`・`day_summaries`・`day_parts`・`fold_gate` (テナントと `folded_at` は除く)
//! - 畳んだ結果は golden の固定入力 (`kosoku/tests/golden/fold_input.json`) を `fold_driver_month` に通したもの
//! - テナントはテストごとに新しい UUID。同じ DB・同じ migration
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**失敗する** (skip して緑にしない)。CI は worker-kintai.yml の `pg-parity` job。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use alc_worker_db::PgClient;
use chrono::NaiveDate;
use common::{database_url, ensure_schema};
use kintai_kosoku::fold_flow::{FoldGateRow, FoldStoreIo};
use kintai_kosoku::kintai_fold::{fold_driver_month, logic_version, FoldUnit};
use kintai_kosoku::kintai_push::{jst_day_bounds, month_date_bounds};
use kintai_kosoku::kosoku::KosokuParams;
use kintai_pg::FoldStore;
use serde_json::Value;
use uuid::Uuid;

const MONTH: &str = "2026-06";

/// Worker の経路 (テナントはテストごとに新しい UUID)。
struct Env {
    pool: sqlx::PgPool,
    worker: FoldStore,
    tenant: Uuid,
}

async fn env() -> Env {
    let url = database_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let tenant = Uuid::new_v4();
    let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Env {
        pool,
        worker: FoldStore::new(PgClient::new(client), tenant),
        tenant,
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

/// 3 表の中身 (テナント列を除く): (shifts, day_summaries, day_parts)。
async fn tables(t: &Env) -> (Value, Value, Value) {
    let mut out = Vec::new();
    for sql in [SHIFTS, DAYS, PARTS] {
        out.push(dump(&t.pool, sql, t.tenant).await);
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

async fn write(t: &Env, u: &FoldUnit, fp: &str, ver: &str) {
    t.worker
        .write_unit(MONTH, u, fp, ver)
        .await
        .expect("worker write_unit");
}

async fn states(
    t: &Env,
    drivers: &[i64],
) -> BTreeMap<i64, kintai_kosoku::kintai_fold::StoredState> {
    t.worker
        .stored_states(drivers, MONTH)
        .await
        .expect("worker stored_states")
}

/// 空 → 初回の書き込み → 上書き (前の行が消える) → 空 unit (全部消える) の各段で、`stored_states` の戻りと 3 表の中身が
/// 書いたものと一致する (往復と件数)。壊れた月は書く前に落ちる。
#[tokio::test(flavor = "multi_thread")]
async fn write_unit_and_stored_states_round_trip() {
    let t = env().await;
    let units = golden_units();
    let ver = logic_version(&KosokuParams::default());
    let (fp1, fp2) = ("1".repeat(64), "2".repeat(64));
    let mut drivers: Vec<i64> = units.iter().map(|u| u.driver_cd).collect();
    drivers.push(999_999); // 保存の無い乗務員 (空の値で入る)

    // 空の drivers は空、保存が無ければ全員 0
    assert!(states(&t, &[]).await.is_empty());
    let before = states(&t, &drivers).await;
    assert_eq!(before.len(), drivers.len());
    assert!(before.values().all(|s| s.is_empty()));

    // 初回: 書いた件数が表の件数と一致する
    for u in &units {
        write(&t, u, &fp1, &ver).await;
    }
    let (shifts, days, parts) = tables(&t).await;
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
    let got = states(&t, &drivers).await;
    for u in &units {
        let s = &got[&u.driver_cd];
        assert_eq!(s.fingerprints, vec![fp1.clone()], "{}", u.driver_cd);
        assert_eq!(s.shifts, u.shifts.len() as i64, "{}", u.driver_cd);
    }
    assert!(got[&999_999].is_empty());

    // 上書き: 先頭の乗務員を勤務 1 本ぶんに縮めて別の指紋で書き直す → 前の行は残らない
    let mut short = units[0].clone();
    let keep = short.shifts[0].start_at;
    short.shifts.truncate(1);
    short.day_summaries.retain(|d| d.shift_start_at == keep);
    short.day_parts.retain(|p| p.shift_start_at == keep);
    assert!(units[0].shifts.len() > 1, "上書きで行が減る形にする");
    write(&t, &short, &fp2, &ver).await;
    let (shifts, days, parts) = tables(&t).await;
    assert_eq!(len(&shifts), n - units[0].shifts.len() + 1);
    let others = &units[1..];
    let day_rows: usize = others.iter().map(|u| u.day_summaries.len()).sum();
    assert_eq!(len(&days), short.day_summaries.len() + day_rows);
    let part_rows: usize = others.iter().map(|u| u.day_parts.len()).sum();
    assert_eq!(len(&parts), short.day_parts.len() + part_rows);
    let s = &states(&t, &drivers).await[&units[0].driver_cd];
    assert_eq!((s.fingerprints.clone(), s.shifts), (vec![fp2.clone()], 1));

    // 空 unit: その乗務員の月の行が全部消える (day_summaries / day_parts は CASCADE)
    let empty = FoldUnit {
        driver_cd: units[0].driver_cd,
        ..FoldUnit::default()
    };
    write(&t, &empty, &fp2, &ver).await;
    let (shifts, days, parts) = tables(&t).await;
    assert_eq!(len(&shifts), n - units[0].shifts.len());
    assert_eq!(len(&days), day_rows);
    assert_eq!(len(&parts), part_rows);
    assert!(states(&t, &drivers).await[&units[0].driver_cd].is_empty());

    // 壊れた月は書く前に落ちる
    let e = t
        .worker
        .write_unit("2026-13", &units[1], &fp1, &ver)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), "bad month: 2026-13");
    assert!(t.worker.stored_states(&drivers, "x").await.is_err());
    assert_eq!(len(&tables(&t).await.0), n - units[0].shifts.len());
}

/// 月ゲート: 無い月は None、書いた値 (digest は `CHAR(64)` なので 64 文字) がそのまま読め、2 回目の書き込みで上書きされる。
#[tokio::test(flavor = "multi_thread")]
async fn fold_gate_round_trips() {
    let t = env().await;
    assert_eq!(t.worker.read_fold_gate(MONTH).await.unwrap(), None);
    for (dtako, punch) in [("d1", "p1"), ("d2", "p2")] {
        let gate = FoldGateRow {
            dtako_digest: dtako.repeat(32),
            punch_digest: punch.repeat(32),
            logic_version: logic_version(&KosokuParams::default()),
        };
        t.worker.write_fold_gate(MONTH, &gate).await.unwrap();
        assert_eq!(t.worker.read_fold_gate(MONTH).await.unwrap(), Some(gate));
        assert_eq!(len(&dump(&t.pool, GATES, t.tenant).await), 1);
    }
    assert_eq!(t.worker.read_fold_gate("2026-07").await.unwrap(), None);
}

/// 打刻の月ゲート材料・運行一覧・運行を持った乗務員CD。窓の外・`alc_app` (押し込み対象外の source)・`unko_no` 空は
/// 落ちる。events が無い月は一覧が空で、digest は events で動く。
#[tokio::test(flavor = "multi_thread")]
async fn event_reads_return_the_fixed_values() {
    let t = env().await;
    let (m0, m1) = month_date_bounds(MONTH).unwrap();
    let (from, to) = (jst_day_bounds(m0).0, jst_day_bounds(m1).0);

    // events が無い月 (一覧は空)
    let empty = t.worker.month_punch_digest(from, to).await.unwrap();
    assert_eq!(
        empty,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
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
    for (cd, at, state, source, unko) in rows {
        sqlx::query(
            "INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw) \
             VALUES ($1, $2, $3::timestamptz, $4, $5, $6, '{}'::jsonb)",
        )
        .bind(t.tenant)
        .bind(cd)
        .bind(at)
        .bind(state)
        .bind(source)
        .bind(unko)
        .execute(&t.pool)
        .await
        .expect("seed events");
    }

    let d = t.worker.month_punch_digest(from, to).await.unwrap();
    assert_eq!(
        d,
        "ea1c9040fd1401ba4a6224840c5a440cbe01641d6824dab83e84e2b6c11bc77c"
    );

    let ops = t.worker.month_operations(from, to).await.unwrap();
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

    let cds = t.worker.operation_driver_cds().await.unwrap();
    assert_eq!(cds, [1130, 1140, 1170].into_iter().collect());
}

/// 100 乗務員ぶんの write_unit を回し、書いた件数が表の件数と一致すること (所要時間は stderr に出す。判定はしない)。
#[tokio::test(flavor = "multi_thread")]
async fn hundred_drivers_round_trip() {
    let t = env().await;
    let golden = golden_units();
    let ver = logic_version(&KosokuParams::default());
    let fp = "3".repeat(64);
    let units: Vec<FoldUnit> = (0..100)
        .map(|i| as_driver(&golden[i % golden.len()], 50_000 + i as i64))
        .collect();
    let started = Instant::now();
    for u in &units {
        t.worker.write_unit(MONTH, u, &fp, &ver).await.unwrap();
    }
    eprintln!(
        "write_unit x100: worker {} ms",
        started.elapsed().as_millis()
    );
    let (shifts, _, _) = tables(&t).await;
    assert_eq!(
        len(&shifts),
        units.iter().map(|u| u.shifts.len()).sum::<usize>()
    );
    let drivers: Vec<i64> = units.iter().map(|u| u.driver_cd).collect();
    let got = states(&t, &drivers).await;
    assert_eq!(got.len(), 100);
    assert!(got.values().all(|s| s.fingerprints == vec![fp.clone()]));
}

/// `stale_state` (月の並びの版の数え上げ) と `recalc_driver_page` (全量再計算の母集団の 1 ページ)。母集団は打刻・保存済み・
/// `extra` の和で、`after`・`stale_only`・`limit`・0 以下の除外が効く。壊れた月・空の並びは落ちる。
#[tokio::test(flavor = "multi_thread")]
async fn stale_state_and_recalc_driver_page_return_the_fixed_values() {
    let t = env().await;
    let units = golden_units();
    let ver = logic_version(&KosokuParams::default());
    let months = vec![MONTH.to_string()];

    // 空の月: stale 0・版の一覧は空、母集団は extra だけ (版の列は CHAR(16))
    let none = BTreeSet::new();
    let r = t.worker.stale_state(&months, &ver).await.unwrap();
    assert_eq!((r.drivers, r.versions.len()), (0, 0));
    let extra = BTreeSet::from([-5, 0, 77]);
    let page = |after, stale_only, limit, extra: BTreeSet<i64>| {
        let t = &t;
        let ver = ver.clone();
        async move {
            t.worker
                .recalc_driver_page(MONTH, &ver, &extra, after, stale_only, limit)
                .await
                .unwrap()
        }
    };
    assert_eq!(page(None, false, 50, extra.clone()).await, vec![77]);
    assert!(page(None, false, 50, none.clone()).await.is_empty());

    // 現行版で 1 人目、古い版で 2 人目を書き、打刻だけの乗務員を 1 人足す
    let (a, b) = (units[0].driver_cd, units[1].driver_cd);
    write(&t, &units[0], &"1".repeat(64), &ver).await;
    write(&t, &units[1], &"2".repeat(64), "0000000000000000").await;
    sqlx::query(
        "INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw) \
         VALUES ($1, 31, '2026-06-03 08:00:00+09', '始業', 'timecard', NULL, '{}'::jsonb)",
    )
    .bind(t.tenant)
    .execute(&t.pool)
    .await
    .expect("seed events");

    let r = t.worker.stale_state(&months, &ver).await.unwrap();
    assert_eq!(r.drivers, 1, "古い版の乗務員だけ");
    assert_eq!(r.logic_version, ver);
    let mut versions = r.versions.clone();
    versions.sort();
    let mut want = vec![ver.clone(), "0000000000000000".to_string()];
    want.sort();
    assert_eq!(versions, want);
    // 月が飛んでいても 1 回で数える (並びの順にも依らない): 2026-05..2026-08 の間の 2026-06 の 1 人を数えるだけ
    let wide = vec!["2026-08".to_string(), "2026-05".to_string()];
    let w = t.worker.stale_state(&wide, &ver).await.unwrap();
    assert_eq!((w.drivers, w.versions.len()), (1, 2));

    let mut all = vec![31, a, b, 77];
    all.sort();
    assert_eq!(page(None, false, 50, extra.clone()).await, all);
    assert_eq!(page(None, false, 2, extra.clone()).await, all[..2].to_vec());
    assert_eq!(
        page(Some(all[1]), false, 50, extra.clone()).await,
        all[2..].to_vec()
    );
    let mut stale = vec![31, b, 77];
    stale.sort();
    assert_eq!(
        page(None, true, 50, extra.clone()).await,
        stale,
        "現行版の行を持つ乗務員は落ちる"
    );

    // 壊れた月・空の並び
    let e = t.worker.stale_state(&[], &ver).await.unwrap_err();
    assert_eq!(e.to_string(), "months が空です");
    let bad = vec!["2026-13".to_string()];
    let e = t.worker.stale_state(&bad, &ver).await.unwrap_err();
    assert_eq!(e.to_string(), "bad month: 2026-13");
    let e = t
        .worker
        .recalc_driver_page("x", &ver, &none, None, false, 5)
        .await
        .unwrap_err();
    assert_eq!(e.to_string(), "bad month: x");
}
