//! 休息の開始の補正 (`kintai.rest_start_overrides`、migration 011) の読み書きを実 DB で確かめる (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! - `put_rest_start_overrides` は同じ鍵 (テナント・乗務員・休息の開始) を置き換える
//! - `rest_start_overrides` は休息の開始が範囲 `[from, to)` の行だけを、JST の壁時計で返す。他のテナントの行は見えない

mod common;

use alc_worker_db::PgClient;
use chrono::{NaiveDate, NaiveDateTime};
use common::{database_url, ensure_schema};
use kintai_logic::fold_events::{pushed_window, PushedEventsRead};
use kintai_logic::rest_overrides::RestStartOverride;
use kintai_pg::FoldStore;
use uuid::Uuid;

async fn store(tenant: Uuid) -> FoldStore {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let (client, conn) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    FoldStore::new(PgClient::new(client), tenant)
}

fn at(d: u32, h: u32, m: u32, s: u32) -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2026, 7, d)
        .unwrap()
        .and_hms_opt(h, m, s)
        .unwrap()
}

fn ov(driver: i64, rest: NaiveDateTime, last: NaiveDateTime) -> RestStartOverride {
    RestStartOverride {
        driver_cd: driver,
        rest_start: rest,
        last_move: last,
        unko_no: "2607150330460000000233".into(),
    }
}

#[tokio::test]
async fn put_replaces_the_same_key_and_list_reads_the_range() {
    let (tenant, other) = (Uuid::new_v4(), Uuid::new_v4());
    let (s, o) = (store(tenant).await, store(other).await);
    let rest = at(17, 16, 46, 9);
    // 1 回目は 16:45:40、2 回目 (同じ鍵) で 16:45:46 に置き換わる
    assert_eq!(
        s.put_rest_start_overrides(vec![ov(11, rest, at(17, 16, 45, 40))])
            .await
            .unwrap(),
        1
    );
    let second = vec![
        ov(11, rest, at(17, 16, 45, 46)),
        ov(11, at(31, 16, 37, 0), at(31, 16, 36, 30)),
    ];
    assert_eq!(s.put_rest_start_overrides(second).await.unwrap(), 2);
    o.put_rest_start_overrides(vec![ov(11, rest, at(17, 16, 45, 1))])
        .await
        .unwrap();

    let span = pushed_window("2026-07-17 00:00:00", "2026-07-18 00:00:00").unwrap();
    assert_eq!(
        s.rest_start_overrides(span).await.unwrap(),
        vec![ov(11, rest, at(17, 16, 45, 46))]
    );
    let month = pushed_window("2026-07-01 00:00:00", "2026-08-01 00:00:00").unwrap();
    assert_eq!(s.rest_start_overrides(month).await.unwrap().len(), 2);
    // 他のテナントは自分の 1 件だけ
    assert_eq!(
        o.rest_start_overrides(month).await.unwrap(),
        vec![ov(11, rest, at(17, 16, 45, 1))]
    );
}
