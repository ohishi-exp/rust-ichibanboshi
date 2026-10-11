//! fold の 4 口 (Refs #322): `GET`/`POST /api/kintai/recalc`・`POST /api/kintai/timecard/window`・`GET /api/kintai/stale-months`。
//!
//! 段取り (遡り起点 → 月ゲート → 畳み → 封 → 応答、打刻の置き換え、窓の畳み直し) は共有 crate の
//! `kintai_kosoku::route_flow` (root と同じもの)。ここは I/O の配線と HTTP の入出力だけ:
//!
//! - 読み (`FoldEventsIo`) = [`crate::fold_events`] (alc は auth-worker の RPC、社内 MariaDB は 1 接続)
//! - 書き (`FoldStoreIo`・`TimecardWindowIo`) = kintai-pg の `FoldStore` (Hyperdrive の 1 接続。window は置き換えと畳み直しを
//!   同じ接続で回す)
//! - 時計 = [`WorkerClock`] (`Date.now()`。wasm32 では `Instant`・`SystemTime`・`Utc::now` を使わない)
//!
//! 検査の順は root の殻と同じで、書き込みの口 (POST の 2 本) だけ先頭に認可が入る:
//! 認可 (403 / 503) → 入力 (400 等) → binding (503) → テナント (503) → connect (502) → 段取り (失敗は 503 / 502)。
//! window の畳み直しの失敗は root と同じく 200 のまま `fold_error`・`fold_error_status`・`stale_error` に載せる。
//! 502 の本文と `fold_error` は種別だけ (DB や上流の message を出さない。kintai-logic の `fold_routes`)。

use chrono::{DateTime, FixedOffset};
use kintai_kosoku::fold_flow::FoldClock;
use kintai_kosoku::kintai_push::TimecardWindow;
use kintai_kosoku::route_flow::{apply_timecard_window, fold_into, recalc_page, RecalcRequest};
use kintai_logic::common::Fail;
use kintai_logic::fold_routes::{
    fold_ctx, jst_from_ms, parse_recalc_body, parse_recalc_query, parse_window, FoldRoute, DB_WHAT,
};
use kintai_logic::write_auth::{authorize, WRITE_TOKEN_HEADER};
use kintai_pg::FoldStore;
use worker::{console_log, Date, Env, Request};

use crate::alc::EnvAlc;
use crate::fold_events::{fold_events, EnvMariadb};
use crate::reads::{open_db, stale_months};
use crate::writes::{load_token, read_body};
use crate::OUTPUT_SHA;

/// Worker の時計。`Date.now()` (ミリ秒) から作る。
struct WorkerClock;

impl FoldClock for WorkerClock {
    fn now_jst(&self) -> DateTime<FixedOffset> {
        jst_from_ms(Date::now().as_millis())
    }

    fn monotonic_ms(&self) -> u64 {
        Date::now().as_millis()
    }
}

/// 口ごとに応答の JSON を返す (失敗は平文の [`Fail`])。
pub(crate) async fn serve(
    route: FoldRoute,
    query: &str,
    req: &mut Request,
    env: &Env,
) -> Result<serde_json::Value, Fail> {
    if route.is_write() {
        let token = req.headers().get(WRITE_TOKEN_HEADER).ok().flatten();
        authorize(load_token(env).await.as_deref(), token.as_deref())?;
    }
    match route {
        FoldRoute::RecalcPreview => recalc(parse_recalc_query(query)?, env).await,
        FoldRoute::Recalc => {
            let (content_type, body) = read_body(req).await?;
            recalc(parse_recalc_body(content_type.as_deref(), &body)?, env).await
        }
        FoldRoute::TimecardWindow => {
            let (content_type, body) = read_body(req).await?;
            window(parse_window(content_type.as_deref(), &body)?, env).await
        }
        FoldRoute::StaleMonths => stale_months(query, env).await,
    }
}

/// 全量再計算の 1 ページ (GET は `apply` を持たないので書かない)。
async fn recalc(req: RecalcRequest, env: &Env) -> Result<serde_json::Value, Fail> {
    let (pg, tenant) = open_db(env, DB_WHAT, true).await?;
    let store = FoldStore::new(pg, tenant);
    let (alc, mariadb) = (EnvAlc(env), EnvMariadb(env));
    let repo = fold_events(&alc, &mariadb);
    let clock = WorkerClock;
    let ctx = fold_ctx(OUTPUT_SHA, &clock);
    recalc_page(&repo, &store, &ctx, &req)
        .await
        .map_err(|e| e.into_fail())
}

/// 窓ぶんの打刻を置き換え、`fold` なら変わった乗務員だけ畳み直す。置き換えの失敗は 503 / 502、畳み直しの失敗は 200 のまま。
async fn window(window: TimecardWindow, env: &Env) -> Result<serde_json::Value, Fail> {
    let (pg, tenant) = open_db(env, DB_WHAT, true).await?;
    let store = FoldStore::new(pg, tenant);
    let clock = WorkerClock;
    let result = apply_timecard_window(&store, &window, &clock)
        .await
        .map_err(|e| e.into_fail())?;
    let (w, d, m) = (result.days_written, result.days_deleted, result.misplaced);
    console_log!("kintai timecard/window: written {w}, deleted {d}, misplaced {m}");
    let mut body = serde_json::json!(result);
    if window.fold {
        let (alc, mariadb) = (EnvAlc(env), EnvMariadb(env));
        let repo = fold_events(&alc, &mariadb);
        let ctx = fold_ctx(OUTPUT_SHA, &clock);
        let status_of = |e: kintai_pg::FoldStoreError| {
            let f = e.into_fail();
            (f.status, f.body)
        };
        fold_into(&repo, &store, &ctx, &window, &result, &mut body, status_of).await;
    }
    Ok(body)
}
