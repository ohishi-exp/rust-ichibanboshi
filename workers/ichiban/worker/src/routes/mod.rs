//! 口の本体。1 リクエスト = 1 接続 (`repo::connect`) で、SQL・クエリの検証・組み立ては ichiban-logic を使う。
//! 下の 7 本はこのファイル、移した 15 本 (#322) は領域別のモジュール (`handle` 1 本ずつ):
//! [`sales_monthly`]・[`sales_daily`]・[`sales_yoy`]・[`unchin`]・[`surcharge`]・[`schema`]。
//!
//! - `POST /probe` — ログインして `SELECT 1`。200 `{"ok":true}`
//! - `GET /health` — 同じく `SELECT 1`。200 `{"status":"ok"}` (オンプレ版の commit 等は返さない)
//! - `GET /api/employees`・`/api/vehicles`・`/api/sales/departments`・`/api/sales/vehicle-daily`・
//!   `/api/costs/vehicle-daily` — オンプレ版と同じ path・クエリ・JSON (`ListResponse` は `ApiResponse` と同じ形)
//!
//! 失敗はクエリの不備が 400 (本文なし、オンプレ版と同じ判定)、SQL Server までの失敗が 502
//! `{"ok":false,"stage":…,"kind":…}`。応答にもログにもエラーの生文言・ホスト・ポート・ユーザー名を出さない
//! (ログは口・stage・失敗の種類 `ErrKind`・所要ミリ秒だけ)。

use std::time::Duration;

use ichiban_logic::api::{self, ListResponse};
use ichiban_logic::costs_daily::{build_costs_daily_rows, CostsDailyQuery};
use ichiban_logic::sql;
use ichiban_logic::vehicle_daily::{build_vehicle_daily_rows, VehicleDailyQuery};
use serde::Serialize;
use tiberius::{Row, ToSql};
use worker::{console_error, console_log, Date, Env};

use crate::probe_logic::{log_line, reply_for, ErrKind, Failure, Reply, Route, Stage};
use crate::repo::{connect, kind_of, timeout};
use crate::rows;

mod sales_daily;
mod sales_monthly;
mod sales_yoy;
mod schema;
mod surcharge;
mod unchin;

/// `/probe`・`/health` の `SELECT 1` の上限。
const PING_TIMEOUT: Duration = Duration::from_secs(10);
/// 一覧のクエリの上限 (vehicle-daily / costs-daily は最大 5000 行)。
pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(60);

/// 口を 1 回走らせ、結果をログに 1 行出して応答を返す。`route` は経路判定で口に当たったもの。
pub(crate) async fn run(env: &Env, route: Route, query: &str) -> Reply {
    let started = Date::now().as_millis();
    let outcome = dispatch(env, route, query).await;
    let ms = Date::now().as_millis().saturating_sub(started);
    let name = route.name();
    match &outcome {
        Ok(_) => console_log!("ichiban {name}: ok ({ms} ms)"),
        Err(Failure::BadRequest) => console_log!("ichiban {name}: bad request ({ms} ms)"),
        Err(Failure::Db(stage, kind)) => console_error!("{}", log_line(route, *stage, kind, ms)),
    }
    reply_for(outcome)
}

async fn dispatch(env: &Env, route: Route, query: &str) -> Result<String, Failure> {
    match route {
        Route::Probe => ping(env).await.map(|()| r#"{"ok":true}"#.to_string()),
        Route::Health => ping(env).await.map(|()| r#"{"status":"ok"}"#.to_string()),
        Route::Employees => {
            let rows = fetch_rows(env, sql::EMPLOYEES_SQL, &[], QUERY_TIMEOUT).await?;
            Ok(list(api::EMPLOYEES_SOURCE, rows::employees(&rows)))
        }
        Route::Vehicles => {
            let rows = fetch_rows(env, sql::VEHICLES_SQL, &[], QUERY_TIMEOUT).await?;
            Ok(list(api::VEHICLES_SOURCE, rows::vehicles(&rows)))
        }
        Route::Departments => {
            let rows = fetch_rows(env, sql::DEPARTMENTS_SQL, &[], QUERY_TIMEOUT).await?;
            Ok(list(api::DEPARTMENTS_SOURCE, rows::departments(&rows)))
        }
        Route::VehicleDaily => vehicle_daily(env, query).await,
        Route::CostsDaily => costs_daily(env, query).await,
        Route::SalesMonthly
        | Route::SalesByDepartment
        | Route::SalesByCustomer
        | Route::SalesYoy => sales_monthly::handle(env, route, query).await,
        Route::SalesDaily | Route::SalesCustomerTrend | Route::SalesCustomerDetail => {
            sales_daily::handle(env, route, query).await
        }
        Route::SalesCustomerYoy | Route::SalesCustomerYoyByDept => {
            sales_yoy::handle(env, route, query).await
        }
        Route::UnchinCandidates
        | Route::UnchinSummary
        | Route::UnchinCustomerNet
        | Route::UnchinCustomerNetDetail => unchin::handle(env, route, query).await,
        Route::SurchargeBase => surcharge::handle(env, route, query).await,
        Route::SchemaColumns => schema::handle(env, route, query).await,
        // 経路判定 (`reply_for_route`) で先に弾いている
        Route::NotFound | Route::MethodNotAllowed => Err(Failure::BadRequest),
    }
}

/// `GET /api/sales/vehicle-daily`。オンプレ版 `src/routes/vehicle_daily.rs` の `vehicle_daily` と同じ判定と bind の順。
async fn vehicle_daily(env: &Env, query: &str) -> Result<String, Failure> {
    let q: VehicleDailyQuery =
        serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let f = q.filters().ok_or(Failure::BadRequest)?;
    let (from, to) = (q.from.as_str(), q.to.as_str());
    let origin = sql::like_pattern(f.origin);
    let dest = sql::like_pattern(f.dest);
    // バインドの順: @P1 from, @P2 to, @P3 vehicle, @P4 customer, @P5 origin, @P6 dest, @P7 driver
    let params: [&dyn ToSql; 7] = [
        &from,
        &to,
        &f.vehicle,
        &f.customer,
        &origin,
        &dest,
        &f.driver,
    ];
    let rows = fetch_rows(
        env,
        &sql::vehicle_daily_sql(f.limit),
        &params,
        QUERY_TIMEOUT,
    )
    .await?;
    let raw = rows::vehicle_daily(&rows);
    Ok(list(
        api::VEHICLE_DAILY_SOURCE,
        build_vehicle_daily_rows(&raw),
    ))
}

/// `GET /api/costs/vehicle-daily`。オンプレ版 `src/routes/costs_daily.rs` の `costs_daily` と同じ判定と bind の順。
async fn costs_daily(env: &Env, query: &str) -> Result<String, Failure> {
    let q: CostsDailyQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let f = q.filters().ok_or(Failure::BadRequest)?;
    let (from, to) = (q.from.as_str(), q.to.as_str());
    // バインドの順: @P1 from, @P2 to, @P3 vehicle, @P4 driver, @P5 kind
    let params: [&dyn ToSql; 5] = [&from, &to, &f.vehicle, &f.driver, &f.kind];
    let rows = fetch_rows(env, &sql::costs_daily_sql(f.limit), &params, QUERY_TIMEOUT).await?;
    let raw = rows::costs_daily(&rows);
    Ok(list(api::COSTS_DAILY_SOURCE, build_costs_daily_rows(&raw)))
}

/// `SELECT 1` を流して 1 が返るかを見る (`/probe`・`/health`)。
async fn ping(env: &Env) -> Result<(), Failure> {
    let rows = fetch_rows(env, sql::HEALTH_SQL, &[], PING_TIMEOUT).await?;
    match rows
        .first()
        .and_then(|r| r.try_get::<i32, _>(0).ok().flatten())
    {
        Some(1) => Ok(()),
        _ => Err(Failure::Db(Stage::Query, ErrKind::Other)),
    }
}

/// 接続して 1 本流し、最初の結果セットを返す。接続は毎回閉じる。
/// bind が無ければオンプレ版と同じく `simple_query` (SQL batch)、あれば `query` (sp_executesql)。
pub(crate) async fn fetch_rows(
    env: &Env,
    sql: &str,
    params: &[&dyn ToSql],
    limit: Duration,
) -> Result<Vec<Row>, Failure> {
    let mut client = connect(env).await?;
    let rows = timeout(limit, async {
        let stream = if params.is_empty() {
            client.simple_query(sql).await
        } else {
            client.query(sql, params).await
        }
        .map_err(|e| kind_of(&e))?;
        stream.into_first_result().await.map_err(|e| kind_of(&e))
    })
    .await;
    let _ = client.close().await;
    match rows {
        None => Err(Failure::Db(Stage::Query, ErrKind::Timeout)),
        Some(Err(kind)) => Err(Failure::Db(Stage::Query, kind)),
        Some(Ok(rows)) => Ok(rows),
    }
}

/// 1 接続で `queries` (SQL と bind の組) を順に流し、それぞれの最初の結果セットを同じ順で返す。接続は最後に 1 回閉じる。
/// 1 リクエストで 2〜3 本流す口 (monthly・yoy・daily・customer-trend・customer-detail・customer-yoy・
/// customer-yoy-by-dept) 用。bind の有無で `simple_query` / `query` を選ぶ規則は [`fetch_rows`] と同じ。
/// `limit` は全体 (接続後の全クエリ) の上限。1 本でも失敗すれば残りは流さず `Stage::Query` で返す。
#[allow(dead_code)] // 領域別のモジュール (#322 の c35〜c39) が使い始めるまで呼び手が無い
pub(crate) async fn fetch_rows_many(
    env: &Env,
    queries: &[(&str, &[&dyn ToSql])],
    limit: Duration,
) -> Result<Vec<Vec<Row>>, Failure> {
    let mut client = connect(env).await?;
    let results = timeout(limit, async {
        let mut out = Vec::with_capacity(queries.len());
        for (sql, params) in queries {
            let stream = if params.is_empty() {
                client.simple_query(*sql).await
            } else {
                client.query(*sql, params).await
            }
            .map_err(|e| kind_of(&e))?;
            out.push(stream.into_first_result().await.map_err(|e| kind_of(&e))?);
        }
        Ok(out)
    })
    .await;
    let _ = client.close().await;
    match results {
        None => Err(Failure::Db(Stage::Query, ErrKind::Timeout)),
        Some(Err(kind)) => Err(Failure::Db(Stage::Query, kind)),
        Some(Ok(rows)) => Ok(rows),
    }
}

/// `{"source_table":…,"data":[…]}` (オンプレ版の `ApiResponse` と同じ形)。
pub(crate) fn list<T: Serialize>(source_table: &str, data: T) -> String {
    let body = ListResponse {
        source_table: source_table.to_string(),
        data,
    };
    // 文字列・数値・bool だけの型なので失敗しない (serde_json は f64 の NaN も null にする)
    serde_json::to_string(&body).unwrap_or_default()
}
