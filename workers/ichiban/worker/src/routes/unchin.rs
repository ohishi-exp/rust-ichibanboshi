//! `/api/unchin/candidates`・`/api/unchin/summary`・`/api/unchin/customer-net`・`/api/unchin/customer-net-detail` の本体。
//! オンプレ版 `src/routes/unchin.rs` の 4 本と同じ既定値・bind の順・`source_table`。各 1 本の SQL なので `fetch_rows`。
//! SQL・Raw 型・組み立ては `ichiban_logic::unchin`、行の詰め直しは `crate::rows::unchin`。
//!
//! 400 はクエリが読めないとき (customer-net-detail の `code`/`h` 欠落を含む) だけ。オンプレ版 (axum の Query) と同じ。

use ichiban_logic::unchin::{
    self as logic, UnchinCustomerNetDetailQuery, UnchinCustomerNetQuery, UnchinQuery,
};
use serde::de::DeserializeOwned;
use tiberius::ToSql;
use worker::Env;

use super::{fetch_rows, list, QUERY_TIMEOUT};
use crate::probe_logic::{Failure, Route};
use crate::rows;

/// 経路判定で上の 4 本に当たったとき。
pub(crate) async fn handle(env: &Env, route: Route, query: &str) -> Result<String, Failure> {
    match route {
        Route::UnchinCandidates => candidates(env, parse(query)?).await,
        Route::UnchinSummary => summary(env, parse(query)?).await,
        Route::UnchinCustomerNet => customer_net(env, parse(query)?).await,
        Route::UnchinCustomerNetDetail => customer_net_detail(env, parse(query)?).await,
        // dispatch が上の 4 本しか渡さない
        _ => Err(Failure::BadRequest),
    }
}

fn parse<T: DeserializeOwned>(query: &str) -> Result<T, Failure> {
    serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)
}

/// `GET /api/unchin/candidates?from=&to=&partner_type=&kind=`
async fn candidates(env: &Env, q: UnchinQuery) -> Result<String, Failure> {
    let (from, to) = q.range();
    let (pt, kind) = (q.partner_type(), q.kind());
    // バインドの順: @P1 from, @P2 to
    let params: [&dyn ToSql; 2] = [&from, &to];
    let sql = logic::candidates_sql(pt, kind);
    let rows = fetch_rows(env, &sql, &params, QUERY_TIMEOUT).await?;
    let data = logic::build_unchin_rows(&rows::unchin::candidates(&rows));
    Ok(list(&logic::partner_source(pt, kind), data))
}

/// `GET /api/unchin/summary?from=&to=&partner_type=&kind=`
async fn summary(env: &Env, q: UnchinQuery) -> Result<String, Failure> {
    let (from, to) = q.range();
    let (pt, kind) = (q.partner_type(), q.kind());
    // バインドの順: @P1 from, @P2 to
    let params: [&dyn ToSql; 2] = [&from, &to];
    let sql = logic::summary_sql(pt, kind);
    let rows = fetch_rows(env, &sql, &params, QUERY_TIMEOUT).await?;
    let data = logic::build_unchin_summary_rows(&rows::unchin::summary(&rows));
    Ok(list(&logic::partner_source(pt, kind), data))
}

/// `GET /api/unchin/customer-net?from=&to=&kind=`
async fn customer_net(env: &Env, q: UnchinCustomerNetQuery) -> Result<String, Failure> {
    let (from, to) = q.range();
    let kind = q.kind();
    // バインドの順: @P1 from, @P2 to
    let params: [&dyn ToSql; 2] = [&from, &to];
    let sql = logic::customer_net_sql(kind);
    let rows = fetch_rows(env, &sql, &params, QUERY_TIMEOUT).await?;
    let data = logic::build_unchin_customer_net_rows(&rows::unchin::customer_net(&rows));
    Ok(list(&logic::customer_net_source(kind), data))
}

/// `GET /api/unchin/customer-net-detail?from=&to=&kind=&code=&h=` (`code`/`h` は必須)
async fn customer_net_detail(
    env: &Env,
    q: UnchinCustomerNetDetailQuery,
) -> Result<String, Failure> {
    let (from, to) = q.range();
    let kind = q.kind();
    let (code, h) = (q.code.as_str(), q.h.as_str());
    // バインドの順: @P1 from, @P2 to, @P3 code, @P4 h
    let params: [&dyn ToSql; 4] = [&from, &to, &code, &h];
    let sql = logic::customer_net_detail_sql(kind);
    let rows = fetch_rows(env, &sql, &params, QUERY_TIMEOUT).await?;
    let raw = rows::unchin::customer_net_detail(&rows);
    let data = logic::build_unchin_customer_net_detail_rows(&raw);
    Ok(list(
        &logic::customer_net_detail_source(code, h, kind),
        data,
    ))
}
