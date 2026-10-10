//! `/api/sales/monthly`・`/api/sales/by-department`・`/api/sales/by-customer`・`/api/sales/yoy` の本体 (Refs #322)。
//! SQL・Raw 型・組み立ては `ichiban_logic::sales_monthly`、行の詰め直しは `crate::rows::sales_monthly`。
//! オンプレ版 `src/routes/sales.rs` の同名ハンドラと同じ既定値・応答。違いは by-customer の limit ≤ 0 を 400 にする点だけ。

use ichiban_logic::sales_monthly::{
    build_customer_sales, build_department_sales, build_monthly_sales, build_yoy_comparison,
    by_customer_sql, CustomerQuery, MonthlyQuery, PeriodQuery, YoyQuery, BY_DEPARTMENT_SQL,
    CUSTOMER_SOURCE, DEPARTMENT_SOURCE, MONTHLY_ALL_SOURCE, YOY_SQL,
};
use tiberius::ToSql;
use worker::Env;

use super::{fetch_rows, fetch_rows_many, list, QUERY_TIMEOUT};
use crate::probe_logic::{Failure, Route};
use crate::rows;

/// 経路判定で上の 4 口に当たったとき。
pub(crate) async fn handle(env: &Env, route: Route, query: &str) -> Result<String, Failure> {
    match route {
        Route::SalesMonthly => monthly(env, query).await,
        Route::SalesByDepartment => by_department(env, query).await,
        Route::SalesByCustomer => by_customer(env, query).await,
        Route::SalesYoy => yoy(env, query).await,
        // dispatch はこの 4 口しか渡さない
        _ => Err(Failure::BadRequest),
    }
}

/// `GET /api/sales/monthly`。当期と前年を 1 接続で流す。bind の順: @P1 from, @P2 to, (@P3 部門)。
async fn monthly(env: &Env, query: &str) -> Result<String, Failure> {
    let q: MonthlyQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let r = q.range();
    let scope = q.scope();
    let dept = scope.param();
    let mut cur: Vec<&dyn ToSql> = vec![&r.from, &r.to];
    let mut prev: Vec<&dyn ToSql> = vec![&r.prev_from, &r.prev_to];
    if let Some(p) = &dept {
        cur.push(p);
        prev.push(p);
    }
    let queries: [(&str, &[&dyn ToSql]); 2] =
        [(scope.current_sql(), &cur), (scope.prev_sql(), &prev)];
    let sets = fetch_rows_many(env, &queries, QUERY_TIMEOUT).await?;
    let current = rows::sales_monthly::monthly(&sets[0]);
    let previous = if scope.prev_has_transport_count() {
        rows::sales_monthly::monthly(&sets[1])
    } else {
        rows::sales_monthly::monthly_prev(&sets[1])
    };
    let data = build_monthly_sales(&current, &previous);
    Ok(list(&scope.source_table(), data))
}

/// `GET /api/sales/by-department`。bind の順: @P1 from, @P2 to。
async fn by_department(env: &Env, query: &str) -> Result<String, Failure> {
    let q: PeriodQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let (from, to) = q.dates();
    let params: [&dyn ToSql; 2] = [&from, &to];
    let found = fetch_rows(env, BY_DEPARTMENT_SQL, &params, QUERY_TIMEOUT).await?;
    let raw = rows::sales_monthly::by_department(&found);
    Ok(list(DEPARTMENT_SOURCE, build_department_sales(&raw)))
}

/// `GET /api/sales/by-customer`。`TOP n` は 1..=100、limit ≤ 0 は 400。bind の順: @P1 from, @P2 to。
async fn by_customer(env: &Env, query: &str) -> Result<String, Failure> {
    let q: CustomerQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let top = q.top().ok_or(Failure::BadRequest)?;
    let (from, to) = q.dates();
    let params: [&dyn ToSql; 2] = [&from, &to];
    let found = fetch_rows(env, &by_customer_sql(top), &params, QUERY_TIMEOUT).await?;
    let raw = rows::sales_monthly::by_customer(&found);
    Ok(list(CUSTOMER_SOURCE, build_customer_sales(&raw)))
}

/// `GET /api/sales/yoy`。同じ SQL を今年・前年で 1 接続に 2 回。bind の順: @P1 from, @P2 to。
async fn yoy(env: &Env, query: &str) -> Result<String, Failure> {
    let q: YoyQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let r = q.range();
    let cur: [&dyn ToSql; 2] = [&r.from, &r.to];
    let prev: [&dyn ToSql; 2] = [&r.prev_from, &r.prev_to];
    let queries: [(&str, &[&dyn ToSql]); 2] = [(YOY_SQL, &cur), (YOY_SQL, &prev)];
    let sets = fetch_rows_many(env, &queries, QUERY_TIMEOUT).await?;
    let current = rows::sales_monthly::month_totals(&sets[0]);
    let previous = rows::sales_monthly::month_totals(&sets[1]);
    let data = build_yoy_comparison(&current, &previous);
    Ok(list(MONTHLY_ALL_SOURCE, data))
}
