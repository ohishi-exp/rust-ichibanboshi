//! `/api/sales/customer-yoy`・`/api/sales/customer-yoy-by-dept` の本体 (Refs #322)。
//! SQL・Raw 型・組み立ては `ichiban_logic::sales_yoy`、行の詰め直しは `crate::rows::sales_yoy`。
//! どちらも当期・前年の 2 本 (by-dept は部門一覧を足して 3 本) を `super::fetch_rows_many` の 1 接続で流す。
//! クエリが読めない (数字でない limit・min_prev 等) ときはオンプレ版 (axum の Query) と同じく 400。

use ichiban_logic::sales_yoy::{
    build_customer_yoy, build_customer_yoy_by_dept, customer_yoy_by_dept_sql,
    CustomerYoyByDeptQuery, CustomerYoyQuery, CUSTOMER_YOY_BY_DEPT_SOURCE, CUSTOMER_YOY_SOURCE,
    CUSTOMER_YOY_SQL,
};
use ichiban_logic::sql::DEPARTMENTS_SQL;
use tiberius::ToSql;
use worker::Env;

use super::{fetch_rows_many, list, QUERY_TIMEOUT};
use crate::probe_logic::{Failure, Route};
use crate::rows;

/// 経路判定で上の口に当たったとき。
pub(crate) async fn handle(env: &Env, route: Route, query: &str) -> Result<String, Failure> {
    match route {
        Route::SalesCustomerYoyByDept => customer_yoy_by_dept(env, query).await,
        _ => customer_yoy(env, query).await,
    }
}

/// `GET /api/sales/customer-yoy`。オンプレ版 `customer_yoy` と同じ既定値・SQL・bind の順。
async fn customer_yoy(env: &Env, query: &str) -> Result<String, Failure> {
    let q: CustomerYoyQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let p = q.period();
    let (from, to) = (p.from_date.as_str(), p.to_date.as_str());
    let (prev_from, prev_to) = (p.prev_from.as_str(), p.prev_to.as_str());
    let cur: [&dyn ToSql; 2] = [&from, &to];
    let prev: [&dyn ToSql; 2] = [&prev_from, &prev_to];
    let queries: [(&str, &[&dyn ToSql]); 2] = [(CUSTOMER_YOY_SQL, &cur), (CUSTOMER_YOY_SQL, &prev)];
    let results = fetch_rows_many(env, &queries, QUERY_TIMEOUT).await?;
    let cur_rows = rows::sales_yoy::customer_totals(&results[0]);
    let prev_rows = rows::sales_yoy::customer_totals(&results[1]);
    Ok(list(
        CUSTOMER_YOY_SOURCE,
        build_customer_yoy(&p, &cur_rows, &prev_rows),
    ))
}

/// `GET /api/sales/customer-yoy-by-dept`。オンプレ版 `customer_yoy_by_dept` と同じ既定値・SQL・bind の順。
/// 部門の指定があれば @P3 に bind する。部門一覧はオンプレ版では別接続だが、ここでは同じ接続の 3 本目。
async fn customer_yoy_by_dept(env: &Env, query: &str) -> Result<String, Failure> {
    let q: CustomerYoyByDeptQuery =
        serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let p = q.period();
    let dept = q.department();
    let sql = customer_yoy_by_dept_sql(dept.is_some());
    let (from, to) = (p.from_date.as_str(), p.to_date.as_str());
    let (prev_from, prev_to) = (p.prev_from.as_str(), p.prev_to.as_str());
    let d = dept.as_deref().unwrap_or_default();
    // バインドの順: @P1 from, @P2 to, (部門指定時のみ) @P3 部門
    let cur_all: [&dyn ToSql; 3] = [&from, &to, &d];
    let prev_all: [&dyn ToSql; 3] = [&prev_from, &prev_to, &d];
    let n = if dept.is_some() { 3 } else { 2 };
    let queries: [(&str, &[&dyn ToSql]); 3] = [
        (&sql, &cur_all[..n]),
        (&sql, &prev_all[..n]),
        (DEPARTMENTS_SQL, &[]),
    ];
    let results = fetch_rows_many(env, &queries, QUERY_TIMEOUT).await?;
    let cur_rows = rows::sales_yoy::customer_dept_totals(&results[0]);
    let prev_rows = rows::sales_yoy::customer_dept_totals(&results[1]);
    let departments = rows::departments(&results[2]);
    Ok(list(
        CUSTOMER_YOY_BY_DEPT_SOURCE,
        build_customer_yoy_by_dept(&p, dept, &cur_rows, &prev_rows, departments),
    ))
}
