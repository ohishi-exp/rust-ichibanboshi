//! `/api/leave/days`・`/api/leave/employees` の本体 (Refs ohishi-exp/rust-leave-worker#1)。
//! SQL・Raw 型・組み立ては `ichiban_logic::leave`、行の詰め直しは `crate::rows::leave`。
//! days は `from`・`to` の欠落・不正・逆順・92 日超を 400 (本文なし)。得意先C も日付も @P にバインドする。

use ichiban_logic::api::EMPLOYEES_SOURCE;
use ichiban_logic::leave::{
    build_leave_days, build_leave_employees, LeaveDaysQuery, LEAVE_CUSTOMER_CODE,
    LEAVE_DAYS_SOURCE, LEAVE_DAYS_SQL, LEAVE_EMPLOYEES_SQL,
};
use tiberius::ToSql;
use worker::Env;

use super::{fetch_rows, list, QUERY_TIMEOUT};
use crate::probe_logic::{Failure, Route};
use crate::rows;

/// 経路判定で上の 2 本に当たったとき。
pub(crate) async fn handle(env: &Env, route: Route, query: &str) -> Result<String, Failure> {
    match route {
        Route::LeaveDays => days(env, query).await,
        Route::LeaveEmployees => employees(env).await,
        // routes::dispatch は上の 2 本しか渡さない
        _ => Err(Failure::BadRequest),
    }
}

/// `GET /api/leave/days`。バインドの順: @P1 得意先C, @P2 from (以上), @P3 to の翌日 (未満)。
async fn days(env: &Env, query: &str) -> Result<String, Failure> {
    let q: LeaveDaysQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let plan = q.plan().ok_or(Failure::BadRequest)?;
    let (from, to) = (plan.from.as_str(), plan.to_exclusive.as_str());
    let params: [&dyn ToSql; 3] = [&LEAVE_CUSTOMER_CODE, &from, &to];
    let rows = fetch_rows(env, LEAVE_DAYS_SQL, &params, QUERY_TIMEOUT).await?;
    let data = build_leave_days(&rows::leave::days(&rows));
    Ok(list(LEAVE_DAYS_SOURCE, data))
}

/// `GET /api/leave/employees`。クエリは取らない (bind なし)。
async fn employees(env: &Env) -> Result<String, Failure> {
    let rows = fetch_rows(env, LEAVE_EMPLOYEES_SQL, &[], QUERY_TIMEOUT).await?;
    let data = build_leave_employees(&rows::leave::employees(&rows));
    Ok(list(EMPLOYEES_SOURCE, data))
}
