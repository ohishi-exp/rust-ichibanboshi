//! `/api/sales/daily`・`/api/sales/customer-trend`・`/api/sales/customer-detail` の本体。
//! オンプレ版 `src/routes/sales.rs` の `daily`・`customer_trend`・`customer_detail` と同じ既定値・SQL・bind の順・JSON。
//! SQL・Raw 型・組み立ては `ichiban_logic::sales_daily`、行の詰め直しは `crate::rows::sales_daily`。
//!
//! 400 (本文なし) にするのは: クエリが読めない (オンプレ版の axum の Query と同じ)・customer-detail の `code` 欠落・
//! daily の `month` に `-` が無い (オンプレ版は panic)・customer-trend の `limit` ≤ 0。

use ichiban_logic::sales_daily::{
    build_customer_detail, build_customer_trend, build_daily_sales, CustomerDetailQuery,
    CustomerDetailResponse, CustomerTrendQuery, DailyQuery, CUSTOMER_DETAIL_MONTHS_SQL,
    CUSTOMER_DETAIL_NAME_SQL, CUSTOMER_SOURCE, CUSTOMER_TREND_MONTHLY_SQL,
};
use tiberius::{Row, ToSql};
use worker::Env;

use super::{fetch_rows_many, list, QUERY_TIMEOUT};
use crate::probe_logic::{ErrKind, Failure, Route, Stage};
use crate::repo::{connect, kind_of, timeout};
use crate::rows;

/// 経路判定で上の 3 本に当たったとき。
pub(crate) async fn handle(env: &Env, route: Route, query: &str) -> Result<String, Failure> {
    match route {
        Route::SalesDaily => daily(env, query).await,
        Route::SalesCustomerTrend => customer_trend(env, query).await,
        Route::SalesCustomerDetail => customer_detail(env, query).await,
        // routes::dispatch は上の 3 本しか渡さない
        _ => Err(Failure::BadRequest),
    }
}

/// `GET /api/sales/daily`。当期と前年の 2 本を 1 接続で流す。
/// バインドの順: @P1 from, @P2 to, (@P3 `%除外部門%`)。前年も同じ並び。
async fn daily(env: &Env, query: &str) -> Result<String, Failure> {
    let q: DailyQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let p = q.plan().ok_or(Failure::BadRequest)?;
    let (from, to) = (p.from.as_str(), p.to.as_str());
    let (prev_from, prev_to) = (p.prev_from.as_str(), p.prev_to.as_str());
    let pattern = p.exclude_pattern.as_deref();
    let mut cur: Vec<&dyn ToSql> = vec![&from, &to];
    let mut prev: Vec<&dyn ToSql> = vec![&prev_from, &prev_to];
    if let Some(pattern) = &pattern {
        cur.push(pattern);
        prev.push(pattern);
    }
    let queries = [
        (p.current_sql.as_str(), cur.as_slice()),
        (p.prev_sql.as_str(), prev.as_slice()),
    ];
    let mut results = fetch_rows_many(env, &queries, QUERY_TIMEOUT)
        .await?
        .into_iter();
    let current = rows::sales_daily::daily(&results.next().unwrap_or_default());
    let prev = rows::sales_daily::daily_prev(&results.next().unwrap_or_default());
    Ok(list(&p.source_table, build_daily_sales(&current, &prev)))
}

/// `GET /api/sales/customer-trend`。TOP n 得意先 → (n 件あれば) 全得意先の月別、を 1 接続で流す。
async fn customer_trend(env: &Env, query: &str) -> Result<String, Failure> {
    let q: CustomerTrendQuery =
        serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let p = q.plan().ok_or(Failure::BadRequest)?;
    let (top, monthly) = customer_trend_rows(env, &p.from, &p.to, &p.top_sql).await?;
    let top = rows::sales_daily::top_customers(&top);
    let monthly = rows::sales_daily::customer_monthly(&monthly);
    Ok(list(CUSTOMER_SOURCE, build_customer_trend(&top, &monthly)))
}

/// customer-trend の 2 本。TOP n が空なら 2 本目は流さない (オンプレ版 `customer_trend_data` と同じ) ので、
/// 全部を流す `fetch_rows_many` は使わずに 1 接続でここで流す。失敗・時間切れの扱いは `fetch_rows_many` と同じ。
/// バインドの順: どちらも @P1 from, @P2 to。
async fn customer_trend_rows(
    env: &Env,
    from: &str,
    to: &str,
    top_sql: &str,
) -> Result<(Vec<Row>, Vec<Row>), Failure> {
    let mut client = connect(env).await?;
    let params: [&dyn ToSql; 2] = [&from, &to];
    let results = timeout(QUERY_TIMEOUT, async {
        let stream = client
            .query(top_sql, &params)
            .await
            .map_err(|e| kind_of(&e))?;
        let top = stream.into_first_result().await.map_err(|e| kind_of(&e))?;
        if top.is_empty() {
            return Ok((top, Vec::new()));
        }
        let stream = client
            .query(CUSTOMER_TREND_MONTHLY_SQL, &params)
            .await
            .map_err(|e| kind_of(&e))?;
        let monthly = stream.into_first_result().await.map_err(|e| kind_of(&e))?;
        Ok((top, monthly))
    })
    .await;
    let _ = client.close().await;
    match results {
        None => Err(Failure::Db(Stage::Query, ErrKind::Timeout)),
        Some(Err(kind)) => Err(Failure::Db(Stage::Query, kind)),
        Some(Ok(rows)) => Ok(rows),
    }
}

/// `GET /api/sales/customer-detail`。得意先名 (TOP 1) と月別の 2 本を 1 接続で流す。バインドは両方 @P1 code。
async fn customer_detail(env: &Env, query: &str) -> Result<String, Failure> {
    let q: CustomerDetailQuery =
        serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let code = q.code.as_str();
    let params: [&dyn ToSql; 1] = [&code];
    let queries = [
        (CUSTOMER_DETAIL_NAME_SQL, &params[..]),
        (CUSTOMER_DETAIL_MONTHS_SQL, &params[..]),
    ];
    let mut results = fetch_rows_many(env, &queries, QUERY_TIMEOUT)
        .await?
        .into_iter();
    let customer_name = rows::sales_daily::customer_name(&results.next().unwrap_or_default());
    let raw = rows::sales_daily::customer_detail(&results.next().unwrap_or_default());
    let body = CustomerDetailResponse {
        customer_code: q.code.clone(),
        customer_name,
        months: build_customer_detail(&raw),
    };
    Ok(list(CUSTOMER_SOURCE, body))
}
