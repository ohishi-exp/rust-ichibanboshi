//! `/api/surcharge/base` の本体 (Refs #322)。
//! SQL・Raw 型・組み立ては `ichiban_logic::surcharge`、行の詰め直しは `crate::rows::surcharge`。
//! 1 本だけ流す口なので `super::fetch_rows` (1 接続)。
//! クエリが読めない (数字でない limit 等) ときはオンプレ版 (axum の Query) と同じく 400。

use ichiban_logic::surcharge::{build_surcharge_rows, surcharge_sql, SurchargeQuery};
use tiberius::ToSql;
use worker::Env;

use super::{fetch_rows, list, QUERY_TIMEOUT};
use crate::probe_logic::{Failure, Route};
use crate::rows;

/// 経路判定で上の口に当たったとき。オンプレ版 `surcharge_base` と同じ既定値・SQL・bind の順。
pub(crate) async fn handle(env: &Env, _route: Route, query: &str) -> Result<String, Failure> {
    let q: SurchargeQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let p = q.params();
    let (from, to) = (p.from_date.as_str(), p.to_date.as_str());
    // バインドの順: @P1 from, @P2 to
    let params: [&dyn ToSql; 2] = [&from, &to];
    let sql = surcharge_sql(p.kind_filter, p.limit);
    let rows = fetch_rows(env, &sql, &params, QUERY_TIMEOUT).await?;
    let raw = rows::surcharge::base_rows(&rows);
    Ok(list(&p.source_table, build_surcharge_rows(&raw)))
}
