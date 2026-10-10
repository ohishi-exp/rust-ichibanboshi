//! `/api/schema/columns` の本体 (Refs #322)。
//! SQL・Raw 型・組み立ては `ichiban_logic::schema`、行の詰め直しは `crate::rows::schema`。
//! `table` が `運転日報明細` 以外・欠落のときは 400 (オンプレ版は任意のテーブルを受ける。ユーザー決定で絞った)。
//! 応答は包まない素の配列 (オンプレ版の `Json<Vec<ColumnInfo>>`)。

use ichiban_logic::schema::{build_columns, ColumnsQuery, COLUMNS_SQL};
use tiberius::ToSql;
use worker::Env;

use super::{fetch_rows, QUERY_TIMEOUT};
use crate::probe_logic::{Failure, Route};
use crate::rows;

/// 経路判定で上の口に当たったとき。テーブル名は許可リストを通った後でも @P1 にバインドする。
pub(crate) async fn handle(env: &Env, _route: Route, query: &str) -> Result<String, Failure> {
    let q: ColumnsQuery = serde_urlencoded::from_str(query).map_err(|_| Failure::BadRequest)?;
    let table = q.table().ok_or(Failure::BadRequest)?;
    let params: [&dyn ToSql; 1] = [&table];
    let rows = fetch_rows(env, COLUMNS_SQL, &params, QUERY_TIMEOUT).await?;
    let columns = build_columns(&rows::schema::columns(&rows));
    // 文字列と整数・null だけの型なので失敗しない
    Ok(serde_json::to_string(&columns).unwrap_or_default())
}
