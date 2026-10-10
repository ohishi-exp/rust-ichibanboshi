//! `/api/surcharge/base` の本体 (#322 で移している途中。c39 が埋める)。
//! SQL・Raw 型・組み立ては `ichiban_logic::surcharge`、行の詰め直しは `crate::rows::surcharge`。
//! 1 リクエストで複数の SQL を流す口は `super::fetch_rows_many` (1 接続) を使う。

use worker::Env;

use crate::probe_logic::{Failure, Route};

/// 経路判定で上の口に当たったとき。中身を移すまでは 501 (本文なし)。
pub(crate) async fn handle(_env: &Env, _route: Route, _query: &str) -> Result<String, Failure> {
    Err(Failure::NotImplemented)
}
