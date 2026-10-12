//! 一番星 SQL Server への接続 (資格情報・TCP・ログイン)。中身は共有 crate `vpc-tds` (workers/shared) で、
//! ここは繋ぎ先 (binding・database・secret) を決めるだけ。1 リクエスト = 1 接続。

use vpc_tds::{Conn, DbError, Target};
use worker::Env;

pub(crate) use vpc_tds::{kind_of, timeout};

/// 本番は Workers VPC の binding `ICHIBAN_VPC`、ローカル検証は var `LOCAL_SQL_ADDR` / `LOCAL_ICHIBAN_SQL_JSON`
/// (本番の vars には置かない。scripts/check-exposure.sh が検査する)。
const TARGET: Target = Target {
    binding: "ICHIBAN_VPC",
    nominal_addr: "ichiban-sql:1433",
    local_addr_var: "LOCAL_SQL_ADDR",
    database: "CAPE#01",
    secret: "ICHIBAN_SQL",
    local_creds_var: "LOCAL_ICHIBAN_SQL_JSON",
};

/// 資格情報を読み、TCP を開いてログインする (database `CAPE#01`、平文 TDS)。
pub(crate) async fn connect(env: &Env) -> Result<Conn, DbError> {
    vpc_tds::connect(env, &TARGET).await
}
