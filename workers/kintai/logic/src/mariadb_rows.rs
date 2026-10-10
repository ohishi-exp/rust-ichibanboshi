//! 社内 MariaDB のテキストプロトコルの行 → JSON (4 種)。
//!
//! root の `src/kintai_repo.rs` の `row_to_json`・`all_row_to_json`・`rest_row_to_json`・`reading_date_row_to_json`
//! の写し (対応表は `workers/kintai/README.md`。**撤去までは片方を直したらもう片方も直す**)。同じキー・同じ null 扱い:
//!
//! - 元のタプルで `String` の列は NULL ならエラー (mysql_async の `FromRow` が落ちて 502 になるのと同じ)
//! - `Option<String>` の列は NULL を JSON の null に (0 や空文字にしない)
//! - `Option<i64>` の列はテキストから整数に戻す。戻せなければエラー
//! - 不正な UTF-8 は NULL と区別してエラー
//!
//! エラーは 502 (`MariaDB query failed: rows:<種別>`)。値そのものは本文に出さない。

use kintai_mysql::response::Row;
use serde_json::{json, Value};

use crate::common::{mariadb_fail, Fail};

/// 1 行を `N` 列の文字列 (NULL は `None`) として読む。列数が違う・UTF-8 でなければエラー。
fn cells<const N: usize>(row: &Row) -> Result<[Option<&str>; N], Fail> {
    if row.len() != N {
        return Err(mariadb_fail("rows:shape"));
    }
    let mut out = [None; N];
    for (slot, cell) in out.iter_mut().zip(row) {
        *slot = match cell {
            None => None,
            Some(bytes) => Some(std::str::from_utf8(bytes).map_err(|_| mariadb_fail("rows:utf8"))?),
        };
    }
    Ok(out)
}

/// 元で `String` の列。NULL はエラー。
fn required(cell: Option<&str>) -> Result<&str, Fail> {
    cell.ok_or_else(|| mariadb_fail("rows:null"))
}

/// 元で `Option<i64>` の列。NULL は null、整数として読めなければエラー。
fn int(cell: Option<&str>) -> Result<Option<i64>, Fail> {
    cell.map(|s| s.parse::<i64>().map_err(|_| mariadb_fail("rows:int")))
        .transpose()
}

/// `EVENTS_SQL` の 7 列 (元 `row_to_json`)。
pub fn event_row(row: &Row) -> Result<Value, Fail> {
    let [datetime, end_datetime, driver_id, source, state, unko_no, vehicle] = cells::<7>(row)?;
    Ok(json!({
        "datetime": required(datetime)?,
        "end_datetime": end_datetime,
        "driver_id": int(driver_id)?,
        "source": required(source)?,
        "state": state,
        "unko_no": unko_no,
        "vehicle": vehicle,
    }))
}

/// `ALL_EVENTS_SQL` の 5 列 (元 `all_row_to_json`)。`unko_no` / `vehicle` は**キーごと出さない**。
pub fn all_event_row(row: &Row) -> Result<Value, Fail> {
    let [datetime, end_datetime, driver_id, source, state] = cells::<5>(row)?;
    Ok(json!({
        "datetime": required(datetime)?,
        "end_datetime": end_datetime,
        "driver_id": int(driver_id)?,
        "source": required(source)?,
        "state": state,
    }))
}

/// `REST_EVENTS_SQL` の 6 列 (元 `rest_row_to_json`)。`vehicle` キーは無い。
pub fn rest_row(row: &Row) -> Result<Value, Fail> {
    let [datetime, end_datetime, driver_id, source, state, unko_no] = cells::<6>(row)?;
    Ok(json!({
        "datetime": required(datetime)?,
        "end_datetime": end_datetime,
        "driver_id": int(driver_id)?,
        "source": required(source)?,
        "state": state,
        "unko_no": unko_no,
    }))
}

/// `OPERATION_READING_DATES_SQL` の 6 列 (元 `reading_date_row_to_json`)。
pub fn reading_date_row(row: &Row) -> Result<Value, Fail> {
    let [driver_cd, unko_no, reading_date, run_date, departure_at, return_at] = cells::<6>(row)?;
    Ok(json!({
        "driver_cd": int(driver_cd)?,
        "unko_no": required(unko_no)?,
        "reading_date": reading_date,
        "run_date": run_date,
        "departure_at": departure_at,
        "return_at": return_at,
    }))
}

/// 全行を `to_json` で変換する。1 行でも失敗すれば全体がエラー (元の `exec` が 1 行の失敗で全体を落とすのと同じ)。
pub fn rows_to_json(
    rows: &[Row],
    to_json: fn(&Row) -> Result<Value, Fail>,
) -> Result<Vec<Value>, Fail> {
    rows.iter().map(to_json).collect()
}
