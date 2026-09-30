//! DO `KyuyoState` の SQLite に置く給与の derived store (Refs #322)。オンプレ版 `src/kyuyo/store.rs` と同じ表・同じ
//! 中身 (応答型の serde JSON を verbatim に保存)。DDL と文は `kyuyo_logic::store_keys`、行の encode / decode は
//! `kyuyo_logic::service`。
//!
//! put は await を挟まずに文を続けて流す (DO の暗黙のトランザクションで一度に確定する)。途中の文が失敗したら
//! その scope の sync_state を消して、行の欠けたキャッシュを命中させない。

use kyuyo_logic::payroll::{EmployeeRow, PayrollRow};
use kyuyo_logic::service::{decode_rows, decode_warnings, encode_rows, encode_warnings};
use kyuyo_logic::store_keys::{
    employees_scope, payroll_scope, EMPLOYEES_DELETE_SQL, EMPLOYEES_INSERT_SQL, EMPLOYEES_ROWS_SQL,
    EMPLOYEES_SYNC_STATE_UPSERT_SQL, PAYROLL_DELETE_SQL, PAYROLL_INSERT_SQL, PAYROLL_ROWS_SQL,
    PAYROLL_SYNC_STATE_UPSERT_SQL, SYNC_STATE_DELETE_SQL, SYNC_STATE_SELECT_SQL,
};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use worker::{SqlStorage, SqlStorageValue};

/// キャッシュ命中 1 件。
pub(crate) struct Cached<T> {
    pub(crate) rows: Vec<T>,
    pub(crate) company_name: String,
    pub(crate) warnings: Vec<String>,
    pub(crate) synced_at: String,
}

/// 読めない・書けない (中身は持たない。ログにも出さない)。
pub(crate) struct StoreError;

impl From<worker::Error> for StoreError {
    fn from(_: worker::Error) -> Self {
        StoreError
    }
}

impl From<String> for StoreError {
    fn from(_: String) -> Self {
        StoreError
    }
}

#[derive(Deserialize)]
struct StateRow {
    synced_at: String,
    company_name: String,
    warnings_json: String,
}

#[derive(Deserialize)]
struct JsonRow {
    row_json: String,
}

fn v(s: &str) -> SqlStorageValue {
    SqlStorageValue::from(s)
}

/// sync_state があれば行を読む (無ければ miss)。行が 1 つでも読めなければ `Err` (呼び出し側は live 読みへ落ちる)。
fn get<T: DeserializeOwned>(
    sql: &SqlStorage,
    scope: &str,
    rows_sql: &str,
    key: Vec<SqlStorageValue>,
) -> Result<Option<Cached<T>>, StoreError> {
    let states: Vec<StateRow> = sql
        .exec(SYNC_STATE_SELECT_SQL, vec![v(scope)])?
        .to_array()?;
    let Some(state) = states.into_iter().next() else {
        return Ok(None);
    };
    let jsons: Vec<JsonRow> = sql.exec(rows_sql, key)?.to_array()?;
    let jsons: Vec<String> = jsons.into_iter().map(|r| r.row_json).collect();
    Ok(Some(Cached {
        rows: decode_rows(&jsons)?,
        company_name: state.company_name,
        warnings: decode_warnings(&state.warnings_json)?,
        synced_at: state.synced_at,
    }))
}

pub(crate) fn get_payroll(
    sql: &SqlStorage,
    company: &str,
    month: &str,
) -> Result<Option<Cached<PayrollRow>>, StoreError> {
    let scope = payroll_scope(company, month);
    get(sql, &scope, PAYROLL_ROWS_SQL, vec![v(company), v(month)])
}

pub(crate) fn get_employees(
    sql: &SqlStorage,
    company: &str,
    nendo: i32,
) -> Result<Option<Cached<EmployeeRow>>, StoreError> {
    let scope = employees_scope(company, nendo);
    let key = vec![v(company), SqlStorageValue::from(nendo)];
    get(sql, &scope, EMPLOYEES_ROWS_SQL, key)
}

/// 1 回の put の書き込み。`key` は (company, month) または (company, nendo)。
struct Put<'a> {
    delete_sql: &'a str,
    insert_sql: &'a str,
    upsert_sql: &'a str,
    scope: String,
    key: Vec<SqlStorageValue>,
    /// sync_state の upsert の scope 以降の引数
    state: Vec<SqlStorageValue>,
}

fn put(sql: &SqlStorage, p: Put<'_>, encoded: &[String]) -> Result<(), StoreError> {
    let write = || -> Result<(), StoreError> {
        sql.exec(p.delete_sql, p.key.clone())?;
        for (seq, json) in encoded.iter().enumerate() {
            let mut args = p.key.clone();
            args.push(SqlStorageValue::from(seq as i64));
            args.push(v(json));
            sql.exec(p.insert_sql, args)?;
        }
        let mut args = vec![v(&p.scope)];
        args.extend(p.state.iter().cloned());
        sql.exec(p.upsert_sql, args)?;
        Ok(())
    };
    let out = write();
    if out.is_err() {
        // 行が欠けたまま命中させない (次の読みは miss → live)
        let _ = sql.exec(SYNC_STATE_DELETE_SQL, vec![v(&p.scope)]);
    }
    out
}

pub(crate) fn put_payroll(
    sql: &SqlStorage,
    company: &str,
    month: &str,
    rows: &[PayrollRow],
    warnings: &[String],
    synced_at: &str,
) -> Result<(), StoreError> {
    let encoded = encode_rows(rows)?;
    let state = vec![
        v(synced_at),
        SqlStorageValue::from(encoded.len() as i64),
        v(&encode_warnings(warnings)),
    ];
    let p = Put {
        delete_sql: PAYROLL_DELETE_SQL,
        insert_sql: PAYROLL_INSERT_SQL,
        upsert_sql: PAYROLL_SYNC_STATE_UPSERT_SQL,
        scope: payroll_scope(company, month),
        key: vec![v(company), v(month)],
        state,
    };
    put(sql, p, &encoded)
}

pub(crate) fn put_employees(
    sql: &SqlStorage,
    company: &str,
    nendo: i32,
    employees: &[EmployeeRow],
    company_name: &str,
    warnings: &[String],
    synced_at: &str,
) -> Result<(), StoreError> {
    let encoded = encode_rows(employees)?;
    let state = vec![
        v(synced_at),
        SqlStorageValue::from(encoded.len() as i64),
        v(company_name),
        v(&encode_warnings(warnings)),
    ];
    let p = Put {
        delete_sql: EMPLOYEES_DELETE_SQL,
        insert_sql: EMPLOYEES_INSERT_SQL,
        upsert_sql: EMPLOYEES_SYNC_STATE_UPSERT_SQL,
        scope: employees_scope(company, nendo),
        key: vec![v(company), SqlStorageValue::from(nendo)],
        state,
    };
    put(sql, p, &encoded)
}
