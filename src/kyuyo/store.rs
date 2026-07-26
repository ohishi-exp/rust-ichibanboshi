//! 給与の SQLite derived store (Refs #106 Phase 1)。
//!
//! 設計は `docs/plan-kyuyo-sqlite-store.md` が正。源泉 (給与大臣 OHKEN) が
//! source of truth で、このファイルは**応答型の serde JSON をそのまま保存する
//! 配信キャッシュ** — 消して再 sync すれば全量再構築できるため、バックアップも
//! migration 管理もしない (`PRAGMA user_version` 不一致は drop → 再作成)。
//!
//! `sqlite.rs` (uriage の LocalStore) と同じ作法: rusqlite は同期 API なので
//! `tokio::task::spawn_blocking` に逃がし、Connection は `Arc<Mutex<_>>` で共有する
//! (低 throughput な service のため Mutex 競合は無視できる)。

use std::sync::Arc;

use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::Mutex;

use super::logic::{EmployeeRow, PayrollRow};

/// schema 版。互換を壊す変更をしたら +1 する — 旧版のファイルは open 時に
/// drop → 再作成され、次の read-through / sync で埋まり直す (derived store)。
pub const KYUYO_STORE_SCHEMA_VERSION: i32 = 1;

#[derive(Debug)]
pub enum KyuyoStoreError {
    OpenFailed(String),
    QueryError(String),
    JoinError(String),
    /// 保存済み row_json が現在の応答型に deserialize できない (schema 版の
    /// 上げ忘れ等)。呼び出し側は live 読みへフォールバックする。
    CorruptRow(String),
}

impl std::fmt::Display for KyuyoStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenFailed(m) => write!(f, "kyuyo store open failed: {m}"),
            Self::QueryError(m) => write!(f, "kyuyo store query error: {m}"),
            Self::JoinError(m) => write!(f, "kyuyo store join error: {m}"),
            Self::CorruptRow(m) => write!(f, "kyuyo store corrupt row: {m}"),
        }
    }
}

impl std::error::Error for KyuyoStoreError {}

/// キャッシュ命中 1 件 (payroll)。行は保存時の応答と等価。
#[derive(Debug, Clone)]
pub struct CachedPayroll {
    pub rows: Vec<PayrollRow>,
    pub warnings: Vec<String>,
    pub synced_at: String,
}

/// キャッシュ命中 1 件 (employees)。
#[derive(Debug, Clone)]
pub struct CachedEmployees {
    pub employees: Vec<EmployeeRow>,
    pub company_name: String,
    pub warnings: Vec<String>,
    pub synced_at: String,
}

/// store の trait 面。route handler はこれ経由でしか触らない (テストで差し替え可)。
#[async_trait]
pub trait KyuyoStoreApi: Send + Sync {
    async fn get_payroll(
        &self,
        company: &str,
        month: &str,
    ) -> Result<Option<CachedPayroll>, KyuyoStoreError>;

    async fn put_payroll(
        &self,
        company: &str,
        month: &str,
        rows: &[PayrollRow],
        warnings: &[String],
        synced_at: &str,
    ) -> Result<(), KyuyoStoreError>;

    async fn get_employees(
        &self,
        company: &str,
        nendo: i32,
    ) -> Result<Option<CachedEmployees>, KyuyoStoreError>;

    async fn put_employees(
        &self,
        company: &str,
        nendo: i32,
        employees: &[EmployeeRow],
        company_name: &str,
        warnings: &[String],
        synced_at: &str,
    ) -> Result<(), KyuyoStoreError>;
}

pub type DynKyuyoStore = Arc<dyn KyuyoStoreApi>;

/// 無効時 (`sqlite_path` 空 / open 失敗) の代替。常に miss・書き込みは無視 —
/// 全経路が従来どおりの live 読みになる (キャッシュ化で読み機能を SQLite の
/// 健全性に依存させない、plan の不変条件)。
pub struct NoopKyuyoStore;

#[async_trait]
impl KyuyoStoreApi for NoopKyuyoStore {
    async fn get_payroll(
        &self,
        _company: &str,
        _month: &str,
    ) -> Result<Option<CachedPayroll>, KyuyoStoreError> {
        Ok(None)
    }

    async fn put_payroll(
        &self,
        _company: &str,
        _month: &str,
        _rows: &[PayrollRow],
        _warnings: &[String],
        _synced_at: &str,
    ) -> Result<(), KyuyoStoreError> {
        Ok(())
    }

    async fn get_employees(
        &self,
        _company: &str,
        _nendo: i32,
    ) -> Result<Option<CachedEmployees>, KyuyoStoreError> {
        Ok(None)
    }

    async fn put_employees(
        &self,
        _company: &str,
        _nendo: i32,
        _employees: &[EmployeeRow],
        _company_name: &str,
        _warnings: &[String],
        _synced_at: &str,
    ) -> Result<(), KyuyoStoreError> {
        Ok(())
    }
}

pub struct KyuyoStore {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for KyuyoStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KyuyoStore").finish_non_exhaustive()
    }
}

fn futures_lock(m: &Mutex<Connection>) -> tokio::sync::MutexGuard<'_, Connection> {
    tokio::runtime::Handle::current().block_on(m.lock())
}

fn q(e: rusqlite::Error) -> KyuyoStoreError {
    KyuyoStoreError::QueryError(e.to_string())
}

impl KyuyoStore {
    /// 指定パス (or `:memory:`) を open し、schema を保証する。
    /// 親ディレクトリが無ければ作る (LocalStore と同じ crash-loop 対策)。
    pub fn open(path: &str) -> Result<Self, KyuyoStoreError> {
        if path != ":memory:" {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        KyuyoStoreError::OpenFailed(format!(
                            "create_dir_all({}) failed: {e}",
                            parent.display()
                        ))
                    })?;
                }
            }
        }
        let conn =
            Connection::open(path).map_err(|e| KyuyoStoreError::OpenFailed(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// schema 初期化。`user_version` が現行版と違えば全テーブル drop → 再作成
    /// (derived store — データは源泉から再構築できるため migration しない)。
    fn init(conn: &Connection) -> Result<(), KyuyoStoreError> {
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(q)?;
        if version != KYUYO_STORE_SCHEMA_VERSION {
            conn.execute_batch(
                "DROP TABLE IF EXISTS kyuyo_payroll;
                 DROP TABLE IF EXISTS kyuyo_employees;
                 DROP TABLE IF EXISTS kyuyo_sync_state;",
            )
            .map_err(q)?;
        }
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS kyuyo_payroll (
               company TEXT NOT NULL,
               month   TEXT NOT NULL,
               seq     INTEGER NOT NULL,   -- 応答配列の順序保存 (0..)
               row_json TEXT NOT NULL,
               PRIMARY KEY (company, month, seq)
             );
             CREATE TABLE IF NOT EXISTS kyuyo_employees (
               company TEXT NOT NULL,
               nendo   INTEGER NOT NULL,
               seq     INTEGER NOT NULL,
               row_json TEXT NOT NULL,
               PRIMARY KEY (company, nendo, seq)
             );
             CREATE TABLE IF NOT EXISTS kyuyo_sync_state (
               scope TEXT NOT NULL PRIMARY KEY,
               synced_at TEXT NOT NULL,
               row_count INTEGER NOT NULL,
               company_name TEXT NOT NULL DEFAULT '',
               warnings_json TEXT NOT NULL
             );
             PRAGMA user_version = {KYUYO_STORE_SCHEMA_VERSION};",
        ))
        .map_err(q)
    }
}

fn payroll_scope(company: &str, month: &str) -> String {
    format!("payroll:{company}:{month}")
}

fn employees_scope(company: &str, nendo: i32) -> String {
    format!("employees:{company}:{nendo}")
}

/// sync_state 1 行 (内部用)。
struct SyncState {
    synced_at: String,
    company_name: String,
    warnings: Vec<String>,
}

fn read_sync_state(conn: &Connection, scope: &str) -> Result<Option<SyncState>, KyuyoStoreError> {
    let row = conn
        .query_row(
            "SELECT synced_at, company_name, warnings_json FROM kyuyo_sync_state WHERE scope = ?1",
            params![scope],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                ))
            },
        )
        .optional()
        .map_err(q)?;
    let Some((synced_at, company_name, warnings_json)) = row else {
        return Ok(None);
    };
    let warnings: Vec<String> = serde_json::from_str(&warnings_json)
        .map_err(|e| KyuyoStoreError::CorruptRow(format!("warnings_json: {e}")))?;
    Ok(Some(SyncState {
        synced_at,
        company_name,
        warnings,
    }))
}

#[async_trait]
impl KyuyoStoreApi for KyuyoStore {
    async fn get_payroll(
        &self,
        company: &str,
        month: &str,
    ) -> Result<Option<CachedPayroll>, KyuyoStoreError> {
        let (company, month) = (company.to_string(), month.to_string());
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let Some(state) = read_sync_state(&guard, &payroll_scope(&company, &month))? else {
                return Ok(None);
            };
            let mut stmt = guard
                .prepare(
                    "SELECT row_json FROM kyuyo_payroll
                     WHERE company = ?1 AND month = ?2
                     ORDER BY seq ASC",
                )
                .map_err(q)?;
            let jsons = stmt
                .query_map(params![company, month], |r| r.get::<_, String>(0))
                .map_err(q)?
                .collect::<Result<Vec<String>, _>>()
                .map_err(q)?;
            let rows = jsons
                .iter()
                .map(|j| serde_json::from_str::<PayrollRow>(j))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| KyuyoStoreError::CorruptRow(format!("payroll row_json: {e}")))?;
            Ok(Some(CachedPayroll {
                rows,
                warnings: state.warnings,
                synced_at: state.synced_at,
            }))
        })
        .await
        .map_err(|e| KyuyoStoreError::JoinError(e.to_string()))?
    }

    async fn put_payroll(
        &self,
        company: &str,
        month: &str,
        rows: &[PayrollRow],
        warnings: &[String],
        synced_at: &str,
    ) -> Result<(), KyuyoStoreError> {
        let (company, month, synced_at) = (
            company.to_string(),
            month.to_string(),
            synced_at.to_string(),
        );
        // 順序は応答配列そのまま (seq) — キャッシュ応答を live と等価に保つ。
        // 同一社員の複数支給回もそのまま別行になる
        let encoded: Vec<String> = rows
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()
            .map_err(|e| KyuyoStoreError::CorruptRow(format!("payroll serialize: {e}")))?;
        let warnings_json = serde_json::to_string(warnings)
            .map_err(|e| KyuyoStoreError::CorruptRow(format!("warnings serialize: {e}")))?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = futures_lock(&conn);
            let tx = guard.transaction().map_err(q)?;
            tx.execute(
                "DELETE FROM kyuyo_payroll WHERE company = ?1 AND month = ?2",
                params![company, month],
            )
            .map_err(q)?;
            for (seq, json) in encoded.iter().enumerate() {
                tx.execute(
                    "INSERT INTO kyuyo_payroll (company, month, seq, row_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![company, month, seq as i64, json],
                )
                .map_err(q)?;
            }
            tx.execute(
                "INSERT INTO kyuyo_sync_state (scope, synced_at, row_count, company_name, warnings_json)
                 VALUES (?1, ?2, ?3, '', ?4)
                 ON CONFLICT (scope) DO UPDATE SET
                   synced_at = excluded.synced_at,
                   row_count = excluded.row_count,
                   warnings_json = excluded.warnings_json",
                params![
                    payroll_scope(&company, &month),
                    synced_at,
                    encoded.len() as i64,
                    warnings_json
                ],
            )
            .map_err(q)?;
            tx.commit().map_err(q)
        })
        .await
        .map_err(|e| KyuyoStoreError::JoinError(e.to_string()))?
    }

    async fn get_employees(
        &self,
        company: &str,
        nendo: i32,
    ) -> Result<Option<CachedEmployees>, KyuyoStoreError> {
        let company = company.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let Some(state) = read_sync_state(&guard, &employees_scope(&company, nendo))? else {
                return Ok(None);
            };
            let mut stmt = guard
                .prepare(
                    "SELECT row_json FROM kyuyo_employees
                     WHERE company = ?1 AND nendo = ?2
                     ORDER BY seq ASC",
                )
                .map_err(q)?;
            let jsons = stmt
                .query_map(params![company, nendo], |r| r.get::<_, String>(0))
                .map_err(q)?
                .collect::<Result<Vec<String>, _>>()
                .map_err(q)?;
            let employees = jsons
                .iter()
                .map(|j| serde_json::from_str::<EmployeeRow>(j))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| KyuyoStoreError::CorruptRow(format!("employee row_json: {e}")))?;
            Ok(Some(CachedEmployees {
                employees,
                company_name: state.company_name,
                warnings: state.warnings,
                synced_at: state.synced_at,
            }))
        })
        .await
        .map_err(|e| KyuyoStoreError::JoinError(e.to_string()))?
    }

    async fn put_employees(
        &self,
        company: &str,
        nendo: i32,
        employees: &[EmployeeRow],
        company_name: &str,
        warnings: &[String],
        synced_at: &str,
    ) -> Result<(), KyuyoStoreError> {
        let (company, company_name, synced_at) = (
            company.to_string(),
            company_name.to_string(),
            synced_at.to_string(),
        );
        let encoded: Vec<String> = employees
            .iter()
            .map(serde_json::to_string)
            .collect::<Result<_, _>>()
            .map_err(|e| KyuyoStoreError::CorruptRow(format!("employee serialize: {e}")))?;
        let warnings_json = serde_json::to_string(warnings)
            .map_err(|e| KyuyoStoreError::CorruptRow(format!("warnings serialize: {e}")))?;
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = futures_lock(&conn);
            let tx = guard.transaction().map_err(q)?;
            tx.execute(
                "DELETE FROM kyuyo_employees WHERE company = ?1 AND nendo = ?2",
                params![company, nendo],
            )
            .map_err(q)?;
            for (seq, json) in encoded.iter().enumerate() {
                tx.execute(
                    "INSERT INTO kyuyo_employees (company, nendo, seq, row_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![company, nendo, seq as i64, json],
                )
                .map_err(q)?;
            }
            tx.execute(
                "INSERT INTO kyuyo_sync_state (scope, synced_at, row_count, company_name, warnings_json)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT (scope) DO UPDATE SET
                   synced_at = excluded.synced_at,
                   row_count = excluded.row_count,
                   company_name = excluded.company_name,
                   warnings_json = excluded.warnings_json",
                params![
                    employees_scope(&company, nendo),
                    synced_at,
                    encoded.len() as i64,
                    company_name,
                    warnings_json
                ],
            )
            .map_err(q)?;
            tx.commit().map_err(q)
        })
        .await
        .map_err(|e| KyuyoStoreError::JoinError(e.to_string()))?
    }
}
