//! タイムカードの SQLite derived store (Refs #106 Phase 2)。
//!
//! 設計は `docs/plan-kyuyo-sqlite-store.md` が正。源泉 (社内 CakePHP) が
//! source of truth で、このファイルは **`/api/kintai/daily` の上流応答をそのまま
//! 保存する配信キャッシュ** — 素通し方針 (行を解釈しない) を保存でも維持するため、
//! 月単位で応答 JSON を丸ごと 1 blob で持つ。消して再 sync (`?refresh=1`) すれば
//! 全量再構築できるため、バックアップも migration 管理もしない。
//!
//! `kyuyo/store.rs` と同じ作法 (rusqlite + `Arc<Mutex<_>>` + `spawn_blocking`)。

use std::sync::Arc;

use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::Mutex;

/// schema 版。互換を壊す変更をしたら +1 (旧版は open 時に drop → 再作成)。
pub const KINTAI_STORE_SCHEMA_VERSION: i32 = 1;

#[derive(Debug)]
pub enum KintaiStoreError {
    OpenFailed(String),
    QueryError(String),
    JoinError(String),
}

impl std::fmt::Display for KintaiStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenFailed(m) => write!(f, "kintai store open failed: {m}"),
            Self::QueryError(m) => write!(f, "kintai store query error: {m}"),
            Self::JoinError(m) => write!(f, "kintai store join error: {m}"),
        }
    }
}

impl std::error::Error for KintaiStoreError {}

/// キャッシュ命中 1 件 — 上流応答の verbatim JSON と鮮度。
#[derive(Debug, Clone)]
pub struct CachedKintai {
    pub response_json: String,
    pub synced_at: String,
}

#[async_trait]
pub trait KintaiStoreApi: Send + Sync {
    async fn get_daily(&self, month: &str) -> Result<Option<CachedKintai>, KintaiStoreError>;

    async fn put_daily(
        &self,
        month: &str,
        response_json: &str,
        row_count: usize,
        synced_at: &str,
    ) -> Result<(), KintaiStoreError>;
}

pub type DynKintaiStore = Arc<dyn KintaiStoreApi>;

/// 無効時 (`sqlite_path` 空 / open 失敗) の代替。常に miss・書き込み無視 =
/// 従来どおりの CakePHP 素通し中継。
pub struct NoopKintaiStore;

#[async_trait]
impl KintaiStoreApi for NoopKintaiStore {
    async fn get_daily(&self, _month: &str) -> Result<Option<CachedKintai>, KintaiStoreError> {
        Ok(None)
    }

    async fn put_daily(
        &self,
        _month: &str,
        _response_json: &str,
        _row_count: usize,
        _synced_at: &str,
    ) -> Result<(), KintaiStoreError> {
        Ok(())
    }
}

pub struct KintaiStore {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for KintaiStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KintaiStore").finish_non_exhaustive()
    }
}

fn futures_lock(m: &Mutex<Connection>) -> tokio::sync::MutexGuard<'_, Connection> {
    tokio::runtime::Handle::current().block_on(m.lock())
}

fn q(e: rusqlite::Error) -> KintaiStoreError {
    KintaiStoreError::QueryError(e.to_string())
}

impl KintaiStore {
    /// 指定パス (or `:memory:`) を open し、schema を保証する。
    pub fn open(path: &str) -> Result<Self, KintaiStoreError> {
        if path != ":memory:" {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        KintaiStoreError::OpenFailed(format!(
                            "create_dir_all({}) failed: {e}",
                            parent.display()
                        ))
                    })?;
                }
            }
        }
        let conn =
            Connection::open(path).map_err(|e| KintaiStoreError::OpenFailed(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn init(conn: &Connection) -> Result<(), KintaiStoreError> {
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(q)?;
        if version != KINTAI_STORE_SCHEMA_VERSION {
            conn.execute_batch("DROP TABLE IF EXISTS kintai_daily;")
                .map_err(q)?;
        }
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS kintai_daily (
               month TEXT NOT NULL PRIMARY KEY,
               response_json TEXT NOT NULL,
               row_count INTEGER NOT NULL,
               synced_at TEXT NOT NULL
             );
             PRAGMA user_version = {KINTAI_STORE_SCHEMA_VERSION};",
        ))
        .map_err(q)
    }
}

#[async_trait]
impl KintaiStoreApi for KintaiStore {
    async fn get_daily(&self, month: &str) -> Result<Option<CachedKintai>, KintaiStoreError> {
        let month = month.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            guard
                .query_row(
                    "SELECT response_json, synced_at FROM kintai_daily WHERE month = ?1",
                    params![month],
                    |r| {
                        Ok(CachedKintai {
                            response_json: r.get(0)?,
                            synced_at: r.get(1)?,
                        })
                    },
                )
                .optional()
                .map_err(q)
        })
        .await
        .map_err(|e| KintaiStoreError::JoinError(e.to_string()))?
    }

    async fn put_daily(
        &self,
        month: &str,
        response_json: &str,
        row_count: usize,
        synced_at: &str,
    ) -> Result<(), KintaiStoreError> {
        let (month, response_json, synced_at) = (
            month.to_string(),
            response_json.to_string(),
            synced_at.to_string(),
        );
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            guard
                .execute(
                    "INSERT INTO kintai_daily (month, response_json, row_count, synced_at)
                     VALUES (?1, ?2, ?3, ?4)
                     ON CONFLICT (month) DO UPDATE SET
                       response_json = excluded.response_json,
                       row_count = excluded.row_count,
                       synced_at = excluded.synced_at",
                    params![month, response_json, row_count as i64, synced_at],
                )
                .map(|_| ())
                .map_err(q)
        })
        .await
        .map_err(|e| KintaiStoreError::JoinError(e.to_string()))?
    }
}
