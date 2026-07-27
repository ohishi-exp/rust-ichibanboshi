//! 拘束サマリの SQLite store (Refs #106 Phase 3)。
//!
//! 設計は `docs/plan-kyuyo-sqlite-store.md` が正。source of truth は
//! **Cloudflare R2 の summary latest** (nuxt-dtako-admin の relay が theearth
//! scrape / 勤怠取り込みで書く原本)。このファイルは relay が push してくる写しで、
//! wage-report の素材 (当月+前月 × theearth+timecard) を 1 fetch で返すための
//! 配信キャッシュ — 消えても relay の resummarize (全月) を回せば再構築できる。
//!
//! 行は relay のサマリ JSON を **verbatim 保存** (解釈しない — kyuyo/kintai store
//! と同じ素通し哲学)。`kyuyo/store.rs` と同じ作法 (rusqlite + `Arc<Mutex<_>>` +
//! `spawn_blocking`)。

use std::sync::Arc;

use async_trait::async_trait;
use rusqlite::{params, Connection, OptionalExtension};
use tokio::sync::Mutex;

/// schema 版。互換を壊す変更をしたら +1 (旧版は open 時に drop → 再作成)。
pub const RESTRAINT_STORE_SCHEMA_VERSION: i32 = 1;

#[derive(Debug)]
pub enum RestraintStoreError {
    OpenFailed(String),
    QueryError(String),
    JoinError(String),
}

impl std::fmt::Display for RestraintStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenFailed(m) => write!(f, "restraint store open failed: {m}"),
            Self::QueryError(m) => write!(f, "restraint store query error: {m}"),
            Self::JoinError(m) => write!(f, "restraint store join error: {m}"),
        }
    }
}

impl std::error::Error for RestraintStoreError {}

/// push される 1 乗務員分。`summary_json` は relay のサマリ JSON verbatim
/// (noData マーカーの時は None)。
#[derive(Debug, Clone)]
pub struct RestraintEntry {
    pub driver_cd: String,
    pub no_data: bool,
    pub summary_json: Option<String>,
    pub fetched_at: Option<String>,
    pub last_verified_at: Option<String>,
}

/// sync 済み 1 件 (メタのみ)。
#[derive(Debug, Clone)]
pub struct RestraintSyncedRow {
    pub source: String,
    pub month: String,
    pub synced_at: String,
    pub row_count: i64,
}

/// 読み出し結果 1 ヶ月分 (source 単位)。
#[derive(Debug, Clone, Default)]
pub struct RestraintMonth {
    pub entries: Vec<RestraintEntry>,
    /// 最後に push を受けた時刻 (RFC3339)。一度も受けていなければ None。
    pub synced_at: Option<String>,
}

#[async_trait]
pub trait RestraintStoreApi: Send + Sync {
    /// 乗務員単位の upsert (**listed driver のみ**、replace-all ではない)。
    /// relay は取り込みの範囲 (乗務員CD range) ごとに push するため、載っていない
    /// 乗務員を消してはいけない。1 リクエスト = 1 トランザクション。
    async fn upsert(
        &self,
        comp_id: &str,
        source: &str,
        ym: &str,
        entries: &[RestraintEntry],
        synced_at: &str,
    ) -> Result<(), RestraintStoreError>;

    /// (comp, source, ym) の全乗務員分を返す (driver_cd 昇順)。
    async fn month(
        &self,
        comp_id: &str,
        source: &str,
        ym: &str,
    ) -> Result<RestraintMonth, RestraintStoreError>;

    /// comp の sync 済み (source, month) 一覧 (Refs nuxt-dtako-admin#460)。
    /// 月タブの「高速表示可」バッジ用メタデータのみ。
    async fn synced(&self, comp_id: &str) -> Result<Vec<RestraintSyncedRow>, RestraintStoreError>;
}

pub type DynRestraintStore = Arc<dyn RestraintStoreApi>;

/// 無効時 (`sqlite_path` 空 / open 失敗) の代替。push / wage-source とも
/// これが刺さっている間は route 側で 503 を返す (このストアはキャッシュではなく
/// 配信の一次置き場なので、黙って空を返すと「データが消えた」ように見える)。
pub struct DisabledRestraintStore;

#[async_trait]
impl RestraintStoreApi for DisabledRestraintStore {
    async fn upsert(
        &self,
        _comp_id: &str,
        _source: &str,
        _ym: &str,
        _entries: &[RestraintEntry],
        _synced_at: &str,
    ) -> Result<(), RestraintStoreError> {
        Err(RestraintStoreError::OpenFailed(
            "store disabled".to_string(),
        ))
    }

    async fn month(
        &self,
        _comp_id: &str,
        _source: &str,
        _ym: &str,
    ) -> Result<RestraintMonth, RestraintStoreError> {
        Err(RestraintStoreError::OpenFailed(
            "store disabled".to_string(),
        ))
    }

    async fn synced(&self, _comp_id: &str) -> Result<Vec<RestraintSyncedRow>, RestraintStoreError> {
        Err(RestraintStoreError::OpenFailed(
            "store disabled".to_string(),
        ))
    }
}

pub struct RestraintStore {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for RestraintStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RestraintStore").finish_non_exhaustive()
    }
}

fn futures_lock(m: &Mutex<Connection>) -> tokio::sync::MutexGuard<'_, Connection> {
    tokio::runtime::Handle::current().block_on(m.lock())
}

fn q(e: rusqlite::Error) -> RestraintStoreError {
    RestraintStoreError::QueryError(e.to_string())
}

impl RestraintStore {
    /// 指定パス (or `:memory:`) を open し、schema を保証する。
    pub fn open(path: &str) -> Result<Self, RestraintStoreError> {
        if path != ":memory:" {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        RestraintStoreError::OpenFailed(format!(
                            "create_dir_all({}) failed: {e}",
                            parent.display()
                        ))
                    })?;
                }
            }
        }
        let conn =
            Connection::open(path).map_err(|e| RestraintStoreError::OpenFailed(e.to_string()))?;
        Self::init(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn init(conn: &Connection) -> Result<(), RestraintStoreError> {
        let version: i32 = conn
            .query_row("PRAGMA user_version", [], |r| r.get(0))
            .map_err(q)?;
        if version != RESTRAINT_STORE_SCHEMA_VERSION {
            conn.execute_batch(
                "DROP TABLE IF EXISTS restraint_summary;
                 DROP TABLE IF EXISTS restraint_sync_state;",
            )
            .map_err(q)?;
        }
        conn.execute_batch(&format!(
            "CREATE TABLE IF NOT EXISTS restraint_summary (
               comp_id TEXT NOT NULL,
               source  TEXT NOT NULL,      -- 'theearth' | 'timecard'
               ym      TEXT NOT NULL,      -- 'YYYY-MM'
               driver_cd TEXT NOT NULL,
               no_data INTEGER NOT NULL DEFAULT 0,
               summary_json TEXT,          -- relay のサマリ JSON verbatim (noData は NULL)
               fetched_at TEXT,
               last_verified_at TEXT,
               PRIMARY KEY (comp_id, source, ym, driver_cd)
             );
             CREATE TABLE IF NOT EXISTS restraint_sync_state (
               scope TEXT NOT NULL PRIMARY KEY,  -- comp:source:ym
               synced_at TEXT NOT NULL,
               row_count INTEGER NOT NULL
             );
             PRAGMA user_version = {RESTRAINT_STORE_SCHEMA_VERSION};",
        ))
        .map_err(q)
    }
}

fn scope(comp_id: &str, source: &str, ym: &str) -> String {
    format!("{comp_id}:{source}:{ym}")
}

#[async_trait]
impl RestraintStoreApi for RestraintStore {
    async fn upsert(
        &self,
        comp_id: &str,
        source: &str,
        ym: &str,
        entries: &[RestraintEntry],
        synced_at: &str,
    ) -> Result<(), RestraintStoreError> {
        let (comp_id, source, ym, synced_at) = (
            comp_id.to_string(),
            source.to_string(),
            ym.to_string(),
            synced_at.to_string(),
        );
        let entries = entries.to_vec();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = futures_lock(&conn);
            let tx = guard.transaction().map_err(q)?;
            for e in &entries {
                tx.execute(
                    "INSERT INTO restraint_summary
                       (comp_id, source, ym, driver_cd, no_data, summary_json, fetched_at, last_verified_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT (comp_id, source, ym, driver_cd) DO UPDATE SET
                       no_data = excluded.no_data,
                       summary_json = excluded.summary_json,
                       fetched_at = excluded.fetched_at,
                       last_verified_at = excluded.last_verified_at",
                    params![
                        comp_id,
                        source,
                        ym,
                        e.driver_cd,
                        e.no_data as i64,
                        e.summary_json,
                        e.fetched_at,
                        e.last_verified_at
                    ],
                )
                .map_err(q)?;
            }
            let count: i64 = tx
                .query_row(
                    "SELECT COUNT(*) FROM restraint_summary
                     WHERE comp_id = ?1 AND source = ?2 AND ym = ?3",
                    params![comp_id, source, ym],
                    |r| r.get(0),
                )
                .map_err(q)?;
            tx.execute(
                "INSERT INTO restraint_sync_state (scope, synced_at, row_count)
                 VALUES (?1, ?2, ?3)
                 ON CONFLICT (scope) DO UPDATE SET
                   synced_at = excluded.synced_at,
                   row_count = excluded.row_count",
                params![scope(&comp_id, &source, &ym), synced_at, count],
            )
            .map_err(q)?;
            tx.commit().map_err(q)
        })
        .await
        .map_err(|e| RestraintStoreError::JoinError(e.to_string()))?
    }

    async fn month(
        &self,
        comp_id: &str,
        source: &str,
        ym: &str,
    ) -> Result<RestraintMonth, RestraintStoreError> {
        let (comp_id, source, ym) = (comp_id.to_string(), source.to_string(), ym.to_string());
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let synced_at = guard
                .query_row(
                    "SELECT synced_at FROM restraint_sync_state WHERE scope = ?1",
                    params![scope(&comp_id, &source, &ym)],
                    |r| r.get::<_, String>(0),
                )
                .optional()
                .map_err(q)?;
            let mut stmt = guard
                .prepare(
                    "SELECT driver_cd, no_data, summary_json, fetched_at, last_verified_at
                     FROM restraint_summary
                     WHERE comp_id = ?1 AND source = ?2 AND ym = ?3
                     ORDER BY driver_cd ASC",
                )
                .map_err(q)?;
            let entries = stmt
                .query_map(params![comp_id, source, ym], |r| {
                    Ok(RestraintEntry {
                        driver_cd: r.get(0)?,
                        no_data: r.get::<_, i64>(1)? != 0,
                        summary_json: r.get(2)?,
                        fetched_at: r.get(3)?,
                        last_verified_at: r.get(4)?,
                    })
                })
                .map_err(q)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(q)?;
            Ok(RestraintMonth { entries, synced_at })
        })
        .await
        .map_err(|e| RestraintStoreError::JoinError(e.to_string()))?
    }

    async fn synced(&self, comp_id: &str) -> Result<Vec<RestraintSyncedRow>, RestraintStoreError> {
        let prefix = format!("{comp_id}:");
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT scope, synced_at, row_count FROM restraint_sync_state
                     WHERE scope LIKE ?1 || '%'
                     ORDER BY scope ASC",
                )
                .map_err(q)?;
            let rows = stmt
                .query_map(params![prefix], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(q)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(q)?;
            // scope = '{comp}:{source}:{ym}'。comp_id に ':' は入らない (route 検証済み)
            Ok(rows
                .into_iter()
                .filter_map(|(scope, synced_at, row_count)| {
                    let rest = scope.strip_prefix(&prefix)?;
                    let (source, month) = rest.split_once(':')?;
                    Some(RestraintSyncedRow {
                        source: source.to_string(),
                        month: month.to_string(),
                        synced_at,
                        row_count,
                    })
                })
                .collect())
        })
        .await
        .map_err(|e| RestraintStoreError::JoinError(e.to_string()))?
    }
}
