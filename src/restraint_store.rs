//! 拘束サマリの SQLite store (Refs #106 Phase 3)。
//!
//! 設計は `docs/plan-kyuyo-sqlite-store.md` が正。source of truth は
//! **Cloudflare R2 の summary latest** (nuxt-dtako-admin の relay が theearth
//! scrape / 勤怠取り込みで書く原本)。このファイルは relay が push してくる写しで、
//! wage-report の素材 (当月+前月 × theearth+timecard) を 1 fetch で返すための
//! 配信キャッシュ — 消えても relay の resummarize (全月) を回せば再構築できる。
//!
//! 行は relay のサマリ JSON を **verbatim 保存** (解釈しない — kintai store
//! と同じ素通し哲学)。`kintai_store.rs` と同じ作法 (rusqlite + `Arc<Mutex<_>>` +
//! `spawn_blocking`)。
//!
//! 表の定義・SQL の文字列・bind の値の並び・一覧の分解は共有 crate `kintai-logic` の `restraint`
//! (勤怠 Worker の D1 と同じもの、Refs #322)。ここは rusqlite との往復だけを持つ。

use std::sync::Arc;

use async_trait::async_trait;
use kintai_logic::restraint::{
    month_rows_binds, summary_binds, sync_state_binds, synced_at_binds, synced_binds, synced_rows,
    Bind, MONTH_ROWS_SQL, SCHEMA_SQL, SYNCED_AT_SQL, SYNCED_SQL, UPSERT_SUMMARY_SQL,
    UPSERT_SYNC_STATE_SQL,
};
use rusqlite::types::Value;
use rusqlite::{params_from_iter, Connection, OptionalExtension};
use tokio::sync::Mutex;

/// 行・一覧・1 ヶ月分の型は共有 crate のもの (勤怠 Worker の D1 と同じ)。
pub use kintai_logic::restraint::{RestraintEntry, RestraintMonth, RestraintSyncedRow};

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

/// 共有 crate の bind の値を rusqlite の値へ。
fn values(binds: &[Bind]) -> Vec<Value> {
    binds
        .iter()
        .map(|b| match b {
            Bind::Text(s) => Value::Text(s.clone()),
            Bind::Int(i) => Value::Integer(*i),
            Bind::Null => Value::Null,
        })
        .collect()
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
        // 表の定義は共有 crate の SCHEMA_SQL (D1 の migration と同じファイル)。版はここだけが持つ
        conn.execute_batch(SCHEMA_SQL).map_err(q)?;
        let pragma = format!("PRAGMA user_version = {RESTRAINT_STORE_SCHEMA_VERSION};");
        conn.execute_batch(&pragma).map_err(q)
    }
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
                let binds = summary_binds(&comp_id, &source, &ym, e);
                tx.execute(UPSERT_SUMMARY_SQL, params_from_iter(values(&binds)))
                    .map_err(q)?;
            }
            // row_count は同じ transaction の中の副問い合わせで数える (D1 の batch と同じ SQL)
            let binds = sync_state_binds(&comp_id, &source, &ym, &synced_at);
            tx.execute(UPSERT_SYNC_STATE_SQL, params_from_iter(values(&binds)))
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
                    SYNCED_AT_SQL,
                    params_from_iter(values(&synced_at_binds(&comp_id, &source, &ym))),
                    |r| r.get::<_, String>(0),
                )
                .optional()
                .map_err(q)?;
            let mut stmt = guard.prepare(MONTH_ROWS_SQL).map_err(q)?;
            let binds = month_rows_binds(&comp_id, &source, &ym);
            let entries = stmt
                .query_map(params_from_iter(values(&binds)), |r| {
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
        let comp_id = comp_id.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard.prepare(SYNCED_SQL).map_err(q)?;
            let rows = stmt
                .query_map(params_from_iter(values(&synced_binds(&comp_id))), |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })
                .map_err(q)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(q)?;
            // scope = '{comp}:{source}:{ym}' を分ける (LIKE の wildcard で当たった別 comp は落とす)
            Ok(synced_rows(&comp_id, rows))
        })
        .await
        .map_err(|e| RestraintStoreError::JoinError(e.to_string()))?
    }
}
