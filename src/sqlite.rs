//! Phase 2 local store (SQLite, issue #762)。
//!
//! 担当者別売上 summary を永続化する。SQL Server (CAPE#01) は source of truth、
//! SQLite は **derived store** で、`compute_person_sum` の結果をキャッシュ + 集計
//! ビュー / drill-down の高速読み出しに使う。`:memory:` でテスト可。
//!
//! rusqlite は同期 API なので、HTTP handler から呼ぶ際は `tokio::task::spawn_blocking`
//! に逃がす。Connection は `Arc<Mutex<Connection>>` で共有 (低 throughput な service
//! のため Mutex 競合は無視できる)。

use std::collections::HashMap;
use std::sync::Arc;

use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::routes::uriage::PersonAccum;

#[derive(Debug)]
pub enum LocalStoreError {
    OpenFailed(String),
    QueryError(String),
    JoinError(String),
}

impl std::fmt::Display for LocalStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OpenFailed(m) => write!(f, "sqlite open failed: {m}"),
            Self::QueryError(m) => write!(f, "sqlite query error: {m}"),
            Self::JoinError(m) => write!(f, "tokio join error: {m}"),
        }
    }
}

impl std::error::Error for LocalStoreError {}

/// 担当者×月×営業所×cal の集計 1 行 (DB から read out した raw 値)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonMonthlyRow {
    pub month: String,
    pub person_name: String,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub kingaku: i64,
    pub yosha_kingaku: i64,
    pub kensuu: i64,
    pub calculated_at: String,
}

/// 担当者×日×営業所×cal の集計 1 行 (drill-down 用)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonDailyRow {
    /// 運行年月日 (`YYYY-MM-DD`)
    pub unko_date: String,
    /// 集計バケット月 (`YYYY-MM`、unko_date の上位 7 文字と一致する想定)
    pub month: String,
    pub person_name: String,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub kingaku: i64,
    pub yosha_kingaku: i64,
    pub kensuu: i64,
    pub calculated_at: String,
}

/// `recalc_jobs` 1 行。`status` 遷移: `queued → computing → computed → r2_synced`。
/// 失敗時は `status='failed' + last_error`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecalcJob {
    pub month: String,
    pub eigyosho_id: i64,
    pub status: String,
    pub fingerprint_before: Option<String>,
    pub fingerprint_after: Option<String>,
    pub raw_path: Option<String>,
    pub created_at: String,
    pub computed_at: Option<String>,
    pub r2_synced_at: Option<String>,
    pub last_error: Option<String>,
}

/// `r2_pending` view の 1 行 (R2 へ送信すべき 月×営業所)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct R2PendingRow {
    pub month: String,
    pub eigyosho_id: i64,
    pub raw_path: String,
    pub fingerprint_after: String,
    pub computed_at: String,
}

/// SQLite local store (Phase 2、担当者別売上 summary)。
#[derive(Clone)]
pub struct LocalStore {
    conn: Arc<Mutex<Connection>>,
}

impl std::fmt::Debug for LocalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalStore").finish_non_exhaustive()
    }
}

impl LocalStore {
    /// 指定パス (or `:memory:`) の SQLite を open し、schema migration を流す。
    ///
    /// 親ディレクトリが存在しない場合は `fs::create_dir_all` で作成する (caller が
    /// mkdir し忘れて crash-loop に陥った実害あり、PR #33 後の deploy で発生)。
    /// 親ディレクトリの作成権限が無い場合は `OpenFailed` を返す (例: `/var/lib/`
    /// など root 所有領域)。
    pub fn open(path: &str) -> Result<Self, LocalStoreError> {
        if path != ":memory:" {
            if let Some(parent) = std::path::Path::new(path).parent() {
                if !parent.as_os_str().is_empty() && !parent.exists() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        LocalStoreError::OpenFailed(format!(
                            "create_dir_all({}) failed: {e}",
                            parent.display()
                        ))
                    })?;
                }
            }
        }
        let conn =
            Connection::open(path).map_err(|e| LocalStoreError::OpenFailed(e.to_string()))?;
        Self::migrate(&conn)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    fn migrate(conn: &Connection) -> Result<(), LocalStoreError> {
        // 担当者×月×営業所×cal 集計。idempotent (CREATE IF NOT EXISTS)。
        // 行は `recalc` 実行毎に (month, eigyosho_id, cal) 単位で delete-then-insert する。
        //
        // recalc_jobs: per (month, eigyosho_id) の recalc 状態 + fingerprint + R2 sync 状態。
        // cal は raw row には影響しない (compute_person_sum の挙動だけ変える) ため
        // recalc_jobs は (month, eigyosho_id) PK で持ち、cal 別の sum は uriage_person_monthly に。
        //
        // r2_pending view: fingerprint 変化があったが R2 未送信の (month, eigyosho_id)。
        // nuxt cron が `GET /api/uriage/r2/pending` で取得し、生 bytes を R2 に putAll → ack。
        conn.execute_batch(
            r#"
            -- 日次集計 (SoT、唯一の persistence)。`(month, eigyosho_id, cal)` 単位で
            -- delete-then-insert する (recalc 毎に全置換)。
            CREATE TABLE IF NOT EXISTS uriage_person_daily (
                unko_date     TEXT    NOT NULL,
                month         TEXT    NOT NULL,
                person_name   TEXT    NOT NULL,
                eigyosho_id   INTEGER NOT NULL,
                cal           INTEGER NOT NULL,
                kingaku       INTEGER NOT NULL,
                yosha_kingaku INTEGER NOT NULL,
                kensuu        INTEGER NOT NULL,
                calculated_at TEXT    NOT NULL,
                PRIMARY KEY (unko_date, person_name, eigyosho_id, cal)
            );
            CREATE INDEX IF NOT EXISTS idx_upd_month_eigyosho
                ON uriage_person_daily (month, eigyosho_id, cal);
            CREATE INDEX IF NOT EXISTS idx_upd_person_date
                ON uriage_person_daily (person_name, unko_date);

            -- 月次集計は日次の SUM 由来の VIEW (= 二重持ちしない、user 指摘 2026-06-30)。
            -- API レスポンスとしては既存の月次 query (`get_person_monthly`) と互換。
            -- `calculated_at` は同 (month, eigyosho_id, cal) 内で日次 row の最新値を採用。
            CREATE VIEW IF NOT EXISTS uriage_person_monthly AS
            SELECT month,
                   person_name,
                   eigyosho_id,
                   cal,
                   SUM(kingaku)        AS kingaku,
                   SUM(yosha_kingaku)  AS yosha_kingaku,
                   SUM(kensuu)         AS kensuu,
                   MAX(calculated_at)  AS calculated_at
            FROM uriage_person_daily
            GROUP BY month, person_name, eigyosho_id, cal;

            CREATE TABLE IF NOT EXISTS recalc_jobs (
                month                TEXT    NOT NULL,
                eigyosho_id          INTEGER NOT NULL,
                status               TEXT    NOT NULL,
                fingerprint_before   TEXT,
                fingerprint_after    TEXT,
                raw_path             TEXT,
                created_at           TEXT    NOT NULL,
                computed_at          TEXT,
                r2_synced_at         TEXT,
                last_error           TEXT,
                PRIMARY KEY (month, eigyosho_id)
            );
            CREATE INDEX IF NOT EXISTS idx_rj_status
                ON recalc_jobs (status);

            CREATE VIEW IF NOT EXISTS r2_pending AS
            SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at
            FROM recalc_jobs
            WHERE status = 'computed'
              AND r2_synced_at IS NULL
              AND raw_path IS NOT NULL
              AND (
                  fingerprint_before IS NULL
                  OR fingerprint_before != fingerprint_after
              )
            ORDER BY computed_at ASC;
            "#,
        )
        .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
        Ok(())
    }

    /// `(month, eigyosho_id, cal)` の集計行を取得 (非ゼロ担当者のみ)。
    pub async fn get_person_monthly(
        &self,
        month: &str,
        eigyosho_id: i64,
        cal: bool,
    ) -> Result<Vec<PersonMonthlyRow>, LocalStoreError> {
        let month = month.to_string();
        let cal_int = if cal { 1 } else { 0 };
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT month, person_name, eigyosho_id, cal, kingaku, yosha_kingaku, \
                            kensuu, calculated_at \
                     FROM uriage_person_monthly \
                     WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3 \
                     ORDER BY kingaku DESC, person_name ASC",
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(params![month, eigyosho_id, cal_int], |r| {
                    Ok(PersonMonthlyRow {
                        month: r.get(0)?,
                        person_name: r.get(1)?,
                        eigyosho_id: r.get(2)?,
                        cal: r.get::<_, i64>(3)? != 0,
                        kingaku: r.get(4)?,
                        yosha_kingaku: r.get(5)?,
                        kensuu: r.get(6)?,
                        calculated_at: r.get(7)?,
                    })
                })
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<PersonMonthlyRow>, LocalStoreError>(rows)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// `(month, eigyosho_id, cal)` の日次集計を一括 upsert (delete-then-insert)。
    ///
    /// `daily_sums` は `unko_date` → 担当者名 → `PersonAccum` の二重 map。
    /// `kensuu == 0 AND kingaku == 0 AND yosha_kingaku == 0` の行は投入しない。
    ///
    /// 返り値 = 実際に insert した行数。
    pub async fn upsert_person_daily(
        &self,
        month: &str,
        eigyosho_id: i64,
        cal: bool,
        daily_sums: &HashMap<String, HashMap<String, PersonAccum>>,
        calculated_at: &str,
    ) -> Result<usize, LocalStoreError> {
        let month = month.to_string();
        let calculated_at = calculated_at.to_string();
        let cal_int = if cal { 1 } else { 0 };
        // (unko_date, name, PersonAccum) flatten
        let entries: Vec<(String, String, PersonAccum)> = daily_sums
            .iter()
            .flat_map(|(date, per_person)| {
                per_person
                    .iter()
                    .filter(|(_, v)| v.kensuu != 0 || v.kingaku != 0 || v.yosha_kingaku != 0)
                    .map(|(name, v)| (date.clone(), name.clone(), *v))
            })
            .collect();

        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = futures_lock(&conn);
            let tx = guard
                .transaction()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            tx.execute(
                "DELETE FROM uriage_person_daily \
                 WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3",
                params![month, eigyosho_id, cal_int],
            )
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let mut inserted = 0usize;
            {
                let mut stmt = tx
                    .prepare(
                        "INSERT INTO uriage_person_daily \
                         (unko_date, month, person_name, eigyosho_id, cal, kingaku, \
                          yosha_kingaku, kensuu, calculated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                    )
                    .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
                for (date, name, accum) in &entries {
                    stmt.execute(params![
                        date,
                        month,
                        name,
                        eigyosho_id,
                        cal_int,
                        accum.kingaku,
                        accum.yosha_kingaku,
                        accum.kensuu,
                        calculated_at,
                    ])
                    .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
                    inserted += 1;
                }
            }
            tx.commit()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<usize, LocalStoreError>(inserted)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// `(month, eigyosho_id, cal)` の日次集計を取得 (非ゼロ担当者のみ、日付昇順)。
    pub async fn get_person_daily(
        &self,
        month: &str,
        eigyosho_id: i64,
        cal: bool,
    ) -> Result<Vec<PersonDailyRow>, LocalStoreError> {
        let month = month.to_string();
        let cal_int = if cal { 1 } else { 0 };
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT unko_date, month, person_name, eigyosho_id, cal, \
                            kingaku, yosha_kingaku, kensuu, calculated_at \
                     FROM uriage_person_daily \
                     WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3 \
                     ORDER BY unko_date ASC, kingaku DESC, person_name ASC",
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(params![month, eigyosho_id, cal_int], |r| {
                    Ok(PersonDailyRow {
                        unko_date: r.get(0)?,
                        month: r.get(1)?,
                        person_name: r.get(2)?,
                        eigyosho_id: r.get(3)?,
                        cal: r.get::<_, i64>(4)? != 0,
                        kingaku: r.get(5)?,
                        yosha_kingaku: r.get(6)?,
                        kensuu: r.get(7)?,
                        calculated_at: r.get(8)?,
                    })
                })
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<PersonDailyRow>, LocalStoreError>(rows)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// 期間 `from`/`to` (YYYY-MM 月 inclusive) で 月 × 担当者 の SUM を返す。
    /// 全営業所合算 (cal フィルタのみ)。`/api/uriage/person-monthly-totals` で使う。
    /// 戻り値は `(month, person_name)` で sort 済み。
    pub async fn person_monthly_totals(
        &self,
        from_month: &str,
        to_month: &str,
        cal: bool,
    ) -> Result<Vec<PersonMonthlyRow>, LocalStoreError> {
        let from_month = from_month.to_string();
        let to_month = to_month.to_string();
        let cal_int = if cal { 1 } else { 0 };
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT month, person_name, 0 AS eigyosho_id, cal, \
                            SUM(kingaku), SUM(yosha_kingaku), SUM(kensuu), \
                            MAX(calculated_at) \
                     FROM uriage_person_daily \
                     WHERE month >= ?1 AND month <= ?2 AND cal = ?3 \
                     GROUP BY month, person_name, cal \
                     ORDER BY month ASC, person_name ASC",
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(params![from_month, to_month, cal_int], |r| {
                    Ok(PersonMonthlyRow {
                        month: r.get(0)?,
                        person_name: r.get(1)?,
                        eigyosho_id: r.get::<_, i64>(2)?,
                        cal: r.get::<_, i64>(3)? != 0,
                        kingaku: r.get(4)?,
                        yosha_kingaku: r.get(5)?,
                        kensuu: r.get(6)?,
                        calculated_at: r.get(7)?,
                    })
                })
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<PersonMonthlyRow>, LocalStoreError>(rows)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// `(month, eigyosho_id, cal)` の最終 calculated_at を返す (未集計なら None)。
    /// recalc が走った形跡を確認する用途。
    pub async fn last_calculated_at(
        &self,
        month: &str,
        eigyosho_id: i64,
        cal: bool,
    ) -> Result<Option<String>, LocalStoreError> {
        let month = month.to_string();
        let cal_int = if cal { 1 } else { 0 };
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let ts: Option<String> = guard
                .query_row(
                    "SELECT MAX(calculated_at) FROM uriage_person_monthly \
                     WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3",
                    params![month, eigyosho_id, cal_int],
                    |r| r.get(0),
                )
                .optional()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .flatten();
            Ok::<Option<String>, LocalStoreError>(ts)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    // ──────────────────────────────────────────────────────────────────
    // recalc_jobs / r2_pending (Phase 2 PR-D)
    // ──────────────────────────────────────────────────────────────────

    /// `(month, eigyosho_id)` の現在の job 行を取得 (なければ None)。
    pub async fn get_recalc_job(
        &self,
        month: &str,
        eigyosho_id: i64,
    ) -> Result<Option<RecalcJob>, LocalStoreError> {
        let month = month.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT month, eigyosho_id, status, fingerprint_before, fingerprint_after, \
                            raw_path, created_at, computed_at, r2_synced_at, last_error \
                     FROM recalc_jobs \
                     WHERE month = ?1 AND eigyosho_id = ?2",
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let row = stmt
                .query_row(params![month, eigyosho_id], |r| {
                    Ok(RecalcJob {
                        month: r.get(0)?,
                        eigyosho_id: r.get(1)?,
                        status: r.get(2)?,
                        fingerprint_before: r.get(3)?,
                        fingerprint_after: r.get(4)?,
                        raw_path: r.get(5)?,
                        created_at: r.get(6)?,
                        computed_at: r.get(7)?,
                        r2_synced_at: r.get(8)?,
                        last_error: r.get(9)?,
                    })
                })
                .optional()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Option<RecalcJob>, LocalStoreError>(row)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// recalc 完了時に呼ぶ。`(month, eigyosho_id)` に対し idempotent upsert で
    /// `status='computed'`, `fingerprint_*`, `raw_path`, `computed_at` を記録する。
    /// `fingerprint_before` は前回の `fingerprint_after` を引き継ぐ (skip 判定用)。
    pub async fn record_recalc_computed(
        &self,
        month: &str,
        eigyosho_id: i64,
        fingerprint_after: &str,
        raw_path: Option<&str>,
        computed_at: &str,
    ) -> Result<(), LocalStoreError> {
        let month = month.to_string();
        let fingerprint_after = fingerprint_after.to_string();
        let raw_path = raw_path.map(str::to_string);
        let computed_at = computed_at.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            // 既存 fingerprint_after を fingerprint_before に持ち上げて upsert。
            // r2_synced_at は新しい fingerprint なら NULL に戻す (再送信が必要)。
            guard
                .execute(
                    "INSERT INTO recalc_jobs \
                     (month, eigyosho_id, status, fingerprint_before, fingerprint_after, \
                      raw_path, created_at, computed_at, r2_synced_at, last_error) \
                 VALUES (?1, ?2, 'computed', NULL, ?3, ?4, ?5, ?5, NULL, NULL) \
                 ON CONFLICT(month, eigyosho_id) DO UPDATE SET \
                     status = 'computed', \
                     fingerprint_before = recalc_jobs.fingerprint_after, \
                     fingerprint_after = excluded.fingerprint_after, \
                     raw_path = excluded.raw_path, \
                     computed_at = excluded.computed_at, \
                     r2_synced_at = CASE \
                         WHEN recalc_jobs.fingerprint_after = excluded.fingerprint_after \
                              THEN recalc_jobs.r2_synced_at \
                         ELSE NULL \
                     END, \
                     last_error = NULL",
                    params![month, eigyosho_id, fingerprint_after, raw_path, computed_at],
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<(), LocalStoreError>(())
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// recalc 失敗時に呼ぶ。`status='failed'` + `last_error` を記録 (raw 状態は触らない)。
    pub async fn record_recalc_failed(
        &self,
        month: &str,
        eigyosho_id: i64,
        last_error: &str,
        now: &str,
    ) -> Result<(), LocalStoreError> {
        let month = month.to_string();
        let last_error = last_error.to_string();
        let now = now.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            guard
                .execute(
                    "INSERT INTO recalc_jobs \
                     (month, eigyosho_id, status, created_at, last_error) \
                 VALUES (?1, ?2, 'failed', ?3, ?4) \
                 ON CONFLICT(month, eigyosho_id) DO UPDATE SET \
                     status = 'failed', \
                     last_error = excluded.last_error",
                    params![month, eigyosho_id, now, last_error],
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<(), LocalStoreError>(())
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// `r2_pending` view を読む (fingerprint 変化あり & 未送信の (month, eigyosho_id))。
    pub async fn list_r2_pending(&self) -> Result<Vec<R2PendingRow>, LocalStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at \
                     FROM r2_pending",
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map([], |r| {
                    Ok(R2PendingRow {
                        month: r.get(0)?,
                        eigyosho_id: r.get(1)?,
                        raw_path: r.get(2)?,
                        fingerprint_after: r.get(3)?,
                        computed_at: r.get(4)?,
                    })
                })
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<R2PendingRow>, LocalStoreError>(rows)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// `(month, eigyosho_id)` の R2 sync 完了を記録。`status='r2_synced'`, `r2_synced_at=now`。
    /// 該当 job が存在しない場合は no-op (= 0 row affected)、affected row 数を返す。
    pub async fn record_r2_synced(
        &self,
        month: &str,
        eigyosho_id: i64,
        synced_at: &str,
    ) -> Result<usize, LocalStoreError> {
        let month = month.to_string();
        let synced_at = synced_at.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let n = guard
                .execute(
                    "UPDATE recalc_jobs SET status = 'r2_synced', r2_synced_at = ?1 \
                     WHERE month = ?2 AND eigyosho_id = ?3 AND status = 'computed'",
                    params![synced_at, month, eigyosho_id],
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<usize, LocalStoreError>(n)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    // ──────────────────────────────────────────────────────────────────
    // Admin: 削除 + 再作成
    // ──────────────────────────────────────────────────────────────────

    /// `(month, eigyosho_id)` を全 cal で削除 (一部リセット用)。
    ///
    /// 対象 table:
    /// - `uriage_person_daily`   (cal=true / cal=false 両方)
    /// - `recalc_jobs`           (1 行)
    ///
    /// 月次 (`uriage_person_monthly`) は日次から導出する VIEW のため自動消滅する。
    ///
    /// 戻り値 = 各 table の affected row 数の合計。
    /// 削除後に再度 `/recalc` を叩けば fresh fingerprint (fp_before=NULL) で
    /// raw NDJSON.gz が再生成され、R2 sync 対象になる。
    pub async fn delete_bucket(
        &self,
        month: &str,
        eigyosho_id: i64,
    ) -> Result<DeleteBucketResult, LocalStoreError> {
        let month = month.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = futures_lock(&conn);
            let tx = guard
                .transaction()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let daily = tx
                .execute(
                    "DELETE FROM uriage_person_daily WHERE month = ?1 AND eigyosho_id = ?2",
                    params![month, eigyosho_id],
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let job = tx
                .execute(
                    "DELETE FROM recalc_jobs WHERE month = ?1 AND eigyosho_id = ?2",
                    params![month, eigyosho_id],
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            tx.commit()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<DeleteBucketResult, LocalStoreError>(DeleteBucketResult {
                daily_deleted: daily,
                jobs_deleted: job,
            })
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// 全 table/view を DROP → 再 migrate (フルリセット、全データ消失)。
    ///
    /// SQLite のスキーマがおかしくなった時の最終手段。`r2_pending` view と
    /// `uriage_person_monthly` view も一緒に再作成される。
    /// 呼び出し後は recalc を叩き直して再生成する必要あり。
    pub async fn rebuild_schema(&self) -> Result<(), LocalStoreError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            guard
                .execute_batch(
                    r#"
                    DROP VIEW  IF EXISTS r2_pending;
                    DROP VIEW  IF EXISTS uriage_person_monthly;
                    DROP TABLE IF EXISTS recalc_jobs;
                    DROP TABLE IF EXISTS uriage_person_daily;
                    "#,
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            // re-create
            Self::migrate(&guard)?;
            Ok::<(), LocalStoreError>(())
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }
}

/// `delete_bucket` の affected rows サマリ。月次は VIEW のため対象外。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct DeleteBucketResult {
    pub daily_deleted: usize,
    pub jobs_deleted: usize,
}

/// `tokio::sync::Mutex` は async lock。`spawn_blocking` 内 (sync 文脈) では
/// `blocking_lock()` を使う必要があるが、tokio runtime context 内なので
/// `blocking_lock` を直接呼ぶと panic する。代わりに `try_lock` の loop は避け、
/// ヘルパで sync lock を取り出す。
///
/// 実装ノート: tokio Mutex の `blocking_lock()` は runtime 内で呼ぶと panic。
/// `spawn_blocking` の中は別スレッドだが、まだ runtime に属しているため
/// `Handle::current().block_on(mutex.lock())` で wait させる必要がある。
fn futures_lock(m: &Mutex<Connection>) -> tokio::sync::MutexGuard<'_, Connection> {
    tokio::runtime::Handle::current().block_on(m.lock())
}

pub type DynLocalStore = Arc<LocalStore>;

#[cfg(test)]
mod tests {
    use super::*;

    fn pa(kingaku: i64, yosha: i64, kensuu: i64) -> PersonAccum {
        PersonAccum {
            kingaku,
            yosha_kingaku: yosha,
            kensuu,
        }
    }

    #[tokio::test]
    async fn open_in_memory_creates_schema() {
        let store = LocalStore::open(":memory:").unwrap();
        // 空の get は空 Vec
        let rows = store.get_person_monthly("2026-06", 1, true).await.unwrap();
        assert!(rows.is_empty());
    }

    /// `upsert_person_daily` 用ヘルパ: 1 日分の (person → accum) を指定。
    fn daily_one_day(
        date: &str,
        per_person: HashMap<String, PersonAccum>,
    ) -> HashMap<String, HashMap<String, PersonAccum>> {
        let mut m = HashMap::new();
        m.insert(date.to_string(), per_person);
        m
    }

    #[tokio::test]
    async fn person_monthly_totals_sums_across_eigyosho() {
        let store = LocalStore::open(":memory:").unwrap();
        // 同月、同 person、異なる eigyosho に入れて SUM されることを確認
        let mut day = HashMap::new();
        day.insert("青井".to_string(), pa(10_000, 5_000, 1));
        let one = {
            let mut m = HashMap::new();
            m.insert("2026-06-15".to_string(), day.clone());
            m
        };
        store
            .upsert_person_daily("2026-06", 1, true, &one, "t1")
            .await
            .unwrap();
        store
            .upsert_person_daily("2026-06", 9, true, &one, "t1")
            .await
            .unwrap();
        // 翌月にも入れて期間フィルタも検証
        let mut day7 = HashMap::new();
        day7.insert("青井".to_string(), pa(3_000, 1_000, 1));
        let one7 = {
            let mut m = HashMap::new();
            m.insert("2026-07-01".to_string(), day7);
            m
        };
        store
            .upsert_person_daily("2026-07", 1, true, &one7, "t2")
            .await
            .unwrap();

        // 2026-06 のみで絞ると SUM = 2 営業所分
        let rows = store
            .person_monthly_totals("2026-06", "2026-06", true)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].month, "2026-06");
        assert_eq!(rows[0].person_name, "青井");
        assert_eq!(rows[0].kingaku, 20_000); // 10,000 × 2 営業所
        assert_eq!(rows[0].kensuu, 2);
        assert_eq!(rows[0].eigyosho_id, 0); // 全営業所合算は 0 固定

        // 2026-06..2026-07 で 2 行
        let rows2 = store
            .person_monthly_totals("2026-06", "2026-07", true)
            .await
            .unwrap();
        assert_eq!(rows2.len(), 2);
        assert!(rows2
            .iter()
            .any(|r| r.month == "2026-06" && r.kingaku == 20_000));
        assert!(rows2
            .iter()
            .any(|r| r.month == "2026-07" && r.kingaku == 3_000));

        // cal=false は空 (入れてないので)
        let nocal = store
            .person_monthly_totals("2026-06", "2026-07", false)
            .await
            .unwrap();
        assert!(nocal.is_empty());
    }

    #[tokio::test]
    async fn upsert_daily_then_monthly_view_aggregates() {
        let store = LocalStore::open(":memory:").unwrap();
        // 2 日分の daily を入れて、monthly VIEW が SUM で返すか確認
        let mut day1 = HashMap::new();
        day1.insert("青井".to_string(), pa(50000, 50000, 5));
        day1.insert("山﨑智".to_string(), pa(10000, 10000, 1));
        let mut day2 = HashMap::new();
        day2.insert("青井".to_string(), pa(40059, 40059, 4));
        day2.insert("山﨑智".to_string(), pa(10020, 10020, 1));

        let mut daily = HashMap::new();
        daily.insert("2026-06-01".to_string(), day1);
        daily.insert("2026-06-02".to_string(), day2);

        let n = store
            .upsert_person_daily("2026-06", 1, true, &daily, "2026-06-30T12:00:00Z")
            .await
            .unwrap();
        assert_eq!(n, 4); // 2 days × 2 persons

        // monthly VIEW が 2 日分を SUM
        let rows = store.get_person_monthly("2026-06", 1, true).await.unwrap();
        assert_eq!(rows.len(), 2);
        let aoi = rows.iter().find(|r| r.person_name == "青井").unwrap();
        assert_eq!(aoi.kingaku, 90059); // 50000 + 40059
        assert_eq!(aoi.kensuu, 9); // 5 + 4
        let yam = rows.iter().find(|r| r.person_name == "山﨑智").unwrap();
        assert_eq!(yam.kingaku, 20020); // 10000 + 10020
    }

    #[tokio::test]
    async fn upsert_daily_overwrites_bucket_in_full() {
        let store = LocalStore::open(":memory:").unwrap();
        // 1 回目
        let mut a = HashMap::new();
        a.insert("青井".to_string(), pa(100, 50, 1));
        store
            .upsert_person_daily(
                "2026-06",
                1,
                true,
                &daily_one_day("2026-06-15", a.clone()),
                "2026-06-29T00:00:00Z",
            )
            .await
            .unwrap();

        // 2 回目: 同じ bucket、別の日付に別の中身 → 1 回目の行は消える
        let mut b = HashMap::new();
        b.insert("青井".to_string(), pa(200, 100, 2));
        b.insert("大石".to_string(), pa(30, 20, 1));
        let n = store
            .upsert_person_daily(
                "2026-06",
                1,
                true,
                &daily_one_day("2026-06-16", b),
                "2026-06-30T00:00:00Z",
            )
            .await
            .unwrap();
        assert_eq!(n, 2);

        // monthly VIEW では 2026-06-16 だけが残っている (2026-06-15 は消えた)
        let rows = store.get_person_monthly("2026-06", 1, true).await.unwrap();
        let aoi = rows.iter().find(|r| r.person_name == "青井").unwrap();
        assert_eq!(aoi.kingaku, 200); // 上書きされている
        assert_eq!(aoi.kensuu, 2);
        assert!(rows.iter().any(|r| r.person_name == "大石"));
    }

    #[tokio::test]
    async fn upsert_daily_isolates_buckets_by_eigyosho_and_cal() {
        let store = LocalStore::open(":memory:").unwrap();
        let mut a = HashMap::new();
        a.insert("青井".to_string(), pa(100, 50, 1));
        let day = daily_one_day("2026-06-15", a);

        store
            .upsert_person_daily("2026-06", 1, true, &day, "t1")
            .await
            .unwrap();
        store
            .upsert_person_daily("2026-06", 2, true, &day, "t1")
            .await
            .unwrap();
        store
            .upsert_person_daily("2026-06", 1, false, &day, "t1")
            .await
            .unwrap();

        // eigyosho=1 / cal=true を空 map で上書き → 他の bucket は残る
        store
            .upsert_person_daily("2026-06", 1, true, &HashMap::new(), "t2")
            .await
            .unwrap();

        assert!(store
            .get_person_monthly("2026-06", 1, true)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store
                .get_person_monthly("2026-06", 2, true)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            store
                .get_person_monthly("2026-06", 1, false)
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn last_calculated_at_returns_max_or_none() {
        let store = LocalStore::open(":memory:").unwrap();
        // 未集計 → None
        assert_eq!(
            store.last_calculated_at("2026-06", 1, true).await.unwrap(),
            None
        );

        let mut s = HashMap::new();
        s.insert("青井".to_string(), pa(100, 50, 1));
        store
            .upsert_person_daily(
                "2026-06",
                1,
                true,
                &daily_one_day("2026-06-15", s),
                "2026-06-30T10:00:00Z",
            )
            .await
            .unwrap();
        // monthly VIEW の calculated_at は MAX で取得される
        assert_eq!(
            store.last_calculated_at("2026-06", 1, true).await.unwrap(),
            Some("2026-06-30T10:00:00Z".to_string())
        );
    }

    #[test]
    fn open_invalid_path_returns_error() {
        // `/dev/null` は char device で、その配下に dir を作れない → create_dir_all 失敗 →
        // OpenFailed が返る (path 自己回復が無理なケースの代表)
        let err = LocalStore::open("/dev/null/sub/state.db").unwrap_err();
        assert!(matches!(err, LocalStoreError::OpenFailed(_)));
        let msg = err.to_string();
        assert!(
            msg.contains("create_dir_all") || msg.contains("sqlite open failed"),
            "unexpected error message: {msg}"
        );
    }

    #[tokio::test]
    async fn open_creates_parent_dir_if_missing() {
        // テスト用一時 dir 配下に存在しない subdir を含むパスを指定 → 自動作成される
        let tmp = std::env::temp_dir().join(format!("ichibanboshi-test-{}", std::process::id()));
        // 前テストの残骸を片付け
        let _ = std::fs::remove_dir_all(&tmp);
        let nested = tmp.join("nested/dir/state.db");
        let store = LocalStore::open(nested.to_str().unwrap())
            .expect("open should auto-create parent dirs");
        let rows = store.get_person_monthly("2026-06", 1, true).await.unwrap();
        assert!(rows.is_empty());
        assert!(nested.exists(), "state.db file should be created");
        // 後始末
        let _ = std::fs::remove_dir_all(&tmp);
    }

    // ──────────────────────────────────────────────────────────────────
    // recalc_jobs / r2_pending tests
    // ──────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn recalc_job_initially_none() {
        let store = LocalStore::open(":memory:").unwrap();
        assert!(store.get_recalc_job("2026-06", 1).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn record_recalc_computed_inserts_then_updates() {
        let store = LocalStore::open(":memory:").unwrap();

        // 1 回目: 新規 insert
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/raw1.gz"), "t1")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.status, "computed");
        assert_eq!(job.fingerprint_before, None);
        assert_eq!(job.fingerprint_after.as_deref(), Some("fp1"));
        assert_eq!(job.raw_path.as_deref(), Some("/tmp/raw1.gz"));
        assert_eq!(job.r2_synced_at, None);

        // 2 回目 (fingerprint 同じ): r2_synced_at が NULL のままなのは初回未送信のため
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/raw1b.gz"), "t2")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.fingerprint_before.as_deref(), Some("fp1"));
        assert_eq!(job.fingerprint_after.as_deref(), Some("fp1"));

        // 3 回目 (fingerprint 変化): r2_synced_at を NULL に強制リセット
        store
            .record_r2_synced("2026-06", 1, "synced_at_x")
            .await
            .unwrap();
        store
            .record_recalc_computed("2026-06", 1, "fp2", Some("/tmp/raw2.gz"), "t3")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.fingerprint_before.as_deref(), Some("fp1"));
        assert_eq!(job.fingerprint_after.as_deref(), Some("fp2"));
        assert_eq!(
            job.r2_synced_at, None,
            "fingerprint 変化で r2_synced_at リセット"
        );
    }

    #[tokio::test]
    async fn r2_pending_only_shows_changed_unsynced() {
        let store = LocalStore::open(":memory:").unwrap();

        // (2026-06, 1) computed, fingerprint_after=fp1、未送信 → pending に出る
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r1.gz"), "t1")
            .await
            .unwrap();
        // (2026-06, 2) computed but raw_path=None → pending に出ない
        store
            .record_recalc_computed("2026-06", 2, "fp1", None, "t1")
            .await
            .unwrap();
        // (2026-07, 1) computed, synced → pending に出ない
        store
            .record_recalc_computed("2026-07", 1, "fp1", Some("/tmp/r2.gz"), "t1")
            .await
            .unwrap();
        store.record_r2_synced("2026-07", 1, "t2").await.unwrap();

        let pending = store.list_r2_pending().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].month, "2026-06");
        assert_eq!(pending[0].eigyosho_id, 1);
        assert_eq!(pending[0].fingerprint_after, "fp1");
    }

    #[tokio::test]
    async fn r2_pending_excludes_when_fingerprint_unchanged() {
        let store = LocalStore::open(":memory:").unwrap();
        // 同 fingerprint で 2 回 record + 1 回 sync → pending から消える
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t1")
            .await
            .unwrap();
        store.record_r2_synced("2026-06", 1, "t1").await.unwrap();
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t2")
            .await
            .unwrap();
        // fingerprint 同じなので r2_synced_at は維持されている
        let pending = store.list_r2_pending().await.unwrap();
        assert!(pending.is_empty());
    }

    #[tokio::test]
    async fn record_recalc_failed_stores_error() {
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_failed("2026-06", 1, "sqlserver timeout", "t1")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.status, "failed");
        assert_eq!(job.last_error.as_deref(), Some("sqlserver timeout"));
    }

    #[tokio::test]
    async fn record_r2_synced_returns_0_for_missing_job() {
        let store = LocalStore::open(":memory:").unwrap();
        let n = store.record_r2_synced("2026-06", 1, "t1").await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn record_r2_synced_only_updates_computed_status() {
        let store = LocalStore::open(":memory:").unwrap();
        // failed 状態の job は r2_synced に遷移しない
        store
            .record_recalc_failed("2026-06", 1, "err", "t1")
            .await
            .unwrap();
        let n = store.record_r2_synced("2026-06", 1, "t2").await.unwrap();
        assert_eq!(n, 0);
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.status, "failed");
    }
}
