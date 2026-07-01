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

use crate::routes::uriage::{PartnerAccum, PartnerKind, PersonAccum, PersonPartnerKey};

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
///
/// `*_y0` は 横横=0 (= 自社運行 or sql_from_other) のみの集計値。
/// 横横除外フィルタ UI で main fields の代わりに使う。
/// 旧 data (recalc 未実行) は 0 になる (= recalc 全期間で正しい値に上書きされる)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonMonthlyRow {
    pub month: String,
    pub person_name: String,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub kingaku: i64,
    pub yosha_kingaku: i64,
    pub kensuu: i64,
    pub kingaku_y0: i64,
    pub yosha_kingaku_y0: i64,
    pub kensuu_y0: i64,
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
    pub kingaku_y0: i64,
    pub yosha_kingaku_y0: i64,
    pub kensuu_y0: i64,
    pub calculated_at: String,
}

/// 担当者×得意先/傭車先 内訳 1 行 (期間集計、全営業所合算)。
/// `GET /api/uriage/person-partner-totals` のデータソース。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonPartnerTotalRow {
    pub partner_code: String,
    pub partner_name: String,
    pub kingaku: i64,
    pub kensuu: i64,
    pub kingaku_y0: i64,
    pub kensuu_y0: i64,
}

/// `recalc_jobs` 1 行。`status` 遷移: `queued → computing → computed → r2_synced`。
/// 失敗時は `status='failed' + last_error`。
///
/// `verified_*` は `verify_coverage` view 経由で LEFT JOIN した verify 集計
/// (= 当該 (month, eigyosho_id) の verify_jobs 行数)。verify 未走行は 0 / 0 / 0。
/// UI 側で `expected_count = days_in_month × 2 cal` と突合して
/// 「全件 verify 済み / 一部のみ / 未走行」を判定する。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecalcJob {
    pub month: String,
    pub eigyosho_id: i64,
    pub status: String,
    pub fingerprint_before: Option<String>,
    pub fingerprint_after: Option<String>,
    pub raw_path: Option<String>,
    pub created_at: String,
    /// 最後に recalc が走った時刻 (fingerprint が変わったかどうかに関わらず更新)。
    /// 「recalc を試した最終時刻」を示す。
    pub computed_at: Option<String>,
    /// **fingerprint が実際に変化した時刻** (= データが変わった時刻)。
    /// fingerprint 不変な再 recalc では更新されない。`computed_at` だけ進んで
    /// `fingerprint_changed_at` が古いままなら「最近 recalc は走ったが data は
    /// 変わっていない」と分かる (user 2026-06-30: 「fingerprint の時間入れないと
    /// わからない」)。
    pub fingerprint_changed_at: Option<String>,
    pub r2_synced_at: Option<String>,
    pub last_error: Option<String>,
    pub verified_count: i64,
    pub verified_ok: i64,
    pub verified_ng: i64,
}

/// `r2_pending` view の 1 行 (R2 へ送信すべき 月×営業所)。
///
/// 2026-06-30 (`verify_jobs` 導入) で verify coverage の summary を同居。
/// `ready=true` のみ R2 sync 対象、`blocker` は scenairo (unverified/ng_present) を示す。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct R2PendingRow {
    pub month: String,
    pub eigyosho_id: i64,
    pub raw_path: String,
    pub fingerprint_after: String,
    pub computed_at: String,
    /// verify_jobs に存在する (date, cal) 行数 (全 cal flag 合計)
    pub verified_count: i64,
    /// verify_jobs.ok = 1 の行数
    pub verified_ok: i64,
    /// verify_jobs.ok = 0 の行数
    pub verified_ng: i64,
}

/// `verify_jobs` table の 1 行 (検証履歴)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifyJobRow {
    pub unko_date: String,
    pub eigyosho_id: i64,
    pub cal: i64, // 0 / 1
    pub month: String,
    pub ok: i64,
    pub skipped_reason: Option<String>,
    pub diff_json: Option<String>,
    pub row_count: i64,
    pub elapsed_ms: i64,
    pub ran_at: String,
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

    /// `sqlite_master.type` を見て TABLE or VIEW を判別し、適切な DROP を発行する。
    ///
    /// `ALTER TABLE ADD COLUMN` を idempotent に実行する helper。
    /// SQLite には `ADD COLUMN IF NOT EXISTS` が無いので `PRAGMA table_info` で
    /// 既存 column を check してから足りないときだけ ALTER する。
    fn ensure_column(
        conn: &Connection,
        table: &str,
        column: &str,
        definition: &str,
    ) -> Result<(), LocalStoreError> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
        let exists = stmt
            .query_map([], |r| r.get::<_, String>(1))
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
            .filter_map(Result::ok)
            .any(|c| c == column);
        if !exists {
            conn.execute_batch(&format!(
                "ALTER TABLE {table} ADD COLUMN {column} {definition}"
            ))
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
        }
        Ok(())
    }

    /// SQLite は `DROP TABLE` を VIEW に対して使うとエラー、`DROP VIEW` を TABLE に
    /// 対して使うともエラーになる。schema 移行時 (TABLE → VIEW 等) で旧型が残っている
    /// 可能性に対応するための helper。対象が存在しなければ no-op。
    fn drop_object_if_exists(conn: &Connection, name: &str) -> Result<(), LocalStoreError> {
        use rusqlite::OptionalExtension;
        let kind: Option<String> = conn
            .query_row(
                "SELECT type FROM sqlite_master WHERE name = ?1",
                rusqlite::params![name],
                |r| r.get(0),
            )
            .optional()
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
        let drop_sql = match kind.as_deref() {
            Some("view") => format!("DROP VIEW IF EXISTS {name}"),
            Some("table") => format!("DROP TABLE IF EXISTS {name}"),
            // index / trigger / その他 or 存在せず → no-op
            _ => return Ok(()),
        };
        conn.execute_batch(&drop_sql)
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
        Ok(())
    }

    fn migrate(conn: &Connection) -> Result<(), LocalStoreError> {
        // 担当者×月×営業所×cal 集計。
        //
        // 設計判断 (Refs #43/#44、2026-06-30):
        // - **table / index**: `CREATE IF NOT EXISTS` で idempotent。既存データ保持。
        // - **view**: SQLite は `CREATE OR REPLACE VIEW` を持たないため schema 変更を
        //   反映するには毎 boot で DROP+CREATE が必要 (= derived なので再生成 cost 0)。
        //   ただし旧 PR で `uriage_person_monthly` を **TABLE → VIEW に変更**した時に
        //   `CREATE VIEW IF NOT EXISTS` が既存 TABLE をスキップしてしまい、prod DB に
        //   TABLE のまま残っている事例があった (#44 deploy で `use DROP TABLE to delete
        //   table` エラーで boot 不能になった実害)。よって **sqlite_master.type を見て
        //   TABLE/VIEW を判別してから適切な DROP を出す** helper を使う (`drop_object_if_exists`)。
        //
        // recalc_jobs: per (month, eigyosho_id) の recalc 状態 + fingerprint + R2 sync 状態。
        // r2_pending view: fingerprint 変化があったが R2 未送信の (month, eigyosho_id)。
        // nuxt cron が `GET /api/uriage/r2/pending` で取得し、生 bytes を R2 に putAll → ack。

        // ── 1. tables / indexes (idempotent、データ保持) ──
        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS uriage_person_daily (
                unko_date         TEXT    NOT NULL,
                month             TEXT    NOT NULL,
                person_name       TEXT    NOT NULL,
                eigyosho_id       INTEGER NOT NULL,
                cal               INTEGER NOT NULL,
                kingaku           INTEGER NOT NULL,
                yosha_kingaku     INTEGER NOT NULL,
                kensuu            INTEGER NOT NULL,
                -- 横横=0 (= 自社運行 or sql_from_other) のみの集計値
                -- (横横除外フィルタ UI 用、user 2026-06-30 要望)
                kingaku_y0        INTEGER NOT NULL DEFAULT 0,
                yosha_kingaku_y0  INTEGER NOT NULL DEFAULT 0,
                kensuu_y0         INTEGER NOT NULL DEFAULT 0,
                calculated_at     TEXT    NOT NULL,
                PRIMARY KEY (unko_date, person_name, eigyosho_id, cal)
            );
            CREATE INDEX IF NOT EXISTS idx_upd_month_eigyosho
                ON uriage_person_daily (month, eigyosho_id, cal);
            CREATE INDEX IF NOT EXISTS idx_upd_person_date
                ON uriage_person_daily (person_name, unko_date);

            -- 担当者×得意先/傭車先 内訳 (`compute_person_partner_sum_by_day` 由来)。
            -- `uriage_person_daily` と同じ (unko_date, person_name, eigyosho_id, cal)
            -- 粒度に `partner_kind`(customer/subcontractor)+`partner_code` 軸を追加。
            CREATE TABLE IF NOT EXISTS uriage_person_partner_daily (
                unko_date         TEXT    NOT NULL,
                month             TEXT    NOT NULL,
                person_name       TEXT    NOT NULL,
                eigyosho_id       INTEGER NOT NULL,
                cal               INTEGER NOT NULL,
                partner_kind      TEXT    NOT NULL,
                partner_code      TEXT    NOT NULL,
                partner_name      TEXT    NOT NULL,
                kingaku           INTEGER NOT NULL,
                kensuu            INTEGER NOT NULL,
                kingaku_y0        INTEGER NOT NULL DEFAULT 0,
                kensuu_y0         INTEGER NOT NULL DEFAULT 0,
                calculated_at     TEXT    NOT NULL,
                PRIMARY KEY (unko_date, person_name, eigyosho_id, cal, partner_kind, partner_code)
            );
            CREATE INDEX IF NOT EXISTS idx_uppd_month_eigyosho
                ON uriage_person_partner_daily (month, eigyosho_id, cal);
            CREATE INDEX IF NOT EXISTS idx_uppd_person_kind
                ON uriage_person_partner_daily (person_name, month, partner_kind);

            CREATE TABLE IF NOT EXISTS recalc_jobs (
                month                   TEXT    NOT NULL,
                eigyosho_id             INTEGER NOT NULL,
                status                  TEXT    NOT NULL,
                fingerprint_before      TEXT,
                fingerprint_after       TEXT,
                raw_path                TEXT,
                created_at              TEXT    NOT NULL,
                computed_at             TEXT,
                fingerprint_changed_at  TEXT,
                r2_synced_at            TEXT,
                last_error              TEXT,
                PRIMARY KEY (month, eigyosho_id)
            );
            CREATE INDEX IF NOT EXISTS idx_rj_status
                ON recalc_jobs (status);

            CREATE TABLE IF NOT EXISTS verify_jobs (
                unko_date         TEXT    NOT NULL,
                eigyosho_id       INTEGER NOT NULL,
                cal               INTEGER NOT NULL,
                month             TEXT    NOT NULL,
                ok                INTEGER NOT NULL,
                skipped_reason    TEXT,
                diff_json         TEXT,
                row_count         INTEGER NOT NULL,
                elapsed_ms        INTEGER NOT NULL,
                ran_at            TEXT    NOT NULL,
                PRIMARY KEY (unko_date, eigyosho_id, cal)
            );
            CREATE INDEX IF NOT EXISTS idx_vj_month_eigyosho
                ON verify_jobs (month, eigyosho_id);
            CREATE INDEX IF NOT EXISTS idx_vj_ran_at
                ON verify_jobs (ran_at DESC);
            "#,
        )
        .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;

        // ── 2a. recalc_jobs に `fingerprint_changed_at` カラムを後付け追加 ──
        // 既存 prod DB は `CREATE TABLE IF NOT EXISTS` で skip されるため、ALTER で
        // 追加する。`PRAGMA table_info` で存在チェックして idempotent に。
        Self::ensure_column(conn, "recalc_jobs", "fingerprint_changed_at", "TEXT")?;

        // ── 2a-bis. uriage_person_daily に y0 (横横=0) 列を後付け追加 ──
        // 既存 prod DB には kingaku/yosha_kingaku/kensuu しか無いので ALTER で追加。
        // DEFAULT 0 で過去行は 0 埋め、recalc し直すと正しい y0 値で UPDATE される
        // (= 全期間 recalc が必要、計算式変更時と同じ手順)。
        Self::ensure_column(
            conn,
            "uriage_person_daily",
            "kingaku_y0",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        Self::ensure_column(
            conn,
            "uriage_person_daily",
            "yosha_kingaku_y0",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        Self::ensure_column(
            conn,
            "uriage_person_daily",
            "kensuu_y0",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        // 既存行の backfill: fingerprint_changed_at がまだ NULL のものは
        // computed_at の値で埋める (= 旧運用での「data 最終更新時刻」推定)
        conn.execute_batch(
            "UPDATE recalc_jobs \
             SET fingerprint_changed_at = computed_at \
             WHERE fingerprint_changed_at IS NULL AND computed_at IS NOT NULL;",
        )
        .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;

        // ── 2a'. PR #51 以前の bug で生まれた不整合行を repair ──
        // user 2026-06-30: 「R2 同期押しても対象なし」
        //
        // 旧 record_recalc_computed が fingerprint 不変な再 recalc でも常に status='computed'
        // に下げていた。`r2_synced_at` は CASE で維持されるため、`status='computed' AND
        // r2_synced_at IS NOT NULL` という不整合データが prod に溜まった。
        // この行は:
        // - 状態サマリには「🟡 R2 同期待ち」と出る (status='computed' なので)
        // - r2_pending view からは除外される (`WHERE r2_synced_at IS NULL` のため)
        // - 結果: R2 同期ボタンを押しても 0 件しか拾えず永久に「同期待ち」のまま
        //
        // 判定の妥当性: r2_synced_at IS NOT NULL = 最後の recalc が fingerprint 一致で
        // r2_synced_at を維持した = data は R2 と同じ → 'r2_synced' が正しい状態。
        // PR #51 の新ロジックでは発生しないが、既存の broken 行は本 migration で修復。
        // (idempotent: 2 回目以降はマッチ行が無いので no-op)
        conn.execute_batch(
            "UPDATE recalc_jobs \
             SET status = 'r2_synced' \
             WHERE status = 'computed' AND r2_synced_at IS NOT NULL;",
        )
        .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;

        // ── 2b. views を一旦 DROP (TABLE/VIEW を sqlite_master で判別) ──
        // 3 名はいずれも現行 schema では VIEW だが、旧 deploy 残骸で TABLE として
        // 存在する可能性があるため (PR #44 deploy 失敗の root cause)、type を見てから
        // 適切な DROP を出す。
        Self::drop_object_if_exists(conn, "r2_pending")?;
        Self::drop_object_if_exists(conn, "verify_coverage")?;
        Self::drop_object_if_exists(conn, "uriage_person_monthly")?;

        // ── 3. views を CREATE (schema 変更は毎 boot 反映) ──
        conn.execute_batch(
            r#"
            -- 月次集計は日次の SUM 由来の VIEW (= 二重持ちしない、user 指摘 2026-06-30)。
            CREATE VIEW uriage_person_monthly AS
            SELECT month,
                   person_name,
                   eigyosho_id,
                   cal,
                   SUM(kingaku)           AS kingaku,
                   SUM(yosha_kingaku)     AS yosha_kingaku,
                   SUM(kensuu)            AS kensuu,
                   SUM(kingaku_y0)        AS kingaku_y0,
                   SUM(yosha_kingaku_y0)  AS yosha_kingaku_y0,
                   SUM(kensuu_y0)         AS kensuu_y0,
                   MAX(calculated_at)     AS calculated_at
            FROM uriage_person_daily
            GROUP BY month, person_name, eigyosho_id, cal;

            -- (month, eigyosho_id) ごとの verify 集計 view。R2 sync gate の判定用。
            CREATE VIEW verify_coverage AS
            SELECT month,
                   eigyosho_id,
                   COUNT(*)                                AS verified_count,
                   SUM(CASE WHEN ok = 1 THEN 1 ELSE 0 END) AS verified_ok,
                   SUM(CASE WHEN ok = 0 THEN 1 ELSE 0 END) AS verified_ng
            FROM verify_jobs
            GROUP BY month, eigyosho_id;

            -- r2_pending: 「R2 同期候補」 view。verify_coverage を LEFT JOIN し、
            -- caller (nuxt) が「(month の日数) × 2 cal」と verified_count を比較して
            -- ready を判定できるようにする。verified_ng>0 は ng_present で gate される。
            --
            -- 条件は 3 つだけ: status='computed' AND r2_synced_at IS NULL AND raw_path IS NOT NULL。
            -- 以前は「fingerprint_before != fingerprint_after」condition も含めていたが、
            -- PR #51 以前の bug code path で「fp_before == fp_after かつ r2_synced_at IS NULL」
            -- の行が永久に view から除外され R2 sync 不能になる事故が出た (user 2026-06-30:
            -- 「ｒ２同期押してもこれ」)。r2_synced_at IS NULL ですでに「未同期 = 要 sync」を
            -- 表現できるので fingerprint 比較は不要 (= 同期済 case は r2_synced_at IS NOT NULL
            -- で既に除外されている)。
            CREATE VIEW r2_pending AS
            SELECT rj.month,
                   rj.eigyosho_id,
                   rj.raw_path,
                   rj.fingerprint_after,
                   rj.computed_at,
                   COALESCE(vc.verified_count, 0) AS verified_count,
                   COALESCE(vc.verified_ok,    0) AS verified_ok,
                   COALESCE(vc.verified_ng,    0) AS verified_ng
            FROM recalc_jobs rj
            LEFT JOIN verify_coverage vc
              ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id
            WHERE rj.status = 'computed'
              AND rj.r2_synced_at IS NULL
              AND rj.raw_path IS NOT NULL
            ORDER BY rj.computed_at ASC;
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
                            kensuu, kingaku_y0, yosha_kingaku_y0, kensuu_y0, calculated_at \
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
                        kingaku_y0: r.get(7)?,
                        yosha_kingaku_y0: r.get(8)?,
                        kensuu_y0: r.get(9)?,
                        calculated_at: r.get(10)?,
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
                    .filter(|(_, v)| {
                        // 合計 or y0 のいずれかが非ゼロなら投入
                        // (y0 だけ非ゼロ = 全て横横=0 行、というケースは現実には合計も非ゼロだが
                        //  概念上の整合性のため両方チェック)
                        v.kensuu != 0
                            || v.kingaku != 0
                            || v.yosha_kingaku != 0
                            || v.kensuu_y0 != 0
                            || v.kingaku_y0 != 0
                            || v.yosha_kingaku_y0 != 0
                    })
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
                          yosha_kingaku, kensuu, kingaku_y0, yosha_kingaku_y0, kensuu_y0, \
                          calculated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
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
                        accum.kingaku_y0,
                        accum.yosha_kingaku_y0,
                        accum.kensuu_y0,
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

    /// `(month, eigyosho_id, cal)` の担当者×得意先/傭車先 内訳を一括 upsert
    /// (delete-then-insert、`upsert_person_daily` の (担当者,取引先) 版)。
    ///
    /// `daily_sums` は `unko_date` → `(担当者, 種別, 取引先複合キー)` → `PartnerAccum`
    /// の二重 map (`compute_person_partner_sum_by_day` の戻り値そのまま)。
    /// 0 行 (`kensuu==0 AND kingaku==0 AND kensuu_y0==0 AND kingaku_y0==0`) は投入しない。
    ///
    /// 返り値 = 実際に insert した行数。
    pub async fn upsert_person_partner_daily(
        &self,
        month: &str,
        eigyosho_id: i64,
        cal: bool,
        daily_sums: &HashMap<String, HashMap<PersonPartnerKey, PartnerAccum>>,
        calculated_at: &str,
    ) -> Result<usize, LocalStoreError> {
        let month = month.to_string();
        let calculated_at = calculated_at.to_string();
        let cal_int = if cal { 1 } else { 0 };
        let entries: Vec<(String, PersonPartnerKey, PartnerAccum)> = daily_sums
            .iter()
            .flat_map(|(date, per_key)| {
                per_key
                    .iter()
                    .filter(|(_, v)| {
                        v.kensuu != 0 || v.kingaku != 0 || v.kensuu_y0 != 0 || v.kingaku_y0 != 0
                    })
                    .map(|(k, v)| (date.clone(), k.clone(), v.clone()))
            })
            .collect();

        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = futures_lock(&conn);
            let tx = guard
                .transaction()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            tx.execute(
                "DELETE FROM uriage_person_partner_daily \
                 WHERE month = ?1 AND eigyosho_id = ?2 AND cal = ?3",
                params![month, eigyosho_id, cal_int],
            )
            .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let mut inserted = 0usize;
            {
                let mut stmt = tx
                    .prepare(
                        "INSERT INTO uriage_person_partner_daily \
                         (unko_date, month, person_name, eigyosho_id, cal, partner_kind, \
                          partner_code, partner_name, kingaku, kensuu, kingaku_y0, kensuu_y0, \
                          calculated_at) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
                    )
                    .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
                for (date, key, accum) in &entries {
                    stmt.execute(params![
                        date,
                        month,
                        key.person_name,
                        eigyosho_id,
                        cal_int,
                        key.partner_kind.as_str(),
                        key.partner_code,
                        accum.partner_name,
                        accum.kingaku,
                        accum.kensuu,
                        accum.kingaku_y0,
                        accum.kensuu_y0,
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

    /// person_name + 期間 (`from_month..=to_month`、月 inclusive) + cal + partner_kind
    /// で得意先 or 傭車先ごとの SUM を返す (全営業所合算)。
    /// `GET /api/uriage/person-partner-totals` のデータソース。
    pub async fn person_partner_totals(
        &self,
        person_name: &str,
        from_month: &str,
        to_month: &str,
        cal: bool,
        partner_kind: PartnerKind,
    ) -> Result<Vec<PersonPartnerTotalRow>, LocalStoreError> {
        let person_name = person_name.to_string();
        let from_month = from_month.to_string();
        let to_month = to_month.to_string();
        let cal_int = if cal { 1 } else { 0 };
        let kind_str = partner_kind.as_str().to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let mut stmt = guard
                .prepare(
                    "SELECT partner_code, partner_name, \
                            SUM(kingaku), SUM(kensuu), SUM(kingaku_y0), SUM(kensuu_y0) \
                     FROM uriage_person_partner_daily \
                     WHERE person_name = ?1 AND month >= ?2 AND month <= ?3 \
                       AND cal = ?4 AND partner_kind = ?5 \
                     GROUP BY partner_code, partner_name \
                     ORDER BY SUM(kingaku) DESC, partner_code ASC",
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(
                    params![person_name, from_month, to_month, cal_int, kind_str],
                    |r| {
                        Ok(PersonPartnerTotalRow {
                            partner_code: r.get(0)?,
                            partner_name: r.get(1)?,
                            kingaku: r.get(2)?,
                            kensuu: r.get(3)?,
                            kingaku_y0: r.get(4)?,
                            kensuu_y0: r.get(5)?,
                        })
                    },
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<PersonPartnerTotalRow>, LocalStoreError>(rows)
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
                            kingaku, yosha_kingaku, kensuu, \
                            kingaku_y0, yosha_kingaku_y0, kensuu_y0, calculated_at \
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
                        kingaku_y0: r.get(8)?,
                        yosha_kingaku_y0: r.get(9)?,
                        kensuu_y0: r.get(10)?,
                        calculated_at: r.get(11)?,
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
                            SUM(kingaku_y0), SUM(yosha_kingaku_y0), SUM(kensuu_y0), \
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
                        kingaku_y0: r.get(7)?,
                        yosha_kingaku_y0: r.get(8)?,
                        kensuu_y0: r.get(9)?,
                        calculated_at: r.get(10)?,
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

    /// `recalc_jobs` を取得 (UI 状態サマリ用)。verify_coverage と LEFT JOIN して
    /// `verified_count / ok / ng` を同梱。
    ///
    /// `from` / `to` (YYYY-MM、inclusive) が **両方** Some なら期間 filter、それ以外は
    /// 全件 (user 2026-06-30: 状態サマリは全期間表示)。
    ///
    /// ORDER BY `month DESC, eigyosho_id ASC` — 直近月を上位に出すための降順
    /// (user 2026-06-30: 「状態サマリは降順」)。同月内は office id 昇順。
    pub async fn list_recalc_jobs(
        &self,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<RecalcJob>, LocalStoreError> {
        let from = from.map(|s| s.to_string());
        let to = to.map(|s| s.to_string());
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let (sql, params): (&str, Vec<&dyn rusqlite::ToSql>) = match (&from, &to) {
                (Some(f), Some(t)) => (
                    "SELECT rj.month, rj.eigyosho_id, rj.status, \
                            rj.fingerprint_before, rj.fingerprint_after, \
                            rj.raw_path, rj.created_at, rj.computed_at, \
                            rj.fingerprint_changed_at, \
                            rj.r2_synced_at, rj.last_error, \
                            COALESCE(vc.verified_count, 0), \
                            COALESCE(vc.verified_ok,    0), \
                            COALESCE(vc.verified_ng,    0) \
                     FROM recalc_jobs rj \
                     LEFT JOIN verify_coverage vc \
                       ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id \
                     WHERE rj.month BETWEEN ?1 AND ?2 \
                     ORDER BY rj.month DESC, rj.eigyosho_id ASC",
                    vec![f, t],
                ),
                _ => (
                    "SELECT rj.month, rj.eigyosho_id, rj.status, \
                            rj.fingerprint_before, rj.fingerprint_after, \
                            rj.raw_path, rj.created_at, rj.computed_at, \
                            rj.fingerprint_changed_at, \
                            rj.r2_synced_at, rj.last_error, \
                            COALESCE(vc.verified_count, 0), \
                            COALESCE(vc.verified_ok,    0), \
                            COALESCE(vc.verified_ng,    0) \
                     FROM recalc_jobs rj \
                     LEFT JOIN verify_coverage vc \
                       ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id \
                     ORDER BY rj.month DESC, rj.eigyosho_id ASC",
                    vec![],
                ),
            };
            let mut stmt = guard
                .prepare(sql)
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                    Ok(RecalcJob {
                        month: r.get(0)?,
                        eigyosho_id: r.get(1)?,
                        status: r.get(2)?,
                        fingerprint_before: r.get(3)?,
                        fingerprint_after: r.get(4)?,
                        raw_path: r.get(5)?,
                        created_at: r.get(6)?,
                        computed_at: r.get(7)?,
                        fingerprint_changed_at: r.get(8)?,
                        r2_synced_at: r.get(9)?,
                        last_error: r.get(10)?,
                        verified_count: r.get(11)?,
                        verified_ok: r.get(12)?,
                        verified_ng: r.get(13)?,
                    })
                })
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<RecalcJob>, LocalStoreError>(rows)
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

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
                    "SELECT rj.month, rj.eigyosho_id, rj.status, \
                            rj.fingerprint_before, rj.fingerprint_after, \
                            rj.raw_path, rj.created_at, rj.computed_at, \
                            rj.fingerprint_changed_at, \
                            rj.r2_synced_at, rj.last_error, \
                            COALESCE(vc.verified_count, 0), \
                            COALESCE(vc.verified_ok,    0), \
                            COALESCE(vc.verified_ng,    0) \
                     FROM recalc_jobs rj \
                     LEFT JOIN verify_coverage vc \
                       ON vc.month = rj.month AND vc.eigyosho_id = rj.eigyosho_id \
                     WHERE rj.month = ?1 AND rj.eigyosho_id = ?2",
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
                        fingerprint_changed_at: r.get(8)?,
                        r2_synced_at: r.get(9)?,
                        last_error: r.get(10)?,
                        verified_count: r.get(11)?,
                        verified_ok: r.get(12)?,
                        verified_ng: r.get(13)?,
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
            //
            // 設計 (user 2026-06-30 「ｒ２同期待ちおかしくね?」「finger verified
            // のほうがよくね?」):
            // - **fingerprint 不変な再 recalc では status / r2_synced_at /
            //   fingerprint_changed_at をすべて維持**する。data が変わっていない
            //   ので R2 既送信オブジェクトは valid のまま。旧 SQL は status='computed'
            //   に下げてしまい「R2 同期済 → 同期待ち」と誤表示するバグがあった。
            // - **fingerprint 変化時は status='computed' + r2_synced_at=NULL +
            //   fingerprint_changed_at=now** で R2 再送信を促す。
            // - `computed_at` は不変/変化どちらでも更新 (= 最終 recalc 試行時刻)。
            guard
                .execute(
                    "INSERT INTO recalc_jobs \
                     (month, eigyosho_id, status, fingerprint_before, fingerprint_after, \
                      raw_path, created_at, computed_at, fingerprint_changed_at, \
                      r2_synced_at, last_error) \
                 VALUES (?1, ?2, 'computed', NULL, ?3, ?4, ?5, ?5, ?5, NULL, NULL) \
                 ON CONFLICT(month, eigyosho_id) DO UPDATE SET \
                     status = CASE \
                         WHEN recalc_jobs.fingerprint_after = excluded.fingerprint_after \
                              THEN recalc_jobs.status \
                         ELSE 'computed' \
                     END, \
                     fingerprint_before = recalc_jobs.fingerprint_after, \
                     fingerprint_after = excluded.fingerprint_after, \
                     raw_path = excluded.raw_path, \
                     computed_at = excluded.computed_at, \
                     fingerprint_changed_at = CASE \
                         WHEN recalc_jobs.fingerprint_after = excluded.fingerprint_after \
                              THEN recalc_jobs.fingerprint_changed_at \
                         ELSE excluded.fingerprint_changed_at \
                     END, \
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
    /// verify_jobs 由来の coverage を同梱して返す (Refs #762 R2 sync gate)。
    ///
    /// `from` / `to` (YYYY-MM、inclusive) が両方 `Some` なら期間内のみ。`None` は全件
    /// (= cron / 後方互換)。UI 操作で期間に従わせる用途は両方 `Some` で叩く。
    pub async fn list_r2_pending(
        &self,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<R2PendingRow>, LocalStoreError> {
        let from = from.map(|s| s.to_string());
        let to = to.map(|s| s.to_string());
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let (sql, params): (&str, Vec<&dyn rusqlite::ToSql>) = match (&from, &to) {
                (Some(f), Some(t)) => (
                    "SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at, \
                            verified_count, verified_ok, verified_ng \
                     FROM r2_pending \
                     WHERE month BETWEEN ?1 AND ?2",
                    vec![f, t],
                ),
                _ => (
                    "SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at, \
                            verified_count, verified_ok, verified_ng \
                     FROM r2_pending",
                    vec![],
                ),
            };
            let mut stmt = guard
                .prepare(sql)
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params.iter()), |r| {
                    Ok(R2PendingRow {
                        month: r.get(0)?,
                        eigyosho_id: r.get(1)?,
                        raw_path: r.get(2)?,
                        fingerprint_after: r.get(3)?,
                        computed_at: r.get(4)?,
                        verified_count: r.get(5)?,
                        verified_ok: r.get(6)?,
                        verified_ng: r.get(7)?,
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

    /// `verify_jobs` に 1 行 UPSERT。primary key = (unko_date, eigyosho_id, cal)。
    #[allow(clippy::too_many_arguments)]
    pub async fn upsert_verify_job(
        &self,
        unko_date: &str,
        eigyosho_id: i64,
        cal: bool,
        ok: bool,
        skipped_reason: Option<&str>,
        diff_json: Option<&str>,
        row_count: i64,
        elapsed_ms: i64,
        ran_at: &str,
    ) -> Result<(), LocalStoreError> {
        let unko_date = unko_date.to_string();
        let month = if unko_date.len() >= 7 {
            unko_date[0..7].to_string()
        } else {
            return Err(LocalStoreError::QueryError(format!(
                "unko_date が短すぎる (YYYY-MM-DD 形式必要): {unko_date}"
            )));
        };
        let cal_int: i64 = if cal { 1 } else { 0 };
        let ok_int: i64 = if ok { 1 } else { 0 };
        let skipped_reason = skipped_reason.map(|s| s.to_string());
        let diff_json = diff_json.map(|s| s.to_string());
        let ran_at = ran_at.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            guard
                .execute(
                    "INSERT INTO verify_jobs \
                         (unko_date, eigyosho_id, cal, month, ok, skipped_reason, \
                          diff_json, row_count, elapsed_ms, ran_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) \
                     ON CONFLICT(unko_date, eigyosho_id, cal) DO UPDATE SET \
                         month          = excluded.month, \
                         ok             = excluded.ok, \
                         skipped_reason = excluded.skipped_reason, \
                         diff_json      = excluded.diff_json, \
                         row_count      = excluded.row_count, \
                         elapsed_ms     = excluded.elapsed_ms, \
                         ran_at         = excluded.ran_at",
                    params![
                        unko_date,
                        eigyosho_id,
                        cal_int,
                        month,
                        ok_int,
                        skipped_reason,
                        diff_json,
                        row_count,
                        elapsed_ms,
                        ran_at,
                    ],
                )
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<(), LocalStoreError>(())
        })
        .await
        .map_err(|e| LocalStoreError::JoinError(e.to_string()))?
    }

    /// `verify_jobs` を範囲指定で取得 (履歴一覧用)。
    /// `eigyosho_id` を指定すれば絞り込み、空なら全営業所。
    pub async fn list_verify_jobs(
        &self,
        from: &str,
        to: &str,
        eigyosho_id: Option<i64>,
    ) -> Result<Vec<VerifyJobRow>, LocalStoreError> {
        let from = from.to_string();
        let to = to.to_string();
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let guard = futures_lock(&conn);
            let (sql, params_vec): (&str, Vec<rusqlite::types::Value>) =
                if let Some(eid) = eigyosho_id {
                    (
                        "SELECT unko_date, eigyosho_id, cal, month, ok, skipped_reason, \
                            diff_json, row_count, elapsed_ms, ran_at \
                     FROM verify_jobs \
                     WHERE unko_date BETWEEN ?1 AND ?2 AND eigyosho_id = ?3 \
                     ORDER BY unko_date ASC, eigyosho_id ASC, cal ASC",
                        vec![from.into(), to.into(), eid.into()],
                    )
                } else {
                    (
                        "SELECT unko_date, eigyosho_id, cal, month, ok, skipped_reason, \
                            diff_json, row_count, elapsed_ms, ran_at \
                     FROM verify_jobs \
                     WHERE unko_date BETWEEN ?1 AND ?2 \
                     ORDER BY unko_date ASC, eigyosho_id ASC, cal ASC",
                        vec![from.into(), to.into()],
                    )
                };
            let mut stmt = guard
                .prepare(sql)
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            let rows = stmt
                .query_map(rusqlite::params_from_iter(params_vec), |r| {
                    Ok(VerifyJobRow {
                        unko_date: r.get(0)?,
                        eigyosho_id: r.get(1)?,
                        cal: r.get(2)?,
                        month: r.get(3)?,
                        ok: r.get(4)?,
                        skipped_reason: r.get(5)?,
                        diff_json: r.get(6)?,
                        row_count: r.get(7)?,
                        elapsed_ms: r.get(8)?,
                        ran_at: r.get(9)?,
                    })
                })
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| LocalStoreError::QueryError(e.to_string()))?;
            Ok::<Vec<VerifyJobRow>, LocalStoreError>(rows)
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
            tx.execute(
                "DELETE FROM uriage_person_partner_daily WHERE month = ?1 AND eigyosho_id = ?2",
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
                    DROP VIEW  IF EXISTS verify_coverage;
                    DROP VIEW  IF EXISTS uriage_person_monthly;
                    DROP TABLE IF EXISTS verify_jobs;
                    DROP TABLE IF EXISTS recalc_jobs;
                    DROP TABLE IF EXISTS uriage_person_partner_daily;
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
            // 既存テストは合計値のみ assert する。y0 は 0 で初期化
            // (= 横横=1 として扱われる、テストでは区別しないので問題なし)
            kingaku_y0: 0,
            yosha_kingaku_y0: 0,
            kensuu_y0: 0,
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
    async fn record_recalc_computed_preserves_status_and_fingerprint_at_when_unchanged() {
        // user 2026-06-30 「ｒ２同期待ちおかしくね?」「finger verified のほうがよくね?」:
        // fingerprint 不変な再 recalc で status='computed' に下げて r2_synced_at は維持、
        // という状態が UI で「R2 同期待ち + r2_synced_at が非 null」の混乱表示を生んでいた。
        //
        // 修正後: fingerprint 不変なら status / r2_synced_at / fingerprint_changed_at を全て維持する。
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t1")
            .await
            .unwrap();
        store.record_r2_synced("2026-06", 1, "t2").await.unwrap();
        // 同 fingerprint で 2 回目 recalc → status は r2_synced のまま維持されるべき
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t3")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.status, "r2_synced", "fingerprint 不変なら status 維持");
        assert_eq!(
            job.r2_synced_at.as_deref(),
            Some("t2"),
            "fingerprint 不変なら r2_synced_at 維持"
        );
        // fingerprint_changed_at は 1 回目の時刻 (t1) のまま維持
        assert_eq!(
            job.fingerprint_changed_at.as_deref(),
            Some("t1"),
            "fingerprint 不変なら fingerprint_changed_at 維持"
        );
        // computed_at は最新の t3 (= 再 recalc 試行時刻)
        assert_eq!(job.computed_at.as_deref(), Some("t3"));
    }

    #[tokio::test]
    async fn migrate_repairs_inconsistent_computed_with_r2_synced_at() {
        // user 2026-06-30 「R2 同期押しても対象なし」 (PR #51 以前の bug の遺物):
        // status='computed' AND r2_synced_at IS NOT NULL という不整合行を migrate() が
        // status='r2_synced' に直すことを確認する。
        //
        // この test は LocalStore::open() を直接叩いて migrate() を走らせて検証。
        // raw connection で broken state を作ってから LocalStore::open() を呼ぶ。
        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-test-repair-broken-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        {
            let conn = rusqlite::Connection::open(&tmp).unwrap();
            conn.execute_batch(
                "CREATE TABLE recalc_jobs (
                    month TEXT NOT NULL,
                    eigyosho_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    fingerprint_before TEXT,
                    fingerprint_after TEXT,
                    raw_path TEXT,
                    created_at TEXT NOT NULL,
                    computed_at TEXT,
                    r2_synced_at TEXT,
                    last_error TEXT,
                    PRIMARY KEY (month, eigyosho_id)
                );
                INSERT INTO recalc_jobs (month, eigyosho_id, status, fingerprint_after, raw_path, created_at, computed_at, r2_synced_at)
                VALUES
                    ('2026-06', 1, 'computed', 'fp1', '/tmp/r.gz', 't0', 't1', 't2'),  -- broken: should be r2_synced
                    ('2026-06', 2, 'computed', 'fp2', '/tmp/r.gz', 't0', 't1', NULL),  -- legit pending
                    ('2026-06', 3, 'r2_synced', 'fp3', '/tmp/r.gz', 't0', 't1', 't2'), -- already correct
                    ('2026-06', 4, 'failed', NULL, NULL, 't0', NULL, NULL);            -- failed
                ",
            )
            .unwrap();
        }
        let store = LocalStore::open(tmp.to_str().unwrap()).expect("open");

        let j1 = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(j1.status, "r2_synced", "broken 行は r2_synced に修復");
        let j2 = store.get_recalc_job("2026-06", 2).await.unwrap().unwrap();
        assert_eq!(j2.status, "computed", "legit pending は触らない");
        let j3 = store.get_recalc_job("2026-06", 3).await.unwrap().unwrap();
        assert_eq!(j3.status, "r2_synced", "既に r2_synced なら触らない");
        let j4 = store.get_recalc_job("2026-06", 4).await.unwrap().unwrap();
        assert_eq!(j4.status, "failed", "failed は触らない");

        let _ = std::fs::remove_file(&tmp);
    }

    #[tokio::test]
    async fn r2_pending_includes_unsynced_even_when_fingerprint_unchanged() {
        // user 2026-06-30 「R2 同期押してもこれ」 — r2_synced_at IS NULL なのに
        // pending list に出ない不具合。原因: 旧 r2_pending view が
        // `fingerprint_before != fingerprint_after` を必須条件にしていた。
        //
        // 2 回連続 record_recalc_computed (同 fp、間に sync なし) → status='computed'、
        // fp_before == fp_after、r2_synced_at IS NULL という状態が作れる (PR #51 前の
        // bug code path の遺物)。本テストでは sync 未実施なので pending に**残る**べき。
        let store = LocalStore::open(":memory:").unwrap();
        // 1 回目: status='computed', fp_before=NULL, fp_after=fp1, r2_synced_at=NULL
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t1")
            .await
            .unwrap();
        // 2 回目 (同 fp): status='computed' のまま (PR #51 後の logic: 元 status='computed' なので
        // CASE で維持), fp_before=fp1, fp_after=fp1, r2_synced_at=NULL のまま (sync 未実施)
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t2")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(job.status, "computed");
        assert_eq!(job.fingerprint_before.as_deref(), Some("fp1"));
        assert_eq!(job.fingerprint_after.as_deref(), Some("fp1"));
        assert_eq!(job.r2_synced_at, None);

        // pending list に **出る** べき (旧 view では fp_before == fp_after で除外されていた)
        let pending = store.list_r2_pending(None, None).await.unwrap();
        assert_eq!(pending.len(), 1, "fp 不変でも未同期なら pending に出るべき");
        assert_eq!(pending[0].month, "2026-06");
        assert_eq!(pending[0].eigyosho_id, 1);
    }

    #[tokio::test]
    async fn record_recalc_computed_updates_fingerprint_at_on_change() {
        // fingerprint 変化時は fingerprint_changed_at が最新 computed_at に更新される
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_computed("2026-06", 1, "fp1", Some("/tmp/r.gz"), "t1")
            .await
            .unwrap();
        store.record_r2_synced("2026-06", 1, "t2").await.unwrap();
        // fingerprint 変化 → status='computed' に降格、r2_synced_at=NULL、
        // fingerprint_changed_at=t3 に更新
        store
            .record_recalc_computed("2026-06", 1, "fp2", Some("/tmp/r2.gz"), "t3")
            .await
            .unwrap();
        let job = store.get_recalc_job("2026-06", 1).await.unwrap().unwrap();
        assert_eq!(
            job.status, "computed",
            "fingerprint 変化なら status を computed に戻す"
        );
        assert_eq!(
            job.r2_synced_at, None,
            "fingerprint 変化で r2_synced_at リセット"
        );
        assert_eq!(
            job.fingerprint_changed_at.as_deref(),
            Some("t3"),
            "fingerprint 変化なら fingerprint_changed_at 更新"
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

        let pending = store.list_r2_pending(None, None).await.unwrap();
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
        let pending = store.list_r2_pending(None, None).await.unwrap();
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
    async fn list_recalc_jobs_joins_verify_coverage() {
        // recalc 1 件 + verify 3 件 (うち 1 件 NG) で aggregate 結果を確認
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_computed(
                "2026-06",
                5,
                "fp1",
                Some("/tmp/r.gz"),
                "2026-06-30T00:00:00Z",
            )
            .await
            .unwrap();
        // 3 件: (06-01, cal=false, ok), (06-01, cal=true, ok), (06-02, cal=false, ng)
        store
            .upsert_verify_job(
                "2026-06-01",
                5,
                false,
                true,
                None,
                None,
                10,
                42,
                "2026-06-30T00:00:01Z",
            )
            .await
            .unwrap();
        store
            .upsert_verify_job(
                "2026-06-01",
                5,
                true,
                true,
                None,
                None,
                7,
                33,
                "2026-06-30T00:00:02Z",
            )
            .await
            .unwrap();
        store
            .upsert_verify_job(
                "2026-06-02",
                5,
                false,
                false,
                None,
                Some("{\"diff\":1}"),
                10,
                40,
                "2026-06-30T00:00:03Z",
            )
            .await
            .unwrap();

        let jobs = store
            .list_recalc_jobs(Some("2026-06"), Some("2026-06"))
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        let j = &jobs[0];
        assert_eq!(j.eigyosho_id, 5);
        assert_eq!(j.verified_count, 3);
        assert_eq!(j.verified_ok, 2);
        assert_eq!(j.verified_ng, 1);
    }

    #[tokio::test]
    async fn list_recalc_jobs_returns_all_when_from_to_none() {
        // from/to が None なら全 month の row を返す (= 状態サマリ全期間表示用)
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_computed("2026-04", 1, "fp", Some("/tmp/a.gz"), "t1")
            .await
            .unwrap();
        store
            .record_recalc_computed("2026-06", 1, "fp", Some("/tmp/b.gz"), "t2")
            .await
            .unwrap();
        store
            .record_recalc_computed("2026-05", 1, "fp", Some("/tmp/c.gz"), "t3")
            .await
            .unwrap();
        let jobs = store.list_recalc_jobs(None, None).await.unwrap();
        assert_eq!(jobs.len(), 3);
        // 降順: 2026-06 → 2026-05 → 2026-04
        assert_eq!(jobs[0].month, "2026-06");
        assert_eq!(jobs[1].month, "2026-05");
        assert_eq!(jobs[2].month, "2026-04");
    }

    #[tokio::test]
    async fn list_r2_pending_filters_by_month_range() {
        // from/to を渡すと当該月の bucket だけ返る (UI 期間 R2 同期用)
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_computed("2026-04", 1, "fp", Some("/tmp/a.gz"), "t1")
            .await
            .unwrap();
        store
            .record_recalc_computed("2026-05", 1, "fp", Some("/tmp/b.gz"), "t2")
            .await
            .unwrap();
        store
            .record_recalc_computed("2026-06", 1, "fp", Some("/tmp/c.gz"), "t3")
            .await
            .unwrap();

        // 2026-05 だけ
        let pending = store
            .list_r2_pending(Some("2026-05"), Some("2026-05"))
            .await
            .unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].month, "2026-05");

        // 全件 (filter なし)
        let all = store.list_r2_pending(None, None).await.unwrap();
        assert_eq!(all.len(), 3);

        // 2026-04 ~ 2026-05 (両端 inclusive)
        let two = store
            .list_r2_pending(Some("2026-04"), Some("2026-05"))
            .await
            .unwrap();
        assert_eq!(two.len(), 2);
    }

    #[tokio::test]
    async fn list_recalc_jobs_returns_zero_for_unverified() {
        // verify_jobs に 1 件も無い時は 0 / 0 / 0 (LEFT JOIN + COALESCE)
        let store = LocalStore::open(":memory:").unwrap();
        store
            .record_recalc_computed(
                "2026-06",
                1,
                "fp1",
                Some("/tmp/r.gz"),
                "2026-06-30T00:00:00Z",
            )
            .await
            .unwrap();
        let jobs = store
            .list_recalc_jobs(Some("2026-06"), Some("2026-06"))
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].verified_count, 0);
        assert_eq!(jobs[0].verified_ok, 0);
        assert_eq!(jobs[0].verified_ng, 0);
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

    // ══════════════════════════════════════════════════════════════
    // uriage_person_partner_daily (担当者×得意先/傭車先 内訳)
    // ══════════════════════════════════════════════════════════════

    fn partner_key(person_name: &str, kind: PartnerKind, code: &str) -> PersonPartnerKey {
        PersonPartnerKey {
            person_name: person_name.to_string(),
            partner_kind: kind,
            partner_code: code.to_string(),
        }
    }

    fn partner_accum(name: &str, kingaku: i64, kensuu: i64) -> PartnerAccum {
        PartnerAccum {
            partner_name: name.to_string(),
            kingaku,
            kensuu,
            kingaku_y0: 0,
            kensuu_y0: 0,
        }
    }

    fn daily_one_day_partner(
        date: &str,
        entries: HashMap<PersonPartnerKey, PartnerAccum>,
    ) -> HashMap<String, HashMap<PersonPartnerKey, PartnerAccum>> {
        let mut m = HashMap::new();
        m.insert(date.to_string(), entries);
        m
    }

    #[tokio::test]
    async fn upsert_partner_daily_then_totals_sums_across_eigyosho_and_month() {
        let store = LocalStore::open(":memory:").unwrap();
        let mut day = HashMap::new();
        day.insert(
            partner_key("青井", PartnerKind::Customer, "CUST1-0"),
            partner_accum("得意先イチ", 10_000, 1),
        );
        day.insert(
            partner_key("青井", PartnerKind::Subcontractor, "000000-0"),
            partner_accum("", 4_000, 1),
        );
        let one = daily_one_day_partner("2026-06-15", day.clone());
        store
            .upsert_person_partner_daily("2026-06", 1, true, &one, "t1")
            .await
            .unwrap();
        // 別営業所にも同じ得意先向けの売上 → SUM されるはず
        store
            .upsert_person_partner_daily("2026-06", 9, true, &one, "t1")
            .await
            .unwrap();
        // 翌月分 (期間フィルタの検証用)
        let mut day7 = HashMap::new();
        day7.insert(
            partner_key("青井", PartnerKind::Customer, "CUST1-0"),
            partner_accum("得意先イチ", 3_000, 1),
        );
        store
            .upsert_person_partner_daily(
                "2026-07",
                1,
                true,
                &daily_one_day_partner("2026-07-01", day7),
                "t2",
            )
            .await
            .unwrap();

        let customers = store
            .person_partner_totals("青井", "2026-06", "2026-06", true, PartnerKind::Customer)
            .await
            .unwrap();
        assert_eq!(customers.len(), 1);
        assert_eq!(customers[0].partner_code, "CUST1-0");
        assert_eq!(customers[0].partner_name, "得意先イチ");
        assert_eq!(customers[0].kingaku, 20_000); // 10,000 × 2 営業所
        assert_eq!(customers[0].kensuu, 2);

        let subcontractors = store
            .person_partner_totals(
                "青井",
                "2026-06",
                "2026-06",
                true,
                PartnerKind::Subcontractor,
            )
            .await
            .unwrap();
        assert_eq!(subcontractors.len(), 1);
        assert_eq!(subcontractors[0].partner_code, "000000-0");
        assert_eq!(subcontractors[0].kingaku, 8_000); // 4,000 × 2 営業所

        // 期間を広げると 2026-07 分も乗る
        let wide = store
            .person_partner_totals("青井", "2026-06", "2026-07", true, PartnerKind::Customer)
            .await
            .unwrap();
        assert_eq!(wide.len(), 1);
        assert_eq!(wide[0].kingaku, 23_000); // 20,000 + 3,000

        // 別担当者 / cal=false / 別 partner_kind では 0 件
        assert!(store
            .person_partner_totals("大石", "2026-06", "2026-06", true, PartnerKind::Customer)
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .person_partner_totals("青井", "2026-06", "2026-06", false, PartnerKind::Customer)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn upsert_partner_daily_overwrites_bucket_in_full() {
        let store = LocalStore::open(":memory:").unwrap();
        let mut a = HashMap::new();
        a.insert(
            partner_key("青井", PartnerKind::Customer, "CUST1-0"),
            partner_accum("得意先イチ", 100, 1),
        );
        store
            .upsert_person_partner_daily(
                "2026-06",
                1,
                true,
                &daily_one_day_partner("2026-06-15", a),
                "t1",
            )
            .await
            .unwrap();

        // 2 回目: 同じ bucket に別の内容 → 1 回目の行は消える (delete-then-insert)
        let mut b = HashMap::new();
        b.insert(
            partner_key("青井", PartnerKind::Customer, "CUST2-0"),
            partner_accum("得意先ニ", 200, 1),
        );
        store
            .upsert_person_partner_daily(
                "2026-06",
                1,
                true,
                &daily_one_day_partner("2026-06-16", b),
                "t2",
            )
            .await
            .unwrap();

        let rows = store
            .person_partner_totals("青井", "2026-06", "2026-06", true, PartnerKind::Customer)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].partner_code, "CUST2-0");
        assert_eq!(rows[0].kingaku, 200);
    }

    #[tokio::test]
    async fn upsert_partner_daily_skips_zero_rows() {
        let store = LocalStore::open(":memory:").unwrap();
        let mut day = HashMap::new();
        day.insert(
            partner_key("青井", PartnerKind::Customer, "CUST1-0"),
            partner_accum("得意先イチ", 0, 0),
        );
        let n = store
            .upsert_person_partner_daily(
                "2026-06",
                1,
                true,
                &daily_one_day_partner("2026-06-15", day),
                "t1",
            )
            .await
            .unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn delete_bucket_also_clears_partner_daily() {
        let store = LocalStore::open(":memory:").unwrap();
        let mut day = HashMap::new();
        day.insert(
            partner_key("青井", PartnerKind::Customer, "CUST1-0"),
            partner_accum("得意先イチ", 100, 1),
        );
        store
            .upsert_person_partner_daily(
                "2026-06",
                1,
                true,
                &daily_one_day_partner("2026-06-15", day),
                "t1",
            )
            .await
            .unwrap();

        store.delete_bucket("2026-06", 1).await.unwrap();

        let rows = store
            .person_partner_totals("青井", "2026-06", "2026-06", true, PartnerKind::Customer)
            .await
            .unwrap();
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn rebuild_schema_recreates_partner_table() {
        let store = LocalStore::open(":memory:").unwrap();
        let mut day = HashMap::new();
        day.insert(
            partner_key("青井", PartnerKind::Customer, "CUST1-0"),
            partner_accum("得意先イチ", 100, 1),
        );
        store
            .upsert_person_partner_daily(
                "2026-06",
                1,
                true,
                &daily_one_day_partner("2026-06-15", day),
                "t1",
            )
            .await
            .unwrap();

        store.rebuild_schema().await.unwrap();

        // rebuild 後は全データ消失するが、schema はそのまま使える
        let rows = store
            .person_partner_totals("青井", "2026-06", "2026-06", true, PartnerKind::Customer)
            .await
            .unwrap();
        assert!(rows.is_empty());
    }
}

#[cfg(test)]
mod migration_upgrade_tests {
    use super::LocalStore;
    use rusqlite::Connection;

    /// 旧 (Phase 2 PR-C2) の r2_pending view 定義 (5 カラム、verify_* 無し)。
    /// 既存 DB に対して新 migrate() が走った時、`DROP VIEW IF EXISTS r2_pending` で
    /// この古い定義を破棄して新定義 (8 カラム) を作り直すかをテストする。
    const OLD_R2_PENDING_VIEW: &str = r#"
        CREATE TABLE IF NOT EXISTS recalc_jobs (
            month TEXT NOT NULL,
            eigyosho_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            fingerprint_before TEXT,
            fingerprint_after TEXT,
            raw_path TEXT,
            created_at TEXT NOT NULL,
            computed_at TEXT,
            r2_synced_at TEXT,
            last_error TEXT,
            PRIMARY KEY (month, eigyosho_id)
        );
        CREATE VIEW IF NOT EXISTS r2_pending AS
        SELECT month, eigyosho_id, raw_path, fingerprint_after, computed_at
        FROM recalc_jobs
        WHERE status = 'computed' AND r2_synced_at IS NULL AND raw_path IS NOT NULL;
    "#;

    #[test]
    fn migrate_replaces_old_r2_pending_view_with_new_columns() {
        // 1. 旧 DB を 1 つ open し、旧 view を手書きで作る (= prod の現状)
        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-test-upgrade-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch(OLD_R2_PENDING_VIEW).unwrap();
            // ここでは LocalStore::open() を呼ばず、生の view だけある状態にする
        }

        // 2. LocalStore::open() で migrate を走らせる (= 新 binary の boot 相当)
        let store = LocalStore::open(tmp.to_str().unwrap())
            .expect("LocalStore should open and migrate existing DB");

        // 3. 新 view が引けるか確認 (verified_count 等のカラムが取れる = 新定義)
        let pending = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(store.list_r2_pending(None, None))
            .expect("list_r2_pending should not fail after migrate replaces old view");
        // 行は空 (recalc_jobs に row 無し) でも query 自体は通る = view が更新済
        assert_eq!(pending.len(), 0);

        // 後始末
        let _ = std::fs::remove_file(&tmp);
    }

    /// prod の post-#44-failed state: `uriage_person_monthly` が **TABLE として残骸**
    /// (旧 deploy で TABLE → VIEW 移行時に `CREATE VIEW IF NOT EXISTS` がスキップして
    /// TABLE が居座ったまま)。新 migrate は `drop_object_if_exists` で sqlite_master
    /// の type を見て DROP TABLE を発行できるか。これが効かないと `use DROP TABLE to
    /// delete table uriage_person_monthly` エラーで binary が boot 不能 (#45 で実測)。
    #[test]
    fn migrate_drops_uriage_person_monthly_when_it_exists_as_table() {
        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-test-monthly-as-table-{}-{}.db",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        // 1. uriage_person_monthly を TABLE として作る (prod 残骸を再現)
        {
            let conn = Connection::open(&tmp).unwrap();
            conn.execute_batch(
                r#"
                CREATE TABLE uriage_person_monthly (
                    month TEXT NOT NULL,
                    person_name TEXT NOT NULL,
                    eigyosho_id INTEGER NOT NULL,
                    cal INTEGER NOT NULL,
                    kingaku INTEGER NOT NULL,
                    yosha_kingaku INTEGER NOT NULL,
                    kensuu INTEGER NOT NULL,
                    calculated_at TEXT NOT NULL,
                    PRIMARY KEY (month, person_name, eigyosho_id, cal)
                );
                "#,
            )
            .unwrap();
        }
        // 2. LocalStore::open → migrate で drop_object_if_exists が TABLE を消す
        //    → CREATE VIEW で再生成、boot が通る
        let store = LocalStore::open(tmp.to_str().unwrap())
            .expect("migrate should handle uriage_person_monthly residual TABLE");
        // 3. view として引けることを確認
        let rows = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(store.get_person_monthly("2026-06", 1, true))
            .expect("get_person_monthly should not fail (view recreated)");
        assert_eq!(rows.len(), 0);
        let _ = std::fs::remove_file(&tmp);
    }
}
