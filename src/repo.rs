use async_trait::async_trait;
use std::sync::Arc;

use crate::routes::uriage::UriageRow;

/// DB 操作の抽象化。本番は TiberiusRepo、テストは MockRepo を使う。
#[async_trait]
pub trait AppRepo: Send + Sync {
    // ── health ──
    async fn health_check(&self) -> Result<(), RepoError>;

    // ── uriage (担当者別売上、#762) ──
    /// `[運転日報明細]` から `compute_person_sum` の入力 1 行を取得する。
    /// PHP `UriageJyuchuDisplayController::make_arrays()` の **5 ケース UNION** を
    /// 1:1 で再現する (傭車 / 営業所傭車 / 傭車傭車 / sql_from_other_with_bumon /
    /// sql_from_other)。`bumon_codes` は受注/稼動部門の IN 条件、`persons_id_list`
    /// は `UriageJyuchuDisplayPersons_id` 相当 (営業所配下の担当者社員C 一覧)。
    /// `sql_options` 系は `入力担当C IN persons` で絞り、`sql_from_other_with_bumon`
    /// は逆に `入力担当C NOT IN persons` で絞る (PHP L1759)。
    async fn uriage_rows(
        &self,
        from: &str,
        to: &str,
        bumon_codes: &[String],
        persons_id_list: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError>;
}

pub type DynRepo = Arc<dyn AppRepo>;

#[derive(Debug)]
pub enum RepoError {
    PoolError,
    QueryError(String),
}

// ── TiberiusRepo: 本番用実装 ──

use crate::db::{DbConn, DbPool};

pub struct TiberiusRepo {
    /// `None` = この instance は SQL Server (CAPE#01) を使うと宣言していない
    /// (`[database] enabled = false`)。Cloud Run のように SQL Server へ到達できない
    /// 実行形態で使う。全メソッドが `RepoError::PoolError` を返し、routes は既存の
    /// マッピングどおり **503 fail-closed** になる (空の結果を返して「0 件」に
    /// 見せることは無い)。
    pool: Option<DbPool>,
}

impl TiberiusRepo {
    pub fn new(pool: DbPool) -> Self {
        Self { pool: Some(pool) }
    }

    /// SQL Server を使わないと宣言した実行形態向けの repo。
    ///
    /// 「繋ぎに行って失敗する」のではなく「そもそも宣言していない」ことを型で
    /// 表すのが要点 — 接続待ちの timeout を各リクエストで払わずに即 503 を返す。
    pub fn disabled() -> Self {
        Self { pool: None }
    }

    async fn conn(&self) -> Result<DbConn<'_>, RepoError> {
        let pool = self.pool.as_ref().ok_or(RepoError::PoolError)?;
        pool.get().await.map_err(|_| RepoError::PoolError)
    }
}

fn decode_cp932(row: &tiberius::Row, idx: usize) -> String {
    row.try_get::<&str, _>(idx)
        .ok()
        .flatten()
        .map(|s| s.trim().to_string())
        .unwrap_or_default()
}

fn get_i64(row: &tiberius::Row, idx: usize) -> i64 {
    row.try_get::<f64, _>(idx)
        .ok()
        .flatten()
        .map(|v| v as i64)
        .or_else(|| {
            row.try_get::<tiberius::numeric::Numeric, _>(idx)
                .ok()
                .flatten()
                .and_then(|d| {
                    let s = format!("{}", d);
                    s.parse::<f64>().ok()
                })
                .map(|v| v as i64)
        })
        .or_else(|| row.try_get::<i32, _>(idx).ok().flatten().map(|v| v as i64))
        .unwrap_or(0)
}

fn get_i32(row: &tiberius::Row, idx: usize) -> i32 {
    row.try_get::<i32, _>(idx).ok().flatten().unwrap_or(0)
}

/// `/health` の生死確認。Worker 側 (`workers/ichiban/logic/src/sql.rs`) と同じ文字列。
const HEALTH_SQL: &str = "SELECT 1";

#[async_trait]
impl AppRepo for TiberiusRepo {
    async fn health_check(&self) -> Result<(), RepoError> {
        let mut conn = self.conn().await?;
        conn.simple_query(HEALTH_SQL)
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        Ok(())
    }

    async fn uriage_rows(
        &self,
        from: &str,
        to: &str,
        bumon_codes: &[String],
        persons_id_list: &[i32],
    ) -> Result<Vec<UriageRow>, RepoError> {
        if bumon_codes.is_empty() {
            return Ok(vec![]);
        }
        let mut conn = self.conn().await?;

        // 受注部門 IN (...) を動的に組む。bumon_codes は呼び出し側で whitelist 済み
        // (営業所マスタから引いた `'010'`,`'011'` 等の固定形式) のため SQL injection
        // 上は安全だが、念のため英数字のみに絞ってから組み立てる。
        let safe_bumon: Vec<String> = bumon_codes
            .iter()
            .filter(|c| {
                !c.is_empty() && c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            })
            .cloned()
            .collect();
        if safe_bumon.is_empty() {
            return Ok(vec![]);
        }
        let bumon_in = safe_bumon
            .iter()
            .map(|c| format!("'{}'", c))
            .collect::<Vec<_>>()
            .join(",");

        // 入力担当C IN (...) — persons_id_list は数値なのでそのまま組み立てる
        // (型は i32、untrusted 入力ではないが念のため format で固定)。
        // 空リストでも SQL は valid であるべき: IN () は SQL Server で構文エラーに
        // なるので NULL (= 全件 false) で埋める fallback を入れる。
        let persons_in = if persons_id_list.is_empty() {
            "NULL".to_string()
        } else {
            persons_id_list
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
                .join(",")
        };

        // PHP `UriageJyuchuDisplayController::make_arrays()` の **5 ケース UNION** を再現。
        //
        // PHP L1769-1791:
        //   foreach ($UriageJyuchuDisplayPersons_id as $ddp) {
        //       $this->make_array(['入力担当C in' => $ddp]);  // sql_options の 3 ケース
        //   }
        //   sql_from_other          → 受注∉ AND 稼動∈ AND 傭車先≠000000
        //   sql_from_other_with_bumon → 受注∈ AND 稼動∉ AND 傭車先=000000 AND 入力担当C ∉ persons
        //
        // sql_options (PHP L1827-1850) は `make_yosha_sql` を base に 3 ケース:
        //   - 傭車:       稼動∈ AND 配車K=1 AND 入力担当C ∈ persons    → 横横=0
        //   - 営業所傭車: 稼動∉ AND 配車K=0 AND 入力担当C ∈ persons    → 横横=1
        //   - 傭車傭車:   稼動∉ AND 配車K=1 AND 入力担当C ∈ persons    → 横横=1
        //
        // つまり 5 つの subquery を UNION ALL する (PHP は array_push なので重複なし
        // のはずだが、PHP の row レベルでも 5 case 互いに排他 = 配車K=0/1 + 稼動部門
        // ∈/∉ で組み分けされている)。
        //
        // 香月 NG 行 (#762、user 2026-06-30) の原因:
        //   営業所 10、入力担当C=1180 (∈ persons)、傭車先=000000、配車K=9 (未配車)、
        //   受注∈ AND 稼動∉。PHP 5 ケース全部 hit せず → $sum 0 円。
        //   旧 Rust SQL は `(受注∈ AND (稼動∉ OR 配車K=1))` で拾ってしまっていた。
        //   配車K=9 は sql_options の 3 ケース (配車K=0/1) からも漏れ、
        //   sql_from_other_with_bumon は 入力担当C ∉ persons が必要で hit せず。
        //
        // 細部:
        // - 品名N の調整行除外は全角空白 U+3000 (PHP L1708-1709 と同形)
        // - `日報K != 3` (PHP L1710)
        // - `請求K=2 → 備考2='表示' or LIKE '売上%'` (PHP L1712/1738/1760 OR superset)
        // - `NOT (請求K='1' AND 備考2='請求のみ')` (PHP L1713)
        // - 社員R は **TOP 1 スカラサブクエリ** で引く (社員ﾏｽﾀ 複数行/社員C による
        //   JOIN ファンアウト防止)
        // - 順序は print テンプレ表示順 (運行年月日 ASC, 管理C ASC, LC ASC)
        // - 請求K / 入力担当C は varchar の可能性あり `TRY_CAST(... AS INT)` で int 化
        // - 共通 WHERE 句を repeat する代わりに subquery を inline で書き、外側で
        //   ORDER BY する形 (UNION ALL は ORDER BY を最外殻に置く制約があるため)
        let select_cols = "\
                 t.[横横] AS [横横], \
                 ISNULL(TRY_CAST(t.[請求K] AS INT), 0) AS [請求K], \
                 ISNULL(t.[備考2], '') AS [備考2], \
                 ISNULL(TRY_CAST(t.[入力担当C] AS INT), 0) AS [入力担当C], \
                 ISNULL(t.[稼動部門], '') AS [稼動部門], \
                 ISNULL(t.[金額], 0) AS [金額], \
                 ISNULL(t.[値引], 0) AS [値引], \
                 ISNULL(t.[割増], 0) AS [割増], \
                 ISNULL(t.[実費], 0) AS [実費], \
                 ISNULL(t.[傭車金額], 0) AS [傭車金額], \
                 ISNULL(t.[傭車値引], 0) AS [傭車値引], \
                 ISNULL(t.[傭車割増], 0) AS [傭車割増], \
                 ISNULL(t.[傭車実費], 0) AS [傭車実費], \
                 ISNULL((SELECT TOP 1 e.[社員R] FROM [社員ﾏｽﾀ] e WHERE e.[社員C] = t.[入力担当C]), '') AS [社員R], \
                 ISNULL(t.[傭車先C], '000000') AS [傭車先C], \
                 CONVERT(varchar(10), t.[運行年月日], 23) AS [運行年月日], \
                 CONVERT(varchar(10), t.[売上年月日], 23) AS [売上年月日], \
                 CONCAT(ISNULL(t.[得意先C], ''), '-', ISNULL(t.[得意先H], '')) AS [得意先複合キー], \
                 ISNULL((SELECT TOP 1 c.[得意先N] FROM [得意先ﾏｽﾀ] c \
                   WHERE c.[得意先C] = t.[得意先C] AND c.[得意先H] = t.[得意先H]), '') AS [得意先N], \
                 CONCAT(ISNULL(t.[傭車先C], ''), '-', ISNULL(t.[傭車先H], '')) AS [傭車先複合キー], \
                 ISNULL((SELECT TOP 1 y.[傭車先N] FROM [傭車先ﾏｽﾀ] y \
                   WHERE y.[傭車先C] = t.[傭車先C] AND y.[傭車先H] = t.[傭車先H]), '') AS [傭車先N], \
                 t.[管理C] AS [管理C], t.[LC] AS [LC]";

        // 各 case 共通の WHERE 述語 (日付・品名・日報K・請求K・備考2 系)
        let common_where = "\
                 [品名N] NOT IN ('※\u{3000}請求一括調整明細\u{3000}※', '※\u{3000}傭車一括調整明細\u{3000}※') \
                 AND ISNULL([日報K], 0) != 3 \
                 AND NOT ([請求K] = '1' AND [備考2] = '請求のみ') \
                 AND ([請求K] != '2' OR [備考2] = '表示' OR [備考2] LIKE '売上%') \
                 AND [運行年月日] >= @P1 AND [運行年月日] <= @P2";

        // Case 1: 傭車 (横横=0)
        //   make_yosha_sql + sql_options('傭車'):
        //   受注∈ AND 稼動∈ AND 配車K='1' AND 入力担当C ∈ persons
        let case_yosha = format!(
            "SELECT 0 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] IN ({bumon_in}) \
               AND [配車K] = '1' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 2: 営業所傭車 (横横=1)
        //   受注∈ AND 稼動∉ AND 配車K='0' AND 入力担当C ∈ persons
        let case_eigyosho_yosha = format!(
            "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND [配車K] = '0' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 3: 傭車傭車 (横横=1)
        //   受注∈ AND 稼動∉ AND 配車K='1' AND 入力担当C ∈ persons
        let case_yosha_yosha = format!(
            "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND [配車K] = '1' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 4: sql_from_other_with_bumon (横横=1、PHP L1747-1767)
        //   受注∈ AND 稼動∉ AND 傭車先=000000 AND 入力担当C ∉ persons
        //   ※ 配車K 条件は無い (= 配車K=9 等もここで拾われる、ただし 入力担当C ∉ persons の人だけ)
        let case_with_bumon = format!(
            "SELECT 1 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] IN ({bumon_in}) AND [稼動部門] NOT IN ({bumon_in}) \
               AND ISNULL([傭車先C], '000000') = '000000' \
               AND ISNULL(TRY_CAST([入力担当C] AS INT), 0) NOT IN ({persons_in}) \
               AND {common_where}"
        );
        // Case 5: sql_from_other (横横=0、PHP L1725-1745)
        //   受注∉ AND 稼動∈ AND 傭車先≠000000
        let case_from_other = format!(
            "SELECT 0 AS [横横], * FROM [運転日報明細] \
             WHERE [受注部門] NOT IN ({bumon_in}) AND [稼動部門] IN ({bumon_in}) \
               AND ISNULL([傭車先C], '000000') != '000000' \
               AND {common_where}"
        );

        let query = format!(
            "SELECT {select_cols} FROM ( \
                 {case_yosha} \
                 UNION ALL {case_eigyosho_yosha} \
                 UNION ALL {case_yosha_yosha} \
                 UNION ALL {case_with_bumon} \
                 UNION ALL {case_from_other} \
             ) AS t \
             ORDER BY t.[運行年月日] ASC, t.[管理C] ASC, t.[LC] ASC"
        );

        let stream = conn
            .query(&query, &[&from, &to])
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;
        let rows = stream
            .into_first_result()
            .await
            .map_err(|e| RepoError::QueryError(e.to_string()))?;

        Ok(Self::rows_to_uriage(&rows))
    }
}

// ── Row → Raw 変換ヘルパー ──
impl TiberiusRepo {
    fn rows_to_uriage(rows: &[tiberius::Row]) -> Vec<UriageRow> {
        rows.iter()
            .map(|r| UriageRow {
                yokoyoko: get_i32(r, 0),
                seikyu_k: get_i32(r, 1),
                biko2: decode_cp932(r, 2),
                nyuryoku_tanto_c: get_i32(r, 3),
                kado_bumon: decode_cp932(r, 4),
                kingaku: get_i64(r, 5),
                nebiki: get_i64(r, 6),
                warimashi: get_i64(r, 7),
                jippi: get_i64(r, 8),
                yosha_kingaku: get_i64(r, 9),
                yosha_nebiki: get_i64(r, 10),
                yosha_warimashi: get_i64(r, 11),
                yosha_jippi: get_i64(r, 12),
                shain_r: decode_cp932(r, 13),
                yoshasaki_c: decode_cp932(r, 14),
                // CONVERT(varchar(10), …, 23) で 'YYYY-MM-DD' 文字列が返る (locale 非依存)
                unko_date: decode_cp932(r, 15),
                uriage_date: decode_cp932(r, 16),
                tokuisaki_key: decode_cp932(r, 17),
                tokuisaki_n: decode_cp932(r, 18),
                yoshasaki_key: decode_cp932(r, 19),
                yoshasaki_n: decode_cp932(r, 20),
            })
            .collect()
    }
}
