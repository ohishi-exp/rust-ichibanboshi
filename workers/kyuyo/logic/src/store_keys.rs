//! 給与の derived store の鍵と DDL (Refs #106 / #322)。オンプレ版 (repo ルートの `src/kyuyo/store.rs`、
//! rusqlite) と Worker の DO `KyuyoState` (Durable Object の SQLite) の両方がこの定義を使う —
//! 表の形・scope の文字列をここ 1 か所で決め、2 つの保存先で食い違わせない。
//!
//! 保存するのは応答型の serde JSON そのもので、源泉 (給与大臣) から全量を作り直せる。だから
//! migration はしない: 版 ([`SCHEMA_VERSION`]) が違えば [`DROP_TABLES_SQL`] → [`CREATE_TABLES_SQL`]。
//! 版の置き場は保存先ごとに違う (オンプレは `PRAGMA user_version`、DO は自前の表)。

/// schema 版。互換を壊す変更をしたら +1 する — 旧版の表は drop → 再作成され、次の read-through /
/// sync で埋まり直す (derived store)。
pub const SCHEMA_VERSION: i32 = 1;

/// 3 表を消す。版が違うときだけ流す。
pub const DROP_TABLES_SQL: &str = "DROP TABLE IF EXISTS kyuyo_payroll;
                 DROP TABLE IF EXISTS kyuyo_employees;
                 DROP TABLE IF EXISTS kyuyo_sync_state;";

/// 3 表を作る (既にあれば何もしない)。
pub const CREATE_TABLES_SQL: &str = "CREATE TABLE IF NOT EXISTS kyuyo_payroll (
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
             );";

/// sync 済み給与明細 (payroll scope) の一覧。列は scope, synced_at, row_count の順。
/// 各行の scope は [`parse_payroll_scope`] で (会社, 月) に分ける。
pub const PAYROLL_SYNCED_SQL: &str = "SELECT scope, synced_at, row_count FROM kyuyo_sync_state
                     WHERE scope LIKE 'payroll:%'
                     ORDER BY scope ASC";

// ── 行の読み書き (Worker の DO 用。プレースホルダは `?` の位置指定) ─────────────────────────
// オンプレ版 (`src/kyuyo/store.rs`、rusqlite の `?1`) と同じ文。DO はこれらを await を挟まずに続けて流すので、
// 1 回の put の書き込みは暗黙のトランザクションで一度に確定する (オンプレ版の明示トランザクションと同じ効果)。

/// sync_state 1 行 (synced_at, company_name, warnings_json)。引数: scope。
pub const SYNC_STATE_SELECT_SQL: &str =
    "SELECT synced_at, company_name, warnings_json FROM kyuyo_sync_state WHERE scope = ?";

/// sync_state 1 行を消す。put が途中で失敗したとき、行の欠けた scope を命中させないために流す。引数: scope。
pub const SYNC_STATE_DELETE_SQL: &str = "DELETE FROM kyuyo_sync_state WHERE scope = ?";

/// 給与明細の row_json (応答配列の順)。引数: company, month。
pub const PAYROLL_ROWS_SQL: &str =
    "SELECT row_json FROM kyuyo_payroll WHERE company = ? AND month = ? ORDER BY seq ASC";

/// 給与明細を消す (put の最初)。引数: company, month。
pub const PAYROLL_DELETE_SQL: &str = "DELETE FROM kyuyo_payroll WHERE company = ? AND month = ?";

/// 給与明細 1 行。引数: company, month, seq, row_json。
pub const PAYROLL_INSERT_SQL: &str =
    "INSERT INTO kyuyo_payroll (company, month, seq, row_json) VALUES (?, ?, ?, ?)";

/// 給与明細の sync_state (company_name は使わないので '' のまま)。引数: scope, synced_at, row_count, warnings_json。
pub const PAYROLL_SYNC_STATE_UPSERT_SQL: &str =
    "INSERT INTO kyuyo_sync_state (scope, synced_at, row_count, company_name, warnings_json) \
     VALUES (?, ?, ?, '', ?) \
     ON CONFLICT (scope) DO UPDATE SET synced_at = excluded.synced_at, \
     row_count = excluded.row_count, warnings_json = excluded.warnings_json";

/// 社員マスタの row_json (応答配列の順)。引数: company, nendo。
pub const EMPLOYEES_ROWS_SQL: &str =
    "SELECT row_json FROM kyuyo_employees WHERE company = ? AND nendo = ? ORDER BY seq ASC";

/// 社員マスタを消す (put の最初)。引数: company, nendo。
pub const EMPLOYEES_DELETE_SQL: &str =
    "DELETE FROM kyuyo_employees WHERE company = ? AND nendo = ?";

/// 社員マスタ 1 行。引数: company, nendo, seq, row_json。
pub const EMPLOYEES_INSERT_SQL: &str =
    "INSERT INTO kyuyo_employees (company, nendo, seq, row_json) VALUES (?, ?, ?, ?)";

/// 社員マスタの sync_state。引数: scope, synced_at, row_count, company_name, warnings_json。
pub const EMPLOYEES_SYNC_STATE_UPSERT_SQL: &str =
    "INSERT INTO kyuyo_sync_state (scope, synced_at, row_count, company_name, warnings_json) \
     VALUES (?, ?, ?, ?, ?) \
     ON CONFLICT (scope) DO UPDATE SET synced_at = excluded.synced_at, \
     row_count = excluded.row_count, company_name = excluded.company_name, \
     warnings_json = excluded.warnings_json";

/// 給与明細 (会社×月) の sync_state の鍵。
pub fn payroll_scope(company: &str, month: &str) -> String {
    format!("payroll:{company}:{month}")
}

/// 社員マスタ (会社×年度) の sync_state の鍵。
pub fn employees_scope(company: &str, nendo: i32) -> String {
    format!("employees:{company}:{nendo}")
}

/// [`payroll_scope`] の逆 (`payroll:{company}:{month}` → (company, month))。形が合わなければ `None`。
/// 種別の部分は見ない — 呼び出し側は [`PAYROLL_SYNCED_SQL`] で payroll に絞ってから渡す。
pub fn parse_payroll_scope(scope: &str) -> Option<(String, String)> {
    let mut parts = scope.splitn(3, ':');
    let _kind = parts.next()?;
    let company = parts.next()?.to_string();
    let month = parts.next()?.to_string();
    Some((company, month))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scopes() {
        assert_eq!(payroll_scope("0100", "2026-06"), "payroll:0100:2026-06");
        assert_eq!(employees_scope("0100", 8), "employees:0100:8");
    }

    #[test]
    fn parse_round_trip() {
        let scope = payroll_scope("0100", "2026-06");
        assert_eq!(
            parse_payroll_scope(&scope),
            Some(("0100".to_string(), "2026-06".to_string()))
        );
    }

    #[test]
    fn parse_keeps_colons_in_month() {
        // splitn(3): 3 つ目以降の ':' は月の側に残る (旧実装と同じ)
        assert_eq!(
            parse_payroll_scope("payroll:0100:a:b"),
            Some(("0100".to_string(), "a:b".to_string()))
        );
    }

    #[test]
    fn parse_rejects_short() {
        assert_eq!(parse_payroll_scope("payroll"), None);
        assert_eq!(parse_payroll_scope("payroll:0100"), None);
    }

    #[test]
    fn row_statements() {
        let cases = [
            (SYNC_STATE_SELECT_SQL, "kyuyo_sync_state", 1),
            (SYNC_STATE_DELETE_SQL, "kyuyo_sync_state", 1),
            (PAYROLL_ROWS_SQL, "kyuyo_payroll", 2),
            (PAYROLL_DELETE_SQL, "kyuyo_payroll", 2),
            (PAYROLL_INSERT_SQL, "kyuyo_payroll", 4),
            (PAYROLL_SYNC_STATE_UPSERT_SQL, "kyuyo_sync_state", 4),
            (EMPLOYEES_ROWS_SQL, "kyuyo_employees", 2),
            (EMPLOYEES_DELETE_SQL, "kyuyo_employees", 2),
            (EMPLOYEES_INSERT_SQL, "kyuyo_employees", 4),
            (EMPLOYEES_SYNC_STATE_UPSERT_SQL, "kyuyo_sync_state", 5),
        ];
        for (sql, table, params) in cases {
            assert!(sql.contains(table), "{sql}");
            assert_eq!(sql.matches('?').count(), params, "{sql}");
            assert!(!sql.contains("  ") && !sql.contains('\n'), "{sql}");
        }
        assert!(PAYROLL_ROWS_SQL.ends_with("ORDER BY seq ASC"));
        assert!(EMPLOYEES_ROWS_SQL.ends_with("ORDER BY seq ASC"));
        // payroll の upsert は company_name を触らない。employees は上書きする
        assert!(!PAYROLL_SYNC_STATE_UPSERT_SQL.contains("company_name = excluded"));
        assert!(EMPLOYEES_SYNC_STATE_UPSERT_SQL.contains("company_name = excluded.company_name"));
    }

    #[test]
    fn ddl_names_three_tables() {
        for t in ["kyuyo_payroll", "kyuyo_employees", "kyuyo_sync_state"] {
            assert!(CREATE_TABLES_SQL.contains(&format!("CREATE TABLE IF NOT EXISTS {t} (")));
            assert!(DROP_TABLES_SQL.contains(&format!("DROP TABLE IF EXISTS {t};")));
        }
        assert!(!CREATE_TABLES_SQL.contains("PRAGMA"));
        assert!(PAYROLL_SYNCED_SQL.contains("'payroll:%'"));
        assert_eq!(SCHEMA_VERSION, 1);
    }
}
