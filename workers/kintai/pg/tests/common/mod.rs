//! 実 DB の parity テストの共通部分 (`KINTAI_TEST_DATABASE_URL` と migration の適用)。各 test binary が `mod common;` で持つ。

// ── 前提 (root の tests/*_pg_test.rs と同じ形。migration は repo ルートの migrations/) ──────────────

pub fn database_url() -> String {
    std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .expect("KINTAI_TEST_DATABASE_URL が要ります (skip して緑にしない)")
}

fn needs_psql_variables(sql: &str) -> bool {
    let ident = |s: &str| {
        s.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    };
    [":'", ":\""].iter().any(|open| {
        sql.match_indices(open)
            .map(|(i, _)| &sql[i + 2..])
            .any(|rest| rest.find(&open[1..]).is_some_and(|n| ident(&rest[..n])))
    })
}

pub async fn ensure_schema(pool: &sqlx::PgPool) {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(1_121_016_001_i64)
        .execute(pool)
        .await
        .expect("advisory lock");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'kintai')")
            .fetch_one(pool)
            .await
            .expect("schema probe");
    if !exists {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../../migrations");
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir(dir)
            .expect("migrations dir")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
            .collect();
        files.sort();
        for entry in files {
            let sql = std::fs::read_to_string(&entry).expect("read migration");
            if needs_psql_variables(&sql) {
                continue;
            }
            sqlx::raw_sql(&sql)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", entry.display()));
        }
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(1_121_016_001_i64)
        .execute(pool)
        .await
        .expect("advisory unlock");
}
