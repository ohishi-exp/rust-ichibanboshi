//! `POST /api/uriage/admin/delete`・`POST /api/uriage/admin/rebuild` の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//! src/routes/uriage.rs の `admin_delete`・`admin_rebuild` と src/sqlite.rs の `delete_bucket`・`rebuild_schema` の写し。
//!
//! - delete: `(month, eigyosho_id)` の日次・取引先別・recalc_jobs を 1 batch で DELETE (verify_jobs は消さない)。
//!   月次は日次の VIEW なので連動して消える。month が空なら本文なしの 400 (オンプレ版と同じ)
//! - rebuild: オンプレ版は DROP → 再 migrate。**Worker は DROP せず 4 表 (日次・取引先別・recalc_jobs・verify_jobs) を
//!   DELETE する** (VIEW は残す。D1 の schema は wrangler の migration が持つ。外部キーは無い)。1 batch

use serde::Serialize;

use crate::api::{AdminDeleteQuery, AdminDeleteResponse, AdminRebuildResponse};
use crate::d1::{Bind, Stmt};
use crate::routes::Reply;

/// 日次の bucket の削除 (全 cal。src/sqlite.rs:1262 の写し。PR-F で root 側を消す)。
pub const DELETE_DAILY_SQL: &str =
    "DELETE FROM uriage_person_daily WHERE month = ?1 AND eigyosho_id = ?2";

/// 取引先別の bucket の削除 (src/sqlite.rs:1272 の写し。PR-F で root 側を消す)。
pub const DELETE_PARTNER_SQL: &str =
    "DELETE FROM uriage_person_partner_daily WHERE month = ?1 AND eigyosho_id = ?2";

/// recalc_jobs の bucket の削除 (src/sqlite.rs:1278 の写し。PR-F で root 側を消す)。
pub const DELETE_JOBS_SQL: &str = "DELETE FROM recalc_jobs WHERE month = ?1 AND eigyosho_id = ?2";

/// rebuild の全削除 (src/sqlite.rs:1297 の `rebuild_schema` の DROP を DELETE にしたもの)。
pub const REBUILD_SQL: [&str; 4] = [
    "DELETE FROM verify_jobs",
    "DELETE FROM recalc_jobs",
    "DELETE FROM uriage_person_partner_daily",
    "DELETE FROM uriage_person_daily",
];

/// delete の 3 文のうち、応答の `daily_deleted` を取る文の位置 (batch の結果の添字)。
const DELETE_DAILY_AT: usize = 0;
/// 同 `jobs_deleted`。
const DELETE_JOBS_AT: usize = 2;

/// delete の 3 文 (日次・取引先別・recalc_jobs の順。1 batch)。month が空なら本文なしの 400。
pub fn delete_statements(q: &AdminDeleteQuery) -> Result<Vec<Stmt>, Reply> {
    if q.month.is_empty() {
        return Err(Reply::bare(400));
    }
    let stmt = |sql: &'static str| Stmt {
        sql,
        binds: vec![Bind::Text(q.month.clone()), Bind::Int(q.eigyosho_id)],
    };
    Ok(vec![
        stmt(DELETE_DAILY_SQL),
        stmt(DELETE_PARTNER_SQL),
        stmt(DELETE_JOBS_SQL),
    ])
}

/// batch の各文の変更行数 (文と同じ順) から、delete の応答を組む。数が合わなければ 500 (`rows:malformed`)。
pub fn delete_response(q: AdminDeleteQuery, changes: &[usize]) -> Reply {
    let (Some(daily), Some(jobs), 3) = (
        changes.get(DELETE_DAILY_AT),
        changes.get(DELETE_JOBS_AT),
        changes.len(),
    ) else {
        return Reply::failure(500, "rows", "malformed");
    };
    json(&AdminDeleteResponse {
        month: q.month,
        eigyosho_id: q.eigyosho_id,
        daily_deleted: *daily,
        jobs_deleted: *jobs,
    })
}

/// rebuild の 4 文 (1 batch)。
pub fn rebuild_statements() -> Vec<Stmt> {
    REBUILD_SQL
        .iter()
        .map(|&sql| Stmt { sql, binds: vec![] })
        .collect()
}

/// rebuild の応答 (`rebuilt_at` は [`crate::recalc::timestamp`] の形)。
pub fn rebuild_response(rebuilt_at: String) -> Reply {
    json(&AdminRebuildResponse { rebuilt_at })
}

fn json<T: Serialize>(body: &T) -> Reply {
    // 文字列と整数だけの型なので失敗しない
    Reply::ok(serde_json::to_string(body).unwrap_or_default())
}
