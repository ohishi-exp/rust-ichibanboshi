use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Extension;

use crate::repo::{DynRepo, RepoError};

/// この instance が「使う」と**宣言した**バックエンドの一覧。
///
/// rust-ichibanboshi はオンプレ (ohishi-data / systemd) と GCP (Cloud Run) の
/// **両方を一級の実行形態として恒久的に持つ**。同じバイナリでも、どのバックエンドを
/// 使うかは設定で決まる。`/health` がそれを外に出すことで、
/// 「起動はしているが実は繋がっていない」という静かな degraded を作らない。
///
/// - `sqlserver`: `[database] enabled`。**宣言したら起動時に接続を検証済み**
///   (繋がらなければそもそも起動していない、`db::create_pool` 参照) なので、
///   `/health` では毎回 `health_check()` で継続的な生死だけを見る。
/// - `mariadb` / `kyuyo`: 設定が揃っているか (`MariadbConfig::enabled()` /
///   `KyuyoConfig::db_enabled()`)。こちらは pool が lazy で起動時検証をしないため、
///   `"declared"` は「使うと宣言した」までしか意味しない。
/// - `kintai_events`: 生イベントを**どこから読んでいるか** (`"mariadb"` / `"http"` /
///   `"disabled"`、Refs #205 実装計画 02)。同じバイナリで読み先が変わるので、
///   宣言だけでなく**採用された経路**を出す — 読み先を間違えたときの壊れ方は
///   「遅い」ではなく「静かに違う数字を返す」ため、外から判別できる必要がある。
#[derive(Debug, Clone, Copy)]
pub struct HealthState {
    pub sqlserver: bool,
    pub mariadb: bool,
    pub kyuyo: bool,
    /// 生イベントの読み先 (`crate::server` が実際に挿した実装の名前)。
    pub kintai_events: &'static str,
}

fn declared(v: bool) -> &'static str {
    if v {
        "declared"
    } else {
        "disabled"
    }
}

/// GET /health — 宣言したバックエンドの状態 + build 情報 (commit / built_at) を返す
///
/// SQL Server を宣言している instance では従来どおり毎回 `SELECT 1` を打ち、
/// 落ちていれば 503。宣言していない instance は SQL Server を触らずに 200 を返し、
/// body の `backends.sqlserver` が `"disabled"` になる。
pub async fn health(
    Extension(repo): Extension<DynRepo>,
    Extension(state): Extension<HealthState>,
) -> Result<impl IntoResponse, StatusCode> {
    let sqlserver = if state.sqlserver {
        repo.health_check().await.map_err(|e| {
            match &e {
                RepoError::PoolError => tracing::error!("DB pool error"),
                RepoError::QueryError(msg) => tracing::error!("DB query error: {msg}"),
            }
            StatusCode::SERVICE_UNAVAILABLE
        })?;
        "ok"
    } else {
        "disabled"
    };

    // build.rs が焼き込む build 識別情報 (commit SHA + build 時刻)。
    // どの build がデプロイされているか /health で判別するため (Refs #14)。
    let body = format!(
        concat!(
            "{{\"status\":\"ok\",\"commit\":\"",
            env!("BUILD_SHA"),
            "\",\"built_at\":\"",
            env!("BUILD_TIME"),
            "\",\"backends\":{{\"sqlserver\":\"{}\",\"mariadb\":\"{}\",\"kyuyo\":\"{}\",",
            "\"kintai_events\":\"{}\"}}}}"
        ),
        sqlserver,
        declared(state.mariadb),
        declared(state.kyuyo),
        state.kintai_events
    );
    Ok(([(header::CONTENT_TYPE, "application/json")], body))
}
