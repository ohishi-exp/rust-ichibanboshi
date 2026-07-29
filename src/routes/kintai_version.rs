//! 勤怠の月別バージョン (ETag) エンドポイント (Refs #184)。
//!
//! `GET /api/kintai/version?month=YYYY-MM` — nuxt-dtako-admin relay
//! (dtako-scraper-relay) の条件付き再検証キャッシュが「上流のデータが変わったか」を
//! 数十バイトで確認するための読み出し口。応答は
//! `{"month":"YYYY-MM","etag":"…"}` で、同じ値を `ETag` ヘッダにも載せる。
//!
//! - **パラメータと認可は `/kintai/kosoku-daily` に揃える** — `month` 必須
//!   (`YYYY-MM`、不正は 400)、認可は CF Access Service Token (edge)。応答は
//!   不透明な版識別子だけで金額どころか個人情報も含まない
//! - `driver` は取らない — relay のキャッシュ鍵は月単位で、etag はその月の
//!   **全乗務員ぶんのソーステーブル**を覆う
//! - etag の材料 (ソーステーブルの列挙・マーカーの形・BUILD_SHA / `KosokuParams`
//!   を畳む理由) は [`crate::kintai_version`] のモジュール doc を参照
//! - 失敗は fail-closed: `[mariadb]` 未設定 503 / クエリ失敗 (GRANT 不足含む) 502。
//!   **絶対に「一部テーブル抜きの etag」へ縮退しない** — それは列挙漏れと同じ
//!   「古い値を返し続ける」事故になる

use std::sync::Arc;

use axum::extract::Query;
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::kintai_version::{fold_etag, DynKintaiVersionRepo};
use crate::kosoku::KosokuParams;
use crate::routes::kintai::{is_valid_month, map_repo_err};

/// `?month=YYYY-MM`。`month` は必須。
#[derive(Debug, Deserialize)]
pub struct VersionQuery {
    pub month: Option<String>,
}

/// GET /api/kintai/version?month=YYYY-MM — 月別バージョン (ETag)。
pub async fn version(
    Query(params): Query<VersionQuery>,
    Extension(repo): Extension<DynKintaiVersionRepo>,
    Extension(params_cfg): Extension<Arc<KosokuParams>>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let markers = repo.fetch_markers(&month).await.map_err(map_repo_err)?;
    // KINTAI_OUTPUT_SHA: 応答を形づくるコード (kosoku* / kintai* / routes/kintai*) の
    //   内容ハッシュ。**リポジトリ全体の BUILD_SHA ではない** — 全体を畳むと ETC・日報など
    //   kintai と無関係なデプロイでも relay の上流キャッシュが全月無効になる (Refs #191)。
    //   対象の決め方と「取りこぼしたら古い値」の警戒点は build.rs 参照
    // KosokuParams: 再ビルド無しの TOML 変更 (丸め方・閾値) でも応答が変わるため畳む
    let etag = fold_etag(
        &month,
        env!("KINTAI_OUTPUT_SHA"),
        &format!("{:?}", *params_cfg),
        &markers,
    );
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let sources = markers.len();
    tracing::info!(month = %month, sources, "kintai version built");
    Ok((
        [(header::ETAG, etag.clone())],
        Json(serde_json::json!({ "month": month, "etag": etag })),
    ))
}
