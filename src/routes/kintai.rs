//! 勤怠 (タイムカード) 中継エンドポイント (Refs #99、ohishi-exp/nuxt-dtako-admin#424)。
//!
//! 社内 LAN の CakePHP (`yhonda-ohishi/nginx`) が持つタイムカードの日別データを、
//! Cloudflare Worker (nuxt-dtako-admin の dtako-scraper-relay) へ中継する。
//! CakePHP は LAN 内にしか居ないため、同一ホストで動く本サービスが橋渡しする
//! (`[cakephp] base_url` は既定で `http://127.0.0.1:120` の loopback)。
//!
//! **中継だけを行い、解釈も変換もしない。** 行は `serde_json::Value` のまま素通しする
//! ので、上流が項目を足しても本サービスの型を触る必要がない。ID の変換・突合も
//! 行わない — CakePHP の `drivers.id` は乗務員CD (= 一番星 `社員ﾏｽﾀ.社員C`) と
//! 同一番号体系なので、受け手がそのまま引き当てられる。
//!
//! ## 認可 — CF Access Service Token (edge)
//!
//! `/employees` (identity-only) と同じ扱いにしている。**前例のコピーではなく、
//! データの ACL で選んだ**:
//!
//! - 応答に含まれるのは識別情報 (社員番号・氏名・所属) と時刻だけで、**金額を含まない**
//! - 消費者は Cloudflare Worker の Durable Object であり、**ブラウザ JWT を持てない**。
//!   `/kyuyo/*` の in-service gate (auth-worker introspect + email allowlist) を要求すると
//!   worker から呼べなくなる
//!
//! 将来この endpoint に金額を足すことになったら、その時点で `/kyuyo/*` と同じ
//! in-service gate へ移すこと。

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::cakephp::{CakephpClient, CakephpError, TimecardDailyResponse};
use crate::kintai_store::DynKintaiStore;

/// `?month=YYYY-MM&refresh=1`。`refresh=1` はキャッシュを飛ばして CakePHP から
/// 引き直す (Refs #106 Phase 2 — 当月の打刻は日々変わるため、relay の取り込みは
/// これを付ける)。
#[derive(Debug, Deserialize)]
pub struct DailyQuery {
    pub month: Option<String>,
    #[serde(default)]
    pub refresh: Option<String>,
}

/// 対象月の書式検証。`YYYY-MM` で月は 01-12。
///
/// 上流は月単位 API (`HolidaysTrait` が `first_day_of_month` を受けて「日」の配列を
/// 返す) なので、任意の日付レンジは受け付けない。
pub fn is_valid_month(month: &str) -> bool {
    let bytes = month.as_bytes();
    if bytes.len() != 7 || bytes[4] != b'-' {
        return false;
    }
    if !bytes[..4].iter().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if !bytes[5..].iter().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let mm: u32 = month[5..].parse().unwrap_or(0);
    (1..=12).contains(&mm)
}

/// CakePHP のエラーを HTTP ステータスへ写す (uriage の `map_cakephp_err` と同方針)。
fn map_cakephp_err(e: CakephpError) -> (StatusCode, String) {
    match e {
        CakephpError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url が未設定".to_string(),
        ),
        CakephpError::RequestFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP fetch failed: {m}"),
        ),
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP returned {status}: {body_excerpt}"),
        ),
        CakephpError::JsonError(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP response parse failed: {m}"),
        ),
    }
}

/// 応答へ出どころメタを足す (素通し方針のため型は変えず extra に載せる)。
fn with_source_meta(
    mut resp: TimecardDailyResponse,
    source: &str,
    synced_at: &str,
) -> TimecardDailyResponse {
    resp.extra
        .insert("source".to_string(), serde_json::Value::from(source));
    resp.extra
        .insert("synced_at".to_string(), serde_json::Value::from(synced_at));
    resp
}

/// GET /api/kintai/daily?month=YYYY-MM — タイムカード日別データの中継。
///
/// Refs #106 Phase 2: read-through — derived store に月があれば CakePHP に触らず
/// 返す (`source:"cache"`)。miss / `refresh=1` は従来どおり CakePHP から取得し
/// write-through で保存する (`source:"live"`)。保存するのは**上流応答の verbatim
/// JSON** (メタ注入前) — 素通し方針を保存でも維持する。
pub async fn daily(
    Query(params): Query<DailyQuery>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Extension(store): Extension<DynKintaiStore>,
) -> Result<Json<TimecardDailyResponse>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let force_refresh = params.refresh.as_deref() == Some("1");
    if !force_refresh {
        match store.get_daily(&month).await {
            Ok(Some(cached)) => {
                match serde_json::from_str::<TimecardDailyResponse>(&cached.response_json) {
                    Ok(resp) => {
                        let rows = resp.rows.len();
                        tracing::info!(month = %month, rows, "kintai daily served from cache");
                        return Ok(Json(with_source_meta(resp, "cache", &cached.synced_at)));
                    }
                    Err(e) => {
                        // schema 版の上げ忘れ等 — live へフォールバック (読みを殺さない)
                        tracing::warn!("kintai store corrupt row — live fallback: {e}");
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("kintai store read failed — live fallback: {e}");
            }
        }
    }
    let resp = cakephp
        .fetch_timecard_daily(&month)
        .await
        .map_err(map_cakephp_err)?;
    // 件数は先に出しておく — `tracing::info!` の引数は購読者が居ないと評価されず、
    // マクロ内に到達しない region が残る (coverage_100 の対象なので実害がある)
    let rows = resp.rows.len();
    tracing::info!(month = %month, rows, "kintai daily relayed");
    let synced_at = chrono::Utc::now().to_rfc3339();
    // String キー + JSON 値しか持たない型なので serialize は失敗しない
    let json = serde_json::to_string(&resp).expect("TimecardDailyResponse serialize");
    if let Err(e) = store.put_daily(&month, &json, rows, &synced_at).await {
        // live 応答はそのまま返す — キャッシュ書き込み失敗で中継を殺さない
        tracing::warn!("kintai store write failed: {e}");
    }
    Ok(Json(with_source_meta(resp, "live", &synced_at)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_months() {
        assert!(is_valid_month("2026-01"));
        assert!(is_valid_month("2026-12"));
    }

    #[test]
    fn invalid_months() {
        assert!(!is_valid_month(""));
        assert!(!is_valid_month("2026-1"));
        assert!(!is_valid_month("2026-00"));
        assert!(!is_valid_month("2026-13"));
        assert!(!is_valid_month("2026/06"));
        assert!(!is_valid_month("20a6-06"));
        assert!(!is_valid_month("2026-0a"));
        assert!(!is_valid_month("2026-006"));
    }

    #[test]
    fn error_mapping() {
        let (s, m) = map_cakephp_err(CakephpError::NotConfigured);
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert!(m.contains("base_url"));

        let (s, m) = map_cakephp_err(CakephpError::RequestFailed("dns".into()));
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("dns"));

        let (s, m) = map_cakephp_err(CakephpError::StatusError {
            status: 500,
            body_excerpt: "boom".into(),
        });
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("500") && m.contains("boom"));

        let (s, m) = map_cakephp_err(CakephpError::JsonError("eof".into()));
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("eof"));
    }
}
