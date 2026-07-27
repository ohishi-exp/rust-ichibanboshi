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
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiRepoError};
use crate::kintai_store::DynKintaiStore;
use crate::kosoku::{daily_summary, KosokuParams};

/// `?month=YYYY-MM&refresh=1`。`refresh=1` はキャッシュを飛ばして CakePHP から
/// 引き直す (Refs #106 Phase 2 — 当月の打刻は日々変わるため、relay の取り込みは
/// これを付ける)。
#[derive(Debug, Deserialize)]
pub struct DailyQuery {
    pub month: Option<String>,
    #[serde(default)]
    pub refresh: Option<String>,
}

/// `?month=YYYY-MM&driver=1051` (Refs #114)。**両方必須**。
#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
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

/// 乗務員CD のパース。**数字のみ**を受ける (空・非数字・負値・桁溢れは None)。
///
/// 乗務員CD = 一番星 `社員ﾏｽﾀ.社員C` と同一番号体系で、DB 側も整数列なので
/// ここで整数にしてから渡す — 文字列のままクエリに載せない。
pub fn parse_driver(driver: &str) -> Option<u64> {
    if driver.is_empty() || !driver.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    driver.parse::<u64>().ok()
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

/// 生イベント読み取りのエラーを HTTP ステータスへ写す。
///
/// 未設定は 503 (`base_url` 未設定と同じ fail-closed)、DB 停止・クエリ失敗は 502。
fn map_repo_err(e: KintaiRepoError) -> (StatusCode, String) {
    match e {
        KintaiRepoError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "MariaDB 接続設定が未設定".to_string(),
        ),
        KintaiRepoError::QueryFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("MariaDB query failed: {m}"),
        ),
    }
}

/// GET /api/kintai/events?month=YYYY-MM&driver=1051 — 打刻と運行イベントの
/// **生の時系列** (Refs #114 / #116、拘束時間の打刻基準化 Phase 1)。
///
/// 拘束時間管理表の残業を打刻基準で計算し直すにあたり、規則を決める前に実データで
/// 各パターン (同日 2 運行・打刻と運行のズレ・細切れ休憩 …) が何件あるかを数える
/// ための読み出し口。**解釈しない** — 勤務の切れ目も休憩の閾値もここでは判断せず、
/// 生行を時刻順に並べて返すだけ。
///
/// データ源は社内 MariaDB の直読み (`kintai_repo`)。`daily` (CakePHP 中継 +
/// derived store) と違い**キャッシュを持たない** — 調査用途で頻度が低く、常に
/// 最新の打刻が要るため。
pub async fn events(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let driver = match parse_driver(params.driver.as_deref().unwrap_or_default()) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "driver は乗務員CD (数字) で指定してください".to_string(),
            ))
        }
    };
    let rows = repo
        .fetch_events(&month, driver)
        .await
        .map_err(map_repo_err)?;
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let count = rows.len();
    tracing::info!(month = %month, driver, rows = count, "kintai events read");
    Ok(Json(serde_json::json!({ "rows": rows })))
}

/// GET /api/kintai/kosoku-daily?month=YYYY-MM&driver=1051 — **打刻基準の日別サマリ**
/// (Refs #118、拘束時間の打刻基準化 Phase 2)。
///
/// `/events` の生イベントを [`crate::kosoku`] の純粋ロジックで日別に畳んで返す。
/// **応答に金額は含めない** — 認可が `/events` と同じ CF Access Service Token
/// (edge) のままでよいのはそのため。金額を足すことになったら `/kyuyo/*` と同じ
/// in-service gate へ移すこと。
///
/// 勤務は**始業日**で当月に振り分ける。月初の勤務は前月末に始まった休息の終わりを
/// 始業とするが、区間イベントは重なりで拾うので `/events` と同じ範囲で足りる。
pub async fn kosoku_daily(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params_cfg): Extension<Arc<KosokuParams>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let driver = match parse_driver(params.driver.as_deref().unwrap_or_default()) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "driver は乗務員CD (数字) で指定してください".to_string(),
            ))
        }
    };
    let rows = repo
        .fetch_events(&month, driver)
        .await
        .map_err(map_repo_err)?;
    let days = daily_summary(&rows, &month, &params_cfg);
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let count = days.len();
    tracing::info!(month = %month, driver, days = count, "kintai kosoku-daily built");
    Ok(Json(serde_json::json!({
        "month": month,
        "driver": driver,
        "days": days,
    })))
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
    fn valid_drivers() {
        assert_eq!(parse_driver("1051"), Some(1051));
        assert_eq!(parse_driver("0"), Some(0));
        assert_eq!(parse_driver("0012"), Some(12));
    }

    #[test]
    fn invalid_drivers() {
        assert_eq!(parse_driver(""), None);
        assert_eq!(parse_driver("10a1"), None);
        assert_eq!(parse_driver("１０５１"), None); // 全角
        assert_eq!(parse_driver("1051 "), None);
        assert_eq!(parse_driver("-1"), None);
        // u64 桁溢れ (書式は数字でもパースできない)
        assert_eq!(parse_driver("99999999999999999999999"), None);
    }

    #[test]
    fn repo_error_mapping() {
        let (s, m) = map_repo_err(KintaiRepoError::NotConfigured);
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert!(m.contains("未設定"));

        let (s, m) = map_repo_err(KintaiRepoError::QueryFailed("boom".into()));
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("boom"));
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
