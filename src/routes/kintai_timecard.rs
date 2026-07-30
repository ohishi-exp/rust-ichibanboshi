//! 打刻の受け口 (Refs #205 の 04b)。**GCP 側の instance が使う**。
//!
//! GCP には社内 MariaDB が無いので打刻が読めず、`shifts_from_timecard` が空になる。
//! 02 のモジュール docs が「#205 の 04 / 05 が埋める穴」と書いていたものを、
//! 穴の定義どおりに埋める — **オンプレが読んで、ここへ渡す**。
//!
//! | | |
//! |---|---|
//! | `GET /api/kintai/timecard/signatures` | 相手が持っている (乗務員, 暦日) の署名 |
//! | `POST /api/kintai/timecard` | 差分の日だけを反映する |
//!
//! ## 全量を送らない
//!
//! 送り側はまず署名を引き、手元の署名と突き合わせて**違う日と消えた日だけ**を送る。
//! 1 か月・全乗務員の打刻は数万行あるので、毎回全部送ると転送も書き込みも無駄になる。
//! 署名の作り方は [`crate::kintai_push`] のモジュール docs。
//!
//! ## 認可は網層が持つ
//!
//! オンプレは Cloudflare Access Service Token (Tunnel の手前)、GCP は Cloud Run IAM。
//! アプリ層で認証を持たないのはこのリポジトリ全体の方針で、`/api/kintai/*` の
//! 既存ルートと同じ。**テナントだけはアプリが決める** — 書き込み先の `tenant_id` は
//! `[kintai_push]` の設定から来るので、呼び出し側は他テナントへ書けない。
//!
//! ## 書き先が無ければ 503
//!
//! `[kintai_push] enabled = false` の instance (= オンプレの既定) では
//! `KintaiPgStore` が挿さらないので、両方 503 で fail-closed。「受け取ったが
//! どこにも書いていない」を作らない。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::kintai_push::{
    apply_timecard_batch, jst_day_bounds, KintaiPgStore, KintaiPushError, TimecardBatch,
};
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::kintai_send::{send_month, DynTimecardTarget, KintaiSendError, DEFAULT_MAX_DRIVERS};
use crate::routes::kintai::is_valid_month;

/// `[kintai_push]` が無効な instance では挿さらない。
pub type DynKintaiPgStore = Option<std::sync::Arc<KintaiPgStore>>;

/// `?month=YYYY-MM&driver_cd=1130`。どちらも必須。
#[derive(Debug, Deserialize)]
pub struct SignaturesQuery {
    pub month: Option<String>,
    pub driver_cd: Option<i64>,
}

fn bad_request(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

/// 書き先が無い / DB が落ちている、を分けて返す。
fn map_push_err(e: KintaiPushError) -> (StatusCode, String) {
    match e {
        // 宣言していない instance に投げられた = 呼び出し側の経路違い
        KintaiPushError::NotConfigured(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
        other => (StatusCode::BAD_GATEWAY, other.to_string()),
    }
}

fn store(store: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    store.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 月の JST 境界を `TIMESTAMPTZ` の対で返す。
fn month_bounds(
    month: &str,
) -> Option<(
    chrono::DateTime<chrono::FixedOffset>,
    chrono::DateTime<chrono::FixedOffset>,
)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = chrono::NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((jst_day_bounds(first).0, jst_day_bounds(next).0))
}

/// `POST /api/kintai/timecard/send` の本体。
#[derive(Debug, Deserialize)]
pub struct SendRequest {
    pub month: String,
    /// 続きから回す位置。前回の応答の `next_after_driver_cd` を渡す。
    #[serde(default)]
    pub after_driver_cd: Option<u64>,
    /// 1 回で回す乗務員数。Tunnel の 30 秒に収める。
    #[serde(default)]
    pub max_drivers: Option<usize>,
    /// **`false` なら 1 件も送らない** (既定)。計画だけ立てて件数を返す。
    #[serde(default)]
    pub apply: bool,
}

fn map_send_err(e: KintaiSendError) -> (StatusCode, String) {
    match e {
        KintaiSendError::NotConfigured(m) => (StatusCode::BAD_REQUEST, m),
        other => (StatusCode::BAD_GATEWAY, other.to_string()),
    }
}

/// POST /api/kintai/timecard/send — 手元の打刻を相手へ送る (**送信側**)。
///
/// **乗務員数で区切って同期で返す。** この口は Cloudflare Tunnel (30 秒上限) を
/// 通って起動されるので、全乗務員を 1 回で回すと必ず 502 になる。応答の
/// `next_after_driver_cd` が `null` になるまで呼び出し側が呼び直す。
pub async fn send(
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(target): Extension<Option<DynTimecardTarget>>,
    Json(req): Json<SendRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&req.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let target = target.ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_send] が無効です (送り先がありません)".to_string(),
    ))?;
    let report = send_month(
        &repo,
        &target,
        &req.month,
        req.after_driver_cd,
        req.max_drivers.unwrap_or(DEFAULT_MAX_DRIVERS),
        req.apply,
    )
    .await
    .map_err(map_send_err)?;
    // マクロは 1 行に収める (CLAUDE.md)
    let (n, s) = (report.drivers, report.days_sent);
    tracing::info!(n, s, "timecard sent");
    Ok(Json(serde_json::json!(report)))
}

/// GET /api/kintai/timecard/signatures?month=&driver_cd= — 相手が持っている日別署名。
pub async fn signatures(
    Query(params): Query<SignaturesQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let driver_cd = params
        .driver_cd
        .ok_or_else(|| bad_request("driver_cd は必須です"))?;
    let (from, to) = month_bounds(&month).ok_or_else(|| bad_request("month が不正です"))?;
    let st = store(&pg)?;
    let sigs = st
        .stored_day_signatures(driver_cd, from, to)
        .await
        .map_err(map_push_err)?;
    Ok(Json(serde_json::json!({
        "month": month,
        "driver_cd": driver_cd,
        "signatures": sigs,
    })))
}

/// POST /api/kintai/timecard — 差分の日だけを反映する。
pub async fn receive(
    Extension(pg): Extension<DynKintaiPgStore>,
    Json(batch): Json<TimecardBatch>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&batch.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let st = store(&pg)?;
    let result = apply_timecard_batch(st, &batch)
        .await
        .map_err(map_push_err)?;
    // 件数は先に出す (tracing の引数は購読者が居ないと評価されない)。
    // **マクロは 1 行に収める** — 折り返すと llvm-cov の行カバレッジに乗らず
    // 100% gate が落ちる (CLAUDE.md)
    let (w, d, m) = (result.days_written, result.days_deleted, result.misplaced);
    tracing::info!(w, d, "timecard applied");
    if result.has_unexpected() {
        tracing::warn!(m, "timecard had odd rows");
    }
    Ok(Json(serde_json::json!(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(month: Option<&str>, driver: Option<i64>) -> Query<SignaturesQuery> {
        Query(SignaturesQuery {
            month: month.map(str::to_string),
            driver_cd: driver,
        })
    }

    fn batch(month: &str) -> TimecardBatch {
        TimecardBatch {
            month: month.to_string(),
            driver_cd: 1130,
            days: Default::default(),
            delete_dates: Vec::new(),
        }
    }

    /// **書き先が無い instance では 503。** オンプレの既定がこれ。
    /// 200 で空を返すと「受け取ったがどこにも書いていない」が作れてしまう。
    #[tokio::test]
    async fn both_routes_fail_closed_without_a_store() {
        let (code, msg) = signatures(q(Some("2026-07"), Some(1130)), Extension(None))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");

        let (code, _) = receive(Extension(None), Json(batch("2026-07")))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// 壊れたパラメータは**書き先を見に行く前に** 400 で落とす。
    #[tokio::test]
    async fn parameters_are_validated_before_the_store() {
        for m in [None, Some(""), Some("2026-7"), Some("nope")] {
            let (code, _) = signatures(q(m, Some(1130)), Extension(None))
                .await
                .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST, "month={m:?}");
        }
        // driver_cd 省略
        let (code, msg) = signatures(q(Some("2026-07"), None), Extension(None))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
        assert!(msg.contains("driver_cd"), "{msg}");

        let (code, _) = receive(Extension(None), Json(batch("nope")))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn month_bounds_is_jst_and_wraps_the_year() {
        let (a, b) = month_bounds("2026-12").unwrap();
        assert_eq!(a.to_rfc3339(), "2026-12-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2027-01-01T00:00:00+09:00");
        let (a, b) = month_bounds("2026-07").unwrap();
        assert_eq!(a.to_rfc3339(), "2026-07-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2026-08-01T00:00:00+09:00");
        assert!(month_bounds("2026-13").is_none());
        assert!(month_bounds("nope").is_none());
        assert!(month_bounds("20xx-07").is_none());
    }

    /// **送り先が無い instance では 503。**
    #[tokio::test]
    async fn send_fails_closed_without_a_target() {
        let repo: DynKintaiEventsRepo =
            std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo);
        let body = |m: &str| {
            Json(SendRequest {
                month: m.to_string(),
                after_driver_cd: None,
                max_drivers: None,
                apply: false,
            })
        };
        let (code, msg) = send(Extension(repo.clone()), Extension(None), body("2026-07"))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_send"), "{msg}");

        // month は送り先を見に行く前に検査する
        let (code, _) = send(Extension(repo), Extension(None), body("nope"))
            .await
            .unwrap_err();
        assert_eq!(code, StatusCode::BAD_REQUEST);
    }

    #[test]
    fn send_errors_separate_the_caller_from_the_remote() {
        let (code, _) = map_send_err(KintaiSendError::NotConfigured("bad month".to_string()));
        assert_eq!(code, StatusCode::BAD_REQUEST);
        let (code, _) = map_send_err(KintaiSendError::Remote("status 503".to_string()));
        assert_eq!(code, StatusCode::BAD_GATEWAY);
    }

    /// 宣言していない (503) と、繋がらない (502) を分ける。
    #[test]
    fn errors_separate_declaration_from_failure() {
        let (code, msg) = map_push_err(KintaiPushError::NotConfigured("nope".to_string()));
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(msg, "nope");

        let (code, _) = map_push_err(KintaiPushError::Db(sqlx::Error::RowNotFound));
        assert_eq!(code, StatusCode::BAD_GATEWAY);
    }
}
