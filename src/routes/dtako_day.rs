//! 乗務員CD + 日付 → その日の運行NO・全イベント・修正用リンク (Refs #205 の 57)。
//!
//! 値ずれ (オンプレ vs GCP) を見つけたあと直す手順は「theearth から csvdata.zip を
//! 落とす → 社内 nginx (CakePHP) のフォームに入れる」で、そのために要る運行NO と
//! リンクをいま毎回手で組み立てている。ここは**その組み立てだけ**を 1 回の `curl` で
//! 出す — アップロードは自動化しない (CSRF cookie が HttpOnly、実機確認済み)。
//!
//! ## ファイル名がなぜ `dtako_day.rs` か (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/routes/` 配下を**ファイル名の接頭辞**
//! (`kintai`) で拾い、拾われたファイルの内容ハッシュが `logic_version` (`/api/kintai/
//! version` の etag) に畳まれる。この endpoint は既存の生イベント読み出しを**そのまま
//! 再利用するだけ**で `/api/kintai/{daily,kosoku-daily,version}` の応答を一切変えない
//! ので、`kintai` で始まる名前を付けると無関係な deploy まで全乗務員 stale にしてしまう。
//!
//! 同じ理由で [`crate::kintai_http_repo`] の `onprem_unko_no` (unko_no の桁変換) も
//! **import せず、同じロジックを共有 crate `kintai-dtako` に独立して持つ** — あちらは
//! グロブ対象なので、ここから依存すると変更のたびに向こうを触ったのと同じ扱いになる。
//!
//! ## 純粋部分は共有 crate `kintai-dtako` (`workers/kintai/dtako`、Refs #322)
//!
//! 日の窓・運行への畳み方・リンクと `zip_request` の組み立て (と、その不変条件の説明) は
//! `kintai_dtako::day` にあり、勤怠 Worker も同じものを使う。ここに残るのは handler
//! (入力の検査・repo 呼び出し・[`DtakoDayLinksConfig`] から base URL を渡すこと) だけ。
//! `kintai-dtako` も `build.rs` の glob の外。
//!
use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use kintai_dtako::day::{body, build_operations, day_range, parse_date, DATE_INVALID};
use serde::Deserialize;

use crate::config::DtakoDayLinksConfig;
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::{map_repo_err, parse_driver};

/// `?driver=1021&date=2026-06-05`。両方必須。
#[derive(Debug, Deserialize)]
pub struct DayEventsQuery {
    pub driver: Option<String>,
    pub date: Option<String>,
}

/// GET /api/kintai/day-events?driver=&date= — 乗務員CD + 日付の運行NO・全イベント・
/// 修正用リンク (Refs #205 の 57)。
///
/// **データ源は `/api/kintai/events` と同じ repo 関数** ([`DynKintaiEventsRepo::
/// fetch_events_between`]) — 日で絞るのはこの呼び出し側で、SQL は増やさない。
pub async fn day_events(
    Query(params): Query<DayEventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(links_cfg): Extension<Arc<DtakoDayLinksConfig>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let driver = match parse_driver(params.driver.as_deref().unwrap_or_default()) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "driver は乗務員CD (数字) で指定してください".to_string(),
            ))
        }
    };
    let date = match params.date.as_deref().and_then(parse_date) {
        Some(d) => d,
        None => return Err((StatusCode::BAD_REQUEST, DATE_INVALID.to_string())),
    };
    let (from, to) = day_range(date);
    let rows = repo
        .fetch_events_between(&from, &to, driver)
        .await
        .map_err(map_repo_err)?;
    let operations = build_operations(&rows, &links_cfg.ryohi_base_url, &links_cfg.dtako_base_url);
    let (ops, evs) = (operations.len(), rows.len());
    tracing::info!(driver, %date, ops, evs, "dtako day-events built");
    Ok(Json(body(driver, date, operations, rows)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::routing::get;
    use axum::Router;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    fn cfg(ryohi: &str, dtako: &str) -> Arc<DtakoDayLinksConfig> {
        Arc::new(DtakoDayLinksConfig {
            ryohi_base_url: ryohi.to_string(),
            dtako_base_url: dtako.to_string(),
        })
    }

    fn dtako_table_row(datetime: &str, driver: i64, unko_no: &str) -> Value {
        json!({
            "datetime": datetime, "end_datetime": null, "driver_id": driver,
            "source": "dtako", "state": "運行開始", "unko_no": unko_no, "vehicle": null
        })
    }

    fn dtako_events_row(
        datetime: &str,
        end: &str,
        driver: i64,
        unko_no: &str,
        vehicle: &str,
    ) -> Value {
        json!({
            "datetime": datetime, "end_datetime": end, "driver_id": driver,
            "source": "dtako_events", "state": "休息", "unko_no": unko_no, "vehicle": vehicle
        })
    }

    /// 呼び出し引数を記録し、仕込んだ結果を返す mock (`tests/kintai_events_test.rs`
    /// の `MockEventsRepo` と同じ形)。
    struct MockRepo {
        rows: Vec<Value>,
    }

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for MockRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            Ok(self.rows.clone())
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("day-events は全乗務員を読まない")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("day-events はフェリーを読まない")
        }
    }

    struct FailingRepo;

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for FailingRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            Err(crate::kintai_repo::KintaiRepoError::QueryFailed(
                "boom".to_string(),
            ))
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }
    }

    fn app(repo: DynKintaiEventsRepo, links_cfg: Arc<DtakoDayLinksConfig>) -> Router {
        Router::new()
            .route("/kintai/day-events", get(day_events))
            .layer(Extension(repo))
            .layer(Extension(links_cfg))
    }

    async fn get_json(router: Router, uri: &str) -> (StatusCode, Value) {
        let res = router
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        // エラー応答は `(StatusCode, String)` の素の文字列 body (JSON ではない)。
        // 成功応答だけ JSON として読み、それ以外はテキストのまま Value::String に包む
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
        };
        (status, body)
    }

    #[tokio::test]
    async fn day_events_returns_operations_with_links_and_zip_request() {
        let rows = vec![
            dtako_table_row("2026-06-05 07:53:30", 1021, "26060507533000000042861"),
            dtako_events_row(
                "2026-06-05 08:00:00",
                "2026-06-05 08:10:00",
                1021,
                "26060507533000000042861",
                "長崎100か4286",
            ),
        ];
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows });
        let router = app(repo, cfg("https://ryohi.example", "https://dtako.example"));
        let (status, body) =
            get_json(router, "/kintai/day-events?driver=1021&date=2026-06-05").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["driver_cd"], json!(1021));
        assert_eq!(body["date"], json!("2026-06-05"));
        assert_eq!(body["operations"].as_array().unwrap().len(), 1);
        assert_eq!(body["events"].as_array().unwrap().len(), 2);
        let op = &body["operations"][0];
        assert!(op["links"]["ryohi"].is_string());
        assert!(op["links"]["search"].is_string());
        assert!(op["links"].get("zip").is_none(), "links に zip を出さない");
        assert!(op["zip_request"]["ope_no"].is_string());
    }

    #[tokio::test]
    async fn day_events_returns_empty_arrays_when_no_operation_that_day() {
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router = app(repo, cfg("", ""));
        let (status, body) =
            get_json(router, "/kintai/day-events?driver=1021&date=2026-06-05").await;
        assert_eq!(status, StatusCode::OK, "運行が無い日も200 (404にしない)");
        assert_eq!(body["operations"], json!([]));
        assert_eq!(body["events"], json!([]));
    }

    #[tokio::test]
    async fn day_events_rejects_missing_or_non_numeric_driver() {
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router = app(repo, cfg("", ""));
        let (status, _) = get_json(router, "/kintai/day-events?date=2026-06-05").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let repo2: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router2 = app(repo2, cfg("", ""));
        let (status2, _) = get_json(router2, "/kintai/day-events?driver=abc&date=2026-06-05").await;
        assert_eq!(status2, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn day_events_rejects_missing_or_malformed_date() {
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router = app(repo, cfg("", ""));
        let (status, _) = get_json(router, "/kintai/day-events?driver=1021").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let repo2: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router2 = app(repo2, cfg("", ""));
        let (status2, _) =
            get_json(router2, "/kintai/day-events?driver=1021&date=2026-13-40").await;
        assert_eq!(status2, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn day_events_maps_repo_failure_to_bad_gateway() {
        let repo: DynKintaiEventsRepo = Arc::new(FailingRepo);
        let router = app(repo, cfg("", ""));
        let (status, _) = get_json(router, "/kintai/day-events?driver=1021&date=2026-06-05").await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }
}
