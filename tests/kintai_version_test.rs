//! /api/kintai/version のテスト (Refs #184、relay の条件付き再検証キャッシュ用)。
//!
//! `KintaiVersionApi` の mock を挿して **DB 無しで** route の振る舞いを固定する。
//! etag の畳み込み規則そのもの (順序不感・材料の効き方) は
//! `src/kintai_version.rs` の unit test 側で網羅しているので、ここで見るのは
//! 「route が検証し、ヘッダと body に同じ etag を載せ、失敗を正しく写すか」。

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::kintai_repo::KintaiRepoError;
use rust_ichibanboshi::kintai_version::{
    DisabledKintaiVersionRepo, DynKintaiVersionRepo, KintaiVersionApi, SourceMarker,
};
use rust_ichibanboshi::kosoku::KosokuParams;
use rust_ichibanboshi::routes;
use serde_json::Value;
use tower::ServiceExt;

struct MockRepo {
    markers: Vec<SourceMarker>,
    fail: Option<KintaiRepoError>,
}

impl MockRepo {
    fn with_markers(markers: Vec<SourceMarker>) -> Arc<Self> {
        Arc::new(Self {
            markers,
            fail: None,
        })
    }

    fn failing(e: KintaiRepoError) -> Arc<Self> {
        Arc::new(Self {
            markers: Vec::new(),
            fail: Some(e),
        })
    }
}

#[async_trait]
impl KintaiVersionApi for MockRepo {
    async fn fetch_markers(&self, _month: &str) -> Result<Vec<SourceMarker>, KintaiRepoError> {
        match &self.fail {
            Some(KintaiRepoError::NotConfigured) => Err(KintaiRepoError::NotConfigured),
            Some(KintaiRepoError::QueryFailed(m)) => Err(KintaiRepoError::QueryFailed(m.clone())),
            None => Ok(self.markers.clone()),
        }
    }
}

fn app(repo: DynKintaiVersionRepo) -> Router {
    Router::new()
        .route("/api/kintai/version", get(routes::kintai_version::version))
        .layer(Extension(repo))
        .layer(Extension(Arc::new(KosokuParams::default())))
}

fn marker(source: &str, count: &str, fp: &str) -> SourceMarker {
    SourceMarker {
        source: source.to_string(),
        count: count.to_string(),
        fingerprint: fp.to_string(),
    }
}

async fn get_response(app: Router, uri: &str) -> (StatusCode, Option<String>, Value) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let etag_header = res
        .headers()
        .get("etag")
        .map(|v| v.to_str().unwrap().to_string());
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, etag_header, json)
}

#[tokio::test]
async fn returns_month_and_etag_with_matching_header() {
    let repo = MockRepo::with_markers(vec![
        marker("time_card_dstate", "10", "12345"),
        marker("dtako_events", "20", "67890"),
    ]);
    let (status, etag_header, json) =
        get_response(app(repo), "/api/kintai/version?month=2026-07").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["month"], "2026-07");
    let etag = json["etag"].as_str().unwrap();
    // HTTP の quoted ETag そのもの — relay は文字列比較だけで済む
    assert!(etag.starts_with('"') && etag.ends_with('"'));
    // ヘッダと body は同じ値
    assert_eq!(etag_header.as_deref(), Some(etag));
}

#[tokio::test]
async fn same_markers_yield_same_etag() {
    let markers = vec![marker("time_card_dstate", "10", "12345")];
    let (_, _, j1) = get_response(
        app(MockRepo::with_markers(markers.clone())),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    let (_, _, j2) = get_response(
        app(MockRepo::with_markers(markers)),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    assert_eq!(j1["etag"], j2["etag"]);
}

#[tokio::test]
async fn changed_marker_changes_etag() {
    let (_, _, before) = get_response(
        app(MockRepo::with_markers(vec![marker(
            "time_card_dstate",
            "10",
            "12345",
        )])),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    // 在数同じ UPDATE (fingerprint だけ動く) でも変わること
    let (_, _, after) = get_response(
        app(MockRepo::with_markers(vec![marker(
            "time_card_dstate",
            "10",
            "12346",
        )])),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    assert_ne!(before["etag"], after["etag"]);
}

#[tokio::test]
async fn different_month_changes_etag() {
    let markers = vec![marker("time_card_dstate", "10", "12345")];
    let (_, _, j1) = get_response(
        app(MockRepo::with_markers(markers.clone())),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    let (_, _, j2) = get_response(
        app(MockRepo::with_markers(markers)),
        "/api/kintai/version?month=2026-08",
    )
    .await;
    assert_ne!(j1["etag"], j2["etag"]);
}

#[tokio::test]
async fn missing_or_invalid_month_is_bad_request() {
    for uri in [
        "/api/kintai/version",
        "/api/kintai/version?month=",
        "/api/kintai/version?month=2026-13",
        "/api/kintai/version?month=2026/07",
    ] {
        let repo = MockRepo::with_markers(vec![]);
        let (status, _, _) = get_response(app(repo), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri: {uri}");
    }
}

#[tokio::test]
async fn not_configured_is_503() {
    let (status, _, _) = get_response(
        app(Arc::new(DisabledKintaiVersionRepo)),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn query_failure_is_502() {
    // GRANT 不足 (drivers 等の新規テーブル) もこの経路で 502 fail-closed になる —
    // 一部テーブル抜きの etag へ縮退しないことが仕様
    let (status, _, _) = get_response(
        app(MockRepo::failing(KintaiRepoError::QueryFailed(
            "SELECT command denied".into(),
        ))),
        "/api/kintai/version?month=2026-07",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}
