//! /api/kintai/daily の中継テスト (Refs #99)。
//!
//! CakePHP は wiremock で stub する (uriage の /recalc 統合テストと同じ方針)。
//! 「素通しであること」— 未知フィールドを落とさない・行を解釈しない — を固定するのが
//! 主眼。ここが崩れると、上流 (yhonda-ohishi/nginx) が項目を足したときに中継で
//! 情報が消え、受け手 (nuxt-dtako-admin) からは原因が見えなくなる。

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::kintai_store::{
    CachedKintai, DynKintaiStore, KintaiStore, KintaiStoreApi, KintaiStoreError, NoopKintaiStore,
};
use rust_ichibanboshi::routes;
use tower::ServiceExt;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// derived store を差し込む版 (Refs #106 Phase 2)。cache 系のテストで使う。
fn app_with_store(base_url: String, store: DynKintaiStore) -> Router {
    Router::new()
        .route("/api/kintai/daily", get(routes::kintai::daily))
        .layer(Extension(Arc::new(
            CakephpClient::new(base_url, 5).expect("client"),
        )))
        .layer(Extension(store))
}

/// 従来テスト用 — store は Noop (= 常に素通し中継)。
fn app(base_url: String) -> Router {
    app_with_store(base_url, Arc::new(NoopKintaiStore))
}

async fn call(app: Router, uri: &str) -> (StatusCode, String) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

/// 実機の応答形 (nginx#776 後)。`sessions` は中抜けの内訳で必ず存在する。
const UPSTREAM_BODY: &str = r#"{
  "rows": [
    {
      "driver_id": 1670,
      "name": "松永　寿乃",
      "date": "2026-06-11",
      "start": "2026-06-11 07:44:15",
      "end": "2026-06-11 16:49:11",
      "restraint_minutes": 544,
      "sessions": [
        {"start": "2026-06-11 07:44:15", "end": "2026-06-11 11:41:10"},
        {"start": "2026-06-11 13:14:38", "end": "2026-06-11 16:49:11"}
      ],
      "holiday": "weekday",
      "office": "大石運輸倉庫㈱ 本社",
      "future_field": {"nested": [1, 2, 3]}
    }
  ],
  "generated_at": "2026-07-26T00:00:00+09:00"
}"#;

#[tokio::test]
async fn relays_rows_and_preserves_unknown_fields() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/daily-json"))
        .and(query_param("month", "2026-06"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);

    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    let row = &v["rows"][0];
    assert_eq!(row["driver_id"], 1670);
    assert_eq!(row["restraint_minutes"], 544);
    assert_eq!(row["sessions"].as_array().unwrap().len(), 2);
    assert_eq!(row["holiday"], "weekday");
    // 行の未知フィールド (将来の追加項目) が落ちない
    assert_eq!(row["future_field"]["nested"][2], 3);
    // トップレベルの未知フィールドも落ちない
    assert_eq!(v["generated_at"], "2026-07-26T00:00:00+09:00");
    // 氏名の非 ASCII が壊れない
    assert_eq!(row["name"], "松永　寿乃");
}

#[tokio::test]
async fn empty_rows_is_ok() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"rows":[]}"#))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    // メタ (source / synced_at) が足される以外は素通し
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["rows"].as_array().unwrap().is_empty());
    assert_eq!(v["source"], "live");
    assert!(v["synced_at"].as_str().unwrap().contains("T"));
}

#[tokio::test]
async fn month_is_required_and_validated() {
    // 上流が呼ばれないことを mount 無しの server で担保する
    let server = MockServer::start().await;
    for uri in [
        "/api/kintai/daily",
        "/api/kintai/daily?month=",
        "/api/kintai/daily?month=2026-6",
        "/api/kintai/daily?month=2026-13",
        "/api/kintai/daily?month=2026/06",
    ] {
        let (status, body) = call(app(server.uri()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri={uri}");
        assert!(body.contains("YYYY-MM"), "uri={uri}");
    }
}

#[tokio::test]
async fn base_url_unset_is_503() {
    let (status, body) = call(app(String::new()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("base_url"));
}

#[tokio::test]
async fn upstream_5xx_is_502() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("500"));
}

#[tokio::test]
async fn upstream_non_json_is_502() {
    // allowlist 登録漏れでログイン画面の HTML が返るケース (nginx#773 で指摘した罠)
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("<!DOCTYPE html><html>login</html>"),
        )
        .mount(&server)
        .await;

    let (status, body) = call(app(server.uri()), "/api/kintai/daily?month=2026-06").await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("parse failed"));
}

#[tokio::test]
async fn upstream_unreachable_is_502() {
    // 到達不能なポート (listen していない) → RequestFailed
    let (status, body) = call(
        app("http://127.0.0.1:1".to_string()),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("fetch failed"));
}

// ══════════════════════════════════════════════════════════════
// SQLite derived store (read-through / refresh、Refs #106 Phase 2)
// ══════════════════════════════════════════════════════════════

/// テスト用の故障 / 破損 store。
#[derive(Default)]
struct BrokenKintaiStore {
    fail_get: bool,
    corrupt: bool,
    fail_put: bool,
}

#[async_trait]
impl KintaiStoreApi for BrokenKintaiStore {
    async fn get_daily(&self, _month: &str) -> Result<Option<CachedKintai>, KintaiStoreError> {
        if self.fail_get {
            return Err(KintaiStoreError::QueryError("boom".to_string()));
        }
        if self.corrupt {
            return Ok(Some(CachedKintai {
                response_json: "not-json".to_string(),
                synced_at: "2026-07-26T00:00:00Z".to_string(),
            }));
        }
        Ok(None)
    }

    async fn put_daily(
        &self,
        _month: &str,
        _response_json: &str,
        _row_count: usize,
        _synced_at: &str,
    ) -> Result<(), KintaiStoreError> {
        if self.fail_put {
            return Err(KintaiStoreError::QueryError("boom".to_string()));
        }
        Ok(())
    }
}

fn memory_store() -> DynKintaiStore {
    Arc::new(KintaiStore::open(":memory:").expect("in-memory store"))
}

#[tokio::test]
async fn read_through_serves_cache_without_cakephp() {
    // 1 回目: live + write-through
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;
    let store = memory_store();
    let (status, live) = call(
        app_with_store(server.uri(), store.clone()),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let live: serde_json::Value = serde_json::from_str(&live).unwrap();
    assert_eq!(live["source"], "live");

    // 2 回目: 上流到達不能でも (= CakePHP 停止相当) キャッシュから返る
    let (status, cached) = call(
        app_with_store("http://127.0.0.1:1".to_string(), store.clone()),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let cached: serde_json::Value = serde_json::from_str(&cached).unwrap();
    assert_eq!(cached["source"], "cache");
    assert_eq!(cached["synced_at"], live["synced_at"]);
    assert_eq!(cached["rows"], live["rows"]);
    // 上流の未知フィールドもキャッシュ経由で保存される (素通し方針の維持)
    assert_eq!(cached["generated_at"], live["generated_at"]);

    // 別月は miss → 上流到達不能なら 502 (キャッシュの取り違えをしない)
    let (status, _) = call(
        app_with_store("http://127.0.0.1:1".to_string(), store),
        "/api/kintai/daily?month=2026-05",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn refresh_bypasses_cache_and_overwrites() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;
    let store = memory_store();
    // 古い内容をキャッシュに仕込む
    store
        .put_daily("2026-06", r#"{"rows":[]}"#, 0, "2026-07-01T00:00:00Z")
        .await
        .unwrap();

    // refresh=1 はキャッシュを飛ばして上流から引き直し、上書きする
    let (status, body) = call(
        app_with_store(server.uri(), store.clone()),
        "/api/kintai/daily?month=2026-06&refresh=1",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["source"], "live");
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);

    // 上書き後の read は新しい内容の cache
    let (status, body) = call(
        app_with_store("http://127.0.0.1:1".to_string(), store),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["source"], "cache");
    assert_eq!(v["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn store_failures_fall_back_to_live_relay() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string(UPSTREAM_BODY))
        .mount(&server)
        .await;

    // get 失敗 + put 失敗 → live 中継はそのまま成功 (warn ログのみ)
    let store: DynKintaiStore = Arc::new(BrokenKintaiStore {
        fail_get: true,
        fail_put: true,
        ..Default::default()
    });
    let (status, body) = call(
        app_with_store(server.uri(), store),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["source"], "live");

    // 破損キャッシュ (parse 不能) → live へフォールバック
    let store: DynKintaiStore = Arc::new(BrokenKintaiStore {
        corrupt: true,
        ..Default::default()
    });
    let (status, body) = call(
        app_with_store(server.uri(), store),
        "/api/kintai/daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["source"], "live");
}
