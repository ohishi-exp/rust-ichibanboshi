//! /api/restraint/* (拘束サマリ push + wage-source 一括配信、Refs #106 Phase 3) の
//! テスト。store は in-memory SQLite、認可は edge (CF Access) 前提なので
//! ハンドラ単体では掛からない。

use std::sync::Arc;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, put};
use axum::{Extension, Router};
use rust_ichibanboshi::restraint_store::{
    DisabledRestraintStore, DynRestraintStore, RestraintEntry, RestraintMonth, RestraintStore,
    RestraintStoreApi, RestraintStoreError,
};
use rust_ichibanboshi::routes;
use tower::ServiceExt;

fn app(store: DynRestraintStore) -> Router {
    Router::new()
        .route(
            "/api/restraint/summaries",
            put(routes::restraint::put_summaries),
        )
        .route(
            "/api/restraint/wage-source",
            get(routes::restraint::wage_source),
        )
        .layer(Extension(store))
}

fn memory_store() -> DynRestraintStore {
    Arc::new(RestraintStore::open(":memory:").expect("in-memory store"))
}

async fn send(
    app: Router,
    method: &str,
    uri: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, serde_json::Value) {
    let mut builder = Request::builder().uri(uri).method(method);
    let body = match body {
        Some(v) => {
            builder = builder.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let res = app.oneshot(builder.body(body).unwrap()).await.unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, json)
}

fn push_body(source: &str, month: &str, entries: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "comp_id": "27324455",
        "source": source,
        "month": month,
        "entries": entries,
    })
}

fn summary_entry(driver_cd: &str, restraint: i64) -> serde_json::Value {
    serde_json::json!({
        "driver_cd": driver_cd,
        "summary": {
            "driverCd": driver_cd,
            "driverName": format!("乗務員{driver_cd}"),
            "restraintMinutes": restraint,
            "days": [{"day": 1, "isRestDay": false}],
        },
        "fetched_at": "2026-07-01T00-00-00Z",
        "last_verified_at": "2026-07-02T00-00-00Z",
    })
}

#[tokio::test]
async fn push_then_wage_source_round_trips_current_and_prev() {
    let store = memory_store();

    // 当月 theearth + timecard、前月 theearth を push
    for (source, month, drivers) in [
        ("theearth", "2026-06", vec!["100", "200"]),
        ("timecard", "2026-06", vec!["300"]),
        ("theearth", "2026-05", vec!["100"]),
    ] {
        let entries: Vec<_> = drivers.iter().map(|d| summary_entry(d, 600)).collect();
        let (status, body) = send(
            app(store.clone()),
            "PUT",
            "/api/restraint/summaries",
            Some(push_body(source, month, serde_json::json!(entries))),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "push {source} {month}");
        assert_eq!(body["saved"], drivers.len());
        assert!(body["synced_at"].as_str().unwrap().contains("T"));
    }

    let (status, body) = send(
        app(store),
        "GET",
        "/api/restraint/wage-source?comp=27324455&month=2026-06",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["month"], "2026-06");
    assert_eq!(body["prev_month"], "2026-05");
    let cur_t = &body["current_theearth"];
    assert_eq!(cur_t["summaries"].as_array().unwrap().len(), 2);
    // サマリ JSON は verbatim (relay 側の camelCase キーのまま)
    assert_eq!(cur_t["summaries"][0]["summary"]["driverCd"], "100");
    assert_eq!(cur_t["summaries"][0]["summary"]["days"][0]["day"], 1);
    assert_eq!(cur_t["summaries"][0]["fetched_at"], "2026-07-01T00-00-00Z");
    assert!(cur_t["synced_at"].as_str().is_some());
    assert_eq!(
        body["current_timecard"]["summaries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        body["prev_theearth"]["summaries"].as_array().unwrap().len(),
        1
    );
    // 一度も push が無い (source, 月) は空 + synced_at null (relay の R2 フォールバック判定)
    assert!(body["prev_timecard"]["summaries"]
        .as_array()
        .unwrap()
        .is_empty());
    assert!(body["prev_timecard"]["synced_at"].is_null());
}

#[tokio::test]
async fn push_upserts_listed_drivers_only() {
    let store = memory_store();
    let entries = serde_json::json!([summary_entry("100", 600), summary_entry("200", 700)]);
    send(
        app(store.clone()),
        "PUT",
        "/api/restraint/summaries",
        Some(push_body("theearth", "2026-06", entries)),
    )
    .await;

    // 100 だけ更新 + 300 を no_data で追加 — 200 は残る (replace-all ではない)
    let partial = serde_json::json!([
        summary_entry("100", 999),
        {"driver_cd": "300", "no_data": true},
    ]);
    let (status, body) = send(
        app(store.clone()),
        "PUT",
        "/api/restraint/summaries",
        Some(push_body("theearth", "2026-06", partial)),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["saved"], 2);

    let (_, body) = send(
        app(store),
        "GET",
        "/api/restraint/wage-source?comp=27324455&month=2026-06",
        None,
    )
    .await;
    let cur = &body["current_theearth"];
    let summaries = cur["summaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 2); // 100 (更新) + 200 (温存)
    assert_eq!(summaries[0]["summary"]["restraintMinutes"], 999);
    assert_eq!(summaries[1]["summary"]["restraintMinutes"], 700);
    assert_eq!(cur["no_data_drivers"], serde_json::json!(["300"]));
}

#[tokio::test]
async fn push_validates_body() {
    let store = memory_store();
    for (body, needle) in [
        (
            push_body(
                "theearth",
                "2026-06",
                serde_json::json!([{"driver_cd": ""}]),
            ),
            "driver_cd",
        ),
        (
            push_body(
                "theearth",
                "2026-06",
                serde_json::json!([{"driver_cd": "1"}]),
            ),
            "summary がありません",
        ),
        (
            push_body("venus", "2026-06", serde_json::json!([])),
            "source",
        ),
        (
            push_body("theearth", "2026-6", serde_json::json!([])),
            "YYYY-MM",
        ),
        (
            serde_json::json!({"comp_id": "a/b", "source": "theearth", "month": "2026-06", "entries": []}),
            "comp_id",
        ),
    ] {
        let (status, res) = send(
            app(store.clone()),
            "PUT",
            "/api/restraint/summaries",
            Some(body),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            res["error"].as_str().unwrap().contains(needle),
            "needle={needle}"
        );
    }
}

#[tokio::test]
async fn wage_source_validates_query() {
    let store = memory_store();
    let (status, _) = send(
        app(store.clone()),
        "GET",
        "/api/restraint/wage-source?comp=&month=2026-06",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let (status, _) = send(
        app(store),
        "GET",
        "/api/restraint/wage-source?comp=27324455&month=junk",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn disabled_store_is_503() {
    let store: DynRestraintStore = Arc::new(DisabledRestraintStore);
    let (status, body) = send(
        app(store.clone()),
        "PUT",
        "/api/restraint/summaries",
        Some(push_body("theearth", "2026-06", serde_json::json!([]))),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body["error"].as_str().unwrap().contains("sqlite_path"));
    let (status, _) = send(
        app(store),
        "GET",
        "/api/restraint/wage-source?comp=27324455&month=2026-06",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

/// QueryError を返す故障 store — map_store_err の 500 側を固定する。
struct BrokenRestraintStore;

#[async_trait]
impl RestraintStoreApi for BrokenRestraintStore {
    async fn upsert(
        &self,
        _comp_id: &str,
        _source: &str,
        _ym: &str,
        _entries: &[RestraintEntry],
        _synced_at: &str,
    ) -> Result<(), RestraintStoreError> {
        Err(RestraintStoreError::QueryError("boom".to_string()))
    }

    async fn month(
        &self,
        _comp_id: &str,
        _source: &str,
        _ym: &str,
    ) -> Result<RestraintMonth, RestraintStoreError> {
        Err(RestraintStoreError::QueryError("boom".to_string()))
    }
}

#[tokio::test]
async fn store_query_error_is_500() {
    let store: DynRestraintStore = Arc::new(BrokenRestraintStore);
    let (status, _) = send(
        app(store.clone()),
        "PUT",
        "/api/restraint/summaries",
        Some(push_body("theearth", "2026-06", serde_json::json!([]))),
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    let (status, _) = send(
        app(store),
        "GET",
        "/api/restraint/wage-source?comp=27324455&month=2026-06",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn broken_summary_json_row_is_skipped_not_fatal() {
    // route の PUT 検証は通らない行 (summary_json が非 JSON / no_data でも summary
    // でもない行) を store へ直接入れ、wage-source が行単位で落として残りを返す
    // ことを固定する
    let raw = RestraintStore::open(":memory:").expect("store");
    raw.upsert(
        "27324455",
        "theearth",
        "2026-06",
        &[
            RestraintEntry {
                driver_cd: "100".to_string(),
                no_data: false,
                summary_json: Some("not-json".to_string()),
                fetched_at: None,
                last_verified_at: None,
            },
            RestraintEntry {
                driver_cd: "150".to_string(),
                no_data: false,
                summary_json: None, // no_data でも summary でもない欠損行
                fetched_at: None,
                last_verified_at: None,
            },
            RestraintEntry {
                driver_cd: "200".to_string(),
                no_data: false,
                summary_json: Some(r#"{"driverCd":"200"}"#.to_string()),
                fetched_at: None,
                last_verified_at: None,
            },
        ],
        "2026-07-01T00:00:00Z",
    )
    .await
    .expect("direct upsert");
    let store: DynRestraintStore = Arc::new(raw);
    let (status, body) = send(
        app(store),
        "GET",
        "/api/restraint/wage-source?comp=27324455&month=2026-06",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let summaries = body["current_theearth"]["summaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["summary"]["driverCd"], "200");
}
