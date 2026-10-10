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
        // RFC3339・ナノ秒 9 桁・+00:00 (書式は kintai-logic の format_synced_at が固定。Worker も同じ)
        let synced_at = body["synced_at"].as_str().unwrap();
        let frac = synced_at.split_once('.').unwrap().1;
        assert_eq!(frac.len(), "123456789+00:00".len(), "{synced_at}");
        assert!(frac.ends_with("+00:00"), "{synced_at}");
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

    async fn synced(
        &self,
        _comp_id: &str,
    ) -> Result<Vec<rust_ichibanboshi::restraint_store::RestraintSyncedRow>, RestraintStoreError>
    {
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

#[tokio::test]
async fn synced_months_lists_pushed_scopes_per_comp() {
    let store = memory_store();
    let app_router = |s: DynRestraintStore| {
        Router::new()
            .route(
                "/api/restraint/summaries",
                put(routes::restraint::put_summaries),
            )
            .route(
                "/api/restraint/synced-months",
                get(routes::restraint::synced_months),
            )
            .layer(Extension(s))
    };
    // comp 27324455 に 2 push、別 comp に 1 push
    for (comp, source, month) in [
        ("27324455", "theearth", "2026-06"),
        ("27324455", "timecard", "2026-06"),
        ("99999999", "theearth", "2026-06"),
    ] {
        let body = serde_json::json!({
            "comp_id": comp, "source": source, "month": month,
            "entries": [summary_entry("100", 600)],
        });
        let (status, _) = send(
            app_router(store.clone()),
            "PUT",
            "/api/restraint/summaries",
            Some(body),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    let (status, body) = send(
        app_router(store.clone()),
        "GET",
        "/api/restraint/synced-months?comp=27324455",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let entries = body["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2); // 別 comp は混ざらない
    assert_eq!(entries[0]["source"], "theearth");
    assert_eq!(entries[0]["month"], "2026-06");
    assert_eq!(entries[0]["row_count"], 1);
    assert!(entries[0]["synced_at"].as_str().unwrap().contains("T"));
    assert_eq!(entries[1]["source"], "timecard");

    // comp 検証 + Disabled 503
    let (status, _) = send(
        app_router(store),
        "GET",
        "/api/restraint/synced-months?comp=a/b",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let disabled: DynRestraintStore = Arc::new(DisabledRestraintStore);
    let (status, _) = send(
        app_router(disabled),
        "GET",
        "/api/restraint/synced-months?comp=27324455",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

// ── 移す前 (dd2b9c4) の応答を固定値にしたスナップショット (Refs #322) ──
// synced_at だけは時刻なので "<synced_at>" に置き換えて比べる (null はそのまま)。

fn normalize_synced_at(v: &mut serde_json::Value) {
    match v {
        serde_json::Value::Object(map) => {
            for (k, child) in map.iter_mut() {
                if k == "synced_at" && child.is_string() {
                    *child = serde_json::json!("<synced_at>");
                } else {
                    normalize_synced_at(child);
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(normalize_synced_at),
        _ => {}
    }
}

fn snapshot_app(store: DynRestraintStore) -> Router {
    Router::new()
        .route(
            "/api/restraint/summaries",
            put(routes::restraint::put_summaries),
        )
        .route(
            "/api/restraint/wage-source",
            get(routes::restraint::wage_source),
        )
        .route(
            "/api/restraint/synced-months",
            get(routes::restraint::synced_months),
        )
        .layer(Extension(store))
}

/// 同じ PUT の列を流した後の 3 口の応答 (synced_at は正規化済み)。
async fn snapshot_responses() -> Vec<serde_json::Value> {
    let store = memory_store();
    let puts = [
        serde_json::json!({"comp_id": "27324455", "source": "theearth", "month": "2026-06",
            "entries": [summary_entry("100", 600), summary_entry("200", 700)]}),
        // 100 を上書き・300 を no_data で足す (200 は残る → row_count 3)
        serde_json::json!({"comp_id": "27324455", "source": "theearth", "month": "2026-06",
            "entries": [summary_entry("100", 999), {"driver_cd": "300", "no_data": true}]}),
        serde_json::json!({"comp_id": "27324455", "source": "timecard", "month": "2026-06",
            "entries": [summary_entry("300", 480)]}),
        serde_json::json!({"comp_id": "27324455", "source": "theearth", "month": "2026-05",
            "entries": [summary_entry("100", 500)]}),
        serde_json::json!({"comp_id": "27324455", "source": "timecard", "month": "2025-12",
            "entries": [{"driver_cd": "400", "no_data": true, "summary": {"x": 1}}]}),
        serde_json::json!({"comp_id": "27324455", "source": "theearth", "month": "2026-01",
            "entries": []}),
        // LIKE の '_' は 1 文字の wildcard — a_b の一覧に axb が混ざらないこと
        serde_json::json!({"comp_id": "a_b", "source": "theearth", "month": "2026-06",
            "entries": [summary_entry("1", 1)]}),
        serde_json::json!({"comp_id": "axb", "source": "timecard", "month": "2026-06",
            "entries": [summary_entry("2", 2)]}),
    ];
    let mut out = Vec::new();
    for body in puts {
        let (status, mut res) = send(
            snapshot_app(store.clone()),
            "PUT",
            "/api/restraint/summaries",
            Some(body),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        normalize_synced_at(&mut res);
        out.push(res);
    }
    for uri in [
        "/api/restraint/wage-source?comp=27324455&month=2026-06",
        "/api/restraint/wage-source?comp=27324455&month=2026-01",
        "/api/restraint/synced-months?comp=27324455",
        "/api/restraint/synced-months?comp=a_b",
        "/api/restraint/synced-months?comp=nobody",
    ] {
        let (status, mut res) = send(snapshot_app(store.clone()), "GET", uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        normalize_synced_at(&mut res);
        out.push(res);
    }
    out
}

/// dd2b9c4 (共有 crate へ移す前・row_count を別の SELECT で数えていた版) で取った応答。
/// 順は `snapshot_responses` の PUT 8 本 → wage-source 2 本 → synced-months 3 本。
const SNAPSHOT_BEFORE_MOVE: [&str; 13] = [
    r#"{"saved":2,"synced_at":"<synced_at>"}"#,
    r#"{"saved":2,"synced_at":"<synced_at>"}"#,
    r#"{"saved":1,"synced_at":"<synced_at>"}"#,
    r#"{"saved":1,"synced_at":"<synced_at>"}"#,
    r#"{"saved":1,"synced_at":"<synced_at>"}"#,
    r#"{"saved":0,"synced_at":"<synced_at>"}"#,
    r#"{"saved":1,"synced_at":"<synced_at>"}"#,
    r#"{"saved":1,"synced_at":"<synced_at>"}"#,
    r#"{"comp_id":"27324455","current_theearth":{"no_data_drivers":["300"],"summaries":[{"driver_cd":"100","fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"100","driverName":"乗務員100","restraintMinutes":999}},{"driver_cd":"200","fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"200","driverName":"乗務員200","restraintMinutes":700}}],"synced_at":"<synced_at>"},"current_timecard":{"no_data_drivers":[],"summaries":[{"driver_cd":"300","fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"300","driverName":"乗務員300","restraintMinutes":480}}],"synced_at":"<synced_at>"},"month":"2026-06","prev_month":"2026-05","prev_theearth":{"no_data_drivers":[],"summaries":[{"driver_cd":"100","fetched_at":"2026-07-01T00-00-00Z","last_verified_at":"2026-07-02T00-00-00Z","summary":{"days":[{"day":1,"isRestDay":false}],"driverCd":"100","driverName":"乗務員100","restraintMinutes":500}}],"synced_at":"<synced_at>"},"prev_timecard":{"no_data_drivers":[],"summaries":[],"synced_at":null}}"#,
    r#"{"comp_id":"27324455","current_theearth":{"no_data_drivers":[],"summaries":[],"synced_at":"<synced_at>"},"current_timecard":{"no_data_drivers":[],"summaries":[],"synced_at":null},"month":"2026-01","prev_month":"2025-12","prev_theearth":{"no_data_drivers":[],"summaries":[],"synced_at":null},"prev_timecard":{"no_data_drivers":["400"],"summaries":[],"synced_at":"<synced_at>"}}"#,
    r#"{"entries":[{"month":"2026-01","row_count":0,"source":"theearth","synced_at":"<synced_at>"},{"month":"2026-05","row_count":1,"source":"theearth","synced_at":"<synced_at>"},{"month":"2026-06","row_count":3,"source":"theearth","synced_at":"<synced_at>"},{"month":"2025-12","row_count":1,"source":"timecard","synced_at":"<synced_at>"},{"month":"2026-06","row_count":1,"source":"timecard","synced_at":"<synced_at>"}]}"#,
    r#"{"entries":[{"month":"2026-06","row_count":1,"source":"theearth","synced_at":"<synced_at>"}]}"#,
    r#"{"entries":[]}"#,
];

#[tokio::test]
async fn responses_match_snapshot_before_move() {
    let got = snapshot_responses().await;
    assert_eq!(got.len(), SNAPSHOT_BEFORE_MOVE.len());
    for (i, (g, want)) in got.iter().zip(SNAPSHOT_BEFORE_MOVE).enumerate() {
        let want: serde_json::Value = serde_json::from_str(want).unwrap();
        assert_eq!(g, &want, "応答 {i} が移す前と違う");
    }
}
