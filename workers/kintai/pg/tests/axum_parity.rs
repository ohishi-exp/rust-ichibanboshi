//! 書き込みの口と signatures の**入力の検査**が、root の Cloud Run 版 (axum の実物の handler) と同じ status・同じ本文に
//! なることを確かめる (Refs #322)。DB は要らない: root の handler に書き先の store を挿さない (`None`) ので、入力が通れば
//! root は 503 (`[kintai_push] が無効です`) を返す — そのとき Worker 側の検査は `Ok` であること。
//!
//! Worker 側は kintai-logic の `parse_batch`・`parse_signatures`・`parse_snapshot` (口が DB の前に通すもの)。
//! axum の `Json` (415 / 413 / 400 / 422)・`Query` (400) の拒否文言もここで実物と突き合わせる。

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::{get, post};
use axum::{Extension, Router};
use kintai_logic::common::Fail;
use kintai_logic::timecard_write::{parse_batch, parse_signatures};
use kintai_logic::wage_write::parse_snapshot;
use rust_ichibanboshi::routes::kintai_timecard::{
    receive, signatures, DynKintaiPgStore, ReadTenant,
};
use rust_ichibanboshi::routes::wage_snapshot::put_wage_snapshot;
use tower::ServiceExt;

fn app() -> Router {
    Router::new()
        .route("/api/kintai/timecard", post(receive))
        .route("/api/kintai/timecard/signatures", get(signatures))
        .route("/api/kintai/wage-snapshot", post(put_wage_snapshot))
        .layer(Extension::<DynKintaiPgStore>(None))
        .layer(Extension(ReadTenant(None)))
}

/// root の (status, 本文)。
async fn root(req: Request<Body>) -> (u16, String) {
    let res = app().oneshot(req).await.unwrap();
    let status = res.status().as_u16();
    let body = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

/// Worker 側の検査の結果と root の応答を突き合わせる。Worker が通したなら root は「書き先が無い」の 503。
fn same<T>(label: &str, worker: Result<T, Fail>, root: (u16, String)) {
    match worker {
        Ok(_) => {
            assert_eq!(
                root.0,
                StatusCode::SERVICE_UNAVAILABLE.as_u16(),
                "{label}: {root:?}"
            );
            assert!(
                root.1.contains("[kintai_push] が無効です"),
                "{label}: {root:?}"
            );
        }
        Err(f) => assert_eq!((f.status, f.body), root, "{label}"),
    }
}

fn post_req(path: &str, content_type: Option<&str>, body: Vec<u8>) -> Request<Body> {
    let mut b = Request::post(path);
    if let Some(ct) = content_type {
        b = b.header("content-type", ct);
    }
    b.body(Body::from(body)).unwrap()
}

const JSON: &str = "application/json";

#[tokio::test]
async fn timecard_body_is_checked_like_the_root_handler() {
    let big = format!(
        r#"{{"month":"2026-06","driver_cd":1,"x":"{}"}}"#,
        "a".repeat(2_100_000)
    );
    let cases: Vec<(&str, Option<&str>, Vec<u8>)> = vec![
        ("ok", Some(JSON), br#"{"month":"2026-06","driver_cd":1130}"#.to_vec()),
        (
            "ok + days",
            Some("application/json; charset=utf-8"),
            br#"{"month":"2026-06","driver_cd":1,"days":{"2026-06-01":[]},"delete_dates":["2026-06-02"]}"#
                .to_vec(),
        ),
        ("content-type 無し", None, b"{}".to_vec()),
        ("text/plain", Some("text/plain"), b"{}".to_vec()),
        ("構文の誤り", Some(JSON), b"{\"month\":".to_vec()),
        ("後ろに余計な文字", Some(JSON), br#"{"month":"2026-06","driver_cd":1} x"#.to_vec()),
        ("型の誤り", Some(JSON), br#"{"month":"2026-06","driver_cd":"x"}"#.to_vec()),
        ("欄が無い", Some(JSON), br#"{"month":"2026-06"}"#.to_vec()),
        ("日付のキーが不正", Some(JSON), br#"{"month":"2026-06","driver_cd":1,"days":{"x":[]}}"#.to_vec()),
        ("month が不正", Some(JSON), br#"{"month":"2026-6","driver_cd":1}"#.to_vec()),
        ("2MB 超", Some(JSON), big.into_bytes()),
    ];
    for (label, ct, body) in cases {
        let worker = parse_batch(ct, &body);
        let root = root(post_req("/api/kintai/timecard", ct, body)).await;
        same(label, worker, root);
    }
}

#[tokio::test]
async fn signatures_query_is_checked_like_the_root_handler() {
    for q in [
        "month=2026-06&driver_cd=1130",
        "",
        "month=2026-13&driver_cd=1",
        "month=2026-06",
        "month=2026-06&driver_cd=abc",
        "month=2026-06&month=2026-07&driver_cd=1",
        "month=2026-06&driver_cd=-5",
    ] {
        let worker = parse_signatures(q);
        let req = Request::get(format!("/api/kintai/timecard/signatures?{q}"))
            .body(Body::empty())
            .unwrap();
        same(q, worker, root(req).await);
    }
}

#[tokio::test]
async fn wage_snapshot_body_is_checked_like_the_root_handler() {
    let ok = r#"{"comp_id":"c1","month":"2026-06","restraint_source":"gcp","wage_logic_version":"w1",
                "masters":{"payroll_synced_at":"2026-07-03T09:12:00Z"},"rows":[{"driver_cd":1}]}"#;
    let cases: Vec<(&str, Option<&str>, String)> = vec![
        ("ok", Some(JSON), ok.to_string()),
        ("text/plain", Some("text/plain"), ok.to_string()),
        (
            "型の誤り",
            Some(JSON),
            ok.replace(r#""driver_cd":1"#, r#""driver_cd":"x""#),
        ),
        (
            "comp_id が空",
            Some(JSON),
            ok.replace(r#""comp_id":"c1""#, r#""comp_id":"""#),
        ),
        ("month が不正", Some(JSON), ok.replace("2026-06", "2026-6")),
        (
            "source が不正",
            Some(JSON),
            ok.replace(r#""gcp""#, r#""x""#),
        ),
        (
            "同期時刻が不正",
            Some(JSON),
            ok.replace("2026-07-03T09:12:00Z", "2026/07/03"),
        ),
        ("構文の誤り", Some(JSON), "{".to_string()),
    ];
    for (label, ct, body) in cases {
        let worker = parse_snapshot(ct, body.as_bytes());
        let root = root(post_req("/api/kintai/wage-snapshot", ct, body.into_bytes())).await;
        same(label, worker, root);
    }
}
