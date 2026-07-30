//! `/health` が「この instance が使うと宣言したバックエンド」を外に出すこと。
//!
//! オンプレ (SQL Server あり) と GCP (SQL Server なし) の両方が一級の実行形態なので、
//! **どちらとして立っているかが外から判別できる**必要がある。
//! 「起動はしているが実は繋がっていない」という静かな degraded を作らないための固定。

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::repo::DynRepo;
use rust_ichibanboshi::routes;
use rust_ichibanboshi::routes::health::HealthState;
use tower::ServiceExt;

fn app(repo: DynRepo, state: HealthState) -> Router {
    Router::new()
        .route("/health", get(routes::health::health))
        .layer(Extension(state))
        .layer(Extension(repo))
}

async fn get_health(repo: DynRepo, state: HealthState) -> (StatusCode, String) {
    let res = app(repo, state)
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 64 * 1024)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

#[tokio::test]
async fn test_health_reports_onprem_shape() {
    // オンプレ: SQL Server を宣言 → 毎回 SELECT 1 を打ち、成功なら "ok"
    let (status, body) = get_health(
        common::mock_repo(),
        HealthState {
            sqlserver: true,
            mariadb: true,
            kyuyo: true,
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"sqlserver\":\"ok\""), "{body}");
    assert!(body.contains("\"mariadb\":\"declared\""), "{body}");
    assert!(body.contains("\"kyuyo\":\"declared\""), "{body}");
}

#[tokio::test]
async fn test_health_reports_gcp_shape_without_touching_sqlserver() {
    // GCP: SQL Server を宣言していない → repo に触らず 200。
    // repo は必ず失敗する error_repo を渡してあるので、200 が返る事実自体が
    // 「SQL Server を一切叩いていない」ことの証明になる。
    let (status, body) = get_health(
        common::error_repo(),
        HealthState {
            sqlserver: false,
            mariadb: false,
            kyuyo: false,
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("\"sqlserver\":\"disabled\""), "{body}");
    assert!(body.contains("\"mariadb\":\"disabled\""), "{body}");
    assert!(body.contains("\"kyuyo\":\"disabled\""), "{body}");
}

#[tokio::test]
async fn test_health_declared_sqlserver_down_is_still_503() {
    // 宣言したのに落ちている = 従来どおり 503。degraded を黙って 200 にしない。
    let (status, _body) = get_health(
        common::error_repo(),
        HealthState {
            sqlserver: true,
            mariadb: false,
            kyuyo: false,
        },
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_health_query_error_on_declared_sqlserver_is_503() {
    let (status, _body) = get_health(
        common::query_error_repo(),
        HealthState {
            sqlserver: true,
            mariadb: false,
            kyuyo: false,
        },
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}
