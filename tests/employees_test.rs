mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower::ServiceExt;

// ══════════════════════════════════════════════════════════════
// ハンドラ: GET /api/employees (社員ﾏｽﾀ、Refs #74)
// ══════════════════════════════════════════════════════════════

#[tokio::test]
async fn test_employees_ok() {
    let app = common::build_app(common::mock_repo());
    let res = app
        .oneshot(
            Request::builder()
                .uri("/api/employees")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["source_table"], "社員ﾏｽﾀ");
    let data = json["data"].as_array().unwrap();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0]["employee_code"], "1001");
    assert_eq!(data[0]["employee_name"], "田中太郎");
    assert_eq!(data[0]["employee_r"], "田中");
    // 名前未設定行も落とさず返す (フィルタは消費側の責務)
    assert_eq!(data[2]["employee_code"], "9999");
    assert_eq!(data[2]["employee_r"], "");
}
