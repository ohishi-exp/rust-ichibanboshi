//! /api/kintai/events の中継テスト (Refs #114)。
//!
//! CakePHP は wiremock で stub する (`kintai_test.rs` と同じ方針)。ここでも主眼は
//! 「素通しであること」— 行を解釈しない・未知フィールドを落とさない。加えて
//! **`driver` が上流へそのまま渡ること**を固定する (省略時に全乗務員を引いて
//! しまうと生イベントは桁が 1 つ増える)。
//!
//! データ 4 ケース (打刻のみ / 運行のみ / 同日 2 運行 / 日跨ぎ) は issue #114 の
//! 受け入れ条件。中継は解釈しないので「行がそのまま通ること」を見るテストになる
//! — 解釈側の規則は Phase 2 (`kosoku-daily`) の担当。

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::cakephp::CakephpClient;
use rust_ichibanboshi::routes;
use tower::ServiceExt;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn app(base_url: String) -> Router {
    Router::new()
        .route("/api/kintai/events", get(routes::kintai::events))
        .layer(Extension(Arc::new(
            CakephpClient::new(base_url, 5).expect("client"),
        )))
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

/// 上流を stub して `/api/kintai/events` を 1 回叩く。
async fn relay(body: &str, uri: &str) -> (StatusCode, serde_json::Value) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/events-json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;
    let (status, body) = call(app(server.uri()), uri).await;
    let v = serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// 打刻 (timecard) と運行 (dtako) が混ざった時系列。未知フィールド付き。
const MIXED_BODY: &str = r#"{
  "rows": [
    {"datetime": "2026-07-23 06:11:45", "driver_id": "1051", "source": "timecard",
     "state": "始業", "unko_no": null, "vehicle": null},
    {"datetime": "2026-07-23 06:16:24", "driver_id": "1051", "source": "dtako",
     "state": "運行開始", "unko_no": "20260723-001", "vehicle": "長崎100か4132",
     "future_field": {"nested": [1, 2, 3]}},
    {"datetime": "2026-07-23 18:02:10", "driver_id": "1051", "source": "dtako",
     "state": "運行終了", "unko_no": "20260723-001", "vehicle": "長崎100か4132"},
    {"datetime": "2026-07-23 18:20:03", "driver_id": "1051", "source": "timecard",
     "state": "終業", "unko_no": null, "vehicle": null}
  ],
  "generated_at": "2026-07-27T00:00:00+09:00"
}"#;

#[tokio::test]
async fn relays_rows_and_preserves_unknown_fields() {
    let (status, v) = relay(MIXED_BODY, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);

    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    // 並び・値をいじらない
    assert_eq!(rows[0]["state"], "始業");
    assert_eq!(rows[0]["source"], "timecard");
    assert_eq!(rows[1]["unko_no"], "20260723-001");
    assert_eq!(rows[1]["vehicle"], "長崎100か4132");
    assert_eq!(rows[3]["datetime"], "2026-07-23 18:20:03");
    // 行の未知フィールドもトップレベルの未知フィールドも落ちない
    assert_eq!(rows[1]["future_field"]["nested"][2], 3);
    assert_eq!(v["generated_at"], "2026-07-27T00:00:00+09:00");
    // daily と違いキャッシュを持たないので source / synced_at のメタは足さない
    assert!(v.get("source").is_none());
    assert!(v.get("synced_at").is_none());
}

#[tokio::test]
async fn driver_and_month_are_forwarded_upstream() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/time-card/events-json"))
        .and(query_param("month", "2026-07"))
        .and(query_param("driver", "1051"))
        .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"rows":[]}"#))
        .mount(&server)
        .await;

    // month / driver のどちらかが欠けた URL で上流を叩いていたら mock に当たらず 502
    let (status, body) = call(
        app(server.uri()),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "body={body}");
}

/// 打刻のみの日 (デジタコに乗らない事務員 / 車に乗らなかった日)
#[tokio::test]
async fn timecard_only_day_passes_through() {
    let body = r#"{"rows":[
      {"datetime":"2026-07-06 08:02:11","driver_id":"1670","source":"timecard",
       "state":"始業","unko_no":null,"vehicle":null},
      {"datetime":"2026-07-06 17:31:45","driver_id":"1670","source":"timecard",
       "state":"終業","unko_no":null,"vehicle":null}
    ]}"#;
    let (status, v) = relay(body, "/api/kintai/events?month=2026-07&driver=1670").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["source"] == "timecard"));
    // 打刻が無い項目は null のまま (欠損を 0 や "" に化かさない)
    assert!(rows[0]["unko_no"].is_null());
}

/// 運行のみの日 (打刻を忘れて出庫した日) — 中継は補完しない
#[tokio::test]
async fn dtako_only_day_passes_through() {
    let body = r#"{"rows":[
      {"datetime":"2026-07-07 05:58:02","driver_id":"1051","source":"dtako",
       "state":"運行開始","unko_no":"20260707-014","vehicle":"長崎100か4132"},
      {"datetime":"2026-07-07 19:44:30","driver_id":"1051","source":"dtako",
       "state":"運行終了","unko_no":"20260707-014","vehicle":"長崎100か4132"}
    ]}"#;
    let (status, v) = relay(body, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|r| r["source"] == "dtako"));
}

/// 同日 2 運行 — 運行の切れ目 (休息・終業) も行として素通しする
#[tokio::test]
async fn two_trips_in_one_day_pass_through() {
    let body = r#"{"rows":[
      {"datetime":"2026-07-08 05:30:00","driver_id":"1051","source":"dtako",
       "state":"運行開始","unko_no":"20260708-001","vehicle":"長崎100か4132"},
      {"datetime":"2026-07-08 11:10:00","driver_id":"1051","source":"dtako",
       "state":"運行終了","unko_no":"20260708-001","vehicle":"長崎100か4132"},
      {"datetime":"2026-07-08 11:20:00","driver_id":"1051","source":"dtako",
       "state":"休息開始","unko_no":"20260708-001","vehicle":"長崎100か4132"},
      {"datetime":"2026-07-08 13:00:00","driver_id":"1051","source":"dtako",
       "state":"休息終了","unko_no":"20260708-002","vehicle":"長崎100か4132"},
      {"datetime":"2026-07-08 13:05:00","driver_id":"1051","source":"dtako",
       "state":"運行開始","unko_no":"20260708-002","vehicle":"長崎100か4132"},
      {"datetime":"2026-07-08 20:15:00","driver_id":"1051","source":"dtako",
       "state":"運行終了","unko_no":"20260708-002","vehicle":"長崎100か4132"}
    ]}"#;
    let (status, v) = relay(body, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 6);
    // 運行NO が 2 つ現れる = 集約されていない (Phase 2 で数える材料が残っている)
    assert_eq!(rows[0]["unko_no"], "20260708-001");
    assert_eq!(rows[4]["unko_no"], "20260708-002");
}

/// 日跨ぎ — 月末に始まって翌月へ出る勤務。中継は日付で切り捨てない
#[tokio::test]
async fn overnight_shift_passes_through() {
    let body = r#"{"rows":[
      {"datetime":"2026-07-31 21:40:00","driver_id":"1051","source":"timecard",
       "state":"始業","unko_no":null,"vehicle":null},
      {"datetime":"2026-07-31 22:05:00","driver_id":"1051","source":"dtako",
       "state":"運行開始","unko_no":"20260731-020","vehicle":"長崎100か4132"},
      {"datetime":"2026-08-01 07:12:00","driver_id":"1051","source":"dtako",
       "state":"運行終了","unko_no":"20260731-020","vehicle":"長崎100か4132"},
      {"datetime":"2026-08-01 07:30:00","driver_id":"1051","source":"timecard",
       "state":"終業","unko_no":null,"vehicle":null}
    ]}"#;
    let (status, v) = relay(body, "/api/kintai/events?month=2026-07&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    let rows = v["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 4);
    // 翌月に出た終業も落ちない (月で切ると拘束の終わりが消える)
    assert_eq!(rows[3]["datetime"], "2026-08-01 07:30:00");
}

#[tokio::test]
async fn month_is_required_and_validated() {
    // 上流が呼ばれないことを mount 無しの server で担保する
    let server = MockServer::start().await;
    for uri in [
        "/api/kintai/events?driver=1051",
        "/api/kintai/events?month=&driver=1051",
        "/api/kintai/events?month=2026-7&driver=1051",
        "/api/kintai/events?month=2026-13&driver=1051",
    ] {
        let (status, body) = call(app(server.uri()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri={uri}");
        assert!(body.contains("YYYY-MM"), "uri={uri}");
    }
}

#[tokio::test]
async fn driver_is_required_and_validated() {
    let server = MockServer::start().await;
    for uri in [
        "/api/kintai/events?month=2026-07",
        "/api/kintai/events?month=2026-07&driver=",
        "/api/kintai/events?month=2026-07&driver=abc",
    ] {
        let (status, body) = call(app(server.uri()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "uri={uri}");
        assert!(body.contains("乗務員CD"), "uri={uri}");
    }
}

#[tokio::test]
async fn base_url_unset_is_503() {
    let (status, body) = call(
        app(String::new()),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
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

    let (status, body) = call(
        app(server.uri()),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("500") && body.contains("boom"));
}

#[tokio::test]
async fn upstream_non_json_is_502() {
    // allowlist (`AppController::addUnauthenticatedActions`) 登録漏れでログイン画面の
    // HTML が返るケース (yhonda-ohishi/nginx#773 で踏んだ罠)
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(200).set_body_string("<!DOCTYPE html><html>login</html>"),
        )
        .mount(&server)
        .await;

    let (status, body) = call(
        app(server.uri()),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("parse failed"));
}

#[tokio::test]
async fn upstream_unreachable_is_502() {
    let (status, body) = call(
        app("http://127.0.0.1:1".to_string()),
        "/api/kintai/events?month=2026-07&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("fetch failed"));
}
