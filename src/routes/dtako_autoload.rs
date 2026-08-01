//! csvdata.zip を受け取り、社内 nginx (CakePHP) の取り込み口
//! (`POST /dtako-events/autoload`) へ 1 件ずつ中継する (Refs #205 の 58 / #274)。
//!
//! ## 全体の経路のうち、ここは 2 段目
//!
//! ```text
//! kyuyo-mcp ─(SCRAPER_RELAY)→ relay ─→ theearth      ① zip を取る (別タスク #205-59)
//! kyuyo-mcp ─(fetchIchibanJson)→ オンプレ rust ─→ nginx  ② 取り込む   ★ここ
//! ```
//!
//! **オンプレだけが社内 nginx (内部アドレス) に届く。** Cloudflare の worker からは
//! 届かない (Refs #205、kintai-ops skill §4.5)。
//!
//! ## ファイル名がなぜ `dtako_autoload.rs` か (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/routes/` 配下を**ファイル名の接頭辞**
//! (`kintai`) で拾い、拾われたファイルの内容ハッシュが `logic_version`
//! (`/api/kintai/version` の etag) に畳まれる。この endpoint は勤怠の計算に一切
//! 関与しない (取り込みの中継だけ) ので、`kintai` で始まる名前を付けると無関係な
//! deploy まで全乗務員 stale にしてしまう ([`crate::routes::dtako_day`] と同じ理由)。
//!
//! ## 一括取り込みを作らない (受け入れ条件3)
//!
//! `dtako_events` の取り込みは既存行を書き換える破壊的操作なので、1 回の POST は
//! **必ず 1 件の `unko_no`** に紐付ける。月まるごと等の一括指定は受け付けない —
//! `unko_no` を必須にし、数字以外や空を 400 で弾くのがその歯止め。
//!
//! ## preview (受け入れ条件4)
//!
//! `?preview=true` は実際には nginx へ送らず、`unko_no` / 送信サイズ / 投げ先
//! (相対パスのみ、host は含めない — 受け入れ条件6) / CakePHP 設定済みかを返す。
//! 実行するかどうかを事前に確認できるようにするための口で、zip の受信自体は
//! preview でも行う (でなければサイズを計算できない)。
//!
//! ## 応答 (受け入れ条件5)
//!
//! 「成功シグナルだけ」には丸めない。CakePHP は MIME 判定に失敗しても展開も
//! エラーも出さず 200 を返す (親が実物で確認済み) ため、HTTP status と PHP の
//! 応答本文の抜粋をそのまま返し、「実際に何が起きたか」の判断材料を呼び出し側
//! (kyuyo-mcp / 人) に渡す。

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::cakephp::{CakephpClient, CakephpError};

/// nginx 側の相対パス。**host は含めない** — 受け入れ条件6 (内部アドレスを
/// commit / PR / docs に書かない)。実際の到達先は `CakephpClient` の
/// `base_url` (env `CAKEPHP_BASE_URL`) が持つ。
const AUTOLOAD_PATH: &str = "/dtako-events/autoload";

/// この route 専用の body size 上限 (20 MiB)。axum の `Bytes` extractor は
/// 既定 2 MiB までしか受けない — 1 件 (1 unko_no) ぶんの csvdata.zip は
/// 数 CSV の集合で通常はごく小さいはずだが、上限自体は
/// `DefaultBodyLimit::max` で個別に緩めておく (他の route の既定 2 MiB には
/// 影響しない、server.rs でこの route にだけ layer する)。
pub const MAX_ZIP_BYTES: usize = 20 * 1024 * 1024;

/// `?unko_no=&file_name=&preview=`
#[derive(Debug, Deserialize)]
pub struct AutoloadQuery {
    pub unko_no: Option<String>,
    pub file_name: Option<String>,
    #[serde(default)]
    pub preview: bool,
}

/// `unko_no` の受け入れ判定。**空・非数字は拒否** — 「対象を名指しで受け取る」
/// (月まるごと等の一括指定を弾く) 歯止め。桁数は固定しない — オンプレ23桁 /
/// GCP・theearth 側22桁で実物の桁が揺れる (Refs #205 の 57 実機確認、
/// `dtako_day.rs` のモジュール doc 参照) ため、「全部数字で最低限それらしい
/// 長さ」だけを見る (12 = `unko_no` 先頭の開始日時 `YYMMDDHHMMSS` の桁数)。
fn parse_unko_no(raw: &str) -> Option<&str> {
    if raw.len() < 12 || !raw.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some(raw)
}

/// CakePHP client のエラーを HTTP ステータスへ写す。`routes/kintai.rs` に同じ形の
/// `map_cakephp_err` があるが、あちらは `build.rs` の glob 対象 (`logic_version` が
/// 動く) なので import せず独立して持つ (`dtako_day.rs` と同じ方針)。
fn map_cakephp_err(e: CakephpError) -> (StatusCode, String) {
    match e {
        CakephpError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url が未設定 (CAKEPHP_BASE_URL)".to_string(),
        ),
        CakephpError::RequestFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("nginx への接続に失敗: {m}"),
        ),
        // post_dtako_autoload は非2xxも Ok で返すので実際には作らないが、
        // CakephpError は他の fetch_* と共有の enum なので網羅のために残す
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP returned {status}: {body_excerpt}"),
        ),
        CakephpError::JsonError(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP response parse failed: {m}"),
        ),
    }
}

/// POST /api/dtako/autoload?unko_no=&file_name=&preview= — csvdata.zip (body) を
/// 1 件だけ社内 nginx の取り込み口へ中継する (Refs #205 の 58 / #274)。
pub async fn autoload(
    Query(params): Query<AutoloadQuery>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    body: Bytes,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let unko_no = match params.unko_no.as_deref().and_then(parse_unko_no) {
        Some(u) => u.to_string(),
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "unko_no は対象を1件、数字だけで指定してください (一括取り込みは不可)".to_string(),
            ))
        }
    };
    if body.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "body が空です。csvdata.zip の中身を送ってください".to_string(),
        ));
    }
    let file_name = params
        .file_name
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "csvdata.zip".to_string());
    let size_bytes = body.len();

    if params.preview {
        tracing::info!(unko_no, size_bytes, "dtako autoload: preview");
        return Ok(Json(serde_json::json!({
            "preview": true,
            "unko_no": unko_no,
            "file_name": file_name,
            "size_bytes": size_bytes,
            "target_path": AUTOLOAD_PATH,
            "configured": cakephp.is_enabled(),
            "note": "preview=true のため実際には送信していません",
        })));
    }

    let res = cakephp
        .post_dtako_autoload(&file_name, body.to_vec())
        .await
        .map_err(map_cakephp_err)?;
    let http_ok = (200..300).contains(&res.status);
    let status = res.status;
    tracing::info!(unko_no, size_bytes, status, "dtako autoload sent");
    Ok(Json(serde_json::json!({
        "preview": false,
        "unko_no": unko_no,
        "file_name": file_name,
        "size_bytes": size_bytes,
        "target_path": AUTOLOAD_PATH,
        "http_status": res.status,
        "http_ok": http_ok,
        "response_excerpt": res.body_excerpt,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::post;
    use axum::Router;
    use serde_json::Value;
    use tower::ServiceExt;

    fn app(cakephp: Arc<CakephpClient>) -> Router {
        Router::new()
            .route("/dtako/autoload", post(autoload))
            .layer(Extension(cakephp))
    }

    async fn call(router: Router, uri: &str, body: Vec<u8>) -> (StatusCode, Value) {
        let res = router
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(uri)
                    .body(axum::body::Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
        };
        (status, body)
    }

    fn unconfigured_client() -> Arc<CakephpClient> {
        Arc::new(CakephpClient::new(String::new(), 30).unwrap())
    }

    #[test]
    fn parse_unko_no_requires_all_digits_and_at_least_12_chars() {
        assert_eq!(
            parse_unko_no("26060507533000000042861"),
            Some("26060507533000000042861")
        );
        assert_eq!(
            parse_unko_no("2602241025060000000272"),
            Some("2602241025060000000272")
        );
        assert_eq!(parse_unko_no(""), None, "空は拒否");
        assert_eq!(
            parse_unko_no("260605075330"),
            Some("260605075330"),
            "12桁ちょうどは受ける"
        );
        assert_eq!(parse_unko_no("26060507533"), None, "11桁は拒否");
        assert_eq!(
            parse_unko_no("2026-06"),
            None,
            "月まるごと等の一括指定っぽい文字列は拒否"
        );
        assert_eq!(
            parse_unko_no("2606050753300000004286a"),
            None,
            "非数字混じりは拒否"
        );
    }

    #[test]
    fn map_cakephp_err_covers_all_variants() {
        assert_eq!(
            map_cakephp_err(CakephpError::NotConfigured).0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            map_cakephp_err(CakephpError::RequestFailed("dns".into())).0,
            StatusCode::BAD_GATEWAY
        );
        let (status, msg) = map_cakephp_err(CakephpError::StatusError {
            status: 404,
            body_excerpt: "not found".into(),
        });
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(msg.contains("404"));
        assert_eq!(
            map_cakephp_err(CakephpError::JsonError("bad".into())).0,
            StatusCode::BAD_GATEWAY
        );
    }

    #[tokio::test]
    async fn autoload_rejects_missing_or_invalid_unko_no() {
        let router = app(unconfigured_client());
        let (status, _) = call(router, "/dtako/autoload?preview=true", vec![1, 2, 3]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let router2 = app(unconfigured_client());
        let (status2, _) = call(
            router2,
            "/dtako/autoload?unko_no=2026-06&preview=true",
            vec![1, 2, 3],
        )
        .await;
        assert_eq!(status2, StatusCode::BAD_REQUEST, "一括っぽい指定は拒否");
    }

    #[tokio::test]
    async fn autoload_rejects_empty_body() {
        let router = app(unconfigured_client());
        let (status, _) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true",
            vec![],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn autoload_preview_never_calls_cakephp_and_never_leaks_the_host() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // preview では 1 回も叩かれないことを expect(0) で保証する
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp);
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["preview"], serde_json::json!(true));
        assert_eq!(
            body["unko_no"],
            serde_json::json!("26060507533000000042861")
        );
        assert_eq!(body["file_name"], serde_json::json!("csvdata.zip"));
        assert_eq!(body["size_bytes"], serde_json::json!(12));
        assert_eq!(
            body["target_path"],
            serde_json::json!("/dtako-events/autoload")
        );
        assert_eq!(body["configured"], serde_json::json!(true));
        assert!(
            !body.to_string().contains(&server.uri()),
            "応答に nginx の host が出てはいけない"
        );
    }

    #[tokio::test]
    async fn autoload_preview_reports_configured_false_when_base_url_is_empty() {
        let router = app(unconfigured_client());
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861&preview=true&file_name=x.zip",
            vec![1, 2, 3],
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["configured"], serde_json::json!(false));
        assert_eq!(body["file_name"], serde_json::json!("x.zip"));
    }

    #[tokio::test]
    async fn autoload_executes_and_returns_the_raw_status_and_body() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            .respond_with(ResponseTemplate::new(200).set_body_string("import queued"))
            .expect(1)
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp);
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            b"PK\x03\x04fake-zip".to_vec(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["preview"], serde_json::json!(false));
        assert_eq!(body["http_status"], serde_json::json!(200));
        assert_eq!(body["http_ok"], serde_json::json!(true));
        assert_eq!(body["response_excerpt"], serde_json::json!("import queued"));
    }

    #[tokio::test]
    async fn autoload_surfaces_a_non_2xx_php_response_instead_of_hiding_it() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let cakephp = Arc::new(CakephpClient::new(server.uri(), 30).unwrap());
        let router = app(cakephp);
        let (status, body) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            vec![9, 9, 9],
        )
        .await;
        // HTTP レベルでは中継自体は成功しているので 200 のまま、
        // 中身 (http_ok=false) で PHP 側の失敗を読ませる (条件5)
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["http_status"], serde_json::json!(500));
        assert_eq!(body["http_ok"], serde_json::json!(false));
        assert_eq!(body["response_excerpt"], serde_json::json!("boom"));
    }

    #[tokio::test]
    async fn autoload_returns_503_when_cakephp_is_not_configured() {
        let router = app(unconfigured_client());
        let (status, _) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            vec![9, 9, 9],
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn autoload_returns_502_when_nginx_is_unreachable() {
        let cakephp = Arc::new(CakephpClient::new("http://127.0.0.1:0".to_string(), 1).unwrap());
        let router = app(cakephp);
        let (status, _) = call(
            router,
            "/dtako/autoload?unko_no=26060507533000000042861",
            vec![9, 9, 9],
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }
}
