//! CakePHP fetch client (Phase 2、issue #762)。
//!
//! `yhonda-ohishi/nginx` の `/uriage-jyuchu-display/masters-json` と
//! `/editable-months` を社内 LAN HTTP で pull する。token 不要 (社内網)、
//! base URL は config (空文字なら fetch 系 endpoint は 503 を返す)。

use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;

// 勤怠の口 (daily・pdf-json・autoload・resetby-unko-no) の URL・multipart・応答の型・エラーの種別は
// 勤怠 Worker と共有の `kintai-logic` が正本 (Refs #322)。ここは reqwest での送受信だけを持つ
use kintai_logic::cakephp as wire;
use kintai_logic::cakephp::urlencode;
pub use kintai_logic::cakephp::{
    CakephpError, DtakoAutoloadResponse, ResetTimecardResponse, TimecardDailyResponse,
};

/// `/uriage-jyuchu-display/masters-json` のレスポンス。
///
/// 例:
/// ```json
/// {
///   "date": "2026-06-29",
///   "offices": {
///     "1": {
///       "display_name": "本社",
///       "persons": {"1499": "青井", ...},
///       "other": {"031": "帯広営業所", ...},
///       "bumon": ["010", "011", "030"]
///     }
///   }
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct MastersResponse {
    pub date: String,
    pub offices: HashMap<String, OfficeMasters>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OfficeMasters {
    pub display_name: String,
    /// 入力担当C (string keys、CakePHP 側 JSON 仕様) → 担当者名
    pub persons: HashMap<String, String>,
    /// 稼動部門コード → 営業所名 (別営業所判定)
    pub other: HashMap<String, String>,
    /// 受注部門コード配列 (PR #766 で追加)
    #[serde(default)]
    pub bumon: Vec<String>,
}

impl OfficeMasters {
    /// `persons` を `HashMap<i32, String>` に変換 (`compute_person_sum` 入力用)
    pub fn persons_as_int_map(&self) -> HashMap<i32, String> {
        self.persons
            .iter()
            .filter_map(|(k, v)| k.parse::<i32>().ok().map(|i| (i, v.clone())))
            .collect()
    }
}

/// `/uriage-jyuchu-display/editable-months` のレスポンス。
///
/// 例:
/// ```json
/// {"operation_month": "2026-07", "editable_months_count": 2,
///  "editable_months": ["2026-06", "2026-07"]}
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct EditableMonthsResponse {
    pub operation_month: String,
    pub editable_months_count: i32,
    pub editable_months: Vec<String>,
}

/// `/uriage-jyuchu-display/print-json` のレスポンス (検証用に使う `.sum` のみ抽出)。
///
/// PHP テンプレ由来の単日 (date) × 営業所 (id) × cal の `$sum` を JSON 化したもの。
/// 担当者名 → `{ 金額, 傭車金額, 件数 }` の map。verify endpoint で Rust 側 sum と
/// 1:1 比較する。他のフィールド (例: meta) は無視 (serde default)。
#[derive(Debug, Clone, Deserialize)]
pub struct PrintJsonResponse {
    #[serde(default)]
    pub sum: serde_json::Value,
}

/// CakePHP fetch client。
///
/// `base_url` 空文字なら NotConfigured を返す。
pub struct CakephpClient {
    base_url: String,
    client: reqwest::Client,
    /// POST (autoload・resetby-unko-no) 専用。**3xx を追わない** (`redirect::Policy::none()`)。
    post_client: reqwest::Client,
}

impl CakephpClient {
    pub fn new(base_url: String, timeout_secs: u64) -> Result<Self, CakephpError> {
        let build = |redirect: reqwest::redirect::Policy| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(timeout_secs))
                // 社内 LAN かつ self-signed cert を許容 (PHP dev vhost 想定)
                .danger_accept_invalid_certs(true)
                .redirect(redirect)
                .build()
                .map_err(|e| CakephpError::RequestFailed(format!("client build: {e}")))
        };
        let client = build(reqwest::redirect::Policy::default())?;
        // 本文を組み立て済みのバイト列で送ると reqwest は 307 / 308 を本文ごと再送して追う
        // (以前の multipart の stream は再送できず追わなかった)。追うと api の無い分岐の先
        // (resetby-unko-no なら最大 100 運行ぶんの書き込み) に入りうるので、POST は明示的に追わない
        let post_client = build(reqwest::redirect::Policy::none())?;
        Ok(Self {
            base_url,
            client,
            post_client,
        })
    }

    /// `base_url` が空でなければ true (= fetch 可能)
    pub fn is_enabled(&self) -> bool {
        !self.base_url.is_empty()
    }

    /// `/uriage-jyuchu-display/masters-json?date=YYYY-MM-DD`
    pub async fn fetch_masters(&self, date: &str) -> Result<MastersResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let url = format!(
            "{}/uriage-jyuchu-display/masters-json?date={}",
            self.base_url.trim_end_matches('/'),
            urlencode(date)
        );
        self.get_json(&url).await
    }

    /// `/uriage-jyuchu-display/editable-months`
    pub async fn fetch_editable_months(&self) -> Result<EditableMonthsResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let url = format!(
            "{}/uriage-jyuchu-display/editable-months",
            self.base_url.trim_end_matches('/')
        );
        self.get_json(&url).await
    }

    /// `/uriage-jyuchu-display/print-json?id=N&date=YYYY-MM-DD[&cal=cal]`
    ///
    /// 単日 × 営業所 × cal の PHP `$sum` を pull (検証 endpoint 用)。`cal=true` (=
    /// 別営業所合算、PHP の既定) なら `cal` パラメータを送らず、`cal=false` のとき
    /// だけ `cal=cal` を付ける (shell の verify script と同じ慣習)。
    pub async fn fetch_print_json(
        &self,
        id: i64,
        date: &str,
        cal: bool,
    ) -> Result<PrintJsonResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let base = self.base_url.trim_end_matches('/');
        let url = if cal {
            format!(
                "{}/uriage-jyuchu-display/print-json?id={}&date={}",
                base,
                id,
                urlencode(date)
            )
        } else {
            format!(
                "{}/uriage-jyuchu-display/print-json?id={}&date={}&cal=cal",
                base,
                id,
                urlencode(date)
            )
        };
        self.get_json(&url).await
    }

    /// `/time-card/daily-json?month=YYYY-MM`
    ///
    /// 勤怠 (タイムカード) の日別データ。1 社員 × 1 日 = 1 行で、日跨ぎ勤務は
    /// 始業日に寄せてある。中抜けの内訳は各行の `sessions` に入る。
    pub async fn fetch_timecard_daily(
        &self,
        month: &str,
    ) -> Result<TimecardDailyResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let url = wire::join(&self.base_url, &wire::daily_json_path(month));
        self.get_json(&url).await
    }

    /// `/time-card/pdf-json?month=YYYY-MM[&driver_id=1021]&recalc=0`
    /// (Refs #143、yhonda-ohishi/nginx#782)
    ///
    /// タイムカード表 **PDF (`TimeCardController::createPdf`) が出す数字**の JSON 版。
    /// [`fetch_timecard_daily`](Self::fetch_timecard_daily) (打刻セッション) とは別物で、
    /// 拘束 (`time_card_kosoku` の日別合計・type 別内訳)・休暇区分・月次集計欄を持つ。
    /// dtako-admin のタイムカード表と 1 vs 1 で突き合わせるための読み出し口。
    /// **`recalc=0` の固定** (読み取り口でいられる理由) は `kintai_logic::cakephp::pdf_json_path`。
    ///
    /// 副次的に速い。実測 (2026-04 / 乗務員 1379): **4.18 秒 → 0.31 秒**。
    /// 全乗務員では上流計測で実行時間の約 65% が再計算だった。
    ///
    /// 応答は [`serde_json::Value`] のまま返す — 上流の形が確定しておらず、かつ
    /// このサービスは中継であって解釈者ではないため、型を持たない。
    pub async fn fetch_timecard_pdf_json(
        &self,
        month: &str,
        driver: Option<u64>,
    ) -> Result<serde_json::Value, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let url = wire::join(&self.base_url, &wire::pdf_json_path(month, driver));
        self.get_json(&url).await
    }

    /// `POST /dtako-events/autoload` — csvdata.zip を社内 nginx の取り込み口へ渡す
    /// (Refs #274 / #205 の 58 / #205 の 61)。**1 回の呼び出しは 1 つの zip だけを
    /// 送る** — 一括取り込みは作らない (`dtako_events` を書き換える破壊的操作のため)。
    ///
    /// 認証・CSRF は不要 (`AppController::beforeFilter` の
    /// `addUnauthenticatedActions` / `Application.php` のホワイトリストに
    /// `DtakoEvents::autoload` が乗っている、親が実物で確認済み)。
    ///
    /// 本文 (`api=1` と、MIME 固定の `file[]`) は `kintai_logic::cakephp::autoload_multipart` が組む
    /// (`api` が必須な理由・MIME を固定する理由もそちら)。timeout は
    /// `DTAKO_AUTOLOAD_TIMEOUT_SECS` (120 秒) で、この client 全体の既定 (`timeout_secs`、他の高速な
    /// GET 用) とは別に、この呼び出しだけ `RequestBuilder::timeout()` で上書きする。
    pub async fn post_dtako_autoload(
        &self,
        file_name: &str,
        zip_bytes: Vec<u8>,
    ) -> Result<DtakoAutoloadResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let url = wire::join(&self.base_url, wire::AUTOLOAD_PATH);
        let form = wire::autoload_multipart(&gen_boundary(), file_name, &zip_bytes);
        let timeout = Duration::from_secs(wire::DTAKO_AUTOLOAD_TIMEOUT_SECS);
        let res = self.post_multipart(&url, form, Some(timeout)).await?;
        let status = res.status().as_u16();
        let location = location_of(&res);
        let body = res.text().await.unwrap_or_default();
        Ok(wire::autoload_response(status, &body, location))
    }

    /// `POST /time-card-dtako/resetby-unko-no/<unko_no>` — 勤務時間の再登録
    /// (③、Refs #205 の 63 / yhonda-ohishi/nginx#795)。**1 回の呼び出しは
    /// 1 つの `unko_no` だけを対象にする** — `dtako_events` と同じく破壊的操作
    /// (`time_card_dtako` への書き戻し) のため一括処理は作らない。
    ///
    /// 本文は `api=1` だけ (無いと最大 100 運行ぶんの書き込みに巻き込まれる。
    /// `kintai_logic::cakephp` の `api_part` の doc)。
    ///
    /// ## 応答は空 200 (yhonda-ohishi/nginx#796)
    ///
    /// 成否は Flash (session) にしか出ないため、`ResetTimecardResponse::status`
    /// を成功の証拠として使ってはいけない (型の doc 参照)。
    pub async fn post_reset_timecard(
        &self,
        unko_no: &str,
    ) -> Result<ResetTimecardResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        let url = wire::join(&self.base_url, &wire::reset_timecard_path(unko_no));
        let form = wire::reset_multipart(&gen_boundary());
        let res = self.post_multipart(&url, form, None).await?;
        let status = res.status().as_u16();
        let location = location_of(&res);
        Ok(ResetTimecardResponse { status, location })
    }

    /// 組み立て済みの multipart を POST する。
    ///
    /// **3xx は追わない** (`post_client`) — 3xx が返ってきたときは Location だけが手がかりなので
    /// 呼び手が保険として拾う。
    async fn post_multipart(
        &self,
        url: &str,
        form: wire::Multipart,
        timeout: Option<Duration>,
    ) -> Result<reqwest::Response, CakephpError> {
        let mut req = self
            .post_client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, form.content_type)
            .body(form.body);
        if let Some(t) = timeout {
            req = req.timeout(t);
        }
        req.send()
            .await
            .map_err(|e| CakephpError::RequestFailed(e.to_string()))
    }

    async fn get_json<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<T, CakephpError> {
        let res = self
            .client
            .get(url)
            .send()
            .await
            .map_err(|e| CakephpError::RequestFailed(e.to_string()))?;
        let status = res.status();
        if !status.is_success() {
            let body = res.text().await.unwrap_or_default();
            return Err(wire::status_error(status.as_u16(), &body));
        }
        res.json::<T>()
            .await
            .map_err(|e| CakephpError::JsonError(e.to_string()))
    }
}

/// 応答の `Location` ヘッダ (無い・読めなければ `None`)。
fn location_of(res: &reqwest::Response) -> Option<String> {
    res.headers()
        .get(reqwest::header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

/// multipart の境界 (reqwest の既定と同じ形・長さ。乱数は uuid v4 から取る)。
fn gen_boundary() -> String {
    let (a, b) = uuid::Uuid::new_v4().as_u64_pair();
    let (c, d) = uuid::Uuid::new_v4().as_u64_pair();
    wire::boundary([a, b, c, d])
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn parse_masters_response() {
        let json = r#"{
            "date": "2026-06-29",
            "offices": {
                "1": {
                    "display_name": "本社",
                    "persons": {"1499": "青井", "1364": "山﨑智"},
                    "other": {"031": "帯広営業所"},
                    "bumon": ["010", "011", "030"]
                },
                "9": {
                    "display_name": "宮崎",
                    "persons": {"2000": "田中"},
                    "other": {},
                    "bumon": ["015"]
                }
            }
        }"#;
        let m: MastersResponse = serde_json::from_str(json).unwrap();
        assert_eq!(m.date, "2026-06-29");
        assert_eq!(m.offices.len(), 2);
        let honsha = &m.offices["1"];
        assert_eq!(honsha.display_name, "本社");
        assert_eq!(honsha.persons.len(), 2);
        assert_eq!(honsha.bumon, vec!["010", "011", "030"]);
    }

    #[test]
    fn parse_masters_response_missing_bumon_defaults_empty() {
        // PR #765 初期は bumon が無かった → default 空配列で fallback
        let json = r#"{
            "date": "2026-06-29",
            "offices": {
                "1": {
                    "display_name": "本社",
                    "persons": {},
                    "other": {}
                }
            }
        }"#;
        let m: MastersResponse = serde_json::from_str(json).unwrap();
        assert!(m.offices["1"].bumon.is_empty());
    }

    #[test]
    fn parse_editable_months() {
        let json = r#"{
            "operation_month": "2026-07",
            "editable_months_count": 2,
            "editable_months": ["2026-06", "2026-07"]
        }"#;
        let e: EditableMonthsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(e.operation_month, "2026-07");
        assert_eq!(e.editable_months_count, 2);
        assert_eq!(e.editable_months, vec!["2026-06", "2026-07"]);
    }

    #[test]
    fn persons_as_int_map_skips_unparseable_keys() {
        let mut m = OfficeMasters {
            display_name: "x".into(),
            persons: HashMap::new(),
            other: HashMap::new(),
            bumon: vec![],
        };
        m.persons.insert("1499".into(), "青井".into());
        m.persons
            .insert("invalid".into(), "should_be_skipped".into());
        m.persons.insert("1364".into(), "山﨑智".into());
        let parsed = m.persons_as_int_map();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.get(&1499), Some(&"青井".to_string()));
        assert_eq!(parsed.get(&1364), Some(&"山﨑智".to_string()));
    }

    #[test]
    fn urlencode_alphanumeric_passthrough() {
        assert_eq!(urlencode("2026-06-29"), "2026-06-29");
        assert_eq!(urlencode("abc.XYZ_123~"), "abc.XYZ_123~");
    }

    #[test]
    fn urlencode_special_chars() {
        assert_eq!(urlencode("a b"), "a%20b");
        assert_eq!(urlencode("a+b"), "a%2Bb");
    }

    #[tokio::test]
    async fn client_not_configured_returns_error() {
        let c = CakephpClient::new(String::new(), 30).unwrap();
        assert!(!c.is_enabled());
        let err = c.fetch_editable_months().await.unwrap_err();
        assert!(matches!(err, CakephpError::NotConfigured));
        let err2 = c.fetch_masters("2026-06-29").await.unwrap_err();
        assert!(matches!(err2, CakephpError::NotConfigured));
        let err3 = c
            .fetch_timecard_pdf_json("2026-04", Some(1021))
            .await
            .unwrap_err();
        assert!(matches!(err3, CakephpError::NotConfigured));
        let err4 = c
            .post_dtako_autoload("csvdata.zip", vec![1, 2, 3])
            .await
            .unwrap_err();
        assert!(matches!(err4, CakephpError::NotConfigured));
    }

    #[test]
    fn cakephp_error_display() {
        assert!(CakephpError::NotConfigured
            .to_string()
            .contains("not configured"));
        assert!(CakephpError::RequestFailed("dns".into())
            .to_string()
            .contains("dns"));
        assert!(CakephpError::StatusError {
            status: 404,
            body_excerpt: "Not Found".into(),
        }
        .to_string()
        .contains("404"));
        assert!(CakephpError::JsonError("bad".into())
            .to_string()
            .contains("bad"));
    }

    #[tokio::test]
    async fn post_dtako_autoload_sends_the_fixed_mime_and_the_zip_as_file_bracket() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/dtako-events/autoload"))
            // ★ 罠1 (MIME 決め打ち): application/zip だと黙って無視されるので、
            // ここを固定して送っていることを直接検証する
            .and(body_string_contains(
                "Content-Type: application/x-zip-compressed",
            ))
            .and(body_string_contains("name=\"file[]\""))
            .and(body_string_contains("filename=\"csvdata.zip\""))
            // ★ 罠2 (#205 の 61): api が無いと PHP 側が 307 で "/" へ redirect する
            .and(body_string_contains("name=\"api\""))
            .respond_with(ResponseTemplate::new(200).set_body_string("import queued"))
            .expect(1)
            .mount(&server)
            .await;

        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_dtako_autoload("csvdata.zip", b"PK\x03\x04fake-zip".to_vec())
            .await
            .unwrap();
        assert_eq!(res.status, 200);
        assert_eq!(res.body_excerpt, "import queued");
        assert_eq!(res.location, None);
    }

    #[tokio::test]
    async fn post_dtako_autoload_surfaces_the_location_header_on_a_3xx_response() {
        // api を送り忘れた (退行) / PHP 側の挙動が変わった等で 3xx が返ってきても、
        // body が空だと何も分からない (#205 の 61) — Location だけは拾えることを保証する
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(307).insert_header("location", "/"))
            .expect(1)
            .mount(&server)
            .await;

        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_dtako_autoload("csvdata.zip", b"PK\x03\x04fake-zip".to_vec())
            .await
            .unwrap();
        assert_eq!(res.status, 307);
        assert_eq!(res.location, Some("/".to_string()));
    }

    #[tokio::test]
    async fn post_dtako_autoload_returns_non_2xx_as_ok_instead_of_collapsing_to_an_error() {
        // 条件5: 成功シグナルだけを返さない — HTTP レベルで失敗しても呼び出し側が
        // 実際の status / 本文を読めるよう、ここで Err に丸めない
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
            .mount(&server)
            .await;

        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_dtako_autoload("csvdata.zip", vec![0u8; 8])
            .await
            .unwrap();
        assert_eq!(res.status, 500);
        assert_eq!(res.body_excerpt, "Internal Server Error");
    }

    #[tokio::test]
    async fn post_dtako_autoload_truncates_the_body_to_2000_chars() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let long_body = "x".repeat(5000);
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string(long_body))
            .mount(&server)
            .await;

        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_dtako_autoload("csvdata.zip", vec![0u8; 4])
            .await
            .unwrap();
        assert_eq!(res.body_excerpt.chars().count(), 2000);
    }

    #[tokio::test]
    async fn post_dtako_autoload_maps_connection_failure_to_request_failed() {
        // port 0 は listen できないアドレスなので必ず接続失敗する
        let c = CakephpClient::new("http://127.0.0.1:0".to_string(), 1).unwrap();
        let err = c
            .post_dtako_autoload("csvdata.zip", vec![0u8; 4])
            .await
            .unwrap_err();
        assert!(matches!(err, CakephpError::RequestFailed(_)));
    }

    #[tokio::test]
    async fn post_reset_timecard_sends_api_flag_to_the_dashed_route() {
        use wiremock::matchers::{body_string_contains, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            // ★ DashedRoute なので action は resetby-unko-no (resetbyUnkoNo ではない)
            .and(path(
                "/time-card-dtako/resetby-unko-no/26060507533000000042861",
            ))
            // ★ #795: api が無いと最大 100 運行ぶんの書き込みに巻き込まれる
            .and(body_string_contains("name=\"api\""))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_reset_timecard("26060507533000000042861")
            .await
            .unwrap();
        assert_eq!(res.status, 200);
        assert_eq!(res.location, None);
    }

    #[tokio::test]
    async fn post_reset_timecard_returns_not_configured_when_base_url_is_empty() {
        let c = CakephpClient::new(String::new(), 30).unwrap();
        let err = c.post_reset_timecard("26060507533000000042861").await;
        assert!(matches!(err.unwrap_err(), CakephpError::NotConfigured));
    }

    #[tokio::test]
    async fn post_reset_timecard_surfaces_the_location_header() {
        // 応答は空 200 のはずだが、万一 3xx が返っても location だけは拾えることを保証する。
        // 307 で確認する — 301/302/303 は reqwest が自動で GET へ追従し得るため、
        // 追従しない (body を再送しない) 307/308 の方が実際に観測される形に近い
        // (`post_dtako_autoload_surfaces_the_location_header_on_a_3xx_response` と同じ理由)。
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(307).insert_header("location", "/time-card-dtako"))
            .expect(1)
            .mount(&server)
            .await;

        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_reset_timecard("26060507533000000042861")
            .await
            .unwrap();
        assert_eq!(res.status, 307);
        assert_eq!(res.location, Some("/time-card-dtako".to_string()));
    }

    /// wiremock が受けたリクエストを 1 つの文字列にする (snapshot 用)。host (port が毎回違う) は除き、
    /// ヘッダーは名前順。本文はバイト単位で escape する (zip・UTF-8 のファイル名も失わない)。
    /// multipart の境界は毎回違うので `BOUNDARY` に置き換える。
    pub(crate) fn render_request(req: &wiremock::Request) -> String {
        let boundary = req
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .and_then(|c| c.split("boundary=").nth(1))
            .map(str::to_string);
        let mut headers: Vec<String> = req
            .headers
            .iter()
            .filter(|(k, _)| k.as_str() != "host")
            .map(|(k, v)| format!("{k}: {}", v.to_str().unwrap()))
            .collect();
        headers.sort();
        let body: String = req
            .body
            .iter()
            .flat_map(|b| std::ascii::escape_default(*b))
            .map(char::from)
            .collect();
        let query = req.url.query().map(|q| format!("?{q}")).unwrap_or_default();
        let path = req.url.path();
        let text = format!(
            "{} {path}{query}\n{}\n\n{body}",
            req.method,
            headers.join("\n")
        );
        match boundary {
            Some(b) => text.replace(&b, "BOUNDARY"),
            None => text,
        }
    }

    /// 1 回の呼び出しで受けたリクエストを全部 render する。
    pub(crate) async fn render_received(server: &wiremock::MockServer) -> String {
        let reqs = server.received_requests().await.unwrap();
        reqs.iter()
            .map(render_request)
            .collect::<Vec<_>>()
            .join("\n---\n")
    }

    /// `name` の snapshot を `tests/fixtures/<file>` の該当節と比べる。
    /// `UPDATE_CAKEPHP_SNAPSHOT=1` のときは比べずに集めて書き出す (基点で 1 回だけ使う)。
    pub(crate) fn check_snapshot(file: &str, cases: &[(String, String)]) {
        let rendered: String = cases
            .iter()
            .map(|(name, text)| format!("===== {name}\n{text}\n"))
            .collect();
        let path = format!("{}/tests/fixtures/{file}", env!("CARGO_MANIFEST_DIR"));
        if std::env::var("UPDATE_CAKEPHP_SNAPSHOT").as_deref() == Ok("1") {
            std::fs::write(&path, &rendered).unwrap();
            return;
        }
        let expected = std::fs::read_to_string(&path).unwrap();
        assert_eq!(rendered, expected, "snapshot {file} がずれた");
    }

    /// 移す前 (基点 dd2b9c4) に CakePHP へ送っていたリクエスト (method・URL・ヘッダー・本文) と、
    /// 応答の読み取り結果を固定する (Refs #322)。純粋部分を kintai-logic へ移した後も同じであること。
    #[tokio::test]
    async fn wire_snapshot_matches_the_baseline() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mut cases: Vec<(String, String)> = Vec::new();

        // daily-json: 未知のトップレベルも素通しで復元する
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/time-card/daily-json"))
            .respond_with(ResponseTemplate::new(200).set_body_string(
                r#"{"rows":[{"driver_id":1021,"sessions":[]}],"month":"2026-06","z":null}"#,
            ))
            .mount(&server)
            .await;
        let c = CakephpClient::new(format!("{}/", server.uri()), 30).unwrap();
        let res = c.fetch_timecard_daily("2026-06").await.unwrap();
        let out = serde_json::to_string(&res).unwrap();
        cases.push((
            "daily".into(),
            format!("{}\n=> {out}", render_received(&server).await),
        ));

        // daily-json: 非 2xx は本文 500 文字までの StatusError
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503).set_body_string("あ".repeat(600)))
            .mount(&server)
            .await;
        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let err = c.fetch_timecard_daily("2026 06").await.unwrap_err();
        cases.push((
            "daily_503".into(),
            format!("{}\n=> {err}", render_received(&server).await),
        ));

        // pdf-json: driver あり・なし (recalc=0 固定)
        for driver in [Some(1021), None] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(200).set_body_string(r#"{"drivers":[1]}"#))
                .mount(&server)
                .await;
            let c = CakephpClient::new(server.uri(), 30).unwrap();
            let res = c.fetch_timecard_pdf_json("2026-04", driver).await.unwrap();
            let name = format!("pdf_json_{driver:?}");
            cases.push((
                name,
                format!("{}\n=> {res}", render_received(&server).await),
            ));
        }

        // autoload: 既定のファイル名・escape が要るファイル名、3xx の location と本文の抜粋
        let names = ["csvdata.zip", "a\"b\\c\r\n日本.zip"];
        for (i, file_name) in names.iter().enumerate() {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(
                    ResponseTemplate::new(307)
                        .insert_header("location", "/")
                        .set_body_string("x".repeat(2100)),
                )
                .mount(&server)
                .await;
            let c = CakephpClient::new(server.uri(), 30).unwrap();
            let zip = b"PK\x03\x04\x00\xff fake-zip \r\n--".to_vec();
            let res = c.post_dtako_autoload(file_name, zip).await.unwrap();
            let out = serde_json::to_string(&res).unwrap();
            cases.push((
                format!("autoload_{i}"),
                format!("{}\n=> {out}", render_received(&server).await),
            ));
        }

        // resetby-unko-no
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
        let c = CakephpClient::new(server.uri(), 30).unwrap();
        let res = c
            .post_reset_timecard("26060507533000000042861")
            .await
            .unwrap();
        let out = serde_json::to_string(&res).unwrap();
        cases.push((
            "reset".into(),
            format!("{}\n=> {out}", render_received(&server).await),
        ));

        check_snapshot("cakephp_wire.txt", &cases);
    }

    #[tokio::test]
    async fn post_reset_timecard_maps_connection_failure_to_request_failed() {
        let c = CakephpClient::new("http://127.0.0.1:0".to_string(), 1).unwrap();
        let err = c
            .post_reset_timecard("26060507533000000042861")
            .await
            .unwrap_err();
        assert!(matches!(err, CakephpError::RequestFailed(_)));
    }
}
