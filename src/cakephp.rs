//! CakePHP fetch client (Phase 2、issue #762)。
//!
//! `yhonda-ohishi/nginx` の `/uriage-jyuchu-display/masters-json` と
//! `/editable-months` を社内 LAN HTTP で pull する。token 不要 (社内網)、
//! base URL は config (空文字なら fetch 系 endpoint は 503 を返す)。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// CakePHP fetch エラー。
#[derive(Debug)]
pub enum CakephpError {
    /// `base_url` 未設定 (= CakePHP fetch 機能が無効化されている)
    NotConfigured,
    /// HTTP request 失敗 (DNS / 接続 / timeout 等)
    RequestFailed(String),
    /// HTTP non-2xx
    StatusError { status: u16, body_excerpt: String },
    /// レスポンス JSON parse 失敗
    JsonError(String),
}

impl std::fmt::Display for CakephpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(f, "CakePHP base_url is not configured"),
            Self::RequestFailed(m) => write!(f, "CakePHP request failed: {m}"),
            Self::StatusError {
                status,
                body_excerpt,
            } => {
                write!(
                    f,
                    "CakePHP returned status {status}, body excerpt: {body_excerpt}"
                )
            }
            Self::JsonError(m) => write!(f, "CakePHP response parse failed: {m}"),
        }
    }
}

impl std::error::Error for CakephpError {}

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

/// `/time-card/daily-json?month=YYYY-MM` のレスポンス (Refs
/// ohishi-exp/nuxt-dtako-admin#424 / yhonda-ohishi/nginx#773, #776)。
///
/// **行は `serde_json::Value` のまま持つ** — このサービスは中継であって解釈者では
/// ないので、上流が項目を足しても型を触らずに素通しできるようにする。同じ理由で
/// `deny_unknown_fields` は付けず、トップレベルの未知フィールドも `extra` に拾って
/// 再シリアライズ時に復元する。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimecardDailyResponse {
    pub rows: Vec<serde_json::Value>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

/// `POST /dtako-events/autoload` の応答 (Refs #274 / #205 の 58)。
///
/// HTTP status と PHP が返した本文 (先頭 2000 文字) を**そのまま**保持する。
/// CakePHP 側は MIME 判定に失敗しても展開もエラー応答も出さず 200 を返す
/// (親が実物で確認済み) ため、ここで「成功/失敗」に丸めない — 呼び出し側
/// (route) が status と本文の両方を見て判断できるようにする。
#[derive(Debug, Clone, Serialize)]
pub struct DtakoAutoloadResponse {
    pub status: u16,
    pub body_excerpt: String,
}

/// CakePHP fetch client。
///
/// `base_url` 空文字なら NotConfigured を返す。
pub struct CakephpClient {
    base_url: String,
    client: reqwest::Client,
}

impl CakephpClient {
    pub fn new(base_url: String, timeout_secs: u64) -> Result<Self, CakephpError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            // 社内 LAN かつ self-signed cert を許容 (PHP dev vhost 想定)
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|e| CakephpError::RequestFailed(format!("client build: {e}")))?;
        Ok(Self { base_url, client })
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
        let url = format!(
            "{}/time-card/daily-json?month={}",
            self.base_url.trim_end_matches('/'),
            urlencode(month)
        );
        self.get_json(&url).await
    }

    /// `/time-card/pdf-json?month=YYYY-MM[&driver_id=1021]`
    /// (Refs #143、yhonda-ohishi/nginx#782)
    ///
    /// タイムカード表 **PDF (`TimeCardController::createPdf`) が出す数字**の JSON 版。
    /// [`fetch_timecard_daily`](Self::fetch_timecard_daily) (打刻セッション) とは別物で、
    /// 拘束 (`time_card_kosoku` の日別合計・type 別内訳)・休暇区分・月次集計欄を持つ。
    /// dtako-admin のタイムカード表と 1 vs 1 で突き合わせるための読み出し口。
    ///
    /// **`driver_id` 省略で全乗務員。** MCP の一括チェックが月 1 リクエストで済むよう
    /// 上流がそう作られている。
    ///
    /// ## `recalc=0` を必ず付ける (yhonda-ohishi/nginx#786)
    ///
    /// 上流は既定 (`recalc=1`) だと拘束時間を**再計算し、値が変われば
    /// `time_card_kosoku` を DELETE + INSERT する**。この endpoint は突合のための
    /// **読み取り口**なので、叩くたびに本番データが書き換わってよいはずがない。
    /// パラメータで選ばせず、ここで固定する — 呼び出し側 (relay / MCP) が付け忘れる
    /// 余地を残さないため。
    ///
    /// 副次的に速い。実測 (2026-04 / 乗務員 1379): **4.18 秒 → 0.31 秒**。
    /// 全乗務員では上流計測で実行時間の約 65% が再計算だった。
    ///
    /// 読むのは保存済みの値になるが、突合の相手は**紙のタイムカード表 = 保存済みの値**
    /// なのでこちらが正しい。再計算が要るなら PDF を出す本来の経路で行う。
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
        let base = self.base_url.trim_end_matches('/');
        let url = match driver {
            Some(d) => format!(
                "{}/time-card/pdf-json?month={}&driver_id={}&recalc=0",
                base,
                urlencode(month),
                d
            ),
            None => format!(
                "{}/time-card/pdf-json?month={}&recalc=0",
                base,
                urlencode(month)
            ),
        };
        self.get_json(&url).await
    }

    /// PHP (`DtakoEventsController::autoload`) が zip として受け付ける唯一の
    /// Content-Type。**`$file->getClientMediaType() === "application/x-zip-compressed"`
    /// でしか判定しない** — OS/ブラウザの一般的な既定 MIME である
    /// `application/zip` で送ると、展開もエラー応答も無く黙って無視される
    /// (親が実物で確認済み、Refs #274)。reqwest は拡張子や中身から MIME を
    /// 推測しないので、ここで固定しないと必ず踏む。
    const DTAKO_AUTOLOAD_MIME: &'static str = "application/x-zip-compressed";

    /// `post_dtako_autoload` 専用の timeout (秒)。PHP 側は取り込みを
    /// `for ($i=1;$i<10;$i++)` で最大 18 回叩き直し、`usleep` のビジーウェイトも
    /// 挟む (親が実物で確認済み、Refs #274) ため応答が遅いことがある。1 回あたりの
    /// 所要時間は明記されていないので、受け入れ条件の下限 (60 秒) の倍を確保し、
    /// 「取り込みは進んでいるのにこちらが先に諦めて失敗と誤判定する」方を避ける —
    /// 待ちすぎるコストより、進行中の書き込みを失敗と誤報するコストの方が高い。
    /// この client 全体の既定 (`timeout_secs`、他の高速な GET 用) とは別に、この
    /// 呼び出しだけ `RequestBuilder::timeout()` で上書きする。
    const DTAKO_AUTOLOAD_TIMEOUT_SECS: u64 = 120;

    /// `POST /dtako-events/autoload` — csvdata.zip を社内 nginx の取り込み口へ渡す
    /// (Refs #274 / #205 の 58)。**1 回の呼び出しは 1 つの zip だけを送る** —
    /// 一括取り込みは作らない (`dtako_events` を書き換える破壊的操作のため)。
    ///
    /// 認証・CSRF は不要 (`AppController::beforeFilter` の
    /// `addUnauthenticatedActions` / `Application.php` のホワイトリストに
    /// `DtakoEvents::autoload` が乗っている、親が実物で確認済み)。
    pub async fn post_dtako_autoload(
        &self,
        file_name: &str,
        zip_bytes: Vec<u8>,
    ) -> Result<DtakoAutoloadResponse, CakephpError> {
        if !self.is_enabled() {
            return Err(CakephpError::NotConfigured);
        }
        // format! を複数行にしない (フォーマット文字列が独立行だと llvm-cov の行
        // カバレッジに乗らないことがある、CLAUDE.md / kintai-ops skill §5)
        let base = self.base_url.trim_end_matches('/');
        let url = format!("{base}/dtako-events/autoload");
        let part = reqwest::multipart::Part::bytes(zip_bytes)
            .file_name(file_name.to_string())
            .mime_str(Self::DTAKO_AUTOLOAD_MIME)
            .expect("DTAKO_AUTOLOAD_MIME is a constant valid MIME string");
        let form = reqwest::multipart::Form::new().part("file[]", part);
        let res = self
            .client
            .post(&url)
            .timeout(Duration::from_secs(Self::DTAKO_AUTOLOAD_TIMEOUT_SECS))
            .multipart(form)
            .send()
            .await
            .map_err(|e| CakephpError::RequestFailed(e.to_string()))?;
        let status = res.status().as_u16();
        let body = res.text().await.unwrap_or_default();
        let body_excerpt: String = body.chars().take(2000).collect();
        Ok(DtakoAutoloadResponse {
            status,
            body_excerpt,
        })
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
            let excerpt: String = body.chars().take(500).collect();
            return Err(CakephpError::StatusError {
                status: status.as_u16(),
                body_excerpt: excerpt,
            });
        }
        res.json::<T>()
            .await
            .map_err(|e| CakephpError::JsonError(e.to_string()))
    }
}

/// 最小限の URL encode (date 文字列が `:` `+` 等を含むことは無い想定だが念のため `%` 関連だけ吸収)
fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            ' ' => "%20".to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}

#[cfg(test)]
mod tests {
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
}
