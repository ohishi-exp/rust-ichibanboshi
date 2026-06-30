//! CakePHP fetch client (Phase 2、issue #762)。
//!
//! `yhonda-ohishi/nginx` の `/uriage-jyuchu-display/masters-json` と
//! `/editable-months` を社内 LAN HTTP で pull する。token 不要 (社内網)、
//! base URL は config (空文字なら fetch 系 endpoint は 503 を返す)。

use serde::Deserialize;
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
}
