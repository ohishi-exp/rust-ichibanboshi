//! CakePHP fetch client (Phase 2、issue #762)。
//!
//! `yhonda-ohishi/nginx` の `/uriage-jyuchu-display/masters-json` と
//! `/editable-months` を社内 LAN HTTP で pull する。token 不要 (社内網)、
//! base URL は config (空文字なら fetch 系 endpoint は 503 を返す)。

use std::time::Duration;

// エラーの種別・URL の escape・非 2xx の読み方は勤怠 Worker と共有の `kintai-logic` が正本 (Refs #322)。
// 勤怠の口 (daily・pdf-json・autoload・resetby-unko-no) はオンプレ版から撤去した (勤怠 Worker が持つ)
use kintai_logic::cakephp as wire;
use kintai_logic::cakephp::urlencode;
pub use kintai_logic::cakephp::CakephpError;

// 売上の口 (masters-json・editable-months・print-json) の応答の型は売上 Worker と共有の `uriage-logic` が正本
// (Refs #322)。path の組み立てと送受信はここに残す
pub use uriage_logic::cakephp::{
    EditableMonthsResponse, MastersResponse, OfficeMasters, PrintJsonResponse,
};

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
            return Err(wire::status_error(status.as_u16(), &body));
        }
        res.json::<T>()
            .await
            .map_err(|e| CakephpError::JsonError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let err3 = c.fetch_print_json(1, "2026-06-29", true).await.unwrap_err();
        assert!(matches!(err3, CakephpError::NotConfigured));
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
