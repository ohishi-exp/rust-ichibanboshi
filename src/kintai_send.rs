//! 打刻を別の instance へ送る (Refs #205 の 04b、**送信側**)。
//!
//! オンプレが MariaDB から読んだ打刻を、GCP 側の `rust-ichibanboshi` へ渡す。
//! 受け側は [`crate::routes::kintai_timecard`]。
//!
//! ## 差分の日だけ送る
//!
//! 1. 相手の `GET /api/kintai/timecard/signatures` で (乗務員, 暦日) の署名を引く
//! 2. 手元の署名と突き合わせ、違う日と消えた日だけを [`plan_batch`] で選ぶ
//! 3. `POST /api/kintai/timecard` で渡す
//!
//! 1 か月・全乗務員の打刻は数万行あるので、毎回全部送ると転送も書き込みも無駄になる。
//! 署名は 04 で作ったものをそのまま流用する ([`crate::kintai_push`])。
//!
//! ## 1 回の呼び出しを乗務員数で区切る
//!
//! この経路は **Cloudflare Tunnel (30 秒上限) を通って起動される**。全乗務員を
//! 1 リクエストで回すと必ず 502 になる (#199 で踏んだ壁そのもの)。
//!
//! そこで **`max_drivers` で区切り、続きの位置を `next_after_driver_cd` で返す**。
//! 呼び出し側 (relay) は `null` が返るまで呼び直す。上流の
//! `GET /api/dtako/events` が `after_driver_cd` で同じことをしているのと同じ形。
//!
//! **背景ジョブにしない。** 走らせっぱなしにすると「起動はしたが失敗した」が
//! 呼び出し側に返らず、#205 のリスク欄の「静かに間違う」に落ちる。区切って
//! 同期で返せば、失敗はその場で HTTP の失敗になる。
//!
//! ## 冪等なので呼び直しは安全
//!
//! 送るのは差分だけで、送った結果が相手の署名に反映される。途中で落ちても
//! 次の呼び出しが続きから (あるいは最初から) やり直せば同じ状態に収束する。

use std::collections::BTreeSet;

use crate::config::KintaiSendConfig;
use crate::kintai_push::{
    dedup_events, group_by_date, parse_rows, plan_batch, read_driver_events, TimecardBatch,
    TimecardBatchResult,
};
use crate::kintai_repo::{exact_month_range, DynKintaiEventsRepo, KintaiRepoError};

/// 受け側の path。
const SIGNATURES_PATH: &str = "/api/kintai/timecard/signatures";
const TIMECARD_PATH: &str = "/api/kintai/timecard";

/// 1 回の呼び出しで回す乗務員数の既定。
///
/// 実測で 1 乗務員あたり 0.2 秒 (単一乗務員版の読み出し) + 相手との 2 往復。
/// 30 秒の枠に対して余裕を見て 10 にしてある。
pub const DEFAULT_MAX_DRIVERS: usize = 10;

/// `max_drivers` の上限。呼び出し側が大きな値を入れて Tunnel を殺すのを防ぐ。
pub const MAX_MAX_DRIVERS: usize = 50;

/// 送信の失敗。
#[derive(Debug)]
pub enum KintaiSendError {
    NotConfigured(String),
    Read(KintaiRepoError),
    /// 相手が返した失敗 (status と本文の先頭)。
    Remote(String),
}

impl std::fmt::Display for KintaiSendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured(m) => write!(f, "kintai send not configured: {m}"),
            Self::Read(e) => write!(f, "kintai events read failed: {e}"),
            Self::Remote(m) => write!(f, "kintai send remote failed: {m}"),
        }
    }
}

impl std::error::Error for KintaiSendError {}

impl From<KintaiRepoError> for KintaiSendError {
    fn from(e: KintaiRepoError) -> Self {
        Self::Read(e)
    }
}

/// 相手を叩く口。テストで差し替えられるよう trait にしてある。
#[async_trait::async_trait]
pub trait TimecardTarget: Send + Sync {
    async fn signatures(
        &self,
        month: &str,
        driver_cd: i64,
    ) -> Result<std::collections::BTreeMap<chrono::NaiveDate, String>, KintaiSendError>;

    async fn send(&self, batch: &TimecardBatch) -> Result<TimecardBatchResult, KintaiSendError>;
}

pub type DynTimecardTarget = std::sync::Arc<dyn TimecardTarget>;

/// HTTP 経由の相手。
pub struct HttpTimecardTarget {
    client: reqwest::Client,
    base: String,
    token: String,
}

impl HttpTimecardTarget {
    pub fn new(cfg: &KintaiSendConfig) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(cfg.timeout_secs))
            .build()
            .map_err(|e| format!("kintai send http client: {e}"))?;
        Ok(Self {
            client,
            base: cfg.target_url.trim().trim_end_matches('/').to_string(),
            token: cfg.auth_token.clone(),
        })
    }

    fn auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if self.token.is_empty() {
            req
        } else {
            req.bearer_auth(&self.token)
        }
    }

    /// 相手の応答を読む。**成功以外は本文の先頭を添えて返す** — 相手が
    /// `[kintai_push]` を宣言していない (503) のか落ちている (502) のかを、
    /// 呼び出し側のログで区別できるようにするため。
    async fn read<T: serde::de::DeserializeOwned>(
        resp: reqwest::Response,
    ) -> Result<T, KintaiSendError> {
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiSendError::Remote(format!("body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiSendError::Remote(format!(
                "status {status}: {excerpt}"
            )));
        }
        serde_json::from_str(&body).map_err(|e| KintaiSendError::Remote(format!("parse: {e}")))
    }
}

#[derive(serde::Deserialize)]
struct SignaturesResponse {
    #[serde(default)]
    signatures: std::collections::BTreeMap<chrono::NaiveDate, String>,
}

#[async_trait::async_trait]
impl TimecardTarget for HttpTimecardTarget {
    async fn signatures(
        &self,
        month: &str,
        driver_cd: i64,
    ) -> Result<std::collections::BTreeMap<chrono::NaiveDate, String>, KintaiSendError> {
        let req = self
            .client
            .get(format!("{}{SIGNATURES_PATH}", self.base))
            .query(&[("month", month), ("driver_cd", &driver_cd.to_string())]);
        let resp = self
            .auth(req)
            .send()
            .await
            .map_err(|e| KintaiSendError::Remote(format!("signatures request: {e}")))?;
        let parsed: SignaturesResponse = Self::read(resp).await?;
        Ok(parsed.signatures)
    }

    async fn send(&self, batch: &TimecardBatch) -> Result<TimecardBatchResult, KintaiSendError> {
        let req = self
            .client
            .post(format!("{}{TIMECARD_PATH}", self.base))
            .json(batch);
        let resp = self
            .auth(req)
            .send()
            .await
            .map_err(|e| KintaiSendError::Remote(format!("timecard request: {e}")))?;
        Self::read(resp).await
    }
}

/// 1 回の呼び出しの集計。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct SendReport {
    pub drivers: usize,
    pub days_sent: usize,
    pub days_deleted: usize,
    pub events_sent: usize,
    /// 相手が「日/乗務員/月が食い違う」として弾いた行数。**0 でないと送り側が壊れている**。
    pub misplaced: usize,
    /// DDL の CHECK に無かった `state` の実値 (送り側 + 相手側)。
    pub unknown_states: BTreeSet<String>,
    /// 続きの位置。`None` なら回りきった。
    pub next_after_driver_cd: Option<u64>,
}

impl SendReport {
    pub fn has_unexpected(&self) -> bool {
        !self.unknown_states.is_empty() || self.misplaced > 0
    }
}

/// 対象月の乗務員を昇順で洗い出し、`after` の次から `max` 人だけ返す。
async fn drivers_after(
    repo: &DynKintaiEventsRepo,
    from: &str,
    to: &str,
    after: Option<u64>,
    max: usize,
) -> Result<(Vec<u64>, Option<u64>), KintaiSendError> {
    let rows = repo.fetch_all_events_between(from, to).await?;
    let all: Vec<u64> = crate::kosoku::split_by_driver(rows)
        .into_iter()
        .map(|(d, _)| d)
        .collect();
    let rest: Vec<u64> = match after {
        Some(a) => all.into_iter().filter(|d| *d > a).collect(),
        None => all,
    };
    let next = rest.get(max).map(|_| rest[max - 1]);
    Ok((rest.into_iter().take(max).collect(), next))
}

/// 対象月の打刻を相手へ送る。**乗務員数で区切る** (モジュール docs 参照)。
pub async fn send_month(
    repo: &DynKintaiEventsRepo,
    target: &DynTimecardTarget,
    month: &str,
    after_driver_cd: Option<u64>,
    max_drivers: usize,
    apply: bool,
) -> Result<SendReport, KintaiSendError> {
    let (from, to) = exact_month_range(month)
        .ok_or_else(|| KintaiSendError::NotConfigured(format!("bad month: {month}")))?;
    let max = max_drivers.clamp(1, MAX_MAX_DRIVERS);
    let (drivers, next) = drivers_after(repo, &from, &to, after_driver_cd, max).await?;

    let mut report = SendReport {
        next_after_driver_cd: next,
        ..Default::default()
    };
    for driver in drivers {
        let rows = read_driver_events(repo, driver, &from, &to)
            .await
            .map_err(|e| KintaiSendError::Remote(e.to_string()))?;
        let parsed = parse_rows(&rows);
        for s in &parsed.unknown_states {
            report.unknown_states.insert(s.clone());
        }
        let local = group_by_date(&dedup_events(parsed.events));
        let remote = target.signatures(month, driver as i64).await?;
        let batch = plan_batch(month, driver as i64, &local, &remote);

        report.drivers += 1;
        report.days_sent += batch.days.len();
        report.days_deleted += batch.delete_dates.len();
        report.events_sent += batch.days.values().map(Vec::len).sum::<usize>();
        if batch.is_empty() || !apply {
            continue;
        }
        let result = target.send(&batch).await?;
        report.misplaced += result.misplaced;
        for s in &result.unknown_states {
            report.unknown_states.insert(s.clone());
        }
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::Mutex;

    /// 相手の代わり。署名を持ち、送られた batch を記録する。
    #[derive(Default)]
    struct StubTarget {
        remote: Mutex<BTreeMap<i64, BTreeMap<chrono::NaiveDate, String>>>,
        got: Mutex<Vec<TimecardBatch>>,
    }

    #[async_trait::async_trait]
    impl TimecardTarget for StubTarget {
        async fn signatures(
            &self,
            _month: &str,
            driver_cd: i64,
        ) -> Result<BTreeMap<chrono::NaiveDate, String>, KintaiSendError> {
            Ok(self
                .remote
                .lock()
                .unwrap()
                .get(&driver_cd)
                .cloned()
                .unwrap_or_default())
        }
        async fn send(
            &self,
            batch: &TimecardBatch,
        ) -> Result<TimecardBatchResult, KintaiSendError> {
            self.got.lock().unwrap().push(batch.clone());
            Ok(TimecardBatchResult::default())
        }
    }

    struct StubRepo(Vec<serde_json::Value>);

    #[async_trait::async_trait]
    impl crate::kintai_repo::KintaiEventsApi for StubRepo {
        async fn fetch_events_between(
            &self,
            _: &str,
            _: &str,
            driver: u64,
        ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
            Ok(self
                .0
                .iter()
                .filter(|r| r["driver_id"].as_u64() == Some(driver))
                .cloned()
                .collect())
        }
        async fn fetch_all_events_between(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
            Ok(self.0.clone())
        }
        async fn fetch_ferry_between(
            &self,
            _: &str,
            _: &str,
            _: Option<u64>,
        ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
            Ok(Vec::new())
        }
    }

    fn punch(driver: u64, at: &str, state: &str) -> serde_json::Value {
        json!({"datetime": at, "driver_id": driver, "source": "timecard", "state": state, "unko_no": null})
    }

    fn repo_of(rows: Vec<serde_json::Value>) -> DynKintaiEventsRepo {
        std::sync::Arc::new(StubRepo(rows))
    }

    #[tokio::test]
    async fn drivers_are_paged_with_a_cursor() {
        // Tunnel の 30 秒に収めるため乗務員数で区切る。続きは cursor で返す
        let rows: Vec<serde_json::Value> = [1, 2, 3, 4, 5]
            .iter()
            .map(|d| punch(*d, "2026-07-01 08:00:00", "始業"))
            .collect();
        let repo = repo_of(rows);

        let (page, next) = drivers_after(&repo, "", "", None, 2).await.unwrap();
        assert_eq!(page, vec![1, 2]);
        assert_eq!(next, Some(2));

        let (page, next) = drivers_after(&repo, "", "", Some(2), 2).await.unwrap();
        assert_eq!(page, vec![3, 4]);
        assert_eq!(next, Some(4));

        // 最後のページは cursor を返さない
        let (page, next) = drivers_after(&repo, "", "", Some(4), 2).await.unwrap();
        assert_eq!(page, vec![5]);
        assert_eq!(next, None);

        // 全部入る大きさなら 1 ページ
        let (page, next) = drivers_after(&repo, "", "", None, 50).await.unwrap();
        assert_eq!(page.len(), 5);
        assert_eq!(next, None);
    }

    #[tokio::test]
    async fn send_month_only_sends_the_differing_days() {
        let repo = repo_of(vec![
            punch(1, "2026-07-01 08:00:00", "始業"),
            punch(1, "2026-07-02 08:00:00", "始業"),
        ]);
        let stub = std::sync::Arc::new(StubTarget::default());
        let target: DynTimecardTarget = stub.clone();

        // 相手は空 → 2 日とも送る
        let r = send_month(&repo, &target, "2026-07", None, 10, true)
            .await
            .unwrap();
        assert_eq!(r.drivers, 1);
        assert_eq!(r.days_sent, 2);
        assert_eq!(r.events_sent, 2);
        assert!(!r.has_unexpected());
        assert_eq!(stub.got.lock().unwrap().len(), 1);

        // 相手が 07-01 を持っていれば 07-02 だけ
        let jul1 = chrono::NaiveDate::from_ymd_opt(2026, 7, 1).unwrap();
        let sig = crate::kintai_push::day_signature(&crate::kintai_push::dedup_events(
            crate::kintai_push::parse_rows(&[punch(1, "2026-07-01 08:00:00", "始業")]).events,
        ));
        stub.remote
            .lock()
            .unwrap()
            .insert(1, [(jul1, sig)].into_iter().collect());
        let r = send_month(&repo, &target, "2026-07", None, 10, true)
            .await
            .unwrap();
        assert_eq!(r.days_sent, 1);
    }

    #[tokio::test]
    async fn send_month_without_apply_sends_nothing() {
        let repo = repo_of(vec![punch(1, "2026-07-01 08:00:00", "始業")]);
        let stub = std::sync::Arc::new(StubTarget::default());
        let target: DynTimecardTarget = stub.clone();
        let r = send_month(&repo, &target, "2026-07", None, 10, false)
            .await
            .unwrap();
        assert_eq!(r.days_sent, 1, "計画は立てる");
        assert!(stub.got.lock().unwrap().is_empty(), "1 件も送っていない");
    }

    #[tokio::test]
    async fn send_month_reports_states_the_ddl_rejects() {
        // time_card_dtako.event_name は自由記述なので想定外が来うる
        let repo = repo_of(vec![
            json!({"datetime": "2026-07-01 09:00:00", "driver_id": 1, "source": "dtako", "state": "点呼"}),
        ]);
        let target: DynTimecardTarget = std::sync::Arc::new(StubTarget::default());
        let r = send_month(&repo, &target, "2026-07", None, 10, true)
            .await
            .unwrap();
        assert!(r.unknown_states.contains("点呼"));
        assert!(r.has_unexpected());
    }

    #[tokio::test]
    async fn send_month_clamps_the_page_size_and_rejects_bad_months() {
        let repo = repo_of(vec![punch(1, "2026-07-01 08:00:00", "始業")]);
        let target: DynTimecardTarget = std::sync::Arc::new(StubTarget::default());
        // 0 は 1 に、大きすぎる値は上限に丸める (Tunnel を殺さない)
        assert_eq!(
            send_month(&repo, &target, "2026-07", None, 0, false)
                .await
                .unwrap()
                .drivers,
            1
        );
        assert_eq!(
            send_month(&repo, &target, "2026-07", None, 9999, false)
                .await
                .unwrap()
                .drivers,
            1
        );
        let err = send_month(&repo, &target, "nope", None, 10, false)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("bad month"), "{err}");
    }

    #[test]
    fn report_knows_when_it_was_surprised() {
        let mut r = SendReport::default();
        assert!(!r.has_unexpected());
        r.misplaced = 1;
        assert!(r.has_unexpected());
        r.misplaced = 0;
        r.unknown_states.insert("点呼".to_string());
        assert!(r.has_unexpected());
    }

    #[test]
    fn errors_say_which_side_failed() {
        let e = KintaiSendError::NotConfigured("x".to_string());
        assert!(e.to_string().contains("not configured"), "{e}");
        let e: KintaiSendError = KintaiRepoError::NotConfigured.into();
        assert!(e.to_string().contains("read failed"), "{e}");
        let e = KintaiSendError::Remote("status 503".to_string());
        assert!(e.to_string().contains("remote failed"), "{e}");
        assert!(format!("{e:?}").contains("Remote"));
    }

    #[test]
    fn target_url_is_normalised_and_token_optional() {
        let mut cfg = KintaiSendConfig {
            enabled: true,
            target_url: "https://relay.example/".to_string(),
            ..Default::default()
        };
        let t = HttpTimecardTarget::new(&cfg).unwrap();
        assert_eq!(t.base, "https://relay.example", "末尾の / を落とす");
        assert!(t.token.is_empty());

        cfg.auth_token = "tok".to_string();
        assert_eq!(HttpTimecardTarget::new(&cfg).unwrap().token, "tok");
    }
}
