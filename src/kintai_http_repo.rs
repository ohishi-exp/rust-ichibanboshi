//! 勤怠の生イベントを `rust-alc-api` の HTTP 経由で読む実装 (Refs #205 実装計画 02)。
//!
//! [`crate::kintai_repo::MariadbKintaiEventsRepo`] の 3 つ目の兄弟。社内 MariaDB に
//! 到達できない実行形態 (GCP / Cloud Run) から同じ形の生イベントを読むための経路で、
//! `GET /api/dtako/events` (ippoan/rust-alc-api#578) を叩く。
//!
//! ## 上流は生 CSV 行を返す — 写すのはこちらの責務
//!
//! 上流は**畳まない**。イベント種別の分類・状態への写像・時刻のパースや TZ 変換・
//! 勤務境界の判定を一切せず、R2 の per-運行 `KUDGIVT.csv` を `{headers, rows}` の
//! まま返す (勤怠計算は一番星固有でマルチテナント基盤に置かないという #205 の決定 3)。
//! よって「生 CSV 行 → [`KintaiEventsApi`] の戻り値」の変換がこのモジュールの仕事で、
//! **`kosoku.rs` / `kosoku_paper.rs` には一切手を入れない**。
//!
//! ## MariaDB の `dtako_events` ブランチと 1 対 1 に写す
//!
//! `EVENTS_SQL` の 3 番目・4 番目の `UNION ALL` ブランチが写し先:
//!
//! | 戻り値のキー | MariaDB | KUDGIVT.csv の列 |
//! |---|---|---|
//! | `datetime` | `DATE_FORMAT(開始日時)` | `開始日時` (`YYYY/MM/DD` → `YYYY-MM-DD`) |
//! | `end_datetime` | `DATE_FORMAT(終了日時)` | `終了日時` (無い CSV は `null`) |
//! | `driver_id` | `対象乗務員CD` | **`対象乗務員CD` → 無ければ `乗務員CD1`** |
//! | `source` | `'dtako_events'` | 固定 |
//! | `state` | `イベント名` | `イベント名` |
//! | `unko_no` | `運行NO` | `運行NO` |
//! | `vehicle` | `dtako_cars` を JOIN した `車輌名` | `車輌名` (無い CSV は `null`) |
//!
//! **`対象乗務員CD` を優先し無ければ `乗務員CD1` にフォールバックする** のは、
//! `乗務員CD1` が運行の主運転者で全行同じ値になるため。2 名乗務の運行では副運転手の
//! 行まで主運転者に付き、引かれた側は丸ごと落ちる (上流の parser も
//! ippoan/rust-alc-api#580 で同じ規則に揃えたところ)。列の有無は**運行ごと**に見る —
//! `headers` がトップレベルに畳まれていないのは、`対象乗務員CD` のように一部の
//! ファイルにしか無い列が実在するから。
//!
//! ## MariaDB 実装との差異 (このモジュールが埋められないもの)
//!
//! 上流が返すのは KUDGIVT = MariaDB の `dtako_events` に相当する 1 種類だけ。
//! `EVENTS_SQL` が `UNION ALL` している残り 2 つには上流に口が無い:
//!
//! | source | 元テーブル | HTTP 経路 |
//! |---|---|---|
//! | `dtako_events` | `dtako_events` | **上流から取る** |
//! | `timecard` | `time_card_dstate` (始業 / 終業の打刻) | 口が無い → `fallback` へ委譲 |
//! | `dtako` | `time_card_dtako` (運行開始 / 終了 / 休息) | 口が無い → `fallback` へ委譲 |
//! | (フェリー) | `dtako_ferry_rows` | 口が無い → `fallback` へ委譲 |
//!
//! そこで `fallback` (通常は MariaDB 実装) を持ち、**上流に口が無いものだけ**そちらへ
//! 委譲する。オンプレでは両方揃うので HTTP 経路の出力を MariaDB と突き合わせて
//! 検証できる。
//!
//! MariaDB が無い実行形態 (GCP) の `fallback` は
//! [`crate::kintai_pg_repo::PgKintaiEventsRepo`] — 04b で `kintai.kintai_events` に
//! 入った打刻を読み返す (#205 の G6)。**かつてここが `None` になり
//! `shifts_from_timecard` が空になっていた穴がこれで閉じる。** どちらも無い形態は
//! 今も打刻が読めないので、起動時に warn を出して静かな欠損にしない
//! (`crate::server::build_kintai_events_repo`)。
//!
//! ## 期間の写し方
//!
//! trait の期間は `[from, to)` の `YYYY-MM-DD HH:MM:SS`、上流は**日付の閉区間**
//! `date_from..=date_to`。粒度が違うので上流には広めに投げ、**返ってきた行を
//! `EVENTS_SQL` と同じ 2 条件で絞り直す** (期間内に始まる区間 + 期間内に終わる区間)。
//!
//! - 上流の期間上限が単一乗務員 366 日 / 全乗務員 31 日なので、超える期間は
//!   [`date_chunks`] で分割して複数回叩く (`month_range` は 32〜34 日になるため
//!   全乗務員版では必ず分割が要る)
//! - 全乗務員版は `page_size` / `after_driver_cd` の keyset ページングを回しきる
//! - 同じ運行が複数の chunk / 複数の乗務員グループに現れるので `運行NO` で重複排除する。
//!   **CSV の中の重複行は落とさない** — 取り込みが 2 回走った重複は `kosoku.rs` 側が
//!   扱う話で、ここで消すと MariaDB 経路と値が割れる

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::NaiveDate;
use serde::de::DeserializeOwned;

use crate::config::KintaiEventsConfig;
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
// alc の応答の型・写し方・合流・期間の分割・読みの流れ・入力の欠けの検知・指紋は共有 crate に置く (Refs #322)。
use kintai_kosoku::alc_events::{
    collect, date_chunks, event_to_json, fetch_all_events, fetch_dtako_month_read,
    is_borrowed_source, merge_all_rows, parse_window, query_dates, sort_rows, upstream_warnings,
    AlcEventsError, AlcEventsGet, EventsPage, RawEvent, UpstreamAll, UpstreamEtags, UpstreamSingle,
    ETAGS_PATH, EVENTS_PATH, MAX_RANGE_DAYS_SINGLE,
};

// ── 上流 warnings・運行の突合 (Refs #205 の 06・32・37) ─────────────────────

// warnings は値で返す (tokio の task_local をやめた、Refs #322)。束ね方・封の判定・運行の突合は
// 共有 crate (`kintai_kosoku::fold_flow` / `kintai_kosoku::unko_diff`) にあり、勤怠 Worker と同じものを使う。
// 呼び出し側のパスを変えないよう再 export する。
pub use kintai_kosoku::fold_flow::{DtakoMonthRead, EtagsRead, UnsplitOperation, Warning};
pub use kintai_kosoku::unko_diff::{
    unko_diff_with_warnings, OnpremOperation, UnkoDiff, UnkoDiffAux, UnkoDiffDriver,
    UnkoDiffDriverSplit, UnkoDiffItem, UnkoKeyTrial, UnkoSample, UnkoShape, MAX_UNKO_DIFF,
    MAX_UNKO_DIFF_DRIVERS, MAX_UNKO_DIFF_SAMPLE,
};

// ── 認証 token の取り方 (設定で与える。コードに焼かない) ────────────────────

/// 上流に付ける Bearer token の供給元。
///
/// 当面は Google 認証 (`gcloud auth print-identity-token` → Cloud Run IAM)、07 で
/// device JWT に差し替える。**どちらもコードに焼かず設定から来る**ため、実装は
/// 「静的な値」「コマンドの出力」「無し」の 3 つだけを知っている。
#[async_trait]
pub trait KintaiTokenProvider: Send + Sync {
    /// `Authorization: Bearer` に載せる値。`None` ならヘッダーを付けない。
    async fn token(&self) -> Result<Option<String>, KintaiRepoError>;
}

/// token 無し (社内 LAN や、網層だけで守る構成)。
pub struct NoTokenProvider;

#[async_trait]
impl KintaiTokenProvider for NoTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        Ok(None)
    }
}

/// 設定に直接書かれた token (device JWT を注入する 07 の受け皿)。
pub struct StaticTokenProvider(pub String);

#[async_trait]
impl KintaiTokenProvider for StaticTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        Ok(Some(self.0.clone()))
    }
}

/// コマンドの標準出力を token として使う (`gcloud auth print-identity-token`)。
///
/// **シェルを経由しない。** 設定文字列を空白で argv に分割して直接 exec するので、
/// 設定がシェル注入の経路にならない。Google の identity token は 1 時間有効なので
/// TTL の間だけ使い回し、毎リクエストで `gcloud` を起こさない。
pub struct CommandTokenProvider {
    argv: Vec<String>,
    ttl: Duration,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl CommandTokenProvider {
    /// `command` を空白で分割して argv にする。空なら `Err`。
    pub fn new(command: &str, ttl_secs: u64) -> Result<Self, String> {
        let argv: Vec<String> = command.split_whitespace().map(str::to_string).collect();
        if argv.is_empty() {
            return Err("auth_token_command is empty".to_string());
        }
        Ok(Self {
            argv,
            ttl: Duration::from_secs(ttl_secs),
            cached: tokio::sync::Mutex::new(None),
        })
    }

    async fn run(&self) -> Result<String, KintaiRepoError> {
        let out = tokio::process::Command::new(&self.argv[0])
            .args(&self.argv[1..])
            .output()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("auth token command: {e}")))?;
        if !out.status.success() {
            let msg = String::from_utf8_lossy(&out.stderr).trim().to_string();
            return Err(KintaiRepoError::QueryFailed(format!(
                "auth token command failed: {} ({msg})",
                out.status
            )));
        }
        let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if token.is_empty() {
            return Err(KintaiRepoError::QueryFailed(
                "auth token command produced no output".to_string(),
            ));
        }
        Ok(token)
    }
}

#[async_trait]
impl KintaiTokenProvider for CommandTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        let mut cached = self.cached.lock().await;
        if let Some((token, at)) = cached.as_ref() {
            if at.elapsed() < self.ttl {
                return Ok(Some(token.clone()));
            }
        }
        let token = self.run().await?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(Some(token))
    }
}

/// GCE / Cloud Run の metadata server。**コンテナの中から** identity token を取る唯一の道。
///
/// `auth_token_command = "gcloud auth print-identity-token"` は**開発機から Cloud Run を
/// 叩く**ための経路で、Cloud Run の中では動かない — `Dockerfile` が入れているのは
/// `ca-certificates` だけで、`gcloud` も `curl` も存在しない。
pub const METADATA_BASE_URL: &str = "http://metadata.google.internal";

/// metadata server の identity token endpoint (audience を query で渡す)。
const METADATA_IDENTITY_PATH: &str =
    "/computeMetadata/v1/instance/service-accounts/default/identity";

/// metadata server が要求する固定ヘッダー。付けないと 403 になる
/// (ブラウザからの SSRF で token を抜かれないための Google 側の作り)。
const METADATA_FLAVOR: (&str, &str) = ("Metadata-Flavor", "Google");

/// Cloud Run IAM 用の identity token を metadata server から取る。
///
/// `audience` は**呼び先の Cloud Run service の URL**。Cloud Run IAM は
/// 「この token は自分宛か」を audience で見るので、ここがずれると 401 になる。
/// 既定では `base_url` をそのまま使う ([`build_token_provider`])。
///
/// token は 1 時間有効なので [`CommandTokenProvider`] と同じく TTL の間だけ使い回す。
#[derive(Debug)]
pub struct MetadataTokenProvider {
    client: reqwest::Client,
    /// `{base}{path}?audience=...`。**base をフィールドに持つ**のはテストが
    /// wiremock を差し込めるようにするため。
    url: String,
    ttl: Duration,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl MetadataTokenProvider {
    pub fn new(metadata_base: &str, audience: &str, ttl_secs: u64) -> Result<Self, String> {
        let audience = audience.trim().trim_end_matches('/');
        if audience.is_empty() {
            return Err("metadata token provider needs an audience (base_url)".to_string());
        }
        let client = reqwest::Client::builder()
            // metadata server は同一ホストなので、届かないなら待たずに落とす
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("metadata token client: {e}"))?;
        Ok(Self {
            client,
            url: format!(
                "{}{METADATA_IDENTITY_PATH}?audience={}",
                metadata_base.trim_end_matches('/'),
                urlencode(audience)
            ),
            ttl: Duration::from_secs(ttl_secs),
            cached: tokio::sync::Mutex::new(None),
        })
    }

    async fn fetch(&self) -> Result<String, KintaiRepoError> {
        let resp = self
            .client
            .get(&self.url)
            .header(METADATA_FLAVOR.0, METADATA_FLAVOR.1)
            .send()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("metadata identity: {e}")))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("metadata identity body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiRepoError::QueryFailed(format!(
                "metadata identity status {status}: {excerpt}"
            )));
        }
        let token = body.trim().to_string();
        if token.is_empty() {
            return Err(KintaiRepoError::QueryFailed(
                "metadata identity returned no token".to_string(),
            ));
        }
        Ok(token)
    }
}

/// query に載せる最小限の percent-encode。audience は URL なので `:` と `/` が出る。
///
/// `url` crate を足さないのは、encode したいのがこの 1 か所だけのため。
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[async_trait]
impl KintaiTokenProvider for MetadataTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        let mut cached = self.cached.lock().await;
        if let Some((token, at)) = cached.as_ref() {
            if at.elapsed() < self.ttl {
                return Ok(Some(token.clone()));
            }
        }
        let token = self.fetch().await?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(Some(token))
    }
}

/// 設定から供給元を決める。`auth_token` → `auth_token_command` → `auth_token_metadata`
/// の順に見て、どれも無ければ token 無し。
/// (2 つ以上の指定は `KintaiEventsConfig::validate` が起動時に弾く)
pub fn build_token_provider(
    cfg: &KintaiEventsConfig,
) -> Result<Arc<dyn KintaiTokenProvider>, String> {
    if !cfg.auth_token.is_empty() {
        return Ok(Arc::new(StaticTokenProvider(cfg.auth_token.clone())));
    }
    if !cfg.auth_token_command.trim().is_empty() {
        let p = CommandTokenProvider::new(&cfg.auth_token_command, cfg.auth_token_ttl_secs)?;
        return Ok(Arc::new(p));
    }
    if cfg.auth_token_metadata {
        let p =
            MetadataTokenProvider::new(METADATA_BASE_URL, &cfg.base_url, cfg.auth_token_ttl_secs)?;
        return Ok(Arc::new(p));
    }
    Ok(Arc::new(NoTokenProvider))
}

// ── alc の応答の型・生 CSV 行の写し方・合流・期間の分割・入力の欠けの検知・月ゲートの指紋 ───────────
//
// 共有 crate (`kintai_kosoku::alc_events`) に移した (Refs #322)。勤怠 Worker も同じものを使う (写さない)。
// ここに残るのは reqwest の往復 ([`AlcEventsGet`] の実装) と単一乗務員版の読み・`today_jst` だけ。

/// いまの日付 (JST)。窓の末尾の期待値を「進行中の月」で切り下げるためだけに使う。
///
/// `pub(crate)` なのは [`crate::routes::kintai`] の tail-gap-probe が同じ切り下げを使うため (実装を複製しない)。
pub(crate) fn today_jst() -> NaiveDate {
    let jst = chrono::FixedOffset::east_opt(crate::kintai_push::JST_OFFSET_SECONDS);
    chrono::Utc::now()
        .with_timezone(&jst.expect("JST offset is in range"))
        .date_naive()
}

/// 共有 crate の流れが期間・月・ページングで落ちたとき。文面は今までの `KintaiRepoError::QueryFailed` のまま。
impl From<AlcEventsError> for KintaiRepoError {
    fn from(e: AlcEventsError) -> Self {
        Self::QueryFailed(e.to_string())
    }
}

// ── repo 本体 ─────────────────────────────────────────────────────────────

/// `rust-alc-api` 経由の生イベント読み取り。
pub struct HttpKintaiEventsRepo {
    client: reqwest::Client,
    /// `{base_url}/api/dtako/events`。**const にせず struct フィールドに持つ** —
    /// テストが wiremock の URL を差し込めるようにするため。
    url: String,
    /// `{base_url}/api/dtako/events/etags`。同じ理由で struct フィールドに持つ。
    etags_url: String,
    tenant_id: String,
    token: Arc<dyn KintaiTokenProvider>,
    /// 上流に口が無い読み出し (打刻 / 運行の確定イベント / フェリー) の委譲先。
    /// 通常は MariaDB 実装。無い実行形態では該当分が欠ける (module docs 参照)。
    fallback: Option<DynKintaiEventsRepo>,
}

impl HttpKintaiEventsRepo {
    pub fn new(
        cfg: &KintaiEventsConfig,
        fallback: Option<DynKintaiEventsRepo>,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build()
            .map_err(|e| format!("kintai events http client: {e}"))?;
        let base = cfg.base_url.trim_end_matches('/');
        Ok(Self {
            client,
            url: format!("{base}{EVENTS_PATH}"),
            etags_url: format!("{base}{ETAGS_PATH}"),
            tenant_id: cfg.tenant_id.clone(),
            token: build_token_provider(cfg)?,
            fallback,
        })
    }

    /// 1 往復。`params` は query string にそのまま載る。
    async fn get<T: DeserializeOwned>(
        &self,
        params: &[(&str, String)],
    ) -> Result<T, KintaiRepoError> {
        let mut req = self
            .client
            .get(&self.url)
            .query(params)
            .header("X-Tenant-ID", &self.tenant_id);
        if let Some(token) = self.token.token().await? {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-events request: {e}")))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-events body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiRepoError::QueryFailed(format!(
                "alc dtako-events status {status}: {excerpt}"
            )));
        }
        serde_json::from_str(&body)
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-events parse: {e}")))
    }

    /// 単一乗務員。上流は運行に相乗りした 2 名分の行を返すので `driver` で絞る
    /// (MariaDB の `WHERE e.対象乗務員CD = :driver` と同じ)。
    async fn fetch_one(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<RawEvent>, KintaiRepoError> {
        let window = parse_window(from, to)?;
        let (qf, qt) = query_dates(window.0, window.1);
        let want = driver as i64;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for (cf, ct) in date_chunks(qf, qt, MAX_RANGE_DAYS_SINGLE) {
            let resp: UpstreamSingle = self
                .get(&[
                    ("driver_cd", driver.to_string()),
                    ("date_from", cf.to_string()),
                    ("date_to", ct.to_string()),
                ])
                .await?;
            // 単一乗務員の読みは畳みに使わないので、warnings は運用ログだけ
            upstream_warnings(&resp.warnings);
            collect(resp.operations, &mut seen, window, &mut out);
        }
        out.retain(|ev| ev.driver_id == Some(want));
        Ok(out)
    }
}

/// alc への往復 2 種 (共有 crate の [`fetch_all_events`] / [`fetch_dtako_month_read`] が回す)。
impl AlcEventsGet for HttpKintaiEventsRepo {
    type Error = KintaiRepoError;

    /// 全乗務員版の 1 ページ。404 を含めて非 2xx は全部 `Err` ([`Self::get`])。
    async fn events_page(&self, page: &EventsPage) -> Result<UpstreamAll, KintaiRepoError> {
        self.get(&page.params()).await
    }

    /// dtako 側の月ゲート材料を 1 往復で取る (Refs #205 実装計画 13)。
    ///
    /// **`Ok(None)` は「口が無い」の意味。** alc がまだ `GET /api/dtako/events/etags`
    /// を持たない環境 (404) では月ゲートそのものを諦める — 呼び出し側
    /// ([`crate::kintai_fold`]) が loud に warn した上で従来どおり全量読みへ degrade する。
    /// それ以外の失敗 (接続断・5xx・応答が壊れている) は `Err` にする — こちらは
    /// 「口はあるはずなのに読めなかった」なので、`None` に潰さず区別して伝える。
    async fn etags(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Option<UpstreamEtags>, KintaiRepoError> {
        let mut req = self
            .client
            .get(&self.etags_url)
            .query(&[
                ("date_from", date_from.to_string()),
                ("date_to", date_to.to_string()),
            ])
            .header("X-Tenant-ID", &self.tenant_id);
        if let Some(token) = self.token.token().await? {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-etags request: {e}")))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-etags body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiRepoError::QueryFailed(format!(
                "alc dtako-etags status {status}: {excerpt}"
            )));
        }
        let parsed: UpstreamEtags = serde_json::from_str(&body)
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-etags parse: {e}")))?;
        Ok(Some(parsed))
    }
}

#[async_trait]
impl KintaiEventsApi for HttpKintaiEventsRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let events = self.fetch_one(from, to, driver).await?;
        let mut rows: Vec<serde_json::Value> = events.into_iter().map(event_to_json).collect();
        if let Some(fb) = &self.fallback {
            let borrowed = fb.fetch_events_between(from, to, driver).await?;
            rows.extend(borrowed.into_iter().filter(is_borrowed_source));
        }
        sort_rows(&mut rows);
        Ok(rows)
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Ok(self.fetch_all_events_with_warnings(from, to).await?.0)
    }

    /// 上流の warnings も返す版 (fold の読み、Refs #322 — task_local をやめて値で返す)。
    async fn fetch_all_events_with_warnings(
        &self,
        from: &str,
        to: &str,
    ) -> Result<(Vec<serde_json::Value>, Vec<Warning>), KintaiRepoError> {
        let (events, warnings) = fetch_all_events(self, from, to).await?;
        let borrowed = match &self.fallback {
            Some(fb) => Some(fb.fetch_all_events_between(from, to).await?),
            None => None,
        };
        Ok((merge_all_rows(events, borrowed), warnings))
    }

    /// フェリー区間は上流に口が無い。突合はオンプレ専用 (#205 の決定 8) なので、
    /// `fallback` が無ければ `NotConfigured` = 503 で fail-closed にする。
    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        match &self.fallback {
            Some(fb) => fb.fetch_ferry_between(from, to, driver).await,
            None => Err(KintaiRepoError::NotConfigured),
        }
    }

    /// 休息のずれの突合もオンプレ専用 (Refs #205 の 41)。左辺の `time_card_dtako` は
    /// 上流に口が無く、`fallback` (MariaDB) からしか読めないため、フェリーと同じく
    /// `fallback` へ委譲し、無ければ `NotConfigured` = 503 で fail-closed にする。
    ///
    /// **`fallback` が `PgKintaiEventsRepo` (GCP) の形では読めない。** あちらの
    /// `kintai.kintai_events` は `休息` を 1 行も持たない
    /// ([`crate::kintai_push::NOT_CARRIED_STATES`]) ので、既定の `NotConfigured` が
    /// 返る = 「答えられない」と分かる形になる。
    async fn fetch_rest_events_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        match &self.fallback {
            Some(fb) => fb.fetch_rest_events_between(from, to, driver).await,
            None => Err(KintaiRepoError::NotConfigured),
        }
    }

    /// 運行の読取日もオンプレ専用 (Refs #205 の 42)。読み先の `dtako_rows` は
    /// 上流に口が無いので、フェリー・休息と同じく `fallback` へ委譲する。
    ///
    /// **alc の `dtako_operations` も `reading_date` を持つが、そちらは使わない。**
    /// 引き当てる相手 (勤務・運行) がオンプレの MariaDB からしか出ないため、
    /// 別システムの日付と突き合わせる形にしない ([`crate::kintai_reading_dates`] の docs)。
    async fn fetch_operation_reading_dates_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        match &self.fallback {
            Some(fb) => {
                fb.fetch_operation_reading_dates_between(from, to, driver)
                    .await
            }
            None => Err(KintaiRepoError::NotConfigured),
        }
    }

    /// 月初をまたぐ運行・勤務の始まり (Refs ohishi-exp/nuxt-dtako-admin#1123)。
    /// 材料は打刻と運行の確定イベントで、上流 (alc) に口が無い — フェリーと同じく
    /// `fallback` へ委譲する。**無ければ空** (= 遡らない、今までどおりの窓) —
    /// 読めないものを 503 にすると、打刻を持たない実行形態で fold ごと止まる。
    async fn fetch_month_head_anchors(
        &self,
        month_start: &str,
        to: &str,
    ) -> Result<std::collections::BTreeMap<u64, String>, KintaiRepoError> {
        match &self.fallback {
            Some(fb) => fb.fetch_month_head_anchors(month_start, to).await,
            None => Ok(std::collections::BTreeMap::new()),
        }
    }

    async fn fetch_dtako_month_digest(
        &self,
        month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        self.fetch_dtako_month_digest_since(month, None).await
    }

    /// 始端を `since` まで下げた月ゲートの dtako 指紋 (Refs
    /// ohishi-exp/nuxt-dtako-admin#1123)。fold が月初より前から読む月は、その分の
    /// 運行の CSV も封の材料に入れる。`None` (遡らない月) は今までと同じ範囲。
    ///
    /// 末尾検知の母集団 (`InputCoverage` の `window_start`) は**月初のまま** —
    /// 前月に始まった運行を数えないのはそちらの docs の決定で、ここでは動かさない。
    async fn fetch_dtako_month_digest_since(
        &self,
        month: &str,
        since: Option<NaiveDate>,
    ) -> Result<Option<String>, KintaiRepoError> {
        Ok(self
            .fetch_dtako_month_read_since(month, since)
            .await?
            .digest)
    }

    /// 月ゲートの dtako 指紋に、etags の控え (`unsplit`・運行一覧) と warnings を添えて返す
    /// (Refs #322 — task_local をやめて値で返す)。
    async fn fetch_dtako_month_read_since(
        &self,
        month: &str,
        since: Option<NaiveDate>,
    ) -> Result<DtakoMonthRead, KintaiRepoError> {
        fetch_dtako_month_read(self, month, since, today_jst()).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    // ── token provider ──

    #[tokio::test]
    async fn no_token_provider_returns_none() {
        assert_eq!(NoTokenProvider.token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn static_token_provider_returns_the_value() {
        let p = StaticTokenProvider("tok".to_string());
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn command_token_provider_runs_and_caches() {
        let p = CommandTokenProvider::new("echo id-token-value", 3600).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
        // 2 回目は TTL 内なのでキャッシュから返る
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
    }

    #[tokio::test]
    async fn command_token_provider_refetches_after_ttl() {
        let p = CommandTokenProvider::new("echo tok", 0).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn command_token_provider_is_loud_about_failures() {
        // 起動できないコマンド
        let err = CommandTokenProvider::new("/nonexistent/token-cmd", 60)
            .unwrap()
            .token()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("auth token command"), "{err}");

        // 非 0 終了
        let err = CommandTokenProvider::new("false", 60)
            .unwrap()
            .token()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("failed"), "{err}");

        // 何も出さない = token 無しで叩いて 401 になるより先に落とす
        let err = CommandTokenProvider::new("true", 60)
            .unwrap()
            .token()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no output"), "{err}");

        assert!(CommandTokenProvider::new("   ", 60).is_err());
    }

    #[test]
    fn build_token_provider_picks_the_configured_route() {
        let mut cfg = KintaiEventsConfig::default();
        assert!(build_token_provider(&cfg).is_ok());
        cfg.auth_token_command = "gcloud auth print-identity-token".to_string();
        assert!(build_token_provider(&cfg).is_ok());
        cfg.auth_token = "tok".to_string();
        assert!(build_token_provider(&cfg).is_ok());
    }

    // ── metadata server (Cloud Run の中で使える唯一の経路) ──

    #[test]
    fn urlencode_escapes_everything_outside_the_unreserved_set() {
        // audience は URL なので `:` と `/` が必ず出る
        assert_eq!(urlencode("https://x.run.app"), "https%3A%2F%2Fx.run.app");
        // RFC 3986 の unreserved はそのまま
        assert_eq!(urlencode("aZ0-_.~"), "aZ0-_.~");
    }

    #[test]
    fn metadata_provider_needs_an_audience() {
        // audience が無いと Cloud Run IAM が「自分宛か」を判定できず必ず 401 になる。
        // 起動時に落とす
        let err = MetadataTokenProvider::new(METADATA_BASE_URL, "   ", 60).unwrap_err();
        assert!(err.contains("audience"), "{err}");
        assert!(MetadataTokenProvider::new(METADATA_BASE_URL, "https://x", 60).is_ok());
    }

    #[test]
    fn metadata_base_url_is_the_documented_host() {
        assert_eq!(METADATA_BASE_URL, "http://metadata.google.internal");
    }

    #[tokio::test]
    async fn metadata_provider_sends_the_flavor_header_and_caches() {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(METADATA_IDENTITY_PATH))
            // audience は base_url そのまま (末尾 `/` は落とす)
            .and(query_param("audience", "https://alc.example"))
            // これが無いと metadata server は 403 を返す
            .and(header(METADATA_FLAVOR.0, METADATA_FLAVOR.1))
            .respond_with(ResponseTemplate::new(200).set_body_string("  id-token-value\n"))
            .expect(1)
            .mount(&server)
            .await;

        let p = MetadataTokenProvider::new(&server.uri(), "https://alc.example/", 3600).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
        // 2 回目は TTL 内なのでキャッシュから返る (expect(1) が保証)
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
    }

    #[tokio::test]
    async fn metadata_provider_refetches_after_ttl() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("tok"))
            .expect(2)
            .mount(&server)
            .await;

        let p = MetadataTokenProvider::new(&server.uri(), "https://alc.example", 0).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn metadata_provider_is_loud_about_failures() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // 403 = Metadata-Flavor 忘れ / metadata server が居ない環境
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;
        let p = MetadataTokenProvider::new(&server.uri(), "https://alc.example", 60).unwrap();
        let err = p.token().await.unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");

        // 200 だが空 — token 無しで叩いて 401 になるより先に落とす
        let empty = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("  \n"))
            .mount(&empty)
            .await;
        let p = MetadataTokenProvider::new(&empty.uri(), "https://alc.example", 60).unwrap();
        let err = p.token().await.unwrap_err();
        assert!(err.to_string().contains("no token"), "{err}");

        // 繋がらない先
        let p =
            MetadataTokenProvider::new("http://127.0.0.1:1", "https://alc.example", 60).unwrap();
        let err = p.token().await.unwrap_err();
        assert!(err.to_string().contains("metadata identity"), "{err}");
    }

    /// `today_jst` は UTC 深夜の前後で日付がずれない (JST 固定オフセット)。
    #[test]
    fn today_jst_is_a_plausible_date() {
        assert!(today_jst() >= d(2026, 1, 1), "2026 年以降のはず");
    }

    #[test]
    fn build_token_provider_uses_metadata_when_declared() {
        let mut cfg = KintaiEventsConfig {
            base_url: "https://alc.example".to_string(),
            auth_token_metadata: true,
            ..Default::default()
        };
        assert!(build_token_provider(&cfg).is_ok());

        // base_url が無いと audience が作れないので起動時に落ちる
        cfg.base_url = String::new();
        assert!(build_token_provider(&cfg).is_err());
    }
}
