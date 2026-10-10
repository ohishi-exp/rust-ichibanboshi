//! 勤怠 (タイムカード) 中継エンドポイント (Refs #99、ohishi-exp/nuxt-dtako-admin#424)。
//!
//! 社内 LAN の CakePHP (`yhonda-ohishi/nginx`) が持つタイムカードの日別データを、
//! Cloudflare Worker (nuxt-dtako-admin の dtako-scraper-relay) へ中継する。
//! CakePHP は LAN 内にしか居ないため、同一ホストで動く本サービスが橋渡しする
//! (`[cakephp] base_url` は既定で `http://127.0.0.1:120` の loopback)。
//!
//! **中継だけを行い、解釈も変換もしない。** 行は `serde_json::Value` のまま素通しする
//! ので、上流が項目を足しても本サービスの型を触る必要がない。ID の変換・突合も
//! 行わない — CakePHP の `drivers.id` は乗務員CD (= 一番星 `社員ﾏｽﾀ.社員C`) と
//! 同一番号体系なので、受け手がそのまま引き当てられる。
//!
//! ## 認可 — CF Access Service Token (edge)
//!
//! `/employees` (identity-only) と同じ扱いにしている。**前例のコピーではなく、
//! データの ACL で選んだ**:
//!
//! - 応答に含まれるのは識別情報 (社員番号・氏名・所属) と時刻だけで、**金額を含まない**
//! - 消費者は Cloudflare Worker の Durable Object であり、**ブラウザ JWT を持てない**。
//!   `/kyuyo/*` の in-service gate (auth-worker introspect + email allowlist) を要求すると
//!   worker から呼べなくなる
//!
//! 将来この endpoint に金額を足すことになったら、その時点で `/kyuyo/*` と同じ
//! in-service gate へ移すこと。

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::cakephp::{CakephpClient, CakephpError, TimecardDailyResponse};
use crate::kintai_fold::{month_anchors, read_window};
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiRepoError};
use crate::kintai_store::DynKintaiStore;
use crate::kosoku::KosokuParams;
use kintai_kosoku::kosoku_daily::{build_driver, for_each_driver, parse_view, ResponseView};

/// `?month=YYYY-MM&refresh=1`。`refresh=1` はキャッシュを飛ばして CakePHP から
/// 引き直す (Refs #106 Phase 2 — 当月の打刻は日々変わるため、relay の取り込みは
/// これを付ける)。
#[derive(Debug, Deserialize)]
pub struct DailyQuery {
    pub month: Option<String>,
    #[serde(default)]
    pub refresh: Option<String>,
}

/// `?month=YYYY-MM&driver=1051` (Refs #114)。`month` は必須。
///
/// `driver` は endpoint で扱いが違う — [`events`] は**必須** (生イベントは日別サマリより
/// 1 桁多く、全乗務員を返す用途が無い)、[`kosoku_daily`] は**省略可**で省略時は全乗務員
/// (Refs #125)。
#[derive(Debug, Deserialize)]
pub struct EventsQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
    /// `view=compare` で**突合に要る項目だけ** (Refs #157)、`view=timecard` で
    /// **画面のタイムカード表に要る項目だけ** (Refs #164) 返す。省略・未知の値は
    /// 従来どおり全項目。
    pub view: Option<String>,
}

/// 対象月の書式検証 (`YYYY-MM` で月は 01-12)。定義は共有 crate (`kintai_kosoku::window`、Refs #322)。
pub use kintai_kosoku::window::is_valid_month;

/// 乗務員CD のパース。**数字のみ**を受ける (空・非数字・負値・桁溢れは None)。
///
/// 乗務員CD = 一番星 `社員ﾏｽﾀ.社員C` と同一番号体系で、DB 側も整数列なので
/// ここで整数にしてから渡す — 文字列のままクエリに載せない。
pub fn parse_driver(driver: &str) -> Option<u64> {
    if driver.is_empty() || !driver.as_bytes().iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    driver.parse::<u64>().ok()
}

/// CakePHP のエラーを HTTP ステータスへ写す (uriage の `map_cakephp_err` と同方針)。
fn map_cakephp_err(e: CakephpError) -> (StatusCode, String) {
    match e {
        CakephpError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url が未設定".to_string(),
        ),
        CakephpError::RequestFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP fetch failed: {m}"),
        ),
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

/// 応答へ出どころメタを足す (素通し方針のため型は変えず extra に載せる)。
fn with_source_meta(
    mut resp: TimecardDailyResponse,
    source: &str,
    synced_at: &str,
) -> TimecardDailyResponse {
    resp.extra
        .insert("source".to_string(), serde_json::Value::from(source));
    resp.extra
        .insert("synced_at".to_string(), serde_json::Value::from(synced_at));
    resp
}

/// GET /api/kintai/daily?month=YYYY-MM — タイムカード日別データの中継。
///
/// Refs #106 Phase 2: read-through — derived store に月があれば CakePHP に触らず
/// 返す (`source:"cache"`)。miss / `refresh=1` は従来どおり CakePHP から取得し
/// write-through で保存する (`source:"live"`)。保存するのは**上流応答の verbatim
/// JSON** (メタ注入前) — 素通し方針を保存でも維持する。
pub async fn daily(
    Query(params): Query<DailyQuery>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Extension(store): Extension<DynKintaiStore>,
) -> Result<Json<TimecardDailyResponse>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let force_refresh = params.refresh.as_deref() == Some("1");
    if !force_refresh {
        match store.get_daily(&month).await {
            Ok(Some(cached)) => {
                match serde_json::from_str::<TimecardDailyResponse>(&cached.response_json) {
                    Ok(resp) => {
                        let rows = resp.rows.len();
                        tracing::info!(month = %month, rows, "kintai daily served from cache");
                        return Ok(Json(with_source_meta(resp, "cache", &cached.synced_at)));
                    }
                    Err(e) => {
                        // schema 版の上げ忘れ等 — live へフォールバック (読みを殺さない)
                        tracing::warn!("kintai store corrupt row — live fallback: {e}");
                    }
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::warn!("kintai store read failed — live fallback: {e}");
            }
        }
    }
    let resp = cakephp
        .fetch_timecard_daily(&month)
        .await
        .map_err(map_cakephp_err)?;
    // 件数は先に出しておく — `tracing::info!` の引数は購読者が居ないと評価されず、
    // マクロ内に到達しない region が残る (coverage_100 の対象なので実害がある)
    let rows = resp.rows.len();
    tracing::info!(month = %month, rows, "kintai daily relayed");
    let synced_at = chrono::Utc::now().to_rfc3339();
    // String キー + JSON 値しか持たない型なので serialize は失敗しない
    let json = serde_json::to_string(&resp).expect("TimecardDailyResponse serialize");
    if let Err(e) = store.put_daily(&month, &json, rows, &synced_at).await {
        // live 応答はそのまま返す — キャッシュ書き込み失敗で中継を殺さない
        tracing::warn!("kintai store write failed: {e}");
    }
    Ok(Json(with_source_meta(resp, "live", &synced_at)))
}

/// GET /api/kintai/pdf-json?month=YYYY-MM[&driver=1021] — タイムカード表 **PDF 相当**
/// の中継 (Refs #143、yhonda-ohishi/nginx#782、ohishi-exp/nuxt-dtako-admin#492)。
///
/// 用途は dtako-admin のタイムカード表と社内 CakePHP の PDF の **1 vs 1 突合**、
/// および MCP での全乗務員一括チェック。[`daily`] (打刻セッション) とはデータが違い、
/// 拘束 (`time_card_kosoku` の日別合計・type 別内訳)・休暇区分・月次集計欄を持つ。
///
/// **キャッシュを持たない** — 突合は「いま上流が何を出しているか」を見るのが目的で、
/// derived store を挟むと nginx 側の修正が反映されたかどうかが分からなくなる。
///
/// `driver` の扱いは [`kosoku_daily`] と揃える — **省略で全乗務員**、`driver=` (空) は
/// 省略ではなく不正として 400。
pub async fn pdf_json(
    Query(params): Query<EventsQuery>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(d),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "driver は乗務員CD (数字) で指定してください".to_string(),
                ))
            }
        },
    };
    let resp = cakephp
        .fetch_timecard_pdf_json(&month, driver)
        .await
        .map_err(map_cakephp_err)?;
    // 値は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let all = driver.is_none();
    tracing::info!(month = %month, all, "kintai pdf-json relayed");
    Ok(Json(resp))
}

/// 生イベント読み取りのエラーを HTTP ステータスへ写す。
///
/// 未設定は 503 (`base_url` 未設定と同じ fail-closed)、DB 停止・クエリ失敗は 502。
/// `/kintai/version` (routes/kintai_version.rs) も同じ写し方を共有する (Refs #184)。
pub(crate) fn map_repo_err(e: KintaiRepoError) -> (StatusCode, String) {
    match e {
        KintaiRepoError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "MariaDB 接続設定が未設定".to_string(),
        ),
        KintaiRepoError::QueryFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("MariaDB query failed: {m}"),
        ),
    }
}

/// GET /api/kintai/events?month=YYYY-MM&driver=1051 — 打刻と運行イベントの
/// **生の時系列** (Refs #114 / #116、拘束時間の打刻基準化 Phase 1)。
///
/// 拘束時間管理表の残業を打刻基準で計算し直すにあたり、規則を決める前に実データで
/// 各パターン (同日 2 運行・打刻と運行のズレ・細切れ休憩 …) が何件あるかを数える
/// ための読み出し口。**解釈しない** — 勤務の切れ目も休憩の閾値もここでは判断せず、
/// 生行を時刻順に並べて返すだけ。
///
/// データ源は社内 MariaDB の直読み (`kintai_repo`)。`daily` (CakePHP 中継 +
/// derived store) と違い**キャッシュを持たない** — 調査用途で頻度が低く、常に
/// 最新の打刻が要るため。
/// `EVENTS_SQL` / `ALL_EVENTS_SQL` 系 (kosoku 計算) の同時実行キャップ。
///
/// 殺到すると MariaDB (HDD + 128MB pool) を食い合って全員が数分待ちの convoy に
/// なる (2026-07-29 実害 — `MARIADB_SESSION_SETUP` の docs 参照)。4 は通常運用
/// (画面 1 つが当月+前月の 2 本) の余裕をみた値。超過分は待たされるだけで
/// 落ちない — `max_statement_time=60` と併せて、待ち行列は最長でも数分で捌ける。
static KOSOKU_DB_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(4);

pub async fn events(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    let _permit = KOSOKU_DB_PERMITS.acquire().await.expect("semaphore open");
    let driver = match parse_driver(params.driver.as_deref().unwrap_or_default()) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "driver は乗務員CD (数字) で指定してください".to_string(),
            ))
        }
    };
    let rows = repo
        .fetch_events(&month, driver)
        .await
        .map_err(map_repo_err)?;
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let count = rows.len();
    tracing::info!(month = %month, driver, rows = count, "kintai events read");
    Ok(Json(serde_json::json!({ "rows": rows })))
}

/// GET /api/kintai/rest-diff?month=YYYY-MM[&driver=1445] — **休息がずれている運行の
/// 一覧** (Refs #205 の 41)。
///
/// `/events` と同じ経路 (社内 MariaDB の直読み、`kintai_repo`) で、同じ運行の
/// `time_card_dtako` 由来の休息と `dtako_events` 由来の休息を突き合わせる。
/// 何を測っているか・なぜずれるかは [`crate::kintai_rest_diff`] のモジュール docs。
///
/// **`driver` は省略可** ([`kosoku_daily`] と同じ扱い)。1 回叩けば月ぶんの対象が
/// 全部出る形にしてある — 押す対象 (`yhonda-ohishi/nginx` の「勤務時間再登録」) を
/// 数えるのが用途なので、乗務員を先に知っている必要が無い。`driver=` (空) は
/// 省略ではなく**不正**として 400 にする (`kosoku_daily` と同じ)。
///
/// **判定には一切入らない。** 拘束も勤務も畳まず、突合の結果だけを返す。
pub async fn rest_diff(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    let Some((from, to)) = crate::kintai_repo::month_range(&month) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    };
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(d),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "driver は乗務員CD (数字) で指定してください".to_string(),
                ))
            }
        },
    };
    let _permit = KOSOKU_DB_PERMITS.acquire().await.expect("semaphore open");
    let rows = repo
        .fetch_rest_events_between(&from, &to, driver)
        .await
        .map_err(map_repo_err)?;
    let diff = crate::kintai_rest_diff::rest_diff(&rows, &from, &to);
    // マクロは 1 行に収める (CLAUDE.md)
    let (total, scanned) = (diff.total, diff.scanned_unko);
    tracing::info!(month = %month, total, scanned, "kintai rest-diff built");
    // 押す対象の数は必ず出す (0 でも黙らない)
    tracing::info!(mismatch = diff.mismatch_total, "kintai rest-diff to fix");
    Ok(Json(serde_json::json!({
        "month": month,
        "driver": driver,
        "from": from,
        "to": to,
        // **押す対象の数を `total` より先に置く** — 混ぜると必ず読み違える
        // ([`crate::kintai_rest_diff::RestDiffKind`] の docs、2026-06 の実測)
        "mismatch_total": diff.mismatch_total,
        "total_by_kind": diff.total_by_kind,
        "total": diff.total,
        "items": diff.items,
        "by_driver": diff.by_driver,
        "scanned_unko": diff.scanned_unko,
        "skipped_rows": diff.skipped_rows,
        "max_items": crate::kintai_rest_diff::MAX_REST_DIFF,
    })))
}

/// GET /api/kintai/reading-dates?month=YYYY-MM[&driver=1107] — **運行を読取日へ
/// 引き当てる** (Refs #205 の 42)。
///
/// 値のずれた勤務は該当の**読取日**を取り直せば直るが、読取日は運行の属性で、
/// 勤務の日とは一致しない (実測: 運行日 06-24 → 読取日 07-06)。この口が
/// 「どの日を取り直せばよいか」を 1 リクエストで返す。何を測っているかは
/// [`crate::kintai_reading_dates`] のモジュール docs。
///
/// **`driver` は省略可** ([`rest_diff`] / [`kosoku_daily`] と同じ)。`driver=` (空) は
/// 省略ではなく**不正**として 400 にする。
///
/// **判定には一切入らない。** 勤務も拘束も畳まない。
pub async fn reading_dates(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    let Some((from, to)) = crate::kintai_repo::month_range(&month) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    };
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(d),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "driver は乗務員CD (数字) で指定してください".to_string(),
                ))
            }
        },
    };
    let _permit = KOSOKU_DB_PERMITS.acquire().await.expect("semaphore open");
    let rows = repo
        .fetch_operation_reading_dates_between(&from, &to, driver)
        .await
        .map_err(map_repo_err)?;
    let mapped = crate::kintai_reading_dates::reading_dates(&rows);
    // マクロは 1 行に収める (CLAUDE.md)
    let (total, dates) = (mapped.total, mapped.by_reading_date.len());
    tracing::info!(month = %month, total, dates, "kintai reading-dates built");
    // 読取日が引けなかった運行は 0 でも出す (答えの取りこぼしが見えるように)
    tracing::info!(
        unknown = mapped.unknown_reading_date,
        "kintai reading-dates gap"
    );
    Ok(Json(serde_json::json!({
        "month": month,
        "driver": driver,
        "from": from,
        "to": to,
        // **取り直す日の一覧が答え。** 上限で切られない
        "by_reading_date": mapped.by_reading_date,
        "unknown_reading_date": mapped.unknown_reading_date,
        "total": mapped.total,
        "items": mapped.items,
        "skipped_rows": mapped.skipped_rows,
        "max_items": crate::kintai_reading_dates::MAX_READING_DATES,
    })))
}

/// GET /api/kintai/tail-gap-probe?month=YYYY-MM[&driver=1078] — **末尾検知 (tail gap)
/// が鳴らしている乗務員を名指しする** (Refs #205)。
///
/// 月ゲートの封の条件 ([`crate::kintai_http_repo::missing_input_warnings`] の
/// tail gap 側) が本物か・「その期間は働いていない」だけかを切り分けるための診断。
/// 何を測っているか・**alc の警告と同じ量ではないこと**は
/// [`crate::kintai_tail_gap_probe`] のモジュール docs。
///
/// `/events` と同じ経路 (社内 MariaDB の直読み) で全乗務員の生イベントを読み、
/// [`crate::kintai_tail_gap_probe::tail_gap_probe`] へそのまま渡す。
///
/// **`driver` は省略可** ([`rest_diff`] / [`reading_dates`] と同じ)。`driver=` (空) は
/// 省略ではなく**不正**として 400 にする。
///
/// **判定には一切入らない。** 月ゲートの閾値・封の条件・warning 文言は変えない。
pub async fn tail_gap_probe(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    let Some((from, to)) = crate::kintai_repo::exact_month_range(&month) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    };
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(d),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "driver は乗務員CD (数字) で指定してください".to_string(),
                ))
            }
        },
    };
    let _permit = KOSOKU_DB_PERMITS.acquire().await.expect("semaphore open");
    let rows = repo
        .fetch_all_events_between(&from, &to)
        .await
        .map_err(map_repo_err)?;
    // 窓の末尾 (= 月末) と「進行中の月は today - 1 日」の小さい方 (alc と同じ切り下げ)。
    // `to` は exact_month_range の翌月 1 日 00:00:00 なので、月末はその前日
    let month_end = chrono::NaiveDate::parse_from_str(&to[..10], "%Y-%m-%d").expect("to is a date")
        - chrono::Duration::days(1);
    let today = crate::kintai_http_repo::today_jst();
    let expected = month_end.min(today - chrono::Duration::days(1));
    let probe = crate::kintai_tail_gap_probe::tail_gap_probe(&rows, &month, expected, driver);
    // マクロは 1 行に収める (CLAUDE.md)
    let (pop, over) = (probe.population, probe.over_threshold_total);
    tracing::info!(month = %month, population = pop, over_threshold = over, "kintai tail-gap probe built");
    // 「働いていない」で説明が付かない候補の数は 0 でも必ず出す
    tracing::info!(
        unpunched = probe.over_threshold_unpunched_total,
        "kintai tail-gap probe real candidates"
    );
    Ok(Json(serde_json::json!({
        "month": probe.month,
        "driver": driver,
        "from": from,
        "to": to,
        "expected": probe.expected,
        "threshold_days": probe.threshold_days,
        "population": probe.population,
        "over_threshold_total": probe.over_threshold_total,
        "over_threshold_unpunched_total": probe.over_threshold_unpunched_total,
        "drivers": probe.drivers,
    })))
}

/// GET /api/kintai/kosoku-daily?month=YYYY-MM[&driver=1051] — **打刻基準の日別サマリ**
/// (Refs #118、拘束時間の打刻基準化 Phase 2)。
///
/// `/events` の生イベントを [`crate::kosoku`] の純粋ロジックで日別に畳んで返す。
/// **応答に金額は含めない** — 認可が `/events` と同じ CF Access Service Token
/// (edge) のままでよいのはそのため。金額を足すことになったら `/kyuyo/*` と同じ
/// in-service gate へ移すこと。
///
/// 勤務は**始業日**で当月に振り分ける。月初の勤務は前月末に始まった休息の終わりを
/// 始業とするが、その区間は `EVENTS_SQL` が「期間内に終わる区間」として拾う。
///
/// **窓の始端は月初をまたぐ運行・勤務の開始まで遡る** (Refs
/// ohishi-exp/nuxt-dtako-admin#1123)。前月に始業して当月に終わる勤務は、始業を
/// 知らないと休息の終わりを始業とする当月の勤務に化けるため。遡り方は fold
/// ([`crate::kintai_fold::read_window`]) と同じ — 画面と保存値を割らない。
/// 診断口 (`/events`・`rest-diff`・`reading-dates`) の窓は変えない。
///
/// ## `driver` を省略すると全乗務員 (Refs #125)
///
/// 画面 (nuxt-dtako-admin のタイムカード表) は全乗務員ぶんが要る。1 名ずつ叩くと
/// 96 名で約 3 秒かかるので、**省略時は 1 リクエストで全員返す** (実測 0.25 秒)。
/// 応答の形は指定の有無で変わる:
///
/// | 呼び方 | 応答 |
/// |---|---|
/// | `driver=1051` | `{month, driver, days}` — **既存の形を変えない** |
/// | 省略 | `{month, drivers: [{driver, days}]}` |
///
/// `driver=` (空) は省略ではなく**不正**として 400 にする — front が値を入れ忘れた
/// ときに、黙って 96 名ぶん (約 1 MB) を返してしまわないため。
pub async fn kosoku_daily(
    Query(params): Query<EventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params_cfg): Extension<Arc<KosokuParams>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    }
    // DB 読みと kosoku 計算全体をキャップ内で行う (convoy 防止)
    let _permit = KOSOKU_DB_PERMITS.acquire().await.expect("semaphore open");
    let view = parse_view(params.view.as_deref());
    let Some(raw_driver) = params.driver else {
        return kosoku_daily_all(&month, repo, &params_cfg, view).await;
    };
    let driver = match parse_driver(&raw_driver) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "driver は乗務員CD (数字) で指定してください".to_string(),
            ))
        }
    };
    // 窓は fold と同じく月初をまたぐ運行・勤務の開始まで遡らせる — 月初 0:00 から
    // 読むと前月始業の勤務の続きが当月始業の勤務に化ける (Refs
    // ohishi-exp/nuxt-dtako-admin#1123)。単一乗務員なので窓はその乗務員の起点から
    let anchors = month_anchors(&repo, &month).await.map_err(map_repo_err)?;
    let (from, to) = read_window(&month, &anchors, Some(driver)).map_err(map_repo_err)?;
    let rows = repo
        .fetch_events_between(&from, &to, driver)
        .await
        .map_err(map_repo_err)?;
    let ferry = ferry_or_empty(&repo, &month, Some(driver)).await;
    // 組み立ては共有 crate (勤怠 Worker と同じもの、Refs #322)
    let built = build_driver(rows, &ferry, &month, &params_cfg, view);
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let count = built.days.len();
    tracing::info!(month = %month, driver, days = count, "kintai kosoku-daily built");
    Ok(Json(built.into_single(&month, driver, view)))
}

/// 紙のタイムカード表がこの月に引いているフェリー控除の行 (Refs #146)。
///
/// 取れなくても日別サマリは返す (控除が 0 になるだけ) — 突合の付帯情報のために本体を落とさない。
async fn ferry_or_empty(
    repo: &DynKintaiEventsRepo,
    month: &str,
    driver: Option<u64>,
) -> Vec<serde_json::Value> {
    match repo.fetch_ferry(month, driver).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!("ferry fetch failed — ferry_minus stays 0: {e}");
            Vec::new()
        }
    }
}

/// `driver` 省略時 — 全乗務員ぶんを 1 リクエストで畳む (Refs #125)。
///
/// 窓は fold と同じ: 全員で 1 回読み、乗務員ごとの起点へ切り戻す (Refs
/// ohishi-exp/nuxt-dtako-admin#1123)。乗務員ごとの組み立て・乗務員CD=0 と
/// 勤務も打刻も無い乗務員の除外は共有 crate の `for_each_driver` (Refs #322)。
async fn kosoku_daily_all(
    month: &str,
    repo: DynKintaiEventsRepo,
    params_cfg: &KosokuParams,
    view: ResponseView,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let anchors = month_anchors(&repo, month).await.map_err(map_repo_err)?;
    let (from, to) = read_window(month, &anchors, None).map_err(map_repo_err)?;
    let rows = repo
        .fetch_all_events_between(&from, &to)
        .await
        .map_err(map_repo_err)?;
    // 全乗務員ぶんを 1 回で引いて乗務員ごとに分ける (Refs #146)
    let ferry = ferry_or_empty(&repo, month, None).await;
    let mut drivers = Vec::new();
    for_each_driver(rows, ferry, month, &anchors, params_cfg, view, |d| {
        drivers.push(d)
    });
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let count = drivers.len();
    tracing::info!(month = %month, drivers = count, "kintai kosoku-daily built for all drivers");
    Ok(Json(serde_json::json!({
        "month": month,
        "drivers": drivers,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_drivers() {
        assert_eq!(parse_driver("1051"), Some(1051));
        assert_eq!(parse_driver("0"), Some(0));
        assert_eq!(parse_driver("0012"), Some(12));
    }

    #[test]
    fn invalid_drivers() {
        assert_eq!(parse_driver(""), None);
        assert_eq!(parse_driver("10a1"), None);
        assert_eq!(parse_driver("１０５１"), None); // 全角
        assert_eq!(parse_driver("1051 "), None);
        assert_eq!(parse_driver("-1"), None);
        // u64 桁溢れ (書式は数字でもパースできない)
        assert_eq!(parse_driver("99999999999999999999999"), None);
    }

    #[test]
    fn repo_error_mapping() {
        let (s, m) = map_repo_err(KintaiRepoError::NotConfigured);
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert!(m.contains("未設定"));

        let (s, m) = map_repo_err(KintaiRepoError::QueryFailed("boom".into()));
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("boom"));
    }

    #[test]
    fn error_mapping() {
        let (s, m) = map_cakephp_err(CakephpError::NotConfigured);
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        assert!(m.contains("base_url"));

        let (s, m) = map_cakephp_err(CakephpError::RequestFailed("dns".into()));
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("dns"));

        let (s, m) = map_cakephp_err(CakephpError::StatusError {
            status: 500,
            body_excerpt: "boom".into(),
        });
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("500") && m.contains("boom"));

        let (s, m) = map_cakephp_err(CakephpError::JsonError("eof".into()));
        assert_eq!(s, StatusCode::BAD_GATEWAY);
        assert!(m.contains("eof"));
    }
}
