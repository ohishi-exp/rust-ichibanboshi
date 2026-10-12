//! 担当者別売上集計 (`/uriage-jyuchu-display/print` の Rust 移植、issue #762)。
//!
//! 元 PHP は `yhonda-ohishi/nginx`
//! `html/app/src/Controller/UriageJyuchuDisplayController::computePersonSum()`
//! (PR #764 で `templates/UriageJyuchuDisplay/print.php` 118-356 行から
//! 共有ヘルパへ抽出済み)。
//!
//! 純粋関数 (担当者振替の写経・日次/取引先別の集計・verify の差分)・raw ndjson と fingerprint・月の扱い・
//! 口の Query と応答の型は、売上 Worker と共有の `uriage-logic` (`workers/uriage/logic/`) が正本
//! (Refs #322)。ここは HTTP・SQLite・CakePHP の fetch・raw の disk 書き出しだけを持ち、移した名前は
//! `pub use` で同じ path に見せる。純粋部分のテストは `workers/uriage/logic/tests/`、HTTP は `tests/uriage_test.rs`。

use axum::extract::{Path, Query};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use uriage_logic::api::{annotate_r2_pending, default_cal};
pub use uriage_logic::api::{
    AdminDeleteQuery, AdminDeleteResponse, AdminRebuildResponse, DailyQuery, DailyResponse,
    PersonMonthlyTotalsQuery, PersonMonthlyTotalsResponse, PersonPartnerTotalsQuery,
    PersonPartnerTotalsResponse, R2PendingItem, R2PendingQuery, R2PendingResponse, RecalcJobResult,
    RecalcJobsQuery, RecalcJobsResponse, RecalcQuery, RecalcResponse, VerifyDebugResponse,
    VerifyDebugRow, VerifyHistoryQuery, VerifyHistoryResponse, VerifyQuery, VerifyResponse,
};
use uriage_logic::cakephp::office_lookup;
pub use uriage_logic::compute::{
    cal_total, compute_person_partner_sum_by_day, compute_person_sum, compute_person_sum_by_day,
    is_oth_yosha, is_yosha, CalTotal, PartnerAccum, PartnerKind, PersonAccum, PersonPartnerKey,
    PersonSumResult, RowDecision, UriageRow,
};
use uriage_logic::period::month_to_range;
use uriage_logic::raw::serialize_ndjson_and_fingerprint;
pub use uriage_logic::verify::VerifyDiff;
use uriage_logic::verify::{compute_verify_diff, parse_php_sum};

use crate::cakephp::{CakephpClient, CakephpError, OfficeMasters};
use crate::config::RawConfig;
use crate::repo::{DynRepo, RepoError};
use crate::sqlite::{DynLocalStore, LocalStoreError};

// ══════════════════════════════════════════════════════════════
// HTTP handler: POST /api/uriage/by-person
// ══════════════════════════════════════════════════════════════

/// `/api/uriage/by-person` のリクエストボディ。
///
/// マスタ (`persons` / `other`) は CakePHP 側 SoT のため呼び出し側で fetch して
/// body に積む。Rust はマスタを persist しない (検証段階の挙動の完全再現が目的)。
#[derive(Debug, Deserialize)]
pub struct ByPersonRequest {
    /// 運行年月日 下限 (YYYY-MM-DD、含む)
    pub from: String,
    /// 運行年月日 上限 (YYYY-MM-DD、含む)
    pub to: String,
    /// 受注部門コード (営業所配下の部門群)。PHP の
    /// `jyuchu_bumon_in_jyuchu_display[$display_id]` 相当
    pub bumon: Vec<String>,
    /// 入力担当C → 担当者名 (`UriageJyuchuDisplayPersons_name`)
    pub persons: HashMap<i32, String>,
    /// 稼動部門コード → 営業所名 (`UriageJyuchuDisplay_other`)
    pub other: HashMap<String, String>,
    /// PHP の `$cal`。truthy で別営業所も合算。省略時 true。
    #[serde(default = "default_cal")]
    pub cal: bool,
}

/// `/api/uriage/by-person` のレスポンス。
#[derive(Debug, Serialize)]
pub struct ByPersonResponse {
    pub source_table: String,
    pub from: String,
    pub to: String,
    pub bumon: Vec<String>,
    pub cal: bool,
    /// 担当者振替判定後の集計 (PHP `$sum` と 1:1)。
    pub sum: HashMap<String, PersonAccum>,
    /// 取得行数 (デバッグ用、`request.K` フィルタ後)。
    pub row_count: usize,
}

fn map_repo_err(e: RepoError) -> StatusCode {
    match &e {
        RepoError::PoolError => StatusCode::SERVICE_UNAVAILABLE,
        RepoError::QueryError(msg) => {
            tracing::error!("Query error: {msg}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// POST `/api/uriage/by-person`
///
/// PHP `/uriage-jyuchu-display/print-json` の Rust 等価 endpoint。検証段階で
/// 1 円単位 diff を取るために、PHP 側で fetch した persons/other をそのまま
/// body に積んで呼び出す。
pub async fn by_person(
    Extension(repo): Extension<DynRepo>,
    Json(req): Json<ByPersonRequest>,
) -> Result<Json<ByPersonResponse>, StatusCode> {
    // 入力検証
    if req.from.is_empty() || req.to.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if req.bumon.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    // PHP `UriageJyuchuDisplayPersons_id` = req.persons の keys (社員C 一覧)。
    // uriage_rows は sql_options ケースで `入力担当C IN persons`、
    // sql_from_other_with_bumon ケースで `入力担当C NOT IN persons` を使う。
    let persons_id_list: Vec<i32> = req.persons.keys().copied().collect();
    let rows = repo
        .uriage_rows(&req.from, &req.to, &req.bumon, &persons_id_list)
        .await
        .map_err(map_repo_err)?;
    let row_count = rows.len();

    let result = compute_person_sum(&rows, &req.persons, &req.other, req.cal);

    Ok(Json(ByPersonResponse {
        source_table: "運転日報明細 + 社員ﾏｽﾀ".to_string(),
        from: req.from,
        to: req.to,
        bumon: req.bumon,
        cal: req.cal,
        sum: result.sum,
        row_count,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: POST /api/uriage/recalc
//   CakePHP pull → editable_months チェック → raw NDJSON.gz 出力 → fingerprint 記録
// ══════════════════════════════════════════════════════════════

/// 宣言していない (503) と、壊れている (500) を分ける。
///
/// **`Disabled` を 500 にしない。** GCP (Cloud Run) の instance は SQL Server に
/// 到達できず売上を持てないので、この口は「今は落ちている」ではなく「この形態には
/// 無い」が正しい (Refs #205 の G4)。オンプレの既定は宣言済みなので変わらない。
fn map_local_store_err(e: LocalStoreError) -> StatusCode {
    if matches!(e, LocalStoreError::Disabled) {
        tracing::info!("local store disabled");
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    tracing::error!("local store error: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
}

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
            format!("CakePHP status {status}: {body_excerpt}"),
        ),
        CakephpError::JsonError(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP response parse failed: {m}"),
        ),
    }
}

/// NDJSON bytes を gzip 圧縮して指定パスに書き出す (親 dir auto-create)。
/// 圧縮とファイル名は売上 Worker と共有 (`uriage_logic::raw`。Worker は同じバイト列を R2 に置く)。
pub(crate) fn write_raw_ndjson_gz(
    raw_dir: &str,
    month: &str,
    eigyosho_id: i64,
    ndjson: &[u8],
) -> std::io::Result<String> {
    let dir = std::path::Path::new(raw_dir).join(month);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(uriage_logic::raw::raw_file_name(eigyosho_id));

    // tmp 経由 atomic rename (PathModified watcher 等への中途半端な書き込み防止)
    let tmp_path = path.with_extension("ndjson.gz.tmp");
    std::fs::write(&tmp_path, uriage_logic::raw::gzip(ndjson)?)?;
    std::fs::rename(&tmp_path, &path)?;
    Ok(path.to_string_lossy().into_owned())
}

/// 1 (month, eigyosho_id) 単位の recalc を実行する。
///
/// 戻り値の `RecalcJobResult.status` は `"computed"` / `"failed"` のいずれか。
/// failed の場合は SQLite recalc_jobs にも `status='failed' + last_error` で記録する。
#[allow(clippy::too_many_arguments)]
async fn recalc_one(
    repo: &DynRepo,
    store: &DynLocalStore,
    raw_dir: &str,
    month: &str,
    eigyosho_id: i64,
    masters: &OfficeMasters,
    from: &str,
    to: &str,
    calculated_at: &str,
) -> RecalcJobResult {
    let mut job = RecalcJobResult {
        month: month.to_string(),
        eigyosho_id,
        status: "computed".to_string(),
        ..Default::default()
    };

    // bumon が空 (= マスタ未設定) は skip しても compute は走らせない方が安全。
    // CakePHP /masters-json の bumon は PR #766 以降の仕様で空配列にはならないはずだが、
    // 未対応の旧 CakePHP が混ざった場合の防御。
    if masters.bumon.is_empty() {
        job.status = "failed".to_string();
        job.error = Some("masters.bumon is empty (CakePHP 旧仕様?)".to_string());
        let _ = store
            .record_recalc_failed(
                month,
                eigyosho_id,
                job.error.as_deref().unwrap_or(""),
                calculated_at,
            )
            .await;
        return job;
    }

    // persons map は SQL 述語 (sql_options の `入力担当C IN persons` / with_bumon の
    // `NOT IN persons`) と compute_person_sum の振替判定の両方で使う。
    let persons = masters.persons_as_int_map();
    let other = masters.other.clone();
    let persons_id_list: Vec<i32> = persons.keys().copied().collect();

    // SQL Server から運転日報明細を取得
    let rows = match repo
        .uriage_rows(from, to, &masters.bumon, &persons_id_list)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("sqlserver: {e:?}");
            job.status = "failed".to_string();
            job.error = Some(msg.clone());
            let _ = store
                .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
                .await;
            return job;
        }
    };
    job.row_count = rows.len();

    // raw fingerprint (cal は raw rows には影響しないので 1 回でよい)
    let (ndjson, fingerprint) = serialize_ndjson_and_fingerprint(&rows);

    // raw NDJSON.gz を disk に出す (失敗しても recalc 本体は続行、raw_path = None)
    let raw_path = match write_raw_ndjson_gz(raw_dir, month, eigyosho_id, &ndjson) {
        Ok(p) => {
            job.raw_path = Some(p.clone());
            Some(p)
        }
        Err(e) => {
            tracing::warn!(
                "raw NDJSON.gz 書き出しに失敗 (recalc は続行): month={month} eigyosho={eigyosho_id} err={e}"
            );
            None
        }
    };

    // cal=true / cal=false それぞれで日次集計を計算 (月次は VIEW なので不要)
    let daily_cal = compute_person_sum_by_day(&rows, &persons, &other, true);
    let daily_nocal = compute_person_sum_by_day(&rows, &persons, &other, false);

    // 日次集計 upsert (失敗は recalc 全体を failed にする)
    match store
        .upsert_person_daily(month, eigyosho_id, true, &daily_cal, calculated_at)
        .await
    {
        Ok(n) => job.daily_count_cal = n,
        Err(e) => {
            let msg = format!("sqlite upsert_daily(cal=true): {e}");
            job.status = "failed".to_string();
            job.error = Some(msg.clone());
            let _ = store
                .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
                .await;
            return job;
        }
    }
    match store
        .upsert_person_daily(month, eigyosho_id, false, &daily_nocal, calculated_at)
        .await
    {
        Ok(n) => job.daily_count_nocal = n,
        Err(e) => {
            let msg = format!("sqlite upsert_daily(cal=false): {e}");
            job.status = "failed".to_string();
            job.error = Some(msg.clone());
            let _ = store
                .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
                .await;
            return job;
        }
    }

    // 担当者×得意先/傭車先 内訳 (Refs 担当者ドリルダウン)。person daily と同じ rows/masters
    // から計算する (compute_person_sum の再実行コストは低い、cal=true/false の2回のみ)。
    let partner_daily_cal = compute_person_partner_sum_by_day(&rows, &persons, &other, true);
    let partner_daily_nocal = compute_person_partner_sum_by_day(&rows, &persons, &other, false);
    if let Err(e) = store
        .upsert_person_partner_daily(month, eigyosho_id, true, &partner_daily_cal, calculated_at)
        .await
    {
        let msg = format!("sqlite upsert_partner_daily(cal=true): {e}");
        job.status = "failed".to_string();
        job.error = Some(msg.clone());
        let _ = store
            .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
            .await;
        return job;
    }
    if let Err(e) = store
        .upsert_person_partner_daily(
            month,
            eigyosho_id,
            false,
            &partner_daily_nocal,
            calculated_at,
        )
        .await
    {
        let msg = format!("sqlite upsert_partner_daily(cal=false): {e}");
        job.status = "failed".to_string();
        job.error = Some(msg.clone());
        let _ = store
            .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
            .await;
        return job;
    }

    // recalc_jobs に記録 (fingerprint + raw_path + status='computed')
    if let Err(e) = store
        .record_recalc_computed(
            month,
            eigyosho_id,
            &fingerprint,
            raw_path.as_deref(),
            calculated_at,
        )
        .await
    {
        // 記録だけ落ちた場合は status は computed のまま (集計は成功している) だが
        // error フィールドで知らせる
        job.error = Some(format!("record_recalc_computed: {e}"));
    }
    job.fingerprint = Some(fingerprint);
    job
}

/// POST `/api/uriage/recalc?month=YYYY-MM&eigyosho_id=N`
///
/// CakePHP から masters + editable_months を pull し、対象 (month, eigyosho_id) について
/// SQL Server から運転日報明細を取得 → compute_person_sum (cal={true,false}) → SQLite
/// upsert + raw NDJSON.gz 出力 + recalc_jobs に fingerprint 記録、を行う。
///
/// **body は受け取らない** (persons/other/bumon は CakePHP から pull)。
///
/// - `month` 未指定: editable_months 全部 (CakePHP の `editable_months` 配列) を処理
/// - `month` 指定: 任意の過去月を受け付ける (SELECT は無制限。経理の `入力可能月数`
///   とは無関係、log だけ残す)
/// - `eigyosho_id` 指定なし: その月の全営業所 (masters.offices) を処理
///
/// 設計判断 (user, 2026-06-30): **入力可能月 ≠ 取得可能月**。CakePHP `基本事項.入力可能月数`
/// は経理 UI の編集制限で、rust 側の再集計 (= 純粋な SELECT) には関係ない。チャート伸長
/// などで過去月の集計を取り直す要件が当然ありうるため、editable_months 422 gate は撤廃
/// (Refs yhonda-ohishi/nginx#762)。`editable_months` はログ用に残す (= CakePHP 側の編集
/// 状態は引き続き参考情報になる)。
pub async fn recalc(
    Extension(repo): Extension<DynRepo>,
    Extension(store): Extension<DynLocalStore>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Extension(raw_config): Extension<Arc<RawConfig>>,
    Query(q): Query<RecalcQuery>,
) -> Result<Json<RecalcResponse>, (StatusCode, String)> {
    // CakePHP 未配線なら 503 (= base_url 空)
    if !cakephp.is_enabled() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url 未設定: /recalc は無効".to_string(),
        ));
    }

    // editable_months を取得 (参考情報として log 出力。recalc 自体は gate しない)
    let editable = cakephp
        .fetch_editable_months()
        .await
        .map_err(map_cakephp_err)?;

    // 処理対象月を決める
    let target_months: Vec<String> = match &q.month {
        Some(m) => {
            if !editable.editable_months.iter().any(|em| em == m) {
                tracing::info!(
                    month = %m,
                    editable_months = ?editable.editable_months,
                    "month は CakePHP 編集可能月の外だが、recalc は実行 (入力可能月 ≠ 取得可能月)"
                );
            }
            vec![m.clone()]
        }
        None => editable.editable_months.clone(),
    };

    let calculated_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut jobs: Vec<RecalcJobResult> = Vec::new();

    for month in &target_months {
        let (from, to, masters_date) = match month_to_range(month) {
            Some(t) => t,
            None => {
                jobs.push(RecalcJobResult {
                    month: month.clone(),
                    status: "failed".to_string(),
                    error: Some(format!("month の形式が不正: {month}")),
                    ..Default::default()
                });
                continue;
            }
        };

        // masters を「月末日」基準で取得 (人事異動等は月跨ぎでは想定外)
        let masters = match cakephp.fetch_masters(&masters_date).await {
            Ok(m) => m,
            Err(e) => {
                jobs.push(RecalcJobResult {
                    month: month.clone(),
                    status: "failed".to_string(),
                    error: Some(format!("CakePHP masters fetch: {e}")),
                    ..Default::default()
                });
                continue;
            }
        };

        let targets: Vec<(i64, &OfficeMasters)> = if let Some(eid) = q.eigyosho_id {
            // 単一営業所指定: masters に居なければ skip
            match office_lookup(&masters, eid) {
                Some(o) => vec![(eid, o)],
                None => {
                    jobs.push(RecalcJobResult {
                        month: month.clone(),
                        eigyosho_id: eid,
                        status: "skipped".to_string(),
                        error: Some(format!("eigyosho_id={eid} が masters に存在しない")),
                        ..Default::default()
                    });
                    continue;
                }
            }
        } else {
            // 全営業所: masters.offices の全エントリ
            let mut v: Vec<(i64, &OfficeMasters)> = masters
                .offices
                .iter()
                .filter_map(|(k, v)| k.parse::<i64>().ok().map(|i| (i, v)))
                .collect();
            // 安定順 (id 昇順) で処理
            v.sort_by_key(|(id, _)| *id);
            v
        };

        for (eid, om) in targets {
            let job = recalc_one(
                &repo,
                &store,
                &raw_config.dir,
                month,
                eid,
                om,
                &from,
                &to,
                &calculated_at,
            )
            .await;
            jobs.push(job);
        }
    }

    Ok(Json(RecalcResponse {
        source_table: "運転日報明細 + CakePHP masters → uriage_person_monthly + recalc_jobs"
            .to_string(),
        months: target_months,
        editable_months_count: editable.editable_months_count,
        calculated_at,
        jobs,
    }))
}

// ══════════════════════════════════════════════════════════════
// R2 sync endpoints
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/r2/pending?from=YYYY-MM&to=YYYY-MM`
///
/// `r2_pending` view を返す (fingerprint 変化があったが R2 未送信の (month, eigyosho_id))。
/// 2026-06-30 以降は **verify_jobs 由来の gate** を同梱:
/// - `expected_count` = month の日数 × 2 cal
/// - `ready` = `verified_count >= expected_count` AND `verified_ng == 0`
/// - `blocker` = `"ng_present"` (NG あり) | `"unverified"` (cells 不足) | `None`
///
/// `from` / `to` (YYYY-MM、両端 inclusive) が **両方** 指定された時のみ期間で filter する。
/// 片方のみ / どちらも未指定は全件 (= cron / backward compat)。UI 操作で
/// 「期間に従う」場合は両方付ける (Refs #762、user 2026-06-30: 「R2 同期も recalc も
/// 期間に従う」)。
pub async fn r2_pending(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<R2PendingQuery>,
) -> Result<Json<R2PendingResponse>, StatusCode> {
    let rows = store
        .list_r2_pending(q.from.as_deref(), q.to.as_deref())
        .await
        .map_err(map_local_store_err)?;
    let items: Vec<R2PendingItem> = rows.into_iter().map(annotate_r2_pending).collect();
    Ok(Json(R2PendingResponse {
        count: items.len(),
        items,
    }))
}

/// GET `/api/uriage/raw/:month/:eigyosho_id`
///
/// recalc が disk に書き出した raw NDJSON.gz の bytes を返す (`Content-Type: application/gzip`)。
/// path は SQLite `recalc_jobs.raw_path` から引く (URL 直接 path traversal 不可)。
pub async fn raw_get(
    Extension(store): Extension<DynLocalStore>,
    Path((month, eigyosho_id)): Path<(String, i64)>,
) -> Result<Response, StatusCode> {
    let job = store
        .get_recalc_job(&month, eigyosho_id)
        .await
        .map_err(map_local_store_err)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let path = job.raw_path.ok_or(StatusCode::NOT_FOUND)?;

    let bytes = tokio::fs::read(&path).await.map_err(|e| {
        tracing::error!("raw read failed: path={path} err={e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut resp = bytes.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/gzip"),
    );
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_str(&format!(
            "attachment; filename=\"{}-eigyosho-{}.ndjson.gz\"",
            month, eigyosho_id
        ))
        .unwrap_or_else(|_| header::HeaderValue::from_static("attachment")),
    );
    Ok(resp)
}

/// POST `/api/uriage/raw/:month/:eigyosho_id/ack`
///
/// R2 への送信完了を記録する (`recalc_jobs.status='r2_synced' + r2_synced_at=now`)。
/// 該当 job が computed 状態に無い場合は 404 (ack 対象が無い)。
pub async fn raw_ack(
    Extension(store): Extension<DynLocalStore>,
    Path((month, eigyosho_id)): Path<(String, i64)>,
) -> Result<Json<RawAckResponse>, StatusCode> {
    let synced_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let n = store
        .record_r2_synced(&month, eigyosho_id, &synced_at)
        .await
        .map_err(map_local_store_err)?;
    if n == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(RawAckResponse {
        month,
        eigyosho_id,
        synced_at,
    }))
}

#[derive(Debug, Serialize)]
pub struct RawAckResponse {
    pub month: String,
    pub eigyosho_id: i64,
    pub synced_at: String,
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/daily?month=YYYY-MM&eigyosho_id=N&cal=true|false
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/daily?month=YYYY-MM&eigyosho_id=N&cal=true`
///
/// `(month, eigyosho_id, cal)` の **日次集計** を SQLite から読んで返す。
/// recalc 後でなければ空配列が返る。drill-down 表示用。
pub async fn daily(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<DailyQuery>,
) -> Result<Json<DailyResponse>, StatusCode> {
    if q.month.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rows = store
        .get_person_daily(&q.month, q.eigyosho_id, q.cal)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(DailyResponse {
        month: q.month,
        eigyosho_id: q.eigyosho_id,
        cal: q.cal,
        rows,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/person-monthly-totals?from=YYYY-MM&to=YYYY-MM&cal=true
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/person-monthly-totals?from=YYYY-MM&to=YYYY-MM&cal=true`
///
/// 期間内の **月 × 担当者 の SUM** を返す (全営業所合算)。
/// 担当者順位推移チャート (bump chart) 用。
pub async fn person_monthly_totals(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<PersonMonthlyTotalsQuery>,
) -> Result<Json<PersonMonthlyTotalsResponse>, StatusCode> {
    if q.from.is_empty() || q.to.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rows = store
        .person_monthly_totals(&q.from, &q.to, q.cal)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(PersonMonthlyTotalsResponse {
        from: q.from,
        to: q.to,
        cal: q.cal,
        rows,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/person-partner-totals
//   担当者ドリルダウン (得意先別・傭車先別 内訳)
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/person-partner-totals?person=<name>&from=YYYY-MM&to=YYYY-MM&cal=true`
///
/// 指定担当者の期間内 得意先別・傭車先別 内訳を返す (全営業所合算)。
/// 「担当者 売上構成順位」テーブルからのドリルダウン用データソース。
pub async fn person_partner_totals(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<PersonPartnerTotalsQuery>,
) -> Result<Json<PersonPartnerTotalsResponse>, StatusCode> {
    if q.person.is_empty() || q.from.is_empty() || q.to.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let customers = store
        .person_partner_totals(&q.person, &q.from, &q.to, q.cal, PartnerKind::Customer)
        .await
        .map_err(map_local_store_err)?;
    let subcontractors = store
        .person_partner_totals(&q.person, &q.from, &q.to, q.cal, PartnerKind::Subcontractor)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(PersonPartnerTotalsResponse {
        person: q.person,
        from: q.from,
        to: q.to,
        cal: q.cal,
        customers,
        subcontractors,
    }))
}

// ══════════════════════════════════════════════════════════════
// Admin: 削除 / 再作成
// ══════════════════════════════════════════════════════════════

/// POST `/api/uriage/admin/delete?month=YYYY-MM&eigyosho_id=N`
///
/// 指定 `(month, eigyosho_id)` の集計を SQLite 上の全 cal で削除し、`recalc_jobs`
/// の該当行も消す (一部リセット用)。月次集計は日次の VIEW なので自動で連動消滅。
/// 再度 `/recalc` を叩けば fingerprint なしで fresh に再計算 → R2 にも再 sync
/// 対象として並ぶ。
pub async fn admin_delete(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<AdminDeleteQuery>,
) -> Result<Json<AdminDeleteResponse>, StatusCode> {
    if q.month.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let r = store
        .delete_bucket(&q.month, q.eigyosho_id)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(AdminDeleteResponse {
        month: q.month,
        eigyosho_id: q.eigyosho_id,
        daily_deleted: r.daily_deleted,
        jobs_deleted: r.jobs_deleted,
    }))
}

/// POST `/api/uriage/admin/rebuild`
///
/// SQLite の全 uriage table (monthly / daily / recalc_jobs / r2_pending view) を
/// DROP → 再 migrate で作り直す **フルリセット**。集計データは全て消える。
/// schema 不整合や fingerprint の全更新が必要な時の最終手段。
pub async fn admin_rebuild(
    Extension(store): Extension<DynLocalStore>,
) -> Result<Json<AdminRebuildResponse>, StatusCode> {
    store.rebuild_schema().await.map_err(map_local_store_err)?;
    Ok(Json(AdminRebuildResponse {
        rebuilt_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/verify
//   PHP print-json vs Rust by-person を単日 × 営業所 × cal で 1:1 diff
//   (Phase 1 検証、shell の verify_uriage_diff_php_vs_rust.sh を rust 内製化)
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/verify?id=N&date=YYYY-MM-DD&cal=true|false`
///
/// 内部で:
/// 1. CakePHP `/masters-json?date=...` を pull (`persons`/`other`/`bumon`)
/// 2. CakePHP `/print-json?id=N&date=...[&cal=cal]` を pull (PHP `$sum`)
/// 3. SQL Server `uriage_rows(date, date, bumon)` で行取得 → `compute_person_sum`
/// 4. PHP `$sum` と Rust `$sum` を 0 entry を除外して 1:1 diff
/// 5. 結果を `verify_jobs` table に UPSERT (PK: unko_date, eigyosho_id, cal、Refs #762)
///
/// shell の `verify_uriage_diff_php_vs_rust.sh` を「1 (date, office, cal) 単位」で
/// HTTP endpoint 化したもの。nuxt UI が並列 iterate する想定。
pub async fn verify(
    Extension(repo): Extension<DynRepo>,
    Extension(store): Extension<DynLocalStore>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Query(q): Query<VerifyQuery>,
) -> Result<Json<VerifyResponse>, (StatusCode, String)> {
    let started = std::time::Instant::now();

    if !cakephp.is_enabled() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url 未設定: /verify は無効".to_string(),
        ));
    }
    if q.date.len() != 10 || !q.date.as_bytes().iter().all(|b| b.is_ascii()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("date 形式不正 (YYYY-MM-DD): {}", q.date),
        ));
    }

    // 1. CakePHP masters (date= 検証対象日)
    let masters = cakephp
        .fetch_masters(&q.date)
        .await
        .map_err(map_cakephp_err)?;
    let office = office_lookup(&masters, q.id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("office_id={} は masters に無い", q.id),
        )
    })?;
    let persons = office.persons_as_int_map();
    let other = office.other.clone();
    let bumon = office.bumon.clone();
    if bumon.is_empty() {
        // shell の `[[ "$bumon" == "[]" ]] && continue` と同じ扱い。受注部門が
        // 未登録の営業所は構造上 検証対象が無いだけで「失敗」ではないので、
        // 200 + skipped_reason フラグで返す (caller は status=skipped に振る)。
        // verify_jobs にも skipped_reason='no_bumon', ok=1 で 1 行記録する
        // (これがないと r2_pending coverage が永遠に埋まらない bucket になる)。
        let elapsed_ms = started.elapsed().as_millis() as u64;
        let ran_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
        let _ = store
            .upsert_verify_job(
                &q.date,
                q.id,
                q.cal,
                true,
                Some("no_bumon"),
                None,
                0,
                elapsed_ms as i64,
                &ran_at,
            )
            .await; // upsert 失敗は致命的でないので log のみで握りつぶす
        return Ok(Json(VerifyResponse {
            office_id: q.id,
            date: q.date,
            cal: q.cal,
            php_sum: HashMap::new(),
            rust_sum: HashMap::new(),
            diff: None,
            ok: true,
            row_count: 0,
            elapsed_ms,
            skipped_reason: Some("no_bumon".to_string()),
        }));
    }

    // 2. CakePHP print-json (PHP $sum)
    let php_resp = cakephp
        .fetch_print_json(q.id, &q.date, q.cal)
        .await
        .map_err(map_cakephp_err)?;
    let php_sum = parse_php_sum(&php_resp.sum).map_err(|m| (StatusCode::BAD_GATEWAY, m))?;

    // 3. Rust SELECT + compute_person_sum (from=to=同一日 の単日)
    // persons の keys (社員C 一覧) を SQL の `入力担当C IN/NOT IN` 述語に渡す
    let persons_id_list: Vec<i32> = persons.keys().copied().collect();
    let rows = repo
        .uriage_rows(&q.date, &q.date, &bumon, &persons_id_list)
        .await
        .map_err(|e| {
            let st = map_repo_err(e);
            (
                st,
                format!("SQL Server fetch failed (status={})", st.as_u16()),
            )
        })?;
    let row_count = rows.len();
    let rust_sum = compute_person_sum(&rows, &persons, &other, q.cal).sum;

    // 4. shell の jq logic 相当: 0 のみ entry を除外して 1:1 diff
    let diff = compute_verify_diff(&php_sum, &rust_sum);
    let ok = diff.is_none();
    let elapsed_ms = started.elapsed().as_millis() as u64;

    // 5. verify_jobs に upsert (Refs #762 R2 sync gate)
    let ran_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let diff_json = diff.as_ref().and_then(|d| serde_json::to_string(d).ok());
    let _ = store
        .upsert_verify_job(
            &q.date,
            q.id,
            q.cal,
            ok,
            None,
            diff_json.as_deref(),
            row_count as i64,
            elapsed_ms as i64,
            &ran_at,
        )
        .await; // upsert 失敗は致命的でない (verify 結果自体は caller に返す)

    Ok(Json(VerifyResponse {
        office_id: q.id,
        date: q.date,
        cal: q.cal,
        php_sum,
        rust_sum,
        diff,
        ok,
        row_count,
        elapsed_ms,
        skipped_reason: None,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/verify-debug
//   verify と同じ計算をして raw rows + decisions を返す (NG 原因特定用、
//   user 2026-06-30 「office しぼれてない」)
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/verify-debug?id=N&date=YYYY-MM-DD&cal=true|false`
///
/// verify と同じパイプラインを実行するが、raw rows と row 単位 decision、bumon list、
/// persons / other map をすべて response に同梱して返す。NG 行の原因特定用。
///
/// verify_jobs テーブルへの upsert は行わない (= 履歴汚染しない)。
pub async fn verify_debug(
    Extension(repo): Extension<DynRepo>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Query(q): Query<VerifyQuery>,
) -> Result<Json<VerifyDebugResponse>, (StatusCode, String)> {
    if !cakephp.is_enabled() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url 未設定: /verify-debug は無効".to_string(),
        ));
    }
    if q.date.len() != 10 || !q.date.as_bytes().iter().all(|b| b.is_ascii()) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("date 形式不正 (YYYY-MM-DD): {}", q.date),
        ));
    }

    // 1. CakePHP masters
    let masters = cakephp
        .fetch_masters(&q.date)
        .await
        .map_err(map_cakephp_err)?;
    let office = office_lookup(&masters, q.id).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            format!("office_id={} は masters に無い", q.id),
        )
    })?;
    let persons = office.persons_as_int_map();
    let other = office.other.clone();
    let bumon = office.bumon.clone();

    // 2. PHP $sum
    let php_resp = cakephp
        .fetch_print_json(q.id, &q.date, q.cal)
        .await
        .map_err(map_cakephp_err)?;
    let php_sum = parse_php_sum(&php_resp.sum).map_err(|m| (StatusCode::BAD_GATEWAY, m))?;

    // 3. Rust SELECT + compute_person_sum
    let persons_id_list: Vec<i32> = persons.keys().copied().collect();
    let rows = repo
        .uriage_rows(&q.date, &q.date, &bumon, &persons_id_list)
        .await
        .map_err(|e| {
            let st = map_repo_err(e);
            (
                st,
                format!("SQL Server fetch failed (status={})", st.as_u16()),
            )
        })?;
    let result = compute_person_sum(&rows, &persons, &other, q.cal);
    let rust_sum = result.sum.clone();

    // 4. diff
    let diff = compute_verify_diff(&php_sum, &rust_sum);

    // 5. raw rows + decisions を 1:1 で zip
    let debug_rows: Vec<VerifyDebugRow> = rows
        .iter()
        .zip(result.rows.iter())
        .map(|(r, d)| VerifyDebugRow::new(r, d))
        .collect();

    Ok(Json(VerifyDebugResponse {
        office_id: q.id,
        date: q.date,
        cal: q.cal,
        bumon,
        persons,
        other,
        rows: debug_rows,
        php_sum,
        rust_sum,
        diff,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/recalc-jobs
//   recalc_jobs を月範囲で読む (UI 状態サマリ用、Refs #762)
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/recalc-jobs?from=YYYY-MM&to=YYYY-MM`
///
/// `recalc_jobs` を読む (UI 状態サマリ用)。`from` / `to` が両方指定された時のみ
/// 期間 filter する。**両方未指定 = 全件取得** (user 2026-06-30: 状態サマリは
/// 全期間表示)。並び順は `month DESC, eigyosho_id ASC` (直近月が上)。
pub async fn list_recalc_jobs(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<RecalcJobsQuery>,
) -> Result<Json<RecalcJobsResponse>, (StatusCode, String)> {
    let from = q.from.as_deref().filter(|s| !s.is_empty());
    let to = q.to.as_deref().filter(|s| !s.is_empty());
    let jobs = store.list_recalc_jobs(from, to).await.map_err(|e| {
        tracing::error!("list_recalc_jobs failed: {e}");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("list_recalc_jobs query failed: {e}"),
        )
    })?;
    Ok(Json(RecalcJobsResponse {
        from: q.from,
        to: q.to,
        count: jobs.len(),
        jobs,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/verify-history
//   verify_jobs を範囲指定で読む (履歴一覧、Refs #762)
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/verify-history?from=YYYY-MM-DD&to=YYYY-MM-DD&eigyosho_id=N`
///
/// `verify_jobs` を範囲指定で読む (履歴一覧)。caller は status コード (緑/赤/灰) や
/// 「過去 N 週間 連続 green」判定に使う。
pub async fn verify_history(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<VerifyHistoryQuery>,
) -> Result<Json<VerifyHistoryResponse>, (StatusCode, String)> {
    if q.from.is_empty() || q.to.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "from と to は必須 (YYYY-MM-DD)".to_string(),
        ));
    }
    let rows = store
        .list_verify_jobs(&q.from, &q.to, q.eigyosho_id)
        .await
        .map_err(|e| {
            tracing::error!("list_verify_jobs failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("verify-history query failed: {e}"),
            )
        })?;
    Ok(Json(VerifyHistoryResponse {
        from: q.from,
        to: q.to,
        eigyosho_id: q.eigyosho_id,
        count: rows.len(),
        rows,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// **宣言していない (503) と壊れている (500) を分ける** (Refs #205 の G4)。
    ///
    /// GCP (Cloud Run) の instance は SQL Server に到達できず売上を持てないので、
    /// この口は「今は落ちている」ではなく「この形態には無い」。500 で返すと
    /// 呼び出し側が retry する筋の話になってしまう。
    #[test]
    fn a_missing_local_store_is_unavailable_not_broken() {
        assert_eq!(
            map_local_store_err(LocalStoreError::Disabled),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            map_local_store_err(LocalStoreError::QueryError("boom".to_string())),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            map_local_store_err(LocalStoreError::OpenFailed("boom".to_string())),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            map_local_store_err(LocalStoreError::JoinError("boom".to_string())),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn write_raw_ndjson_gz_creates_file_and_can_be_decoded() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-raw-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let raw_dir = tmp.to_string_lossy().into_owned();

        let payload = b"line1\nline2\n";
        let path = write_raw_ndjson_gz(&raw_dir, "2026-06", 9, payload).unwrap();
        assert!(path.contains("2026-06"));
        assert!(path.ends_with("eigyosho-9.ndjson.gz"));
        assert!(std::path::Path::new(&path).exists());

        // gunzip して中身が一致するか
        let bytes = std::fs::read(&path).unwrap();
        let mut dec = GzDecoder::new(&bytes[..]);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        assert_eq!(out, payload);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn write_raw_ndjson_gz_atomic_rename_leaves_no_tmp() {
        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-raw-tmp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let raw_dir = tmp.to_string_lossy().into_owned();
        let path = write_raw_ndjson_gz(&raw_dir, "2026-06", 1, b"x").unwrap();
        // 同じ dir に .tmp は残らない
        let tmp_path = std::path::Path::new(&path).with_extension("ndjson.gz.tmp");
        assert!(!tmp_path.exists());
        let _ = std::fs::remove_dir_all(&tmp);
    }
}
