//! 給与大臣 (OHKEN) 読み取りエンドポイント (Refs #82)。
//!
//! 消費者は ohishi-exp/nuxt-dtako-admin の給与比較 (XLS 手動取込の置き換え)。
//! 出力は DB の素直な JSON — SalaryCsvRow 互換は持たない (変換は消費側の責務)。
//!
//! 認可は CF Access でなく in-service: auth-worker introspect + email allowlist
//! (`kyuyo::introspect::authorize`)。LAN 内直叩きも同じゲートを通る。

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::Extension;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::kyuyo::introspect::{authorize, KyuyoAuthState};
use crate::kyuyo::logic::{
    build_companies, build_employee_rows, build_payroll_rows, kydata_db_name, month_period,
    nendo_for_month, parse_month, CompanyInfo, EmployeeRow, PayrollRow, RawKoumokuRow,
    ALLOWED_COMPANIES,
};
use crate::kyuyo::repo::{DynKyuyoRepo, KyuyoRepoError};
use crate::kyuyo::store::{DynKyuyoStore, PayrollSyncedRow};

/// エラーレスポンス本文。
#[derive(Serialize, Debug)]
pub struct ErrorBody {
    pub error: String,
}

type ApiError = (StatusCode, Json<ErrorBody>);

fn err(status: StatusCode, message: impl Into<String>) -> ApiError {
    (
        status,
        Json(ErrorBody {
            error: message.into(),
        }),
    )
}

/// 給与ルートの同時実行制限。OHKEN (給与大臣 PC) は非力なため、DB を触る区間を
/// 同時 1 本に絞る — 超過分は拒否せず順番待ちさせる (Refs #369 ヘルスチェック)。
pub struct KyuyoLimiter {
    pub semaphore: tokio::sync::Semaphore,
}

impl KyuyoLimiter {
    pub fn new() -> Self {
        Self {
            semaphore: tokio::sync::Semaphore::new(1),
        }
    }
}

impl Default for KyuyoLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// `payroll_month` の「DB が開けない」エラー (存在しない年度 / restore 由来の
/// 権限抜け — SQL Server error 4060 はどちらも "Cannot open database") を 404 に
/// 変換する。それ以外は共通の [`map_repo_err`]。
fn map_db_open_err(e: KyuyoRepoError, db: &str) -> ApiError {
    if let KyuyoRepoError::QueryError(message) = &e {
        if message.contains("Cannot open database") || message.contains("4060") {
            return err(
                StatusCode::NOT_FOUND,
                format!(
                    "{db} を開けません (この会社×年度の給与データが未作成、またはデータ復旧で作られた DB で権限の再付与が必要です)"
                ),
            );
        }
    }
    map_repo_err(e)
}

/// company / month の共通検証。OK なら (year, month) を返す。
fn validate_company_month(company: &str, month: &str) -> Result<(i32, u32), ApiError> {
    if !ALLOWED_COMPANIES.contains(&company) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!(
                "company は {} のいずれかで指定してください",
                ALLOWED_COMPANIES.join(" / ")
            ),
        ));
    }
    parse_month(month).ok_or_else(|| {
        err(
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください",
        )
    })
}

fn map_repo_err(e: KyuyoRepoError) -> ApiError {
    match &e {
        KyuyoRepoError::NotConfigured => err(
            StatusCode::SERVICE_UNAVAILABLE,
            "給与 DB 接続が未設定です ([kyuyo] config)",
        ),
        KyuyoRepoError::PoolError(m) => {
            tracing::error!("kyuyo pool error: {m}");
            err(
                StatusCode::SERVICE_UNAVAILABLE,
                "給与 DB に接続できません (給与大臣 PC の稼働を確認してください)",
            )
        }
        KyuyoRepoError::QueryError(m) => {
            tracing::error!("kyuyo query error: {m}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "給与 DB クエリに失敗しました",
            )
        }
    }
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/databases (高速な DB 名一覧 — 差分更新用)
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug)]
pub struct DatabasesResponse {
    /// `KYDATA{会社4桁}_{年度3桁}C` 形式の DB 名一覧 (昇順)。
    pub databases: Vec<String>,
}

/// KYDATA DB 名の一覧のみ (`sys.databases` メタデータ、ミリ秒)。
/// 消費側 (nuxt-dtako-admin) が D1 に持つリストとの差分更新に使う。
/// 会社名・権限チェック込みの完全版は [`companies`] (遅い方)。
pub async fn databases(
    Extension(repo): Extension<DynKyuyoRepo>,
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(limiter): Extension<Arc<KyuyoLimiter>>,
    headers: HeaderMap,
) -> Result<Json<DatabasesResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;

    let _permit = limiter
        .semaphore
        .acquire()
        .await
        .expect("kyuyo limiter semaphore closed");

    let databases = repo
        .list_kydata_database_names()
        .await
        .map_err(map_repo_err)?;
    Ok(Json(DatabasesResponse { databases }))
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/companies
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug)]
pub struct CompaniesResponse {
    pub companies: Vec<CompanyInfo>,
    pub warnings: Vec<String>,
}

/// 会社コード×アクセス可能年度の一覧。アクセス不可 DB は warnings で列挙する
/// (restore 由来の権限抜け検知、#82 受け入れ条件)。
pub async fn companies(
    Extension(repo): Extension<DynKyuyoRepo>,
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(limiter): Extension<Arc<KyuyoLimiter>>,
    headers: HeaderMap,
) -> Result<Json<CompaniesResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;

    // DB を触る区間は同時 1 本 (payroll と同じ制限を共有)
    let _permit = limiter
        .semaphore
        .acquire()
        .await
        .expect("kyuyo limiter semaphore closed");

    let databases = repo.list_kydata_databases().await.map_err(map_repo_err)?;

    // 会社名は補助情報 — KYCOMSTD が読めなくても一覧自体は返す
    let mut warnings: Vec<String> = Vec::new();
    let names: HashMap<String, String> = match repo.company_names().await {
        Ok(pairs) => pairs.into_iter().collect(),
        Err(e) => {
            tracing::warn!("kyuyo company_names error: {e}");
            warnings.push("会社名マスタ (KYCOMSTD) を読めませんでした".to_string());
            HashMap::new()
        }
    };

    let (companies, mut access_warnings) = build_companies(&databases, &names);
    warnings.append(&mut access_warnings);

    Ok(Json(CompaniesResponse {
        companies,
        warnings,
    }))
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/employees?company=0100&month=2026-06 (識別情報のみ)
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct EmployeesQuery {
    /// 会社コード 4 桁 ([`ALLOWED_COMPANIES`] のみ)。
    pub company: String,
    /// 参照する年度 DB を決めるための月 "YYYY-MM"。
    /// **賃金期間の絞り込みには使わない** (社員マスタは支給実績と独立)。
    pub month: String,
}

#[derive(Serialize, Debug)]
pub struct EmployeesResponse {
    pub company: String,
    /// `KYCOMSTD.SELDATA.CONAME1` 由来の正式会社名 (取れなければ空文字 + warning)。
    /// 消費側 (社員マスタ) はこれを会社ラベルに使う (Refs nuxt-dtako-admin#367)。
    pub company_name: String,
    pub month: String,
    /// 参照した年度 DB 名。
    pub database: String,
    pub employees: Vec<EmployeeRow>,
    pub warnings: Vec<String>,
    /// このデータの出どころ (Refs #106): "cache" = SQLite derived store /
    /// "live" = OHKEN 直読み (write-through でキャッシュ済み)。
    pub source: &'static str,
    /// キャッシュの鮮度 (RFC3339)。live 読みでは今回の取得時刻。
    pub synced_at: String,
}

/// 社員マスタの live 読み結果 (read-through と sync の共通部)。
struct EmployeesLive {
    employees: Vec<EmployeeRow>,
    company_name: String,
    warnings: Vec<String>,
}

/// OHKEN から社員マスタを読む (従来の employees 本体)。
async fn fetch_employees_live(
    repo: &DynKyuyoRepo,
    limiter: &KyuyoLimiter,
    company: &str,
    db: &str,
) -> Result<EmployeesLive, ApiError> {
    // payroll と同じ理由で DB を触る区間を直列化する (OHKEN は同時 2 接続)
    let _permit = limiter
        .semaphore
        .acquire()
        .await
        .expect("kyuyo limiter semaphore closed");

    let raw = repo
        .employees(db)
        .await
        .map_err(|e| map_db_open_err(e, db))?;

    // 会社名は補助情報 — KYCOMSTD が読めなくても社員一覧自体は返す
    let mut warnings: Vec<String> = Vec::new();
    let company_name = match repo.company_names().await {
        Ok(pairs) => pairs
            .into_iter()
            .find(|(code, _)| code == company)
            .map(|(_, name)| name)
            .unwrap_or_default(),
        Err(e) => {
            tracing::warn!("kyuyo company_names error: {e}");
            warnings.push("会社名マスタ (KYCOMSTD) を読めませんでした".to_string());
            String::new()
        }
    };

    Ok(EmployeesLive {
        employees: build_employee_rows(&raw),
        company_name,
        warnings,
    })
}

/// 会社×年度の**社員マスタ** (社員番号・氏名・所属・給与体系・退職フラグ)。
///
/// 金額は一切返さない — 消費者は ohishi-exp/nuxt-dtako-admin の社員マスタ
/// (Refs #367) で、給与明細 CSV をブラウザに貼らずに社員を登録するために使う。
/// [`payroll`] と違い `KYUYO` を読まないので、その月に支給が無い社員 (入社直後
/// など) も返る。
///
/// Refs #106: read-through — SQLite derived store に (company, 年度) があれば
/// OHKEN に触らず返す。miss は live 読み + write-through。強制引き直しは [`sync`]。
pub async fn employees(
    Extension(repo): Extension<DynKyuyoRepo>,
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(limiter): Extension<Arc<KyuyoLimiter>>,
    Extension(store): Extension<DynKyuyoStore>,
    headers: HeaderMap,
    Query(params): Query<EmployeesQuery>,
) -> Result<Json<EmployeesResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;

    let (year, month) = validate_company_month(&params.company, &params.month)?;
    let nendo = nendo_for_month(year, month);
    let db = kydata_db_name(&params.company, nendo);

    match store.get_employees(&params.company, nendo).await {
        Ok(Some(cached)) => {
            return Ok(Json(EmployeesResponse {
                company: params.company,
                company_name: cached.company_name,
                month: params.month,
                database: db,
                employees: cached.employees,
                warnings: cached.warnings,
                source: "cache",
                synced_at: cached.synced_at,
            }));
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("kyuyo store read failed — live fallback: {e}");
        }
    }

    let live = fetch_employees_live(&repo, &limiter, &params.company, &db).await?;
    let synced_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = store
        .put_employees(
            &params.company,
            nendo,
            &live.employees,
            &live.company_name,
            &live.warnings,
            &synced_at,
        )
        .await
    {
        // live 応答はそのまま返す — キャッシュ書き込み失敗で読みを殺さない
        tracing::warn!("kyuyo store write failed (employees): {e}");
    }

    Ok(Json(EmployeesResponse {
        company: params.company,
        company_name: live.company_name,
        month: params.month,
        database: db,
        employees: live.employees,
        warnings: live.warnings,
        source: "live",
        synced_at,
    }))
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/payroll?company=0100&month=2026-06
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct PayrollQuery {
    /// 会社コード 4 桁 ([`ALLOWED_COMPANIES`] のみ)。
    pub company: String,
    /// 賃金期間の対象月 "YYYY-MM"。
    pub month: String,
}

#[derive(Serialize, Debug)]
pub struct PayrollResponse {
    pub company: String,
    pub month: String,
    /// 参照した年度 DB 名。
    pub database: String,
    pub rows: Vec<PayrollRow>,
    pub warnings: Vec<String>,
    /// このデータの出どころ (Refs #106): "cache" = SQLite derived store /
    /// "live" = OHKEN 直読み (write-through でキャッシュ済み)。
    pub source: &'static str,
    /// キャッシュの鮮度 (RFC3339)。live 読みでは今回の取得時刻。
    pub synced_at: String,
}

/// 給与明細の live 読み結果 (read-through と sync の共通部)。
struct PayrollLive {
    rows: Vec<PayrollRow>,
    warnings: Vec<String>,
}

/// OHKEN から給与明細を読む (従来の payroll 本体)。
async fn fetch_payroll_live(
    repo: &DynKyuyoRepo,
    limiter: &KyuyoLimiter,
    month_label: &str,
    db: &str,
    year: i32,
    month: u32,
) -> Result<PayrollLive, ApiError> {
    // OHKEN は同時 2 接続 + AUTO_CLOSE で重いので、給与 DB を触る区間全体を
    // セマフォで直列化する — 並列に叩かれてもプール枯渇 (15s timeout → 偽 503)
    // にならず順番待ちになる (本番ヘルスチェックの並列実行で実害があった)
    let _permit = limiter
        .semaphore
        .acquire()
        .await
        .expect("kyuyo limiter semaphore closed");

    // 旧実装はここで sys.databases × HAS_DBACCESS の事前確認をしていたが、
    // AUTO_CLOSE の全 DB (68 個) を毎回開いて回り 1 リクエスト 10 秒級の主因
    // だったため廃止 — 対象 DB へ直接クエリし、開けないエラーを 404 に変換する
    // (存在しない/権限抜けの区別は SQL Server のエラーからは付かないため統合)。
    // HAS_DBACCESS による権限抜けの網羅検知は /api/kyuyo/companies に残っている
    let (from, to) = month_period(year, month);
    let raw = repo
        .payroll_month(db, &from, &to)
        .await
        .map_err(|e| map_db_open_err(e, db))?;

    let koumoku: HashMap<String, RawKoumokuRow> = repo
        .koumoku(db)
        .await
        .map_err(map_repo_err)?
        .into_iter()
        .map(|r| (r.taikeikouno.clone(), r))
        .collect();

    // 対象月に現れた支給回インデックスごとに SHUKEI1 の計算済み集計を引く
    // (通常は 1 つ。月内複数支給があれば複数になる)
    let mut month_indexes: Vec<i32> = raw.iter().map(|r| r.month_index).collect();
    month_indexes.sort_unstable();
    month_indexes.dedup();
    let mut shukei = Vec::new();
    for idx in month_indexes {
        shukei.extend(repo.shukei_totals(db, idx).await.map_err(map_repo_err)?);
    }

    let (rows, mut warnings) = build_payroll_rows(&raw, &koumoku, &shukei);
    if rows.is_empty() {
        warnings.push(format!(
            "{} の {} に賃金期間が一致する支給回がありません",
            db, month_label
        ));
    }
    Ok(PayrollLive { rows, warnings })
}

/// 会社×月の給与明細 (社員×支給項目×金額 + SHUKEI1 計算済み合計)。
///
/// Refs #106: read-through — SQLite derived store に (company, month) があれば
/// OHKEN に触らず返す (給与大臣 PC 停止中でも sync 済み月は表示できる)。
/// miss は live 読み + write-through。強制引き直しは [`sync`]。
pub async fn payroll(
    Extension(repo): Extension<DynKyuyoRepo>,
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(limiter): Extension<Arc<KyuyoLimiter>>,
    Extension(store): Extension<DynKyuyoStore>,
    headers: HeaderMap,
    Query(params): Query<PayrollQuery>,
) -> Result<Json<PayrollResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;

    let (year, month) = validate_company_month(&params.company, &params.month)?;
    let db = kydata_db_name(&params.company, nendo_for_month(year, month));

    match store.get_payroll(&params.company, &params.month).await {
        Ok(Some(cached)) => {
            return Ok(Json(PayrollResponse {
                company: params.company,
                month: params.month,
                database: db,
                rows: cached.rows,
                warnings: cached.warnings,
                source: "cache",
                synced_at: cached.synced_at,
            }));
        }
        Ok(None) => {}
        Err(e) => {
            tracing::warn!("kyuyo store read failed — live fallback: {e}");
        }
    }

    let live = fetch_payroll_live(&repo, &limiter, &params.month, &db, year, month).await?;
    let synced_at = chrono::Utc::now().to_rfc3339();
    if let Err(e) = store
        .put_payroll(
            &params.company,
            &params.month,
            &live.rows,
            &live.warnings,
            &synced_at,
        )
        .await
    {
        // live 応答はそのまま返す — キャッシュ書き込み失敗で読みを殺さない
        tracing::warn!("kyuyo store write failed (payroll): {e}");
    }

    Ok(Json(PayrollResponse {
        company: params.company,
        month: params.month,
        database: db,
        rows: live.rows,
        warnings: live.warnings,
        source: "live",
        synced_at,
    }))
}

// ══════════════════════════════════════════════════════════════
// GET /api/kyuyo/synced-months (sync 済み月の一覧、Refs nuxt-dtako-admin#460)
// ══════════════════════════════════════════════════════════════

#[derive(Serialize, Debug)]
pub struct SyncedMonthEntry {
    pub company: String,
    pub month: String,
    pub synced_at: String,
    pub row_count: i64,
}

#[derive(Serialize, Debug)]
pub struct SyncedMonthsResponse {
    pub entries: Vec<SyncedMonthEntry>,
}

/// derived store に給与明細が入っている (会社, 月) の一覧。消費側 (nuxt-dtako-admin
/// の月タブ) が「給与取り込み済み」バッジを出すためのメタデータのみ — 金額は返さない。
/// OHKEN には触らない (SQLite のみ、ミリ秒)。
pub async fn synced_months(
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(store): Extension<DynKyuyoStore>,
    headers: HeaderMap,
) -> Result<Json<SyncedMonthsResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;
    let rows: Vec<PayrollSyncedRow> = store.payroll_synced().await.map_err(|e| {
        tracing::error!("kyuyo store synced_months error: {e}");
        err(
            StatusCode::INTERNAL_SERVER_ERROR,
            "キャッシュ一覧の読み出しに失敗しました",
        )
    })?;
    Ok(Json(SyncedMonthsResponse {
        entries: rows
            .into_iter()
            .map(|r| SyncedMonthEntry {
                company: r.company,
                month: r.month,
                synced_at: r.synced_at,
                row_count: r.row_count,
            })
            .collect(),
    }))
}

// ══════════════════════════════════════════════════════════════
// POST /api/kyuyo/sync?company=0100&month=2026-06 (強制引き直し、Refs #106)
// ══════════════════════════════════════════════════════════════

#[derive(Deserialize)]
pub struct SyncQuery {
    /// 会社コード 4 桁 ([`ALLOWED_COMPANIES`] のみ)。
    pub company: String,
    /// 対象月 "YYYY-MM"。
    pub month: String,
}

#[derive(Serialize, Debug)]
pub struct SyncResponse {
    pub company: String,
    pub month: String,
    pub database: String,
    /// 保存した給与明細の行数。
    pub payroll_rows: usize,
    /// 保存した社員マスタの人数。
    pub employees: usize,
    pub synced_at: String,
    pub warnings: Vec<String>,
}

/// キャッシュの有無に関わらず OHKEN から引き直して derived store を上書きする
/// (= 画面の「再取得」ボタン。給与の遡り修正・社員マスタ更新の後に使う)。
///
/// 給与明細と社員マスタ (同じ年度 DB) をまとめて更新する。read-through と違い、
/// **store へ書けなければ 500 で loud fail** — sync の成功 = キャッシュが最新、
/// を成立させるため。
pub async fn sync(
    Extension(repo): Extension<DynKyuyoRepo>,
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(limiter): Extension<Arc<KyuyoLimiter>>,
    Extension(store): Extension<DynKyuyoStore>,
    headers: HeaderMap,
    Query(params): Query<SyncQuery>,
) -> Result<Json<SyncResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;

    let (year, month) = validate_company_month(&params.company, &params.month)?;
    let nendo = nendo_for_month(year, month);
    let db = kydata_db_name(&params.company, nendo);

    let payroll = fetch_payroll_live(&repo, &limiter, &params.month, &db, year, month).await?;
    let employees = fetch_employees_live(&repo, &limiter, &params.company, &db).await?;

    let synced_at = chrono::Utc::now().to_rfc3339();
    store
        .put_payroll(
            &params.company,
            &params.month,
            &payroll.rows,
            &payroll.warnings,
            &synced_at,
        )
        .await
        .map_err(|e| {
            tracing::error!("kyuyo store write failed (sync payroll): {e}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "キャッシュへの保存に失敗しました (payroll)",
            )
        })?;
    store
        .put_employees(
            &params.company,
            nendo,
            &employees.employees,
            &employees.company_name,
            &employees.warnings,
            &synced_at,
        )
        .await
        .map_err(|e| {
            tracing::error!("kyuyo store write failed (sync employees): {e}");
            err(
                StatusCode::INTERNAL_SERVER_ERROR,
                "キャッシュへの保存に失敗しました (employees)",
            )
        })?;

    let mut warnings = payroll.warnings;
    warnings.extend(employees.warnings);
    Ok(Json(SyncResponse {
        company: params.company,
        month: params.month,
        database: db,
        payroll_rows: payroll.rows.len(),
        employees: employees.employees.len(),
        synced_at,
        warnings,
    }))
}
