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
    build_companies, build_payroll_rows, kydata_db_name, month_period, nendo_for_month,
    parse_month, CompanyInfo, PayrollRow, ALLOWED_COMPANIES,
};
use crate::kyuyo::repo::{DynKyuyoRepo, KyuyoRepoError};

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
}

/// 会社×月の給与明細 (社員×支給項目×金額 + SHUKEI1 計算済み合計)。
pub async fn payroll(
    Extension(repo): Extension<DynKyuyoRepo>,
    Extension(auth): Extension<Arc<KyuyoAuthState>>,
    Extension(limiter): Extension<Arc<KyuyoLimiter>>,
    headers: HeaderMap,
    Query(params): Query<PayrollQuery>,
) -> Result<Json<PayrollResponse>, ApiError> {
    authorize(&headers, &auth)
        .await
        .map_err(|(status, message)| err(status, message))?;

    if !ALLOWED_COMPANIES.contains(&params.company.as_str()) {
        return Err(err(
            StatusCode::BAD_REQUEST,
            format!(
                "company は {} のいずれかで指定してください",
                ALLOWED_COMPANIES.join(" / ")
            ),
        ));
    }
    let Some((year, month)) = parse_month(&params.month) else {
        return Err(err(
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください",
        ));
    };

    let db = kydata_db_name(&params.company, nendo_for_month(year, month));

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
        .payroll_month(&db, &from, &to)
        .await
        .map_err(|e| map_db_open_err(e, &db))?;

    let koumoku: HashMap<String, String> = repo
        .koumoku(&db)
        .await
        .map_err(map_repo_err)?
        .into_iter()
        .collect();

    // 対象月に現れた支給回インデックスごとに SHUKEI1 の計算済み集計を引く
    // (通常は 1 つ。月内複数支給があれば複数になる)
    let mut month_indexes: Vec<i32> = raw.iter().map(|r| r.month_index).collect();
    month_indexes.sort_unstable();
    month_indexes.dedup();
    let mut shukei = Vec::new();
    for idx in month_indexes {
        shukei.extend(repo.shukei_totals(&db, idx).await.map_err(map_repo_err)?);
    }

    let (rows, mut warnings) = build_payroll_rows(&raw, &koumoku, &shukei);
    if rows.is_empty() {
        warnings.push(format!(
            "{} の {} に賃金期間が一致する支給回がありません",
            db, params.month
        ));
    }
    Ok(Json(PayrollResponse {
        company: params.company,
        month: params.month,
        database: db,
        rows,
        warnings,
    }))
}
