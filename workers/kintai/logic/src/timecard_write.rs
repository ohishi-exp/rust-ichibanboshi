//! `GET /api/kintai/timecard/signatures`・`POST /api/kintai/timecard` の入力の検査と応答 (Refs #322)。
//!
//! 元は root の `src/routes/kintai_timecard.rs` の `signatures`・`receive` (Cloud Run 版)。検査の順・400 の条件と文言・
//! 応答の JSON は元と同じ。差分の計画 (`plan_received_batch`)・署名・SQL・bind の束は共有 crate
//! `kintai_kosoku::kintai_push` (オンプレ版・Cloud Run 版と同じもの)。DB との往復は `kintai-pg`。
//!
//! **テナントは `KINTAI_TENANT_ID` の設定 pin (`X-Tenant-ID` は読まない)。** 元は `X-Tenant-ID` を読み、設定の pin と
//! 食い違えば 403・無ければ 400 だった。Worker は pin だけで決めるので、その 403 / 400 は無い (pin が無ければ 503)。

use std::collections::BTreeMap;

use chrono::{DateTime, FixedOffset, NaiveDate};
use kintai_kosoku::kintai_push::{
    jst_day_bounds, month_date_bounds, TimecardBatch, TimecardBatchResult,
};
use serde::Deserialize;

use crate::common::{bad_request, is_valid_month, parse_json, parse_query, Fail};

/// 502 の本文の頭 (元の `KintaiPushError::Db` の `kintai push db failed: …`)。
pub const DB_WHAT: &str = "kintai push db";

const BAD_MONTH: &str = "month は YYYY-MM で指定してください";

/// `?month=YYYY-MM&driver_cd=1130`。どちらも必須 (`driver_cd` が数でなければ Query の段で 400)。
#[derive(Debug, Default, Deserialize)]
pub struct SignaturesQuery {
    pub month: Option<String>,
    pub driver_cd: Option<i64>,
}

/// 検査済みの signatures の入力。`from` / `to` は月の JST の境界 (`TIMESTAMPTZ`)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignaturesRequest {
    pub month: String,
    pub driver_cd: i64,
    pub from: DateTime<FixedOffset>,
    pub to: DateTime<FixedOffset>,
}

/// signatures のクエリ文字列を読む (元と同じ順: Query → month → driver_cd)。
pub fn parse_signatures(query: &str) -> Result<SignaturesRequest, Fail> {
    let q: SignaturesQuery = parse_query(query)?;
    let month = q.month.unwrap_or_default();
    if !is_valid_month(&month) {
        return Err(bad_request(BAD_MONTH));
    }
    let driver_cd = q
        .driver_cd
        .ok_or_else(|| bad_request("driver_cd は必須です"))?;
    let (m0, m1) = month_date_bounds(&month).ok_or_else(|| bad_request("month が不正です"))?;
    Ok(SignaturesRequest {
        month,
        driver_cd,
        from: jst_day_bounds(m0).0,
        to: jst_day_bounds(m1).0,
    })
}

/// signatures の応答 `{"month", "driver_cd", "signatures": {"YYYY-MM-DD": sha256}}`。
pub fn signatures_respond(
    req: &SignaturesRequest,
    sigs: &BTreeMap<NaiveDate, String>,
) -> serde_json::Value {
    serde_json::json!({
        "month": req.month,
        "driver_cd": req.driver_cd,
        "signatures": sigs,
    })
}

/// `POST /api/kintai/timecard` の本文を読む (元と同じ順: Json の拒否 (415/413/400/422) → month (400))。
pub fn parse_batch(content_type: Option<&str>, body: &[u8]) -> Result<TimecardBatch, Fail> {
    let batch: TimecardBatch = parse_json(content_type, body)?;
    if !is_valid_month(&batch.month) {
        return Err(bad_request(BAD_MONTH));
    }
    Ok(batch)
}

/// `POST /api/kintai/timecard` の応答 (`TimecardBatchResult` をそのまま)。
pub fn batch_respond(result: &TimecardBatchResult) -> serde_json::Value {
    serde_json::json!(result)
}
