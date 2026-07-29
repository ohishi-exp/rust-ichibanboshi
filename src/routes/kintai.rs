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
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiRepoError};
use crate::kintai_store::DynKintaiStore;
use crate::kosoku::{
    apply_ferry_minus, daily_summary, drop_duplicate_rows, ferry_minus_by_date, month_punches,
    split_by_driver, split_ferry_by_driver, DayPart, DaySummary, KosokuParams, ShiftSource,
};
use crate::kosoku_paper::{paper_daily_minutes, paper_drift_by_date};

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

/// 突合用に日別を絞る (Refs #157)。
///
/// 全項目だと 1 日 516 B・19 キーあり、2026-05 の全乗務員で **1.71 MB**。突合
/// (`timecard-compare` / `get_timecard_diff`) が使うのは**日付・拘束・フェリー控除**と、
/// 暦日按分のための `parts` の日付・拘束だけで、残り 15 キーは受け取って捨てられていた。
/// 絞ると **108 KB (16 分の 1)**。
///
/// この経路は社内から Cloudflare Tunnel を通って出ていくので、応答サイズがそのまま
/// 応答時間になる (実測: DB 0.48 秒 / rust 0.46 秒 なのにブラウザで 14〜57 秒)。
///
/// **キー名は元のまま**にする。短縮すると消費側 (relay / kyuyo-mcp) のパーサを
/// 2 通り持つことになり、削れるのは数 % しかない。
fn compare_days(days: &[DaySummary]) -> Vec<serde_json::Value> {
    days.iter()
        .map(|d| {
            let parts: Vec<serde_json::Value> = d
                .parts
                .iter()
                .map(|p| {
                    let mut o = serde_json::json!({
                        "date": p.date,
                        "restraint_minutes": p.restraint_minutes,
                    });
                    if p.run_gap_minutes != 0 {
                        o["run_gap_minutes"] = serde_json::json!(p.run_gap_minutes);
                    }
                    if p.punch_tail_minutes != 0 {
                        o["punch_tail_minutes"] = serde_json::json!(p.punch_tail_minutes);
                    }
                    if p.punch_head_minutes != 0 {
                        o["punch_head_minutes"] = serde_json::json!(p.punch_head_minutes);
                    }
                    if p.run_head_minutes != 0 {
                        o["run_head_minutes"] = serde_json::json!(p.run_head_minutes);
                    }
                    if p.lunch_overlap_minutes != 0 {
                        o["lunch_overlap_minutes"] = serde_json::json!(p.lunch_overlap_minutes);
                    }
                    // 日跨ぎ勤務のフェリー控除は**内訳側が正** — 突合 (relay の
                    // kosokuPartsByDate) は parts がある勤務を parts だけで暦日合算
                    // するので、ここに載せないと控除が丸ごと落ちて unknown になる
                    // (実測 1714 井上: 単日勤務の 03-08 だけ ferry が付き、日跨ぎの
                    // 03-05/06/15/22/29 は 71〜75 分がそのまま残差になっていた)
                    if p.ferry_minus_minutes != 0 {
                        o["ferry_minus_minutes"] = serde_json::json!(p.ferry_minus_minutes);
                    }
                    o
                })
                .collect();
            let mut o = serde_json::json!({
                "date": d.date,
                "restraint_minutes": d.restraint_minutes,
            });
            // **0 は載せない** (Refs #157)。フェリー控除がある日は月に数十日しか無いのに
            // `"ferry_minus_minutes":0,` が全日に付くと 3,128 日で約 75 KB (応答の 29%)
            // を食う。消費側は欠けを 0 として読む
            if d.ferry_minus_minutes != 0 {
                o["ferry_minus_minutes"] = serde_json::json!(d.ferry_minus_minutes);
            }
            // 休息控除も同じ扱い。拘束からは既に外してあるので突合の値は動かないが、
            // 「この日は休息を何分外したか」が無いと残差の説明が付かない
            if d.rest_minus_minutes != 0 {
                o["rest_minus_minutes"] = serde_json::json!(d.rest_minus_minutes);
            }
            // 運行の継ぎ目 (cause "run-gap" の実額) も 0 は載せない
            if d.run_gap_minutes != 0 {
                o["run_gap_minutes"] = serde_json::json!(d.run_gap_minutes);
            }
            // 日跨ぎ終業の尻尾 (cause "punch-tail" の実額) も同じ規則
            if d.punch_tail_minutes != 0 {
                o["punch_tail_minutes"] = serde_json::json!(d.punch_tail_minutes);
            }
            // 日跨ぎ始業の頭 (cause "punch-head" の実額) も同じ規則
            if d.punch_head_minutes != 0 {
                o["punch_head_minutes"] = serde_json::json!(d.punch_head_minutes);
            }
            // 始業前の運行の頭 (cause "run-head" の実額、紙が大きくなる向き) も同じ規則
            if d.run_head_minutes != 0 {
                o["run_head_minutes"] = serde_json::json!(d.run_head_minutes);
            }
            // 昼休の窓との重なり (cause "lunch" の実額) も同じ規則
            if d.lunch_overlap_minutes != 0 {
                o["lunch_overlap_minutes"] = serde_json::json!(d.lunch_overlap_minutes);
            }
            // 1 日で終わる勤務は内訳が本体と同じなので載せない (元の応答と同じ規則)
            if !parts.is_empty() {
                o["parts"] = serde_json::Value::Array(parts);
            }
            o
        })
        .collect()
}

/// 画面のタイムカード表用に日別を絞る (Refs #164)。
///
/// [`compare_days`] (突合用) と同じ発想の**画面経路**版。全項目だと全乗務員で
/// 月 ~1.7 MB あり、それが社内から Cloudflare Tunnel を通って毎回出ていく
/// (方針は「圧縮より先にデータを減らす」— #156 revert 時のユーザー決定)。
///
/// 消費側は 2 つ — nuxt-dtako-admin front の `app/utils/kosoku-daily.ts`
/// (`toKosokuDay`) と relay の `workers/dtako-scraper-relay/src/kosoku-daily.ts`
/// (`parseKosokuDaily`)。残す/落とすはどちらの実コードにも合わせてある:
///
/// - **常に残す**: `date` / `start` / `end` — 消費側はどれかが欠けた日を捨てる
/// - **既定と違うときだけ載せる**: `source` は `rest` のみ (消費側は `=== 'rest'`
///   判定)、`is_legal_holiday` / `over_24h` は `true` のみ (`=== true` 判定)、
///   分数は非 0 のみ (欠けは 0 に落ちる)
/// - `punches` (勤務の中の打刻 = 表の出勤/退社列の原本) と `parts` (暦日按分)
///   は**空でなければ**残す。part 側も `date` + 非 0 分数だけ
/// - **落とす**: `rest_minus_minutes` (compare の診断専用で画面は読まない)
///
/// **キー名は元のまま** (compare_days と同じ理由 — 消費側のパーサを 2 通りに
/// しない)。
fn timecard_days(days: &[DaySummary]) -> Vec<serde_json::Value> {
    days.iter()
        .map(|d| {
            let mut o = serde_json::json!({
                "date": d.date,
                "start": d.start,
                "end": d.end,
            });
            // 消費側は `=== 'rest'` で見るので、既定の `timecard` は書かない
            if d.source == ShiftSource::Rest {
                o["source"] = serde_json::json!(d.source);
            }
            // `=== true` 判定なので false は書かない
            if d.is_legal_holiday {
                o["is_legal_holiday"] = serde_json::json!(true);
            }
            if d.over_24h {
                o["over_24h"] = serde_json::json!(true);
            }
            // **0 は載せない** (Refs #157 と同じ規則)。日別 13 個の分数の過半は 0 で、
            // 消費側は欠けを 0 として読む
            for (key, v) in [
                ("restraint_minutes", d.restraint_minutes),
                ("break_minutes", d.break_minutes),
                ("working_minutes", d.working_minutes),
                ("statutory_minutes", d.statutory_minutes),
                (
                    "within_statutory_overtime_minutes",
                    d.within_statutory_overtime_minutes,
                ),
                ("overtime_minutes", d.overtime_minutes),
                ("legal_holiday_minutes", d.legal_holiday_minutes),
                ("night_minutes", d.night_minutes),
                ("overtime_night_minutes", d.overtime_night_minutes),
                ("legal_holiday_night_minutes", d.legal_holiday_night_minutes),
                ("ferry_minus_minutes", d.ferry_minus_minutes),
            ] {
                if v != 0 {
                    o[key] = serde_json::json!(v);
                }
            }
            // 休息由来の勤務は空 — 空配列を全日ぶら下げない
            if !d.punches.is_empty() {
                o["punches"] = serde_json::json!(d.punches);
            }
            // 1 日で終わる勤務は空 (元の応答と同じ規則)
            if !d.parts.is_empty() {
                o["parts"] = serde_json::Value::Array(timecard_parts(&d.parts));
            }
            o
        })
        .collect()
}

/// [`timecard_days`] の暦日按分 — `date` + 非 0 分数だけ (Refs #164)。
fn timecard_parts(parts: &[DayPart]) -> Vec<serde_json::Value> {
    parts
        .iter()
        .map(|p| {
            let mut o = serde_json::json!({ "date": p.date });
            for (key, v) in [
                ("restraint_minutes", p.restraint_minutes),
                ("working_minutes", p.working_minutes),
                ("overtime_minutes", p.overtime_minutes),
                ("legal_holiday_minutes", p.legal_holiday_minutes),
                ("night_minutes", p.night_minutes),
                ("overtime_night_minutes", p.overtime_night_minutes),
                ("legal_holiday_night_minutes", p.legal_holiday_night_minutes),
                ("ferry_minus_minutes", p.ferry_minus_minutes),
            ] {
                if v != 0 {
                    o[key] = serde_json::json!(v);
                }
            }
            o
        })
        .collect()
}

/// 応答の絞り方。**未知の値は [`Full`](ResponseView::Full)** — 綴り間違いで黙って
/// 情報が減らないように、従来どおり全項目へ倒す (壊さない方に倒す)。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResponseView {
    /// 従来どおり全項目。
    Full,
    /// 突合に要る項目だけ (Refs #157)。
    Compare,
    /// 画面のタイムカード表に要る項目だけ (Refs #164)。
    Timecard,
}

fn parse_view(view: Option<&str>) -> ResponseView {
    match view {
        Some("compare") => ResponseView::Compare,
        Some("timecard") => ResponseView::Timecard,
        _ => ResponseView::Full,
    }
}

/// 対象月の書式検証。`YYYY-MM` で月は 01-12。
///
/// 上流は月単位 API (`HolidaysTrait` が `first_day_of_month` を受けて「日」の配列を
/// 返す) なので、任意の日付レンジは受け付けない。
pub fn is_valid_month(month: &str) -> bool {
    let bytes = month.as_bytes();
    if bytes.len() != 7 || bytes[4] != b'-' {
        return false;
    }
    if !bytes[..4].iter().all(|b| b.is_ascii_digit()) {
        return false;
    }
    if !bytes[5..].iter().all(|b| b.is_ascii_digit()) {
        return false;
    }
    let mm: u32 = month[5..].parse().unwrap_or(0);
    (1..=12).contains(&mm)
}

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
fn map_repo_err(e: KintaiRepoError) -> (StatusCode, String) {
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

/// GET /api/kintai/kosoku-daily?month=YYYY-MM[&driver=1051] — **打刻基準の日別サマリ**
/// (Refs #118、拘束時間の打刻基準化 Phase 2)。
///
/// `/events` の生イベントを [`crate::kosoku`] の純粋ロジックで日別に畳んで返す。
/// **応答に金額は含めない** — 認可が `/events` と同じ CF Access Service Token
/// (edge) のままでよいのはそのため。金額を足すことになったら `/kyuyo/*` と同じ
/// in-service gate へ移すこと。
///
/// 勤務は**始業日**で当月に振り分ける。月初の勤務は前月末に始まった休息の終わりを
/// 始業とするが、その区間は `EVENTS_SQL` が「期間内に終わる区間」として拾うので、
/// 範囲は `/events` と同じでよい。
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
    let Some(raw_driver) = params.driver else {
        return kosoku_daily_all(
            &month,
            repo,
            &params_cfg,
            parse_view(params.view.as_deref()),
        )
        .await;
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
    let rows = repo
        .fetch_events(&month, driver)
        .await
        .map_err(map_repo_err)?;
    // 取り込みが 2 回走ると全列同一の行が入る。**紙は二重計上する**ので件数を返す
    let (rows, duplicate_rows) = drop_duplicate_rows(rows);
    let mut days = daily_summary(&rows, &month, &params_cfg);
    // 紙のタイムカード表がこの月に引いているフェリー控除を載せる (Refs #146)。
    // **拘束の計算には入れない** — 突合で差の原因を説明するためだけ。
    // 取れなくても日別サマリは返す (控除が 0 になるだけ) — 突合の付帯情報のために
    // 本体を落とさない
    match repo.fetch_ferry(&month, Some(driver)).await {
        Ok(ferry) => apply_ferry_minus(&mut days, &ferry_minus_by_date(&ferry)),
        Err(e) => tracing::warn!("ferry fetch failed — ferry_minus stays 0: {e}"),
    }
    // 件数は先に出す — `tracing::info!` の引数は購読者が居ないと評価されない
    let count = days.len();
    tracing::info!(month = %month, driver, days = count, "kintai kosoku-daily built");
    match parse_view(params.view.as_deref()) {
        ResponseView::Compare => {
            // 突合は打刻を見ない
            let mut o = serde_json::json!({
                "month": month,
                "driver": driver,
                "days": compare_days(&days),
            });
            // 無い方が普通なので、あるときだけ載せる (フェリー控除と同じ規則)
            if !duplicate_rows.is_empty() {
                o["duplicate_rows"] = serde_json::json!(duplicate_rows);
            }
            // 紙の再現値との日別の差 (cause `rounding` の実額、Refs
            // ohishi-exp/nuxt-dtako-admin#501)。突合しか使わないので compare だけに載せる
            let drift = paper_drift_by_date(&days, &paper_daily_minutes(&rows, &month));
            if !drift.is_empty() {
                o["paper_drift_by_date"] = serde_json::json!(drift);
            }
            Ok(Json(o))
        }
        // 画面は月全打刻 (`punches`) も `duplicate_rows` 診断も読まない (Refs #164)
        ResponseView::Timecard => Ok(Json(serde_json::json!({
            "month": month,
            "driver": driver,
            "days": timecard_days(&days),
        }))),
        ResponseView::Full => Ok(Json(serde_json::json!({
            "month": month,
            "driver": driver,
            "days": days,
            "duplicate_rows": duplicate_rows,
            "punches": month_punches(&rows, &month),
        }))),
    }
}

/// `driver` 省略時 — 全乗務員ぶんを 1 リクエストで畳む (Refs #125)。
///
/// [`daily_summary`] は乗務員を知らない純粋関数なので、**先に
/// [`split_by_driver`] で分けてから**乗務員ごとに呼ぶ。混ぜたまま渡すと他人の
/// 休息で勤務が切れる。
///
/// **勤務が 1 日も組めなかった乗務員は落とす。** 期間内に打刻も休息も無い人 (退職者・
/// 内勤) まで空配列で並べると応答が膨らむだけで、受け手にとって「居ない」と同じ。
async fn kosoku_daily_all(
    month: &str,
    repo: DynKintaiEventsRepo,
    params_cfg: &KosokuParams,
    view: ResponseView,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let rows = repo.fetch_all_events(month).await.map_err(map_repo_err)?;
    // 全乗務員ぶんを 1 回で引いて乗務員ごとに分ける (Refs #146)。取れなければ空 =
    // 控除 0 で続ける — 突合の付帯情報のために日別サマリを落とさない
    let ferry_by_driver = match repo.fetch_ferry(month, None).await {
        Ok(rows) => split_ferry_by_driver(rows),
        Err(e) => {
            tracing::warn!("ferry fetch failed — ferry_minus stays 0: {e}");
            Default::default()
        }
    };
    let drivers: Vec<serde_json::Value> = split_by_driver(rows)
        .into_iter()
        .map(|(driver, rows)| {
            // 乗務員ごとに落とす — 全列同一の行は同じ乗務員にしか現れない
            let (rows, duplicate_rows) = drop_duplicate_rows(rows);
            let mut days = daily_summary(&rows, month, params_cfg);
            if let Some(ferry) = ferry_by_driver.get(&driver) {
                apply_ferry_minus(&mut days, &ferry_minus_by_date(ferry));
            }
            // 紙の再現値との日別の差 (cause `rounding` の実額)。突合経路だけで計算する
            let drift = if view == ResponseView::Compare {
                paper_drift_by_date(&days, &paper_daily_minutes(&rows, month))
            } else {
                Default::default()
            };
            // 打刻は勤務と切り離して返す — 対になる終業が無い始業も表に出すため (#137)
            let punches = month_punches(&rows, month);
            (driver, days, punches, duplicate_rows, drift)
        })
        .filter(|(_, days, punches, _, _)| !days.is_empty() || !punches.is_empty())
        .map(|(driver, days, punches, duplicate_rows, drift)| {
            // 画面は月全打刻 (`punches` = `month_punches` 由来) を読まない —
            // `days[].punches` と実質重複しており、丸ごと落とせる (Refs #164)。
            // `duplicate_rows` 診断も画面は読まない
            if view == ResponseView::Timecard {
                return serde_json::json!({ "driver": driver, "days": timecard_days(&days) });
            }
            let mut o = if view == ResponseView::Compare {
                // 突合は打刻を見ない。日別も要る項目だけに絞る (Refs #157)
                serde_json::json!({ "driver": driver, "days": compare_days(&days) })
            } else {
                serde_json::json!({ "driver": driver, "days": days, "punches": punches })
            };
            // 無い方が普通なので、あるときだけ載せる (フェリー控除と同じ規則)
            if !duplicate_rows.is_empty() {
                o["duplicate_rows"] = serde_json::json!(duplicate_rows);
            }
            if !drift.is_empty() {
                o["paper_drift_by_date"] = serde_json::json!(drift);
            }
            o
        })
        .collect();
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
    fn valid_months() {
        assert!(is_valid_month("2026-01"));
        assert!(is_valid_month("2026-12"));
    }

    #[test]
    fn invalid_months() {
        assert!(!is_valid_month(""));
        assert!(!is_valid_month("2026-1"));
        assert!(!is_valid_month("2026-00"));
        assert!(!is_valid_month("2026-13"));
        assert!(!is_valid_month("2026/06"));
        assert!(!is_valid_month("20a6-06"));
        assert!(!is_valid_month("2026-0a"));
        assert!(!is_valid_month("2026-006"));
    }

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
    fn view_parsing() {
        assert_eq!(parse_view(None), ResponseView::Full);
        assert_eq!(parse_view(Some("compare")), ResponseView::Compare);
        assert_eq!(parse_view(Some("timecard")), ResponseView::Timecard);
        // 未知の値は全項目へ倒す (壊さない方に倒す)
        assert_eq!(parse_view(Some("full")), ResponseView::Full);
        assert_eq!(parse_view(Some("")), ResponseView::Full);
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
