//! 全量再計算の口 (Refs #205 の 06 の HTTP 版)。**GCP 側の instance が使う**。
//!
//! 窓の受け口 ([`crate::routes::kintai_timecard::receive_window`]) が畳み直すのは
//! **その窓で打刻が変わった乗務員だけ**で、定常時はほぼ 0 人。畳んだ結果が古くなる
//! 経路はもう 1 つあり、そちらはこの口が受け持つ:
//!
//! | 古くなる原因 | 誰が直すか |
//! |---|---|
//! | 打刻が増えた / 直った | 窓の受け口 (変わった乗務員だけ) |
//! | **`kosoku.rs` を deploy した** | **この口** (全乗務員が stale) |
//! | **TOML の閾値・丸め方を変えた** | **この口** (同上) |
//!
//! 後者は `logic_version` が変わるので**全単位が一斉に stale になる**。窓 1 往復で
//! 畳み直そうとすると必ず時間切れになるため、口を分けて `after_driver_cd` で
//! ページングする。stale かどうかは窓の応答の `stale.drivers` で分かる。
//!
//! ## 1 ページの費用は「全量読み × ページ数」であって乗務員数ではない
//!
//! `GET /api/kintai/timecard/drivers` の 50 ([`crate::kintai_diff::DEFAULT_MAX_DRIVERS`])
//! とは費用の形が違う。あちらは乗務員CD を数える 1 クエリで 1 ページぶんが
//! 返るのに対し、こちらは**乗務員数によらず月ぶんの生イベントを 1 回丸ごと読む**
//! ([`crate::kintai_fold::fold_month`] — alc / R2 への往復)。母集団の決定 ([`recalc_driver_page`])
//! もこの 1 回読みを使い回すので、1 乗務員だけ畳んでも 100 人畳んでも読み出し側の
//! 費用は同じだけ払う。ページを増やす意味は「1 回のリクエストで書く乗務員数」に
//! しかない。
//!
//! 実測 (2026-07-31、2026-06 = 94 名、[`DEFAULT_MAX_FOLD_DRIVERS`] = 20 名/ページ時):
//!
//! | 内訳 | 時間 |
//! |---|---|
//! | 全量読み (alc / R2) | 25〜55 秒 (ページ数・乗務員数に非依存) |
//! | fold (20 名ぶん書く) | 3〜4.6 秒 |
//!
//! **費用がページ数 × 全量読みなので、1 ページの乗務員数を上げるほど総費用が減る。**
//! [`DEFAULT_MAX_FOLD_DRIVERS`] を 100 に上げると 94 名が 1 回で終わり、全量読みを
//! 2 回払わずに済む。100 名分の fold は実測の線形外挿で 15〜23 秒、合計しても
//! auth-worker proxy の 100 秒上限に収まる (40〜78 秒)。内訳は応答の
//! `elapsed_ms` (全体) と `fold.elapsed_ms` (fold だけ) の差で読める
//! — 見積もりで上限を決めない、が [`crate::kintai_diff`] と同じ方針。
//!
//! ## 月ゲート (実装計画 13) — 変化が無ければ全量読みそのものを省く
//!
//! 上の実測は「毎回 R2 から読み直す」前提。前回 fold 時から dtako 側 (alc の
//! etag) も打刻側 (Pg) も変わっていなければ、`month_gate_report` が刺さり、
//! `fold_month` を 1 回も呼ばずに「変化無し」を返す (25〜55 秒 → 索引参照だけの
//! 数十ミリ秒)。定常時 (当日以外) はほぼこちら。当月は毎日データが増えるので、
//! 日 1 回程度は上の実測どおり全量読みになる。
//!
//! **この口は「そのページで月まるごとを完結させたときだけ」gate を書く** —
//! `apply` かつ `after_driver_cd` 無し (1 ページ目から) かつ `next_after_driver_cd`
//! が無い (回りきった) かつ `stale_only` でない (母集団を狭めていない) かつ
//! 上流 warnings が空 (R2 の分割遅れ中の欠けた入力を「最新」と刻まない) の
//! 5 条件が揃ったときだけ。母集団 (実測 132 名) は `DEFAULT_MAX_FOLD_DRIVERS`
//! (100) より上の `max_drivers` を渡せば 1 ページで収まる。書く digest は
//! ページ処理の**冒頭**で判定した値のまま (fold の後に作り直さない —
//! [`crate::kintai_fold::MonthGate::Miss`] の docs)。それ以外のページ (2 ページ目
//! 以降・stale_only・dry-run 等) は読むだけで書かない — 1 ページ目だけ処理した
//! 時点で「この月は最新」と刻むと、未処理ページの乗務員が古い fingerprint の
//! まま取り残される (#225 / #234 と同型の事故) ため。
//!
//! ### 運用: `kosoku.rs` deploy 直後の gate 書き込みは**2 段構え**
//!
//! `kosoku.rs` を deploy すると `logic_version` が変わり全乗務員が stale になる
//! (モジュール冒頭の表)。この直後に**いきなり 1 ページで全員 apply しようとしない
//! こと** — #205-15 の本番実測 (2026-06、137 名) では、実際に書き込みが乗ると
//! 1 ページ 100 名で応答 109 秒 (fold だけで 82.6 秒)。137 名を 1 ページに収めて
//! 全員書かせようとすると 150 秒超が見込まれ、auth-worker proxy の 100 秒上限に
//! 触りうる。
//!
//! 収束済ませてから封をする、の 2 回に分ける:
//!
//! 1. **収束させる**: 既定のページング (`max_drivers` 既定 100) のまま
//!    `apply=true` を最後のページ (`next_after_driver_cd` が `null` になる) まで
//!    回す。実際の書き込みはここで終わる (137 名なら 2 ページ、実測 109 秒 + 57 秒)
//! 2. **封をする**: 続けて `max_drivers=150` (母集団を 1 ページに収める) +
//!    `apply=true` を**もう 1 回**。1 で全員書き終えた直後なので全員 unchanged —
//!    fold だけで済み (実測 fold 23.5 秒、応答 50 秒程度)、上の 5 条件
//!    (1 ページ目・回りきる・stale_only でない・warnings 空) を満たして
//!    **その回が gate の初回書き込みを兼ねる**
//!
//! 以後は月ゲートが刺さり続け、次に `kosoku.rs` や TOML を変えるまで実質ゼロ読み
//! になる。**5 条件は「そのページで実際に何か書いたか」を問わない** — 2 の呼び出しが
//! 全員 unchanged でも、月まるごと 1 ページで回りきっていれば gate は書かれる。
//!
//! ## GET は絶対に書かない
//!
//! `GET` は `apply` を持たない。「読むだけのつもりが全乗務員を書き直していた」を
//! 作れないようにするため、書ける口は `POST` だけにしてある。auth-worker の
//! `/ichibanboshi-proxy` は **path + method の完全一致 allowlist** なので、
//! 口を足したら `nuxt-dtako-admin` 側の登録も要る。

use axum::extract::Query;
use axum::http::{HeaderMap, StatusCode};
use axum::Extension;
use axum::Json;
use serde::Deserialize;

use crate::kintai_fold::{recalc_driver_page, recalc_drivers_from_units, stale_state, FoldReport};
use crate::kintai_push::KintaiPgStore;
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{assert_same_tenant, map_push_err, store_for};
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// 1 回で畳み直す乗務員数の既定。
///
/// **2026-07-31 の実測 (2026-06 = 94 名) に基づき 20 → 100 へ引き上げ。**
/// モジュール docs のとおり費用の主成分は乗務員数に依存しない全量読みなので、
/// 20 のままだと 94 名を畳むのに全量読みを 5 回払っていた。100 なら 1 回で終わる。
pub const DEFAULT_MAX_FOLD_DRIVERS: usize = 100;

/// `max_drivers` の上限。呼び出し側が大きな値を入れて proxy を殺すのを防ぐ。
///
/// 既定の 1.5 倍。**150 は未実測** — 100 名ぶんの fold (実測の線形外挿で
/// 15〜23 秒) より上は測っていない。[`crate::kintai_diff::MAX_MAX_DRIVERS`] と
/// 同じ考え方 (踏んだら下げる) で先に開けてある。
pub const MAX_MAX_FOLD_DRIVERS: usize = 150;

/// `?month=&after_driver_cd=&max_drivers=&stale_only=`。
#[derive(Debug, Default, Deserialize)]
pub struct RecalcQuery {
    pub month: Option<String>,
    /// 続きから回す位置。前回の応答の `next_after_driver_cd` を渡す。
    #[serde(default)]
    pub after_driver_cd: Option<i64>,
    #[serde(default)]
    pub max_drivers: Option<usize>,
    /// 現行の `logic_version` の行を 1 つも持たない乗務員だけに絞る。
    ///
    /// **既定 `false`。** 勤務が 1 本も立たない乗務員は保存行を作れないので
    /// `true` では毎回この網に残る (収束しない) — 全量を回すときは既定のまま使う。
    #[serde(default)]
    pub stale_only: bool,
}

/// `POST /api/kintai/recalc` の本体。`GET` の query と同じ項目 + `apply`。
#[derive(Debug, Default, Deserialize)]
pub struct RecalcRequest {
    pub month: String,
    #[serde(default)]
    pub after_driver_cd: Option<i64>,
    #[serde(default)]
    pub max_drivers: Option<usize>,
    #[serde(default)]
    pub stale_only: bool,
    /// **`true` で初めて書く。** 既定は 1 行も書かない。
    #[serde(default)]
    pub apply: bool,
}

fn bad_request(msg: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.to_string())
}

/// GET /api/kintai/recalc — **1 行も書かずに**、いま畳み直すと何が変わるかを返す。
pub async fn preview(
    headers: HeaderMap,
    Query(q): Query<RecalcQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let req = RecalcRequest {
        month: q.month.unwrap_or_default(),
        after_driver_cd: q.after_driver_cd,
        max_drivers: q.max_drivers,
        stale_only: q.stale_only,
        apply: false,
    };
    run(headers, pg, repo, params, read_tenant, req).await
}

/// POST /api/kintai/recalc — 1 ページぶんを畳み直す。`apply` が無ければ書かない。
pub async fn recalc(
    headers: HeaderMap,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(params): Extension<std::sync::Arc<crate::kosoku::KosokuParams>>,
    Extension(read_tenant): Extension<ReadTenant>,
    Json(req): Json<RecalcRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    run(headers, pg, repo, params, read_tenant, req).await
}

async fn run(
    headers: HeaderMap,
    pg: DynKintaiPgStore,
    repo: DynKintaiEventsRepo,
    params: std::sync::Arc<crate::kosoku::KosokuParams>,
    read_tenant: ReadTenant,
    req: RecalcRequest,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    if !is_valid_month(&req.month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let started = std::time::Instant::now();
    let st = store_for(&pg, &headers)?;
    // 読みと書きが別テナントのまま畳まない (窓の受け口と同じ検査)
    assert_same_tenant(read_tenant, st.tenant_id())?;

    // 月ゲート (実装計画 13) — 前回 fold 時から dtako 側 (alc) / 打刻側 (Pg) の
    // どちらも変わっていなければ、fold_month の全量読みを 1 バイトも払わずに
    // 「変化無し」を返す ([`crate::kintai_fold::MonthGate`] docs)。miss / unavailable
    // なら通常どおり進む — miss の digest は末尾の書き込み判定まで運ぶ
    // (fold 後に作り直さない。理由は MonthGate::Miss の docs)
    let gate = crate::kintai_fold::month_gate_report(&repo, &st, &params, &req.month, req.apply)
        .await
        .map_err(map_push_err)?;
    if let crate::kintai_fold::MonthGate::Hit(report) = &gate {
        let months = [req.month.clone()];
        let stale = stale_state(&st, &months, &params)
            .await
            .map_err(map_push_err)?;
        let (n, w) = (report.drivers, report.drivers_written);
        tracing::info!(n, w, "kintai recalc page done (gate)");
        return Ok(Json(serde_json::json!({
            "month": req.month,
            "apply": req.apply,
            "drivers": Vec::<i64>::new(),
            "next_after_driver_cd": Option::<i64>::None,
            "fold": report,
            "stale": stale,
            "elapsed_ms": started.elapsed().as_millis() as u64,
        })));
    }

    let max = req
        .max_drivers
        .unwrap_or(DEFAULT_MAX_FOLD_DRIVERS)
        .clamp(1, MAX_MAX_FOLD_DRIVERS);

    // 生イベントは月 1 回だけ読み、母集団の決定 (#205-12) と畳みの両方に使い回す。
    // ここで 2 回読むと 1 ページの費用 (全量読み 25〜55 秒、モジュール docs) が
    // 単純に倍になる。上流 warnings を握り潰さない
    // ([`crate::kintai_http_repo::with_warning_sink`])
    let (all_units, warnings) = crate::kintai_http_repo::with_warning_sink(
        crate::kintai_fold::fold_month(&repo, &params, &req.month, None),
    )
    .await;
    let all_units = all_units.map_err(map_push_err)?;
    // rest-only 乗務員 (Postgres に 1 行も無い) を母集団に足すための集合
    // ([`recalc_driver_page`] docs)
    let extra: std::collections::BTreeSet<i64> = all_units
        .iter()
        .map(|(cd, _, _)| *cd as i64)
        .filter(|cd| *cd > 0)
        .collect();

    let page = recalc_driver_page(
        &st,
        &req.month,
        &params,
        &extra,
        req.after_driver_cd,
        req.stale_only,
        max,
    )
    .await
    .map_err(map_push_err)?;
    // 回りきったかは「1 ページに満たなかったか」で決める。次を空で返すより往復が 1 回減る
    let next = match page.len() < max {
        true => None,
        false => page.last().copied(),
    };
    let (folded, stale) = fold_page(&st, &params, &req, &page, all_units, warnings).await?;

    // 月ゲート (実装計画 13) — このページで月まるごとを完結させたときだけ書く。
    // 5 条件は crate::kintai_fold::month_gate_report の呼び出し元向け docs 参照。
    // digest は呼び出し冒頭で判定した値のまま (fold 後に作り直さない)
    if let crate::kintai_fold::MonthGate::Miss {
        dtako_digest,
        punch_digest,
        logic_version,
    } = gate
    {
        let completed_the_whole_month = req.apply
            && req.after_driver_cd.is_none()
            && next.is_none()
            && !req.stale_only
            && folded.warnings.is_empty();
        if completed_the_whole_month {
            crate::kintai_fold::write_fold_gate(
                &st,
                &req.month,
                &dtako_digest,
                &punch_digest,
                &logic_version,
            )
            .await
            .map_err(map_push_err)?;
        }
    }

    // マクロは 1 行に収める (CLAUDE.md)
    let (n, w) = (folded.drivers, folded.drivers_written);
    tracing::info!(n, w, "kintai recalc page done");
    Ok(Json(serde_json::json!({
        "month": req.month,
        "apply": req.apply,
        "drivers": page,
        "next_after_driver_cd": next,
        "fold": folded,
        "stale": stale,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })))
}

/// 1 ページぶんを畳み、`stale` を数える。
///
/// `all_units` は `run` が既に読んでいる全乗務員ぶん ([`crate::kintai_fold::fold_month`])。
/// ここでは読み直さない ([`recalc_drivers_from_units`])。
async fn fold_page(
    st: &KintaiPgStore,
    params: &crate::kosoku::KosokuParams,
    req: &RecalcRequest,
    page: &[i64],
    all_units: Vec<(u64, crate::kintai_fold::FoldUnit, String)>,
    warnings: Vec<String>,
) -> Result<(FoldReport, crate::kintai_fold::StaleReport), (StatusCode, String)> {
    let drivers: Vec<u64> = page.iter().filter(|d| **d > 0).map(|d| *d as u64).collect();
    let mut folded =
        recalc_drivers_from_units(st, params, &req.month, &drivers, all_units, req.apply)
            .await
            .map_err(map_push_err)?;
    folded.warnings = warnings;
    let months = [req.month.clone()];
    let stale = stale_state(st, &months, params)
        .await
        .map_err(map_push_err)?;
    Ok((folded, stale))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdr() -> HeaderMap {
        HeaderMap::new()
    }

    fn repo() -> DynKintaiEventsRepo {
        std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo)
    }

    fn params() -> std::sync::Arc<crate::kosoku::KosokuParams> {
        std::sync::Arc::new(crate::kosoku::KosokuParams::default())
    }

    /// 壊れた月は**書き先を見に行く前に** 400 で落とす。
    #[tokio::test]
    async fn the_month_is_validated_before_the_store() {
        for m in ["", "2026-7", "nope"] {
            let (code, _) = recalc(
                hdr(),
                Extension(None),
                Extension(repo()),
                Extension(params()),
                Extension(ReadTenant(None)),
                Json(RecalcRequest {
                    month: m.to_string(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err();
            assert_eq!(code, StatusCode::BAD_REQUEST, "month={m:?}");
        }
    }

    /// **書き先が無い instance では 503。** オンプレの既定がこれ。
    #[tokio::test]
    async fn it_fails_closed_without_a_store() {
        let (code, msg) = recalc(
            hdr(),
            Extension(None),
            Extension(repo()),
            Extension(params()),
            Extension(ReadTenant(None)),
            Json(RecalcRequest {
                month: "2026-07".to_string(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }

    /// **GET は `apply` を持たない。** 読むだけの口から書けないようにしてある。
    #[tokio::test]
    async fn the_get_route_cannot_apply() {
        let (code, _) = preview(
            hdr(),
            Query(RecalcQuery {
                month: Some("2026-07".to_string()),
                ..Default::default()
            }),
            Extension(None),
            Extension(repo()),
            Extension(params()),
            Extension(ReadTenant(None)),
        )
        .await
        .unwrap_err();
        // 書き先が無いので 503 まで進む = month は通っている
        assert_eq!(code, StatusCode::SERVICE_UNAVAILABLE);
        // GET の query には apply が無い (コンパイル時に保証されている)
        assert!(!RecalcRequest::default().apply);
    }

    #[test]
    fn the_page_size_is_clamped() {
        assert_eq!(DEFAULT_MAX_FOLD_DRIVERS.clamp(1, MAX_MAX_FOLD_DRIVERS), 100);
        assert_eq!(0_usize.clamp(1, MAX_MAX_FOLD_DRIVERS), 1);
        assert_eq!(999_usize.clamp(1, MAX_MAX_FOLD_DRIVERS), 150);
    }
}
