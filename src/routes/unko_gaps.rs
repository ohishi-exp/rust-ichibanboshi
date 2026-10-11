//! 取り込み漏れ候補 (`also_in_month`) の GCP にしか無い運行の運行NO を返す口
//! (Refs `ohishi-exp/nuxt-dtako-admin#623` の 1)。
//!
//! `/restraint-wage` の「オンプレ vs Supabase」タブに取り込み漏れの候補
//! (同じ月にオンプレの運行も在るのに GCP に無い運行がある乗務員) が出るように
//! なったが、候補の**運行NO そのもの**がどの応答にも出ていない。既存の 3 つは
//! いずれも使えない: `UnkoDiffDriverSplit` は件数だけ、
//! `unko_diff_gcp_only_in_month_by_driver` は乗務員までしか分からない、
//! `unko_diff_gcp_only_sample` は窓ぜんたい (対象外の運行を含む) の先頭 10 件で
//! 候補とは別物。運行NO が出れば、既存の「MySQL (dtako) 側を取り直す」口
//! (`run_dtako_reimport` 相当、運行 1 件単位) にそのまま渡せる — 新しい取り込み
//! 経路は要らない。
//!
//! ## ファイル名は `unko_gaps.rs` で固定 (`kintai` / `kosoku` で始めない)
//!
//! [`crate::routes::stale_months`] と同じ理由。`build.rs` の
//! `KINTAI_OUTPUT_GLOBS` はディレクトリ + ファイル名前方一致
//! (`("src","kosoku")` / `("src","kintai")` / `("src/routes","kintai")`) で
//! `logic_version` の指紋を作るので、そこに入ると 1 バイトの変更でも
//! 全乗務員・全月が stale になる。**この口が読む部品
//! (`with_unko_diff_sink` / `collected_etag_unko_nos` / `collected_etag_driver_cds` /
//! `fetch_dtako_month_digest` / `unko_no_start_date`) はすべて既に `pub` /
//! `pub(crate)` で公開済み**なので、glob 内 (`kintai_http_repo.rs` /
//! `kintai_fold.rs` / `kintai_push.rs` / `kintai_repo.rs`) を**1 行も変えずに**
//! 呼ぶだけで組める。
//!
//! ## sink を埋める — glob を触らずに `fetch_etags` (private) へ届く経路
//!
//! `fetch_etags` (alc の `GET /api/dtako/events/etags` を叩く実装) は
//! `HttpKintaiEventsRepo` の private メソッドで、外から直接呼べない。だが
//! **`KintaiEventsApi::fetch_dtako_month_digest(month)` という `pub` トレイト
//! メソッドが、内部で `fetch_etags` を「対象月だけの窓」で呼んでいる**
//! ([`crate::kintai_http_repo::HttpKintaiEventsRepo`] の実装、
//! `month_etags_bounds` = `[月初, 翌月初]` の閉区間)。これは
//! [`crate::kintai_fold::compute_month_digests`] が月ゲートの指紋を取るのと
//! **全く同じ呼び方** — 呼んだ結果 (digest 文字列) は使わず、副作用として
//! [`crate::kintai_http_repo::UNKO_SINK`] task-local に積まれる
//! `unko_no` 集合と `unko_no → driver_cds` を
//! [`crate::kintai_http_repo::collected_etag_unko_nos`] /
//! [`crate::kintai_http_repo::collected_etag_driver_cds`] で読むだけ。
//!
//! **fold が使う窓 (実測 `2026-04-20..2026-07-02`、前後 2.5 か月ぶん) より
//! 狭い** — この口は `also_in_month` の判定に対象月だけで足りるので、
//! 狭めたぶん alc への往復は軽くなるはず (次項参照)。
//!
//! ## オンプレ側の読みは `stored_month_operations` を使わない
//!
//! `KintaiPgStore::stored_month_operations` は内部で `self.tenant_id`
//! (= `[kintai_push] tenant_id` の書き込み pin) を bind する。本番 GCP は
//! これを設定しない運用 ([`crate::routes::stale_months`] の docs、
//! Refs #205 の 23) なので、素直に呼ぶと本番で常に 0 件になる —
//! 「読み先を `X-Tenant-ID` で選べる口を allowlist に通すと危険」と同じ根の
//! 事故を書き込み pin 側でも起こす形。[`crate::routes::stale_months`] が
//! 自前の SQL + 解決済みの読みテナントで叩いているのと同じく、ここも
//! [`crate::kintai_push::MONTH_OPERATIONS_SQL`] (SQL 文字列だけ再利用) を
//! 自前の bind で叩く。
//!
//! ## パラメータと応答
//!
//! `GET /api/kintai/unko-gaps?month=YYYY-MM&driver_cd=<i64>` (`driver_cd` は任意)。
//!
//! - `driver_cd` を指定: その乗務員の GCP-only-in-month 運行NO 一覧を返す
//!   (`also_in_month` かどうかは問わない — 呼び出し側が既に候補と分かっている
//!   前提)
//! - 省略: `also_in_month` (= その月にオンプレの運行も 1 件以上在る) の
//!   候補乗務員**全員**ぶん
//!
//! ## 「無い」と「引けていない」を区別する
//!
//! - `gcp_etags_available`: `false` なら alc の etags 口が使えない環境
//!   (`collected_etag_unko_nos` が `None`)。この状態の `drivers: []` は
//!   「候補が居ない」ではなく「判定できない」
//! - `driver_cds_available`: `false` なら etags は引けたが `driver_cds`
//!   (乗務員別の内訳) を 1 件も持たない環境。この状態でも GCP-only-in-month の
//!   運行そのものは存在しうる — それは `unknown_driver_unko_nos` に集める
//!   (`UnkoDiff::gcp_only_in_month_unknown_driver` と同じ考え方)。**空を
//!   「候補が居ない」と読ませない**という要求 (alc が `driver_cds` を返さない
//!   環境では正常に空) はここで満たす
//!
//! ## 運行NO は 22 桁 (GCP 側) — 23 桁に変換しない
//!
//! ここが返す `unko_no` は etags (alc) 由来なので**常に GCP 側の 22 桁**。
//! オンプレ (MariaDB) の `unko_no` は 23 桁 (運行NO 22 桁 + 対象CD 1 桁) で、
//! 候補の運行はまだオンプレに無い以上対象CD を決められない。**23 桁への変換は
//! 呼び出し側の問題** — ここでは変換しない (詰めると存在しない運行を指す)。
//!
//! ## ★ ページ表示で叩く口ではない (on-demand 専用)
//!
//! この口のコストの本体は alc への etags HTTP 往復 (`fetch_dtako_month_digest`)
//! で、対象月だけに窓を狭めてはいるが**実測できていない** — この変更を検証した
//! 環境 (branch push のみ・GCP 側の alc-backed instance に直接届く経路が無い)
//! では計測不能だった。参考として、fold が使う広い窓 (前後 2.5 か月ぶん) の
//! 実測は 25〜55 秒 ([`crate::routes::kintai_recalc`] の module docs) — 窓を
//! 1 か月に狭めても**「速い」と決め打たない**。deploy 後に本物の応答時間
//! (`elapsed_ms`) を計測してから、ページ表示で自動的に叩いてよいかを判断する
//! こと。当面は「候補が出た後にボタンを押して呼ぶ」用途に限る。
//!
//! Postgres 側 (自前クエリ 1 発) だけの実測は本ファイルの pg テスト
//! (`tests/unko_gaps_pg_test.rs`) 参照。
//!
//! ## 純粋部分は共有 crate (`kintai_logic::unko_gaps`)
//!
//! 判定・整形の核 (`build_gaps`・`drop_crew_suffix`・上限・応答の組み立て) と `month` の検査は勤怠 Worker
//! (`workers/kintai`) と共有する (写さない。Refs #322)。ここに残るのは axum・sqlx・alc の etags の sink と
//! `elapsed_ms` だけ。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use kintai_logic::common::Fail;
use kintai_logic::unko_gaps::{check_month, respond, Onprem, Window, DB_WHAT};

pub use kintai_logic::unko_gaps::{UnkoGapsQuery, MAX_UNKO_GAPS_DRIVERS, MAX_UNKO_GAPS_PER_DRIVER};

use crate::kintai_push::{KintaiPgStore, MONTH_OPERATIONS_SQL, PUSHED_SOURCES};
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

fn bad_request(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.into())
}

/// 共有 crate の失敗 (400 だけ) を axum の形に。
fn from_fail(f: Fail) -> (StatusCode, String) {
    (
        StatusCode::from_u16(f.status).unwrap_or(StatusCode::BAD_REQUEST),
        f.body,
    )
}

/// [`crate::routes::stale_months`] と同じ文言・同じ形。
fn store(pg: &DynKintaiPgStore) -> Result<&KintaiPgStore, (StatusCode, String)> {
    pg.as_deref().ok_or((
        StatusCode::SERVICE_UNAVAILABLE,
        "[kintai_push] が無効です (書き先がありません)".to_string(),
    ))
}

/// 読み先のテナント。[`crate::routes::stale_months::read_tenant_of`] と同じ形
/// (モジュール docs の「オンプレ側の読みは…」参照) — **どちらも無ければ 503**。
fn read_tenant_of(read: ReadTenant, pin: uuid::Uuid) -> Result<uuid::Uuid, (StatusCode, String)> {
    if let Some(t) = read.0 {
        if !t.is_nil() {
            return Ok(t);
        }
    }
    if !pin.is_nil() {
        return Ok(pin);
    }
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        "読み先のテナントが決まりません ([kintai_events] tenant_id を設定してください)".to_string(),
    ))
}

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (StatusCode::BAD_GATEWAY, format!("{DB_WHAT} failed: {e}"))
}

/// GET /api/kintai/unko-gaps?month=YYYY-MM&driver_cd=<i64> — 取り込み漏れ候補
/// (`also_in_month`) の GCP にしか無い運行の運行NO を返す (Refs
/// `ohishi-exp/nuxt-dtako-admin#623` の 1)。**書かない** — 読むだけ。
pub async fn unko_gaps(
    Query(q): Query<UnkoGapsQuery>,
    Extension(pg): Extension<DynKintaiPgStore>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(read_tenant): Extension<ReadTenant>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let started = std::time::Instant::now();
    let month = check_month(q.month.clone()).map_err(from_fail)?;
    let st = store(&pg)?;
    let tenant = read_tenant_of(read_tenant, st.tenant_id())?;

    // オンプレ側 (押し込み済み `kintai.kintai_events`)。窓は kintai_fold の
    // measure_unko_diff が使う month_range と同じにして、既存の onprem_in_month
    // の実測 (2026-06: 1445→5, 1740→10) と揃える。tenant_id は解決済みの読み
    // テナントを bind する (モジュール docs — self.tenant_id を使う既存メソッドは
    // 使わない)
    let bad_month = || bad_request(format!("month が壊れています: {month}"));
    let window = Window::of(&month).ok_or_else(bad_month)?;
    use sqlx::Row;
    let rows = sqlx::query(MONTH_OPERATIONS_SQL)
        .bind(tenant)
        .bind(window.from)
        .bind(window.to)
        .bind(&PUSHED_SOURCES[..])
        .fetch_all(st.pool())
        .await
        .map_err(db_err)?;
    let pairs: Vec<(i64, String)> = rows
        .iter()
        .map(|r| (r.get("driver_cd"), r.get("unko_no")))
        .collect();
    let onprem = Onprem::from_rows(pairs.iter().map(|(d, u)| (*d, u.as_str())));

    // GCP 側の etags — 対象月だけの narrow window (モジュール docs 参照)。
    // `collected_etag_unko_nos` / `collected_etag_driver_cds` は
    // `with_unko_diff_sink` が張る task-local の中でしか実体を持たないので、
    // 呼び出し (`fetch_dtako_month_digest`) と同じ future の中で読む
    let ((digest, gcp_unko_nos, gcp_driver_cds), _unused_diff) =
        crate::kintai_http_repo::with_unko_diff_sink(async {
            let d = repo.fetch_dtako_month_digest(&month).await;
            let u = crate::kintai_http_repo::collected_etag_unko_nos();
            let dc = crate::kintai_http_repo::collected_etag_driver_cds();
            (d, u, dc)
        })
        .await;
    if let Err(e) = &digest {
        tracing::warn!(month = %month, error = %e, "kintai unko-gaps dtako digest failed");
    }
    let gcp_etags_available = gcp_unko_nos.is_some();
    let gcp = gcp_etags_available.then_some(&gcp_driver_cds);

    let mut body = respond(&month, &window, q.driver_cd, &onprem, gcp);
    let n = body["drivers"].as_array().map_or(0, Vec::len);
    tracing::info!(n, gcp_etags_available, "kintai unko-gaps read");
    body["elapsed_ms"] = serde_json::json!(started.elapsed().as_millis() as u64);
    Ok(Json(body))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uuid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    // ── read_tenant_of / store (stale_months.rs と同じ形の再検査) ──────────────

    #[test]
    fn read_tenant_wins_over_the_write_pin() {
        assert_eq!(
            read_tenant_of(ReadTenant(Some(uuid(1))), uuid::Uuid::nil()),
            Ok(uuid(1))
        );
        assert_eq!(
            read_tenant_of(ReadTenant(Some(uuid(1))), uuid(2)),
            Ok(uuid(1))
        );
    }

    #[test]
    fn without_a_read_tenant_the_write_pin_is_used() {
        assert_eq!(read_tenant_of(ReadTenant(None), uuid(2)), Ok(uuid(2)));
        assert_eq!(
            read_tenant_of(ReadTenant(Some(uuid::Uuid::nil())), uuid(2)),
            Ok(uuid(2))
        );
    }

    #[test]
    fn no_tenant_at_all_is_service_unavailable() {
        let (status, msg) = read_tenant_of(ReadTenant(None), uuid::Uuid::nil())
            .expect_err("must fail without any tenant");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_events"), "{msg}");
    }

    #[test]
    fn store_missing_is_service_unavailable() {
        let pg: DynKintaiPgStore = None;
        let (status, msg) = store(&pg).expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(msg.contains("kintai_push"), "{msg}");
    }

    #[tokio::test]
    async fn the_handler_fails_closed_without_a_store() {
        let repo: DynKintaiEventsRepo =
            std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo);
        let (status, _msg) = unko_gaps(
            Query(UnkoGapsQuery {
                month: Some("2026-06".to_string()),
                driver_cd: None,
            }),
            Extension(None),
            Extension(repo),
            Extension(ReadTenant(None)),
        )
        .await
        .expect_err("must fail without a store");
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn month_is_required() {
        let repo: DynKintaiEventsRepo =
            std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo);
        let (status, msg) = unko_gaps(
            Query(UnkoGapsQuery::default()),
            Extension(None),
            Extension(repo),
            Extension(ReadTenant(None)),
        )
        .await
        .expect_err("must fail without month");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("month"), "{msg}");
    }

    #[tokio::test]
    async fn a_malformed_month_is_bad_request() {
        let repo: DynKintaiEventsRepo =
            std::sync::Arc::new(crate::kintai_repo::DisabledKintaiEventsRepo);
        let (status, msg) = unko_gaps(
            Query(UnkoGapsQuery {
                month: Some("nope".to_string()),
                driver_cd: None,
            }),
            Extension(None),
            Extension(repo),
            Extension(ReadTenant(None)),
        )
        .await
        .expect_err("must fail on a malformed month");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(msg.contains("month"), "{msg}");
    }
}
