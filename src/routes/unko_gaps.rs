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

use std::collections::{HashMap, HashSet};

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::Datelike;
use serde::Deserialize;

use crate::kintai_http_repo::unko_no_start_date;
use crate::kintai_push::{KintaiPgStore, JST_OFFSET_SECONDS, MONTH_OPERATIONS_SQL, PUSHED_SOURCES};
use crate::kintai_repo::{month_range, DynKintaiEventsRepo};
use crate::routes::kintai::is_valid_month;
use crate::routes::kintai_timecard::{DynKintaiPgStore, ReadTenant};

/// 応答に載せる乗務員数の上限 (`UnkoDiffDriverSplit` の `MAX_UNKO_DIFF_DRIVERS`
/// と同じ思想 — 桁違いの入力が来たときに応答を膨らませない蓋)。
pub const MAX_UNKO_GAPS_DRIVERS: usize = 300;

/// 乗務員 1 人あたり (および `unknown_driver_unko_nos`) の運行NO 件数の上限。
/// 実測 (2026-06) は候補 1 人あたり 1 件だが、同じ理由で蓋を置く。
pub const MAX_UNKO_GAPS_PER_DRIVER: usize = 200;

#[derive(Debug, Default, Deserialize)]
pub struct UnkoGapsQuery {
    pub month: Option<String>,
    pub driver_cd: Option<i64>,
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, msg.into())
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

/// `crate::kintai_repo::month_range` が返す `"YYYY-MM-DD HH:MM:SS"` (JST 壁時計) を
/// `DateTime<FixedOffset>` に。
fn parse_jst(s: &str) -> Option<chrono::DateTime<chrono::FixedOffset>> {
    let naive = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()?;
    let off = chrono::FixedOffset::east_opt(JST_OFFSET_SECONDS)?;
    naive.and_local_timezone(off).single()
}

/// オンプレの `unko_no` から対象CD (末尾 1 文字) を落として運行NO (GCP と同じ
/// 22 桁) にする。`kintai_http_repo::onprem_unko_no` と同じ規則 (`ONPREM_CREW_SUFFIX_LEN`
/// = 1) だが、あちらは private + glob 内なので呼べない — ここで同じ 1 行を
/// 再現する。規則そのものは固定値でドリフトの心配は無い。
fn drop_crew_suffix(unko_no: &str) -> String {
    let kept = unko_no.chars().count().saturating_sub(1);
    if kept == 0 {
        return unko_no.to_string();
    }
    let cut: usize = unko_no.chars().take(kept).map(char::len_utf8).sum();
    unko_no[..cut].to_string()
}

fn db_err(e: sqlx::Error) -> (StatusCode, String) {
    (
        StatusCode::BAD_GATEWAY,
        format!("kintai.kintai_events unko read failed: {e}"),
    )
}

/// 1 乗務員ぶんの応答行。
#[derive(Debug, Clone, PartialEq, Eq)]
struct DriverGaps {
    driver_cd: String,
    unko_nos: Vec<String>,
    truncated: bool,
}

fn cap_sorted(mut v: Vec<String>, max: usize) -> (Vec<String>, bool) {
    v.sort_unstable();
    let truncated = v.len() > max;
    v.truncate(max);
    (v, truncated)
}

/// I/O から切り離した判定・整形の核。**DB も alc も見ない** — 呼び出し側が
/// 引いてきた材料だけを受け取り、gap の抽出・乗務員別への分割・
/// `also_in_month` の絞り込み・上限の適用をやる。
///
/// `onprem_seen` は対象月の窓に在るオンプレの `unko_no` (対象CD 落とし済み、
/// [`drop_crew_suffix`]) の集合、`onprem_in_month` は乗務員別の件数
/// (どちらも [`MONTH_OPERATIONS_SQL`] の行から作る)。`gcp_driver_cds` は
/// [`crate::kintai_http_repo::collected_etag_driver_cds`] の生の値
/// (窓ぜんたい — 対象月に絞る前)。
fn build_gaps(
    year: i32,
    month_num: u32,
    onprem_seen: &HashSet<String>,
    onprem_in_month: &HashMap<i64, usize>,
    gcp_driver_cds: &HashMap<String, Vec<String>>,
    driver_cd_filter: Option<i64>,
) -> (Vec<DriverGaps>, bool, Vec<String>, bool) {
    let mut by_driver: HashMap<String, Vec<String>> = HashMap::new();
    let mut unknown_driver: Vec<String> = Vec::new();
    for (unko_no, driver_cds) in gcp_driver_cds {
        if onprem_seen.contains(unko_no.as_str()) {
            continue; // 一致済み — 漏れではない
        }
        let Some(start) = unko_no_start_date(unko_no) else {
            continue; // 開始日が読めない = 対象月かどうか判定できない (安全側で外す)
        };
        if start.year() != year || start.month() != month_num {
            continue; // 対象月に始まった運行だけ (窓の外の運行を混ぜない)
        }
        if driver_cds.is_empty() {
            unknown_driver.push(unko_no.clone());
        } else {
            for dcd in driver_cds {
                by_driver
                    .entry(dcd.clone())
                    .or_default()
                    .push(unko_no.clone());
            }
        }
    }

    let is_candidate = |cd: &str| -> bool {
        match driver_cd_filter {
            // 乗務員CD 指定時はその 1 人だけ (バケットは問わない — 呼び出し側は
            // 既に候補と分かっている前提)
            Some(want) => cd.trim().parse::<i64>() == Ok(want),
            // 省略時は also_in_month (= 対象月にオンプレの運行も在る) の候補全員
            None => cd
                .trim()
                .parse::<i64>()
                .ok()
                .and_then(|n| onprem_in_month.get(&n))
                .is_some_and(|&n| n > 0),
        }
    };

    let mut driver_rows: Vec<(String, Vec<String>)> = by_driver
        .into_iter()
        .filter(|(cd, _)| is_candidate(cd))
        .collect();
    driver_rows.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));
    let drivers_truncated = driver_rows.len() > MAX_UNKO_GAPS_DRIVERS;
    driver_rows.truncate(MAX_UNKO_GAPS_DRIVERS);

    let drivers: Vec<DriverGaps> = driver_rows
        .into_iter()
        .map(|(driver_cd, unko_nos)| {
            let (unko_nos, truncated) = cap_sorted(unko_nos, MAX_UNKO_GAPS_PER_DRIVER);
            DriverGaps {
                driver_cd,
                unko_nos,
                truncated,
            }
        })
        .collect();

    let (unknown_driver, unknown_driver_truncated) =
        cap_sorted(unknown_driver, MAX_UNKO_GAPS_PER_DRIVER);
    (
        drivers,
        drivers_truncated,
        unknown_driver,
        unknown_driver_truncated,
    )
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
    let month = q
        .month
        .clone()
        .ok_or_else(|| bad_request("month は必須です (YYYY-MM)"))?;
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    let st = store(&pg)?;
    let tenant = read_tenant_of(read_tenant, st.tenant_id())?;

    let bad_month = || bad_request(format!("month が壊れています: {month}"));
    let year: i32 = month
        .get(..4)
        .and_then(|s| s.parse().ok())
        .ok_or_else(bad_month)?;
    let month_num: u32 = month
        .get(5..7)
        .and_then(|s| s.parse().ok())
        .ok_or_else(bad_month)?;

    // オンプレ側 (押し込み済み `kintai.kintai_events`)。窓は kintai_fold の
    // measure_unko_diff が使う month_range と同じにして、既存の onprem_in_month
    // の実測 (2026-06: 1445→5, 1740→10) と揃える。tenant_id は解決済みの読み
    // テナントを bind する (モジュール docs — self.tenant_id を使う既存メソッドは
    // 使わない)
    let (from_s, to_s) = month_range(&month).ok_or_else(bad_month)?;
    let from = parse_jst(&from_s).ok_or_else(bad_month)?;
    let to = parse_jst(&to_s).ok_or_else(bad_month)?;
    use sqlx::Row;
    let rows = sqlx::query(MONTH_OPERATIONS_SQL)
        .bind(tenant)
        .bind(from)
        .bind(to)
        .bind(&PUSHED_SOURCES[..])
        .fetch_all(st.pool())
        .await
        .map_err(db_err)?;
    let mut onprem_in_month: HashMap<i64, usize> = HashMap::new();
    let mut onprem_seen: HashSet<String> = HashSet::new();
    for r in &rows {
        let driver_cd: i64 = r.get("driver_cd");
        let unko_no: String = r.get("unko_no");
        *onprem_in_month.entry(driver_cd).or_default() += 1;
        onprem_seen.insert(drop_crew_suffix(&unko_no));
    }

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
    let driver_cds_available = !gcp_driver_cds.is_empty();

    let (drivers, drivers_truncated, unknown_driver_unko_nos, unknown_driver_truncated) =
        if gcp_etags_available {
            build_gaps(
                year,
                month_num,
                &onprem_seen,
                &onprem_in_month,
                &gcp_driver_cds,
                q.driver_cd,
            )
        } else {
            (Vec::new(), false, Vec::new(), false)
        };

    let n = drivers.len();
    tracing::info!(n, gcp_etags_available, "kintai unko-gaps read");
    Ok(Json(serde_json::json!({
        "month": month,
        "driver_cd": q.driver_cd,
        "gcp_etags_available": gcp_etags_available,
        "driver_cds_available": driver_cds_available,
        "unko_no_digits": 22,
        "drivers": drivers.iter().map(|d| serde_json::json!({
            "driver_cd": d.driver_cd,
            "unko_nos": d.unko_nos,
            "truncated": d.truncated,
        })).collect::<Vec<_>>(),
        "drivers_truncated": drivers_truncated,
        "unknown_driver_unko_nos": unknown_driver_unko_nos,
        "unknown_driver_unko_nos_truncated": unknown_driver_truncated,
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })))
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

    // ── drop_crew_suffix ────────────────────────────────────────────────────

    #[test]
    fn drop_crew_suffix_drops_only_the_last_character() {
        assert_eq!(
            drop_crew_suffix("26060610055500000023021"),
            "2606061005550000002302"
        );
        assert_eq!(drop_crew_suffix(""), "");
        assert_eq!(drop_crew_suffix("1"), "1");
    }

    // ── build_gaps (I/O から切り離した核) ────────────────────────────────────

    fn gcp(pairs: &[(&str, &[&str])]) -> HashMap<String, Vec<String>> {
        pairs
            .iter()
            .map(|(u, ds)| (u.to_string(), ds.iter().map(|s| s.to_string()).collect()))
            .collect()
    }

    /// 22 桁の GCP 側 `unko_no`。先頭 6 桁が `YYMMDD` (運行開始日)。
    fn u(ymd: &str, seq: u32) -> String {
        format!("{ymd}{seq:016}")
    }

    #[test]
    fn a_gap_is_attributed_to_its_driver_when_onprem_has_that_month() {
        let seen = HashSet::new(); // オンプレに何も無い = 一致するものが無い
        let mut in_month = HashMap::new();
        in_month.insert(1445, 5); // also_in_month の実測と同じ形 (onprem_in_month > 0)
        let cds = gcp(&[(&u("260610", 1), &["1445"])]);

        let (drivers, dt, unknown, ut) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(!dt && !ut);
        assert!(unknown.is_empty());
        assert_eq!(drivers.len(), 1, "{drivers:?}");
        assert_eq!(drivers[0].driver_cd, "1445");
        assert_eq!(drivers[0].unko_nos, vec![u("260610", 1)]);
    }

    #[test]
    fn a_matched_unko_no_is_not_a_gap() {
        let mut seen = HashSet::new();
        let key = u("260610", 1);
        seen.insert(key.clone()); // オンプレ側に (対象CD 落とし後) 同じ値がある
        let mut in_month = HashMap::new();
        in_month.insert(1445, 1);
        let cds = gcp(&[(&key, &["1445"])]);

        let (drivers, _, unknown, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(drivers.is_empty(), "{drivers:?}");
        assert!(unknown.is_empty());
    }

    #[test]
    fn a_driver_without_onprem_this_month_is_not_a_default_candidate() {
        let seen = HashSet::new();
        let in_month = HashMap::new(); // 9999 は対象月にオンプレの運行が無い
        let cds = gcp(&[(&u("260615", 1), &["9999"])]);

        let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(
            drivers.is_empty(),
            "省略時は also_in_month だけ: {drivers:?}"
        );
    }

    #[test]
    fn an_explicit_driver_cd_bypasses_the_also_in_month_bucket() {
        let seen = HashSet::new();
        let in_month = HashMap::new(); // 9999 は候補ではないが、明示指定なら返す
        let cds = gcp(&[(&u("260615", 1), &["9999"])]);

        let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, Some(9999));
        assert_eq!(drivers.len(), 1, "{drivers:?}");
        assert_eq!(drivers[0].driver_cd, "9999");
    }

    #[test]
    fn an_explicit_driver_cd_that_has_no_gap_returns_empty_not_an_error() {
        let seen = HashSet::new();
        let in_month = HashMap::new();
        let cds = gcp(&[(&u("260615", 1), &["9999"])]);

        let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, Some(1));
        assert!(drivers.is_empty(), "{drivers:?}");
    }

    #[test]
    fn a_gap_outside_the_target_month_is_excluded() {
        let seen = HashSet::new();
        let mut in_month = HashMap::new();
        in_month.insert(1445, 3);
        // 開始日が前月 (etags の窓は読取日で引くので前月以前の運行が混ざりうる —
        // UnkoDiff::gcp_only_in_month の docs と同じ現象)
        let cds = gcp(&[(&u("260531", 1), &["1445"])]);

        let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(drivers.is_empty(), "対象月の外は数えない: {drivers:?}");
    }

    #[test]
    fn an_unparseable_start_date_is_excluded_safely() {
        let seen = HashSet::new();
        let mut in_month = HashMap::new();
        in_month.insert(1445, 1);
        let cds = gcp(&[("not-a-date", &["1445"])]);

        let (drivers, _, unknown, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(drivers.is_empty());
        assert!(
            unknown.is_empty(),
            "判定できない = 安全側で外す。候補にも unknown にも出さない"
        );
    }

    #[test]
    fn driver_cds_empty_falls_into_unknown_driver_not_silently_dropped() {
        let seen = HashSet::new();
        let mut in_month = HashMap::new();
        in_month.insert(1445, 1);
        // alc が driver_cds を返さない環境 (前方互換フィールドの既定 = 空配列)
        let cds = gcp(&[(&u("260610", 1), &[])]);

        let (drivers, _, unknown, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(
            drivers.is_empty(),
            "乗務員が引けないので drivers には出ない"
        );
        assert_eq!(unknown, vec![u("260610", 1)], "空を候補無しに読ませない");
    }

    #[test]
    fn a_two_crew_operation_attributes_the_gap_to_both_drivers() {
        let seen = HashSet::new();
        let mut in_month = HashMap::new();
        in_month.insert(1445, 1);
        in_month.insert(1740, 1);
        let cds = gcp(&[(&u("260610", 1), &["1445", "1740"])]);

        let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        let mut got: Vec<&str> = drivers.iter().map(|d| d.driver_cd.as_str()).collect();
        got.sort_unstable();
        assert_eq!(got, vec!["1445", "1740"]);
    }

    #[test]
    fn driver_count_above_the_cap_is_truncated_and_flagged() {
        let seen = HashSet::new();
        let mut in_month = HashMap::new();
        let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
        for i in 0..(MAX_UNKO_GAPS_DRIVERS + 5) {
            let cd = (2000 + i as i64).to_string();
            in_month.insert(2000 + i as i64, 1);
            pairs.push((u("260610", i as u32), vec![cd]));
        }
        let cds: HashMap<String, Vec<String>> = pairs.into_iter().collect();

        let (drivers, truncated, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(truncated);
        assert_eq!(drivers.len(), MAX_UNKO_GAPS_DRIVERS);
    }

    #[test]
    fn unko_no_count_above_the_cap_is_truncated_and_flagged_per_driver() {
        let seen = HashSet::new();
        let mut in_month = HashMap::new();
        in_month.insert(1445, 1);
        let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
        for i in 0..(MAX_UNKO_GAPS_PER_DRIVER + 5) {
            pairs.push((u("260610", i as u32), vec!["1445".to_string()]));
        }
        let cds: HashMap<String, Vec<String>> = pairs.into_iter().collect();

        let (drivers, _, _, _) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert_eq!(drivers.len(), 1);
        assert!(drivers[0].truncated);
        assert_eq!(drivers[0].unko_nos.len(), MAX_UNKO_GAPS_PER_DRIVER);
    }

    #[test]
    fn unknown_driver_count_above_the_cap_is_truncated_and_flagged() {
        let seen = HashSet::new();
        let in_month = HashMap::new();
        let mut pairs: Vec<(String, Vec<String>)> = Vec::new();
        for i in 0..(MAX_UNKO_GAPS_PER_DRIVER + 5) {
            pairs.push((u("260610", i as u32), Vec::new()));
        }
        let cds: HashMap<String, Vec<String>> = pairs.into_iter().collect();

        let (_, _, unknown, truncated) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(truncated);
        assert_eq!(unknown.len(), MAX_UNKO_GAPS_PER_DRIVER);
    }

    #[test]
    fn empty_gcp_data_yields_no_drivers_and_no_unknown() {
        let seen = HashSet::new();
        let in_month = HashMap::new();
        let cds = HashMap::new();
        let (drivers, dt, unknown, ut) = build_gaps(2026, 6, &seen, &in_month, &cds, None);
        assert!(drivers.is_empty() && !dt);
        assert!(unknown.is_empty() && !ut);
    }
}
