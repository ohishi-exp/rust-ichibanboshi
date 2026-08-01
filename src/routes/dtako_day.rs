//! 乗務員CD + 日付 → その日の運行NO・全イベント・修正用リンク (Refs #205 の 57)。
//!
//! 値ずれ (オンプレ vs GCP) を見つけたあと直す手順は「theearth から csvdata.zip を
//! 落とす → 社内 nginx (CakePHP) のフォームに入れる」で、そのために要る運行NO と
//! リンクをいま毎回手で組み立てている。ここは**その組み立てだけ**を 1 回の `curl` で
//! 出す — アップロードは自動化しない (CSRF cookie が HttpOnly、実機確認済み)。
//!
//! ## ファイル名がなぜ `dtako_day.rs` か (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/routes/` 配下を**ファイル名の接頭辞**
//! (`kintai`) で拾い、拾われたファイルの内容ハッシュが `logic_version` (`/api/kintai/
//! version` の etag) に畳まれる。この endpoint は既存の生イベント読み出しを**そのまま
//! 再利用するだけ**で `/api/kintai/{daily,kosoku-daily,version}` の応答を一切変えない
//! ので、`kintai` で始まる名前を付けると無関係な deploy まで全乗務員 stale にしてしまう。
//!
//! 同じ理由で [`crate::kintai_http_repo`] の `onprem_unko_no` (unko_no の桁変換) も
//! **import せず、同じロジックをこのファイル内に独立して持つ** — あちらはグロブ対象
//! なので、ここから依存すると変更のたびに向こうを触ったのと同じ扱いになる。
//!
//! ## `ope_no` は 22 桁 (`unko_no` の 23 桁そのままではない)
//!
//! オンプレの `unko_no` は 23 桁 (末尾 1 桁が対象CD)、GCP/theearth 側は 22 桁
//! (`nuxt-dtako-admin` `workers/dtako-scraper-relay/src/theearth-report-client.ts` の
//! `OPE_NO_RE = /^\d{22}$/`、実機確認 2026-08-01)。**`ryohi` リンクと `unko_no`
//! フィールドは 23 桁の `unko_no` をそのまま使う** (社内 nginx 側のキー) が、
//! **`zip_request.ope_no` だけ末尾 1 桁を落として 22 桁にする** — theearth 自身が
//! その形式でしか受け付けないための変換で、GCP/オンプレの桁を取り違えているわけ
//! ではない。**どちらの桁を使っているかフィールド名で読めるよう、23 桁は
//! `unko_no`・22 桁は `ope_no` と呼び分ける** (親レビュー 2026-08-01)。
//!
//! ## `startOpe` の書式 (実機確認 2026-08-01)
//!
//! 同ファイルの `START_OPE_RE = /^\d{4}\/\d{2}\/\d{2} \d{1,2}:\d{2}:\d{2}$/`
//! (スラッシュ区切り、時は 0 埋めしない — 実測値 `"2026/07/07 1:03:16"`)。
//! `unko_no` 先頭 12 桁 (`YYMMDDHHMMSS`) から組む。
//!
//! ## `links` の不変条件: 中身は全部押せる — `zip` はリンクとして出さない
//!
//! `daily-report-api/zip` は SPA の `authHeaders()` (Bearer token +
//! `X-Theearth-Comp-Id`/`X-Theearth-User-B64` ヘッダ) を要求する。ブラウザの素の
//! リンクナビゲーションはカスタムヘッダを送れないので、ログイン済みでもこの URL を
//! 直接開くと失敗する。`daily-report-edit.vue` に `?operationNo=` のような deep-link
//! も無い (乗務員CD/日付で検索し、行ごとの「csvdata.zip」ボタンを押す設計)。
//!
//! **押しても動かない URL を出して注記で打ち消すのは、期待させたうえで取り消す形**
//! (条件8: できないことを黙って期待させない、親レビュー 2026-08-01)。なので
//! `links` には**押せるものだけ**を入れる (`ryohi` / `search`)。`zip` に要る材料
//! (`ope_no`・`start_ope`) はリンクではなく `zip_request` という別フィールドで返し、
//! 「押すものではない」と名前で分かるようにする。
//!
//! ## 運行が無い日
//!
//! `operations` / `events` とも空配列を返す (200)。404 にはしない —
//! `/api/kintai/events` / `/api/kintai/rest-diff` と同じ流儀。

use std::sync::Arc;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use serde::Deserialize;

use crate::config::DtakoDayLinksConfig;
use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::{map_repo_err, parse_driver};

/// `?driver=1021&date=2026-06-05`。両方必須。
#[derive(Debug, Deserialize)]
pub struct DayEventsQuery {
    pub driver: Option<String>,
    pub date: Option<String>,
}

/// `date` の書式検証 (`YYYY-MM-DD`)。実在しない日付 (`2026-02-30` 等) も弾く。
fn parse_date(date: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

/// `[date 00:00:00, 翌日 00:00:00)` を `fetch_events_between` にそのまま渡せる形で。
///
/// `succ_opt()` が `None` を返すのは `NaiveDate::MAX` (西暦 262143 年末) だけ —
/// `date` は既に `parse_date` を通っているので事実上到達しない。`unwrap_or(date)`
/// で fail-closed 用の別分岐を持たず、その万一だけ窓幅 0 (0 件) に倒す。
fn day_range(date: NaiveDate) -> (String, String) {
    let next = date.succ_opt().unwrap_or(date);
    (format!("{date} 00:00:00"), format!("{next} 00:00:00"))
}

/// `unko_no` 先頭 12 桁 (`YYMMDDHHMMSS`) を運行開始日時として読む。
///
/// 実機で 966 運行中パース不能 0・不一致 0 (`kintai_http_repo.rs` の
/// `unko_no_start_date` doc、Refs #205 の 37)。あちらは日付だけを返すが、ここは
/// `startOpe` の秒まで要るので独立して持つ (モジュール doc の「ファイル名がなぜ」参照)。
fn unko_no_start_datetime(unko_no: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(unko_no.get(..12)?, "%y%m%d%H%M%S").ok()
}

/// `unko_no` (23 桁) の末尾 1 桁 (対象CD、オンプレのみ持つ) を落として `ope_no`
/// (theearth/GCP 側、22 桁) にする。文字数が 1 以下ならそのまま返す (壊れた入力で
/// panic しない)。
///
/// `kintai_http_repo.rs` の `onprem_unko_no` と同じ変換を独立して持つ (モジュール
/// doc の「ファイル名がなぜ」参照 — あちらはグロブ対象で `logic_version` が動く)。
/// **同じ考え方の実装が 2 か所にある**ので、桁の境目 (`ONPREM_CREW_SUFFIX_LEN` =
/// 1) が変わったら両方直すこと。
fn to_ope_no(unko_no: &str) -> &str {
    let kept = unko_no.chars().count().saturating_sub(1);
    if kept == 0 {
        return unko_no;
    }
    let cut: usize = unko_no.chars().take(kept).map(char::len_utf8).sum();
    &unko_no[..cut]
}

/// `NaiveDateTime` を theearth の `StartOpe` 書式 (`"YYYY/MM/DD H:mm:ss"`、時は
/// 0 埋めしない) へ。`%-H` は chrono の非 0 埋め修飾子。
fn to_start_ope(dt: &NaiveDateTime) -> String {
    dt.format("%Y/%m/%d %-H:%M:%S").to_string()
}

/// 1 運行ぶんのリンクを組む。**中身は全部押せるものだけ** (base URL が空ならその
/// 項目は `null` — 押せない URL は出さない)。`zip` はここに入れない
/// ([`build_zip_request`] 参照)。
fn build_links(unko_no: &str, cfg: &DtakoDayLinksConfig) -> serde_json::Value {
    let ryohi = if cfg.ryohi_base_url.is_empty() {
        None
    } else {
        let base = cfg.ryohi_base_url.trim_end_matches('/');
        Some(format!("{base}/ryohi-rows/view/{unko_no}"))
    };
    let search = if cfg.dtako_base_url.is_empty() {
        None
    } else {
        let base = cfg.dtako_base_url.trim_end_matches('/');
        Some(format!("{base}/daily-report-edit"))
    };
    serde_json::json!({ "ryohi": ryohi, "search": search })
}

/// `daily-report-api/zip` を投げるための材料。**リンクではない** — SPA の
/// `authHeaders()` (Bearer token + 専用ヘッダ) が無いと直接開いても失敗するので、
/// `links.search` を開いて人がブラウザ内から検索・クリックする前提の参考値として返す。
/// `start_dt` が組めない (unko_no の先頭12桁が読めない) ときは `None`。
fn build_zip_request(unko_no: &str, start_dt: Option<NaiveDateTime>) -> Option<serde_json::Value> {
    let dt = start_dt?;
    Some(serde_json::json!({
        "path": "/daily-report-api/zip",
        "ope_no": to_ope_no(unko_no),
        "start_ope": to_start_ope(&dt),
        "note": "URLを直接開いても取得できない (専用ヘッダが要る)。links.searchを開き、driver_cd/dateで検索して該当行のcsvdata.zipボタンを押す。",
    }))
}

/// `events`(生行) から `unko_no` ごとに 1 運行へ畳む。順序は初出順 (= 時刻順、行は
/// 既に `ORDER BY datetime, source` で来る)。`vehicle` は `dtako_events` 由来の行に
/// しか付かない (`time_card_dtako` 側は常に `null`) ので、同じ運行の行を跨いで拾う。
fn build_operations(
    rows: &[serde_json::Value],
    cfg: &DtakoDayLinksConfig,
) -> Vec<serde_json::Value> {
    let mut order: Vec<String> = Vec::new();
    let mut vehicles: std::collections::HashMap<String, Option<String>> =
        std::collections::HashMap::new();
    for row in rows {
        let Some(unko_no) = row.get("unko_no").and_then(|v| v.as_str()) else {
            continue;
        };
        if !vehicles.contains_key(unko_no) {
            order.push(unko_no.to_string());
            vehicles.insert(unko_no.to_string(), None);
        }
        if let Some(v) = row.get("vehicle").and_then(|v| v.as_str()) {
            vehicles.insert(unko_no.to_string(), Some(v.to_string()));
        }
    }
    order
        .into_iter()
        .map(|unko_no| {
            let vehicle = vehicles.get(&unko_no).cloned().flatten();
            let start_dt = unko_no_start_datetime(&unko_no);
            let run_start = start_dt.map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string());
            let links = build_links(&unko_no, cfg);
            let zip_request = build_zip_request(&unko_no, start_dt);
            serde_json::json!({
                "unko_no": unko_no,
                "run_start": run_start,
                "vehicle": vehicle,
                "links": links,
                "zip_request": zip_request,
            })
        })
        .collect()
}

/// GET /api/kintai/day-events?driver=&date= — 乗務員CD + 日付の運行NO・全イベント・
/// 修正用リンク (Refs #205 の 57)。
///
/// **データ源は `/api/kintai/events` と同じ repo 関数** ([`DynKintaiEventsRepo::
/// fetch_events_between`]) — 日で絞るのはこの呼び出し側で、SQL は増やさない。
pub async fn day_events(
    Query(params): Query<DayEventsQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
    Extension(links_cfg): Extension<Arc<DtakoDayLinksConfig>>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let driver = match parse_driver(params.driver.as_deref().unwrap_or_default()) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "driver は乗務員CD (数字) で指定してください".to_string(),
            ))
        }
    };
    let date = match params.date.as_deref().and_then(parse_date) {
        Some(d) => d,
        None => {
            return Err((
                StatusCode::BAD_REQUEST,
                "date は YYYY-MM-DD で指定してください".to_string(),
            ))
        }
    };
    let (from, to) = day_range(date);
    let rows = repo
        .fetch_events_between(&from, &to, driver)
        .await
        .map_err(map_repo_err)?;
    let operations = build_operations(&rows, &links_cfg);
    let (ops, evs) = (operations.len(), rows.len());
    tracing::info!(driver, %date, ops, evs, "dtako day-events built");
    Ok(Json(serde_json::json!({
        "driver_cd": driver,
        "date": date.to_string(),
        "operations": operations,
        "events": rows,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::routing::get;
    use axum::Router;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    fn cfg(ryohi: &str, dtako: &str) -> Arc<DtakoDayLinksConfig> {
        Arc::new(DtakoDayLinksConfig {
            ryohi_base_url: ryohi.to_string(),
            dtako_base_url: dtako.to_string(),
        })
    }

    #[test]
    fn parse_date_rejects_garbage_and_impossible_dates() {
        assert_eq!(
            parse_date("2026-06-05"),
            NaiveDate::from_ymd_opt(2026, 6, 5)
        );
        assert_eq!(parse_date("2026-02-30"), None, "2月30日は存在しない");
        assert_eq!(parse_date("2026/06/05"), None, "区切りが違う");
        assert_eq!(parse_date(""), None);
    }

    #[test]
    fn day_range_is_a_half_open_24h_window() {
        let d = NaiveDate::from_ymd_opt(2026, 6, 5).unwrap();
        assert_eq!(
            day_range(d),
            (
                "2026-06-05 00:00:00".to_string(),
                "2026-06-06 00:00:00".to_string()
            )
        );
    }

    #[test]
    fn day_range_rolls_over_month_and_year() {
        let d = NaiveDate::from_ymd_opt(2026, 12, 31).unwrap();
        assert_eq!(
            day_range(d),
            (
                "2026-12-31 00:00:00".to_string(),
                "2027-01-01 00:00:00".to_string()
            )
        );
    }

    #[test]
    fn day_range_falls_back_to_a_zero_width_window_at_the_naivedate_boundary() {
        // NaiveDate::MAX の翌日は表現できない (`succ_opt()` が None)。fail-closed
        // 用の別分岐を持たない代わりに、窓幅 0 (= 0 件) に倒れることを固定する。
        assert_eq!(
            day_range(NaiveDate::MAX),
            (
                format!("{} 00:00:00", NaiveDate::MAX),
                format!("{} 00:00:00", NaiveDate::MAX)
            )
        );
    }

    #[test]
    fn unko_no_start_datetime_reads_the_leading_12_digits() {
        let dt = unko_no_start_datetime("26060507533000000042861").unwrap();
        assert_eq!(dt.to_string(), "2026-06-05 07:53:30");
        assert!(unko_no_start_datetime("2602241025060000000272").is_some());
        assert_eq!(unko_no_start_datetime("U1"), None, "12桁に満たない");
        assert_eq!(
            unko_no_start_datetime("269999123456000000"),
            None,
            "日付として不正"
        );
    }

    #[test]
    fn to_ope_no_drops_only_the_crew_suffix() {
        assert_eq!(
            to_ope_no("26060507533000000042861"),
            "2606050753300000004286"
        );
        assert_eq!(to_ope_no("U1"), "U", "2文字でも1文字は落とす");
        assert_eq!(to_ope_no("X"), "X", "1文字は落とさない");
        assert_eq!(to_ope_no(""), "", "空も落とさない");
    }

    #[test]
    fn to_start_ope_does_not_zero_pad_the_hour() {
        let dt = NaiveDateTime::parse_from_str("2026-07-07 01:03:16", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(
            to_start_ope(&dt),
            "2026/07/07 1:03:16",
            "実機値と一致 (時は0埋めなし)"
        );
        let dt2 =
            NaiveDateTime::parse_from_str("2026-07-07 18:31:06", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(to_start_ope(&dt2), "2026/07/07 18:31:06");
    }

    #[test]
    fn build_links_is_null_for_each_item_when_its_base_url_is_empty() {
        let c = DtakoDayLinksConfig {
            ryohi_base_url: String::new(),
            dtako_base_url: String::new(),
        };
        let links = build_links("26060507533000000042861", &c);
        assert_eq!(links["ryohi"], Value::Null);
        assert_eq!(links["search"], Value::Null);
    }

    #[test]
    fn build_links_builds_both_when_configured_and_never_contains_zip() {
        let c = DtakoDayLinksConfig {
            ryohi_base_url: "https://ryohi.example/".to_string(),
            dtako_base_url: "https://dtako.example/".to_string(),
        };
        let links = build_links("26060507533000000042861", &c);
        assert_eq!(
            links["ryohi"],
            json!("https://ryohi.example/ryohi-rows/view/26060507533000000042861"),
            "末尾の / は畳む"
        );
        assert_eq!(
            links["search"],
            json!("https://dtako.example/daily-report-edit")
        );
        assert!(
            links.get("zip").is_none(),
            "links には押せるものしか入れない — zip はここに出さない"
        );
    }

    #[test]
    fn build_zip_request_is_none_when_start_dt_is_missing() {
        assert_eq!(build_zip_request("26060507533000000042861", None), None);
    }

    #[test]
    fn build_zip_request_gives_the_22_digit_ope_no_and_unpadded_start_ope() {
        let dt = NaiveDateTime::parse_from_str("2026-06-05 07:53:30", "%Y-%m-%d %H:%M:%S").unwrap();
        let req = build_zip_request("26060507533000000042861", Some(dt)).unwrap();
        assert_eq!(req["path"], json!("/daily-report-api/zip"));
        assert_eq!(req["ope_no"], json!("2606050753300000004286"), "23桁→22桁");
        assert_eq!(
            req["start_ope"],
            json!("2026/06/05 7:53:30"),
            "時は0埋めなし"
        );
        assert!(req["note"].as_str().unwrap().contains("links.search"));
    }

    fn timecard_row(datetime: &str, driver: i64) -> Value {
        json!({
            "datetime": datetime, "end_datetime": null, "driver_id": driver,
            "source": "timecard", "state": "始業", "unko_no": null, "vehicle": null
        })
    }

    fn dtako_table_row(datetime: &str, driver: i64, unko_no: &str) -> Value {
        json!({
            "datetime": datetime, "end_datetime": null, "driver_id": driver,
            "source": "dtako", "state": "運行開始", "unko_no": unko_no, "vehicle": null
        })
    }

    fn dtako_events_row(
        datetime: &str,
        end: &str,
        driver: i64,
        unko_no: &str,
        vehicle: &str,
    ) -> Value {
        json!({
            "datetime": datetime, "end_datetime": end, "driver_id": driver,
            "source": "dtako_events", "state": "休息", "unko_no": unko_no, "vehicle": vehicle
        })
    }

    #[test]
    fn build_operations_groups_by_unko_no_and_borrows_vehicle_from_dtako_events() {
        let rows = vec![
            timecard_row("2026-06-05 07:00:00", 1021),
            dtako_table_row("2026-06-05 07:53:30", 1021, "26060507533000000042861"),
            dtako_events_row(
                "2026-06-05 08:00:00",
                "2026-06-05 08:10:00",
                1021,
                "26060507533000000042861",
                "長崎100か4286",
            ),
        ];
        let c = DtakoDayLinksConfig {
            ryohi_base_url: "https://ryohi.example".to_string(),
            dtako_base_url: String::new(),
        };
        let ops = build_operations(&rows, &c);
        assert_eq!(
            ops.len(),
            1,
            "同じ unko_no は1件に畳む (timecard 行は数えない)"
        );
        assert_eq!(ops[0]["unko_no"], json!("26060507533000000042861"));
        assert_eq!(ops[0]["run_start"], json!("2026-06-05 07:53:30"));
        assert_eq!(
            ops[0]["vehicle"],
            json!("長崎100か4286"),
            "time_card_dtako 側は vehicle=null でも dtako_events 側から拾う"
        );
        assert!(ops[0]["links"]["ryohi"]
            .as_str()
            .unwrap()
            .ends_with("/ryohi-rows/view/26060507533000000042861"));
        assert!(
            ops[0]["links"].get("zip").is_none(),
            "links には zip を入れない (押せるものだけ)"
        );
        assert_eq!(
            ops[0]["zip_request"]["ope_no"],
            json!("2606050753300000004286")
        );
    }

    #[test]
    fn build_operations_is_empty_when_no_row_has_an_unko_no() {
        let rows = vec![timecard_row("2026-06-05 07:00:00", 1021)];
        let c = cfg("", "");
        assert!(build_operations(&rows, &c).is_empty());
    }

    /// 呼び出し引数を記録し、仕込んだ結果を返す mock (`tests/kintai_events_test.rs`
    /// の `MockEventsRepo` と同じ形)。
    struct MockRepo {
        rows: Vec<Value>,
    }

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for MockRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            Ok(self.rows.clone())
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("day-events は全乗務員を読まない")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("day-events はフェリーを読まない")
        }
    }

    struct FailingRepo;

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for FailingRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            Err(crate::kintai_repo::KintaiRepoError::QueryFailed(
                "boom".to_string(),
            ))
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }

        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("unused")
        }
    }

    fn app(repo: DynKintaiEventsRepo, links_cfg: Arc<DtakoDayLinksConfig>) -> Router {
        Router::new()
            .route("/kintai/day-events", get(day_events))
            .layer(Extension(repo))
            .layer(Extension(links_cfg))
    }

    async fn get_json(router: Router, uri: &str) -> (StatusCode, Value) {
        let res = router
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        // エラー応答は `(StatusCode, String)` の素の文字列 body (JSON ではない)。
        // 成功応答だけ JSON として読み、それ以外はテキストのまま Value::String に包む
        let body: Value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).to_string()))
        };
        (status, body)
    }

    #[tokio::test]
    async fn day_events_returns_operations_with_links_and_zip_request() {
        let rows = vec![
            dtako_table_row("2026-06-05 07:53:30", 1021, "26060507533000000042861"),
            dtako_events_row(
                "2026-06-05 08:00:00",
                "2026-06-05 08:10:00",
                1021,
                "26060507533000000042861",
                "長崎100か4286",
            ),
        ];
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows });
        let router = app(repo, cfg("https://ryohi.example", "https://dtako.example"));
        let (status, body) =
            get_json(router, "/kintai/day-events?driver=1021&date=2026-06-05").await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["driver_cd"], json!(1021));
        assert_eq!(body["date"], json!("2026-06-05"));
        assert_eq!(body["operations"].as_array().unwrap().len(), 1);
        assert_eq!(body["events"].as_array().unwrap().len(), 2);
        let op = &body["operations"][0];
        assert!(op["links"]["ryohi"].is_string());
        assert!(op["links"]["search"].is_string());
        assert!(op["links"].get("zip").is_none(), "links に zip を出さない");
        assert!(op["zip_request"]["ope_no"].is_string());
    }

    #[tokio::test]
    async fn day_events_returns_empty_arrays_when_no_operation_that_day() {
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router = app(repo, cfg("", ""));
        let (status, body) =
            get_json(router, "/kintai/day-events?driver=1021&date=2026-06-05").await;
        assert_eq!(status, StatusCode::OK, "運行が無い日も200 (404にしない)");
        assert_eq!(body["operations"], json!([]));
        assert_eq!(body["events"], json!([]));
    }

    #[tokio::test]
    async fn day_events_rejects_missing_or_non_numeric_driver() {
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router = app(repo, cfg("", ""));
        let (status, _) = get_json(router, "/kintai/day-events?date=2026-06-05").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let repo2: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router2 = app(repo2, cfg("", ""));
        let (status2, _) = get_json(router2, "/kintai/day-events?driver=abc&date=2026-06-05").await;
        assert_eq!(status2, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn day_events_rejects_missing_or_malformed_date() {
        let repo: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router = app(repo, cfg("", ""));
        let (status, _) = get_json(router, "/kintai/day-events?driver=1021").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);

        let repo2: DynKintaiEventsRepo = Arc::new(MockRepo { rows: Vec::new() });
        let router2 = app(repo2, cfg("", ""));
        let (status2, _) =
            get_json(router2, "/kintai/day-events?driver=1021&date=2026-13-40").await;
        assert_eq!(status2, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn day_events_maps_repo_failure_to_bad_gateway() {
        let repo: DynKintaiEventsRepo = Arc::new(FailingRepo);
        let router = app(repo, cfg("", ""));
        let (status, _) = get_json(router, "/kintai/day-events?driver=1021&date=2026-06-05").await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
    }
}
