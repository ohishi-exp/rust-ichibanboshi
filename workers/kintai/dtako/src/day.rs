//! 乗務員CD + 日付 → その日の運行NO・修正用リンク (`GET /api/kintai/day-events` の純粋部分、Refs #205 の 57)。
//!
//! repo ルートの `src/routes/dtako_day.rs` から中身を変えずに移した (Refs #322)。handler (入力の検査・
//! repo 呼び出し・設定の読み方) はオンプレ版と Worker のそれぞれに残り、ここは日の窓と運行への畳み方だけ。
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

use chrono::NaiveDate;
use chrono::NaiveDateTime;
use kintai_kosoku::window::unko_no_start_datetime;

/// `date` が無い・読めないときの 400 の本文。
pub const DATE_INVALID: &str = "date は YYYY-MM-DD で指定してください";

/// `date` の書式検証 (`YYYY-MM-DD`)。実在しない日付 (`2026-02-30` 等) も弾く。
pub fn parse_date(date: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

/// `[date 00:00:00, 翌日 00:00:00)` を `fetch_events_between` にそのまま渡せる形で。
///
/// `succ_opt()` が `None` を返すのは `NaiveDate::MAX` (西暦 262143 年末) だけ —
/// `date` は既に `parse_date` を通っているので事実上到達しない。`unwrap_or(date)`
/// で fail-closed 用の別分岐を持たず、その万一だけ窓幅 0 (0 件) に倒す。
pub fn day_range(date: NaiveDate) -> (String, String) {
    let next = date.succ_opt().unwrap_or(date);
    (format!("{date} 00:00:00"), format!("{next} 00:00:00"))
}

/// `unko_no` (23 桁) の末尾 1 桁 (対象CD、オンプレのみ持つ) を落として `ope_no`
/// (theearth/GCP 側、22 桁) にする。文字数が 1 以下ならそのまま返す (壊れた入力で
/// panic しない)。
///
/// root の `kintai_http_repo.rs` の `onprem_unko_no` と同じ変換を独立して持つ (あちらは
/// `build.rs` の glob の中で、ここから借りると `logic_version` が動く)。
/// **同じ考え方の実装が 2 か所にある**ので、桁の境目 (`ONPREM_CREW_SUFFIX_LEN` =
/// 1) が変わったら両方直すこと。
pub fn to_ope_no(unko_no: &str) -> &str {
    let kept = unko_no.chars().count().saturating_sub(1);
    if kept == 0 {
        return unko_no;
    }
    let cut: usize = unko_no.chars().take(kept).map(char::len_utf8).sum();
    &unko_no[..cut]
}

/// `NaiveDateTime` を theearth の `StartOpe` 書式 (`"YYYY/MM/DD H:mm:ss"`、時は
/// 0 埋めしない) へ。`%-H` は chrono の非 0 埋め修飾子。
pub fn to_start_ope(dt: &NaiveDateTime) -> String {
    dt.format("%Y/%m/%d %-H:%M:%S").to_string()
}

/// 1 運行ぶんのリンクを組む。**中身は全部押せるものだけ** (base URL が空ならその
/// 項目は `null` — 押せない URL は出さない)。`zip` はここに入れない
/// ([`build_zip_request`] 参照)。base URL は社内 nginx (`ryohi_base_url`) と
/// dtako-admin (`dtako_base_url`) で、値は呼び手の設定から渡す。
pub fn build_links(unko_no: &str, ryohi_base_url: &str, dtako_base_url: &str) -> serde_json::Value {
    let ryohi = if ryohi_base_url.is_empty() {
        None
    } else {
        let base = ryohi_base_url.trim_end_matches('/');
        Some(format!("{base}/ryohi-rows/view/{unko_no}"))
    };
    let search = if dtako_base_url.is_empty() {
        None
    } else {
        let base = dtako_base_url.trim_end_matches('/');
        Some(format!("{base}/daily-report-edit"))
    };
    serde_json::json!({ "ryohi": ryohi, "search": search })
}

/// `daily-report-api/zip` を投げるための材料。**リンクではない** — SPA の
/// `authHeaders()` (Bearer token + 専用ヘッダ) が無いと直接開いても失敗するので、
/// `links.search` を開いて人がブラウザ内から検索・クリックする前提の参考値として返す。
/// `start_dt` が組めない (unko_no の先頭12桁が読めない) ときは `None`。
pub fn build_zip_request(
    unko_no: &str,
    start_dt: Option<NaiveDateTime>,
) -> Option<serde_json::Value> {
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
pub fn build_operations(
    rows: &[serde_json::Value],
    ryohi_base_url: &str,
    dtako_base_url: &str,
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
            let links = build_links(&unko_no, ryohi_base_url, dtako_base_url);
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

/// 200 の本文。`events` は読んだ生行をそのまま返す。
pub fn body(
    driver: u64,
    date: NaiveDate,
    operations: Vec<serde_json::Value>,
    events: Vec<serde_json::Value>,
) -> serde_json::Value {
    serde_json::json!({
        "driver_cd": driver,
        "date": date.to_string(),
        "operations": operations,
        "events": events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

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

    /// 元の `dtako_day.rs` の写しを消して `kintai_kosoku::window` のものを使う。読み方が同じことを固定する。
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
        let links = build_links("26060507533000000042861", "", "");
        assert_eq!(links["ryohi"], Value::Null);
        assert_eq!(links["search"], Value::Null);
    }

    #[test]
    fn build_links_builds_both_when_configured_and_never_contains_zip() {
        let links = build_links(
            "26060507533000000042861",
            "https://ryohi.example/",
            "https://dtako.example/",
        );
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
        let ops = build_operations(&rows, "https://ryohi.example", "");
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
        assert!(build_operations(&rows, "", "").is_empty());
    }

    #[test]
    fn body_has_the_four_keys() {
        let d = NaiveDate::from_ymd_opt(2026, 6, 5).unwrap();
        let b = body(1021, d, Vec::new(), vec![json!({"x": 1})]);
        assert_eq!(
            b,
            json!({"driver_cd": 1021, "date": "2026-06-05", "operations": [], "events": [{"x": 1}]})
        );
    }
}
