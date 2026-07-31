//! 運行を**読取日**へ引き当てる (Refs #205 の 42)。
//!
//! **観測の口で、判定には入らない。** 勤務も拘束も畳まない — `dtako_rows` の行を
//! 運行ごとに並べ、読取日で束ねるだけ。
//!
//! ## 何のためにあるのか
//!
//! 値のずれた勤務は**該当の読取日を取り直せば直る**ことが実測で確定している
//! (乗務員 1107 / 2026-06、3 回とも同じ形):
//!
//! ```text
//! 06-03 を取り直した   break 87 → 76 / working 537 → 548   ← オンプレと一致
//! 06-08 を取り直した   break 97 → 85 / working 518 → 530
//! 06-29 を取り直した   break 29 → 18 / working 548 → 559
//! 取り直していない日   変化なし
//! ```
//!
//! 原因はデジタコのイベント分類 (休憩⇄運転) を後から編集したのに、alc の R2 の CSV が
//! 古いまま残っていたこと。**休息のずれではない** ([`crate::kintai_rest_diff`] の実測で
//! `mismatch` が 0 件だったことから絞り込めた。)
//!
//! ところが**取り直す読取日を人が手で探していた** — ずれは
//! `乗務員CD | 暦日 | 勤務開始時刻` で出るのに、**読取日は運行の属性**で、しかも
//! **勤務の日とは一致しない**。長距離は 1 運行が数日〜10 日を覆い、読取日は運行終了の
//! 後になる (実測: 運行日 06-24 → 読取日 07-06、最大 +11 日)。ここがこの口の仕事。
//!
//! ## 単位は「運行」であって「勤務」ではない
//!
//! **取り直す単位は読取日で、読取日は運行の属性**だから。勤務は休息で切るので運行と
//! 1:1 にならず (#205 の 41 がまさにその話)、勤務を単位にすると
//! [`crate::kosoku`] を通すことになる = **判定ロジックに乗る**。
//!
//! 代わりに**運行が覆う日付の範囲** ([`ReadingDateItem::run_start_date`] 〜
//! [`ReadingDateItem::run_end_date`]) を返す。ずれた `乗務員CD | 暦日` に対して
//! 「その日を覆う運行」を引けるので、勤務を畳み直さずに読取日へ辿り着ける。
//!
//! ## 読み先は**オンプレの `dtako_rows`**。alc は呼ばない
//!
//! alc の `dtako_operations` も `reading_date` を持つが、**この口の消費者はオンプレ**で、
//! オンプレは alc を呼べない (`[kintai_events] source = "mariadb"` なので
//! [`crate::kintai_http_repo::HttpKintaiEventsRepo`] を構築すらしていない)。
//! そして**勤務の側はオンプレの MariaDB からしか出ない** — 別システムの日付と
//! 突き合わせる形にすると、#205 の 37 で `運行NO` の正規化に費やしたのと同じ種類の
//! 問題を自分で作ることになる。
//!
//! **値が割れる心配は無い。** alc の `reading_date` もオンプレの `読取日` も、
//! デジタコの CSV ヘッダ `読取日` をそのまま取り込んだ**同じベンダー列**:
//!
//! ```text
//! alc      crates/alc-csv-parser/src/kudgivt.rs:50
//!            let reading_date = require_col(headers, "読取日", &mut missing);
//! CakePHP  html/app/src/Controller/DtakoEventsController.php:2055
//!            in_array($dd_title1[$kk1], ['読取日','運行日', …])
//! ```

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

/// 応答に載せる運行の上限。総数は [`ReadingDates::total`] に別に返す
/// (`rest_diff` / `unko_diff` と同じ作法)。
///
/// **2,000 は実測の約 2 倍。** 2026-06 の etags の item 総数が 1,130 件だった
/// (`crate::kintai_http_repo` の `unko_diff` の docs) ので、月ぶんの運行はその桁。
/// 束ねた [`ReadingDates::by_reading_date`] は**この上限で切られない**ので、
/// 「どの日を取り直すか」は切られても答えが出る。
pub const MAX_READING_DATES: usize = 2000;

/// 1 運行ぶんの引き当て。
#[derive(Debug, Serialize)]
pub struct ReadingDateItem {
    /// `dtako_rows.対象乗務員CD`。**2 名乗務は運行NO ごと別行**になるので、
    /// ここは 1 つ (`rest_diff` の `driver_cds` と違う)。読めなければ `null`。
    pub driver_cd: Option<i64>,
    pub unko_no: String,
    /// **取り直す日** (`dtako_rows.読取日`)。読めなければ `null`。
    pub reading_date: Option<String>,
    /// `dtako_rows.運行日`。読めなければ `null`。
    pub run_date: Option<String>,
    /// 出庫日時の日付。**運行が覆う範囲の始まり。**
    pub run_start_date: Option<String>,
    /// 帰庫日時の日付。**運行が覆う範囲の終わり。** 未帰庫なら `null`。
    pub run_end_date: Option<String>,
    /// 出庫日時 (秒まで)。日付だけでは足りないとき用。
    pub departure_at: Option<String>,
    /// 帰庫日時 (秒まで)。
    pub return_at: Option<String>,
}

/// 引き当ての結果ぜんたい。
#[derive(Debug, Serialize)]
pub struct ReadingDates {
    /// **読取日 → その日に読み取られた運行NO** (どちらも昇順・重複無し)。
    /// **[`MAX_READING_DATES`] で切られない** — 「どの日を取り直すか」が
    /// この口の答えなので、切ってはいけない。
    pub by_reading_date: BTreeMap<String, Vec<String>>,
    /// 運行 (先頭 [`MAX_READING_DATES`] 件)。
    pub items: Vec<ReadingDateItem>,
    /// 運行の**総数**。`items` が切られたことが分かるように別に返す。
    pub total: usize,
    /// **読取日が引けなかった運行の数。** 0 でないときは
    /// [`ReadingDates::by_reading_date`] がその分だけ答えを持っていない。
    pub unknown_reading_date: usize,
    /// 突合に使えなかった行数 (`運行NO` が空・重複)。
    pub skipped_rows: usize,
}

fn str_field<'a>(row: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    row.get(key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

/// `YYYY-MM-DD HH:MM:SS` の日付部分。日付だけの値はそのまま返す。
fn date_of(dt: Option<&str>) -> Option<String> {
    dt.map(|s| s.split(' ').next().unwrap_or(s).to_string())
}

/// `dtako_rows` の行から、運行 → 読取日の引き当てを組む。
///
/// `rows` は
/// [`crate::kintai_repo::KintaiEventsApi::fetch_operation_reading_dates_between`]
/// が返す形 (`driver_cd` / `unko_no` / `reading_date` / `run_date` /
/// `departure_at` / `return_at`)。
pub fn reading_dates(rows: &[serde_json::Value]) -> ReadingDates {
    let mut items: Vec<ReadingDateItem> = Vec::new();
    let mut by_reading_date: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut seen: BTreeSet<(Option<i64>, String)> = BTreeSet::new();
    let mut unknown_reading_date = 0usize;
    let mut skipped_rows = 0usize;
    for row in rows {
        let Some(unko_no) = str_field(row, "unko_no") else {
            skipped_rows += 1;
            continue;
        };
        let driver_cd = row.get("driver_cd").and_then(|v| v.as_i64());
        // 同じ (乗務員, 運行) が 2 行来ても 1 件に畳む (取り込みが 2 回走った形)
        if !seen.insert((driver_cd, unko_no.to_string())) {
            skipped_rows += 1;
            continue;
        }
        let reading_date = str_field(row, "reading_date").map(str::to_string);
        match &reading_date {
            Some(d) => {
                by_reading_date
                    .entry(d.clone())
                    .or_default()
                    .insert(unko_no.to_string());
            }
            // **推測で埋めない。** 運行NO の先頭 6 桁は運行開始日であって読取日ではない
            None => unknown_reading_date += 1,
        }
        let departure_at = str_field(row, "departure_at").map(str::to_string);
        let return_at = str_field(row, "return_at").map(str::to_string);
        items.push(ReadingDateItem {
            driver_cd,
            unko_no: unko_no.to_string(),
            reading_date,
            run_date: str_field(row, "run_date").map(str::to_string),
            run_start_date: date_of(departure_at.as_deref()),
            run_end_date: date_of(return_at.as_deref()),
            departure_at,
            return_at,
        });
    }
    // 並びは乗務員CD → 運行NO (SQL と同じだが、上流の並びに依存しない)
    items.sort_by(|a, b| (a.driver_cd, &a.unko_no).cmp(&(b.driver_cd, &b.unko_no)));
    let total = items.len();
    items.truncate(MAX_READING_DATES);
    ReadingDates {
        by_reading_date: by_reading_date
            .into_iter()
            .map(|(d, u)| (d, u.into_iter().collect()))
            .collect(),
        items,
        total,
        unknown_reading_date,
        skipped_rows,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(driver: i64, unko: &str, reading: &str, dep: &str, ret: &str) -> serde_json::Value {
        json!({
            "driver_cd": driver,
            "unko_no": unko,
            "reading_date": reading,
            "run_date": dep.split(' ').next().unwrap_or(dep),
            "departure_at": dep,
            "return_at": ret,
        })
    }

    /// **読取日は運行日と一致しない。** 実測の形 (運行日 06-24 → 読取日 07-06) が
    /// そのまま出ること。
    #[test]
    fn a_long_run_maps_to_a_reading_date_days_later() {
        let rows = vec![row(
            1107,
            "26062408000000000011071",
            "2026-07-06",
            "2026-06-24 08:00:00",
            "2026-07-04 19:30:00",
        )];
        let d = reading_dates(&rows);
        assert_eq!(d.total, 1);
        let it = &d.items[0];
        assert_eq!(it.driver_cd, Some(1107));
        assert_eq!(it.reading_date.as_deref(), Some("2026-07-06"));
        assert_eq!(it.run_start_date.as_deref(), Some("2026-06-24"));
        assert_eq!(it.run_end_date.as_deref(), Some("2026-07-04"));
        // 取り直す日はこれ 1 つ
        assert_eq!(
            d.by_reading_date.get("2026-07-06").map(Vec::as_slice),
            Some(["26062408000000000011071".to_string()].as_slice())
        );
        assert_eq!(d.unknown_reading_date, 0);
        assert_eq!(d.skipped_rows, 0);
    }

    /// 同じ読取日の運行は 1 つの桶にまとまる (取り直しは日単位なので)。
    #[test]
    fn runs_read_on_the_same_day_share_a_bucket() {
        let rows = vec![
            row(
                1107,
                "26060308000000000011071",
                "2026-06-03",
                "2026-06-03 08:00:00",
                "2026-06-03 19:00:00",
            ),
            row(
                1018,
                "26060307000000000010181",
                "2026-06-03",
                "2026-06-03 07:00:00",
                "2026-06-03 18:00:00",
            ),
        ];
        let d = reading_dates(&rows);
        assert_eq!(d.by_reading_date.len(), 1);
        assert_eq!(d.by_reading_date["2026-06-03"].len(), 2);
        assert_eq!(d.total, 2);
    }

    /// **読取日が引けない運行は数える。推測で埋めない。**
    #[test]
    fn a_run_without_a_reading_date_is_counted_not_guessed() {
        let rows = vec![json!({
            "driver_cd": 1412,
            "unko_no": "26060208000000000014121",
            "reading_date": serde_json::Value::Null,
            "departure_at": "2026-06-02 08:00:00",
        })];
        let d = reading_dates(&rows);
        assert_eq!(d.total, 1);
        assert_eq!(d.unknown_reading_date, 1);
        assert!(d.items[0].reading_date.is_none());
        // 運行NO の先頭 6 桁 (運行開始日) で埋めたりしない
        assert!(d.by_reading_date.is_empty());
        // 帰庫していない運行は範囲の終わりが null
        assert!(d.items[0].run_end_date.is_none());
        assert_eq!(d.items[0].run_start_date.as_deref(), Some("2026-06-02"));
    }

    /// `運行NO` が無い行と、同じ (乗務員, 運行) の重複は突合に使わない。
    #[test]
    fn rows_that_cannot_be_used_are_counted() {
        let r = row(
            7,
            "26060208000000000000071",
            "2026-06-02",
            "2026-06-02 08:00:00",
            "2026-06-02 18:00:00",
        );
        let rows = vec![
            r.clone(),
            r.clone(),
            json!({"driver_cd": 7, "unko_no": "   ", "reading_date": "2026-06-02"}),
            json!({"driver_cd": 7, "reading_date": "2026-06-02"}),
        ];
        let d = reading_dates(&rows);
        assert_eq!(d.total, 1);
        assert_eq!(d.skipped_rows, 3);
    }

    /// 2 名乗務は**運行NO ごとに別行**なので、乗務員で潰れない。
    #[test]
    fn a_two_person_crew_keeps_both_rows() {
        let rows = vec![
            row(
                1412,
                "26060208000000000014121",
                "2026-06-03",
                "2026-06-02 08:00:00",
                "2026-06-02 18:00:00",
            ),
            row(
                1255,
                "26060208000000000012552",
                "2026-06-03",
                "2026-06-02 08:00:00",
                "2026-06-02 18:00:00",
            ),
        ];
        let d = reading_dates(&rows);
        assert_eq!(d.total, 2);
        assert_eq!(d.by_reading_date["2026-06-03"].len(), 2);
        // 並びは乗務員CD 昇順
        assert_eq!(d.items[0].driver_cd, Some(1255));
        assert_eq!(d.items[1].driver_cd, Some(1412));
    }

    /// 乗務員CD が読めない行も落とさない (運行と読取日は引けるため)。
    #[test]
    fn a_row_without_a_driver_still_maps() {
        let rows = vec![json!({
            "unko_no": "26060208000000000000001",
            "reading_date": "2026-06-02",
        })];
        let d = reading_dates(&rows);
        assert_eq!(d.total, 1);
        assert!(d.items[0].driver_cd.is_none());
        assert_eq!(d.by_reading_date["2026-06-02"].len(), 1);
        assert!(d.items[0].run_start_date.is_none());
    }

    /// **上限で切っても `by_reading_date` は切らない** — 取り直す日が答えなので。
    #[test]
    fn the_item_cap_never_truncates_the_answer() {
        let n = MAX_READING_DATES + 5;
        let rows: Vec<serde_json::Value> = (0..n)
            .map(|i| {
                row(
                    i as i64,
                    &format!("260602{i:017}"),
                    "2026-06-03",
                    "2026-06-02 08:00:00",
                    "2026-06-02 18:00:00",
                )
            })
            .collect();
        let d = reading_dates(&rows);
        assert_eq!(d.items.len(), MAX_READING_DATES);
        assert_eq!(d.total, n);
        assert_eq!(d.by_reading_date["2026-06-03"].len(), n);
    }

    /// 日付だけの値 (時刻無し) が来ても日付として読める。
    #[test]
    fn a_date_only_value_is_read_as_is() {
        let rows = vec![json!({
            "unko_no": "26060208000000000000001",
            "reading_date": "2026-06-02",
            "departure_at": "2026-06-02",
        })];
        let d = reading_dates(&rows);
        assert_eq!(d.items[0].run_start_date.as_deref(), Some("2026-06-02"));
    }
}
