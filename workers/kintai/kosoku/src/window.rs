//! 月の窓・月初をまたぐ遡り起点・`unko_no` の先頭桁の読み方 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版の `kintai_repo` / `kintai_http_repo` / `kintai_push` / `kintai_fold` から
//! 中身を変えずに移したもの。DB も I/O も持たない。

use chrono::{NaiveDate, NaiveDateTime};

/// 生行 / 署名の日時書式。`EVENTS_SQL` の `DATE_FORMAT(..., '%Y-%m-%d %H:%i:%s')` と同じ。
pub const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

pub fn parse_dt(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, DATETIME_FORMAT).ok()
}

/// 対象月の取得範囲 `[月初, 翌月+1日)` を `YYYY-MM-DD HH:MM:SS` で返す。
///
/// 翌月 1 日ではなく**翌月 2 日**まで広げるのは、日跨ぎ勤務の終わり (終業打刻・
/// 帰庫) が翌月にはみ出すため — 月で切ると拘束の終わりが消える (上流 `daily-json`
/// が `queryEnd = nextMonth + 1day` にしているのと同じ考え方)。
///
/// `month` は呼び出し側 (`is_valid_month`) で検証済みの `YYYY-MM` 前提。
/// 対象月**ちょうど**の範囲 `[月初, 翌月初)`。
///
/// [`month_range`] が翌月 2 日まで広げるのは日跨ぎ勤務の終わりを拾うためだが、
/// フェリー控除は上流が `開始日時 >= $date_f && < $date_next` で切っている。
/// **紙と同じ数字を出すのが目的**なので、そちらに合わせる。
pub fn exact_month_range(month: &str) -> Option<(String, String)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = chrono::NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((
        format!("{} 00:00:00", first.format("%Y-%m-%d")),
        format!("{} 00:00:00", next.format("%Y-%m-%d")),
    ))
}

pub fn month_range(month: &str) -> Option<(String, String)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = chrono::NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next_month = if mm == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    let end = next_month.succ_opt()?;
    Some((format!("{first} 00:00:00"), format!("{end} 00:00:00")))
}

/// `unko_no` の先頭に埋まっている運行開始日時の桁数 (`YYMMDDHHMMSS`)。
const UNKO_NO_DATE_DIGITS: usize = 6;

/// etags の窓の末尾がこれより長く空いていたら「入力が欠けている」と見なす (日)。
///
/// ## 2 日 → 7 日 (Refs #205 の 37、**暫定値**)
///
/// **元の 2 日は「月・全乗務員を通した単一の `last`」向けの値**だった。下の実測は
/// 「全乗務員のうち誰か 1 人でも走っていれば窓は埋まる」前提で数えたもので、
/// 誰か 1 人が窓の端まで走っていれば gap は 0 になる。
///
/// #205 の 32 が粒度を**乗務員別**に割った際、閾値はこの 2 日のまま据え置かれた。
/// 乗務員 1 人を見れば**土日を挟むだけで gap は 3〜4 日**になるので、本番 2026-06
/// では母集団 113 名のうち **72 名**が鳴り続けた。warning が立つと月ゲートが封を
/// しないため、#205 の主目的 (fold の全量読みを省く) が無効化されたままになる。
///
/// **7 日 = 週末 + 1 日。「1 週間以上音沙汰が無い」は業務として異常**と言える位置で、
/// 実際に欠けている 1078 / 1517 / 1688 (gap 8 前後) は拾える。14 日まで緩めると
/// この本物を取りこぼす (親の判断、2026-07-31)。
///
/// **暫定値なのは、この閾値が見ているのが etags の運行開始日で、閾値を選ぶ根拠に
/// 使った分布 (`day_summaries` の最終勤務日) とは別の量だから。** 実際に何名鳴るかは
/// deploy して測るまで分からない。十分下がらなければ再調整する。
///
/// ## 元の実測 (単一 `last` 時代、2 日の根拠)
///
/// オンプレの生イベント口 (`/api/kintai/events`) から乗務員 47 名 (全 141 名の 1/3)
/// × 4 か月の 966 運行を引いて、窓 `[月初, 翌月初]` の日ごとの運行開始件数を数えると:
///
/// | 月 | 運行開始が 0 件の日 (窓の途中) | 窓の末尾の空き |
/// |---|---|---|
/// | 2025-12 (年末) | 無し | 1 日 |
/// | 2026-01 (年始) | 無し | 0 日 |
/// | 2026-05 | 2 日 (05-03 / 05-23) | 0 日 |
/// | 2026-06 | 1 日 (06-13) | 0 日 |
///
/// **年末年始でも運行開始は途切れない** (12/27〜12/31 も毎日 4〜10 件、01/01 も
/// 2 件)。1/3 の抽出でこれなので、全乗務員なら空き日はさらに減る方向にしか動かない
/// (部分集合のゼロ日 ⊇ 全体のゼロ日)。
///
/// `pub` なのは [`crate::kintai_tail_gap_probe`] が同じ値を読むだけの診断
/// (Refs #205、鳴っている 12 名を名指しする口) に使うため。**値はここが唯一の
/// 真実**で、診断側は複製しない — 複製すると drift したときに気付けない。
pub const MAX_TAIL_GAP_DAYS: i64 = 7;

/// `unko_no` の先頭 6 桁 (`YYMMDD`) = **運行開始日**。
///
/// 定義を書いた場所は alc にも本リポにも無い (`運行NO` はデジタコ由来の不透明な
/// キーとして通されているだけ) ので、実データで裏を取った値。上記 966 運行で
/// `unko_no[..12]` を `YYMMDDHHMMSS` として読むと、**不一致 0 / パース不能 0** で、
/// うち 922 件はその運行の `運行開始` の点イベントと**秒まで一致**した
/// (例: `26060610055500000023021` → `2026-06-06 10:05:55`)。
///
/// 末尾 (車輌コード) の長さは可変 (実データは 23 桁、22 桁の実物も居る) なので、
/// **先頭だけを見て後ろは一切見ない**。
///
/// `pub` なのは [`crate::kintai_rest_diff`] が休息のずれの一覧に
/// 運行日を添えるため (Refs #205 の 41)。読み方は 1 か所に置く。
pub fn unko_no_start_date(unko_no: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(unko_no.get(..UNKO_NO_DATE_DIGITS)?, "%y%m%d").ok()
}

/// `unko_no` の先頭 12 桁 (`YYMMDDHHMMSS`) = **運行開始日時** (Refs
/// ohishi-exp/nuxt-dtako-admin#1123)。裏取りは [`unko_no_start_date`] と同じ実測。
///
/// fold の読み窓を月初をまたぐ運行の開始まで遡らせるのに使う
/// ([`crate::window::month_head_anchors`])。日付版は 6 桁しか要らない呼び出し
/// (短い fixture を含む) のために別に残す。
///
/// `routes/dtako_day.rs` にも同じ 1 行があるが、あちらは `build.rs` の glob の外に
/// 置くために別に持っている。fold の出力を決めるこちらを glob の外から借りると、
/// 変えても `logic_version` が回らなくなるのでここに置く。
pub fn unko_no_start_datetime(unko_no: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(unko_no.get(..12)?, "%y%m%d%H%M%S").ok()
}

/// `kintai_http_repo::in_window` の中身。**開始・終了 (区間なら) だけで判定する**ので、fold が
/// 読んだ行を乗務員ごとの窓へ切り戻すとき (`kintai_fold`、Refs
/// ohishi-exp/nuxt-dtako-admin#1123) も同じ述語を使う — 絞り方が 2 実装に
/// ならないように。
pub fn window_holds(
    start: NaiveDateTime,
    end: Option<NaiveDateTime>,
    from: NaiveDateTime,
    to: NaiveDateTime,
) -> bool {
    if start >= from && start < to {
        return true;
    }
    match end {
        Some(end) => start < from && end >= from && end < to,
        None => false,
    }
}

/// 月初時点の打刻の姿 (乗務員 1 名ぶん)。[`month_head_anchors`] の材料。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadPunch {
    pub driver: u64,
    /// 窓 `[月初, to)` で最初の打刻 (`始業` / `終業`)。無ければ `None`。
    pub first_state: Option<String>,
    /// 月初より前の最後の始業 (`YYYY-MM-DD HH:MM:SS`)。
    pub last_start: Option<String>,
    /// 月初より前の最後の終業。
    pub last_end: Option<String>,
}

/// 乗務員ごとの遡り起点 (Refs ohishi-exp/nuxt-dtako-admin#1123)。**日数の上限は
/// 置かない** — どこまで遡るかはデータで決める (ユーザー決定)。
///
/// | 種類 | 条件 | 値 |
/// |---|---|---|
/// | 運行 | 窓に `運行終了` があり、`unko_no` 先頭 12 桁 (運行開始日時) が月初より前 | その運行開始日時 |
/// | 打刻 | 月初時点で始業が開いている (最後の始業 > 最後の終業) **かつ** 窓で最初の打刻が終業 | その始業 |
///
/// 両方あれば早いほう。打刻の 2 つ目の条件は「終業を打ち忘れた数週間前の始業」が
/// 起点になり続けるのを抑える — 当月の最初が始業なら、前月の始業は閉じていないまま
/// 捨てられる (`kosoku::shifts_from_timecard` が次の始業で置き換えるのと同じ意味)。
///
/// `run_ends` は `(乗務員CD, unko_no)`。`month_start` は `YYYY-MM-DD HH:MM:SS` で、
/// 比較は同じ形の文字列同士で行う。
pub fn month_head_anchors(
    month_start: &str,
    run_ends: &[(u64, String)],
    punches: &[HeadPunch],
) -> std::collections::BTreeMap<u64, String> {
    let mut out: std::collections::BTreeMap<u64, String> = std::collections::BTreeMap::new();
    let mut offer = |driver: u64, at: String| {
        let slot = out.entry(driver).or_insert_with(|| at.clone());
        if at < *slot {
            *slot = at;
        }
    };
    for (driver, unko_no) in run_ends {
        let Some(start) = unko_no_start_datetime(unko_no) else {
            continue;
        };
        let start = start.format("%Y-%m-%d %H:%M:%S").to_string();
        if start.as_str() < month_start {
            offer(*driver, start);
        }
    }
    for p in punches {
        let Some(start) = p.last_start.as_deref() else {
            continue;
        };
        let open = p.last_end.as_deref().is_none_or(|end| start > end);
        let closes_first = p.first_state.as_deref() == Some("終業");
        if open && closes_first && start < month_start {
            offer(p.driver, start.to_string());
        }
    }
    out
}

/// 読みの始端 = 遡り起点のうち最も早いもの、無ければ月初 (Refs
/// ohishi-exp/nuxt-dtako-admin#1123)。**読みは全員で 1 回**なので、窓の始端は
/// 全乗務員の最小に揃え、乗務員ごとの窓へは読んだ後に切り戻す
/// (`kintai_fold::clip_to_anchors`)。
pub fn lookback_from(
    month_start: &str,
    anchors: &std::collections::BTreeMap<u64, String>,
) -> String {
    anchors
        .values()
        .map(String::as_str)
        .fold(month_start, |a, b| a.min(b))
        .to_string()
}

/// 読む窓 `[from, to)` = [`month_range`] の始端を遡り起点まで下げたもの。`driver` 指定ならその乗務員の
/// 起点 (無ければ月初)、省略なら全員の最小 ([`lookback_from`])。月が読めなければ `None`
/// (呼び手が自分のエラー型に写す)。
pub fn read_window(
    month: &str,
    anchors: &std::collections::BTreeMap<u64, String>,
    driver: Option<u64>,
) -> Option<(String, String)> {
    let (from, to) = month_range(month)?;
    let from = match driver {
        Some(d) => anchors.get(&d).cloned().unwrap_or(from),
        None => lookback_from(&from, anchors),
    };
    Some((from, to))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, DATETIME_FORMAT).unwrap()
    }

    const APRIL: &str = "2026-04-01 00:00:00";

    fn punch(
        driver: u64,
        first: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
    ) -> HeadPunch {
        HeadPunch {
            driver,
            first_state: first.map(str::to_string),
            last_start: start.map(str::to_string),
            last_end: end.map(str::to_string),
        }
    }

    /// 生行と同じ書式だけを読む。
    #[test]
    fn parse_dt_reads_the_row_format_only() {
        assert_eq!(
            parse_dt("2026-04-01 05:06:07"),
            Some(dt("2026-04-01 05:06:07"))
        );
        assert_eq!(parse_dt("2026/04/01 05:06:07"), None);
        assert_eq!(parse_dt(""), None);
    }

    /// 対象月ちょうど `[月初, 翌月初)`。12 月は年を跨ぐ。
    #[test]
    fn exact_month_range_is_the_calendar_month() {
        let (from, to) = exact_month_range("2026-07").unwrap();
        assert_eq!(from, "2026-07-01 00:00:00");
        assert_eq!(to, "2026-08-01 00:00:00");
        let (from, to) = exact_month_range("2026-12").unwrap();
        assert_eq!(from, "2026-12-01 00:00:00");
        assert_eq!(to, "2027-01-01 00:00:00");
        assert!(exact_month_range("").is_none());
        assert!(exact_month_range("2026-13").is_none());
        assert!(exact_month_range("20a6-07").is_none());
        assert!(exact_month_range("2026-0a").is_none());
    }

    #[test]
    fn month_range_covers_next_month_first_day() {
        let (from, to) = month_range("2026-07").unwrap();
        assert_eq!(from, "2026-07-01 00:00:00");
        // 日跨ぎの終業を拾うため翌月 2 日まで
        assert_eq!(to, "2026-08-02 00:00:00");
    }

    #[test]
    fn month_range_rolls_over_year() {
        let (from, to) = month_range("2026-12").unwrap();
        assert_eq!(from, "2026-12-01 00:00:00");
        assert_eq!(to, "2027-01-02 00:00:00");
    }

    #[test]
    fn month_range_rejects_garbage() {
        assert!(month_range("").is_none());
        assert!(month_range("2026-13").is_none());
        assert!(month_range("20a6-07").is_none());
        assert!(month_range("2026-0a").is_none());
    }

    /// 1194 の 2026-04: 運行は 3/31 21:39:47 開始、始業は 3/31 21:36:28 — 早い方。
    #[test]
    fn the_earlier_of_run_and_punch_wins() {
        let runs = vec![(1194, "26033121394700000043241".to_string())];
        let punches = vec![punch(
            1194,
            Some("終業"),
            Some("2026-03-31 21:36:28"),
            Some("2026-03-30 17:00:00"),
        )];
        let got = month_head_anchors(APRIL, &runs, &punches);
        assert_eq!(
            got.get(&1194).map(String::as_str),
            Some("2026-03-31 21:36:28")
        );
        // 運行だけなら運行開始日時
        let got = month_head_anchors(APRIL, &runs, &[]);
        assert_eq!(
            got.get(&1194).map(String::as_str),
            Some("2026-03-31 21:39:47")
        );
        // 打刻の方が遅ければ運行が勝つ (順番に依らない)
        let late = vec![punch(1194, Some("終業"), Some("2026-03-31 23:00:00"), None)];
        let got = month_head_anchors(APRIL, &runs, &late);
        assert_eq!(
            got.get(&1194).map(String::as_str),
            Some("2026-03-31 21:39:47")
        );
    }

    /// 当月に始まった運行と、読めない `unko_no` は起点にならない。
    #[test]
    fn runs_begun_in_the_month_or_unreadable_do_not_anchor() {
        let runs = vec![
            (1300, "26040108000000000043241".to_string()),
            (1301, "U1".to_string()),
            (1302, "269999123456000".to_string()),
        ];
        assert!(month_head_anchors(APRIL, &runs, &[]).is_empty());
    }

    /// 閉じ忘れ運行 (1731 型) は日数の上限なしで遡る。
    #[test]
    fn a_long_forgotten_run_still_anchors() {
        let runs = vec![(1731, "26022105000000000012341".to_string())];
        let got = month_head_anchors("2026-03-01 00:00:00", &runs, &[]);
        assert_eq!(
            got.get(&1731).map(String::as_str),
            Some("2026-02-21 05:00:00")
        );
    }

    /// 打刻は「月初時点で開いている始業」かつ「当月の最初が終業」のときだけ。
    #[test]
    fn a_punch_anchors_only_when_open_and_closed_first_in_the_month() {
        let s = Some("2026-03-20 08:00:00");
        let cases = [
            // 終業が無い / 始業より前 → 開いている。当月の最初が終業 → 遡る
            (punch(1, Some("終業"), s, None), true),
            (punch(2, Some("終業"), s, Some("2026-03-19 17:00:00")), true),
            // 当月の最初が始業 → 前月の始業は捨てられる (終業忘れの化石)
            (punch(3, Some("始業"), s, None), false),
            // 当月に打刻が無い
            (punch(4, None, s, None), false),
            // 閉じている (終業が始業より後)
            (
                punch(5, Some("終業"), s, Some("2026-03-20 17:00:00")),
                false,
            ),
            // 前月に始業が無い
            (punch(6, Some("終業"), None, None), false),
            // 始業が月初以降 (材料の取り違え) は採らない
            (punch(7, Some("終業"), Some(APRIL), None), false),
        ];
        for (p, want) in cases {
            let cd = p.driver;
            let got = month_head_anchors(APRIL, &[], &[p]);
            assert_eq!(got.contains_key(&cd), want, "乗務員 {cd}");
        }
    }

    #[test]
    fn lookback_from_is_the_earliest_anchor_or_the_month_start() {
        let none = std::collections::BTreeMap::new();
        assert_eq!(lookback_from(APRIL, &none), APRIL);
        let anchors = [
            (1, "2026-03-31 21:36:28".to_string()),
            (2, "2026-03-30 08:00:00".to_string()),
        ]
        .into_iter()
        .collect();
        assert_eq!(lookback_from(APRIL, &anchors), "2026-03-30 08:00:00");
    }

    /// `unko_no` の実物 (23 桁) / テスト fixture の 22 桁 / 読めない形。
    #[test]
    fn unko_no_start_date_reads_the_leading_yymmdd_only() {
        let real = unko_no_start_date("26060610055500000023021");
        assert_eq!(real, Some(d(2026, 6, 6)), "実物 23 桁");
        let short_tail = unko_no_start_date("2602241025060000000272");
        assert_eq!(short_tail, Some(d(2026, 2, 24)), "車輌コードが短い 22 桁");
        assert_eq!(unko_no_start_date("U1"), None, "6 桁に満たない");
        assert_eq!(unko_no_start_date("269999123456"), None, "日付として不正");
        assert_eq!(unko_no_start_date("26060X10055500"), None, "数字でない");
    }

    /// 先頭 12 桁 = 運行開始日時 (Refs ohishi-exp/nuxt-dtako-admin#1123)。
    #[test]
    fn unko_no_start_datetime_reads_the_leading_12_digits() {
        let got = unko_no_start_datetime("26033121394700000043241");
        assert_eq!(got, Some(dt("2026-03-31 21:39:47")), "1194 の実物");
        let short_tail = unko_no_start_datetime("2602241025060000000272");
        assert_eq!(short_tail, Some(dt("2026-02-24 10:25:06")), "22 桁");
        assert_eq!(unko_no_start_datetime("260331"), None, "12 桁に満たない");
        assert_eq!(
            unko_no_start_datetime("269999123456"),
            None,
            "日付として不正"
        );
        assert_eq!(
            unko_no_start_datetime("260331256000"),
            None,
            "時刻として不正"
        );
    }

    /// 切り出した述語は区間の 2 ブランチと点をそのまま判定する。
    #[test]
    fn window_holds_matches_the_two_sql_branches() {
        let (from, to) = (dt("2026-04-01 00:00:00"), dt("2026-05-02 00:00:00"));
        let before = dt("2026-03-31 21:36:28");
        assert!(!window_holds(before, None, from, to), "点は開始で判定");
        let inside = Some(dt("2026-04-01 04:38:56"));
        assert!(window_holds(before, inside, from, to), "期間内に終わる区間");
        assert!(window_holds(from, None, from, to), "下端は含む");
        assert!(!window_holds(to, None, from, to), "上端は含まない");
    }

    /// 指定した乗務員はその起点から (無ければ月初)、省略は全員の最小から。終端は動かない。
    #[test]
    fn read_window_starts_at_the_anchor() {
        let anchors: std::collections::BTreeMap<u64, String> = [
            (1194, "2026-03-31 21:36:28".to_string()),
            (1300, "2026-03-30 08:00:00".to_string()),
        ]
        .into_iter()
        .collect();
        let to = "2026-05-02 00:00:00".to_string();
        let got = read_window("2026-04", &anchors, Some(1194));
        assert_eq!(got, Some(("2026-03-31 21:36:28".to_string(), to.clone())));
        let got = read_window("2026-04", &anchors, Some(1500));
        assert_eq!(got, Some((APRIL.to_string(), to.clone())));
        let got = read_window("2026-04", &anchors, None);
        assert_eq!(got, Some(("2026-03-30 08:00:00".to_string(), to)));
        assert_eq!(read_window("2026-13", &anchors, None), None);
    }

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
}
