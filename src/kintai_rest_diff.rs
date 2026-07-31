//! `time_card_dtako` と `dtako_events` の**休息のずれ**を運行別に名指しする
//! (Refs #205 の 41)。
//!
//! **直す口ではなく、見える口。** ここは判定に一切入らない — 拘束も勤務も畳まず、
//! 「どの運行で 2 つの表の休息が食い違っているか」を並べるだけ。
//!
//! ## 何を測っているのか (2026-07-31 に実証済みの事故)
//!
//! オンプレ (`yhonda-ohishi/nginx`、CakePHP) の
//! `TimeCardDtakoController::_setbyUnkoNo($id)` は `dtako_events` の 休息 /
//! 運行開始 / 運行終了 を読んで `time_card_dtako` へ**書き戻す**が、
//! `newEmptyEntity()` で **INSERT しかしない**。削除も更新もしない。
//! `TimeCardDtakoTable` の主キーは `(unko_no, event_name, datetime)` なので:
//!
//! | 状況 | 起きること |
//! |---|---|
//! | `datetime` が同じ | 主キー衝突で `save()` が失敗し、**何も起きない** |
//! | `datetime` が違う | **古い行が残ったまま**、新しい行が別レコードとして増える |
//!
//! ⇒ デジタコ側が休息を切り直しても `time_card_dtako` は追従しない。実証された
//! 1 件 (運行 `26061409573000000034471` / 乗務員 1445 / 2026-06) では、古い側が
//! 47 時間の休息を 88 秒の中断で 2 本に割っており、そこから始まる勤務が 2 日ぶん
//! 余計に生まれていた。`DELETE` してから `setbyUnkoNo` で作り直すと
//! `06-17 13:32:38 → 06-19 13:22:00` の 1 本になり、デジタコ側と秒まで一致した。
//!
//! 直す口は `yhonda-ohishi/nginx` PR #792 (`resetbyUnkoNo` + 旅費編集画面の
//! 「勤務時間再登録」ボタン)。**このモジュールは押す対象を一覧にするだけ。**
//!
//! ## 突合の作法 — 時刻の集合を、同じ窓で切ってから比べる
//!
//! 左辺 `time_card_dtako` (`source = "dtako"`) は**点**の行、右辺 `dtako_events`
//! (`source = "dtako_events"`) は**区間** (`開始日時` / `終了日時`) で形が違う。
//! CakePHP は区間の両端をそれぞれ 1 行として書き戻す (開始 = state 20 / 終了 = 21 で
//! **どちらも名前は「休息」**。[`crate::kintai_push::NOT_CARRIED_STATES`] の docs) ので、
//! **両端の時刻の集合**同士なら形を揃えて比べられる。
//!
//! **窓の外の端は両側とも数えない。** 月の窓 `[from, to)` は
//! [`crate::kintai_repo::month_range`] のものをそのまま使い、左辺は行の `datetime`、
//! 右辺は `開始日時` / `終了日時` の**各端**を同じ窓で切る。片側だけ窓に入る端を
//! 数えると、月末に始まって翌々日に終わる休息が毎月「ずれ」に化ける (窓の縁の作り物)。
//!
//! ## 束ねる鍵は `運行NO` だけ。乗務員CD は鍵にしない
//!
//! 2 名乗務では運行まるごとが別の乗務員のまま記録される
//! (`EVENTS_SQL` の `対象乗務員CD` と同じ話) ため、`(乗務員CD, 運行NO)` で束ねると
//! 表ごとに乗務員が割れた運行が「片側にしか無い」に化ける。**`運行NO` で束ね、
//! 見えた乗務員CD は [`RestDiffUnko::driver_cds`] に全部載せる。**

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

/// 応答に載せる運行の上限。総数は [`RestDiff::total`] に別に返す
/// (`unko_diff` / `unko_diff_total` と同じ作法)。
pub const MAX_REST_DIFF: usize = 500;

/// 突合の対象にする `state` (両表とも解決後の名前で `休息`)。
///
/// `kosoku.rs` が休息区間を拾う条件 (`e.state == "休息" && e.source == "dtako_events"`)
/// と同じ文字列。番号ではなく名前で見るのは [`crate::kintai_push::NOT_CARRIED_STATES`]
/// と同じ理由 — `event_name` は自由記述なので、番号で見ると「state 20 だが別の名前」の
/// 行を取り違える。
pub const REST_STATE: &str = "休息";

/// 左辺 (`time_card_dtako` 由来) の `source`。
pub const SOURCE_DTAKO: &str = "dtako";
/// 右辺 (`dtako_events` 由来) の `source`。
pub const SOURCE_DTAKO_EVENTS: &str = "dtako_events";

/// 食い違いの**形**。「押す対象」と「押しても無駄な対象」を分けるためにある。
///
/// **2026-06 の本番実測 (2026-07-31) で分けることになった。** 239 件出た食い違いが
/// **全部 [`RestDiffKind::DtakoMissing`]** で、[`RestDiffKind::Mismatch`] は
/// **0 件**だった。形を分けずに `total` だけ返すと「239 運行がずれている」と読めて
/// しまうが、**「勤務時間再登録」を押す対象は 0 件**だった。
/// 誤読する出力は、出力しないより悪いことがある。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RestDiffKind {
    /// **両側に休息があって食い違う。** 書き戻しが追従していない疑い —
    /// `yhonda-ohishi/nginx` PR #792 の「勤務時間再登録」を押す対象はこれだけ。
    Mismatch,
    /// `time_card_dtako` に休息が**1 行も無い**。書き戻しが一度も走っていないか、
    /// その乗務員が打刻系に居ない (実測: `dtako_events` だけの乗務員が居る)。
    /// **押しても直る保証が無い**ので [`RestDiffKind::Mismatch`] と混ぜない。
    DtakoMissing,
    /// `dtako_events` 側に休息が無く `time_card_dtako` にだけある。
    /// デジタコ側から消えた休息が残っている形。
    EventsMissing,
}

impl RestDiffKind {
    /// 全ての形。`total_by_kind` を 0 込みで埋めるために持つ。
    pub const ALL: [RestDiffKind; 3] = [
        RestDiffKind::Mismatch,
        RestDiffKind::DtakoMissing,
        RestDiffKind::EventsMissing,
    ];

    /// JSON のキー (`serde` の `snake_case` と同じ綴り)。
    pub fn as_str(self) -> &'static str {
        match self {
            RestDiffKind::Mismatch => "mismatch",
            RestDiffKind::DtakoMissing => "dtako_missing",
            RestDiffKind::EventsMissing => "events_missing",
        }
    }

    /// 並べ替えの優先順。**`Mismatch` が最小** = 上限で切られても残る。
    fn rank(self) -> u8 {
        match self {
            RestDiffKind::Mismatch => 0,
            RestDiffKind::DtakoMissing => 1,
            RestDiffKind::EventsMissing => 2,
        }
    }
}

/// 休息が食い違っている運行 1 本。
#[derive(Debug, Serialize)]
pub struct RestDiffUnko {
    pub unko_no: String,
    /// 食い違いの形。**押す対象は [`RestDiffKind::Mismatch`] だけ** (enum の docs)。
    pub kind: RestDiffKind,
    /// この運行で見えた乗務員CD (昇順・重複無し)。2 名乗務や表ごとの割れで
    /// 2 つ以上になることがある (モジュール docs)。
    pub driver_cds: Vec<i64>,
    /// `運行NO` の先頭 6 桁 (`YYMMDD`) から読んだ運行開始日。読めなければ `null`。
    pub run_date: Option<String>,
    /// `time_card_dtako` の休息**行数** (窓の中)。両端が別行なので、健全なら
    /// おおむね `dtako_events_rest_intervals` の 2 倍になる。
    pub dtako_rest_rows: usize,
    /// `dtako_events` の休息**区間数** (窓の中)。
    pub dtako_events_rest_intervals: usize,
    /// `time_card_dtako` にしか無い時刻 (昇順)。**古い行が残っている疑い。**
    pub dtako_only: Vec<String>,
    /// `dtako_events` にしか無い時刻 (昇順)。書き戻しが届いていない疑い。
    pub dtako_events_only: Vec<String>,
}

/// 突合の結果ぜんたい。
#[derive(Debug, Serialize)]
pub struct RestDiff {
    /// 食い違っている運行 (先頭 [`MAX_REST_DIFF`] 件)。
    pub items: Vec<RestDiffUnko>,
    /// 食い違っている運行の**総数**。`items` が切られたことが分かるように別に返す。
    ///
    /// **この数を「押す対象の数」と読まないこと** — 内訳は [`RestDiff::total_by_kind`]。
    pub total: usize,
    /// **押す対象の数** ([`RestDiffKind::Mismatch`] の件数)。`total_by_kind` にも
    /// 入っているが、**読み手の目に最初に入る位置に単独で置く** — 実測では
    /// `total` 239 に対してこれが 0 で、混ぜると必ず読み違える。
    pub mismatch_total: usize,
    /// 形ごとの件数 ([`RestDiffKind`])。**`items` の上限では切られない総数**。
    /// 出ない形のキーも 0 で必ず出す (「無い」と「数えていない」を分けるため)。
    pub total_by_kind: BTreeMap<String, usize>,
    /// 乗務員CD → 食い違っている運行数。**`items` の上限では切られない** ので、
    /// 「どの乗務員が対象か」は総数のまま読める。
    pub by_driver: BTreeMap<String, usize>,
    /// 突合の母集団になった運行数 (休息の行が片側にでもあった `運行NO` の数)。
    pub scanned_unko: usize,
    /// 突合に使えなかった行数 (`運行NO` が空・`datetime` が無い・窓の外・
    /// `休息` でない・知らない `source`)。0 でないこと自体は異常ではない。
    pub skipped_rows: usize,
}

/// 1 運行ぶんの集計途中。
#[derive(Default)]
struct Bucket {
    driver_cds: BTreeSet<i64>,
    /// 時刻 → 本数 (同じ時刻が 2 行あることを潰さない)
    dtako: BTreeMap<String, usize>,
    dtako_events: BTreeMap<String, usize>,
    dtako_rows: usize,
    dtako_intervals: usize,
}

fn str_field<'a>(row: &'a serde_json::Value, key: &str) -> Option<&'a str> {
    row.get(key).and_then(|v| v.as_str())
}

/// `[from, to)` に入る時刻か。両方 `YYYY-MM-DD HH:MM:SS` なので辞書順 = 時刻順。
fn in_window(ts: &str, from: &str, to: &str) -> bool {
    ts >= from && ts < to
}

/// 集合の差 (多重度つき)。`a` にあって `b` に足りないぶんを昇順で返す。
fn only_in(a: &BTreeMap<String, usize>, b: &BTreeMap<String, usize>) -> Vec<String> {
    let mut out = Vec::new();
    for (ts, n) in a {
        let mine = *n;
        let theirs = b.get(ts).copied().unwrap_or(0);
        for _ in theirs..mine {
            out.push(ts.clone());
        }
    }
    out
}

/// 生イベントの行から、休息が食い違っている運行を並べる。
///
/// `rows` は [`crate::kintai_repo::KintaiEventsApi::fetch_rest_events_between`] が
/// 返す形 (`datetime` / `end_datetime` / `driver_id` / `source` / `state` / `unko_no`)。
/// `from` / `to` は [`crate::kintai_repo::month_range`] の窓をそのまま渡す。
pub fn rest_diff(rows: &[serde_json::Value], from: &str, to: &str) -> RestDiff {
    let mut buckets: BTreeMap<String, Bucket> = BTreeMap::new();
    let mut skipped_rows = 0usize;
    for row in rows {
        if !collect_row(row, from, to, &mut buckets) {
            skipped_rows += 1;
        }
    }
    let scanned_unko = buckets.len();
    let mut items: Vec<RestDiffUnko> = buckets
        .into_iter()
        .filter_map(|(unko_no, b)| to_item(unko_no, b))
        .collect();
    // 乗務員別と形別の内訳は上限で切る前に数える (切られても対象が読めるように)。
    // 出ない形も 0 で置く — 「無い」と「数えていない」を分けるため
    let mut by_driver: BTreeMap<String, usize> = BTreeMap::new();
    let mut total_by_kind: BTreeMap<String, usize> = RestDiffKind::ALL
        .iter()
        .map(|k| (k.as_str().to_string(), 0))
        .collect();
    for it in &items {
        for cd in &it.driver_cds {
            *by_driver.entry(cd.to_string()).or_default() += 1;
        }
        *total_by_kind
            .entry(it.kind.as_str().to_string())
            .or_default() += 1;
    }
    let mismatch_total = total_by_kind[RestDiffKind::Mismatch.as_str()];
    // 並びは **形 → 乗務員CD → 運行NO**。`Mismatch` (押す対象) を先頭に置くのは、
    // 上限で切られたときに真っ先に消えるのが押す対象では意味が無いため
    items.sort_by(|a, b| {
        (a.kind.rank(), a.driver_cds.first(), &a.unko_no).cmp(&(
            b.kind.rank(),
            b.driver_cds.first(),
            &b.unko_no,
        ))
    });
    let total = items.len();
    items.truncate(MAX_REST_DIFF);
    RestDiff {
        items,
        total,
        mismatch_total,
        total_by_kind,
        by_driver,
        scanned_unko,
        skipped_rows,
    }
}

/// 1 行を桶へ入れる。入れられなければ `false` (呼び出し側が `skipped_rows` に数える)。
fn collect_row(
    row: &serde_json::Value,
    from: &str,
    to: &str,
    buckets: &mut BTreeMap<String, Bucket>,
) -> bool {
    if str_field(row, "state").map(str::trim) != Some(REST_STATE) {
        return false;
    }
    let unko_no = str_field(row, "unko_no").map(str::trim).unwrap_or_default();
    if unko_no.is_empty() {
        return false;
    }
    let source = str_field(row, "source").unwrap_or_default();
    let start = str_field(row, "datetime").unwrap_or_default();
    let end = str_field(row, "end_datetime").unwrap_or_default();
    let ends: Vec<&str> = match source {
        SOURCE_DTAKO => vec![start],
        SOURCE_DTAKO_EVENTS => vec![start, end],
        _ => return false,
    };
    let hits: Vec<&str> = ends
        .into_iter()
        .filter(|ts| in_window(ts, from, to))
        .collect();
    if hits.is_empty() {
        return false;
    }
    let b = buckets.entry(unko_no.to_string()).or_default();
    if let Some(cd) = row.get("driver_id").and_then(|v| v.as_i64()) {
        b.driver_cds.insert(cd);
    }
    match source {
        SOURCE_DTAKO => {
            b.dtako_rows += 1;
            for ts in hits {
                *b.dtako.entry(ts.to_string()).or_default() += 1;
            }
        }
        _ => {
            b.dtako_intervals += 1;
            for ts in hits {
                *b.dtako_events.entry(ts.to_string()).or_default() += 1;
            }
        }
    }
    true
}

/// 食い違いがある桶だけを 1 件に畳む。一致していれば `None`。
fn to_item(unko_no: String, b: Bucket) -> Option<RestDiffUnko> {
    let dtako_only = only_in(&b.dtako, &b.dtako_events);
    let dtako_events_only = only_in(&b.dtako_events, &b.dtako);
    if dtako_only.is_empty() && dtako_events_only.is_empty() {
        return None;
    }
    let run_date = crate::kintai_http_repo::unko_no_start_date(&unko_no).map(|d| d.to_string());
    // 片側が丸ごと空なら「食い違い」ではなく「片側が無い」— 押しても直る保証が無い
    // ので分ける (`RestDiffKind` の docs、2026-06 の実測で 239/239 がこちらだった)
    let kind = match (b.dtako_rows, b.dtako_intervals) {
        (0, _) => RestDiffKind::DtakoMissing,
        (_, 0) => RestDiffKind::EventsMissing,
        _ => RestDiffKind::Mismatch,
    };
    Some(RestDiffUnko {
        unko_no,
        kind,
        driver_cds: b.driver_cds.into_iter().collect(),
        run_date,
        dtako_rest_rows: b.dtako_rows,
        dtako_events_rest_intervals: b.dtako_intervals,
        dtako_only,
        dtako_events_only,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const FROM: &str = "2026-06-01 00:00:00";
    const TO: &str = "2026-07-02 00:00:00";

    fn tc(unko: &str, driver: i64, at: &str) -> serde_json::Value {
        json!({
            "datetime": at,
            "end_datetime": serde_json::Value::Null,
            "driver_id": driver,
            "source": SOURCE_DTAKO,
            "state": REST_STATE,
            "unko_no": unko,
        })
    }

    fn ev(unko: &str, driver: i64, start: &str, end: &str) -> serde_json::Value {
        json!({
            "datetime": start,
            "end_datetime": end,
            "driver_id": driver,
            "source": SOURCE_DTAKO_EVENTS,
            "state": REST_STATE,
            "unko_no": unko,
        })
    }

    /// **両端が秒まで一致していれば 1 件も出ない。** 直した後の姿。
    #[test]
    fn a_run_whose_two_tables_agree_is_not_listed() {
        let rows = vec![
            ev(
                "26061409573000000034471",
                1445,
                "2026-06-17 13:32:38",
                "2026-06-19 13:22:00",
            ),
            tc("26061409573000000034471", 1445, "2026-06-17 13:32:38"),
            tc("26061409573000000034471", 1445, "2026-06-19 13:22:00"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert!(d.items.is_empty(), "{:?}", d.items);
        assert_eq!(d.total, 0);
        assert_eq!(d.scanned_unko, 1);
        assert_eq!(d.skipped_rows, 0);
        assert!(d.by_driver.is_empty());
        // 出ない形も 0 で必ず出す (「無い」と「数えていない」を分ける)
        assert_eq!(d.mismatch_total, 0);
        assert_eq!(d.total_by_kind.get("mismatch"), Some(&0));
        assert_eq!(d.total_by_kind.get("dtako_missing"), Some(&0));
        assert_eq!(d.total_by_kind.get("events_missing"), Some(&0));
    }

    /// 実証された事故の形 — 古い `time_card_dtako` が残り、`dtako_events` の
    /// 現在の 1 本と食い違う。
    #[test]
    fn a_stale_time_card_dtako_is_named() {
        let unko = "26061409573000000034471";
        let rows = vec![
            ev(unko, 1445, "2026-06-17 13:32:38", "2026-06-19 13:22:00"),
            tc(unko, 1445, "2026-06-18 07:50:36"),
            tc(unko, 1445, "2026-06-18 07:52:04"),
            tc(unko, 1445, "2026-06-19 13:22:22"),
            tc(unko, 1445, "2026-06-19 16:27:55"),
            tc(unko, 1445, "2026-06-22 10:01:57"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 1);
        let it = &d.items[0];
        assert_eq!(it.unko_no, unko);
        assert_eq!(it.driver_cds, vec![1445]);
        assert_eq!(it.run_date.as_deref(), Some("2026-06-14"));
        assert_eq!(it.dtako_rest_rows, 5);
        assert_eq!(it.dtako_events_rest_intervals, 1);
        assert_eq!(it.dtako_only.len(), 5);
        // 22 秒ずれた 13:22:22 は「dtako にしか無い」、13:22:00 は「events にしか無い」
        assert!(it.dtako_only.contains(&"2026-06-19 13:22:22".to_string()));
        assert_eq!(
            it.dtako_events_only,
            vec!["2026-06-17 13:32:38", "2026-06-19 13:22:00"]
        );
        assert_eq!(d.by_driver.get("1445"), Some(&1));
        // 両側に休息があって食い違う = 「勤務時間再登録」を押す対象
        assert_eq!(it.kind, RestDiffKind::Mismatch);
        assert_eq!(d.mismatch_total, 1);
        assert_eq!(d.total_by_kind.get("mismatch"), Some(&1));
    }

    /// **2026-06 の実測がこの形だった** — `time_card_dtako` に休息が 1 行も無い
    /// 運行が 239 件。押しても直る保証が無いので `Mismatch` と混ぜない。
    #[test]
    fn a_run_with_no_time_card_dtako_rest_is_not_a_mismatch() {
        let unko = "26060403504800000011011";
        let rows = vec![ev(unko, 1742, "2026-06-04 17:47:01", "2026-06-05 02:45:32")];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 1);
        assert_eq!(d.items[0].kind, RestDiffKind::DtakoMissing);
        assert_eq!(d.items[0].dtako_rest_rows, 0);
        // **押す対象は 0 件**。`total` (1) と混ぜて読まないための数字
        assert_eq!(d.mismatch_total, 0);
        assert_eq!(d.total_by_kind.get("dtako_missing"), Some(&1));
        assert_eq!(d.total_by_kind.get("mismatch"), Some(&0));
    }

    /// 鏡像 — `dtako_events` 側だけ休息が無い形。
    #[test]
    fn a_run_with_no_dtako_events_rest_is_its_own_kind() {
        let unko = "26060403504800000011011";
        let rows = vec![tc(unko, 1742, "2026-06-04 17:47:01")];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.items[0].kind, RestDiffKind::EventsMissing);
        assert_eq!(d.mismatch_total, 0);
        assert_eq!(d.total_by_kind.get("events_missing"), Some(&1));
    }

    /// **上限で切られても押す対象は残る。** `Mismatch` を先頭に並べているため。
    #[test]
    fn the_item_cap_never_drops_a_mismatch() {
        let mut rows = Vec::new();
        // 押す対象 1 件 — 乗務員CD も運行NO も最後に来る値にしておく
        let hot = "99999999999999999999999";
        rows.push(ev(hot, 9999, "2026-06-10 10:00:00", "2026-06-10 12:00:00"));
        rows.push(tc(hot, 9999, "2026-06-10 10:00:01"));
        // 押しても無駄な対象を上限より多く積む
        for i in 0..(MAX_REST_DIFF + 20) {
            rows.push(ev(&format!("260602{i:017}"), 1, "2026-06-02 08:00:00", ""));
        }
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.items.len(), MAX_REST_DIFF);
        assert_eq!(d.total, MAX_REST_DIFF + 21);
        assert_eq!(d.mismatch_total, 1);
        // 切られても先頭に残る
        assert_eq!(d.items[0].kind, RestDiffKind::Mismatch);
        assert_eq!(d.items[0].unko_no, hot);
    }

    /// JSON のキーと `serde` の綴りが割れない (`total_by_kind` の鍵と `kind` の値)。
    #[test]
    fn the_kind_keys_match_their_serde_spelling() {
        for k in RestDiffKind::ALL {
            let json = serde_json::to_string(&k).unwrap();
            assert_eq!(json, format!("\"{}\"", k.as_str()));
        }
    }

    /// **窓の外の端は両側とも数えない。** 月末に始まって窓の先で終わる休息が
    /// 毎月「ずれ」に化けないこと。
    #[test]
    fn an_endpoint_outside_the_window_is_counted_on_neither_side() {
        let unko = "26063020000000000012341";
        let rows = vec![
            // 終了は窓 (7/2) の先 — 右辺の端として数えない
            ev(unko, 1107, "2026-06-30 20:00:00", "2026-07-03 05:00:00"),
            tc(unko, 1107, "2026-06-30 20:00:00"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 0, "{:?}", d.items);
        assert_eq!(d.scanned_unko, 1);
        assert_eq!(d.skipped_rows, 0);
    }

    /// 窓の**手前**に始まって窓の中で終わる休息も同じ (開始側だけ落ちる)。
    #[test]
    fn a_run_starting_before_the_window_keeps_only_the_end() {
        let unko = "26053118000000000012341";
        let rows = vec![
            ev(unko, 1018, "2026-05-31 18:00:00", "2026-06-01 06:00:00"),
            tc(unko, 1018, "2026-06-01 06:00:00"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 0, "{:?}", d.items);
    }

    /// 行がまるごと窓の外なら桶すら作らない (`skipped_rows` に数える)。
    #[test]
    fn a_row_entirely_outside_the_window_is_skipped() {
        let rows = vec![tc("26050112000000000012341", 1652, "2026-05-01 12:00:00")];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.scanned_unko, 0);
        assert_eq!(d.skipped_rows, 1);
        assert_eq!(d.total, 0);
    }

    /// 休息でない行・`運行NO` が空の行・知らない `source` は突合に使わない。
    #[test]
    fn rows_that_cannot_be_matched_are_counted_not_guessed() {
        let rows = vec![
            json!({"datetime": "2026-06-02 08:00:00", "source": SOURCE_DTAKO,
                   "state": "運行開始", "unko_no": "26060208000000000012341", "driver_id": 1}),
            json!({"datetime": "2026-06-02 08:00:00", "source": SOURCE_DTAKO,
                   "state": REST_STATE, "unko_no": "   ", "driver_id": 1}),
            json!({"datetime": "2026-06-02 08:00:00", "source": "timecard",
                   "state": REST_STATE, "unko_no": "26060208000000000012341", "driver_id": 1}),
            // state が無い / unko_no のキーが無い
            json!({"datetime": "2026-06-02 08:00:00", "source": SOURCE_DTAKO, "driver_id": 1}),
            json!({"datetime": "2026-06-02 08:00:00", "source": SOURCE_DTAKO,
                   "state": REST_STATE, "driver_id": 1}),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.skipped_rows, 5);
        assert_eq!(d.scanned_unko, 0);
    }

    /// `driver_id` が無い行でも突合はする (乗務員が引けないだけ)。
    #[test]
    fn a_row_without_a_driver_still_matches() {
        let unko = "26060208000000000012341";
        let rows = vec![json!({
            "datetime": "2026-06-02 08:00:00", "end_datetime": serde_json::Value::Null,
            "source": SOURCE_DTAKO, "state": REST_STATE, "unko_no": unko,
        })];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 1);
        assert!(d.items[0].driver_cds.is_empty());
        // 乗務員が引けない運行は内訳に立たない
        assert!(d.by_driver.is_empty());
    }

    /// **同じ時刻の 2 行を 1 行に潰さない。** 取り込みが 2 回走った形が消えないこと。
    #[test]
    fn duplicate_timestamps_keep_their_multiplicity() {
        let unko = "26060208000000000012341";
        let rows = vec![
            ev(unko, 7, "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
            tc(unko, 7, "2026-06-02 08:00:00"),
            tc(unko, 7, "2026-06-02 08:00:00"),
            tc(unko, 7, "2026-06-02 09:00:00"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 1);
        assert_eq!(d.items[0].dtako_only, vec!["2026-06-02 08:00:00"]);
        assert!(d.items[0].dtako_events_only.is_empty());
    }

    /// 2 名乗務で表ごとに乗務員が割れても、`運行NO` で束ねるので片側扱いにならない。
    #[test]
    fn a_run_split_across_drivers_stays_one_bucket() {
        let unko = "26060208000000000012341";
        let rows = vec![
            ev(unko, 1412, "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
            tc(unko, 1255, "2026-06-02 08:00:00"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 1);
        assert_eq!(d.items[0].driver_cds, vec![1255, 1412]);
        assert_eq!(d.items[0].dtako_events_only, vec!["2026-06-02 09:00:00"]);
        // 両方の乗務員に 1 件ずつ立つ
        assert_eq!(d.by_driver.get("1255"), Some(&1));
        assert_eq!(d.by_driver.get("1412"), Some(&1));
    }

    /// `運行NO` から運行日が読めなければ `run_date` は `null` (推測で埋めない)。
    #[test]
    fn an_unreadable_unko_no_leaves_the_run_date_null() {
        let rows = vec![tc("nope", 1, "2026-06-02 08:00:00")];
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.total, 1);
        assert!(d.items[0].run_date.is_none());
    }

    /// **上限で切っても総数と乗務員別の内訳は総数のまま。**
    #[test]
    fn the_item_cap_keeps_the_total_and_the_driver_split() {
        let n = MAX_REST_DIFF + 7;
        let rows: Vec<serde_json::Value> = (0..n)
            .map(|i| tc(&format!("260602{i:017}"), 1041, "2026-06-02 08:00:00"))
            .collect();
        let d = rest_diff(&rows, FROM, TO);
        assert_eq!(d.items.len(), MAX_REST_DIFF);
        assert_eq!(d.total, n);
        assert_eq!(d.by_driver.get("1041"), Some(&n));
        assert_eq!(d.scanned_unko, n);
    }

    /// 同じ形どうしの並びは乗務員CD → 運行NO。
    #[test]
    fn items_are_ordered_by_driver_then_unko_no() {
        let rows = vec![
            tc("26060300000000000000002", 1742, "2026-06-03 08:00:00"),
            tc("26060300000000000000001", 1742, "2026-06-03 08:00:00"),
            tc("26060300000000000000003", 1726, "2026-06-03 08:00:00"),
        ];
        let d = rest_diff(&rows, FROM, TO);
        let got: Vec<&str> = d.items.iter().map(|i| i.unko_no.as_str()).collect();
        assert_eq!(
            got,
            vec![
                "26060300000000000000003",
                "26060300000000000000001",
                "26060300000000000000002"
            ]
        );
    }
}
