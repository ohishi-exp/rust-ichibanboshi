//! `dtako_events` の**作業区分 (層 A) を 乗務員 × 暦日 × 区分 の秒数**にして返す
//! (Refs ohishi-exp/nuxt-dtako-admin#612 の PR-2)。
//!
//! 拘束サマリの 6 指標のうち「運転」「荷役」は打刻からは原理的に作れず、出せるのは
//! デジタコの運行イベントだけ (#612)。この口はその**材料だけ**を出す。
//!
//! ## ★ この口は「運転」「荷役」を名乗らない
//!
//! 返すのは `dtako_events` の `イベント名` そのままの 6 区分で、
//! **`荷役 = 積み + 降し` の合成はしない。** 足し方の判断は呼ぶ側 (relay、#612 の
//! PR-3) 1 か所に閉じ込める。
//!
//! 理由は実測で出ている: **`待機` を荷役に足すと壊れる** (2026-06 の 1 名で
//! +542 分)。`待機` は拘束には入るが theearth の運転にも荷役にも入っていない独立の
//! 区分で、しかも 38 名中 4 名しか持たないので**中央値では気づけない**。ここで
//! 合成してしまうと、その判断が 2 か所 (この口と呼ぶ側) に散る。
//!
//! ## 層 A とは — 足してよいものと、足すと二重計上になるもの
//!
//! `dtako_events` の区間は**入れ子ではなく、並行する 3 つの層**だった (#612 の実測、
//! 76 driver-month・運行 延べ約 700 本):
//!
//! | 層 | イベント名 | 性質 |
//! |---|---|---|
//! | **A: 作業区分** | `運転` `積み` `降し` `休憩` `休息` `待機` | **運行スパンを過不足なく敷き詰める** |
//! | B: 道路種別 × 積空 | `一般道実車` `一般道空車` `専用道` `高速道` | 同じく敷き詰める**並行の層**。A に足すと 2 倍になる |
//! | C: 重畳マーカー | `アイドリング` `連続運転` `一般道速度オーバー` `高速道速度オーバー` `専用道速度オーバー` | A/B と**自由に重なる**。足すと二重計上 |
//! | D: 境界 | `運行開始` `運行終了` | 長さ 0 |
//!
//! 層 A の区間を運行ごとに時刻順へ並べると、隣接ペアの「前の終了 == 次の開始」が
//! **100% 成立**し、総和が `運行開始 → 運行終了` に秒まで一致する。
//! **`待機` を層 A に入れないと破れる** — 入れないと 38 名中 4 名で隣接しない
//! ペアが出る。この 1 語で層 A の定義が確定した。
//!
//! ⇒ **二重計上は「層をまたいで足したとき」だけ起きる。層 A の中だけで足せば
//! 起きない。** 層 B/C/D の行は数だけ数えて捨てる (`ignored_rows`)。
//!
//! ## ★ 知らないイベント名は黙って捨てない
//!
//! 層 A〜D のどれにも当たらない名前は `unclassified_states` に**名前と件数を出す**。
//! `待機` の件が示すとおり、**層 A に足りない 1 語は静かに数字を減らすだけ**で、
//! 合計を見ても気づけない。上流に新しい区分が増えたらここで鳴る。
//!
//! ## ファイル名がなぜ `dtako_worktime.rs` か (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/` と `src/routes/` をファイル名の
//! **接頭辞** (`kintai` / `kosoku`) で拾い、拾われた内容のハッシュが `logic_version`
//! (`/api/kintai/version` の etag) に畳まれる。この口は既存の生イベント読み出しを
//! **そのまま再利用するだけ**で `/api/kintai/{daily,kosoku-daily,version}` の応答を
//! 一切変えないので、`kintai` で始まる名前を付けると無関係な deploy まで
//! 全乗務員を stale にしてしまう ([`crate::routes::dtako_day`] と同じ理由)。
//!
//! ## 秒で返す — 分に丸めるのは呼ぶ側
//!
//! **`minutes` ではなく `seconds` (整数、丸めなし) を返す。** 区間の端は秒まで
//! あり、暦日クリップでも秒が出る。ここで (乗務員 × 暦日 × 区分) ごとに分へ丸めると、
//! 月合計で最大 数十分ぶんの丸め誤差が積み上がる — #612 が測った 運転 の月差の
//! 中央値が **4.5 分**なので、その丸めは突合の結論そのものを壊す大きさになる。
//! 秒は丸めていない実測値で、分への変換 (と丸め方の選択) は合成と同じく
//! **呼ぶ側 1 か所**に置く。
//!
//! ## 読む窓 — 暦日にクリップするので `exact_month_range`
//!
//! [`crate::kintai_repo::exact_month_range`] の `[月初, 翌月初)` を使う
//! ([`month_range`](crate::kintai_repo::month_range) の翌月 2 日まで広げる形では
//! ない) — 月外の暦日はどのみち捨てるため。**月初にかかる区間 (開始が前月) は
//! `EVENTS_SQL` の 2 本目のブランチ (「期間内に終わる区間」) が拾う**ので、月初が
//! 短く出ることはない。実測でも `2026-05-31 17:39 → 2026-06-01 04:36` の休息が
//! `month=2026-06` で返っている。
//!
//! **拾えない形が 1 つだけある**: 開始が月初より前で、終了が翌月初以降の区間
//! (= 1 か月を丸ごと覆う区間)。どちらのブランチの条件も満たさない。実データで
//! 観測していないが、**観測していないだけで検出もできない**ので注記として残す。
//!
//! ## read-only
//!
//! 読むだけ。書き込みは 1 本も持たない。判定もしない — 拘束も勤務も畳まず、
//! 区分ごとの秒数をそのまま返す ([`crate::routes::kintai::rest_diff`] と同じ流儀)。

use std::collections::BTreeMap;

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use serde::Deserialize;

use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::{map_repo_err, parse_driver};

/// 集計対象にする `source`。`運行開始` / `運行終了` / `休息` は
/// `time_card_dtako` 由来 (`source = "dtako"`) にも**同じ名前で**出るので、
/// source で絞らないと打刻由来の行を区間として数えてしまう。
const DTAKO_EVENTS_SOURCE: &str = "dtako_events";

/// **層 A (作業区分)。** 運行スパンを過不足なく敷き詰める 6 つ。
/// 応答の `seconds` はこの順で並ぶ。
pub const LAYER_A_STATES: [&str; 6] = ["運転", "積み", "降し", "休憩", "休息", "待機"];

/// 層 B (道路種別 × 積空)。層 A と**並行に**もう一度敷き詰めるので、足すと 2 倍。
const LAYER_B_STATES: [&str; 4] = ["一般道実車", "一般道空車", "専用道", "高速道"];

/// 層 C (重畳マーカー)。層 A/B と自由に重なるので、足すと二重計上。
const LAYER_C_STATES: [&str; 5] = [
    "アイドリング",
    "連続運転",
    "一般道速度オーバー",
    "高速道速度オーバー",
    "専用道速度オーバー",
];

/// 層 D (境界)。長さ 0。
const LAYER_D_STATES: [&str; 2] = ["運行開始", "運行終了"];

/// 月ぶんの生イベント読み出しは重い (全乗務員で 10 万行規模) ので、この口だけで
/// 同時実行を絞る。
///
/// **`kintai.rs` の `KOSOKU_DB_PERMITS` とは別枠**で、合算の上限にはならない
/// (あちらは private static で、`pub` にすると `logic_version` が動くため共有
/// できない)。守れるのは「この口が単独で DB を溢れさせないこと」だけ。
static DTAKO_WORKTIME_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

/// `?month=2026-06[&driver=1041]`。`month` 必須・`driver` は省略可
/// (省略 = 全乗務員)。`driver=` (空) は省略ではなく**不正**として 400。
#[derive(Debug, Deserialize)]
pub struct WorktimeQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
}

/// イベント名がどの層のものか。層 A だけ [`LAYER_A_STATES`] 内の位置を持つ。
#[derive(Debug, PartialEq)]
enum Layer {
    A(usize),
    B,
    C,
    D,
}

/// イベント名 → 層。どの層にも無ければ `None` (= `unclassified_states` 行き)。
fn layer_of(state: &str) -> Option<Layer> {
    if let Some(i) = LAYER_A_STATES.iter().position(|s| *s == state) {
        return Some(Layer::A(i));
    }
    if LAYER_B_STATES.contains(&state) {
        return Some(Layer::B);
    }
    if LAYER_C_STATES.contains(&state) {
        return Some(Layer::C);
    }
    if LAYER_D_STATES.contains(&state) {
        return Some(Layer::D);
    }
    None
}

/// `YYYY-MM-DD HH:MM:SS` を読む。repo が `DATE_FORMAT` で作る唯一の書式。
fn parse_dt(s: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()
}

/// 1 (乗務員, 暦日) ぶんの層 A 秒数。添字は [`LAYER_A_STATES`] の位置。
type DaySeconds = [i64; LAYER_A_STATES.len()];

/// 集計結果。**捨てた行も必ず数える** — 黙って減るのがこの領域で一番高くつく。
#[derive(Debug, Default)]
struct Aggregate {
    /// `(乗務員CD, 暦日)` → 区分ごとの秒数。**居る日は「実測して 0」・
    /// 居ない日は「層 A の区間が 1 本も無い」**で、両者を潰さない。
    days: BTreeMap<(u64, NaiveDate), DaySeconds>,
    /// 秒数に足した行数 (層 A・長さあり・窓に重なる)
    counted_rows: u64,
    /// 層 B として捨てた行数
    layer_b_rows: u64,
    /// 層 C として捨てた行数
    layer_c_rows: u64,
    /// 層 D として捨てた行数
    layer_d_rows: u64,
    /// `source != "dtako_events"` で捨てた行数 (打刻・`time_card_dtako`)
    other_source_rows: u64,
    /// **どの層にも無いイベント名** → 件数。0 件でない = 上流に新しい区分が出た
    unclassified_states: BTreeMap<String, u64>,
    /// `driver_id` が読めなかった行数
    bad_driver_rows: u64,
    /// `datetime` / `end_datetime` が読めなかった行数
    bad_datetime_rows: u64,
    /// `end_datetime` が無い行数 (層 A では起きないはず)
    missing_end_rows: u64,
    /// 終了 < 開始 の行数
    negative_span_rows: u64,
    /// 終了 == 開始 の行数 (長さ 0 の層 A 行。暦日を作らない)
    zero_length_rows: u64,
    /// 窓の外に落として数えなかった秒数 (月をまたぐ区間の対象月外の部分)
    clipped_outside_window_seconds: i64,
}

impl Aggregate {
    /// 層 A の 1 区間を暦日へ切り分けて足す。`[win_from, win_to)` の外は捨て、
    /// 捨てた秒数を [`Aggregate::clipped_outside_window_seconds`] に残す。
    fn add_span(
        &mut self,
        driver_cd: u64,
        idx: usize,
        start: NaiveDateTime,
        end: NaiveDateTime,
        win_from: NaiveDateTime,
        win_to: NaiveDateTime,
    ) {
        let total = (end - start).num_seconds();
        let (lo, hi) = (start.max(win_from), end.min(win_to));
        if hi <= lo {
            self.clipped_outside_window_seconds += total;
            return;
        }
        self.clipped_outside_window_seconds += total - (hi - lo).num_seconds();
        self.counted_rows += 1;
        let mut cur = lo;
        while cur < hi {
            let day = cur.date();
            // 翌日 00:00 で切る。`succ_opt()` が None になるのは NaiveDate::MAX
            // だけで、そのときは窓の終わりまで一息に足す (日跨ぎを諦める)
            let midnight = day.succ_opt().and_then(|d| d.and_hms_opt(0, 0, 0));
            let stop = match midnight {
                Some(m) if m < hi => m,
                _ => hi,
            };
            let row = self.days.entry((driver_cd, day)).or_insert([0; 6]);
            row[idx] += (stop - cur).num_seconds();
            cur = stop;
        }
    }

    /// 応答 JSON。`days` は `(乗務員CD, 暦日)` 昇順、`seconds` は
    /// [`LAYER_A_STATES`] の順で 6 個そろえる (欠けたキーを作らない)。
    fn to_json(&self, month: &str, driver: Option<u64>, from: &str, to: &str) -> serde_json::Value {
        let days: Vec<serde_json::Value> = self
            .days
            .iter()
            .map(|((driver_cd, date), secs)| {
                let seconds: serde_json::Map<String, serde_json::Value> = LAYER_A_STATES
                    .iter()
                    .zip(secs.iter())
                    .map(|(name, v)| ((*name).to_string(), serde_json::json!(v)))
                    .collect();
                serde_json::json!({
                    "driver_cd": driver_cd,
                    "date": date.to_string(),
                    "seconds": seconds,
                })
            })
            .collect();
        serde_json::json!({
            "month": month,
            "driver": driver,
            "from": from,
            "to": to,
            // 足してよい区分の一覧そのもの。呼ぶ側が「荷役」を組むための材料
            "layer_a_states": LAYER_A_STATES,
            "days": days,
            "counted_rows": self.counted_rows,
            // **黙って減らさない** — 捨てた行は層ごとに数を出す
            "ignored_rows": {
                "layer_b": self.layer_b_rows,
                "layer_c": self.layer_c_rows,
                "layer_d": self.layer_d_rows,
                "other_source": self.other_source_rows,
            },
            // ★ 空でなければ上流に新しい区分が出ている (層 A の取りこぼし候補)
            "unclassified_states": self.unclassified_states,
            "unusable_rows": {
                "bad_driver": self.bad_driver_rows,
                "bad_datetime": self.bad_datetime_rows,
                "missing_end": self.missing_end_rows,
                "negative_span": self.negative_span_rows,
                "zero_length": self.zero_length_rows,
            },
            "clipped_outside_window_seconds": self.clipped_outside_window_seconds,
        })
    }
}

/// 生行 → 集計。**判定はしない** — 層 A の区間を暦日に切って足すだけ。
fn aggregate(
    rows: &[serde_json::Value],
    win_from: NaiveDateTime,
    win_to: NaiveDateTime,
) -> Aggregate {
    let mut agg = Aggregate::default();
    for row in rows {
        if row.get("source").and_then(|v| v.as_str()) != Some(DTAKO_EVENTS_SOURCE) {
            agg.other_source_rows += 1;
            continue;
        }
        // 名前が無い行は空文字キーで `unclassified_states` に出る (黙って消さない)
        let state = row
            .get("state")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let idx = match layer_of(state) {
            Some(Layer::A(i)) => i,
            Some(Layer::B) => {
                agg.layer_b_rows += 1;
                continue;
            }
            Some(Layer::C) => {
                agg.layer_c_rows += 1;
                continue;
            }
            Some(Layer::D) => {
                agg.layer_d_rows += 1;
                continue;
            }
            None => {
                *agg.unclassified_states
                    .entry(state.to_string())
                    .or_insert(0) += 1;
                continue;
            }
        };
        let Some(driver_cd) = row.get("driver_id").and_then(|v| v.as_u64()) else {
            agg.bad_driver_rows += 1;
            continue;
        };
        let Some(start) = row
            .get("datetime")
            .and_then(|v| v.as_str())
            .and_then(parse_dt)
        else {
            agg.bad_datetime_rows += 1;
            continue;
        };
        let Some(end_raw) = row.get("end_datetime").and_then(|v| v.as_str()) else {
            agg.missing_end_rows += 1;
            continue;
        };
        let Some(end) = parse_dt(end_raw) else {
            agg.bad_datetime_rows += 1;
            continue;
        };
        if end < start {
            agg.negative_span_rows += 1;
            continue;
        }
        if end == start {
            agg.zero_length_rows += 1;
            continue;
        }
        agg.add_span(driver_cd, idx, start, end, win_from, win_to);
    }
    agg
}

/// GET /api/dtako/worktime?month=YYYY-MM[&driver=1041] — `dtako_events` の
/// **層 A (作業区分) を 乗務員 × 暦日 × 区分 の秒数**で返す
/// (Refs ohishi-exp/nuxt-dtako-admin#612 の PR-2)。
///
/// **データ源は `/api/kintai/events` と同じ repo 関数**
/// ([`crate::kintai_repo::KintaiEventsApi`]) — SQL は 1 本も増やさない。
/// `driver` を省くと全乗務員版 (`fetch_all_events_between`) を 1 回だけ叩く。
///
/// **read-only・判定なし・合成なし。** 「運転」「荷役」は名乗らない
/// (モジュール docs 参照)。
pub async fn worktime(
    Query(params): Query<WorktimeQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    let Some((from, to)) = crate::kintai_repo::exact_month_range(&month) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    };
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
    let _permit = DTAKO_WORKTIME_PERMITS
        .acquire()
        .await
        .expect("semaphore open");
    let rows = match driver {
        Some(d) => repo.fetch_events_between(&from, &to, d).await,
        None => repo.fetch_all_events_between(&from, &to).await,
    }
    .map_err(map_repo_err)?;
    // `exact_month_range` が作った書式なので必ず読める
    let win_from = parse_dt(&from).expect("exact_month_range の from");
    let win_to = parse_dt(&to).expect("exact_month_range の to");
    let agg = aggregate(&rows, win_from, win_to);
    // マクロは 1 行に収める (CLAUDE.md)
    let (days, counted) = (agg.days.len(), agg.counted_rows);
    tracing::info!(month = %month, days, counted, "dtako worktime built");
    // 知らない区分は 0 でも出す (層 A の取りこぼしが見えるように)
    let unknown = agg.unclassified_states.len();
    tracing::info!(unknown, "dtako worktime unknown");
    Ok(Json(agg.to_json(&month, driver, &from, &to)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::routing::get;
    use axum::Router;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn ev(state: &str, start: &str, end: Value, driver: Value) -> Value {
        json!({
            "source": "dtako_events",
            "state": state,
            "datetime": start,
            "end_datetime": end,
            "driver_id": driver,
        })
    }

    fn span(state: &str, start: &str, end: &str) -> Value {
        ev(state, start, json!(end), json!(1041))
    }

    fn win() -> (NaiveDateTime, NaiveDateTime) {
        (
            parse_dt("2026-06-01 00:00:00").unwrap(),
            parse_dt("2026-07-01 00:00:00").unwrap(),
        )
    }

    fn secs_of(agg: &Aggregate, driver: u64, date: &str) -> DaySeconds {
        let d = NaiveDate::parse_from_str(date, "%Y-%m-%d").unwrap();
        *agg.days.get(&(driver, d)).unwrap()
    }

    #[test]
    fn layer_of_classifies_every_known_state() {
        assert_eq!(layer_of("運転"), Some(Layer::A(0)));
        assert_eq!(layer_of("待機"), Some(Layer::A(5)));
        assert_eq!(layer_of("一般道実車"), Some(Layer::B));
        assert_eq!(layer_of("高速道"), Some(Layer::B));
        assert_eq!(layer_of("アイドリング"), Some(Layer::C));
        assert_eq!(layer_of("専用道速度オーバー"), Some(Layer::C));
        assert_eq!(layer_of("運行開始"), Some(Layer::D));
        assert_eq!(layer_of("運行終了"), Some(Layer::D));
        assert_eq!(layer_of("荷役"), None);
    }

    /// ★ `待機` が層 A から落ちると 38 名中 4 名で敷き詰めが破れる (#612 の実測)。
    /// 落ちても合計は「少し減る」だけで、値を見ても気づけない — 落ちていないことを
    /// 名指しで固定する (陰性対照つき: `荷役` は層 A に無い)。
    #[test]
    fn layer_a_is_exactly_the_six_states() {
        assert_eq!(
            LAYER_A_STATES,
            ["運転", "積み", "降し", "休憩", "休息", "待機"]
        );
        assert!(LAYER_A_STATES.contains(&"待機"));
        assert!(!LAYER_A_STATES.contains(&"荷役"));
        assert!(!LAYER_A_STATES.contains(&"一般道実車"));
    }

    #[test]
    fn parse_dt_reads_repo_format_only() {
        assert!(parse_dt("2026-06-01 08:34:08").is_some());
        assert!(parse_dt("2026-06-01T08:34:08").is_none());
        assert!(parse_dt("").is_none());
    }

    /// 同じ暦日に収まる区間はその日だけに乗る。
    #[test]
    fn same_day_span_lands_on_one_day() {
        let (f, t) = win();
        let rows = vec![span("運転", "2026-06-02 08:00:00", "2026-06-02 09:30:30")];
        let agg = aggregate(&rows, f, t);
        assert_eq!(agg.counted_rows, 1);
        assert_eq!(agg.days.len(), 1);
        assert_eq!(secs_of(&agg, 1041, "2026-06-02")[0], 5430);
        assert_eq!(agg.clipped_outside_window_seconds, 0);
    }

    /// ★ 日跨ぎは暦日で割る (実測: 休息 `05-31 17:39 → 06-01 04:36` は普通に出る)。
    /// 3 日にまたがる区間で「途中の丸 1 日 = 86400 秒」まで確かめる。
    #[test]
    fn cross_midnight_span_is_clipped_per_calendar_day() {
        let (f, t) = win();
        let rows = vec![span("休息", "2026-06-10 22:00:00", "2026-06-12 03:00:00")];
        let agg = aggregate(&rows, f, t);
        assert_eq!(agg.days.len(), 3);
        assert_eq!(secs_of(&agg, 1041, "2026-06-10")[4], 7200);
        assert_eq!(secs_of(&agg, 1041, "2026-06-11")[4], 86400);
        assert_eq!(secs_of(&agg, 1041, "2026-06-12")[4], 10800);
    }

    /// 月初にかかる区間 (開始が前月) は、対象月にかかった分だけ数える。
    /// 落とした分は `clipped_outside_window_seconds` に残す。
    #[test]
    fn span_starting_before_window_keeps_only_the_in_month_part() {
        let (f, t) = win();
        let rows = vec![span("休息", "2026-05-31 22:00:00", "2026-06-01 04:00:00")];
        let agg = aggregate(&rows, f, t);
        assert_eq!(agg.days.len(), 1);
        assert_eq!(secs_of(&agg, 1041, "2026-06-01")[4], 14400);
        assert_eq!(agg.clipped_outside_window_seconds, 7200);
    }

    /// 窓に 1 秒も重ならない区間は 1 日も作らず、秒数を全部 outside に落とす。
    #[test]
    fn span_entirely_outside_window_creates_no_day() {
        let (f, t) = win();
        let rows = vec![span("運転", "2026-05-30 01:00:00", "2026-05-30 02:00:00")];
        let agg = aggregate(&rows, f, t);
        assert!(agg.days.is_empty());
        assert_eq!(agg.counted_rows, 0);
        assert_eq!(agg.clipped_outside_window_seconds, 3600);
    }

    /// ★ 層 B/C/D は 1 秒も足さない (足すと二重計上)。件数だけ残す。
    #[test]
    fn other_layers_are_counted_but_never_summed() {
        let (f, t) = win();
        let rows = vec![
            span("運転", "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
            span("一般道実車", "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
            span("アイドリング", "2026-06-02 08:10:00", "2026-06-02 08:20:00"),
            span("運行開始", "2026-06-02 08:00:00", "2026-06-02 08:00:00"),
            json!({"source": "dtako", "state": "運行開始", "datetime": "2026-06-02 08:00:00", "end_datetime": null, "driver_id": 1041}),
        ];
        let agg = aggregate(&rows, f, t);
        assert_eq!(secs_of(&agg, 1041, "2026-06-02"), [3600, 0, 0, 0, 0, 0]);
        assert_eq!(agg.layer_b_rows, 1);
        assert_eq!(agg.layer_c_rows, 1);
        assert_eq!(agg.layer_d_rows, 1);
        assert_eq!(agg.other_source_rows, 1);
    }

    /// ★ 知らない名前は黙って捨てず、名前と件数で鳴らす。
    #[test]
    fn unknown_states_are_reported_by_name() {
        let (f, t) = win();
        let rows = vec![
            span("新区分", "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
            span("新区分", "2026-06-03 08:00:00", "2026-06-03 09:00:00"),
            json!({"source": "dtako_events", "datetime": "2026-06-04 08:00:00", "end_datetime": "2026-06-04 09:00:00", "driver_id": 1041}),
        ];
        let agg = aggregate(&rows, f, t);
        assert_eq!(agg.unclassified_states.get("新区分"), Some(&2));
        assert_eq!(agg.unclassified_states.get(""), Some(&1));
        assert!(agg.days.is_empty());
    }

    /// 読めない行は種類ごとに数える (0 で埋めて「実測した」ことにしない)。
    #[test]
    fn unusable_rows_are_counted_by_kind() {
        let (f, t) = win();
        let rows = vec![
            ev(
                "運転",
                "2026-06-02 08:00:00",
                json!("2026-06-02 09:00:00"),
                json!("x"),
            ),
            ev(
                "運転",
                "not-a-date",
                json!("2026-06-02 09:00:00"),
                json!(1041),
            ),
            ev("運転", "2026-06-02 08:00:00", json!(null), json!(1041)),
            ev("運転", "2026-06-02 08:00:00", json!("nope"), json!(1041)),
            span("運転", "2026-06-02 09:00:00", "2026-06-02 08:00:00"),
            span("運転", "2026-06-02 08:00:00", "2026-06-02 08:00:00"),
        ];
        let agg = aggregate(&rows, f, t);
        assert_eq!(agg.bad_driver_rows, 1);
        assert_eq!(agg.bad_datetime_rows, 2);
        assert_eq!(agg.missing_end_rows, 1);
        assert_eq!(agg.negative_span_rows, 1);
        assert_eq!(agg.zero_length_rows, 1);
        assert!(agg.days.is_empty());
    }

    /// ★ 合成しない — `積み` と `降し` は別々のまま返る (荷役は呼ぶ側が組む)。
    /// `待機` も独立したまま (荷役に足すと壊れる)。
    #[test]
    fn loading_states_stay_separate_and_taiki_is_independent() {
        let (f, t) = win();
        let rows = vec![
            span("積み", "2026-06-02 08:00:00", "2026-06-02 08:30:00"),
            span("降し", "2026-06-02 13:00:00", "2026-06-02 13:20:00"),
            span("待機", "2026-06-02 15:00:00", "2026-06-02 23:00:00"),
        ];
        let agg = aggregate(&rows, f, t);
        let s = secs_of(&agg, 1041, "2026-06-02");
        assert_eq!(s[1], 1800);
        assert_eq!(s[2], 1200);
        assert_eq!(s[5], 28800);
    }

    fn app(repo: Arc<dyn crate::kintai_repo::KintaiEventsApi>) -> Router {
        Router::new()
            .route("/api/dtako/worktime", get(worktime))
            .layer(Extension(repo as DynKintaiEventsRepo))
    }

    async fn call(
        repo: Arc<dyn crate::kintai_repo::KintaiEventsApi>,
        uri: &str,
    ) -> (StatusCode, Value) {
        let res = app(repo)
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
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
        (status, body)
    }

    /// 読み口 2 本を差し替えるだけの mock。`fail` で `NotConfigured` (= 503) 側へ
    /// 倒す。**2 つの struct に分けない** — 分けると使わない側の impl が丸ごと
    /// 未カバーになり、100% gate が「テストで触っていない足場」で落ちる。
    struct MockRepo {
        one: Vec<Value>,
        all: Vec<Value>,
        fail: bool,
    }

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for MockRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            if self.fail {
                return Err(crate::kintai_repo::KintaiRepoError::NotConfigured);
            }
            Ok(self.one.clone())
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            if self.fail {
                return Err(crate::kintai_repo::KintaiRepoError::NotConfigured);
            }
            Ok(self.all.clone())
        }

        /// trait の必須メソッドなので実装は要るが、この口は**フェリーを読まない**。
        /// 空配列で埋めると「読んでも 0 件」と区別が付かなくなるので、触ったら
        /// 落ちるようにしておく (下の `#[should_panic]` が発火を固定している)。
        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("worktime はフェリーを読まない")
        }
    }

    fn mock() -> Arc<MockRepo> {
        Arc::new(MockRepo {
            fail: false,
            one: vec![span("運転", "2026-06-02 08:00:00", "2026-06-02 09:00:00")],
            all: vec![
                span("運転", "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
                ev(
                    "積み",
                    "2026-06-02 10:00:00",
                    json!("2026-06-02 10:30:00"),
                    json!(1368),
                ),
            ],
        })
    }

    #[tokio::test]
    async fn rejects_bad_month_and_bad_driver() {
        let (s, _) = call(mock(), "/api/dtako/worktime").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let (s, _) = call(mock(), "/api/dtako/worktime?month=2026-13").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let (s, _) = call(mock(), "/api/dtako/worktime?month=2026-06&driver=").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let (s, _) = call(mock(), "/api/dtako/worktime?month=2026-06&driver=abc").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
    }

    fn failing() -> Arc<MockRepo> {
        Arc::new(MockRepo {
            fail: true,
            one: Vec::new(),
            all: Vec::new(),
        })
    }

    /// 読めなかったら 503 で fail-closed (空配列で「0 件」に見せない)。
    /// **全乗務員版と 1 名版の両方**を測る — 分岐が 2 本あるので片方だけだと
    /// もう片方の失敗経路が未検証のまま残る。
    #[tokio::test]
    async fn repo_error_maps_to_status_on_both_paths() {
        let (s, _) = call(failing(), "/api/dtako/worktime?month=2026-06").await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        let (s, _) = call(failing(), "/api/dtako/worktime?month=2026-06&driver=1041").await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// ★ この口はフェリーを読まない。読み口を足したときに黙って通らないよう、
    /// mock が落ちること自体を固定する (陰性対照)。
    #[tokio::test]
    #[should_panic(expected = "worktime はフェリーを読まない")]
    async fn ferry_is_never_read() {
        use crate::kintai_repo::KintaiEventsApi;
        let _ = mock().fetch_ferry_between("a", "b", None).await;
    }

    /// `driver` 省略 = 全乗務員版を読む。窓と層一覧も応答に出る。
    #[tokio::test]
    async fn all_drivers_shape() {
        let (s, body) = call(mock(), "/api/dtako/worktime?month=2026-06").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["month"], json!("2026-06"));
        assert_eq!(body["driver"], json!(null));
        assert_eq!(body["from"], json!("2026-06-01 00:00:00"));
        assert_eq!(body["to"], json!("2026-07-01 00:00:00"));
        assert_eq!(body["layer_a_states"], json!(LAYER_A_STATES));
        assert_eq!(body["days"].as_array().unwrap().len(), 2);
        assert_eq!(body["days"][0]["driver_cd"], json!(1041));
        assert_eq!(body["days"][0]["date"], json!("2026-06-02"));
        assert_eq!(body["days"][0]["seconds"]["運転"], json!(3600));
        assert_eq!(body["days"][0]["seconds"]["積み"], json!(0));
        assert_eq!(body["days"][1]["driver_cd"], json!(1368));
        assert_eq!(body["days"][1]["seconds"]["積み"], json!(1800));
        assert_eq!(body["counted_rows"], json!(2));
        assert_eq!(body["unclassified_states"], json!({}));
        assert_eq!(body["ignored_rows"]["layer_b"], json!(0));
        assert_eq!(body["unusable_rows"]["zero_length"], json!(0));
        assert_eq!(body["clipped_outside_window_seconds"], json!(0));
    }

    /// `driver` 指定 = 1 名版を読む。
    #[tokio::test]
    async fn single_driver_shape() {
        let (s, body) = call(mock(), "/api/dtako/worktime?month=2026-06&driver=1041").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["driver"], json!(1041));
        assert_eq!(body["days"].as_array().unwrap().len(), 1);
        assert_eq!(body["days"][0]["seconds"]["運転"], json!(3600));
    }
}
