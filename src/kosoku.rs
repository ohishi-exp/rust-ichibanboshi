//! 拘束時間の日別サマリ — 純粋ロジック (拘束時間の打刻基準化 Phase 2、Refs #118)。
//!
//! `/api/kintai/events` が返す**解釈しない生イベント列**を受け取り、日別サマリへ畳む。
//! **DB も HTTP も触らない** — 入力は `Vec<serde_json::Value>`、出力は `Vec<DaySummary>`
//! だけなので、実データを使わずにテストできる。
//!
//! ## 規則 (#118 で確定、2026-07-27)
//!
//! ### 就業時間 (始業・終業) の決め方
//!
//! 1. **打刻があればタイムカードを使う** — 実測で打刻は運行を完全に包んでいた
//!    (乗務員 1018 / 2026-06 の全 23 日で、始業打刻 → 運行開始が中央 +7 分、
//!    運行終了 → 終業打刻が中央 +3 分、逆転ゼロ)
//! 2. **打刻が無ければ休息イベント** — 休息の終了 = 始業、休息の開始 = 終業
//!
//! **運行の開始・終了では切らない。同日の運行の継ぎ目は「作業」**であって休憩ではない
//! (拘束にも実働にも残す)。ただし継ぎ目に終業打刻があればそこで勤務が切れ、次の始業まで
//! は休息になる — 打刻優先の規則がそのまま効く。実測 (96 名 / 2026-06) では同日の継ぎ目
//! 95 箇所の内訳が「終業打刻あり 27 / 何もない 68 / 休息イベントあり 0」で、この 2 通りで
//! 全件を処理できる。 実測では運行の継ぎ目が 4〜112 分 (中央 8 分)
//! しかなく、勤務の切れ目ではない (荷を降ろして次の伝票を積んで出るだけ)。運行で
//! 切ると乗務員 1119 の 2026-06 は 28 勤務になるが、切らないと 1 か月が 1 勤務
//! (拘束 767 時間) になる。休息で切ると 26 勤務・拘束 16,394 分で、現行の
//! 出勤 26 日・16,278 分とほぼ一致した。
//!
//! 打刻と休息は補完関係にある (長距離で日跨ぎの乗務員は打刻の機会が無い代わりに
//! 休息が記録され、日帰りの乗務員は打刻が揃う代わりに休息が発生しない)。
//!
//! ### 時間区分
//!
//! | 区分 | 範囲 | 割増 |
//! |---|---|---|
//! | 所定内 | 実働 0〜7.5h | 1.00 |
//! | 法定内残業 | 7.5〜8h | 1.00 |
//! | 法定時間外 | 8h 超 | 1.25 |
//! | 法定休日労働 | 日曜の実働すべて | 1.35 |
//!
//! **法定休日 = 日曜日。** 祝日は割増に使わない (所定休日であり固有の割増を持たない)。
//! 賃金側の実測でも法定休日は日曜の労働時間とちょうど一致していた (685 乗務員月で
//! 例外ゼロ)。祝日はタイムカード表示用に別途 API 化する (#119)。
//!
//! ### 深夜 (22:00〜05:00)
//!
//! **所定内/時間外 × 平日/法定休日**の直積で持つ。法定外休日は持たない — 固有の
//! 割増が無い (通常労働 + 週 40 時間超で 1.25) ため構造的に不要。
//!
//! - [`DaySummary::night_minutes`] … 平日の所定内・法定内残業に重なる深夜。どちらも
//!   基礎が 1.00 なので深夜の 0.25 を上乗せするだけで足り、区別する必要がない
//! - [`DaySummary::overtime_night_minutes`] … 平日の法定時間外に重なる深夜 (1.25+0.25)
//! - [`DaySummary::legal_holiday_night_minutes`] … 法定休日に重なる深夜 (1.35+0.25)
//!
//! **深夜と時間外深夜は排他。** 同じ 1 分が両方に入ることはない。
//!
//! ### 休憩
//!
//! 閾値 (既定 10 分) 以上の `休憩` イベントだけを数える。**拘束からは外さない** —
//! 拘束 = 終業 − 始業 − 中の休息、実働 = 拘束 − 休憩。
//!
//! ### 勤務の中に残った休息
//!
//! 休息は勤務の境界だが ([`shifts_from_rest`])、**打刻優先** (#118) の勤務では中に
//! 残る。休憩として数えられないので、外さないと**実働として払う**ことになる。
//! **拘束からも実働からも外す** (ユーザー決定 2026-07-28、
//! Refs ohishi-exp/nuxt-dtako-admin#501)。外した分は
//! [`DaySummary::rest_minus_minutes`] に出す。
//!
//! 実測 (1194 陣野 / 2026-03) は夜勤 11 日すべてで 6〜7 時間の休息が勤務の中にあり、
//! 03-11 は 拘束 18h09m / 休憩 1h39m (= 休憩イベント 63 + 36 分ちょうど) / 実働
//! 16h30m で、402 分の休息がまるごと実働に入っていた。紙のタイムカード表 (社内
//! CakePHP) は同じ 402 分を引いて 684 分にしており、11 日ぶんの残差 (-4464 分) は
//! 休息の暦日按分と **合計 -15 分** まで一致する。
//!
//! **運行に出ていない勤務は昼休憩 (12:00-13:00) を引く** (ユーザー決定 2026-07-28)。
//! 休憩イベントはデジタコにしか残らないので、事務・作業・整備、および乗務員が構内
//! 作業だけした日は休憩が 1 分も出ず、実働 = 拘束 になっていた (実測 2026-04: 事務
//! 1065 は 23 日すべて休憩 0 で拘束 293.8h = 実働 293.8h、一方 乗務 1021 は 26 日で
//! 休憩 63.1h)。運行中かどうかは [`has_operation`] が判定し、**イベント由来の休憩と
//! 昼休憩は排他** — 運行中の日に昼休憩を足すと昼を二重に引く。
//!
//! **昼の窓に 1 分も掛からない勤務は、拘束が 6 時間を超えていれば 1 時間を勤務の
//! まん中に置く** ([`off_hours_break`]、ユーザー指示 2026-07-28)。昼休憩は時計の窓
//! なので夜勤には当たらず、作業員 1196 の 23:45 → 08:00 の夜勤 18 日が全日 休憩 0
//! (拘束 149.9h = 実働) のまま残っていた。6 時間以下を除くのは 19:00 → 00:00 の
//! 5 時間勤務 (事務 1706 / 1707) や 3 時間勤務 (1573) に 1 時間を引かないため。
//!
//! ### 24 時間を超える拘束
//!
//! たまに日付を跨いで 24 時間以上続く運行がある (実測では最長 39.2 時間)。以前は
//! 24 時間で打ち切っていたが、**打ち切らない** (Refs #152)。帰宅日が混ざったのでは
//! なく実在する長時間拘束だと実測で確かめたうえ、打ち切ると改善基準違反がその分だけ
//! 小さく見えるため。[`DaySummary::over_24h`] を立てて遵守チェックに回す。
//!
//! ## 月境界
//!
//! **勤務は始業日で当月に振り分ける。** 月初の勤務の始業は、前月末に始まって月初に
//! 終わる休息の**終わり**で決まる。読み出し側 ([`crate::kintai_repo`] の `EVENTS_SQL`) は
//! 「期間内に始まる区間」に加えて「期間内に終わる区間」も拾うので、この休息は範囲に入る。
//! 拾い漏らすと毎月 1 日目の勤務が静かに欠ける。

use std::collections::{BTreeMap, BTreeSet};

use chrono::{Datelike, Duration, NaiveDateTime, Timelike, Weekday};
use serde::Serialize;

/// 生イベントの日時書式 (`kintai_repo` が `DATE_FORMAT` で文字列化したもの)。
const FMT: &str = "%Y-%m-%d %H:%M:%S";

/// 深夜の開始時 (この時以降)。
const NIGHT_FROM_HOUR: u32 = 22;
/// 深夜の終了時 (この時未満)。
const NIGHT_TO_HOUR: u32 = 5;

/// 昼休憩の開始時 / 終了時。**運行に出ていない勤務だけ**この窓を休憩として引く
/// ([`has_operation`] / [`lunch_windows`])。
const LUNCH_FROM_HOUR: u32 = 12;
const LUNCH_TO_HOUR: u32 = 13;

/// 昼の窓に 1 分も掛からない勤務 (夜勤・夕方だけ等) に入れる休憩 (分)。
///
/// 昼休憩の窓は時計で決めているので、夜勤にはそもそも当たらない。実測 2026-04 の
/// 作業員 1196 は 23:45 → 08:00 の夜勤 18 日が全日 休憩 0 (拘束 149.9h = 実働) の
/// まま残っていた。**1 時間引く** (ユーザー指示 2026-07-28)。
const OFF_HOURS_BREAK_MINUTES: i64 = 60;

/// 昼の窓に掛からない勤務へ休憩を入れる下限の拘束 (分)。**これ以下は入れない。**
///
/// 労基法 34 条が休憩を義務づける最初の閾値 (6 時間超) に合わせる。実測では
/// 19:00 → 00:00 の 5 時間勤務 (事務 1706 / 1707) や 07:00 → 10:00 の 3 時間勤務
/// (1573) があり、これらに 1 時間を引くのは実態にも法にも合わない。
const OFF_HOURS_BREAK_MIN_RESTRAINT_MINUTES: i64 = 6 * 60;

/// 1 勤務の拘束の上限 (分)。これを超えたら**打ち切る**。
///
/// たまに日付を跨いで 24 時間以上続く運行がある (実測では最長 2288 分 = 38.1 時間、
/// 2026-04 の乗務員 1442)。**例外的な外れ値**であり、24 時間を超える拘束は
/// 改善基準告示に照らして明確な違反なので、正確に積み上げる意味がない。
/// これを超えた勤務は [`DaySummary::over_24h`] を立てて遵守チェックに回す。
/// **値は打ち切らない** — 打ち切ると違反がその分だけ小さく見える (Refs #152)。
const MAX_RESTRAINT_MINUTES: i64 = 24 * 60;

/// 勤務の**秒の落とし方**。紙のタイムカード表 (社内 CakePHP) との突合で 1 分ずれる
/// 原因がここだったので、あとで戻せるよう設定にした (Refs ohishi-exp/nuxt-dtako-admin#501)。
///
/// 実測 (2026-03、129 名): 両方に居る乗務員の暦日 467 日のうち **204 日が 1〜2 分差**で、
/// その大半がこの違いによるもの。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestraintRounding {
    /// **紙と同じ**: 経過時間を切り捨てる。`floor(end - start)`。
    ///
    /// nginx 側は `date_diff()` の `->h * 60 + ->i` で**差の秒を捨てて**いる
    /// (`TimeCardKosokuController::_make_tc_to_tc`)。始業 09:00:30 / 終業 17:00:20 なら
    /// 経過 7:59:50 → **479 分**。
    TruncateElapsed,
    /// 従来: 両端をそれぞれ分に切り捨ててから引く。`floor(end) - floor(start)`。
    ///
    /// 同じ例で 09:00 → 17:00 の **480 分**。終業の秒が始業の秒より小さいとき、
    /// [`RestraintRounding::TruncateElapsed`] より 1 分**大きく**なる。
    FloorEndpoints,
}

/// 日別サマリの計算パラメータ。
#[derive(Debug, Clone, Copy)]
pub struct KosokuParams {
    /// 休憩として数える最小の長さ (分)。これ未満の `休憩` イベントは無視する。
    pub break_threshold_minutes: i64,
    /// 所定労働時間 (分)。既定 450 = 7.5 時間。
    pub prescribed_minutes: i64,
    /// 法定労働時間 (分)。既定 480 = 8 時間。`prescribed_minutes` との差が法定内残業。
    pub legal_minutes: i64,
    /// 秒の落とし方。既定は紙に合わせた [`RestraintRounding::TruncateElapsed`]。
    pub restraint_rounding: RestraintRounding,
}

impl Default for KosokuParams {
    fn default() -> Self {
        Self {
            break_threshold_minutes: 10,
            prescribed_minutes: 450,
            legal_minutes: 480,
            restraint_rounding: RestraintRounding::TruncateElapsed,
        }
    }
}

/// 生行から必要な列だけ取り出したイベント。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub start: NaiveDateTime,
    /// 区間イベント (`dtako_events` 由来) だけ持つ。
    pub end: Option<NaiveDateTime>,
    pub source: String,
    pub state: String,
}

/// 勤務 1 回。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Shift {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
    /// 始業・終業をどちらの規則で決めたか。
    pub source: ShiftSource,
}

/// 勤務の境界をどこから取ったか。応答に載せて、後から差分の理由を追えるようにする。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ShiftSource {
    /// 打刻 (`始業` / `終業`)
    Timecard,
    /// 休息イベント (休息の終了 → 次の休息の開始)
    Rest,
}

/// 日跨ぎ勤務を**暦日で按分**した 1 日分 (Refs #130)。
///
/// 現行の拘束時間管理表 (社内 CakePHP) は拘束を暦日へ配っている。勤務を始業日へ丸ごと
/// 寄せた `DaySummary` とは日別の見え方が変わる (月合計は一致) ため、同じ基準で読める
/// ように内訳を添える。
///
/// **1 日で終わる勤務では空** — その場合は内訳が `DaySummary` そのものになるので、
/// 応答を膨らませる意味が無い。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DayPart {
    /// 暦日 (`YYYY-MM-DD`)。
    pub date: String,
    /// その日に乗った拘束 (休憩を含む)。
    pub restraint_minutes: i64,
    /// その日に乗った実働。
    pub working_minutes: i64,
    /// その日に乗った法定時間外。
    pub overtime_minutes: i64,
    /// その日に乗った法定休日労働。
    pub legal_holiday_minutes: i64,
    /// その日に乗った深夜 (所定内・法定内残業ぶん)。
    pub night_minutes: i64,
    /// その日に乗った時間外深夜。
    pub overtime_night_minutes: i64,
    /// その日に乗った法定休日の深夜。
    pub legal_holiday_night_minutes: i64,
    /// **紙のタイムカード表がこの暦日から引いている同日フェリー控除** (Refs #146)。
    ///
    /// こちらの拘束には**入っていない** (足しても引いてもいない)。紙との差の原因を
    /// 説明するためだけに載せる。詳細は [`ferry_minus_by_date`]。
    pub ferry_minus_minutes: i64,
}

impl DayPart {
    fn new(date: chrono::NaiveDate) -> Self {
        Self {
            date: date.format("%Y-%m-%d").to_string(),
            restraint_minutes: 0,
            working_minutes: 0,
            overtime_minutes: 0,
            legal_holiday_minutes: 0,
            night_minutes: 0,
            overtime_night_minutes: 0,
            legal_holiday_night_minutes: 0,
            ferry_minus_minutes: 0,
        }
    }
}

/// 勤務を構成した打刻 1 つ (Refs #128)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Punch {
    /// 打刻時刻 (`YYYY-MM-DD HH:MM:SS`)。**秒を落とさない** — 打刻カードの元の値。
    pub at: String,
    /// `始業` / `終業`。
    pub state: String,
}

/// 日別サマリ 1 行。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DaySummary {
    /// 始業日 (`YYYY-MM-DD`)。日跨ぎ勤務もここに寄せる。
    pub date: String,
    pub start: String,
    pub end: String,
    pub source: ShiftSource,
    /// この勤務の中にあった**打刻そのもの** (時刻順、Refs #128)。
    ///
    /// `start` / `end` は勤務としての解釈 (分に丸め、24 時間で打ち切り) が入っているが、
    /// **こちらは生の打刻**。社内タイムカード表は打刻を日ごとに並べただけのもので、
    /// 勤務という単位を持たないため、同じ表を作るには元の時刻が要る。
    ///
    /// - **打ち切り前の区間から拾う** — 24 時間で切ると、切った先にある終業打刻が
    ///   落ちる (実測: 乗務員 1194 の 2026-04-01 始業 → 2026-04-03 16:47 終業)
    /// - 休息イベント由来の勤務 (`source: rest`) は空
    pub punches: Vec<Punch>,
    /// **暦日按分の内訳** (Refs #130)。日跨ぎ勤務だけ入り、1 日で終わる勤務は空。
    ///
    /// この行の各分数は勤務を**始業日へ丸ごと寄せた**値。現行の拘束時間管理表は
    /// 暦日へ配っているので、同じ基準で読みたい消費者はこちらを足し合わせる。
    pub parts: Vec<DayPart>,
    /// 始業日が日曜か。
    pub is_legal_holiday: bool,
    /// 拘束が 24 時間を超えたため打ち切ったか。**改善基準告示に照らして違反**であり、
    /// 拘束が 24 時間を超えた勤務。**値は頭打ちにしない** — 遵守チェックで拾う目印
    /// であって、上限ではない (Refs #152)。
    pub over_24h: bool,

    /// 拘束 = 終業 − 始業 − 中の休息 ([`rest_minus_minutes`](Self::rest_minus_minutes))。
    pub restraint_minutes: i64,
    /// 閾値以上の休憩の合計。
    pub break_minutes: i64,
    /// 実働 = 拘束 − 休憩。
    pub working_minutes: i64,
    /// **勤務の中にあった休息の合計** (ユーザー決定 2026-07-28、Refs
    /// ohishi-exp/nuxt-dtako-admin#501)。拘束からも実働からも**外してある**。
    ///
    /// 休息は勤務の境界に使う ([`shifts_from_rest`]) が、打刻優先の勤務では中に
    /// 残ることがある。実測 (1194 陣野 / 2026-03) は夜勤 11 日すべてで 6〜7 時間の
    /// 休息が勤務の中にあり、休憩として数えないまま実働に入っていた
    /// (03-11: 拘束 18h09m / 休憩 1h39m / 実働 16h30m のうち 402 分が休息)。
    pub rest_minus_minutes: i64,

    /// 所定内 (0〜所定)。法定休日は 0。
    pub statutory_minutes: i64,
    /// 法定内残業 (所定〜法定、割増 1.0)。法定休日は 0。
    pub within_statutory_overtime_minutes: i64,
    /// 法定時間外 (法定超、割増 1.25)。法定休日は 0。
    pub overtime_minutes: i64,
    /// 法定休日労働 (割増 1.35)。平日は 0。
    pub legal_holiday_minutes: i64,

    /// 平日の所定内・法定内残業に重なる深夜。
    pub night_minutes: i64,
    /// 平日の法定時間外に重なる深夜。
    pub overtime_night_minutes: i64,
    /// 法定休日に重なる深夜。
    pub legal_holiday_night_minutes: i64,
    /// **紙のタイムカード表がこの勤務の暦日から引いている同日フェリー控除の合計**
    /// (Refs #146)。日跨ぎ勤務は `parts` の合計と一致する。
    ///
    /// こちらの拘束には入っていない。詳細は [`ferry_minus_by_date`]。
    pub ferry_minus_minutes: i64,
}

/// 生行 (`serde_json::Value`) をイベントへ。**壊れた行は黙って捨てる** —
/// 上流が列を足しても落ちないようにするため、必要な列が揃わない行だけ落とす。
pub fn parse_events(rows: &[serde_json::Value]) -> Vec<Event> {
    let mut out = Vec::new();
    for row in rows {
        let (Some(dt), Some(source), Some(state)) = (
            row.get("datetime").and_then(|v| v.as_str()),
            row.get("source").and_then(|v| v.as_str()),
            row.get("state").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let Ok(start) = NaiveDateTime::parse_from_str(dt, FMT) else {
            continue;
        };
        let end = row
            .get("end_datetime")
            .and_then(|v| v.as_str())
            .and_then(|s| NaiveDateTime::parse_from_str(s, FMT).ok());
        out.push(Event {
            start,
            end,
            source: source.to_string(),
            state: state.to_string(),
        });
    }
    out.sort_by_key(|e| e.start);
    out
}

/// 秒を切り捨てて分に丸める。拘束・実働は分単位で出すので、境界も分に揃える。
fn floor_min(dt: NaiveDateTime) -> NaiveDateTime {
    // 秒とナノ秒を 0 にするだけなので、範囲外にはならない
    dt - Duration::seconds(dt.second() as i64) - Duration::nanoseconds(dt.nanosecond() as i64)
}

/// 勤務の両端を**分に揃える**。中の区間 (休憩・運行・深夜) はすべて分単位で扱うので、
/// ここで境界を確定させてから畳む。
///
/// 始業は常に切り捨てる。終業の置き方だけが [`RestraintRounding`] で変わる:
///
/// - [`RestraintRounding::TruncateElapsed`] — 始業から**経過の切り捨てぶんだけ**進めた
///   時刻に置く。拘束が `floor(end - start)` になり紙と一致する
/// - [`RestraintRounding::FloorEndpoints`] — 終業も単に切り捨てる (従来)
///
/// どちらでも終業は分の境界に乗るので、下流の区間計算は変わらない。**拘束・実働が
/// 最大 1 分小さくなるだけ**で、`拘束 = 実働 + 休憩` の関係も崩れない。
fn align_shift(s: &Shift, rounding: RestraintRounding) -> Shift {
    let start = floor_min(s.start);
    let end = match rounding {
        RestraintRounding::TruncateElapsed => {
            start + Duration::minutes((s.end - s.start).num_minutes())
        }
        RestraintRounding::FloorEndpoints => floor_min(s.end),
    };
    Shift {
        start,
        end,
        source: s.source,
    }
}

/// 深夜帯 (22:00〜05:00) か。
fn is_night(t: NaiveDateTime) -> bool {
    let h = t.hour();
    !(NIGHT_TO_HOUR..NIGHT_FROM_HOUR).contains(&h)
}

/// 打刻から勤務を組む。`始業` の後に来る最初の `終業` と対にする。
///
/// **終業が見つからない始業は、次の休息の開始で終わらせる** (Refs #137)。以前は捨てて
/// いたが、それだとその日の勤務が 1 本も組まれず、**実働も残業も出ないまま表から消える**
/// (実測: 乗務員 1021 の 2026-04-17 06:46 の始業は対になる終業が翌月まで無く、17 日の
/// 行が丸ごと空になっていた)。休息由来の勤務は「休息の終了」からしか始まらないので、
/// 運行に出た直後の区間はこの手当てが無いと拾えない。
///
/// 次の休息も無ければ捨てる — 終わりを決める手がかりが何も無いため。
fn shifts_from_timecard(events: &[Event]) -> Vec<Shift> {
    let mut out = Vec::new();
    let mut pending: Option<NaiveDateTime> = None;
    for e in events.iter().filter(|e| e.source == "timecard") {
        match e.state.as_str() {
            "始業" => {
                // 前の始業が終業を持たないまま次の始業が来たら、前の分を休息で閉じる
                if let Some(start) = pending.take() {
                    if let Some(end) = next_rest_start(events, start) {
                        if end > start {
                            out.push(Shift {
                                start,
                                end,
                                source: ShiftSource::Timecard,
                            });
                        }
                    }
                }
                pending = Some(e.start);
            }
            "終業" => {
                if let Some(start) = pending.take() {
                    if e.start > start {
                        out.push(Shift {
                            start,
                            end: e.start,
                            source: ShiftSource::Timecard,
                        });
                    }
                }
            }
            _ => {}
        }
    }
    if let Some(start) = pending {
        if let Some(end) = next_rest_start(events, start) {
            if end > start {
                out.push(Shift {
                    start,
                    end,
                    source: ShiftSource::Timecard,
                });
            }
        }
    }
    out.sort_by_key(|s| s.start);
    out
}

/// `after` より後で最初に始まる休息の開始時刻 (Refs #137)。
fn next_rest_start(events: &[Event], after: NaiveDateTime) -> Option<NaiveDateTime> {
    events
        .iter()
        .filter(|e| e.state == "休息" && e.source == "dtako_events")
        .map(|e| e.start)
        .filter(|t| *t > after)
        .min()
}

/// `after` より後で最初の**終業打刻** (Refs ohishi-exp/nuxt-dtako-admin#501)。
fn next_punch_out(events: &[Event], after: NaiveDateTime) -> Option<NaiveDateTime> {
    events
        .iter()
        .filter(|e| e.source == "timecard" && e.state == "終業")
        .map(|e| e.start)
        .filter(|t| *t > after)
        .min()
}

/// `after` より後で最初の**始業打刻** (Refs ohishi-exp/nuxt-dtako-admin#501)。
fn next_punch_in(events: &[Event], after: NaiveDateTime) -> Option<NaiveDateTime> {
    events
        .iter()
        .filter(|e| e.source == "timecard" && e.state == "始業")
        .map(|e| e.start)
        .filter(|t| *t > after)
        .min()
}

/// 区間を持つ休息 (`dtako_events`) を集める。**同じ暦日の終業打刻が休息の中に
/// あれば、休息はその打刻から**始まったことにする (nginx に合わせる、
/// Refs ohishi-exp/nuxt-dtako-admin#501)。
///
/// デジタコは車を停めた時刻から休息を記録するが、乗務員はそのあと構内作業や
/// 事務を済ませてから終業を打刻する。紙のタイムカード表はこの間
/// (休息開始 → 終業打刻) を TC_DC の拘束として数えている。実測 (2026-03):
///
/// - 1072 立山 03-21: 休息 11:05→17:10 の中に終業 16:51。紙は 11:05→16:51 の
///   345 分を拘束に足す (紙 888 = デジタコ 543 + TC_DC 345)
/// - 1072 立山 03-13: 休息 17:27→翌08:37 の中に終業 18:47 (+79)
/// - 1684 倉掛 03-29: 休息 07:43→翌06:30 の中に終業 12:16 (+272)
///
/// **暦日を跨ぐ打刻では動かさない** — 翌朝の打刻で夜通しの休息が丸ごと拘束に
/// なるのを防ぐ (紙もその打刻は翌日の行で数える)。
fn rest_spans(events: &[Event]) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let mut rests: Vec<(NaiveDateTime, NaiveDateTime)> = events
        .iter()
        .filter(|e| e.state == "休息" && e.source == "dtako_events")
        .filter_map(|e| e.end.map(|end| (e.start, end)))
        .filter(|(s, e)| e > s)
        .collect();
    for r in rests.iter_mut() {
        let clip = events
            .iter()
            .filter(|e| e.source == "timecard" && e.state == "終業")
            .map(|e| e.start)
            .filter(|t| *t > r.0 && *t < r.1 && t.date() == r.0.date())
            .max();
        if let Some(p) = clip {
            r.0 = p;
        }
    }
    rests.sort();
    rests
}

/// 休息イベントから勤務を組む。**休息の終了 = 始業、次の休息の開始 = 終業。**
///
/// 区間を持つ `dtako_events` 由来の `休息` だけを使う (`dtako` 側は開始・終了が
/// 別行の点イベントで、対にする根拠が無い)。
///
/// **最後の休息は終業打刻で閉じる** (Refs ohishi-exp/nuxt-dtako-admin#501)。
/// 次の休息が無いと対が作れず、その勤務が**丸ごと落ちて**いた。終業打刻の相手の
/// 始業が無ければ [`shifts_from_timecard`] も捨てるので、両方から漏れる。
/// #137 で入れた「終業の無い始業は次の休息で終わらせる」の裏返し。
///
/// 実測 (乗務員 1526 陣内 / 2026-03): 758 分の休息が明けた 03-02 17:32 から
/// 03-03 05:57 の終業打刻までの **745 分が欠けていた** (紙との差 03-02 +388 /
/// 03-03 は行ごと無し 356 = 暦日按分そのもの)。
///
/// 運行終了では閉じない — 打刻の無い日を運行で代替するかは別の判断 (型D)。
fn shifts_from_rest(events: &[Event]) -> Vec<Shift> {
    let rests = rest_spans(events);
    let mut out = Vec::new();
    for (i, r) in rests.iter().enumerate() {
        let next_rest = rests.get(i + 1).map(|n| n.0);
        let punch = next_punch_out(events, r.1);
        // 次の休息と終業打刻の**早い方**で閉じる
        let end = match (next_rest, punch) {
            (Some(rest), Some(punch)) => rest.min(punch),
            (Some(rest), None) => rest,
            (None, Some(punch)) => punch,
            (None, None) => continue,
        };
        if end > r.1 {
            out.push(Shift {
                start: r.1,
                end,
                source: ShiftSource::Rest,
            });
        }
        // 終業打刻で早めに閉じたとき、次の休息までに**打刻の無い運行**が残っていれば
        // それも勤務にする (nginx に合わせる、Refs nuxt-dtako-admin#501)
        if let (Some(rest), Some(punch)) = (next_rest, punch) {
            if punch < rest {
                out.extend(unpunched_ops_shift(events, punch, rest));
            }
        }
    }
    out
}

/// 終業打刻と次の休息の間に残った**打刻の無い運行**を勤務として拾う
/// (ユーザー決定 2026-07-28「nginx に合わせる」、Refs nuxt-dtako-admin#501)。
///
/// 終業を打刻したあと、始業を打刻せずに再度運行へ出る乗務員が居る。打刻優先の
/// 勤務は終業で閉じ、休息由来の勤務も終業打刻で閉じる (#162) ので、その後の運行は
/// **どの勤務にも入らず丸ごと欠けて**いた。紙のタイムカード表は暦日ごとの
/// 「最初のイベント → 最後のイベント − 休息」なので、この区間を拘束に数えている。
///
/// 実測 (2026-03):
/// - 1021 鈴木 03-19: 終業 12:47 のあと 13:38 に運行開始、18:54 に休息入り。
///   紙 797 に対しこちら 431 (差 366 = 12:47→18:54 そのもの)
/// - 1072 立山 03-25: 03-23 11:33 終業のあと打刻なしで 03-25 06:41→17:42 に運行。
///   紙 661 に対しこちらは行ごと無し
///
/// 規則:
/// - 見る範囲は終業打刻から**次の始業打刻まで** — そこから先は打刻由来の勤務の
///   領分で、拾うと二重になる (1526 の次の運行は始業 06:30 の 1 分後に始まる)
/// - **その暦日に始業打刻が無い終業**と同じ暦日に運行が始まるなら、勤務は
///   **終業打刻から**始める — 紙は日ごとに打刻を見ており、日の中で孤立した終業と
///   隣の運行の隙間を TC_DC で埋める (1021 の 50 分 / 1130 の 12 分。どちらも
///   始業は前日で、その日の紙の出勤欄は空)
/// - **同じ暦日に始業がある終業からは伸ばさない** — 紙は対の打刻をその組だけで
///   数え、打刻後の空白を拘束にしない (1541 吉田 03-14: 対 06:06〜07:52 のあと
///   打刻なしで 12:33〜17:14 に運行。紙 382 = 対 101 + 運行 281 で、間の
///   4.7 時間は入らない)
/// - 別の日に始まるなら**最初のイベントから** — 終業打刻から始めると間の空日
///   (1072 の 03-24) が丸ごと拘束になってしまう
/// - 終わりは次の休息の開始。ただし運行がそれより早く途切れていればそこまで
/// - 拾った勤務が打刻由来の勤務と重なる場合は従来どおり打刻が勝つ
///   ([`merge_shifts`]) — 打刻のある通常の運行を二重に数えない
fn unpunched_ops_shift(
    events: &[Event],
    punch_out: NaiveDateTime,
    next_rest: NaiveDateTime,
) -> Option<Shift> {
    let bound = next_punch_in(events, punch_out).map_or(next_rest, |p| p.min(next_rest));
    let ops: Vec<&Event> = events
        .iter()
        .filter(|e| e.source != "timecard" && e.state != "休息")
        .filter(|e| e.start > punch_out && e.start < bound)
        .collect();
    let first = ops.iter().map(|e| e.start).min()?;
    let latest = ops.iter().map(|e| e.end.unwrap_or(e.start)).max()?;
    let day_lone = !events.iter().any(|e| {
        e.source == "timecard"
            && e.state == "始業"
            && e.start < punch_out
            && e.start.date() == punch_out.date()
    });
    let start = if day_lone && first.date() == punch_out.date() {
        punch_out
    } else {
        first
    };
    // 終わりは**運行が窓の中で終わったかどうか**で決める。「最後のイベントの終わり」
    // (latest) は全乗務員経路では使えない — `ALL_EVENTS_SQL` は応答を抑えるため
    // イベント名を 休息/休憩/運行開始/運行終了 に絞っており、休息まで続く運転・実車
    // イベントが見えず、途中の休憩の終わりで切れてしまう (実測 1072 立山 03-25:
    // 1 名指定は 660、全乗務員は 553 と**同じ日の値が経路で割れた**)。
    // 運行開始・運行終了の点イベントはどちらの SQL にもあるので、これで判定する。
    let last_run_start = ops
        .iter()
        .filter(|e| e.state == "運行開始")
        .map(|e| e.start)
        .max();
    let last_run_end = ops
        .iter()
        .filter(|e| e.state == "運行終了")
        .map(|e| e.start)
        .max();
    let end = match (last_run_start, last_run_end) {
        // 最後の運行が窓の中で終わっている → そこまで (1541 の 17:14)
        (Some(s), Some(e)) if e >= s => e,
        (None, Some(e)) => e,
        // 運行が休息まで続いている → 次の休息 (=bound) まで (1072 03-25 / 1130)
        (Some(_), _) => bound,
        // 運行の点イベントが無い (休憩だけ等) → 従来どおり最後のイベントまで
        (None, None) => bound.min(latest),
    };
    (end > start).then_some(Shift {
        start,
        end,
        source: ShiftSource::Rest,
    })
}

/// **全列が同じ行を落とす** (Refs ohishi-exp/nuxt-dtako-admin#501)。
///
/// デジタコの取り込みが 2 回走ると、`dtako_events` に**同じ運行NO・同じ車輌・
/// 同じ読取日で全列同一の行**が入る (実測 1732 / 2026-07-16: id 32553689〜691 と
/// 32553773〜775 の 2 ブロック)。こちらの計算は元から重複に強い
/// ([`merge_intervals`] / [`shifts_from_rest`] の `windows(2)`) が、**紙の
/// タイムカード表は二重計上して拘束が 2 倍になる**ので、落とした件数を暦日ごとに
/// 返して突合で気付けるようにする。
///
/// 読み出し SQL は `id` を選んでいないので、**残った列が全部同じ = 区別する
/// 情報が無い** = 重複と断じてよい。
pub fn drop_duplicate_rows(
    rows: Vec<serde_json::Value>,
) -> (Vec<serde_json::Value>, BTreeMap<String, i64>) {
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut dropped: BTreeMap<String, i64> = BTreeMap::new();
    let kept = rows
        .into_iter()
        .filter(|r| {
            if seen.insert(r.to_string()) {
                return true;
            }
            let date = r
                .get("datetime")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .chars()
                .take(10)
                .collect::<String>();
            *dropped.entry(date).or_insert(0) += 1;
            false
        })
        .collect();
    (kept, dropped)
}

/// 区間 `[from, to)` を暦日で切り、日ごとの分数を返す (Refs #130)。
///
/// 日跨ぎ勤務の拘束を**暦日で按分**するため。月合計は始業日へ丸ごと寄せた場合と
/// 一致するが、日別と月境界が変わる。
fn split_by_date(start: NaiveDateTime, end: NaiveDateTime) -> Vec<(chrono::NaiveDate, i64)> {
    let mut out = Vec::new();
    let mut cur = start;
    while cur < end {
        // 翌日 0:00 (最終日は勤務の終わり) までがその日の分
        let next_midnight = (cur.date() + Duration::days(1))
            .and_hms_opt(0, 0, 0)
            .expect("0:00 は常に有効");
        let bound = next_midnight.min(end);
        out.push((cur.date(), (bound - cur).num_minutes()));
        cur = bound;
    }
    out
}

/// 区間 `[from, to]` に入る打刻 (`始業` / `終業`) を時刻順に拾う (Refs #128)。
///
/// **両端を含む** — 勤務の始業・終業そのものを落とさないため。比較は**分に丸めてから**
/// 行う: 勤務の端は秒を切り捨ててあるので、`16:47:04` の終業打刻を `16:47:00` の
/// 終業と比べると範囲外になって落ちる。
///
/// 呼び出し側は [`RestraintRounding::TruncateElapsed`] で**削った 1 分を足し戻して**
/// 渡すこと ([`punch_window_end`])。削るのは拘束の数え方の話で、その 1 分に乗っている
/// 終業打刻は依然この勤務のものだから。
/// 打刻を拾うときの終端。
///
/// [`RestraintRounding::TruncateElapsed`] は経過の端数を落とすので、勤務の `end` が
/// 終業打刻より最大 1 分手前に来る。そのまま [`punches_in`] に渡すと**終業打刻が
/// 落ちる** (実測: 始業 09:25:37 / 終業 19:39:04 の勤務で終業が消えた)。削った 1 分を
/// 足し戻して拾う。
///
/// 足すのは 1 分だけなので、無関係な打刻を巻き込むことはない — その 1 分は
/// 元々この勤務の中にあった時間そのもの。
fn punch_window_end(shift: &Shift, rounding: RestraintRounding) -> NaiveDateTime {
    match rounding {
        RestraintRounding::TruncateElapsed => shift.end + Duration::minutes(1),
        RestraintRounding::FloorEndpoints => shift.end,
    }
}

fn punches_in(events: &[Event], from: NaiveDateTime, to: NaiveDateTime) -> Vec<Punch> {
    events
        .iter()
        .filter(|e| e.source == "timecard" && (e.state == "始業" || e.state == "終業"))
        .filter(|e| floor_min(e.start) >= from && floor_min(e.start) <= to)
        .map(|e| Punch {
            at: e.start.format(FMT).to_string(),
            state: e.state.clone(),
        })
        .collect()
}

/// **24 時間を超える打刻の勤務を、中の休息イベントで切り直す** (Refs #133)。
///
/// 長距離は打刻が運行 1 本 (数日) をまるごと挟む。実測: 乗務員 1021 は
/// `2026-04-03 06:33 始業 → 2026-04-09 15:16 終業` の**1 勤務 6 日間**で、24 時間で
/// 打ち切ると 4/5 以降に配る値が何も無くなる (画面が空欄になる)。
///
/// 打刻優先 (#118) は維持したまま、**明らかに包みすぎている勤務だけ**休息で割る。
/// 社内 CakePHP (`CalDtakoKosoku`) も運行単位に区切ってから暦日へ配っており、
/// 打刻と運行イベントを同列に扱っていない (2026-07-27 ユーザー指摘)。
///
/// - 切るのは **24 時間を超えた勤務だけ**。通常の勤務の中の休息では切らない
///   (#118 の「運行では切らない」を崩さない)
/// - 休息は勤務の中に切り詰めてから使う。重なった休息はまとめる
/// - 割った後も 24 時間を超える区間が残ったら、それは呼び出し側で打ち切られる
///   (休息を挟まず 24 時間以上走り続けた = 本当に違反の可能性が高い区間)
fn split_long_shift(shift: &Shift, events: &[Event]) -> Vec<Shift> {
    if (shift.end - shift.start).num_minutes() <= MAX_RESTRAINT_MINUTES {
        return vec![shift.clone()];
    }
    // 休息は打刻で切り詰めた後の区間で切る ([`rest_spans`]) — 切り詰め前で切ると、
    // 休息の中に終業打刻がある日 (1684 倉掛 03-29: 休息 07:43→翌06:30 の中に終業
    // 12:16) で、休息開始→打刻の 272 分が欠片の外に落ちる。紙は TC_DC で数えている
    let mut rests: Vec<(NaiveDateTime, NaiveDateTime)> = rest_spans(events)
        .into_iter()
        .map(|(s, e)| (floor_min(s), floor_min(e)))
        .map(|(s, e)| (s.max(shift.start), e.min(shift.end)))
        .filter(|(s, e)| e > s)
        .collect();
    rests.sort();
    let mut merged: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    for (s, e) in rests {
        match merged.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    let mut out = Vec::new();
    let mut cur = shift.start;
    for (s, e) in merged {
        if s > cur {
            out.push(Shift {
                start: cur,
                end: s,
                source: shift.source,
            });
        }
        cur = cur.max(e);
    }
    if shift.end > cur {
        out.push(Shift {
            start: cur,
            end: shift.end,
            source: shift.source,
        });
    }
    // 休息が 1 つも無ければ元のまま (打ち切りに任せる)
    if out.is_empty() {
        out.push(shift.clone());
    }
    out.into_iter()
        // 休息が記録されていない帰宅は「運行終了 → 次の運行開始」の空きで割る (Refs #135)
        .flat_map(|s| split_by_run_gaps(&s, events))
        .collect()
}

/// 休息と見なす「運行終了 → 次の運行開始」の空き (分)。
///
/// 改善基準告示の休息期間 (継続 8 時間以上) に合わせる。実測 (96 名 / 2026-06) では
/// **同日の運行の継ぎ目は 4〜112 分 (中央 8 分)** しかないので、この閾値なら
/// 「荷を降ろして次の伝票を積んで出るだけ」の継ぎ目 (#123) を割ることはない。
const RUN_GAP_REST_MINUTES: i64 = 8 * 60;

/// 24 時間を超えたままの勤務を、**運行終了 → 次の運行開始 の長い空きで割る** (Refs #135)。
///
/// [`end_at_last_run_end`] は**最後の**運行終了しか見ないので、帰宅を挟んで運行が
/// 何本も続く勤務では効かない。実測: 乗務員 1108 / 2026-04 は
///
/// ```text
/// 04-11 03:11  休息終了 (= 始業)
/// 04-11 09:28  運行終了      ← ここで帰宅
/// 04-13 07:31  運行開始      ← 46 時間空く
/// 04-13 12:10  運行終了
/// 04-14 07:13  運行開始      ← 19 時間空く
/// 04-14 10:48  終業打刻      (= 勤務の終わり)
/// ```
///
/// で、最後の運行終了 (04-13 12:10) で切っても 2 日超が残り、24 時間打ち切りに
/// 戻っていた (2026-07-28 ユーザー指摘「11 土 … 退社不明 (拘束 24 時間で打ち切り)」)。
///
/// - **24 時間を超えた勤務だけ**が対象。通常の勤務は運行の継ぎ目で切らない (#123)
/// - 割るのは空きが [`RUN_GAP_REST_MINUTES`] 以上の所だけ
/// - 運行終了の後に運行開始が無ければ、そこで勤務を終える (帰宅したまま = #135 の形)
fn split_by_run_gaps(shift: &Shift, events: &[Event]) -> Vec<Shift> {
    if (shift.end - shift.start).num_minutes() <= MAX_RESTRAINT_MINUTES {
        return vec![shift.clone()];
    }
    let inside = |t: &NaiveDateTime| *t > shift.start && *t < shift.end;
    let mut run_ends: Vec<NaiveDateTime> = events
        .iter()
        .filter(|e| e.state == "運行終了")
        .map(|e| floor_min(e.start))
        .filter(inside)
        .collect();
    run_ends.sort();
    let mut run_starts: Vec<NaiveDateTime> = events
        .iter()
        .filter(|e| e.state == "運行開始")
        .map(|e| floor_min(e.start))
        .filter(inside)
        .collect();
    run_starts.sort();

    let mut out = Vec::new();
    let mut cur = shift.start;
    for end in run_ends {
        if end <= cur {
            continue;
        }
        let next_start = run_starts.iter().copied().find(|s| *s > end);
        let gap = next_start.unwrap_or(shift.end) - end;
        if gap.num_minutes() < RUN_GAP_REST_MINUTES {
            continue;
        }
        out.push(Shift {
            start: cur,
            end,
            source: shift.source,
        });
        match next_start {
            Some(s) => cur = s,
            // 次の運行が無い = 帰宅したまま。残りは勤務ではない
            None => return out,
        }
    }
    if shift.end > cur {
        out.push(Shift {
            start: cur,
            end: shift.end,
            source: shift.source,
        });
    }
    out
}

/// 2 つの区間が重なるか。
fn overlaps(a: (NaiveDateTime, NaiveDateTime), b: (NaiveDateTime, NaiveDateTime)) -> bool {
    a.0 < b.1 && b.0 < a.1
}

/// 打刻由来と休息由来を統合する。**打刻を優先し、打刻の勤務と重なる休息由来は捨てる。**
///
/// 打刻と休息は実測では補完関係にあり重ならないが、両方ある乗務員が現れても
/// 二重計上しないようにしておく。
fn merge_shifts(timecard: Vec<Shift>, rest: Vec<Shift>) -> Vec<Shift> {
    let mut out = timecard;
    for r in rest {
        if !out
            .iter()
            .any(|t| overlaps((t.start, t.end), (r.start, r.end)))
        {
            out.push(r);
        }
    }
    out.sort_by_key(|e| e.start);
    out
}

/// 勤務に重なる休憩区間を、閾値でふるって勤務内へ切り詰めて返す。
///
/// 閾値の判定は**切り詰める前の長さ**で行う — 勤務の端に半分だけ掛かった 30 分の
/// 休憩を「10 分未満だから無視」にしないため。
fn breaks_in(
    events: &[Event],
    shift: &Shift,
    threshold: i64,
) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let out: Vec<(NaiveDateTime, NaiveDateTime)> = events
        .iter()
        .filter(|e| e.state == "休憩")
        .filter_map(|e| e.end.map(|end| (floor_min(e.start), floor_min(end))))
        .filter(|(s, e)| (*e - *s).num_minutes() >= threshold)
        .map(|(s, e)| (s.max(shift.start), e.min(shift.end)))
        .filter(|(s, e)| e > s)
        .collect();
    merge_intervals(out)
}

/// 時刻順に並べて重なりをまとめる (重複して引かないため)。
fn merge_intervals(
    mut v: Vec<(NaiveDateTime, NaiveDateTime)>,
) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    v.sort();
    let mut merged: Vec<(NaiveDateTime, NaiveDateTime)> = Vec::new();
    for (s, e) in v {
        match merged.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    merged
}

/// この勤務が**運行**か (デジタコに運行のイベントが残っているか、ユーザー決定 2026-07-28)。
///
/// 運行中なら休憩はイベントから出せる (運転を止めた時間がそのまま記録されている)。
/// 運行に出ていない勤務 — 事務・作業・整備、および乗務員が構内作業だけした日 — は
/// デジタコに何も残らないので、イベントからは休憩が 1 分も出ず**実働 = 拘束**に
/// なっていた。そういう日は昼休憩を引く ([`lunch_windows`])。
///
/// - **`休息` は運行の証拠にしない** — 休息は勤務の**外側**の境界であって
///   ([`shifts_from_rest`])、その勤務中に運行したことを意味しない
/// - 打刻 (`timecard`) も証拠にしない — 打刻は事務員にもある
/// - 点イベント (`dtako` の 運行開始 / 運行終了) は区間を持たないので、勤務に
///   含まれるかどうかで見る
fn has_operation(events: &[Event], shift: &Shift) -> bool {
    events
        .iter()
        .filter(|e| e.source != "timecard" && e.state != "休息")
        .any(|e| match e.end {
            Some(end) => overlaps((e.start, end), (shift.start, shift.end)),
            None => e.start >= shift.start && e.start <= shift.end,
        })
}

/// 昼の窓に 1 分も掛からない勤務へ入れる休憩 (ユーザー指示 2026-07-28)。
///
/// 昼休憩は時計の窓なので夜勤には当たらない ([`lunch_windows`])。拘束が
/// [`OFF_HOURS_BREAK_MIN_RESTRAINT_MINUTES`] を超える勤務にだけ
/// [`OFF_HOURS_BREAK_MINUTES`] を入れる。
///
/// **置く場所は勤務のまん中。** 夜勤は時計の窓で決められず、端に寄せると深夜
/// (22:00-05:00) の内訳が寄せ方しだいで動いてしまう。まん中なら実際に休憩を取る
/// 時間帯に最も近く、恣意的な偏りも入らない。
fn off_hours_break(shift: &Shift) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let restraint = (shift.end - shift.start).num_minutes();
    if restraint <= OFF_HOURS_BREAK_MIN_RESTRAINT_MINUTES {
        return Vec::new();
    }
    let start = shift.start + Duration::minutes((restraint - OFF_HOURS_BREAK_MINUTES) / 2);
    vec![(start, start + Duration::minutes(OFF_HOURS_BREAK_MINUTES))]
}

/// 勤務が跨ぐ暦日ごとの昼休憩の窓 (12:00-13:00) を、勤務内へ切り詰めて返す。
///
/// 窓は消費側 (`nuxt-dtako-admin` の `timecard-summary.ts`) が打刻から実働を出す
/// ときに使っているものと同じ — 同じ人の同じ日が、経路によって違う実働になるのを
/// 防ぐ。勤務は 24 時間を超えることがあるので、跨ぐ暦日は 3 日以上になりうる。
fn lunch_windows(shift: &Shift) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let mut out = Vec::new();
    let mut day = shift.start.date();
    let last = shift.end.date();
    while day <= last {
        let from = day
            .and_hms_opt(LUNCH_FROM_HOUR, 0, 0)
            .expect("12:00 は常に有効");
        let to = day
            .and_hms_opt(LUNCH_TO_HOUR, 0, 0)
            .expect("13:00 は常に有効");
        // 勤務の外へはみ出す分は落とす (昼をまたがない勤務は 1 件も出ない)
        let s = from.max(shift.start);
        let e = to.min(shift.end);
        if e > s {
            out.push((s, e));
        }
        day += Duration::days(1);
    }
    out
}

/// `base` の各区間から `cut` を差し引く。`cut` は時刻順・重なり無しであること
/// ([`merge_intervals`] を通したもの)。
fn subtract_intervals(
    base: &[(NaiveDateTime, NaiveDateTime)],
    cut: &[(NaiveDateTime, NaiveDateTime)],
) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    let mut out = Vec::new();
    for (bs, be) in base {
        let mut cur = *bs;
        for (s, e) in cut {
            if *e <= cur || *s >= *be {
                continue;
            }
            if *s > cur {
                out.push((cur, *s));
            }
            cur = cur.max(*e);
        }
        if *be > cur {
            out.push((cur, *be));
        }
    }
    out
}

/// 勤務に重なる**休息**区間を、勤務内へ切り詰めて返す (ユーザー決定 2026-07-28)。
///
/// 区間を持つ `dtako_events` 由来の `休息` だけを使う — `dtako` 側は点イベントで
/// 長さを持たない ([`shifts_from_rest`] と同じ理由)。
///
/// 休息は本来なら勤務の境界になる ([`shifts_from_rest`]) が、**打刻優先** (#118) の
/// 勤務では中に残る。休憩には数えられないので、外さないと**実働として払う**ことに
/// なる (実測 1194 陣野 / 2026-03-11: 402 分の休息が実働 16h30m の中にあった)。
/// 紙のタイムカード表 (社内 CakePHP) も同じだけ引いている。
///
/// **閾値は設けない** — 休憩イベント ([`breaks_in`]) と違い、休息はデジタコが
/// 「運行を終えて休んだ」と判定した区間そのものなので、短くても休憩ではない。
fn rests_in(events: &[Event], shift: &Shift) -> Vec<(NaiveDateTime, NaiveDateTime)> {
    // 休息は打刻で切り詰めた後の区間を使う ([`rest_spans`]) — 切り詰め前で引くと、
    // 勤務の終わりを終業打刻まで伸ばした分 (休息開始→打刻) をここで引き戻してしまう
    let out: Vec<(NaiveDateTime, NaiveDateTime)> = rest_spans(events)
        .into_iter()
        .map(|(s, e)| (floor_min(s), floor_min(e)))
        .map(|(s, e)| (s.max(shift.start), e.min(shift.end)))
        .filter(|(s, e)| e > s)
        .collect();
    merge_intervals(out)
}

/// 勤務 1 回を日別サマリへ畳む。
///
/// 実働区間を**時刻順に 1 分ずつ**歩いて、経過実働で所定内 / 法定内残業 / 法定時間外に
/// 振り分けながら、その分が深夜帯かを見る。1 勤務は最長でも数千分なので素直に回す。
fn summarize(shift: &Shift, events: &[Event], p: &KosokuParams) -> DaySummary {
    // **打刻は打ち切り前の区間から拾う** — 24 時間で切ると、切った先にある終業打刻
    // (実測: 乗務員 1194 の 2026-04-03 16:47) が落ちる。拘束の値は打ち切ったままでよいが、
    // 打刻カードとして出す側にはその時刻が要る (Refs #128)
    let punches = punches_in(
        events,
        shift.start,
        punch_window_end(shift, p.restraint_rounding),
    );
    // 24 時間を超える拘束はここで打ち切る (法令違反なので積み上げる意味がない)
    // **打ち切らない** (Refs #152)。24 時間を超えた勤務は、帰宅日の混入ではなく
    // 実在する長時間拘束だと実測で確かめた (乗務員 1674 / 2026-04-07 01:08 →
    // 04-08 16:22 = 39.2 時間。中の休憩は最長 152 分、運行の空きは 36 分、打刻なし)。
    // 1440 で頭打ちにすると**改善基準違反をその分だけ小さく見せる**ので、実測のまま
    // 出して `over_24h` で目立たせる。
    // 勤務の中に残った休息は拘束からも実働からも外す (ユーザー決定 2026-07-28)。
    // 拘束の実体はここから下の `restraint` — 以降は「終業 − 始業」を直接使わない
    let rests = rests_in(events, shift);
    let restraint = subtract_intervals(&[(shift.start, shift.end)], &rests);
    // **外して何も残らないなら外さない** ([`split_long_shift`] の打ち切りと同じ扱い)。
    // 勤務を丸ごと覆う休息はデータの壊れ方であって「拘束 0 の出勤日」ではない
    let (rests, restraint) = if restraint.is_empty() {
        (Vec::new(), vec![(shift.start, shift.end)])
    } else {
        (rests, restraint)
    };
    let restraint_minutes: i64 = restraint.iter().map(|(s, e)| (*e - *s).num_minutes()).sum();
    let rest_minus_minutes: i64 = rests.iter().map(|(s, e)| (*e - *s).num_minutes()).sum();
    let over_24h = restraint_minutes > MAX_RESTRAINT_MINUTES;
    // 運行に出ていない勤務は昼休憩を引く (ユーザー決定 2026-07-28)。運行中なら
    // 休憩はデジタコのイベントから出る — 両方を足すと昼を二重に引くので排他にする
    let breaks = if has_operation(events, shift) {
        breaks_in(events, shift, p.break_threshold_minutes)
    } else {
        // **運行に出ていない勤務にはデジタコの休憩イベントも無い** — 休憩イベントは
        // 運行に紐づいて記録されるので、勤務に重なる休憩が 1 件でもあれば
        // `has_operation` が true になりこちらへは来ない。足し合わせは起きえない
        let lunch = lunch_windows(shift);
        // 夜勤のように昼の窓へ 1 分も掛からない勤務は、時計の窓では休憩を置けない
        if lunch.is_empty() {
            off_hours_break(shift)
        } else {
            lunch
        }
    };
    // 休息の中に入った休憩は二重に引かない (昼休憩の窓が休息に丸ごと重なる場合など)
    let breaks = subtract_intervals(&breaks, &rests);
    let break_minutes: i64 = breaks.iter().map(|(s, e)| (*e - *s).num_minutes()).sum();
    let intervals = subtract_intervals(&restraint, &breaks);
    let is_legal_holiday = shift.start.weekday() == Weekday::Sun;

    let mut elapsed = 0i64;
    let (mut statutory, mut within, mut overtime, mut holiday) = (0i64, 0i64, 0i64, 0i64);
    let (mut night, mut ot_night, mut hol_night) = (0i64, 0i64, 0i64);
    // 暦日按分の内訳 (Refs #130)。1 分ずつ歩くついでにその分が乗る暦日へ振る
    let mut parts: BTreeMap<chrono::NaiveDate, DayPart> = BTreeMap::new();

    for (s, e) in &intervals {
        let mut t = *s;
        while t < *e {
            let n = is_night(t);
            let part = parts
                .entry(t.date())
                .or_insert_with(|| DayPart::new(t.date()));
            if is_legal_holiday {
                holiday += 1;
                part.legal_holiday_minutes += 1;
                if n {
                    hol_night += 1;
                    part.legal_holiday_night_minutes += 1;
                }
            } else if elapsed < p.prescribed_minutes {
                statutory += 1;
                if n {
                    night += 1;
                    part.night_minutes += 1;
                }
            } else if elapsed < p.legal_minutes {
                within += 1;
                // 法定内残業は割増 1.0 なので、深夜は所定内と同じ 0.25 上乗せで足りる
                if n {
                    night += 1;
                    part.night_minutes += 1;
                }
            } else {
                overtime += 1;
                part.overtime_minutes += 1;
                if n {
                    ot_night += 1;
                    part.overtime_night_minutes += 1;
                }
            }
            part.working_minutes += 1;
            elapsed += 1;
            t += Duration::minutes(1);
        }
    }
    // 拘束は休憩も含む区間なので、実働の歩きとは別に暦日へ切り分ける。
    // **休息を抜いた区間ごとに**配る — 勤務の端から端で配ると外したはずの休息が戻る
    for (s, e) in &restraint {
        for (date, minutes) in split_by_date(*s, *e) {
            parts
                .entry(date)
                .or_insert_with(|| DayPart::new(date))
                .restraint_minutes += minutes;
        }
    }

    DaySummary {
        // フェリー控除は勤務の計算に混ぜない。日別サマリを組み終えてから
        // `apply_ferry_minus` で暦日ごとに載せる (Refs #146)
        ferry_minus_minutes: 0,
        date: shift.start.format("%Y-%m-%d").to_string(),
        start: shift.start.format(FMT).to_string(),
        end: shift.end.format(FMT).to_string(),
        source: shift.source,
        punches,
        // 1 日で終わる勤務は内訳がこの行そのものなので出さない (応答を膨らませない)
        parts: if parts.len() > 1 {
            parts.into_values().collect()
        } else {
            Vec::new()
        },
        is_legal_holiday,
        over_24h,
        restraint_minutes,
        break_minutes,
        working_minutes: elapsed,
        rest_minus_minutes,
        statutory_minutes: statutory,
        within_statutory_overtime_minutes: within,
        overtime_minutes: overtime,
        legal_holiday_minutes: holiday,
        night_minutes: night,
        overtime_night_minutes: ot_night,
        legal_holiday_night_minutes: hol_night,
    }
}

/// 全乗務員ぶんの生イベント列を**乗務員ごとに分ける** (Refs #125)。
///
/// [`daily_summary`] は乗務員を知らない — 1 人ぶんのイベント列を前提に休息で勤務を
/// 切るので、混ざったまま渡すと他人の休息で勤務が切れる。**畳む前にここで分ける。**
///
/// - 乗務員CD 昇順で返す (行の並びは入力のまま維持する)
/// - **`driver_id` が無い / 数でない / 負の行は捨てる。** 誰のものか決められない行を
///   どこかの乗務員に混ぜると、その人の拘束が静かに伸びる
pub fn split_by_driver(rows: Vec<serde_json::Value>) -> Vec<(u64, Vec<serde_json::Value>)> {
    let mut by_driver: BTreeMap<u64, Vec<serde_json::Value>> = BTreeMap::new();
    for row in rows {
        let Some(driver) = row.get("driver_id").and_then(|v| v.as_u64()) else {
            continue;
        };
        by_driver.entry(driver).or_default().push(row);
    }
    by_driver.into_iter().collect()
}

/// 対象月に押された打刻 (`始業` / `終業`) を時刻順にそのまま返す (Refs #137)。
///
/// **勤務に紐づけない。** `DaySummary.punches` は勤務を構成した打刻なので、対になる
/// 終業が無い始業は勤務が組めず落ちる (実測: 乗務員 1021 の 2026-04-17 06:46 の始業は
/// 対になる終業が翌月まで無く、表から消えていた)。**タイムカードは押された打刻を
/// そのまま並べるもの**なので、画面にはこちらを渡す。
pub fn month_punches(rows: &[serde_json::Value], month: &str) -> Vec<Punch> {
    parse_events(rows)
        .iter()
        .filter(|e| e.source == "timecard" && (e.state == "始業" || e.state == "終業"))
        .map(|e| Punch {
            at: e.start.format(FMT).to_string(),
            state: e.state.clone(),
        })
        .filter(|p| p.at.starts_with(month))
        .collect()
}

/// フェリー区間 → **暦日ごとの控除額** (分)。上流 `_make_kosoku_time` の再現
/// (Refs #146、yhonda-ohishi/nginx#788 の引き継ぎ)。
///
/// 紙のタイムカード表は同日フェリーぶんを拘束から引いており、それが突合の差の
/// 原因になっている。**こちらの拘束はこの値を使わない** — 差の説明のために出すだけ。
///
/// 上流の条件をそのまま写す:
///
/// - **同日で始まって同日で終わるフェリーだけ**。
///
///   日跨ぎのフェリーは**珍しくない** — 2025-11〜2026-06 の実データで
///   同日 662 件 / **日跨ぎ 526 件 (44%)**、最長 24 時間。上流にも日跨ぎ用の分岐が
///   あり、開始日と終了日へ割る作りになっている (`_make_kosoku_time` の
///   「フェリーの日付が異なる」ブロック)。
///
///   ただしその分岐は `$d_time_2->d = 0` (代入。`==` の書き損じ) で条件が常に false
///   になり、**一度も走っていない**。しかも仮に `==` へ直しても条件は
///   `h < 4 && d == 0` = 「総時間 4 時間未満」なので、**実データの日跨ぎ 526 件は
///   4 時間未満が 0 件**で 1 件も該当しない。よって同日のみで紙と一致する
/// - **4 時間未満**だけ (`$d_time_1->h < 4`)。`h` は `DateInterval` の時コンポーネント
///   なので、同日前提では「総時間 < 4h」と同じ
/// - 控除額は `h * 60 + i` = **秒を切り捨てた総分**
///
/// 月の絞り込みは呼び出し側 (`fetch_ferry` が `[月初, 翌月初)` で引く)。
pub fn ferry_minus_by_date(rows: &[serde_json::Value]) -> BTreeMap<String, i64> {
    let mut out: BTreeMap<String, i64> = BTreeMap::new();
    for row in rows {
        let (Some(s), Some(e)) = (
            row.get("start_datetime").and_then(|v| v.as_str()),
            row.get("end_datetime").and_then(|v| v.as_str()),
        ) else {
            continue;
        };
        let (Ok(start), Ok(end)) = (
            NaiveDateTime::parse_from_str(s, FMT),
            NaiveDateTime::parse_from_str(e, FMT),
        ) else {
            continue;
        };
        if start.date() != end.date() {
            continue;
        }
        let secs = (end - start).num_seconds();
        if !(0..4 * 3600).contains(&secs) {
            continue;
        }
        *out.entry(start.format("%Y-%m-%d").to_string()).or_insert(0) += secs / 60;
    }
    out
}

/// フェリー区間を乗務員CD ごとに分ける (全乗務員ぶんを 1 回で引いたとき用)。
pub fn split_ferry_by_driver(
    rows: Vec<serde_json::Value>,
) -> BTreeMap<u64, Vec<serde_json::Value>> {
    let mut out: BTreeMap<u64, Vec<serde_json::Value>> = BTreeMap::new();
    for row in rows {
        let Some(driver) = row.get("driver_id").and_then(|v| v.as_u64()) else {
            continue;
        };
        out.entry(driver).or_default().push(row);
    }
    out
}

/// 日別サマリへ暦日ごとのフェリー控除を載せる。
///
/// 日跨ぎ勤務は `parts` の暦日へ、1 日で終わる勤務は勤務の日へ。**勤務の無い暦日の
/// 控除は載らない** — フェリーは運行に紐づき、運行があれば勤務も立つので実データでは
/// 起きない想定だが、起きても静かに落ちるだけで拘束の数字は動かない。
///
/// ## 同じ暦日に複数の勤務があっても 1 回だけ載せる
///
/// **控除は暦日に対する値**で、勤務に対する値ではない。ところが**フェリー自体が
/// 休息イベントなので勤務が割れる** — 実データ (1726 / 2026-03-14) は 4 時間近い
/// フェリーが 2 本あるせいで 1 日が 4 勤務 (拘束 1 / 16 / 82 / 222 分) になる。
/// 日付が一致する勤務すべてに載せると、消費側が暦日で合算したときに
/// 433 分が 4 回足されて 1732 分になる。**最初の 1 つにだけ載せる。**
pub fn apply_ferry_minus(days: &mut [DaySummary], ferry: &BTreeMap<String, i64>) {
    if ferry.is_empty() {
        return;
    }
    // 既に載せた暦日。勤務は時刻順なので「最初の勤務が持つ」で決まる
    let mut used: BTreeSet<String> = BTreeSet::new();
    let mut take = |date: &str| -> i64 {
        match ferry.get(date) {
            Some(&m) if used.insert(date.to_string()) => m,
            _ => 0,
        }
    };
    for day in days.iter_mut() {
        if day.parts.is_empty() {
            day.ferry_minus_minutes = take(&day.date);
            continue;
        }
        let mut total = 0;
        for part in day.parts.iter_mut() {
            part.ferry_minus_minutes = take(&part.date);
            total += part.ferry_minus_minutes;
        }
        day.ferry_minus_minutes = total;
    }
}

/// 生イベント列 → 対象月の日別サマリ。
///
/// `month` (`YYYY-MM`) は**始業日**で絞る。前後にはみ出した勤務は落ちる/入る:
/// 前月に始業した勤務は当月に出さず、当月末に始業して翌月に終業する勤務は
/// 拘束を丸ごと当月に載せる。
pub fn daily_summary(rows: &[serde_json::Value], month: &str, p: &KosokuParams) -> Vec<DaySummary> {
    let events = parse_events(rows);
    let shifts = merge_shifts(shifts_from_timecard(&events), shifts_from_rest(&events));
    shifts
        .iter()
        .map(|s| align_shift(s, p.restraint_rounding))
        .filter(|s| s.end > s.start)
        // 打刻が数日をまとめて挟んだ勤務は、中の休息で切り直す (Refs #133)
        .flat_map(|s| split_long_shift(&s, &events))
        .map(|s| summarize(&s, &events, p))
        .filter(|d| d.date.starts_with(month))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ferry(start: &str, end: &str) -> serde_json::Value {
        json!({"start_datetime": start, "end_datetime": end, "driver_id": 1726})
    }

    // ---- フェリー控除 (Refs #146) ----
    //
    // 紙のタイムカード表を再現するだけの値。**拘束の計算には入れない**。

    #[test]
    fn ferry_same_day_under_4h_is_deducted() {
        // 実データ: 1726 / 2026-03-21 は 78 分 (16:18:41 → 17:36:56)
        let m = ferry_minus_by_date(&[ferry("2026-03-21 16:18:41", "2026-03-21 17:36:56")]);
        assert_eq!(m.get("2026-03-21"), Some(&78));
    }

    #[test]
    fn ferry_same_day_sums_and_can_exceed_the_day_total() {
        // 実データ: 1726 / 2026-03-14 は 2 本で 433 分。積算 321 分を食い破って -112 になる
        let m = ferry_minus_by_date(&[
            ferry("2026-03-14 07:38:40", "2026-03-14 11:22:33"),
            ferry("2026-03-14 16:31:06", "2026-03-14 20:01:24"),
        ]);
        assert_eq!(m.get("2026-03-14"), Some(&433));
    }

    #[test]
    fn ferry_4h_or_longer_is_not_deducted() {
        // 上流の `$d_time_1->h < 4`。4 時間ちょうどは対象外
        assert!(
            ferry_minus_by_date(&[ferry("2026-03-14 00:00:00", "2026-03-14 04:00:00")]).is_empty()
        );
        assert_eq!(
            ferry_minus_by_date(&[ferry("2026-03-14 00:00:00", "2026-03-14 03:59:59")])
                .get("2026-03-14"),
            Some(&239),
        );
    }

    #[test]
    fn ferry_crossing_midnight_is_not_deducted() {
        // 日跨ぎは実データで 44% (526/1188、2025-11〜2026-06) と珍しくないが、
        // 上流の日跨ぎ分岐は `$d_time_2->d = 0` (代入) で常に false → 一度も走らない。
        // 直しても条件は総時間 4 時間未満で、日跨ぎのうち 4 時間未満は実データ 0 件
        assert!(
            ferry_minus_by_date(&[ferry("2026-03-14 23:00:00", "2026-03-15 01:00:00")]).is_empty()
        );
    }

    #[test]
    fn ferry_seconds_are_truncated_not_rounded() {
        // `h * 60 + i` = 秒を切り捨てた総分
        assert_eq!(
            ferry_minus_by_date(&[ferry("2026-03-14 10:00:00", "2026-03-14 10:01:59")])
                .get("2026-03-14"),
            Some(&1),
        );
    }

    #[test]
    fn ferry_broken_rows_are_dropped() {
        let rows = vec![
            json!({"end_datetime": "2026-03-14 11:00:00"}),
            json!({"start_datetime": "2026-03-14 10:00:00"}),
            json!({"start_datetime": "x", "end_datetime": "2026-03-14 11:00:00"}),
            json!({"start_datetime": "2026-03-14 10:00:00", "end_datetime": "y"}),
            // 終わりが始まりより前 (負) — 引かない
            ferry("2026-03-14 11:00:00", "2026-03-14 10:00:00"),
        ];
        assert!(ferry_minus_by_date(&rows).is_empty());
    }

    #[test]
    fn ferry_splits_by_driver_and_drops_rows_without_one() {
        let rows = vec![
            json!({"start_datetime": "2026-03-14 10:00:00", "end_datetime": "2026-03-14 11:00:00", "driver_id": 1726}),
            json!({"start_datetime": "2026-03-15 10:00:00", "end_datetime": "2026-03-15 11:00:00", "driver_id": 1726}),
            json!({"start_datetime": "2026-03-14 10:00:00", "end_datetime": "2026-03-14 11:00:00", "driver_id": 1021}),
            json!({"start_datetime": "2026-03-14 10:00:00", "end_datetime": "2026-03-14 11:00:00"}),
        ];
        let by = split_ferry_by_driver(rows);
        assert_eq!(by.len(), 2);
        assert_eq!(by[&1726].len(), 2);
        assert_eq!(by[&1021].len(), 1);
    }

    #[test]
    fn apply_ferry_minus_puts_it_on_the_calendar_day() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let mut days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert!(days[0].parts.is_empty());
        let ferry = ferry_minus_by_date(&[ferry("2026-06-02 10:00:00", "2026-06-02 11:00:00")]);
        apply_ferry_minus(&mut days, &ferry);
        assert_eq!(days[0].ferry_minus_minutes, 60);
        // **拘束は動かない** — 控除は紙の側の話
        assert_eq!(days[0].restraint_minutes, 540);
    }

    #[test]
    fn apply_ferry_minus_splits_across_parts_of_an_overnight_shift() {
        let rows = vec![
            tc("2026-06-02 22:00:00", "始業"),
            tc("2026-06-03 08:00:00", "終業"),
        ];
        let mut days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days[0].parts.len(), 2);
        let mut ferry = BTreeMap::new();
        ferry.insert("2026-06-02".to_string(), 30);
        ferry.insert("2026-06-03".to_string(), 45);
        apply_ferry_minus(&mut days, &ferry);
        assert_eq!(days[0].parts[0].ferry_minus_minutes, 30);
        assert_eq!(days[0].parts[1].ferry_minus_minutes, 45);
        // 勤務の値は内訳の合計
        assert_eq!(days[0].ferry_minus_minutes, 75);
    }

    #[test]
    fn apply_ferry_minus_counts_a_calendar_day_once_even_with_several_shifts() {
        // 実データ (1726 / 2026-03-14) の形: 打刻が無く、**フェリー自体が休息イベント**
        // なので勤務が休息のたびに割れる (実測 4 勤務・拘束 1 / 16 / 82 / 222 分)。
        // 日付の一致する勤務すべてに載せると、消費側の暦日合算で 433 が 4 回足される
        let rows = vec![
            ev("2026-06-02 05:00:00", "2026-06-02 06:00:00", "休息"),
            ev("2026-06-02 07:00:00", "2026-06-02 10:40:00", "休息"),
            ev("2026-06-02 12:00:00", "2026-06-02 15:30:00", "休息"),
            ev("2026-06-02 18:00:00", "2026-06-02 19:00:00", "休息"),
        ];
        let mut days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert!(days.len() > 1, "休息で勤務が割れる前提のテスト");
        let ferry = ferry_minus_by_date(&[
            ferry("2026-06-02 07:00:00", "2026-06-02 10:40:00"),
            ferry("2026-06-02 12:00:00", "2026-06-02 15:30:00"),
        ]);
        assert_eq!(ferry.get("2026-06-02"), Some(&430));
        apply_ferry_minus(&mut days, &ferry);
        // 暦日の合計は 1 回ぶん
        let total: i64 = days.iter().map(|d| d.ferry_minus_minutes).sum();
        assert_eq!(total, 430);
        // 最初の勤務が持つ
        assert_eq!(days[0].ferry_minus_minutes, 430);
        assert!(days[1..].iter().all(|d| d.ferry_minus_minutes == 0));
    }

    #[test]
    fn apply_ferry_minus_leaves_zero_when_there_is_none() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let mut days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        // 空なら何も触らない (早期 return)
        apply_ferry_minus(&mut days, &BTreeMap::new());
        assert_eq!(days[0].ferry_minus_minutes, 0);
        // 別の日の控除しか無ければ 0 のまま
        let mut other = BTreeMap::new();
        other.insert("2026-06-09".to_string(), 60);
        apply_ferry_minus(&mut days, &other);
        assert_eq!(days[0].ferry_minus_minutes, 0);
    }

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, FMT).unwrap()
    }

    fn tc(datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "end_datetime": null, "source": "timecard", "state": state})
    }

    fn dtako(datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "end_datetime": null, "source": "dtako", "state": state})
    }

    fn ev(start: &str, end: &str, state: &str) -> serde_json::Value {
        json!({"datetime": start, "end_datetime": end, "source": "dtako_events", "state": state})
    }

    #[test]
    fn parse_skips_broken_rows() {
        let rows = vec![
            tc("2026-06-02 09:25:00", "始業"),
            json!({"datetime": "not a date", "source": "timecard", "state": "始業"}),
            json!({"datetime": null, "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 09:25:00", "source": null, "state": "始業"}),
            json!({"datetime": "2026-06-02 09:25:00", "source": "timecard", "state": null}),
            // end_datetime が壊れていても行自体は生きる (end だけ None)
            ev("2026-06-02 12:00:00", "bogus", "休憩"),
        ];
        let out = parse_events(&rows);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].state, "始業");
        assert!(out[1].end.is_none());
    }

    #[test]
    fn parse_sorts_by_datetime() {
        let rows = vec![
            tc("2026-06-02 19:39:00", "終業"),
            tc("2026-06-02 09:25:00", "始業"),
        ];
        let out = parse_events(&rows);
        assert_eq!(out[0].state, "始業");
    }

    #[test]
    fn floor_min_drops_seconds() {
        assert_eq!(
            floor_min(dt("2026-06-02 09:25:41")),
            dt("2026-06-02 09:25:00")
        );
    }

    #[test]
    fn night_window_is_22_to_5() {
        assert!(is_night(dt("2026-06-02 22:00:00")));
        assert!(is_night(dt("2026-06-02 23:59:00")));
        assert!(is_night(dt("2026-06-02 04:59:00")));
        assert!(!is_night(dt("2026-06-02 05:00:00")));
        assert!(!is_night(dt("2026-06-02 21:59:00")));
    }

    // --- 打刻のみ (1018 型: 日帰り、休息イベントなし) ---

    #[test]
    fn timecard_only_day_shift() {
        // 2026-06-02 は火曜。09:25〜19:39 = 614 分、休憩 96 分 → 実働 518 分
        let rows = vec![
            tc("2026-06-02 09:25:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:36:00", "休憩"),
            tc("2026-06-02 19:39:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        let d = &d[0];
        assert_eq!(d.source, ShiftSource::Timecard);
        assert!(!d.is_legal_holiday);
        assert_eq!(d.restraint_minutes, 614);
        assert_eq!(d.break_minutes, 96);
        assert_eq!(d.working_minutes, 518);
        // 450 所定内 + 30 法定内残業 + 38 法定時間外
        assert_eq!(d.statutory_minutes, 450);
        assert_eq!(d.within_statutory_overtime_minutes, 30);
        assert_eq!(d.overtime_minutes, 38);
        assert_eq!(d.legal_holiday_minutes, 0);
        // 昼勤なので深夜は無い
        assert_eq!(d.night_minutes, 0);
        assert_eq!(d.overtime_night_minutes, 0);
    }

    #[test]
    fn working_equals_statutory_plus_overtime() {
        let rows = vec![
            tc("2026-06-02 09:25:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:36:00", "休憩"),
            tc("2026-06-02 19:39:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(
            d.working_minutes,
            d.statutory_minutes
                + d.within_statutory_overtime_minutes
                + d.overtime_minutes
                + d.legal_holiday_minutes
        );
        assert_eq!(d.restraint_minutes, d.working_minutes + d.break_minutes);
    }

    #[test]
    fn short_shift_has_no_overtime() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"), // 360 分
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        // 運行に出ていない勤務なので昼休憩 60 分が引かれる (360 - 60)
        assert_eq!(d.statutory_minutes, 300);
        assert_eq!(d.within_statutory_overtime_minutes, 0);
        assert_eq!(d.overtime_minutes, 0);
    }

    #[test]
    fn timecard_without_close_is_dropped() {
        let rows = vec![tc("2026-06-02 09:25:00", "始業")];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn timecard_close_before_open_is_dropped() {
        let rows = vec![
            tc("2026-06-02 19:39:00", "始業"),
            // 始業より前の終業 (壊れた打刻) — parse で並べ替わるので終業が先に来る
            tc("2026-06-02 09:25:00", "終業"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn timecard_ignores_other_states() {
        let rows = vec![
            json!({"datetime": "2026-06-02 08:00:00", "end_datetime": null,
                   "source": "timecard", "state": "その他"}),
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ];
        assert_eq!(
            daily_summary(&rows, "2026-06", &KosokuParams::default()).len(),
            1
        );
    }

    // --- 休息のみ (1119 型: 長距離、打刻なし) ---

    #[test]
    fn rest_only_shift_spans_midnight() {
        // 休息 6/1 16:19→6/2 04:42、次の休息 6/2 16:18→6/3 06:01
        // → 勤務は 6/2 04:42〜16:18 = 696 分 (実測値と同じ)
        let rows = vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].source, ShiftSource::Rest);
        assert_eq!(d[0].start, "2026-06-02 04:42:00");
        assert_eq!(d[0].end, "2026-06-02 16:18:00");
        assert_eq!(d[0].restraint_minutes, 696);
        // 04:42〜05:00 の 18 分が深夜
        assert_eq!(d[0].night_minutes, 18);
    }

    #[test]
    fn rest_needs_two_events_to_make_a_shift() {
        let rows = vec![ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息")];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn rest_without_end_is_ignored() {
        let rows = vec![
            json!({"datetime": "2026-06-01 16:19:00", "end_datetime": null,
                   "source": "dtako_events", "state": "休息"}),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn point_rest_from_dtako_is_ignored() {
        // dtako 側の休息は点イベント — 対にする根拠が無いので使わない
        let rows = vec![
            json!({"datetime": "2026-06-01 16:19:00", "end_datetime": "2026-06-02 04:42:00",
                   "source": "dtako", "state": "休息"}),
            json!({"datetime": "2026-06-02 16:18:00", "end_datetime": "2026-06-03 06:01:00",
                   "source": "dtako", "state": "休息"}),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn zero_length_rest_is_ignored() {
        let rows = vec![
            ev("2026-06-01 16:19:00", "2026-06-01 16:19:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn overlapping_rests_make_no_shift() {
        // 次の休息の開始が前の休息の終了より前 = 勤務時間が無い
        let rows = vec![
            ev("2026-06-01 16:00:00", "2026-06-02 10:00:00", "休息"),
            ev("2026-06-02 08:00:00", "2026-06-02 20:00:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    // --- 打刻優先 ---

    #[test]
    fn timecard_wins_over_overlapping_rest() {
        let rows = vec![
            ev("2026-06-01 20:00:00", "2026-06-02 08:00:00", "休息"),
            tc("2026-06-02 09:25:00", "始業"),
            tc("2026-06-02 19:39:00", "終業"),
            ev("2026-06-02 22:00:00", "2026-06-03 08:00:00", "休息"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        // 休息由来 (08:00〜22:00) は打刻由来 (09:25〜19:39) と重なるので捨てる
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].source, ShiftSource::Timecard);
        assert_eq!(d[0].start, "2026-06-02 09:25:00");
    }

    #[test]
    fn rest_shift_kept_when_not_overlapping_timecard() {
        // 6/2 は打刻、6/5〜6/6 は休息だけ — 両方残る
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
            ev("2026-06-04 20:00:00", "2026-06-05 06:00:00", "休息"),
            ev("2026-06-05 18:00:00", "2026-06-06 06:00:00", "休息"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].source, ShiftSource::Timecard);
        assert_eq!(d[1].source, ShiftSource::Rest);
        assert_eq!(d[1].date, "2026-06-05");
    }

    // --- 法定休日 (日曜) ---

    #[test]
    fn sunday_is_legal_holiday_and_has_no_overtime_split() {
        // 2026-06-07 は日曜
        let rows = vec![
            tc("2026-06-07 08:00:00", "始業"),
            tc("2026-06-07 20:00:00", "終業"), // 720 分
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.is_legal_holiday);
        // 運行に出ていない勤務なので昼休憩 60 分が引かれる (720 - 60)
        assert_eq!(d.legal_holiday_minutes, 660);
        assert_eq!(d.statutory_minutes, 0);
        assert_eq!(d.within_statutory_overtime_minutes, 0);
        assert_eq!(d.overtime_minutes, 0);
    }

    #[test]
    fn sunday_night_goes_to_legal_holiday_night() {
        // 日曜 21:00〜翌 02:00 → 深夜は 22:00〜02:00 の 240 分
        let rows = vec![
            tc("2026-06-07 21:00:00", "始業"),
            tc("2026-06-08 02:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.is_legal_holiday);
        assert_eq!(d.legal_holiday_night_minutes, 240);
        assert_eq!(d.night_minutes, 0);
        assert_eq!(d.overtime_night_minutes, 0);
    }

    // --- 深夜の振り分け ---

    #[test]
    fn night_splits_between_statutory_and_overtime() {
        // 月曜 18:00〜翌 06:00 = 拘束 720 分。昼の窓に掛からないので休憩 60 分が
        // まん中 (23:30-00:30、深夜帯) に入り、実働は 660 分
        //   実働 0〜450 分  = 18:00〜23:30 + 00:30〜02:30 … うち深夜 210 分 → night
        //   実働 450〜480   = 02:30〜03:00 … 全部深夜 30 分            → night (法定内残業)
        //   実働 480〜660   = 03:00〜06:00 … うち深夜 03:00〜05:00 = 120 分 → overtime_night
        let rows = vec![
            tc("2026-06-08 18:00:00", "始業"),
            tc("2026-06-09 06:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.working_minutes, 660);
        assert_eq!(d.night_minutes, 240);
        assert_eq!(d.overtime_night_minutes, 120);
        assert_eq!(d.legal_holiday_night_minutes, 0);
        // 深夜と時間外深夜は排他 — 合計は深夜帯 (22:00〜05:00 = 420 分) から
        // 深夜に入った休憩 60 分を引いた 360 分
        assert_eq!(d.night_minutes + d.overtime_night_minutes, 360);
    }

    // --- 休憩 ---

    #[test]
    fn breaks_below_threshold_are_ignored() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 10:09:00", "休憩"), // 9 分
            ev("2026-06-02 12:00:00", "2026-06-02 12:10:00", "休憩"), // 10 分
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 10);
    }

    #[test]
    fn break_threshold_is_configurable() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 10:09:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let p = KosokuParams {
            break_threshold_minutes: 5,
            ..KosokuParams::default()
        };
        assert_eq!(daily_summary(&rows, "2026-06", &p)[0].break_minutes, 9);
    }

    #[test]
    fn overlapping_breaks_are_counted_once() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:00:00", "休憩"),
            ev("2026-06-02 12:30:00", "2026-06-02 13:30:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 90);
        assert_eq!(d.working_minutes, 540 - 90);
    }

    #[test]
    fn break_outside_shift_is_clipped_away() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 07:00:00", "2026-06-02 08:00:00", "休憩"), // 勤務前
            ev("2026-06-02 19:00:00", "2026-06-02 20:00:00", "休憩"), // 勤務後
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        // 勤務外の 2 件はどちらも落ちている — 残っているのは昼休憩の 60 分だけ
        // (落ちていなければ 120 分が上乗せされる)
        assert_eq!(d.break_minutes, 60);
        assert_eq!(d.working_minutes, 540 - 60);
    }

    // --- 運行に出ていない勤務の昼休憩 (ユーザー決定 2026-07-28) ---

    #[test]
    fn shift_without_operation_gets_lunch_break() {
        // 事務員の 1 日。デジタコには何も残らないので、休憩イベントは 1 件も無い
        let rows = vec![
            tc("2026-06-02 08:00:00", "始業"),
            tc("2026-06-02 18:00:00", "終業"), // 600 分
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 60);
        assert_eq!(d.working_minutes, 540);
        // 拘束は昼休憩を引く前のまま (拘束 = 終業 − 始業)
        assert_eq!(d.restraint_minutes, 600);
    }

    #[test]
    fn shift_with_operation_keeps_event_breaks_only() {
        // 運行に出ている日は昼休憩を足さない (デジタコの休憩イベントが正)
        let rows = vec![
            tc("2026-06-02 08:00:00", "始業"),
            ev("2026-06-02 09:00:00", "2026-06-02 17:00:00", "運転"),
            ev("2026-06-02 15:00:00", "2026-06-02 15:30:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 30);
        assert_eq!(d.working_minutes, 570);
    }

    #[test]
    fn operation_without_any_break_event_still_gets_no_lunch() {
        // 運行に出ていれば、休憩イベントが 0 件でも昼休憩は足さない
        let rows = vec![
            tc("2026-06-02 08:00:00", "始業"),
            ev("2026-06-02 09:00:00", "2026-06-02 17:00:00", "運転"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
        assert_eq!(d.working_minutes, 600);
    }

    #[test]
    fn point_operation_event_inside_shift_counts_as_operation() {
        // 運行開始 / 運行終了 (`dtako`) は区間を持たない点イベント
        let rows = vec![
            tc("2026-06-02 08:00:00", "始業"),
            dtako("2026-06-02 09:00:00", "運行開始"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
    }

    #[test]
    fn point_operation_event_outside_shift_is_not_operation() {
        // 勤務の外にある運行イベントはこの勤務の証拠にならない
        let rows = vec![
            tc("2026-06-02 08:00:00", "始業"),
            dtako("2026-06-02 20:00:00", "運行開始"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 60);
    }

    #[test]
    fn rest_event_alone_is_not_operation() {
        // 休息は勤務の**外側**の境界なので、運行した証拠にはしない。
        // 06/02 06:00 に終わる休息 → 始業、06/03 06:00 に始まる休息 → 終業
        let rows = vec![
            ev("2026-06-01 20:00:00", "2026-06-02 06:00:00", "休息"),
            ev("2026-06-03 06:00:00", "2026-06-03 16:00:00", "休息"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.source, ShiftSource::Rest);
        // 入るのは 6/2 の昼だけ — 6/3 の昼は終業 (06:00) より後なので切り落とされる
        assert_eq!(d.break_minutes, 60);
    }

    #[test]
    fn lunch_is_clipped_to_the_shift() {
        // 12:30 始業 → 入るのは 12:30-13:00 の 30 分だけ
        let rows = vec![
            tc("2026-06-02 12:30:00", "始業"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 30);
    }

    #[test]
    fn shift_not_covering_noon_has_no_lunch() {
        // 5 時間なので夜勤側の 1 時間も入らない (6 時間以下)
        let rows = vec![
            tc("2026-06-02 13:00:00", "始業"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
        assert_eq!(d.working_minutes, 300);
    }

    // --- 昼の窓に掛からない勤務の休憩 (ユーザー指示 2026-07-28) ---

    #[test]
    fn night_shift_gets_one_hour_in_the_middle() {
        // 実測の作業員 1196 と同じ形 (23:45 → 翌 08:00 = 495 分)。
        // まん中 = 始業 + (495 - 60) / 2 = +217 分 → 03:22-04:22
        let rows = vec![
            tc("2026-06-02 23:45:00", "始業"),
            tc("2026-06-03 08:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.restraint_minutes, 495);
        assert_eq!(d.break_minutes, 60);
        assert_eq!(d.working_minutes, 435);
        // 休憩は 03:22-04:22 = すべて深夜帯なので、深夜がそのぶん減る。
        // 深夜は 23:45-05:00 の 315 分から 60 分引いた 255 分
        assert_eq!(d.night_minutes, 255);
    }

    #[test]
    fn evening_shift_of_six_hours_or_less_gets_no_break() {
        // 事務 1706 / 1707 と同じ形 (19:00 → 翌 00:00 = 5 時間)
        let rows = vec![
            tc("2026-06-02 19:00:00", "始業"),
            tc("2026-06-03 00:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
        assert_eq!(d.working_minutes, 300);
    }

    #[test]
    fn exactly_six_hours_off_hours_gets_no_break() {
        // 境界: 6 時間ちょうどは「6 時間超」ではないので入れない
        let rows = vec![
            tc("2026-06-02 18:00:00", "始業"),
            tc("2026-06-03 00:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
    }

    #[test]
    fn one_minute_over_six_hours_off_hours_gets_the_break() {
        let rows = vec![
            tc("2026-06-02 17:59:00", "始業"),
            tc("2026-06-03 00:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 60);
    }

    #[test]
    fn night_shift_on_an_operation_day_keeps_event_breaks() {
        // 運行に出ている夜勤には入れない (デジタコの休憩イベントが正)
        let rows = vec![
            tc("2026-06-02 23:45:00", "始業"),
            ev("2026-06-03 00:30:00", "2026-06-03 07:00:00", "運転"),
            tc("2026-06-03 08:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 0);
        assert_eq!(d.working_minutes, 495);
    }

    #[test]
    fn break_event_alone_counts_as_operation_so_no_lunch_is_added() {
        // 休憩イベントは運行に紐づいて記録される = その勤務は運行に出ている。
        // 昼と重なっていても足し合わせない (12:00-13:30 の 90 分にはならない)
        let rows = vec![
            tc("2026-06-02 08:00:00", "始業"),
            ev("2026-06-02 12:30:00", "2026-06-02 13:30:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 60);
        assert_eq!(d.working_minutes, 540);
    }

    #[test]
    fn break_straddling_shift_start_is_clipped_not_dropped() {
        // 08:45〜09:15 の 30 分休憩。勤務内は 09:00〜09:15 の 15 分だけ
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            ev("2026-06-02 08:45:00", "2026-06-02 09:15:00", "休憩"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.break_minutes, 15);
    }

    #[test]
    fn break_without_end_is_ignored() {
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            json!({"datetime": "2026-06-02 12:00:00", "end_datetime": null,
                   "source": "dtako_events", "state": "休憩"}),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        assert_eq!(
            daily_summary(&rows, "2026-06", &KosokuParams::default())[0].break_minutes,
            0
        );
    }

    // --- 同日の運行の継ぎ目 ---

    #[test]
    fn same_day_run_joint_without_punch_is_work_not_break() {
        // 乗務員 1029 / 2026-06-23 と同じ形 — 継ぎ目 5 分、間に打刻なし (2 本目は
        // 同じ乗務員の 06-24 で観測した 1 分の継ぎ目を同日に寄せたもの)。
        // 継ぎ目は勤務の中に残り、休憩にも入らない (作業として拘束・実働に計上)
        let rows = vec![
            tc("2026-06-23 00:04:00", "始業"),
            dtako("2026-06-23 00:18:00", "運行終了"),
            dtako("2026-06-23 00:23:00", "運行開始"),
            dtako("2026-06-23 02:15:00", "運行終了"),
            dtako("2026-06-23 02:16:00", "運行開始"),
            tc("2026-06-23 15:03:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].restraint_minutes, 899);
        assert_eq!(d[0].break_minutes, 0);
        assert_eq!(d[0].working_minutes, 899);
    }

    #[test]
    fn same_day_run_joint_with_close_punch_splits_into_two_shifts() {
        // 乗務員 1026 / 2026-06-02 と同じ形 — 継ぎ目 14:07→23:26 に終業打刻がある。
        // 勤務が切れ、間の時間は拘束に入らない (休息)
        let rows = vec![
            tc("2026-06-02 01:28:00", "始業"),
            dtako("2026-06-02 14:07:00", "運行終了"),
            tc("2026-06-02 14:14:00", "終業"),
            tc("2026-06-02 23:32:00", "始業"),
            dtako("2026-06-02 23:26:00", "運行開始"),
            tc("2026-06-03 15:13:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 2);
        assert_eq!(d[0].restraint_minutes, 766);
        assert_eq!(d[1].restraint_minutes, 941);
        // 14:14〜23:32 の 558 分はどちらの拘束にも入らない
        assert_eq!(d[0].end, "2026-06-02 14:14:00");
        assert_eq!(d[1].start, "2026-06-02 23:32:00");
    }

    // --- 24 時間超の打ち切り ---

    #[test]
    fn restraint_over_24h_is_kept_and_flagged() {
        // 実測の外れ値と同じ長さ (2288 分 = 38.1 時間) を与える。**打ち切らない**
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        assert_eq!(d.restraint_minutes, 2288);
        assert_eq!(d.end, "2026-06-03 20:08:00");
        // 内訳の合計は実働に一致する
        assert_eq!(
            d.working_minutes,
            d.statutory_minutes
                + d.within_statutory_overtime_minutes
                + d.overtime_minutes
                + d.legal_holiday_minutes
        );
    }

    #[test]
    fn exactly_24h_is_not_flagged() {
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 06:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(!d.over_24h);
        assert_eq!(d.restraint_minutes, 1440);
    }

    #[test]
    fn breaks_in_an_over_24h_shift_are_counted() {
        // 以前は 24 時間で打ち切っていたので翌日 10:00 の休憩が勤務外になっていた。
        // 打ち切らなくなったので**勤務の中の休憩として数える**
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            ev("2026-06-03 10:00:00", "2026-06-03 11:00:00", "休憩"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        // 6/3 10:00-11:00 の休憩 (60 分) + 6/2 と 6/3 の昼休憩は入らない
        // (休憩イベントが 1 件でもある勤務は運行扱いで昼休憩を入れない)
        assert_eq!(d.break_minutes, 60);
        assert_eq!(d.working_minutes, 2288 - 60);
    }

    // --- 秒の落とし方 (Refs ohishi-exp/nuxt-dtako-admin#501) ---

    #[test]
    fn truncate_elapsed_matches_the_paper_timecard() {
        // 始業 09:00:30 / 終業 17:00:20 → 経過 7:59:50。
        // 紙 (nginx `date_diff()->h*60 + ->i`) は秒を捨てて 479 分
        let rows = vec![
            tc("2026-06-02 09:00:30", "始業"),
            tc("2026-06-02 17:00:20", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.restraint_minutes, 479);
    }

    #[test]
    fn floor_endpoints_keeps_the_old_number() {
        // 同じ入力を従来の丸めで引くと 09:00 → 17:00 の 480 分。
        // **1 分大きい** — これが紙との差の正体だった
        let rows = vec![
            tc("2026-06-02 09:00:30", "始業"),
            tc("2026-06-02 17:00:20", "終業"),
        ];
        let p = KosokuParams {
            restraint_rounding: RestraintRounding::FloorEndpoints,
            ..KosokuParams::default()
        };
        let d = &daily_summary(&rows, "2026-06", &p)[0];
        assert_eq!(d.restraint_minutes, 480);
    }

    #[test]
    fn truncate_elapsed_changes_nothing_when_seconds_do_not_wrap() {
        // 終業の秒 >= 始業の秒 なら両者は一致する。ずれるのは繰り下がるときだけ
        let rows = vec![
            tc("2026-06-02 09:00:10", "始業"),
            tc("2026-06-02 17:00:40", "終業"),
        ];
        let trunc = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        let p = KosokuParams {
            restraint_rounding: RestraintRounding::FloorEndpoints,
            ..KosokuParams::default()
        };
        let floor = &daily_summary(&rows, "2026-06", &p)[0];
        assert_eq!(trunc.restraint_minutes, 480);
        assert_eq!(floor.restraint_minutes, 480);
    }

    #[test]
    fn truncate_elapsed_keeps_restraint_equal_to_working_plus_break() {
        // 1 分削っても `拘束 = 実働 + 休憩` は崩さない (削るのは終業側なので実働が吸う)
        let rows = vec![
            tc("2026-06-02 09:00:30", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:00:00", "休憩"),
            tc("2026-06-02 17:00:20", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.restraint_minutes, d.working_minutes + d.break_minutes);
        assert_eq!(d.restraint_minutes, 479);
    }

    #[test]
    fn truncate_elapsed_still_keeps_the_closing_punch() {
        // 削った 1 分の上に終業打刻が乗っていても落とさない (punch_window_end)
        let rows = vec![
            tc("2026-06-02 09:25:37", "始業"),
            tc("2026-06-02 19:39:04", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.end, "2026-06-02 19:38:00");
        assert_eq!(
            d.punches.len(),
            2,
            "終業打刻が勤務の end より後になっても拾う"
        );
        assert_eq!(d.punches[1].state, "終業");
    }

    // --- 勤務を構成した打刻 (Refs #128) ---

    #[test]
    fn punches_keep_the_raw_stamps() {
        // 秒を落とさない (打刻カードの元の値)
        let rows = vec![
            tc("2026-06-02 09:25:37", "始業"),
            tc("2026-06-02 19:39:04", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(
            d.punches,
            vec![
                Punch {
                    at: "2026-06-02 09:25:37".into(),
                    state: "始業".into()
                },
                Punch {
                    at: "2026-06-02 19:39:04".into(),
                    state: "終業".into()
                },
            ]
        );
        // 勤務としての解釈 (分に丸め) は start / end のまま
        assert_eq!(d.start, "2026-06-02 09:25:00");
    }

    #[test]
    fn punches_survive_an_over_24h_shift() {
        // 乗務員 1194 / 2026-04 と同じ形 — 打刻が運行 1 本 (2 晩) を挟み 43 時間になる。
        // 拘束は 43 時間のまま出し、終業打刻も落とさない
        let rows = vec![
            tc("2026-06-01 21:31:32", "始業"),
            tc("2026-06-03 16:47:04", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        assert_eq!(d.restraint_minutes, 2595);
        // 秒は経過時間の切り捨て (#149) — 終業の秒 04 < 始業の秒 32 なので 1 分手前
        assert_eq!(d.end, "2026-06-03 16:46:00");
        // 打刻は実際の終業まで残る
        assert_eq!(d.punches.len(), 2);
        assert_eq!(d.punches[1].at, "2026-06-03 16:47:04");
    }

    #[test]
    fn punches_are_empty_for_rest_shifts() {
        let rows = vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.source, ShiftSource::Rest);
        assert!(d.punches.is_empty());
    }

    #[test]
    fn punches_exclude_other_shifts_and_non_punch_events() {
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            dtako("2026-06-02 06:30:00", "運行開始"),
            tc("2026-06-02 15:00:00", "終業"),
            // 次の勤務の打刻は混ぜない
            tc("2026-06-02 20:00:00", "始業"),
            tc("2026-06-03 05:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].punches.len(), 2);
        assert_eq!(days[0].punches[1].at, "2026-06-02 15:00:00");
        assert_eq!(days[1].punches[0].at, "2026-06-02 20:00:00");
    }

    #[test]
    fn punch_traits_are_wired() {
        let p = Punch {
            at: "2026-06-02 09:25:37".into(),
            state: "始業".into(),
        };
        assert_eq!(p.clone(), p);
        assert!(format!("{p:?}").contains("始業"));
    }

    // --- 24 時間超の勤務を休息で切り直す (Refs #133) ---

    #[test]
    fn long_punch_shift_is_split_by_rests() {
        // 乗務員 1021 / 2026-04 と同じ形 — 打刻が 6 日間を 1 勤務として挟む
        let rows = vec![
            tc("2026-06-03 06:33:00", "始業"),
            ev("2026-06-03 20:00:00", "2026-06-04 06:00:00", "休息"),
            ev("2026-06-04 21:00:00", "2026-06-05 07:00:00", "休息"),
            tc("2026-06-05 15:16:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 3);
        assert_eq!(days[0].start, "2026-06-03 06:33:00");
        assert_eq!(days[0].end, "2026-06-03 20:00:00");
        assert_eq!(days[1].start, "2026-06-04 06:00:00");
        assert_eq!(days[1].end, "2026-06-04 21:00:00");
        assert_eq!(days[2].start, "2026-06-05 07:00:00");
        assert_eq!(days[2].end, "2026-06-05 15:16:00");
        // 休息で割れたので打ち切りは残らない
        assert!(days.iter().all(|d| !d.over_24h));
        // 打刻は端の勤務にだけ付く (中の勤務は休息で切っただけなので打刻が無い)
        assert_eq!(days[0].punches.len(), 1);
        assert_eq!(days[0].punches[0].state, "始業");
        assert!(days[1].punches.is_empty());
        assert_eq!(days[2].punches[0].state, "終業");
    }

    #[test]
    fn splitting_removes_the_24h_cap_where_rests_exist() {
        let rows = vec![
            tc("2026-06-03 06:00:00", "始業"),
            ev("2026-06-03 20:00:00", "2026-06-04 06:00:00", "休息"),
            tc("2026-06-04 18:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert!(days.iter().all(|d| !d.over_24h));
        assert_eq!(days[0].restraint_minutes, 840); // 06:00〜20:00
        assert_eq!(days[1].restraint_minutes, 720); // 翌 06:00〜18:00
    }

    #[test]
    fn a_39h_shift_with_only_short_breaks_is_reported_in_full() {
        // 乗務員 1674 / 2026-04-07 01:08 → 04-08 16:22 と同じ形 (Refs #152)。
        // 打刻なし・休息は両端だけ・中の休憩は最長 152 分・運行の空きは 36 分。
        // 切る根拠がどこにも無く、**本物の 39.2 時間拘束**だった
        let rows = vec![
            ev("2026-06-06 19:00:00", "2026-06-07 01:08:00", "休息"),
            ev("2026-06-07 06:21:00", "2026-06-07 08:53:00", "休憩"), // 152 分
            dtako("2026-06-08 07:49:00", "運行終了"),
            dtako("2026-06-08 08:25:00", "運行開始"), // 36 分 = 閾値未満
            ev("2026-06-08 16:22:00", "2026-06-09 02:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        let d = &days[0];
        assert!(d.over_24h);
        // 01:08 → 16:22 = 2354 分。1440 に丸めない
        assert_eq!(d.restraint_minutes, 2354);
        assert_eq!(d.end, "2026-06-08 16:22:00");
        // 3 暦日に配られる (打ち切っていたら 6/9 が出ない)
        assert_eq!(d.parts.len(), 2);
        assert_eq!(
            d.parts.iter().map(|p| p.restraint_minutes).sum::<i64>(),
            2354
        );
    }

    #[test]
    fn a_long_shift_without_rests_keeps_its_real_length() {
        // 休息を挟まず 24 時間以上 = 切る根拠が無い。**実測のまま出して旗を立てる**
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert!(days[0].over_24h);
        assert_eq!(days[0].restraint_minutes, 2288);
    }

    #[test]
    fn a_normal_shift_is_not_split_by_a_rest_inside() {
        // 24 時間以内の勤務は休息があっても**切らない** (#118 の「運行では切らない」を
        // 崩さない)。ただし休息そのものは拘束から外す (2026-07-28)
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:00:00", "休息"),
            tc("2026-06-02 20:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].restraint_minutes, 780); // 840 − 休息 60
        assert_eq!(days[0].rest_minus_minutes, 60);
    }

    #[test]
    fn rests_hanging_off_the_edges_are_clipped() {
        // 勤務の前後にはみ出した休息で先頭・末尾を削らない
        let rows = vec![
            tc("2026-06-03 06:00:00", "始業"),
            ev("2026-06-02 20:00:00", "2026-06-03 07:00:00", "休息"), // 前にはみ出す
            ev("2026-06-03 22:00:00", "2026-06-04 08:00:00", "休息"),
            tc("2026-06-04 18:00:00", "終業"), // 全体では 36 時間
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        // 06:00〜07:00 は休息に食われる (はみ出した分は勤務の中へ切り詰める)
        assert_eq!(days[0].start, "2026-06-03 07:00:00");
        assert_eq!(days[0].end, "2026-06-03 22:00:00");
        assert_eq!(days[1].start, "2026-06-04 08:00:00");
        assert_eq!(days[1].end, "2026-06-04 18:00:00");
    }

    #[test]
    fn overlapping_rests_are_merged_before_splitting() {
        let rows = vec![
            tc("2026-06-03 06:00:00", "始業"),
            ev("2026-06-04 02:00:00", "2026-06-04 10:00:00", "休息"),
            ev("2026-06-04 08:00:00", "2026-06-04 12:00:00", "休息"),
            tc("2026-06-04 20:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].end, "2026-06-04 02:00:00");
        assert_eq!(days[1].start, "2026-06-04 12:00:00");
    }

    #[test]
    fn a_rest_covering_the_whole_shift_leaves_it_alone() {
        // 切ると何も残らない場合は元の勤務のまま (打ち切りに任せる)
        let rows = vec![
            tc("2026-06-03 06:00:00", "始業"),
            ev("2026-06-03 06:00:00", "2026-06-05 06:00:00", "休息"),
            tc("2026-06-05 06:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert!(days[0].over_24h);
        // 外して何も残らないので外さない (拘束 0 の出勤日にはしない)
        assert_eq!(days[0].restraint_minutes, 2880);
        assert_eq!(days[0].rest_minus_minutes, 0);
    }

    // --- 最後の休息を終業打刻で閉じる (Refs nuxt-dtako-admin#501) ---

    #[test]
    fn the_last_rest_is_closed_by_a_punch_out() {
        // 乗務員 1526 陣内 / 2026-03-02 の形。次の休息が無いので勤務ごと落ちていた
        let rows = vec![
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
            tc("2026-03-03 05:57:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].start, "2026-03-02 17:32:00");
        assert_eq!(days[0].end, "2026-03-03 05:57:00");
        assert_eq!(days[0].restraint_minutes, 745);
        // 暦日按分は 03-02 が 388 分・03-03 が 357 分 (紙との残差そのもの)
        let parts = &days[0].parts;
        assert_eq!(parts[0].restraint_minutes, 388);
        assert_eq!(parts[1].restraint_minutes, 357);
    }

    // --- 休息の中の終業打刻 (nginx に合わせる、Refs nuxt-dtako-admin#501) ---

    #[test]
    fn a_punch_out_inside_the_next_rest_extends_the_shift_to_the_punch() {
        // 1072 立山 / 2026-03-13 の形。休息 17:27→翌08:37 の中に終業 18:47。
        // 紙は 17:27→18:47 の 80 分を TC_DC で拘束に足す (665 = 586 + 79)
        let rows = vec![
            ev("2026-03-12 17:20:00", "2026-03-13 07:41:00", "休息"),
            tc("2026-03-13 18:47:00", "終業"),
            ev("2026-03-13 17:27:00", "2026-03-14 08:37:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].start, "2026-03-13 07:41:00");
        assert_eq!(days[0].end, "2026-03-13 18:47:00");
        assert_eq!(days[0].restraint_minutes, 666);
        // 休息 (17:27→18:47 の部分) を引き戻さない
        assert_eq!(days[0].rest_minus_minutes, 0);
    }

    #[test]
    fn a_punch_out_inside_a_mid_day_rest_keeps_the_pre_punch_rest_as_restraint() {
        // 1072 立山 / 2026-03-21 の形。昼の休息 11:05→17:10 の中に終業 16:51。
        // 紙 888 = デジタコ 543 (02:15→11:05 + 17:10→17:23) + TC_DC 345 (11:05→16:51)
        let rows = vec![
            ev("2026-03-20 16:57:00", "2026-03-21 02:15:00", "休息"),
            ev("2026-03-21 11:05:00", "2026-03-21 17:10:00", "休息"),
            tc("2026-03-21 16:51:00", "終業"),
            ev("2026-03-21 17:23:00", "2026-03-23 05:43:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        // 勤務は打刻まで伸び、11:05→16:51 は拘束のまま (休息は 16:51 から)
        assert_eq!(days[0].start, "2026-03-21 02:15:00");
        assert_eq!(days[0].end, "2026-03-21 16:51:00");
        assert_eq!(days[0].rest_minus_minutes, 0);
        // 休息明け 17:10 → 次の休息 17:23 の欠片はそのまま
        assert_eq!(days[1].start, "2026-03-21 17:10:00");
        assert_eq!(days[1].end, "2026-03-21 17:23:00");
        let total: i64 = days.iter().map(|d| d.restraint_minutes).sum();
        assert_eq!(total, 876 + 13);
    }

    #[test]
    fn a_long_punch_pair_is_split_at_the_clipped_rest_not_the_raw_one() {
        // 1684 倉掛 / 2026-03-28〜29 の形。33.7 時間の打刻ペアを休息で割るとき、
        // **打刻で切り詰めた後の休息**で割る。生の休息 (07:43 開始) で割ると
        // 07:43→12:16 の 272 分が欠片の外に落ちる (紙は TC_DC で数えている)
        let rows = vec![
            tc("2026-03-28 02:32:00", "始業"),
            ev("2026-03-28 14:17:00", "2026-03-29 04:23:00", "休息"),
            ev("2026-03-29 07:43:00", "2026-03-30 06:30:00", "休息"),
            tc("2026-03-29 12:16:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].start, "2026-03-28 02:32:00");
        assert_eq!(days[0].end, "2026-03-28 14:17:00");
        assert_eq!(days[1].start, "2026-03-29 04:23:00");
        assert_eq!(days[1].end, "2026-03-29 12:16:00");
        assert_eq!(days[1].restraint_minutes, 473);
    }

    #[test]
    fn a_punch_out_on_another_day_does_not_clip_the_rest() {
        // 翌朝の打刻で夜通しの休息が拘束になってはいけない
        let rows = vec![
            ev("2026-03-12 17:20:00", "2026-03-13 07:41:00", "休息"),
            ev("2026-03-13 17:27:00", "2026-03-14 08:37:00", "休息"),
            tc("2026-03-14 07:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].end, "2026-03-13 17:27:00");
    }

    // --- 打刻の無い運行を拾う (nginx に合わせる、Refs nuxt-dtako-admin#501) ---

    #[test]
    fn a_paired_punch_out_does_not_bridge_the_midday_gap() {
        // 1541 吉田 / 2026-03-14 の形。対の打刻 (06:06〜07:52) のあと打刻なしで
        // 12:33〜17:14 に運行。紙 382 = 対 101 + 運行 281 — **間の 4.7 時間は入らない**。
        // 次の休息は 2 日先 (03-16 22:34) で、03-16 には打刻のある次の運行がある
        let rows = vec![
            ev("2026-03-13 19:12:00", "2026-03-14 06:11:00", "休息"),
            tc("2026-03-14 06:06:00", "始業"),
            tc("2026-03-14 07:52:00", "終業"),
            dtako("2026-03-14 12:33:00", "運行開始"),
            ev("2026-03-14 12:33:00", "2026-03-14 17:14:00", "運転"),
            dtako("2026-03-14 17:14:00", "運行終了"),
            tc("2026-03-16 05:06:00", "始業"),
            dtako("2026-03-16 05:02:00", "運行開始"),
            ev("2026-03-16 22:34:00", "2026-03-17 04:11:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        // 対の勤務 + 午後の運行 (12:33 から — 終業打刻からではない)
        let afternoon = days
            .iter()
            .find(|d| d.start == "2026-03-14 12:33:00")
            .unwrap();
        assert_eq!(afternoon.end, "2026-03-14 17:14:00");
        assert_eq!(afternoon.restraint_minutes, 281);
        assert!(days.iter().any(|d| d.start == "2026-03-14 06:06:00"));
    }

    #[test]
    fn unpunched_ops_stop_at_the_next_punch_in() {
        // 次の始業打刻から先は打刻由来の勤務の領分 — 拾うと二重になる。
        // 始業のあとの運行しか無ければ、拾う勤務は生まれない
        let rows = vec![
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
            tc("2026-03-03 05:57:00", "終業"),
            tc("2026-03-06 06:30:00", "始業"),
            ev("2026-03-06 06:31:00", "2026-03-06 20:30:00", "運転"),
            tc("2026-03-06 20:35:00", "終業"),
            ev("2026-03-06 22:00:00", "2026-03-07 06:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].end, "2026-03-03 05:57:00");
        assert_eq!(days[1].start, "2026-03-06 06:30:00");
    }

    #[test]
    fn unpunched_ops_after_a_punch_out_become_a_shift_from_the_punch() {
        // 1021 鈴木 / 2026-03-19 の形。終業 12:47 のあと始業を打刻せず 13:38 に再出発し
        // 18:54 に休息入り。紙は 05:36→18:54 を通しで数える (797) — 打刻後の空白 50 分も
        // 拘束に入るので、**同じ暦日なら勤務は終業打刻から**始める
        let rows = vec![
            ev("2026-03-18 15:46:00", "2026-03-19 05:36:00", "休息"),
            tc("2026-03-19 12:47:00", "終業"),
            dtako("2026-03-19 13:38:00", "運行開始"),
            ev("2026-03-19 13:38:00", "2026-03-19 14:36:00", "運転"),
            // 実データには次の休息を跨いで終わる区間イベントもある (一般道実車が
            // 03-21 まで) — 終わりは次の休息で必ず切る
            ev("2026-03-19 18:38:00", "2026-03-21 07:59:00", "一般道実車"),
            ev("2026-03-19 18:54:00", "2026-03-21 07:52:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].start, "2026-03-19 05:36:00");
        assert_eq!(days[0].end, "2026-03-19 12:47:00");
        assert_eq!(days[1].start, "2026-03-19 12:47:00");
        assert_eq!(days[1].end, "2026-03-19 18:54:00");
        // 2 本の合計 = 通しのスパン (05:36→18:54 = 798)。紙と同じ数え方になる
        let total: i64 = days.iter().map(|d| d.restraint_minutes).sum();
        assert_eq!(total, 798);
    }

    #[test]
    fn unpunched_ops_on_a_later_day_start_at_their_first_event() {
        // 1072 立山 / 2026-03-25 の形。03-23 11:33 終業のあと打刻なしで 03-25 だけ運行。
        // 終業打刻から始めると間の空日 (03-24) が丸ごと拘束になる — 最初のイベントから
        let rows = vec![
            ev("2026-03-21 17:23:00", "2026-03-23 05:43:00", "休息"),
            tc("2026-03-23 11:33:00", "終業"),
            ev("2026-03-25 06:41:00", "2026-03-25 17:42:00", "運転"),
            ev("2026-03-25 17:42:00", "2026-03-26 03:46:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[1].start, "2026-03-25 06:41:00");
        assert_eq!(days[1].end, "2026-03-25 17:42:00");
        // 紙の 661 と一致。03-24 の行は無い
        assert_eq!(days[1].restraint_minutes, 661);
        assert!(days.iter().all(|d| d.date != "2026-03-24"));
    }

    #[test]
    fn no_ops_after_the_punch_out_adds_no_shift() {
        // 終業のあと何も無ければ従来どおり (帰宅しただけ)
        let rows = vec![
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
            tc("2026-03-03 05:57:00", "終業"),
            ev("2026-03-06 22:00:00", "2026-03-07 06:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].end, "2026-03-03 05:57:00");
    }

    #[test]
    fn unpunched_ops_covered_by_a_punch_shift_are_not_double_counted() {
        // 1526 陣内の形 — 終業のあとの運行が**打刻のある次の運行** (始業 06:30) なら、
        // それは打刻由来の勤務が数える。拾った勤務は重なりで捨てられる (打刻優先)
        let rows = vec![
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
            tc("2026-03-03 05:57:00", "終業"),
            tc("2026-03-06 06:30:00", "始業"),
            dtako("2026-03-06 06:31:00", "運行開始"),
            ev("2026-03-06 06:31:00", "2026-03-06 20:30:00", "運転"),
            tc("2026-03-06 20:35:00", "終業"),
            ev("2026-03-06 22:00:00", "2026-03-07 06:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].end, "2026-03-03 05:57:00"); // #162 の修正はそのまま
        assert_eq!(days[1].start, "2026-03-06 06:30:00"); // 打刻由来だけ
        assert_eq!(days[1].end, "2026-03-06 20:35:00");
    }

    #[test]
    fn unpunched_ops_ending_before_the_next_rest_end_the_shift_there() {
        // 運行が休息よりずっと前に途切れていれば、終わりは最後のイベントまで
        let rows = vec![
            ev("2026-03-18 15:46:00", "2026-03-19 05:36:00", "休息"),
            tc("2026-03-19 12:47:00", "終業"),
            ev("2026-03-19 13:38:00", "2026-03-19 15:00:00", "運転"),
            ev("2026-03-19 22:00:00", "2026-03-20 07:52:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[1].end, "2026-03-19 15:00:00");
    }

    #[test]
    fn a_rest_shift_ends_at_the_punch_out_not_a_far_away_next_rest() {
        // 1526 陣内 / 2026-03-02 の実形。次の休息は 4 日先 (次の運行) で、その間に
        // 終業打刻がある。**早い方で閉じる**。次の休息まで伸ばすと 24 時間超の勤務に
        // なり、後続の打刻由来の勤務と重なって `merge_shifts` に丸ごと捨てられる —
        // 本番で 745 分が消えていた原因がこれ
        let rows = vec![
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
            tc("2026-03-03 05:57:00", "終業"),
            ev("2026-03-06 22:00:00", "2026-03-07 06:00:00", "休息"),
            tc("2026-03-06 06:30:00", "始業"),
            tc("2026-03-06 20:35:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        let d = days
            .iter()
            .find(|d| d.start.contains("03-02 17:32"))
            .unwrap();
        assert_eq!(d.end, "2026-03-03 05:57:00");
        assert_eq!(d.restraint_minutes, 745);
        // 03-06 の打刻由来の勤務は残る (休息由来に食われない)
        assert!(days.iter().any(|d| d.source == ShiftSource::Timecard));
    }

    #[test]
    fn the_last_rest_stays_dropped_without_a_punch_out() {
        // 終業打刻が無ければ従来どおり捨てる — 運行終了では閉じない (型D は別判断)
        let rows = vec![
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
            dtako("2026-03-03 05:49:00", "運行終了"),
        ];
        assert!(daily_summary(&rows, "2026-03", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn a_punch_out_before_the_last_rest_does_not_close_it() {
        // 休息より前の終業打刻では閉じない (負の勤務を作らない)
        let rows = vec![
            tc("2026-03-02 03:00:00", "終業"),
            ev("2026-03-02 04:54:00", "2026-03-02 17:32:00", "休息"),
        ];
        assert!(daily_summary(&rows, "2026-03", &KosokuParams::default()).is_empty());
    }

    // --- 全列同一の行を落とす (Refs nuxt-dtako-admin#501) ---

    #[test]
    fn duplicate_rows_are_dropped_and_counted_by_date() {
        // 取り込みが 2 回走った形 (実測 1732 / 2026-07-16)
        let rows = vec![
            ev("2026-07-16 04:04:36", "2026-07-16 04:58:24", "一般道空車"),
            ev("2026-07-16 04:04:36", "2026-07-16 04:58:24", "一般道空車"),
            tc("2026-07-17 06:00:00", "始業"),
        ];
        let (kept, dropped) = drop_duplicate_rows(rows);
        assert_eq!(kept.len(), 2);
        assert_eq!(dropped.get("2026-07-16"), Some(&1));
        assert_eq!(dropped.len(), 1);
    }

    #[test]
    fn rows_that_differ_anywhere_are_kept() {
        // 同じ時刻・同じイベントでも運行NO が違えば別物 (実測 1526 の ...011 / ...012)
        let a = serde_json::json!({"datetime": "2026-03-01 00:49:24", "end_datetime": null,
            "source": "dtako", "state": "休息", "unko_no": "26022506251200000023011"});
        let b = serde_json::json!({"datetime": "2026-03-01 00:49:24", "end_datetime": null,
            "source": "dtako", "state": "休息", "unko_no": "26022506251200000023012"});
        let (kept, dropped) = drop_duplicate_rows(vec![a, b]);
        assert_eq!(kept.len(), 2);
        assert!(dropped.is_empty());
    }

    #[test]
    fn dropping_duplicates_on_an_empty_list() {
        let (kept, dropped) = drop_duplicate_rows(Vec::new());
        assert!(kept.is_empty());
        assert!(dropped.is_empty());
    }

    // --- 勤務の中に残った休息を外す (ユーザー決定 2026-07-28、Refs nuxt-dtako-admin#501) ---

    #[test]
    fn a_rest_inside_a_night_shift_leaves_restraint_working_and_night() {
        // 乗務員 1194 陣野 / 2026-03-11 そのもの。紙は 684 分、こちらは 1089 分だった
        let rows = vec![
            tc("2026-03-10 22:43:00", "始業"),
            ev("2026-03-11 00:42:00", "2026-03-11 07:24:00", "休息"), // 402 分
            ev("2026-03-11 07:26:00", "2026-03-11 08:29:00", "休憩"), // 63 分
            ev("2026-03-11 10:33:00", "2026-03-11 11:09:00", "休憩"), // 36 分
            tc("2026-03-11 16:52:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        let d = &days[0];
        assert_eq!(d.rest_minus_minutes, 402);
        assert_eq!(d.restraint_minutes, 687); // 1089 − 402
        assert_eq!(d.break_minutes, 99); // 休憩イベント 63 + 36 のまま
        assert_eq!(d.working_minutes, 588); // 拘束 − 休憩
        assert_eq!(d.restraint_minutes, d.working_minutes + d.break_minutes);
        // 深夜も休息のぶんだけ減る (22:43→24:00 の 77 分 + 00:00→00:42 の 42 分)
        assert_eq!(d.night_minutes + d.overtime_night_minutes, 119);
    }

    #[test]
    fn a_rest_is_taken_off_each_calendar_day_it_covers() {
        // 暦日按分の内訳からも外れる — 勤務の端から端で配ると休息が戻ってしまう
        let rows = vec![
            tc("2026-03-10 22:43:00", "始業"),
            ev("2026-03-11 00:42:00", "2026-03-11 07:24:00", "休息"),
            ev("2026-03-11 07:26:00", "2026-03-11 08:29:00", "休憩"),
            tc("2026-03-11 16:52:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        let parts = &days[0].parts;
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].restraint_minutes, 77); // 03-10 22:43→24:00
        assert_eq!(parts[1].restraint_minutes, 610); // 03-11 00:00→00:42 + 07:24→16:52
        let total: i64 = parts.iter().map(|p| p.restraint_minutes).sum();
        assert_eq!(total, days[0].restraint_minutes);
    }

    #[test]
    fn a_break_inside_a_rest_is_not_deducted_twice() {
        // 休息の中に休憩が入っていても、引くのは休息の 1 回だけ
        let rows = vec![
            tc("2026-03-02 06:00:00", "始業"),
            ev("2026-03-02 10:00:00", "2026-03-02 14:00:00", "休息"), // 240 分
            ev("2026-03-02 12:00:00", "2026-03-02 13:00:00", "休憩"), // 休息の中
            tc("2026-03-02 20:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days[0].rest_minus_minutes, 240);
        assert_eq!(days[0].restraint_minutes, 600); // 840 − 240
        assert_eq!(days[0].break_minutes, 0);
        assert_eq!(days[0].working_minutes, 600);
    }

    #[test]
    fn a_point_rest_without_a_span_is_not_deducted() {
        // `dtako` 側の休息は点イベントで長さを持たない — 引く根拠が無い
        let rows = vec![
            tc("2026-03-02 06:00:00", "始業"),
            dtako("2026-03-02 12:00:00", "休息"),
            ev("2026-03-02 09:00:00", "2026-03-02 09:30:00", "運転"),
            tc("2026-03-02 20:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-03", &KosokuParams::default());
        assert_eq!(days[0].rest_minus_minutes, 0);
        assert_eq!(days[0].restraint_minutes, 840);
    }

    // --- 終業の無い始業 / 月の打刻 (Refs #137) ---

    #[test]
    fn an_unpaired_punch_in_starts_a_shift_that_ends_at_the_next_rest() {
        // 乗務員 1021 / 2026-04-17 の形 — 始業打刻の相手が翌月まで無く、17 日の行が
        // 丸ごと空になっていた
        let rows = vec![
            tc("2026-06-17 06:46:00", "始業"),
            ev("2026-06-17 16:00:00", "2026-06-18 05:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].start, "2026-06-17 06:46:00");
        assert_eq!(days[0].end, "2026-06-17 16:00:00");
        assert_eq!(days[0].source, ShiftSource::Timecard);
        assert_eq!(days[0].punches.len(), 1);
    }

    #[test]
    fn an_unpaired_punch_in_without_a_rest_is_dropped() {
        // 終わりを決める手がかりが無ければ従来どおり捨てる
        let rows = vec![tc("2026-06-17 06:46:00", "始業")];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn two_punch_ins_in_a_row_close_the_first_at_the_next_rest() {
        let rows = vec![
            tc("2026-06-17 06:46:00", "始業"),
            ev("2026-06-17 16:00:00", "2026-06-18 05:00:00", "休息"),
            tc("2026-06-18 06:00:00", "始業"),
            tc("2026-06-18 18:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 2);
        assert_eq!(days[0].end, "2026-06-17 16:00:00");
        assert_eq!(days[1].start, "2026-06-18 06:00:00");
        assert_eq!(days[1].end, "2026-06-18 18:00:00");
    }

    #[test]
    fn month_punches_returns_every_punch_in_the_month() {
        // 勤務に紐づかない打刻も返す (対になる終業が無い始業を表から消さない)
        let rows = vec![
            tc("2026-06-03 06:33:38", "始業"),
            tc("2026-06-09 15:16:02", "終業"),
            tc("2026-06-17 06:46:04", "始業"),
            tc("2026-07-01 06:10:07", "始業"),
            dtako("2026-06-05 08:00:00", "運行開始"),
        ];
        let p = month_punches(&rows, "2026-06");
        assert_eq!(p.len(), 3);
        assert_eq!(p[0].at, "2026-06-03 06:33:38");
        assert_eq!(p[2].at, "2026-06-17 06:46:04");
        // 打刻以外は入れない / 翌月は入れない
        assert!(p.iter().all(|x| x.state == "始業" || x.state == "終業"));
    }

    // --- 24 時間超は最後の運行終了で終わらせる (Refs #135) ---

    #[test]
    fn a_rest_shift_ends_at_the_last_run_end() {
        // 乗務員 1021 / 2026-04-28 の形 — 運行終了して帰宅したが、次の運行まで休息が
        // 無いので勤務が終わらず 24 時間で打ち切られていた
        let rows = vec![
            ev("2026-06-27 16:13:00", "2026-06-28 06:47:00", "休息"),
            dtako("2026-06-28 08:14:00", "運行終了"),
            // 次の休息は 3 日後 (その間は帰宅していて休息イベントが出ない)
            ev("2026-07-01 16:00:00", "2026-07-02 06:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].start, "2026-06-28 06:47:00");
        assert_eq!(days[0].end, "2026-06-28 08:14:00");
        assert!(!days[0].over_24h);
    }

    #[test]
    fn only_the_last_run_end_cuts_a_long_shift() {
        // 途中の継ぎ目では切らない (#123) — 切るのは最後の運行終了だけ
        let rows = vec![
            ev("2026-06-27 16:13:00", "2026-06-28 06:47:00", "休息"),
            dtako("2026-06-28 10:00:00", "運行終了"),
            dtako("2026-06-28 10:30:00", "運行開始"),
            dtako("2026-06-29 04:00:00", "運行終了"),
            ev("2026-07-01 16:00:00", "2026-07-02 06:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].end, "2026-06-29 04:00:00");
        assert!(!days[0].over_24h);
    }

    #[test]
    fn a_long_shift_splits_at_every_long_run_gap() {
        // 乗務員 1108 / 2026-04 の形 — 帰宅を挟んで運行が 3 本続く。最後の運行終了
        // (= 6/14 10:48) で切るだけでは 3 日超が残り、24 時間打ち切りに戻っていた
        let rows = vec![
            ev("2026-06-10 20:00:00", "2026-06-11 03:11:00", "休息"),
            dtako("2026-06-11 09:28:00", "運行終了"),
            // 同じ帰宅の中にもう 1 本運行終了があっても、割る所は増えない
            dtako("2026-06-11 10:00:00", "運行終了"),
            dtako("2026-06-13 07:31:00", "運行開始"),
            dtako("2026-06-13 12:10:00", "運行終了"),
            dtako("2026-06-14 07:13:00", "運行開始"),
            dtako("2026-06-14 10:48:00", "運行終了"),
            ev("2026-06-16 19:05:00", "2026-06-17 06:13:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(
            days.iter()
                .map(|d| (d.start.as_str(), d.end.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("2026-06-11 03:11:00", "2026-06-11 09:28:00"),
                ("2026-06-13 07:31:00", "2026-06-13 12:10:00"),
                // 最後の運行終了より後は帰宅している — 次の休息までは勤務にしない
                ("2026-06-14 07:13:00", "2026-06-14 10:48:00"),
            ],
        );
        assert!(days.iter().all(|d| !d.over_24h));
        assert_eq!(days[0].restraint_minutes, 6 * 60 + 17);
    }

    #[test]
    fn a_long_shift_is_not_split_at_a_short_run_gap() {
        // 継ぎ目 (実測 4〜112 分) では割らない — 8 時間未満は同じ勤務の続き (#123)
        let rows = vec![
            ev("2026-06-10 20:00:00", "2026-06-11 03:00:00", "休息"),
            dtako("2026-06-11 12:00:00", "運行終了"),
            dtako("2026-06-11 19:00:00", "運行開始"), // 7 時間空き = 閾値未満
            dtako("2026-06-12 20:00:00", "運行終了"),
            ev("2026-06-14 19:00:00", "2026-06-15 06:00:00", "休息"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days.len(), 1);
        assert_eq!(days[0].start, "2026-06-11 03:00:00");
        // 割る根拠が無いまま 24 時間を超えた = 7 時間しか空けずに続けた本物の違反。
        // 打ち切らないので、勤務は**最後の運行終了**まで伸びる (その先に運行開始が
        // 無いので `split_by_run_gaps` がそこで閉じる)
        assert_eq!(days[0].end, "2026-06-12 20:00:00");
        assert!(days[0].over_24h);
    }

    #[test]
    fn a_long_shift_without_a_run_end_keeps_its_real_length() {
        // 運行終了が無ければ切る根拠が無い。それでも打ち切らない
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert!(days[0].over_24h);
        assert_eq!(days[0].restraint_minutes, 2288);
    }

    #[test]
    fn a_normal_shift_is_not_cut_at_a_run_end() {
        // 24 時間以内は運行終了で切らない (#123 の「運行では切らない」を崩さない)
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            dtako("2026-06-02 14:00:00", "運行終了"),
            tc("2026-06-02 20:00:00", "終業"),
        ];
        let days = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(days[0].end, "2026-06-02 20:00:00");
    }

    // --- 暦日按分の内訳 (Refs #130) ---

    #[test]
    fn parts_split_an_overnight_shift_by_calendar_day() {
        let rows = vec![
            tc("2026-06-02 22:00:00", "始業"),
            tc("2026-06-03 08:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.parts.len(), 2);
        assert_eq!(d.parts[0].date, "2026-06-02");
        assert_eq!(d.parts[0].restraint_minutes, 120); // 22:00〜24:00
        assert_eq!(d.parts[1].date, "2026-06-03");
        assert_eq!(d.parts[1].restraint_minutes, 480); // 0:00〜8:00
                                                       // 内訳の合計は行の値と一致する (月合計は寄せ方によらない)
        assert_eq!(
            d.parts.iter().map(|p| p.restraint_minutes).sum::<i64>(),
            d.restraint_minutes
        );
        assert_eq!(
            d.parts.iter().map(|p| p.working_minutes).sum::<i64>(),
            d.working_minutes
        );
        assert_eq!(
            d.parts.iter().map(|p| p.night_minutes).sum::<i64>(),
            d.night_minutes
        );
    }

    #[test]
    fn parts_put_night_minutes_on_the_day_they_fall() {
        let rows = vec![
            tc("2026-06-02 22:00:00", "始業"),
            tc("2026-06-03 08:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        // 拘束 600 分。昼の窓に掛からないので休憩 60 分がまん中 (02:30-03:30) に入る
        // 深夜は 22:00〜24:00 (120 分) と 0:00〜5:00 の 300 分 − 休憩 60 分 = 240 分
        assert_eq!(d.parts[0].night_minutes, 120);
        assert_eq!(d.parts[1].night_minutes, 240);
        // 時間外は経過実働 8h 超 = 6/3 07:00 以降なので翌日側にだけ乗る
        assert_eq!(d.parts[0].overtime_minutes, 0);
        assert_eq!(d.parts[1].overtime_minutes, 60);
    }

    #[test]
    fn parts_are_empty_for_a_same_day_shift() {
        // 内訳が行そのものになるので出さない (応答を膨らませない)
        let rows = vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 18:00:00", "終業"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default())[0]
            .parts
            .is_empty());
    }

    #[test]
    fn parts_cover_a_legal_holiday_that_spills_into_monday() {
        let rows = vec![
            tc("2026-06-07 20:00:00", "始業"), // 日曜
            tc("2026-06-08 04:00:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.is_legal_holiday);
        // 拘束 480 分。昼の窓に掛からないので休憩 60 分がまん中 (23:30-00:30) に入り、
        // 月曜側の実働は 00:30〜04:00 の 210 分 (すべて深夜帯)
        // 法定休日の判定は勤務単位 (始業日) — 月曜へこぼれた分も法定休日のまま
        assert_eq!(d.parts[1].legal_holiday_minutes, 210);
        assert_eq!(d.parts[1].legal_holiday_night_minutes, 210);
        assert_eq!(
            d.parts.iter().map(|p| p.legal_holiday_minutes).sum::<i64>(),
            d.legal_holiday_minutes
        );
    }

    #[test]
    fn parts_cover_the_whole_over_24h_shift() {
        // 24 時間超の勤務も、暦日の内訳は全区間ぶん配る (打ち切らないので端が欠けない)
        let rows = vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert!(d.over_24h);
        assert_eq!(
            d.parts.iter().map(|p| p.restraint_minutes).sum::<i64>(),
            2288
        );
        assert_eq!(d.parts.last().unwrap().date, "2026-06-03");
    }

    #[test]
    fn split_by_date_cuts_at_midnight() {
        let v = split_by_date(dt("2026-06-02 22:00:00"), dt("2026-06-04 08:00:00"));
        assert_eq!(
            v,
            vec![
                (chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap(), 120),
                (chrono::NaiveDate::from_ymd_opt(2026, 6, 3).unwrap(), 1440),
                (chrono::NaiveDate::from_ymd_opt(2026, 6, 4).unwrap(), 480),
            ]
        );
        // 同じ日で終わる区間は 1 つだけ
        assert_eq!(
            split_by_date(dt("2026-06-02 09:00:00"), dt("2026-06-02 18:00:00")).len(),
            1
        );
        // 空区間は何も返さない
        assert!(split_by_date(dt("2026-06-02 09:00:00"), dt("2026-06-02 09:00:00")).is_empty());
    }

    #[test]
    fn day_part_traits_are_wired() {
        let p = DayPart::new(chrono::NaiveDate::from_ymd_opt(2026, 6, 2).unwrap());
        assert_eq!(p.clone(), p);
        assert!(format!("{p:?}").contains("2026-06-02"));
    }

    // --- 乗務員ごとの分割 (Refs #125) ---

    fn tc_of(driver: i64, datetime: &str, state: &str) -> serde_json::Value {
        json!({"datetime": datetime, "end_datetime": null, "driver_id": driver,
               "source": "timecard", "state": state})
    }

    #[test]
    fn split_by_driver_groups_and_sorts() {
        let rows = vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1018, "2026-06-02 09:25:00", "始業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
            tc_of(1018, "2026-06-02 19:39:00", "終業"),
        ];
        let split = split_by_driver(rows);
        assert_eq!(split.len(), 2);
        // 乗務員CD 昇順
        assert_eq!(split[0].0, 1018);
        assert_eq!(split[1].0, 1119);
        // 行の並びは入力のまま
        assert_eq!(split[0].1.len(), 2);
        assert_eq!(split[0].1[0]["datetime"], "2026-06-02 09:25:00");
        assert_eq!(split[1].1[0]["datetime"], "2026-06-02 06:00:00");
    }

    #[test]
    fn split_by_driver_drops_rows_without_a_driver() {
        // 誰のものか決められない行を混ぜると、その人の拘束が静かに伸びる
        let rows = vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            json!({"datetime": "2026-06-02 07:00:00", "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 08:00:00", "driver_id": null,
                   "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 09:00:00", "driver_id": "1119",
                   "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-06-02 10:00:00", "driver_id": -1,
                   "source": "timecard", "state": "始業"}),
        ];
        let split = split_by_driver(rows);
        assert_eq!(split.len(), 1);
        assert_eq!(split[0].0, 1119);
        assert_eq!(split[0].1.len(), 1);
    }

    #[test]
    fn split_by_driver_keeps_shifts_apart() {
        // 混ざったまま畳むと、他人の休息で勤務が切れる
        let rows = vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1018, "2026-06-02 07:00:00", "始業"),
            tc_of(1018, "2026-06-02 12:00:00", "終業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
        ];
        let p = KosokuParams::default();
        // 分けずに畳むと、後から来た始業が前の始業を上書きし、1119 の 06:00 が消えて
        // 1018 の 07:00〜12:00 だけが 1 勤務として残る (1119 の拘束が丸ごと落ちる)
        let mixed = daily_summary(&rows.clone(), "2026-06", &p);
        assert_eq!(mixed.len(), 1);
        assert_eq!(mixed[0].restraint_minutes, 300);
        // 分ければそれぞれの拘束が出る
        let split = split_by_driver(rows);
        let d1018 = daily_summary(&split[0].1, "2026-06", &p);
        let d1119 = daily_summary(&split[1].1, "2026-06", &p);
        assert_eq!(d1018[0].restraint_minutes, 300);
        assert_eq!(d1119[0].restraint_minutes, 720);
    }

    #[test]
    fn split_by_driver_on_empty_input() {
        assert!(split_by_driver(Vec::new()).is_empty());
    }

    // --- 月境界 ---

    #[test]
    fn shift_is_filed_under_its_start_date() {
        // 6/30 22:00 始業 → 7/1 08:00 終業。丸ごと 6 月に載る
        let rows = vec![
            tc("2026-06-30 22:00:00", "始業"),
            tc("2026-07-01 08:00:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].date, "2026-06-30");
        assert_eq!(d[0].restraint_minutes, 600);
        // 7 月として集計すると出てこない
        assert!(daily_summary(&rows, "2026-07", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn previous_month_shift_is_excluded() {
        let rows = vec![
            tc("2026-05-31 09:00:00", "始業"),
            tc("2026-05-31 18:00:00", "終業"),
            tc("2026-06-01 09:00:00", "始業"),
            tc("2026-06-01 18:00:00", "終業"),
        ];
        let d = daily_summary(&rows, "2026-06", &KosokuParams::default());
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].date, "2026-06-01");
    }

    #[test]
    fn seconds_are_floored_on_shift_bounds() {
        let rows = vec![
            tc("2026-06-02 09:25:41", "始業"),
            tc("2026-06-02 19:39:59", "終業"),
        ];
        let d = &daily_summary(&rows, "2026-06", &KosokuParams::default())[0];
        assert_eq!(d.start, "2026-06-02 09:25:00");
        assert_eq!(d.end, "2026-06-02 19:39:00");
        assert_eq!(d.restraint_minutes, 614);
    }

    #[test]
    fn sub_minute_shift_is_dropped() {
        let rows = vec![
            tc("2026-06-02 09:25:10", "始業"),
            tc("2026-06-02 09:25:50", "終業"),
        ];
        assert!(daily_summary(&rows, "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn empty_input_is_empty_output() {
        assert!(daily_summary(&[], "2026-06", &KosokuParams::default()).is_empty());
    }

    #[test]
    fn default_params_are_7h30_and_8h() {
        let p = KosokuParams::default();
        assert_eq!(p.break_threshold_minutes, 10);
        assert_eq!(p.prescribed_minutes, 450);
        assert_eq!(p.legal_minutes, 480);
    }

    #[test]
    fn shift_source_serializes_lowercase() {
        assert_eq!(
            serde_json::to_string(&ShiftSource::Timecard).unwrap(),
            "\"timecard\""
        );
        assert_eq!(
            serde_json::to_string(&ShiftSource::Rest).unwrap(),
            "\"rest\""
        );
    }

    #[test]
    fn event_and_shift_are_debug_and_comparable() {
        // 派生 trait を使う経路を 1 本通しておく (Debug / PartialEq / Clone)
        let e = Event {
            start: dt("2026-06-02 09:00:00"),
            end: None,
            source: "timecard".into(),
            state: "始業".into(),
        };
        assert_eq!(e.clone(), e);
        assert!(format!("{e:?}").contains("始業"));
        let s = Shift {
            start: dt("2026-06-02 09:00:00"),
            end: dt("2026-06-02 18:00:00"),
            source: ShiftSource::Timecard,
        };
        assert_eq!(s.clone(), s);
        assert!(format!("{s:?}").contains("Timecard"));
        let p = KosokuParams::default();
        assert!(format!("{:?}", p.clone()).contains("450"));
        let d = &daily_summary(
            &[
                tc("2026-06-02 09:00:00", "始業"),
                tc("2026-06-02 18:00:00", "終業"),
            ],
            "2026-06",
            &p,
        )[0];
        assert_eq!(d.clone(), *d);
        assert!(format!("{d:?}").contains("2026-06-02"));
    }
}
