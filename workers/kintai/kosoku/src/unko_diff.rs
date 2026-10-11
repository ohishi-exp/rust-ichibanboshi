//! **オンプレの運行 (押し込み済みの `kintai.kintai_events`) と GCP の運行 (alc の etags) の突合** (Refs #205 の 37・39)。
//!
//! root の `src/kintai_http_repo.rs` から移した (Refs ohishi-exp/rust-ichibanboshi#322)。月ゲートの流れ
//! ([`crate::fold_flow`]) が突合の warning で封を止めるので、流れと同じ crate に置く。純粋な計算だけで I/O は持たない。

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::NaiveDate;
use serde::Serialize;

use crate::fold_flow::Warning;
use crate::window::unko_no_start_date;

// ── 運行の突合 (オンプレ × GCP、Refs #205 の 37) ────────────────────────────

/// 応答に載せる `unko_diff` の上限。総数は [`UnkoDiff::total`] に別に返す
/// (`unsplit` / `unsplit_total` と同じ作法)。
pub const MAX_UNKO_DIFF: usize = 500;

/// **突合の左辺 — 「押し込み済みの運行」1 本** (Refs #205 の 37)。
///
/// ## 「オンプレの運行」ではない。射程はこれより狭い
///
/// 実体は Postgres の `kintai.kintai_events` で、**オンプレ MariaDB の写しでは
/// なく押し込み済みのぶんだけ**。**2 段のふるいが掛かっている**:
///
/// 1. **`source`** — `unko_no` を持つのは `dtako` (= `time_card_dtako`) 由来の行
///    だけ ([`crate::kintai_push::PUSHED_SOURCES`])。**`dtako_events` は決定 5 で
///    push していない** (R2 に永続化済みのため)。`timecard` は `unko_no` が NULL
///    なので落ちる
/// 2. **`state`** — [`crate::kintai_push::NOT_CARRIED_STATES`] (`休息`) は
///    **運ばないと決めてある**。開始 (20) と終了 (21) が同じ名前で来るため、
///    `dtako_events` を運ばないこの経路では読み分けられないから
///
/// **残るのは実質 `運行開始` / `運行終了` の点だけ**で、実測では 1 運行あたり
/// **1〜2 行**しかない (2026-06 の 5 乗務員 31 運行、`/api/kintai/events`)。
///
/// **`dtako_events` にしか出てこない運行はここに 1 本も現れない。** 同じ実測で
/// **8 本 (26%) が `time_card_dtako` に 1 行も持たず**、その 8 本すべてが乗務員
/// 1688 だった — **1688 の運行はこの突合の射程外**で、`unko_diff` に原理的に
/// 出ない。名前に引かれて「オンプレ全部と比べている」と読まないこと
/// (2026-07-31 に監督とユーザーが数ラウンド誤読した)。
///
/// **新しい口は作らない** — 突合の両側とも既に fold が読んでいるものだけで組む。
/// GCP 側の instance から MariaDB へは届かないので、射程を広げるなら別経路が要る。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnpremOperation {
    pub driver_cd: i64,
    pub unko_no: String,
    /// **`kintai_events` に押し込み済みの行**が覆う暦日 (JST) の最小 / 最大。
    ///
    /// **運行そのものの広がりではない。** 数えるのは `PUSHED_SOURCES` かつ
    /// [`crate::kintai_push::NOT_CARRIED_STATES`] を除いた行 = 実質
    /// `運行開始` / `運行終了` の点だけなので、**両端がそのまま出るのではなく
    /// 「記録されている端」しか出ない**。
    ///
    /// 実測 (1078 の `26062411…23021`): MariaDB には 91 行 / 06-24〜07-01 あるが、
    /// `source` を通るのは 18 行、そのうち 17 行が `休息` で運ばれず、**残るのは
    /// 06-24 11:40:06 の `運行開始` 1 行だけ** → `first`=`last`=06-24。
    /// **`運行終了` がまだ無い運行はこうなる。** これは欠けではなく仕様。
    /// **この値で運行の期間を判断しない** (2026-07-31 にそれで誤った)。
    pub first_date: NaiveDate,
    pub last_date: NaiveDate,
}

/// **オンプレに在って GCP (alc の etags) に無い運行** 1 件 (Refs #205 の 37)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnkoDiffItem {
    pub driver_cd: i64,
    pub unko_no: String,
    /// `unko_no` の先頭 6 桁 (`YYMMDD`) から読んだ運行開始日。読めなければ `null`。
    pub start_date: Option<String>,
    /// **押し込み済みの行**が覆う暦日 (JST) の範囲 ([`OnpremOperation::first_date`]
    /// の注意書き — 運行そのものの広がりではない)。
    pub first_date: String,
    pub last_date: String,
}

/// 突合が噛み合わないときに**両側の生の文字列を並べて見る**ための標本の件数
/// (Refs #205 の 37)。
///
/// 本番 2026-06 の初回実測で `gcp_only` が etags の item 総数 (1,130) と一致した
/// = **2 集合の重なりがゼロ**だった。データの欠落ではなく突合キーが一度も一致して
/// いないので、まず「どう違うか」を実物で見る。10 件あれば形は分かる。
pub const MAX_UNKO_DIFF_SAMPLE: usize = 10;

/// 突合キーの標本 1 件 (Refs #205 の 37)。
///
/// **`len` を添えるのは推測を挟まないため。** 桁が違うのか・前後に空白が付いて
/// いるのか・そもそも別物なのかは、生の文字列と長さを並べれば数えずに分かる
/// (JSON では前後の空白が読み飛ばされやすい)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnkoSample {
    pub unko_no: String,
    /// 文字数 (前後の空白も数える)。
    pub len: usize,
}

impl UnkoSample {
    fn new(unko_no: &str) -> Self {
        Self {
            unko_no: unko_no.to_string(),
            len: unko_no.chars().count(),
        }
    }
}

/// 突合キーの**形**の実測 (Refs #205 の 37)。片側 10 件の標本では「たまたまその
/// 10 件がそうだった」を排除できないので、月ぶん全件の分布を返す。
///
/// - `len_counts` … **桁が固定かどうか。** 先頭 N 桁で比較してよいかは、桁が
///   揃っていて初めて言える (`unko_no` の末尾の長さは可変という実測メモがある)
/// - `last_char_counts` … 余分な 1 文字が何なのか。全部同じ値なら定数、
///   複数の値を取るなら**意味を持つ桁**なので落としてよいか別途考える必要がある
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UnkoShape {
    pub len_counts: BTreeMap<usize, usize>,
    pub last_char_counts: BTreeMap<String, usize>,
}

impl UnkoShape {
    fn measure<'a>(keys: impl Iterator<Item = &'a str>) -> Self {
        let mut out = Self::default();
        for k in keys {
            *out.len_counts.entry(k.chars().count()).or_default() += 1;
            if let Some(c) = k.chars().last() {
                *out.last_char_counts.entry(c.to_string()).or_default() += 1;
            }
        }
        out
    }
}

/// 突合キーの候補と、その候補で切り替えたときに何が起きるかの**試算**
/// (Refs #205 の 37)。**突合そのものは `raw` のまま — 試算は判定に一切入らない。**
///
/// 「先頭 22 桁で本当に重なるか」は推測ではなく数で答えるべきなので、候補ごとに
/// 一致数を出して並べる。
///
/// ## `collisions` は「運行が消える」ことの証明にはならない
///
/// オンプレの 23 桁目は**対象CD** (その運行の何人目の乗務員か。1 = 運転手 /
/// 2 = 副運転手) で、GCP 側の 22 桁は運行NO そのもの (ユーザー、2026-07-31)。
/// つまり `...1` と `...2` は**同じ 1 本の運行**なので、先頭 22 桁で 1 つに
/// なるのは**正しい畳み方**であって情報の欠落ではない。
///
/// **実測でも裏が取れている** (`/api/kintai/events`、1740 / 2026-06-11):
/// `...39752` は独立した運行ではなく `...39751` の**部分集合**だった —
/// 行は休息 3 件だけで、3 件とも `...39751` 側にも同じ時刻で在り、
/// `運行開始` も `dtako_events` も 1 行も持たない。
///
/// **`collisions` が数えるのは「同じ乗務員・同じ正規化キーに複数の生の値がある」
/// ことだけ**で、それが対象CD 違いなのか本当に別の運行なのかは**キーだけでは
/// 区別できない**。0 でなければ中身を見に行く合図、という位置づけ。
/// (2026-07-31 に「枝番だから潰すと運行が消える」と読んで報告したが、
/// イベント列まで見て自分で否定した)
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UnkoKeyTrial {
    /// 候補の名前 ([`KEY_CANDIDATES`])。
    pub key: String,
    /// その候補でオンプレ側が GCP 側と一致した件数。
    pub matched: usize,
    /// 一致しなかった件数 (= その候補を採ったときの `unko_diff_total`)。
    pub unmatched: usize,
    /// 逆方向 (GCP 側にあってオンプレ側に無い) のキー数。
    pub gcp_only: usize,
    /// **同じ乗務員・同じ正規化キーに複数の生の `unko_no` がある件数** (struct docs)。
    /// 対象CD 違い (= 同じ運行) なら畳んで正しい。0 でなければ中身を見る合図。
    pub collisions: usize,
}

/// **逆方向 (GCP にしか無い運行) を分けて数えるための材料** (Refs #205 の 39)。
///
/// 突合そのものには一切入らない — [`UnkoDiff::gcp_only_in_month`] を乗務員別に
/// 割るためだけに要る。**両方とも「無ければ空」で通す**: alc が `driver_cds` を
/// 返さない環境や、`onprem_ever` を引けなかった回でも突合は今までどおり動く。
#[derive(Debug, Clone, Default)]
pub struct UnkoDiffAux {
    /// etags の `unko_no` → `driver_cds` (`ippoan/rust-alc-api#587`)。
    /// alc が返さない環境では空 = 乗務員別の内訳が出ない。
    pub gcp_drivers: HashMap<String, Vec<String>>,
    /// **押し込み済み (`kintai.kintai_events`) に `unko_no` 付きの行を
    /// 「一度でも」持ったことがある乗務員CD**
    /// (root の `kintai_push::OPERATION_DRIVER_CDS_SQL`)。
    ///
    /// 「一度でも」の範囲は**押し込み済みのぶんだけ**で、オンプレ MariaDB の
    /// 全履歴ではない。push していない月のことは分からない。
    pub onprem_ever: HashSet<i64>,
}

/// 逆方向の 1 乗務員ぶん (Refs #205 の 39)。**この 3 つを並べると
/// 「射程外 (A)」と「欠落 (B)」が分かれる** — [`UnkoDiffDriverSplit`]。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UnkoDiffDriver {
    /// etags が返した生の乗務員CD 文字列。
    pub driver_cd: String,
    /// **対象月に始まった、GCP にしか無い運行**の数。
    pub gcp_only: usize,
    /// 同じ月に押し込み済みへ在る運行の数 (`MONTH_OPERATIONS_SQL` の行数)。
    pub onprem_in_month: usize,
    /// 押し込み済みに `unko_no` 付きの行を一度でも持ったか
    /// ([`UnkoDiffAux::onprem_ever`])。
    pub onprem_ever: bool,
}

/// **「射程外」と「欠落」を分ける 3 つの桶** (Refs #205 の 39)。
///
/// | 桶 | 形 | 読み |
/// |---|---|---|
/// | `never_onprem` | `time_card_dtako` を一度も持ったことがない乗務員 | **射程外 (A)** — 打刻機と連動していない。`unko_diff` に原理的に出ない |
/// | `other_month_only` | 過去には持っていたのに**対象月だけ 0 件** | **欠落 (B) が最も濃い** — 本来出るはずのものが落ちている |
/// | `also_in_month` | 同じ月に押し込み済みの運行も在る乗務員 | **一部だけ欠けている (B)** — 全部落ちているわけではない |
///
/// **`drivers` は乗務員数、`ops` は運行数** (2 名乗務は 1 運行が 2 人に乗るので
/// `ops` の合計は [`UnkoDiff::gcp_only_in_month`] を超え得る)。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UnkoDiffDriverSplit {
    pub never_onprem_drivers: usize,
    pub never_onprem_ops: usize,
    pub other_month_only_drivers: usize,
    pub other_month_only_ops: usize,
    pub also_in_month_drivers: usize,
    pub also_in_month_ops: usize,
}

impl UnkoDiffDriverSplit {
    /// 乗務員別の表から 3 つの桶へ畳む。
    fn measure(rows: &[UnkoDiffDriver]) -> Self {
        let mut out = Self::default();
        for r in rows {
            let (d, o) = match (r.onprem_in_month > 0, r.onprem_ever) {
                (true, _) => (&mut out.also_in_month_drivers, &mut out.also_in_month_ops),
                (false, true) => (
                    &mut out.other_month_only_drivers,
                    &mut out.other_month_only_ops,
                ),
                (false, false) => (&mut out.never_onprem_drivers, &mut out.never_onprem_ops),
            };
            *d += 1;
            *o += r.gcp_only;
        }
        out
    }
}

/// 応答に載せる乗務員別の内訳の上限 (Refs #205 の 39)。乗務員は実測 137 名なので
/// 通常は切られない — 桁違いの値が来たときに応答を膨らませないための蓋。
pub const MAX_UNKO_DIFF_DRIVERS: usize = 300;

/// 試算する突合キーの候補。`None` = そのまま、`Some(n)` = 先頭 n 文字。
///
/// `22` は「オンプレ 23 桁 − 余分な 1 文字」、`12` は `YYMMDDHHMMSS`
/// (乗務員 47 名 × 4 か月 / 966 運行で不一致 0・パース不能 0 の実測がある)。
const KEY_CANDIDATES: [(&str, Option<usize>); 3] = [
    ("raw", None),
    ("prefix22", Some(22)),
    ("prefix12", Some(12)),
];

/// 候補キーへ写す。短すぎる値はそのまま返す (切らない = 一致しないだけ)。
fn normalize_key(unko_no: &str, prefix: Option<usize>) -> &str {
    match prefix {
        None => unko_no,
        Some(n) => unko_no.get(..n).unwrap_or(unko_no),
    }
}

/// 候補 1 つぶんの試算。
fn key_trial(
    name: &str,
    prefix: Option<usize>,
    onprem: &[OnpremOperation],
    gcp: &HashSet<String>,
) -> UnkoKeyTrial {
    let theirs: HashSet<&str> = gcp.iter().map(|u| normalize_key(u, prefix)).collect();
    let matched = onprem
        .iter()
        .filter(|o| theirs.contains(normalize_key(&o.unko_no, prefix)))
        .count();
    let mine: HashSet<&str> = onprem
        .iter()
        .map(|o| normalize_key(&o.unko_no, prefix))
        .collect();
    let gcp_only = theirs.iter().filter(|k| !mine.contains(*k)).count();
    let mut grouped: std::collections::HashMap<(i64, &str), HashSet<&str>> =
        std::collections::HashMap::new();
    for o in onprem {
        grouped
            .entry((o.driver_cd, normalize_key(&o.unko_no, prefix)))
            .or_default()
            .insert(o.unko_no.as_str());
    }
    let collisions = grouped
        .values()
        .filter(|v| v.len() > 1)
        .map(HashSet::len)
        .sum();
    UnkoKeyTrial {
        key: name.to_string(),
        matched,
        unmatched: onprem.len() - matched,
        gcp_only,
        collisions,
    }
}

/// **オンプレの `unko_no` の末尾に付いている対象CD の桁数** (Refs #205 の 37)。
///
/// オンプレ (MariaDB `time_card_dtako`) は **運行NO + 対象CD** を key にしている
/// (ユーザー、2026-07-31)。GCP (alc) 側は `運行NO` と `対象乗務員区分` を**別の列**で
/// 持つので `unko_no` は運行NO そのもの — `ippoan/rust-alc-api` の
/// `crates/alc-csv-parser/src/kudguri.rs:74` (`運行NO`) と `:92` (`対象乗務員区分`)、
/// `migrations/054_dtako_tables.sql:53-54` の 2 列 + `:88` の
/// `UNIQUE(tenant_id, unko_no, crew_role)`。**alc 側に切り詰めは存在しない**
/// (`unko_no[..]` / `truncate` / `chars().take` の grep がヒット 0)。
///
/// 本番実測でも両側の桁は一様 (オンプレ 683 件すべて 23 / GCP 1,130 件すべて 22)。
const ONPREM_CREW_SUFFIX_LEN: usize = 1;

/// **オンプレの key から対象CD を落として運行NO にする** (Refs #205 の 37)。
///
/// 「先頭 22 桁」ではなく「末尾の対象CD ぶんを落とす」と書くのは、22 が
/// どこから来た数字か読めるようにするため。短すぎる値は落とさずそのまま返す
/// (一致しないだけで、静かに空文字にはしない)。
fn onprem_unko_no(unko_no: &str) -> &str {
    let kept = unko_no
        .chars()
        .count()
        .saturating_sub(ONPREM_CREW_SUFFIX_LEN);
    if kept == 0 {
        return unko_no;
    }
    let cut: usize = unko_no.chars().take(kept).map(char::len_utf8).sum();
    &unko_no[..cut]
}

/// `unko_no` を `YYYY-MM` の桶へ。読めなければ `"?"`。
fn unko_no_month(unko_no: &str) -> String {
    match unko_no_start_date(unko_no) {
        Some(d) => d.format("%Y-%m").to_string(),
        None => "?".to_string(),
    }
}

/// 運行の突合の結果 (Refs #205 の 37)。**判定には使わない — 応答に載せるだけ。**
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct UnkoDiff {
    /// オンプレに在って GCP に無い運行 (先頭 [`MAX_UNKO_DIFF`] 件)。
    ///
    /// **並びはオンプレ側の `ORDER BY driver_cd, unko_no` のまま**なので、
    /// 切られるのは**乗務員CD の大きい側**。`total` がこれを超えているときに
    /// 「ある乗務員が 1 件も出ていない」のは、欠けていないのではなく**切られた**
    /// 可能性がある (本番 2026-06 の 683 件で 1688 が 0 件に見えたのがこれ)。
    pub items: Vec<UnkoDiffItem>,
    /// 切る前の総数。
    pub total: usize,
    /// **逆方向** (GCP に在ってオンプレに無い) の運行数。**窓ぜんたい**で数えた値。
    ///
    /// **これは大きく出るのが正常。** GCP 側の etags は**読取日**で引くので窓の中に
    /// 「ずっと前に始まった運行」が混ざる (本番 2026-06 の実測で `unko_no` の開始日が
    /// 2026-04-20..2026-07-02)。一方オンプレ側は対象月のイベントしか持たないので、
    /// 月の外の運行はすべてここに落ちる。**異常かどうかは
    /// [`Self::gcp_only_in_month`] で見ること。**
    pub gcp_only: usize,
    /// **対象月に始まった運行だけに絞った逆方向。** ここが 0 でなければ
    /// 「GCP にしか無い対象月の運行」が実在する = 欠落とは逆向きの異常。
    pub gcp_only_in_month: usize,
    /// 逆方向を運行開始日の `YYYY-MM` で分けた件数。窓の差で説明が付くのか、
    /// 対象月そのものが余っているのかを 1 目で見るため。
    pub gcp_only_by_month: BTreeMap<String, usize>,
    /// **対象月に始まった逆方向を運行開始日 (`YYYY-MM-DD`) で割った件数**
    /// (Refs #205 の 39)。特定の日に固まっているのか月ぜんたいに散っているのかで
    /// 意味がまるで違うので数える。
    pub gcp_only_in_month_by_day: BTreeMap<String, usize>,
    /// **対象月に始まった逆方向の乗務員別の内訳** (Refs #205 の 39、
    /// [`UnkoDiffDriver`])。`gcp_only` の多い順、同数なら乗務員CD 順。
    /// alc が `driver_cds` を返さない環境では空。
    pub gcp_only_in_month_by_driver: Vec<UnkoDiffDriver>,
    /// **乗務員が引けなかった逆方向の件数。** `driver_cds` が空 (alc 未対応) だと
    /// [`Self::gcp_only_in_month`] がまるごとここに落ちる — 内訳が空なのが
    /// 「乗務員が居ない」ではなく「引けていない」ことを区別するため。
    pub gcp_only_in_month_unknown_driver: usize,
    /// 乗務員別の内訳を 3 つの桶へ畳んだもの ([`UnkoDiffDriverSplit`])。
    pub gcp_only_driver_split: UnkoDiffDriverSplit,
    /// 逆方向の実物 (辞書順の先頭 [`MAX_UNKO_DIFF_SAMPLE`] 件)。
    pub gcp_only_sample: Vec<UnkoSample>,
    /// **突き合わせた相手側**の実物 (オンプレの `unko_no`、同じく辞書順の先頭)。
    /// 片側だけ見ても違いは分からないので、必ず対で返す。
    pub onprem_sample: Vec<UnkoSample>,
    /// 両側の**形**の実測 ([`UnkoShape`]) — 標本 10 件では言い切れない部分。
    pub onprem_shape: UnkoShape,
    pub gcp_shape: UnkoShape,
    /// 突合キーの候補ごとの試算 ([`UnkoKeyTrial`])。突合そのものは
    /// [`onprem_unko_no`] (= `prefix22`) を採用済みなので、ここは**残る候補との
    /// 比較を続けられるようにするための観測**。
    pub trials: Vec<UnkoKeyTrial>,
}

/// **オンプレの `(乗務員CD, 運行NO)` と etags の `運行NO` を突き合わせる**
/// (Refs #205 の 37)。
///
/// GCP 側で畳んだ 2026-06 の勤怠がオンプレ基準より 143 行少ない件は、調査の結果
/// 「乗務員ごとに最後の 1 本の `運行NO` がまるごと GCP 側の運行一覧に無い」形だと
/// 分かっている。**どの運行が欠けているかを名指しできる**ようにするのがこの関数。
///
/// ## 2 つの集合を使い分ける
///
/// | 向き | 単位 | 理由 |
/// |---|---|---|
/// | 前 (オンプレ → GCP) | **`(乗務員CD, 運行NO)` のペア** | 2 名乗務は同じ運行NO が 2 人に紐づく。運行NO だけで畳むと**副運転手が消える** |
/// | 逆 (GCP → オンプレ) | **`{運行NO}` の集合** | etags は運行NO 単位に dedup 済み (`alc dtako_events.rs:530-531`)。ペアと比べると全件が逆方向に落ちる |
///
/// **正規化するのは運行NO の部分だけで、`driver_cd` は落とさない。**
///
/// 標本は両側とも辞書順の先頭 — 呼ぶたびに違う 10 件が出ると比較にならないため
/// (`HashSet` の反復順は不定)。
fn diff_unko(
    onprem: &[OnpremOperation],
    gcp: &HashSet<String>,
    aux: &UnkoDiffAux,
    month: Option<(i32, u32)>,
) -> UnkoDiff {
    let mut items = Vec::new();
    let mut total = 0;
    for op in onprem {
        // 判定は運行NO 部分だけ。2 名乗務なら 2 エントリとも同じ判定になり、2 件とも残る
        if gcp.contains(onprem_unko_no(&op.unko_no)) {
            continue;
        }
        total += 1;
        if items.len() < MAX_UNKO_DIFF {
            items.push(UnkoDiffItem {
                driver_cd: op.driver_cd,
                unko_no: op.unko_no.clone(),
                start_date: unko_no_start_date(&op.unko_no).map(|d| d.to_string()),
                first_date: op.first_date.to_string(),
                last_date: op.last_date.to_string(),
            });
        }
    }
    // 逆方向は運行NO の集合同士 (乗務員CD で膨らませない)
    let seen: HashSet<&str> = onprem.iter().map(|o| onprem_unko_no(&o.unko_no)).collect();
    let mut only: Vec<&str> = gcp
        .iter()
        .map(String::as_str)
        .filter(|u| !seen.contains(u))
        .collect();
    only.sort_unstable();
    let mut by_month: BTreeMap<String, usize> = BTreeMap::new();
    for u in &only {
        *by_month.entry(unko_no_month(u)).or_default() += 1;
    }
    // 開始日を読めたものだけが残るので、日別 (Refs #205 の 39) は `?` を持たない
    let only_in_month: Vec<(&str, NaiveDate)> = match month {
        Some((y, m)) => only
            .iter()
            .filter_map(|u| unko_no_start_date(u).map(|d| (*u, d)))
            .filter(|(_, d)| in_month(*d, y, m))
            .collect(),
        None => Vec::new(),
    };
    let mut by_day: BTreeMap<String, usize> = BTreeMap::new();
    for (_, d) in &only_in_month {
        *by_day.entry(d.to_string()).or_default() += 1;
    }
    let (by_driver, unknown_driver) = gcp_only_by_driver(&only_in_month, onprem, aux);
    let mut mine: Vec<&str> = seen.into_iter().collect();
    mine.sort_unstable();
    UnkoDiff {
        items,
        total,
        gcp_only: only.len(),
        gcp_only_in_month: only_in_month.len(),
        gcp_only_by_month: by_month,
        gcp_only_in_month_by_day: by_day,
        gcp_only_driver_split: UnkoDiffDriverSplit::measure(&by_driver),
        gcp_only_in_month_by_driver: by_driver,
        gcp_only_in_month_unknown_driver: unknown_driver,
        gcp_only_sample: sample(&only),
        onprem_sample: sample(&mine),
        onprem_shape: UnkoShape::measure(onprem.iter().map(|o| o.unko_no.as_str())),
        gcp_shape: UnkoShape::measure(gcp.iter().map(String::as_str)),
        trials: KEY_CANDIDATES
            .iter()
            .map(|(name, prefix)| key_trial(name, *prefix, onprem, gcp))
            .collect(),
    }
}

/// **対象月の逆方向を乗務員別に割る** (Refs #205 の 39)。返すのは
/// `(乗務員別の表, 乗務員が引けなかった件数)`。
///
/// **2 名乗務の 1 運行は 2 人とも 1 件ずつ数える** — 「その乗務員から見て何本
/// 見えていないか」が知りたい値なので、運行数へ畳むと副運転手が消える。
/// そのぶん `gcp_only` の合計は [`UnkoDiff::gcp_only_in_month`] を超え得る。
fn gcp_only_by_driver(
    only_in_month: &[(&str, NaiveDate)],
    onprem: &[OnpremOperation],
    aux: &UnkoDiffAux,
) -> (Vec<UnkoDiffDriver>, usize) {
    let mut per: BTreeMap<&str, usize> = BTreeMap::new();
    let mut unknown = 0;
    for (u, _) in only_in_month {
        match aux.gcp_drivers.get(*u) {
            Some(ds) if !ds.is_empty() => {
                for d in ds {
                    *per.entry(d.as_str()).or_default() += 1;
                }
            }
            _ => unknown += 1,
        }
    }
    let mut in_month_ops: HashMap<i64, usize> = HashMap::new();
    for o in onprem {
        *in_month_ops.entry(o.driver_cd).or_default() += 1;
    }
    let mut rows: Vec<UnkoDiffDriver> = per
        .into_iter()
        .map(|(cd, gcp_only)| {
            // 引けない乗務員CD は「押し込み済みに無い」側へ倒す (静かに一致させない)
            let n = cd.trim().parse::<i64>().ok();
            UnkoDiffDriver {
                driver_cd: cd.to_string(),
                gcp_only,
                onprem_in_month: n
                    .and_then(|n| in_month_ops.get(&n))
                    .copied()
                    .unwrap_or(0usize),
                onprem_ever: n.is_some_and(|n| aux.onprem_ever.contains(&n)),
            }
        })
        .collect();
    // 多い順。同数なら乗務員CD 順 (BTreeMap から来ているので既に整列済み)
    rows.sort_by_key(|r| std::cmp::Reverse(r.gcp_only));
    rows.truncate(MAX_UNKO_DIFF_DRIVERS);
    (rows, unknown)
}

/// その日付が `(y, m)` の暦月に入っているか。
fn in_month(d: NaiveDate, y: i32, m: u32) -> bool {
    use chrono::Datelike;
    d.year() == y && d.month() == m
}

/// 辞書順に並べ終わった一覧の頭 [`MAX_UNKO_DIFF_SAMPLE`] 件。
fn sample(sorted: &[&str]) -> Vec<UnkoSample> {
    sorted
        .iter()
        .take(MAX_UNKO_DIFF_SAMPLE)
        .map(|u| UnkoSample::new(u))
        .collect()
}

/// **突合して warning を添える** (Refs #205 の 37)。オンプレに在って GCP に無い運行が
/// 1 件でもあれば 1 行足す — 欠けた入力のまま月ゲートに「最新」の封をさせないため
/// (`missing_input_warnings` と同じ安全側の倒し方)。warning はどれも**封を止める**。
///
/// ## 正規化が効いていないことを静かに通さない
///
/// [`onprem_unko_no`] は「末尾 1 文字が対象CD」という**実測に乗った仮定**なので、
/// 前提が崩れたら黙って全件不一致に戻る。それを検知するために 2 つ warning を出す:
///
/// 1. **GCP 側の桁が揃っていない** — 揃っている前提で末尾を落としているため
/// 2. **正規化しても 1 件も一致しない** — 2026-07-31 に踏んだ形そのもの。
///    「重なりゼロ」を `unko_diff_total` が大きいだけの状態と区別する
pub fn unko_diff_with_warnings(
    onprem: &[OnpremOperation],
    gcp: &HashSet<String>,
    aux: &UnkoDiffAux,
    month: Option<(i32, u32)>,
) -> (UnkoDiff, Vec<Warning>) {
    let diff = diff_unko(onprem, gcp, aux, month);
    let mut warnings = Vec::new();
    if diff.total > 0 {
        let n = diff.total;
        let w = format!("dtako 入力欠け: 押し込み済みに在って GCP に無い運行 {n} 件");
        warnings.push(Warning::blocking(w));
    }
    if diff.gcp_shape.len_counts.len() > 1 {
        let k = diff.gcp_shape.len_counts.len();
        let w = format!("dtako 突合: GCP 側の運行NO の桁が揃っていない ({k} 種)");
        warnings.push(Warning::blocking(w));
    }
    if !onprem.is_empty() && !gcp.is_empty() && diff.total == onprem.len() {
        let n = onprem.len();
        let w = format!("dtako 突合: 正規化しても運行が 1 件も一致しない ({n} 件)");
        warnings.push(Warning::blocking(w));
    }
    let (all, in_month) = (diff.gcp_only, diff.gcp_only_in_month);
    tracing::info!(all, in_month, "kintai dtako unko diff reverse");
    (diff, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    // ── 運行の突合 (オンプレ × GCP、Refs #205 の 37) ────────────────────────
    //
    // オンプレ 23 桁 = 運行NO(22) + 対象CD(1) / GCP 22 桁 = 運行NO そのもの。
    // 値はすべて本番の実物 (`/api/kintai/events` と etags 応答の標本)。

    fn op(driver_cd: i64, unko_no: &str) -> OnpremOperation {
        OnpremOperation {
            driver_cd,
            unko_no: unko_no.to_string(),
            first_date: d(2026, 6, 24),
            last_date: d(2026, 6, 26),
        }
    }

    fn gcp_set(list: &[&str]) -> HashSet<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    /// **末尾の対象CD ぶんだけ落とす。** 短すぎる値は落とさない (静かに空にしない)。
    #[test]
    fn onprem_unko_no_drops_only_the_crew_suffix() {
        let real = onprem_unko_no("26062411400600000023021");
        assert_eq!(real, "2606241140060000002302", "対象CD 1 桁が落ちる");
        assert_eq!(onprem_unko_no("U1"), "U", "2 文字でも 1 文字は落とす");
        assert_eq!(onprem_unko_no("X"), "X", "1 文字は落とさない");
        assert_eq!(onprem_unko_no(""), "", "空も落とさない");
    }

    /// **本題。** 押し込み済みに在って GCP に無い運行が名指しで出る。
    /// 判定は運行NO 部分だけで、応答に載るのは**生の `unko_no`** (対象CD 込み)。
    #[test]
    fn diff_unko_names_the_operations_missing_from_gcp() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1517, "26062606141000000042291"),
        ];
        // GCP は 1517 の運行しか持っていない
        let diff = diff_unko(
            &onprem,
            &gcp_set(&["2606260614100000004229"]),
            &UnkoDiffAux::default(),
            None,
        );
        assert_eq!(diff.total, 1, "{diff:?}");
        assert_eq!(diff.items.len(), 1);
        assert_eq!(diff.items[0].driver_cd, 1078);
        assert_eq!(diff.items[0].unko_no, "26062411400600000023021", "生の値");
        assert_eq!(diff.items[0].start_date.as_deref(), Some("2026-06-24"));
        assert_eq!(diff.items[0].first_date, "2026-06-24");
        assert_eq!(diff.items[0].last_date, "2026-06-26");
        assert_eq!(diff.gcp_only, 0, "逆方向は無い");
    }

    /// 両方揃っていれば「欠け」はゼロ。標本は突合が成立していても出す
    /// (キーが噛み合っているかを毎回目視できるようにするため)。
    #[test]
    fn diff_unko_is_empty_when_both_sides_agree() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1517, "26062606141000000042291"),
        ];
        let gcp = gcp_set(&["2606241140060000002302", "2606260614100000004229"]);
        let diff = diff_unko(&onprem, &gcp, &UnkoDiffAux::default(), None);
        assert_eq!(diff.total, 0);
        assert_eq!(diff.gcp_only, 0);
        assert!(diff.items.is_empty());
        assert!(diff.gcp_only_sample.is_empty(), "逆方向が無いので空");
        let mine: Vec<&str> = diff.onprem_sample.iter().map(|s| &*s.unko_no).collect();
        assert_eq!(mine.len(), 2, "自分側は正規化後の運行NO で出る: {mine:?}");
    }

    /// **★ 2 名乗務が正規化後も 2 エントリとして残る** (Refs #205 の 37)。
    ///
    /// 同じ運行NO が運転手 (対象CD=1) と副運転手 (対象CD=2) で**別の乗務員**に
    /// 紐づく。正規化するのは運行NO の部分だけで `driver_cd` は落とさないので、
    /// 2 人ぶんが 2 件とも出力に残る — ここを運行NO だけで畳むと副運転手が消える。
    #[test]
    fn diff_unko_keeps_both_crew_members_of_a_two_person_operation() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1234, "26062411400600000023022"),
        ];
        let diff = diff_unko(&onprem, &gcp_set(&[]), &UnkoDiffAux::default(), None);
        assert_eq!(diff.total, 2, "2 人ぶん残る: {diff:?}");
        let drivers: Vec<i64> = diff.items.iter().map(|i| i.driver_cd).collect();
        assert_eq!(drivers, vec![1078, 1234], "運転手と副運転手の両方");
    }

    /// **2 名乗務の判定は必ず両方一致する。**
    ///
    /// GCP 側は運行NO 単位に dedup 済み (alc `dtako_events.rs:530-531`) なので、
    /// **「片方だけ GCP に在る」状態はそもそも表現できない** — この不変条件は
    /// 「2 エントリの判定が揃う」ところまでしかテストで固定できない
    /// (親の判断、2026-07-31)。alc が `driver_cds` を返すようになった今も、
    /// 逆方向は運行NO の集合で比べている。
    #[test]
    fn diff_unko_judges_both_crew_members_the_same_way() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1234, "26062411400600000023022"),
        ];
        let present = diff_unko(
            &onprem,
            &gcp_set(&["2606241140060000002302"]),
            &UnkoDiffAux::default(),
            None,
        );
        assert_eq!(present.total, 0, "在れば 2 件とも消える: {present:?}");
        let absent = diff_unko(&onprem, &gcp_set(&[]), &UnkoDiffAux::default(), None);
        assert_eq!(absent.total, 2, "無ければ 2 件とも残る");
    }

    /// **逆方向は運行NO の集合同士。** 乗務員CD で膨らませない — 2 名乗務で
    /// オンプレが 2 行持っていても、GCP 側の 1 件と過不足なく突き合う。
    #[test]
    fn diff_unko_counts_the_reverse_direction_by_unko_no_not_by_pair() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1234, "26062411400600000023022"),
        ];
        let gcp = gcp_set(&["2606241140060000002302", "2606260614100000004229"]);
        let diff = diff_unko(&onprem, &gcp, &UnkoDiffAux::default(), None);
        assert_eq!(diff.total, 0, "オンプレ側の欠けは無い");
        assert_eq!(diff.gcp_only, 1, "GCP にしか無いのは 1517 の 1 本だけ");
    }

    /// **★ 逆方向は対象月で絞ってからでないと異常かどうか判定できない。**
    ///
    /// etags は**読取日**で引くので、窓の中に前月以前に始まった運行が混ざる
    /// (本番 2026-06 の実測で開始日が 2026-04-20..2026-07-02、逆方向 478 件)。
    /// 月の外はオンプレの当月分と一致しなくて当然なので、`gcp_only` そのものは
    /// 大きく出る。**`gcp_only_in_month` が 0 でないときだけ異常。**
    #[test]
    fn the_reverse_direction_is_also_counted_within_the_target_month() {
        let gcp = gcp_set(&[
            "2604202128500000002536", // 4 月に始まり 6 月に読まれた運行
            "2605060751400000002536", // 5 月
            "2606241140060000002302", // 6 月 = 対象月
        ]);
        let diff = diff_unko(&[], &gcp, &UnkoDiffAux::default(), Some((2026, 6)));
        assert_eq!(diff.gcp_only, 3, "窓ぜんたいでは 3 件");
        assert_eq!(diff.gcp_only_in_month, 1, "対象月に始まったのは 1 件だけ");
        let want = BTreeMap::from([
            ("2026-04".to_string(), 1),
            ("2026-05".to_string(), 1),
            ("2026-06".to_string(), 1),
        ]);
        assert_eq!(diff.gcp_only_by_month, want, "月別の内訳で内訳が読める");
    }

    // ── 逆方向の内訳 (Refs #205 の 39) ────────────────────────────────────
    //
    // 401 件 (2026-06 本番) が「射程外 (A)」なのか「欠落 (B)」なのかを
    // 分けるための材料。**突合そのものには一切入らない。**

    /// `unko_no` → `driver_cds` の 1 件。
    fn gd(unko_no: &str, drivers: &[&str]) -> (String, Vec<String>) {
        (
            unko_no.to_string(),
            drivers.iter().map(|s| s.to_string()).collect(),
        )
    }

    fn aux(drivers: &[(String, Vec<String>)], ever: &[i64]) -> UnkoDiffAux {
        UnkoDiffAux {
            gcp_drivers: drivers.iter().cloned().collect(),
            onprem_ever: ever.iter().copied().collect(),
        }
    }

    /// **本題 (Refs #205 の 39)。** 対象月の逆方向が乗務員別に割れ、3 つの桶に
    /// 分かれる — 射程外 (一度も押し込み済みに居ない) / 当月だけ落ちた /
    /// 当月にも押し込み済みが在る。
    #[test]
    fn the_reverse_direction_splits_drivers_into_out_of_scope_and_missing() {
        // 1688 は押し込み済みに一度も居ない (射程外)、2 本とも GCP にしかない
        // 1078 は過去には居たのに 6 月は 0 件、1517 は 6 月にも 1 本ある
        let onprem = vec![op(1517, "26062411400600000023021")];
        let gcp = gcp_set(&[
            "2606010930290000006654", // 1688
            "2606280911250000006654", // 1688
            "2606050751400000002536", // 1078
            "2606260614100000004229", // 1517
        ]);
        let drivers = [
            gd("2606010930290000006654", &["1688"]),
            gd("2606280911250000006654", &["1688"]),
            gd("2606050751400000002536", &["1078"]),
            gd("2606260614100000004229", &["1517"]),
        ];
        let diff = diff_unko(
            &onprem,
            &gcp,
            &aux(&drivers, &[1078, 1517]),
            Some((2026, 6)),
        );
        assert_eq!(diff.gcp_only_in_month, 4, "{diff:?}");
        assert_eq!(
            diff.gcp_only_in_month_unknown_driver, 0,
            "全件で乗務員が引けた"
        );
        let rows = &diff.gcp_only_in_month_by_driver;
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].driver_cd, "1688", "多い順に並ぶ");
        assert_eq!(rows[0].gcp_only, 2);
        assert_eq!(rows[0].onprem_in_month, 0);
        assert!(!rows[0].onprem_ever, "一度も押し込み済みに居ない = 射程外");
        let s = &diff.gcp_only_driver_split;
        assert_eq!(
            (s.never_onprem_drivers, s.never_onprem_ops),
            (1, 2),
            "射程外"
        );
        let other = (s.other_month_only_drivers, s.other_month_only_ops);
        assert_eq!(other, (1, 1), "1078 は過去に居たのに当月 0 件 = 欠落が濃い");
        let also = (s.also_in_month_drivers, s.also_in_month_ops);
        assert_eq!(also, (1, 1), "1517 は当月にも押し込み済みが在る");
    }

    /// **日別の内訳。** 特定の日に固まっているのか散っているのかを見る。
    #[test]
    fn the_reverse_direction_is_counted_by_start_day() {
        let gcp = gcp_set(&[
            "2606010930290000006654",
            "2606011140060000002302",
            "2606280911250000006654",
            "2605060751400000002536", // 対象月の外 — 日別に入らない
        ]);
        let diff = diff_unko(&[], &gcp, &UnkoDiffAux::default(), Some((2026, 6)));
        let want = BTreeMap::from([("2026-06-01".to_string(), 2), ("2026-06-28".to_string(), 1)]);
        assert_eq!(diff.gcp_only_in_month_by_day, want, "{diff:?}");
    }

    /// **alc が `driver_cds` を返さない環境では内訳が空になる** — それを
    /// 「乗務員が居ない」と読ませないために、引けなかった件数を別に返す。
    #[test]
    fn the_reverse_direction_counts_operations_whose_driver_is_unknown() {
        let gcp = gcp_set(&["2606010930290000006654", "2606280911250000006654"]);
        // 1 件は driver_cds が空配列 (前方互換フィールドの既定) で来る
        let drivers = [gd("2606010930290000006654", &[])];
        let diff = diff_unko(&[], &gcp, &aux(&drivers, &[]), Some((2026, 6)));
        assert_eq!(diff.gcp_only_in_month, 2);
        assert!(diff.gcp_only_in_month_by_driver.is_empty(), "{diff:?}");
        assert_eq!(
            diff.gcp_only_in_month_unknown_driver, 2,
            "空配列も未知も同じ扱い"
        );
    }

    /// **2 名乗務は 2 人とも 1 件ずつ数える。** 運行数へ畳むと副運転手が消える。
    /// 数字にならない乗務員CD は「押し込み済みに無い」側へ倒す (静かに一致させない)。
    #[test]
    fn the_reverse_direction_counts_both_crew_members_and_tolerates_a_bad_driver_cd() {
        let gcp = gcp_set(&["2606010930290000006654"]);
        let drivers = [gd("2606010930290000006654", &["1688", "x"])];
        let diff = diff_unko(&[], &gcp, &aux(&drivers, &[1688]), Some((2026, 6)));
        let rows = &diff.gcp_only_in_month_by_driver;
        assert_eq!(rows.len(), 2, "1 運行が 2 人に乗る: {rows:?}");
        assert_eq!(rows[0].driver_cd, "1688");
        assert!(rows[0].onprem_ever, "こちらは押し込み済みに居た");
        assert_eq!(rows[1].driver_cd, "x");
        assert!(!rows[1].onprem_ever, "読めない乗務員CD は一致させない");
        assert_eq!(diff.gcp_only_in_month_unknown_driver, 0);
    }

    /// 月を渡さなければ月で絞った数は出さない (0 のまま)。読めない `unko_no` は
    /// `"?"` の桶に落ちる。
    #[test]
    fn the_reverse_direction_without_a_month_reports_only_the_whole_window() {
        let diff = diff_unko(&[], &gcp_set(&["U1"]), &UnkoDiffAux::default(), None);
        assert_eq!(diff.gcp_only, 1);
        assert_eq!(diff.gcp_only_in_month, 0, "月が無ければ絞れない");
        assert_eq!(
            diff.gcp_only_by_month,
            BTreeMap::from([("?".to_string(), 1)])
        );
    }

    /// **キーが 1 つも一致しないとき、両側の実物が対で返る** (Refs #205 の 37)。
    /// 標本は**辞書順**で、`HashSet` の反復順に振り回されない。
    #[test]
    fn diff_unko_samples_both_sides_in_a_stable_order() {
        let onprem = vec![op(1078, "B21"), op(1517, "B11")];
        let diff = diff_unko(
            &onprem,
            &gcp_set(&["A2", "A1", "A3"]),
            &UnkoDiffAux::default(),
            None,
        );
        let theirs: Vec<&str> = diff.gcp_only_sample.iter().map(|s| &*s.unko_no).collect();
        assert_eq!(theirs, vec!["A1", "A2", "A3"], "GCP 側は辞書順");
        let mine: Vec<&str> = diff.onprem_sample.iter().map(|s| &*s.unko_no).collect();
        assert_eq!(mine, vec!["B1", "B2"], "オンプレ側も辞書順 (正規化後)");
        assert_eq!(diff.gcp_only, 3);
        assert_eq!(diff.total, 2);
    }

    /// **`len` は前後の空白も数える。** 桁違いと空白混入をその場で見分けるため。
    #[test]
    fn the_sample_reports_the_length_including_padding() {
        let diff = diff_unko(
            &[op(1078, "26062411400600000023021")],
            &gcp_set(&[" 2606241140060000002302 "]),
            &UnkoDiffAux::default(),
            None,
        );
        assert_eq!(diff.onprem_sample[0].len, 22, "対象CD を落とした後の長さ");
        assert_eq!(diff.gcp_only_sample[0].len, 24, "前後の空白を数える");
    }

    /// **本番の実物で形を測る** (Refs #205 の 37)。`shape` は**生の値**で測る —
    /// 正規化が効いているかどうかを、正規化の前の形から判断するため。
    ///
    /// 値は `/api/kintai/events` から引いた実データ (乗務員 1021 / 1078 / 1517 /
    /// 1688 / 1740 の 2026-06、31 運行) と、本番の etags 応答の標本そのもの。
    #[test]
    fn the_shape_of_both_sides_is_measured_over_the_whole_month() {
        let onprem = vec![
            // 1517 / 長崎100か4229 と 1688 / 帯広100か6654 の実物
            op(1517, "26052106075900000042291"),
            op(1688, "26062809112500000066541"),
            // 1740 の同じ運行が対象CD 1 / 2 の 2 行に分かれている実物
            // (…2 は休息 3 件だけで、3 件とも …1 側にも在る = 独立した運行ではない)
            op(1740, "26061105351800000039751"),
            op(1740, "26061105351800000039752"),
        ];
        let diff = diff_unko(
            &onprem,
            &gcp_set(&["2604202128500000002536"]),
            &UnkoDiffAux::default(),
            None,
        );
        assert_eq!(diff.onprem_shape.len_counts, BTreeMap::from([(23, 4)]));
        assert_eq!(diff.gcp_shape.len_counts, BTreeMap::from([(22, 1)]));
        let last = &diff.onprem_shape.last_char_counts;
        let want = BTreeMap::from([("1".to_string(), 3), ("2".to_string(), 1)]);
        assert_eq!(*last, want, "対象CD は 1 とは限らない");
    }

    /// **候補ごとの試算は突合の採否と別に出し続ける** (Refs #205 の 37)。
    /// `raw` (対象CD 込み) では 1 件も一致せず、`prefix22` で全件一致する —
    /// 本番 2026-06 の実測がこの形 (raw 0/683、prefix22 654/683)。
    #[test]
    fn the_key_trials_measure_each_candidate() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1517, "26062606141000000042291"),
        ];
        let gcp = gcp_set(&["2606241140060000002302", "2606260614100000004229"]);
        let diff = diff_unko(&onprem, &gcp, &UnkoDiffAux::default(), None);
        assert_eq!(diff.total, 0, "突合は正規化済み = 全件一致");

        let by = |k: &str| {
            diff.trials
                .iter()
                .find(|t| t.key == k)
                .expect("候補が並んでいる")
                .clone()
        };
        assert_eq!((by("raw").matched, by("raw").unmatched), (0, 2));
        assert_eq!(by("raw").gcp_only, 2);
        assert_eq!((by("prefix22").matched, by("prefix22").unmatched), (2, 0));
        assert_eq!(by("prefix22").gcp_only, 0, "逆方向も消える");
        assert_eq!(by("prefix12").matched, 2, "日時 12 桁でも一致する");
    }

    /// **同じ正規化キーに複数の生の値が乗ることを数える。** 1740 の 2026-06-11 は
    /// 対象CD 1 / 2 の 2 行があり、先頭 22 桁では 1 つになる。
    ///
    /// **これは「運行が消える」ではない** — 対象CD 違いは同じ 1 本の運行なので、
    /// 畳むのが正しい (`UnkoKeyTrial` の docs の実測)。本番でも `collisions` = 2 と
    /// オンプレ側の末尾 `"2"` = 2 件がぴったり一致した。
    #[test]
    fn the_key_trials_count_operations_that_collapse_into_one_key() {
        let onprem = vec![
            op(1740, "26061105351800000039751"),
            op(1740, "26061105351800000039752"),
            op(1517, "26052106075900000042291"),
        ];
        let diff = diff_unko(&onprem, &gcp_set(&[]), &UnkoDiffAux::default(), None);
        let by = |k: &str| diff.trials.iter().find(|t| t.key == k).unwrap().clone();
        assert_eq!(by("raw").collisions, 0, "生のキーは潰れない");
        assert_eq!(by("prefix22").collisions, 2, "1740 の 2 行が 1 つになる");
        assert_eq!(by("prefix12").collisions, 2, "12 桁でも同じ 2 行が畳まれる");
    }

    /// 候補キーより短い値は切らずにそのまま比べる (一致しないだけ)。
    #[test]
    fn a_key_shorter_than_the_candidate_is_left_alone() {
        assert_eq!(normalize_key("U1", Some(22)), "U1");
        assert_eq!(
            normalize_key("26062411400600000023021", None),
            "26062411400600000023021"
        );
    }

    /// 標本は 10 件で切る (実物の形が分かればよく、全量は `total` で足りる)。
    #[test]
    fn the_sample_is_capped() {
        let onprem: Vec<OnpremOperation> = (0..MAX_UNKO_DIFF_SAMPLE + 3)
            .map(|i| op(1000 + i as i64, &format!("U{i:03}x")))
            .collect();
        let diff = diff_unko(&onprem, &gcp_set(&[]), &UnkoDiffAux::default(), None);
        assert_eq!(diff.onprem_sample.len(), MAX_UNKO_DIFF_SAMPLE);
        assert_eq!(diff.onprem_sample[0].unko_no, "U000", "頭から (正規化後)");
    }

    /// **上限 500 で切り、総数は実数のまま。** `unsplit` と同じ作法。
    #[test]
    fn diff_unko_caps_the_items_but_keeps_the_real_total() {
        let onprem: Vec<OnpremOperation> = (0..MAX_UNKO_DIFF + 5)
            .map(|i| op(1000 + i as i64, &format!("2606241005550000002{i:04}")))
            .collect();
        let diff = diff_unko(&onprem, &gcp_set(&[]), &UnkoDiffAux::default(), None);
        assert_eq!(diff.items.len(), MAX_UNKO_DIFF, "頭 500 件だけ");
        assert_eq!(diff.total, MAX_UNKO_DIFF + 5, "総数は切らない");
    }

    /// `unko_no` が読めない形は `start_date` が `null` になるだけで落とさない。
    #[test]
    fn diff_unko_tolerates_an_unreadable_unko_no() {
        let diff = diff_unko(
            &[op(1078, "U1")],
            &gcp_set(&[]),
            &UnkoDiffAux::default(),
            None,
        );
        assert_eq!(diff.total, 1);
        assert_eq!(diff.items[0].start_date, None, "6 桁に満たない");
    }

    fn texts(ws: &[Warning]) -> Vec<String> {
        ws.iter().map(|w| w.text.clone()).collect()
    }

    /// **突合の結果と warning 1 行が値で返る** (task-local の sink の代わり)。
    #[test]
    fn unko_diff_with_warnings_reports_the_diff_and_a_warning() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1517, "26062606141000000042291"),
        ];
        // GCP は別の運行 1 本しか持っていない (= 全件不一致ではない形にする)
        let (diff, warnings) = unko_diff_with_warnings(
            &onprem,
            &gcp_set(&["2606241140060000002302"]),
            &UnkoDiffAux::default(),
            None,
        );
        assert_eq!(diff.total, 1);
        assert_eq!(diff.gcp_only, 0);
        let warnings = texts(&warnings);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let w = &warnings[0];
        assert!(
            w.contains("押し込み済みに在って GCP に無い運行 1 件"),
            "{w}"
        );
    }

    /// **突合の warning はどれも封を止める** (診断専用にしない)。
    #[test]
    fn unko_diff_warnings_block_the_seal() {
        let (_, warnings) = unko_diff_with_warnings(
            &[op(1078, "U1")],
            &gcp_set(&["ZZ"]),
            &UnkoDiffAux::default(),
            None,
        );
        assert!(!warnings.is_empty());
        assert!(warnings.iter().all(|w| w.blocks_seal), "{warnings:?}");
    }

    /// **正規化が効いていない状態を静かに通さない** (Refs #205 の 37)。
    /// 2026-07-31 に踏んだ「重なりゼロ」を、ただ欠けが多いだけの状態と区別する。
    #[test]
    fn unko_diff_warns_when_nothing_matches_at_all() {
        let (_, warnings) = unko_diff_with_warnings(
            &[op(1078, "U1")],
            &gcp_set(&["ZZ"]),
            &UnkoDiffAux::default(),
            None,
        );
        let warnings = texts(&warnings);
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        let hit = warnings.iter().any(|w| w.contains("1 件も一致しない"));
        assert!(hit, "{warnings:?}");
    }

    /// **GCP 側の桁が揃っていなければ警告する。** 末尾を落とす正規化は
    /// 「向こうは揃っている」前提に乗っているため。
    #[test]
    fn unko_diff_warns_when_the_gcp_lengths_are_not_uniform() {
        let gcp = gcp_set(&["2606241140060000002302", "260626061410000000422"]);
        let (_, warnings) = unko_diff_with_warnings(
            &[op(1078, "26062411400600000023021")],
            &gcp,
            &UnkoDiffAux::default(),
            None,
        );
        let warnings = texts(&warnings);
        let hit = warnings
            .iter()
            .any(|w| w.contains("桁が揃っていない (2 種)"));
        assert!(hit, "{warnings:?}");
    }

    /// 差が無ければ warning は立たない (月ゲートを無駄に開けない)。
    #[test]
    fn unko_diff_is_silent_when_nothing_is_missing() {
        let gcp = gcp_set(&["2606241140060000002302"]);
        let (diff, warnings) = unko_diff_with_warnings(
            &[op(1078, "26062411400600000023021")],
            &gcp,
            &UnkoDiffAux::default(),
            None,
        );
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(diff.total, 0);
        assert_eq!(diff.gcp_only, 0);
    }
}
