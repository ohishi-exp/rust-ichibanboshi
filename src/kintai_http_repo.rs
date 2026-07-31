//! 勤怠の生イベントを `rust-alc-api` の HTTP 経由で読む実装 (Refs #205 実装計画 02)。
//!
//! [`crate::kintai_repo::MariadbKintaiEventsRepo`] の 3 つ目の兄弟。社内 MariaDB に
//! 到達できない実行形態 (GCP / Cloud Run) から同じ形の生イベントを読むための経路で、
//! `GET /api/dtako/events` (ippoan/rust-alc-api#578) を叩く。
//!
//! ## 上流は生 CSV 行を返す — 写すのはこちらの責務
//!
//! 上流は**畳まない**。イベント種別の分類・状態への写像・時刻のパースや TZ 変換・
//! 勤務境界の判定を一切せず、R2 の per-運行 `KUDGIVT.csv` を `{headers, rows}` の
//! まま返す (勤怠計算は一番星固有でマルチテナント基盤に置かないという #205 の決定 3)。
//! よって「生 CSV 行 → [`KintaiEventsApi`] の戻り値」の変換がこのモジュールの仕事で、
//! **`kosoku.rs` / `kosoku_paper.rs` には一切手を入れない**。
//!
//! ## MariaDB の `dtako_events` ブランチと 1 対 1 に写す
//!
//! `EVENTS_SQL` の 3 番目・4 番目の `UNION ALL` ブランチが写し先:
//!
//! | 戻り値のキー | MariaDB | KUDGIVT.csv の列 |
//! |---|---|---|
//! | `datetime` | `DATE_FORMAT(開始日時)` | `開始日時` (`YYYY/MM/DD` → `YYYY-MM-DD`) |
//! | `end_datetime` | `DATE_FORMAT(終了日時)` | `終了日時` (無い CSV は `null`) |
//! | `driver_id` | `対象乗務員CD` | **`対象乗務員CD` → 無ければ `乗務員CD1`** |
//! | `source` | `'dtako_events'` | 固定 |
//! | `state` | `イベント名` | `イベント名` |
//! | `unko_no` | `運行NO` | `運行NO` |
//! | `vehicle` | `dtako_cars` を JOIN した `車輌名` | `車輌名` (無い CSV は `null`) |
//!
//! **`対象乗務員CD` を優先し無ければ `乗務員CD1` にフォールバックする** のは、
//! `乗務員CD1` が運行の主運転者で全行同じ値になるため。2 名乗務の運行では副運転手の
//! 行まで主運転者に付き、引かれた側は丸ごと落ちる (上流の parser も
//! ippoan/rust-alc-api#580 で同じ規則に揃えたところ)。列の有無は**運行ごと**に見る —
//! `headers` がトップレベルに畳まれていないのは、`対象乗務員CD` のように一部の
//! ファイルにしか無い列が実在するから。
//!
//! ## MariaDB 実装との差異 (このモジュールが埋められないもの)
//!
//! 上流が返すのは KUDGIVT = MariaDB の `dtako_events` に相当する 1 種類だけ。
//! `EVENTS_SQL` が `UNION ALL` している残り 2 つには上流に口が無い:
//!
//! | source | 元テーブル | HTTP 経路 |
//! |---|---|---|
//! | `dtako_events` | `dtako_events` | **上流から取る** |
//! | `timecard` | `time_card_dstate` (始業 / 終業の打刻) | 口が無い → `fallback` へ委譲 |
//! | `dtako` | `time_card_dtako` (運行開始 / 終了 / 休息) | 口が無い → `fallback` へ委譲 |
//! | (フェリー) | `dtako_ferry_rows` | 口が無い → `fallback` へ委譲 |
//!
//! そこで `fallback` (通常は MariaDB 実装) を持ち、**上流に口が無いものだけ**そちらへ
//! 委譲する。オンプレでは両方揃うので HTTP 経路の出力を MariaDB と突き合わせて
//! 検証できる。
//!
//! MariaDB が無い実行形態 (GCP) の `fallback` は
//! [`crate::kintai_pg_repo::PgKintaiEventsRepo`] — 04b で `kintai.kintai_events` に
//! 入った打刻を読み返す (#205 の G6)。**かつてここが `None` になり
//! `shifts_from_timecard` が空になっていた穴がこれで閉じる。** どちらも無い形態は
//! 今も打刻が読めないので、起動時に warn を出して静かな欠損にしない
//! (`crate::server::build_kintai_events_repo`)。
//!
//! ## 期間の写し方
//!
//! trait の期間は `[from, to)` の `YYYY-MM-DD HH:MM:SS`、上流は**日付の閉区間**
//! `date_from..=date_to`。粒度が違うので上流には広めに投げ、**返ってきた行を
//! `EVENTS_SQL` と同じ 2 条件で絞り直す** (期間内に始まる区間 + 期間内に終わる区間)。
//!
//! - 上流の期間上限が単一乗務員 366 日 / 全乗務員 31 日なので、超える期間は
//!   [`date_chunks`] で分割して複数回叩く (`month_range` は 32〜34 日になるため
//!   全乗務員版では必ず分割が要る)
//! - 全乗務員版は `page_size` / `after_driver_cd` の keyset ページングを回しきる
//! - 同じ運行が複数の chunk / 複数の乗務員グループに現れるので `運行NO` で重複排除する。
//!   **CSV の中の重複行は落とさない** — 取り込みが 2 回走った重複は `kosoku.rs` 側が
//!   扱う話で、ここで消すと MariaDB 経路と値が割れる

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::{NaiveDate, NaiveDateTime};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::KintaiEventsConfig;
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};

/// 上流の endpoint path。
const EVENTS_PATH: &str = "/api/dtako/events";

/// 上流の月ゲート用 etags endpoint (Refs #205 実装計画 13)。R2 の LIST だけで
/// `{unko_no, etag}` を返す — CSV は読まない。alc に口が無い環境 (404) では
/// [`HttpKintaiEventsRepo::fetch_etags`] が `Ok(None)` を返し、呼び出し側が
/// 月ゲートを諦めて従来どおり全量読みへ degrade する。
const ETAGS_PATH: &str = "/api/dtako/events/etags";

/// 上流の期間上限 (単一乗務員、閉区間の日数)。`MAX_RANGE_DAYS_SINGLE` と同値。
const MAX_RANGE_DAYS_SINGLE: i64 = 366;

/// 上流の期間上限 (全乗務員)。`MAX_RANGE_DAYS_ALL` と同値。
const MAX_RANGE_DAYS_ALL: i64 = 31;

/// 全乗務員版の 1 ページあたり乗務員数。上流の上限 (50) に合わせて往復を減らす。
const PAGE_SIZE: i64 = 50;

/// ページングの打ち切り。無限ループを黙って回さず error にする。
const MAX_PAGES: usize = 200;

/// 上流に投げる期間を何日前から始めるか。
///
/// `EVENTS_SQL` の 4 番目のブランチ (期間内に終わる区間、開始は期間より前) を
/// 拾うために要る。区間の長さで一番長いのは休息 (実測 1,123 分 ≒ 19 時間) なので
/// 2 日あれば足りる — #205 の再計算が「差分日 ± 2 日」を対象にしているのと同じ幅。
const LOOKBACK_DAYS: i64 = 2;

/// CSV の日時形式 (`2026/02/24 14:40:56`)。
const CSV_DATETIME_FORMATS: [&str; 2] = ["%Y/%m/%d %H:%M:%S", "%Y/%m/%d %k:%M:%S"];

/// 戻り値の日時形式 (`DATE_FORMAT(..., '%Y-%m-%d %H:%i:%s')` と同じ)。
const OUT_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// この経路が名乗る `source`。MariaDB の 3 / 4 番目のブランチと同じ値。
const SOURCE_DTAKO_EVENTS: &str = "dtako_events";

// ── 上流 warnings の持ち出し ───────────────────────────────────────────────

/// 1 回の呼び出しで持ち帰る warnings の上限。
///
/// 上流は運行ごとに 1 本出すので、R2 の分割がまるごと遅れていると月ぶんの
/// 運行数 (実測 1,100 件超) が並ぶ。応答を膨らませても読めないので頭だけ。
const MAX_COLLECTED_WARNINGS: usize = 20;

tokio::task_local! {
    /// いま集めている最中の warnings。[`with_warning_sink`] の中だけで立つ。
    ///
    /// `Mutex` ではなく `RefCell` — task local なので触るのは 1 つの task だけで、
    /// `.await` を挟んで借りたままにする箇所も無い。
    static WARNING_SINK: std::cell::RefCell<Vec<String>>;
}

/// **上流の warnings を集めながら `fut` を走らせる** (Refs #205 の 06 の HTTP 版)。
///
/// R2 の分割遅れ (`NoSuchKey`) の最中に畳むと、欠けた入力を指紋付きで「最新」として
/// 保存してしまう。指紋は入力から作るので次に運行が揃えば畳み直されるが、**その間は
/// 静かに少ない拘束を返す** — #205 のリスク欄の筆頭そのものなので、`tracing` に
/// 落として終わりにせず呼び出し側の応答まで運ぶ。
///
/// **task local にしてあるのは混線させないため。** repo は `Arc` で全リクエストに
/// 共有されているので、struct にバッファを持たせると隣のリクエストの warnings が
/// 混ざる (逆に、隣に持って行かれて自分の分が消える)。axum のハンドラは 1
/// リクエスト = 1 task なので、その task に閉じた置き場なら取り違えが起きない。
pub async fn with_warning_sink<F: std::future::Future>(fut: F) -> (F::Output, Vec<String>) {
    WARNING_SINK
        .scope(std::cell::RefCell::new(Vec::new()), async move {
            let out = fut.await;
            let collected = collected_warnings();
            (out, collected)
        })
        .await
}

/// いま集まっている warnings。sink が無ければ空。
fn collected_warnings() -> Vec<String> {
    WARNING_SINK
        .try_with(|sink| sink.borrow().clone())
        .unwrap_or_default()
}

/// 集めている最中なら記録する。**同じ文面は 1 回だけ**、上限まで。
///
/// `pub(crate)` なのは [`crate::kintai_fold`] が push 窓の欠けをここへ載せるため
/// (Refs #205 の 30)。上流 warnings と同じ器に入れることで、月ゲートの
/// `warnings.is_empty()` がそのまま効く。
pub(crate) fn record_warning(w: &str) {
    let _ = WARNING_SINK.try_with(|sink| {
        let mut v = sink.borrow_mut();
        if v.len() < MAX_COLLECTED_WARNINGS && !v.iter().any(|s| s == w) {
            v.push(w.to_string());
        }
    });
}

/// テスト専用の [`record_warning`] 穴あけ (実装計画 13)。`record_warning` 自体は
/// private なので、他モジュールの `KintaiEventsApi` スタブ実装 (pg 統合テストの
/// 上流 warnings 疑似発火など) から呼べない。それだけの理由で公開する薄い口。
#[doc(hidden)]
pub fn record_warning_for_test(w: &str) {
    record_warning(w);
}

/// いま収集中の warnings があるか (Refs #205-17)。**収集器の外なら `None`**
/// (「無い」と「分からない」を区別する — [`recalc_month`](crate::kintai_fold::recalc_month)
/// が `write_fold_gate` を書いてよいかの判断に使う)。sink を消費しない —
/// [`with_warning_sink`] の戻り値の warnings がここで空にならないよう `borrow` だけ。
pub fn warnings_seen() -> Option<bool> {
    WARNING_SINK.try_with(|s| !s.borrow().is_empty()).ok()
}

// ── `unsplit` (has_kudgivt = FALSE) の素通し (Refs #205 の 32) ──────────────

tokio::task_local! {
    /// いま集めている最中の `unsplit`。[`with_unsplit_sink`] の中だけで立つ。
    /// `warnings` (件数無制限で毎運行 1 本) と違い 1 回の etags 応答につき
    /// 高々 1 組なので `Option` — 上書きでよい (蓄積・重複排除は要らない)。
    static UNSPLIT_SINK: std::cell::RefCell<Option<(Vec<UnsplitOperation>, usize)>>;
}

/// **`unsplit` を集めながら `fut` を走らせる。** `with_warning_sink` と同じ
/// task-local パターン (docs 参照) — ここでは判定に使わず素通しするだけなので、
/// [`fetch_dtako_month_digest`] を包む呼び出し側がこの戻り値をそのまま応答に載せる。
pub async fn with_unsplit_sink<F: std::future::Future>(
    fut: F,
) -> (F::Output, Vec<UnsplitOperation>, usize) {
    UNSPLIT_SINK
        .scope(std::cell::RefCell::new(None), async move {
            let out = fut.await;
            let (items, total) = collected_unsplit();
            (out, items, total)
        })
        .await
}

/// いま集まっている `unsplit`。sink が無い/未記録なら空。
fn collected_unsplit() -> (Vec<UnsplitOperation>, usize) {
    UNSPLIT_SINK
        .try_with(|sink| sink.borrow().clone())
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// 集めている最中なら記録する (上書き)。
fn record_unsplit(items: Vec<UnsplitOperation>, total: usize) {
    let _ = UNSPLIT_SINK.try_with(|sink| {
        *sink.borrow_mut() = Some((items, total));
    });
}

// ── 運行の突合 (オンプレ × GCP、Refs #205 の 37) ────────────────────────────

/// 応答に載せる `unko_diff` の上限。総数は [`UnkoDiff::total`] に別に返す
/// (`unsplit` / `unsplit_total` と同じ作法)。
pub const MAX_UNKO_DIFF: usize = 500;

/// **オンプレ側が持っている運行 1 本** (Refs #205 の 37)。
///
/// 実体は Postgres の `kintai.kintai_events` — オンプレの MariaDB
/// (`time_card_dtako`) から押し込んだ打刻で、`dtako` 由来の行だけが `unko_no` を
/// 持つ ([`crate::kintai_push::PUSHED_SOURCES`])。**新しい口は作らない** —
/// 突合の両側とも既に fold が読んでいるものだけで組む。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OnpremOperation {
    pub driver_cd: i64,
    pub unko_no: String,
    /// その運行のイベントが覆う暦日 (JST) の最小 / 最大。
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
    /// オンプレのイベントが覆う暦日 (JST) の範囲。
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
/// 一致数を出して並べる。`collisions` があるのは、**桁を落とすと別の運行が同じ
/// キーに潰れうる**ため — 実測で 1740 の 2026-06-11 は同じ車輌・同じ秒に始まる
/// 2 本 (`...39751` / `...39752`) があり、先頭 22 桁では 1 本に潰れる。
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
    /// **同じ乗務員の別の運行が同じキーに潰れる件数。** 0 でなければ情報が落ちる。
    pub collisions: usize,
}

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
    /// **逆方向** (GCP に在ってオンプレに無い) の運行数。ゼロでなければ別の異常。
    pub gcp_only: usize,
    /// 逆方向の実物 (辞書順の先頭 [`MAX_UNKO_DIFF_SAMPLE`] 件)。
    pub gcp_only_sample: Vec<UnkoSample>,
    /// **突き合わせた相手側**の実物 (オンプレの `unko_no`、同じく辞書順の先頭)。
    /// 片側だけ見ても違いは分からないので、必ず対で返す。
    pub onprem_sample: Vec<UnkoSample>,
    /// 両側の**形**の実測 ([`UnkoShape`]) — 標本 10 件では言い切れない部分。
    pub onprem_shape: UnkoShape,
    pub gcp_shape: UnkoShape,
    /// 突合キーの候補ごとの試算 ([`UnkoKeyTrial`])。**採用はしていない。**
    pub trials: Vec<UnkoKeyTrial>,
}

/// **オンプレの `(乗務員CD, unko_no)` 集合と etags の `unko_no` 集合を突き合わせる**
/// (Refs #205 の 37)。
///
/// GCP 側で畳んだ 2026-06 の勤怠がオンプレ基準より 143 行少ない件は、調査の結果
/// 「乗務員ごとに最後の 1 本の `運行NO` がまるごと GCP 側の運行一覧に無い」形だと
/// 分かっている。**どの運行が欠けているかを名指しできる**ようにするのがこの関数。
///
/// - 突合の単位は **`(乗務員CD, unko_no)`** — 2 名乗務の運行は同じ `unko_no` が
///   2 人に紐づくので、片方だけ欠けている形も見える
/// - 逆方向は**件数と標本**。ゼロでないなら「オンプレに無い運行を GCP が持っている」
///   という別の異常なので、名指しより先に気づけることが要る
/// - **両側の標本を必ず対で返す** ([`UnkoSample`])。本番の初回実測で重なりがゼロ
///   だった = キーが一度も一致していないので、片側だけでは何が違うのか分からない。
///   標本は辞書順の先頭 — 呼ぶたびに違う 10 件が出ると比較にならないため
///   (`HashSet` の反復順は不定)
fn diff_unko(onprem: &[OnpremOperation], gcp: &HashSet<String>) -> UnkoDiff {
    let mut items = Vec::new();
    let mut total = 0;
    for op in onprem {
        if gcp.contains(&op.unko_no) {
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
    let seen: HashSet<&str> = onprem.iter().map(|o| o.unko_no.as_str()).collect();
    let mut only: Vec<&str> = gcp
        .iter()
        .map(String::as_str)
        .filter(|u| !seen.contains(u))
        .collect();
    only.sort_unstable();
    let mut mine: Vec<&str> = seen.into_iter().collect();
    mine.sort_unstable();
    UnkoDiff {
        items,
        total,
        gcp_only: only.len(),
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

/// 辞書順に並べ終わった一覧の頭 [`MAX_UNKO_DIFF_SAMPLE`] 件。
fn sample(sorted: &[&str]) -> Vec<UnkoSample> {
    sorted
        .iter()
        .take(MAX_UNKO_DIFF_SAMPLE)
        .map(|u| UnkoSample::new(u))
        .collect()
}

/// [`with_unko_diff_sink`] が抱える 2 つの値。
///
/// **etags の一覧と突合の結果を 1 つの task local に相乗りさせている。** 集める
/// 場所 ([`HttpKintaiEventsRepo::fetch_etags`]) と突き合わせる場所
/// ([`crate::kintai_fold`] — Postgres に触れるのはあちら) が別モジュールなので、
/// scope を 2 重に張らずに済ませるため。
#[derive(Debug, Clone, Default)]
struct UnkoSink {
    etags: Option<HashSet<String>>,
    diff: Option<UnkoDiff>,
}

tokio::task_local! {
    /// いま集めている最中の運行の突合。[`with_unko_diff_sink`] の中だけで立つ。
    static UNKO_SINK: std::cell::RefCell<UnkoSink>;
}

/// **運行の突合を集めながら `fut` を走らせる** (Refs #205 の 37)。
/// `with_warning_sink` / `with_unsplit_sink` と同じ task-local パターン。
pub async fn with_unko_diff_sink<F: std::future::Future>(fut: F) -> (F::Output, UnkoDiff) {
    UNKO_SINK
        .scope(std::cell::RefCell::new(UnkoSink::default()), async move {
            let out = fut.await;
            let diff = UNKO_SINK.with(|s| s.borrow().diff.clone());
            (out, diff.unwrap_or_default())
        })
        .await
}

/// etags から引いた `unko_no` の集合。sink の外 / 未記録なら `None`
/// (「無い」と「引けていない」を混同しない)。
pub fn collected_etag_unko_nos() -> Option<HashSet<String>> {
    UNKO_SINK
        .try_with(|s| s.borrow().etags.clone())
        .ok()
        .flatten()
}

/// 集めている最中なら etags の一覧を控える (上書き)。
fn record_etag_unko_nos(unko_nos: HashSet<String>) {
    let _ = UNKO_SINK.try_with(|s| s.borrow_mut().etags = Some(unko_nos));
}

/// **突合して記録する** (Refs #205 の 37)。オンプレに在って GCP に無い運行が
/// 1 件でもあれば `warnings` に 1 行足す — 欠けた入力のまま月ゲートに「最新」の
/// 封をさせないため (`missing_input_warnings` と同じ安全側の倒し方)。
pub fn record_unko_diff(onprem: &[OnpremOperation], gcp: &HashSet<String>) -> UnkoDiff {
    let diff = diff_unko(onprem, gcp);
    if diff.total > 0 {
        let n = diff.total;
        let w = format!("dtako 入力欠け: オンプレに在って GCP に無い運行 {n} 件");
        record_warning(&w);
    }
    if diff.gcp_only > 0 {
        let gcp_only = diff.gcp_only;
        tracing::warn!(gcp_only, "kintai dtako unko diff reverse");
    }
    let _ = UNKO_SINK.try_with(|s| s.borrow_mut().diff = Some(diff.clone()));
    diff
}

// ── 認証 token の取り方 (設定で与える。コードに焼かない) ────────────────────

/// 上流に付ける Bearer token の供給元。
///
/// 当面は Google 認証 (`gcloud auth print-identity-token` → Cloud Run IAM)、07 で
/// device JWT に差し替える。**どちらもコードに焼かず設定から来る**ため、実装は
/// 「静的な値」「コマンドの出力」「無し」の 3 つだけを知っている。
#[async_trait]
pub trait KintaiTokenProvider: Send + Sync {
    /// `Authorization: Bearer` に載せる値。`None` ならヘッダーを付けない。
    async fn token(&self) -> Result<Option<String>, KintaiRepoError>;
}

/// token 無し (社内 LAN や、網層だけで守る構成)。
pub struct NoTokenProvider;

#[async_trait]
impl KintaiTokenProvider for NoTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        Ok(None)
    }
}

/// 設定に直接書かれた token (device JWT を注入する 07 の受け皿)。
pub struct StaticTokenProvider(pub String);

#[async_trait]
impl KintaiTokenProvider for StaticTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        Ok(Some(self.0.clone()))
    }
}

/// コマンドの標準出力を token として使う (`gcloud auth print-identity-token`)。
///
/// **シェルを経由しない。** 設定文字列を空白で argv に分割して直接 exec するので、
/// 設定がシェル注入の経路にならない。Google の identity token は 1 時間有効なので
/// TTL の間だけ使い回し、毎リクエストで `gcloud` を起こさない。
pub struct CommandTokenProvider {
    argv: Vec<String>,
    ttl: Duration,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl CommandTokenProvider {
    /// `command` を空白で分割して argv にする。空なら `Err`。
    pub fn new(command: &str, ttl_secs: u64) -> Result<Self, String> {
        let argv: Vec<String> = command.split_whitespace().map(str::to_string).collect();
        if argv.is_empty() {
            return Err("auth_token_command is empty".to_string());
        }
        Ok(Self {
            argv,
            ttl: Duration::from_secs(ttl_secs),
            cached: tokio::sync::Mutex::new(None),
        })
    }

    async fn run(&self) -> Result<String, KintaiRepoError> {
        let out = tokio::process::Command::new(&self.argv[0])
            .args(&self.argv[1..])
            .output()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("auth token command: {e}")))?;
        if !out.status.success() {
            let msg = String::from_utf8_lossy(&out.stderr).trim().to_string();
            return Err(KintaiRepoError::QueryFailed(format!(
                "auth token command failed: {} ({msg})",
                out.status
            )));
        }
        let token = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if token.is_empty() {
            return Err(KintaiRepoError::QueryFailed(
                "auth token command produced no output".to_string(),
            ));
        }
        Ok(token)
    }
}

#[async_trait]
impl KintaiTokenProvider for CommandTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        let mut cached = self.cached.lock().await;
        if let Some((token, at)) = cached.as_ref() {
            if at.elapsed() < self.ttl {
                return Ok(Some(token.clone()));
            }
        }
        let token = self.run().await?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(Some(token))
    }
}

/// GCE / Cloud Run の metadata server。**コンテナの中から** identity token を取る唯一の道。
///
/// `auth_token_command = "gcloud auth print-identity-token"` は**開発機から Cloud Run を
/// 叩く**ための経路で、Cloud Run の中では動かない — `Dockerfile` が入れているのは
/// `ca-certificates` だけで、`gcloud` も `curl` も存在しない。
pub const METADATA_BASE_URL: &str = "http://metadata.google.internal";

/// metadata server の identity token endpoint (audience を query で渡す)。
const METADATA_IDENTITY_PATH: &str =
    "/computeMetadata/v1/instance/service-accounts/default/identity";

/// metadata server が要求する固定ヘッダー。付けないと 403 になる
/// (ブラウザからの SSRF で token を抜かれないための Google 側の作り)。
const METADATA_FLAVOR: (&str, &str) = ("Metadata-Flavor", "Google");

/// Cloud Run IAM 用の identity token を metadata server から取る。
///
/// `audience` は**呼び先の Cloud Run service の URL**。Cloud Run IAM は
/// 「この token は自分宛か」を audience で見るので、ここがずれると 401 になる。
/// 既定では `base_url` をそのまま使う ([`build_token_provider`])。
///
/// token は 1 時間有効なので [`CommandTokenProvider`] と同じく TTL の間だけ使い回す。
#[derive(Debug)]
pub struct MetadataTokenProvider {
    client: reqwest::Client,
    /// `{base}{path}?audience=...`。**base をフィールドに持つ**のはテストが
    /// wiremock を差し込めるようにするため。
    url: String,
    ttl: Duration,
    cached: tokio::sync::Mutex<Option<(String, Instant)>>,
}

impl MetadataTokenProvider {
    pub fn new(metadata_base: &str, audience: &str, ttl_secs: u64) -> Result<Self, String> {
        let audience = audience.trim().trim_end_matches('/');
        if audience.is_empty() {
            return Err("metadata token provider needs an audience (base_url)".to_string());
        }
        let client = reqwest::Client::builder()
            // metadata server は同一ホストなので、届かないなら待たずに落とす
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|e| format!("metadata token client: {e}"))?;
        Ok(Self {
            client,
            url: format!(
                "{}{METADATA_IDENTITY_PATH}?audience={}",
                metadata_base.trim_end_matches('/'),
                urlencode(audience)
            ),
            ttl: Duration::from_secs(ttl_secs),
            cached: tokio::sync::Mutex::new(None),
        })
    }

    async fn fetch(&self) -> Result<String, KintaiRepoError> {
        let resp = self
            .client
            .get(&self.url)
            .header(METADATA_FLAVOR.0, METADATA_FLAVOR.1)
            .send()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("metadata identity: {e}")))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("metadata identity body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiRepoError::QueryFailed(format!(
                "metadata identity status {status}: {excerpt}"
            )));
        }
        let token = body.trim().to_string();
        if token.is_empty() {
            return Err(KintaiRepoError::QueryFailed(
                "metadata identity returned no token".to_string(),
            ));
        }
        Ok(token)
    }
}

/// query に載せる最小限の percent-encode。audience は URL なので `:` と `/` が出る。
///
/// `url` crate を足さないのは、encode したいのがこの 1 か所だけのため。
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[async_trait]
impl KintaiTokenProvider for MetadataTokenProvider {
    async fn token(&self) -> Result<Option<String>, KintaiRepoError> {
        let mut cached = self.cached.lock().await;
        if let Some((token, at)) = cached.as_ref() {
            if at.elapsed() < self.ttl {
                return Ok(Some(token.clone()));
            }
        }
        let token = self.fetch().await?;
        *cached = Some((token.clone(), Instant::now()));
        Ok(Some(token))
    }
}

/// 設定から供給元を決める。`auth_token` → `auth_token_command` → `auth_token_metadata`
/// の順に見て、どれも無ければ token 無し。
/// (2 つ以上の指定は `KintaiEventsConfig::validate` が起動時に弾く)
pub fn build_token_provider(
    cfg: &KintaiEventsConfig,
) -> Result<Arc<dyn KintaiTokenProvider>, String> {
    if !cfg.auth_token.is_empty() {
        return Ok(Arc::new(StaticTokenProvider(cfg.auth_token.clone())));
    }
    if !cfg.auth_token_command.trim().is_empty() {
        let p = CommandTokenProvider::new(&cfg.auth_token_command, cfg.auth_token_ttl_secs)?;
        return Ok(Arc::new(p));
    }
    if cfg.auth_token_metadata {
        let p =
            MetadataTokenProvider::new(METADATA_BASE_URL, &cfg.base_url, cfg.auth_token_ttl_secs)?;
        return Ok(Arc::new(p));
    }
    Ok(Arc::new(NoTokenProvider))
}

// ── 上流の応答 (要る部分だけ deserialize する) ──────────────────────────────

/// 1 運行分の生 CSV。`headers` を運行ごとに持つ形をそのまま受ける。
#[derive(Debug, Clone, Deserialize)]
struct UpstreamOperation {
    #[serde(default)]
    unko_no: String,
    #[serde(default)]
    headers: Vec<String>,
    #[serde(default)]
    rows: Vec<Vec<String>>,
}

#[derive(Debug, Clone, Deserialize)]
struct UpstreamSingle {
    #[serde(default)]
    operations: Vec<UpstreamOperation>,
    #[serde(default)]
    warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct UpstreamDriverGroup {
    #[serde(default)]
    operations: Vec<UpstreamOperation>,
}

#[derive(Debug, Clone, Deserialize)]
struct UpstreamAll {
    #[serde(default)]
    drivers: Vec<UpstreamDriverGroup>,
    #[serde(default)]
    next_after_driver_cd: Option<String>,
    #[serde(default)]
    warnings: Vec<String>,
}

/// `GET /api/dtako/events/etags` の 1 件。`etag` が無い (R2 に無い / upload 未完了)
/// 運行は `None` のまま持ち出す — 呼び出し側 (digest 計算) が空文字と区別できるように
/// する。
///
/// `driver_cds` は Refs #205 の 32 で足す前方互換フィールド。alc がまだ返さない
/// (現行) 環境では常に空 — その場合 [`InputCoverage`] は全 item を 1 グループ
/// として扱い、既存の (粒度が粗い) 判定にそのまま揃う。alc 側が `driver_cds` を
/// 返すようになった時点で乗務員別の検知が自動的に有効化される。
///
/// **複数形なのは 1 運行に複数の乗務員 (主 / 副運転) が付き得るため** —
/// `list_operations_for_drivers` は `DISTINCT ON (driver_id, unko_no)` なので同じ
/// `unko_no` が複数の `driver_id` に紐づく。この場合その運行の開始日は
/// `driver_cds` 全員の `last` に寄与させる (親の判断、2026-07-31)。
#[derive(Debug, Clone, Deserialize)]
struct UpstreamEtagItem {
    unko_no: String,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    driver_cds: Vec<String>,
}

/// `has_kudgivt = FALSE` (=読み取り側 3 クエリの母集団からも欠け検知からも同時に
/// 消えている運行) の 1 件。Refs #205 の 32 で alc (#205-36) が足す前方互換フィールド。
///
/// **判定には使わない — `run_kintai_recalc` の応答まで素通しするだけ。**
/// #205 の 142 行差の仮説 (`has_kudgivt = FALSE` が原因) を検証するための実データ
/// 突き合わせ材料で、判定ロジックへの組み込みは実データを見てから別途決める。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct UnsplitOperation {
    pub unko_no: String,
    pub driver_cd: String,
    pub reading_date: String,
}

#[derive(Debug, Clone, Deserialize)]
struct UpstreamEtags {
    #[serde(default)]
    items: Vec<UpstreamEtagItem>,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    unsplit: Vec<UnsplitOperation>,
    #[serde(default)]
    unsplit_total: usize,
}

// ── 列の解決 ──────────────────────────────────────────────────────────────

/// 1 運行の `headers` から要る列の位置を引く。
///
/// `KUDGIVT.csv` は upload 時点のデジタコ出力に依存し、列の並びも有無も運行ごとに
/// 違い得る。だから毎運行ごとに引き直す。
#[derive(Debug, Clone, PartialEq, Eq)]
struct EventCols {
    unko_no: Option<usize>,
    driver_cd: Option<usize>,
    start_at: usize,
    end_at: Option<usize>,
    event_name: usize,
    vehicle: Option<usize>,
}

fn find_col(headers: &[String], name: &str) -> Option<usize> {
    headers.iter().position(|h| h.trim() == name)
}

impl EventCols {
    /// 必須列 (`開始日時` / `イベント名`) が無ければ `None`。
    ///
    /// `driver_cd` は `対象乗務員CD` を優先し、無ければ `乗務員CD1` に落とす
    /// (どちらも無ければ `None` = `driver_id` が `null` になる)。
    fn resolve(headers: &[String]) -> Option<Self> {
        Some(Self {
            unko_no: find_col(headers, "運行NO"),
            driver_cd: find_col(headers, "対象乗務員CD").or_else(|| find_col(headers, "乗務員CD1")),
            start_at: find_col(headers, "開始日時")?,
            end_at: find_col(headers, "終了日時"),
            event_name: find_col(headers, "イベント名")?,
            vehicle: find_col(headers, "車輌名"),
        })
    }
}

fn field(row: &[String], idx: usize) -> Option<&str> {
    row.get(idx).map(|s| s.trim()).filter(|s| !s.is_empty())
}

fn opt_field(row: &[String], idx: Option<usize>) -> Option<&str> {
    idx.and_then(|i| field(row, i))
}

fn parse_csv_datetime(s: &str) -> Option<NaiveDateTime> {
    CSV_DATETIME_FORMATS
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(s, f).ok())
}

// ── 生 CSV 行 → 戻り値 ─────────────────────────────────────────────────────

/// MariaDB の `dtako_events` ブランチ 1 行に相当する中間表現。
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawEvent {
    start: NaiveDateTime,
    end: Option<NaiveDateTime>,
    driver_id: Option<i64>,
    /// `イベント名`。空欄は `null` にする (MariaDB の `NULL` と同じ扱い)。
    state: Option<String>,
    unko_no: Option<String>,
    vehicle: Option<String>,
}

/// CSV 1 行を写す。`開始日時` が読めない行は捨てる (MariaDB では NOT NULL の列で、
/// 上流の parser も同じ行を落としている)。
fn row_to_event(cols: &EventCols, row: &[String], op_unko_no: &str) -> Option<RawEvent> {
    let start = parse_csv_datetime(field(row, cols.start_at)?)?;
    let unko_no = opt_field(row, cols.unko_no)
        .map(str::to_string)
        .or_else(|| Some(op_unko_no.to_string()).filter(|s| !s.is_empty()));
    Some(RawEvent {
        start,
        end: opt_field(row, cols.end_at).and_then(parse_csv_datetime),
        driver_id: opt_field(row, cols.driver_cd).and_then(|s| s.parse::<i64>().ok()),
        state: field(row, cols.event_name).map(str::to_string),
        unko_no,
        vehicle: opt_field(row, cols.vehicle).map(str::to_string),
    })
}

/// `EVENTS_SQL` の 3 / 4 番目のブランチと同じ絞り込み。
///
/// - 期間内に**始まる**区間 (`開始日時 >= from AND < to`)
/// - 期間内に**終わる**区間で開始が期間より前 (`終了日時 >= from AND < to AND 開始日時 < from`)
///
/// 2 つは `開始日時` の条件で排他なので重複しない。
fn in_window(ev: &RawEvent, from: NaiveDateTime, to: NaiveDateTime) -> bool {
    if ev.start >= from && ev.start < to {
        return true;
    }
    match ev.end {
        Some(end) => ev.start < from && end >= from && end < to,
        None => false,
    }
}

fn fmt_dt(dt: NaiveDateTime) -> String {
    dt.format(OUT_DATETIME_FORMAT).to_string()
}

/// 単一乗務員版の 1 行 (`row_to_json` と同じキー構成)。
fn event_to_json(ev: RawEvent) -> serde_json::Value {
    serde_json::json!({
        "datetime": fmt_dt(ev.start),
        "end_datetime": ev.end.map(fmt_dt),
        "driver_id": ev.driver_id,
        "source": SOURCE_DTAKO_EVENTS,
        "state": ev.state,
        "unko_no": ev.unko_no,
        "vehicle": ev.vehicle,
    })
}

/// 全乗務員版の 1 行。`unko_no` / `vehicle` を**キーごと出さない** —
/// `all_row_to_json` が「読んでいない列は `null` にしない」としているのに揃える。
fn event_to_all_json(ev: RawEvent) -> serde_json::Value {
    serde_json::json!({
        "datetime": fmt_dt(ev.start),
        "end_datetime": ev.end.map(fmt_dt),
        "driver_id": ev.driver_id,
        "source": SOURCE_DTAKO_EVENTS,
        "state": ev.state,
    })
}

/// `ORDER BY datetime, source` 相当。
fn sort_rows(rows: &mut [serde_json::Value]) {
    rows.sort_by_key(key_dt_source);
}

/// `ORDER BY driver_id, datetime, source` 相当 (`driver_id` の NULL は先頭)。
fn sort_rows_by_driver(rows: &mut [serde_json::Value]) {
    rows.sort_by(|a, b| {
        (a["driver_id"].as_i64(), key_dt_source(a))
            .cmp(&(b["driver_id"].as_i64(), key_dt_source(b)))
    });
}

fn key_dt_source(v: &serde_json::Value) -> (String, String) {
    let s = |k: &str| v[k].as_str().unwrap_or_default().to_string();
    (s("datetime"), s("source"))
}

/// `fallback` から借りるのは**上流に口が無い source だけ** (打刻と運行の確定イベント)。
/// `dtako_events` を混ぜると HTTP 経路の行と二重になる。
fn is_borrowed_source(v: &serde_json::Value) -> bool {
    v["source"].as_str() != Some(SOURCE_DTAKO_EVENTS)
}

// ── 期間の分割 ────────────────────────────────────────────────────────────

/// 月ゲート用の etags 取得範囲 `[月初, 翌月初]` (両端含む NaiveDate)。
///
/// **[`crate::kintai_repo::month_range`] が読む窓と同じものを覆う。** あちらは
/// `[月初 00:00, 翌月2日 00:00)` = 「暦月 + 翌月 1 日ぶん」を読むので、月ゲートの
/// digest がこの範囲を漏れなく覆っていないと、月末に日跨ぎする勤務の変化を見逃して
/// 「変わっていない」と誤判定しうる (安全側に倒せていない = 一番危ない形の bug)。
/// 閉区間なので上端は「翌月**初日**」(month_range の排他境界 翌月2日の 1 日前)。
fn month_etags_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next_first = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next_first))
}

// ── 入力 (dtako_events) の欠けの検知 (Refs #205 の 21) ─────────────────────

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
const MAX_TAIL_GAP_DAYS: i64 = 7;

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
fn unko_no_start_date(unko_no: &str) -> Option<NaiveDate> {
    NaiveDate::parse_from_str(unko_no.get(..UNKO_NO_DATE_DIGITS)?, "%y%m%d").ok()
}

/// etags の一覧から測った「入力がどこまで届いているか」(Refs #205 の 21)。
///
/// **警告が立つかどうかに関わらず毎回 `tracing::info!` に出す** — 閾値
/// ([`MAX_TAIL_GAP_DAYS`]) は実測で決めたが、母集団は alc の `dtako_operations` を
/// `reading_date` で引いたもので、その分布はこちらから観測できない。ベースラインが
/// ずれていて毎月立ち続けたとしても、**その場のログに実測 gap が出ていれば
/// 1 回の追い PR で締められる**。「外したときに自己診断できる」形にしておくのが
/// 閾値を先に入れる条件 (親の判断、2026-07-31)。
///
/// ## 末尾検知は乗務員別 (Refs #205 の 32)
///
/// 検知は月ぶん・全乗務員を跨いだ「たった 1 つの `last`」で測っていたため、132 名
/// のうち 1 人でも窓の端まで運行があれば `last` がそこに張り付き、故障している他の
/// 乗務員が埋もれて沈黙した (2026-06 本番、49 名影響・8 回計測して warnings 0)。
/// **粒度を `driver_cd` 別に割り、乗務員ごとに `gap = expected - last` を出して
/// 閾値超えを数える。** `driver_cd` が無い item (alc がまだ返さない環境) は空文字
/// キーの 1 グループにまとまり、旧実装 (全体で 1 つの `last`) と同じ結果になる —
/// alc が `driver_cd` を返し始めた時点で自動的に乗務員別の検知に切り替わる。
///
/// ## 母集団は「その月に運行が始まった乗務員」だけ (Refs #205 の 37)
///
/// etags は**読み取り日**で引くので、窓の中に「ずっと前に始まった運行」が混ざる
/// (2026-06 の本番実測で窓の下端が 2026-04-20)。その古い運行しか窓に無い乗務員を
/// 母集団に入れると、`last` が 2 か月前に張り付いて全員が「末尾が欠けている」に
/// なる — 本番で `乗務員73名の末尾が37日超` と鳴っていたのがこれで、実データには
/// 37 日超の乗務員はオンプレにも GCP にも 1 人も居ない。**運行が無いのは「末尾が
/// 欠けた」ではなく「そもそも稼働していない」**なので、窓の月に始まった運行を
/// 1 件も持たない乗務員は `per_driver_last` に入れない。
///
/// **「その乗務員の運行が丸ごと全部消えた」形はこの検知では見えなくなる** が、
/// そちらは [`diff_unko`] (オンプレの `(乗務員CD, unko_no)` との突合) が名指しで
/// 拾う — etags だけを見ていては「働いていない」と原理的に区別が付かない。
/// `no_etag` (R2 に CSV が無い) の検知は母集団に関係なく従来どおり。
///
/// **母集団を絞っただけでは足りず、閾値も乗務員別に合わせ直した**
/// ([`MAX_TAIL_GAP_DAYS`] 2 → 7)。母集団 137 → 113 名で最大 gap は 37 → 27 日に
/// 落ちたが、それでも 72 名が鳴っていた — 乗務員 1 人を見れば土日を挟むだけで
/// gap 3〜4 日になるため。2 つは別の原因で、片方だけでは静かにならない。
struct InputCoverage {
    /// etags の item 数 (= 窓の中の運行数)。
    items: usize,
    /// `etag` が `null` の item 数 (R2 に CSV が無い)。
    no_etag: usize,
    /// 運行開始日の最小 / 最大 (全乗務員通し、summary 表示用)。1 件も読めなければ `None`。
    first: Option<NaiveDate>,
    last: Option<NaiveDate>,
    /// 末尾がここまで届いていてほしい日 (進行中の月は `today - 1 日` に切り下げ)。
    expected: NaiveDate,
    /// `driver_cds` (無ければ空文字 1 本) → その乗務員の運行開始日の最大値。
    /// **窓の月に始まった運行を持つ乗務員だけ** (struct docs の母集団の節)。
    per_driver_last: std::collections::HashMap<String, NaiveDate>,
    /// `driver_cds` を持つ item が 1 件でもあったか。`false` なら alc がまだ
    /// `driver_cds` を返さない環境 — 乗務員別ではなく全体 1 グループの粗い判定に
    /// フォールバックしている、という意味 (Refs #205 の 32)。
    driver_cd_source: bool,
}

impl InputCoverage {
    /// `pairs` を 1 度だけ走査して測る (応答は実測 1,100 件、O(n) に収める)。
    ///
    /// **1 運行が複数の `driver_cds` を持つ場合、その運行開始日は全員の `last` に
    /// 寄与する** — `unko_no` が主 / 副運転で複数の乗務員に紐づき得るため
    /// (`list_operations_for_drivers` の `DISTINCT ON (driver_id, unko_no)`)。
    ///
    /// **`window_start` より前に始まった運行は `per_driver_last` に入れない**
    /// (母集団の節)。`first` / `last` / `no_etag` は窓の中ぜんぶを見たままにする —
    /// summary の実測値と `no_etag` の検知は母集団の話ではないため。
    fn measure(
        pairs: &[(String, Option<String>, Vec<String>)],
        window_start: NaiveDate,
        window_end: NaiveDate,
        today: NaiveDate,
    ) -> Self {
        let mut no_etag = 0;
        let (mut first, mut last) = (None, None);
        let mut per_driver_last: std::collections::HashMap<String, NaiveDate> =
            std::collections::HashMap::new();
        let mut driver_cd_source = false;
        for (unko_no, etag, driver_cds) in pairs {
            if etag.is_none() {
                no_etag += 1;
            }
            if let Some(d) = unko_no_start_date(unko_no) {
                first = Some(first.map_or(d, |f: NaiveDate| f.min(d)));
                last = Some(last.map_or(d, |l: NaiveDate| l.max(d)));
                driver_cd_source = driver_cd_source || !driver_cds.is_empty();
                // 窓の月より前に始まった運行は末尾検知の母集団に入れない (#205 の 37)
                if d < window_start {
                    continue;
                }
                let keys = match driver_cds.is_empty() {
                    true => vec![String::new()],
                    false => driver_cds.clone(),
                };
                for key in keys {
                    per_driver_last
                        .entry(key)
                        .and_modify(|l: &mut NaiveDate| *l = (*l).max(d))
                        .or_insert(d);
                }
            }
        }
        let expected = window_end.min(today - chrono::Duration::days(1));
        let items = pairs.len();
        Self {
            items,
            no_etag,
            first,
            last,
            expected,
            per_driver_last,
            driver_cd_source,
        }
    }

    /// 末尾の不足日数 (全乗務員通し)。運行開始日が 1 件も読めなければ `None`。
    /// ログの `gap` フィールドと `gap_days` 系テストが使う互換値 — 実際の警告判定は
    /// [`Self::tail_gap`] (乗務員別) が行う。
    fn gap_days(&self) -> Option<i64> {
        self.last.map(|l| (self.expected - l).num_days())
    }

    /// 閾値 ([`MAX_TAIL_GAP_DAYS`]) を超えた乗務員数と、その中の最大不足日数。
    /// 1 人も超えていなければ `None`。
    fn tail_gap(&self) -> Option<(usize, i64)> {
        let over: Vec<i64> = self
            .per_driver_last
            .values()
            .map(|l| (self.expected - *l).num_days())
            .filter(|g| *g > MAX_TAIL_GAP_DAYS)
            .collect();
        over.iter().max().map(|&max_gap| (over.len(), max_gap))
    }

    /// ログにも警告本文にも載せる 1 行の実測値。
    ///
    /// **`対象` = 末尾検知の母集団** (その月に運行が始まった乗務員の数)。本番で
    /// 過剰発火したとき「何名を測っているのか」がログから読めなかったので足した
    /// (Refs #205 の 37)。
    fn summary(&self) -> String {
        let f = self
            .first
            .map_or_else(|| "?".to_string(), |d| d.to_string());
        let l = self.last.map_or_else(|| "?".to_string(), |d| d.to_string());
        let (n, z, e) = (self.items, self.no_etag, self.expected);
        let m = self.per_driver_last.len();
        format!("n={n} etag無={z} {f}..{l} 期待={e} 対象={m}名")
    }
}

/// **入力の欠けを etags の一覧から見つける** (Refs #205 の 21)。
///
/// `fetch_etags` は月ゲートのために毎回引いているので、**追加の往復ゼロ・alc の
/// 改修ゼロ**で日別のカバー状況が作れる。見るのは 2 つ:
///
/// 1. `etag` が `None` = alc の DB に運行はあるのに R2 に `KUDGIVT.csv` が無い。
///    閾値の要らない確実な欠け (upload / split 未完了)
/// 2. 窓の末尾の空き。#205-19 が実証した「末尾の `dtako_events` が欠けると打刻の
///    無い勤務が黙って消える」形は、**運行そのものが alc の索引に無い**ので上流は
///    警告を出せない。こちらで「運行開始日が窓の端まで届いているか」を見るしかない
///
/// **進行中の月**は末尾に運行が無くて当然なので、期待する末尾は `today - 1 日`
/// までに切り下げる (`today` 当日は読み取りがまだ上がっていないのが普通)。
///
/// 文面には**実測値 ([`InputCoverage::summary`]) を必ず入れる** —
/// `FoldReport::warnings` は応答に出るので、呼び出し側がそれだけ見て判断できる形にする。
///
/// 誤検知は**安全側**に倒れる — warning が立つと月ゲートが封をしないので、最悪でも
/// 「毎回全量読みに戻る (遅いが正しい)」で済む。逆 (見逃し) は静かに間違う。
fn missing_input_warnings(cov: &InputCoverage) -> Vec<String> {
    let mut out = Vec::new();
    let s = cov.summary();
    if cov.no_etag > 0 {
        let n = cov.no_etag;
        out.push(format!("dtako 入力欠け: R2 に CSV の無い運行 {n} 件 ({s})"));
    }
    if cov.last.is_none() {
        out.push(format!("dtako 入力欠け: 運行開始日が 1 件も読めない ({s})"));
    } else if let Some((n, g)) = cov.tail_gap() {
        out.push(format!("dtako 入力欠け: 乗務員{n}名の末尾が{g}日超 ({s})"));
    }
    out
}

/// いまの日付 (JST)。窓の末尾の期待値を「進行中の月」で切り下げるためだけに使う。
fn today_jst() -> NaiveDate {
    let jst = chrono::FixedOffset::east_opt(crate::kintai_push::JST_OFFSET_SECONDS);
    chrono::Utc::now()
        .with_timezone(&jst.expect("JST offset is in range"))
        .date_naive()
}

/// `(unko_no, etag, driver_cds)` の一覧から月ゲートの dtako 側 digest を作る。
///
/// `etag` が `None` (R2 に無い / upload 未完了) の運行は空文字として畳む —
/// 存在しない扱いにして無視すると、**upload 中の運行がこっそり digest から
/// 抜け落ち**、揃った後もゲートが「変わっていない」と誤判定しうる。空文字で
/// 織り込めば、揃った瞬間に digest が変わってゲートが必ず外れる (安全側)。
///
/// `driver_cds` は digest に**含めない** — alc が新たに返し始めた時点で全月の
/// gate がいっせいに外れる (無駄な全量読み直し) のを避けるため。乗務員の同定は
/// [`InputCoverage`] の検知だけに使う。
fn digest_from_pairs(pairs: &[(String, Option<String>, Vec<String>)]) -> String {
    let mut lines: Vec<String> = pairs
        .iter()
        .map(|(unko_no, etag, _driver_cds)| format!("{unko_no}:{}", etag.as_deref().unwrap_or("")))
        .collect();
    lines.sort();
    let mut h = Sha256::new();
    h.update(lines.join("\n").as_bytes());
    format!("{:x}", h.finalize())
}

/// 日付の閉区間 `from..=to` を `max_days` 日以内の閉区間へ割る。`from > to` なら空。
fn date_chunks(from: NaiveDate, to: NaiveDate, max_days: i64) -> Vec<(NaiveDate, NaiveDate)> {
    let mut out = Vec::new();
    let mut start = from;
    while start <= to {
        let end = (start + chrono::Duration::days(max_days - 1)).min(to);
        out.push((start, end));
        start = end + chrono::Duration::days(1);
    }
    out
}

/// trait の `[from, to)` (`YYYY-MM-DD HH:MM:SS`) を、上流に投げる日付の閉区間へ。
fn query_dates(from: NaiveDateTime, to: NaiveDateTime) -> (NaiveDate, NaiveDate) {
    (
        from.date() - chrono::Duration::days(LOOKBACK_DAYS),
        (to - chrono::Duration::seconds(1)).date(),
    )
}

fn parse_window(from: &str, to: &str) -> Result<(NaiveDateTime, NaiveDateTime), KintaiRepoError> {
    let p = |s: &str| {
        NaiveDateTime::parse_from_str(s, OUT_DATETIME_FORMAT)
            .map_err(|e| KintaiRepoError::QueryFailed(format!("bad range {s:?}: {e}")))
    };
    Ok((p(from)?, p(to)?))
}

// ── repo 本体 ─────────────────────────────────────────────────────────────

/// `rust-alc-api` 経由の生イベント読み取り。
pub struct HttpKintaiEventsRepo {
    client: reqwest::Client,
    /// `{base_url}/api/dtako/events`。**const にせず struct フィールドに持つ** —
    /// テストが wiremock の URL を差し込めるようにするため。
    url: String,
    /// `{base_url}/api/dtako/events/etags`。同じ理由で struct フィールドに持つ。
    etags_url: String,
    tenant_id: String,
    token: Arc<dyn KintaiTokenProvider>,
    /// 上流に口が無い読み出し (打刻 / 運行の確定イベント / フェリー) の委譲先。
    /// 通常は MariaDB 実装。無い実行形態では該当分が欠ける (module docs 参照)。
    fallback: Option<DynKintaiEventsRepo>,
}

impl HttpKintaiEventsRepo {
    pub fn new(
        cfg: &KintaiEventsConfig,
        fallback: Option<DynKintaiEventsRepo>,
    ) -> Result<Self, String> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .build()
            .map_err(|e| format!("kintai events http client: {e}"))?;
        let base = cfg.base_url.trim_end_matches('/');
        Ok(Self {
            client,
            url: format!("{base}{EVENTS_PATH}"),
            etags_url: format!("{base}{ETAGS_PATH}"),
            tenant_id: cfg.tenant_id.clone(),
            token: build_token_provider(cfg)?,
            fallback,
        })
    }

    /// 1 往復。`params` は query string にそのまま載る。
    async fn get<T: DeserializeOwned>(
        &self,
        params: &[(&str, String)],
    ) -> Result<T, KintaiRepoError> {
        let mut req = self
            .client
            .get(&self.url)
            .query(params)
            .header("X-Tenant-ID", &self.tenant_id);
        if let Some(token) = self.token.token().await? {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-events request: {e}")))?;
        let status = resp.status();
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-events body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiRepoError::QueryFailed(format!(
                "alc dtako-events status {status}: {excerpt}"
            )));
        }
        serde_json::from_str(&body)
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-events parse: {e}")))
    }

    /// 上流から取った運行を写して `[from, to)` で絞る。`運行NO` で重複排除する。
    fn collect(
        &self,
        ops: Vec<UpstreamOperation>,
        seen: &mut HashSet<String>,
        window: (NaiveDateTime, NaiveDateTime),
        out: &mut Vec<RawEvent>,
    ) {
        for op in ops {
            if !op.unko_no.is_empty() && !seen.insert(op.unko_no.clone()) {
                continue;
            }
            let Some(cols) = EventCols::resolve(&op.headers) else {
                tracing::warn!(unko_no = %op.unko_no, "KUDGIVT に開始日時/イベント名が無い");
                continue;
            };
            for row in &op.rows {
                if let Some(ev) = row_to_event(&cols, row, &op.unko_no) {
                    if in_window(&ev, window.0, window.1) {
                        out.push(ev);
                    }
                }
            }
        }
    }

    /// 上流の warnings を握り潰さない (R2 の分割遅れで一部の運行が欠けたことが分かる)。
    ///
    /// `tracing` は運用ログ、[`record_warning`] は**呼び出し側の応答**への持ち出し。
    /// 畳んだ結果を返す口は「この計算の入力が欠けていたか」を出せないと、
    /// 欠けたまま最新として保存した値を静かに返すことになる。
    fn log_warnings(warnings: &[String]) {
        for w in warnings {
            tracing::warn!(warning = %w, "alc dtako-events warning");
            record_warning(w);
        }
    }

    /// 単一乗務員。上流は運行に相乗りした 2 名分の行を返すので `driver` で絞る
    /// (MariaDB の `WHERE e.対象乗務員CD = :driver` と同じ)。
    async fn fetch_one(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<RawEvent>, KintaiRepoError> {
        let window = parse_window(from, to)?;
        let (qf, qt) = query_dates(window.0, window.1);
        let want = driver as i64;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for (cf, ct) in date_chunks(qf, qt, MAX_RANGE_DAYS_SINGLE) {
            let resp: UpstreamSingle = self
                .get(&[
                    ("driver_cd", driver.to_string()),
                    ("date_from", cf.to_string()),
                    ("date_to", ct.to_string()),
                ])
                .await?;
            Self::log_warnings(&resp.warnings);
            self.collect(resp.operations, &mut seen, window, &mut out);
        }
        out.retain(|ev| ev.driver_id == Some(want));
        Ok(out)
    }

    /// 全乗務員。chunk × keyset ページングを回しきる。
    async fn fetch_all(&self, from: &str, to: &str) -> Result<Vec<RawEvent>, KintaiRepoError> {
        let window = parse_window(from, to)?;
        let (qf, qt) = query_dates(window.0, window.1);
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for (cf, ct) in date_chunks(qf, qt, MAX_RANGE_DAYS_ALL) {
            let mut after: Option<String> = None;
            for page in 0..=MAX_PAGES {
                if page == MAX_PAGES {
                    return Err(KintaiRepoError::QueryFailed(
                        "alc dtako-events paging did not terminate".to_string(),
                    ));
                }
                let mut params = vec![
                    ("date_from", cf.to_string()),
                    ("date_to", ct.to_string()),
                    ("page_size", PAGE_SIZE.to_string()),
                ];
                if let Some(ref a) = after {
                    params.push(("after_driver_cd", a.clone()));
                }
                let resp: UpstreamAll = self.get(&params).await?;
                Self::log_warnings(&resp.warnings);
                for group in resp.drivers {
                    self.collect(group.operations, &mut seen, window, &mut out);
                }
                match resp.next_after_driver_cd {
                    Some(next) => after = Some(next),
                    None => break,
                }
            }
        }
        Ok(out)
    }

    /// dtako 側の月ゲート材料を 1 往復で取る (Refs #205 実装計画 13)。
    ///
    /// **`Ok(None)` は「口が無い」の意味。** alc がまだ `GET /api/dtako/events/etags`
    /// を持たない環境 (404) では月ゲートそのものを諦める — 呼び出し側
    /// ([`crate::kintai_fold`]) が loud に warn した上で従来どおり全量読みへ degrade する。
    /// それ以外の失敗 (接続断・5xx・応答が壊れている) は `Err` にする — こちらは
    /// 「口はあるはずなのに読めなかった」なので、`None` に潰さず区別して伝える。
    async fn fetch_etags(
        &self,
        date_from: NaiveDate,
        date_to: NaiveDate,
    ) -> Result<Option<Vec<(String, Option<String>, Vec<String>)>>, KintaiRepoError> {
        let mut req = self
            .client
            .get(&self.etags_url)
            .query(&[
                ("date_from", date_from.to_string()),
                ("date_to", date_to.to_string()),
            ])
            .header("X-Tenant-ID", &self.tenant_id);
        if let Some(token) = self.token.token().await? {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-etags request: {e}")))?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = resp
            .text()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-etags body: {e}")))?;
        if !status.is_success() {
            let excerpt: String = body.chars().take(200).collect();
            return Err(KintaiRepoError::QueryFailed(format!(
                "alc dtako-etags status {status}: {excerpt}"
            )));
        }
        let parsed: UpstreamEtags = serde_json::from_str(&body)
            .map_err(|e| KintaiRepoError::QueryFailed(format!("alc dtako-etags parse: {e}")))?;
        Self::log_warnings(&parsed.warnings);
        record_unsplit(parsed.unsplit, parsed.unsplit_total);
        let pairs: Vec<(String, Option<String>, Vec<String>)> = parsed
            .items
            .into_iter()
            .map(|it| (it.unko_no, it.etag, it.driver_cds))
            .collect();
        // 突合 (Refs #205 の 37) 用に GCP 側の運行一覧を控える。**`items` の使い方も
        // digest の材料も変えない** — 読むだけで、月ゲートの指紋には一切触らない
        record_etag_unko_nos(pairs.iter().map(|(u, _, _)| u.clone()).collect());
        Ok(Some(pairs))
    }
}

#[async_trait]
impl KintaiEventsApi for HttpKintaiEventsRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let events = self.fetch_one(from, to, driver).await?;
        let mut rows: Vec<serde_json::Value> = events.into_iter().map(event_to_json).collect();
        if let Some(fb) = &self.fallback {
            let borrowed = fb.fetch_events_between(from, to, driver).await?;
            rows.extend(borrowed.into_iter().filter(is_borrowed_source));
        }
        sort_rows(&mut rows);
        Ok(rows)
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let events = self.fetch_all(from, to).await?;
        let mut rows: Vec<serde_json::Value> = events.into_iter().map(event_to_all_json).collect();
        if let Some(fb) = &self.fallback {
            let borrowed = fb.fetch_all_events_between(from, to).await?;
            rows.extend(borrowed.into_iter().filter(is_borrowed_source));
        }
        sort_rows_by_driver(&mut rows);
        Ok(rows)
    }

    /// フェリー区間は上流に口が無い。突合はオンプレ専用 (#205 の決定 8) なので、
    /// `fallback` が無ければ `NotConfigured` = 503 で fail-closed にする。
    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        match &self.fallback {
            Some(fb) => fb.fetch_ferry_between(from, to, driver).await,
            None => Err(KintaiRepoError::NotConfigured),
        }
    }

    async fn fetch_dtako_month_digest(
        &self,
        month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        let (from, to) = month_etags_bounds(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        let pairs = self.fetch_etags(from, to).await?;
        // 入力の欠けは alc からは見えない (索引に無い運行は warnings に出ない) ので、
        // 引いてきた一覧の形から自分で見つけて `record_warning` へ流す (Refs #205 の 21)
        // `None` (alc に口が無い) は「欠けている」ではなく「判定できない」— 検知しない
        if let Some(p) = pairs.as_deref() {
            let cov = InputCoverage::measure(p, from, to, today_jst());
            // **警告の有無に関わらず毎回出す** (閾値を後から締めるための実測値)。
            // マクロは 1 行に収める (CLAUDE.md — 折り返すと行カバレッジに乗らない)
            let (gap, cover) = (cov.gap_days().unwrap_or(-1), cov.summary());
            tracing::info!(gap, %cover, "kintai dtako input coverage");
            // alc がまだ driver_cds を返さない環境の切り分け用 (Refs #205 の 32)
            if !cov.driver_cd_source {
                tracing::info!("kintai dtako driver_cds 未対応: 月ぜんたい判定にフォールバック");
            }
            for w in missing_input_warnings(&cov) {
                tracing::warn!(warning = %w, "kintai dtako input gap");
                record_warning(&w);
            }
        }
        Ok(pairs.map(|p| digest_from_pairs(&p)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, OUT_DATETIME_FORMAT).unwrap()
    }

    fn headers(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    fn row(list: &[&str]) -> Vec<String> {
        list.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn resolve_prefers_taisho_driver_cd() {
        // 乗務員CD1 は運行の主運転者で全行同じ値。優先すると 2 名乗務で取り違える
        let h = headers(&[
            "運行NO",
            "乗務員CD1",
            "対象乗務員CD",
            "開始日時",
            "イベント名",
        ]);
        let cols = EventCols::resolve(&h).unwrap();
        assert_eq!(cols.driver_cd, Some(2), "対象乗務員CD (index 2) を採る");
    }

    #[test]
    fn resolve_falls_back_to_driver_cd1() {
        let h = headers(&["運行NO", "乗務員CD1", "開始日時", "イベント名"]);
        let cols = EventCols::resolve(&h).unwrap();
        assert_eq!(cols.driver_cd, Some(1));
        assert_eq!(cols.end_at, None);
        assert_eq!(cols.vehicle, None);
    }

    #[test]
    fn resolve_allows_missing_driver_columns() {
        let h = headers(&["開始日時", "イベント名"]);
        let cols = EventCols::resolve(&h).unwrap();
        assert_eq!(cols.driver_cd, None);
        assert_eq!(cols.unko_no, None);
    }

    #[test]
    fn resolve_requires_start_and_event_name() {
        assert!(EventCols::resolve(&headers(&["運行NO", "イベント名"])).is_none());
        assert!(EventCols::resolve(&headers(&["運行NO", "開始日時"])).is_none());
    }

    #[test]
    fn resolve_trims_header_whitespace() {
        let h = headers(&[" 開始日時 ", " イベント名"]);
        assert!(EventCols::resolve(&h).is_some());
    }

    #[test]
    fn row_to_event_maps_every_column() {
        let h = headers(&[
            "運行NO",
            "車輌名",
            "対象乗務員CD",
            "開始日時",
            "終了日時",
            "イベント名",
        ]);
        let cols = EventCols::resolve(&h).unwrap();
        let ev = row_to_event(
            &cols,
            &row(&[
                "2602241025060000000272",
                "帯広100け272",
                "1130",
                "2026/02/24 14:40:56",
                "2026/02/25 09:23:56",
                "休息",
            ]),
            "ignored",
        )
        .unwrap();
        assert_eq!(ev.start, dt("2026-02-24 14:40:56"));
        assert_eq!(ev.end, Some(dt("2026-02-25 09:23:56")));
        assert_eq!(ev.driver_id, Some(1130));
        assert_eq!(ev.state.as_deref(), Some("休息"));
        assert_eq!(ev.unko_no.as_deref(), Some("2602241025060000000272"));
        assert_eq!(ev.vehicle.as_deref(), Some("帯広100け272"));
    }

    #[test]
    fn row_to_event_accepts_space_padded_hour() {
        let cols = EventCols::resolve(&headers(&["開始日時", "イベント名"])).unwrap();
        let ev = row_to_event(&cols, &row(&["2026/03/01  9:05:00", "運転"]), "").unwrap();
        assert_eq!(ev.start, dt("2026-03-01 09:05:00"));
    }

    #[test]
    fn row_to_event_drops_rows_without_usable_start() {
        let cols = EventCols::resolve(&headers(&["開始日時", "イベント名"])).unwrap();
        assert!(row_to_event(&cols, &row(&["", "運転"]), "").is_none());
        assert!(row_to_event(&cols, &row(&["INVALID", "運転"]), "").is_none());
        // 列が足りない短い行も落とす
        assert!(row_to_event(&cols, &row(&[]), "").is_none());
    }

    #[test]
    fn row_to_event_keeps_nulls_and_borrows_unko_no() {
        let h = headers(&[
            "対象乗務員CD",
            "開始日時",
            "終了日時",
            "イベント名",
            "車輌名",
        ]);
        let cols = EventCols::resolve(&h).unwrap();
        // 空欄・壊れた値は 0 や空文字に化かさず null にする
        let ev = row_to_event(
            &cols,
            &row(&["", "2026/03/01 08:00:00", "bad", "", ""]),
            "OP-1",
        )
        .unwrap();
        assert_eq!(ev.driver_id, None);
        assert_eq!(ev.end, None);
        assert_eq!(ev.state, None);
        assert_eq!(ev.vehicle, None);
        // 運行NO 列が無い CSV は運行のメタデータから借りる
        assert_eq!(ev.unko_no.as_deref(), Some("OP-1"));
    }

    #[test]
    fn row_to_event_leaves_unko_no_null_when_unknown() {
        let cols = EventCols::resolve(&headers(&["開始日時", "イベント名"])).unwrap();
        let ev = row_to_event(&cols, &row(&["2026/03/01 08:00:00", "運転"]), "").unwrap();
        assert_eq!(ev.unko_no, None);
    }

    #[test]
    fn row_to_event_ignores_non_numeric_driver_cd() {
        let h = headers(&["乗務員CD1", "開始日時", "イベント名"]);
        let cols = EventCols::resolve(&h).unwrap();
        let ev = row_to_event(&cols, &row(&["DR01", "2026/03/01 08:00:00", "運転"]), "").unwrap();
        assert_eq!(ev.driver_id, None, "数値でない CD は null (0 に化かさない)");
    }

    fn ev(start: &str, end: Option<&str>) -> RawEvent {
        RawEvent {
            start: dt(start),
            end: end.map(dt),
            driver_id: Some(1),
            state: Some("休息".to_string()),
            unko_no: None,
            vehicle: None,
        }
    }

    #[test]
    fn in_window_matches_the_two_sql_branches() {
        let (f, t) = (dt("2026-07-01 00:00:00"), dt("2026-08-02 00:00:00"));
        // 期間内に始まる
        assert!(in_window(&ev("2026-07-01 00:00:00", None), f, t));
        assert!(in_window(&ev("2026-08-01 23:59:59", None), f, t));
        // 期間内に終わる (開始は期間より前) — 月初の勤務を組むのに要る
        assert!(in_window(
            &ev("2026-06-30 22:00:00", Some("2026-07-01 06:00:00")),
            f,
            t
        ));
        // 外
        assert!(!in_window(&ev("2026-08-02 00:00:00", None), f, t));
        assert!(!in_window(&ev("2026-06-30 22:00:00", None), f, t));
        assert!(!in_window(
            &ev("2026-06-01 00:00:00", Some("2026-06-02 00:00:00")),
            f,
            t
        ));
        // 終了が期間の外
        assert!(!in_window(
            &ev("2026-06-30 22:00:00", Some("2026-08-02 00:00:00")),
            f,
            t
        ));
    }

    #[test]
    fn json_shapes_match_the_mariadb_rows() {
        let e = RawEvent {
            start: dt("2026-06-02 06:00:00"),
            end: Some(dt("2026-06-02 06:20:00")),
            driver_id: Some(1119),
            state: Some("休憩".to_string()),
            unko_no: Some("OP-1".to_string()),
            vehicle: Some("帯広100け272".to_string()),
        };
        let single = event_to_json(e.clone());
        assert_eq!(single["datetime"], "2026-06-02 06:00:00");
        assert_eq!(single["end_datetime"], "2026-06-02 06:20:00");
        assert_eq!(single["driver_id"], 1119);
        assert_eq!(single["source"], "dtako_events");
        assert_eq!(single["state"], "休憩");
        assert_eq!(single["unko_no"], "OP-1");
        assert_eq!(single["vehicle"], "帯広100け272");

        // 全乗務員版は読んでいない列をキーごと出さない
        let all = event_to_all_json(e);
        assert!(all.get("unko_no").is_none());
        assert!(all.get("vehicle").is_none());
        assert_eq!(all["state"], "休憩");
    }

    #[test]
    fn json_keeps_nulls() {
        let v = event_to_json(RawEvent {
            start: dt("2026-06-02 06:00:00"),
            end: None,
            driver_id: None,
            state: None,
            unko_no: None,
            vehicle: None,
        });
        assert!(v["end_datetime"].is_null());
        assert!(v["driver_id"].is_null());
        assert!(v["state"].is_null());
        assert!(v["unko_no"].is_null());
        assert!(v["vehicle"].is_null());
    }

    #[test]
    fn sorting_matches_the_sql_order_by() {
        let mut rows = vec![
            serde_json::json!({"datetime": "2026-06-02 08:00:00", "source": "timecard", "driver_id": 2}),
            serde_json::json!({"datetime": "2026-06-02 07:00:00", "source": "dtako_events", "driver_id": 2}),
            serde_json::json!({"datetime": "2026-06-02 08:00:00", "source": "dtako_events", "driver_id": 1}),
        ];
        sort_rows(&mut rows);
        assert_eq!(rows[0]["datetime"], "2026-06-02 07:00:00");
        assert_eq!(rows[1]["source"], "dtako_events", "同時刻は source 順");
        assert_eq!(rows[2]["source"], "timecard");

        sort_rows_by_driver(&mut rows);
        assert_eq!(rows[0]["driver_id"], 1, "全乗務員版は driver_id が先");
        assert_eq!(rows[1]["datetime"], "2026-06-02 07:00:00");
        assert_eq!(rows[2]["datetime"], "2026-06-02 08:00:00");
    }

    #[test]
    fn sorting_puts_null_driver_first() {
        let mut rows = vec![
            serde_json::json!({"datetime": "2026-06-02 08:00:00", "source": "dtako_events", "driver_id": 1}),
            serde_json::json!({"datetime": "2026-06-02 07:00:00", "source": "dtako_events"}),
        ];
        sort_rows_by_driver(&mut rows);
        assert!(rows[0]["driver_id"].is_null());
    }

    #[test]
    fn borrowed_source_excludes_dtako_events() {
        assert!(is_borrowed_source(
            &serde_json::json!({"source": "timecard"})
        ));
        assert!(is_borrowed_source(&serde_json::json!({"source": "dtako"})));
        assert!(is_borrowed_source(&serde_json::json!({})));
        assert!(!is_borrowed_source(
            &serde_json::json!({"source": "dtako_events"})
        ));
    }

    #[test]
    fn date_chunks_splits_at_the_upstream_limit() {
        // 全乗務員版の上限 31 日。month_range は 32〜34 日になるので必ず割れる
        let chunks = date_chunks(d(2026, 6, 29), d(2026, 8, 1), MAX_RANGE_DAYS_ALL);
        assert_eq!(
            chunks,
            vec![
                (d(2026, 6, 29), d(2026, 7, 29)),
                (d(2026, 7, 30), d(2026, 8, 1)),
            ]
        );
        // ちょうど上限なら 1 本
        assert_eq!(date_chunks(d(2026, 7, 1), d(2026, 7, 31), 31).len(), 1);
        // 単一乗務員の上限では 1 か月は割れない
        assert_eq!(
            date_chunks(d(2026, 6, 29), d(2026, 8, 1), MAX_RANGE_DAYS_SINGLE).len(),
            1
        );
        // 逆転は空
        assert!(date_chunks(d(2026, 7, 2), d(2026, 7, 1), 31).is_empty());
    }

    #[test]
    fn month_etags_bounds_covers_month_range_exactly() {
        // month_range("2026-07") は [07-01 00:00, 08-02 00:00) = 7 月 + 翌月 1 日ぶん。
        // 閉区間の date_to は排他境界の 1 日前 = 08-01
        let (from, to) = month_etags_bounds("2026-07").unwrap();
        assert_eq!(from, d(2026, 7, 1));
        assert_eq!(to, d(2026, 8, 1));
    }

    #[test]
    fn month_etags_bounds_rolls_over_the_year() {
        let (from, to) = month_etags_bounds("2026-12").unwrap();
        assert_eq!(from, d(2026, 12, 1));
        assert_eq!(to, d(2027, 1, 1));
    }

    #[test]
    fn month_etags_bounds_rejects_garbage() {
        assert!(month_etags_bounds("").is_none());
        assert!(month_etags_bounds("2026-13").is_none());
        assert!(month_etags_bounds("nope").is_none());
    }

    #[test]
    fn digest_from_pairs_is_order_independent() {
        // 入力の並びが違っても sort してから畳むので同じ digest になる
        let a = digest_from_pairs(&[
            ("U1".to_string(), Some("etag1".to_string()), vec![]),
            ("U2".to_string(), Some("etag2".to_string()), vec![]),
        ]);
        let b = digest_from_pairs(&[
            ("U2".to_string(), Some("etag2".to_string()), vec![]),
            ("U1".to_string(), Some("etag1".to_string()), vec![]),
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn digest_from_pairs_changes_when_an_etag_changes() {
        let before = digest_from_pairs(&[("U1".to_string(), Some("etag1".to_string()), vec![])]);
        let after = digest_from_pairs(&[("U1".to_string(), Some("etag1-new".to_string()), vec![])]);
        assert_ne!(before, after);
    }

    #[test]
    fn digest_from_pairs_treats_missing_etag_as_distinct_from_present() {
        // None (R2 に未着) と Some("") はどちらも空文字として畳まれるが、
        // None と Some(other) は必ず違う digest になる (揃った瞬間に gate が外れる)
        let missing = digest_from_pairs(&[("U1".to_string(), None, vec![])]);
        let present = digest_from_pairs(&[("U1".to_string(), Some("etag1".to_string()), vec![])]);
        assert_ne!(missing, present);
        let empty_string = digest_from_pairs(&[("U1".to_string(), Some(String::new()), vec![])]);
        assert_eq!(missing, empty_string, "None は空文字と同じ畳み方");
    }

    /// digest は `driver_cds` を含めない — alc が新たに返し始めても、既存の gate が
    /// いっせいに外れて無駄な全量読み直しが起きないようにするため (Refs #205 の 32)。
    #[test]
    fn digest_from_pairs_ignores_driver_cds() {
        let without = digest_from_pairs(&[("U1".to_string(), Some("etag1".to_string()), vec![])]);
        let with = digest_from_pairs(&[(
            "U1".to_string(),
            Some("etag1".to_string()),
            vec!["D1".to_string()],
        )]);
        assert_eq!(without, with, "driver_cds の有無で digest が変わらない");
    }

    #[test]
    fn digest_from_pairs_of_empty_input_is_deterministic() {
        assert_eq!(digest_from_pairs(&[]), digest_from_pairs(&[]));
    }

    #[test]
    fn query_dates_widens_backwards_and_excludes_the_upper_bound() {
        let (f, t) = query_dates(dt("2026-07-01 00:00:00"), dt("2026-08-02 00:00:00"));
        // 期間内に終わる区間 (開始は期間より前) を拾うため 2 日遡る
        assert_eq!(f, d(2026, 6, 29));
        // 上端は排他なので 08-02 00:00:00 は 08-01 まで
        assert_eq!(t, d(2026, 8, 1));
    }

    #[test]
    fn parse_window_rejects_garbage() {
        assert!(parse_window("2026-07-01 00:00:00", "2026-08-02 00:00:00").is_ok());
        let err = parse_window("nope", "2026-08-02 00:00:00").unwrap_err();
        assert!(err.to_string().contains("bad range"), "{err}");
        assert!(parse_window("2026-07-01 00:00:00", "nope").is_err());
    }

    #[test]
    fn find_col_trims() {
        assert_eq!(find_col(&headers(&["a", " b "]), "b"), Some(1));
        assert_eq!(find_col(&headers(&["a"]), "z"), None);
    }

    // ── token provider ──

    #[tokio::test]
    async fn no_token_provider_returns_none() {
        assert_eq!(NoTokenProvider.token().await.unwrap(), None);
    }

    #[tokio::test]
    async fn static_token_provider_returns_the_value() {
        let p = StaticTokenProvider("tok".to_string());
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn command_token_provider_runs_and_caches() {
        let p = CommandTokenProvider::new("echo id-token-value", 3600).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
        // 2 回目は TTL 内なのでキャッシュから返る
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
    }

    #[tokio::test]
    async fn command_token_provider_refetches_after_ttl() {
        let p = CommandTokenProvider::new("echo tok", 0).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn command_token_provider_is_loud_about_failures() {
        // 起動できないコマンド
        let err = CommandTokenProvider::new("/nonexistent/token-cmd", 60)
            .unwrap()
            .token()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("auth token command"), "{err}");

        // 非 0 終了
        let err = CommandTokenProvider::new("false", 60)
            .unwrap()
            .token()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("failed"), "{err}");

        // 何も出さない = token 無しで叩いて 401 になるより先に落とす
        let err = CommandTokenProvider::new("true", 60)
            .unwrap()
            .token()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("no output"), "{err}");

        assert!(CommandTokenProvider::new("   ", 60).is_err());
    }

    #[test]
    fn build_token_provider_picks_the_configured_route() {
        let mut cfg = KintaiEventsConfig::default();
        assert!(build_token_provider(&cfg).is_ok());
        cfg.auth_token_command = "gcloud auth print-identity-token".to_string();
        assert!(build_token_provider(&cfg).is_ok());
        cfg.auth_token = "tok".to_string();
        assert!(build_token_provider(&cfg).is_ok());
    }

    // ── metadata server (Cloud Run の中で使える唯一の経路) ──

    #[test]
    fn urlencode_escapes_everything_outside_the_unreserved_set() {
        // audience は URL なので `:` と `/` が必ず出る
        assert_eq!(urlencode("https://x.run.app"), "https%3A%2F%2Fx.run.app");
        // RFC 3986 の unreserved はそのまま
        assert_eq!(urlencode("aZ0-_.~"), "aZ0-_.~");
    }

    #[test]
    fn metadata_provider_needs_an_audience() {
        // audience が無いと Cloud Run IAM が「自分宛か」を判定できず必ず 401 になる。
        // 起動時に落とす
        let err = MetadataTokenProvider::new(METADATA_BASE_URL, "   ", 60).unwrap_err();
        assert!(err.contains("audience"), "{err}");
        assert!(MetadataTokenProvider::new(METADATA_BASE_URL, "https://x", 60).is_ok());
    }

    #[test]
    fn metadata_base_url_is_the_documented_host() {
        assert_eq!(METADATA_BASE_URL, "http://metadata.google.internal");
    }

    #[tokio::test]
    async fn metadata_provider_sends_the_flavor_header_and_caches() {
        use wiremock::matchers::{header, method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(METADATA_IDENTITY_PATH))
            // audience は base_url そのまま (末尾 `/` は落とす)
            .and(query_param("audience", "https://alc.example"))
            // これが無いと metadata server は 403 を返す
            .and(header(METADATA_FLAVOR.0, METADATA_FLAVOR.1))
            .respond_with(ResponseTemplate::new(200).set_body_string("  id-token-value\n"))
            .expect(1)
            .mount(&server)
            .await;

        let p = MetadataTokenProvider::new(&server.uri(), "https://alc.example/", 3600).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
        // 2 回目は TTL 内なのでキャッシュから返る (expect(1) が保証)
        assert_eq!(p.token().await.unwrap().as_deref(), Some("id-token-value"));
    }

    #[tokio::test]
    async fn metadata_provider_refetches_after_ttl() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("tok"))
            .expect(2)
            .mount(&server)
            .await;

        let p = MetadataTokenProvider::new(&server.uri(), "https://alc.example", 0).unwrap();
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
        assert_eq!(p.token().await.unwrap().as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn metadata_provider_is_loud_about_failures() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        // 403 = Metadata-Flavor 忘れ / metadata server が居ない環境
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(403).set_body_string("Forbidden"))
            .mount(&server)
            .await;
        let p = MetadataTokenProvider::new(&server.uri(), "https://alc.example", 60).unwrap();
        let err = p.token().await.unwrap_err();
        assert!(err.to_string().contains("403"), "{err}");

        // 200 だが空 — token 無しで叩いて 401 になるより先に落とす
        let empty = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("  \n"))
            .mount(&empty)
            .await;
        let p = MetadataTokenProvider::new(&empty.uri(), "https://alc.example", 60).unwrap();
        let err = p.token().await.unwrap_err();
        assert!(err.to_string().contains("no token"), "{err}");

        // 繋がらない先
        let p =
            MetadataTokenProvider::new("http://127.0.0.1:1", "https://alc.example", 60).unwrap();
        let err = p.token().await.unwrap_err();
        assert!(err.to_string().contains("metadata identity"), "{err}");
    }

    /// **上流 warnings が呼び出し側まで届く。**
    ///
    /// R2 の分割遅れ (`NoSuchKey`) の最中に畳むと、欠けた入力を「最新」として
    /// 保存する。`tracing` に落とすだけでは応答から見えない。
    #[tokio::test]
    async fn upstream_warnings_come_back_to_the_caller() {
        let (out, warnings) = with_warning_sink(async {
            HttpKintaiEventsRepo::log_warnings(&[
                "NoSuchKey: 1234/KUDGIVT.csv".to_string(),
                "NoSuchKey: 5678/KUDGIVT.csv".to_string(),
            ]);
            42
        })
        .await;
        assert_eq!(out, 42, "中身の戻り値はそのまま");
        assert_eq!(warnings.len(), 2);
        assert!(warnings[0].contains("NoSuchKey"), "{warnings:?}");
    }

    /// **同じ文面は 1 回だけ、頭だけ。** 月ぶんの運行が全部欠けると 1,100 本並ぶ。
    #[tokio::test]
    async fn warnings_are_deduped_and_capped() {
        let (_, warnings) = with_warning_sink(async {
            let same = vec!["same".to_string(); 3];
            HttpKintaiEventsRepo::log_warnings(&same);
            let many: Vec<String> = (0..MAX_COLLECTED_WARNINGS + 10)
                .map(|i| format!("w{i}"))
                .collect();
            HttpKintaiEventsRepo::log_warnings(&many);
        })
        .await;
        assert_eq!(warnings.iter().filter(|w| *w == "same").count(), 1);
        assert_eq!(warnings.len(), MAX_COLLECTED_WARNINGS);
    }

    /// **集めていないときは何も起きない。** 大半の呼び出しがこちら。
    #[test]
    fn warnings_outside_a_sink_are_dropped() {
        HttpKintaiEventsRepo::log_warnings(&["orphan".to_string()]);
        assert!(collected_warnings().is_empty());
    }

    /// [`record_warning_for_test`] は private な [`record_warning`] へそのまま
    /// 委譲するだけ — 実装計画 13 のテストが他モジュールから warnings を
    /// 疑似発火するための穴。
    #[tokio::test]
    async fn record_warning_for_test_delegates_to_the_real_sink() {
        let (_, warnings) = with_warning_sink(async {
            record_warning_for_test("from another module");
        })
        .await;
        assert_eq!(warnings, vec!["from another module".to_string()]);
    }

    /// **`warnings_seen()` の 3 分岐** (Refs #205-17)。`recalc_month` が
    /// `write_fold_gate` を書いてよいかの判断に使う — `Some(false)` だけが書いてよい。
    #[tokio::test]
    async fn warnings_seen_reports_all_three_states() {
        // 収集器の外 — 「分からない」
        assert_eq!(warnings_seen(), None);

        // 収集器の中・warnings ゼロ — 書いてよい
        let (seen_empty, _) = with_warning_sink(async { warnings_seen() }).await;
        assert_eq!(seen_empty, Some(false));

        // 収集器の中・warnings あり — 書かない
        let (seen_some, _) = with_warning_sink(async {
            record_warning_for_test("R2 分割遅れ");
            warnings_seen()
        })
        .await;
        assert_eq!(seen_some, Some(true));
    }

    // ── `unsplit` の素通し (Refs #205 の 32) ────────────────────────────────

    fn unsplit_op(unko_no: &str, driver_cd: &str, reading_date: &str) -> UnsplitOperation {
        UnsplitOperation {
            unko_no: unko_no.to_string(),
            driver_cd: driver_cd.to_string(),
            reading_date: reading_date.to_string(),
        }
    }

    /// `record_unsplit` で記録した内容がそのまま `with_unsplit_sink` の戻り値に乗る。
    #[tokio::test]
    async fn unsplit_recorded_inside_the_sink_comes_back_to_the_caller() {
        let (out, items, total) = with_unsplit_sink(async {
            record_unsplit(vec![unsplit_op("U1", "D1", "2026-06-15")], 3);
            42
        })
        .await;
        assert_eq!(out, 42, "中身の戻り値はそのまま");
        assert_eq!(items, vec![unsplit_op("U1", "D1", "2026-06-15")]);
        assert_eq!(total, 3);
    }

    /// 記録しなければ空・0 のまま (alc がまだ `unsplit` を返さない環境相当)。
    #[tokio::test]
    async fn unsplit_sink_defaults_to_empty_when_nothing_is_recorded() {
        let (_, items, total) = with_unsplit_sink(async {}).await;
        assert!(items.is_empty());
        assert_eq!(total, 0);
    }

    /// **集めていないときは何も起きない。**
    #[test]
    fn record_unsplit_outside_a_sink_is_dropped() {
        record_unsplit(vec![unsplit_op("U1", "D1", "2026-06-15")], 1);
        assert_eq!(collected_unsplit(), (Vec::new(), 0));
    }

    // ── 入力欠けの検知 (Refs #205 の 21) ──────────────────────────────────

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

    /// `driver_cds` 無し (alc がまだ返さない現行環境相当)。
    fn pair(unko_no: &str, etag: Option<&str>) -> Pair {
        (unko_no.to_string(), etag.map(str::to_string), Vec::new())
    }

    /// `driver_cds` 1 名 (Refs #205 の 32 が alc に足す前方互換フィールド)。
    fn driver_pair(unko_no: &str, etag: Option<&str>, driver_cd: &str) -> Pair {
        crew_pair(unko_no, etag, &[driver_cd])
    }

    /// `driver_cds` 複数名 — 1 運行に主 / 副運転が付くケース (Refs #205 の 32)。
    fn crew_pair(unko_no: &str, etag: Option<&str>, driver_cds: &[&str]) -> Pair {
        (
            unko_no.to_string(),
            etag.map(str::to_string),
            driver_cds.iter().map(|s| s.to_string()).collect(),
        )
    }

    type Pair = (String, Option<String>, Vec<String>);

    /// 母集団の絞り込み (Refs #205 の 37) を効かせない旧来の呼び方。窓の下端を
    /// 十分古い日に置くので、末尾検知そのものの規則だけを見る形になる。
    fn warns(pairs: &[Pair], end: NaiveDate, today: NaiveDate) -> Vec<String> {
        warns_in(pairs, d(2000, 1, 1), end, today)
    }

    /// 窓 `[start, end]` を明示する版 (母集団の確認用)。
    fn warns_in(pairs: &[Pair], start: NaiveDate, end: NaiveDate, today: NaiveDate) -> Vec<String> {
        missing_input_warnings(&InputCoverage::measure(pairs, start, end, today))
    }

    /// **揃っている月は静か。** 窓の端まで運行開始が届いていれば warning ゼロ。
    #[test]
    fn missing_input_warnings_is_silent_when_the_window_is_covered() {
        let pairs = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26070110000000000023021", Some("e2")),
        ];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert!(w.is_empty(), "窓の端 (07-01) まで在るので静か: {w:?}");
    }

    /// **実測の自然な空き (1 日) では立たない。** 閾値の余裕ぶんの確認。
    #[test]
    fn missing_input_warnings_tolerates_the_measured_natural_tail_gap() {
        let pairs = vec![pair("25123110000000000023021", Some("e1"))];
        let w = warns(&pairs, d(2026, 1, 1), d(2026, 1, 20));
        assert!(w.is_empty(), "年末の 1 日空きは自然: {w:?}");
    }

    /// **#205-19 が実証した形。** 末尾の運行が丸ごと欠けたら立つ。
    /// 文面には実測値 (不足日数と covered の範囲) が入る。
    #[test]
    fn missing_input_warnings_fires_when_the_tail_of_the_month_is_missing() {
        let pairs = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26062310000000000023021", Some("e2")),
        ];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1, "末尾 8 日ぶんの欠け: {w:?}");
        // driver_cd 無しなので全員 1 グループにまとまり、その 1 グループが超過する
        assert!(
            w[0].contains("乗務員1名の末尾が8日超"),
            "不足日数を書く: {w:?}"
        );
        assert!(w[0].contains("2026-06-01..2026-06-23"), "範囲を書く: {w:?}");
        assert!(
            w[0].contains("期待=2026-07-01"),
            "期待した末尾も書く: {w:?}"
        );
    }

    /// **閾値 7 日の境目を両側から縛る** (Refs #205 の 37)。
    ///
    /// 乗務員別に割った以上、**土日を挟むだけで gap は 3〜4 日**になる — 2 日の
    /// ままでは本番 2026-06 で 113 名中 72 名が鳴り続けた。7 日は「週末 + 1 日」で、
    /// **実際に欠けている 1078 / 1517 / 1688 (gap 8 前後) は拾える**位置。
    #[test]
    fn the_tail_gap_threshold_tolerates_a_weekend_but_catches_the_real_gaps() {
        // 金曜まで走って月曜の朝に測る形 = gap 4 日。業務として正常
        let weekend = vec![driver_pair("26062610000000000023021", Some("e1"), "D1")];
        let w = warns_in(&weekend, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "週末ぶんの空きでは鳴らない: {w:?}");

        // ちょうど 7 日はまだ許容 (閾値は「超えたら」)
        let exactly = vec![driver_pair("26062310000000000023021", Some("e1"), "D1")];
        let w = warns_in(&exactly, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "gap 7 日ちょうどは境界の内側: {w:?}");

        // 1078 の実際の欠け (最終運行 06-24 が丸ごと無く 06-22 で切れる) = gap 8 日
        let real = vec![driver_pair("26062210000000000023021", Some("e1"), "D1")];
        let w = warns_in(&real, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w.len(), 1, "本物の欠けは拾う: {w:?}");
        assert!(w[0].contains("乗務員1名の末尾が8日超"), "{w:?}");
    }

    /// **進行中の月は末尾に運行が無くて当然。** 期待値を `today - 1` に切り下げる。
    #[test]
    fn missing_input_warnings_clamps_the_expectation_to_yesterday() {
        let pairs = vec![pair("26070310000000000023021", Some("e1"))];
        // 7 月を 07-04 に畳む — 窓の端 (08-01) はまだ来ていない
        let w = warns(&pairs, d(2026, 8, 1), d(2026, 7, 4));
        assert!(w.is_empty(), "当月の末尾の空きは欠けではない: {w:?}");
    }

    /// **#205-32 の本題。** 他の乗務員が窓の端まで在ると、月・全乗務員通しの
    /// `last` はそこに張り付き、1 名だけの末尾欠けが埋もれて沈黙する。
    /// `driver_cd` 無し (現行相当) では鳴らず、`driver_cd` 有り (乗務員別) では鳴る。
    #[test]
    fn missing_input_warnings_catches_a_single_drivers_tail_gap_when_others_cover_the_window() {
        // D1 / D2 は窓の端 (06-30) まで在る。D3 は 06-20 で切れている (10 日不足)。
        let no_driver = vec![
            pair("26063010000000000023021", Some("e1")),
            pair("26063010000000000023022", Some("e2")),
            pair("26062010000000000023023", Some("e3")),
        ];
        let w_old = warns(&no_driver, d(2026, 6, 30), d(2026, 7, 5));
        assert!(
            w_old.is_empty(),
            "driver_cd 無し (現行相当) は沈黙: {w_old:?}"
        );

        let with_driver = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26063010000000000023022", Some("e2"), "D2"),
            driver_pair("26062010000000000023023", Some("e3"), "D3"),
        ];
        let w_new = warns(&with_driver, d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w_new.len(), 1, "{w_new:?}");
        assert!(
            w_new[0].contains("乗務員1名の末尾が10日超"),
            "D3 だけ検出する: {w_new:?}"
        );
    }

    /// 全乗務員が窓の端まで揃っていれば (driver_cd 有りでも) 鳴らない。
    #[test]
    fn missing_input_warnings_is_silent_when_every_driver_covers_the_window() {
        let pairs = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26063010000000000023022", Some("e2"), "D2"),
            // 1 日の空きは実測の自然な揺らぎとして許容範囲内 (MAX_TAIL_GAP_DAYS = 7)
            driver_pair("26062910000000000023023", Some("e3"), "D3"),
        ];
        let w = warns(&pairs, d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "全員揃っているので静か: {w:?}");
    }

    /// 進行中の月の `today - 1 日` への切り下げは、乗務員別に割っても効く。
    #[test]
    fn missing_input_warnings_clamps_the_expectation_to_yesterday_per_driver() {
        // 7 月を対象、today=07-04 → 期待は 07-03 に切り下がる。D1/D2 とも
        // そこまで在るので静か (1 名だけの窓だと偶然揃うのと区別が付かないため 2 名で確認)。
        let pairs = vec![
            driver_pair("26070310000000000023021", Some("e1"), "D1"),
            driver_pair("26070310000000000023022", Some("e2"), "D2"),
        ];
        let w = warns(&pairs, d(2026, 8, 1), d(2026, 7, 4));
        assert!(w.is_empty(), "乗務員別でも当月クランプが効く: {w:?}");
    }

    /// **1 運行に複数の乗務員 (主 / 副運転) が付く場合、その開始日は両方の `last`
    /// に寄与する。** `list_operations_for_drivers` の `DISTINCT ON (driver_id,
    /// unko_no)` により同じ `unko_no` が複数 `driver_cd` に紐づき得るため。
    #[test]
    fn missing_input_warnings_lets_one_operation_cover_multiple_crew_drivers() {
        // D1/D2 は同じ運行 (主/副運転) で窓の端まで在る。D3 は単独で 06-20 止まり。
        let pairs = vec![
            crew_pair("26063010000000000023021", Some("e1"), &["D1", "D2"]),
            driver_pair("26062010000000000023023", Some("e3"), "D3"),
        ];
        let w = warns(&pairs, d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w.len(), 1, "{w:?}");
        assert!(
            w[0].contains("乗務員1名の末尾が10日超"),
            "D1/D2 は揃っているので D3 だけ: {w:?}"
        );
    }

    /// `InputCoverage::driver_cd_source` は `driver_cds` を持つ item が 1 件でも
    /// あれば `true`。alc がまだ返さない環境と、乗務員別検知が効いている環境の
    /// 切り分けに使う (Refs #205 の 32)。
    #[test]
    fn input_coverage_reports_whether_driver_cds_were_present() {
        let none = InputCoverage::measure(
            &[pair("26063010000000000023021", Some("e1"))],
            d(2026, 6, 1),
            d(2026, 6, 30),
            d(2026, 7, 5),
        );
        assert!(!none.driver_cd_source, "driver_cds 無しはフォールバック");

        let some = InputCoverage::measure(
            &[driver_pair("26063010000000000023021", Some("e1"), "D1")],
            d(2026, 6, 1),
            d(2026, 6, 30),
            d(2026, 7, 5),
        );
        assert!(some.driver_cd_source, "driver_cds 有りは乗務員別判定");
    }

    /// **1 件も日付が読めなければ「判定できない」ではなく警告。** 安全側。
    #[test]
    fn missing_input_warnings_fires_when_no_unko_no_carries_a_date() {
        let pairs = vec![pair("U1", Some("e1")), pair("U2", Some("e2"))];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1);
        assert!(w[0].contains("1 件も読めない"), "{w:?}");
        assert!(
            w[0].contains("n=2 etag無=0 ?..?"),
            "実測値は ? で埋める: {w:?}"
        );
    }

    /// **`etag: null` は閾値の要らない確実な欠け** (R2 に CSV がまだ無い)。
    #[test]
    fn missing_input_warnings_fires_for_operations_without_an_r2_etag() {
        let pairs = vec![
            pair("26060110000000000023021", Some("e1")),
            pair("26070110000000000023021", None),
        ];
        let w = warns(&pairs, d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1, "末尾は埋まっているので etag の 1 本だけ: {w:?}");
        assert!(w[0].contains("R2 に CSV の無い運行 1 件"), "{w:?}");
    }

    /// 空の一覧は「末尾が欠けている」ではなく「1 件も読めない」で立つ。
    #[test]
    fn missing_input_warnings_fires_on_an_empty_list() {
        let w = warns(&[], d(2026, 7, 1), d(2026, 7, 20));
        assert_eq!(w.len(), 1, "{w:?}");
        assert_eq!(
            InputCoverage::measure(&[], d(2026, 6, 1), d(2026, 7, 1), d(2026, 7, 20)).gap_days(),
            None,
            "gap は測れない"
        );
    }

    /// **#205 の 37 の本題 (その 2)。** 末尾検知の母集団は「その月に運行が始まった
    /// 乗務員」だけ。etags は読み取り日で引くので窓の中に 2 か月前に始まった運行が
    /// 混ざり、それしか無い乗務員を数えると全員が「末尾が欠けている」になる
    /// (本番 2026-06 の `乗務員73名の末尾が37日超`)。
    #[test]
    fn tail_gap_ignores_drivers_without_an_operation_started_in_the_month() {
        // 窓は 2026-06。D1 は月内を端まで走っている。D2 は 04-20 に始まった運行が
        // 6 月に読まれただけで、6 月には 1 本も走っていない
        let pairs = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26042010000000000023022", Some("e2"), "D2"),
        ];
        let w = warns_in(&pairs, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert!(w.is_empty(), "稼働していない D2 は末尾欠けではない: {w:?}");

        // 母集団を絞らなければ D2 が 71 日の末尾欠けとして鳴っていた (旧挙動)
        let old = warns(&pairs, d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(old.len(), 1, "{old:?}");
        assert!(old[0].contains("乗務員1名の末尾が71日超"), "{old:?}");
    }

    /// 母集団から外れた乗務員が居ても `no_etag` の検知は従来どおり
    /// (R2 に CSV が無いのは閾値も母集団も関係ない確実な欠け)。
    #[test]
    fn tail_gap_population_does_not_touch_the_no_etag_check() {
        let pairs = vec![driver_pair("26042010000000000023022", None, "D2")];
        let w = warns_in(&pairs, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        assert_eq!(w.len(), 1, "etag 無しの 1 本だけ: {w:?}");
        assert!(w[0].contains("R2 に CSV の無い運行 1 件"), "{w:?}");
    }

    /// summary に末尾検知の母集団 (`対象`) が出る — 過剰発火したときに
    /// 「何名を測っているのか」をログから読めるようにするため (Refs #205 の 37)。
    #[test]
    fn the_summary_reports_the_tail_gap_population() {
        let pairs = vec![
            driver_pair("26063010000000000023021", Some("e1"), "D1"),
            driver_pair("26042010000000000023022", Some("e2"), "D2"),
        ];
        let cov = InputCoverage::measure(&pairs, d(2026, 6, 1), d(2026, 6, 30), d(2026, 7, 5));
        let s = cov.summary();
        assert!(s.contains("n=2"), "{s}");
        assert!(s.contains("対象=1名"), "母集団は D1 だけ: {s}");
    }

    // ── 運行の突合 (オンプレ × GCP、Refs #205 の 37) ────────────────────────

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

    /// **本題。** オンプレに在って GCP に無い運行が名指しで出る。
    #[test]
    fn diff_unko_names_the_operations_missing_from_gcp() {
        let onprem = vec![
            op(1078, "26062410055500000023021"),
            op(1517, "26062610055500000023022"),
        ];
        let diff = diff_unko(&onprem, &gcp_set(&["26062610055500000023022"]));
        assert_eq!(diff.total, 1, "{diff:?}");
        assert_eq!(diff.items.len(), 1);
        assert_eq!(diff.items[0].driver_cd, 1078);
        assert_eq!(diff.items[0].unko_no, "26062410055500000023021");
        assert_eq!(diff.items[0].start_date.as_deref(), Some("2026-06-24"));
        assert_eq!(diff.items[0].first_date, "2026-06-24");
        assert_eq!(diff.items[0].last_date, "2026-06-26");
        assert_eq!(diff.gcp_only, 0, "逆方向は無い");
    }

    /// 両方揃っていれば「欠け」はゼロ。標本は突合が成立していても出す
    /// (キーが噛み合っているかどうかを毎回目視できるようにするため)。
    #[test]
    fn diff_unko_is_empty_when_both_sides_agree() {
        let onprem = vec![op(1078, "U1"), op(1517, "U2")];
        let diff = diff_unko(&onprem, &gcp_set(&["U1", "U2"]));
        assert_eq!(diff.total, 0);
        assert_eq!(diff.gcp_only, 0);
        assert!(diff.items.is_empty());
        assert!(diff.gcp_only_sample.is_empty(), "逆方向が無いので空");
        let mine: Vec<&str> = diff.onprem_sample.iter().map(|s| &*s.unko_no).collect();
        assert_eq!(mine, vec!["U1", "U2"], "自分側は辞書順で出る");
    }

    /// **キーが 1 つも一致しないとき、両側の実物が対で返る** (Refs #205 の 37)。
    /// 本番 2026-06 の初回実測がこの形だった (重なりゼロ)。標本は**辞書順**で、
    /// `HashSet` の反復順に振り回されない (呼ぶたびに違う 10 件だと比較にならない)。
    #[test]
    fn diff_unko_samples_both_sides_in_a_stable_order() {
        let onprem = vec![op(1078, "B2"), op(1517, "B1")];
        let diff = diff_unko(&onprem, &gcp_set(&["A2", "A1", "A3"]));
        let theirs: Vec<&str> = diff.gcp_only_sample.iter().map(|s| &*s.unko_no).collect();
        assert_eq!(theirs, vec!["A1", "A2", "A3"], "GCP 側は辞書順");
        let mine: Vec<&str> = diff.onprem_sample.iter().map(|s| &*s.unko_no).collect();
        assert_eq!(mine, vec!["B1", "B2"], "オンプレ側も辞書順");
        assert_eq!(diff.gcp_only, 3);
        assert_eq!(diff.total, 2);
    }

    /// **`len` は前後の空白も数える。** 桁違いと空白混入をその場で見分けるため。
    #[test]
    fn the_sample_reports_the_length_including_padding() {
        let diff = diff_unko(
            &[op(1078, "26062411400600000023021")],
            &gcp_set(&[" 2606241140060000002302 "]),
        );
        assert_eq!(diff.onprem_sample[0].len, 23);
        assert_eq!(diff.gcp_only_sample[0].len, 24, "前後の空白を数える");
    }

    /// **本番の実物で形を測る** (Refs #205 の 37)。オンプレ 23 桁 = `YYMMDDHHMMSS`
    /// (12) + ゼロ埋めの車番 (10) + 余分な 1 文字、GCP 22 桁 = 前 22 桁だけ。
    ///
    /// 値は `/api/kintai/events` から引いた実データ (乗務員 1021 / 1078 / 1517 /
    /// 1688 / 1740 の 2026-06、31 運行) と、本番の etags 応答の標本そのもの。
    #[test]
    fn the_shape_of_both_sides_is_measured_over_the_whole_month() {
        let onprem = vec![
            // 1517 / 長崎100か4229 と 1688 / 帯広100か6654 の実物
            op(1517, "26052106075900000042291"),
            op(1688, "26062809112500000066541"),
            // 1740 は同じ車輌・同じ秒に始まる 2 本があり、末尾だけが 1 / 2 で違う
            op(1740, "26061105351800000039751"),
            op(1740, "26061105351800000039752"),
        ];
        let diff = diff_unko(&onprem, &gcp_set(&["2604202128500000002536"]));
        assert_eq!(diff.onprem_shape.len_counts, BTreeMap::from([(23, 4)]));
        assert_eq!(diff.gcp_shape.len_counts, BTreeMap::from([(22, 1)]));
        let last = &diff.onprem_shape.last_char_counts;
        let want = BTreeMap::from([("1".to_string(), 3), ("2".to_string(), 1)]);
        assert_eq!(*last, want, "余分な 1 文字は定数ではない");
    }

    /// **「先頭 22 桁で本当に重なるか」を推測ではなく数で返す** (Refs #205 の 37)。
    /// `raw` では 1 件も一致せず、`prefix22` で全件一致する — 本番で疑っている形。
    #[test]
    fn the_key_trials_measure_each_candidate_without_adopting_it() {
        let onprem = vec![
            op(1078, "26062411400600000023021"),
            op(1517, "26062606141000000042291"),
        ];
        let gcp = gcp_set(&["2606241140060000002302", "2606260614100000004229"]);
        let diff = diff_unko(&onprem, &gcp);
        assert_eq!(diff.total, 2, "突合そのものは raw のまま = 全件不一致");

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

    /// **桁を落とすと別の運行が潰れることまで数える。** 1740 の 2026-06-11 は
    /// 同じ車輌・同じ秒に始まる 2 本があり、先頭 22 桁では 1 本になる。
    #[test]
    fn the_key_trials_count_operations_that_collapse_into_one_key() {
        let onprem = vec![
            op(1740, "26061105351800000039751"),
            op(1740, "26061105351800000039752"),
            op(1517, "26052106075900000042291"),
        ];
        let diff = diff_unko(&onprem, &gcp_set(&[]));
        let by = |k: &str| diff.trials.iter().find(|t| t.key == k).unwrap().clone();
        assert_eq!(by("raw").collisions, 0, "生のキーは潰れない");
        assert_eq!(by("prefix22").collisions, 2, "1740 の 2 本が 1 つになる");
        assert_eq!(by("prefix12").collisions, 2, "12 桁でも同じ 2 本が潰れる");
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
            .map(|i| op(1000 + i as i64, &format!("U{i:03}")))
            .collect();
        let diff = diff_unko(&onprem, &gcp_set(&[]));
        assert_eq!(diff.onprem_sample.len(), MAX_UNKO_DIFF_SAMPLE);
        assert_eq!(diff.onprem_sample[0].unko_no, "U000", "頭から");
    }

    /// **逆方向 (GCP に在ってオンプレに無い) は件数だけ数える。**
    #[test]
    fn diff_unko_counts_the_reverse_direction() {
        let onprem = vec![op(1078, "U1")];
        let diff = diff_unko(&onprem, &gcp_set(&["U1", "U2", "U3"]));
        assert_eq!(diff.total, 0, "オンプレ側の欠けは無い");
        assert_eq!(diff.gcp_only, 2, "U2 / U3");
    }

    /// 同じ運行が 2 名 (主 / 副運転) に紐づくときは **`(乗務員CD, unko_no)` 単位**で
    /// 数える。`unko_no` が読めない形は `start_date` が `null` になるだけ。
    #[test]
    fn diff_unko_counts_per_driver_and_tolerates_an_unreadable_unko_no() {
        let onprem = vec![op(1078, "U1"), op(1517, "U1")];
        let diff = diff_unko(&onprem, &gcp_set(&[]));
        assert_eq!(diff.total, 2, "2 名ぶん: {diff:?}");
        assert_eq!(diff.items[0].start_date, None, "6 桁に満たない");
    }

    /// **上限 500 で切り、総数は実数のまま。** `unsplit` と同じ作法。
    #[test]
    fn diff_unko_caps_the_items_but_keeps_the_real_total() {
        let onprem: Vec<OnpremOperation> = (0..MAX_UNKO_DIFF + 5)
            .map(|i| op(1000 + i as i64, &format!("2606241005550000002{i:04}")))
            .collect();
        let diff = diff_unko(&onprem, &gcp_set(&[]));
        assert_eq!(diff.items.len(), MAX_UNKO_DIFF, "頭 500 件だけ");
        assert_eq!(diff.total, MAX_UNKO_DIFF + 5, "総数は切らない");
    }

    /// etags の一覧は sink の中でだけ拾える。外なら `None` (「無い」と区別する)。
    #[tokio::test]
    async fn the_etag_unko_nos_come_back_only_inside_the_sink() {
        assert_eq!(collected_etag_unko_nos(), None, "sink の外は None");
        let (_, diff) = with_unko_diff_sink(async {
            assert_eq!(collected_etag_unko_nos(), None, "未記録も None");
            record_etag_unko_nos(gcp_set(&["U1"]));
            assert_eq!(collected_etag_unko_nos(), Some(gcp_set(&["U1"])));
        })
        .await;
        assert_eq!(diff, UnkoDiff::default(), "突合していなければ空");
    }

    /// **記録した突合が `with_unko_diff_sink` の戻り値に乗り、warnings に 1 行出る。**
    #[tokio::test]
    async fn record_unko_diff_reports_through_the_sink_and_the_warnings() {
        let ((out, warnings), diff) = with_unko_diff_sink(with_warning_sink(async {
            let onprem = vec![op(1078, "26062410055500000023021"), op(1517, "U2")];
            record_unko_diff(&onprem, &gcp_set(&["U9"]));
            7
        }))
        .await;
        assert_eq!(out, 7);
        assert_eq!(diff.total, 2);
        assert_eq!(diff.gcp_only, 1, "U9 はオンプレに無い");
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        let w = &warnings[0];
        assert!(w.contains("オンプレに在って GCP に無い運行 2 件"), "{w}");
    }

    /// 差が無ければ warning は立たない (月ゲートを無駄に開けない)。
    #[tokio::test]
    async fn record_unko_diff_is_silent_when_nothing_is_missing() {
        let ((_, warnings), diff) = with_unko_diff_sink(with_warning_sink(async {
            record_unko_diff(&[op(1078, "U1")], &gcp_set(&["U1"]));
        }))
        .await;
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(diff.total, 0);
        assert_eq!(diff.gcp_only, 0);
    }

    /// **集めていないときは何も起きない** (`record_unsplit` と同じ扱い)。
    #[test]
    fn record_unko_diff_outside_a_sink_is_dropped() {
        let diff = record_unko_diff(&[op(1078, "U1")], &gcp_set(&[]));
        assert_eq!(diff.total, 1, "戻り値は返る");
        assert_eq!(collected_etag_unko_nos(), None, "sink は立っていない");
    }

    /// `today_jst` は UTC 深夜の前後で日付がずれない (JST 固定オフセット)。
    #[test]
    fn today_jst_is_a_plausible_date() {
        assert!(today_jst() >= d(2026, 1, 1), "2026 年以降のはず");
    }

    #[test]
    fn build_token_provider_uses_metadata_when_declared() {
        let mut cfg = KintaiEventsConfig {
            base_url: "https://alc.example".to_string(),
            auth_token_metadata: true,
            ..Default::default()
        };
        assert!(build_token_provider(&cfg).is_ok());

        // base_url が無いと audience が作れないので起動時に落ちる
        cfg.base_url = String::new();
        assert!(build_token_provider(&cfg).is_err());
    }
}
