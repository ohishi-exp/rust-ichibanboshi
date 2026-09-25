//! 勤怠の生イベント読み取り (社内 MariaDB 直読み、Refs #116)。
//!
//! `/api/kintai/events` の materialize 元。**打刻と運行イベントを解釈せず、
//! 時刻順に並べた生行のまま返す** — 拘束時間の規則 (何を勤務の切れ目と見るか、
//! 何分から休憩と数えるか) は消費側 (Phase 2 の `kosoku-daily` / MCP) の担当で、
//! ここでは一切判断しない。
//!
//! ## なぜ CakePHP 中継ではなく直読みか (#114 → #116 で方針変更)
//!
//! 返すのが解釈しない生行なので CakePHP の ORM を挟む意味が薄く、挟めば
//! 「CakePHP の解釈」と「本サービスの中継」の 2 段になって切り分けが増える。
//! 上流に相当エンドポイントが無く、新設には CI の無い本番直結リポ
//! (`yhonda-ohishi/nginx`) を触る必要があったことも理由。
//!
//! 日別サマリ (`/api/kintai/daily`) は CakePHP 中継のまま**変えない** — あちらは
//! 休日判定やセッション組み立てという解釈が上流に入っており、直読みで再現すると
//! 二重実装になる。
//!
//! ## 読むテーブル
//!
//! | テーブル | 中身 |
//! |---|---|
//! | `time_card_dstate` | 人が確定させた打刻 (`state` 30=始業 / 31=終業、`id` が乗務員CD) |
//! | `time_card_dtako` | 運行に紐づく確定イベント (10=運行開始 / 11=運行終了 / 20=休息、`unko_no` 付き) |
//! | `time_card_dtako_state` | `state` → 名称のマスタ |
//! | `dtako_events` | デジタコ生イベント (運転 / 積み / 降し / 休憩 / 休息)。区間を持つ |
//! | `dtako_cars` | `車輌CD` → 車番 |
//!
//! ## 乗務員は `対象乗務員CD` で引く (`乗務員CD1` ではない)
//!
//! `dtako_events` には乗務員の列が 2 つある。**`乗務員CD1` で引くと他人の運行を
//! 拾う。** 2 名乗務・交替の運行では、運行まるごとが `乗務員CD1` = 別の乗務員の
//! まま記録される (実測: 運行 26061105351800000039752 の 42 行すべてが
//! `乗務員CD1=1740` / `対象乗務員CD=1130`)。2026-06 で 1740 の休息・休憩を数えると
//! `乗務員CD1` では 114 件、`対象乗務員CD` では 83 件 — 31 件は 1130 の分だった。
//! 引かれた側 (1130) は逆に取りこぼす。
//!
//! 速度も段違い。`対象乗務員CD` には `idx_driver_datetime (対象乗務員CD, 開始日時)`
//! があり covering index が効くが、`乗務員CD1` には索引が無い:
//!
//! | 絞り方 | EXPLAIN | 見込み行数 | 実測 |
//! |---|---|---|---|
//! | `乗務員CD1` | range / `testin` (開始日時) | 213,884 | 0.202 秒 |
//! | `対象乗務員CD` | range / `idx_driver_datetime` (covering) | 866 | **0.00042 秒** |

use std::sync::Arc;

use async_trait::async_trait;
use mysql_async::prelude::Queryable;
use mysql_async::{params, Pool};

use crate::config::MariadbConfig;

/// 生イベント読み取りのエラー。
#[derive(Debug)]
pub enum KintaiRepoError {
    /// `[mariadb]` 未設定 (= 機能無効)。fail-closed で 503 にする
    NotConfigured,
    /// 接続 / クエリ失敗
    QueryFailed(String),
}

impl std::fmt::Display for KintaiRepoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => write!(f, "MariaDB 接続設定が未設定"),
            Self::QueryFailed(m) => write!(f, "MariaDB query failed: {m}"),
        }
    }
}

impl std::error::Error for KintaiRepoError {}

/// push 対象の `source` (`timecard` / `dtako`) の行か。
///
/// **`dtako_events` を落とすための判定。** 表を分けて読めない実装 (HTTP 版) 向けの
/// 保険で、MariaDB 実装は SQL 側で落とすのでここを通らない。
fn is_pushed_source(row: &serde_json::Value) -> bool {
    row.get("source")
        .and_then(|v| v.as_str())
        .is_some_and(|s| crate::kintai_push::PUSHED_SOURCES.contains(&s))
}

/// この経路で運ぶ行か。push 対象の `source` で、かつ運ばないと決めた `state`
/// ([`crate::kintai_push::NOT_CARRIED_STATES`]) でないもの。
///
/// MariaDB 実装は [`TIMECARD_EVENTS_SQL`] が同じものを SQL で落とすのでここを
/// 通らない。両方に置いているのは HTTP 版 (GCP 側) と結果を揃えるため。
fn is_carried(row: &serde_json::Value) -> bool {
    if !is_pushed_source(row) {
        return false;
    }
    !row.get("state")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .is_some_and(|s| crate::kintai_push::NOT_CARRIED_STATES.contains(&s))
}

/// 打刻の行から乗務員CD を昇順・重複無しで拾う。
///
/// **0 以下は捨てる。** 乗務員CD ではない — 空の乗務員が 0 として出てきて、
/// 1 ページぶんの枠を食っていた (2026-06 の dry-run で実測)。
fn timecard_driver_cds(rows: Vec<serde_json::Value>) -> Vec<u64> {
    rows.iter()
        .filter(|r| is_pushed_source(r))
        .filter_map(|r| r.get("driver_id").and_then(|v| v.as_u64()))
        .filter(|d| *d > 0)
        .collect::<std::collections::BTreeSet<u64>>()
        .into_iter()
        .collect()
}

/// 生イベントの読み出し口。DB 実装と mock を差し替えるための trait
/// (`DynRepo` と同じ形 — route のテストを DB 無しで回すため)。
#[async_trait]
pub trait KintaiEventsApi: Send + Sync {
    /// 任意の期間 `[from, to)` × 乗務員CD の生イベントを時刻昇順で返す。
    ///
    /// 期間の決め方は呼び出し側の担当。期間をまたぐ区間イベントは
    /// `EVENTS_SQL` 側が「期間内に終わる区間」として拾うので、
    /// 呼び出し側が遡る日数を決め打ちする必要はない。
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError>;

    /// 対象月 (`YYYY-MM`) × 乗務員CD の生イベント。[`month_range`] を当てるだけ。
    async fn fetch_events(
        &self,
        month: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        self.fetch_events_between(&from, &to, driver).await
    }

    /// 任意の期間 `[from, to)` の**全乗務員**の生イベント (Refs #125)。
    ///
    /// 日別サマリを全員ぶん組むための読み出し口。1 名ずつ [`fetch_events_between`]
    /// を 96 回叩くと約 3 秒かかるのを 1 リクエストにまとめる。
    ///
    /// [`fetch_events_between`]: KintaiEventsApi::fetch_events_between
    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError>;

    /// 対象月 (`YYYY-MM`) の全乗務員の生イベント。[`month_range`] を当てるだけ。
    async fn fetch_all_events(
        &self,
        month: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        self.fetch_all_events_between(&from, &to).await
    }

    /// 任意の期間 `[from, to)` × 乗務員CD の**打刻 2 表だけ** (Refs #205 の 04b)。
    ///
    /// GCP へ渡すのは打刻だけ ([`crate::kintai_push::PUSHED_SOURCES`]) なので、
    /// `dtako_events` は読むだけ無駄になる — [`crate::kintai_push::parse_row`] が
    /// `NotPushedSource` で全部捨てる。**畳むのに要るデジタコ生イベントは GCP が
    /// alc から直接引く**ので、この経路が運ぶ必要はない (#205 の決定 5)。
    ///
    /// 既定は [`fetch_events_between`] を `source` で絞ったもの。MariaDB 実装は
    /// SQL 側で落とすので、そもそも読まない。
    ///
    /// [`fetch_events_between`]: KintaiEventsApi::fetch_events_between
    async fn fetch_timecard_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let rows = self.fetch_events_between(from, to, driver).await?;
        Ok(rows.into_iter().filter(is_carried).collect())
    }

    /// 期間ぶんの打刻を**全乗務員まとめて**返す (Refs #205 の 04b)。
    ///
    /// 1 名ずつ引かない理由は [`TIMECARD_WINDOW_SQL`] を参照 — 費用は往復の回数で
    /// あって転送量ではない。既定は [`fetch_all_events_between`] を絞ったもので、
    /// MariaDB 実装は SQL 側で落とす。
    ///
    /// [`fetch_all_events_between`]: KintaiEventsApi::fetch_all_events_between
    async fn fetch_timecard_window(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let rows = self.fetch_all_events_between(from, to).await?;
        Ok(rows.into_iter().filter(is_carried).collect())
    }

    /// 対象期間に**打刻がある**乗務員CD を昇順で返す (Refs #205 の 04b)。
    ///
    /// 乗務員の洗い出しに [`fetch_all_events_between`] を使うと、行を 1 つも使わない
    /// のに月ぶんの `dtako_events` を JSON にして捨てることになる。**要るのは CD の
    /// 集合だけ**なので、MariaDB 実装は `SELECT ... UNION` 1 本で済ませる。
    ///
    /// [`fetch_all_events_between`]: KintaiEventsApi::fetch_all_events_between
    async fn fetch_timecard_driver_cds_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<u64>, KintaiRepoError> {
        Ok(timecard_driver_cds(
            self.fetch_all_events_between(from, to).await?,
        ))
    }

    /// 対象月の**フェリー区間** (Refs #146、yhonda-ohishi/nginx#788 からの引き継ぎ)。
    ///
    /// 紙のタイムカード表が拘束から引いている「同日フェリー控除」を再現するための
    /// 読み出し口。**控除は紙の側の誤り**で、こちらの拘束には影響させない — 突合で
    /// 差の原因を説明するためだけに使う。
    ///
    /// `driver` を省略すると全乗務員。範囲は**その月ちょうど** `[月初, 翌月初)` で、
    /// イベント側の `[月初, 翌月+1日)` とは違う — 上流の条件
    /// (`$fr->開始日時 >= $date_f && < $date_next`) をそのまま写すため。
    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError>;

    /// 対象月 (`YYYY-MM`) のフェリー区間。範囲はその月ちょうど。
    async fn fetch_ferry(
        &self,
        month: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = exact_month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        self.fetch_ferry_between(&from, &to, driver).await
    }

    /// 任意の期間 `[from, to)` の**休息だけ**を、`運行NO` 付きで両表から読む
    /// (Refs #205 の 41)。`driver` を省略すると全乗務員。
    ///
    /// [`crate::kintai_rest_diff`] が `time_card_dtako` (`source = "dtako"`) と
    /// `dtako_events` (`source = "dtako_events"`) の休息を突き合わせ、書き戻しが
    /// 追従していない運行を名指しするための読み出し口。
    ///
    /// **`fetch_all_events_between` では代わりにならない。** あちらは速さのために
    /// `運行NO` を落としており ([`ALL_EVENTS_SQL`])、運行を鍵にした突合ができない。
    /// 1 名ずつ [`fetch_events_between`] を叩けば `運行NO` は付くが、94 名で 33.6 秒
    /// かかる ([`TIMECARD_WINDOW_SQL`] の実測) ので月まるごとの診断には使えない。
    ///
    /// **既定は `NotConfigured` = 503。** フェリー ([`fetch_ferry_between`]) と同じ
    /// オンプレ専用の口で、`dtako_events` を持たない実行形態では答えようがない。
    ///
    /// [`fetch_events_between`]: KintaiEventsApi::fetch_events_between
    /// [`fetch_ferry_between`]: KintaiEventsApi::fetch_ferry_between
    async fn fetch_rest_events_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }

    /// 対象月 (`YYYY-MM`) の休息。範囲は [`month_range`] (イベントと同じ窓)。
    async fn fetch_rest_events(
        &self,
        month: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        self.fetch_rest_events_between(&from, &to, driver).await
    }

    /// 任意の期間 `[from, to)` に**かかる運行**と、その**読取日** (Refs #205 の 42)。
    /// `driver` を省略すると全乗務員。
    ///
    /// [`crate::kintai_reading_dates`] が「値のずれた勤務を直すのにどの読取日を
    /// 取り直せばよいか」を答えるための読み出し口。読み先は `dtako_rows` で、
    /// **alc は呼ばない** — 理由はあちらのモジュール docs。
    ///
    /// **既定は `NotConfigured` = 503。** フェリー ([`fetch_ferry_between`]) と
    /// 休息のずれ ([`fetch_rest_events_between`]) と同じオンプレ専用の口。
    ///
    /// [`fetch_ferry_between`]: KintaiEventsApi::fetch_ferry_between
    /// [`fetch_rest_events_between`]: KintaiEventsApi::fetch_rest_events_between
    async fn fetch_operation_reading_dates_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }

    /// 対象月 (`YYYY-MM`) にかかる運行の読取日。範囲は [`month_range`]。
    async fn fetch_operation_reading_dates(
        &self,
        month: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = month_range(month)
            .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))?;
        self.fetch_operation_reading_dates_between(&from, &to, driver)
            .await
    }

    /// 対象月の dtako 側 (alc) 指紋材料 (Refs #205 実装計画 13、月ゲート)。
    ///
    /// `fetch_all_events_between` を実際に読まなくても「前回 fold したときと入力が
    /// 変わっていないか」を安く判定するための材料。**既定は `Ok(None)`** — 月ゲートは
    /// これを「使えない」と読み、安全側 (従来どおり全量読み) に倒す。
    ///
    /// [`crate::kintai_http_repo::HttpKintaiEventsRepo`] だけがこれを上書きして、alc の
    /// `GET /api/dtako/events/etags` (R2 の LIST だけで済む、CSV は読まない) を呼ぶ。
    /// MariaDB 直読み (`MariadbKintaiEventsRepo`) はそもそも R2 を経由しないので既定の
    /// ままでよい — 直読みは元から速く、ゲートで削る費用が無い。
    async fn fetch_dtako_month_digest(
        &self,
        _month: &str,
    ) -> Result<Option<String>, KintaiRepoError> {
        Ok(None)
    }

    /// [`fetch_dtako_month_digest`] の始端を `since` (日付) まで下げた版 (Refs
    /// ohishi-exp/nuxt-dtako-admin#1123)。fold が月初より前から読む月だけ `Some` で
    /// 呼ばれる。**既定は `since` を無視して今までの範囲** — 月ゲートを上書きしない
    /// 実装 (MariaDB・mock) の挙動を変えないため。
    ///
    /// [`fetch_dtako_month_digest`]: KintaiEventsApi::fetch_dtako_month_digest
    async fn fetch_dtako_month_digest_since(
        &self,
        month: &str,
        _since: Option<chrono::NaiveDate>,
    ) -> Result<Option<String>, KintaiRepoError> {
        self.fetch_dtako_month_digest(month).await
    }

    /// **月初をまたいで続く運行・勤務の始まり**を乗務員ごとに返す (Refs
    /// ohishi-exp/nuxt-dtako-admin#1123)。値は `YYYY-MM-DD HH:MM:SS`。
    ///
    /// fold と画面は対象月を `[月初, 翌月 2 日)` で読むので、前月に始業した勤務の
    /// 続き (月初の休息・運行終了・終業) だけが見え、休息の終わりを始業とする余分な
    /// 勤務を当月に立てていた (乗務員 1194 の 2026-04)。ここが返す時刻まで窓を
    /// 遡らせれば、勤務は前月始業のまま組まれ、当月の出力 (始業日で絞る) から外れる。
    /// 規則は [`month_head_anchors`]。
    ///
    /// **既定は空** (= 遡らない)。打刻と運行の確定イベントを持つ実装だけが上書きする。
    async fn fetch_month_head_anchors(
        &self,
        _month_start: &str,
        _to: &str,
    ) -> Result<std::collections::BTreeMap<u64, String>, KintaiRepoError> {
        Ok(std::collections::BTreeMap::new())
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
        let Some(start) = crate::kintai_http_repo::unko_no_start_datetime(unko_no) else {
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

pub type DynKintaiEventsRepo = Arc<dyn KintaiEventsApi>;

/// `[mariadb]` 未設定時の実装 — 常に `NotConfigured` (= 503)。
///
/// 接続情報が無いまま起動したときに「空配列が返って 0 件に見える」ことを防ぐ。
pub struct DisabledKintaiEventsRepo;

#[async_trait]
impl KintaiEventsApi for DisabledKintaiEventsRepo {
    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }

    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }
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

/// 打刻 (`time_card_dstate`) / 運行の確定イベント (`time_card_dtako`) /
/// デジタコ生イベント (`dtako_events`) を `UNION ALL` して時刻順に並べる。
///
/// - 日付は `DATE_FORMAT` で文字列にして取り出す — 応答がそのまま
///   `YYYY-MM-DD HH:MM:SS` になり、DB driver の時刻型と timezone 解釈を
///   経路に持ち込まない
/// - `dtako_events` だけ `end_datetime` を持つ (区間イベントのため)。
///   **区間長の判定はしない** — 何分から休憩と数えるかは規則側の話
/// - `dtako_events` は 2 ブランチに分ける。**期間内に始まる区間**に加えて、
///   **期間内に終わる区間 (開始は期間より前)** も拾う — `kosoku-daily` は
///   「休息の終了 = 始業」で勤務を切るので、月をまたぐ休息を落とすと月初の勤務が
///   組めない。2 つは `開始日時` の条件で排他なので重複しない
/// - **`COALESCE(終了日時, 開始日時) >= :from` で 1 本にまとめてはいけない。**
///   関数適用で索引が効かず `type=ALL` の全表走査 (427 万行) になる。実機で
///   0.2 秒が 4 分超になった (#121 → #122 で revert)。`開始日時` と `終了日時` は
///   それぞれ索引を持つので、条件を分けて両方に効かせる (各 0.2 秒)
const EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state,
       NULL                                         AS unko_no,
       NULL                                         AS vehicle
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.id = :driver AND d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name),
       t.unko_no,
       NULL
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.driver_id = :driver AND t.datetime >= :from AND t.datetime < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`,
       c.`車輌名`
  FROM dtako_events e
  LEFT JOIN dtako_cars c ON c.`車輌CD` = e.`車輌CD`
 WHERE e.`対象乗務員CD` = :driver AND e.`開始日時` >= :from AND e.`開始日時` < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`,
       c.`車輌名`
  FROM dtako_events e
  LEFT JOIN dtako_cars c ON c.`車輌CD` = e.`車輌CD`
 WHERE e.`対象乗務員CD` = :driver
   AND e.`終了日時` >= :from AND e.`終了日時` < :to
   AND e.`開始日時` < :from
 ORDER BY datetime, source
"#;

/// 全乗務員ぶんを 1 リクエストで読む (Refs #125)。`EVENTS_SQL` から
/// **乗務員の絞り込みを外し、`運行NO` と `車輌名` を落とした**もの。
///
/// - **`運行NO` / `車輌名` と `dtako_cars` の JOIN を返さない。** 日別サマリ
///   ([`crate::kosoku::daily_summary`]) はどちらも使っていないので値は変わらないが、
///   実測ではここが支配的だった — 2026-06 の全乗務員で **1.20 秒 → 0.25 秒 (約 5 倍)**。
///   22,092 行それぞれで車輌マスタを引き当て、23 桁の `運行NO` を転送していた分。
///   「どの運行・どの車か」に降りるときは 1 名分の `/api/kintai/events` を叩く
/// - **`dtako_events` はイベント名で絞らない** (2026-07-29 に絞りを撤回、
///   Refs ohishi-exp/nuxt-dtako-admin#501)。かつて 休息/休憩/運行開始/運行終了 の
///   4 種に絞っていた (105,771 行 → 22,092 行) が、**単一乗務員経路と値が割れる**
///   事故を 2 度起こした — #167 (拾った運行の終わりが経路で変わる) と、1556 林田
///   03-26 (終業打刻後の運転イベントが見えず `unpunched_ops_shift` が不発、
///   紙 979 に対し 864 で +115 の未説明差)。この SQL は in-process 消費
///   ([`kosoku_daily_all`](crate::routes::kintai::kosoku_daily)) で Tunnel を
///   通らないため、行数よりも**両経路の同値**を優先する
/// - **`/api/kintai/events` は絞らない。** あちらは数字がおかしいときに 1 名分の生時系列へ
///   降りるための口で、種別を絞ると調査ができなくなる (#116 の「解釈しない読み出し口」)
/// - 2 ブランチに分ける理由・`COALESCE` で 1 本にまとめてはいけない理由は
///   [`EVENTS_SQL`] と同じ
const ALL_EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name)
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.datetime >= :from AND t.datetime < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`
  FROM dtako_events e
 WHERE e.`開始日時` >= :from AND e.`開始日時` < :to
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`
  FROM dtako_events e
 WHERE e.`終了日時` >= :from AND e.`終了日時` < :to
   AND e.`開始日時` < :from
 ORDER BY driver_id, datetime, source
"#;

/// 打刻 2 表だけを 1 乗務員ぶん読む (Refs #205 の 04b)。
///
/// [`EVENTS_SQL`] から **`dtako_events` の 2 ブランチと `dtako_cars` の JOIN を
/// 落とした**もの。列は [`EventRow`] と同じ 7 列のままで、`vehicle` は常に NULL —
/// 車輌名は `dtako_events` 側にしか無く、打刻には最初から付いていない。
///
/// 落としてよい理由は [`crate::kintai_push::PUSHED_SOURCES`] が
/// `timecard` / `dtako` の 2 つだけだから。`dtako_events` の行は読んでも
/// [`crate::kintai_push::parse_row`] が `NotPushedSource` で捨てるので、
/// **捨てる行のために月ぶんの最大表を読んで Tunnel 越しに転送していた**ことになる。
/// `ALL_EVENTS_SQL` の実測 (1.20 秒 → 0.25 秒) が示すとおり、支配的なのは
/// `dtako_cars` の引き当てと 23 桁の `運行NO` の転送。
///
/// `unko_no` は残す — `time_card_dtako.unko_no` から取れるので、
/// 「どの運行のイベントか」は失われない。
///
/// **`休息` も読まない** ([`crate::kintai_push::NOT_CARRIED_STATES`])。開始 (20) と
/// 終了 (21) が同じ名前で来るため、`dtako_events` を運ばないこの経路では読み分けが
/// できない。畳むのに要る休息区間は GCP が alc から直接引く。
///
/// 落とすのは**解決後の名前**であって `state` の番号ではない。`event_name` は
/// 自由記述なので、番号で落とすと「state 20 だが別の名前」の行まで消える。
/// 名前で落とせば、知らない値が来たときは今までどおり `unknown_states` に出る。
const TIMECARD_EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state,
       NULL                                         AS unko_no,
       NULL                                         AS vehicle
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.id = :driver AND d.datetime >= :from AND d.datetime < :to
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name),
       t.unko_no,
       NULL
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.driver_id = :driver AND t.datetime >= :from AND t.datetime < :to
   AND COALESCE(t.event_name, s.name) <> '休息'
 ORDER BY datetime, source
"#;

/// 期間ぶんの打刻を**全乗務員まとめて** 1 回で読む (Refs #205 の 04b)。
///
/// [`TIMECARD_EVENTS_SQL`] から乗務員の絞り込みを外しただけ。**列は同じ 7 列**なので、
/// 1 名ずつ読んだときと `raw` が一致する = 既に書いた日の署名が変わらない。
/// (`ALL_EVENTS_SQL` は速さのために `運行NO` を落としているので、あれは使えない。)
///
/// ## なぜ 1 名ずつ引かないのか
///
/// 2026-07-31 の実測: 署名の引き当てを乗務員ごとに 1 往復していたレグが
/// **33.6 秒 / 全体の 94%** を占めていた (94 名 × 約 358ms)。往復の回数が費用で、
/// 転送量ではない — 同じ月の全打刻は Tunnel 越しでも 1.3 秒で運べている。
///
/// `ORDER BY driver_id` は受け側が乗務員ごとに束ねるため。
const TIMECARD_WINDOW_SQL: &str = r#"
SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       d.id                                         AS driver_id,
       'timecard'                                   AS source,
       s.name                                       AS state,
       NULL                                         AS unko_no,
       NULL                                         AS vehicle
  FROM time_card_dstate d
  LEFT JOIN time_card_dtako_state s ON s.id = d.state
 WHERE d.datetime >= :from AND d.datetime < :to AND d.id > 0
UNION ALL
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s'),
       NULL,
       t.driver_id,
       'dtako',
       COALESCE(t.event_name, s.name),
       t.unko_no,
       NULL
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.datetime >= :from AND t.datetime < :to AND t.driver_id > 0
   AND COALESCE(t.event_name, s.name) <> '休息'
 ORDER BY driver_id, datetime, source
"#;

/// 対象期間に打刻がある乗務員CD だけを昇順で返す (Refs #205 の 04b)。
///
/// **行を返さない。** 乗務員の洗い出しに `ALL_EVENTS_SQL` を使うと、CD の集合しか
/// 使わないのに月ぶんの全行を JSON にして捨てることになる。`UNION` (`UNION ALL`
/// ではない) が重複を潰すので、呼び出し側での dedup も要らない。
///
/// **`> 0` で絞る。** 乗務員CD を持たない行が 0 として出てきて、relay の
/// 1 ページぶんの枠を食っていた (2026-06 の dry-run で実測)。
const TIMECARD_DRIVERS_SQL: &str = r#"
SELECT d.id AS driver_id
  FROM time_card_dstate d
 WHERE d.datetime >= :from AND d.datetime < :to AND d.id > 0
UNION
SELECT t.driver_id
  FROM time_card_dtako t
 WHERE t.datetime >= :from AND t.datetime < :to AND t.driver_id > 0
 ORDER BY driver_id
"#;

/// 窓の中の**運行終了**と、その `unko_no` (Refs ohishi-exp/nuxt-dtako-admin#1123)。
/// 月初をまたぐ運行を見つける材料 ([`month_head_anchors`] の運行の行)。
///
/// 運行開始日時は `unko_no` の先頭 12 桁から Rust 側で取る — `state = 10` の行と
/// 突き合わせない (運行開始の行が無い運行でも引けるように)。月初より前かの判定も
/// Rust 側。`datetime` の索引で窓を絞るのは [`ALL_EVENTS_SQL`] と同じ。
const HEAD_RUN_ENDS_SQL: &str = r#"
SELECT t.driver_id, t.unko_no
  FROM time_card_dtako t
 WHERE t.state = 11 AND t.datetime >= :from AND t.datetime < :to
   AND t.driver_id > 0 AND t.unko_no IS NOT NULL
"#;

/// 窓に打刻がある乗務員ごとの「月初時点の打刻の姿」(Refs
/// ohishi-exp/nuxt-dtako-admin#1123、[`HeadPunch`])。
///
/// 相関サブクエリは 3 本とも `time_card_dstate` を乗務員 (`id`) と `datetime` で
/// 引く ([`EVENTS_SQL`] の単一乗務員ブランチと同じ経路)。`MAX(… < :from)` は
/// 月初から遡って最初に当たった 1 行で止まる。同時刻の始業・終業は始業を先に置く
/// (`state` 30 < 31)。
const HEAD_PUNCHES_SQL: &str = r#"
SELECT f.id AS driver_id,
       (SELECT IF(s.state = 30, '始業', '終業')
          FROM time_card_dstate s
         WHERE s.id = f.id AND s.state IN (30, 31)
           AND s.datetime >= :from AND s.datetime < :to
         ORDER BY s.datetime, s.state
         LIMIT 1) AS first_state,
       (SELECT DATE_FORMAT(MAX(b.datetime), '%Y-%m-%d %H:%i:%s')
          FROM time_card_dstate b
         WHERE b.id = f.id AND b.state = 30 AND b.datetime < :from) AS last_start,
       (SELECT DATE_FORMAT(MAX(e.datetime), '%Y-%m-%d %H:%i:%s')
          FROM time_card_dstate e
         WHERE e.id = f.id AND e.state = 31 AND e.datetime < :from) AS last_end
  FROM (SELECT DISTINCT d.id
          FROM time_card_dstate d
         WHERE d.datetime >= :from AND d.datetime < :to AND d.id > 0) f
"#;

/// `HEAD_PUNCHES_SQL` の 1 行 (列の順序と 1:1)。
type HeadPunchRow = (i64, Option<String>, Option<String>, Option<String>);

/// [`HeadPunchRow`] を [`HeadPunch`] へ。0 以下の CD は SQL で落としてある。
fn head_punch(row: HeadPunchRow) -> HeadPunch {
    let (driver, first_state, last_start, last_end) = row;
    HeadPunch {
        driver: driver.max(0) as u64,
        first_state,
        last_start,
        last_end,
    }
}

/// MariaDB から遡り起点を引く (Refs ohishi-exp/nuxt-dtako-admin#1123)。
///
/// 接続を受け取るのは、画面の etag ([`crate::kintai_version`]) が**自分の pool** で
/// 同じ起点を引くため — 起点の決め方を 2 実装にしない。
pub(crate) async fn mariadb_month_head_anchors(
    conn: &mut mysql_async::Conn,
    from: &str,
    to: &str,
) -> Result<std::collections::BTreeMap<u64, String>, KintaiRepoError> {
    let q = |e: mysql_async::Error| KintaiRepoError::QueryFailed(e.to_string());
    let runs: Vec<(i64, String)> = conn
        .exec(HEAD_RUN_ENDS_SQL, params! { "from" => from, "to" => to })
        .await
        .map_err(q)?;
    let punches: Vec<HeadPunchRow> = conn
        .exec(HEAD_PUNCHES_SQL, params! { "from" => from, "to" => to })
        .await
        .map_err(q)?;
    let runs: Vec<(u64, String)> = runs
        .into_iter()
        .map(|(d, u)| (d.max(0) as u64, u))
        .collect();
    let punches: Vec<HeadPunch> = punches.into_iter().map(head_punch).collect();
    Ok(month_head_anchors(from, &runs, &punches))
}

/// 休息だけを `運行NO` 付きで両表から読む (Refs #205 の 41)。
///
/// [`EVENTS_SQL`] から **`timecard` のブランチと `dtako_cars` の JOIN を落とし、
/// `休息` に絞った**もの。列は 6 つで `vehicle` を持たない — 突合に要るのは
/// 「どの運行の休息が何時か」だけで、車輌名は使わない。
///
/// - **`ALL_EVENTS_SQL` と違い `運行NO` を返す。** これが鍵なので落とせない。
///   代わりに `dtako_cars` の引き当てを外し、`休息` で絞って行数を落とす
///   (`ALL_EVENTS_SQL` の実測で支配的だったのは JOIN と全イベントの転送)
/// - **絞りは解決後の名前で行う** (`COALESCE(t.event_name, s.name) = '休息'`)。
///   `state` の番号で絞ると「state 20 だが別の名前」の行を取り違える
///   ([`crate::kintai_push::NOT_CARRIED_STATES`] と同じ理由)
/// - `dtako_events` を 2 ブランチに分ける理由・`COALESCE` で 1 本にまとめては
///   いけない理由は [`EVENTS_SQL`] と同じ
/// - `:driver` が NULL なら全乗務員 (`fetch_rest_events_between` の `driver: None`)
const REST_EVENTS_SQL: &str = r#"
SELECT DATE_FORMAT(t.datetime, '%Y-%m-%d %H:%i:%s') AS datetime,
       NULL                                         AS end_datetime,
       t.driver_id                                  AS driver_id,
       'dtako'                                      AS source,
       COALESCE(t.event_name, s.name)               AS state,
       t.unko_no                                    AS unko_no
  FROM time_card_dtako t
  LEFT JOIN time_card_dtako_state s ON s.id = t.state
 WHERE t.datetime >= :from AND t.datetime < :to
   AND (:driver IS NULL OR t.driver_id = :driver)
   AND COALESCE(t.event_name, s.name) = '休息'
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`
  FROM dtako_events e
 WHERE e.`開始日時` >= :from AND e.`開始日時` < :to
   AND (:driver IS NULL OR e.`対象乗務員CD` = :driver)
   AND e.`イベント名` = '休息'
UNION ALL
SELECT DATE_FORMAT(e.`開始日時`, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(e.`終了日時`, '%Y-%m-%d %H:%i:%s'),
       e.`対象乗務員CD`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`
  FROM dtako_events e
 WHERE e.`終了日時` >= :from AND e.`終了日時` < :to
   AND e.`開始日時` < :from
   AND (:driver IS NULL OR e.`対象乗務員CD` = :driver)
   AND e.`イベント名` = '休息'
 ORDER BY unko_no, datetime, source
"#;

/// `REST_EVENTS_SQL` の 1 行 (列の順序と 1:1)。
type RestEventRow = (
    String,
    Option<String>,
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
);

/// 休息の 1 行を JSON へ。`vehicle` は**キーごと出さない** (読んでいないため)。
fn rest_row_to_json(row: RestEventRow) -> serde_json::Value {
    let (datetime, end_datetime, driver_id, source, state, unko_no) = row;
    serde_json::json!({
        "datetime": datetime,
        "end_datetime": end_datetime,
        "driver_id": driver_id,
        "source": source,
        "state": state,
        "unko_no": unko_no,
    })
}

/// 期間にかかる運行と、その**読取日** (Refs #205 の 42)。
///
/// `dtako_rows` は**1 運行 × 対象乗務員で 1 行**の表で、`読取日` / `運行日` /
/// `運行NO` / `対象乗務員CD` / `出庫日時` / `帰庫日時` を全部持っている
/// (`yhonda-ohishi/nginx` の `Model/Entity/DtakoRow.php` / `Model/Table/DtakoRowsTable.php`)。
/// JOIN も集約も要らない。
///
/// - **期間の条件は 3 つの OR。** 出庫・帰庫・運行日のどれかが窓に入れば拾う。
///   - `出庫日時` / `帰庫日時` の 2 本立ては [`FERRY_SQL`] と同じで、上流の
///     「当月に出庫**または**帰庫した運行」を写したもの
///   - `運行日` を足すのは #205 の 38 と同じ理由 — **日時だけだと月末の運行が落ちる**
///     (alc も `reading_date` 単独から `reading_date OR operation_date` へ直した)
///   - **`COALESCE` で 1 本にまとめないこと。** 関数適用で索引が効かなくなる
///     ([`EVENTS_SQL`] が 0.2 秒 → 4 分になった罠と同じ)。列ごとに条件を分ける
/// - **`読取日` で絞らない。** 読取日は運行終了の後に付く (実測: 運行日 06-24 →
///   読取日 07-06) ので、読取日で窓を切ると月末の運行が丸ごと落ちる
/// - `:driver` が NULL なら全乗務員
///
/// ## `kintai_reader` の GRANT は未確認 (Refs #205 の 42)
///
/// `dtako_rows` 自体は [`FERRY_SQL`] が既に読んでいる (`運行NO` / `対象乗務員CD` /
/// `帰庫日時` / `出庫日時`) が、**`読取日` / `運行日` が GRANT に入っているかは
/// 確かめられていない**。`dtako_ferry_rows` は料金列があるため列単位 GRANT で、
/// 列を足すときは GRANT の追加が要る (そちらの docs)。`dtako_rows` が表単位か
/// 列単位かは読み取れていない。
///
/// **外れても黙って 0 件にはならない** — `map_repo_err` が MariaDB のエラー文を
/// そのまま載せて 502 になる。落ちるのはこの口だけ。
const OPERATION_READING_DATES_SQL: &str = r#"
SELECT r.`対象乗務員CD`                                 AS driver_cd,
       r.`運行NO`                                       AS unko_no,
       DATE_FORMAT(r.`読取日`, '%Y-%m-%d')              AS reading_date,
       DATE_FORMAT(r.`運行日`, '%Y-%m-%d')              AS run_date,
       DATE_FORMAT(r.`出庫日時`, '%Y-%m-%d %H:%i:%s')   AS departure_at,
       DATE_FORMAT(r.`帰庫日時`, '%Y-%m-%d %H:%i:%s')   AS return_at
  FROM dtako_rows r
 WHERE (:driver IS NULL OR r.`対象乗務員CD` = :driver)
   AND (   (r.`出庫日時` >= :from AND r.`出庫日時` < :to)
        OR (r.`帰庫日時` >= :from AND r.`帰庫日時` < :to)
        OR (r.`運行日` >= DATE(:from) AND r.`運行日` < DATE(:to)) )
 ORDER BY r.`対象乗務員CD`, r.`運行NO`
"#;

/// `OPERATION_READING_DATES_SQL` の 1 行 (列の順序と 1:1)。
type ReadingDateRow = (
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// 運行 1 行を JSON へ。`null` は `null` のまま (欠損を化かさない)。
fn reading_date_row_to_json(row: ReadingDateRow) -> serde_json::Value {
    let (driver_cd, unko_no, reading_date, run_date, departure_at, return_at) = row;
    serde_json::json!({
        "driver_cd": driver_cd,
        "unko_no": unko_no,
        "reading_date": reading_date,
        "run_date": run_date,
        "departure_at": departure_at,
        "return_at": return_at,
    })
}

/// フェリー区間 (Refs #146)。
///
/// - **`dtako_ferry_rows` は 3 列しか読めない** (`運行NO` / `開始日時` / `終了日時`)。
///   このテーブルは `標準料金` / `契約料金` を持つので、`kintai_reader` には列単位で
///   GRANT してある。列を足すときは GRANT の追加が要る
/// - 乗務員は `dtako_rows.対象乗務員CD` から取る。**`dtako_ferry_rows.乗務員CD1` は
///   使わない** — 2 名乗務では運行まるごとが別の乗務員のまま記録される (`EVENTS_SQL`
///   と同じ理由)
/// - 突合の鍵は `運行NO`。上流は `substr($dtako_row->運行NO, 0, 22) . "1"` で引いて
///   いるので、`CONCAT(LEFT(r.運行NO, 22), '1')` で同じものを作る
/// - 運行側も月で絞る。上流は当月に出庫 **または** 帰庫した運行だけを回しているので、
///   その条件も写す (ferry の月内条件だけだと、月をまたいだ運行のフェリーを拾って
///   紙と数字がずれる)
/// - `:driver` が NULL なら全乗務員 (`fetch_ferry_between` の `driver: None`)
const FERRY_SQL: &str = r#"
SELECT DATE_FORMAT(f.`開始日時`, '%Y-%m-%d %H:%i:%s') AS start_datetime,
       DATE_FORMAT(f.`終了日時`, '%Y-%m-%d %H:%i:%s') AS end_datetime,
       r.`対象乗務員CD`                               AS driver_id
  FROM dtako_ferry_rows f
  JOIN dtako_rows r ON f.`運行NO` = CONCAT(LEFT(r.`運行NO`, 22), '1')
 WHERE f.`開始日時` >= :from AND f.`開始日時` < :to
   AND (:driver IS NULL OR r.`対象乗務員CD` = :driver)
   AND (   (r.`帰庫日時` >= :from AND r.`帰庫日時` < :to)
        OR (r.`出庫日時` >= :from AND r.`出庫日時` < :to) )
 ORDER BY r.`対象乗務員CD`, f.`開始日時`
"#;

/// `FERRY_SQL` の 1 行。
type FerryRow = (String, String, Option<i64>);

fn ferry_row_to_json(r: FerryRow) -> serde_json::Value {
    serde_json::json!({
        "start_datetime": r.0,
        "end_datetime": r.1,
        "driver_id": r.2,
    })
}

/// DB から取り出した 1 行 (列の順序は `EVENTS_SQL` と 1:1)。
type EventRow = (
    String,
    Option<String>,
    Option<i64>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// 行を JSON へ。`null` は `null` のまま出す (欠損を 0 や空文字に化かさない)。
fn row_to_json(row: EventRow) -> serde_json::Value {
    let (datetime, end_datetime, driver_id, source, state, unko_no, vehicle) = row;
    serde_json::json!({
        "datetime": datetime,
        "end_datetime": end_datetime,
        "driver_id": driver_id,
        "source": source,
        "state": state,
        "unko_no": unko_no,
        "vehicle": vehicle,
    })
}

/// 全乗務員ぶんの 1 行 (列の順序は `ALL_EVENTS_SQL` と 1:1)。`運行NO` / `車輌名` が無い。
type AllEventRow = (String, Option<String>, Option<i64>, String, Option<String>);

/// 全乗務員ぶんの行を JSON へ。`unko_no` / `vehicle` は**キーごと出さない** —
/// 読んでいない列を `null` で埋めると「値が無い」と「読んでいない」が混ざる。
fn all_row_to_json(row: AllEventRow) -> serde_json::Value {
    let (datetime, end_datetime, driver_id, source, state) = row;
    serde_json::json!({
        "datetime": datetime,
        "end_datetime": end_datetime,
        "driver_id": driver_id,
        "source": source,
        "state": state,
    })
}

/// MariaDB 実装。
pub struct MariadbKintaiEventsRepo {
    pool: Pool,
}

/// MariaDB セッションの初期化文 (`OptsBuilder::setup` — pool が接続を使い回す際の
/// `COM_RESET_CONNECTION` 後にも再適用される)。
///
/// クライアント (relay/ブラウザ) が約 100 秒で切断しても**サーバー側では SELECT が
/// 走り続け**、リロード・月切替のたびに積み上がって HDD を食い合う convoy になる
/// (2026-07-29 実害: kintai_reader のクエリが 10 本超 × 最長 582 秒滞留、全リクエストが
/// 数分待ちに)。`max_statement_time` はこのセッションの 60 秒超のステートメントを
/// MariaDB が自動 abort する — **サーバー設定 (my.cnf) は触らない** (セッション変数)。
pub(crate) const MARIADB_SESSION_SETUP: &str = "SET SESSION max_statement_time=60";

impl MariadbKintaiEventsRepo {
    /// config から接続 pool を組む。**接続はここでは張らない** (mysql_async の
    /// pool は lazy) ので、DB 停止中でも起動は失敗しない — 実際に読むときに 502。
    pub fn new(cfg: &MariadbConfig) -> Self {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(cfg.host.clone())
            .tcp_port(cfg.port)
            .user(Some(cfg.user.clone()))
            .pass(Some(cfg.password.clone()))
            .db_name(Some(cfg.database.clone()))
            .setup(vec![MARIADB_SESSION_SETUP.to_string()]);
        Self {
            pool: Pool::new(opts),
        }
    }
}

#[async_trait]
impl KintaiEventsApi for MariadbKintaiEventsRepo {
    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<FerryRow> = conn
            .exec(
                FERRY_SQL,
                params! {
                    "from" => from,
                    "to" => to,
                    "driver" => driver,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(ferry_row_to_json).collect())
    }

    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<EventRow> = conn
            .exec(
                EVENTS_SQL,
                params! {
                    "driver" => driver,
                    "from" => from,
                    "to" => to,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(row_to_json).collect())
    }

    /// 期間にかかる運行と読取日 (Refs #205 の 42、[`OPERATION_READING_DATES_SQL`])。
    async fn fetch_operation_reading_dates_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<ReadingDateRow> = conn
            .exec(
                OPERATION_READING_DATES_SQL,
                params! {
                    "from" => from,
                    "to" => to,
                    "driver" => driver,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(reading_date_row_to_json).collect())
    }

    /// 休息だけを `運行NO` 付きで両表から読む (Refs #205 の 41、[`REST_EVENTS_SQL`])。
    async fn fetch_rest_events_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<RestEventRow> = conn
            .exec(
                REST_EVENTS_SQL,
                params! {
                    "from" => from,
                    "to" => to,
                    "driver" => driver,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(rest_row_to_json).collect())
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<AllEventRow> = conn
            .exec(
                ALL_EVENTS_SQL,
                params! {
                    "from" => from,
                    "to" => to,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(all_row_to_json).collect())
    }

    /// 月初をまたぐ運行・勤務の始まり ([`mariadb_month_head_anchors`])。
    async fn fetch_month_head_anchors(
        &self,
        month_start: &str,
        to: &str,
    ) -> Result<std::collections::BTreeMap<u64, String>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        mariadb_month_head_anchors(&mut conn, month_start, to).await
    }

    /// 既定実装 (読んでから捨てる) を上書きし、**`dtako_events` を読まない**。
    async fn fetch_timecard_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<EventRow> = conn
            .exec(
                TIMECARD_EVENTS_SQL,
                params! {
                    "driver" => driver,
                    "from" => from,
                    "to" => to,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(row_to_json).collect())
    }

    /// 既定実装 (読んでから捨てる) を上書きし、**全乗務員ぶんを 1 クエリで**返す。
    async fn fetch_timecard_window(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        let rows: Vec<EventRow> = conn
            .exec(
                TIMECARD_WINDOW_SQL,
                params! {
                    "from" => from,
                    "to" => to,
                },
            )
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))?;
        Ok(rows.into_iter().map(row_to_json).collect())
    }

    /// 既定実装 (全行を読んで CD だけ拾う) を上書きし、**CD しか転送しない**。
    async fn fetch_timecard_driver_cds_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<u64>, KintaiRepoError> {
        let mut conn = self
            .pool
            .get_conn()
            .await
            .map_err(|e| KintaiRepoError::QueryFailed(format!("connect: {e}")))?;
        conn.exec(
            TIMECARD_DRIVERS_SQL,
            params! {
                "from" => from,
                "to" => to,
            },
        )
        .await
        .map_err(|e| KintaiRepoError::QueryFailed(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn row_to_json_keeps_nulls() {
        let v = row_to_json((
            "2026-07-23 06:11:45".to_string(),
            None,
            Some(1051),
            "timecard".to_string(),
            Some("始業".to_string()),
            None,
            None,
        ));
        assert_eq!(v["datetime"], "2026-07-23 06:11:45");
        assert_eq!(v["driver_id"], 1051);
        assert_eq!(v["state"], "始業");
        assert!(v["end_datetime"].is_null());
        assert!(v["unko_no"].is_null());
        assert!(v["vehicle"].is_null());
    }

    #[test]
    fn all_row_to_json_omits_unread_columns() {
        let v = all_row_to_json((
            "2026-06-02 06:00:00".to_string(),
            Some("2026-06-02 06:20:00".to_string()),
            Some(1119),
            "dtako_events".to_string(),
            Some("休憩".to_string()),
        ));
        assert_eq!(v["driver_id"], 1119);
        assert_eq!(v["end_datetime"], "2026-06-02 06:20:00");
        // 読んでいない列はキーごと出さない (`null` にしない)
        assert!(v.get("unko_no").is_none());
        assert!(v.get("vehicle").is_none());
    }

    #[test]
    fn all_events_sql_drops_the_vehicle_join() {
        // 1.20 秒 → 0.25 秒 の差はここ。うっかり戻さないよう固定する
        assert!(!ALL_EVENTS_SQL.contains("dtako_cars"));
        assert!(!ALL_EVENTS_SQL.contains("運行NO"));
        // 生イベントの読み出し口 (`/events`) は絞らないまま
        assert!(EVENTS_SQL.contains("dtako_cars"));
        assert!(!EVENTS_SQL.contains("イベント名` IN"));
    }

    #[test]
    fn all_events_sql_does_not_filter_event_names() {
        // イベント名の絞りは単一乗務員経路と値が割れる事故を 2 度起こした
        // (#167 と 1556 林田 03-26、Refs ohishi-exp/nuxt-dtako-admin#501)。
        // うっかり戻さないよう固定する
        assert!(!ALL_EVENTS_SQL.contains("イベント名` IN"));
    }

    #[tokio::test]
    async fn disabled_repo_is_not_configured() {
        let err = DisabledKintaiEventsRepo
            .fetch_events("2026-07", 1051)
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
        assert!(err.to_string().contains("未設定"));
        let err = DisabledKintaiEventsRepo
            .fetch_all_events("2026-07")
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
        // 月が壊れていれば DB へ行く前に落とす
        let err = DisabledKintaiEventsRepo
            .fetch_all_events("2026-13")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("bad month"));
        assert!(KintaiRepoError::QueryFailed("boom".into())
            .to_string()
            .contains("boom"));
    }

    /// 打刻が無い instance でも**打刻専用の口は fail-closed**。
    /// 空配列を返すと「その月は誰も打刻していない」と区別が付かない。
    #[tokio::test]
    async fn disabled_repo_fails_closed_on_the_timecard_routes() {
        let err = DisabledKintaiEventsRepo
            .fetch_timecard_events_between("2026-07-01 00:00:00", "2026-08-01 00:00:00", 1130)
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
        let err = DisabledKintaiEventsRepo
            .fetch_timecard_driver_cds_between("2026-07-01 00:00:00", "2026-08-01 00:00:00")
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
    }

    // ── 月初をまたぐ運行・勤務の起点 (Refs ohishi-exp/nuxt-dtako-admin#1123) ──

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

    #[test]
    fn head_punch_maps_the_row_columns() {
        let p = head_punch((1194, Some("終業".into()), Some("a".into()), None));
        assert_eq!(p, punch(1194, Some("終業"), Some("a"), None));
    }

    /// MariaDB は CI に無いので SQL を文字列で固定する (運行終了は `state = 11`、
    /// 打刻は 30 = 始業 / 31 = 終業、月初より前の MAX と窓の最初)。
    #[test]
    fn the_head_sql_reads_run_ends_and_the_punch_edges() {
        assert!(HEAD_RUN_ENDS_SQL.contains("FROM time_card_dtako t"));
        assert!(HEAD_RUN_ENDS_SQL.contains("t.state = 11"));
        assert!(HEAD_RUN_ENDS_SQL.contains("t.datetime >= :from AND t.datetime < :to"));
        assert!(HEAD_PUNCHES_SQL.contains("IF(s.state = 30, '始業', '終業')"));
        assert!(HEAD_PUNCHES_SQL.contains("ORDER BY s.datetime, s.state"));
        assert!(HEAD_PUNCHES_SQL.contains("b.state = 30 AND b.datetime < :from"));
        assert!(HEAD_PUNCHES_SQL.contains("e.state = 31 AND e.datetime < :from"));
        assert!(
            !HEAD_PUNCHES_SQL.contains("COALESCE"),
            "索引を殺す関数を当てない"
        );
    }

    /// 既定は遡らない (空)、dtako 指紋の `since` 版は今までの範囲に落ちる。
    #[tokio::test]
    async fn the_defaults_do_not_look_back() {
        let got = DisabledKintaiEventsRepo
            .fetch_month_head_anchors(APRIL, "2026-05-02 00:00:00")
            .await
            .unwrap();
        assert!(got.is_empty());
        let d = DisabledKintaiEventsRepo
            .fetch_dtako_month_digest_since("2026-04", chrono::NaiveDate::from_ymd_opt(2026, 3, 31))
            .await
            .unwrap();
        assert!(d.is_none());
    }

    fn row(driver: Option<i64>, source: &str) -> serde_json::Value {
        let mut v = serde_json::json!({ "source": source });
        if let Some(d) = driver {
            v["driver_id"] = serde_json::json!(d);
        }
        v
    }

    /// 運ばないと決めた `state` は**そもそも読まない** — SQL に落とし込まれている。
    ///
    /// 定数と SQL が 2 実装になると、片方だけ直して静かに運び始める。
    #[test]
    fn the_sql_drops_what_we_do_not_carry() {
        for s in crate::kintai_push::NOT_CARRIED_STATES {
            assert!(
                TIMECARD_EVENTS_SQL.contains(&format!("<> '{s}'")),
                "{s} が SQL で落とされていない"
            );
        }
        // 番号ではなく解決後の名前で落とす (event_name が自由記述のため)
        assert!(TIMECARD_EVENTS_SQL.contains("COALESCE(t.event_name, s.name) <>"));
    }

    /// 既定実装も同じものを落とす (HTTP 版と MariaDB 版で結果を揃える)。
    #[test]
    fn the_default_filter_drops_what_we_do_not_carry() {
        let mut rest = row(Some(1130), "dtako");
        rest["state"] = serde_json::json!("休息");
        assert!(!is_carried(&rest));
        // 前後の空白は上流の整形なのでこちらで吸う
        rest["state"] = serde_json::json!("  休息  ");
        assert!(!is_carried(&rest));
        // 読み替え済みの開始 / 終了は運ぶ
        rest["state"] = serde_json::json!("休息開始");
        assert!(is_carried(&rest));
        // state を持たない行は state 以外の理由で落ちるので、ここでは通す
        assert!(is_carried(&row(Some(1130), "timecard")));
        assert!(!is_carried(&row(Some(1130), "dtako_events")));
    }

    /// push する 2 つの `source` だけを通す。
    #[test]
    fn only_the_pushed_sources_survive() {
        assert!(is_pushed_source(&row(Some(1130), "timecard")));
        assert!(is_pushed_source(&row(Some(1130), "dtako")));
        // デジタコ生イベントは R2 にあるので運ばない (#205 の決定 5)
        assert!(!is_pushed_source(&row(Some(1130), "dtako_events")));
        assert!(!is_pushed_source(&serde_json::json!({})));
    }

    /// 乗務員CD は昇順・重複無し。**0 と乗務員の無い行は落ちる。**
    #[test]
    fn driver_cds_are_sorted_deduped_and_positive() {
        let cds = timecard_driver_cds(vec![
            row(Some(1300), "timecard"),
            row(Some(1130), "dtako"),
            row(Some(1130), "timecard"),
            // 乗務員CD ではない値 — ページの枠を食っていた実物
            row(Some(0), "timecard"),
            row(Some(-1), "timecard"),
            row(None, "timecard"),
            // 打刻を持たない乗務員は対象外
            row(Some(9999), "dtako_events"),
        ]);
        assert_eq!(cds, vec![1130, 1300]);
    }

    /// 既定実装は `dtako_events` を**読んでから**落とす (SQL を分けられない実装向け)。
    #[tokio::test]
    async fn the_default_timecard_read_filters_by_source() {
        struct Mixed;
        #[async_trait]
        impl KintaiEventsApi for Mixed {
            async fn fetch_events_between(
                &self,
                _: &str,
                _: &str,
                _: u64,
            ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
                Ok(vec![
                    row(Some(1130), "timecard"),
                    row(Some(1130), "dtako_events"),
                    row(Some(1130), "dtako"),
                ])
            }
            async fn fetch_all_events_between(
                &self,
                _: &str,
                _: &str,
            ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
                Ok(vec![
                    row(Some(1130), "timecard"),
                    row(Some(9999), "dtako_events"),
                ])
            }
            async fn fetch_ferry_between(
                &self,
                _: &str,
                _: &str,
                _: Option<u64>,
            ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
                unreachable!()
            }
        }
        let got = Mixed
            .fetch_timecard_events_between("a", "b", 1130)
            .await
            .unwrap();
        assert_eq!(
            got,
            vec![row(Some(1130), "timecard"), row(Some(1130), "dtako")]
        );
        assert_eq!(
            Mixed
                .fetch_timecard_driver_cds_between("a", "b")
                .await
                .unwrap(),
            vec![1130]
        );
    }
}
