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
