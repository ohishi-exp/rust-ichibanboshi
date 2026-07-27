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
}

/// 対象月の取得範囲 `[月初, 翌月+1日)` を `YYYY-MM-DD HH:MM:SS` で返す。
///
/// 翌月 1 日ではなく**翌月 2 日**まで広げるのは、日跨ぎ勤務の終わり (終業打刻・
/// 帰庫) が翌月にはみ出すため — 月で切ると拘束の終わりが消える (上流 `daily-json`
/// が `queryEnd = nextMonth + 1day` にしているのと同じ考え方)。
///
/// `month` は呼び出し側 (`is_valid_month`) で検証済みの `YYYY-MM` 前提。
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

/// MariaDB 実装。
pub struct MariadbKintaiEventsRepo {
    pool: Pool,
}

impl MariadbKintaiEventsRepo {
    /// config から接続 pool を組む。**接続はここでは張らない** (mysql_async の
    /// pool は lazy) ので、DB 停止中でも起動は失敗しない — 実際に読むときに 502。
    pub fn new(cfg: &MariadbConfig) -> Self {
        let opts = mysql_async::OptsBuilder::default()
            .ip_or_hostname(cfg.host.clone())
            .tcp_port(cfg.port)
            .user(Some(cfg.user.clone()))
            .pass(Some(cfg.password.clone()))
            .db_name(Some(cfg.database.clone()));
        Self {
            pool: Pool::new(opts),
        }
    }
}

#[async_trait]
impl KintaiEventsApi for MariadbKintaiEventsRepo {
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

    #[tokio::test]
    async fn disabled_repo_is_not_configured() {
        let err = DisabledKintaiEventsRepo
            .fetch_events("2026-07", 1051)
            .await
            .unwrap_err();
        assert!(matches!(err, KintaiRepoError::NotConfigured));
        assert!(err.to_string().contains("未設定"));
        assert!(KintaiRepoError::QueryFailed("boom".into())
            .to_string()
            .contains("boom"));
    }
}
