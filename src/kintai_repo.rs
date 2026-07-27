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
    /// 期間の決め方は呼び出し側の担当。区間イベントは重なりで拾うので、
    /// 期間の前に始まった長い休息もこの範囲で取れる (`EVENTS_SQL` 参照)。
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
/// - 区間イベントは**開始ではなく重なりで絞る** (`開始日時 < to AND 終了日時 >= from`)。
///   開始で絞ると、月初を跨ぐ長い休息が丸ごと落ちる — `kosoku-daily` は「休息の終了 =
///   始業」で勤務を切るので、月初の勤務が組めなくなる。車輌故障で 1 週間止まったような
///   ケースもあるため、遡る日数を決め打ちにする対処では塞げない。`終了日時` が NULL の
///   行は `開始日時` で代替して従来どおり扱う
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
       e.`乗務員CD1`,
       'dtako_events',
       e.`イベント名`,
       e.`運行NO`,
       c.`車輌名`
  FROM dtako_events e
  LEFT JOIN dtako_cars c ON c.`車輌CD` = e.`車輌CD`
 WHERE e.`乗務員CD1` = :driver
   AND e.`開始日時` < :to
   AND COALESCE(e.`終了日時`, e.`開始日時`) >= :from
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
