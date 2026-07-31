//! 打刻を `kintai.kintai_events` から読み返す実装 (Refs #205 の G6)。
//!
//! [`crate::kintai_repo::MariadbKintaiEventsRepo`] /
//! [`crate::kintai_http_repo::HttpKintaiEventsRepo`] に続く 3 つ目の
//! [`KintaiEventsApi`] 実装。**書いたものを読み返すだけ**で、写しも解釈もしない。
//!
//! ## 何の穴を埋めるか
//!
//! 02 のモジュール docs が残していた宿題そのもの:
//!
//! > `fallback` が無い実行形態 (GCP) では打刻が読めないため `shifts_from_timecard`
//! > が空になる — これは #205 の 04 / 05 (打刻の push) が埋める穴
//!
//! GCP には MariaDB が無いので [`HttpKintaiEventsRepo`] の `fallback` が `None` に
//! なり、`timecard` / `dtako` の行が 1 つも返らない。04b で打刻は
//! `POST /api/kintai/timecard/window` から `kintai.kintai_events` に入るように
//! なった (2026-05+06 で 95 名 / 6,923 events) ので、**読み返す口を足せば穴が閉じる**。
//!
//! ```text
//! HttpKintaiEventsRepo          … dtako_events (alc / R2 から)
//!   └ fallback = PgKintaiEventsRepo … timecard / dtako (Supabase から)  ← ここ
//! ```
//!
//! [`HttpKintaiEventsRepo`]: crate::kintai_http_repo::HttpKintaiEventsRepo
//!
//! ## MariaDB の打刻 2 ブランチと 1 対 1 に写す
//!
//! 返す行の形は [`crate::kintai_repo`] の `TIMECARD_EVENTS_SQL` (単一乗務員) /
//! `ALL_EVENTS_SQL` (全乗務員) と**同じキー構成**にする。読み先が変わっても行の形が
//! 変わらないことが要る — `kosoku::drop_duplicate_rows` は行の完全一致で重複を
//! 判定し、[`crate::kintai_fold`] の指紋は**行まるごと**を材料にするので、キーが
//! 1 つ違うだけで「中身は同じなのに毎回 stale」に倒れる。
//!
//! | キー | 値 | なぜ |
//! |---|---|---|
//! | `datetime` | `to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', …)` | driver の時刻型と TZ 解釈を経路に持ち込まない (`STORED_SIGNATURES_SQL` と同じ流儀) |
//! | `end_datetime` | 常に `null` | 打刻は点で、区間を持つのは `dtako_events` だけ |
//! | `driver_id` | `driver_cd` | MariaDB 側の列名に合わせる |
//! | `source` | `timecard` / `dtako` | [`PUSHED_SOURCES`] に絞る |
//! | `state` | そのまま | |
//! | `unko_no` | そのまま (単一乗務員版のみ) | |
//! | `vehicle` | 常に `null` (単一乗務員版のみ) | 車輌名は `dtako_events` 側にしか無く、打刻には最初から付いていない |
//!
//! **`raw` は載せない。** 追跡用に保存してある元の生行そのもので、載せると指紋の
//! 材料が数倍に膨らむうえ、MariaDB 経路には無いキーなので形が割れる。
//!
//! 全乗務員版が `unko_no` / `vehicle` を**キーごと出さない**のは `ALL_EVENTS_SQL` と
//! 同じ理由 — 読んでいない列を `null` で埋めると「値が無い」と「読んでいない」が
//! 混ざる。こちらは SELECT にも書かない。
//!
//! ## 期間は範囲比較のまま投げる
//!
//! trait の `[from, to)` は JST の壁時計文字列なので、[`crate::kintai_push::jst_at`]
//! で `TIMESTAMPTZ` にしてから `occurred_at >= $from AND occurred_at < $to` で比べる。
//! `(occurred_at AT TIME ZONE 'Asia/Tokyo')::date = ANY(...)` のように関数を当てると
//! `kintai_events_driver_time` が効かなくなる (`DELETE_DAYS_SQL` と同じ罠)。
//!
//! ## テナントは書き先の pin と別物
//!
//! 読みのテナントは `[kintai_events] tenant_id` — alc へ `X-Tenant-ID` として
//! 送っているのと同じ値で固定する。GCP は `KINTAI_PUSH_TENANT_ID` を設定しない運用
//! (pin 無し、書き先は `X-Tenant-ID` を名乗ったリクエストが決める) なので、
//! [`KintaiPgStore::tenant_id`] は nil のことがあり、そちらは使えない。
//!
//! **したがって「読み出しのテナント = 設定の 1 つ」という暗黙の前提が入る。**
//! `/api/kintai/kosoku-daily` を別テナントのリクエストが叩いても、この repo は
//! 設定のテナントの打刻を返す。単一テナント運用では正しいが、束ねのタスク側で
//! 「リクエスト tenant と設定 tenant が一致すること」を assert すること。
//!
//! [`PUSHED_SOURCES`]: crate::kintai_push::PUSHED_SOURCES
//! [`KintaiPgStore::tenant_id`]: crate::kintai_push::KintaiPgStore::tenant_id

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use sqlx::Row;

use crate::kintai_push::{jst_at, KintaiPgStore, PUSHED_SOURCES};
use crate::kintai_repo::{KintaiEventsApi, KintaiRepoError};

/// 単一乗務員ぶんの打刻。`kintai_events_driver_time`
/// (`tenant_id, driver_cd, occurred_at` INCLUDE `state, source, unko_no`) だけで
/// 済むので index-only scan になる。
///
/// `ORDER BY` は `TIMECARD_EVENTS_SQL` の `ORDER BY datetime, source` と同じ並び。
/// 文字列ではなく `occurred_at` で並べるが、単一 TZ の壁時計なので順序は同じ。
/// `source` に `COLLATE "C"` を当てるのは、DB の照合順序で並びが変わらないように
/// するため (`STORED_SIGNATURES_SQL` と同じ理由)。
const EVENTS_SQL: &str = r#"
SELECT to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS datetime,
       driver_cd AS driver_id,
       source,
       state,
       unko_no
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND driver_cd = $2
   AND occurred_at >= $3 AND occurred_at < $4
   AND source = ANY($5)
 ORDER BY occurred_at, source COLLATE "C"
"#;

/// 全乗務員ぶんの打刻。[`EVENTS_SQL`] から乗務員の絞り込みを外し、
/// **`unko_no` を SELECT ごと落とした**もの (`ALL_EVENTS_SQL` と同じ形)。
///
/// `occurred_at` は索引の 3 列目なので範囲でシークはできないが、走査は
/// `tenant_id` の枝の中に閉じ、必要な列は INCLUDE に載っている。
const ALL_EVENTS_SQL: &str = r#"
SELECT to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS datetime,
       driver_cd AS driver_id,
       source,
       state
  FROM kintai.kintai_events
 WHERE tenant_id = $1
   AND occurred_at >= $2 AND occurred_at < $3
   AND source = ANY($4)
 ORDER BY driver_cd, occurred_at, source COLLATE "C"
"#;

/// `kintai.kintai_events` から打刻を読む [`KintaiEventsApi`] 実装。
pub struct PgKintaiEventsRepo {
    /// 接続は [`KintaiPgStore`] のものを共有する (`max_connections = 1`)。
    /// 読み用に 2 本目の pool を張らないのは、Supabase の session mode pooler で
    /// 接続数が上限に効くのと、書き込みと読み出しが同じ時刻の同じ行を見るため。
    ///
    /// **代償: 打刻の受け口が書いている間、読みはその 1 本を待つ** (`acquire_timeout`
    /// = `[kintai_push] connect_timeout_secs` を超えると失敗 = 503)。1 本に絞って
    /// いるのは delete-then-insert を交差させないためで、そちらを崩す方が高くつく。
    store: Arc<KintaiPgStore>,
    /// 読み出しのテナント。**書き先の pin とは別に持つ** (モジュール docs 参照)。
    tenant_id: uuid::Uuid,
}

impl PgKintaiEventsRepo {
    pub fn new(store: Arc<KintaiPgStore>, tenant_id: uuid::Uuid) -> Self {
        Self { store, tenant_id }
    }

    /// 読み出しのテナント。**束ねのタスクが「リクエストの tenant と一致するか」を
    /// assert するための口** (モジュール docs の暗黙の前提)。
    pub fn tenant_id(&self) -> uuid::Uuid {
        self.tenant_id
    }
}

/// trait の `[from, to)` (JST の壁時計) を `TIMESTAMPTZ` へ渡せる形に。
fn window(
    from: &str,
    to: &str,
) -> Result<(DateTime<FixedOffset>, DateTime<FixedOffset>), KintaiRepoError> {
    let at = |s: &str| jst_at(s).map_err(|e| KintaiRepoError::QueryFailed(e.to_string()));
    Ok((at(from)?, at(to)?))
}

fn db_err(e: sqlx::Error) -> KintaiRepoError {
    KintaiRepoError::QueryFailed(format!("kintai_events read failed: {e}"))
}

/// 単一乗務員版の 1 行 (`row_to_json` / `event_to_json` と同じ 7 キー)。
fn row_to_json(r: &sqlx::postgres::PgRow) -> Result<serde_json::Value, KintaiRepoError> {
    let get = |k: &str| r.try_get::<String, _>(k).map_err(db_err);
    Ok(serde_json::json!({
        "datetime": get("datetime")?,
        "end_datetime": serde_json::Value::Null,
        "driver_id": r.try_get::<i64, _>("driver_id").map_err(db_err)?,
        "source": get("source")?,
        "state": get("state")?,
        "unko_no": r.try_get::<Option<String>, _>("unko_no").map_err(db_err)?,
        "vehicle": serde_json::Value::Null,
    }))
}

/// 全乗務員版の 1 行 (`all_row_to_json` / `event_to_all_json` と同じ 5 キー)。
fn all_row_to_json(r: &sqlx::postgres::PgRow) -> Result<serde_json::Value, KintaiRepoError> {
    let get = |k: &str| r.try_get::<String, _>(k).map_err(db_err);
    Ok(serde_json::json!({
        "datetime": get("datetime")?,
        "end_datetime": serde_json::Value::Null,
        "driver_id": r.try_get::<i64, _>("driver_id").map_err(db_err)?,
        "source": get("source")?,
        "state": get("state")?,
    }))
}

#[async_trait]
impl KintaiEventsApi for PgKintaiEventsRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = window(from, to)?;
        let driver_cd = i64::try_from(driver)
            .map_err(|e| KintaiRepoError::QueryFailed(format!("driver: {e}")))?;
        let rows = sqlx::query(EVENTS_SQL)
            .bind(self.tenant_id)
            .bind(driver_cd)
            .bind(from)
            .bind(to)
            .bind(&PUSHED_SOURCES[..])
            .fetch_all(self.store.pool())
            .await
            .map_err(db_err)?;
        rows.iter().map(row_to_json).collect()
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        let (from, to) = window(from, to)?;
        let rows = sqlx::query(ALL_EVENTS_SQL)
            .bind(self.tenant_id)
            .bind(from)
            .bind(to)
            .bind(&PUSHED_SOURCES[..])
            .fetch_all(self.store.pool())
            .await
            .map_err(db_err)?;
        rows.iter().map(all_row_to_json).collect()
    }

    /// フェリー区間は `kintai` スキーマに無い。**突合はオンプレ専用** (#205 の決定 8:
    /// CakePHP が private で GCP からは `pdf-json` を引けない) で、畳む側
    /// ([`crate::kintai_fold`]) もフェリーを使わないため、口ごと持たない。
    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
        Err(KintaiRepoError::NotConfigured)
    }
}
