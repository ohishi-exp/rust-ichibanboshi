//! relay が送った打刻の写し (`kintai.kintai_events`) を fold のために読む SQL と、行の形 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root (Cloud Run 版) の `PgKintaiEventsRepo` (`src/kintai_pg_repo.rs`、sqlx) と勤怠 Worker の fold の読み
//! (kintai-pg の `FoldStore`、tokio-postgres の `query_typed`) が同じものを使う (写さない)。移す前の root の本文と同じことは
//! `tests/pushed_events_snapshot.rs` が基点 (2b98104) の sha256 で縛る。
//!
//! - 全 SQL の `$1` はテナント、`$2`・`$3` は `[from, to)` の `TIMESTAMPTZ` ([`crate::kintai_push::jst_at`] で作る)
//! - [`ALL_EVENTS_SQL`] の `$4` は [`crate::kintai_push::PUSHED_SOURCES`] (`text[]`)。`休息` 等の state では絞らない
//!   (写しに入る state は送る側が決める)
//! - 行の形は MariaDB の `ALL_EVENTS_SQL` の 5 キー ([`all_event_json`]) と、遡り起点の材料 ([`head_run_end`]・[`head_punch`])

use crate::window::HeadPunch;

/// 全乗務員ぶんの打刻。root の単一乗務員版 (`src/kintai_pg_repo.rs` の `EVENTS_SQL`) から乗務員の絞り込みを外し、
/// **`unko_no` を SELECT ごと落とした**もの (`ALL_EVENTS_SQL` と同じ形)。
///
/// `occurred_at` は索引の 3 列目なので範囲でシークはできないが、走査は
/// `tenant_id` の枝の中に閉じ、必要な列は INCLUDE に載っている。
pub const ALL_EVENTS_SQL: &str = r#"
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

/// 窓の中の**運行終了**と、その `unko_no` (Refs ohishi-exp/nuxt-dtako-admin#1123)。
///
/// MariaDB の `HEAD_RUN_ENDS_SQL` (`state = 11`) と同じ意味で、`source = 'dtako'`
/// の `運行終了` に絞る。`MONTH_OPERATIONS_SQL` (`kintai_push`) は流用しない —
/// あちらは `state` で絞らず「窓の中に何かある運行」を拾うので、月初より前に
/// 始まって窓の中に休息だけ持つ運行まで起点にしてしまい、MariaDB とずれる。
/// 月初より前に始まったかの判定は Rust 側 ([`crate::window::month_head_anchors`])。
pub const HEAD_RUN_ENDS_SQL: &str = r#"
SELECT driver_cd, unko_no
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND source = 'dtako' AND state = '運行終了'
   AND occurred_at >= $2 AND occurred_at < $3
   AND unko_no IS NOT NULL AND driver_cd > 0
"#;

/// 窓に打刻がある乗務員ごとの「月初時点の打刻の姿」(MariaDB の `HEAD_PUNCHES_SQL`
/// と同じ形)。相関サブクエリは 3 本とも `kintai_events_driver_time`
/// (`tenant_id, driver_cd, occurred_at`) を乗務員で引く。同時刻の始業・終業は
/// 始業を先に置く (`COLLATE "C"` で 始 < 終)。
pub const HEAD_PUNCHES_SQL: &str = r#"
SELECT f.driver_cd,
       (SELECT s.state
          FROM kintai.kintai_events s
         WHERE s.tenant_id = $1 AND s.driver_cd = f.driver_cd AND s.source = 'timecard'
           AND s.state IN ('始業', '終業')
           AND s.occurred_at >= $2 AND s.occurred_at < $3
         ORDER BY s.occurred_at, s.state COLLATE "C"
         LIMIT 1) AS first_state,
       (SELECT to_char(max(b.occurred_at) AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
          FROM kintai.kintai_events b
         WHERE b.tenant_id = $1 AND b.driver_cd = f.driver_cd AND b.source = 'timecard'
           AND b.state = '始業' AND b.occurred_at < $2) AS last_start,
       (SELECT to_char(max(e.occurred_at) AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
          FROM kintai.kintai_events e
         WHERE e.tenant_id = $1 AND e.driver_cd = f.driver_cd AND e.source = 'timecard'
           AND e.state = '終業' AND e.occurred_at < $2) AS last_end
  FROM (SELECT DISTINCT driver_cd
          FROM kintai.kintai_events
         WHERE tenant_id = $1 AND source = 'timecard' AND driver_cd > 0
           AND occurred_at >= $2 AND occurred_at < $3) f
"#;

/// [`ALL_EVENTS_SQL`] の 1 行を、MariaDB の `ALL_EVENTS_SQL` と同じ 5 キーの JSON に (`end_datetime` は常に null —
/// 打刻は点で、区間を持つのは `dtako_events` だけ)。
pub fn all_event_json(
    datetime: String,
    driver_id: i64,
    source: String,
    state: String,
) -> serde_json::Value {
    serde_json::json!({
        "datetime": datetime,
        "end_datetime": serde_json::Value::Null,
        "driver_id": driver_id,
        "source": source,
        "state": state,
    })
}

/// [`HEAD_RUN_ENDS_SQL`] の 1 行 (0 以下の CD は SQL で落としてある)。
pub fn head_run_end(driver_cd: i64, unko_no: String) -> (u64, String) {
    (driver_cd as u64, unko_no)
}

/// [`HEAD_PUNCHES_SQL`] の 1 行 (0 以下の CD は SQL で落としてある)。
pub fn head_punch(
    driver_cd: i64,
    first_state: Option<String>,
    last_start: Option<String>,
    last_end: Option<String>,
) -> HeadPunch {
    HeadPunch {
        driver: driver_cd as u64,
        first_state,
        last_start,
        last_end,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_take_the_shape_of_the_mariadb_rows() {
        let row = all_event_json(
            "2026-06-01 07:30:00".into(),
            1101,
            "timecard".into(),
            "始業".into(),
        );
        let want = serde_json::json!({"datetime": "2026-06-01 07:30:00", "end_datetime": null,
            "driver_id": 1101, "source": "timecard", "state": "始業"});
        assert_eq!(row, want);
        assert_eq!(head_run_end(7, "u".into()), (7, "u".to_string()));
        let p = head_punch(
            8,
            Some("終業".into()),
            None,
            Some("2026-05-31 18:00:00".into()),
        );
        assert_eq!((p.driver, p.first_state.as_deref()), (8, Some("終業")));
        assert_eq!(
            (p.last_start, p.last_end.as_deref()),
            (None, Some("2026-05-31 18:00:00"))
        );
    }
}
