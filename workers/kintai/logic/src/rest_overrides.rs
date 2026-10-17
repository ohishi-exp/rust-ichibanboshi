//! 休息の開始の補正 (`kintai.rest_start_overrides`、migration 011) の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! theearth の拘束時間管理表は、**休息の直前の運転**を休息ボタンの時刻 (alc の `休息` の行の開始) ではなく、車載器の
//! 速度データ (NET780 の `.spd`) で最後に車が動いた時刻で終える (休憩・運行終了の直前の運転は区間時間どおり)。
//! 最後の動きとボタンが同じ分に収まらないときだけ、運転と拘束が 1 分ずれる (実測 2026-10-17、北海大運の 2 件)。
//!
//! 勤怠 Worker は速度データを持たないので、relay が管理表と 1 分ずれた勤務だけ速度データを読み、最後の動きを
//! `PUT /api/kintai/rest-start-overrides` で書く。fold は読んだ alc の行のうち、乗務員と開始 (秒まで) が補正と一致する
//! `休息` の行の開始を最後の動きに差し替える ([`apply_rest_start_overrides`])。alc の行 (元データ) は書き換えない。
//!
//! - `PUT` の本文: `{ "items": [{ "driver_cd", "rest_start_at", "last_move_at", "unko_no" }] }`。時刻は JST の
//!   `YYYY-MM-DD HH:MM:SS`。同じ (乗務員, 休息の開始) の行は置き換える
//! - `GET` のクエリ: `from`・`to` (JST の `YYYY-MM-DD`、`to` の日を含む)。休息の開始がその範囲の行を返す

use chrono::{Duration, NaiveDate, NaiveDateTime};
use serde::Deserialize;
use serde_json::Value;

use crate::common::{bad_request, parse_json, parse_query, Fail};

/// 時刻の書式 (alc の行の `datetime` と同じ)。
const FMT: &str = "%Y-%m-%d %H:%M:%S";

/// 1 回の `PUT` で書ける件数の上限 (1 か月の補正は多くても数件)。
pub const MAX_ITEMS: usize = 200;

/// 最後の動きが休息の開始からどれだけ前まで許すか (migration 011 の CHECK と同じ)。
pub const MAX_LEAD: Duration = Duration::hours(1);

/// 今の `source` (migration 011 の CHECK と同じ)。
pub const SOURCE_NET780: &str = "net780";

/// 補正 1 件 (fold が使う形)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestStartOverride {
    pub driver_cd: i64,
    /// alc の `休息` の行の開始 (JST の壁時計、秒まで)
    pub rest_start: NaiveDateTime,
    /// 速度データで最後に車が動いた時刻 (JST の壁時計)
    pub last_move: NaiveDateTime,
    pub unko_no: String,
}

/// `PUT` の本文の 1 件 (検査前)。
#[derive(Debug, Deserialize)]
struct ItemInput {
    driver_cd: i64,
    rest_start_at: String,
    last_move_at: String,
    unko_no: String,
}

#[derive(Debug, Deserialize)]
struct PutBody {
    items: Vec<ItemInput>,
}

fn at(name: &str, s: &str) -> Result<NaiveDateTime, Fail> {
    NaiveDateTime::parse_from_str(s, FMT).map_err(|_| {
        bad_request(format!(
            "{name} は YYYY-MM-DD HH:MM:SS で指定してください: {s}"
        ))
    })
}

fn checked(item: ItemInput) -> Result<RestStartOverride, Fail> {
    if item.driver_cd <= 0 {
        return Err(bad_request(format!(
            "driver_cd が不正です: {}",
            item.driver_cd
        )));
    }
    let rest_start = at("rest_start_at", &item.rest_start_at)?;
    let last_move = at("last_move_at", &item.last_move_at)?;
    if !(last_move < rest_start && rest_start - last_move <= MAX_LEAD) {
        let msg = "last_move_at は rest_start_at より前の 1 時間以内にしてください";
        return Err(bad_request(format!("{msg}: {}", item.rest_start_at)));
    }
    if item.unko_no.len() != 22 || !item.unko_no.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad_request(format!(
            "unko_no は 22 桁の数字です: {}",
            item.unko_no
        )));
    }
    Ok(RestStartOverride {
        driver_cd: item.driver_cd,
        rest_start,
        last_move,
        unko_no: item.unko_no,
    })
}

/// `PUT /api/kintai/rest-start-overrides` の本文。Json の拒否 → 件数 (1〜[`MAX_ITEMS`]) → 1 件ずつの検査。
pub fn parse_put_body(
    content_type: Option<&str>,
    body: &[u8],
) -> Result<Vec<RestStartOverride>, Fail> {
    let body: PutBody = parse_json(content_type, body)?;
    if body.items.is_empty() || body.items.len() > MAX_ITEMS {
        return Err(bad_request(format!(
            "items は 1〜{MAX_ITEMS} 件にしてください"
        )));
    }
    body.items.into_iter().map(checked).collect()
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    from: String,
    to: String,
}

/// `GET` の範囲 (`[from, to)` の JST の壁時計。`to` は翌日の 0 時)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListRequest {
    pub from: NaiveDateTime,
    pub to: NaiveDateTime,
}

/// `GET /api/kintai/rest-start-overrides` のクエリ。Query → 日付 → `from <= to`。
pub fn parse_list_query(query: &str) -> Result<ListRequest, Fail> {
    let q: ListQuery = parse_query(query)?;
    let date = |name: &str, s: &str| {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|_| bad_request(format!("{name} は YYYY-MM-DD で指定してください")))
    };
    let (from, to) = (date("from", &q.from)?, date("to", &q.to)?);
    if from > to {
        return Err(bad_request("from は to 以前にしてください"));
    }
    Ok(ListRequest {
        from: from.and_time(Default::default()),
        to: (to + Duration::days(1)).and_time(Default::default()),
    })
}

/// 補正の読み。`$1` テナント・`$2`/`$3` 休息の開始の範囲 `[from, to)` (TIMESTAMPTZ)。時刻は JST の壁時計の文字列で返す。
pub const LIST_SQL: &str = "\
SELECT driver_cd,
       to_char(rest_start_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS rest_start,
       to_char(last_move_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS') AS last_move,
       unko_no
  FROM kintai.rest_start_overrides
 WHERE tenant_id = $1 AND rest_start_at >= $2 AND rest_start_at < $3
 ORDER BY driver_cd, rest_start_at";

/// 補正の書き込み (同じ鍵は置き換え)。`$1` テナント・`$2` 乗務員CD・`$3` 休息の開始・`$4` 最後の動き (どちらも
/// JST の壁時計の文字列を `Asia/Tokyo` で読む)・`$5` 運行NO・`$6` source。
pub const UPSERT_SQL: &str = "\
INSERT INTO kintai.rest_start_overrides (tenant_id, driver_cd, rest_start_at, last_move_at, unko_no, source)
VALUES ($1, $2, ($3::timestamp AT TIME ZONE 'Asia/Tokyo'), ($4::timestamp AT TIME ZONE 'Asia/Tokyo'), $5, $6)
ON CONFLICT (tenant_id, driver_cd, rest_start_at)
DO UPDATE SET last_move_at = EXCLUDED.last_move_at, unko_no = EXCLUDED.unko_no,
              source = EXCLUDED.source, recorded_at = now()";

/// 読んだ 1 行 (`LIST_SQL` の列) を補正に。時刻が読めない行は `None` (DB の書式なので通常は起きない)。
pub fn override_from_row(
    driver_cd: i64,
    rest_start: &str,
    last_move: &str,
    unko_no: String,
) -> Option<RestStartOverride> {
    let parse = |s: &str| NaiveDateTime::parse_from_str(s, FMT).ok();
    Some(RestStartOverride {
        driver_cd,
        rest_start: parse(rest_start)?,
        last_move: parse(last_move)?,
        unko_no,
    })
}

/// 補正の JSON (`GET` の応答の 1 件)。
pub fn override_json(o: &RestStartOverride) -> Value {
    serde_json::json!({
        "driver_cd": o.driver_cd,
        "rest_start_at": o.rest_start.format(FMT).to_string(),
        "last_move_at": o.last_move.format(FMT).to_string(),
        "unko_no": o.unko_no,
    })
}

/// fold が読んだ行に補正を当てる: 乗務員 (`driver_id`) と開始 (`datetime`、秒まで) が補正と一致する `休息` の行の開始を
/// 最後の動きに差し替える。一致しない補正は何もしない (alc の行が後から直されたときは補正が効かなくなるだけ)。
pub fn apply_rest_start_overrides(rows: &mut [Value], overrides: &[RestStartOverride]) -> usize {
    let mut applied = 0;
    for row in rows.iter_mut() {
        if row["state"].as_str() != Some("休息") {
            continue;
        }
        let (driver, start) = (row["driver_id"].as_i64(), row["datetime"].as_str());
        let hit = overrides.iter().find(|o| {
            Some(o.driver_cd) == driver
                && Some(o.rest_start.format(FMT).to_string().as_str()) == start
        });
        if let Some(o) = hit {
            row["datetime"] = Value::String(o.last_move.format(FMT).to_string());
            applied += 1;
        }
    }
    applied
}
