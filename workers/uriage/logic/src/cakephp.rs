//! 社内 CakePHP (`/uriage-jyuchu-display/*`) の応答の型 (root の `src/cakephp.rs` から移した。
//! Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 通信 (オンプレ版は reqwest、Worker は VPC の fetch) と path の組み立ては呼び手が持つ。

use serde::Deserialize;
use std::collections::HashMap;

/// `/uriage-jyuchu-display/masters-json` のレスポンス。
///
/// 例:
/// ```json
/// {
///   "date": "2026-06-29",
///   "offices": {
///     "1": {
///       "display_name": "本社",
///       "persons": {"1499": "青井", ...},
///       "other": {"031": "帯広営業所", ...},
///       "bumon": ["010", "011", "030"]
///     }
///   }
/// }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct MastersResponse {
    pub date: String,
    pub offices: HashMap<String, OfficeMasters>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct OfficeMasters {
    pub display_name: String,
    /// 入力担当C (string keys、CakePHP 側 JSON 仕様) → 担当者名
    pub persons: HashMap<String, String>,
    /// 稼動部門コード → 営業所名 (別営業所判定)
    pub other: HashMap<String, String>,
    /// 受注部門コード配列 (PR #766 で追加)
    #[serde(default)]
    pub bumon: Vec<String>,
}

impl OfficeMasters {
    /// `persons` を `HashMap<i32, String>` に変換 (`compute_person_sum` 入力用)
    pub fn persons_as_int_map(&self) -> HashMap<i32, String> {
        self.persons
            .iter()
            .filter_map(|(k, v)| k.parse::<i32>().ok().map(|i| (i, v.clone())))
            .collect()
    }
}

/// `/uriage-jyuchu-display/editable-months` のレスポンス。
///
/// 例:
/// ```json
/// {"operation_month": "2026-07", "editable_months_count": 2,
///  "editable_months": ["2026-06", "2026-07"]}
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct EditableMonthsResponse {
    pub operation_month: String,
    pub editable_months_count: i32,
    pub editable_months: Vec<String>,
}

/// `/uriage-jyuchu-display/print-json` のレスポンス (検証用に使う `.sum` のみ抽出)。
///
/// PHP テンプレ由来の単日 (date) × 営業所 (id) × cal の `$sum` を JSON 化したもの。
/// 担当者名 → `{ 金額, 傭車金額, 件数 }` の map。verify endpoint で Rust 側 sum と
/// 1:1 比較する。他のフィールド (例: meta) は無視 (serde default)。
#[derive(Debug, Clone, Deserialize)]
pub struct PrintJsonResponse {
    #[serde(default)]
    pub sum: serde_json::Value,
}

/// `MastersResponse.offices` (HashMap<String, _>) から i64 key で OfficeMasters を引く。
pub fn office_lookup(masters: &MastersResponse, eigyosho_id: i64) -> Option<&OfficeMasters> {
    masters.offices.get(&eigyosho_id.to_string())
}

// ── 売上 Worker が Workers VPC (HTTP) で CakePHP を叩くときの部品 ──
// 定数は workers/kintai/logic/src/cakephp_relay.rs、join・urlencode・is_success は workers/kintai/logic/src/cakephp.rs の写し
// (別 workspace なので依存しない)。path はオンプレ版 (root の src/cakephp.rs) の fetch_masters・fetch_editable_months と同じ

/// CakePHP への口 (Workers VPC の VPC Service、HTTP)。wrangler.toml のトップレベルにだけ置く。
pub const CAKEPHP_VPC_BINDING: &str = "URIAGE_CAKEPHP_VPC";

/// fetch の URL の origin。宛先の host:port は VPC Service の側で決まる (社内のホスト名は書かない)。**`Host` は CakePHP に
/// 届き、CakePHP は `Host` で応答を変える** — `localhost` (port は VPC Service と同じ 120) なら JSON を返す (勤怠 Worker で実測)。
pub const VPC_ORIGIN: &str = "http://localhost:120";

/// 待ちの上限 (秒)。オンプレ版の `[cakephp] timeout_secs` の既定と同じ。
pub const TIMEOUT_SECS: u64 = 30;

/// `/uriage-jyuchu-display/editable-months`
pub const EDITABLE_MONTHS_PATH: &str = "/uriage-jyuchu-display/editable-months";

/// `/uriage-jyuchu-display/masters-json?date=YYYY-MM-DD`
pub fn masters_path(date: &str) -> String {
    let date = urlencode(date);
    format!("{MASTERS_PATH}?date={date}")
}

const MASTERS_PATH: &str = "/uriage-jyuchu-display/masters-json";

/// `base_url` (末尾の `/` は落とす) に相対パスを足す。
pub fn join(base_url: &str, path: &str) -> String {
    format!("{}{path}", base_url.trim_end_matches('/'))
}

/// クエリの値の escape (オンプレ版が送る URL と同じにする)。
pub fn urlencode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            ' ' => "%20".to_string(),
            _ => format!("%{:02X}", c as u32),
        })
        .collect()
}

/// CakePHP の失敗の種類 (宛先・本文は持たない)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CakephpError {
    /// 届かない (fetch の失敗)
    Request,
    /// 待ちを打ち切った
    Timeout,
    /// 2xx でない (3xx も。fetch は redirect を追わない)
    Status(u16),
    /// 2xx の本文が読めない
    Json,
}

impl CakephpError {
    /// 応答の `kind`・job の error に出す名前 (`request`・`timeout`・`status:500`・`json`)。
    pub fn label(&self) -> String {
        match self {
            Self::Request => "request".to_string(),
            Self::Timeout => "timeout".to_string(),
            Self::Status(s) => format!("status:{s}"),
            Self::Json => "json".to_string(),
        }
    }
}

/// 応答 (status・本文) を型に読む。2xx 以外は `Status` (オンプレ版の reqwest の `is_success` と同じ範囲)。
pub fn read_json<T: serde::de::DeserializeOwned>(
    status: u16,
    body: &[u8],
) -> Result<T, CakephpError> {
    if !(200..300).contains(&status) {
        return Err(CakephpError::Status(status));
    }
    serde_json::from_slice(body).map_err(|_| CakephpError::Json)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_masters_response() {
        let json = r#"{
            "date": "2026-06-29",
            "offices": {
                "1": {
                    "display_name": "本社",
                    "persons": {"1499": "青井", "1364": "山﨑智"},
                    "other": {"031": "帯広営業所"},
                    "bumon": ["010", "011", "030"]
                },
                "9": {
                    "display_name": "宮崎",
                    "persons": {"2000": "田中"},
                    "other": {},
                    "bumon": ["015"]
                }
            }
        }"#;
        let m: MastersResponse = serde_json::from_str(json).unwrap();
        assert_eq!(m.date, "2026-06-29");
        assert_eq!(m.offices.len(), 2);
        let honsha = &m.offices["1"];
        assert_eq!(honsha.display_name, "本社");
        assert_eq!(honsha.persons.len(), 2);
        assert_eq!(honsha.bumon, vec!["010", "011", "030"]);
    }

    #[test]
    fn parse_masters_response_missing_bumon_defaults_empty() {
        // PR #765 初期は bumon が無かった → default 空配列で fallback
        let json = r#"{
            "date": "2026-06-29",
            "offices": {
                "1": {
                    "display_name": "本社",
                    "persons": {},
                    "other": {}
                }
            }
        }"#;
        let m: MastersResponse = serde_json::from_str(json).unwrap();
        assert!(m.offices["1"].bumon.is_empty());
    }

    #[test]
    fn parse_editable_months() {
        let json = r#"{
            "operation_month": "2026-07",
            "editable_months_count": 2,
            "editable_months": ["2026-06", "2026-07"]
        }"#;
        let e: EditableMonthsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(e.operation_month, "2026-07");
        assert_eq!(e.editable_months_count, 2);
        assert_eq!(e.editable_months, vec!["2026-06", "2026-07"]);
    }

    #[test]
    fn persons_as_int_map_skips_unparseable_keys() {
        let mut m = OfficeMasters {
            display_name: "x".into(),
            persons: HashMap::new(),
            other: HashMap::new(),
            bumon: vec![],
        };
        m.persons.insert("1499".into(), "青井".into());
        m.persons
            .insert("invalid".into(), "should_be_skipped".into());
        m.persons.insert("1364".into(), "山﨑智".into());
        let parsed = m.persons_as_int_map();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed.get(&1499), Some(&"青井".to_string()));
        assert_eq!(parsed.get(&1364), Some(&"山﨑智".to_string()));
    }

    #[test]
    fn office_lookup_parses_string_keys() {
        let mut offices = std::collections::HashMap::new();
        offices.insert(
            "1".to_string(),
            OfficeMasters {
                display_name: "本社".to_string(),
                persons: HashMap::new(),
                other: HashMap::new(),
                bumon: vec!["010".to_string()],
            },
        );
        offices.insert(
            "9".to_string(),
            OfficeMasters {
                display_name: "宮崎".to_string(),
                persons: HashMap::new(),
                other: HashMap::new(),
                bumon: vec!["015".to_string()],
            },
        );
        let masters = MastersResponse {
            date: "2026-06-29".to_string(),
            offices,
        };
        assert_eq!(office_lookup(&masters, 1).unwrap().display_name, "本社");
        assert_eq!(office_lookup(&masters, 9).unwrap().display_name, "宮崎");
        assert!(office_lookup(&masters, 7).is_none());
    }

    #[test]
    fn paths_match_the_onprem_urls() {
        let base = join(VPC_ORIGIN, EDITABLE_MONTHS_PATH);
        assert_eq!(
            base,
            "http://localhost:120/uriage-jyuchu-display/editable-months"
        );
        let masters = join("http://h/", &masters_path("2026-06-30"));
        assert_eq!(
            masters,
            "http://h/uriage-jyuchu-display/masters-json?date=2026-06-30"
        );
        assert_eq!(urlencode("a b+~"), "a%20b%2B~");
    }

    #[test]
    fn read_json_maps_status_and_parse_failures() {
        let ok: Result<EditableMonthsResponse, _> = read_json(
            200,
            br#"{"operation_month":"2026-07","editable_months_count":1,"editable_months":["2026-07"]}"#,
        );
        assert_eq!(ok.unwrap().editable_months, vec!["2026-07"]);
        let redirect = read_json::<EditableMonthsResponse>(302, b"{}").unwrap_err();
        assert_eq!(redirect, CakephpError::Status(302));
        let html = read_json::<EditableMonthsResponse>(200, b"<html>").unwrap_err();
        assert_eq!(html, CakephpError::Json);
        let low = read_json::<EditableMonthsResponse>(199, b"{}").unwrap_err();
        assert_eq!(low.label(), "status:199");
        let labels = [
            CakephpError::Request,
            CakephpError::Timeout,
            CakephpError::Json,
        ];
        let labels: Vec<String> = labels.iter().map(CakephpError::label).collect();
        assert_eq!(labels, ["request", "timeout", "json"]);
    }
}
