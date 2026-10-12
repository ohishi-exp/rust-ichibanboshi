//! 書き込みの口と signatures の**入力の検査** (kintai-logic の `parse_batch`・`parse_signatures`・`parse_snapshot`。口が DB の
//! 前に通すもの) が、固定した status・本文を返すことを確かめる (Refs #322)。DB は要らない。
//!
//! 期待値は、root の Cloud Run 版 (axum の実物の handler) と一致していた値 (axum の `Json` の 415 / 413 / 400 / 422・`Query` の
//! 400 の拒否文言を含む) を書き出したもの。root を撤去した後は、この表が Worker の応答の正本。`None` は検査を通る (= 書き先へ進む)。
//! ファイル名の `axum_parity` は、比較の相手を外す前の名前を残している。

use kintai_logic::common::Fail;
use kintai_logic::timecard_write::{parse_batch, parse_signatures};
use kintai_logic::wage_write::parse_snapshot;

const JSON: Option<&str> = Some("application/json");

const NO_CT: &str = "Expected request with `Content-Type: application/json`";
const MONTH_FMT: &str = "month は YYYY-MM で指定してください";

/// (ラベル, 期待する失敗)。`None` は通る。
type Want = Option<(u16, &'static str)>;

/// 検査の結果を (status, 本文) に直して期待値と比べる。
fn check<T>(label: &str, got: Result<T, Fail>, want: Want) {
    let got = got.err().map(|f| (f.status, f.body));
    assert_eq!(got, want.map(|(s, b)| (s, b.to_string())), "{label}");
}

#[test]
fn timecard_body_is_checked_with_fixed_responses() {
    let big = format!(
        r#"{{"month":"2026-06","driver_cd":1,"x":"{}"}}"#,
        "a".repeat(2_100_000)
    );
    let cases: Vec<(&str, Option<&str>, Vec<u8>, Want)> = vec![
        (
            "ok",
            JSON,
            br#"{"month":"2026-06","driver_cd":1130}"#.to_vec(),
            None,
        ),
        (
            "ok + days",
            Some("application/json; charset=utf-8"),
            br#"{"month":"2026-06","driver_cd":1,"days":{"2026-06-01":[]},"delete_dates":["2026-06-02"]}"#
                .to_vec(),
            None,
        ),
        ("content-type 無し", None, b"{}".to_vec(), Some((415, NO_CT))),
        (
            "text/plain",
            Some("text/plain"),
            b"{}".to_vec(),
            Some((415, NO_CT)),
        ),
        (
            "構文の誤り",
            JSON,
            b"{\"month\":".to_vec(),
            Some((
                400,
                "Failed to parse the request body as JSON: month: EOF while parsing a value at line 1 column 9",
            )),
        ),
        (
            "後ろに余計な文字",
            JSON,
            br#"{"month":"2026-06","driver_cd":1} x"#.to_vec(),
            Some((
                400,
                "Failed to parse the request body as JSON: trailing characters at line 1 column 35",
            )),
        ),
        (
            "型の誤り",
            JSON,
            br#"{"month":"2026-06","driver_cd":"x"}"#.to_vec(),
            Some((
                422,
                "Failed to deserialize the JSON body into the target type: driver_cd: invalid type: string \"x\", expected i64 at line 1 column 34",
            )),
        ),
        (
            "欄が無い",
            JSON,
            br#"{"month":"2026-06"}"#.to_vec(),
            Some((
                422,
                "Failed to deserialize the JSON body into the target type: missing field `driver_cd` at line 1 column 19",
            )),
        ),
        (
            "日付のキーが不正",
            JSON,
            br#"{"month":"2026-06","driver_cd":1,"days":{"x":[]}}"#.to_vec(),
            Some((
                422,
                "Failed to deserialize the JSON body into the target type: days.x: input contains invalid characters at line 1 column 44",
            )),
        ),
        (
            "month が不正",
            JSON,
            br#"{"month":"2026-6","driver_cd":1}"#.to_vec(),
            Some((400, MONTH_FMT)),
        ),
        (
            "2MB 超",
            JSON,
            big.into_bytes(),
            Some((413, "Failed to buffer the request body: length limit exceeded")),
        ),
    ];
    for (label, ct, body, want) in cases {
        check(label, parse_batch(ct, &body), want);
    }
}

#[test]
fn signatures_query_is_checked_with_fixed_responses() {
    let cases: [(&str, Want); 7] = [
        ("month=2026-06&driver_cd=1130", None),
        ("", Some((400, MONTH_FMT))),
        ("month=2026-13&driver_cd=1", Some((400, MONTH_FMT))),
        ("month=2026-06", Some((400, "driver_cd は必須です"))),
        (
            "month=2026-06&driver_cd=abc",
            Some((
                400,
                "Failed to deserialize query string: driver_cd: invalid digit found in string",
            )),
        ),
        (
            "month=2026-06&month=2026-07&driver_cd=1",
            Some((
                400,
                "Failed to deserialize query string: duplicate field `month`",
            )),
        ),
        ("month=2026-06&driver_cd=-5", None),
    ];
    for (q, want) in cases {
        check(q, parse_signatures(q), want);
    }
}

#[test]
fn wage_snapshot_body_is_checked_with_fixed_responses() {
    let ok = r#"{"comp_id":"c1","month":"2026-06","restraint_source":"gcp","wage_logic_version":"w1",
                "masters":{"payroll_synced_at":"2026-07-03T09:12:00Z"},"rows":[{"driver_cd":1}]}"#;
    let cases: Vec<(&str, Option<&str>, String, Want)> = vec![
        ("ok", JSON, ok.to_string(), None),
        ("text/plain", Some("text/plain"), ok.to_string(), Some((415, NO_CT))),
        (
            "型の誤り",
            JSON,
            ok.replace(r#""driver_cd":1"#, r#""driver_cd":"x""#),
            Some((
                422,
                "Failed to deserialize the JSON body into the target type: rows[0].driver_cd: invalid type: string \"x\", expected i64 at line 2 column 95",
            )),
        ),
        (
            "comp_id が空",
            JSON,
            ok.replace(r#""comp_id":"c1""#, r#""comp_id":"""#),
            Some((400, "comp_id は必須です")),
        ),
        (
            "month が不正",
            JSON,
            ok.replace("2026-06", "2026-6"),
            Some((400, MONTH_FMT)),
        ),
        (
            "source が不正",
            JSON,
            ok.replace(r#""gcp""#, r#""x""#),
            Some((400, "restraint_source は gcp / current のいずれかです")),
        ),
        (
            "同期時刻が不正",
            JSON,
            ok.replace("2026-07-03T09:12:00Z", "2026/07/03"),
            Some((
                400,
                "masters.payroll_synced_at は RFC3339 で指定してください",
            )),
        ),
        (
            "構文の誤り",
            JSON,
            "{".to_string(),
            Some((
                400,
                "Failed to parse the request body as JSON: EOF while parsing an object at line 1 column 1",
            )),
        ),
    ];
    for (label, ct, body, want) in cases {
        check(label, parse_snapshot(ct, body.as_bytes()), want);
    }
}
