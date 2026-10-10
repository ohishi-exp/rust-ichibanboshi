//! 名前付き引数の展開 (`kintai_mysql::bind`) の単体テスト。

use chrono::NaiveDate;
use kintai_kosoku::sql::{
    ALL_EVENTS_SQL, EVENTS_SQL, OPERATION_READING_DATES_SQL, REST_EVENTS_SQL,
};
use kintai_mysql::bind::{expand, names, BindError, Digits, Value};

fn dt(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> Value {
    let dt = NaiveDate::from_ymd_opt(y, m, d)
        .unwrap()
        .and_hms_opt(h, mi, s)
        .unwrap();
    Value::DateTime(dt)
}

#[test]
fn digits_accepts_only_1_to_32_ascii_digits() {
    let d = Digits::new("26060507533000000042861").unwrap();
    assert_eq!(d.as_str(), "26060507533000000042861");
    assert_eq!(Digits::new("0").unwrap().as_str(), "0");
    assert!(Digits::new(&"9".repeat(32)).is_some(), "32 桁は受ける");
    assert_eq!(Digits::new(&"9".repeat(33)), None, "33 桁は拒否");
    assert_eq!(Digits::new(""), None, "空は拒否");
    for bad in [
        "1'", "1 ", " 1", "-1", "1a", "１", "1\\", "1;--", "0x1f", "1.5",
    ] {
        assert_eq!(Digits::new(bad), None, "{bad:?} は数字以外を含む");
    }
}

#[test]
fn digits_expand_to_a_quoted_literal() {
    let sql = "WHERE e.`運行NO` IN (:v1, :v2) AND x = ':v1'";
    let v1 = Value::Digits(Digits::new("26060507533000000042861").unwrap());
    let v2 = Value::Digits(Digits::new("26060507533000000042862").unwrap());
    let got = expand(sql, &[("v1", v1), ("v2", v2)]).unwrap();
    assert_eq!(
        got,
        "WHERE e.`運行NO` IN ('26060507533000000042861', '26060507533000000042862') AND x = ':v1'"
    );
}

#[test]
fn same_name_on_both_sides_gets_same_value() {
    let sql = "WHERE (:driver IS NULL OR t.driver_id = :driver)";
    let got = expand(sql, &[("driver", Value::UInt(1051))]).unwrap();
    assert_eq!(got, "WHERE (1051 IS NULL OR t.driver_id = 1051)");
    let got = expand(sql, &[("driver", Value::Null)]).unwrap();
    assert_eq!(got, "WHERE (NULL IS NULL OR t.driver_id = NULL)");
}

#[test]
fn every_occurrence_is_replaced() {
    let sql = "a >= :from AND b >= :from AND c < :to AND DATE(:from)";
    let got = expand(
        sql,
        &[
            ("from", dt(2026, 6, 1, 0, 0, 0)),
            ("to", dt(2026, 7, 2, 0, 0, 0)),
        ],
    )
    .unwrap();
    assert_eq!(
        got,
        "a >= '2026-06-01 00:00:00' AND b >= '2026-06-01 00:00:00' AND c < '2026-07-02 00:00:00' AND DATE('2026-06-01 00:00:00')"
    );
}

#[test]
fn colon_inside_quotes_is_kept() {
    let sql =
        "SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s'), \":x\" FROM t WHERE d.id = :driver";
    let got = expand(sql, &[("driver", Value::UInt(7))]).unwrap();
    assert_eq!(
        got,
        "SELECT DATE_FORMAT(d.datetime, '%Y-%m-%d %H:%i:%s'), \":x\" FROM t WHERE d.id = 7"
    );
}

#[test]
fn escapes_inside_quotes_are_skipped() {
    // `\'` と `''` で閉じない。中の :a は名前ではない
    let sql = r"SELECT 'it\'s :a', 'it''s :a', `a``:b` FROM t WHERE x = :v";
    let got = expand(sql, &[("v", Value::UInt(1))]).unwrap();
    assert_eq!(
        got,
        r"SELECT 'it\'s :a', 'it''s :a', `a``:b` FROM t WHERE x = 1"
    );
}

#[test]
fn identifiers_in_backquotes_are_kept() {
    let sql = "WHERE e.`開始日時` >= :from AND e.`対象乗務員CD` = :driver";
    let got = expand(
        sql,
        &[
            ("from", dt(2026, 1, 2, 3, 4, 5)),
            ("driver", Value::UInt(1078)),
        ],
    )
    .unwrap();
    assert_eq!(
        got,
        "WHERE e.`開始日時` >= '2026-01-02 03:04:05' AND e.`対象乗務員CD` = 1078"
    );
}

#[test]
fn colon_not_followed_by_identifier_is_kept() {
    let sql = "SELECT ':', a := 1, b:1, c: FROM t WHERE x = :_v1";
    let got = expand(sql, &[("_v1", Value::Null)]).unwrap();
    assert_eq!(got, "SELECT ':', a := 1, b:1, c: FROM t WHERE x = NULL");
    assert_eq!(expand("x:", &[]).unwrap(), "x:");
}

#[test]
fn unknown_unused_and_duplicate_names_fail() {
    let err = expand("x = :a", &[]).unwrap_err();
    assert_eq!(err, BindError::UnknownName("a".to_string()));
    assert_eq!(err.kind(), "bind_unknown_name");

    let err = expand("x = 1", &[("a", Value::Null)]).unwrap_err();
    assert_eq!(err, BindError::UnusedName("a".to_string()));
    assert_eq!(err.kind(), "bind_unused_name");

    let err = expand("x = :a", &[("a", Value::Null), ("a", Value::UInt(1))]).unwrap_err();
    assert_eq!(err, BindError::DuplicateName("a".to_string()));
    assert_eq!(err.kind(), "bind_duplicate_name");
}

#[test]
fn unterminated_quotes_fail() {
    for sql in ["'abc", "\"abc", "`abc", r"'abc\'", "'ab''"] {
        let err = expand(sql, &[]).unwrap_err();
        assert_eq!(err, BindError::Unterminated, "{sql}");
        assert_eq!(err.kind(), "bind_unterminated");
        assert_eq!(names(sql).unwrap_err(), BindError::Unterminated);
    }
}

#[test]
fn comments_fail() {
    for sql in ["x # c", "x /* c */", "x -- c", "x --\tc", "x --"] {
        let err = expand(sql, &[]).unwrap_err();
        assert_eq!(err, BindError::Comment, "{sql}");
        assert_eq!(err.kind(), "bind_comment");
    }
    // コメントでないもの: 引用符の中・`--` の直後が空白でない・単独の `/` と `-`
    assert_eq!(
        expand("'#' a--1 a/2 a-1 '/*' '-- '", &[]).unwrap(),
        "'#' a--1 a/2 a-1 '/*' '-- '"
    );
}

#[test]
fn names_are_unique_in_order() {
    assert_eq!(
        names("a = :to OR b = :from OR c = :to").unwrap(),
        vec!["to", "from"]
    );
    assert!(names("SELECT 1").unwrap().is_empty());
}

fn sorted(sql: &str) -> Vec<String> {
    let mut n = names(sql).unwrap();
    n.sort();
    n
}

/// 4 本の口が渡す名前の集合と、SQL に出てくる名前の集合がちょうど一致する
/// (Worker の kintai-logic `mariadb_reads` の binds と同じ集合)。
#[test]
fn sql_constants_use_exactly_the_passed_names() {
    assert_eq!(sorted(EVENTS_SQL), ["driver", "from", "to"]);
    assert_eq!(sorted(REST_EVENTS_SQL), ["driver", "from", "to"]);
    assert_eq!(
        sorted(OPERATION_READING_DATES_SQL),
        ["driver", "from", "to"]
    );
    assert_eq!(sorted(ALL_EVENTS_SQL), ["from", "to"]);
}

/// 4 本の SQL を実際に展開すると、名前が 1 つも残らず、`DATE_FORMAT` の書式は壊れない。
#[test]
fn sql_constants_expand_cleanly() {
    let (from, to) = (dt(2026, 6, 1, 0, 0, 0), dt(2026, 7, 2, 0, 0, 0));
    for (sql, driver) in [
        (EVENTS_SQL, Some(Value::UInt(1078))),
        (REST_EVENTS_SQL, Some(Value::Null)),
        (OPERATION_READING_DATES_SQL, Some(Value::UInt(1107))),
        (ALL_EVENTS_SQL, None),
    ] {
        let mut params = vec![("from", from), ("to", to)];
        if let Some(d) = driver {
            params.push(("driver", d));
        }
        let got = expand(sql, &params).unwrap();
        assert!(names(&got).unwrap().is_empty());
        assert_eq!(
            got.matches("'%Y-%m-%d %H:%i:%s'").count(),
            sql.matches("'%Y-%m-%d %H:%i:%s'").count()
        );
        assert!(got.contains("'2026-06-01 00:00:00'"));
    }
}
