//! **[`LOGIC_BASE`] の上げ忘れを落とす golden テスト** (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 勤怠の版 (`logic_version`) はソースのハッシュではなく手で上げる [`LOGIC_BASE`] から作る。上げ忘れると、計算結果が
//! 変わったのに保存済みの行が stale に見えず古い値を返し続ける。そこで固定入力 (`golden/fold_input.json`) を畳んだ出力
//! (3 表に書く行) のハッシュを、`golden/logic_outputs.tsv` の「`LOGIC_BASE` → 出力ハッシュ」の対応表で縛る。
//!
//! - 今の `LOGIC_BASE` の行があり出力ハッシュが一致すること、キーが重複しないことを検査する
//! - **出力を変えたら `LOGIC_BASE` を上げ、`KINTAI_GOLDEN_APPEND=1 cargo test -p kintai-kosoku --test logic_golden`
//!   で新しい行を追記する。** 追記するのは今の `LOGIC_BASE` の行が無いときだけで、既存の行は書き換えない
//!   (= 出力を変えるには版を上げるしかない。行を手で書き換えればそれは diff に出る)
//! - 残る穴: 固定入力が通らない分岐だけを変えた変更は検知できない。入力が通っている分岐は
//!   `the_input_goes_through_every_branch` が固定する

use std::collections::BTreeMap;
use std::fmt::Write as _;

use kintai_kosoku::kintai_fold::{fold_driver_month, FoldUnit, LOGIC_BASE};
use kintai_kosoku::kosoku::KosokuParams;
use sha2::{Digest, Sha256};

const INPUT: &str = include_str!("golden/fold_input.json");
const TABLE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/golden/logic_outputs.tsv"
);
const APPEND_ENV: &str = "KINTAI_GOLDEN_APPEND";

/// 固定入力を乗務員CD の順に畳む。
fn fold_input() -> Vec<FoldUnit> {
    let v: serde_json::Value = serde_json::from_str(INPUT).expect("fold_input.json");
    let month = v["month"].as_str().expect("month");
    let drivers: BTreeMap<i64, Vec<serde_json::Value>> = v["drivers"]
        .as_object()
        .expect("drivers")
        .iter()
        .map(|(cd, rows)| {
            (
                cd.parse().expect("cd"),
                rows.as_array().expect("rows").clone(),
            )
        })
        .collect();
    let p = KosokuParams::default();
    drivers
        .into_iter()
        .map(|(cd, rows)| fold_driver_month(cd, month, &p, rows).0)
        .collect()
}

/// 3 表に書く行と skip を 1 行ずつ文字にする (列の順は固定。行の型の名前や Debug には依らない)。
fn canonical(units: &[FoldUnit]) -> String {
    let mut out = String::new();
    for u in units {
        for s in &u.shifts {
            writeln!(
                out,
                "shift|{}|{}|{}|{}",
                s.driver_cd, s.start_at, s.end_at, s.shift_source
            )
            .unwrap();
        }
        for d in &u.day_summaries {
            let nw = serde_json::to_string(&d.non_working).unwrap();
            let m = [
                d.restraint_minutes,
                d.working_minutes,
                d.break_minutes,
                d.rest_minus_minutes,
                d.statutory_minutes,
                d.within_statutory_overtime_minutes,
                d.overtime_minutes,
                d.legal_holiday_minutes,
                d.night_minutes,
                d.overtime_night_minutes,
                d.legal_holiday_night_minutes,
            ];
            let (cd, date, at, src) = (d.driver_cd, d.date, d.shift_start_at, d.shift_source);
            // 計上日は値があるときだけ足す — 打刻のある会社 (固定入力) の出力を変えない
            let count = d
                .count_date
                .map(|c| format!("|count={c}"))
                .unwrap_or_default();
            writeln!(out, "day|{cd}|{date}|{at}|{src}|{m:?}|{nw}{count}").unwrap();
        }
        for p in &u.day_parts {
            let m = [p.restraint_minutes, p.working_minutes, p.night_minutes];
            writeln!(
                out,
                "part|{}|{}|{}|{m:?}",
                p.driver_cd, p.shift_start_at, p.date
            )
            .unwrap();
        }
        for s in &u.skipped {
            writeln!(
                out,
                "skip|{}|{}",
                u.driver_cd,
                serde_json::to_string(s).unwrap()
            )
            .unwrap();
        }
    }
    out
}

fn output_hash(units: &[FoldUnit]) -> String {
    format!("{:x}", Sha256::digest(canonical(units).as_bytes()))
}

/// 対応表を読む (`#` の行と空行は飛ばす)。キーの重複はエラー。
fn parse_table(table: &str) -> Result<BTreeMap<String, String>, String> {
    let mut map = BTreeMap::new();
    for line in table
        .lines()
        .filter(|l| !l.trim().is_empty() && !l.starts_with('#'))
    {
        let (base, hash) = line
            .split_once('\t')
            .ok_or_else(|| format!("タブ区切りでない行: {line}"))?;
        if map.insert(base.to_string(), hash.to_string()).is_some() {
            return Err(format!("LOGIC_BASE {base} の行が重複しています"));
        }
    }
    Ok(map)
}

/// 対応表と今の出力を突き合わせる。`append` が真で今の `base` の行が無いときだけ、末尾に 1 行足した表を返す
/// (既存の行は 1 文字も変えない)。一致すれば `Ok(None)`。
fn check(table: &str, base: &str, hash: &str, append: bool) -> Result<Option<String>, String> {
    match parse_table(table)?.get(base) {
        Some(want) if want == hash => Ok(None),
        Some(want) => Err(format!(
            "LOGIC_BASE {base} の出力が変わりました (表 {want} / 今 {hash})。計算結果を変えたなら LOGIC_BASE を上げて \
             {APPEND_ENV}=1 で追記してください。表の既存の行は書き換えません"
        )),
        None if append => {
            let mut t = table.to_string();
            if !t.is_empty() && !t.ends_with('\n') {
                t.push('\n');
            }
            t.push_str(&format!("{base}\t{hash}\n"));
            Ok(Some(t))
        }
        None => Err(format!(
            "対応表に LOGIC_BASE {base} の行がありません。{APPEND_ENV}=1 cargo test -p kintai-kosoku --test logic_golden \
             で追記してください (出力ハッシュ {hash})"
        )),
    }
}

/// **本体。** 今の `LOGIC_BASE` の行が対応表にあり、固定入力の出力と一致する。
#[test]
fn the_output_matches_the_logic_base() {
    let table = std::fs::read_to_string(TABLE_PATH).expect("logic_outputs.tsv");
    let hash = output_hash(&fold_input());
    let append = std::env::var(APPEND_ENV).is_ok_and(|v| v == "1");
    match check(&table, LOGIC_BASE, &hash, append) {
        Ok(None) => {}
        Ok(Some(next)) => std::fs::write(TABLE_PATH, next).expect("write logic_outputs.tsv"),
        Err(e) => panic!("{e}"),
    }
}

/// 固定入力が計算の分岐を一通り通っている (通らなくなったら golden が守る範囲が黙って縮む)。
#[test]
fn the_input_goes_through_every_branch() {
    let units = fold_input();
    let days: Vec<_> = units.iter().flat_map(|u| &u.day_summaries).collect();
    let any = |f: &dyn Fn(&&kintai_kosoku::kintai_fold::DaySummaryRow) -> bool| days.iter().any(f);
    assert!(any(&|d| d.break_minutes > 0), "休憩");
    assert!(any(&|d| d.night_minutes > 0), "深夜");
    assert!(any(&|d| d.legal_holiday_minutes > 0), "法定休日 (日曜)");
    assert!(
        any(&|d| d.legal_holiday_night_minutes > 0),
        "法定休日の深夜"
    );
    assert!(any(&|d| d.overtime_minutes > 0), "残業");
    assert!(
        any(&|d| d.within_statutory_overtime_minutes > 0),
        "法定内残業"
    );
    assert!(any(&|d| d.overtime_night_minutes > 0), "残業の深夜");
    assert!(any(&|d| !d.non_working.is_empty()), "実働でない区間");
    assert!(any(&|d| d.shift_source == "timecard"), "打刻で決めた勤務");
    assert!(
        any(&|d| d.shift_source == "rest"),
        "休息で決めた勤務 (打刻無し)"
    );
    let parts: Vec<_> = units.iter().flat_map(|u| &u.day_parts).collect();
    assert!(
        parts.iter().any(|p| p.date != p.shift_start_at.date()),
        "日跨ぎ"
    );
    assert!(
        parts.iter().any(|p| p.date.to_string() == "2026-07-01"),
        "月末跨ぎ"
    );
    assert!(
        units.iter().any(|u| u.shifts.len() >= 3),
        "休息で割った長距離"
    );
}

// ── 対応表の扱い (陰性対照) ──

#[test]
fn a_changed_output_fails_without_a_new_logic_base_even_when_appending() {
    let table = "# c\nb1\taaa\n";
    assert_eq!(check(table, "b1", "aaa", false), Ok(None));
    for append in [false, true] {
        let e = check(table, "b1", "bbb", append).unwrap_err();
        assert!(e.contains("出力が変わりました"), "{e}");
    }
}

#[test]
fn appending_adds_a_row_only_for_a_new_logic_base_and_keeps_the_old_rows() {
    let table = "# c\nb1\taaa";
    let next = check(table, "b2", "bbb", true).unwrap().unwrap();
    assert_eq!(next, "# c\nb1\taaa\nb2\tbbb\n");
    assert!(next.starts_with(table), "既存の行はそのまま");
    assert_eq!(
        check(&next, "b2", "bbb", true),
        Ok(None),
        "2 回目は何もしない"
    );
    assert_eq!(check("", "b1", "aaa", true).unwrap().unwrap(), "b1\taaa\n");
    let e = check(table, "b2", "bbb", false).unwrap_err();
    assert!(e.contains("行がありません"), "{e}");
}

#[test]
fn a_broken_table_is_rejected() {
    let e = check("b1\taaa\nb1\taaa\n", "b1", "aaa", false).unwrap_err();
    assert!(e.contains("重複"), "{e}");
    let e = check("b1 aaa\n", "b1", "aaa", false).unwrap_err();
    assert!(e.contains("タブ区切り"), "{e}");
}
