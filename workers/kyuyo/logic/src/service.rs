//! Worker の DO が SQL Server を開く 5 口 (`databases` / `companies` / `employees` / `payroll` / `sync`) の純粋部分
//! (Refs #322)。挙動はオンプレ版 `src/routes/kyuyo.rs` の各ハンドラと同じ: 入力の検証 (400)、失敗の写像
//! (`map_repo_err` / `map_db_open_err`)、行の組み立て、応答 JSON、derived store の行の encode / decode。
//! 接続・SQL 文 ([`crate::sql`])・DO の SQLite には触らない。
//!
//! エラーの文言はオンプレ版の各所と同じ (並走期間に応答を比べるため)。

use std::collections::HashMap;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::api::{CompaniesResponse, ErrorBody};
use crate::payroll::{
    build_companies, build_payroll_rows, parse_month, PayrollRow, RawKoumokuRow, RawKyuyoRow,
    RawShukeiRow, ALLOWED_COMPANIES,
};
use crate::{ErrKind, Reply, Stage};

/// 給与大臣を読む処理の失敗 (Worker の repo が返す)。本文は持たない (stage と種類だけ)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbError {
    pub stage: Stage,
    pub kind: ErrKind,
}

impl DbError {
    /// ログ 1 行 (`kyuyo payroll: failed at query (12 ms) kind=server:208/16/1`)。
    pub fn log_line(&self, what: &str, elapsed_ms: u64) -> String {
        crate::log_line_for(what, self.stage, &self.kind, elapsed_ms)
    }

    /// 「DB が開けない」(SQL Server error 4060)。オンプレ版 `map_db_open_err` の判定 ("Cannot open database"
    /// は 4060 の文言) を、文言でなくエラー番号で行う。
    pub fn cannot_open(&self) -> bool {
        matches!(self.kind, ErrKind::Server { code: 4060, .. })
    }
}

fn error_reply(status: u16, error: impl Into<String>) -> Reply {
    Reply {
        status,
        body: to_json(&ErrorBody {
            error: error.into(),
        }),
    }
}

/// 応答型を 200 の JSON にする。
pub fn ok_reply<T: Serialize>(body: &T) -> Reply {
    Reply {
        status: 200,
        body: to_json(body),
    }
}

fn to_json<T: Serialize>(v: &T) -> String {
    // 応答型は String / 数値 / bool / map / Vec だけなので serialize は失敗しない
    serde_json::to_string(v).unwrap_or_default()
}

/// オンプレ版 `map_repo_err` と同じ写像。
///
/// | stage | オンプレ版 | status |
/// |---|---|---|
/// | secret | `NotConfigured` | 503 |
/// | connect / login | `PoolError` (接続・ログインの失敗、15 秒の待ち切れ) | 503 |
/// | query | `QueryError` | 500 |
pub fn repo_error_reply(e: &DbError) -> Reply {
    match e.stage {
        Stage::Secret => error_reply(503, "給与 DB 接続が未設定です ([kyuyo] config)"),
        Stage::Connect | Stage::Login => error_reply(
            503,
            "給与 DB に接続できません (給与大臣 PC の稼働を確認してください)",
        ),
        Stage::Query => error_reply(500, "給与 DB クエリに失敗しました"),
    }
}

/// オンプレ版 `map_db_open_err` と同じ写像 (employees と payroll の本体クエリだけに使う)。DB が開けなければ 404、
/// それ以外は [`repo_error_reply`]。
pub fn db_open_error_reply(e: &DbError, db: &str) -> Reply {
    if e.stage == Stage::Query && e.cannot_open() {
        let tail = "を開けません (この会社×年度の給与データが未作成、またはデータ復旧で作られた DB で権限の再付与が必要です)";
        return error_reply(404, format!("{db} {tail}"));
    }
    repo_error_reply(e)
}

/// オンプレ版 `validate_company_month` と同じ検証。OK なら (年, 月)、だめなら 400。
pub fn validate_company_month(company: &str, month: &str) -> Result<(i32, u32), Reply> {
    if !ALLOWED_COMPANIES.contains(&company) {
        let list = ALLOWED_COMPANIES.join(" / ");
        let msg = format!("company は {list} のいずれかで指定してください");
        return Err(error_reply(400, msg));
    }
    parse_month(month).ok_or_else(|| error_reply(400, "month は YYYY-MM で指定してください"))
}

/// query string から `company` と `month` を取り出す (オンプレ版は axum の `Query` で必須。無ければ 400)。
/// 値は URL デコード済みのものを渡す。
pub fn company_month_params(pairs: &[(String, String)]) -> Result<(String, String), Reply> {
    let get = |key: &str| pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone());
    match (get("company"), get("month")) {
        (Some(company), Some(month)) => Ok((company, month)),
        _ => Err(error_reply(400, "company と month を指定してください")),
    }
}

/// payroll の本体クエリの結果に現れた支給回インデックス (昇順・重複なし)。SHUKEI1 はこの回ごとに引く
/// (通常は 1 つ。月内複数支給があれば複数)。
pub fn month_indexes(raw: &[RawKyuyoRow]) -> Vec<i32> {
    let mut v: Vec<i32> = raw.iter().map(|r| r.month_index).collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// 給与明細を組み立てる (オンプレ版 `fetch_payroll_live` の後半)。行が無ければ warning を足す。
pub fn payroll_rows(
    raw: &[RawKyuyoRow],
    koumoku: Vec<RawKoumokuRow>,
    shukei: &[RawShukeiRow],
    db: &str,
    month_label: &str,
) -> (Vec<PayrollRow>, Vec<String>) {
    let koumoku: HashMap<String, RawKoumokuRow> = koumoku
        .into_iter()
        .map(|r| (r.taikeikouno.clone(), r))
        .collect();
    let (rows, mut warnings) = build_payroll_rows(raw, &koumoku, shukei);
    if rows.is_empty() {
        let msg = format!("{db} の {month_label} に賃金期間が一致する支給回がありません");
        warnings.push(msg);
    }
    (rows, warnings)
}

/// 会社名マスタが読めなかったときの warning (companies / employees 共通、オンプレ版と同じ文言)。
pub const COMPANY_NAMES_WARNING: &str = "会社名マスタ (KYCOMSTD) を読めませんでした";

/// `GET /kyuyo/companies` の本文。`names` は会社名マスタ (`None` = 読めなかった → warning を先頭に足す)。
pub fn companies_response(
    databases: &[(String, Option<i32>)],
    names: Option<Vec<(String, String)>>,
) -> CompaniesResponse {
    let mut warnings: Vec<String> = Vec::new();
    let names: HashMap<String, String> = match names {
        Some(pairs) => pairs.into_iter().collect(),
        None => {
            warnings.push(COMPANY_NAMES_WARNING.to_string());
            HashMap::new()
        }
    };
    let (companies, mut access_warnings) = build_companies(databases, &names);
    warnings.append(&mut access_warnings);
    CompaniesResponse {
        companies,
        warnings,
    }
}

/// employees の会社名と warnings。`names` が `None` (読めなかった) なら空文字 + warning。
pub fn company_name(names: Option<Vec<(String, String)>>, company: &str) -> (String, Vec<String>) {
    match names {
        Some(pairs) => {
            let name = pairs
                .into_iter()
                .find(|(code, _)| code == company)
                .map(|(_, name)| name)
                .unwrap_or_default();
            (name, Vec::new())
        }
        None => (String::new(), vec![COMPANY_NAMES_WARNING.to_string()]),
    }
}

/// sync で store へ書けなかったとき (500、loud fail)。`what` は `payroll` / `employees`。
pub fn store_write_error(what: &str) -> Reply {
    error_reply(500, format!("キャッシュへの保存に失敗しました ({what})"))
}

/// derived store の row_json に行を詰める (応答配列の順のまま)。
pub fn encode_rows<T: Serialize>(rows: &[T]) -> Result<Vec<String>, String> {
    rows.iter()
        .map(|r| serde_json::to_string(r).map_err(|e| e.to_string()))
        .collect()
}

/// derived store の row_json を行に戻す。1 行でも読めなければ `Err` (呼び出し側は live 読みへ落ちる)。
pub fn decode_rows<T: DeserializeOwned>(jsons: &[String]) -> Result<Vec<T>, String> {
    jsons
        .iter()
        .map(|j| serde_json::from_str::<T>(j).map_err(|e| e.to_string()))
        .collect()
}

/// derived store の warnings_json を読む。
pub fn decode_warnings(json: &str) -> Result<Vec<String>, String> {
    serde_json::from_str(json).map_err(|e| e.to_string())
}

/// derived store の warnings_json を作る。
pub fn encode_warnings(warnings: &[String]) -> String {
    to_json(&warnings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payroll::EmployeeRow;

    fn db_err(stage: Stage, kind: ErrKind) -> DbError {
        DbError { stage, kind }
    }

    fn server(code: u32) -> ErrKind {
        ErrKind::Server {
            code,
            class: 16,
            state: 1,
        }
    }

    fn body(r: &Reply) -> serde_json::Value {
        serde_json::from_str(&r.body).unwrap()
    }

    #[test]
    fn repo_errors() {
        let r = repo_error_reply(&db_err(Stage::Secret, ErrKind::Other));
        assert_eq!(r.status, 503);
        assert_eq!(
            body(&r)["error"],
            "給与 DB 接続が未設定です ([kyuyo] config)"
        );
        for stage in [Stage::Connect, Stage::Login] {
            let r = repo_error_reply(&db_err(stage, ErrKind::Timeout));
            assert_eq!(r.status, 503);
            assert_eq!(
                body(&r)["error"],
                "給与 DB に接続できません (給与大臣 PC の稼働を確認してください)"
            );
        }
        let r = repo_error_reply(&db_err(Stage::Query, server(4060)));
        assert_eq!(
            (r.status, r.body.as_str()),
            (500, r#"{"error":"給与 DB クエリに失敗しました"}"#)
        );
    }

    #[test]
    fn db_open_errors() {
        let r = db_open_error_reply(&db_err(Stage::Query, server(4060)), "KYDATA0100_126C");
        assert_eq!(r.status, 404);
        assert_eq!(
            body(&r)["error"],
            "KYDATA0100_126C を開けません (この会社×年度の給与データが未作成、またはデータ復旧で作られた DB で権限の再付与が必要です)"
        );
        // 4060 以外 (208 = Invalid object name など) は 500、接続の失敗は 503
        let r = db_open_error_reply(&db_err(Stage::Query, server(208)), "X");
        assert_eq!(r.status, 500);
        let r = db_open_error_reply(&db_err(Stage::Login, server(4060)), "X");
        assert_eq!(r.status, 503);
        let r = db_open_error_reply(&db_err(Stage::Query, ErrKind::Timeout), "X");
        assert_eq!(r.status, 500);
    }

    #[test]
    fn cannot_open_by_code() {
        assert!(db_err(Stage::Query, server(4060)).cannot_open());
        assert!(!db_err(Stage::Query, server(208)).cannot_open());
        assert!(!db_err(Stage::Query, ErrKind::Io("Other".into())).cannot_open());
    }

    #[test]
    fn log_line() {
        let e = db_err(Stage::Query, server(208));
        assert_eq!(
            e.log_line("payroll", 12),
            "kyuyo payroll: failed at query (12 ms) kind=server:208/16/1"
        );
    }

    #[test]
    fn company_month() {
        assert_eq!(validate_company_month("0100", "2026-06"), Ok((2026, 6)));
        let r = validate_company_month("0500", "2026-06").unwrap_err();
        assert_eq!(r.status, 400);
        assert_eq!(
            body(&r)["error"],
            "company は 0100 / 0200 / 0300 / 0400 のいずれかで指定してください"
        );
        let r = validate_company_month("0100", "2026-6").unwrap_err();
        assert_eq!(
            (r.status, r.body.as_str()),
            (400, r#"{"error":"month は YYYY-MM で指定してください"}"#)
        );
    }

    #[test]
    fn params() {
        let p = |v: &[(&str, &str)]| -> Vec<(String, String)> {
            v.iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        let got = company_month_params(&p(&[("month", "2026-06"), ("company", "0100")]));
        assert_eq!(got, Ok(("0100".to_string(), "2026-06".to_string())));
        // 同じキーが 2 つなら先の方
        let got = company_month_params(&p(&[
            ("company", "0100"),
            ("company", "0200"),
            ("month", "m"),
        ]));
        assert_eq!(got, Ok(("0100".to_string(), "m".to_string())));
        for bad in [
            p(&[]),
            p(&[("company", "0100")]),
            p(&[("month", "2026-06")]),
        ] {
            let r = company_month_params(&bad).unwrap_err();
            assert_eq!(r.status, 400);
        }
    }

    fn kyuyo(shain: i32, month_index: i32) -> RawKyuyoRow {
        RawKyuyoRow {
            shain,
            month_index,
            pay_date: "2026-06-25".into(),
            period_start: "2026-06-01".into(),
            period_end: "2026-06-30".into(),
            employee_code: format!("{shain:04}"),
            employee_name: "試験 太郎".into(),
            taikyu: 0,
            department: "本社".into(),
            taikei: 1,
            money: vec![0; crate::payroll::MONEY_COLUMNS],
            kindata: vec![0; crate::payroll::KINDATA_COLUMNS],
        }
    }

    #[test]
    fn indexes() {
        let raw = [kyuyo(1, 7), kyuyo(2, 6), kyuyo(3, 7)];
        assert_eq!(month_indexes(&raw), vec![6, 7]);
        assert!(month_indexes(&[]).is_empty());
    }

    #[test]
    fn payroll_rows_warn_when_empty() {
        let (rows, warnings) = payroll_rows(&[], vec![], &[], "KYDATA0100_126C", "2026-06");
        assert!(rows.is_empty());
        assert_eq!(
            warnings,
            vec!["KYDATA0100_126C の 2026-06 に賃金期間が一致する支給回がありません".to_string()]
        );
        let mut raw = kyuyo(1, 6);
        raw.money[0] = 1000;
        let koumoku = vec![RawKoumokuRow {
            taikeikouno: "01018".into(),
            name: "基本給".into(),
            kazei: 1,
            meisai: 0,
            gengaku: 0,
        }];
        let (rows, warnings) = payroll_rows(&[raw], koumoku, &[], "D", "2026-06");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].payments.get("基本給"), Some(&1000));
        assert!(!warnings.iter().any(|w| w.contains("支給回がありません")));
    }

    #[test]
    fn companies() {
        let dbs = vec![
            ("KYDATA0100_126C".to_string(), Some(1)),
            ("KYDATA0100_125C".to_string(), Some(0)),
        ];
        let r = companies_response(&dbs, Some(vec![("0100".into(), "試験運輸".into())]));
        assert_eq!(r.companies.len(), 1);
        assert_eq!(r.companies[0].name, "試験運輸");
        assert_eq!(r.companies[0].years, vec![2026]);
        assert_eq!(r.warnings.len(), 1);
        assert!(r.warnings[0].starts_with("KYDATA0100_125C "));
        let r = companies_response(&dbs, None);
        assert_eq!(r.companies[0].name, "");
        assert_eq!(r.warnings[0], COMPANY_NAMES_WARNING);
        assert_eq!(r.warnings.len(), 2);
    }

    #[test]
    fn company_names() {
        let names = vec![("0100".into(), "A".into()), ("0200".into(), "B".into())];
        assert_eq!(
            company_name(Some(names.clone()), "0200"),
            ("B".into(), vec![])
        );
        assert_eq!(company_name(Some(names), "0300"), (String::new(), vec![]));
        assert_eq!(
            company_name(None, "0100"),
            (String::new(), vec![COMPANY_NAMES_WARNING.to_string()])
        );
    }

    #[test]
    fn store_write() {
        let r = store_write_error("payroll");
        assert_eq!(
            (r.status, r.body.as_str()),
            (
                500,
                r#"{"error":"キャッシュへの保存に失敗しました (payroll)"}"#
            )
        );
    }

    #[test]
    fn ok() {
        let r = ok_reply(&serde_json::json!({"a": 1}));
        assert_eq!((r.status, r.body.as_str()), (200, r#"{"a":1}"#));
    }

    #[test]
    fn rows_round_trip() {
        let raw = crate::payroll::RawEmployeeRow {
            employee_code: "0012".into(),
            employee_name: "試験 花子".into(),
            taikyu: 0,
            department: "本社".into(),
            taikei: 1,
            department_code: 1,
            branch_name: String::new(),
            job_name: String::new(),
            kkubun: 1,
            hire_date: "2020-04-01".into(),
            retire_date: String::new(),
            taikbn: 0,
        };
        let rows = crate::payroll::build_employee_rows(&[raw]);
        let enc = encode_rows(&rows).unwrap();
        assert_eq!(enc.len(), 1);
        let dec: Vec<EmployeeRow> = decode_rows(&enc).unwrap();
        assert_eq!(dec, rows);
        assert!(decode_rows::<EmployeeRow>(&["{}".to_string()]).is_err());
        assert!(decode_rows::<EmployeeRow>(&[]).unwrap().is_empty());
    }

    #[test]
    fn encode_error() {
        // map のキーが文字列でないと serde_json は失敗する
        let bad: Vec<HashMap<(i32, i32), i32>> = vec![HashMap::from([((1, 2), 3)])];
        assert!(encode_rows(&bad).is_err());
    }

    #[test]
    fn warnings_round_trip() {
        let w = vec!["a".to_string(), "日本語".to_string()];
        let json = encode_warnings(&w);
        assert_eq!(json, r#"["a","日本語"]"#);
        assert_eq!(decode_warnings(&json), Ok(w));
        assert!(decode_warnings("x").is_err());
        assert_eq!(encode_warnings(&[]), "[]");
    }
}
