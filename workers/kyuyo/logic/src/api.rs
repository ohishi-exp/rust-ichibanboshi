//! `/api/kyuyo/*` の応答型 (Refs #322)。オンプレ版 (repo ルートの `src/routes/kyuyo.rs`) と Worker の
//! 両方がこの定義を使う — 並走期間に応答を比べるので、JSON の形 (キーとその順) はここ 1 か所で決める。
//! 形を変えると消費側 (nuxt-dtako-admin) が壊れる。下の unit test がキーと順を固定している。

use serde::Serialize;

use crate::bonus::BonusRow;
use crate::payroll::{CompanyInfo, EmployeeRow, PayrollRow};

/// エラーレスポンス本文。
#[derive(Serialize, Debug)]
pub struct ErrorBody {
    pub error: String,
}

/// `GET /api/kyuyo/databases`。
#[derive(Serialize, Debug)]
pub struct DatabasesResponse {
    /// `KYDATA{会社4桁}_{年度3桁}C` 形式の DB 名一覧 (昇順)。
    pub databases: Vec<String>,
}

/// `GET /api/kyuyo/companies`。
#[derive(Serialize, Debug)]
pub struct CompaniesResponse {
    pub companies: Vec<CompanyInfo>,
    pub warnings: Vec<String>,
}

/// `GET /api/kyuyo/employees`。
#[derive(Serialize, Debug)]
pub struct EmployeesResponse {
    pub company: String,
    /// `KYCOMSTD.SELDATA.CONAME1` 由来の正式会社名 (取れなければ空文字 + warning)。
    /// 消費側 (社員マスタ) はこれを会社ラベルに使う (Refs nuxt-dtako-admin#367)。
    pub company_name: String,
    pub month: String,
    /// 参照した年度 DB 名。
    pub database: String,
    pub employees: Vec<EmployeeRow>,
    pub warnings: Vec<String>,
    /// このデータの出どころ (Refs #106): "cache" = SQLite derived store /
    /// "live" = OHKEN 直読み (write-through でキャッシュ済み)。
    pub source: &'static str,
    /// キャッシュの鮮度 (RFC3339)。live 読みでは今回の取得時刻。
    pub synced_at: String,
}

/// `GET /api/kyuyo/payroll`。
#[derive(Serialize, Debug)]
pub struct PayrollResponse {
    pub company: String,
    pub month: String,
    /// 参照した年度 DB 名。
    pub database: String,
    pub rows: Vec<PayrollRow>,
    pub warnings: Vec<String>,
    /// このデータの出どころ (Refs #106): "cache" = SQLite derived store /
    /// "live" = OHKEN 直読み (write-through でキャッシュ済み)。
    pub source: &'static str,
    /// キャッシュの鮮度 (RFC3339)。live 読みでは今回の取得時刻。
    pub synced_at: String,
}

/// `GET /api/kyuyo/synced-months` の 1 件。
#[derive(Serialize, Debug)]
pub struct SyncedMonthEntry {
    pub company: String,
    pub month: String,
    pub synced_at: String,
    pub row_count: i64,
}

/// `GET /kyuyo/synced-months` の賞与 1 件 (Refs #419)。1 件 = (会社, 暦年, 支給日)。
#[derive(Serialize, Debug)]
pub struct SyncedBonusEntry {
    pub company: String,
    /// 支給した暦年。
    pub year: i32,
    /// 支給日 ("YYYY-MM-DD")。取り込んだが賞与が 0 件の年は null (`row_count` 0) の 1 件だけ。
    pub pay_date: Option<String>,
    pub synced_at: String,
    /// その支給日の行数 (社員 × 支給回)。
    pub row_count: i64,
}

/// `GET /api/kyuyo/synced-months`。
#[derive(Serialize, Debug)]
pub struct SyncedMonthsResponse {
    pub entries: Vec<SyncedMonthEntry>,
    /// 保存済みの賞与 (Refs #419)。`entries` の形は変えずに別キーで足す。
    pub bonus: Vec<SyncedBonusEntry>,
}

/// `POST /api/kyuyo/sync`。
#[derive(Serialize, Debug)]
pub struct SyncResponse {
    pub company: String,
    pub month: String,
    pub database: String,
    /// 保存した給与明細の行数。
    pub payroll_rows: usize,
    /// 保存した社員マスタの人数。
    pub employees: usize,
    /// 取り直した賞与の暦年 (`month` を含む年、Refs #419)。
    pub bonus_year: i32,
    /// 保存した賞与の行数 (社員 × 支給回)。
    pub bonus_rows: usize,
    pub synced_at: String,
    pub warnings: Vec<String>,
}

/// `GET /api/kyuyo/access`: 「この人は給与データを見てよいか」の答え。
///
/// **`allowed` は常に `true`** — allowlist 外なら認可が 403 を返して応答はここまで来ない。
/// `{allowed: false}` を返す分岐を作らないのは、**呼び出し側が status を無視して body だけ
/// 読む実装になるのを防ぐため**。この口の答えは HTTP status が正で、body は「誰として通ったか」を
/// 添えるだけ。
#[derive(Serialize, Debug)]
pub struct AccessResponse {
    /// 常に `true` (上の docs 参照)。
    pub allowed: bool,
    /// 判定に使った email (認可の応答由来)。
    pub email: String,
}

/// `GET /kyuyo/bonus?company=&year=` (Refs #414)。暦年 `year` に支給した賞与 (`SHOYO`) の全支給回。
#[derive(Serialize, Debug)]
pub struct BonusResponse {
    pub company: String,
    /// 支給した暦年。
    pub year: i32,
    /// 参照した年度 DB 名 (`_{year - 1900}C`)。
    pub database: String,
    pub rows: Vec<BonusRow>,
    pub warnings: Vec<String>,
    /// このデータの出どころ (Refs #419): "cache" = DO の SQLite / "live" = 給与大臣の直読み (保存済み)。
    pub source: &'static str,
    /// 保存の鮮度 (RFC3339)。live 読みでは今回の取得時刻。
    pub synced_at: String,
}

/// `GET /kyuyo/schema` のテーブル 1 件 (Refs #414)。件数は `sys.partitions` のメタデータ (行は読まない)。
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SchemaTable {
    pub name: String,
    pub row_count: i64,
}

/// `GET /kyuyo/schema` の列 1 件 (`INFORMATION_SCHEMA.COLUMNS`)。
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SchemaColumn {
    pub table: String,
    pub column: String,
    pub data_type: String,
}

/// `GET /kyuyo/schema` の項目マスタ 1 件 (`KOUMOKU`。個人の値は含まない)。
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SchemaKoumoku {
    pub taikeikouno: String,
    pub name: String,
    pub kazei: i32,
    pub meisai: i32,
    pub gengaku: i32,
}

/// `GET /kyuyo/schema` の「列の値ごとの件数」1 件。支給回 (`MONTH` 等) と支給日 (`SHIKYUBI*`) の列だけを数える。
#[derive(Serialize, Debug, Clone, PartialEq)]
pub struct SchemaValueCount {
    pub table: String,
    pub column: String,
    pub value: String,
    pub count: i64,
}

/// `GET /kyuyo/schema?company=&year=` (Refs #414)。賞与のテーブルを実物で特定するための読み取り専用の口。
/// 返すのは指定 DB の全テーブル・全列のメタデータ、項目マスタ、支給回/支給日の列の値ごとの件数だけで、
/// 個人の行・氏名・金額は返さない。
#[derive(Serialize, Debug)]
pub struct SchemaResponse {
    pub company: String,
    /// 年度 (DB 名の `_{年度 - 1900}C`。12 月分の給与は翌年度 DB に入る)。
    pub year: i32,
    pub database: String,
    pub tables: Vec<SchemaTable>,
    pub columns: Vec<SchemaColumn>,
    pub koumoku: Vec<SchemaKoumoku>,
    pub value_counts: Vec<SchemaValueCount>,
    pub warnings: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json<T: Serialize>(v: &T) -> String {
        serde_json::to_string(v).unwrap()
    }

    fn s(v: &str) -> String {
        v.to_string()
    }

    #[test]
    fn error_body() {
        let v = ErrorBody { error: s("e") };
        assert_eq!(json(&v), r#"{"error":"e"}"#);
    }

    #[test]
    fn databases() {
        let v = DatabasesResponse {
            databases: vec![s("KYDATA0100_008C")],
        };
        assert_eq!(json(&v), r#"{"databases":["KYDATA0100_008C"]}"#);
    }

    #[test]
    fn companies() {
        let v = CompaniesResponse {
            companies: vec![CompanyInfo {
                company: s("0100"),
                name: s("n"),
                years: vec![8],
            }],
            warnings: vec![s("w")],
        };
        assert_eq!(
            json(&v),
            r#"{"companies":[{"company":"0100","name":"n","years":[8]}],"warnings":["w"]}"#
        );
    }

    #[test]
    fn employees() {
        let v = EmployeesResponse {
            company: s("0100"),
            company_name: s("n"),
            month: s("2026-06"),
            database: s("KYDATA0100_008C"),
            employees: vec![],
            warnings: vec![],
            source: "cache",
            synced_at: s("t"),
        };
        let want = concat!(
            r#"{"company":"0100","company_name":"n","month":"2026-06","database":"KYDATA0100_008C","#,
            r#""employees":[],"warnings":[],"source":"cache","synced_at":"t"}"#
        );
        assert_eq!(json(&v), want);
    }

    #[test]
    fn payroll() {
        let v = PayrollResponse {
            company: s("0100"),
            month: s("2026-06"),
            database: s("KYDATA0100_008C"),
            rows: vec![],
            warnings: vec![s("w")],
            source: "live",
            synced_at: s("t"),
        };
        let want = concat!(
            r#"{"company":"0100","month":"2026-06","database":"KYDATA0100_008C","rows":[],"#,
            r#""warnings":["w"],"source":"live","synced_at":"t"}"#
        );
        assert_eq!(json(&v), want);
    }

    #[test]
    fn synced_months() {
        let v = SyncedMonthsResponse {
            entries: vec![SyncedMonthEntry {
                company: s("0100"),
                month: s("2026-06"),
                synced_at: s("t"),
                row_count: 3,
            }],
            bonus: vec![
                SyncedBonusEntry {
                    company: s("0100"),
                    year: 2025,
                    pay_date: Some(s("2025-08-08")),
                    synced_at: s("t"),
                    row_count: 2,
                },
                SyncedBonusEntry {
                    company: s("0100"),
                    year: 2024,
                    pay_date: None,
                    synced_at: s("t"),
                    row_count: 0,
                },
            ],
        };
        let want = concat!(
            r#"{"entries":[{"company":"0100","month":"2026-06","synced_at":"t","row_count":3}],"#,
            r#""bonus":[{"company":"0100","year":2025,"pay_date":"2025-08-08","synced_at":"t","row_count":2},"#,
            r#"{"company":"0100","year":2024,"pay_date":null,"synced_at":"t","row_count":0}]}"#
        );
        assert_eq!(json(&v), want);
        let empty = SyncedMonthsResponse {
            entries: vec![],
            bonus: vec![],
        };
        assert_eq!(json(&empty), r#"{"entries":[],"bonus":[]}"#);
    }

    #[test]
    fn sync() {
        let v = SyncResponse {
            company: s("0100"),
            month: s("2026-06"),
            database: s("KYDATA0100_008C"),
            payroll_rows: 2,
            employees: 1,
            bonus_year: 2026,
            bonus_rows: 4,
            synced_at: s("t"),
            warnings: vec![],
        };
        let want = concat!(
            r#"{"company":"0100","month":"2026-06","database":"KYDATA0100_008C","#,
            r#""payroll_rows":2,"employees":1,"bonus_year":2026,"bonus_rows":4,"#,
            r#""synced_at":"t","warnings":[]}"#
        );
        assert_eq!(json(&v), want);
    }

    #[test]
    fn access() {
        let v = AccessResponse {
            allowed: true,
            email: s("a@example.com"),
        };
        assert_eq!(json(&v), r#"{"allowed":true,"email":"a@example.com"}"#);
    }

    #[test]
    fn bonus() {
        let v = BonusResponse {
            company: s("0100"),
            year: 2025,
            database: s("KYDATA0100_125C"),
            rows: vec![],
            warnings: vec![s("w")],
            source: "cache",
            synced_at: s("t"),
        };
        let want = concat!(
            r#"{"company":"0100","year":2025,"database":"KYDATA0100_125C","rows":[],"#,
            r#""warnings":["w"],"source":"cache","synced_at":"t"}"#
        );
        assert_eq!(json(&v), want);
    }

    #[test]
    fn schema() {
        let v = SchemaResponse {
            company: s("0100"),
            year: 2026,
            database: s("KYDATA0100_126C"),
            tables: vec![SchemaTable {
                name: s("SHOYO"),
                row_count: 3,
            }],
            columns: vec![SchemaColumn {
                table: s("SHOYO"),
                column: s("MONTH"),
                data_type: s("smallint"),
            }],
            koumoku: vec![SchemaKoumoku {
                taikeikouno: s("01098"),
                name: s("n"),
                kazei: 1,
                meisai: 0,
                gengaku: 0,
            }],
            value_counts: vec![SchemaValueCount {
                table: s("SHOYO"),
                column: s("MONTH"),
                value: s("1"),
                count: 3,
            }],
            warnings: vec![],
        };
        let want = concat!(
            r#"{"company":"0100","year":2026,"database":"KYDATA0100_126C","#,
            r#""tables":[{"name":"SHOYO","row_count":3}],"#,
            r#""columns":[{"table":"SHOYO","column":"MONTH","data_type":"smallint"}],"#,
            r#""koumoku":[{"taikeikouno":"01098","name":"n","kazei":1,"meisai":0,"gengaku":0}],"#,
            r#""value_counts":[{"table":"SHOYO","column":"MONTH","value":"1","count":3}],"#,
            r#""warnings":[]}"#
        );
        assert_eq!(json(&v), want);
    }

    #[test]
    fn debug_is_derived() {
        // Debug は root の handler テストが使う
        let v = ErrorBody { error: s("e") };
        assert!(format!("{v:?}").contains("ErrorBody"));
    }
}
