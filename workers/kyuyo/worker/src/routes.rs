//! SQL Server を開く 5 口 (`databases` / `companies` / `employees` / `payroll` / `sync`) (Refs #322)。DO `KyuyoState` の
//! ロックの中からだけ呼ぶ。挙動はオンプレ版 `src/routes/kyuyo.rs` の各ハンドラと同じ:
//!
//! - `databases` / `companies` — 毎回 SQL Server を読む。`companies` は会社名マスタが読めなくても warning を付けて返す
//! - `employees` / `payroll` — read-through。DO の SQLite に (会社, 年度) / (会社, 月) があれば SQL Server を開かずに返す
//!   (`source: "cache"`)。miss なら読んで put (put の失敗は warn して読みは返す、`source: "live"`)
//! - `sync` — キャッシュに関わらず payroll と employees を読み直して put。**put の失敗は 500** (sync 成功 = 保存が最新)
//!
//! 1 リクエスト = 1 接続。失敗ログは 1 行 (口の名前・stage・種類・所要ミリ秒だけ)。

use kyuyo_logic::api::{DatabasesResponse, EmployeesResponse, PayrollResponse, SyncResponse};
use kyuyo_logic::payroll::{
    build_employee_rows, kydata_db_name, month_period, nendo_for_month, EmployeeRow, PayrollRow,
};
use kyuyo_logic::service::{
    companies_response, company_month_params, company_name, db_open_error_reply, month_indexes,
    ok_reply, payroll_rows, repo_error_reply, store_write_error, validate_company_month, DbError,
};
use kyuyo_logic::{Endpoint, Reply};
use worker::{console_error, console_log, console_warn, Date, Env, SqlStorage, Url};

use crate::repo::{open, Db};
use crate::store;

/// 5 口のどれかを走らせて応答を返す (`endpoint` は `opens_sql_server()` のもの)。
pub(crate) async fn run(env: &Env, sql: &SqlStorage, endpoint: Endpoint, url: &Url) -> Reply {
    let ctx = Ctx {
        env,
        sql,
        started: Date::now().as_millis(),
    };
    match endpoint {
        Endpoint::Databases => ctx.databases().await,
        Endpoint::Companies => ctx.companies().await,
        Endpoint::Employees => ctx.employees(url).await,
        Endpoint::Payroll => ctx.payroll(url).await,
        _ => ctx.sync(url).await,
    }
}

struct Ctx<'a> {
    env: &'a Env,
    /// DO の SQLite (derived store)
    sql: &'a SqlStorage,
    started: u64,
}

/// 検証済みの (company, month) と、そこから決まる年度・DB 名。
struct Target {
    company: String,
    month: String,
    year: i32,
    month_num: u32,
    nendo: i32,
    db: String,
}

fn target(url: &Url) -> Result<Target, Reply> {
    let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
    let (company, month) = company_month_params(&pairs)?;
    let (year, month_num) = validate_company_month(&company, &month)?;
    let nendo = nendo_for_month(year, month_num);
    let db = kydata_db_name(&company, nendo);
    Ok(Target {
        company,
        month,
        year,
        month_num,
        nendo,
        db,
    })
}

/// `synced_at` (RFC3339)。オンプレ版の `chrono::Utc::now().to_rfc3339()` と同じ形 (精度はミリ秒)。
fn now_rfc3339() -> String {
    let ms = Date::now().as_millis() as i64;
    chrono::DateTime::from_timestamp_millis(ms)
        .unwrap_or_default()
        .to_rfc3339()
}

/// 社員マスタの live 読み結果 (オンプレ版 `EmployeesLive`)。
struct EmployeesLive {
    employees: Vec<EmployeeRow>,
    company_name: String,
    warnings: Vec<String>,
}

/// 給与明細の live 読み結果 (オンプレ版 `PayrollLive`)。
struct PayrollLive {
    rows: Vec<PayrollRow>,
    warnings: Vec<String>,
}

impl Ctx<'_> {
    fn ms(&self) -> u64 {
        Date::now().as_millis().saturating_sub(self.started)
    }

    fn log_fail(&self, what: &str, e: &DbError) {
        console_error!("{}", e.log_line(what, self.ms()));
    }

    fn log_ok(&self, what: &str, source: &str) {
        console_log!("kyuyo {what}: ok {source} ({} ms)", self.ms());
    }

    /// 接続を開く。失敗はログ 1 行と写像済みの応答 (503 など)。
    async fn open(&self, what: &str) -> Result<Db, Reply> {
        open(self.env).await.map_err(|e| {
            self.log_fail(what, &e);
            repo_error_reply(&e)
        })
    }

    async fn databases(&self) -> Reply {
        let mut db = match self.open("databases").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        let out = db.database_names().await;
        db.close().await;
        match out {
            Ok(databases) => {
                self.log_ok("databases", "live");
                ok_reply(&DatabasesResponse { databases })
            }
            Err(e) => {
                self.log_fail("databases", &e);
                repo_error_reply(&e)
            }
        }
    }

    async fn companies(&self) -> Reply {
        let mut db = match self.open("companies").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        let out = async {
            let databases = db.databases_with_access().await?;
            // 会社名は補助情報 — KYCOMSTD が読めなくても一覧自体は返す
            let names = self.company_names(&mut db, "companies").await;
            Ok::<_, DbError>((databases, names))
        }
        .await;
        db.close().await;
        match out {
            Ok((databases, names)) => {
                self.log_ok("companies", "live");
                ok_reply(&companies_response(&databases, names))
            }
            Err(e) => {
                self.log_fail("companies", &e);
                repo_error_reply(&e)
            }
        }
    }

    /// 会社名マスタ。読めなければ warn ログを 1 行出して `None`。
    async fn company_names(&self, db: &mut Db, what: &str) -> Option<Vec<(String, String)>> {
        match db.company_names().await {
            Ok(pairs) => Some(pairs),
            Err(e) => {
                console_warn!("{} (company_names)", e.log_line(what, self.ms()));
                None
            }
        }
    }

    /// SQL Server から社員マスタを読む (オンプレ版 `fetch_employees_live`)。
    async fn employees_live(
        &self,
        db: &mut Db,
        what: &str,
        t: &Target,
    ) -> Result<EmployeesLive, Reply> {
        let raw = db.employees(&t.db).await.map_err(|e| {
            self.log_fail(what, &e);
            db_open_error_reply(&e, &t.db)
        })?;
        let names = self.company_names(db, what).await;
        let (company_name, warnings) = company_name(names, &t.company);
        Ok(EmployeesLive {
            employees: build_employee_rows(&raw),
            company_name,
            warnings,
        })
    }

    /// SQL Server から給与明細を読む (オンプレ版 `fetch_payroll_live`)。
    async fn payroll_live(
        &self,
        db: &mut Db,
        what: &str,
        t: &Target,
    ) -> Result<PayrollLive, Reply> {
        let fail = |e: DbError| {
            self.log_fail(what, &e);
            repo_error_reply(&e)
        };
        let (from, to) = month_period(t.year, t.month_num);
        // 開けない DB (4060) は 404。存在確認の事前クエリはしない (オンプレ版と同じ)
        let raw = db.payroll_month(&t.db, &from, &to).await.map_err(|e| {
            self.log_fail(what, &e);
            db_open_error_reply(&e, &t.db)
        })?;
        let koumoku = db.koumoku(&t.db).await.map_err(fail)?;
        let mut shukei = Vec::new();
        for idx in month_indexes(&raw) {
            shukei.extend(db.shukei_totals(&t.db, idx).await.map_err(fail)?);
        }
        let (rows, warnings) = payroll_rows(&raw, koumoku, &shukei, &t.db, &t.month);
        Ok(PayrollLive { rows, warnings })
    }

    async fn employees(&self, url: &Url) -> Reply {
        let t = match target(url) {
            Ok(t) => t,
            Err(reply) => return reply,
        };
        let sql = self.sql;
        match store::get_employees(sql, &t.company, t.nendo) {
            Ok(Some(cached)) => {
                self.log_ok("employees", "cache");
                return ok_reply(&EmployeesResponse {
                    company: t.company,
                    company_name: cached.company_name,
                    month: t.month,
                    database: t.db,
                    employees: cached.rows,
                    warnings: cached.warnings,
                    source: "cache",
                    synced_at: cached.synced_at,
                });
            }
            Ok(None) => {}
            Err(_) => console_warn!("kyuyo employees: store read failed, live fallback"),
        }

        let mut db = match self.open("employees").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        let live = self.employees_live(&mut db, "employees", &t).await;
        db.close().await;
        let live = match live {
            Ok(live) => live,
            Err(reply) => return reply,
        };
        let synced_at = now_rfc3339();
        let put = store::put_employees(
            sql,
            &t.company,
            t.nendo,
            &live.employees,
            &live.company_name,
            &live.warnings,
            &synced_at,
        );
        if put.is_err() {
            // live 応答はそのまま返す — キャッシュ書き込み失敗で読みを殺さない
            console_warn!("kyuyo employees: store write failed");
        }
        self.log_ok("employees", "live");
        ok_reply(&EmployeesResponse {
            company: t.company,
            company_name: live.company_name,
            month: t.month,
            database: t.db,
            employees: live.employees,
            warnings: live.warnings,
            source: "live",
            synced_at,
        })
    }

    async fn payroll(&self, url: &Url) -> Reply {
        let t = match target(url) {
            Ok(t) => t,
            Err(reply) => return reply,
        };
        let sql = self.sql;
        match store::get_payroll(sql, &t.company, &t.month) {
            Ok(Some(cached)) => {
                self.log_ok("payroll", "cache");
                return ok_reply(&PayrollResponse {
                    company: t.company,
                    month: t.month,
                    database: t.db,
                    rows: cached.rows,
                    warnings: cached.warnings,
                    source: "cache",
                    synced_at: cached.synced_at,
                });
            }
            Ok(None) => {}
            Err(_) => console_warn!("kyuyo payroll: store read failed, live fallback"),
        }

        let mut db = match self.open("payroll").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        let live = self.payroll_live(&mut db, "payroll", &t).await;
        db.close().await;
        let live = match live {
            Ok(live) => live,
            Err(reply) => return reply,
        };
        let synced_at = now_rfc3339();
        let put = store::put_payroll(
            sql,
            &t.company,
            &t.month,
            &live.rows,
            &live.warnings,
            &synced_at,
        );
        if put.is_err() {
            // live 応答はそのまま返す — キャッシュ書き込み失敗で読みを殺さない
            console_warn!("kyuyo payroll: store write failed");
        }
        self.log_ok("payroll", "live");
        ok_reply(&PayrollResponse {
            company: t.company,
            month: t.month,
            database: t.db,
            rows: live.rows,
            warnings: live.warnings,
            source: "live",
            synced_at,
        })
    }

    async fn sync(&self, url: &Url) -> Reply {
        let t = match target(url) {
            Ok(t) => t,
            Err(reply) => return reply,
        };
        let mut db = match self.open("sync").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        // 給与明細 → 社員マスタの順 (オンプレ版と同じ)。どちらかが失敗したらその応答を返す
        let live = async {
            let payroll = self.payroll_live(&mut db, "sync", &t).await?;
            let employees = self.employees_live(&mut db, "sync", &t).await?;
            Ok::<_, Reply>((payroll, employees))
        }
        .await;
        db.close().await;
        let (payroll, employees) = match live {
            Ok(live) => live,
            Err(reply) => return reply,
        };

        // read-through と違い、store へ書けなければ 500 で loud fail (sync の成功 = キャッシュが最新)
        let sql = self.sql;
        let synced_at = now_rfc3339();
        let put = store::put_payroll(
            sql,
            &t.company,
            &t.month,
            &payroll.rows,
            &payroll.warnings,
            &synced_at,
        );
        if put.is_err() {
            console_error!("kyuyo sync: store write failed (payroll)");
            return store_write_error("payroll");
        }
        let put = store::put_employees(
            sql,
            &t.company,
            t.nendo,
            &employees.employees,
            &employees.company_name,
            &employees.warnings,
            &synced_at,
        );
        if put.is_err() {
            console_error!("kyuyo sync: store write failed (employees)");
            return store_write_error("employees");
        }

        self.log_ok("sync", "live");
        let mut warnings = payroll.warnings;
        warnings.extend(employees.warnings);
        ok_reply(&SyncResponse {
            company: t.company,
            month: t.month,
            database: t.db,
            payroll_rows: payroll.rows.len(),
            employees: employees.employees.len(),
            synced_at,
            warnings,
        })
    }
}
