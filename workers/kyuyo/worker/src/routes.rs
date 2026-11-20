//! SQL Server を開く 7 口 (`databases` / `companies` / `employees` / `payroll` / `sync` / `schema` / `bonus`) (Refs #322 / #414)。DO `KyuyoState` の
//! ロックの中からだけ呼ぶ。挙動はオンプレ版 `src/routes/kyuyo.rs` の各ハンドラと同じ:
//!
//! - `databases` / `companies` — 毎回 SQL Server を読む。`companies` は会社名マスタが読めなくても warning を付けて返す
//! - `employees` / `payroll` — read-through。DO の SQLite に (会社, 年度) / (会社, 月) があれば SQL Server を開かずに返す
//!   (`source: "cache"`)。miss なら読んで put (put の失敗は warn して読みは返す、`source: "live"`)
//! - `sync` — キャッシュに関わらず payroll と employees と賞与 (month を含む暦年、Refs #419) を読み直して put。
//!   **月例の put の失敗は 500** (sync 成功 = 保存が最新)。賞与の読み・保存の失敗は warning に落とし
//!   `bonus_rows: null` で返す (月例が主で、賞与で月例の sync を巻き込まない)
//! - `schema` — 読み取り専用のスキーマ探索 (Refs #414)。全テーブルの列・項目マスタ・支給回/支給日の件数だけ。
//!   キャッシュしない
//! - `bonus` — 暦年の賞与 (`SHOYO`) の全支給回 (Refs #414)。payroll と同じ形の行。read-through (Refs #419):
//!   (会社, 暦年) が保存済みなら SQL Server を開かずに返す (`source: "cache"`)。規約は payroll と同じ
//!
//! 1 リクエスト = 1 接続。失敗ログは 1 行 (口の名前・stage・種類・所要ミリ秒だけ)。

use kyuyo_logic::api::{
    BonusResponse, DatabasesResponse, EmployeesResponse, PayrollResponse, SchemaResponse,
    SyncResponse,
};
use kyuyo_logic::bonus::{year_period, BonusRow};
use kyuyo_logic::payroll::{
    build_employee_rows, kydata_db_name, month_period, nendo_for_month, EmployeeRow, PayrollRow,
};
use kyuyo_logic::service::{
    bonus_read_warning, bonus_rows, bonus_store_warning, companies_response, company_month_params,
    company_name, company_year_target, count_targets, count_warning, db_open_error_reply,
    month_indexes, ok_reply, payroll_rows, repo_error_reply, schema_koumoku, store_write_error,
    validate_company_month, DbError,
};
use kyuyo_logic::{log_line_for, Endpoint, Reply};
use worker::{console_error, console_log, console_warn, Date, Env, SqlStorage, Url};

use crate::repo::{open, Db};
use crate::store;

/// 7 口のどれかを走らせて応答を返す (`endpoint` は `opens_sql_server()` のもの)。
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
        Endpoint::Schema => ctx.schema(url).await,
        Endpoint::Bonus => ctx.bonus(url).await,
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

/// 賞与 (暦年) の live 読み結果 (Refs #419)。
struct BonusLive {
    rows: Vec<BonusRow>,
    warnings: Vec<String>,
}

/// 賞与の読みの失敗。`opening` は本体クエリ (`SHOYO`) での失敗 — 単体の口はこのときだけ 4060 を 404 にする。
struct BonusFail {
    e: DbError,
    opening: bool,
}

impl Ctx<'_> {
    fn ms(&self) -> u64 {
        Date::now().as_millis().saturating_sub(self.started)
    }

    fn log_fail(&self, what: &str, e: &DbError) {
        console_error!("{}", log_line_for(what, e.stage, &e.kind, self.ms()));
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
                let line = log_line_for(what, e.stage, &e.kind, self.ms());
                console_warn!("{line} (company_names)");
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

    async fn schema(&self, url: &Url) -> Reply {
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let (company, year, dbn) = match company_year_target(&pairs) {
            Ok(t) => t,
            Err(reply) => return reply,
        };
        let mut db = match self.open("schema").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        let fail = |e: DbError| {
            self.log_fail("schema", &e);
            repo_error_reply(&e)
        };
        let out = async {
            // 開けない DB (4060) は 404 (payroll と同じ)
            let tables = db.schema_tables(&dbn).await.map_err(|e| {
                self.log_fail("schema", &e);
                db_open_error_reply(&e, &dbn)
            })?;
            let columns = db.schema_columns(&dbn).await.map_err(fail)?;
            let koumoku = schema_koumoku(db.koumoku(&dbn).await.map_err(fail)?);
            let (targets, mut warnings) = count_targets(&columns);
            let mut value_counts = Vec::new();
            for t in &targets {
                // 1 列が読めなくても残りは返す (warning を足す)
                match db.value_counts(&dbn, t).await {
                    Ok(v) => value_counts.extend(v),
                    Err(e) => {
                        let line = log_line_for("schema", e.stage, &e.kind, self.ms());
                        console_warn!("{line} (value_counts)");
                        warnings.push(count_warning(t));
                    }
                }
            }
            Ok::<_, Reply>(SchemaResponse {
                company,
                year,
                database: dbn.clone(),
                tables,
                columns,
                koumoku,
                value_counts,
                warnings,
            })
        }
        .await;
        db.close().await;
        match out {
            Ok(body) => {
                self.log_ok("schema", "live");
                ok_reply(&body)
            }
            Err(reply) => reply,
        }
    }

    /// SQL Server から暦年 `year` の賞与を読む (Refs #414)。`dbn` は `KYDATA{company}_{year-1900}C`。
    /// 失敗はログ 1 行を出して [`BonusFail`] で返す (応答への写像は呼び出し側: 単体の口は status、sync は warning)。
    async fn bonus_live(
        &self,
        db: &mut Db,
        what: &str,
        dbn: &str,
        year: i32,
    ) -> Result<BonusLive, BonusFail> {
        let fail = |opening: bool| {
            move |e: DbError| {
                self.log_fail(what, &e);
                BonusFail { e, opening }
            }
        };
        let (from, to) = year_period(year);
        let raw = db.bonus(dbn, &from, &to).await.map_err(fail(true))?;
        let koumoku = db.koumoku(dbn).await.map_err(fail(false))?;
        let shukei = db.dated_shukei(dbn).await.map_err(fail(false))?;
        let (rows, warnings) = bonus_rows(&raw, koumoku, &shukei, dbn, year);
        Ok(BonusLive { rows, warnings })
    }

    async fn bonus(&self, url: &Url) -> Reply {
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        let (company, year, dbn) = match company_year_target(&pairs) {
            Ok(t) => t,
            Err(reply) => return reply,
        };
        let sql = self.sql;
        match store::get_bonus(sql, &company, year) {
            Ok(Some(cached)) => {
                self.log_ok("bonus", "cache");
                return ok_reply(&BonusResponse {
                    company,
                    year,
                    database: dbn,
                    rows: cached.rows,
                    warnings: cached.warnings,
                    source: "cache",
                    synced_at: cached.synced_at,
                });
            }
            Ok(None) => {}
            Err(_) => console_warn!("kyuyo bonus: store read failed, live fallback"),
        }

        let mut db = match self.open("bonus").await {
            Ok(db) => db,
            Err(reply) => return reply,
        };
        let live = self.bonus_live(&mut db, "bonus", &dbn, year).await;
        db.close().await;
        let live = match live {
            Ok(live) => live,
            // 開けない DB (4060) は本体クエリだけ 404 (payroll と同じ)
            Err(f) if f.opening => return db_open_error_reply(&f.e, &dbn),
            Err(f) => return repo_error_reply(&f.e),
        };
        let synced_at = now_rfc3339();
        let put = store::put_bonus(sql, &company, year, &live.rows, &live.warnings, &synced_at);
        if put.is_err() {
            // live 応答はそのまま返す — キャッシュ書き込み失敗で読みを殺さない
            console_warn!("kyuyo bonus: store write failed");
        }
        self.log_ok("bonus", "live");
        ok_reply(&BonusResponse {
            company,
            year,
            database: dbn,
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
        // 給与明細 → 社員マスタ → 賞与 (month を含む暦年、Refs #419) の順。月例が失敗したらその応答を返す。
        // 賞与は付け足し: 読めなくても月例の sync は成功させ、warning に段と種別を残す (bonus_rows は null)
        let bonus_db = kydata_db_name(&t.company, t.year - 1900);
        let live = async {
            let payroll = self.payroll_live(&mut db, "sync", &t).await?;
            let employees = self.employees_live(&mut db, "sync", &t).await?;
            let bonus = self.bonus_live(&mut db, "sync", &bonus_db, t.year).await;
            Ok::<_, Reply>((payroll, employees, bonus))
        }
        .await;
        db.close().await;
        let (payroll, employees, bonus) = match live {
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
        let (bonus_rows, bonus_warnings) = self.sync_bonus(&t, bonus, &synced_at);

        self.log_ok("sync", "live");
        let mut warnings = payroll.warnings;
        warnings.extend(employees.warnings);
        warnings.extend(bonus_warnings);
        ok_reply(&SyncResponse {
            company: t.company,
            month: t.month,
            database: t.db,
            payroll_rows: payroll.rows.len(),
            employees: employees.employees.len(),
            bonus_year: t.year,
            bonus_rows,
            synced_at,
            warnings,
        })
    }

    /// sync の賞与の後半: 読めていれば保存する。読み・保存の失敗は warning (段と種別) に落とし、行数は `None`。
    fn sync_bonus(
        &self,
        t: &Target,
        bonus: Result<BonusLive, BonusFail>,
        synced_at: &str,
    ) -> (Option<usize>, Vec<String>) {
        let bonus = match bonus {
            Ok(bonus) => bonus,
            Err(f) => return (None, vec![bonus_read_warning(t.year, &f.e)]),
        };
        let put = store::put_bonus(
            self.sql,
            &t.company,
            t.year,
            &bonus.rows,
            &bonus.warnings,
            synced_at,
        );
        if put.is_err() {
            console_warn!("kyuyo sync: store write failed (bonus)");
            let mut warnings = bonus.warnings;
            warnings.push(bonus_store_warning(t.year));
            return (None, warnings);
        }
        (Some(bonus.rows.len()), bonus.warnings)
    }
}
