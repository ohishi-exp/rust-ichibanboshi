//! 経路判定・失敗の stage と応答の写像・資格情報 JSON の検証 (純粋ロジック)。`POST /probe` と管理画面の GET 6 本で共用する。
//! workers/kyuyo の `logic/src/lib.rs` (kyuyo-logic) から要る分だけを写している (path 依存は作らない)。

use serde::Deserialize;

/// 経路判定の結果。path とクエリはオンプレ版 (repo ルートの `src/routes/`) と同じ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Route {
    /// `POST /probe` — 到達の切り分け用 (ログインして `SELECT 1`)
    Probe,
    /// `GET /health`
    Health,
    /// `GET /api/employees`
    Employees,
    /// `GET /api/vehicles`
    Vehicles,
    /// `GET /api/sales/departments`
    Departments,
    /// `GET /api/sales/vehicle-daily`
    VehicleDaily,
    /// `GET /api/costs/vehicle-daily`
    CostsDaily,
    // ── 移している途中の 15 本 (#322)。中身は領域別の routes/<領域>.rs。埋まるまでは 501 ──
    /// `GET /api/sales/monthly`
    SalesMonthly,
    /// `GET /api/sales/by-department`
    SalesByDepartment,
    /// `GET /api/sales/by-customer`
    SalesByCustomer,
    /// `GET /api/sales/yoy`
    SalesYoy,
    /// `GET /api/sales/daily`
    SalesDaily,
    /// `GET /api/sales/customer-trend`
    SalesCustomerTrend,
    /// `GET /api/sales/customer-detail`
    SalesCustomerDetail,
    /// `GET /api/sales/customer-yoy`
    SalesCustomerYoy,
    /// `GET /api/sales/customer-yoy-by-dept`
    SalesCustomerYoyByDept,
    /// `GET /api/unchin/candidates`
    UnchinCandidates,
    /// `GET /api/unchin/summary`
    UnchinSummary,
    /// `GET /api/unchin/customer-net`
    UnchinCustomerNet,
    /// `GET /api/unchin/customer-net-detail`
    UnchinCustomerNetDetail,
    /// `GET /api/surcharge/base`
    SurchargeBase,
    /// `GET /api/schema/columns`
    SchemaColumns,
    NotFound,
    MethodNotAllowed,
}

/// 口は上の 22 本だけ。path が合って method が違えば 405、それ以外の path は 404。
pub(crate) fn route(method: &str, path: &str) -> Route {
    let (found, want) = match path {
        "/probe" => (Route::Probe, "POST"),
        "/health" => (Route::Health, "GET"),
        "/api/employees" => (Route::Employees, "GET"),
        "/api/vehicles" => (Route::Vehicles, "GET"),
        "/api/sales/departments" => (Route::Departments, "GET"),
        "/api/sales/vehicle-daily" => (Route::VehicleDaily, "GET"),
        "/api/costs/vehicle-daily" => (Route::CostsDaily, "GET"),
        "/api/sales/monthly" => (Route::SalesMonthly, "GET"),
        "/api/sales/by-department" => (Route::SalesByDepartment, "GET"),
        "/api/sales/by-customer" => (Route::SalesByCustomer, "GET"),
        "/api/sales/yoy" => (Route::SalesYoy, "GET"),
        "/api/sales/daily" => (Route::SalesDaily, "GET"),
        "/api/sales/customer-trend" => (Route::SalesCustomerTrend, "GET"),
        "/api/sales/customer-detail" => (Route::SalesCustomerDetail, "GET"),
        "/api/sales/customer-yoy" => (Route::SalesCustomerYoy, "GET"),
        "/api/sales/customer-yoy-by-dept" => (Route::SalesCustomerYoyByDept, "GET"),
        "/api/unchin/candidates" => (Route::UnchinCandidates, "GET"),
        "/api/unchin/summary" => (Route::UnchinSummary, "GET"),
        "/api/unchin/customer-net" => (Route::UnchinCustomerNet, "GET"),
        "/api/unchin/customer-net-detail" => (Route::UnchinCustomerNetDetail, "GET"),
        "/api/surcharge/base" => (Route::SurchargeBase, "GET"),
        "/api/schema/columns" => (Route::SchemaColumns, "GET"),
        _ => return Route::NotFound,
    };
    if method == want {
        found
    } else {
        Route::MethodNotAllowed
    }
}

impl Route {
    /// ログに出す口の名前。
    pub(crate) fn name(self) -> &'static str {
        match self {
            Route::Probe => "probe",
            Route::Health => "health",
            Route::Employees => "employees",
            Route::Vehicles => "vehicles",
            Route::Departments => "departments",
            Route::VehicleDaily => "vehicle-daily",
            Route::CostsDaily => "costs-daily",
            Route::SalesMonthly => "sales_monthly",
            Route::SalesByDepartment => "sales_by_department",
            Route::SalesByCustomer => "sales_by_customer",
            Route::SalesYoy => "sales_yoy",
            Route::SalesDaily => "sales_daily",
            Route::SalesCustomerTrend => "sales_customer_trend",
            Route::SalesCustomerDetail => "sales_customer_detail",
            Route::SalesCustomerYoy => "sales_customer_yoy",
            Route::SalesCustomerYoyByDept => "sales_customer_yoy_by_dept",
            Route::UnchinCandidates => "unchin_candidates",
            Route::UnchinSummary => "unchin_summary",
            Route::UnchinCustomerNet => "unchin_customer_net",
            Route::UnchinCustomerNetDetail => "unchin_customer_net_detail",
            Route::SurchargeBase => "surcharge_base",
            Route::SchemaColumns => "schema_columns",
            Route::NotFound => "not-found",
            Route::MethodNotAllowed => "method-not-allowed",
        }
    }
}

/// どこで失敗したか。応答とログに出すのはこの名前だけ。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    /// 資格情報の JSON が読めない・キー欠け・空
    Secret,
    /// TCP が開けない、または上限時間内にログインまで終わらない
    Connect,
    /// TDS のログインが拒否された
    Login,
    /// クエリが失敗した・時間切れ (`/probe`・`/health` は 1 が返らないときも)
    Query,
}

impl Stage {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Stage::Secret => "secret",
            Stage::Connect => "connect",
            Stage::Login => "login",
            Stage::Query => "query",
        }
    }
}

/// 失敗の種類。ログ 1 行に出すのはこの分類だけで、エラーの本文 (ホスト・ポート・ユーザー名を含みうる) は持たない。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ErrKind {
    /// 上限時間内に終わらない
    Timeout,
    /// VPC binding からの Socket 取得・connect の失敗
    Transport,
    /// I/O エラー。`std::io::ErrorKind` の名前だけ (例 "Other"、"ConnectionReset")
    Io(String),
    /// SQL Server が返したエラー (番号・クラス・状態だけ)
    Server { code: u32, class: u8, state: u8 },
    /// TDS のプロトコル違反
    Protocol,
    /// 文字コードの不一致
    Encoding,
    /// TLS のハンドシェイク失敗
    Tls,
    /// サーバが別アドレスへの接続を要求した
    Routing,
    /// 上のどれでもない
    Other,
}

impl ErrKind {
    /// ログ用の短い名前 (`io:Other`、`server:18456/14/1` など)。
    pub(crate) fn label(&self) -> String {
        match self {
            ErrKind::Timeout => "timeout".to_string(),
            ErrKind::Transport => "transport".to_string(),
            ErrKind::Io(name) => format!("io:{name}"),
            ErrKind::Server { code, class, state } => format!("server:{code}/{class}/{state}"),
            ErrKind::Protocol => "protocol".to_string(),
            ErrKind::Encoding => "encoding".to_string(),
            ErrKind::Tls => "tls".to_string(),
            ErrKind::Routing => "routing".to_string(),
            ErrKind::Other => "other".to_string(),
        }
    }
}

/// 接続の失敗 (stage と種類だけ。本文は持たない)。
#[derive(Debug)]
pub(crate) struct DbError {
    pub(crate) stage: Stage,
    pub(crate) kind: ErrKind,
}

/// 口の失敗。応答にはこの分類だけを写す (エラーの本文は持たない)。
#[derive(Debug)]
pub(crate) enum Failure {
    /// クエリが読めない・絞り込みが 1 つも無い (オンプレ版と同じ 400、本文なし)
    BadRequest,
    /// SQL Server までの途中 (資格情報・接続・ログイン・クエリ) で失敗した
    Db(Stage, ErrKind),
    /// 経路はあるが中身を移し終えていない口 (#322 の移行途中)。501、本文なし
    NotImplemented,
}

impl From<DbError> for Failure {
    fn from(e: DbError) -> Self {
        Failure::Db(e.stage, e.kind)
    }
}

/// 失敗ログの 1 行。メッセージ本文を受け取らない (口・stage・種類・所要ミリ秒だけ)。
pub(crate) fn log_line(route: Route, stage: Stage, kind: &ErrKind, elapsed_ms: u64) -> String {
    let (name, stage, kind) = (route.name(), stage.as_str(), kind.label());
    format!("ichiban {name}: failed at {stage} ({elapsed_ms} ms) kind={kind}")
}

/// 応答の status と JSON 本文。本文が空なら content-type を付けない (オンプレ版の 400 と同じ)。
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Reply {
    pub(crate) status: u16,
    pub(crate) body: String,
}

/// 経路判定で弾いた行き先の応答。口に当たったら `None` (先へ進む)。
pub(crate) fn reply_for_route(route: Route) -> Option<Reply> {
    let status = match route {
        Route::NotFound => 404,
        Route::MethodNotAllowed => 405,
        _ => return None,
    };
    Some(Reply {
        status,
        body: r#"{"ok":false}"#.to_string(),
    })
}

/// 口の結果の応答。成功は 200 と JSON 本文、絞り込みの不備は 400 (本文なし)、移し終えていない口は 501 (本文なし)、
/// SQL Server までの失敗は 502 `{"ok":false,"stage":…,"kind":…}`。
pub(crate) fn reply_for(outcome: Result<String, Failure>) -> Reply {
    match outcome {
        Ok(body) => Reply { status: 200, body },
        Err(Failure::BadRequest) => Reply {
            status: 400,
            body: String::new(),
        },
        Err(Failure::NotImplemented) => Reply {
            status: 501,
            body: String::new(),
        },
        Err(Failure::Db(stage, kind)) => Reply {
            status: 502,
            body: format!(
                r#"{{"ok":false,"stage":"{}","kind":"{}"}}"#,
                stage.as_str(),
                kind.label()
            ),
        },
    }
}

/// SQL Server 認証の資格情報 (Secrets Store の JSON `{"user":…,"pass":…}`)。
/// `Debug` を持たせない (うっかりログに出さない)。
#[derive(Deserialize)]
pub(crate) struct Creds {
    pub(crate) user: String,
    pub(crate) pass: String,
}

/// 資格情報の JSON を検証する。JSON が読めない・キー欠け・文字列でない・空は `Err(Stage::Secret)`。
pub(crate) fn parse_creds(json: &str) -> Result<Creds, Stage> {
    let creds: Creds = serde_json::from_str(json).map_err(|_| Stage::Secret)?;
    if creds.user.is_empty() || creds.pass.is_empty() {
        return Err(Stage::Secret);
    }
    Ok(creds)
}
