//! Durable Object `KyuyoState` (SQLite)。インスタンスは名前 `"kyuyo"` の 1 つだけで、Worker の fetch は
//! 全リクエストをここへ転送する。
//!
//! - **SQL Server を開く処理はここ (`sql_server` のロックの中) だけ**。DO は await 中に次のリクエストが
//!   割り込むので明示のロックで直列化し、給与大臣 PC への同時接続を 1 本に保つ (オンプレ版の
//!   `KyuyoLimiter` と同じ役目)。`POST /probe` と `/kyuyo/*` の 5 口 (#c322-7) がこれを通る
//! - 保存先は `kyuyo_logic::store_keys` の DDL の 3 表 (オンプレ版の derived store と同じ形) +
//!   自前の `schema_version` 表 (PRAGMA user_version は使わない)。版が違えば 3 表を drop → 再作成
//!   (derived なので migration しない)
//! - `/kyuyo/*` の認可は Worker 側で済んでいる。email は内部ヘッダ `EMAIL_HEADER` で届く
//!   (Worker は DO へのリクエストを新しく組み立てるので、外から来た同名ヘッダは届かない)。
//!   DO はこの Worker の binding からしか届かない

use std::cell::Cell;

use kyuyo_logic::auth::{
    access_reply, not_implemented, server_error, store_error, synced_months_reply, EMAIL_HEADER,
};
use kyuyo_logic::store_keys::{
    CREATE_TABLES_SQL, DROP_TABLES_SQL, PAYROLL_SYNCED_SQL, SCHEMA_VERSION,
};
use kyuyo_logic::{reply_for_route, route, Endpoint, Route};
use serde::Deserialize;
use tokio::sync::Mutex;
use worker::{
    console_error, durable_object, DurableObject, Env, Headers, Request, RequestInit, Response,
    Result, SqlStorage, SqlStorageValue, State,
};

use crate::{probe, respond};

/// wrangler.toml の `[[durable_objects.bindings]]` の name。
const STATE_BINDING: &str = "KYUYO_STATE";
/// DO のインスタンス名 (1 つだけ)。
const STATE_NAME: &str = "kyuyo";

#[durable_object]
pub struct KyuyoState {
    sql: SqlStorage,
    env: Env,
    /// SQL Server を開く区間の直列化
    sql_server: Mutex<()>,
    /// この instance で schema を確かめたか
    schema_ready: Cell<bool>,
}

impl DurableObject for KyuyoState {
    fn new(state: State, env: Env) -> Self {
        Self {
            sql: state.storage().sql(),
            env,
            sql_server: Mutex::new(()),
            schema_ready: Cell::new(false),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let route = route(req.method().as_ref(), &req.path());
        if let Some(reply) = reply_for_route(&route) {
            return respond(reply);
        }
        if self.ensure_schema().is_err() {
            console_error!("kyuyo state: schema init failed");
            return respond(store_error());
        }
        match route {
            Route::Kyuyo(endpoint) => {
                // 認可済みの印。Worker が必ず付けるので、無ければ通さない (fail-closed)
                let Some(email) = req.headers().get(EMAIL_HEADER)?.filter(|e| !e.is_empty()) else {
                    return respond(server_error());
                };
                self.kyuyo(endpoint, &email).await
            }
            _ => {
                let _one = self.sql_server.lock().await;
                respond(probe::run(&self.env).await)
            }
        }
    }
}

#[derive(Deserialize)]
struct VersionRow {
    version: i32,
}

#[derive(Deserialize)]
struct SyncedRow {
    scope: String,
    synced_at: String,
    row_count: i64,
}

impl KyuyoState {
    async fn kyuyo(&self, endpoint: Endpoint, email: &str) -> Result<Response> {
        match endpoint {
            // SQL Server を開かない 2 口
            Endpoint::Access => {
                let res = respond(access_reply(email))?;
                res.headers().set("cache-control", "no-store")?;
                Ok(res)
            }
            Endpoint::SyncedMonths => match self.payroll_synced() {
                Ok(rows) => respond(synced_months_reply(rows)),
                Err(_) => {
                    console_error!("kyuyo state: synced-months read failed");
                    respond(store_error())
                }
            },
            // 残りの 5 口は #c322-7 で実装する。SQL Server を開くのはこのロックの中だけ
            _ => {
                let _one = self.sql_server.lock().await;
                respond(not_implemented())
            }
        }
    }

    fn ensure_schema(&self) -> Result<()> {
        if self.schema_ready.get() {
            return Ok(());
        }
        self.sql.exec(
            "CREATE TABLE IF NOT EXISTS schema_version (version INTEGER NOT NULL)",
            None,
        )?;
        let rows: Vec<VersionRow> = self
            .sql
            .exec("SELECT version FROM schema_version", None)?
            .to_array()?;
        if rows.len() != 1 || rows[0].version != SCHEMA_VERSION {
            self.sql.exec(DROP_TABLES_SQL, None)?;
            self.sql.exec("DELETE FROM schema_version", None)?;
            self.sql.exec(
                "INSERT INTO schema_version (version) VALUES (?)",
                vec![SqlStorageValue::from(SCHEMA_VERSION)],
            )?;
        }
        self.sql.exec(CREATE_TABLES_SQL, None)?;
        self.schema_ready.set(true);
        Ok(())
    }

    fn payroll_synced(&self) -> Result<Vec<(String, String, i64)>> {
        let rows: Vec<SyncedRow> = self.sql.exec(PAYROLL_SYNCED_SQL, None)?.to_array()?;
        Ok(rows
            .into_iter()
            .map(|r| (r.scope, r.synced_at, r.row_count))
            .collect())
    }
}

/// Worker 側: リクエストを DO へ転送する。DO へのリクエストは method と URL だけから新しく組み立て、
/// 外から来たヘッダ・本文は 1 つも写さない。`email` (認可済み) があるときだけ内部ヘッダに載せる。
pub(crate) async fn forward(env: &Env, req: &Request, email: Option<&str>) -> Result<Response> {
    let stub = env.durable_object(STATE_BINDING)?.get_by_name(STATE_NAME)?;
    let headers = Headers::new();
    if let Some(email) = email {
        headers.set(EMAIL_HEADER, email)?;
    }
    let mut init = RequestInit::new();
    init.with_method(req.method()).with_headers(headers);
    let inner = Request::new_with_init(req.url()?.as_str(), &init)?;
    stub.fetch_with_request(inner).await
}
