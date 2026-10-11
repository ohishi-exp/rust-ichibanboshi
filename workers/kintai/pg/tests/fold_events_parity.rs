//! fold の読み (`FoldEventsIo`) の root (本番の Cloud Run 版の構成 = `HttpKintaiEventsRepo` が alc を reqwest で読み、fallback の
//! `PgKintaiEventsRepo` が relay の送った写し `kintai.kintai_events` を読んで合流する) と勤怠 Worker
//! (`kintai_logic::fold_events::WorkerFoldEvents`、auth-worker の RPC と kintai-pg の `FoldStore` の写しの読み) に、
//! **同じ写し (実 DB) と同じ上流の応答**を与えて、上流に投げた要求の並びと戻り値が一致することを確かめる (Refs #322)。
//!
//! 合流・ページング・指紋・入力の欠けの検知は両方とも共有 crate (`kintai_kosoku::alc_events`)、写しの SQL と行の形も共有 crate
//! (`kintai_kosoku::pushed_events`) を通るので、ここで突き合わせるのは差の出る所 — 往復の実装 (query の鍵と順・31 日 chunk ×
//! keyset のページング・404 / 非 2xx の扱い)、写しの bind (テナント・JST の期間・`PUSHED_SOURCES`)・行の読み取り、alc の行と写しの
//! 行の合流の順番、遡り起点。root 側の上流は同じプロセスの axum (tokio の上の HTTP)、Worker 側は fake の RPC。
//!
//! `KINTAI_TEST_DATABASE_URL` が無ければ**失敗する** (skip して緑にしない)。CI は worker-kintai.yml の `pg-parity` job。
//! この crate に置くのは、repo ルートの package を dev-dependency に持つのがここだけだから。

mod common;

use std::sync::{Arc, Mutex};

use alc_worker_db::PgClient;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use chrono::NaiveDate;
use common::{database_url, ensure_schema};
use kintai_kosoku::fold_flow::FoldEventsIo;
use kintai_logic::common::Fail;
use kintai_logic::fold_events::{AlcRpc, PushedEventsRead, WorkerFoldEvents};
use kintai_logic::unko_gaps::RpcResult;
use kintai_pg::FoldStore;
use rust_ichibanboshi::config::{KintaiEventsConfig, KintaiEventsSource};
use rust_ichibanboshi::kintai_http_repo::HttpKintaiEventsRepo;
use rust_ichibanboshi::kintai_pg_repo::PgKintaiEventsRepo;
use rust_ichibanboshi::kintai_push::{jst_at, KintaiPgStore};
use rust_ichibanboshi::kintai_repo::DynKintaiEventsRepo;
use serde_json::{json, Value};
use uuid::Uuid;

/// 上流の振る舞い。
#[derive(Debug, Clone, Copy, PartialEq)]
enum Mode {
    Normal,
    /// 生イベントがこの状態コードを返す。
    EventsStatus(u16),
    /// etags がこの状態コードを返す。
    EtagsStatus(u16),
}

/// 受けた要求 (`events` / `etags`、query の鍵と値の並び)。
type Seen = Vec<(&'static str, Vec<(String, String)>)>;

const HEADERS: [&str; 5] = [
    "運行NO",
    "対象乗務員CD",
    "開始日時",
    "終了日時",
    "イベント名",
];

fn op(unko_no: &str, rows: &[[&str; 4]]) -> Value {
    let rows: Vec<Vec<&str>> = rows
        .iter()
        .map(|[d, s, e, n]| vec![unko_no, *d, *s, *e, *n])
        .collect();
    json!({"unko_no": unko_no, "headers": HEADERS, "rows": rows})
}

fn query_value<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

/// 上流の応答 (両方の経路が同じものを受け取る)。2026-06 の読み `[06-01, 07-02)` は 2 日遡って 05-30..07-01 で、
/// 31 日で 2 chunk に割れる。1 本目の chunk は 2 ページ (2 名乗務の運行が両方のページに出る)。
fn upstream(mode: Mode, path: &str, q: &[(String, String)]) -> (u16, String) {
    if path == "events" {
        if let Mode::EventsStatus(s) = mode {
            return (s, "events down".to_string());
        }
        let page = match (
            query_value(q, "date_from"),
            query_value(q, "after_driver_cd"),
        ) {
            (Some("2026-05-30"), None) => json!({
                "drivers": [{"operations": [
                    op("26060108000000000011011", &[
                        ["1101", "2026/06/01 08:00:00", "", "運行開始"],
                        ["1101", "2026/06/01 12:00:00", "2026/06/01 13:00:00", "休憩"],
                    ]),
                    // 窓の前に始まり窓の中で終わる休息 (EVENTS_SQL の 4 番目のブランチ)
                    op("26053120000000000011011", &[
                        ["1101", "2026/05/31 20:00:00", "2026/06/01 06:00:00", "休息"],
                        ["1101", "2026/05/30 10:00:00", "", "運行開始"],
                    ]),
                ]}],
                "next_after_driver_cd": "1101",
                "warnings": ["NoSuchKey: 1/KUDGIVT.csv"],
            }),
            (Some("2026-05-30"), Some("1101")) => json!({
                "drivers": [{"operations": [
                    // 2 名乗務: 1 ページ目と同じ運行NO は数えない
                    op("26060108000000000011011", &[["1102", "2026/06/01 08:00:00", "", "運行開始"]]),
                    op("26061509000000000011022", &[["1102", "2026/06/15 09:00:00", "", "運行開始"]]),
                ]}],
                "next_after_driver_cd": null,
                "warnings": [],
            }),
            (Some("2026-06-30"), None) => json!({
                "drivers": [{"operations": [
                    op("26070108000000000011033", &[
                        ["1103", "2026/07/01 08:00:00", "", "運行開始"],
                        ["1103", "2026/07/02 08:00:00", "", "運行終了"],
                    ]),
                ]}],
                "warnings": ["NoSuchKey: 2/KUDGIVT.csv"],
            }),
            other => panic!("想定外の要求 {other:?}"),
        };
        return (200, page.to_string());
    }
    if let Mode::EtagsStatus(s) = mode {
        return (s, "etags down".to_string());
    }
    let body = json!({
        "items": [
            {"unko_no": "26060108000000000011011", "etag": "e1", "driver_cds": ["1101", "1102"]},
            {"unko_no": "26061509000000000011022", "etag": null, "driver_cds": ["1102"]},
            {"unko_no": "26052008000000000011044", "etag": "e4", "driver_cds": ["1104"]},
        ],
        "warnings": ["etags warning"],
        "unsplit": [{"unko_no": "26061509000000000011022", "driver_cd": "1102", "reading_date": "2026-06-16"}],
        "unsplit_total": 1,
    });
    (200, body.to_string())
}

// ── 写し (実 DB の kintai.kintai_events) ───────────────────────────────────

/// 写しの 1 行 (乗務員CD, JST の時刻, state, source, 運行NO)。
type CopyRow = (
    i64,
    &'static str,
    &'static str,
    &'static str,
    Option<&'static str>,
);

/// テナント T の写し。`timecard`・`dtako` に加えて、`PUSHED_SOURCES` 外の source (`alc_app`) と窓の外の行を混ぜる。
/// 1104 は月初より前に始まった運行の運行終了 (遡り起点 = 運行の開始)、1105 は前月の始業が開いたまま当月の最初が終業
/// (遡り起点 = その始業)、1101 は前月の始業・終業が閉じていて当月の最初が始業 (遡らない)。
const COPY: [CopyRow; 12] = [
    (1101, "2026-05-20 08:00:00", "始業", "timecard", None),
    (1101, "2026-05-20 18:00:00", "終業", "timecard", None),
    (1101, "2026-06-01 07:30:00", "始業", "timecard", None),
    (
        1101,
        "2026-06-01 08:00:00",
        "運行開始",
        "dtako",
        Some("26060108000000000011011"),
    ),
    (
        1101,
        "2026-06-01 12:00:00",
        "休息開始",
        "dtako",
        Some("26060108000000000011011"),
    ),
    (1102, "2026-06-02 07:00:00", "始業", "alc_app", None),
    (
        1102,
        "2026-06-15 18:00:00",
        "運行終了",
        "dtako",
        Some("26061509000000000011022"),
    ),
    (1103, "2026-07-02 08:00:00", "終業", "timecard", None),
    (
        1104,
        "2026-06-01 05:00:00",
        "運行終了",
        "dtako",
        Some("26053120000000000011044"),
    ),
    (1105, "2026-05-31 22:00:00", "始業", "timecard", None),
    (1105, "2026-06-01 06:00:00", "終業", "timecard", None),
    (1105, "2026-06-01 09:00:00", "始業", "timecard", None),
];

/// 別テナント U の写し (T の読みに混ざらないこと)。
const OTHER: [CopyRow; 2] = [
    (1106, "2026-06-03 07:00:00", "始業", "timecard", None),
    (
        1106,
        "2026-06-01 05:00:00",
        "運行終了",
        "dtako",
        Some("26053118000000000011066"),
    ),
];

async fn seed(pool: &sqlx::PgPool, tenant: Uuid, rows: &[CopyRow]) {
    for (cd, at, state, source, unko_no) in rows {
        sqlx::query(
            "INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source, unko_no) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(tenant)
        .bind(cd)
        .bind(jst_at(at).expect("occurred_at"))
        .bind(state)
        .bind(source)
        .bind(unko_no)
        .execute(pool)
        .await
        .expect("seed kintai_events");
    }
}

/// 写しを入れた DB と、root (sqlx の pool) / Worker (tokio-postgres の 1 接続) の両方の読み口。
struct Db {
    pool: sqlx::PgPool,
    tenant: Uuid,
}

async fn db() -> Db {
    let url = database_url();
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("sqlx connect");
    ensure_schema(&pool).await;
    let (tenant, other) = (Uuid::new_v4(), Uuid::new_v4());
    seed(&pool, tenant, &COPY).await;
    seed(&pool, other, &OTHER).await;
    Db { pool, tenant }
}

/// Worker の写しの読み (`FoldStore`、tokio-postgres の 1 接続)。
async fn worker_store(tenant: Uuid) -> FoldStore {
    let (client, conn) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    FoldStore::new(PgClient::new(client), tenant)
}

// ── root 側: 同じプロセスの axum を alc に見立て、fallback に写しの Pg を挿す (本番の Cloud Run 版の構成) ──

#[derive(Clone)]
struct Upstream {
    mode: Mode,
    seen: Arc<Mutex<Seen>>,
}

fn answer(up: &Upstream, path: &'static str, q: Vec<(String, String)>) -> (StatusCode, String) {
    let (status, body) = upstream(up.mode, path, &q);
    up.seen.lock().unwrap().push((path, q));
    (StatusCode::from_u16(status).unwrap(), body)
}

async fn events(
    State(up): State<Upstream>,
    Query(q): Query<Vec<(String, String)>>,
) -> (StatusCode, String) {
    answer(&up, "events", q)
}

async fn etags(
    State(up): State<Upstream>,
    Query(q): Query<Vec<(String, String)>>,
) -> (StatusCode, String) {
    answer(&up, "etags", q)
}

async fn root_repo(mode: Mode, db: &Db) -> (DynKintaiEventsRepo, Arc<Mutex<Seen>>) {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/api/dtako/events", get(events))
        .route("/api/dtako/events/etags", get(etags))
        .with_state(Upstream {
            mode,
            seen: seen.clone(),
        });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let cfg = KintaiEventsConfig {
        source: KintaiEventsSource::Http,
        base_url: format!("http://{addr}"),
        tenant_id: db.tenant.to_string(),
        timeout_secs: 10,
        auth_token: String::new(),
        auth_token_command: String::new(),
        auth_token_metadata: false,
        auth_token_ttl_secs: 900,
    };
    // 書き先の pin は GCP では空 (nil)。読みのテナントは `[kintai_events] tenant_id` (server.rs と同じ組み方)
    let store = Arc::new(KintaiPgStore::from_pool(db.pool.clone(), Uuid::nil()));
    let fb: DynKintaiEventsRepo = Arc::new(PgKintaiEventsRepo::new(store, db.tenant));
    let repo: DynKintaiEventsRepo = Arc::new(HttpKintaiEventsRepo::new(&cfg, Some(fb)).unwrap());
    (repo, seen)
}

// ── Worker 側: fake の RPC ─────────────────────────────────────────────────

struct FakeAlc {
    mode: Mode,
    seen: Mutex<Seen>,
}

fn pairs_of(search: &str) -> Vec<(String, String)> {
    search
        .split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap();
            (k.to_string(), v.to_string())
        })
        .collect()
}

impl FakeAlc {
    fn call(&self, path: &'static str, search: &str) -> RpcResult {
        let q = pairs_of(search);
        let (status, body) = upstream(self.mode, path, &q);
        self.seen.lock().unwrap().push((path, q));
        RpcResult { status, body }
    }
}

impl AlcRpc for FakeAlc {
    async fn dtako_events(&self, search: &str) -> Result<RpcResult, Fail> {
        Ok(self.call("events", search))
    }

    async fn dtako_etags(&self, search: &str) -> Result<RpcResult, Fail> {
        Ok(self.call("etags", search))
    }
}

fn fake_alc(mode: Mode) -> FakeAlc {
    FakeAlc {
        mode,
        seen: Mutex::new(Vec::new()),
    }
}

const TODAY: NaiveDate = NaiveDate::from_ymd_opt(2026, 10, 11).unwrap();
const FROM: &str = "2026-06-01 00:00:00";
const TO: &str = "2026-07-02 00:00:00";

// ── 突き合わせ ───────────────────────────────────────────────────────────

/// 全乗務員の行: 上流への要求の並び (chunk × ページ、query の鍵と順) と、alc の行と写しの行を合流した結果・warnings が一致する。
/// 写しから入るのは `PUSHED_SOURCES` (timecard・dtako) で窓の中の、テナント T の行だけ。
#[tokio::test]
async fn all_events_between_reads_the_copy_and_merges_like_root() {
    let db = db().await;
    let (root, root_seen) = root_repo(Mode::Normal, &db).await;
    let (root_rows, root_warnings) = FoldEventsIo::all_events_between(&*root, FROM, TO)
        .await
        .unwrap();

    let alc = fake_alc(Mode::Normal);
    let store = worker_store(db.tenant).await;
    let worker = WorkerFoldEvents {
        alc: &alc,
        pushed: &store,
        today: TODAY,
    };
    let (rows, warnings) = worker.all_events_between(FROM, TO).await.unwrap();

    let root_seen = root_seen.lock().unwrap().clone();
    assert_eq!(
        root_seen.len(),
        3,
        "2 chunk × (2 + 1) ページ: {root_seen:?}"
    );
    assert_eq!(*alc.seen.lock().unwrap(), root_seen);
    assert_eq!(rows, root_rows);
    assert_eq!(warnings, root_warnings);
    // 写しの行の形 (5 キー・end_datetime は null)
    let copy: Vec<&Value> = rows
        .iter()
        .filter(|r| r["source"] != "dtako_events")
        .collect();
    let got: Vec<(i64, &str, &str, &str)> = copy
        .iter()
        .map(|r| {
            assert_eq!(r.as_object().unwrap().len(), 5, "{r}");
            assert!(r["end_datetime"].is_null(), "{r}");
            (
                r["driver_id"].as_i64().unwrap(),
                r["datetime"].as_str().unwrap(),
                r["source"].as_str().unwrap(),
                r["state"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            (1101, "2026-06-01 07:30:00", "timecard", "始業"),
            (1101, "2026-06-01 08:00:00", "dtako", "運行開始"),
            (1101, "2026-06-01 12:00:00", "dtako", "休息開始"),
            (1102, "2026-06-15 18:00:00", "dtako", "運行終了"),
            (1104, "2026-06-01 05:00:00", "dtako", "運行終了"),
            (1105, "2026-06-01 06:00:00", "timecard", "終業"),
            (1105, "2026-06-01 09:00:00", "timecard", "始業"),
        ],
        "alc_app・窓の外・別テナントの行は入らない"
    );
    assert_eq!(warnings.len(), 2);
}

/// 遡り起点: 写しの運行終了 (月初より前に始まった運行) と打刻の姿から、root と同じ起点になる。別テナントは混ざらない。
#[tokio::test]
async fn month_head_anchors_read_the_copy_like_root() {
    let db = db().await;
    let (root, root_seen) = root_repo(Mode::Normal, &db).await;
    let want = FoldEventsIo::month_head_anchors(&*root, FROM, TO)
        .await
        .unwrap();

    let alc = fake_alc(Mode::Normal);
    let store = worker_store(db.tenant).await;
    let worker = WorkerFoldEvents {
        alc: &alc,
        pushed: &store,
        today: TODAY,
    };
    let got = worker.month_head_anchors(FROM, TO).await.unwrap();

    assert_eq!(got, want);
    let pairs: Vec<(u64, &str)> = got.iter().map(|(k, v)| (*k, v.as_str())).collect();
    assert_eq!(
        pairs,
        vec![(1104, "2026-05-31 20:00:00"), (1105, "2026-05-31 22:00:00")]
    );
    assert!(root_seen.lock().unwrap().is_empty(), "alc は読まない");
    assert!(alc.seen.lock().unwrap().is_empty(), "alc は読まない");
}

/// dtako の指紋: etags の期間 (遡り起点まで下げる)・指紋・控え・warnings の分類が一致する (写しは読まない)。
#[tokio::test]
async fn dtako_month_read_matches_root() {
    let db = db().await;
    let since = Some(NaiveDate::from_ymd_opt(2026, 5, 20).unwrap());
    let (root, root_seen) = root_repo(Mode::Normal, &db).await;
    let want = FoldEventsIo::dtako_month_read_since(&*root, "2026-06", since)
        .await
        .unwrap();

    let alc = fake_alc(Mode::Normal);
    let store = worker_store(db.tenant).await;
    let worker = WorkerFoldEvents {
        alc: &alc,
        pushed: &store,
        today: TODAY,
    };
    let got = worker
        .dtako_month_read_since("2026-06", since)
        .await
        .unwrap();

    assert_eq!(*alc.seen.lock().unwrap(), *root_seen.lock().unwrap());
    let q = &alc.seen.lock().unwrap()[0].1;
    assert_eq!(query_value(q, "date_from"), Some("2026-05-20"));
    assert_eq!(query_value(q, "date_to"), Some("2026-07-01"));
    assert_eq!(got, want);
    assert!(got.digest.is_some());
    let kinds: Vec<bool> = got.warnings.iter().map(|w| w.blocks_seal).collect();
    assert_eq!(kinds, vec![true, true, false], "{:?}", got.warnings);
}

/// 404: 生イベントは両方とも失敗 (口が無いのは「読めない」)、etags は両方とも「口なし」(指紋 `None`)。
#[tokio::test]
async fn not_found_is_handled_like_root() {
    let db = db().await;
    let store = worker_store(db.tenant).await;
    let (root, _) = root_repo(Mode::EventsStatus(404), &db).await;
    let root_err = FoldEventsIo::all_events_between(&*root, FROM, TO)
        .await
        .unwrap_err();
    let alc = fake_alc(Mode::EventsStatus(404));
    let worker = WorkerFoldEvents {
        alc: &alc,
        pushed: &store,
        today: TODAY,
    };
    let err = worker.all_events_between(FROM, TO).await.unwrap_err();
    assert!(
        root_err.to_string().contains("alc dtako-events status 404"),
        "{root_err}"
    );
    assert!(
        err.body
            .starts_with("alc dtako-events status 404: events down"),
        "{err:?}"
    );
    assert_eq!(err.status, 502);

    let (root, _) = root_repo(Mode::EtagsStatus(404), &db).await;
    let want = FoldEventsIo::dtako_month_read_since(&*root, "2026-06", None)
        .await
        .unwrap();
    let alc = fake_alc(Mode::EtagsStatus(404));
    let worker = WorkerFoldEvents {
        alc: &alc,
        pushed: &store,
        today: TODAY,
    };
    let got = worker
        .dtako_month_read_since("2026-06", None)
        .await
        .unwrap();
    assert_eq!(got, want);
    assert_eq!(got.digest, None);
}

/// 404 以外の非 2xx: どちらの口も両方とも失敗 (`None` に潰さない)。
#[tokio::test]
async fn other_non_2xx_fail_like_root() {
    let db = db().await;
    let store = worker_store(db.tenant).await;
    for (mode, word) in [
        (Mode::EventsStatus(500), "alc dtako-events status 500"),
        (Mode::EtagsStatus(503), "alc dtako-etags status 503"),
    ] {
        let (root, _) = root_repo(mode, &db).await;
        let alc = fake_alc(mode);
        let worker = WorkerFoldEvents {
            alc: &alc,
            pushed: &store,
            today: TODAY,
        };
        let (root_err, err) = match mode {
            Mode::EventsStatus(_) => (
                FoldEventsIo::all_events_between(&*root, FROM, TO)
                    .await
                    .unwrap_err()
                    .to_string(),
                worker.all_events_between(FROM, TO).await.unwrap_err(),
            ),
            _ => (
                FoldEventsIo::dtako_month_read_since(&*root, "2026-06", None)
                    .await
                    .unwrap_err()
                    .to_string(),
                worker
                    .dtako_month_read_since("2026-06", None)
                    .await
                    .unwrap_err(),
            ),
        };
        assert!(root_err.contains(word), "{root_err}");
        assert!(err.body.starts_with(word), "{err:?}");
        assert_eq!(err.status, 502);
    }
}

/// 写しの読みの DB の失敗は 502 で、本文は頭 (root の `kintai_events read failed`) と種別だけ (DB の message・接続先を出さない)。
#[tokio::test]
async fn db_failures_carry_only_the_kind() {
    let (client, conn) = tokio_postgres::connect(&database_url(), tokio_postgres::NoTls)
        .await
        .expect("tokio-postgres connect");
    tokio::spawn(conn).abort();
    let store = FoldStore::new(PgClient::new(client), Uuid::new_v4());
    let span = kintai_logic::fold_events::pushed_window(FROM, TO).unwrap();
    let want = Fail::new(502, "kintai_events read failed: closed");
    assert_eq!(store.pushed_all_events(span).await, Err(want.clone()));
    assert_eq!(store.pushed_head_run_ends(span).await, Err(want.clone()));
    assert_eq!(store.pushed_head_punches(span).await, Err(want));
}
