//! fold の読み (`FoldEventsIo`) の root (Cloud Run 版、`HttpKintaiEventsRepo` が alc を reqwest で読み MariaDB の fallback を
//! 合流する) と勤怠 Worker (`kintai_logic::fold_events::WorkerFoldEvents`、auth-worker の RPC と MariaDB の往復) に、
//! **同じ上流の応答**を与えて、上流に投げた要求の並びと戻り値が一致することを確かめる (Refs #322)。DB は要らない。
//!
//! 合流・ページング・指紋・入力の欠けの検知は両方とも共有 crate (`kintai_kosoku::alc_events`) を通るので、ここで突き合わせる
//! のは差の出る所 — 往復の実装 (query の鍵と順・31 日 chunk × keyset のページング・404 / 非 2xx の扱い) と、
//! alc の行と MariaDB の行の合流の順番。root 側の上流は同じプロセスの axum (tokio の上の HTTP)、Worker 側は fake の RPC。
//!
//! この crate に置くのは、repo ルートの package を dev-dependency に持つのがここだけだから。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::routing::get;
use axum::Router;
use chrono::NaiveDate;
use kintai_kosoku::fold_flow::FoldEventsIo;
use kintai_logic::common::Fail;
use kintai_logic::fold_events::{AlcRpc, MariadbRun, WorkerFoldEvents};
use kintai_logic::unko_gaps::RpcResult;
use kintai_mysql::response::Row;
use rust_ichibanboshi::config::{KintaiEventsConfig, KintaiEventsSource};
use rust_ichibanboshi::kintai_http_repo::HttpKintaiEventsRepo;
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use serde_json::{json, Value};

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

// ── root 側: 同じプロセスの axum を alc に見立てる ─────────────────────────

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

/// root の fallback (MariaDB) の代役。全乗務員の行だけを返す (遡り起点は既定の空)。
struct Fallback;

/// MariaDB の `ALL_EVENTS_SQL` が返す行 (3 表の UNION。`dtako_events` も混ざる)。
fn mariadb_rows() -> Vec<[Option<&'static str>; 5]> {
    vec![
        [
            Some("2026-06-01 07:30:00"),
            None,
            Some("1101"),
            Some("timecard"),
            Some("始業"),
        ],
        [
            Some("2026-06-01 08:00:00"),
            None,
            Some("1101"),
            Some("dtako_events"),
            Some("運行開始"),
        ],
        [
            Some("2026-06-15 18:00:00"),
            None,
            Some("1102"),
            Some("dtako"),
            Some("運行終了"),
        ],
        [
            Some("2026-06-02 07:00:00"),
            None,
            None,
            Some("timecard"),
            Some("始業"),
        ],
    ]
}

#[async_trait]
impl KintaiEventsApi for Fallback {
    async fn fetch_events_between(
        &self,
        _: &str,
        _: &str,
        _: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(Vec::new())
    }

    async fn fetch_all_events_between(
        &self,
        _: &str,
        _: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        let row = |r: [Option<&str>; 5]| {
            let driver = r[2].map(|d| d.parse::<i64>().unwrap());
            json!({"datetime": r[0], "end_datetime": r[1], "driver_id": driver, "source": r[3], "state": r[4]})
        };
        Ok(mariadb_rows().into_iter().map(row).collect())
    }

    async fn fetch_ferry_between(
        &self,
        _: &str,
        _: &str,
        _: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(Vec::new())
    }
}

async fn root_repo(mode: Mode) -> (DynKintaiEventsRepo, Arc<Mutex<Seen>>) {
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
        tenant_id: "parity-test-tenant".to_string(),
        timeout_secs: 10,
        auth_token: String::new(),
        auth_token_command: String::new(),
        auth_token_metadata: false,
        auth_token_ttl_secs: 900,
    };
    let fb: DynKintaiEventsRepo = Arc::new(Fallback);
    let repo: DynKintaiEventsRepo = Arc::new(HttpKintaiEventsRepo::new(&cfg, Some(fb)).unwrap());
    (repo, seen)
}

// ── Worker 側: fake の RPC と MariaDB ───────────────────────────────────────

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

struct FakeMariadb;

impl MariadbRun for FakeMariadb {
    async fn run(&self, sqls: &[String]) -> Result<Vec<Vec<Row>>, Fail> {
        assert_eq!(sqls.len(), 1, "全乗務員の行の 1 本だけ");
        let rows = mariadb_rows()
            .into_iter()
            .map(|r| r.iter().map(|c| c.map(|s| s.as_bytes().to_vec())).collect())
            .collect();
        Ok(vec![rows])
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

/// 全乗務員の行: 上流への要求の並び (chunk × ページ、query の鍵と順) と、合流した行・warnings が一致する。
#[tokio::test]
async fn all_events_between_reads_and_merges_like_root() {
    let (root, root_seen) = root_repo(Mode::Normal).await;
    let (root_rows, root_warnings) = FoldEventsIo::all_events_between(&*root, FROM, TO)
        .await
        .unwrap();

    let alc = fake_alc(Mode::Normal);
    let worker = WorkerFoldEvents {
        alc: &alc,
        mariadb: &FakeMariadb,
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
    // 合流の中身: MariaDB の dtako_events は借りない・driver_id の NULL は先頭・窓の外 (07-02 08:00) は落ちる
    let sources: Vec<(Option<i64>, &str)> = rows
        .iter()
        .map(|r| (r["driver_id"].as_i64(), r["source"].as_str().unwrap()))
        .collect();
    assert_eq!(
        sources,
        vec![
            (None, "timecard"),
            (Some(1101), "dtako_events"),
            (Some(1101), "timecard"),
            (Some(1101), "dtako_events"),
            (Some(1101), "dtako_events"),
            (Some(1102), "dtako_events"),
            (Some(1102), "dtako"),
            (Some(1103), "dtako_events"),
        ]
    );
    assert_eq!(warnings.len(), 2);
}

/// dtako の指紋: etags の期間 (遡り起点まで下げる)・指紋・控え・warnings の分類が一致する。
#[tokio::test]
async fn dtako_month_read_matches_root() {
    let since = Some(NaiveDate::from_ymd_opt(2026, 5, 20).unwrap());
    let (root, root_seen) = root_repo(Mode::Normal).await;
    let want = FoldEventsIo::dtako_month_read_since(&*root, "2026-06", since)
        .await
        .unwrap();

    let alc = fake_alc(Mode::Normal);
    let worker = WorkerFoldEvents {
        alc: &alc,
        mariadb: &FakeMariadb,
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
    let (root, _) = root_repo(Mode::EventsStatus(404)).await;
    let root_err = FoldEventsIo::all_events_between(&*root, FROM, TO)
        .await
        .unwrap_err();
    let alc = fake_alc(Mode::EventsStatus(404));
    let worker = WorkerFoldEvents {
        alc: &alc,
        mariadb: &FakeMariadb,
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

    let (root, _) = root_repo(Mode::EtagsStatus(404)).await;
    let want = FoldEventsIo::dtako_month_read_since(&*root, "2026-06", None)
        .await
        .unwrap();
    let alc = fake_alc(Mode::EtagsStatus(404));
    let worker = WorkerFoldEvents {
        alc: &alc,
        mariadb: &FakeMariadb,
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
    for (mode, word) in [
        (Mode::EventsStatus(500), "alc dtako-events status 500"),
        (Mode::EtagsStatus(503), "alc dtako-etags status 503"),
    ] {
        let (root, _) = root_repo(mode).await;
        let alc = fake_alc(mode);
        let worker = WorkerFoldEvents {
            alc: &alc,
            mariadb: &FakeMariadb,
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
