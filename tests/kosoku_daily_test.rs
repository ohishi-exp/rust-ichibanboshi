//! /api/kintai/kosoku-daily のテスト (Refs #118、拘束時間の打刻基準化 Phase 2)。
//!
//! `KintaiEventsApi` の mock を挿して **DB 無しで** route の振る舞いを固定する。
//! 畳み込みの規則そのものは `src/kosoku.rs` の unit test 側で網羅しているので、
//! ここで見るのは「route が期間を正しく組み、純粋ロジックへ渡し、応答の形を
//! 保つか」— つまり配線と検証と失敗の写し方。

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum::{Extension, Router};
use rust_ichibanboshi::kintai_repo::{
    DisabledKintaiEventsRepo, DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError,
};
use rust_ichibanboshi::kosoku::KosokuParams;
use rust_ichibanboshi::routes;
use serde_json::{json, Value};
use tower::ServiceExt;

struct MockRepo {
    rows: Vec<Value>,
    /// フェリー区間 (Refs #146)
    ferry: Vec<Value>,
    fail: Option<KintaiRepoError>,
    /// 1 名分の呼び出し (`from`, `to`, 乗務員CD)
    calls: Mutex<Vec<(String, String, u64)>>,
    /// 全乗務員の呼び出し (`from`, `to`)
    all_calls: Mutex<Vec<(String, String)>>,
    /// フェリーの呼び出し (`from`, `to`, 乗務員CD)
    ferry_calls: Mutex<Vec<(String, String, Option<u64>)>>,
}

impl MockRepo {
    fn with_rows(rows: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            rows,
            ferry: Vec::new(),
            fail: None,
            calls: Mutex::new(Vec::new()),
            all_calls: Mutex::new(Vec::new()),
            ferry_calls: Mutex::new(Vec::new()),
        })
    }

    /// フェリー区間つき (Refs #146)。控除は拘束に混ぜないことを見るため。
    fn with_ferry(rows: Vec<Value>, ferry: Vec<Value>) -> Arc<Self> {
        Arc::new(Self {
            rows,
            ferry,
            fail: None,
            calls: Mutex::new(Vec::new()),
            all_calls: Mutex::new(Vec::new()),
            ferry_calls: Mutex::new(Vec::new()),
        })
    }

    fn failing(e: KintaiRepoError) -> Arc<Self> {
        Arc::new(Self {
            rows: Vec::new(),
            ferry: Vec::new(),
            fail: Some(e),
            calls: Mutex::new(Vec::new()),
            all_calls: Mutex::new(Vec::new()),
            ferry_calls: Mutex::new(Vec::new()),
        })
    }

    fn result(&self) -> Result<Vec<Value>, KintaiRepoError> {
        match &self.fail {
            Some(KintaiRepoError::NotConfigured) => Err(KintaiRepoError::NotConfigured),
            Some(KintaiRepoError::QueryFailed(m)) => Err(KintaiRepoError::QueryFailed(m.clone())),
            None => Ok(self.rows.clone()),
        }
    }
}

#[async_trait]
impl KintaiEventsApi for MockRepo {
    async fn fetch_events_between(
        &self,
        from: &str,
        to: &str,
        driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.calls
            .lock()
            .unwrap()
            .push((from.to_string(), to.to_string(), driver));
        self.result()
    }

    async fn fetch_all_events_between(
        &self,
        from: &str,
        to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.all_calls
            .lock()
            .unwrap()
            .push((from.to_string(), to.to_string()));
        self.result()
    }

    async fn fetch_ferry_between(
        &self,
        from: &str,
        to: &str,
        driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        self.ferry_calls
            .lock()
            .unwrap()
            .push((from.to_string(), to.to_string(), driver));
        match &self.fail {
            Some(KintaiRepoError::NotConfigured) => Err(KintaiRepoError::NotConfigured),
            Some(KintaiRepoError::QueryFailed(m)) => Err(KintaiRepoError::QueryFailed(m.clone())),
            None => Ok(self.ferry.clone()),
        }
    }
}

fn app(repo: DynKintaiEventsRepo) -> Router {
    Router::new()
        .route(
            "/api/kintai/kosoku-daily",
            get(routes::kintai::kosoku_daily),
        )
        .layer(Extension(repo))
        .layer(Extension(Arc::new(KosokuParams::default())))
}

async fn call(app: Router, uri: &str) -> (StatusCode, String) {
    let res = app
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = res.status();
    let bytes = axum::body::to_bytes(res.into_body(), 1_000_000)
        .await
        .unwrap();
    (status, String::from_utf8(bytes.to_vec()).unwrap())
}

/// rows を仕込んで 1 回叩き、JSON を返す。
async fn serve(rows: Vec<Value>, uri: &str) -> (StatusCode, Value) {
    let (status, body) = call(app(MockRepo::with_rows(rows)), uri).await;
    let json = serde_json::from_str(&body).unwrap_or(Value::Null);
    (status, json)
}

fn tc(datetime: &str, state: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "source": "timecard", "state": state})
}

fn ev(start: &str, end: &str, state: &str) -> Value {
    json!({"datetime": start, "end_datetime": end, "source": "dtako_events", "state": state})
}

fn dtako(datetime: &str, state: &str, unko: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "source": "dtako",
           "state": state, "unko_no": unko})
}

// --- 期間の組み立て ---

#[tokio::test]
async fn uses_the_plain_month_range() {
    let repo = MockRepo::with_rows(vec![]);
    let (status, _) = call(
        app(repo.clone()),
        "/api/kintai/kosoku-daily?month=2026-07&driver=0012",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    // `/events` と同じ範囲でよい — 月をまたぐ休息は SQL 側が
    // 「期間内に終わる区間」として拾うので、route が遡る日数を決め打ちしない
    assert_eq!(
        *repo.calls.lock().unwrap(),
        vec![(
            "2026-07-01 00:00:00".to_string(),
            "2026-08-02 00:00:00".to_string(),
            12
        )]
    );
}

#[tokio::test]
async fn month_range_rolls_over_year() {
    let repo = MockRepo::with_rows(vec![]);
    let (status, _) = call(
        app(repo.clone()),
        "/api/kintai/kosoku-daily?month=2026-12&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(repo.calls.lock().unwrap()[0].1, "2027-01-02 00:00:00");
}

#[tokio::test]
async fn long_rest_starting_before_the_month_still_yields_day_one() {
    // 車輌故障で 1 週間以上止まった想定 — 5/20 に始まり 6/1 に終わる休息。
    // SQL が「期間内に終わる区間」を拾えていれば 6/1 の勤務が組める
    let (status, body) = serve(
        vec![
            ev("2026-05-20 18:00:00", "2026-06-01 06:58:00", "休息"),
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-01");
    assert_eq!(days[0]["start"], "2026-06-01 06:58:00");
    assert_eq!(days[0]["restraint_minutes"], 561);
}

// --- 打刻のみ (日帰り) ---

#[tokio::test]
async fn timecard_only_shift() {
    let (status, body) = serve(
        vec![
            tc("2026-06-02 09:25:00", "始業"),
            ev("2026-06-02 12:00:00", "2026-06-02 13:36:00", "休憩"),
            tc("2026-06-02 19:39:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1018",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["month"], "2026-06");
    assert_eq!(body["driver"], 1018);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-02");
    assert_eq!(days[0]["source"], "timecard");
    assert_eq!(days[0]["restraint_minutes"], 614);
    assert_eq!(days[0]["break_minutes"], 96);
    assert_eq!(days[0]["working_minutes"], 518);
    assert_eq!(days[0]["over_24h"], false);
}

// --- 運行のみ (打刻欠け → 休息で切る) ---

#[tokio::test]
async fn rest_only_shift_when_timecard_is_missing() {
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            dtako("2026-06-02 06:00:00", "運行開始", "A"),
            dtako("2026-06-02 14:10:00", "運行終了", "A"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["source"], "rest");
    assert_eq!(days[0]["start"], "2026-06-02 04:42:00");
    assert_eq!(days[0]["end"], "2026-06-02 16:18:00");
    assert_eq!(days[0]["restraint_minutes"], 696);
}

// --- 同日 2 運行 ---

#[tokio::test]
async fn two_runs_same_day_without_rest_stay_one_shift() {
    // 運行終了 → 8 分後に運行開始。運行では切らないので 1 勤務のまま
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            dtako("2026-06-02 06:00:00", "運行開始", "A"),
            dtako("2026-06-02 13:47:00", "運行終了", "A"),
            dtako("2026-06-02 13:55:00", "運行開始", "B"),
            dtako("2026-06-02 15:00:00", "運行終了", "B"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["restraint_minutes"], 696);
}

#[tokio::test]
async fn two_runs_same_day_with_rest_between_are_two_shifts() {
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 10:00:00", "2026-06-02 20:00:00", "休息"),
            ev("2026-06-02 23:00:00", "2026-06-03 09:00:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 2);
    assert_eq!(days[0]["start"], "2026-06-02 04:42:00");
    assert_eq!(days[0]["end"], "2026-06-02 10:00:00");
    assert_eq!(days[1]["start"], "2026-06-02 20:00:00");
    assert_eq!(days[1]["end"], "2026-06-02 23:00:00");
}

// --- 日跨ぎ ---

#[tokio::test]
async fn overnight_shift_is_filed_under_start_date() {
    let (status, body) = serve(
        vec![
            tc("2026-06-30 22:00:00", "始業"),
            tc("2026-07-01 08:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-30");
    assert_eq!(days[0]["restraint_minutes"], 600);
    // 昼の窓に掛からない勤務なので、休憩 60 分がまん中 (02:30-03:30、深夜帯) に入る。
    // 深夜は 22:00〜05:00 の 420 分から休憩ぶんを引いた 360 分
    assert_eq!(days[0]["night_minutes"], 360);
}

#[tokio::test]
async fn previous_month_shift_is_not_returned() {
    // 遡って読んだ前月分は当月の応答に混ぜない
    let (status, body) = serve(
        vec![
            tc("2026-05-28 09:00:00", "始業"),
            tc("2026-05-28 18:00:00", "終業"),
            tc("2026-06-01 09:00:00", "始業"),
            tc("2026-06-01 18:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let days = body["days"].as_array().unwrap();
    assert_eq!(days.len(), 1);
    assert_eq!(days[0]["date"], "2026-06-01");
}

// --- 法定休日 / 24 時間超 ---

#[tokio::test]
async fn sunday_is_legal_holiday() {
    let (status, body) = serve(
        vec![
            tc("2026-06-07 08:00:00", "始業"), // 日曜
            tc("2026-06-07 20:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["is_legal_holiday"], true);
    // 運行に出ていない勤務なので昼休憩 60 分が引かれる (720 - 60)
    assert_eq!(d["legal_holiday_minutes"], 660);
    assert_eq!(d["overtime_minutes"], 0);
}

#[tokio::test]
async fn over_24h_is_flagged_but_not_capped() {
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-03 20:08:00", "終業"), // 38.1 時間
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["over_24h"], true);
    // **打ち切らない** (Refs #152) — 旗は立てるが値は実測のまま
    assert_eq!(d["restraint_minutes"], 2288);
}

// --- driver 省略 = 全乗務員 (Refs #125) ---

/// 乗務員CD 付きの打刻行 (一括読みは `driver_id` を持つ)。
fn tc_of(driver: i64, datetime: &str, state: &str) -> Value {
    json!({"datetime": datetime, "end_datetime": null, "driver_id": driver,
           "source": "timecard", "state": state})
}

#[tokio::test]
async fn omitting_driver_returns_every_driver() {
    let (status, body) = serve(
        vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1018, "2026-06-02 09:25:00", "始業"),
            tc_of(1018, "2026-06-02 19:39:00", "終業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["month"], "2026-06");
    // 1 名指定の形 (`driver` / `days`) は出さない
    assert!(body.get("driver").is_none());
    assert!(body.get("days").is_none());
    let drivers = body["drivers"].as_array().unwrap();
    assert_eq!(drivers.len(), 2);
    // 乗務員CD 昇順
    assert_eq!(drivers[0]["driver"], 1018);
    assert_eq!(drivers[1]["driver"], 1119);
    // 乗務員ごとに畳む — 混ぜたまま畳むと 06:00〜19:39 の 1 勤務になる
    assert_eq!(drivers[0]["days"][0]["restraint_minutes"], 614);
    assert_eq!(drivers[1]["days"][0]["restraint_minutes"], 720);
}

#[tokio::test]
async fn bulk_uses_the_same_month_range_and_reads_once() {
    let repo = MockRepo::with_rows(vec![]);
    let (status, _) = call(app(repo.clone()), "/api/kintai/kosoku-daily?month=2026-12").await;
    assert_eq!(status, StatusCode::OK);
    // 一括の読みは 1 回だけ (96 名を 1 名ずつ叩かない)
    assert_eq!(
        *repo.all_calls.lock().unwrap(),
        vec![(
            "2026-12-01 00:00:00".to_string(),
            "2027-01-02 00:00:00".to_string()
        )]
    );
    assert!(repo.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bulk_drops_drivers_without_any_shift_or_punch() {
    // 勤務も打刻も無い乗務員は並べない。**打刻だけの乗務員は並べる** — 対になる終業が
    // 無い始業も表に出すため (Refs #137)
    let (status, body) = serve(
        vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
            tc_of(1442, "2026-06-02 06:00:00", "始業"),
            json!({"datetime": "2026-06-02 08:00:00", "end_datetime": null, "driver_id": 1500,
                   "source": "dtako", "state": "運行開始"}),
        ],
        "/api/kintai/kosoku-daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let drivers = body["drivers"].as_array().unwrap();
    assert_eq!(drivers.len(), 2);
    assert_eq!(drivers[0]["driver"], 1119);
    assert_eq!(drivers[0]["punches"].as_array().unwrap().len(), 2);
    // 勤務は組めないが打刻はある乗務員
    assert_eq!(drivers[1]["driver"], 1442);
    assert_eq!(drivers[1]["days"].as_array().unwrap().len(), 0);
    assert_eq!(drivers[1]["punches"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn bulk_with_no_events_is_empty_drivers_not_error() {
    let (status, body) = serve(vec![], "/api/kintai/kosoku-daily?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["drivers"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn bulk_failures_map_the_same_way() {
    let (status, body) = call(
        app(MockRepo::failing(KintaiRepoError::QueryFailed(
            "boom".into(),
        ))),
        "/api/kintai/kosoku-daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("boom"));

    let (status, body) = call(
        app(Arc::new(DisabledKintaiEventsRepo)),
        "/api/kintai/kosoku-daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"));
}

// --- 検証 ---

#[tokio::test]
async fn rejects_bad_month() {
    for uri in [
        "/api/kintai/kosoku-daily?driver=1051",
        "/api/kintai/kosoku-daily?month=&driver=1051",
        "/api/kintai/kosoku-daily?month=2026-13&driver=1051",
        "/api/kintai/kosoku-daily?month=2026/06&driver=1051",
    ] {
        let repo = MockRepo::with_rows(vec![]);
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body.contains("YYYY-MM"));
        // 検証で弾いた分は DB を叩かない (1 名分も一括も)
        assert!(repo.calls.lock().unwrap().is_empty());
        assert!(repo.all_calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn rejects_bad_driver() {
    for uri in [
        // 空は「省略」ではなく不正 — front の入れ忘れで 96 名ぶんを返さない
        "/api/kintai/kosoku-daily?month=2026-06&driver=",
        "/api/kintai/kosoku-daily?month=2026-06&driver=10a1",
        "/api/kintai/kosoku-daily?month=2026-06&driver=-1",
    ] {
        let repo = MockRepo::with_rows(vec![]);
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{uri}");
        assert!(body.contains("乗務員CD"));
        assert!(repo.calls.lock().unwrap().is_empty());
    }
}

// --- 失敗の写し方 ---

#[tokio::test]
async fn query_failure_is_502() {
    let (status, body) = call(
        app(MockRepo::failing(KintaiRepoError::QueryFailed(
            "boom".into(),
        ))),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY);
    assert!(body.contains("boom"));
}

#[tokio::test]
async fn not_configured_is_503() {
    let (status, body) = call(
        app(MockRepo::failing(KintaiRepoError::NotConfigured)),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"));
}

#[tokio::test]
async fn disabled_repo_is_503() {
    // `[mariadb]` 未設定で起動したときと同じ状態 — 空配列で「0 件」に見せない
    let (status, body) = call(
        app(Arc::new(DisabledKintaiEventsRepo)),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1051",
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("未設定"));
}

#[tokio::test]
async fn no_events_is_empty_days_not_error() {
    let (status, body) = serve(vec![], "/api/kintai/kosoku-daily?month=2026-06&driver=1051").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["days"].as_array().unwrap().len(), 0);
}

// ---- フェリー控除 (Refs #146) ----
//
// 紙のタイムカード表が拘束から引いている分。**こちらの拘束には入れない** — 突合で
// 差の原因を説明するためだけに載せる。取れなくても日別サマリは返す。

fn ferry_row(start: &str, end: &str, driver: u64) -> Value {
    serde_json::json!({"start_datetime": start, "end_datetime": end, "driver_id": driver})
}

#[tokio::test]
async fn kosoku_daily_puts_ferry_minus_on_the_day_without_touching_restraint() {
    let repo = MockRepo::with_ferry(
        vec![
            tc_of(1726, "2026-06-02 09:00:00", "始業"),
            tc_of(1726, "2026-06-02 18:00:00", "終業"),
        ],
        vec![ferry_row(
            "2026-06-02 10:00:00",
            "2026-06-02 11:18:56",
            1726,
        )],
    );
    let (status, body) = call(
        app(repo.clone()),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1726",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    let day = &v["days"][0];
    assert_eq!(day["ferry_minus_minutes"], 78);
    // 拘束は控除の影響を受けない
    assert_eq!(day["restraint_minutes"], 540);

    // フェリーは**その月ちょうど**で引く (イベント側の翌月+1日とは違う)
    let calls = repo.ferry_calls.lock().unwrap();
    assert_eq!(
        calls[0],
        (
            "2026-06-01 00:00:00".to_string(),
            "2026-07-01 00:00:00".to_string(),
            Some(1726)
        )
    );
}

#[tokio::test]
async fn kosoku_daily_all_drivers_splits_ferry_per_driver() {
    let repo = MockRepo::with_ferry(
        vec![
            tc_of(1726, "2026-06-02 09:00:00", "始業"),
            tc_of(1726, "2026-06-02 18:00:00", "終業"),
            tc_of(1021, "2026-06-02 09:00:00", "始業"),
            tc_of(1021, "2026-06-02 18:00:00", "終業"),
        ],
        vec![
            ferry_row("2026-06-02 10:00:00", "2026-06-02 11:00:00", 1726),
            ferry_row("2026-06-02 10:00:00", "2026-06-02 10:30:00", 1021),
        ],
    );
    let (status, body) = call(app(repo.clone()), "/api/kintai/kosoku-daily?month=2026-06").await;
    assert_eq!(status, StatusCode::OK);
    let v: Value = serde_json::from_str(&body).unwrap();
    let of = |cd: u64| -> i64 {
        v["drivers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["driver"] == cd)
            .unwrap()["days"][0]["ferry_minus_minutes"]
            .as_i64()
            .unwrap()
    };
    // 他人の控除を混ぜない
    assert_eq!(of(1726), 60);
    assert_eq!(of(1021), 30);
    // 全乗務員は 1 回で引く (driver 指定なし)
    let calls = repo.ferry_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].2, None);
}

#[tokio::test]
async fn kosoku_daily_survives_a_ferry_failure() {
    // フェリーは突合の付帯情報。取れなくても日別サマリは返す (控除 0)
    struct FerryFails(Vec<Value>);
    #[async_trait]
    impl KintaiEventsApi for FerryFails {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, KintaiRepoError> {
            Ok(self.0.clone())
        }
        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, KintaiRepoError> {
            Ok(self.0.clone())
        }
        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, KintaiRepoError> {
            Err(KintaiRepoError::QueryFailed("ferry table gone".into()))
        }
    }
    let rows = vec![
        tc_of(1726, "2026-06-02 09:00:00", "始業"),
        tc_of(1726, "2026-06-02 18:00:00", "終業"),
    ];
    let repo: DynKintaiEventsRepo = Arc::new(FerryFails(rows));

    for uri in [
        "/api/kintai/kosoku-daily?month=2026-06&driver=1726",
        "/api/kintai/kosoku-daily?month=2026-06",
    ] {
        let (status, body) = call(app(repo.clone()), uri).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        assert!(body.contains("\"ferry_minus_minutes\":0"), "{uri}");
        assert!(body.contains("\"restraint_minutes\":540"), "{uri}");
    }
}

// --- view=compare (Refs #157) ---

#[tokio::test]
async fn compare_view_returns_only_what_the_diff_needs() {
    // 突合が使うのは日付・拘束・フェリー控除だけ。19 キーのうち 16 キーは捨てられていた
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    let keys: Vec<&str> = d.as_object().unwrap().keys().map(|k| k.as_str()).collect();
    assert_eq!(
        keys,
        vec!["date", "lunch_overlap_minutes", "restraint_minutes"]
    );
    assert_eq!(d["restraint_minutes"], 540);
    // 打刻は突合で使わないので付けない
    assert!(body.get("punches").is_none());
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_rest_deduction() {
    // 休息を外した日だけ rest_minus_minutes を載せる (Refs nuxt-dtako-admin#501)。
    // 拘束は既に外した後の値なので、残差の説明にはこの内訳が要る
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 12:00:00", "休息"),
            tc("2026-06-02 20:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["rest_minus_minutes"], 120);
    assert_eq!(d["restraint_minutes"], 720); // 840 − 120
}

#[tokio::test]
async fn compare_view_omits_a_zero_rest_deduction() {
    // 休息の無い日に `"rest_minus_minutes":0` を全日ぶら下げない (Refs #157 と同じ規則)
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["days"][0].get("rest_minus_minutes").is_none());
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_run_gap() {
    // 運行の継ぎ目のある日だけ run_gap_minutes を載せる (Refs rust-ichibanboshi#170)。
    // 継ぎ目が日を跨げば内訳 (parts) にも載る
    let (status, body) = serve(
        vec![
            ev("2026-06-01 06:00:00", "2026-06-02 20:00:00", "休息"),
            dtako("2026-06-02 23:00:00", "運行終了", "u1"),
            dtako("2026-06-03 01:00:00", "運行開始", "u2"),
            ev("2026-06-03 10:00:00", "2026-06-04 04:00:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["run_gap_minutes"], 120);
    assert_eq!(d["parts"][0]["run_gap_minutes"], 60);
    assert_eq!(d["parts"][1]["run_gap_minutes"], 60);
    // 継ぎ目の無い翌勤務には付かない (0 を全日ぶら下げない)
    assert!(body["days"][1].get("run_gap_minutes").is_none());
}

#[tokio::test]
async fn compare_view_keeps_ferry_on_parts_of_an_overnight_shift() {
    // 日跨ぎ勤務のフェリー控除は parts 側が正 — 突合は parts だけで暦日合算する
    // (実測 1714 井上 03-15: 14:12→翌00:49 の勤務でフェリー 75 分が落ちていた)
    let rows = vec![
        tc_of(1714, "2026-06-15 14:12:00", "始業"),
        tc_of(1714, "2026-06-16 00:49:00", "終業"),
    ];
    let ferry = vec![json!({
        "start_datetime": "2026-06-15 17:34:00",
        "end_datetime": "2026-06-15 18:49:00",
        "driver_id": 1714,
    })];
    let (status, body) = call(
        app(MockRepo::with_ferry(rows, ferry)),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1714&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    let d = &body["days"][0];
    assert_eq!(d["ferry_minus_minutes"], 75);
    assert_eq!(d["parts"][0]["date"], "2026-06-15");
    assert_eq!(d["parts"][0]["ferry_minus_minutes"], 75);
    // 控除の無い翌日の内訳には付かない
    assert!(d["parts"][1].get("ferry_minus_minutes").is_none());
}

#[tokio::test]
async fn compare_view_carries_the_ferry_map_even_when_no_shift_covers_the_day() {
    // 1026 一瀬 2026-05-01 の形 (Refs nuxt-dtako-admin#501): 前月に始業した勤務だけが
    // 覆う日のフェリー控除は、当月応答のどの勤務にも貼れずに落ちる。日別マップを
    // そのまま載せ、突合はマップを優先して読む
    let rows = vec![
        tc("2026-06-10 06:00:00", "始業"),
        tc("2026-06-10 15:00:00", "終業"),
    ];
    let ferry = vec![json!({
        // 勤務の無い 06-05 のフェリー (前月始業の勤務が覆う想定)
        "start_datetime": "2026-06-05 09:40:41",
        "end_datetime": "2026-06-05 10:56:51",
        "driver_id": 1026,
    })];
    let (status, body) = call(
        app(MockRepo::with_ferry(rows, ferry)),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1026&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    // 勤務には貼れない (06-05 の日別行が無い) が、マップには居る
    assert!(body["days"][0].get("ferry_minus_minutes").is_none());
    assert_eq!(body["ferry_minus_by_date"]["2026-06-05"], 76);
}

#[tokio::test]
async fn compare_view_carries_the_ferry_map_for_all_drivers_too() {
    let with_driver = |dt: &str, st: &str| {
        let mut v = tc(dt, st);
        v["driver_id"] = json!(1026);
        v
    };
    let rows = vec![
        with_driver("2026-06-10 06:00:00", "始業"),
        with_driver("2026-06-10 15:00:00", "終業"),
    ];
    let ferry = vec![json!({
        "start_datetime": "2026-06-05 09:40:41",
        "end_datetime": "2026-06-05 10:56:51",
        "driver_id": 1026,
    })];
    let (status, body) = call(
        app(MockRepo::with_ferry(rows, ferry)),
        "/api/kintai/kosoku-daily?month=2026-06&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let body: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["drivers"][0]["ferry_minus_by_date"]["2026-06-05"], 76);
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_punch_tail() {
    // 日跨ぎ終業の尻尾 (Refs #501、1708 松江 03-13 の形)。0 の日には載せない。
    // 尻尾が 0 時を跨げば内訳 (parts) にも配られる
    let (status, body) = serve(
        vec![
            tc("2026-06-13 06:11:00", "始業"),
            dtako("2026-06-13 06:49:00", "運行開始", "u1"),
            dtako("2026-06-13 21:28:00", "運行終了", "u1"),
            tc("2026-06-14 00:30:02", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1708&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["punch_tail_minutes"], 182); // 21:28 → 翌 00:30
    assert_eq!(d["parts"][0]["punch_tail_minutes"], 152);
    assert_eq!(d["parts"][1]["punch_tail_minutes"], 30);
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_punch_head() {
    // 日跨ぎ始業の頭 (Refs #501、1108 福留 03-05 の形)。頭は 0 時を跨ぐので
    // 内訳 (parts) にも配られる。0 の日には載せない
    let (status, body) = serve(
        vec![
            tc("2026-06-05 07:41:00", "始業"),
            dtako("2026-06-06 08:36:00", "運行開始", "u1"),
            ev("2026-06-06 13:54:00", "2026-06-07 04:54:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1108&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["punch_head_minutes"], 1495);
    assert_eq!(d["parts"][0]["punch_head_minutes"], 979);
    assert_eq!(d["parts"][1]["punch_head_minutes"], 516);
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_run_head() {
    // 始業前の運行の頭 (Refs #501、1026 一瀬の形)。0 の日には載せない
    let (status, body) = serve(
        vec![
            dtako("2026-06-11 23:50:00", "運行開始", "u1"),
            tc("2026-06-12 00:05:00", "始業"),
            tc("2026-06-12 13:58:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1026&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["run_head_minutes"], 15);
    // 頭が 0 時を跨ぐので内訳にも配られる
    assert_eq!(d["parts"][0]["run_head_minutes"], 10);
    assert_eq!(d["parts"][1]["run_head_minutes"], 5);
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_lunch_overlap() {
    // 昼休の窓との重なり (Refs #501、1714 井上 03-04 の形)。日跨ぎの対は
    // 両日の窓が内訳 (parts) に配られる。0 の日には載せない
    let (status, body) = serve(
        vec![
            tc("2026-06-04 08:00:00", "始業"),
            tc("2026-06-05 17:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1714&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["lunch_overlap_minutes"], 120);
    assert_eq!(d["parts"][0]["lunch_overlap_minutes"], 60);
    assert_eq!(d["parts"][1]["lunch_overlap_minutes"], 60);
}

// --- 全列同一の行の検知 (Refs nuxt-dtako-admin#501) ---

#[tokio::test]
async fn duplicate_rows_are_reported_per_calendar_day() {
    // 取り込みが 2 回走った形。落とした件数を暦日ごとに返す (紙は二重計上する)
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 11:00:00", "休憩"),
            ev("2026-06-02 10:00:00", "2026-06-02 11:00:00", "休憩"),
            tc("2026-06-02 20:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["duplicate_rows"]["2026-06-02"], 1);
    // 落としたので休憩は 1 回だけ引かれる
    assert_eq!(body["days"][0]["break_minutes"], 60);
}

#[tokio::test]
async fn compare_view_reports_duplicate_rows_too() {
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            ev("2026-06-02 10:00:00", "2026-06-02 11:00:00", "休憩"),
            ev("2026-06-02 10:00:00", "2026-06-02 11:00:00", "休憩"),
            tc("2026-06-02 20:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["duplicate_rows"]["2026-06-02"], 1);
}

#[tokio::test]
async fn compare_view_omits_duplicate_rows_when_there_are_none() {
    let (status, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.get("duplicate_rows").is_none());
}

#[tokio::test]
async fn duplicate_rows_are_reported_for_all_drivers_too() {
    let (status, body) = serve(
        vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
            tc_of(1018, "2026-06-02 09:00:00", "始業"),
            tc_of(1018, "2026-06-02 17:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let drivers = body["drivers"].as_array().unwrap();
    let a = drivers.iter().find(|d| d["driver"] == 1119).unwrap();
    let b = drivers.iter().find(|d| d["driver"] == 1018).unwrap();
    assert_eq!(a["duplicate_rows"]["2026-06-02"], 1);
    // 重複の無い乗務員には付けない
    assert!(b.get("duplicate_rows").is_none());
}

#[tokio::test]
async fn compare_view_keeps_parts_for_the_calendar_split() {
    // 日跨ぎ勤務は暦日按分に parts が要る。ただし日付と拘束だけ (夜勤 = 昼窓なし)
    let (status, body) = serve(
        vec![
            tc("2026-06-02 20:00:00", "始業"),
            tc("2026-06-03 04:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let parts = body["days"][0]["parts"].as_array().unwrap();
    assert_eq!(parts.len(), 2);
    let keys: Vec<&str> = parts[0]
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(keys, vec!["date", "restraint_minutes"]);
}

#[tokio::test]
async fn compare_view_omits_parts_for_a_single_day_shift() {
    // 1 日で終わる勤務は内訳が本体と同じなので載せない (元の応答と同じ規則)
    let (_, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert!(body["days"][0].get("parts").is_none());
}

#[tokio::test]
async fn compare_view_drops_punches_for_all_drivers_too() {
    // 全乗務員の経路は driver_id で分けるので、行に乗務員CD が要る
    let with_driver = |dt: &str, st: &str| {
        let mut v = tc(dt, st);
        v["driver_id"] = json!(1442);
        v
    };
    let (status, body) = serve(
        vec![
            with_driver("2026-06-02 06:00:00", "始業"),
            with_driver("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d0 = &body["drivers"][0];
    assert!(d0.get("punches").is_none());
    let keys: Vec<&str> = d0["days"][0]
        .as_object()
        .unwrap()
        .keys()
        .map(|k| k.as_str())
        .collect();
    assert_eq!(
        keys,
        vec!["date", "lunch_overlap_minutes", "restraint_minutes"]
    );
}

#[tokio::test]
async fn compare_view_carries_the_paper_drift() {
    // 運行の無い対はこちらが昼休を休憩として数え (拘束 540)、紙は拘束から引く (480)。
    // その差 60 が紙の再現値との drift として乗る (Refs nuxt-dtako-admin#501)。
    // 単一乗務員と全乗務員の両経路で同じ値になること
    let with_driver = |dt: &str, st: &str| {
        let mut v = tc(dt, st);
        v["driver_id"] = json!(1442);
        v
    };
    let rows = vec![
        with_driver("2026-06-02 06:00:00", "始業"),
        with_driver("2026-06-02 15:00:00", "終業"),
    ];
    let (status, body) = serve(
        rows.clone(),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["paper_drift_by_date"]["2026-06-02"], 60);
    let (status, body) = serve(rows, "/api/kintai/kosoku-daily?month=2026-06&view=compare").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["drivers"][0]["paper_drift_by_date"]["2026-06-02"], 60);
}

#[tokio::test]
async fn compare_view_carries_the_paper_outside_map() {
    // 終業打刻の後の構内ミニ運行 (1018 金原 2026-03-03 の形、Refs #182) は紙だけが
    // 数える — その実額 (digi 1 分 + 継ぎ目 5 分) を `paper_outside_by_date` として
    // 載せる。単一乗務員と全乗務員の両経路で同じ値になること
    let with_driver = |mut v: serde_json::Value| {
        v["driver_id"] = json!(1018);
        v
    };
    let rows = vec![
        with_driver(tc("2026-06-02 09:23:49", "始業")),
        with_driver(tc("2026-06-02 20:25:27", "終業")),
        with_driver(dtako("2026-06-02 20:36:57", "運行開始", "u1")),
        with_driver(ev("2026-06-02 20:36:57", "2026-06-02 20:37:15", "運転")),
        with_driver(dtako("2026-06-02 20:37:15", "運行終了", "u1")),
        with_driver(dtako("2026-06-02 20:42:29", "運行開始", "u2")),
        with_driver(dtako("2026-06-02 20:42:56", "運行終了", "u2")),
    ];
    let (status, body) = serve(
        rows.clone(),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1018&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["paper_outside_by_date"]["2026-06-02"], 6);
    let (status, body) = serve(rows, "/api/kintai/kosoku-daily?month=2026-06&view=compare").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["drivers"][0]["paper_outside_by_date"]["2026-06-02"], 6);
}

#[tokio::test]
async fn paper_drift_counts_duplicate_rows_the_way_the_paper_does() {
    // 取り込み 2 回で全列同一の行が入ると**紙は二重計上する** (実測 1339 渡邊
    // 2026-04-04)。再現は重複除去の**前**の行で計算しないと drift が 0 になり
    // 差が unknown に残る。こちらの拘束は除去後 (540) のまま
    let rows = vec![
        tc("2026-06-02 06:00:00", "始業"),
        ev("2026-06-02 12:00:00", "2026-06-02 13:00:00", "休憩"),
        ev("2026-06-02 12:00:00", "2026-06-02 13:00:00", "休憩"),
        tc("2026-06-02 15:00:00", "終業"),
    ];
    let (status, body) = serve(
        rows,
        "/api/kintai/kosoku-daily?month=2026-06&driver=1339&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["days"][0]["restraint_minutes"], 540);
    // 紙 = TC_DC 540 − 昼休 60 + デジタコ 60×2 (重複) = 600 → drift 540 − 600 = -60。
    // 除去後の行で計算すると -0 になってしまう
    assert_eq!(body["paper_drift_by_date"]["2026-06-02"], -60);
    // 重複そのものの診断は従来どおり出る
    assert_eq!(body["duplicate_rows"]["2026-06-02"], 1);
}

#[tokio::test]
async fn full_view_does_not_carry_the_paper_drift() {
    // 突合しか使わないので compare 以外には載せない (#157 の絞りと同じ方針)
    let (_, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442",
    )
    .await;
    assert!(body.get("paper_drift_by_date").is_none());
}

#[tokio::test]
async fn an_unknown_view_falls_back_to_the_full_shape() {
    // 綴り間違いで黙って情報が減らないように、既定 (全項目) へ倒す
    let (_, body) = serve(
        vec![
            tc("2026-06-02 06:00:00", "始業"),
            tc("2026-06-02 15:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=full",
    )
    .await;
    assert!(body.get("punches").is_some());
    assert!(body["days"][0].get("working_minutes").is_some());
}

// --- view=timecard (Refs #164) ---
//
// 画面のタイムカード表用。消費側 (nuxt-dtako-admin front の `toKosokuDay` / relay の
// `parseKosokuDaily`) は date/start/end のどれかが欠けた日を捨て、数値の欠けを 0、
// `source` を `=== 'rest'`、旗を `=== true` で読む — その読み方に合わせて既定値を
// 省略する。

#[tokio::test]
async fn timecard_view_keeps_date_start_end_and_omits_defaults() {
    // 所定 (7.5h) ちょうどで収まる平日 — 分数の過半が 0 の、いちばん普通の日
    let (status, body) = serve(
        vec![
            tc("2026-06-02 09:00:00", "始業"),
            tc("2026-06-02 17:30:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1018&view=timecard",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["month"], "2026-06");
    assert_eq!(body["driver"], 1018);
    let d = &body["days"][0];
    // 消費側が行を捨てないための必須 3 つ
    assert_eq!(d["date"], "2026-06-02");
    assert_eq!(d["start"], "2026-06-02 09:00:00");
    assert_eq!(d["end"], "2026-06-02 17:30:00");
    // 非 0 の分数は残る
    assert_eq!(d["restraint_minutes"], 510);
    assert_eq!(d["break_minutes"], 60);
    assert_eq!(d["working_minutes"], 450);
    assert_eq!(d["statutory_minutes"], 450);
    // 0 の分数・既定値の旗・既定の source は書かない (消費側が 0 / false /
    // 'timecard' に落とす)
    for key in [
        "within_statutory_overtime_minutes",
        "overtime_minutes",
        "legal_holiday_minutes",
        "night_minutes",
        "overtime_night_minutes",
        "legal_holiday_night_minutes",
        "ferry_minus_minutes",
        "source",
        "is_legal_holiday",
        "over_24h",
        // compare の診断専用 — 画面は読まない
        "rest_minus_minutes",
    ] {
        assert!(d.get(key).is_none(), "{key} should be omitted");
    }
    // 打刻は表の出勤/退社列の原本なので残す
    assert_eq!(d["punches"].as_array().unwrap().len(), 2);
    assert_eq!(d["punches"][0]["at"], "2026-06-02 09:00:00");
    assert_eq!(d["punches"][0]["state"], "始業");
    // 1 日で終わる勤務に parts は無い
    assert!(d.get("parts").is_none());
    // 月全打刻 (`month_punches`) は days[].punches と重複するので載せない
    assert!(body.get("punches").is_none());
    assert!(body.get("duplicate_rows").is_none());
}

#[tokio::test]
async fn timecard_view_keeps_a_rest_source() {
    // 休息由来の勤務だけ source を書く (消費側は `=== 'rest'` 判定)
    let (status, body) = serve(
        vec![
            ev("2026-06-01 16:19:00", "2026-06-02 04:42:00", "休息"),
            ev("2026-06-02 16:18:00", "2026-06-03 06:01:00", "休息"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1119&view=timecard",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["source"], "rest");
    // 休息由来は打刻を持たない — 空配列をぶら下げない
    assert!(d.get("punches").is_none());
}

#[tokio::test]
async fn timecard_view_keeps_true_flags_and_parts() {
    // 日曜始まりの 24 時間超 — 旗 2 つと暦日按分が同時に出る形
    let (status, body) = serve(
        vec![
            tc("2026-06-07 06:00:00", "始業"), // 日曜
            tc("2026-06-08 08:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&driver=1442&view=timecard",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &body["days"][0];
    assert_eq!(d["is_legal_holiday"], true);
    assert_eq!(d["over_24h"], true);
    // 日跨ぎなので暦日按分が付く。part も date + 非 0 分数だけ
    let parts = d["parts"].as_array().unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0]["date"], "2026-06-07");
    assert!(parts[0]["restraint_minutes"].as_i64().unwrap() > 0);
    // 日曜 (法定休日) の part に時間外は無い
    assert!(parts[0].get("overtime_minutes").is_none());
    assert!(parts[0].get("ferry_minus_minutes").is_none());
}

#[tokio::test]
async fn timecard_view_keeps_a_non_zero_ferry_deduction() {
    let repo = MockRepo::with_ferry(
        vec![
            tc_of(1726, "2026-06-02 09:00:00", "始業"),
            tc_of(1726, "2026-06-02 18:00:00", "終業"),
        ],
        vec![ferry_row(
            "2026-06-02 10:00:00",
            "2026-06-02 11:18:56",
            1726,
        )],
    );
    let (status, body) = call(
        app(repo),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1726&view=timecard",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &serde_json::from_str::<Value>(&body).unwrap()["days"][0];
    assert_eq!(d["ferry_minus_minutes"], 78);
}

#[tokio::test]
async fn timecard_view_drops_driver_level_punches_for_all_drivers() {
    // 一括応答の drivers[].punches (月全打刻) は消費側のどちらも読まない (Refs #164)。
    // days[].punches と重複しているだけなので落とす。duplicate_rows 診断も落とす
    let (status, body) = serve(
        vec![
            tc_of(1119, "2026-06-02 06:00:00", "始業"),
            tc_of(1119, "2026-06-02 06:00:00", "始業"), // 全列同一 = 取り込み二重
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&view=timecard",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d0 = &body["drivers"][0];
    assert_eq!(d0["driver"], 1119);
    assert!(d0.get("punches").is_none());
    assert!(d0.get("duplicate_rows").is_none());
    // 日別の打刻 (表の原本) は残る
    let day = &d0["days"][0];
    assert_eq!(day["punches"].as_array().unwrap().len(), 2);
    assert_eq!(day["restraint_minutes"], 720);
}

#[tokio::test]
async fn timecard_view_is_smaller_than_the_full_shape() {
    // 同じデータを全項目と timecard で serialize してバイト数が減っていることを見る。
    // 実データの実測 (month=2026-05) はデプロイ後に行う
    let rows = vec![
        tc_of(1119, "2026-06-02 09:00:00", "始業"),
        tc_of(1119, "2026-06-02 18:00:00", "終業"),
        tc_of(1442, "2026-06-07 06:00:00", "始業"), // 日曜跨ぎ (parts あり)
        tc_of(1442, "2026-06-08 08:00:00", "終業"),
    ];
    let (_, full) = call(
        app(MockRepo::with_rows(rows.clone())),
        "/api/kintai/kosoku-daily?month=2026-06",
    )
    .await;
    let (_, timecard) = call(
        app(MockRepo::with_rows(rows)),
        "/api/kintai/kosoku-daily?month=2026-06&view=timecard",
    )
    .await;
    println!(
        "full={} bytes / timecard={} bytes ({}%)",
        full.len(),
        timecard.len(),
        timecard.len() * 100 / full.len()
    );
    assert!(
        timecard.len() < full.len(),
        "timecard ({}) should be smaller than full ({})",
        timecard.len(),
        full.len()
    );
}

#[tokio::test]
async fn an_unknown_view_falls_back_to_the_full_shape_for_all_drivers_too() {
    let (_, body) = serve(
        vec![
            tc_of(1119, "2026-06-02 09:00:00", "始業"),
            tc_of(1119, "2026-06-02 18:00:00", "終業"),
        ],
        "/api/kintai/kosoku-daily?month=2026-06&view=timecards",
    )
    .await;
    let d0 = &body["drivers"][0];
    // 綴り間違いは全項目のまま — 黙って情報が減らない
    assert!(d0.get("punches").is_some());
    assert!(d0["days"][0].get("working_minutes").is_some());
}

#[tokio::test]
async fn compare_view_keeps_a_non_zero_ferry_deduction() {
    // 控除がある日だけ ferry_minus_minutes を載せる (Refs #157)
    let repo = MockRepo::with_ferry(
        vec![
            tc_of(1726, "2026-06-02 09:00:00", "始業"),
            tc_of(1726, "2026-06-02 18:00:00", "終業"),
        ],
        vec![ferry_row(
            "2026-06-02 10:00:00",
            "2026-06-02 11:18:56",
            1726,
        )],
    );
    let (status, body) = call(
        app(repo),
        "/api/kintai/kosoku-daily?month=2026-06&driver=1726&view=compare",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let d = &serde_json::from_str::<Value>(&body).unwrap()["days"][0];
    assert!(d["ferry_minus_minutes"].as_i64().unwrap() > 0);
}
