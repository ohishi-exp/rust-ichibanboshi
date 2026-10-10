//! `dtako_events` の**作業区分 (層 A) を 乗務員 × 暦日 × 区分 の秒数**にして返す
//! (Refs ohishi-exp/nuxt-dtako-admin#612 の PR-2)。
//!
//! 拘束サマリの 6 指標のうち「運転」「荷役」は打刻からは原理的に作れず、出せるのは
//! デジタコの運行イベントだけ (#612)。この口はその**材料だけ**を出す。
//!
//! ## 純粋部分は共有 crate `kintai-dtako` (`workers/kintai/dtako`、Refs #322)
//!
//! 層 A〜D の分類・暦日への切り分け・応答の組み立て (と、「運転」「荷役」を名乗らない・
//! 知らない名前を黙って捨てない・秒で返す・読む窓の説明) は `kintai_dtako::worktime` にあり、
//! 勤怠 Worker も同じものを使う。ここに残るのは handler (入力の検査・同時実行の絞り・
//! repo 呼び出し) だけ。`kintai-dtako` も `build.rs` の glob の外。
//!
//! ## ファイル名がなぜ `dtako_worktime.rs` か (`kintai`/`kosoku` で始めない)
//!
//! `build.rs` の `KINTAI_OUTPUT_GLOBS` は `src/` と `src/routes/` をファイル名の
//! **接頭辞** (`kintai` / `kosoku`) で拾い、拾われた内容のハッシュが `logic_version`
//! (`/api/kintai/version` の etag) に畳まれる。この口は既存の生イベント読み出しを
//! **そのまま再利用するだけ**で `/api/kintai/{daily,kosoku-daily,version}` の応答を
//! 一切変えないので、`kintai` で始まる名前を付けると無関係な deploy まで
//! 全乗務員を stale にしてしまう ([`crate::routes::dtako_day`] と同じ理由)。
//!
//! ## read-only
//!
//! 読むだけ。書き込みは 1 本も持たない。判定もしない — 拘束も勤務も畳まず、
//! 区分ごとの秒数をそのまま返す ([`crate::routes::kintai::rest_diff`] と同じ流儀)。

use axum::extract::Query;
use axum::http::StatusCode;
use axum::Extension;
use axum::Json;
use kintai_dtako::worktime::{aggregate, parse_dt};
use serde::Deserialize;

use crate::kintai_repo::DynKintaiEventsRepo;
use crate::routes::kintai::{map_repo_err, parse_driver};

/// 月ぶんの生イベント読み出しは重い (全乗務員で 10 万行規模) ので、この口だけで
/// 同時実行を絞る。
///
/// **`kintai.rs` の `KOSOKU_DB_PERMITS` とは別枠**で、合算の上限にはならない
/// (あちらは private static で、`pub` にすると `logic_version` が動くため共有
/// できない)。守れるのは「この口が単独で DB を溢れさせないこと」だけ。
static DTAKO_WORKTIME_PERMITS: tokio::sync::Semaphore = tokio::sync::Semaphore::const_new(2);

/// `?month=2026-06[&driver=1041]`。`month` 必須・`driver` は省略可
/// (省略 = 全乗務員)。`driver=` (空) は省略ではなく**不正**として 400。
#[derive(Debug, Deserialize)]
pub struct WorktimeQuery {
    pub month: Option<String>,
    pub driver: Option<String>,
}

/// GET /api/dtako/worktime?month=YYYY-MM[&driver=1041] — `dtako_events` の
/// **層 A (作業区分) を 乗務員 × 暦日 × 区分 の秒数**で返す
/// (Refs ohishi-exp/nuxt-dtako-admin#612 の PR-2)。
///
/// **データ源は `/api/kintai/events` と同じ repo 関数**
/// ([`crate::kintai_repo::KintaiEventsApi`]) — SQL は 1 本も増やさない。
/// `driver` を省くと全乗務員版 (`fetch_all_events_between`) を 1 回だけ叩く。
///
/// **read-only・判定なし・合成なし。** 「運転」「荷役」は名乗らない
/// (モジュール docs 参照)。
pub async fn worktime(
    Query(params): Query<WorktimeQuery>,
    Extension(repo): Extension<DynKintaiEventsRepo>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let month = params.month.unwrap_or_default();
    let Some((from, to)) = crate::kintai_repo::exact_month_range(&month) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "month は YYYY-MM で指定してください".to_string(),
        ));
    };
    let driver = match params.driver {
        None => None,
        Some(raw) => match parse_driver(&raw) {
            Some(d) => Some(d),
            None => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "driver は乗務員CD (数字) で指定してください".to_string(),
                ))
            }
        },
    };
    let _permit = DTAKO_WORKTIME_PERMITS
        .acquire()
        .await
        .expect("semaphore open");
    let rows = match driver {
        Some(d) => repo.fetch_events_between(&from, &to, d).await,
        None => repo.fetch_all_events_between(&from, &to).await,
    }
    .map_err(map_repo_err)?;
    // `exact_month_range` が作った書式なので必ず読める
    let win_from = parse_dt(&from).expect("exact_month_range の from");
    let win_to = parse_dt(&to).expect("exact_month_range の to");
    let agg = aggregate(&rows, win_from, win_to);
    // マクロは 1 行に収める (CLAUDE.md)
    let (days, counted) = (agg.days.len(), agg.counted_rows);
    tracing::info!(month = %month, days, counted, "dtako worktime built");
    // 知らない区分は 0 でも出す (層 A の取りこぼしが見えるように)
    let unknown = agg.unclassified_states.len();
    tracing::info!(unknown, "dtako worktime unknown");
    Ok(Json(agg.to_json(&month, driver, &from, &to)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use axum::routing::get;
    use axum::Router;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn ev(state: &str, start: &str, end: Value, driver: Value) -> Value {
        json!({
            "source": "dtako_events",
            "state": state,
            "datetime": start,
            "end_datetime": end,
            "driver_id": driver,
        })
    }

    fn span(state: &str, start: &str, end: &str) -> Value {
        ev(state, start, json!(end), json!(1041))
    }

    fn app(repo: Arc<dyn crate::kintai_repo::KintaiEventsApi>) -> Router {
        Router::new()
            .route("/api/dtako/worktime", get(worktime))
            .layer(Extension(repo as DynKintaiEventsRepo))
    }

    async fn call(
        repo: Arc<dyn crate::kintai_repo::KintaiEventsApi>,
        uri: &str,
    ) -> (StatusCode, Value) {
        let res = app(repo)
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)));
        (status, body)
    }

    /// 読み口 2 本を差し替えるだけの mock。`fail` で `NotConfigured` (= 503) 側へ
    /// 倒す。**2 つの struct に分けない** — 分けると使わない側の impl が丸ごと
    /// 未カバーになり、100% gate が「テストで触っていない足場」で落ちる。
    struct MockRepo {
        one: Vec<Value>,
        all: Vec<Value>,
        fail: bool,
    }

    #[async_trait]
    impl crate::kintai_repo::KintaiEventsApi for MockRepo {
        async fn fetch_events_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: u64,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            if self.fail {
                return Err(crate::kintai_repo::KintaiRepoError::NotConfigured);
            }
            Ok(self.one.clone())
        }

        async fn fetch_all_events_between(
            &self,
            _from: &str,
            _to: &str,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            if self.fail {
                return Err(crate::kintai_repo::KintaiRepoError::NotConfigured);
            }
            Ok(self.all.clone())
        }

        /// trait の必須メソッドなので実装は要るが、この口は**フェリーを読まない**。
        /// 空配列で埋めると「読んでも 0 件」と区別が付かなくなるので、触ったら
        /// 落ちるようにしておく (下の `#[should_panic]` が発火を固定している)。
        async fn fetch_ferry_between(
            &self,
            _from: &str,
            _to: &str,
            _driver: Option<u64>,
        ) -> Result<Vec<Value>, crate::kintai_repo::KintaiRepoError> {
            panic!("worktime はフェリーを読まない")
        }
    }

    fn mock() -> Arc<MockRepo> {
        Arc::new(MockRepo {
            fail: false,
            one: vec![span("運転", "2026-06-02 08:00:00", "2026-06-02 09:00:00")],
            all: vec![
                span("運転", "2026-06-02 08:00:00", "2026-06-02 09:00:00"),
                ev(
                    "積み",
                    "2026-06-02 10:00:00",
                    json!("2026-06-02 10:30:00"),
                    json!(1368),
                ),
            ],
        })
    }

    #[tokio::test]
    async fn rejects_bad_month_and_bad_driver() {
        let (s, _) = call(mock(), "/api/dtako/worktime").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let (s, _) = call(mock(), "/api/dtako/worktime?month=2026-13").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let (s, _) = call(mock(), "/api/dtako/worktime?month=2026-06&driver=").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
        let (s, _) = call(mock(), "/api/dtako/worktime?month=2026-06&driver=abc").await;
        assert_eq!(s, StatusCode::BAD_REQUEST);
    }

    fn failing() -> Arc<MockRepo> {
        Arc::new(MockRepo {
            fail: true,
            one: Vec::new(),
            all: Vec::new(),
        })
    }

    /// 読めなかったら 503 で fail-closed (空配列で「0 件」に見せない)。
    /// **全乗務員版と 1 名版の両方**を測る — 分岐が 2 本あるので片方だけだと
    /// もう片方の失敗経路が未検証のまま残る。
    #[tokio::test]
    async fn repo_error_maps_to_status_on_both_paths() {
        let (s, _) = call(failing(), "/api/dtako/worktime?month=2026-06").await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
        let (s, _) = call(failing(), "/api/dtako/worktime?month=2026-06&driver=1041").await;
        assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
    }

    /// ★ この口はフェリーを読まない。読み口を足したときに黙って通らないよう、
    /// mock が落ちること自体を固定する (陰性対照)。
    #[tokio::test]
    #[should_panic(expected = "worktime はフェリーを読まない")]
    async fn ferry_is_never_read() {
        use crate::kintai_repo::KintaiEventsApi;
        let _ = mock().fetch_ferry_between("a", "b", None).await;
    }

    /// `driver` 省略 = 全乗務員版を読む。窓と層一覧も応答に出る。
    #[tokio::test]
    async fn all_drivers_shape() {
        let (s, body) = call(mock(), "/api/dtako/worktime?month=2026-06").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["month"], json!("2026-06"));
        assert_eq!(body["driver"], json!(null));
        assert_eq!(body["from"], json!("2026-06-01 00:00:00"));
        assert_eq!(body["to"], json!("2026-07-01 00:00:00"));
        assert_eq!(
            body["layer_a_states"],
            json!(kintai_dtako::worktime::LAYER_A_STATES)
        );
        assert_eq!(body["days"].as_array().unwrap().len(), 2);
        assert_eq!(body["days"][0]["driver_cd"], json!(1041));
        assert_eq!(body["days"][0]["date"], json!("2026-06-02"));
        assert_eq!(body["days"][0]["seconds_by_state"]["運転"], json!(3600));
        assert_eq!(body["days"][0]["seconds_by_state"]["積み"], json!(0));
        // ★ 単位はフィールド名で持つ。裸の `seconds` / `minutes` では返さない —
        // 秒を分の名前で渡すのがこの設計でいちばん危ない取り違えなので、
        // 名前が戻ったらここで落ちる
        assert!(body["days"][0].get("seconds_by_state").is_some());
        assert!(body["days"][0].get("minutes").is_none());
        assert!(body["days"][0].get("seconds").is_none());
        assert_eq!(body["days"][1]["driver_cd"], json!(1368));
        assert_eq!(body["days"][1]["seconds_by_state"]["積み"], json!(1800));
        assert_eq!(body["counted_rows"], json!(2));
        assert_eq!(body["unclassified_states"], json!({}));
        assert_eq!(body["ignored_rows"]["layer_b"], json!(0));
        assert_eq!(body["unusable_rows"]["zero_length"], json!(0));
        assert_eq!(body["clipped_outside_window_seconds"], json!(0));
    }

    /// `driver` 指定 = 1 名版を読む。
    #[tokio::test]
    async fn single_driver_shape() {
        let (s, body) = call(mock(), "/api/dtako/worktime?month=2026-06&driver=1041").await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(body["driver"], json!(1041));
        assert_eq!(body["days"].as_array().unwrap().len(), 1);
        assert_eq!(body["days"][0]["seconds_by_state"]["運転"], json!(3600));
    }
}
