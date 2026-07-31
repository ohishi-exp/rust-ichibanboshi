//! 生イベントの HTTP 読み先 (`rust-alc-api` の `GET /api/dtako/events`) の統合テスト。
//!
//! **本番 API は一切叩かない。** 上流は wiremock で stub する (`kintai_test.rs` /
//! `kyuyo_introspect_test.rs` と同じ方針。base URL を const にせず
//! `KintaiEventsConfig::base_url` で受けるのはこのため)。
//!
//! 固定したいのは「生 CSV 行 → `KintaiEventsApi` の戻り値」の写し方 —
//! 列名の解決 (`対象乗務員CD` → `乗務員CD1` のフォールバック)、`[from, to)` の
//! 絞り込み、期間の分割とページング、そして上流に口が無い読み出しの委譲。

use std::sync::Arc;

use async_trait::async_trait;
use rust_ichibanboshi::config::{KintaiEventsConfig, KintaiEventsSource};
use rust_ichibanboshi::kintai_http_repo::HttpKintaiEventsRepo;
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use serde_json::{json, Value};
use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// KUDGIVT の代表的なヘッダ (`車輌名` と `終了日時` を持つ完全形)。
const FULL_HEADERS: &[&str] = &[
    "運行NO",
    "読取日",
    "車輌CD",
    "車輌名",
    "乗務員CD1",
    "乗務員名１",
    "対象乗務員CD",
    "対象乗務員区分",
    "開始日時",
    "終了日時",
    "イベントCD",
    "イベント名",
];

fn cfg(base_url: &str) -> KintaiEventsConfig {
    KintaiEventsConfig {
        source: KintaiEventsSource::Http,
        base_url: base_url.to_string(),
        tenant_id: "11111111-2222-3333-4444-555555555555".to_string(),
        timeout_secs: 10,
        auth_token: "test-id-token".to_string(),
        auth_token_command: String::new(),
        auth_token_metadata: false,
        auth_token_ttl_secs: 900,
    }
}

fn repo(base_url: &str) -> HttpKintaiEventsRepo {
    HttpKintaiEventsRepo::new(&cfg(base_url), None).unwrap()
}

/// 1 運行分の応答要素。`headers` は運行ごとに持つ形をそのまま組む。
fn operation(unko_no: &str, headers: &[&str], rows: Vec<Vec<&str>>) -> Value {
    json!({
        "unko_no": unko_no,
        "crew_role": 1,
        "departure_at": null,
        "return_at": null,
        "headers": headers,
        "rows": rows,
    })
}

fn single_body(operations: Vec<Value>, warnings: Vec<&str>) -> Value {
    json!({
        "driver": {"cd": "1130", "name": "テスト乗務員"},
        "period": {"date_from": "2026-06-29", "date_to": "2026-08-01"},
        "operations": operations,
        "warnings": warnings,
    })
}

async fn stub_single(server: &MockServer, body: Value) {
    Mock::given(method("GET"))
        .and(path("/api/dtako/events"))
        .and(header(
            "x-tenant-id",
            "11111111-2222-3333-4444-555555555555",
        ))
        .and(header("authorization", "Bearer test-id-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(server)
        .await;
}

// ── 生 CSV 行 → 戻り値 ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_maps_raw_csv_rows_to_the_mariadb_row_shape() {
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![operation(
                "2602241025060000000272",
                FULL_HEADERS,
                vec![vec![
                    "2602241025060000000272",
                    "2026/07/02 00:00:00",
                    "272",
                    "帯広100け272",
                    "1740",
                    "梅津　政弘",
                    "1130",
                    "2",
                    "2026/07/02 14:40:56",
                    "2026/07/03 09:23:56",
                    "302",
                    "休息",
                ]],
            )],
            vec![],
        ),
    )
    .await;

    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();

    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    // 日付は DATE_FORMAT('%Y-%m-%d %H:%i:%s') と同じ形に直す (CSV は YYYY/MM/DD)
    assert_eq!(r["datetime"], "2026-07-02 14:40:56");
    assert_eq!(r["end_datetime"], "2026-07-03 09:23:56");
    // 対象乗務員CD (1130) を採る。乗務員CD1 (1740) は運行の主運転者で全行同じ値
    assert_eq!(r["driver_id"], 1130);
    assert_eq!(r["source"], "dtako_events");
    assert_eq!(r["state"], "休息");
    assert_eq!(r["unko_no"], "2602241025060000000272");
    assert_eq!(r["vehicle"], "帯広100け272");
}

#[tokio::test]
async fn test_two_person_crew_rows_are_split_by_taisho_driver_cd() {
    // 1 運行の CSV には相乗りした 2 名分の行が入る。対象乗務員CD で分けないと
    // 副運転手のイベントが主運転者に付き、引かれた側は丸ごと落ちる。
    let server = MockServer::start().await;
    let headers = &[
        "運行NO",
        "乗務員CD1",
        "対象乗務員CD",
        "対象乗務員区分",
        "開始日時",
        "イベント名",
    ];
    stub_single(
        &server,
        single_body(
            vec![operation(
                "OP-1",
                headers,
                vec![
                    vec!["OP-1", "1740", "1740", "1", "2026/07/02 08:00:00", "出庫"],
                    vec!["OP-1", "1740", "1130", "2", "2026/07/02 09:00:00", "休息"],
                ],
            )],
            vec![],
        ),
    )
    .await;

    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "対象乗務員CD = 1130 の 1 行だけ");
    assert_eq!(rows[0]["state"], "休息");
    assert_eq!(rows[0]["driver_id"], 1130);
}

#[tokio::test]
async fn test_falls_back_to_driver_cd1_when_taisho_column_is_absent() {
    // 対象乗務員CD を持たない古い CSV (1 人乗務)。列の有無は運行ごとに違い得るので、
    // 2 運行で片方だけ列を持つ応答を組んで両方の経路を通す。
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![
                operation(
                    "OP-OLD",
                    &["運行NO", "乗務員CD1", "開始日時", "イベント名"],
                    vec![vec!["OP-OLD", "1130", "2026/07/02 08:00:00", "出庫"]],
                ),
                operation(
                    "OP-NEW",
                    &[
                        "運行NO",
                        "乗務員CD1",
                        "対象乗務員CD",
                        "開始日時",
                        "イベント名",
                    ],
                    vec![vec![
                        "OP-NEW",
                        "9999",
                        "1130",
                        "2026/07/02 12:00:00",
                        "休憩",
                    ]],
                ),
            ],
            vec![],
        ),
    )
    .await;

    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // 時刻昇順 (ORDER BY datetime, source)
    assert_eq!(rows[0]["state"], "出庫");
    assert_eq!(rows[0]["unko_no"], "OP-OLD");
    assert_eq!(rows[1]["state"], "休憩");
    assert_eq!(rows[1]["unko_no"], "OP-NEW");
}

#[tokio::test]
async fn test_missing_end_and_vehicle_columns_become_null() {
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![operation(
                "OP-1",
                &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                vec![vec!["OP-1", "1130", "2026/07/02 08:00:00", "運転"]],
            )],
            vec![],
        ),
    )
    .await;

    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert!(rows[0]["end_datetime"].is_null());
    assert!(rows[0]["vehicle"].is_null(), "車輌名 列が無ければ null");
}

// ── `[from, to)` の絞り込み (EVENTS_SQL の 2 ブランチ) ──────────────────────

#[tokio::test]
async fn test_window_keeps_intervals_that_end_inside_the_range() {
    // 上流には 2 日遡って投げるので、期間の手前で始まる休息も応答に入ってくる。
    // 「期間内に終わる区間」だけを残し、期間外で終わるものは落とす。
    let server = MockServer::start().await;
    let headers = &[
        "運行NO",
        "対象乗務員CD",
        "開始日時",
        "終了日時",
        "イベント名",
    ];
    stub_single(
        &server,
        single_body(
            vec![operation(
                "OP-1",
                headers,
                vec![
                    // 期間内に終わる区間 (開始は期間より前) → 残る。月初の勤務を組むのに要る
                    vec![
                        "OP-1",
                        "1130",
                        "2026/06/30 22:00:00",
                        "2026/07/01 06:00:00",
                        "休息",
                    ],
                    // 期間より前に始まり前に終わる → 落とす
                    vec![
                        "OP-1",
                        "1130",
                        "2026/06/29 10:00:00",
                        "2026/06/29 20:00:00",
                        "休息",
                    ],
                    // 上端は排他。8/2 00:00:00 は入らない
                    vec![
                        "OP-1",
                        "1130",
                        "2026/08/02 00:00:00",
                        "2026/08/02 01:00:00",
                        "休息",
                    ],
                    // 期間内に始まる → 残る
                    vec![
                        "OP-1",
                        "1130",
                        "2026/08/01 23:00:00",
                        "2026/08/02 07:00:00",
                        "休息",
                    ],
                ],
            )],
            vec![],
        ),
    )
    .await;

    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    let times: Vec<&str> = rows
        .iter()
        .map(|r| r["datetime"].as_str().unwrap())
        .collect();
    assert_eq!(times, vec!["2026-06-30 22:00:00", "2026-08-01 23:00:00"]);
}

#[tokio::test]
async fn test_rows_with_broken_start_datetime_are_dropped() {
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![operation(
                "OP-1",
                &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                vec![
                    vec!["OP-1", "1130", "INVALID", "運転"],
                    vec!["OP-1", "1130", "2026/07/02 08:00:00", "運転"],
                ],
            )],
            vec![],
        ),
    )
    .await;
    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

#[tokio::test]
async fn test_operation_without_required_columns_is_skipped_not_fatal() {
    // 開始日時 / イベント名 が無い運行は捨てるが、他の運行は返す
    // (R2 の分割遅れと同じ「一部欠け」の扱い)。
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![
                operation(
                    "OP-BAD",
                    &["運行NO", "対象乗務員CD"],
                    vec![vec!["OP-BAD", "1130"]],
                ),
                operation(
                    "OP-OK",
                    &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                    vec![vec!["OP-OK", "1130", "2026/07/02 08:00:00", "運転"]],
                ),
            ],
            vec![],
        ),
    )
    .await;
    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["unko_no"], "OP-OK");
}

#[tokio::test]
async fn test_upstream_warnings_do_not_fail_the_read() {
    // R2 の分割遅れ (NoSuchKey) は上流が warnings に落として 200 で返す。
    // こちらは log に出して読み出しは続ける (握り潰さない・落とさない)。
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![operation(
                "OP-1",
                &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                vec![vec!["OP-1", "1130", "2026/07/02 08:00:00", "運転"]],
            )],
            vec!["OP-2: KUDGIVT 取得失敗 (NoSuchKey)"],
        ),
    )
    .await;
    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
}

// ── 期間の分割 (上流の上限を超える月次) ─────────────────────────────────────

#[tokio::test]
async fn test_single_driver_month_is_one_request() {
    // month_range は 2026-07-01 → 2026-08-02。2 日遡って 06-29..08-01 = 34 日で、
    // 単一乗務員の上限 366 日には収まるので 1 往復。
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/dtako/events"))
        .and(query_param("driver_cd", "1130"))
        .and(query_param("date_from", "2026-06-29"))
        .and(query_param("date_to", "2026-08-01"))
        .and(query_param_is_missing("page_size"))
        .respond_with(ResponseTemplate::new(200).set_body_json(single_body(vec![], vec![])))
        .expect(1)
        .mount(&server)
        .await;

    let rows = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert!(rows.is_empty());
}

#[tokio::test]
async fn test_all_drivers_month_is_split_to_fit_the_31_day_limit() {
    // 全乗務員版の上限は 31 日。34 日ぶんを 06-29..07-29 と 07-30..08-01 に割る。
    let server = MockServer::start().await;
    let body = |unko_no: &str, at: &str| {
        json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [{
                "driver": {"cd": "1130", "name": "n"},
                "operations": [operation(
                    unko_no,
                    &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                    vec![vec![unko_no, "1130", at, "運転"]],
                )],
            }],
            "next_after_driver_cd": null,
            "warnings": [],
        })
    };
    Mock::given(method("GET"))
        .and(query_param("date_from", "2026-06-29"))
        .and(query_param("date_to", "2026-07-29"))
        .and(query_param_is_missing("driver_cd"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body("OP-A", "2026/07/05 08:00:00")))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("date_from", "2026-07-30"))
        .and(query_param("date_to", "2026-08-01"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body("OP-B", "2026/07/31 08:00:00")))
        .expect(1)
        .mount(&server)
        .await;

    let rows = repo(&server.uri())
        .fetch_all_events("2026-07")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    // 全乗務員版は unko_no / vehicle をキーごと出さない
    assert!(rows[0].get("unko_no").is_none());
    assert!(rows[0].get("vehicle").is_none());
    assert_eq!(rows[0]["datetime"], "2026-07-05 08:00:00");
    assert_eq!(rows[1]["datetime"], "2026-07-31 08:00:00");
}

#[tokio::test]
async fn test_all_drivers_follows_keyset_paging() {
    let server = MockServer::start().await;
    let page = |unko_no: &str, at: &str, next: Value| {
        json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [{
                "driver": {"cd": "1130", "name": "n"},
                "operations": [operation(
                    unko_no,
                    &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                    vec![vec![unko_no, "1130", at, "運転"]],
                )],
            }],
            "next_after_driver_cd": next,
            "warnings": [],
        })
    };
    // 1 ページ目は after_driver_cd なし → 次ページあり
    Mock::given(method("GET"))
        .and(query_param("date_from", "2026-07-05"))
        .and(query_param_is_missing("after_driver_cd"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "OP-P1",
            "2026/07/05 08:00:00",
            json!("1130"),
        )))
        .mount(&server)
        .await;
    // 2 ページ目 → 最終
    Mock::given(method("GET"))
        .and(query_param("after_driver_cd", "1130"))
        .respond_with(ResponseTemplate::new(200).set_body_json(page(
            "OP-P2",
            "2026/07/06 08:00:00",
            json!(null),
        )))
        .mount(&server)
        .await;

    let rows = repo(&server.uri())
        .fetch_all_events_between("2026-07-07 00:00:00", "2026-07-08 00:00:00")
        .await
        .unwrap();
    assert_eq!(rows.len(), 0, "どちらも期間外なので絞り込みで落ちる");
    // 2 ページとも取得したことは wiremock の受信数で確かめる
    assert_eq!(server.received_requests().await.unwrap().len(), 2);
}

#[tokio::test]
async fn test_paging_that_never_terminates_is_an_error() {
    // 上流が `next_after_driver_cd` を返し続けたら黙って無限に回さず error にする
    // (打ち切りが無いと 1 リクエストがサーバーを占有し続ける)。
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [],
            "next_after_driver_cd": "always-more",
            "warnings": [],
        })))
        .mount(&server)
        .await;
    let err = repo(&server.uri())
        .fetch_all_events_between("2026-07-05 00:00:00", "2026-07-06 00:00:00")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("did not terminate"), "{err}");
}

#[tokio::test]
async fn test_same_operation_is_not_counted_twice() {
    // 2 名乗務の運行は 2 つの乗務員グループに現れる。運行NO で重複排除する
    // (CSV の中の重複行は落とさない — それは kosoku.rs 側の話)。
    let server = MockServer::start().await;
    let op = operation(
        "OP-SHARED",
        &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
        vec![
            vec!["OP-SHARED", "1740", "2026/07/05 08:00:00", "出庫"],
            vec!["OP-SHARED", "1130", "2026/07/05 09:00:00", "休息"],
        ],
    );
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [
                {"driver": {"cd": "1740", "name": "a"}, "operations": [op.clone()]},
                {"driver": {"cd": "1130", "name": "b"}, "operations": [op]},
            ],
            "next_after_driver_cd": null,
            "warnings": [],
        })))
        .mount(&server)
        .await;

    let rows = repo(&server.uri())
        .fetch_all_events_between("2026-07-05 00:00:00", "2026-07-06 00:00:00")
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "2 グループに現れても行は 1 運行分だけ");
    // ORDER BY driver_id, datetime
    assert_eq!(rows[0]["driver_id"], 1130);
    assert_eq!(rows[1]["driver_id"], 1740);
}

// ── 畳むときの往復数 (Refs #205 実装計画 05) ───────────────────────────────

/// 月を畳んでも上流への往復は**全乗務員版の読み 1 回ぶん**しか起きない。
///
/// 乗務員ごとに読んでいた頃は、この 1 回に加えて**乗務員 1 名につき 1 往復**が
/// 乗っていた (95 名 × 2 か月で約 190 往復)。HTTP 実装では 1 往復が `rust-alc-api`
/// への 1 往復 = 裏で R2 の GET 群になるので、ここが効く。
///
/// 「1 回ぶん」が 2 リクエストなのは上流の期間上限 (全乗務員 31 日) で
/// [`month_range`] の 34 日が 2 つに割れるため — 乗務員の数では増えない。
#[tokio::test]
async fn test_folding_a_month_costs_one_all_drivers_read() {
    use rust_ichibanboshi::kintai_fold::fold_month;
    use rust_ichibanboshi::kosoku::KosokuParams;

    let server = MockServer::start().await;
    // 休息 2 本で勤務が 1 本立つ形を 2 名ぶん。乗務員の数が往復に効かないことを見る
    let op = |unko_no: &str, driver: &str| {
        operation(
            unko_no,
            &[
                "運行NO",
                "対象乗務員CD",
                "開始日時",
                "終了日時",
                "イベント名",
            ],
            vec![
                vec![
                    unko_no,
                    driver,
                    "2026/07/03 02:00:00",
                    "2026/07/03 11:00:00",
                    "休息",
                ],
                vec![
                    unko_no,
                    driver,
                    "2026/07/03 13:00:00",
                    "2026/07/03 18:00:00",
                    "運転",
                ],
                vec![
                    unko_no,
                    driver,
                    "2026/07/04 02:00:00",
                    "2026/07/04 11:00:00",
                    "休息",
                ],
            ],
        )
    };
    Mock::given(method("GET"))
        .and(query_param_is_missing("driver_cd"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [
                {"driver": {"cd": "1130", "name": "a"}, "operations": [op("OP-1130", "1130")]},
                {"driver": {"cd": "1740", "name": "b"}, "operations": [op("OP-1740", "1740")]},
            ],
            "next_after_driver_cd": null,
            "warnings": [],
        })))
        .mount(&server)
        .await;

    let repo: DynKintaiEventsRepo = Arc::new(repo(&server.uri()));
    let units = fold_month(&repo, &KosokuParams::default(), "2026-07", None)
        .await
        .unwrap();

    assert_eq!(
        units.iter().map(|(cd, ..)| *cd).collect::<Vec<_>>(),
        vec![1130, 1740],
        "2 名とも畳めている"
    );
    assert!(
        units.iter().all(|(_, u, _)| !u.shifts.is_empty()),
        "勤務が立っていないと往復数だけ見ても意味が無い"
    );

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(
        reqs.len(),
        2,
        "期間分割の 2 回だけ (乗務員の数では増えない)"
    );
    for r in &reqs {
        assert!(
            !r.url.query_pairs().any(|(k, _)| k == "driver_cd"),
            "乗務員を名指しした読みが混ざっている: {}",
            r.url
        );
    }
}

// ── 失敗の伝え方 ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_upstream_error_status_is_reported_not_swallowed() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;
    let err = repo(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("500"), "{msg}");
    assert!(msg.contains("boom"), "{msg}");
}

#[tokio::test]
async fn test_non_json_body_is_an_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("<html>nope</html>"))
        .mount(&server)
        .await;
    let err = repo(&server.uri())
        .fetch_all_events("2026-07")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("parse"), "{err}");
}

#[tokio::test]
async fn test_unreachable_upstream_is_an_error() {
    // 実在しない相手。**本番 API は叩かない** (ポート 1 は listen していない)
    let err = repo("http://127.0.0.1:1")
        .fetch_events("2026-07", 1130)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("request"), "{err}");
}

#[tokio::test]
async fn test_broken_month_fails_before_any_request() {
    let server = MockServer::start().await;
    let r = repo(&server.uri());
    assert!(r.fetch_events("2026-13", 1130).await.is_err());
    assert!(r.fetch_all_events("nope").await.is_err());
    assert!(r
        .fetch_events_between("bad", "2026-08-02 00:00:00", 1130)
        .await
        .is_err());
    assert!(r
        .fetch_all_events_between("bad", "2026-08-02 00:00:00")
        .await
        .is_err());
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_base_url_trailing_slash_is_tolerated() {
    let server = MockServer::start().await;
    let mut c = cfg(&format!("{}/", server.uri()));
    c.auth_token = String::new();
    Mock::given(method("GET"))
        .and(path("/api/dtako/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(single_body(vec![], vec![])))
        .expect(1)
        .mount(&server)
        .await;
    let rows = HttpKintaiEventsRepo::new(&c, None)
        .unwrap()
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    assert!(rows.is_empty());
}

// ── 上流に口が無い読み出しの委譲 ───────────────────────────────────────────

/// 打刻 (`timecard`) / 運行の確定イベント (`dtako`) / フェリーを持つ委譲先の代役。
/// 実物では MariaDB 実装が入る。
struct FallbackStub;

#[async_trait]
impl KintaiEventsApi for FallbackStub {
    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(vec![
            json!({"datetime": "2026-07-02 07:00:00", "end_datetime": null, "driver_id": 1130,
                   "source": "timecard", "state": "始業", "unko_no": null, "vehicle": null}),
            // MariaDB は 3 表を UNION するので dtako_events も混ざって返る。
            // 借りるのは上流に口が無い source だけ (二重計上しない)
            json!({"datetime": "2026-07-02 14:40:56", "end_datetime": null, "driver_id": 1130,
                   "source": "dtako_events", "state": "休息", "unko_no": null, "vehicle": null}),
        ])
    }

    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(vec![
            json!({"datetime": "2026-07-05 07:00:00", "end_datetime": null,
                   "driver_id": 1130, "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-07-05 08:00:00", "end_datetime": null,
                   "driver_id": 1130, "source": "dtako_events", "state": "出庫"}),
        ])
    }

    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(vec![json!({"start_datetime": "2026-07-02 10:00:00",
                       "end_datetime": "2026-07-02 14:00:00", "driver_id": 1130})])
    }
}

fn repo_with_fallback(base_url: &str) -> HttpKintaiEventsRepo {
    let fb: DynKintaiEventsRepo = Arc::new(FallbackStub);
    HttpKintaiEventsRepo::new(&cfg(base_url), Some(fb)).unwrap()
}

#[tokio::test]
async fn test_punches_come_from_the_fallback_and_are_merged_in_order() {
    let server = MockServer::start().await;
    stub_single(
        &server,
        single_body(
            vec![operation(
                "OP-1",
                &["運行NO", "対象乗務員CD", "開始日時", "イベント名"],
                vec![vec!["OP-1", "1130", "2026/07/02 08:00:00", "出庫"]],
            )],
            vec![],
        ),
    )
    .await;

    let rows = repo_with_fallback(&server.uri())
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    // 始業 (07:00 打刻、委譲先) → 出庫 (08:00、HTTP)。委譲先の dtako_events は捨てる
    let got: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| {
            (
                r["datetime"].as_str().unwrap(),
                r["source"].as_str().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("2026-07-02 07:00:00", "timecard"),
            ("2026-07-02 08:00:00", "dtako_events"),
        ]
    );
    assert_eq!(rows[1]["state"], "出庫", "dtako_events は HTTP 側の行");
}

#[tokio::test]
async fn test_all_events_merges_punches_from_the_fallback() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [],
            "next_after_driver_cd": null,
            "warnings": [],
        })))
        .mount(&server)
        .await;
    let rows = repo_with_fallback(&server.uri())
        .fetch_all_events_between("2026-07-05 00:00:00", "2026-07-06 00:00:00")
        .await
        .unwrap();
    assert_eq!(rows.len(), 1, "借りるのは timecard だけ");
    assert_eq!(rows[0]["source"], "timecard");
}

#[tokio::test]
async fn test_ferry_needs_the_fallback_and_is_fail_closed_without_it() {
    let server = MockServer::start().await;
    // 上流にフェリーの口は無い。委譲先が無ければ 503 (fail-closed)
    let err = repo(&server.uri())
        .fetch_ferry("2026-07", Some(1130))
        .await
        .unwrap_err();
    assert!(matches!(err, KintaiRepoError::NotConfigured));

    // オンプレ (委譲先あり) では従来どおり読める
    let rows = repo_with_fallback(&server.uri())
        .fetch_ferry("2026-07", Some(1130))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["driver_id"], 1130);
}

// ── token の渡し方 ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_token_command_output_is_sent_as_bearer() {
    // 当面の Google 認証: `gcloud auth print-identity-token` の出力を Bearer に載せる。
    // ここでは echo で代用する (取得方法をコードに焼いていないことの固定)。
    let server = MockServer::start().await;
    let mut c = cfg(&server.uri());
    c.auth_token = String::new();
    c.auth_token_command = "echo gcloud-identity-token".to_string();
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer gcloud-identity-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(single_body(vec![], vec![])))
        .expect(1)
        .mount(&server)
        .await;
    HttpKintaiEventsRepo::new(&c, None)
        .unwrap()
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_broken_token_command_fails_the_read() {
    let server = MockServer::start().await;
    let mut c = cfg(&server.uri());
    c.auth_token = String::new();
    c.auth_token_command = "false".to_string();
    let err = HttpKintaiEventsRepo::new(&c, None)
        .unwrap()
        .fetch_events("2026-07", 1130)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("auth token command"), "{err}");
    assert_eq!(server.received_requests().await.unwrap().len(), 0);
}

#[tokio::test]
async fn test_no_token_configured_sends_no_authorization_header() {
    let server = MockServer::start().await;
    let mut c = cfg(&server.uri());
    c.auth_token = String::new();
    Mock::given(method("GET"))
        .and(header(
            "x-tenant-id",
            "11111111-2222-3333-4444-555555555555",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(single_body(vec![], vec![])))
        .expect(1)
        .mount(&server)
        .await;
    HttpKintaiEventsRepo::new(&c, None)
        .unwrap()
        .fetch_events("2026-07", 1130)
        .await
        .unwrap();
    let reqs = server.received_requests().await.unwrap();
    assert!(reqs[0].headers.get("authorization").is_none());
}

#[tokio::test]
async fn test_empty_token_command_is_rejected_at_construction() {
    let mut c = cfg("http://127.0.0.1:1");
    c.auth_token = String::new();
    c.auth_token_command = "  ".to_string();
    // 空白だけの command は「無指定」と同じ (token 無しで組める)
    assert!(HttpKintaiEventsRepo::new(&c, None).is_ok());
}
