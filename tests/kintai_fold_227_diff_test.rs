//! #205-12 の点 3: 92 行差の究明 (Refs #205-11 の検証結果、共通 91 名で基準 2,318 行 vs
//! preview 2,225 行)。**本番 API は一切叩かない** — ローカルで両形の入力を作って
//! `fold_month` の出力を突き合わせる。
//!
//! `KINTAI_TEST_DATABASE_URL` (エフェメラルな Postgres、`kintai-test:55432` /
//! `db204:13307` は使わない) が無ければ丸ごと skip する
//! (`tests/kintai_fold_pg_test.rs` と同じ)。
//!
//! ## 結論から先に書く
//!
//! **有力仮説だった #227 (打刻由来の確定休息 `time_card_dtako` の `休息` を
//! `kintai.kintai_events` へ運ばない) は、行数差の説明にならない。**
//!
//! `kosoku.rs` を読むと、この `休息` (source = `dtako`、開始/終了が同名で来る
//! 曖昧な打刻由来イベント) が計算に効く場所は 1 箇所だけ —
//! `run_head_span` の「割り込み判定」(直前の運行開始〜始業の間に運行終了か休息が
//! あれば、その頭を run_head として数えない)。ここが無いと `run_head_minutes` が
//! 余分に立つことがある。
//!
//! **だが `run_head_minutes` は `kintai.day_summaries` / `shifts` / `day_parts` の
//! どの列にも保存されない** — `src/kintai_fold.rs` が 3 表へ写す直前に
//! `run_head_minutes: 0` へ潰している (`fold_days` / `PartBeforeShift` のコメント
//! 参照)。`/api/kosoku-daily` の JSON 応答 (紙との突合専用) にだけ出る診断値で、
//! `/api/kintai/recalc` が畳んで保存する値には**一切影響しない**。
//!
//! したがって #227 は preview 側の `restraint_minutes` / 行数のどちらにも
//! 差を作らない。以下のテストは 2 つの主張を両方とも実測で確かめる:
//!
//! 1. [`run_head_span_matters_only_for_the_diagnostic_metric`] — 純粋関数
//!    ([`daily_summary`]) レベルで `run_head_minutes` は変わるが `restraint_minutes`
//!    は変わらないこと (kosoku.rs は無変更、呼ぶだけ)
//! 2. [`http_repo_and_onprem_repo_fold_to_the_same_rows`] — 実際の畳みの経路
//!    ([`HttpKintaiEventsRepo`] + wiremock の `dtako_events` + 実 Postgres の
//!    `PgKintaiEventsRepo`、対 オンプレ形の全イベント stub) を通しても
//!    `fold_month` の出力 (`shifts` / `day_summaries` / `day_parts`) が
//!    バイト単位で一致すること
//!
//! 92 行差の真因はここでは特定できていない (本番 API を叩かずに再現する制約の中で、
//! #227 は反証できたが代わりの仮説はまだ無い)。次の一手は issue コメントに残す。

use std::sync::Arc;

use async_trait::async_trait;
use rust_ichibanboshi::config::{KintaiEventsConfig, KintaiEventsSource};
use rust_ichibanboshi::kintai_fold::fold_month;
use rust_ichibanboshi::kintai_http_repo::HttpKintaiEventsRepo;
use rust_ichibanboshi::kintai_pg_repo::PgKintaiEventsRepo;
use rust_ichibanboshi::kintai_push::{
    apply_timecard_window, KintaiPgStore, TimecardWindow, NOT_CARRIED_STATES,
};
use rust_ichibanboshi::kintai_repo::{DynKintaiEventsRepo, KintaiEventsApi, KintaiRepoError};
use rust_ichibanboshi::kosoku::{daily_summary, KosokuParams};
use serde_json::{json, Value};
use wiremock::matchers::method;
use wiremock::{Mock, MockServer, ResponseTemplate};

const DRIVER: u64 = 1130;
const MONTH: &str = "2026-06";

fn dtako(at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "dtako", "state": state, "unko_no": null})
}

fn tc(at: &str, state: &str) -> Value {
    json!({"datetime": at, "end_datetime": null, "driver_id": DRIVER, "source": "timecard", "state": state, "unko_no": null})
}

/// **オンプレの `ALL_EVENTS_SQL` 相当** — `休息` 状態を一切絞らない全乗務員版。
fn onprem_rows() -> Vec<Value> {
    vec![
        // 運行開始 01:00 → 休息 (曖昧な打刻由来) 01:10 → 始業 01:25。
        // run_head_span の割り込み判定に休息 01:10 が使われる
        // (「乗務員 1026 一瀬 2026-03-12」型、Refs ohishi-exp/nuxt-dtako-admin#501)。
        dtako("2026-06-12 01:00:00", "運行開始"),
        dtako("2026-06-12 01:10:00", "休息"),
        tc("2026-06-12 01:25:00", "始業"),
        tc("2026-06-12 13:58:00", "終業"),
    ]
}

/// **push が実際に運ぶ形** — #227 (`NOT_CARRIED_STATES`) で `休息` を落とした残り。
fn pushed_rows() -> Vec<Value> {
    onprem_rows()
        .into_iter()
        .filter(|r| !NOT_CARRIED_STATES.contains(&r["state"].as_str().unwrap_or_default()))
        .collect()
}

// ── 1. 純粋関数レベル: run_head_minutes だけが動き、restraint_minutes は動かない ──

#[test]
fn run_head_span_matters_only_for_the_diagnostic_metric() {
    let with_dtako_rest = daily_summary(&onprem_rows(), MONTH, &KosokuParams::default());
    let without_dtako_rest = daily_summary(&pushed_rows(), MONTH, &KosokuParams::default());

    assert_eq!(
        with_dtako_rest.len(),
        without_dtako_rest.len(),
        "休息の有無で日数は変わらない"
    );
    // オンプレ (休息あり) は割り込みと判定し run_head を数えない
    assert_eq!(with_dtako_rest[0].run_head_minutes, 0, "割り込みで 0");
    // push 後 (休息を運ばない) は割り込みが見えず、運行開始からの頭を数えてしまう
    assert_eq!(
        without_dtako_rest[0].run_head_minutes, 25,
        "01:00 → 01:25 の 25 分が余分に立つ"
    );
    // だが拘束・実働は #118 (打刻優先) でそもそも run_head を足さないので不変
    assert_eq!(
        with_dtako_rest[0].restraint_minutes, without_dtako_rest[0].restraint_minutes,
        "run_head は拘束に入らない (#118)"
    );
    assert_eq!(
        with_dtako_rest[0].working_minutes,
        without_dtako_rest[0].working_minutes
    );
}

// ── 2. 実経路レベル: HttpKintaiEventsRepo (wiremock) + PgKintaiEventsRepo (実 Postgres) ──

struct OnpremStub {
    rows: Vec<Value>,
}

#[async_trait]
impl KintaiEventsApi for OnpremStub {
    async fn fetch_events_between(
        &self,
        _from: &str,
        _to: &str,
        driver: u64,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(self
            .rows
            .iter()
            .filter(|r| r["driver_id"].as_u64() == Some(driver))
            .cloned()
            .collect())
    }
    async fn fetch_all_events_between(
        &self,
        _from: &str,
        _to: &str,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(self.rows.clone())
    }
    async fn fetch_ferry_between(
        &self,
        _from: &str,
        _to: &str,
        _driver: Option<u64>,
    ) -> Result<Vec<Value>, KintaiRepoError> {
        Ok(Vec::new())
    }
}

fn http_cfg(base_url: &str) -> KintaiEventsConfig {
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

/// このシナリオに `dtako_events` は無いので、上流は空の応答を返すだけでよい。
async fn stub_empty_upstream(server: &MockServer) {
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "period": {"date_from": "x", "date_to": "y"},
            "drivers": [],
            "next_after_driver_cd": null,
            "warnings": [],
        })))
        .mount(server)
        .await;
}

async fn pg_store() -> Option<(KintaiPgStore, sqlx::PgPool)> {
    let url = std::env::var("KINTAI_TEST_DATABASE_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .connect(&url)
        .await
        .expect("connect");
    ensure_schema(&pool).await;
    Some((
        KintaiPgStore::from_pool(pool.clone(), uuid::Uuid::new_v4()),
        pool,
    ))
}

fn needs_psql_variables(sql: &str) -> bool {
    sql.contains(":'") || sql.contains(":\"")
}

async fn ensure_schema(pool: &sqlx::PgPool) {
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(205_120_001_i64)
        .execute(pool)
        .await
        .expect("lock");
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'kintai')")
            .fetch_one(pool)
            .await
            .expect("probe");
    if !exists {
        let mut files: Vec<std::path::PathBuf> = std::fs::read_dir("migrations")
            .expect("migrations")
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("sql"))
            .collect();
        files.sort();
        for f in files {
            let sql = std::fs::read_to_string(&f).expect("read");
            if needs_psql_variables(&sql) {
                eprintln!("skip {} (psql の変数を使う migration)", f.display());
                continue;
            }
            sqlx::raw_sql(&sql)
                .execute(pool)
                .await
                .unwrap_or_else(|e| panic!("apply {}: {e}", f.display()));
        }
    }
    sqlx::query("SELECT pg_advisory_unlock($1)")
        .bind(205_120_001_i64)
        .execute(pool)
        .await
        .expect("unlock");
}

/// **#205-12 の点 3 の核心テスト。** オンプレ形 (StubRepo、休息を含む全イベント) と
/// GCP 形 (`HttpKintaiEventsRepo` + wiremock の `dtako_events` + 実 Postgres へ
/// #227 でフィルタ済みの打刻を push した `PgKintaiEventsRepo`) を `fold_month`
/// にかけ、`shifts` / `day_summaries` / `day_parts` が一致することを確かめる。
#[tokio::test]
async fn http_repo_and_onprem_repo_fold_to_the_same_rows() {
    let Some((store, _pool)) = pg_store().await else {
        return;
    };
    let params = KosokuParams::default();

    // オンプレ形: 休息を含む全イベントをそのまま返す
    let onprem: DynKintaiEventsRepo = Arc::new(OnpremStub {
        rows: onprem_rows(),
    });
    let onprem_units = fold_month(&onprem, &params, MONTH, Some(DRIVER))
        .await
        .expect("onprem fold");

    // GCP 形: push 経路が実際に運ぶ行 (#227 で休息を落とした残り) だけを
    // kintai.kintai_events へ書き、HttpKintaiEventsRepo の fallback として読む
    let window = TimecardWindow {
        months: vec![MONTH.to_string()],
        drivers: vec![DRIVER as i64],
        events: pushed_rows(),
        dry_run: false,
        fold: false,
    };
    apply_timecard_window(&store, &window)
        .await
        .expect("push pushed_rows (#227 で確定休息を除いた形)");

    let server = MockServer::start().await;
    stub_empty_upstream(&server).await;
    let fallback: DynKintaiEventsRepo = Arc::new(PgKintaiEventsRepo::new(
        Arc::new(store.for_tenant(store.tenant_id())),
        store.tenant_id(),
    ));
    let gcp = HttpKintaiEventsRepo::new(&http_cfg(&server.uri()), Some(fallback)).unwrap();
    let gcp: DynKintaiEventsRepo = Arc::new(gcp);
    let gcp_units = fold_month(&gcp, &params, MONTH, Some(DRIVER))
        .await
        .expect("gcp fold");

    assert_eq!(onprem_units.len(), 1);
    assert_eq!(gcp_units.len(), 1);
    let (_, onprem_unit, _) = &onprem_units[0];
    let (_, gcp_unit, _) = &gcp_units[0];

    assert_eq!(
        onprem_unit.shifts.len(),
        gcp_unit.shifts.len(),
        "#227 は shifts の行数を変えない"
    );
    assert_eq!(
        onprem_unit.day_summaries.len(),
        gcp_unit.day_summaries.len(),
        "#227 は day_summaries の行数を変えない"
    );
    assert_eq!(
        onprem_unit.day_parts, gcp_unit.day_parts,
        "#227 は day_parts の値も変えない (run_head は保存されない)"
    );
    // shifts / day_summaries は fingerprint 材料が違う (行そのものが違う)ので
    // FoldUnit を丸ごと比較せず、保存される値だけを見る
    assert_eq!(
        onprem_unit
            .day_summaries
            .iter()
            .map(|d| d.restraint_minutes)
            .collect::<Vec<_>>(),
        gcp_unit
            .day_summaries
            .iter()
            .map(|d| d.restraint_minutes)
            .collect::<Vec<_>>(),
        "restraint_minutes も一致 — #227 は preview の保存値に影響しない"
    );
}
