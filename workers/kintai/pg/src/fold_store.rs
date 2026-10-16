//! fold の書き込み側 [`FoldStoreIo`] の Worker 実装 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `impl FoldStoreIo for KintaiPgStore` (`src/kintai_fold.rs`、sqlx) と同じ SQL・同じ bind の束を、
//! alc-worker-db の `PgClient::tenant_tx` の中で型付きの名前なしの文 (`query_typed*` / `execute_typed`) だけで流す。
//! SQL と束は共有 crate (`kintai_kosoku::{kintai_fold, kintai_push}`) のもの。表の中身が root と同じことは
//! `tests/fold_store_parity.rs` (実 DB)。
//!
//! - 接続は 1 本を [`Mutex`] で包んで持ち、各メソッドが `lock` してから `tenant_tx` を 1 回呼ぶ
//!   (trait は `&self`、`tenant_tx` は `&mut PgClient`)。メソッドごとに繋ぎ直さない
//! - transaction の単位は root と同じ: `write_unit` は 1 乗務員 1 か月で 1 transaction、他は 1 文 1 transaction
//! - root の gate の読み書き・digest・運行一覧は pool 直 (transaction もテナント設定も無し) だが、ここは全部
//!   `tenant_tx` を通る (経路はより厳しい。結果が同じことは parity で見る)
//! - 全 SQL の `$1` はテナント ([`FoldStore::new`] で固定)
//! - 打刻の置き換え [`TimecardWindowIo`] (window の口) もここに impl する — 置き換えと畳み直しを同じ接続で回すため。
//!   root の `impl TimecardWindowIo for KintaiPgStore` との一致は `tests/window_parity.rs` (実 DB)
//! - fold の読みのうち送った写しの 3 本 (`kintai_logic::fold_events::PushedEventsRead`) もここに impl する — 書きと同じ接続で
//!   読むため。root の `PgKintaiEventsRepo` との一致は `tests/fold_events_parity.rs` (実 DB)

use std::collections::{BTreeMap, BTreeSet, HashSet};

use alc_worker_db::{kind, PgClient, TenantTx};
use chrono::{DateTime, FixedOffset, NaiveDate};
use futures_util::lock::Mutex;
use kintai_kosoku::fold_flow::{BadMonth, FoldGateRow, FoldStoreIo};
use kintai_kosoku::kintai_fold::{
    day_part_columns, day_summary_columns, months_span, shift_columns, FoldUnit, StaleReport,
    StoredState, DELETE_SHIFTS_SQL, FOLD_GATE_SELECT_SQL, FOLD_GATE_UPSERT_SQL,
    INSERT_DAY_PARTS_SQL, INSERT_DAY_SUMMARIES_SQL, INSERT_SHIFTS_SQL, RECALC_DRIVER_PAGE_SQL,
    STALE_STATE_SQL, STORED_STATES_SQL,
};
use kintai_kosoku::kintai_push::{
    jst_day_bounds, month_date_bounds, DriverPlan, MONTH_OPERATIONS_SQL, MONTH_PUNCH_DIGEST_SQL,
    OPERATION_DRIVER_CDS_SQL, STORED_WINDOW_SIGNATURES_SQL,
};
use kintai_kosoku::pushed_events::{
    all_event_json, head_punch, head_run_end, ALL_EVENTS_SQL, HEAD_PUNCHES_SQL, HEAD_RUN_ENDS_SQL,
};
use kintai_kosoku::route_flow::{BadWindow, TimecardWindowIo};
use kintai_kosoku::window::HeadPunch;
use kintai_logic::common::{db_fail, Fail};
use kintai_logic::fold_events::{PushedEventsRead, Span};
use kintai_logic::fold_routes::{not_configured, read_failed, DB_WHAT};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::{set_timeout, sources, Out};

/// [`FoldStore`] の失敗。fold の段取り (`kintai_kosoku::route_flow`) は読み (`FoldEventsIo`) と窓の失敗もここへ寄せる。
#[derive(Debug)]
pub enum FoldStoreError {
    /// 月が読めない (root の `KintaiPushError::NotConfigured("bad month: …")` と同じ文言)
    BadMonth(String),
    /// 月の並び・窓が読めない (`months_span`・`window_spans` の文面そのまま: `bad month: …` / `months が空です`)
    BadMonths(String),
    /// fold の読み (alc の RPC・送った写し) の失敗 (root の `KintaiPushError::Read`)
    Read(Fail),
    /// DB の失敗
    Db(Error),
}

impl FoldStoreError {
    /// 応答の status と本文 (root の `map_push_err` と同じ分け方: 月・窓 = 503、読み・DB = 502)。
    /// **502 の本文には種別だけを載せる** — DB の message は出さず kit の `kind` だけ、上流の本文は
    /// `kintai_logic::fold_routes::kind_only` で削る。
    pub fn into_fail(&self) -> Fail {
        match self {
            Self::BadMonth(m) => not_configured(&format!("bad month: {m}")),
            Self::BadMonths(m) => not_configured(m),
            Self::Read(f) => read_failed(f),
            Self::Db(e) => db_fail(DB_WHAT, &kind(e)),
        }
    }
}

/// 本文は [`FoldStoreError::into_fail`] と同じ (window の `stale_error` はこれを載せる。DB の message を出さない)。
impl std::fmt::Display for FoldStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.into_fail().body)
    }
}

impl From<Error> for FoldStoreError {
    fn from(e: Error) -> Self {
        Self::Db(e)
    }
}

/// fold の読み (`kintai_logic::fold_events::WorkerFoldEvents`) の失敗。
impl From<Fail> for FoldStoreError {
    fn from(f: Fail) -> Self {
        Self::Read(f)
    }
}

/// 段取りが月を読めなかった。
impl From<BadMonth> for FoldStoreError {
    fn from(e: BadMonth) -> Self {
        Self::BadMonth(e.0)
    }
}

/// 窓の月が読めなかった (`apply_timecard_window`)。
impl From<BadWindow> for FoldStoreError {
    fn from(e: BadWindow) -> Self {
        Self::BadMonths(e.0)
    }
}

/// Supabase の勤怠スキーマ (`kintai.*`) の fold の読み書き (1 接続・1 テナント)。
pub struct FoldStore {
    pg: Mutex<PgClient>,
    tenant: Uuid,
}

impl FoldStore {
    /// 接続とテナントを受け取る (Worker は Hyperdrive の接続と `KINTAI_TENANT_ID`)。
    pub fn new(pg: PgClient, tenant: Uuid) -> Self {
        Self {
            pg: Mutex::new(pg),
            tenant,
        }
    }

    /// `tenant_tx` を 1 回 (`statement_timeout` を張ってから本文)。
    async fn tx<T, F>(&self, f: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: for<'t> FnOnce(
                &'t TenantTx<'t>,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<T, Error>> + Send + 't>,
            > + Send
            + 'static,
    {
        let mut pg = self.pg.lock().await;
        let out = pg
            .tenant_tx(self.tenant, move |tx| {
                Box::pin(async move {
                    set_timeout(tx).await?;
                    f(tx).await.map(Out)
                })
            })
            .await?;
        Ok(out.0)
    }
}

fn bounds(month: &str) -> Result<(NaiveDate, NaiveDate), FoldStoreError> {
    month_date_bounds(month).ok_or_else(|| FoldStoreError::BadMonth(month.to_string()))
}

fn stored_state(r: &Row) -> Result<(i64, StoredState), Error> {
    Ok((
        r.try_get("driver_cd")?,
        StoredState {
            fingerprints: r.try_get("fps")?,
            shifts: r.try_get("n_shifts")?,
            day_summaries: r.try_get("n_days")?,
            day_parts: r.try_get("n_parts")?,
        },
    ))
}

impl FoldStoreIo for FoldStore {
    type Error = FoldStoreError;

    async fn stored_states(
        &self,
        drivers: &[i64],
        month: &str,
    ) -> Result<BTreeMap<i64, StoredState>, FoldStoreError> {
        let (m0, m1) = bounds(month)?;
        if drivers.is_empty() {
            return Ok(BTreeMap::new());
        }
        let (tenant, drivers) = (self.tenant, drivers.to_vec());
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let rows = tx
                        .query_typed(
                            STORED_STATES_SQL,
                            &[
                                (&tenant, Type::UUID),
                                (&drivers, Type::INT8_ARRAY),
                                (&m0, Type::DATE),
                                (&m1, Type::DATE),
                            ],
                        )
                        .await?;
                    rows.iter().map(stored_state).collect::<Result<_, _>>()
                })
            })
            .await?;
        Ok(out)
    }

    /// root の `write_unit_versioned` と同じ順: DELETE (day_summaries / day_parts は CASCADE) → 3 表の INSERT。
    async fn write_unit(
        &self,
        month: &str,
        unit: &FoldUnit,
        fingerprint: &str,
        logic_version: &str,
    ) -> Result<(), FoldStoreError> {
        let (m0, m1) = bounds(month)?;
        let tenant = self.tenant;
        let driver_cd = unit.driver_cd;
        let (shifts, days, parts) = (
            shift_columns(unit),
            day_summary_columns(unit),
            day_part_columns(unit),
        );
        let (fp, ver) = (fingerprint.to_string(), logic_version.to_string());
        self.tx(move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    DELETE_SHIFTS_SQL,
                    &[
                        (&tenant, Type::UUID),
                        (&driver_cd, Type::INT8),
                        (&m0, Type::DATE),
                        (&m1, Type::DATE),
                    ],
                )
                .await?;
                for c in &shifts {
                    tx.execute_typed(
                        INSERT_SHIFTS_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&c.driver_cd, Type::INT8_ARRAY),
                            (&c.start_at, Type::TIMESTAMPTZ_ARRAY),
                            (&c.end_at, Type::TIMESTAMPTZ_ARRAY),
                            (&c.shift_source, Type::TEXT_ARRAY),
                            (&fp, Type::TEXT),
                            (&ver, Type::TEXT),
                        ],
                    )
                    .await?;
                }
                for c in &days {
                    let mut params: Vec<(&(dyn ToSql + Sync), Type)> = vec![
                        (&tenant, Type::UUID),
                        (&c.driver_cd, Type::INT8_ARRAY),
                        (&c.date, Type::DATE_ARRAY),
                        (&c.shift_start_at, Type::TIMESTAMPTZ_ARRAY),
                        (&c.shift_source, Type::TEXT_ARRAY),
                    ];
                    // 分の 11 列 ($6〜$16) は束の並びが SQL の並び
                    for m in &c.minutes {
                        params.push((m, Type::INT4_ARRAY));
                    }
                    params.push((&c.non_working, Type::JSONB_ARRAY));
                    params.push((&c.count_date, Type::DATE_ARRAY));
                    params.push((&fp, Type::TEXT));
                    params.push((&ver, Type::TEXT));
                    tx.execute_typed(INSERT_DAY_SUMMARIES_SQL, &params).await?;
                }
                for c in &parts {
                    tx.execute_typed(
                        INSERT_DAY_PARTS_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&c.driver_cd, Type::INT8_ARRAY),
                            (&c.shift_start_at, Type::TIMESTAMPTZ_ARRAY),
                            (&c.date, Type::DATE_ARRAY),
                            (&c.restraint_minutes, Type::INT4_ARRAY),
                            (&c.working_minutes, Type::INT4_ARRAY),
                            (&c.night_minutes, Type::INT4_ARRAY),
                        ],
                    )
                    .await?;
                }
                Ok(())
            })
        })
        .await?;
        Ok(())
    }

    async fn read_fold_gate(&self, month: &str) -> Result<Option<FoldGateRow>, FoldStoreError> {
        let (tenant, month) = (self.tenant, month.to_string());
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let row = tx
                        .query_typed_opt(
                            FOLD_GATE_SELECT_SQL,
                            &[(&tenant, Type::UUID), (&month, Type::TEXT)],
                        )
                        .await?;
                    row.map(|r| -> Result<_, Error> {
                        Ok(FoldGateRow {
                            dtako_digest: r.try_get("dtako_digest")?,
                            punch_digest: r.try_get("punch_digest")?,
                            logic_version: r.try_get("logic_version")?,
                        })
                    })
                    .transpose()
                })
            })
            .await?;
        Ok(out)
    }

    async fn write_fold_gate(&self, month: &str, gate: &FoldGateRow) -> Result<(), FoldStoreError> {
        let (tenant, month, gate) = (self.tenant, month.to_string(), gate.clone());
        self.tx(move |tx| {
            Box::pin(async move {
                tx.execute_typed(
                    FOLD_GATE_UPSERT_SQL,
                    &[
                        (&tenant, Type::UUID),
                        (&month, Type::TEXT),
                        (&gate.dtako_digest, Type::TEXT),
                        (&gate.punch_digest, Type::TEXT),
                        (&gate.logic_version, Type::TEXT),
                    ],
                )
                .await
                .map(|_| ())
            })
        })
        .await?;
        Ok(())
    }

    async fn month_punch_digest(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<String, FoldStoreError> {
        let tenant = self.tenant;
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let src = sources();
                    tx.query_typed_one(
                        MONTH_PUNCH_DIGEST_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&from, Type::TIMESTAMPTZ),
                            (&to, Type::TIMESTAMPTZ),
                            (&src, Type::TEXT_ARRAY),
                        ],
                    )
                    .await?
                    .try_get("digest")
                })
            })
            .await?;
        Ok(out)
    }

    async fn month_operations(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<Vec<(i64, String, NaiveDate, NaiveDate)>, FoldStoreError> {
        let tenant = self.tenant;
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let src = sources();
                    let rows = tx
                        .query_typed(
                            MONTH_OPERATIONS_SQL,
                            &[
                                (&tenant, Type::UUID),
                                (&from, Type::TIMESTAMPTZ),
                                (&to, Type::TIMESTAMPTZ),
                                (&src, Type::TEXT_ARRAY),
                            ],
                        )
                        .await?;
                    let op = |r: &Row| -> Result<_, Error> {
                        Ok((
                            r.try_get("driver_cd")?,
                            r.try_get("unko_no")?,
                            r.try_get("first_date")?,
                            r.try_get("last_date")?,
                        ))
                    };
                    rows.iter().map(op).collect::<Result<Vec<_>, _>>()
                })
            })
            .await?;
        Ok(out)
    }

    async fn operation_driver_cds(&self) -> Result<HashSet<i64>, FoldStoreError> {
        let tenant = self.tenant;
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let src = sources();
                    let rows = tx
                        .query_typed(
                            OPERATION_DRIVER_CDS_SQL,
                            &[(&tenant, Type::UUID), (&src, Type::TEXT_ARRAY)],
                        )
                        .await?;
                    rows.iter()
                        .map(|r| r.try_get("driver_cd"))
                        .collect::<Result<HashSet<i64>, _>>()
                })
            })
            .await?;
        Ok(out)
    }

    async fn stale_state(
        &self,
        months: &[String],
        logic_version: &str,
    ) -> Result<StaleReport, FoldStoreError> {
        let (lo, hi) = months_span(months).map_err(FoldStoreError::BadMonths)?;
        let (tenant, version) = (self.tenant, logic_version.to_string());
        let (drivers, versions) = self
            .tx(move |tx| {
                Box::pin(async move {
                    let row = tx
                        .query_typed_one(
                            STALE_STATE_SQL,
                            &[
                                (&tenant, Type::UUID),
                                (&lo, Type::DATE),
                                (&hi, Type::DATE),
                                (&version, Type::TEXT),
                            ],
                        )
                        .await?;
                    let drivers: i64 = row.try_get("stale_drivers")?;
                    let versions: Vec<String> = row.try_get("versions")?;
                    Ok((drivers, versions))
                })
            })
            .await?;
        Ok(StaleReport {
            logic_version: logic_version.to_string(),
            drivers: drivers as usize,
            versions,
        })
    }

    /// root と同じ束: `$2`/`$3` = 月の JST 00:00 (TIMESTAMPTZ)、`$4`/`$5` = 月の DATE、`$6` = `after` (無ければ `i64::MIN`)。
    async fn recalc_driver_page(
        &self,
        month: &str,
        logic_version: &str,
        extra: &BTreeSet<i64>,
        after: Option<i64>,
        stale_only: bool,
        limit: usize,
    ) -> Result<Vec<i64>, FoldStoreError> {
        let (m0, m1) = bounds(month)?;
        let (from, to) = (jst_day_bounds(m0).0, jst_day_bounds(m1).0);
        let (tenant, version) = (self.tenant, logic_version.to_string());
        let extra: Vec<i64> = extra.iter().copied().collect();
        let (after, limit) = (after.unwrap_or(i64::MIN), limit as i64);
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let rows = tx
                        .query_typed(
                            RECALC_DRIVER_PAGE_SQL,
                            &[
                                (&tenant, Type::UUID),
                                (&from, Type::TIMESTAMPTZ),
                                (&to, Type::TIMESTAMPTZ),
                                (&m0, Type::DATE),
                                (&m1, Type::DATE),
                                (&after, Type::INT8),
                                (&stale_only, Type::BOOL),
                                (&version, Type::TEXT),
                                (&limit, Type::INT8),
                                (&extra, Type::INT8_ARRAY),
                            ],
                        )
                        .await?;
                    rows.iter()
                        .map(|r| r.try_get("driver_cd"))
                        .collect::<Result<Vec<i64>, _>>()
                })
            })
            .await?;
        Ok(out)
    }
}

/// 打刻の置き換え (`POST /api/kintai/timecard/window`)。root の `impl TimecardWindowIo for KintaiPgStore` と同じ SQL・同じ値。
impl TimecardWindowIo for FoldStore {
    type Error = FoldStoreError;

    /// 名乗った乗務員の `[from, to)` の日別署名 (`STORED_WINDOW_SIGNATURES_SQL`、1 transaction)。
    async fn stored_window_signatures(
        &self,
        drivers: &[i64],
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<BTreeMap<i64, BTreeMap<NaiveDate, String>>, FoldStoreError> {
        let (tenant, drivers) = (self.tenant, drivers.to_vec());
        let out = self
            .tx(move |tx| {
                Box::pin(async move {
                    let src = sources();
                    let rows = tx
                        .query_typed(
                            STORED_WINDOW_SIGNATURES_SQL,
                            &[
                                (&tenant, Type::UUID),
                                (&drivers, Type::INT8_ARRAY),
                                (&from, Type::TIMESTAMPTZ),
                                (&to, Type::TIMESTAMPTZ),
                                (&src, Type::TEXT_ARRAY),
                            ],
                        )
                        .await?;
                    let mut out: BTreeMap<i64, BTreeMap<NaiveDate, String>> = BTreeMap::new();
                    for r in &rows {
                        let sig: String = r.try_get("sig")?;
                        let (cd, d): (i64, NaiveDate) = (r.try_get("driver_cd")?, r.try_get("d")?);
                        out.entry(cd).or_default().insert(d, sig);
                    }
                    Ok(out)
                })
            })
            .await?;
        Ok(out)
    }

    /// 計画どおりに日を丸ごと置き換える (窓ぜんたいで 1 transaction。変更履歴も残す)。既存の [`crate::replace_window`]
    /// (計画を値で受ける) を同じ接続で呼ぶ。
    async fn replace_window(
        &self,
        plans: &BTreeMap<i64, DriverPlan>,
    ) -> Result<(), FoldStoreError> {
        let mut pg = self.pg.lock().await;
        crate::replace_window(&mut pg, self.tenant, plans.clone()).await?;
        Ok(())
    }
}

/// fold の読みのうち、relay が送った打刻の写し (`kintai.kintai_events`) の 3 本 (`WorkerFoldEvents` の `pushed`)。
/// root の `PgKintaiEventsRepo` (`fetch_all_events_between`・`fetch_month_head_anchors`) と同じ SQL・同じ bind・同じ行の形
/// (`kintai_kosoku::pushed_events`)。書きと同じ接続なので、window の口は置き換えた写しをそのまま畳み直しで読む。
/// 失敗は 502 `kintai_events read failed: <種別>` (root の `db_err` と同じ頭。DB の message・接続先は出さない)。
impl PushedEventsRead for FoldStore {
    async fn pushed_all_events(&self, span: Span) -> Result<Vec<serde_json::Value>, Fail> {
        let (tenant, (from, to)) = (self.tenant, span);
        let out = self.tx(move |tx| {
            Box::pin(async move {
                let src = sources();
                let rows = tx
                    .query_typed(
                        ALL_EVENTS_SQL,
                        &[
                            (&tenant, Type::UUID),
                            (&from, Type::TIMESTAMPTZ),
                            (&to, Type::TIMESTAMPTZ),
                            (&src, Type::TEXT_ARRAY),
                        ],
                    )
                    .await?;
                let row = |r: &Row| -> Result<_, Error> {
                    Ok(all_event_json(
                        r.try_get("datetime")?,
                        r.try_get("driver_id")?,
                        r.try_get("source")?,
                        r.try_get("state")?,
                    ))
                };
                rows.iter().map(row).collect::<Result<Vec<_>, _>>()
            })
        });
        out.await.map_err(pushed_fail)
    }

    async fn pushed_head_run_ends(&self, span: Span) -> Result<Vec<(u64, String)>, Fail> {
        let (tenant, (from, to)) = (self.tenant, span);
        let out = self.tx(move |tx| {
            Box::pin(async move {
                let rows = tx
                    .query_typed(HEAD_RUN_ENDS_SQL, &window_params(&tenant, &from, &to))
                    .await?;
                let row = |r: &Row| -> Result<_, Error> {
                    Ok(head_run_end(r.try_get("driver_cd")?, r.try_get("unko_no")?))
                };
                rows.iter().map(row).collect::<Result<Vec<_>, _>>()
            })
        });
        out.await.map_err(pushed_fail)
    }

    async fn pushed_head_punches(&self, span: Span) -> Result<Vec<HeadPunch>, Fail> {
        let (tenant, (from, to)) = (self.tenant, span);
        let out = self.tx(move |tx| {
            Box::pin(async move {
                let rows = tx
                    .query_typed(HEAD_PUNCHES_SQL, &window_params(&tenant, &from, &to))
                    .await?;
                let row = |r: &Row| -> Result<_, Error> {
                    Ok(head_punch(
                        r.try_get("driver_cd")?,
                        r.try_get("first_state")?,
                        r.try_get("last_start")?,
                        r.try_get("last_end")?,
                    ))
                };
                rows.iter().map(row).collect::<Result<Vec<_>, _>>()
            })
        });
        out.await.map_err(pushed_fail)
    }
}

/// 遡り起点の 2 本の bind (`$1` テナント・`$2`/`$3` 窓)。
fn window_params<'a>(
    tenant: &'a Uuid,
    from: &'a DateTime<FixedOffset>,
    to: &'a DateTime<FixedOffset>,
) -> [(&'a (dyn ToSql + Sync), Type); 3] {
    [
        (tenant, Type::UUID),
        (from, Type::TIMESTAMPTZ),
        (to, Type::TIMESTAMPTZ),
    ]
}

/// 写しの読みの失敗の本文の頭 (root の `kintai_pg_repo::db_err` と同じ)。
pub const PUSHED_WHAT: &str = "kintai_events read";

/// 写しの読みの失敗 (502、種別だけ)。
fn pushed_fail(e: Error) -> Fail {
    db_fail(PUSHED_WHAT, &kind(&e))
}
