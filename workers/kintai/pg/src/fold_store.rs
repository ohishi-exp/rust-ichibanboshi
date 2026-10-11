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

use std::collections::{BTreeMap, HashSet};

use alc_worker_db::{PgClient, TenantTx};
use chrono::{DateTime, FixedOffset, NaiveDate};
use futures_util::lock::Mutex;
use kintai_kosoku::fold_flow::{FoldGateRow, FoldStoreIo};
use kintai_kosoku::kintai_fold::{
    day_part_columns, day_summary_columns, shift_columns, FoldUnit, StoredState, DELETE_SHIFTS_SQL,
    FOLD_GATE_SELECT_SQL, FOLD_GATE_UPSERT_SQL, INSERT_DAY_PARTS_SQL, INSERT_DAY_SUMMARIES_SQL,
    INSERT_SHIFTS_SQL, STORED_STATES_SQL,
};
use kintai_kosoku::kintai_push::{
    month_date_bounds, MONTH_OPERATIONS_SQL, MONTH_PUNCH_DIGEST_SQL, OPERATION_DRIVER_CDS_SQL,
};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
use uuid::Uuid;

use crate::{set_timeout, sources, Out};

/// [`FoldStore`] の失敗。
#[derive(Debug)]
pub enum FoldStoreError {
    /// 月が読めない (root の `KintaiPushError::NotConfigured("bad month: …")` と同じ文言)
    BadMonth(String),
    /// DB の失敗
    Db(Error),
}

impl std::fmt::Display for FoldStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BadMonth(m) => write!(f, "bad month: {m}"),
            Self::Db(e) => write!(f, "kintai fold db failed: {e}"),
        }
    }
}

impl From<Error> for FoldStoreError {
    fn from(e: Error) -> Self {
        Self::Db(e)
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
}
