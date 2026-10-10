//! 打刻を Supabase (`kintai` スキーマ) へ push する (Refs #205 実装計画 04)。
//!
//! `time_card_dstate` (打刻) と `time_card_dtako` (運行の確定イベント) を
//! `kintai.kintai_events` へ写す。**畳まない** — ここは #205 の 6 層構成でいう
//! 「入力」層で、改修中に出力が変わったとき入力へ遡るために持つ (決定 6)。
//! 読み出し経路はこの表を見ない。
//!
//! デジタコ生イベント (`dtako_events`) は**対象外**。R2 に
//! `{tenant}/unko/{unko_no}/KUDGIVT.csv` として永続化済みと #204 で実測確認したため
//! (決定 5)。よって push するのは `source` が `timecard` / `dtako` の 2 つだけ。
//!
//! ## 方向は push だけ
//!
//! GCP からオンプレへは到達できないので、書くのは常にオンプレ側
//! (`ohishi-data` の `rust-ichibanboshi`)。**`--apply` を付けない限り書かない** —
//! 既定は dry-run。
//!
//! ## 日単位チェックサムで差分を検知する
//!
//! 毎回全件を消して入れ直すと、変わっていない日まで書き換わって `ingested_at` が
//! 動き、「いつ入力が変わったか」が読めなくなる。そこで **(乗務員, 暦日) ごとの
//! 署名**を両側で作って突き合わせ、**違う日だけ** delete-then-insert する。
//!
//! 署名は 1 行を `YYYY-MM-DD HH:MM:SS|state|source|unko_no` に畳んで `\n` で連ね、
//! sha256 を取ったもの。突き合わせる相手は Postgres 側の同じ式
//! ([`STORED_SIGNATURES_SQL`]) で、こちらは索引
//! (`kintai_events_driver_time` の INCLUDE) だけで済む。
//!
//! - **`raw` は署名に入れない。** 追跡用のメタデータで、これが変わっても勤怠の
//!   入力は変わらない。入れると上流が列を 1 つ足しただけで全日が差分になる
//! - **並べ替えは `COLLATE "C"`。** Postgres の既定 collation は locale 依存で、
//!   日本語のイベント名では Rust の `str` の順 (UTF-8 バイト順) と一致しない。
//!   揃えないと中身が同じでも署名が毎回割れて、全日を書き直し続ける
//! - 時刻は `AT TIME ZONE 'Asia/Tokyo'` で JST の壁時計に戻してから文字列にする。
//!   `DATE_FORMAT` で文字列にしてから畳む [`crate::kintai_version`] と同じ理由で、
//!   driver の時刻型と timezone 解釈を署名に持ち込まないため
//!
//! ## 主キーの衝突は Rust 側で決着させる
//!
//! `kintai.kintai_events` の PK は `(tenant_id, driver_cd, occurred_at, state)` で
//! **`source` を含まない**。同じ乗務員の同じ秒に同じ state が
//! `time_card_dstate` と `time_card_dtako` の両方にあると衝突する。
//!
//! DB 側で `ON CONFLICT DO NOTHING` に任せると「どちらが残るか」が挿入順で決まり、
//! 署名が Rust 側の計算と割れる。よって **[`dedup_events`] が挿入前に決着させる** —
//! `timecard` を残す (人が確定させた打刻の方が上位) 。署名は残った側だけで作る。

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};

use crate::config::KintaiPushConfig;
use crate::kintai_repo::{exact_month_range, DynKintaiEventsRepo, KintaiRepoError};

/// 純粋部分 (生行の写し・重複の決着・署名・差分の計画・SQL 定数・bind の束) は共有 crate に移した (Refs #322)。
/// ここは sqlx の pool・transaction・bind だけを持つ。呼び出し側のパスを変えないよう再 export する。
pub use kintai_kosoku::kintai_push::{
    day_signature, dedup_events, delete_days, diff_days, event_columns, group_by_date,
    jst_day_bounds, month_date_bounds, parse_row, parse_rows, plan_batch, plan_received_batch,
    plan_window, window_spans, DayDiff, DayDiffKind, DriverPlan, ParseOutcome, PushEvent,
    PushReport, RejectReason, TimecardBatch, TimecardBatchResult, TimecardWindow,
    TimecardWindowResult, ALLOWED_STATES, DATETIME_FORMAT, DELETE_DAYS_SQL, INSERT_CHUNK,
    INSERT_EVENTS_SQL, JST_OFFSET_SECONDS, MAX_REPORTED_STATES, MONTH_PUNCH_DIGEST_SQL,
    NOT_CARRIED_STATES, PUSHED_SOURCES, STORED_SIGNATURES_SQL, STORED_WINDOW_SIGNATURES_SQL,
};
/// **オンプレ側の運行一覧** (Refs #205 の 37)。GCP 側 (alc の etags) と突き合わせて
/// 「オンプレに在って GCP に無い運行」を名指しするための材料。
///
/// 実体は押し込み済みの `kintai.kintai_events` で、`unko_no` を持つのは `dtako`
/// (= オンプレ MariaDB の `time_card_dtako`) 由来の行だけ ([`PUSHED_SOURCES`])。
/// 打刻だけの行 (`timecard`) は `unko_no` が NULL なので自然に落ちる。
///
/// **窓は `fold_month` が実際に読む窓と同じにする** — 終端は
/// [`crate::kintai_repo::month_range`] (翌月 2 日)、始端は月初。ただし月初をまたぐ
/// 運行・勤務の遡り起点がある月は、始端を起点の最小 (`from_global`、
/// [`crate::kintai_fold::read_window`]) まで下げる (Refs ohishi-exp/nuxt-dtako-admin#1123)。
/// 窓は呼び出し側 ([`crate::kintai_fold`] の月ゲート) が渡す。ずらすと「fold の入力には
/// 在るのに突合には出ない」運行ができる。日付は署名 SQL と同じく JST の暦日で返す。
pub const MONTH_OPERATIONS_SQL: &str = r#"
SELECT driver_cd,
       unko_no,
       min((occurred_at AT TIME ZONE 'Asia/Tokyo')::date) AS first_date,
       max((occurred_at AT TIME ZONE 'Asia/Tokyo')::date) AS last_date
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
   AND source = ANY($4) AND unko_no IS NOT NULL AND unko_no <> ''
 GROUP BY 1, 2
 ORDER BY 1, 2
"#;

/// **押し込み済みに `unko_no` 付きの行を「一度でも」持ったことがある乗務員CD**
/// (Refs #205 の 39)。
///
/// [`MONTH_OPERATIONS_SQL`] の `WHERE` から**窓だけ外した**もの。逆方向
/// (GCP にしか無い運行) が「そもそも `time_card_dtako` に出ない乗務員」なのか
/// 「出るはずなのに対象月だけ落ちた」のかを分けるために要る
/// ([`crate::kintai_http_repo::UnkoDiffDriverSplit`])。
///
/// **「一度でも」の範囲は押し込み済みのぶんだけ**で、オンプレ MariaDB の全履歴
/// ではない。push していない月のことはこの問いでは分からない。
pub const OPERATION_DRIVER_CDS_SQL: &str = r#"
SELECT DISTINCT driver_cd
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND source = ANY($2) AND unko_no IS NOT NULL AND unko_no <> ''
"#;

// ── Postgres 側 ────────────────────────────────────────────────────────────

/// push まわりの失敗。
#[derive(Debug)]
pub enum KintaiPushError {
    /// `[kintai_push]` の宣言が足りない / 壊れている。
    NotConfigured(String),
    /// 生イベントの読み出しに失敗した。
    Read(KintaiRepoError),
    /// Postgres 側の失敗。
    Db(sqlx::Error),
}

impl std::fmt::Display for KintaiPushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured(m) => write!(f, "kintai push not configured: {m}"),
            Self::Read(e) => write!(f, "kintai events read failed: {e}"),
            Self::Db(e) => write!(f, "kintai push db failed: {e}"),
        }
    }
}

impl std::error::Error for KintaiPushError {}

impl From<KintaiRepoError> for KintaiPushError {
    fn from(e: KintaiRepoError) -> Self {
        Self::Read(e)
    }
}

impl From<sqlx::Error> for KintaiPushError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

/// `kintai` スキーマへの書き込み口。
///
/// 接続は**バッチ 1 本ぶん**しか張らない (`max_connections = 1`)。同時に複数の
/// トランザクションを開くと delete-then-insert が交差し得るし、走るのは
/// systemd timer からの単発ジョブなので並列度を上げる意味が無い。
#[derive(Debug)]
pub struct KintaiPgStore {
    pool: sqlx::PgPool,
    tenant_id: uuid::Uuid,
}

impl KintaiPgStore {
    /// 宣言から接続する。**pool は lazy ではなく実際に 1 本張る** — 「起動はしたが
    /// 実は繋がっていない」を作らないため (`[database] enabled` と同じ流儀)。
    pub async fn connect(cfg: &KintaiPushConfig) -> Result<Self, KintaiPushError> {
        if !cfg.enabled {
            return Err(KintaiPushError::NotConfigured(
                "[kintai_push] enabled = false".to_string(),
            ));
        }
        // 空なら nil = **pin 無し**。書き先のテナントはリクエストが名乗り、
        // 受け口が [`Self::for_tenant`] で差し替える。ヘッダを持たない CLI 経路は
        // nil のまま走らせない (`main.rs` が起動前に弾く)
        let tenant_id = if cfg.tenant_id.trim().is_empty() {
            uuid::Uuid::nil()
        } else {
            uuid::Uuid::parse_str(cfg.tenant_id.trim())
                .map_err(|e| KintaiPushError::NotConfigured(format!("tenant_id: {e}")))?
        };
        let statement_timeout_ms = cfg.statement_timeout_secs.saturating_mul(1000);
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .acquire_timeout(std::time::Duration::from_secs(cfg.connect_timeout_secs))
            .after_connect(move |conn, _meta| {
                Box::pin(async move {
                    // 暴走した 1 文でバッチ全体を止めない。応答が返らないより
                    // 落ちて journal に残る方が読める
                    sqlx::query(&format!("SET statement_timeout = {statement_timeout_ms}"))
                        .execute(conn)
                        .await?;
                    Ok(())
                })
            })
            .connect(cfg.database_url.trim())
            .await?;
        Ok(Self { pool, tenant_id })
    }

    /// テスト用。既に張った pool から作る。
    pub fn from_pool(pool: sqlx::PgPool, tenant_id: uuid::Uuid) -> Self {
        Self { pool, tenant_id }
    }

    /// テナントだけ差し替えた複製。**受け口が 1 リクエストごとに呼ぶ。**
    ///
    /// `PgPool` は内部が `Arc` なので複製しても接続は張り直されない
    /// (`max_connections = 1` の同じ pool を共有する)。テナントを
    /// `KintaiPgStore` の外に出して引数で回す形にしないのは、`kintai_fold` まで
    /// 含めた全ての SQL が `store.tenant_id()` を bind しており、渡し忘れが
    /// 「別テナントへ書く」になるため。
    pub fn for_tenant(&self, tenant_id: uuid::Uuid) -> Self {
        Self {
            pool: self.pool.clone(),
            tenant_id,
        }
    }

    pub fn tenant_id(&self) -> uuid::Uuid {
        self.tenant_id
    }

    pub fn pool(&self) -> &sqlx::PgPool {
        &self.pool
    }

    /// Postgres 側の (暦日, 署名)。[`day_signature`] と同じ値になる。
    pub async fn stored_day_signatures(
        &self,
        driver_cd: i64,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<BTreeMap<NaiveDate, String>, KintaiPushError> {
        use sqlx::Row;
        let rows = sqlx::query(STORED_SIGNATURES_SQL)
            .bind(self.tenant_id)
            .bind(driver_cd)
            .bind(from)
            .bind(to)
            .bind(&PUSHED_SOURCES[..])
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<NaiveDate, _>("d"), r.get::<String, _>("sig")))
            .collect())
    }

    /// [`stored_day_signatures`] の複数乗務員版。`(乗務員, 暦日) → 署名`。
    ///
    /// 引くのは**送り主が名乗った乗務員だけ**。全乗務員を引くと、送り主が知らない
    /// 乗務員の日まで「相手に無い日」と見なして消しにいく。
    ///
    /// [`stored_day_signatures`]: KintaiPgStore::stored_day_signatures
    pub async fn stored_window_signatures(
        &self,
        drivers: &[i64],
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<BTreeMap<i64, BTreeMap<NaiveDate, String>>, KintaiPushError> {
        use sqlx::Row;
        let rows = sqlx::query(STORED_WINDOW_SIGNATURES_SQL)
            .bind(self.tenant_id)
            .bind(drivers)
            .bind(from)
            .bind(to)
            .bind(&PUSHED_SOURCES[..])
            .fetch_all(&self.pool)
            .await?;
        let mut out: BTreeMap<i64, BTreeMap<NaiveDate, String>> = BTreeMap::new();
        for r in rows {
            out.entry(r.get::<i64, _>("driver_cd"))
                .or_default()
                .insert(r.get::<NaiveDate, _>("d"), r.get::<String, _>("sig"));
        }
        Ok(out)
    }

    /// 打刻側 (Pg) の月ゲート材料。対象月まるごとを 1 行の sha256 に畳んだもの
    /// (Refs #205 実装計画 13、[`MONTH_PUNCH_DIGEST_SQL`])。
    ///
    /// 索引 `kintai_events_driver_time (tenant_id, driver_cd, occurred_at)
    /// INCLUDE (state, source, unko_no)` の tenant_id 部分だけを使う index-only
    /// scan になる (driver_cd を等値で絞らないので occurred_at の範囲条件はここでは
    /// leaf を絞れないが、必要な列は全て INCLUDE 済みなのでヒープへは行かない)。
    pub async fn stored_month_punch_digest(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<String, KintaiPushError> {
        use sqlx::Row;
        let row = sqlx::query(MONTH_PUNCH_DIGEST_SQL)
            .bind(self.tenant_id)
            .bind(from)
            .bind(to)
            .bind(&PUSHED_SOURCES[..])
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get::<String, _>("digest"))
    }

    /// **オンプレ側の運行一覧** (Refs #205 の 37、[`MONTH_OPERATIONS_SQL`])。
    /// `(乗務員CD, unko_no)` と、その運行のイベントが覆う暦日 (JST) の範囲。
    ///
    /// 突合の材料を返すだけで**判定には使わない** — 呼び出し側
    /// ([`crate::kintai_fold`]) が etags の一覧と突き合わせて応答に載せる。
    pub async fn stored_month_operations(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<Vec<(i64, String, NaiveDate, NaiveDate)>, KintaiPushError> {
        use sqlx::Row;
        let rows = sqlx::query(MONTH_OPERATIONS_SQL)
            .bind(self.tenant_id)
            .bind(from)
            .bind(to)
            .bind(&PUSHED_SOURCES[..])
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                (
                    r.get::<i64, _>("driver_cd"),
                    r.get::<String, _>("unko_no"),
                    r.get::<NaiveDate, _>("first_date"),
                    r.get::<NaiveDate, _>("last_date"),
                )
            })
            .collect())
    }

    /// **`unko_no` 付きの行を一度でも持った乗務員CD**
    /// (Refs #205 の 39、[`OPERATION_DRIVER_CDS_SQL`])。突合の内訳を割るだけで
    /// **判定には使わない**。
    pub async fn stored_operation_driver_cds(
        &self,
    ) -> Result<std::collections::HashSet<i64>, KintaiPushError> {
        use sqlx::Row;
        let rows = sqlx::query(OPERATION_DRIVER_CDS_SQL)
            .bind(self.tenant_id)
            .bind(&PUSHED_SOURCES[..])
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| r.get::<i64, _>("driver_cd"))
            .collect())
    }

    /// 差分のあった日だけを delete-then-insert する。**1 トランザクション**。
    ///
    /// 途中で落ちたら 1 日も書かれていない状態に戻る。日ごとにコミットすると
    /// 「一部の日だけ新しい」状態が残り、再計算の指紋がその日だけ進んで
    /// 静かな不整合になる。
    /// 差分のあった日だけを delete-then-insert する。**窓ぜんたいで 1 トランザクション**。
    ///
    /// 途中で落ちたら 1 日も書かれていない状態に戻る。日ごと / 乗務員ごとにコミット
    /// すると「一部だけ新しい」状態が残り、再計算の指紋がそこだけ進んで静かな
    /// 不整合になる。
    ///
    /// ## 行ごとに往復しない
    ///
    /// 2026-07-31 に踏んだ: 1 日 1 DELETE・1 イベント 1 INSERT で往復していて、
    /// 2 か月・95 名の初回投入が **10,157 往復**になり Cloudflare の 524 (100 秒) を
    /// 超えた。supavisor 越しの 1 往復が数 ms でも、回数が効く。
    ///
    /// `unnest` で畳んで **DELETE 1 文 + INSERT 数文**にする。INSERT だけ
    /// [`INSERT_CHUNK`] 行で刻むのは、`raw` を積むと 1 文の本文が数 MB に育つため
    /// (同じトランザクションの中なので、刻んでも全か無かは変わらない)。
    ///
    /// **DELETE をやめて UPSERT にはできない。** 上流から消えた行がその日に残る。
    /// 日単位で「丸ごと置き換える」のが署名と対になっている。
    pub async fn replace_window(
        &self,
        plans: &BTreeMap<i64, DriverPlan>,
    ) -> Result<(), KintaiPushError> {
        // 消す日 (乗務員, 日の境界) を 1 本の配列に畳む
        let days = delete_days(plans);
        if days.is_empty() {
            return Ok(());
        }

        let mut tx = self.pool.begin().await?;
        // BYPASSRLS の kintai_writer では不要だが、RLS の効くロールで動かしても
        // 同じ結果になるように必ず名乗る
        sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
            .bind(self.tenant_id.to_string())
            .execute(&mut *tx)
            .await?;
        // 消す前に旧 events を読み、変わった日の前後を残す (Refs nuxt-dtako-admin#1133)
        crate::change_log::record_changes(
            &mut tx,
            self.tenant_id,
            plans,
            (&days.driver_cd, &days.from, &days.to),
        )
        .await?;

        sqlx::query(DELETE_DAYS_SQL)
            .bind(self.tenant_id)
            .bind(&days.driver_cd)
            .bind(&days.from)
            .bind(&days.to)
            .bind(&PUSHED_SOURCES[..])
            .execute(&mut *tx)
            .await?;

        for cols in event_columns(plans) {
            sqlx::query(INSERT_EVENTS_SQL)
                .bind(self.tenant_id)
                .bind(&cols.driver_cd)
                .bind(&cols.occurred_at)
                .bind(&cols.state)
                .bind(&cols.source)
                .bind(&cols.unko_no)
                .bind(&cols.raw)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// 1 乗務員ぶん。[`replace_window`] に畳んで渡すだけ — 実装を 2 つ持たない。
    ///
    /// [`replace_window`]: KintaiPgStore::replace_window
    pub async fn replace_days(
        &self,
        driver_cd: i64,
        changed: &BTreeMap<NaiveDate, Vec<PushEvent>>,
        deleted: &[NaiveDate],
    ) -> Result<(), KintaiPushError> {
        self.replace_window(&BTreeMap::from([(
            driver_cd,
            DriverPlan {
                changed: changed.clone(),
                deleted: deleted.to_vec(),
            },
        )]))
        .await
    }
}

/// `YYYY-MM-DD HH:MM:SS` (JST 壁時計) を `TIMESTAMPTZ` へ渡せる形に。
///
/// 読み返す側 ([`crate::kintai_pg_repo`]) も同じ変換で範囲を作る。**写さない** —
/// 書きと読みで壁時計の解釈が 1 箇所でも割れると、書いた行が読めない日ができる。
pub fn jst_at(s: &str) -> Result<DateTime<FixedOffset>, KintaiPushError> {
    let naive = NaiveDateTime::parse_from_str(s, DATETIME_FORMAT)
        .map_err(|e| KintaiPushError::NotConfigured(format!("bad range {s:?}: {e}")))?;
    Ok(FixedOffset::east_opt(JST_OFFSET_SECONDS)
        .expect("JST offset is in range")
        .from_local_datetime(&naive)
        .single()
        .expect("JST has no DST gap"))
}

// ── push 本体 ──────────────────────────────────────────────────────────────

/// `push` / `sync` の引数。
#[derive(Debug, Clone)]
pub struct PushOptions {
    /// `YYYY-MM`。
    pub month: String,
    /// 1 名だけに絞るなら `Some`。
    pub driver: Option<u64>,
    /// **`false` なら 1 行も書かない** (既定)。
    pub apply: bool,
}

/// 対象月の対象乗務員を洗い出す。
///
/// **打刻がある乗務員だけ。** ここで `dtako_events` しか無い乗務員を拾っても、
/// [`parse_row`] が全行 `NotPushedSource` で捨てるので空の batch にしかならない。
async fn target_drivers(
    repo: &DynKintaiEventsRepo,
    opts: &PushOptions,
    from: &str,
    to: &str,
) -> Result<Vec<u64>, KintaiPushError> {
    if let Some(d) = opts.driver {
        return Ok(vec![d]);
    }
    Ok(repo.fetch_timecard_driver_cds_between(from, to).await?)
}

/// 1 乗務員 1 か月ぶんの打刻を読む。
///
/// **push / diff 専用。畳む側 ([`crate::kintai_fold`]) から呼んではいけない。**
/// ここは `dtako_events` を落とした「打刻 2 表だけ」なので、畳むのに要る休息
/// イベントが入っていない。`kosoku::daily_summary` はそれで勤務を切るため、
/// これを渡すと休息由来の勤務が丸ごと消える。畳む側は
/// `repo.fetch_events_between` を直に呼ぶこと (#225 の絞りを fold へ広げた
/// 2026-07-31 の回帰)。
///
/// **全乗務員版ではなく単一乗務員版を使う。** 全乗務員版の SQL は速さのために
/// `運行NO` を落としている (`ALL_EVENTS_SQL`) ので、入力層に残したい
/// 「どの運行のイベントか」が消える。バッチなので往復の回数より情報量を採る。
///
/// **`dtako_events` は読まない** ([`PUSHED_SOURCES`])。押し出さない行なので、
/// 読めば [`parse_row`] が捨てるだけ。畳むのに要るデジタコ生イベントは GCP が
/// alc から直接引く (#205 の決定 5) ので、この経路が運ぶ必要もない。
pub async fn read_driver_events(
    repo: &DynKintaiEventsRepo,
    driver: u64,
    from: &str,
    to: &str,
) -> Result<Vec<serde_json::Value>, KintaiPushError> {
    Ok(repo.fetch_timecard_events_between(from, to, driver).await?)
}

/// 対象月の打刻を push する (実装計画 04)。
///
/// 期間は**その月ちょうど** `[月初, 翌月初)`。[`crate::kintai_repo::month_range`] の
/// ように翌月 2 日まで広げると、翌月頭の 2 日ぶんを「その 2 日しか見ていない状態」で
/// 署名してしまい、翌月の実行と食い違って毎回書き直すことになる。
pub async fn push_month(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    opts: &PushOptions,
) -> Result<PushReport, KintaiPushError> {
    let (from, to) = exact_month_range(&opts.month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {}", opts.month)))?;
    let (tz_from, tz_to) = (jst_at(&from)?, jst_at(&to)?);
    let drivers = target_drivers(repo, opts, &from, &to).await?;

    let mut report = PushReport::default();
    for driver in drivers {
        let rows = read_driver_events(repo, driver, &from, &to).await?;
        report.drivers += 1;
        report.rows_read += rows.len();

        let parsed = parse_rows(&rows);
        report.merge(&parsed);
        let before = parsed.events.len();
        let events = dedup_events(parsed.events);
        report.deduped += before - events.len();
        report.events_pushed += events.len();

        let by_date = group_by_date(&events);
        let local: BTreeMap<NaiveDate, String> = by_date
            .iter()
            .map(|(d, evs)| (*d, day_signature(evs)))
            .collect();
        let stored = store
            .stored_day_signatures(driver as i64, tz_from, tz_to)
            .await?;

        let mut changed: BTreeMap<NaiveDate, Vec<PushEvent>> = BTreeMap::new();
        let mut deleted: Vec<NaiveDate> = Vec::new();
        for diff in diff_days(&local, &stored) {
            match diff.kind {
                DayDiffKind::Unchanged => report.days_unchanged += 1,
                DayDiffKind::Changed => {
                    report.days_changed += 1;
                    changed.insert(diff.date, by_date[&diff.date].clone());
                }
                DayDiffKind::Deleted => {
                    report.days_deleted += 1;
                    deleted.push(diff.date);
                }
            }
        }
        if opts.apply && (!changed.is_empty() || !deleted.is_empty()) {
            store
                .replace_days(driver as i64, &changed, &deleted)
                .await?;
        }
    }
    Ok(report)
}

// ── 04b: オンプレ → GCP の打刻転送 ─────────────────────────────────────────
//
// GCP 側には MariaDB が無いので打刻が読めない (`shifts_from_timecard` が空になる)。
// #205 の 02 が「04 / 05 が埋める穴」と書いていたものを、穴の定義どおりに埋める:
// **オンプレが読んで GCP へ渡す。**
//
// 送るのは**差分の日だけ**。オンプレが (乗務員, 暦日) の署名を作り、GCP 側の署名を
// [`STORED_SIGNATURES_SQL`] で引いて突き合わせ、違う日と消えた日だけを載せる。
// 全量を送ると 1 か月・全乗務員で数万行になる。
//
// **生行のまま送る。** 受け側が [`parse_rows`] → [`dedup_events`] → [`replace_days`]
// を回すので、写しと重複解決の実装は 1 つのまま。オンプレ側で畳んでから送ると
// 両側に parser が要る。

/// 受け取った batch を `kintai_events` に反映する (GCP 側で走る)。
///
/// **送り主を信用しない。** 日のキーと行の中身が食い違う行、対象の乗務員でない行、
/// 対象月から外れた日は落として数える。信用すると、1 リクエストで別の乗務員や
/// 別の月を静かに書き換えられる口になる。
pub async fn apply_timecard_batch(
    store: &KintaiPgStore,
    batch: &TimecardBatch,
) -> Result<TimecardBatchResult, KintaiPushError> {
    let (plans, result) = plan_received_batch(batch).map_err(KintaiPushError::NotConfigured)?;
    store.replace_window(&plans).await?;
    Ok(result)
}

/// 窓ぶんを受けて、**変わった日だけ**を反映する (GCP 側で走る)。
///
/// 突き合わせはここ — 送り主に署名を引かせない。同じ DB の中なので往復がゼロ、
/// かつ [`day_signature`] と [`STORED_WINDOW_SIGNATURES_SQL`] は既に同値が
/// 検証済みなので、新しい実装は増えない。
///
/// **送り主を信用しない。** 窓の外の日と、名乗っていない乗務員の行は落として数える。
pub async fn apply_timecard_window(
    store: &KintaiPgStore,
    window: &TimecardWindow,
) -> Result<TimecardWindowResult, KintaiPushError> {
    let started = std::time::Instant::now();
    let (spans, lo, hi) = window_spans(&window.months).map_err(KintaiPushError::NotConfigured)?;

    let declared: BTreeSet<i64> = window.drivers.iter().copied().collect();
    let drivers: Vec<i64> = declared.iter().copied().collect();
    let remote = store
        .stored_window_signatures(&drivers, jst_day_bounds(lo).0, jst_day_bounds(hi).0)
        .await?;

    let (plans, mut result) = plan_window(&spans, &declared, &window.events, &remote);
    result.dry_run = window.dry_run;
    if !window.dry_run {
        // **乗務員ごとに往復しない。** 窓ぜんたいで 1 トランザクション
        store.replace_window(&plans).await?;
    }
    result.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    // ── 失敗の伝え方 ──
    //
    // どれも「静かに 0 件成功に見える」を防ぐための経路なので、メッセージが
    // 何を指しているかまで固定する。

    #[test]
    fn errors_say_which_layer_failed() {
        let e = KintaiPushError::NotConfigured("enabled = false".to_string());
        assert!(e.to_string().contains("not configured"), "{e}");
        assert!(e.to_string().contains("enabled = false"), "{e}");

        let e: KintaiPushError = KintaiRepoError::NotConfigured.into();
        assert!(e.to_string().contains("read failed"), "{e}");
        assert!(matches!(e, KintaiPushError::Read(_)));

        let e: KintaiPushError = sqlx::Error::RowNotFound.into();
        assert!(e.to_string().contains("db failed"), "{e}");
        assert!(matches!(e, KintaiPushError::Db(_)));

        // Debug も潰れていない (journal に出るのはこちら)
        assert!(format!("{e:?}").contains("Db"));
    }

    #[tokio::test]
    async fn connect_refuses_before_it_dials_when_the_declaration_is_wrong() {
        // 宣言していないのに繋ぎに行かない
        let mut cfg = KintaiPushConfig::default();
        let e = KintaiPgStore::connect(&cfg).await.unwrap_err();
        assert!(e.to_string().contains("enabled = false"), "{e}");

        // UUID でない tenant_id は接続前に弾く (別テナントへ書くより先に落とす)
        cfg.enabled = true;
        cfg.tenant_id = "not-a-uuid".to_string();
        cfg.database_url = "postgres://nobody@127.0.0.1:1/none".to_string();
        let e = KintaiPgStore::connect(&cfg).await.unwrap_err();
        assert!(e.to_string().contains("tenant_id"), "{e}");
    }

    #[tokio::test]
    async fn target_drivers_honours_the_filter_without_reading() {
        // --driver を付けたら全乗務員版を叩かない (叩けば panic する repo で確かめる)
        struct Exploding;
        #[async_trait]
        impl crate::kintai_repo::KintaiEventsApi for Exploding {
            async fn fetch_events_between(
                &self,
                _: &str,
                _: &str,
                _: u64,
            ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
                unreachable!()
            }
            async fn fetch_all_events_between(
                &self,
                _: &str,
                _: &str,
            ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
                panic!("--driver 指定なのに全乗務員版を叩いた")
            }
            async fn fetch_ferry_between(
                &self,
                _: &str,
                _: &str,
                _: Option<u64>,
            ) -> Result<Vec<serde_json::Value>, KintaiRepoError> {
                unreachable!()
            }
        }
        let repo: DynKintaiEventsRepo = std::sync::Arc::new(Exploding);
        let opts = PushOptions {
            month: "2026-07".to_string(),
            driver: Some(1130),
            apply: false,
        };
        assert_eq!(
            target_drivers(&repo, &opts, "2026-07-01 00:00:00", "2026-08-01 00:00:00")
                .await
                .unwrap(),
            vec![1130]
        );
    }
    #[test]
    fn allowed_states_match_the_ddl() {
        // DDL の CHECK と 1 対 1。増減したら片方だけ直す事故を防ぐ
        let ddl = std::fs::read_to_string("migrations/001_kintai_schema.sql").unwrap();
        for s in ALLOWED_STATES {
            assert!(ddl.contains(&format!("'{s}'")), "{s} が DDL に無い");
        }
        assert_eq!(ALLOWED_STATES.len(), 7);
    }
}
