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
use sha2::{Digest, Sha256};

use crate::config::KintaiPushConfig;
use crate::kintai_repo::{exact_month_range, DynKintaiEventsRepo, KintaiRepoError};

/// JST。日本標準時に夏時間は無いので固定オフセットで表せる。
///
/// `chrono-tz` を足さないのは、この 1 か所のためだけに timezone データベースを
/// 抱えることになるため。`AT TIME ZONE 'Asia/Tokyo'` (Postgres 側) との一致は
/// オフセットが恒久的に +09:00 であることに依る。
pub const JST_OFFSET_SECONDS: i32 = 9 * 3600;

/// 生行 / 署名の日時書式。`EVENTS_SQL` の `DATE_FORMAT(..., '%Y-%m-%d %H:%i:%s')` と同じ。
pub const DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S";

/// `kintai.kintai_events.state` の CHECK 制約と同じ集合 (001_kintai_schema.sql)。
///
/// **DDL より広く受けない。** 制約に無い値を送ると INSERT が落ちてトランザクション
/// ごと巻き戻るので、送る前にこちらで弾いて「何が弾かれたか」を数える。
pub const ALLOWED_STATES: [&str; 7] = [
    "始業",
    "終業",
    "運行開始",
    "運行終了",
    "休息開始",
    "休息終了",
    "除外",
];

/// push 対象の `source`。DDL の CHECK は `alc_app` も許すが、こちらは作らない。
///
/// `dtako_events` (デジタコ生イベント) が入っていないのは決定 5 のとおり
/// R2 に永続化済みだから。
pub const PUSHED_SOURCES: [&str; 2] = ["timecard", "dtako"];

/// **運ばないと決めた `state` の実値** (2026-07-31 のユーザー判断)。
///
/// `time_card_dtako` の休息は開始 (state 20) と終了 (21) が**同じ名前「休息」**で
/// 来る ([`crate::kosoku_paper`] の `tc_stream` が実データから確認済み)。紙との突合は
/// `dtako_events` の休息区間の端と時刻照合して `休息開始` / `休息終了` に読み替えて
/// いるが、**この経路は `dtako_events` を運ばない** (決定 5) ので同じ手が使えない。
///
/// 畳むのに要る休息区間は GCP が alc から直接引くため、**打刻由来の確定休息は
/// 運ばない**。読んでから捨てるのではなく [`crate::kintai_repo`] の SQL で落とす。
///
/// [`ALLOWED_STATES`] からは外さない — 万一 GCP 側に届いたときに DDL の CHECK で
/// 落ちるより、`UnknownState` として実値が報告されるほうが原因に辿り着ける。
pub const NOT_CARRIED_STATES: [&str; 1] = ["休息"];

/// 同じ `(occurred_at, state)` が衝突したときに残す `source` の優先順。
///
/// 添字が小さい方を残す。`timecard` が上なのは、人が確定させた打刻であり
/// `time_card_dtako` の運行由来イベントより上位の事実だから (#118「勤務はイベントで
/// 切る」も打刻を優先している)。
const SOURCE_PRIORITY: [&str; 2] = ["timecard", "dtako"];

/// `kintai.kintai_events` に入れる 1 行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushEvent {
    pub driver_cd: i64,
    /// JST の壁時計。`TIMESTAMPTZ` へは [`JST_OFFSET_SECONDS`] を付けて渡す。
    pub occurred_at: NaiveDateTime,
    pub state: String,
    pub source: String,
    pub unko_no: Option<String>,
    /// 元の生行そのまま。追跡用で、署名には入れない。
    pub raw: serde_json::Value,
}

impl PushEvent {
    /// `TIMESTAMPTZ` へ渡す値。
    pub fn occurred_at_tz(&self) -> chrono::DateTime<FixedOffset> {
        FixedOffset::east_opt(JST_OFFSET_SECONDS)
            .expect("JST offset is in range")
            .from_local_datetime(&self.occurred_at)
            .single()
            .expect("JST has no DST gap")
    }

    /// この行が乗る暦日 (JST)。
    pub fn date(&self) -> NaiveDate {
        self.occurred_at.date()
    }

    /// 署名の 1 行。Postgres 側の [`STORED_SIGNATURES_SQL`] と同じ組み立て。
    fn signature_line(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.occurred_at.format(DATETIME_FORMAT),
            self.state,
            self.source,
            self.unko_no.as_deref().unwrap_or("")
        )
    }

    /// 並べ替えのキー。`ORDER BY occurred_at, state COLLATE "C", source COLLATE "C"`。
    fn sort_key(&self) -> (NaiveDateTime, &str, &str) {
        (self.occurred_at, &self.state, &self.source)
    }
}

/// 生行 1 つを [`PushEvent`] へ。push 対象でない行は `None`。
///
/// 落とす理由を区別できるよう [`RejectReason`] を返す。**黙って捨てない** —
/// 入力層が静かに欠けると、あとで出力の差を入力へ遡れなくなる。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RejectReason {
    /// `source` が push 対象外 (`dtako_events` など)。想定内なので数えるだけ。
    NotPushedSource,
    /// `driver_id` が無い / 数値でない。
    NoDriver,
    /// `datetime` が読めない。
    BadDatetime,
    /// `state` が空。
    NoState,
    /// `state` が DDL の CHECK 制約に無い。**これだけは想定外**。
    UnknownState,
}

/// 生行を写す。`Err` は落とした理由。
pub fn parse_row(row: &serde_json::Value) -> Result<PushEvent, RejectReason> {
    let source = row
        .get("source")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if !PUSHED_SOURCES.contains(&source) {
        return Err(RejectReason::NotPushedSource);
    }
    let driver_cd = row
        .get("driver_id")
        .and_then(value_as_i64)
        .ok_or(RejectReason::NoDriver)?;
    let dt = row
        .get("datetime")
        .and_then(|v| v.as_str())
        .ok_or(RejectReason::BadDatetime)?;
    let occurred_at = NaiveDateTime::parse_from_str(dt, DATETIME_FORMAT)
        .map_err(|_| RejectReason::BadDatetime)?;
    let state = row
        .get("state")
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or(RejectReason::NoState)?;
    if !ALLOWED_STATES.contains(&state) {
        return Err(RejectReason::UnknownState);
    }
    Ok(PushEvent {
        driver_cd,
        occurred_at,
        state: state.to_string(),
        source: source.to_string(),
        unko_no: row
            .get("unko_no")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string),
        raw: row.clone(),
    })
}

/// `driver_id` は経路によって数値だったり文字列だったりする (MariaDB driver 依存)。
fn value_as_i64(v: &serde_json::Value) -> Option<i64> {
    v.as_i64()
        .or_else(|| v.as_u64().and_then(|n| i64::try_from(n).ok()))
        .or_else(|| v.as_str().and_then(|s| s.trim().parse::<i64>().ok()))
}

/// 生行の並びを写して、落とした行を理由ごとに数える。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ParseOutcome {
    pub events: Vec<PushEvent>,
    /// 理由ごとの件数。
    pub rejected: BTreeMap<RejectReason, usize>,
    /// CHECK 制約に無かった `state` の実値 (最大 [`MAX_REPORTED_STATES`] 種)。
    /// **何が来たか分からないまま「弾きました」とだけ言わない**ため。
    pub unknown_states: BTreeSet<String>,
}

/// 報告に載せる未知 state の上限。壊れた上流で無制限に膨らませない。
pub const MAX_REPORTED_STATES: usize = 20;

pub fn parse_rows(rows: &[serde_json::Value]) -> ParseOutcome {
    let mut out = ParseOutcome::default();
    for row in rows {
        match parse_row(row) {
            Ok(ev) => out.events.push(ev),
            Err(reason) => {
                *out.rejected.entry(reason).or_default() += 1;
                if reason == RejectReason::UnknownState
                    && out.unknown_states.len() < MAX_REPORTED_STATES
                {
                    if let Some(s) = row.get("state").and_then(|v| v.as_str()) {
                        out.unknown_states.insert(s.trim().to_string());
                    }
                }
            }
        }
    }
    out
}

/// PK `(driver_cd, occurred_at, state)` の重複を決着させ、署名と同じ順に並べる。
///
/// 残すのは [`SOURCE_PRIORITY`] が上の `source`。同順位なら先に来た方。
pub fn dedup_events(mut events: Vec<PushEvent>) -> Vec<PushEvent> {
    let prio = |s: &str| {
        SOURCE_PRIORITY
            .iter()
            .position(|p| *p == s)
            .unwrap_or(SOURCE_PRIORITY.len())
    };
    // 先に「残す方」が前に来る順で並べ、あとは署名の順に整える
    events.sort_by(|a, b| {
        (a.driver_cd, a.occurred_at, &a.state, prio(&a.source)).cmp(&(
            b.driver_cd,
            b.occurred_at,
            &b.state,
            prio(&b.source),
        ))
    });
    events.dedup_by(|a, b| {
        a.driver_cd == b.driver_cd && a.occurred_at == b.occurred_at && a.state == b.state
    });
    events.sort_by(|a, b| (a.driver_cd, a.sort_key()).cmp(&(b.driver_cd, b.sort_key())));
    events
}

/// 暦日 (JST) ごとに束ねる。[`dedup_events`] 済みの並びを前提に順序を保つ。
pub fn group_by_date(events: &[PushEvent]) -> BTreeMap<NaiveDate, Vec<PushEvent>> {
    let mut out: BTreeMap<NaiveDate, Vec<PushEvent>> = BTreeMap::new();
    for ev in events {
        out.entry(ev.date()).or_default().push(ev.clone());
    }
    out
}

/// 1 日ぶんの署名 (sha256 hex)。[`STORED_SIGNATURES_SQL`] と同じ値になる。
pub fn day_signature(events: &[PushEvent]) -> String {
    let mut sorted: Vec<&PushEvent> = events.iter().collect();
    sorted.sort_by_key(|e| e.sort_key());
    let body = sorted
        .iter()
        .map(|e| e.signature_line())
        .collect::<Vec<_>>()
        .join("\n");
    let mut h = Sha256::new();
    h.update(body.as_bytes());
    format!("{:x}", h.finalize())
}

/// Postgres 側の (暦日, 署名)。**`kintai_events_driver_time` の INCLUDE だけで済む**
/// 形にしてある (`state` / `source` / `unko_no` が索引に載っている)。
///
/// `COLLATE "C"` と `AT TIME ZONE 'Asia/Tokyo'` の理由はモジュール docs 参照。
pub const STORED_SIGNATURES_SQL: &str = r#"
SELECT (occurred_at AT TIME ZONE 'Asia/Tokyo')::date AS d,
       encode(sha256(convert_to(string_agg(
           to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
             || '|' || state || '|' || source || '|' || coalesce(unko_no, ''),
           E'\n' ORDER BY occurred_at, state COLLATE "C", source COLLATE "C"), 'UTF8')), 'hex') AS sig
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND driver_cd = $2
   AND occurred_at >= $3 AND occurred_at < $4
 GROUP BY 1
"#;

/// 差分の判定結果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayDiff {
    pub date: NaiveDate,
    pub kind: DayDiffKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DayDiffKind {
    /// 署名が一致。**何もしない**。
    Unchanged,
    /// 署名が違う / 相手に無い。delete-then-insert する。
    Changed,
    /// こちらに無く相手にある。元が消えたので**消す** (テスト計画の 4 番目)。
    Deleted,
}

/// 手元の日別署名と Postgres 側の日別署名を突き合わせる。
///
/// `local` に無く `stored` にある日は [`DayDiffKind::Deleted`] — MariaDB 側で
/// 打刻が消されたら Supabase 側からも消えなければ、古い値が正常に見える。
pub fn diff_days(
    local: &BTreeMap<NaiveDate, String>,
    stored: &BTreeMap<NaiveDate, String>,
) -> Vec<DayDiff> {
    let mut dates: BTreeSet<NaiveDate> = local.keys().copied().collect();
    dates.extend(stored.keys().copied());
    dates
        .into_iter()
        .map(|date| {
            let kind = match (local.get(&date), stored.get(&date)) {
                (Some(a), Some(b)) if a == b => DayDiffKind::Unchanged,
                (Some(_), _) => DayDiffKind::Changed,
                (None, _) => DayDiffKind::Deleted,
            };
            DayDiff { date, kind }
        })
        .collect()
}

/// 1 回の push の集計。`--dry-run` でも同じものを作る (書かないだけ)。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PushReport {
    pub drivers: usize,
    pub rows_read: usize,
    pub events_pushed: usize,
    pub days_changed: usize,
    pub days_deleted: usize,
    pub days_unchanged: usize,
    pub rejected: BTreeMap<RejectReason, usize>,
    pub unknown_states: BTreeSet<String>,
    /// 重複 PK で捨てた行数。
    pub deduped: usize,
}

impl PushReport {
    /// 書き込みが起きたか。06 が「1 日でも書いたら再計算」を判断するのに使う。
    pub fn wrote_anything(&self) -> bool {
        self.days_changed > 0 || self.days_deleted > 0
    }

    /// 想定外があったか。`state` が CHECK 制約に無いのは上流の変化なので、
    /// 黙って続けず呼び出し側が非 0 終了できるようにする。
    pub fn has_unexpected(&self) -> bool {
        !self.unknown_states.is_empty()
            || self
                .rejected
                .keys()
                .any(|r| !matches!(r, RejectReason::NotPushedSource))
    }

    pub fn merge(&mut self, other: &ParseOutcome) {
        for (k, v) in &other.rejected {
            *self.rejected.entry(*k).or_default() += v;
        }
        for s in &other.unknown_states {
            if self.unknown_states.len() < MAX_REPORTED_STATES {
                self.unknown_states.insert(s.clone());
            }
        }
    }
}

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
            .fetch_all(&self.pool)
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<NaiveDate, _>("d"), r.get::<String, _>("sig")))
            .collect())
    }

    /// 差分のあった日だけを delete-then-insert する。**1 トランザクション**。
    ///
    /// 途中で落ちたら 1 日も書かれていない状態に戻る。日ごとにコミットすると
    /// 「一部の日だけ新しい」状態が残り、再計算の指紋がその日だけ進んで
    /// 静かな不整合になる。
    pub async fn replace_days(
        &self,
        driver_cd: i64,
        changed: &BTreeMap<NaiveDate, Vec<PushEvent>>,
        deleted: &[NaiveDate],
    ) -> Result<(), KintaiPushError> {
        let mut tx = self.pool.begin().await?;
        // BYPASSRLS の kintai_writer では不要だが、RLS の効くロールで動かしても
        // 同じ結果になるように必ず名乗る
        sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
            .bind(self.tenant_id.to_string())
            .execute(&mut *tx)
            .await?;

        for date in deleted.iter().chain(changed.keys()) {
            let (from, to) = jst_day_bounds(*date);
            sqlx::query(DELETE_DAY_SQL)
                .bind(self.tenant_id)
                .bind(driver_cd)
                .bind(from)
                .bind(to)
                .execute(&mut *tx)
                .await?;
        }
        for events in changed.values() {
            for ev in events {
                sqlx::query(INSERT_EVENT_SQL)
                    .bind(self.tenant_id)
                    .bind(ev.driver_cd)
                    .bind(ev.occurred_at_tz())
                    .bind(&ev.state)
                    .bind(&ev.source)
                    .bind(ev.unko_no.as_deref())
                    .bind(&ev.raw)
                    .execute(&mut *tx)
                    .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }
}

const DELETE_DAY_SQL: &str = r#"
DELETE FROM kintai.kintai_events
 WHERE tenant_id = $1 AND driver_cd = $2 AND occurred_at >= $3 AND occurred_at < $4
"#;

const INSERT_EVENT_SQL: &str = r#"
INSERT INTO kintai.kintai_events
       (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw)
VALUES ($1, $2, $3, $4, $5, $6, $7)
"#;

/// JST の暦日 1 日ぶんの `[00:00, 翌 00:00)`。
pub fn jst_day_bounds(date: NaiveDate) -> (DateTime<FixedOffset>, DateTime<FixedOffset>) {
    let jst = FixedOffset::east_opt(JST_OFFSET_SECONDS).expect("JST offset is in range");
    let at = |d: NaiveDate| {
        jst.from_local_datetime(&d.and_hms_opt(0, 0, 0).expect("midnight exists"))
            .single()
            .expect("JST has no DST gap")
    };
    (at(date), at(date.succ_opt().expect("date has a successor")))
}

/// `YYYY-MM-DD HH:MM:SS` (JST 壁時計) を `TIMESTAMPTZ` へ渡せる形に。
fn jst_at(s: &str) -> Result<DateTime<FixedOffset>, KintaiPushError> {
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

/// 転送 1 回ぶんの本体。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TimecardBatch {
    /// 対象月 (`YYYY-MM`)。受け側が範囲外の日を弾くのに使う。
    pub month: String,
    pub driver_cd: i64,
    /// 送る日ごとの**生行**。キーは JST の暦日。
    #[serde(default)]
    pub days: BTreeMap<NaiveDate, Vec<serde_json::Value>>,
    /// こちらに無く相手にある日。元が消えたので相手からも消す。
    #[serde(default)]
    pub delete_dates: Vec<NaiveDate>,
}

impl TimecardBatch {
    /// 書き込みが起きるか。空の batch を送っても害は無いが、往復を省ける。
    pub fn is_empty(&self) -> bool {
        self.days.is_empty() && self.delete_dates.is_empty()
    }
}

/// 受け側が返す結果。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TimecardBatchResult {
    pub days_written: usize,
    pub days_deleted: usize,
    pub events_written: usize,
    pub deduped: usize,
    /// 受け側で弾いた行の理由と件数 (`Debug` 名で返す — 送り側のログに残すため)。
    #[serde(default)]
    pub rejected: BTreeMap<String, usize>,
    /// DDL の CHECK に無かった `state` の実値。
    #[serde(default)]
    pub unknown_states: BTreeSet<String>,
    /// 日のキーと中身が食い違っていた行の数。**0 でないなら送り側が壊れている**。
    pub misplaced: usize,
}

impl TimecardBatchResult {
    pub fn has_unexpected(&self) -> bool {
        !self.unknown_states.is_empty() || self.misplaced > 0
    }
}

/// 受け取った batch を `kintai_events` に反映する (GCP 側で走る)。
///
/// **送り主を信用しない。** 日のキーと行の中身が食い違う行、対象の乗務員でない行、
/// 対象月から外れた日は落として数える。信用すると、1 リクエストで別の乗務員や
/// 別の月を静かに書き換えられる口になる。
pub async fn apply_timecard_batch(
    store: &KintaiPgStore,
    batch: &TimecardBatch,
) -> Result<TimecardBatchResult, KintaiPushError> {
    let (m0, m1) = month_date_bounds(&batch.month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {}", batch.month)))?;
    let mut result = TimecardBatchResult::default();
    let mut changed: BTreeMap<NaiveDate, Vec<PushEvent>> = BTreeMap::new();

    for (date, rows) in &batch.days {
        if *date < m0 || *date >= m1 {
            result.misplaced += rows.len();
            continue;
        }
        let parsed = parse_rows(rows);
        for (reason, n) in &parsed.rejected {
            *result.rejected.entry(format!("{reason:?}")).or_default() += n;
        }
        for s in &parsed.unknown_states {
            if result.unknown_states.len() < MAX_REPORTED_STATES {
                result.unknown_states.insert(s.clone());
            }
        }
        // 日のキーと中身、乗務員の一致を確かめる
        let before = parsed.events.len();
        let kept: Vec<PushEvent> = parsed
            .events
            .into_iter()
            .filter(|e| e.date() == *date && e.driver_cd == batch.driver_cd)
            .collect();
        result.misplaced += before - kept.len();

        let deduped = dedup_events(kept);
        result.deduped += before - result.misplaced - deduped.len();
        result.events_written += deduped.len();
        changed.insert(*date, deduped);
    }

    let deleted: Vec<NaiveDate> = batch
        .delete_dates
        .iter()
        .copied()
        .filter(|d| *d >= m0 && *d < m1)
        .collect();
    result.days_written = changed.len();
    result.days_deleted = deleted.len();

    if !changed.is_empty() || !deleted.is_empty() {
        store
            .replace_days(batch.driver_cd, &changed, &deleted)
            .await?;
    }
    Ok(result)
}

/// 対象月の `[月初, 翌月初)` を `DATE` の境界で返す。
fn month_date_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next))
}

/// 送り側が「何を送るか」を決める。相手の署名と手元の署名を突き合わせるだけ。
pub fn plan_batch(
    month: &str,
    driver_cd: i64,
    local: &BTreeMap<NaiveDate, Vec<PushEvent>>,
    remote: &BTreeMap<NaiveDate, String>,
) -> TimecardBatch {
    let local_sigs: BTreeMap<NaiveDate, String> = local
        .iter()
        .map(|(d, evs)| (*d, day_signature(evs)))
        .collect();
    let mut days = BTreeMap::new();
    let mut delete_dates = Vec::new();
    for diff in diff_days(&local_sigs, remote) {
        match diff.kind {
            DayDiffKind::Unchanged => {}
            DayDiffKind::Changed => {
                // 生行のまま送る (受け側が同じ parser を回す)
                let rows = local[&diff.date].iter().map(|e| e.raw.clone()).collect();
                days.insert(diff.date, rows);
            }
            DayDiffKind::Deleted => delete_dates.push(diff.date),
        }
    }
    TimecardBatch {
        month: month.to_string(),
        driver_cd,
        days,
        delete_dates,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    fn dt(s: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(s, DATETIME_FORMAT).unwrap()
    }

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    fn ev(at: &str, state: &str, source: &str) -> PushEvent {
        PushEvent {
            driver_cd: 1130,
            occurred_at: dt(at),
            state: state.to_string(),
            source: source.to_string(),
            unko_no: None,
            raw: json!({}),
        }
    }

    #[test]
    fn parse_row_maps_a_timecard_punch() {
        let ev = parse_row(&json!({
            "datetime": "2026-07-01 08:00:00",
            "end_datetime": null,
            "driver_id": 1130,
            "source": "timecard",
            "state": "始業",
            "unko_no": null,
        }))
        .unwrap();
        assert_eq!(ev.driver_cd, 1130);
        assert_eq!(ev.occurred_at, dt("2026-07-01 08:00:00"));
        assert_eq!(ev.state, "始業");
        assert_eq!(ev.source, "timecard");
        assert_eq!(ev.unko_no, None);
    }

    #[test]
    fn parse_row_keeps_unko_no_from_the_dtako_branch() {
        let ev = parse_row(&json!({
            "datetime": "2026-07-01 09:00:00",
            "driver_id": 1130,
            "source": "dtako",
            "state": "運行開始",
            "unko_no": "2607011025060000000272",
        }))
        .unwrap();
        assert_eq!(ev.unko_no.as_deref(), Some("2607011025060000000272"));
    }

    #[test]
    fn parse_row_skips_sources_that_live_in_r2() {
        // dtako_events は R2 に永続化済み (決定 5) なので push しない
        let err = parse_row(&json!({
            "datetime": "2026-07-01 09:00:00",
            "driver_id": 1130,
            "source": "dtako_events",
            "state": "休息",
        }))
        .unwrap_err();
        assert_eq!(err, RejectReason::NotPushedSource);
    }

    #[test]
    fn parse_row_rejects_states_outside_the_ddl_check() {
        // CHECK 制約に無い値を送ると INSERT ごと落ちる。送る前に弾く
        let err = parse_row(&json!({
            "datetime": "2026-07-01 09:00:00",
            "driver_id": 1130,
            "source": "dtako",
            "state": "点呼",
        }))
        .unwrap_err();
        assert_eq!(err, RejectReason::UnknownState);
    }

    #[test]
    fn parse_row_reports_each_missing_field_separately() {
        let base = json!({
            "datetime": "2026-07-01 09:00:00",
            "driver_id": 1130,
            "source": "dtako",
            "state": "運行開始",
        });
        let without = |k: &str| {
            let mut v = base.clone();
            v.as_object_mut().unwrap().remove(k);
            v
        };
        assert_eq!(
            parse_row(&without("driver_id")).unwrap_err(),
            RejectReason::NoDriver
        );
        assert_eq!(
            parse_row(&without("datetime")).unwrap_err(),
            RejectReason::BadDatetime
        );
        assert_eq!(
            parse_row(&without("state")).unwrap_err(),
            RejectReason::NoState
        );

        let mut bad = base.clone();
        bad["datetime"] = json!("2026/07/01 09:00:00");
        assert_eq!(parse_row(&bad).unwrap_err(), RejectReason::BadDatetime);

        let mut blank = base.clone();
        blank["state"] = json!("   ");
        assert_eq!(parse_row(&blank).unwrap_err(), RejectReason::NoState);
    }

    #[test]
    fn parse_row_accepts_driver_id_as_string() {
        // MariaDB driver によって数値でなく文字列で返ることがある
        let ev = parse_row(&json!({
            "datetime": "2026-07-01 09:00:00",
            "driver_id": "1130",
            "source": "dtako",
            "state": "運行開始",
        }))
        .unwrap();
        assert_eq!(ev.driver_cd, 1130);
    }

    #[test]
    fn parse_rows_counts_what_it_dropped() {
        let out = parse_rows(&[
            json!({"datetime": "2026-07-01 08:00:00", "driver_id": 1, "source": "timecard", "state": "始業"}),
            json!({"datetime": "2026-07-01 09:00:00", "driver_id": 1, "source": "dtako_events", "state": "休息"}),
            json!({"datetime": "2026-07-01 10:00:00", "driver_id": 1, "source": "dtako", "state": "点呼"}),
            json!({"datetime": "2026-07-01 11:00:00", "driver_id": 1, "source": "dtako", "state": "待機"}),
        ]);
        assert_eq!(out.events.len(), 1);
        assert_eq!(out.rejected[&RejectReason::NotPushedSource], 1);
        assert_eq!(out.rejected[&RejectReason::UnknownState], 2);
        // 何が来たのか実値で分かるようにする
        assert_eq!(
            out.unknown_states,
            ["待機".to_string(), "点呼".to_string()]
                .into_iter()
                .collect()
        );
    }

    #[test]
    fn dedup_keeps_the_timecard_row_on_a_pk_collision() {
        // PK は (tenant, driver, occurred_at, state) で source を含まない
        let kept = dedup_events(vec![
            ev("2026-07-01 08:00:00", "始業", "dtako"),
            ev("2026-07-01 08:00:00", "始業", "timecard"),
        ]);
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].source, "timecard", "人が確定させた打刻を残す");
    }

    #[test]
    fn dedup_is_order_independent() {
        let a = dedup_events(vec![
            ev("2026-07-01 08:00:00", "始業", "timecard"),
            ev("2026-07-01 08:00:00", "始業", "dtako"),
        ]);
        let b = dedup_events(vec![
            ev("2026-07-01 08:00:00", "始業", "dtako"),
            ev("2026-07-01 08:00:00", "始業", "timecard"),
        ]);
        assert_eq!(a, b);
    }

    #[test]
    fn dedup_keeps_rows_that_differ_in_state_or_time() {
        let kept = dedup_events(vec![
            ev("2026-07-01 08:00:00", "始業", "timecard"),
            ev("2026-07-01 08:00:00", "運行開始", "dtako"),
            ev("2026-07-01 08:00:01", "始業", "dtako"),
        ]);
        assert_eq!(kept.len(), 3);
    }

    #[test]
    fn dedup_separates_drivers() {
        let mut other = ev("2026-07-01 08:00:00", "始業", "dtako");
        other.driver_cd = 1131;
        let kept = dedup_events(vec![ev("2026-07-01 08:00:00", "始業", "timecard"), other]);
        assert_eq!(kept.len(), 2);
    }

    #[test]
    fn group_by_date_uses_the_jst_calendar_day() {
        let g = group_by_date(&dedup_events(vec![
            ev("2026-07-01 23:59:59", "終業", "timecard"),
            ev("2026-07-02 00:00:00", "始業", "timecard"),
        ]));
        assert_eq!(g.len(), 2);
        assert_eq!(g[&d(2026, 7, 1)].len(), 1);
        assert_eq!(g[&d(2026, 7, 2)].len(), 1);
    }

    #[test]
    fn day_signature_is_stable_and_order_independent() {
        let a = day_signature(&[
            ev("2026-07-01 08:00:00", "始業", "timecard"),
            ev("2026-07-01 18:00:00", "終業", "timecard"),
        ]);
        let b = day_signature(&[
            ev("2026-07-01 18:00:00", "終業", "timecard"),
            ev("2026-07-01 08:00:00", "始業", "timecard"),
        ]);
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn day_signature_changes_when_any_signed_field_changes() {
        let base = vec![ev("2026-07-01 08:00:00", "始業", "timecard")];
        let sig = day_signature(&base);

        let mut t = base.clone();
        t[0].occurred_at = dt("2026-07-01 08:00:01");
        assert_ne!(day_signature(&t), sig, "時刻");

        let mut s = base.clone();
        s[0].state = "終業".to_string();
        assert_ne!(day_signature(&s), sig, "state");

        let mut src = base.clone();
        src[0].source = "dtako".to_string();
        assert_ne!(day_signature(&src), sig, "source");

        let mut u = base.clone();
        u[0].unko_no = Some("OP-1".to_string());
        assert_ne!(day_signature(&u), sig, "unko_no");
    }

    #[test]
    fn day_signature_ignores_raw() {
        // raw は追跡用のメタデータ。上流が列を足しただけで全日が差分になっては困る
        let mut with_raw = vec![ev("2026-07-01 08:00:00", "始業", "timecard")];
        let sig = day_signature(&with_raw);
        with_raw[0].raw = json!({"何か": "増えた列"});
        assert_eq!(day_signature(&with_raw), sig);
    }

    #[test]
    fn day_signature_of_nothing_is_the_empty_hash() {
        // 相手に行が無い日と「空の日」を同じ扱いにしない — 空の日は local に現れない
        assert_eq!(day_signature(&[]).len(), 64);
    }

    #[test]
    fn diff_days_classifies_each_case() {
        let local: BTreeMap<NaiveDate, String> = [
            (d(2026, 7, 1), "same".to_string()),
            (d(2026, 7, 2), "new".to_string()),
            (d(2026, 7, 3), "only-local".to_string()),
        ]
        .into_iter()
        .collect();
        let stored: BTreeMap<NaiveDate, String> = [
            (d(2026, 7, 1), "same".to_string()),
            (d(2026, 7, 2), "old".to_string()),
            (d(2026, 7, 4), "only-stored".to_string()),
        ]
        .into_iter()
        .collect();
        let got = diff_days(&local, &stored);
        assert_eq!(
            got,
            vec![
                DayDiff {
                    date: d(2026, 7, 1),
                    kind: DayDiffKind::Unchanged
                },
                DayDiff {
                    date: d(2026, 7, 2),
                    kind: DayDiffKind::Changed
                },
                DayDiff {
                    date: d(2026, 7, 3),
                    kind: DayDiffKind::Changed
                },
                // 元が消えた日は Supabase 側からも消す
                DayDiff {
                    date: d(2026, 7, 4),
                    kind: DayDiffKind::Deleted
                },
            ]
        );
    }

    #[test]
    fn occurred_at_tz_is_jst() {
        let e = ev("2026-07-01 08:00:00", "始業", "timecard");
        assert_eq!(e.occurred_at_tz().to_rfc3339(), "2026-07-01T08:00:00+09:00");
    }

    #[test]
    fn report_knows_when_it_wrote_and_when_it_was_surprised() {
        let mut r = PushReport::default();
        assert!(!r.wrote_anything());
        assert!(!r.has_unexpected());

        r.days_changed = 1;
        assert!(r.wrote_anything());

        // 想定内の読み飛ばし (dtako_events) は「想定外」に数えない
        r.rejected.insert(RejectReason::NotPushedSource, 100);
        assert!(!r.has_unexpected());

        r.rejected.insert(RejectReason::UnknownState, 1);
        assert!(r.has_unexpected());
    }

    #[test]
    fn report_merges_parse_outcomes() {
        let mut r = PushReport::default();
        let mut o = ParseOutcome::default();
        o.rejected.insert(RejectReason::NotPushedSource, 3);
        o.unknown_states.insert("点呼".to_string());
        r.merge(&o);
        r.merge(&o);
        assert_eq!(r.rejected[&RejectReason::NotPushedSource], 6);
        assert_eq!(r.unknown_states.len(), 1);
    }

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
