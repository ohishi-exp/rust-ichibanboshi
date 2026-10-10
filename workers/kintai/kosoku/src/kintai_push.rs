//! 打刻の Supabase (`kintai` スキーマ) への書き込みの純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/kintai_push.rs` から移した。**DB も I/O も持たない** — SQL 定数・生行の写し・重複の決着・
//! 日単位の署名・差分の計画・bind に渡す「列ごとの Vec の束」を作るところまで。bind と transaction は
//! root (sqlx) と勤怠 Worker (tokio-postgres の `query_typed` / `execute_typed`) がそれぞれ持つ。
//! 束の中身と SQL の文字列は移す前と同じ (`tests/pg_write_snapshot.rs` が基点の値で縛る)。
//!
//! 署名・主キーの衝突・`COLLATE "C"` の理由は root の `src/kintai_push.rs` のモジュール docs。
//!
//! **型なしの `ANY($5)` (`DELETE_DAYS_SQL`) は書き換えない。** `query_typed` は引数の型を送るので要らず、
//! SQL のバイト一致を保つ。

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
use sha2::{Digest, Sha256};

/// 生行 / 署名の日時書式。
pub use crate::window::DATETIME_FORMAT;

/// JST。日本標準時に夏時間は無いので固定オフセットで表せる。
///
/// `chrono-tz` を足さないのは、この 1 か所のためだけに timezone データベースを
/// 抱えることになるため。`AT TIME ZONE 'Asia/Tokyo'` (Postgres 側) との一致は
/// オフセットが恒久的に +09:00 であることに依る。
pub const JST_OFFSET_SECONDS: i32 = 9 * 3600;

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
/// 運ばない**。読んでから捨てるのではなく root の `kintai_repo` の SQL で落とす。
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
   AND source = ANY($5)
 GROUP BY 1
"#;

/// 打刻側の月ゲート材料 (Refs #205 実装計画 13)。[`STORED_SIGNATURES_SQL`] と
/// 同じ列の組み立てを月まるごと 1 行の sha256 へ畳む — 乗務員ごと・暦日ごとの
/// 表を Rust 側へ引いてから畳み直さず、集計そのものを Postgres の 1 クエリで
/// 終わらせる。`coalesce(string_agg(...), '')` は打刻が 1 件も無い月でも
/// `sha256('')` の固定値を返すため (`string_agg` は行が無いと NULL になり、
/// `sha256(NULL)` も NULL — `fold_gate.punch_digest` は NOT NULL なのでここで潰す)。
pub const MONTH_PUNCH_DIGEST_SQL: &str = r#"
SELECT encode(sha256(convert_to(coalesce(string_agg(
           driver_cd || '|' ||
           to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
             || '|' || state || '|' || source || '|' || coalesce(unko_no, ''),
           E'\n' ORDER BY driver_cd, occurred_at, state COLLATE "C", source COLLATE "C"), ''),
         'UTF8')), 'hex') AS digest
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3 AND source = ANY($4)
"#;

/// [`STORED_SIGNATURES_SQL`] の**複数乗務員版**。式は 1 文字も変えない。
///
/// 署名の突き合わせを**受け側の中**でやるための口 (Refs #205 の 04b)。送り側が
/// 乗務員ごとに `GET /signatures` を叩いていた頃は 94 名で 33.6 秒かかっていた —
/// 往復の回数が費用で、突き合わせそのものは同じ DB の中なら実質ただ。
///
/// `driver_cd` を先頭に足しただけなので `kintai_events_driver_time` の索引順に
/// 乗る。**式を写し間違えると「中身は同じなのに毎回全日が違う」**ので、
/// 2 つが同じであることはテストで縛る。
pub const STORED_WINDOW_SIGNATURES_SQL: &str = r#"
SELECT driver_cd,
       (occurred_at AT TIME ZONE 'Asia/Tokyo')::date AS d,
       encode(sha256(convert_to(string_agg(
           to_char(occurred_at AT TIME ZONE 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI:SS')
             || '|' || state || '|' || source || '|' || coalesce(unko_no, ''),
           E'\n' ORDER BY occurred_at, state COLLATE "C", source COLLATE "C"), 'UTF8')), 'hex') AS sig
  FROM kintai.kintai_events
 WHERE tenant_id = $1 AND driver_cd = ANY($2)
   AND occurred_at >= $3 AND occurred_at < $4
   AND source = ANY($5)
 GROUP BY 1, 2
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

/// 1 文に載せる INSERT の行数。`raw` を積むので本文サイズで刻む。
///
/// 畳む側 (`crate::kintai_fold` の束を書く root と Worker) も同じ数で刻む — 刻み幅を 2 つ
/// 持つと、片方だけ直したときに「どちらの経路で 524 が出たか」が分からなくなる。
pub const INSERT_CHUNK: usize = 2000;

/// 消す日を **1 文で**。`unnest` で (乗務員, 日の境界) の並びを行に開く。
///
/// 範囲比較のままなので `kintai_events_driver_time` の索引に乗る。
/// `(occurred_at AT TIME ZONE 'Asia/Tokyo')::date = ANY(...)` と書くと関数適用で
/// 索引が効かなくなる (`kintai_repo` の `COALESCE` で同じ罠を踏んでいる)。
/// **この経路が作った行しか消さない。** DDL は `alc_app` も許すが
/// ([`PUSHED_SOURCES`] のとおり) ここは作らない。絞らずに日ごと消すと、他が書いた
/// 行を巻き添えで消して二度と戻せない — 手元の payload から再生できないため。
///
/// [`STORED_SIGNATURES_SQL`] 側も同じ `source` で絞る。**片方だけ絞ると
/// 「中身は同じなのに毎回全日が違う」に倒れる。**
pub const DELETE_DAYS_SQL: &str = r#"
DELETE FROM kintai.kintai_events e
 USING unnest($2::int8[], $3::timestamptz[], $4::timestamptz[]) AS d(driver_cd, from_ts, to_ts)
 WHERE e.tenant_id = $1
   AND e.driver_cd = d.driver_cd
   AND e.occurred_at >= d.from_ts
   AND e.occurred_at < d.to_ts
   AND e.source = ANY($5)
"#;

/// 入れる行を **1 文で**。列ごとの配列を `unnest` で行に開く。
pub const INSERT_EVENTS_SQL: &str = r#"
INSERT INTO kintai.kintai_events
       (tenant_id, driver_cd, occurred_at, state, source, unko_no, raw)
SELECT $1, d.driver_cd, d.occurred_at, d.state, d.source, d.unko_no, d.raw
  FROM unnest($2::int8[], $3::timestamptz[], $4::text[], $5::text[], $6::text[], $7::jsonb[])
       AS d(driver_cd, occurred_at, state, source, unko_no, raw)
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

/// 窓ぶんの打刻を**まるごと**受け取る本体 (Refs #205 の 04b)。
///
/// 送り主は乗務員でも日でも刻まない。理由は実測 — 乗務員ごとに署名を引いていた
/// レグが 94 名で **33.6 秒 / 全体の 94%** を占めていた一方、同じ月の全打刻の転送は
/// **1.3 秒**で済んでいた。費用は往復の回数であって転送量ではない。
///
/// ## なぜ「新しいぶんだけ」にしないのか
///
/// **始業 / 終業は後から直る。** 積み増しだけにすると、直された打刻が永久に
/// 反映されない。よって窓 (既定は当月 + 前月) を毎回まるごと送り直す。
/// 書き込みが無駄にならないのは日単位署名が守るから — **変わった日しか書かない**。
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct TimecardWindow {
    /// 覆う月 (`YYYY-MM`)。**送り主はこの月ぶんを漏れなく送っていること** —
    /// 受け側はこの範囲の内側でしか書かないし、消さない。
    pub months: Vec<String>,
    /// 送り主が見つけた乗務員CD。**消してよい範囲を決めるのはこれ。**
    ///
    /// 全乗務員を対象にすると、送り主が知らない乗務員の日まで「元が消えた」と
    /// 見なして消しにいく。名乗った範囲だけに閉じる。
    #[serde(default)]
    pub drivers: Vec<i64>,
    /// 生行。乗務員も日も混ざったまま。束ねるのは受け側。
    #[serde(default)]
    pub events: Vec<serde_json::Value>,
    /// **`true` なら 1 行も書かない。** 計画だけ立てて件数を返す。
    ///
    /// 既定が `false` (= 書く) なのは、この口が「窓を渡す」以外の意味を持たない
    /// から。dry-run は呼び出し側が明示する — CLI の `--apply` とは既定が逆で、
    /// **口の外 (relay / MCP tool) が「apply が無ければ dry_run を立てる」**形で
    /// 安全側を作る。
    #[serde(default)]
    pub dry_run: bool,
    /// 反映のあとに**畳み直すか** (Refs #205 の 06)。既定 `true`。
    ///
    /// 既定を「畳む」にしてあるのが 06 そのもの — 読み出しは計算しないので、
    /// 打刻を運んだのに畳み直していない状態は遅いのではなく**静かに間違う**。
    /// 切れるようにしてあるのは、運ぶのと畳むのを別々に試したい場合のため。
    #[serde(default = "yes")]
    pub fold: bool,
}

/// `serde(default)` 用。既定を「畳む」にするため。
fn yes() -> bool {
    true
}

impl Default for TimecardWindow {
    fn default() -> Self {
        Self {
            months: Vec::new(),
            drivers: Vec::new(),
            events: Vec::new(),
            dry_run: false,
            fold: true,
        }
    }
}

/// root の `apply_timecard_window` の結果。
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct TimecardWindowResult {
    /// 送り主が名乗った乗務員数。
    pub drivers: usize,
    /// 実際に書き換えた乗務員数。**大半は 0 のはず** (打刻はほとんど戻らない)。
    pub drivers_written: usize,
    /// 書き換えた乗務員CD。**apply 後に畳み直す対象がこれ** (Refs #205 の 06)。
    ///
    /// 件数だけでは「誰を畳み直すか」が決まらない。窓の受け口はこの並びを
    /// root の `kintai_fold::recalc_drivers` に渡して、変わった乗務員だけを
    /// 畳み直す — 定常時はほぼ空なので、束ねても proxy の 100 秒に収まる。
    #[serde(default)]
    pub drivers_changed: Vec<i64>,
    pub days_written: usize,
    pub days_deleted: usize,
    pub events_written: usize,
    pub deduped: usize,
    #[serde(default)]
    pub rejected: BTreeMap<String, usize>,
    #[serde(default)]
    pub unknown_states: BTreeSet<String>,
    /// 窓の外 / 名乗っていない乗務員の行。**0 でないなら送り側が壊れている**。
    pub misplaced: usize,
    /// **`true` なら件数は計画であって実績ではない** (1 行も書いていない)。
    ///
    /// 応答に出さないと、dry-run の `days_written` を書けたものと読み違える。
    pub dry_run: bool,
    pub elapsed_ms: u64,
}

impl TimecardWindowResult {
    pub fn has_unexpected(&self) -> bool {
        self.misplaced > 0 || !self.unknown_states.is_empty()
    }
}

/// 1 乗務員ぶんの書き換え計画。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DriverPlan {
    /// 書き直す日 (delete-then-insert)。
    pub changed: BTreeMap<NaiveDate, Vec<PushEvent>>,
    /// 消す日。**窓の内側だけ。**
    pub deleted: Vec<NaiveDate>,
}

/// 受け取った窓と、既に持っている署名から、**書き換える日だけ**を決める。
///
/// DB を触らない — root の `apply_timecard_window` が前後で読み書きする。`plan_batch` を
/// 純粋にしてあるのと同じ理由で、判定そのものはテストで縛れるようにする。
///
/// **変化の無い乗務員は返さない。** 打刻はほとんど戻らないので、大半はここで落ちる。
pub fn plan_window(
    spans: &[(NaiveDate, NaiveDate)],
    declared: &BTreeSet<i64>,
    events: &[serde_json::Value],
    remote: &BTreeMap<i64, BTreeMap<NaiveDate, String>>,
) -> (BTreeMap<i64, DriverPlan>, TimecardWindowResult) {
    let in_window = |d: NaiveDate| spans.iter().any(|(a, b)| d >= *a && d < *b);
    let mut result = TimecardWindowResult {
        drivers: declared.len(),
        ..Default::default()
    };

    let parsed = parse_rows(events);
    for (reason, n) in &parsed.rejected {
        *result.rejected.entry(format!("{reason:?}")).or_default() += n;
    }
    for s in &parsed.unknown_states {
        if result.unknown_states.len() < MAX_REPORTED_STATES {
            result.unknown_states.insert(s.clone());
        }
    }
    let before = parsed.events.len();
    let kept: Vec<PushEvent> = parsed
        .events
        .into_iter()
        .filter(|e| in_window(e.date()) && declared.contains(&e.driver_cd))
        .collect();
    result.misplaced = before - kept.len();
    let deduped = dedup_events(kept);
    result.deduped = before - result.misplaced - deduped.len();

    let mut local: BTreeMap<i64, BTreeMap<NaiveDate, Vec<PushEvent>>> = BTreeMap::new();
    for ev in deduped {
        local
            .entry(ev.driver_cd)
            .or_default()
            .entry(ev.date())
            .or_default()
            .push(ev);
    }

    let mut plans: BTreeMap<i64, DriverPlan> = BTreeMap::new();
    for driver in declared {
        let mine = local.remove(driver).unwrap_or_default();
        let theirs = remote.get(driver).cloned().unwrap_or_default();
        let local_sigs: BTreeMap<NaiveDate, String> = mine
            .iter()
            .map(|(d, evs)| (*d, day_signature(evs)))
            .collect();
        let mut plan = DriverPlan::default();
        for diff in diff_days(&local_sigs, &theirs) {
            match diff.kind {
                DayDiffKind::Unchanged => {}
                DayDiffKind::Changed => {
                    plan.changed.insert(diff.date, mine[&diff.date].clone());
                }
                // 署名の引き当ては `lo..hi` なので、月が飛んでいると隙間ぶんが
                // 混ざる。**窓の外は消さない** — 送り主が覆っていない範囲だから
                DayDiffKind::Deleted if in_window(diff.date) => plan.deleted.push(diff.date),
                DayDiffKind::Deleted => {}
            }
        }
        if plan.changed.is_empty() && plan.deleted.is_empty() {
            continue;
        }
        result.drivers_written += 1;
        result.drivers_changed.push(*driver);
        result.days_written += plan.changed.len();
        result.days_deleted += plan.deleted.len();
        result.events_written += plan.changed.values().map(Vec::len).sum::<usize>();
        plans.insert(*driver, plan);
    }
    (plans, result)
}

/// 対象月の `[月初, 翌月初)` を `DATE` の境界で返す。
pub fn month_date_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
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

// ── 受け口の純粋部分と bind の束 (Refs #322) ─────────────────────────────────

/// 受け取った 1 乗務員ぶんの batch を、書き換えの計画と応答に (root の `apply_timecard_batch` の純粋部分)。
///
/// **送り主を信用しない。** 日のキーと行の中身が食い違う行、対象の乗務員でない行、対象月から外れた日は
/// 落として数える。計画は書くものが無ければ空 (書き手は往復しない)。`Err` は月が読めないときの文言。
pub fn plan_received_batch(
    batch: &TimecardBatch,
) -> Result<(BTreeMap<i64, DriverPlan>, TimecardBatchResult), String> {
    let (m0, m1) =
        month_date_bounds(&batch.month).ok_or_else(|| format!("bad month: {}", batch.month))?;
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

    let mut plans = BTreeMap::new();
    if !changed.is_empty() || !deleted.is_empty() {
        plans.insert(batch.driver_cd, DriverPlan { changed, deleted });
    }
    Ok((plans, result))
}

/// 窓の月の並びを `[月初, 翌月初)` の並びと、署名の引き当てに使う端から端まで (`lo`, `hi`) に
/// (root の `apply_timecard_window` の純粋部分)。`Err` は応答の文言 (`months が空です` / `bad month: …`)。
#[allow(clippy::type_complexity)]
pub fn window_spans(
    months: &[String],
) -> Result<(Vec<(NaiveDate, NaiveDate)>, NaiveDate, NaiveDate), String> {
    if months.is_empty() {
        return Err("months が空です".to_string());
    }
    let mut spans: Vec<(NaiveDate, NaiveDate)> = Vec::new();
    for m in months {
        spans.push(month_date_bounds(m).ok_or_else(|| format!("bad month: {m}"))?);
    }
    // 署名の引き当ては 1 クエリで済ませたいので端から端まで。月が飛んでいると
    // 隙間ぶんが混ざるが、書く / 消すの判定は [`plan_window`] が月ごとに閉じる
    let lo = spans.iter().map(|s| s.0).min().unwrap_or_default();
    let hi = spans.iter().map(|s| s.1).max().unwrap_or_default();
    Ok((spans, lo, hi))
}

/// [`DELETE_DAYS_SQL`] の `$2`〜`$4` (乗務員, 日の始まり, 日の終わり)。`OLD_EVENTS_SQL` (変更履歴) も同じ束を読む。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeleteDays {
    pub driver_cd: Vec<i64>,
    pub from: Vec<DateTime<FixedOffset>>,
    pub to: Vec<DateTime<FixedOffset>>,
}

impl DeleteDays {
    pub fn is_empty(&self) -> bool {
        self.driver_cd.is_empty()
    }
}

/// 置き換える日 (消す日と書き直す日) を 1 本の配列に畳む。空なら書くものが無い。
pub fn delete_days(plans: &BTreeMap<i64, DriverPlan>) -> DeleteDays {
    let mut out = DeleteDays::default();
    for (driver, plan) in plans {
        for date in plan.deleted.iter().chain(plan.changed.keys()) {
            let (from, to) = jst_day_bounds(*date);
            out.driver_cd.push(*driver);
            out.from.push(from);
            out.to.push(to);
        }
    }
    out
}

/// [`INSERT_EVENTS_SQL`] の `$2`〜`$7` (int8[]・timestamptz[]・text[]・text[]・text[]・jsonb[])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventColumns {
    pub driver_cd: Vec<i64>,
    pub occurred_at: Vec<DateTime<FixedOffset>>,
    pub state: Vec<String>,
    pub source: Vec<String>,
    pub unko_no: Vec<Option<String>>,
    pub raw: Vec<serde_json::Value>,
}

/// 書き直す日の行を [`INSERT_CHUNK`] 行ごとの束に (1 束 = INSERT 1 文)。
pub fn event_columns(plans: &BTreeMap<i64, DriverPlan>) -> Vec<EventColumns> {
    event_columns_by(plans, INSERT_CHUNK)
}

fn event_columns_by(plans: &BTreeMap<i64, DriverPlan>, chunk_rows: usize) -> Vec<EventColumns> {
    let rows: Vec<&PushEvent> = plans
        .values()
        .flat_map(|p| p.changed.values())
        .flatten()
        .collect();
    rows.chunks(chunk_rows)
        .map(|chunk| EventColumns {
            driver_cd: chunk.iter().map(|e| e.driver_cd).collect(),
            occurred_at: chunk.iter().map(|e| e.occurred_at_tz()).collect(),
            state: chunk.iter().map(|e| e.state.clone()).collect(),
            source: chunk.iter().map(|e| e.source.clone()).collect(),
            unko_no: chunk.iter().map(|e| e.unko_no.clone()).collect(),
            raw: chunk.iter().map(|e| e.raw.clone()).collect(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
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

    // ── 窓ぶんをまるごと受ける (Refs #205 の 04b) ────────────────────────────

    /// 生行 1 つ。`parse_rows` が読む形。
    fn raw(driver: i64, at: &str, state: &str) -> serde_json::Value {
        json!({
            "datetime": at,
            "end_datetime": null,
            "driver_id": driver,
            "source": "timecard",
            "state": state,
            "unko_no": null,
            "vehicle": null,
        })
    }

    fn spans_of(months: &[&str]) -> Vec<(NaiveDate, NaiveDate)> {
        months
            .iter()
            .map(|m| month_date_bounds(m).unwrap())
            .collect()
    }

    fn declared_of(ids: &[i64]) -> BTreeSet<i64> {
        ids.iter().copied().collect()
    }

    /// 署名が一致する乗務員は**計画に現れない**。打刻はほとんど戻らないので、
    /// 窓を毎回送り直しても書き込みは出ない。
    #[test]
    fn plan_window_skips_drivers_that_did_not_change() {
        let events = vec![raw(1130, "2026-06-01 08:00:00", "始業")];
        let sig = day_signature(&parse_rows(&events).events);
        let remote = BTreeMap::from([(1130, BTreeMap::from([(d(2026, 6, 1), sig)]))]);
        let (plans, result) = plan_window(
            &spans_of(&["2026-06"]),
            &declared_of(&[1130]),
            &events,
            &remote,
        );
        assert!(plans.is_empty(), "{plans:?}");
        assert_eq!(result.drivers, 1);
        assert_eq!(result.drivers_written, 0);
        assert_eq!(result.days_written, 0);
    }

    /// 始業が直されたら**その日だけ**書き直す。他の日は触らない。
    #[test]
    fn plan_window_rewrites_only_the_edited_day() {
        let events = vec![
            raw(1130, "2026-06-01 08:00:00", "始業"),
            raw(1130, "2026-06-02 07:30:00", "始業"),
        ];
        let parsed = parse_rows(&events);
        let by_day = group_by_date(&dedup_events(parsed.events));
        // 1 日は一致、2 日は相手が古い値を持っている
        let remote = BTreeMap::from([(
            1130,
            BTreeMap::from([
                (d(2026, 6, 1), day_signature(&by_day[&d(2026, 6, 1)])),
                (d(2026, 6, 2), "stale".to_string()),
            ]),
        )]);
        let (plans, result) = plan_window(
            &spans_of(&["2026-06"]),
            &declared_of(&[1130]),
            &events,
            &remote,
        );
        assert_eq!(
            plans[&1130].changed.keys().copied().collect::<Vec<_>>(),
            vec![d(2026, 6, 2)]
        );
        assert!(plans[&1130].deleted.is_empty());
        assert_eq!(result.days_written, 1);
        assert_eq!(result.drivers_written, 1);
    }

    /// 相手にあってこちらに無い日は消す (元の打刻が消えた)。
    #[test]
    fn plan_window_deletes_days_the_source_no_longer_has() {
        let remote = BTreeMap::from([(
            1130,
            BTreeMap::from([(d(2026, 6, 10), "whatever".to_string())]),
        )]);
        let (plans, result) =
            plan_window(&spans_of(&["2026-06"]), &declared_of(&[1130]), &[], &remote);
        assert_eq!(plans[&1130].deleted, vec![d(2026, 6, 10)]);
        assert_eq!(result.days_deleted, 1);
    }

    /// **窓の外は消さない。** 月が飛んでいると署名の引き当てに隙間月が混ざるが、
    /// 送り主が覆っていない範囲なので触ってはいけない。
    #[test]
    fn plan_window_never_deletes_outside_the_window() {
        let remote = BTreeMap::from([(
            1130,
            BTreeMap::from([
                (d(2026, 6, 10), "in".to_string()),
                // 隙間の 7 月。送り主は覆っていない
                (d(2026, 7, 10), "gap".to_string()),
                (d(2026, 8, 10), "in".to_string()),
            ]),
        )]);
        let (plans, _) = plan_window(
            &spans_of(&["2026-06", "2026-08"]),
            &declared_of(&[1130]),
            &[],
            &remote,
        );
        assert_eq!(plans[&1130].deleted, vec![d(2026, 6, 10), d(2026, 8, 10)]);
    }

    /// **名乗っていない乗務員・窓の外の日は書かない。** 数えて報告する。
    #[test]
    fn plan_window_refuses_rows_outside_what_the_sender_declared() {
        let events = vec![
            // 名乗っていない乗務員
            raw(9999, "2026-06-01 08:00:00", "始業"),
            // 窓の外の月
            raw(1130, "2026-05-01 08:00:00", "始業"),
            raw(1130, "2026-06-01 08:00:00", "始業"),
        ];
        let (plans, result) = plan_window(
            &spans_of(&["2026-06"]),
            &declared_of(&[1130]),
            &events,
            &BTreeMap::new(),
        );
        assert_eq!(result.misplaced, 2);
        assert_eq!(plans.len(), 1);
        assert_eq!(
            plans[&1130].changed.keys().copied().collect::<Vec<_>>(),
            vec![d(2026, 6, 1)]
        );
    }

    /// 窓の SQL は 1 名版と**式が 1 文字も違わない**。写し間違えると
    /// 「中身は同じなのに毎回全日が違う」になる。
    #[test]
    fn the_window_signature_sql_matches_the_single_driver_one() {
        let normalise = |s: &str| {
            s.replace("driver_cd = ANY($2)", "driver_cd = $2")
                .replace(
                    "SELECT driver_cd,\n       (occurred_at",
                    "SELECT (occurred_at",
                )
                .replace("GROUP BY 1, 2", "GROUP BY 1")
        };
        assert_eq!(
            normalise(STORED_WINDOW_SIGNATURES_SQL).replace([' ', '\n'], ""),
            STORED_SIGNATURES_SQL.replace([' ', '\n'], "")
        );
    }

    // ── 受け口の純粋部分と bind の束 ──

    fn batch(month: &str) -> TimecardBatch {
        TimecardBatch {
            month: month.to_string(),
            driver_cd: 1130,
            days: BTreeMap::new(),
            delete_dates: Vec::new(),
        }
    }

    #[test]
    fn a_received_batch_with_a_bad_month_is_refused() {
        assert_eq!(
            plan_received_batch(&batch("2026-13")).unwrap_err(),
            "bad month: 2026-13"
        );
    }

    #[test]
    fn an_empty_received_batch_plans_nothing() {
        let (plans, result) = plan_received_batch(&batch("2026-06")).unwrap();
        assert!(plans.is_empty());
        assert_eq!(result, TimecardBatchResult::default());
    }

    #[test]
    fn a_received_batch_drops_what_the_sender_should_not_have_sent() {
        let mut b = batch("2026-06");
        // 月の外の日 (行ごと落とす)
        b.days.insert(
            d(2026, 7, 1),
            vec![raw(1130, "2026-07-01 08:00:00", "始業")],
        );
        b.days.insert(
            d(2026, 6, 1),
            vec![
                raw(1130, "2026-06-01 08:00:00", "始業"),
                // 日のキーと中身が違う
                raw(1130, "2026-06-02 08:00:00", "始業"),
                // 別の乗務員
                raw(9999, "2026-06-01 08:00:00", "始業"),
                // 読めない state
                raw(1130, "2026-06-01 09:00:00", "点呼"),
            ],
        );
        b.delete_dates = vec![d(2026, 6, 3), d(2026, 5, 31)];
        let (plans, result) = plan_received_batch(&b).unwrap();
        assert_eq!(result.misplaced, 3);
        assert_eq!(result.days_written, 1);
        assert_eq!(result.days_deleted, 1);
        assert_eq!(result.events_written, 1);
        assert_eq!(result.rejected["UnknownState"], 1);
        assert!(result.unknown_states.contains("点呼"));
        assert!(result.has_unexpected());
        let plan = &plans[&1130];
        assert_eq!(plan.changed[&d(2026, 6, 1)].len(), 1);
        assert_eq!(plan.deleted, vec![d(2026, 6, 3)]);
    }

    #[test]
    fn a_received_batch_caps_the_reported_states() {
        let mut b = batch("2026-06");
        let rows = (0..MAX_REPORTED_STATES + 3)
            .map(|i| raw(1130, "2026-06-01 08:00:00", &format!("未知{i}")))
            .collect();
        b.days.insert(d(2026, 6, 1), rows);
        let (_, result) = plan_received_batch(&b).unwrap();
        assert_eq!(result.unknown_states.len(), MAX_REPORTED_STATES);
    }

    #[test]
    fn window_spans_cover_the_ends_and_refuse_bad_months() {
        assert_eq!(window_spans(&[]).unwrap_err(), "months が空です");
        assert_eq!(
            window_spans(&["2026-6".to_string()]).unwrap_err(),
            "bad month: 2026-6"
        );
        let (spans, lo, hi) =
            window_spans(&["2026-08".to_string(), "2026-06".to_string()]).unwrap();
        assert_eq!(spans.len(), 2);
        assert_eq!(lo, d(2026, 6, 1));
        assert_eq!(hi, d(2026, 9, 1));
    }

    #[test]
    fn the_bind_columns_follow_the_plans() {
        let mut changed = BTreeMap::new();
        changed.insert(
            d(2026, 6, 1),
            vec![
                ev("2026-06-01 08:00:00", "始業", "timecard"),
                ev("2026-06-01 18:00:00", "終業", "timecard"),
            ],
        );
        let plans = BTreeMap::from([(
            1130,
            DriverPlan {
                changed,
                deleted: vec![d(2026, 6, 2)],
            },
        )]);
        let days = delete_days(&plans);
        assert!(!days.is_empty());
        // 消す日が先、書き直す日が後 (元の replace_window と同じ順)
        assert_eq!(days.driver_cd, vec![1130, 1130]);
        assert_eq!(days.from[0].to_rfc3339(), "2026-06-02T00:00:00+09:00");
        assert_eq!(days.to[1].to_rfc3339(), "2026-06-02T00:00:00+09:00");
        assert!(delete_days(&BTreeMap::new()).is_empty());

        assert_eq!(event_columns(&plans).len(), 1);
        let chunks = event_columns_by(&plans, 1);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[1].state, vec!["終業".to_string()]);
        assert!(event_columns(&BTreeMap::new()).is_empty());
    }

    #[test]
    fn a_window_body_defaults_to_write_and_fold() {
        let w: TimecardWindow = serde_json::from_value(json!({"months": ["2026-06"]})).unwrap();
        assert!(!w.dry_run);
        assert!(w.fold, "既定は畳み直す");
        let d = TimecardWindow::default();
        assert!(d.fold && !d.dry_run && d.months.is_empty());
    }

    #[test]
    fn results_say_when_the_sender_is_broken() {
        let mut r = TimecardWindowResult::default();
        assert!(!r.has_unexpected());
        r.misplaced = 1;
        assert!(r.has_unexpected());
        let r = TimecardWindowResult {
            unknown_states: BTreeSet::from(["点呼".to_string()]),
            ..Default::default()
        };
        assert!(r.has_unexpected());
        assert!(!TimecardBatchResult::default().has_unexpected());
    }

    #[test]
    fn plan_window_reports_rejected_rows_and_caps_the_states() {
        let mut events: Vec<serde_json::Value> = (0..MAX_REPORTED_STATES + 2)
            .map(|i| raw(1130, "2026-06-01 08:00:00", &format!("未知{i}")))
            .collect();
        events.push(json!({"source": "dtako_events"}));
        let (plans, result) = plan_window(
            &spans_of(&["2026-06"]),
            &declared_of(&[1130]),
            &events,
            &BTreeMap::new(),
        );
        assert!(plans.is_empty());
        assert_eq!(result.rejected["UnknownState"], MAX_REPORTED_STATES + 2);
        assert_eq!(result.rejected["NotPushedSource"], 1);
        assert_eq!(result.unknown_states.len(), MAX_REPORTED_STATES);
    }

    #[test]
    fn month_date_bounds_wraps_the_year() {
        assert_eq!(
            month_date_bounds("2026-12"),
            Some((d(2026, 12, 1), d(2027, 1, 1)))
        );
        assert_eq!(month_date_bounds("2026-13"), None);
    }

    #[test]
    fn plan_batch_sends_raw_rows_for_changed_days_and_names_deleted_days() {
        let events = dedup_events(
            parse_rows(&[
                raw(1130, "2026-06-01 08:00:00", "始業"),
                raw(1130, "2026-06-02 08:00:00", "始業"),
            ])
            .events,
        );
        let local = group_by_date(&events);
        let remote = BTreeMap::from([
            (d(2026, 6, 1), day_signature(&local[&d(2026, 6, 1)])),
            (d(2026, 6, 2), "stale".to_string()),
            (d(2026, 6, 3), "gone".to_string()),
        ]);
        let batch = plan_batch("2026-06", 1130, &local, &remote);
        assert_eq!(batch.month, "2026-06");
        assert_eq!(batch.driver_cd, 1130);
        assert_eq!(
            batch.days.keys().copied().collect::<Vec<_>>(),
            vec![d(2026, 6, 2)]
        );
        assert_eq!(batch.days[&d(2026, 6, 2)][0]["state"], "始業");
        assert_eq!(batch.delete_dates, vec![d(2026, 6, 3)]);
        assert!(!batch.is_empty());
        assert!(plan_batch("2026-06", 1130, &BTreeMap::new(), &BTreeMap::new()).is_empty());
    }
}
