//! `kosoku.rs` の出力を `kintai` スキーマへ畳んで保存する (Refs #205 実装計画 05)。
//!
//! **`kosoku.rs` を 1 行も変えない。** 畳む処理を写すと 2 実装になり、改修のたびに
//! 両方を直すことになる (#205 のリスク欄)。ここがやるのは
//! [`crate::kosoku::daily_summary`] の戻り値を 3 表の行に**写すだけ**。
//!
//! | 表 | 1 行の単位 |
//! |---|---|
//! | `kintai.shifts` | 勤務 1 本 |
//! | `kintai.day_summaries` | 勤務 1 本 (002 で PK に `shift_start_at` を足した) |
//! | `kintai.day_parts` | 勤務 × 乗った暦日 |
//!
//! ## 読み出し経路と同じ手順で計算する
//!
//! `/api/kintai/kosoku-daily` の全乗務員経路 (`routes/kintai.rs` の
//! `kosoku_daily_all`) と**同じ順序**で呼ぶ — 違う手順で畳むと、画面と保存値が
//! 食い違ったときに原因が追えない。
//!
//! 1. `fetch_all_events_between` (対象月を**全乗務員まとめて 1 回**読む)
//! 2. `split_by_driver` (`daily_summary` は乗務員を知らない純粋関数なので先に分ける)
//! 3. `drop_duplicate_rows` (取り込みが 2 回走った重複を落とす)
//! 4. `daily_summary`
//!
//! **1 と 2 を「乗務員ごとに 1 回読む」に置き換えない。** GCP の HTTP 実装
//! ([`crate::kintai_http_repo`]) では 1 乗務員 1 往復が `rust-alc-api` への
//! 1 往復 (裏で R2 の GET 群) になる。95 名 × 2 か月で約 190 往復、しかも乗務員の
//! 列挙で全量をもう 1 周読んでいた。読みは月 1 回に畳み、分けるのは in-process でやる。
//!
//! **フェリー控除 (`apply_ferry_minus`) は呼ばない。** あれが埋めるのは
//! `ferry_minus_minutes` だけで、これは 3 表のどこにも列が無い (紙との差を説明する
//! ためだけの値で、拘束にも実働にも入っていない)。呼んでも保存値は 1 つも変わらず、
//! 乗務員ごとに 1 往復増えるだけになる。
//!
//! ## 指紋 — 何が変わったら再計算するか
//!
//! ```text
//! logic_version = sha256(LOGIC_BASE + "|" + KosokuParams)[..16]
//! fingerprint   = sha256(
//!     logic_version               // ロジックの版と設定を畳んだ 16 桁 hex
//!   + "|" + 乗務員CD + 対象月
//!   + "|" + その乗務員の月の生行 (正規化して並べたもの)
//! )
//! ```
//!
//! - **`KosokuParams` を材料に入れるのが必須。** `restraint_rounding` などは TOML で
//!   再ビルド無しに変えられて出力を変える。入れないと切り替えても全単位が stale に
//!   ならず、古い集計が永久にスキップされる (#205 のリスク欄)
//! - **`LOGIC_BASE` は手で上げる版** (共有 crate の `kintai_fold::LOGIC_BASE`)。計算結果を
//!   変えるときだけ上げる。コメント・移設では上げない (= 全員 stale にならない)。上げ忘れは
//!   golden テスト (`workers/kintai/kosoku/tests/logic_golden.rs`) が落とす。ソースのハッシュ
//!   (`KINTAI_OUTPUT_SHA`) は材料にせず、記録用に [`code_sha`] として応答に載せるだけ (Refs #322)
//! - 材料の生行は**行まるごと**を畳む。`daily_summary` が読むのは 4 列だけだが、
//!   `drop_duplicate_rows` は行の完全一致で重複を判定するので、列が増えれば
//!   結果が変わり得る。**広い方に倒す** — 余分に再計算するのは安全側で、
//!   取りこぼしだけが危ない
//!
//! ### 材料の行は**全乗務員版の形**
//!
//! 全乗務員版の行は `unko_no` / `vehicle` を**キーごと持たない**
//! (`kintai_repo::all_row_to_json` / `kintai_kosoku::alc_events::event_to_all_json`)。
//! 単一乗務員版 (`/api/kintai/events`) は持つので、**同じ打刻でも行 JSON が経路で
//! 違い、指紋も違う値になる。**
//!
//! 畳んだ値そのものは変わらない。[`crate::kosoku::parse_events`] が読むのは
//! `datetime` / `end_datetime` / `source` / `state` の 4 キーだけで、
//! [`crate::kosoku::Event`] に運行NO の場所が無い。効くのは
//! [`crate::kosoku::drop_duplicate_rows`] の判定材料としてだけ —
//! 「同時刻・同イベントで運行NO だけ違う 2 行」(実測 1526 の `…011` / `…012`) が、
//! 全乗務員版では区別が付かず 1 行に潰れる。潰れても拘束・実働・深夜は動かない
//! (休憩も休息も `merge_intervals` / `windows(2)` で畳むので重複に強い)。
//!
//! **だから fold は `driver` 指定の有無にかかわらず常に全乗務員版で読む。**
//! 経路によって形を変えると、同じ (乗務員, 月) の指紋が読み方で割れ、
//! 単一指定の再計算と月次バッチが**互いに相手の書いた行を stale と見て毎回書き直す**。
//! 指紋を跨いで比べてよいのは「同じ読み方をした指紋」だけ。
//!
//! ## `logic_version` は保存行に焼く「ロジックの版 + 設定」の印
//!
//! `LOGIC_BASE` **単体ではない** ([`logic_version`])。単体にすると TOML の
//! 閾値・丸め方を変えたときに保存行の `logic_version` が変わらず、`SELECT DISTINCT
//! logic_version` では設定変更由来の stale が捕まらない — 指紋には入っているので
//! 再計算そのものは走るが、「走らせるべきか」を**読むだけで判る**手段が消える。
//!
//! 指紋は乗務員ごと・月ごとに違うので、stale かどうかを聞くには全単位を数える
//! ことになる。`logic_version` は全単位で同じ値なので 1 クエリで済む
//! ([`stale_state`])。`CHAR(16)` のまま収まるよう先頭 16 桁で切る。
//!
//! ## 単位は (乗務員, 月)
//!
//! issue は「指紋が変わった (乗務員, 日) だけ再計算」と書いているが、**勤務は日を
//! 跨ぐので日を単独では計算できない** (issue 自身も「再計算の対象は差分日 ± 2 日」と
//! している)。月単位のバッチではそれを最後まで広げた形 = (乗務員, 月) が素直で、
//! 差分日 ± 2 日を必ず含む。指紋は行ごとに持たせるので、
//! **書き込みは「その乗務員の月が変わったときだけ」**に絞られる。

use chrono::{DateTime, FixedOffset, NaiveDate};

use crate::kintai_push::{jst_day_bounds, KintaiPgStore, KintaiPushError};
use crate::kintai_repo::{DynKintaiEventsRepo, KintaiRepoError};
use crate::kosoku::KosokuParams;

// 乗務員ごとの窓への切り戻しは共有 crate に置く (Refs #322)。
pub use kintai_kosoku::anchors::{clip_to_anchors, HeadAnchors};

/// `build.rs` が焼き込む「出力に効くコード」の内容ハッシュ (16 桁 hex)。
///
/// **記録用。** 版 ([`logic_version`]) にも指紋にも入れない — 入れるとコメント 1 文字で全乗務員・全月が
/// stale になる (Refs #322)。recalc の応答 ([`FoldReport::code_sha`]) と CLI の表示に載せるだけ。
pub fn code_sha() -> &'static str {
    env!("KINTAI_OUTPUT_SHA")
}

// 3 表の行の型・`DaySummary` からの写し・保存済みの姿・SQL 定数・bind の束・指紋と畳みの純粋部分・報告の型は共有 crate に移した (Refs #322)。
// ここは記録用の `code_sha` と時計を渡す口・sqlx の pool・transaction・bind だけを持つ。呼び出し側のパスを変えないよう再 export する。
pub use kintai_kosoku::kintai_fold::{
    day_part_columns, day_summary_columns, fingerprint, fold_days, fold_driver_month,
    logic_version, shift_columns, tz, DayPartRow, DaySummaryRow, FoldReport, FoldUnit, MonthGate,
    ShiftRow, SkipReason, StaleReport, StoredState, DELETE_SHIFTS_SQL, FOLD_GATE_SELECT_SQL,
    FOLD_GATE_UPSERT_SQL, INSERT_DAY_PARTS_SQL, INSERT_DAY_SUMMARIES_SQL, INSERT_SHIFTS_SQL,
    STALE_STATE_SQL, STORED_STATES_SQL, STORED_STATE_SQL,
};
use kintai_kosoku::kintai_push::month_date_bounds;

// ── 保存 ──────────────────────────────────────────────────────────────────

/// 全量再計算の対象乗務員を 1 ページ (`driver_cd` の keyset ページング)。
///
/// **母集団は Postgres の distinct driver_cd と `extra` (呼び出し側の `unnest`) の和。**
///
/// 昔は Postgres の中だけで決めていた — 生イベントの読み先 (alc / MariaDB) に
/// 聞くと、乗務員を数えるためだけに月ぶんの `dtako_events` を JSON にして捨てる
/// ことになる (`kintai_diff::drivers_page` が同じ理由で `UNION` 1 本に絞ったのと
/// 同じ話)、という考え方自体は正しい。**が、それだと打刻ゼロ・休息由来だけの
/// 乗務員が母集団から永久に漏れる** — `kintai_events` は打刻由来の行しか持たず、
/// `day_summaries` は最低 1 回畳まれていないと現れない。1 回も畳んだことが無い
/// rest-only 乗務員はどちらの表にも 1 行も無い (#205-12、2026-06 実測で 42 名)。
///
/// 呼び出し元 ([`crate::routes::kintai_recalc::run`]) は母集団を決める前に
/// [`fold_month`] を 1 回読んでいる (ページの畳みそのものに要るので、ここで
/// もう 1 回生イベントを読み直す必要が無い)。その `split_by_driver` の
/// キー集合を `extra` として渡せば、月ぶんの読みを増やさずに埋まる。
const RECALC_DRIVER_PAGE_SQL: &str = r#"
WITH pop AS (
    SELECT DISTINCT driver_cd FROM kintai.kintai_events
     WHERE tenant_id = $1 AND occurred_at >= $2 AND occurred_at < $3
    UNION
    SELECT DISTINCT driver_cd FROM kintai.day_summaries
     WHERE tenant_id = $1 AND date >= $4 AND date < $5
    UNION
    SELECT driver_cd FROM unnest($10::int8[]) AS e(driver_cd)
)
SELECT driver_cd FROM pop
 WHERE driver_cd > $6 AND driver_cd > 0
   AND (NOT $7 OR NOT EXISTS (
         SELECT 1 FROM kintai.day_summaries s
          WHERE s.tenant_id = $1 AND s.driver_cd = pop.driver_cd
            AND s.date >= $4 AND s.date < $5
            AND s.logic_version = $8))
 ORDER BY driver_cd
 LIMIT $9
"#;

/// 保存済みの姿を読む。
pub async fn stored_state(
    store: &KintaiPgStore,
    driver_cd: i64,
    month: &str,
) -> Result<StoredState, KintaiPushError> {
    use sqlx::Row;
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let row = sqlx::query(STORED_STATE_SQL)
        .bind(store.tenant_id())
        .bind(driver_cd)
        .bind(m0)
        .bind(m1)
        .fetch_one(store.pool())
        .await?;
    Ok(StoredState {
        fingerprints: row.get::<Vec<String>, _>("fps"),
        shifts: row.get::<i64, _>("n_shifts"),
        day_summaries: row.get::<i64, _>("n_days"),
        day_parts: row.get::<i64, _>("n_parts"),
    })
}

/// [`stored_state`] の**複数乗務員版** — ページの全乗務員を 1 往復で読む。
///
/// 返すのは `driver_cd → StoredState`。**保存が 1 件も無い乗務員も
/// [`StoredState::is_empty`] な値で必ず入る** ([`STORED_STATES_SQL`] の
/// `unnest` 側の行が残るため)。呼び出し側は「鍵が無い = 保存が無い」と
/// 「鍵が無い = 読めていない」を混同せずに済む。
///
/// `drivers` が空なら**1 往復もしない**。
pub async fn stored_states(
    store: &KintaiPgStore,
    drivers: &[i64],
    month: &str,
) -> Result<std::collections::BTreeMap<i64, StoredState>, KintaiPushError> {
    use sqlx::Row;
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    if drivers.is_empty() {
        return Ok(std::collections::BTreeMap::new());
    }
    let rows = sqlx::query(STORED_STATES_SQL)
        .bind(store.tenant_id())
        .bind(drivers)
        .bind(m0)
        .bind(m1)
        .fetch_all(store.pool())
        .await?;
    let mut out = std::collections::BTreeMap::new();
    for row in rows {
        out.insert(
            row.get::<i64, _>("driver_cd"),
            StoredState {
                fingerprints: row.get::<Vec<String>, _>("fps"),
                shifts: row.get::<i64, _>("n_shifts"),
                day_summaries: row.get::<i64, _>("n_days"),
                day_parts: row.get::<i64, _>("n_parts"),
            },
        );
    }
    Ok(out)
}

/// 期間に載っている `logic_version` を数える (stale 検知)。
///
/// 窓の受け口が応答に載せるのはこれ。**畳み直しは起こさない** — 全量再計算は
/// 費用が違う別の口 (`POST /api/kintai/recalc`) の仕事で、ここは「要るかどうか」
/// だけを 1 クエリで答える。
pub async fn stale_state(
    store: &KintaiPgStore,
    months: &[String],
    params: &KosokuParams,
) -> Result<StaleReport, KintaiPushError> {
    use sqlx::Row;
    let version = logic_version(params);
    let (lo, hi) = months_span(months)?;
    let row = sqlx::query(STALE_STATE_SQL)
        .bind(store.tenant_id())
        .bind(lo)
        .bind(hi)
        .bind(&version)
        .fetch_one(store.pool())
        .await?;
    Ok(StaleReport {
        logic_version: version,
        drivers: row.get::<i64, _>("stale_drivers") as usize,
        versions: row.get::<Vec<String>, _>("versions"),
    })
}

/// 全量再計算の対象乗務員を 1 ページ返す。`after` は前ページの最後の乗務員CD。
///
/// `extra` は Postgres の外から足す母集団 (rest-only 乗務員の穴埋め、
/// [`RECALC_DRIVER_PAGE_SQL`] docs 参照)。`driver_cd <= 0` は呼び出し側が
/// 混ぜていても SQL 側で落とす ([`crate::kintai_repo`] の「0 以下は捨てる」と同じ規則)。
///
/// `stale_only` は「対象月に**現行版の行を 1 つも持たない**乗務員」に絞る。
/// 勤務が 1 本も立たない乗務員 (打刻はあるが全部落ちた月) は保存行を作れないので
/// **毎回この網に残る** — 収束しないので、全量を回す用途では `false` で使う。
pub async fn recalc_driver_page(
    store: &KintaiPgStore,
    month: &str,
    params: &KosokuParams,
    extra: &std::collections::BTreeSet<i64>,
    after: Option<i64>,
    stale_only: bool,
    limit: usize,
) -> Result<Vec<i64>, KintaiPushError> {
    use sqlx::Row;
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let extra: Vec<i64> = extra.iter().copied().collect();
    let rows = sqlx::query(RECALC_DRIVER_PAGE_SQL)
        .bind(store.tenant_id())
        .bind(jst_day_bounds(m0).0)
        .bind(jst_day_bounds(m1).0)
        .bind(m0)
        .bind(m1)
        .bind(after.unwrap_or(i64::MIN))
        .bind(stale_only)
        .bind(logic_version(params))
        .bind(limit as i64)
        .bind(&extra)
        .fetch_all(store.pool())
        .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.get::<i64, _>("driver_cd"))
        .collect())
}

/// 月の並びぜんたいを覆う `[最初の月初, 最後の翌月初)`。
///
/// 月が飛んでいても 1 クエリで数える — 隙間ぶんの乗務員が混ざっても
/// 「stale が多めに出る」だけで、取りこぼす側には倒れない。
fn months_span(months: &[String]) -> Result<(NaiveDate, NaiveDate), KintaiPushError> {
    let mut lo: Option<NaiveDate> = None;
    let mut hi: Option<NaiveDate> = None;
    for m in months {
        let (a, b) = month_date_bounds(m)
            .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {m}")))?;
        lo = Some(lo.map_or(a, |c: NaiveDate| c.min(a)));
        hi = Some(hi.map_or(b, |c: NaiveDate| c.max(b)));
    }
    match (lo, hi) {
        (Some(a), Some(b)) => Ok((a, b)),
        _ => Err(KintaiPushError::NotConfigured(
            "months が空です".to_string(),
        )),
    }
}

/// 1 乗務員 1 か月ぶんを置き換える。**1 トランザクション**。
///
/// `shifts` を先に消してから入れ直す — `day_summaries` / `day_parts` は
/// `shifts` への FK を `ON DELETE CASCADE` で持つので、消し忘れが起きない。
///
/// ## 行ごとに往復しない
///
/// 1 行 1 INSERT で回すと、初回 fold (2 か月・95 名で `shifts` 約 1,900 +
/// `day_summaries` 数千 + `day_parts`) が数千往復になり、push 側が #231 で潰した
/// 「10,157 往復 → Cloudflare の 524 (100 秒)」と同型になる。
/// [`crate::kintai_push::KintaiPgStore::replace_window`] と同じく `unnest` で
/// **DELETE 1 文 + 表ごとに INSERT 数文**に畳む。
///
/// 刻み幅は push と同じ [`crate::kintai_push::INSERT_CHUNK`] (束は共有 crate の `*_columns` が刻む)。刻んでも同じ
/// トランザクションの中に居るので、全か無かは変わらない。
pub async fn write_unit(
    store: &KintaiPgStore,
    month: &str,
    unit: &FoldUnit,
    fingerprint: &str,
    params: &KosokuParams,
) -> Result<(), KintaiPushError> {
    write_unit_versioned(store, month, unit, fingerprint, &logic_version(params)).await
}

/// [`write_unit`] の本体 — 版 (`logic_version`) を呼び出し側から受け取る (共有 crate の流れが渡す)。
async fn write_unit_versioned(
    store: &KintaiPgStore,
    month: &str,
    unit: &FoldUnit,
    fingerprint: &str,
    version: &str,
) -> Result<(), KintaiPushError> {
    let (m0, m1) = month_date_bounds(month)
        .ok_or_else(|| KintaiPushError::NotConfigured(format!("bad month: {month}")))?;
    let tenant = store.tenant_id();
    let mut tx = store.pool().begin().await?;
    sqlx::query("SELECT set_config('app.current_tenant_id', $1, true)")
        .bind(tenant.to_string())
        .execute(&mut *tx)
        .await?;
    sqlx::query(DELETE_SHIFTS_SQL)
        .bind(tenant)
        .bind(unit.driver_cd)
        .bind(m0)
        .bind(m1)
        .execute(&mut *tx)
        .await?;

    for cols in shift_columns(unit) {
        sqlx::query(INSERT_SHIFTS_SQL)
            .bind(tenant)
            .bind(&cols.driver_cd)
            .bind(&cols.start_at)
            .bind(&cols.end_at)
            .bind(&cols.shift_source)
            .bind(fingerprint)
            .bind(version)
            .execute(&mut *tx)
            .await?;
    }

    for cols in day_summary_columns(unit) {
        let mut q = sqlx::query(INSERT_DAY_SUMMARIES_SQL)
            .bind(tenant)
            .bind(cols.driver_cd)
            .bind(cols.date)
            .bind(cols.shift_start_at)
            .bind(cols.shift_source);
        // 分の 11 列 ($6〜$16) は束の並びが SQL の並び
        for minutes in cols.minutes {
            q = q.bind(minutes);
        }
        q.bind(cols.non_working)
            .bind(fingerprint)
            .bind(version)
            .execute(&mut *tx)
            .await?;
    }

    for cols in day_part_columns(unit) {
        sqlx::query(INSERT_DAY_PARTS_SQL)
            .bind(tenant)
            .bind(&cols.driver_cd)
            .bind(&cols.shift_start_at)
            .bind(&cols.date)
            .bind(&cols.restraint_minutes)
            .bind(&cols.working_minutes)
            .bind(&cols.night_minutes)
            .execute(&mut *tx)
            .await?;
    }

    tx.commit().await?;
    Ok(())
}

// ── 流れ (共有 crate、Refs #322) ─────────────────────────────────────────
//
// 月ゲート・運行の突合・畳み・保存済みとの突き合わせは共有 crate (`kintai_kosoku::fold_flow`) にあり、
// 勤怠 Worker と同じものを使う (写さない)。ここは root の I/O (alc の HTTP / MariaDB の読み = `dyn KintaiEventsApi`、
// Supabase の読み書き = [`KintaiPgStore`] の sqlx) と `build.rs` の版・時計を渡すラッパーだけを持つ。
//
// warnings・`unsplit`・運行の突合は呼び出し側が [`FoldSink`] / [`Warnings`] を作って `&mut` で渡す
// (tokio の task_local をやめた)。上限・重複排除・封の判定の規則は共有 crate の docs。
//
// ### 月初をまたぐ運行・勤務 (Refs ohishi-exp/nuxt-dtako-admin#1123)
//
// 月初 0:00 から読むと、前月に始業した勤務の続き (月初の休息・運行終了・終業) だけが
// 見え、休息の終わりを始業とする余分な勤務を当月に立てる (乗務員 1194 の 2026-04)。
// 窓を「月初をまたぐ運行・勤務の開始」まで乗務員ごとに遡らせて直す。
//
// - 読みは全員で 1 回: 始端は起点の最小 ([`kintai_kosoku::window::lookback_from`])。乗務員ごとの窓へは
//   読んだ後に [`clip_to_anchors`] で切り戻す — 指紋は行まるごとなので、切り戻さ
//   ないと他人の起点で自分の行 (と指紋) が動き、毎回 stale になる
// - 当月への絞り込み (`kosoku::daily_summary` の始業日) と DELETE の範囲は変えない。
//   前月始業の勤務は今までどおり当月に出ない

pub use kintai_kosoku::fold_flow::{
    push_window_gap_warning, FoldGateRow, FoldSink, Warning, Warnings, MAX_COLLECTED_WARNINGS,
};

use kintai_kosoku::fold_flow::{self as flow, FoldClock, FoldCtx, FoldStoreIo};

/// root の時計 (JST の今と、経過時間を測る単調時計)。
pub struct SystemClock;

impl FoldClock for SystemClock {
    fn now_jst(&self) -> DateTime<FixedOffset> {
        now_jst()
    }

    fn monotonic_ms(&self) -> u64 {
        static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
        START
            .get_or_init(std::time::Instant::now)
            .elapsed()
            .as_millis() as u64
    }
}

/// 流れに渡す設定 (記録用の [`code_sha`]、時計は [`SystemClock`])。
fn ctx(params: &KosokuParams) -> FoldCtx<'_> {
    FoldCtx {
        params,
        code_sha: code_sha(),
        clock: &SystemClock,
    }
}

/// Supabase の勤怠スキーマ (`kintai.*`) の読み書き (sqlx)。SQL は共有 crate の定数。
impl FoldStoreIo for KintaiPgStore {
    type Error = KintaiPushError;

    async fn stored_states(
        &self,
        drivers: &[i64],
        month: &str,
    ) -> Result<std::collections::BTreeMap<i64, StoredState>, KintaiPushError> {
        stored_states(self, drivers, month).await
    }

    async fn write_unit(
        &self,
        month: &str,
        unit: &FoldUnit,
        fingerprint: &str,
        logic_version: &str,
    ) -> Result<(), KintaiPushError> {
        write_unit_versioned(self, month, unit, fingerprint, logic_version).await
    }

    async fn read_fold_gate(&self, month: &str) -> Result<Option<FoldGateRow>, KintaiPushError> {
        use sqlx::Row;
        let row = sqlx::query(FOLD_GATE_SELECT_SQL)
            .bind(self.tenant_id())
            .bind(month)
            .fetch_optional(self.pool())
            .await?;
        Ok(row.map(|r| FoldGateRow {
            dtako_digest: r.get::<String, _>("dtako_digest"),
            punch_digest: r.get::<String, _>("punch_digest"),
            logic_version: r.get::<String, _>("logic_version"),
        }))
    }

    async fn write_fold_gate(
        &self,
        month: &str,
        gate: &FoldGateRow,
    ) -> Result<(), KintaiPushError> {
        sqlx::query(FOLD_GATE_UPSERT_SQL)
            .bind(self.tenant_id())
            .bind(month)
            .bind(&gate.dtako_digest)
            .bind(&gate.punch_digest)
            .bind(&gate.logic_version)
            .execute(self.pool())
            .await?;
        Ok(())
    }

    async fn month_punch_digest(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<String, KintaiPushError> {
        self.stored_month_punch_digest(from, to).await
    }

    async fn month_operations(
        &self,
        from: DateTime<FixedOffset>,
        to: DateTime<FixedOffset>,
    ) -> Result<Vec<(i64, String, NaiveDate, NaiveDate)>, KintaiPushError> {
        self.stored_month_operations(from, to).await
    }

    async fn operation_driver_cds(
        &self,
    ) -> Result<std::collections::HashSet<i64>, KintaiPushError> {
        self.stored_operation_driver_cds().await
    }
}

/// 対象月の遡り起点を引く。月が壊れていれば読む前に落とす。
pub async fn month_anchors(
    repo: &DynKintaiEventsRepo,
    month: &str,
) -> Result<HeadAnchors, KintaiRepoError> {
    flow::month_anchors(&**repo, month).await
}

/// 読む窓 `[from, to)`。`driver` 指定ならその乗務員の起点 (無ければ月初)、
/// 省略なら全員の最小。窓の決め方は共有 crate (`kintai_kosoku::window::read_window`、
/// 勤怠 Worker と同じもの、Refs #322)。ここは月が読めないときの写しだけ。
pub fn read_window(
    month: &str,
    anchors: &HeadAnchors,
    driver: Option<u64>,
) -> Result<(String, String), KintaiRepoError> {
    kintai_kosoku::window::read_window(month, anchors, driver)
        .ok_or_else(|| KintaiRepoError::QueryFailed(format!("bad month: {month}")))
}

/// 対象月を**生イベント 1 回読み**で乗務員ごとに畳む。返すのは乗務員CD 昇順
/// (本体は共有 crate の `fold_flow::fold_month`)。
///
/// 期間は [`month_range`] = `[月初, 翌月 2 日)`。読み出し経路と同じで、日跨ぎ勤務の
/// 終業打刻を拾うために翌月へはみ出す (push の `exact_month_range` とは違う —
/// あちらは「その日の全部を見た上で署名する」必要があるため)。
///
/// **打刻 2 表だけの読み ([`crate::kintai_push::read_driver_events`]) を使わない。**
/// あれは #225 で push / diff 用に絞ったもので `dtako_events` が落ちている。
/// [`crate::kosoku::daily_summary`] は休息イベントで勤務を切るので、渡すと休息由来の勤務が
/// 丸ごと消える (2026-07-31 の回帰、#234)。全乗務員版は 3 表を `UNION ALL` した
/// ものなので、この穴が構造的に開かない。
///
/// 上流の warnings・push 窓ずれ・読み窓の遡りは `warnings` に積む。`today` は push 窓ずれの基準日で、
/// **本番の呼び出しは全て `None`** (実時計)。固定日を渡せるのはテストだけ (Refs #286-1)。
pub async fn fold_month(
    repo: &DynKintaiEventsRepo,
    params: &KosokuParams,
    month: &str,
    driver: Option<u64>,
    today: Option<NaiveDate>,
    warnings: &mut Warnings,
) -> Result<Vec<(u64, FoldUnit, String)>, KintaiPushError> {
    flow::fold_month(&**repo, &ctx(params), month, driver, today, warnings).await
}

/// [`fold_month`] の本体 — 遡り起点を呼び出し側から受け取る (Refs
/// ohishi-exp/nuxt-dtako-admin#1123)。[`recalc_month`] が月ゲートと畳みに**同じ
/// 起点**を渡すための口。窓は [`read_window`]、切り戻しは [`clip_to_anchors`]。
pub async fn fold_month_with_anchors(
    repo: &DynKintaiEventsRepo,
    params: &KosokuParams,
    month: &str,
    driver: Option<u64>,
    today: Option<NaiveDate>,
    anchors: &HeadAnchors,
    warnings: &mut Warnings,
) -> Result<Vec<(u64, FoldUnit, String)>, KintaiPushError> {
    let ctx = ctx(params);
    flow::fold_month_with_anchors(&**repo, &ctx, month, driver, today, anchors, warnings).await
}

/// gate の書き込み。**書けなくても呼び出し元の口を落とさない** (Refs #205 の 20) —
/// 書けなければ gate が立たないだけで、次回また全量読みに落ちる安全側。
pub(crate) async fn write_fold_gate_best_effort(
    store: &KintaiPgStore,
    month: &str,
    dtako_digest: &str,
    punch_digest: &str,
    logic_version: &str,
) {
    let gate = FoldGateRow {
        dtako_digest: dtako_digest.to_string(),
        punch_digest: punch_digest.to_string(),
        logic_version: logic_version.to_string(),
    };
    flow::write_fold_gate_best_effort(store, month, &gate).await
}

/// 月ゲートを判定する (実装計画 13)。**読むだけで書かない** — 書くかどうかは
/// 呼び出し側が「月まるごとを 1 呼び出しで完結させたか」を見て決める
/// ([`recalc_month`] / [`crate::routes::kintai_recalc::run`] の docs 参照)。
///
/// **`kintai.fold_gate` が読めなくても `Err` にしない** ([`MonthGate::Unavailable`]
/// に倒す、Refs #205 の 20)。指紋を取る読みの warnings・`unsplit`・運行の突合は `sink` に積む。
pub async fn month_gate_report(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    apply: bool,
    sink: &mut FoldSink,
) -> Result<MonthGate, KintaiPushError> {
    flow::month_gate_report(&**repo, store, &ctx(params), month, apply, sink).await
}

/// [`month_gate_report`] の本体 — 遡り起点を呼び出し側から受け取る
/// ([`fold_month_with_anchors`] と同じ値を渡すため)。
pub async fn month_gate_report_with_anchors(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    apply: bool,
    anchors: &HeadAnchors,
    sink: &mut FoldSink,
) -> Result<MonthGate, KintaiPushError> {
    let ctx = ctx(params);
    flow::month_gate_report_with_anchors(&**repo, store, &ctx, month, apply, anchors, sink).await
}

/// 対象月を再計算して保存する (実装計画 05、本体は共有 crate の `fold_flow::recalc_month`)。
///
/// `driver` を省略した呼び出し (CLI の `Recalc` / `Sync`) は月ゲートを通り、**`sink.warnings` に
/// 封を止める warning が無いときだけ** gate を書く (Refs #205-17 / #205-51)。束ぜんたいを見るので、
/// 呼び出し側が渡す前に積んだ warning も数える。**診断専用 (tail gap) は数えない。**
///
/// `today` は [`fold_month`] にそのまま渡す。**呼び出し元は `None` を渡すこと** (Refs #286-1)。
#[allow(clippy::too_many_arguments)]
pub async fn recalc_month(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    driver: Option<u64>,
    apply: bool,
    today: Option<NaiveDate>,
    sink: &mut FoldSink,
) -> Result<FoldReport, KintaiPushError> {
    let ctx = ctx(params);
    flow::recalc_month(&**repo, store, &ctx, month, driver, apply, today, sink).await
}

/// **名指しした乗務員だけ**を再計算する (Refs #205 の 06 の HTTP 版、本体は共有 crate)。
///
/// 窓の受け口が apply 後に呼ぶのはこちら。**`drivers` が空なら 1 往復もしない。**
/// 月ゲートは**読むだけで書かない**。読み方は [`fold_month`] と同じ全乗務員版 1 回読み。
///
/// ## `apply = false` は「保存済みの入力で畳んだら」の報告
///
/// 生イベントは**読み先から読み直す**ので、dry-run の窓と束ねたときの報告は
/// 「いま届いた打刻を反映したら」ではなく「**すでに保存されている打刻で畳んだら**」に
/// なる (dry-run の窓は `kintai_events` を書かないため)。
pub async fn recalc_drivers(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    drivers: &[u64],
    apply: bool,
    sink: &mut FoldSink,
) -> Result<FoldReport, KintaiPushError> {
    let ctx = ctx(params);
    flow::recalc_drivers(&**repo, store, &ctx, month, drivers, apply, sink).await
}

/// [`recalc_drivers`] の続き — **生イベントの読みを呼び出し側から受け取る**。
///
/// 呼び出し元が [`fold_month`] を既に済ませている場合 (`kintai_recalc` が母集団の
/// 決定 (#205-12) と畳みで同じ 1 回読みを使い回すとき) はこちらを直接呼ぶ。
pub async fn recalc_drivers_from_units(
    store: &KintaiPgStore,
    params: &KosokuParams,
    month: &str,
    drivers: &[u64],
    all: Vec<(u64, FoldUnit, String)>,
    apply: bool,
) -> Result<FoldReport, KintaiPushError> {
    flow::recalc_drivers_from_units(store, &ctx(params), month, drivers, all, apply).await
}

/// 今の時刻 (JST)。
fn now_jst() -> DateTime<FixedOffset> {
    use chrono::TimeZone;
    let jst = FixedOffset::east_opt(crate::kintai_push::JST_OFFSET_SECONDS)
        .expect("JST offset is in range");
    jst.from_utc_datetime(&chrono::Utc::now().naive_utc())
}

// ── 06: push と再計算を束ねる ──────────────────────────────────────────────

/// [`sync_month`] の集計。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncReport {
    pub push: crate::kintai_push::PushReport,
    pub fold: FoldReport,
}

impl SyncReport {
    /// 想定外があったか (呼び出し側が非 0 終了するのに使う)。
    pub fn has_unexpected(&self) -> bool {
        self.push.has_unexpected() || self.fold.has_unexpected()
    }
}

/// 打刻の push と再計算を 1 回で回す (実装計画 06)。
///
/// **push が 1 日でも書いたら再計算を必ず走らせる。** 「push しただけで計算して
/// いない」状態を作らないため — 読み出しは計算しないので、畳んだ値が古いままだと
/// 遅いのではなく**静かに間違う** (#205 のリスク欄の筆頭)。
///
/// 逆に差分が無ければ再計算も指紋で弾かれるので、実質何もせずに終わる。
/// 再計算を呼ぶこと自体は常に行う — push が差分ゼロでも、`kosoku.rs` や TOML の
/// 変更で指紋が変わっている可能性があるため。
///
/// warnings は再計算の分だけを `sink` に積む (push の読みの warnings は積まない。push の窓
/// `[月初, 翌月初)` は畳みの読みの窓 `[月初 (遡りあり), 翌月 2 日)` に含まれるので、封の判定は
/// 畳みの読みの warnings で足りる — Refs #322)。
pub async fn sync_month(
    repo: &DynKintaiEventsRepo,
    store: &KintaiPgStore,
    params: &KosokuParams,
    opts: &crate::kintai_push::PushOptions,
    sink: &mut FoldSink,
) -> Result<SyncReport, KintaiPushError> {
    let push = crate::kintai_push::push_month(repo, store, opts).await?;
    // 実時計 (Refs #286-1) — CLI の Sync は常に本番の「今日」で判定する
    let fold = recalc_month(
        repo,
        store,
        params,
        &opts.month,
        opts.driver,
        opts.apply,
        None,
        sink,
    )
    .await?;
    Ok(SyncReport { push, fold })
}

/// `day_parts` の暦日境界。テストと診断で使う。
pub fn day_bounds(date: NaiveDate) -> (DateTime<FixedOffset>, DateTime<FixedOffset>) {
    jst_day_bounds(date)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kosoku::{DayPart, DaySummary, ShiftSource};

    fn day(date: &str, start: &str, end: &str) -> DaySummary {
        DaySummary {
            date: date.to_string(),
            start: start.to_string(),
            end: end.to_string(),
            source: ShiftSource::Timecard,
            punches: Vec::new(),
            parts: Vec::new(),
            is_legal_holiday: false,
            over_24h: false,
            restraint_minutes: 600,
            break_minutes: 60,
            working_minutes: 540,
            rest_minus_minutes: 0,
            statutory_minutes: 450,
            within_statutory_overtime_minutes: 30,
            overtime_minutes: 60,
            legal_holiday_minutes: 0,
            night_minutes: 0,
            overtime_night_minutes: 0,
            legal_holiday_night_minutes: 0,
            ferry_minus_minutes: 0,
            run_gap_minutes: 0,
            punch_tail_minutes: 0,
            punch_head_minutes: 0,
            run_head_minutes: 0,
            lunch_overlap_minutes: 0,
            non_working: Vec::new(),
        }
    }

    fn part(date: &str, restraint: i64, working: i64, night: i64) -> DayPart {
        let mut p = DayPart {
            date: date.to_string(),
            restraint_minutes: restraint,
            working_minutes: working,
            overtime_minutes: 0,
            legal_holiday_minutes: 0,
            night_minutes: night,
            overtime_night_minutes: 0,
            legal_holiday_night_minutes: 0,
            ferry_minus_minutes: 0,
            run_gap_minutes: 0,
            punch_tail_minutes: 0,
            punch_head_minutes: 0,
            run_head_minutes: 0,
            lunch_overlap_minutes: 0,
        };
        p.run_head_minutes = 0;
        p
    }

    fn rows() -> Vec<serde_json::Value> {
        vec![
            serde_json::json!({"datetime": "2026-07-01 08:00:00", "source": "timecard", "state": "始業"}),
            serde_json::json!({"datetime": "2026-07-01 18:00:00", "source": "timecard", "state": "終業"}),
        ]
    }

    #[test]
    fn the_code_sha_is_the_build_hash() {
        // build.rs が焼く 16 桁 hex (記録用。版には入らない)
        assert_eq!(code_sha().len(), 16);
        assert!(code_sha().chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// 月が飛んでいても端から端まで 1 本の期間にする。
    #[test]
    fn months_span_covers_the_ends() {
        let span = months_span(&["2026-07".to_string(), "2026-05".to_string()]).unwrap();
        assert_eq!(
            span,
            (
                NaiveDate::from_ymd_opt(2026, 5, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()
            )
        );
        let one = months_span(&["2026-12".to_string()]).unwrap();
        assert_eq!(one.1, NaiveDate::from_ymd_opt(2027, 1, 1).unwrap());
        assert!(months_span(&[]).is_err(), "空は書き先が決まらない");
        assert!(months_span(&["nope".to_string()]).is_err());
    }

    /// 版は `LOGIC_BASE` と設定だけで決まり、`build.rs` のソースのハッシュに依らない (Refs #322)。
    /// 値そのもの (本番の `1b8f00ba48e9a77b`) は共有 crate の `the_logic_version_is_the_one_in_production` が縛る。
    #[test]
    fn the_logic_version_does_not_depend_on_the_build_hash() {
        use kintai_kosoku::kintai_fold as shared;
        let p = KosokuParams::default();
        assert_eq!(logic_version(&p).len(), shared::LOGIC_VERSION_LEN);
        assert_ne!(
            logic_version(&p),
            code_sha(),
            "ソースのハッシュそのものではない"
        );
        assert_eq!(fingerprint(1, "2026-07", &p, &rows()).len(), 64);
        // 流れに渡すのは記録用の code_sha
        assert_eq!(ctx(&p).code_sha, code_sha());
    }

    #[test]
    fn now_jst_is_a_jst_timestamp() {
        let s = now_jst().to_rfc3339();
        assert!(s.ends_with("+09:00"), "{s}");
        assert!(chrono::DateTime::parse_from_rfc3339(&s).is_ok(), "{s}");
    }
    #[test]
    fn month_date_bounds_wraps_the_year() {
        assert_eq!(
            month_date_bounds("2026-12"),
            Some((
                NaiveDate::from_ymd_opt(2026, 12, 1).unwrap(),
                NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()
            ))
        );
        assert_eq!(
            month_date_bounds("2026-07"),
            Some((
                NaiveDate::from_ymd_opt(2026, 7, 1).unwrap(),
                NaiveDate::from_ymd_opt(2026, 8, 1).unwrap()
            ))
        );
        assert_eq!(month_date_bounds("nope"), None);
        assert_eq!(month_date_bounds("2026-13"), None);
    }

    /// 既知の skip は**報告に残るが非 0 終了にしない**。
    ///
    /// 2026-06 の乗務員 1518 (06-17 19:09、start = end) のような零長勤務は毎月
    /// 実在する。数えると CLI が毎回 exit 3 になり、本当の想定外が埋もれる。
    #[test]
    fn known_skips_are_reported_but_not_unexpected() {
        let degenerate = day("2026-06-17", "2026-06-17 19:09:00", "2026-06-17 19:09:00");
        let mut early_part = day("2026-07-02", "2026-07-02 00:30:00", "2026-07-02 18:00:00");
        early_part.parts = vec![part("2026-07-01", 5, 0, 0)];
        let unit = fold_days(1518, &[degenerate, early_part]);

        let report = FoldReport {
            skipped: unit.skipped.clone(),
            ..Default::default()
        };
        assert_eq!(report.skipped.len(), 2, "落としたことは報告に残す");
        assert!(matches!(
            report.skipped[0],
            SkipReason::DegenerateShift { .. }
        ));
        assert!(matches!(
            report.skipped[1],
            SkipReason::PartBeforeShift { .. }
        ));
        assert!(!report.has_unexpected(), "既知の形は想定外に数えない");

        let sync = SyncReport {
            push: crate::kintai_push::PushReport::default(),
            fold: report,
        };
        assert!(!sync.has_unexpected(), "sync も同じ");
    }

    /// 上流由来の想定外は引き続き数える (`sync` は push の判定をそのまま含む)。
    #[test]
    fn sync_still_counts_rejected_rows_and_unknown_states() {
        let skipped = fold_days(
            1,
            &[day(
                "2026-06-17",
                "2026-06-17 19:09:00",
                "2026-06-17 19:09:00",
            )],
        )
        .skipped;
        let fold = FoldReport {
            skipped,
            ..Default::default()
        };
        assert!(!fold.has_unexpected());

        let mut push = crate::kintai_push::PushReport::default();
        push.rejected
            .insert(crate::kintai_push::RejectReason::NoDriver, 1);
        let with_rejected = SyncReport {
            push: push.clone(),
            fold: fold.clone(),
        };
        assert!(with_rejected.has_unexpected(), "読み飛ばした行は想定外");

        let mut push = crate::kintai_push::PushReport::default();
        push.unknown_states.insert("知らない".to_string());
        let with_unknown_state = SyncReport { push, fold };
        assert!(
            with_unknown_state.has_unexpected(),
            "CHECK に無い state は想定外"
        );
    }

    #[test]
    fn day_bounds_is_jst_midnight() {
        let (a, b) = day_bounds(NaiveDate::from_ymd_opt(2026, 7, 1).unwrap());
        assert_eq!(a.to_rfc3339(), "2026-07-01T00:00:00+09:00");
        assert_eq!(b.to_rfc3339(), "2026-07-02T00:00:00+09:00");
    }

    /// root の時計: JST の今と、減らない単調時計 (push 窓ずれ・経過時間の材料。本体の判定は共有 crate)。
    #[test]
    fn the_system_clock_reads_jst_and_never_goes_back() {
        use kintai_kosoku::fold_flow::FoldClock;
        let now = SystemClock.now_jst();
        assert_eq!(now.offset().local_minus_utc(), 9 * 3600);
        assert!(now.date_naive() > NaiveDate::from_ymd_opt(2020, 1, 1).unwrap());
        let a = SystemClock.monotonic_ms();
        assert!(SystemClock.monotonic_ms() >= a);
    }
}
