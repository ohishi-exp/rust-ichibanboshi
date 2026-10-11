//! `GET /api/kintai/unko-gaps?month=YYYY-MM[&driver_cd=<i64>]` — 取り込み漏れ候補 (`also_in_month`) の GCP にしか無い
//! 運行の運行NO の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! root の `src/routes/unko_gaps.rs` (オンプレ版・Cloud Run 版) から移した。**写しではなく共有** — root も path 依存で
//! これを使い、axum・sqlx・alc への reqwest (sink 経由) と `elapsed_ms` だけを持つ。勤怠 Worker は Hyperdrive の
//! `query_typed` と auth-worker の RPC (`KintaiAlcEntrypoint.dtakoEtags`) で材料を引いてここへ渡す。口の意味 (「無い」と
//! 「引けていない」の区別・22 桁・上限) は root の module docs。
//!
//! ただし次の 2 つは **root の勤怠の版 (`KINTAI_OUTPUT_SHA`) の glob の中** (`src/kintai_push.rs`・`src/kintai_http_repo.rs`)
//! にあり root はそちらを使い続けるので、ここに置くのは**写し** (fold を移す段で解消する。対応表は README):
//!
//! - [`MONTH_OPERATIONS_SQL`]・[`PUSHED_SOURCES`] (root の `kintai_push`。一致は `pg/tests/unko_gaps_parity.rs`)
//! - alc の etags の path・期間・応答の読み方 ([`ETAGS_PATH`]・[`etags_search`]・[`read_etags`]。root の
//!   `kintai_http_repo` の `ETAGS_PATH`・`month_etags_bounds`・`UpstreamEtags`・`fetch_etags`。private なので
//!   一致は同じテストが root のソースの文字列で固定する)

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDateTime};
use postgres_types::Type;
use serde::Deserialize;
use uuid::Uuid;

use crate::common::{bad_request, is_valid_month, month_bounds, parse_query, Fail, Param};
use kintai_kosoku::window::{month_range, unko_no_start_date};

/// 応答に載せる乗務員数の上限 (`UnkoDiffDriverSplit` の `MAX_UNKO_DIFF_DRIVERS` と同じ思想 — 桁違いの入力が
/// 来たときに応答を膨らませない蓋)。
pub const MAX_UNKO_GAPS_DRIVERS: usize = 300;

/// 乗務員 1 人あたり (および `unknown_driver_unko_nos`) の運行NO 件数の上限。
/// 実測 (2026-06) は候補 1 人あたり 1 件だが、同じ理由で蓋を置く。
pub const MAX_UNKO_GAPS_PER_DRIVER: usize = 200;

/// 502 の本文の頭 (root の `db_err` と同じ)。
pub const DB_WHAT: &str = "kintai.kintai_events unko read";

/// JST の UTC からのずれ (秒)。root の `kintai_push::JST_OFFSET_SECONDS` と同じ値。
const JST_OFFSET_SECONDS: i32 = 9 * 3600;

#[derive(Debug, Default, Deserialize)]
pub struct UnkoGapsQuery {
    pub month: Option<String>,
    pub driver_cd: Option<i64>,
}

/// 検査済みの入力。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub month: String,
    pub driver_cd: Option<i64>,
}

/// クエリ文字列を読んで検査する (Worker 用。axum の `Query` と同じ拒否文言)。
pub fn parse(query: &str) -> Result<Request, Fail> {
    let q: UnkoGapsQuery = parse_query(query)?;
    let month = check_month(q.month)?;
    Ok(Request {
        month,
        driver_cd: q.driver_cd,
    })
}

/// `month` の検査 (root の handler と同じ順・同じ文言)。無い → 必須、形が違う → YYYY-MM。
pub fn check_month(month: Option<String>) -> Result<String, Fail> {
    let month = month.ok_or_else(|| bad_request("month は必須です (YYYY-MM)"))?;
    if !is_valid_month(&month) {
        return Err(bad_request("month は YYYY-MM で指定してください"));
    }
    Ok(month)
}

/// 対象月の年・月とオンプレ側を読む窓。窓は `kintai_kosoku::window::month_range` (`[月初, 翌月 2 日)`、JST 壁時計) を
/// JST の時刻にしたもの (kintai_fold の measure_unko_diff と同じ窓)。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    pub year: i32,
    pub month_num: u32,
    pub from: DateTime<FixedOffset>,
    pub to: DateTime<FixedOffset>,
}

/// 400 (`check_month` が通したのに年・月・窓が作れない月。実際には起きない。root の元の文言と同じ)。
pub fn broken_month(month: &str) -> Fail {
    bad_request(format!("month が壊れています: {month}"))
}

impl Window {
    /// `check_month` が通した月では常に `Ok`。作れなければ [`broken_month`] の 400。
    pub fn of(month: &str) -> Result<Self, Fail> {
        Self::read(month).ok_or_else(|| broken_month(month))
    }

    fn read(month: &str) -> Option<Self> {
        let year: i32 = month.get(..4)?.parse().ok()?;
        let month_num: u32 = month.get(5..7)?.parse().ok()?;
        let (from, to) = month_range(month)?;
        Some(Self {
            year,
            month_num,
            from: parse_jst(&from)?,
            to: parse_jst(&to)?,
        })
    }
}

/// `month_range` が返す `"YYYY-MM-DD HH:MM:SS"` (JST 壁時計) を `DateTime<FixedOffset>` に。
fn parse_jst(s: &str) -> Option<DateTime<FixedOffset>> {
    let naive = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok()?;
    let off = FixedOffset::east_opt(JST_OFFSET_SECONDS)?;
    naive.and_local_timezone(off).single()
}

// ── オンプレ側 (押し込み済み `kintai.kintai_events`) ─────────────────────────

/// **写し** — root の `src/kintai_push.rs` の `MONTH_OPERATIONS_SQL` (glob の中)。一致は `pg/tests/unko_gaps_parity.rs`。
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

/// **写し** — root の `kintai_kosoku::kintai_push::PUSHED_SOURCES` を root の `kintai_push` が再 export したもの。
pub const PUSHED_SOURCES: [&str; 2] = ["timecard", "dtako"];

/// [`MONTH_OPERATIONS_SQL`] の引数。`$1` = テナント (UUID の pin)、`$2`/`$3` = [`Window`] (TIMESTAMPTZ)、
/// `$4` = [`PUSHED_SOURCES`] (TEXT[])。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binds {
    pub tenant: Uuid,
    pub from: DateTime<FixedOffset>,
    pub to: DateTime<FixedOffset>,
    pub sources: Vec<&'static str>,
}

impl Binds {
    pub fn new(tenant: Uuid, window: &Window) -> Self {
        Self {
            tenant,
            from: window.from,
            to: window.to,
            sources: PUSHED_SOURCES.to_vec(),
        }
    }

    pub fn params(&self) -> Vec<Param<'_>> {
        vec![
            (&self.tenant, Type::UUID),
            (&self.from, Type::TIMESTAMPTZ),
            (&self.to, Type::TIMESTAMPTZ),
            (&self.sources, Type::TEXT_ARRAY),
        ]
    }
}

/// オンプレ側の材料。`seen` は窓に在る `unko_no` (対象CD 落とし済み)、`in_month` は乗務員別の件数。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Onprem {
    pub seen: HashSet<String>,
    pub in_month: HashMap<i64, usize>,
}

impl Onprem {
    /// [`MONTH_OPERATIONS_SQL`] の行 (`driver_cd`・`unko_no`) から作る。
    pub fn from_rows<'a>(rows: impl IntoIterator<Item = (i64, &'a str)>) -> Self {
        let mut out = Self::default();
        for (driver_cd, unko_no) in rows {
            *out.in_month.entry(driver_cd).or_default() += 1;
            out.seen.insert(drop_crew_suffix(unko_no));
        }
        out
    }
}

/// オンプレの `unko_no` から対象CD (末尾 1 文字) を落として運行NO (GCP と同じ 22 桁) にする。
/// `kintai_http_repo::onprem_unko_no` と同じ規則 (`ONPREM_CREW_SUFFIX_LEN` = 1) だが、あちらは private + glob 内なので
/// 呼べない — ここで同じ 1 行を再現する。規則そのものは固定値でドリフトの心配は無い。
pub fn drop_crew_suffix(unko_no: &str) -> String {
    let kept = unko_no.chars().count().saturating_sub(1);
    if kept == 0 {
        return unko_no.to_string();
    }
    let cut: usize = unko_no.chars().take(kept).map(char::len_utf8).sum();
    unko_no[..cut].to_string()
}

// ── GCP 側 (alc の etags。auth-worker の RPC 越し) ────────────────────────────

/// **写し** — root の `kintai_http_repo::ETAGS_PATH`。勤怠 Worker からは渡さない (auth-worker の `KintaiAlcEntrypoint` が
/// path・method (GET)・tenant を固定して転送する)。ここに置くのは root と同じ path を叩いていることを固定するためだけ。
pub const ETAGS_PATH: &str = "/api/dtako/events/etags";

/// auth-worker の `KintaiAlcEntrypoint` (勤怠 Worker 専用の entrypoint) への Service Binding (wrangler.toml のトップレベルにだけ置く)。
pub const ALC_RPC_BINDING: &str = "KINTAI_ALC_RPC";

/// `dtakoEtags(search)` に渡す query 文字列 `date_from=<月初>&date_to=<翌月初>`。期間は root の `month_etags_bounds` と同じ
/// `[月初, 翌月初]` (閉区間の暦日)。**RPC の引数はこれだけ** (path・method・tenant は auth-worker 側で固定)。`None` は月が読めないとき。
pub fn etags_search(month: &str) -> Option<String> {
    let (first, next_first) = month_bounds(month)?;
    Some(format!("date_from={first}&date_to={next_first}"))
}

/// RPC の戻り (`AlcRpcResult`)。`contentType` は読まない。
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct RpcResult {
    pub status: u16,
    pub body: String,
}

/// **写し** — root の `kintai_http_repo::UpstreamEtagItem`。
#[derive(Debug, Clone, Deserialize)]
struct UpstreamEtagItem {
    unko_no: String,
    #[serde(default)]
    #[allow(dead_code)]
    etag: Option<String>,
    #[serde(default)]
    driver_cds: Vec<String>,
}

/// **写し** — root の `kintai_http_repo::UnsplitOperation` (応答の型を root と同じにするためだけに持つ。値は使わない)。
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct UnsplitOperation {
    unko_no: String,
    driver_cd: String,
    reading_date: String,
}

/// **写し** — root の `kintai_http_repo::UpstreamEtags`。
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
struct UpstreamEtags {
    #[serde(default)]
    items: Vec<UpstreamEtagItem>,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    unsplit: Vec<UnsplitOperation>,
    #[serde(default)]
    unsplit_total: usize,
}

/// etags の `unko_no` → `driver_cds`。同じ `unko_no` が 2 回来たら後勝ち (root の `collect` と同じ)。
pub type GcpDriverCds = HashMap<String, Vec<String>>;

/// RPC の戻りを読む。**404 だけが「口なし」** (`Ok(None)` = `gcp_etags_available: false`、root の `fetch_etags` と同じ)。
///
/// root は 404 以外の失敗も warn だけで `gcp_etags_available: false` に畳むが、勤怠 Worker は**畳まずに 502 で名指しする**
/// (黙って判定不能にしない。親の判断 Refs #322)。auth-worker 自身の拒否 (tenant 未設定の 503 `kintai_alc_tenant_unset`・
/// query 不正の 400) も同じく 502 で、本文の先頭 200 字に auth-worker の error の語が載る。
pub fn read_etags(res: &RpcResult) -> Result<Option<GcpDriverCds>, Fail> {
    if res.status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&res.status) {
        let (status, excerpt): (u16, String) = (res.status, res.body.chars().take(200).collect());
        let msg = format!("alc dtako-etags status {status}: {excerpt}");
        return Err(Fail::new(502, msg));
    }
    let parsed: UpstreamEtags = serde_json::from_str(&res.body)
        .map_err(|e| Fail::new(502, format!("alc dtako-etags parse: {e}")))?;
    let map = parsed
        .items
        .into_iter()
        .map(|it| (it.unko_no, it.driver_cds))
        .collect();
    Ok(Some(map))
}

/// 503 (`KINTAI_ALC_RPC` の binding が無い)。
pub fn no_alc_rpc() -> Fail {
    Fail::new(
        503,
        "auth-worker の binding (KINTAI_ALC_RPC) が無い (alc の etags を読めません)",
    )
}

/// 502 (RPC そのものが落ちた。auth-worker の中の fetch の throw を含む)。文言は種別だけ。
pub fn alc_rpc_failed() -> Fail {
    Fail::new(502, "alc dtako-etags request: rpc")
}

// ── 判定・整形の核 ───────────────────────────────────────────────────────────

/// 1 乗務員ぶんの応答行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverGaps {
    pub driver_cd: String,
    pub unko_nos: Vec<String>,
    pub truncated: bool,
}

/// 乗務員CD 指定時の、その乗務員の対象月のオンプレ側の運行件数 (無ければ 0)。
/// 指定なしは `None`。指定時は `also_in_month` の絞り込みが外れる ([`build_gaps`]) ので、
/// 呼び出し側が「0 件 = 照らし合わせる相手が無い」を見分けるための材料として返す。
pub fn onprem_count_for(
    onprem_in_month: &HashMap<i64, usize>,
    driver_cd: Option<i64>,
) -> Option<usize> {
    driver_cd.map(|cd| onprem_in_month.get(&cd).copied().unwrap_or(0))
}

fn cap_sorted(mut v: Vec<String>, max: usize) -> (Vec<String>, bool) {
    v.sort_unstable();
    let truncated = v.len() > max;
    v.truncate(max);
    (v, truncated)
}

/// I/O から切り離した判定・整形の核。**DB も alc も見ない** — 呼び出し側が
/// 引いてきた材料だけを受け取り、gap の抽出・乗務員別への分割・
/// `also_in_month` の絞り込み・上限の適用をやる。
///
/// `onprem_seen` は対象月の窓に在るオンプレの `unko_no` (対象CD 落とし済み、
/// [`drop_crew_suffix`]) の集合、`onprem_in_month` は乗務員別の件数
/// (どちらも [`MONTH_OPERATIONS_SQL`] の行から作る)。`gcp_driver_cds` は
/// etags の `unko_no` → `driver_cds` の生の値 (窓ぜんたい — 対象月に絞る前)。
pub fn build_gaps(
    year: i32,
    month_num: u32,
    onprem_seen: &HashSet<String>,
    onprem_in_month: &HashMap<i64, usize>,
    gcp_driver_cds: &HashMap<String, Vec<String>>,
    driver_cd_filter: Option<i64>,
) -> (Vec<DriverGaps>, bool, Vec<String>, bool) {
    let mut by_driver: HashMap<String, Vec<String>> = HashMap::new();
    let mut unknown_driver: Vec<String> = Vec::new();
    for (unko_no, driver_cds) in gcp_driver_cds {
        if onprem_seen.contains(unko_no.as_str()) {
            continue; // 一致済み — 漏れではない
        }
        let Some(start) = unko_no_start_date(unko_no) else {
            continue; // 開始日が読めない = 対象月かどうか判定できない (安全側で外す)
        };
        if start.year() != year || start.month() != month_num {
            continue; // 対象月に始まった運行だけ (窓の外の運行を混ぜない)
        }
        if driver_cds.is_empty() {
            unknown_driver.push(unko_no.clone());
        } else {
            for dcd in driver_cds {
                by_driver
                    .entry(dcd.clone())
                    .or_default()
                    .push(unko_no.clone());
            }
        }
    }

    let is_candidate = |cd: &str| -> bool {
        match driver_cd_filter {
            // 乗務員CD 指定時はその 1 人だけ (バケットは問わない — 呼び出し側は
            // 既に候補と分かっている前提)
            Some(want) => cd.trim().parse::<i64>() == Ok(want),
            // 省略時は also_in_month (= 対象月にオンプレの運行も在る) の候補全員
            None => cd
                .trim()
                .parse::<i64>()
                .ok()
                .and_then(|n| onprem_in_month.get(&n))
                .is_some_and(|&n| n > 0),
        }
    };

    let mut driver_rows: Vec<(String, Vec<String>)> = by_driver
        .into_iter()
        .filter(|(cd, _)| is_candidate(cd))
        .collect();
    driver_rows.sort_by(|a, b| b.1.len().cmp(&a.1.len()).then_with(|| a.0.cmp(&b.0)));
    let drivers_truncated = driver_rows.len() > MAX_UNKO_GAPS_DRIVERS;
    driver_rows.truncate(MAX_UNKO_GAPS_DRIVERS);

    let drivers: Vec<DriverGaps> = driver_rows
        .into_iter()
        .map(|(driver_cd, unko_nos)| {
            let (unko_nos, truncated) = cap_sorted(unko_nos, MAX_UNKO_GAPS_PER_DRIVER);
            DriverGaps {
                driver_cd,
                unko_nos,
                truncated,
            }
        })
        .collect();

    let (unknown_driver, unknown_driver_truncated) =
        cap_sorted(unknown_driver, MAX_UNKO_GAPS_PER_DRIVER);
    (
        drivers,
        drivers_truncated,
        unknown_driver,
        unknown_driver_truncated,
    )
}

/// 応答の JSON (`elapsed_ms` を除く。root はこれに `elapsed_ms` を足して返す)。`gcp` が `None` = etags の口が無い
/// (`gcp_etags_available: false`。この状態の `drivers: []` は「候補が居ない」ではなく「判定できない」)。
pub fn respond(
    month: &str,
    window: &Window,
    driver_cd: Option<i64>,
    onprem: &Onprem,
    gcp: Option<&GcpDriverCds>,
) -> serde_json::Value {
    let gcp_etags_available = gcp.is_some();
    let driver_cds_available = gcp.is_some_and(|m| !m.is_empty());
    let (drivers, drivers_truncated, unknown_driver_unko_nos, unknown_driver_truncated) = match gcp
    {
        Some(m) => build_gaps(
            window.year,
            window.month_num,
            &onprem.seen,
            &onprem.in_month,
            m,
            driver_cd,
        ),
        None => (Vec::new(), false, Vec::new(), false),
    };
    serde_json::json!({
        "month": month,
        "driver_cd": driver_cd,
        "onprem_operations_in_month": onprem_count_for(&onprem.in_month, driver_cd),
        "gcp_etags_available": gcp_etags_available,
        "driver_cds_available": driver_cds_available,
        "unko_no_digits": 22,
        "drivers": drivers.iter().map(|d| serde_json::json!({
            "driver_cd": d.driver_cd,
            "unko_nos": d.unko_nos,
            "truncated": d.truncated,
        })).collect::<Vec<_>>(),
        "drivers_truncated": drivers_truncated,
        "unknown_driver_unko_nos": unknown_driver_unko_nos,
        "unknown_driver_unko_nos_truncated": unknown_driver_truncated,
    })
}
