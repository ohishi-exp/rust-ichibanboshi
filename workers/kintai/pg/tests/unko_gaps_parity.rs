//! unko-gaps の写し 2 つ (`kintai_logic::unko_gaps` の `MONTH_OPERATIONS_SQL`・`PUSHED_SOURCES` と、alc の etags の
//! path・期間・応答の読み方) が root と一致することを固定する (Refs #322)。DB は要らない。
//!
//! どちらも root の勤怠の版 (`KINTAI_OUTPUT_SHA`) の glob の中 (`src/kintai_push.rs`・`src/kintai_http_repo.rs`) にあり、
//! root を動かすと logic_version が変わるので共有にできない (fold を移す段で解消する)。
//!
//! - SQL は root の `pub const` と値で比べる
//! - etags は root 側が private (`ETAGS_PATH`・`UpstreamEtagItem`・`UpstreamEtags`・`UnsplitOperation`・`month_etags_bounds`・
//!   `fetch_etags`) で値を外から取れないので、root のソースの該当の定義を**文字列で**固定する (text pin)。
//!   ここが落ちたら root の etags の読み方が変わっている — `logic/src/unko_gaps.rs` の写しも直してから pin を更新する
//!
//! この crate に置くのは、repo ルートの package を dev-dependency に持つのがここだけだから。

use kintai_logic::unko_gaps::{
    etags_search, read_etags, RpcResult, ETAGS_PATH, MONTH_OPERATIONS_SQL, PUSHED_SOURCES,
};

/// root の alc の etags を読む実装 (`HttpKintaiEventsRepo` の `fetch_etags` まわり)。
const ROOT_HTTP_REPO: &str = include_str!("../../../../src/kintai_http_repo.rs");

#[test]
fn month_operations_sql_and_sources_match_root() {
    assert_eq!(
        MONTH_OPERATIONS_SQL,
        rust_ichibanboshi::kintai_push::MONTH_OPERATIONS_SQL
    );
    assert_eq!(
        PUSHED_SOURCES,
        rust_ichibanboshi::kintai_push::PUSHED_SOURCES
    );
}

/// root のソースに `snippet` がそのまま 1 回だけ在ること。
fn pinned(snippet: &str) {
    let n = ROOT_HTTP_REPO.matches(snippet).count();
    assert_eq!(n, 1, "root の src/kintai_http_repo.rs の定義が変わった (写しを直してから pin を更新):\n{snippet}");
}

/// pin: root の etags の path の定数 (写しは `ETAGS_PATH`)。
#[test]
fn the_etags_path_matches_root() {
    pinned(r#"const ETAGS_PATH: &str = "/api/dtako/events/etags";"#);
    assert_eq!(ETAGS_PATH, "/api/dtako/events/etags");
}

/// pin: root の etags の応答の型 3 つ (写しは `logic/src/unko_gaps.rs` の同名の private な型。欄・型・`serde(default)` が同じ)。
#[test]
fn the_etags_response_types_match_root() {
    pinned(
        "struct UpstreamEtagItem {
    unko_no: String,
    #[serde(default)]
    etag: Option<String>,
    #[serde(default)]
    driver_cds: Vec<String>,
}",
    );
    pinned(
        "pub struct UnsplitOperation {
    pub unko_no: String,
    pub driver_cd: String,
    pub reading_date: String,
}",
    );
    pinned(
        "struct UpstreamEtags {
    #[serde(default)]
    items: Vec<UpstreamEtagItem>,
    #[serde(default)]
    warnings: Vec<String>,
    #[serde(default)]
    unsplit: Vec<UnsplitOperation>,
    #[serde(default)]
    unsplit_total: usize,
}",
    );
}

/// pin: root の etags の期間 `[月初, 翌月初]` (写しは `etags_search` の `date_from`・`date_to`)。unko-gaps は
/// `fetch_dtako_month_digest` (遡らない = `since: None`) から呼ぶので始端は月初。
#[test]
fn the_etags_window_matches_root() {
    pinned(
        "fn month_etags_bounds(month: &str) -> Option<(NaiveDate, NaiveDate)> {
    let year: i32 = month.get(..4)?.parse().ok()?;
    let mm: u32 = month.get(5..7)?.parse().ok()?;
    let first = NaiveDate::from_ymd_opt(year, mm, 1)?;
    let next_first = if mm == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mm + 1, 1)?
    };
    Some((first, next_first))
}",
    );
    pinned("self.fetch_dtako_month_digest_since(month, None).await");
    pinned(
        "        let from = since.map_or(first, |d| d.min(first));
        let pairs = self.fetch_etags(from, to).await?;",
    );
    assert_eq!(
        etags_search("2026-12").as_deref(),
        Some("date_from=2026-12-01&date_to=2027-01-01")
    );
}

/// pin: root の etags の 1 往復 (GET・期間の 2 欄・`X-Tenant-ID`・404 だけが「口なし」・`unko_no` → `driver_cds` の collect)。
/// 写しは `etags_search` (期間の 2 欄) と `read_etags`。GET・path・`X-Tenant-ID` は auth-worker の `KintaiAlcEntrypoint` が固定する。404 以外の非 2xx・parse 失敗は、root は Err (unko-gaps は
/// warn だけで `gcp_etags_available: false`)、Worker は 502 (README の対照表)。
#[test]
fn the_etags_round_trip_matches_root() {
    pinned(
        "            .get(&self.etags_url)
            .query(&[
                (\"date_from\", date_from.to_string()),
                (\"date_to\", date_to.to_string()),
            ])
            .header(\"X-Tenant-ID\", &self.tenant_id);",
    );
    pinned(
        "        if status == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }",
    );
    pinned(
        "        let drivers = pairs
            .iter()
            .map(|(u, _, ds)| (u.clone(), ds.clone()))
            .collect();",
    );
    let not_found = RpcResult {
        status: 404,
        body: String::new(),
    };
    assert_eq!(read_etags(&not_found), Ok(None));
}
