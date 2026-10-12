//! `/api/uriage/*` の Query と応答の型、ローカルの store (オンプレ版は SQLite、Worker は D1) から読んだ行の型
//! (root の `src/routes/uriage.rs`・`src/sqlite.rs` から移した。Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! JSON の形はオンプレ版と Worker で同じであること (呼び手の nuxt-ichibanboshi は区別しない)。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::compute::{PersonAccum, RowDecision, UriageRow};
use crate::verify::VerifyDiff;

/// Query の `cal` の既定 (省略時 true = 別営業所も合算。PHP の既定)。
pub fn default_cal() -> bool {
    true
}

/// `/api/uriage/recalc` のクエリパラメータ。
///
/// 全パラメータ optional。**body 不要** (CakePHP から persons/other/bumon を pull するため)。
///
/// - `month` 未指定: editable_months 全部を処理
/// - `month` 指定 + `eigyosho_id` 未指定: その月の全営業所を処理
/// - 両方指定: 単一 (month, eigyosho_id) のみ処理
#[derive(Debug, Deserialize, Default)]
pub struct RecalcQuery {
    /// 集計対象月 (`YYYY-MM`)。editable_months 内である必要がある (外なら 422)
    pub month: Option<String>,
    /// 営業所 id (`MastersResponse.offices` のキー)。指定時はその営業所のみ処理
    pub eigyosho_id: Option<i64>,
}

/// 1 (month, eigyosho_id) 単位の recalc 結果。
///
/// 月次テーブルは廃止 (日次 SUM の VIEW に降格、user 指摘 2026-06-30) のため
/// monthly counts は持たない。日次に投入された (日 × 担当者) 行数のみ持つ。
#[derive(Debug, Default, Serialize)]
pub struct RecalcJobResult {
    pub month: String,
    pub eigyosho_id: i64,
    /// `"computed"` | `"failed"` | `"skipped"` (営業所が masters に居ない等)
    pub status: String,
    /// 取得 raw row 数
    pub row_count: usize,
    /// 日次集計に投入された (日 × 担当者) 行数 (cal=true)
    pub daily_count_cal: usize,
    /// 〃 cal=false
    pub daily_count_nocal: usize,
    /// 32 hex 桁の sha256 prefix (fingerprint_after)
    pub fingerprint: Option<String>,
    /// raw NDJSON.gz の絶対 path
    pub raw_path: Option<String>,
    /// 失敗時の error 文言
    pub error: Option<String>,
}

/// `/api/uriage/recalc` のレスポンス。
#[derive(Debug, Serialize)]
pub struct RecalcResponse {
    pub source_table: String,
    /// 処理対象になった editable_months (1 つに絞った場合も配列)
    pub months: Vec<String>,
    /// CakePHP `editable_months_count` のエコー
    pub editable_months_count: i32,
    pub calculated_at: String,
    pub jobs: Vec<RecalcJobResult>,
}

/// `/r2/pending` の optional 期間 filter。
#[derive(Debug, Deserialize)]
pub struct R2PendingQuery {
    /// 下限月 (YYYY-MM、inclusive)。`to` と両方指定で filter 有効。
    pub from: Option<String>,
    /// 上限月 (YYYY-MM、inclusive)。
    pub to: Option<String>,
}

/// `r2_pending` row に verify gate (`expected_count` / `ready` / `blocker`) を付与する。
pub fn annotate_r2_pending(row: R2PendingRow) -> R2PendingItem {
    let expected_count = crate::period::days_in_month(&row.month)
        .map(|d| (d as i64) * 2)
        .unwrap_or(0);
    let (ready, blocker) = if row.verified_ng > 0 {
        (false, Some("ng_present".to_string()))
    } else if expected_count > 0 && row.verified_count < expected_count {
        (false, Some("unverified".to_string()))
    } else if expected_count == 0 {
        // month 形式不正 (理論上ありえないが防御): unverified 扱い
        (false, Some("unverified".to_string()))
    } else {
        (true, None)
    };
    R2PendingItem {
        month: row.month,
        eigyosho_id: row.eigyosho_id,
        raw_path: row.raw_path,
        fingerprint_after: row.fingerprint_after,
        computed_at: row.computed_at,
        verified_count: row.verified_count,
        verified_ok: row.verified_ok,
        verified_ng: row.verified_ng,
        expected_count,
        ready,
        blocker,
    }
}

#[derive(Debug, Serialize)]
pub struct R2PendingResponse {
    pub count: usize,
    pub items: Vec<R2PendingItem>,
}

/// `/r2/pending` の 1 entry。SQLite の `R2PendingRow` に verify gate (expected_count /
/// ready / blocker) を付与した形。
#[derive(Debug, Serialize)]
pub struct R2PendingItem {
    pub month: String,
    pub eigyosho_id: i64,
    pub raw_path: String,
    pub fingerprint_after: String,
    pub computed_at: String,
    pub verified_count: i64,
    pub verified_ok: i64,
    pub verified_ng: i64,
    /// 月の日数 × 2 cal (= 完全 verify を満たす期待 cell 数)
    pub expected_count: i64,
    /// `true` なら caller は /raw + /ack を進めて良い
    pub ready: bool,
    /// `Some("ng_present" | "unverified")` なら blocker。`None` = `ready`
    pub blocker: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct DailyQuery {
    /// 集計対象月 (`YYYY-MM`、必須)
    pub month: String,
    /// 営業所 id (必須)
    pub eigyosho_id: i64,
    /// PHP の `$cal`。truthy で別営業所も合算。省略時 true
    #[serde(default = "default_cal")]
    pub cal: bool,
}

#[derive(Debug, Serialize)]
pub struct DailyResponse {
    pub month: String,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub rows: Vec<PersonDailyRow>,
}

#[derive(Debug, Deserialize)]
pub struct PersonMonthlyTotalsQuery {
    /// 期間下限 (`YYYY-MM`、inclusive)
    pub from: String,
    /// 期間上限 (`YYYY-MM`、inclusive)
    pub to: String,
    /// `cal` フラグ (省略時 true、別営業所合算)
    #[serde(default = "default_cal")]
    pub cal: bool,
}

#[derive(Debug, Serialize)]
pub struct PersonMonthlyTotalsResponse {
    pub from: String,
    pub to: String,
    pub cal: bool,
    /// `(month, person_name)` で sort 済 (eigyosho_id は 0 固定 = 全営業所合算)
    pub rows: Vec<PersonMonthlyRow>,
}

#[derive(Debug, Deserialize)]
pub struct PersonPartnerTotalsQuery {
    /// 担当者名 (`uriage_person_daily.person_name` と同じ namespace、URL エンコード必須)。
    pub person: String,
    /// 期間下限 (`YYYY-MM`、inclusive)
    pub from: String,
    /// 期間上限 (`YYYY-MM`、inclusive)
    pub to: String,
    /// `cal` フラグ (省略時 true、別営業所合算)
    #[serde(default = "default_cal")]
    pub cal: bool,
}

#[derive(Debug, Serialize)]
pub struct PersonPartnerTotalsResponse {
    pub person: String,
    pub from: String,
    pub to: String,
    pub cal: bool,
    /// 得意先別内訳 (`kingaku` DESC)。
    pub customers: Vec<PersonPartnerTotalRow>,
    /// 傭車先別内訳 (`kingaku` DESC、`compute_person_partner_sum_by_day` のコメント参照:
    /// 横横=1 の行では金額側がここにコピーされているため、実際の外部業者への支払いと
    /// 完全一致するとは限らない)。
    pub subcontractors: Vec<PersonPartnerTotalRow>,
}

#[derive(Debug, Deserialize)]
pub struct AdminDeleteQuery {
    pub month: String,
    pub eigyosho_id: i64,
}

#[derive(Debug, Serialize)]
pub struct AdminDeleteResponse {
    pub month: String,
    pub eigyosho_id: i64,
    pub daily_deleted: usize,
    pub jobs_deleted: usize,
}

#[derive(Debug, Serialize)]
pub struct AdminRebuildResponse {
    pub rebuilt_at: String,
}

/// `/api/uriage/verify` のクエリパラメータ。
///
/// shell script (`scripts/verify_uriage_diff_php_vs_rust.sh`) が呼ぶ単日 × 営業所
/// × cal の 1 単位を rust 内製の HTTP endpoint に化けたもの。nuxt UI が大量の
/// 組み合わせを iterate する想定。
#[derive(Debug, Deserialize)]
pub struct VerifyQuery {
    /// 営業所 id (`masters-json.offices` のキー)
    pub id: i64,
    /// 運行年月日 (`YYYY-MM-DD`、単日)
    pub date: String,
    /// `true` で別営業所合算 (PHP 既定)、`false` で除外モード
    #[serde(default = "default_cal")]
    pub cal: bool,
}

/// `/api/uriage/verify` のレスポンス。
#[derive(Debug, Serialize)]
pub struct VerifyResponse {
    pub office_id: i64,
    pub date: String,
    pub cal: bool,
    pub php_sum: HashMap<String, PersonAccum>,
    pub rust_sum: HashMap<String, PersonAccum>,
    /// 差分なし (= 一致) なら `null`
    pub diff: Option<VerifyDiff>,
    /// `diff is None` と同等
    pub ok: bool,
    /// Rust 側 SELECT 行数 (debug 用)
    pub row_count: usize,
    pub elapsed_ms: u64,
    /// 検証スキップ理由 (例: `"no_bumon"`)。検証していないが errors ではないケース。
    /// shell の `[[ "$bumon" == "[]" ]] && continue` 相当。`None` = 検証実施済。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct VerifyDebugRow {
    /// 横横==1 (= 受注∈bumon AND 稼動∉bumon)
    pub yokoyoko: i32,
    /// 請求K (0/1/2)
    pub seikyu_k: i32,
    /// 備考2
    pub biko2: String,
    /// 入力担当C (int)
    pub nyuryoku_tanto_c: i32,
    /// 稼動部門
    pub kado_bumon: String,
    pub kingaku: i64,
    pub nebiki: i64,
    pub warimashi: i64,
    pub jippi: i64,
    pub yosha_kingaku: i64,
    pub yosha_nebiki: i64,
    pub yosha_warimashi: i64,
    pub yosha_jippi: i64,
    /// 社員R (社員ﾏｽﾀ から TOP 1)
    pub shain_r: String,
    pub yosha_saki_c: String,
    /// 運行年月日 (Rust の WHERE フィルタはこれを使う)
    pub unko_date: String,
    /// 売上年月日 (月計テーブル一致条件はこちら。PHP `/print-json` がこの列で
    /// フィルタしている可能性あり = NG 原因候補、user 2026-06-30)
    pub uriage_date: String,
    /// `compute_person_sum` の判定結果 (B1-B6 のどこに行ったか + 振替後担当)
    pub decision: RowDecision,
}

#[derive(Debug, Serialize)]
pub struct VerifyDebugResponse {
    pub office_id: i64,
    pub date: String,
    pub cal: bool,
    /// office の bumon list (SQL `WHERE 受注部門 IN (...) OR 稼動部門 IN (...)` の右辺)
    pub bumon: Vec<String>,
    /// 入力担当C → 担当者名 (Rust の persons map)
    pub persons: HashMap<i32, String>,
    /// 稼動部門 → 担当者名 (Rust の other map)
    pub other: HashMap<String, String>,
    /// SQL から fetched された raw rows (decisions 同梱)
    pub rows: Vec<VerifyDebugRow>,
    /// PHP $sum (`/print-json` の結果、0 entry 含む)
    pub php_sum: HashMap<String, PersonAccum>,
    /// Rust $sum (`compute_person_sum` の結果、0 entry 含む)
    pub rust_sum: HashMap<String, PersonAccum>,
    /// php_sum vs rust_sum の diff (0 entry 除外後)
    pub diff: Option<VerifyDiff>,
}

#[derive(Debug, Deserialize)]
pub struct RecalcJobsQuery {
    /// 期間下限 (YYYY-MM、inclusive)。`to` と両方指定で filter 有効。
    pub from: Option<String>,
    /// 期間上限 (YYYY-MM、inclusive)。
    pub to: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RecalcJobsResponse {
    pub from: Option<String>,
    pub to: Option<String>,
    pub count: usize,
    pub jobs: Vec<RecalcJob>,
}

#[derive(Debug, Deserialize)]
pub struct VerifyHistoryQuery {
    /// 期間下限 (`YYYY-MM-DD`、inclusive)
    pub from: String,
    /// 期間上限 (`YYYY-MM-DD`、inclusive)
    pub to: String,
    /// 営業所 id (省略で全営業所)
    pub eigyosho_id: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct VerifyHistoryResponse {
    pub from: String,
    pub to: String,
    pub eigyosho_id: Option<i64>,
    pub count: usize,
    pub rows: Vec<VerifyJobRow>,
}

impl VerifyDebugRow {
    /// SQL の 1 行と、その行の `compute_person_sum` の判定を並べる (`verify-debug` の `rows[]`)。
    pub fn new(r: &UriageRow, d: &RowDecision) -> Self {
        Self {
            yokoyoko: r.yokoyoko,
            seikyu_k: r.seikyu_k,
            biko2: r.biko2.clone(),
            nyuryoku_tanto_c: r.nyuryoku_tanto_c,
            kado_bumon: r.kado_bumon.clone(),
            kingaku: r.kingaku,
            nebiki: r.nebiki,
            warimashi: r.warimashi,
            jippi: r.jippi,
            yosha_kingaku: r.yosha_kingaku,
            yosha_nebiki: r.yosha_nebiki,
            yosha_warimashi: r.yosha_warimashi,
            yosha_jippi: r.yosha_jippi,
            shain_r: r.shain_r.clone(),
            yosha_saki_c: r.yoshasaki_c.clone(),
            unko_date: r.unko_date.clone(),
            uriage_date: r.uriage_date.clone(),
            decision: d.clone(),
        }
    }
}

/// 担当者×月×営業所×cal の集計 1 行 (DB から read out した raw 値)。
///
/// `*_y0` は 横横=0 (= 自社運行 or sql_from_other) のみの集計値。
/// 横横除外フィルタ UI で main fields の代わりに使う。
/// 旧 data (recalc 未実行) は 0 になる (= recalc 全期間で正しい値に上書きされる)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonMonthlyRow {
    pub month: String,
    pub person_name: String,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub kingaku: i64,
    pub yosha_kingaku: i64,
    pub kensuu: i64,
    pub kingaku_y0: i64,
    pub yosha_kingaku_y0: i64,
    pub kensuu_y0: i64,
    pub calculated_at: String,
}

/// 担当者×日×営業所×cal の集計 1 行 (drill-down 用)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonDailyRow {
    /// 運行年月日 (`YYYY-MM-DD`)
    pub unko_date: String,
    /// 集計バケット月 (`YYYY-MM`、unko_date の上位 7 文字と一致する想定)
    pub month: String,
    pub person_name: String,
    pub eigyosho_id: i64,
    pub cal: bool,
    pub kingaku: i64,
    pub yosha_kingaku: i64,
    pub kensuu: i64,
    pub kingaku_y0: i64,
    pub yosha_kingaku_y0: i64,
    pub kensuu_y0: i64,
    pub calculated_at: String,
}

/// 担当者×得意先/傭車先 内訳 1 行 (期間集計、全営業所合算)。
/// `GET /api/uriage/person-partner-totals` のデータソース。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PersonPartnerTotalRow {
    pub partner_code: String,
    pub partner_name: String,
    pub kingaku: i64,
    pub kensuu: i64,
    pub kingaku_y0: i64,
    pub kensuu_y0: i64,
}

/// `recalc_jobs` 1 行。`status` 遷移: `queued → computing → computed → r2_synced`。
/// 失敗時は `status='failed' + last_error`。
///
/// `verified_*` は `verify_coverage` view 経由で LEFT JOIN した verify 集計
/// (= 当該 (month, eigyosho_id) の verify_jobs 行数)。verify 未走行は 0 / 0 / 0。
/// UI 側で `expected_count = days_in_month × 2 cal` と突合して
/// 「全件 verify 済み / 一部のみ / 未走行」を判定する。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RecalcJob {
    pub month: String,
    pub eigyosho_id: i64,
    pub status: String,
    pub fingerprint_before: Option<String>,
    pub fingerprint_after: Option<String>,
    pub raw_path: Option<String>,
    pub created_at: String,
    /// 最後に recalc が走った時刻 (fingerprint が変わったかどうかに関わらず更新)。
    /// 「recalc を試した最終時刻」を示す。
    pub computed_at: Option<String>,
    /// **fingerprint が実際に変化した時刻** (= データが変わった時刻)。
    /// fingerprint 不変な再 recalc では更新されない。`computed_at` だけ進んで
    /// `fingerprint_changed_at` が古いままなら「最近 recalc は走ったが data は
    /// 変わっていない」と分かる (user 2026-06-30: 「fingerprint の時間入れないと
    /// わからない」)。
    pub fingerprint_changed_at: Option<String>,
    pub r2_synced_at: Option<String>,
    pub last_error: Option<String>,
    pub verified_count: i64,
    pub verified_ok: i64,
    pub verified_ng: i64,
}

/// `r2_pending` view の 1 行 (R2 へ送信すべき 月×営業所)。
///
/// 2026-06-30 (`verify_jobs` 導入) で verify coverage の summary を同居。
/// `ready=true` のみ R2 sync 対象、`blocker` は scenairo (unverified/ng_present) を示す。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct R2PendingRow {
    pub month: String,
    pub eigyosho_id: i64,
    pub raw_path: String,
    pub fingerprint_after: String,
    pub computed_at: String,
    /// verify_jobs に存在する (date, cal) 行数 (全 cal flag 合計)
    pub verified_count: i64,
    /// verify_jobs.ok = 1 の行数
    pub verified_ok: i64,
    /// verify_jobs.ok = 0 の行数
    pub verified_ng: i64,
}

/// `verify_jobs` table の 1 行 (検証履歴)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct VerifyJobRow {
    pub unko_date: String,
    pub eigyosho_id: i64,
    pub cal: i64, // 0 / 1
    pub month: String,
    pub ok: i64,
    pub skipped_reason: Option<String>,
    pub diff_json: Option<String>,
    pub row_count: i64,
    pub elapsed_ms: i64,
    pub ran_at: String,
}
