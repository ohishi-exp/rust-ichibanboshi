//! 担当者別売上集計 (`/uriage-jyuchu-display/print` の Rust 移植、issue #762)。
//!
//! 元 PHP は `yhonda-ohishi/nginx`
//! `html/app/src/Controller/UriageJyuchuDisplayController::computePersonSum()`
//! (PR #764 で `templates/UriageJyuchuDisplay/print.php` 118-356 行から
//! 共有ヘルパへ抽出済み)。
//!
//! 本 module はその純粋関数を 1:1 で写経したもの。**挙動の完全再現が最優先**
//! のため、条件式・符号・正規表現の意味は一字一句変えない (リファクタ禁止)。
//! テストは `tests/uriage_test.rs` で同じ 17 ケース + 全分岐網羅。

use axum::extract::{Path, Query};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::LazyLock;

use regex::Regex;
use sha2::{Digest, Sha256};

use crate::cakephp::{CakephpClient, CakephpError, MastersResponse, OfficeMasters};
use crate::config::RawConfig;
use crate::repo::{DynRepo, RepoError};
use crate::sqlite::{DynLocalStore, LocalStoreError};

/// `preg_match("/(.+)→(.+)/u", $cleaned, $pregtest)` の写経。
///
/// 備考2 から「売上 」プレフィクスを除去した文字列を `→` で先頭一致 split する。
/// `(.+)` は greedy なので "A→B→C" は A="A→B", B="C" を返す (PHP/PCRE と同じ)。
static ARROW_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Rust regex の `.` はデフォルトで Unicode 1 文字に対応する (Unicode mode on)。
    // PHP の `/u` 修飾子と等価。
    Regex::new("(.+)→(.+)").expect("static regex compiles")
});

/// 運転日報明細 + マスタ JOIN 由来の入力 1 行 (PHP `$ichi` 相当)。
///
/// 担当者振替に必要な列だけを切り出した中間表現。SQL Server 側から
/// tiberius で取り出す repo 層 (後続 PR4-5) はこの struct を埋める。
#[derive(Debug, Clone, Default)]
pub struct UriageRow {
    /// 横横 (0/1)。1 なら自社側 (`金額/値引/割増/実費`) を傭車側にコピー。
    pub yokoyoko: i32,
    /// 請求K (0/1/2)。
    pub seikyu_k: i32,
    /// 備考2。
    pub biko2: String,
    /// 入力担当C。
    pub nyuryoku_tanto_c: i32,
    /// 稼動部門 (例 `"010"`, `"021"`)。
    pub kado_bumon: String,
    /// 金額。
    pub kingaku: i64,
    /// 値引 (PHP では正数で来るため `+=` で加算する)。
    pub nebiki: i64,
    /// 割増。
    pub warimashi: i64,
    /// 実費。
    pub jippi: i64,
    /// 傭車金額。
    pub yosha_kingaku: i64,
    /// 傭車値引。
    pub yosha_nebiki: i64,
    /// 傭車割増。
    pub yosha_warimashi: i64,
    /// 傭車実費。
    pub yosha_jippi: i64,
    /// 社員R (担当割当が無いときの表示用 fallback)。
    pub shain_r: String,
    /// 傭車先C (`"000000"` なら自車、それ以外は傭車)。
    /// `compute_person_sum` は読まないが、`cal_total` / `is_yosha` で使う。
    pub yoshasaki_c: String,
    /// 運行年月日 (`YYYY-MM-DD`)。日次集計 (`uriage_person_daily`) と raw NDJSON.gz
    /// の drill-down に使う。`compute_person_sum` の振替判定では使わないが、
    /// `compute_person_sum_by_day` で行を日付ごとにグルーピングするのに必須。
    pub unko_date: String,
}

/// `cal_total()` (PHP 803-819) 等価。営業所別月次集計で使う 1 行ぶんのサブ
/// トータル群。
///
/// **担当者別集計 (`compute_person_sum`) とは値引の符号が違う**:
/// - cal_total: `金額 + 割増 − 値引` (値引マイナス)
/// - 担当者別: `金額 + 値引 + 割増` (値引プラス)
///
/// 元 PHP の `is_yosha()` 分岐は dead code (`if (!$this->is_yosha()) { ... } else
/// { ... }` の両 branch が同形になっているためコメントアウト済み) のため、
/// 本写経では分岐させずすべて計算する。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
pub struct CalTotal {
    /// 実費抜き合計 (`金額 + 割増 − 値引`)。
    pub total_ex_jippi: i64,
    /// 実費込み合計 (`total_ex_jippi + 実費`)。
    pub total: i64,
    /// 傭車実費抜き合計 (`傭車金額 + 傭車割増 − 傭車値引`)。
    pub ytotal_ex_jippi: i64,
    /// 傭車実費込み合計 (`ytotal_ex_jippi + 傭車実費`)。
    pub ytotal: i64,
    /// 支払 (`傭車金額 + 傭車割増 − 傭車値引 + 傭車実費`、ytotal と同値だが
    /// PHP では別変数に積んでいるので構造を保つ)。
    pub shiharai: i64,
}

/// 担当者ごとの集計値。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct PersonAccum {
    /// 傭車金額累計 (`傭車金額 + 傭車値引 + 傭車割増 + 傭車実費`)。
    #[serde(rename = "傭車金額")]
    pub yosha_kingaku: i64,
    /// 金額累計 (`金額 + 値引 + 割増 + 実費`)。
    #[serde(rename = "金額")]
    pub kingaku: i64,
    /// 件数。
    #[serde(rename = "件数")]
    pub kensuu: i64,
}

/// 1 行ごとの判定結果 (テンプレ表示用)。
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RowDecision {
    /// 「請求のみ」でスキップしたかどうか (表示も集計もしない)。
    pub skip: bool,
    /// 括弧表示フラグ (`$ck`)。
    pub ck: bool,
    /// 担当一致 (赤字でない)。
    pub matched: bool,
    /// 振替後担当 (`$tanto`)。空文字なら表示しない or 未確定。
    pub tanto: String,
}

/// `compute_person_sum` の戻り値。PHP では `['sum' => ..., 'rows' => ...]`。
#[derive(Debug, Clone, Serialize)]
pub struct PersonSumResult {
    pub sum: HashMap<String, PersonAccum>,
    pub rows: Vec<RowDecision>,
}

/// `UriageJyuchuDisplayController::computePersonSum()` を 1:1 で写経した純粋関数。
///
/// # 引数
/// - `rows`: 運転日報明細 + 社員ﾏｽﾀ JOIN 済みの行
/// - `persons`: 入力担当C → 担当者名 (`UriageJyuchuDisplayPersons_name`)。
///   値 (担当者名) の集合がマスタ。
/// - `other`: 稼動部門コード → 営業所名 (`UriageJyuchuDisplay_other`)。
///   キー存在 = その部門は「別営業所」扱い。
/// - `cal`: テンプレ `$uriageDiff`。truthy で別営業所も合算する (PHP `$id != "cal"`)。
///
/// # 振替判定 (6 段階 if-elseif)
/// 1. 備考2 が `A→B` の形で A がマスタ → A に積算
/// 2. 〃 B がマスタ → B
/// 3. 備考2 の空白・「売上」除去後がマスタ → そこに積算 (条件あり)
/// 4. 備考2 == "表示のみ" AND 請求K != 2 → 入力担当名のみ表示、積算しない
/// 5. 入力担当C がマスタ → そこに積算
/// 6. いずれも該当せず → Fr / To / 社員R の順で表示のみ
pub fn compute_person_sum(
    rows: &[UriageRow],
    persons: &HashMap<i32, String>,
    other: &HashMap<String, String>,
    cal: bool,
) -> PersonSumResult {
    // print.php:118-122 — 担当名で 0 初期化。`persons` の値 (名前) が同じ key を
    // 持つ場合は最後の値で上書き相当だが、値はどれも 0 なので結果は同じ。
    let mut sum: HashMap<String, PersonAccum> = HashMap::new();
    for name in persons.values() {
        sum.entry(name.clone()).or_default();
    }

    // `in_array($name, $persons)` 高速化用の名前 set
    let person_names: HashSet<&str> = persons.values().map(String::as_str).collect();

    let mut decisions: Vec<RowDecision> = Vec::with_capacity(rows.len());

    for ichi in rows {
        // print.php:204 — 請求のみはスキップ (表示も集計もしない)
        if ichi.seikyu_k == 1 && ichi.biko2 == "請求のみ" {
            decisions.push(RowDecision {
                skip: true,
                ck: false,
                matched: false,
                tanto: String::new(),
            });
            continue;
        }

        // print.php:230-236 — 横横==1 は傭車側に金額/値引/割増/実費を写し替え
        let (kingaku, nebiki, warimashi, jippi) =
            (ichi.kingaku, ichi.nebiki, ichi.warimashi, ichi.jippi);
        let (yosha_kingaku, yosha_nebiki, yosha_warimashi, yosha_jippi) = if ichi.yokoyoko == 1 {
            (kingaku, nebiki, warimashi, jippi)
        } else {
            (
                ichi.yosha_kingaku,
                ichi.yosha_nebiki,
                ichi.yosha_warimashi,
                ichi.yosha_jippi,
            )
        };

        // 加算用 (B1〜B3, B5 で共通)
        let kingaku_sum = kingaku + nebiki + warimashi + jippi;
        let yosha_sum = yosha_kingaku + yosha_nebiki + yosha_warimashi + yosha_jippi;

        // print.php:240-250 — 備考2 の "売上 " 接頭辞除去 + "A→B" パース
        let cleaned_biko = strip_uriage_prefix(&ichi.biko2);
        let (uriage_fr_name, uriage_to_name) = match ARROW_RE.captures(&cleaned_biko) {
            Some(caps) if ichi.seikyu_k != 1 => (
                Some(caps.get(1).unwrap().as_str().to_string()),
                Some(caps.get(2).unwrap().as_str().to_string()),
            ),
            _ => (None, None),
        };

        // print.php:255 — テンプレ $uriageDiff = $cal
        let uriage_diff = cal;

        // print.php:268-274 — $matched
        let lookup_key = normalize_biko_for_lookup(&ichi.biko2);
        let matched = persons.contains_key(&ichi.nyuryoku_tanto_c)
            || person_names.contains(lookup_key.as_str())
            || uriage_fr_name
                .as_deref()
                .is_some_and(|s| person_names.contains(s))
            || uriage_to_name
                .as_deref()
                .is_some_and(|s| person_names.contains(s));

        // print.php:257-265 — $ck (括弧表示フラグ)
        let ck = (ichi.biko2 == "表示" && ichi.seikyu_k == 2)
            || (ichi.biko2 == "表示のみ" && ichi.seikyu_k == 1)
            || (!uriage_diff
                && uriage_fr_name
                    .as_deref()
                    .is_some_and(|s| person_names.contains(s)))
            || (!uriage_diff && other.contains_key(&ichi.kado_bumon));

        // 別営業所合算判定
        let should_aggregate = uriage_diff || !other.contains_key(&ichi.kado_bumon);

        let mut tanto = String::new();

        // print.php:284-355 — 6 段階 if-elseif
        if let Some(fr) = uriage_fr_name
            .as_deref()
            .filter(|s| person_names.contains(s))
        {
            // B1: A→B のうち A (Fr) がマスタに居る
            tanto.push_str(fr);
            if should_aggregate {
                accumulate(&mut sum, fr, kingaku_sum, yosha_sum);
            }
        } else if let Some(to) = uriage_to_name
            .as_deref()
            .filter(|s| person_names.contains(s))
        {
            // B2: A→B のうち B (To) がマスタに居る
            tanto.push_str(to);
            if should_aggregate {
                accumulate(&mut sum, to, kingaku_sum, yosha_sum);
            }
        } else if person_names.contains(lookup_key.as_str()) {
            // B3: 備考2 を空白除去・「売上」除去した結果がマスタに居る
            //   元 PHP 条件: 備考2 != "表示" AND
            //                (請求K != 2 OR (請求K == 2 AND 備考2 に "売上" 含む))
            //   → 後段の `請求K == 2 AND ...` の前提条件は前段で既に補集合で
            //     カバーされているため、論理等価で次のように簡約できる:
            //     備考2 != "表示" AND (請求K != 2 OR 備考2 に "売上" 含む)
            let allow_seikyu =
                ichi.biko2 != "表示" && (ichi.seikyu_k != 2 || ichi.biko2.contains("売上"));
            if allow_seikyu && should_aggregate {
                accumulate(&mut sum, &lookup_key, kingaku_sum, yosha_sum);
            }
            tanto = lookup_key.clone();
        } else if ichi.biko2 == "表示のみ" && ichi.seikyu_k != 2 {
            // B4: 表示のみ AND 請求K != 2 → 入力担当名を表示するだけ (集計せず)。
            //
            //   元 PHP は `if (!$ichi["請求K"] == 1) $tanto = ...` で演算子優先度
            //   により `(!請求K) == 1` と解釈される。請求K==0 のとき `!0`=true=1
            //   で条件成立し $tanto 設定、請求K==1 のとき `!1`=false=0 で不成立。
            //   (請求K==2 は外側 elseif で既に弾かれている)
            if ichi.seikyu_k == 0 {
                if let Some(name) = persons.get(&ichi.nyuryoku_tanto_c) {
                    tanto = name.clone();
                }
            }
        } else if let Some(name) = persons.get(&ichi.nyuryoku_tanto_c) {
            // B5: 入力担当C がマスタに居る
            //   例外: 備考2 == "表示" AND 請求K == 2 (料金合計) は積算しない
            tanto = name.clone();
            if !(ichi.biko2 == "表示" && ichi.seikyu_k == 2) && should_aggregate {
                let name = name.clone();
                accumulate(&mut sum, &name, kingaku_sum, yosha_sum);
            }
        } else {
            // B6: いずれも該当せず
            //   表示のみ AND 請求K == 1 → 表示せず (tanto は空のまま)
            //   それ以外: Fr → To → 社員R の順で表示
            if !(ichi.biko2 == "表示のみ" && ichi.seikyu_k == 1) {
                tanto = uriage_fr_name
                    .clone()
                    .or_else(|| uriage_to_name.clone())
                    .unwrap_or_else(|| ichi.shain_r.clone());
            }
        }

        decisions.push(RowDecision {
            skip: false,
            ck,
            matched,
            tanto,
        });
    }

    PersonSumResult {
        sum,
        rows: decisions,
    }
}

/// 日次集計版。rows を `unko_date` でグルーピングし、各日について
/// `compute_person_sum` を呼ぶ。返り値は `unko_date` → 担当者名 → `PersonAccum`。
///
/// 月集計 (`compute_person_sum` を月 rows 全体に 1 度呼ぶ) と日集計 (本関数) の
/// 合計値は `cal` 値が同じなら一致する (compute_person_sum が行ごとに独立して
/// 担当者を決めるため、和は順序非依存)。
pub fn compute_person_sum_by_day(
    rows: &[UriageRow],
    persons: &HashMap<i32, String>,
    other: &HashMap<String, String>,
    cal: bool,
) -> HashMap<String, HashMap<String, PersonAccum>> {
    // unko_date → Vec<&UriageRow> でグルーピング
    let mut by_day: HashMap<String, Vec<UriageRow>> = HashMap::new();
    for r in rows {
        by_day
            .entry(r.unko_date.clone())
            .or_default()
            .push(r.clone());
    }
    // 各日で compute_person_sum を呼ぶ。`persons` 由来の 0 初期化エントリは捨てて
    // 非ゼロ担当者だけ返す (SQLite に投入する upsert 側で zero filter する前提)。
    let mut out: HashMap<String, HashMap<String, PersonAccum>> = HashMap::new();
    for (date, day_rows) in by_day {
        let res = compute_person_sum(&day_rows, persons, other, cal);
        let non_zero: HashMap<String, PersonAccum> = res
            .sum
            .into_iter()
            .filter(|(_, v)| v.kensuu != 0 || v.kingaku != 0 || v.yosha_kingaku != 0)
            .collect();
        if !non_zero.is_empty() {
            out.insert(date, non_zero);
        }
    }
    out
}

/// 集計加算 (`$sum[$name]['金額'] += k_sum` 相当)。
fn accumulate(sum: &mut HashMap<String, PersonAccum>, name: &str, k_sum: i64, y_sum: i64) {
    let acc = sum.entry(name.to_string()).or_default();
    acc.kingaku += k_sum;
    acc.yosha_kingaku += y_sum;
    acc.kensuu += 1;
}

/// `cal_total()` の写経。営業所別月次集計 (`make_month_arrays`、後続 PR で実装) で
/// 使う 1 行ぶんのサブトータルを計算する。
///
/// 元 PHP: UriageJyuchuDisplayController::cal_total() (803-819)。
pub fn cal_total(ichi: &UriageRow) -> CalTotal {
    let total_ex_jippi = ichi.kingaku + ichi.warimashi - ichi.nebiki;
    let total = total_ex_jippi + ichi.jippi;
    let ytotal_ex_jippi = ichi.yosha_kingaku + ichi.yosha_warimashi - ichi.yosha_nebiki;
    let ytotal = ytotal_ex_jippi + ichi.yosha_jippi;
    let shiharai = ichi.yosha_kingaku + ichi.yosha_warimashi - ichi.yosha_nebiki + ichi.yosha_jippi;
    CalTotal {
        total_ex_jippi,
        total,
        ytotal_ex_jippi,
        ytotal,
        shiharai,
    }
}

/// `is_yosha()` の写経 — 自車/傭車判定。
///
/// `傭車先C == "000000"` (6 桁ゼロ) なら自車、それ以外は傭車。
/// 元 PHP: UriageJyuchuDisplayController::is_yosha() (787-790)。
pub fn is_yosha(yoshasaki_c: &str) -> bool {
    yoshasaki_c != "000000"
}

/// `is_oth_yosha()` の写経 — 別営業所判定。
///
/// 受注部門の営業所と稼動部門の営業所が異なる場合 true。
/// 部門コード → 営業所名/id へのマッピングは呼び出し側で済ませ、本関数は
/// 解決済みの **営業所識別子** を 2 つ受け取って比較するだけ (PHP 側も
/// `$this->jbmn->dply->id != $this->kbmn->dply->id` と解決後を比較している)。
///
/// 元 PHP: UriageJyuchuDisplayController::is_oth_yosha() (798-801)。
pub fn is_oth_yosha(jyuchu_eigyosho_id: &str, kado_eigyosho_id: &str) -> bool {
    jyuchu_eigyosho_id != kado_eigyosho_id
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: POST /api/uriage/by-person
// ══════════════════════════════════════════════════════════════

/// `/api/uriage/by-person` のリクエストボディ。
///
/// マスタ (`persons` / `other`) は CakePHP 側 SoT のため呼び出し側で fetch して
/// body に積む。Rust はマスタを persist しない (検証段階の挙動の完全再現が目的)。
#[derive(Debug, Deserialize)]
pub struct ByPersonRequest {
    /// 運行年月日 下限 (YYYY-MM-DD、含む)
    pub from: String,
    /// 運行年月日 上限 (YYYY-MM-DD、含む)
    pub to: String,
    /// 受注部門コード (営業所配下の部門群)。PHP の
    /// `jyuchu_bumon_in_jyuchu_display[$display_id]` 相当
    pub bumon: Vec<String>,
    /// 入力担当C → 担当者名 (`UriageJyuchuDisplayPersons_name`)
    pub persons: HashMap<i32, String>,
    /// 稼動部門コード → 営業所名 (`UriageJyuchuDisplay_other`)
    pub other: HashMap<String, String>,
    /// PHP の `$cal`。truthy で別営業所も合算。省略時 true。
    #[serde(default = "default_cal")]
    pub cal: bool,
}

fn default_cal() -> bool {
    true
}

/// `/api/uriage/by-person` のレスポンス。
#[derive(Debug, Serialize)]
pub struct ByPersonResponse {
    pub source_table: String,
    pub from: String,
    pub to: String,
    pub bumon: Vec<String>,
    pub cal: bool,
    /// 担当者振替判定後の集計 (PHP `$sum` と 1:1)。
    pub sum: HashMap<String, PersonAccum>,
    /// 取得行数 (デバッグ用、`request.K` フィルタ後)。
    pub row_count: usize,
}

fn map_repo_err(e: RepoError) -> StatusCode {
    match &e {
        RepoError::PoolError => StatusCode::SERVICE_UNAVAILABLE,
        RepoError::QueryError(msg) => {
            tracing::error!("Query error: {msg}");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

/// POST `/api/uriage/by-person`
///
/// PHP `/uriage-jyuchu-display/print-json` の Rust 等価 endpoint。検証段階で
/// 1 円単位 diff を取るために、PHP 側で fetch した persons/other をそのまま
/// body に積んで呼び出す。
pub async fn by_person(
    Extension(repo): Extension<DynRepo>,
    Json(req): Json<ByPersonRequest>,
) -> Result<Json<ByPersonResponse>, StatusCode> {
    // 入力検証
    if req.from.is_empty() || req.to.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    if req.bumon.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let rows = repo
        .uriage_rows(&req.from, &req.to, &req.bumon)
        .await
        .map_err(map_repo_err)?;
    let row_count = rows.len();

    let result = compute_person_sum(&rows, &req.persons, &req.other, req.cal);

    Ok(Json(ByPersonResponse {
        source_table: "運転日報明細 + 社員ﾏｽﾀ".to_string(),
        from: req.from,
        to: req.to,
        bumon: req.bumon,
        cal: req.cal,
        sum: result.sum,
        row_count,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: POST /api/uriage/recalc
//   CakePHP pull → editable_months チェック → raw NDJSON.gz 出力 → fingerprint 記録
// ══════════════════════════════════════════════════════════════

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

fn map_local_store_err(e: LocalStoreError) -> StatusCode {
    tracing::error!("local store error: {e}");
    StatusCode::INTERNAL_SERVER_ERROR
}

fn map_cakephp_err(e: CakephpError) -> (StatusCode, String) {
    match e {
        CakephpError::NotConfigured => (
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url が未設定".to_string(),
        ),
        CakephpError::RequestFailed(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP fetch failed: {m}"),
        ),
        CakephpError::StatusError {
            status,
            body_excerpt,
        } => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP status {status}: {body_excerpt}"),
        ),
        CakephpError::JsonError(m) => (
            StatusCode::BAD_GATEWAY,
            format!("CakePHP response parse failed: {m}"),
        ),
    }
}

/// `month` (YYYY-MM) → 月初/月末 日付 (YYYY-MM-DD) を返す。
/// SQL Server の `運行年月日 >=` と `<=` の inclusive 区間に使う。
pub(crate) fn month_to_range(month: &str) -> Option<(String, String, String)> {
    // YYYY-MM 形式チェック
    if month.len() != 7 || month.as_bytes()[4] != b'-' {
        return None;
    }
    let year: i32 = month[..4].parse().ok()?;
    let m: u32 = month[5..].parse().ok()?;
    if !(1..=12).contains(&m) {
        return None;
    }
    // chrono で月末日を計算
    let first = chrono::NaiveDate::from_ymd_opt(year, m, 1)?;
    // 月末 = 翌月初の前日
    let next = if m == 12 {
        chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        chrono::NaiveDate::from_ymd_opt(year, m + 1, 1)?
    };
    let last = next.pred_opt()?;
    // masters fetch 用に「月末日」を date として返す (CakePHP `/masters-json?date=` 用)
    Some((
        first.format("%Y-%m-%d").to_string(),
        last.format("%Y-%m-%d").to_string(),
        last.format("%Y-%m-%d").to_string(),
    ))
}

/// raw rows を JSON-serializable 構造体に変換 (sort 済み列順は repo 側で保証済み、再 sort しない)。
///
/// `unko_date` を含めることで、R2 にある raw NDJSON.gz から後で日次再集計できる。
#[derive(Serialize)]
struct RawRowOut<'a> {
    unko_date: &'a str,
    yokoyoko: i32,
    seikyu_k: i32,
    biko2: &'a str,
    nyuryoku_tanto_c: i32,
    kado_bumon: &'a str,
    kingaku: i64,
    nebiki: i64,
    warimashi: i64,
    jippi: i64,
    yosha_kingaku: i64,
    yosha_nebiki: i64,
    yosha_warimashi: i64,
    yosha_jippi: i64,
    shain_r: &'a str,
    yoshasaki_c: &'a str,
}

impl<'a> From<&'a UriageRow> for RawRowOut<'a> {
    fn from(r: &'a UriageRow) -> Self {
        Self {
            unko_date: &r.unko_date,
            yokoyoko: r.yokoyoko,
            seikyu_k: r.seikyu_k,
            biko2: &r.biko2,
            nyuryoku_tanto_c: r.nyuryoku_tanto_c,
            kado_bumon: &r.kado_bumon,
            kingaku: r.kingaku,
            nebiki: r.nebiki,
            warimashi: r.warimashi,
            jippi: r.jippi,
            yosha_kingaku: r.yosha_kingaku,
            yosha_nebiki: r.yosha_nebiki,
            yosha_warimashi: r.yosha_warimashi,
            yosha_jippi: r.yosha_jippi,
            shain_r: &r.shain_r,
            yoshasaki_c: &r.yoshasaki_c,
        }
    }
}

/// raw rows を NDJSON (改行区切り JSON) bytes にシリアライズし、その bytes の sha256 hex を
/// fingerprint として返す。bytes は gzip 圧縮前の確定形 (row order = repo sort 順)。
pub(crate) fn serialize_ndjson_and_fingerprint(rows: &[UriageRow]) -> (Vec<u8>, String) {
    let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 200);
    for r in rows {
        let out = RawRowOut::from(r);
        // serde_json::to_writer は trailing \n を付けないので明示的に push する
        let _ = serde_json::to_writer(&mut buf, &out);
        buf.push(b'\n');
    }
    let mut hasher = Sha256::new();
    hasher.update(&buf);
    let fingerprint = format!("{:x}", hasher.finalize());
    (buf, fingerprint)
}

/// NDJSON bytes を gzip 圧縮して指定パスに書き出す (親 dir auto-create)。
pub(crate) fn write_raw_ndjson_gz(
    raw_dir: &str,
    month: &str,
    eigyosho_id: i64,
    ndjson: &[u8],
) -> std::io::Result<String> {
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use std::io::Write;

    let dir = std::path::Path::new(raw_dir).join(month);
    std::fs::create_dir_all(&dir)?;
    let path = dir.join(format!("eigyosho-{}.ndjson.gz", eigyosho_id));

    // tmp 経由 atomic rename (PathModified watcher 等への中途半端な書き込み防止)
    let tmp_path = path.with_extension("ndjson.gz.tmp");
    {
        let f = std::fs::File::create(&tmp_path)?;
        let mut enc = GzEncoder::new(f, Compression::default());
        enc.write_all(ndjson)?;
        enc.finish()?;
    }
    std::fs::rename(&tmp_path, &path)?;
    Ok(path.to_string_lossy().into_owned())
}

/// 1 (month, eigyosho_id) 単位の recalc を実行する。
///
/// 戻り値の `RecalcJobResult.status` は `"computed"` / `"failed"` のいずれか。
/// failed の場合は SQLite recalc_jobs にも `status='failed' + last_error` で記録する。
#[allow(clippy::too_many_arguments)]
async fn recalc_one(
    repo: &DynRepo,
    store: &DynLocalStore,
    raw_dir: &str,
    month: &str,
    eigyosho_id: i64,
    masters: &OfficeMasters,
    from: &str,
    to: &str,
    calculated_at: &str,
) -> RecalcJobResult {
    let mut job = RecalcJobResult {
        month: month.to_string(),
        eigyosho_id,
        status: "computed".to_string(),
        ..Default::default()
    };

    // bumon が空 (= マスタ未設定) は skip しても compute は走らせない方が安全。
    // CakePHP /masters-json の bumon は PR #766 以降の仕様で空配列にはならないはずだが、
    // 未対応の旧 CakePHP が混ざった場合の防御。
    if masters.bumon.is_empty() {
        job.status = "failed".to_string();
        job.error = Some("masters.bumon is empty (CakePHP 旧仕様?)".to_string());
        let _ = store
            .record_recalc_failed(
                month,
                eigyosho_id,
                job.error.as_deref().unwrap_or(""),
                calculated_at,
            )
            .await;
        return job;
    }

    // SQL Server から運転日報明細を取得
    let rows = match repo.uriage_rows(from, to, &masters.bumon).await {
        Ok(r) => r,
        Err(e) => {
            let msg = format!("sqlserver: {e:?}");
            job.status = "failed".to_string();
            job.error = Some(msg.clone());
            let _ = store
                .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
                .await;
            return job;
        }
    };
    job.row_count = rows.len();

    let persons = masters.persons_as_int_map();
    let other = masters.other.clone();

    // raw fingerprint (cal は raw rows には影響しないので 1 回でよい)
    let (ndjson, fingerprint) = serialize_ndjson_and_fingerprint(&rows);

    // raw NDJSON.gz を disk に出す (失敗しても recalc 本体は続行、raw_path = None)
    let raw_path = match write_raw_ndjson_gz(raw_dir, month, eigyosho_id, &ndjson) {
        Ok(p) => {
            job.raw_path = Some(p.clone());
            Some(p)
        }
        Err(e) => {
            tracing::warn!(
                "raw NDJSON.gz 書き出しに失敗 (recalc は続行): month={month} eigyosho={eigyosho_id} err={e}"
            );
            None
        }
    };

    // cal=true / cal=false それぞれで日次集計を計算 (月次は VIEW なので不要)
    let daily_cal = compute_person_sum_by_day(&rows, &persons, &other, true);
    let daily_nocal = compute_person_sum_by_day(&rows, &persons, &other, false);

    // 日次集計 upsert (失敗は recalc 全体を failed にする)
    match store
        .upsert_person_daily(month, eigyosho_id, true, &daily_cal, calculated_at)
        .await
    {
        Ok(n) => job.daily_count_cal = n,
        Err(e) => {
            let msg = format!("sqlite upsert_daily(cal=true): {e}");
            job.status = "failed".to_string();
            job.error = Some(msg.clone());
            let _ = store
                .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
                .await;
            return job;
        }
    }
    match store
        .upsert_person_daily(month, eigyosho_id, false, &daily_nocal, calculated_at)
        .await
    {
        Ok(n) => job.daily_count_nocal = n,
        Err(e) => {
            let msg = format!("sqlite upsert_daily(cal=false): {e}");
            job.status = "failed".to_string();
            job.error = Some(msg.clone());
            let _ = store
                .record_recalc_failed(month, eigyosho_id, &msg, calculated_at)
                .await;
            return job;
        }
    }

    // recalc_jobs に記録 (fingerprint + raw_path + status='computed')
    if let Err(e) = store
        .record_recalc_computed(
            month,
            eigyosho_id,
            &fingerprint,
            raw_path.as_deref(),
            calculated_at,
        )
        .await
    {
        // 記録だけ落ちた場合は status は computed のまま (集計は成功している) だが
        // error フィールドで知らせる
        job.error = Some(format!("record_recalc_computed: {e}"));
    }
    job.fingerprint = Some(fingerprint);
    job
}

/// POST `/api/uriage/recalc?month=YYYY-MM&eigyosho_id=N`
///
/// CakePHP から masters + editable_months を pull し、対象 (month, eigyosho_id) について
/// SQL Server から運転日報明細を取得 → compute_person_sum (cal={true,false}) → SQLite
/// upsert + raw NDJSON.gz 出力 + recalc_jobs に fingerprint 記録、を行う。
///
/// **body は受け取らない** (persons/other/bumon は CakePHP から pull)。
///
/// - `month` 未指定: editable_months 全部 (CakePHP の `editable_months` 配列) を処理
/// - `month` 指定: 任意の過去月を受け付ける (SELECT は無制限。経理の `入力可能月数`
///   とは無関係、log だけ残す)
/// - `eigyosho_id` 指定なし: その月の全営業所 (masters.offices) を処理
///
/// 設計判断 (user, 2026-06-30): **入力可能月 ≠ 取得可能月**。CakePHP `基本事項.入力可能月数`
/// は経理 UI の編集制限で、rust 側の再集計 (= 純粋な SELECT) には関係ない。チャート伸長
/// などで過去月の集計を取り直す要件が当然ありうるため、editable_months 422 gate は撤廃
/// (Refs yhonda-ohishi/nginx#762)。`editable_months` はログ用に残す (= CakePHP 側の編集
/// 状態は引き続き参考情報になる)。
pub async fn recalc(
    Extension(repo): Extension<DynRepo>,
    Extension(store): Extension<DynLocalStore>,
    Extension(cakephp): Extension<Arc<CakephpClient>>,
    Extension(raw_config): Extension<Arc<RawConfig>>,
    Query(q): Query<RecalcQuery>,
) -> Result<Json<RecalcResponse>, (StatusCode, String)> {
    // CakePHP 未配線なら 503 (= base_url 空)
    if !cakephp.is_enabled() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "CakePHP base_url 未設定: /recalc は無効".to_string(),
        ));
    }

    // editable_months を取得 (参考情報として log 出力。recalc 自体は gate しない)
    let editable = cakephp
        .fetch_editable_months()
        .await
        .map_err(map_cakephp_err)?;

    // 処理対象月を決める
    let target_months: Vec<String> = match &q.month {
        Some(m) => {
            if !editable.editable_months.iter().any(|em| em == m) {
                tracing::info!(
                    month = %m,
                    editable_months = ?editable.editable_months,
                    "month は CakePHP 編集可能月の外だが、recalc は実行 (入力可能月 ≠ 取得可能月)"
                );
            }
            vec![m.clone()]
        }
        None => editable.editable_months.clone(),
    };

    let calculated_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mut jobs: Vec<RecalcJobResult> = Vec::new();

    for month in &target_months {
        let (from, to, masters_date) = match month_to_range(month) {
            Some(t) => t,
            None => {
                jobs.push(RecalcJobResult {
                    month: month.clone(),
                    status: "failed".to_string(),
                    error: Some(format!("month の形式が不正: {month}")),
                    ..Default::default()
                });
                continue;
            }
        };

        // masters を「月末日」基準で取得 (人事異動等は月跨ぎでは想定外)
        let masters = match cakephp.fetch_masters(&masters_date).await {
            Ok(m) => m,
            Err(e) => {
                jobs.push(RecalcJobResult {
                    month: month.clone(),
                    status: "failed".to_string(),
                    error: Some(format!("CakePHP masters fetch: {e}")),
                    ..Default::default()
                });
                continue;
            }
        };

        let targets: Vec<(i64, &OfficeMasters)> = if let Some(eid) = q.eigyosho_id {
            // 単一営業所指定: masters に居なければ skip
            match office_lookup(&masters, eid) {
                Some(o) => vec![(eid, o)],
                None => {
                    jobs.push(RecalcJobResult {
                        month: month.clone(),
                        eigyosho_id: eid,
                        status: "skipped".to_string(),
                        error: Some(format!("eigyosho_id={eid} が masters に存在しない")),
                        ..Default::default()
                    });
                    continue;
                }
            }
        } else {
            // 全営業所: masters.offices の全エントリ
            let mut v: Vec<(i64, &OfficeMasters)> = masters
                .offices
                .iter()
                .filter_map(|(k, v)| k.parse::<i64>().ok().map(|i| (i, v)))
                .collect();
            // 安定順 (id 昇順) で処理
            v.sort_by_key(|(id, _)| *id);
            v
        };

        for (eid, om) in targets {
            let job = recalc_one(
                &repo,
                &store,
                &raw_config.dir,
                month,
                eid,
                om,
                &from,
                &to,
                &calculated_at,
            )
            .await;
            jobs.push(job);
        }
    }

    Ok(Json(RecalcResponse {
        source_table: "運転日報明細 + CakePHP masters → uriage_person_monthly + recalc_jobs"
            .to_string(),
        months: target_months,
        editable_months_count: editable.editable_months_count,
        calculated_at,
        jobs,
    }))
}

/// `MastersResponse.offices` (HashMap<String, _>) から i64 key で OfficeMasters を引く。
fn office_lookup(masters: &MastersResponse, eigyosho_id: i64) -> Option<&OfficeMasters> {
    masters.offices.get(&eigyosho_id.to_string())
}

// ══════════════════════════════════════════════════════════════
// R2 sync endpoints
// ══════════════════════════════════════════════════════════════

/// GET `/api/uriage/r2/pending`
///
/// `r2_pending` view を返す (fingerprint 変化があったが R2 未送信の (month, eigyosho_id))。
/// nuxt cron が叩いて、結果の各 entry の `/raw/:month/:eigyosho_id` を fetch → R2 put →
/// `/raw/:month/:eigyosho_id/ack` を順に叩く。
pub async fn r2_pending(
    Extension(store): Extension<DynLocalStore>,
) -> Result<Json<R2PendingResponse>, StatusCode> {
    let rows = store.list_r2_pending().await.map_err(map_local_store_err)?;
    Ok(Json(R2PendingResponse {
        count: rows.len(),
        items: rows,
    }))
}

#[derive(Debug, Serialize)]
pub struct R2PendingResponse {
    pub count: usize,
    pub items: Vec<crate::sqlite::R2PendingRow>,
}

/// GET `/api/uriage/raw/:month/:eigyosho_id`
///
/// recalc が disk に書き出した raw NDJSON.gz の bytes を返す (`Content-Type: application/gzip`)。
/// path は SQLite `recalc_jobs.raw_path` から引く (URL 直接 path traversal 不可)。
pub async fn raw_get(
    Extension(store): Extension<DynLocalStore>,
    Path((month, eigyosho_id)): Path<(String, i64)>,
) -> Result<Response, StatusCode> {
    let job = store
        .get_recalc_job(&month, eigyosho_id)
        .await
        .map_err(map_local_store_err)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let path = job.raw_path.ok_or(StatusCode::NOT_FOUND)?;

    let bytes = tokio::fs::read(&path).await.map_err(|e| {
        tracing::error!("raw read failed: path={path} err={e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;

    let mut resp = bytes.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/gzip"),
    );
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        header::HeaderValue::from_str(&format!(
            "attachment; filename=\"{}-eigyosho-{}.ndjson.gz\"",
            month, eigyosho_id
        ))
        .unwrap_or_else(|_| header::HeaderValue::from_static("attachment")),
    );
    Ok(resp)
}

/// POST `/api/uriage/raw/:month/:eigyosho_id/ack`
///
/// R2 への送信完了を記録する (`recalc_jobs.status='r2_synced' + r2_synced_at=now`)。
/// 該当 job が computed 状態に無い場合は 404 (ack 対象が無い)。
pub async fn raw_ack(
    Extension(store): Extension<DynLocalStore>,
    Path((month, eigyosho_id)): Path<(String, i64)>,
) -> Result<Json<RawAckResponse>, StatusCode> {
    let synced_at = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let n = store
        .record_r2_synced(&month, eigyosho_id, &synced_at)
        .await
        .map_err(map_local_store_err)?;
    if n == 0 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(RawAckResponse {
        month,
        eigyosho_id,
        synced_at,
    }))
}

#[derive(Debug, Serialize)]
pub struct RawAckResponse {
    pub month: String,
    pub eigyosho_id: i64,
    pub synced_at: String,
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/daily?month=YYYY-MM&eigyosho_id=N&cal=true|false
// ══════════════════════════════════════════════════════════════

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
    pub rows: Vec<crate::sqlite::PersonDailyRow>,
}

/// GET `/api/uriage/daily?month=YYYY-MM&eigyosho_id=N&cal=true`
///
/// `(month, eigyosho_id, cal)` の **日次集計** を SQLite から読んで返す。
/// recalc 後でなければ空配列が返る。drill-down 表示用。
pub async fn daily(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<DailyQuery>,
) -> Result<Json<DailyResponse>, StatusCode> {
    if q.month.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rows = store
        .get_person_daily(&q.month, q.eigyosho_id, q.cal)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(DailyResponse {
        month: q.month,
        eigyosho_id: q.eigyosho_id,
        cal: q.cal,
        rows,
    }))
}

// ══════════════════════════════════════════════════════════════
// HTTP handler: GET /api/uriage/person-monthly-totals?from=YYYY-MM&to=YYYY-MM&cal=true
// ══════════════════════════════════════════════════════════════

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
    pub rows: Vec<crate::sqlite::PersonMonthlyRow>,
}

/// GET `/api/uriage/person-monthly-totals?from=YYYY-MM&to=YYYY-MM&cal=true`
///
/// 期間内の **月 × 担当者 の SUM** を返す (全営業所合算)。
/// 担当者順位推移チャート (bump chart) 用。
pub async fn person_monthly_totals(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<PersonMonthlyTotalsQuery>,
) -> Result<Json<PersonMonthlyTotalsResponse>, StatusCode> {
    if q.from.is_empty() || q.to.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let rows = store
        .person_monthly_totals(&q.from, &q.to, q.cal)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(PersonMonthlyTotalsResponse {
        from: q.from,
        to: q.to,
        cal: q.cal,
        rows,
    }))
}

// ══════════════════════════════════════════════════════════════
// Admin: 削除 / 再作成
// ══════════════════════════════════════════════════════════════

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

/// POST `/api/uriage/admin/delete?month=YYYY-MM&eigyosho_id=N`
///
/// 指定 `(month, eigyosho_id)` の集計を SQLite 上の全 cal で削除し、`recalc_jobs`
/// の該当行も消す (一部リセット用)。月次集計は日次の VIEW なので自動で連動消滅。
/// 再度 `/recalc` を叩けば fingerprint なしで fresh に再計算 → R2 にも再 sync
/// 対象として並ぶ。
pub async fn admin_delete(
    Extension(store): Extension<DynLocalStore>,
    Query(q): Query<AdminDeleteQuery>,
) -> Result<Json<AdminDeleteResponse>, StatusCode> {
    if q.month.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let r = store
        .delete_bucket(&q.month, q.eigyosho_id)
        .await
        .map_err(map_local_store_err)?;
    Ok(Json(AdminDeleteResponse {
        month: q.month,
        eigyosho_id: q.eigyosho_id,
        daily_deleted: r.daily_deleted,
        jobs_deleted: r.jobs_deleted,
    }))
}

#[derive(Debug, Serialize)]
pub struct AdminRebuildResponse {
    pub rebuilt_at: String,
}

/// POST `/api/uriage/admin/rebuild`
///
/// SQLite の全 uriage table (monthly / daily / recalc_jobs / r2_pending view) を
/// DROP → 再 migrate で作り直す **フルリセット**。集計データは全て消える。
/// schema 不整合や fingerprint の全更新が必要な時の最終手段。
pub async fn admin_rebuild(
    Extension(store): Extension<DynLocalStore>,
) -> Result<Json<AdminRebuildResponse>, StatusCode> {
    store.rebuild_schema().await.map_err(map_local_store_err)?;
    Ok(Json(AdminRebuildResponse {
        rebuilt_at: chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
    }))
}

/// `preg_replace("/売上　|売上\s/", "", $str)` の写経。
///
/// "売上" の直後が **全角スペース U+3000 または ASCII 系空白**
/// (` `, `\t`, `\n`, `\r`, `\x0c`, `\x0b`) のときだけ "売上" + その空白 1 文字を
/// 削る。**他の Unicode 空白 (U+00A0 等) は対象外** (PHP の `\s` は ASCII 空白限定、
/// パターン側に `/u` 修飾子も無いため)。
pub(crate) fn strip_uriage_prefix(s: &str) -> String {
    const URIAGE: &str = "売上";
    let mut result = String::with_capacity(s.len());
    let mut rest = s;
    while !rest.is_empty() {
        if let Some(after) = rest.strip_prefix(URIAGE) {
            if let Some(first) = after.chars().next() {
                if is_php_ascii_whitespace_or_ideographic(first) {
                    // "売上" + 空白 1 文字を削って続行
                    rest = &after[first.len_utf8()..];
                    continue;
                }
            }
        }
        // 1 文字進める
        let c = rest.chars().next().unwrap();
        result.push(c);
        rest = &rest[c.len_utf8()..];
    }
    result
}

/// `preg_replace("/売上/", "", preg_replace("/　|\s+/", "", $str))` の写経。
///
/// 全空白 (全角 + ASCII 系) を除去してから "売上" を **全箇所** 除去する。
/// 担当者マスタへの完全一致 lookup 用キー生成。
pub(crate) fn normalize_biko_for_lookup(s: &str) -> String {
    let no_ws: String = s
        .chars()
        .filter(|c| !is_php_ascii_whitespace_or_ideographic(*c))
        .collect();
    no_ws.replace("売上", "")
}

/// PHP の `[ \t\n\r\x0b\x0c]` (= ASCII whitespace、`\s` without `/u` 修飾子) と
/// 全角スペース `　` (U+3000) のみ true。それ以外の Unicode 空白 (NBSP 等) は false。
fn is_php_ascii_whitespace_or_ideographic(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\x0c' | '\x0b' | '\u{3000}')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_uriage_prefix_removes_uriage_with_fullwidth_space() {
        assert_eq!(strip_uriage_prefix("売上\u{3000}山﨑"), "山﨑");
    }

    #[test]
    fn strip_uriage_prefix_removes_uriage_with_ascii_space() {
        assert_eq!(strip_uriage_prefix("売上 山﨑"), "山﨑");
    }

    #[test]
    fn strip_uriage_prefix_keeps_uriage_without_trailing_whitespace() {
        // PHP: preg_replace("/売上　|売上\s/", "", "売上") → "売上" (置換無し)
        assert_eq!(strip_uriage_prefix("売上"), "売上");
    }

    #[test]
    fn strip_uriage_prefix_keeps_uriage_followed_by_kanji() {
        // 元 PHP は "売上山﨑" の場合は 売上 を残す (preg は売上 + 空白の連結を要求)
        assert_eq!(strip_uriage_prefix("売上山﨑"), "売上山﨑");
    }

    #[test]
    fn normalize_biko_strips_uriage_and_whitespace() {
        assert_eq!(normalize_biko_for_lookup("売上\u{3000}山﨑智"), "山﨑智");
        assert_eq!(normalize_biko_for_lookup("売上 山﨑智"), "山﨑智");
        assert_eq!(normalize_biko_for_lookup("\u{3000}青井"), "青井");
        assert_eq!(normalize_biko_for_lookup(""), "");
    }

    #[test]
    fn arrow_re_splits_greedy() {
        // 単純な A→B
        let caps = ARROW_RE.captures("山﨑→青井").unwrap();
        assert_eq!(&caps[1], "山﨑");
        assert_eq!(&caps[2], "青井");

        // A→B→C は greedy で A="A→B", B="C"
        let caps = ARROW_RE.captures("山﨑智→山﨑→誰か").unwrap();
        assert_eq!(&caps[1], "山﨑智→山﨑");
        assert_eq!(&caps[2], "誰か");

        // → が無いときは None
        assert!(ARROW_RE.captures("売上").is_none());
    }

    #[test]
    fn cal_total_basic_formula() {
        let row = UriageRow {
            kingaku: 10000,
            warimashi: 500,
            nebiki: 100,
            jippi: 200,
            yosha_kingaku: 8000,
            yosha_warimashi: 300,
            yosha_nebiki: 50,
            yosha_jippi: 150,
            ..UriageRow::default()
        };
        let t = cal_total(&row);
        // total_ex_jippi = 10000 + 500 - 100 = 10400
        assert_eq!(t.total_ex_jippi, 10400);
        // total = 10400 + 200 = 10600
        assert_eq!(t.total, 10600);
        // ytotal_ex_jippi = 8000 + 300 - 50 = 8250
        assert_eq!(t.ytotal_ex_jippi, 8250);
        // ytotal = 8250 + 150 = 8400
        assert_eq!(t.ytotal, 8400);
        // shiharai = 8000 + 300 - 50 + 150 = 8400
        assert_eq!(t.shiharai, 8400);
    }

    #[test]
    fn cal_total_all_zero() {
        let t = cal_total(&UriageRow::default());
        assert_eq!(t, CalTotal::default());
    }

    #[test]
    fn is_yosha_six_zeros_is_jisha() {
        assert!(!is_yosha("000000"));
    }

    #[test]
    fn is_yosha_non_zero_is_yosha() {
        assert!(is_yosha("021970"));
        assert!(is_yosha("999999"));
        // 5 桁 0 でも傭車先C != "000000" なので傭車扱い (PHP 文字列比較)
        assert!(is_yosha("00000"));
        assert!(is_yosha(""));
    }

    #[test]
    fn is_oth_yosha_compares_eigyosho_ids() {
        // 同一営業所 → false
        assert!(!is_oth_yosha("本社", "本社"));
        assert!(!is_oth_yosha("1", "1"));
        // 別営業所 → true
        assert!(is_oth_yosha("本社", "佐賀営業所"));
        assert!(is_oth_yosha("1", "8"));
    }

    // ──────────────────────────────────────────────────────────────────
    // PR-C2 / D helpers
    // ──────────────────────────────────────────────────────────────────

    #[test]
    fn month_to_range_jan() {
        let (from, to, date) = month_to_range("2026-01").unwrap();
        assert_eq!(from, "2026-01-01");
        assert_eq!(to, "2026-01-31");
        assert_eq!(date, "2026-01-31");
    }

    #[test]
    fn month_to_range_feb_handles_leap() {
        // 2024 はうるう年
        let (from, to, date) = month_to_range("2024-02").unwrap();
        assert_eq!(from, "2024-02-01");
        assert_eq!(to, "2024-02-29");
        assert_eq!(date, "2024-02-29");
        // 2026 は平年
        let (from, to, _) = month_to_range("2026-02").unwrap();
        assert_eq!(from, "2026-02-01");
        assert_eq!(to, "2026-02-28");
    }

    #[test]
    fn month_to_range_dec_year_rollover() {
        let (from, to, _) = month_to_range("2026-12").unwrap();
        assert_eq!(from, "2026-12-01");
        assert_eq!(to, "2026-12-31");
    }

    #[test]
    fn month_to_range_invalid_format_returns_none() {
        assert!(month_to_range("2026-1").is_none()); // 1 桁月
        assert!(month_to_range("26-01").is_none()); // 短い年
        assert!(month_to_range("2026/01").is_none()); // 区切り違い
        assert!(month_to_range("2026-13").is_none()); // 月範囲外
        assert!(month_to_range("2026-00").is_none()); // 0 月
        assert!(month_to_range("").is_none());
        assert!(month_to_range("abcd-ef").is_none());
    }

    #[test]
    fn serialize_ndjson_empty_rows() {
        let (bytes, fp) = serialize_ndjson_and_fingerprint(&[]);
        assert!(bytes.is_empty());
        // 空 bytes でも sha256 は固定値
        // sha256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assert_eq!(
            fp,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn serialize_ndjson_deterministic_for_same_rows() {
        let rows = vec![
            UriageRow {
                kingaku: 1000,
                biko2: "売上 山﨑".to_string(),
                ..UriageRow::default()
            },
            UriageRow {
                kingaku: 2000,
                biko2: "売上 青井".to_string(),
                ..UriageRow::default()
            },
        ];
        let (b1, fp1) = serialize_ndjson_and_fingerprint(&rows);
        let (b2, fp2) = serialize_ndjson_and_fingerprint(&rows);
        assert_eq!(b1, b2);
        assert_eq!(fp1, fp2);
        // 2 行 → 改行で分かれている
        assert_eq!(b1.iter().filter(|&&c| c == b'\n').count(), 2);
    }

    #[test]
    fn serialize_ndjson_changes_when_row_changes() {
        let r1 = vec![UriageRow {
            kingaku: 1000,
            ..UriageRow::default()
        }];
        let r2 = vec![UriageRow {
            kingaku: 1001,
            ..UriageRow::default()
        }];
        let (_, fp1) = serialize_ndjson_and_fingerprint(&r1);
        let (_, fp2) = serialize_ndjson_and_fingerprint(&r2);
        assert_ne!(fp1, fp2);
    }

    #[test]
    fn write_raw_ndjson_gz_creates_file_and_can_be_decoded() {
        use flate2::read::GzDecoder;
        use std::io::Read;

        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-raw-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let raw_dir = tmp.to_string_lossy().into_owned();

        let payload = b"line1\nline2\n";
        let path = write_raw_ndjson_gz(&raw_dir, "2026-06", 9, payload).unwrap();
        assert!(path.contains("2026-06"));
        assert!(path.ends_with("eigyosho-9.ndjson.gz"));
        assert!(std::path::Path::new(&path).exists());

        // gunzip して中身が一致するか
        let bytes = std::fs::read(&path).unwrap();
        let mut dec = GzDecoder::new(&bytes[..]);
        let mut out = Vec::new();
        dec.read_to_end(&mut out).unwrap();
        assert_eq!(out, payload);

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn write_raw_ndjson_gz_atomic_rename_leaves_no_tmp() {
        let tmp = std::env::temp_dir().join(format!(
            "ichibanboshi-raw-tmp-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&tmp);
        let raw_dir = tmp.to_string_lossy().into_owned();
        let path = write_raw_ndjson_gz(&raw_dir, "2026-06", 1, b"x").unwrap();
        // 同じ dir に .tmp は残らない
        let tmp_path = std::path::Path::new(&path).with_extension("ndjson.gz.tmp");
        assert!(!tmp_path.exists());
        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn office_lookup_parses_string_keys() {
        use crate::cakephp::OfficeMasters;
        let mut offices = std::collections::HashMap::new();
        offices.insert(
            "1".to_string(),
            OfficeMasters {
                display_name: "本社".to_string(),
                persons: HashMap::new(),
                other: HashMap::new(),
                bumon: vec!["010".to_string()],
            },
        );
        offices.insert(
            "9".to_string(),
            OfficeMasters {
                display_name: "宮崎".to_string(),
                persons: HashMap::new(),
                other: HashMap::new(),
                bumon: vec!["015".to_string()],
            },
        );
        let masters = MastersResponse {
            date: "2026-06-29".to_string(),
            offices,
        };
        assert_eq!(office_lookup(&masters, 1).unwrap().display_name, "本社");
        assert_eq!(office_lookup(&masters, 9).unwrap().display_name, "宮崎");
        assert!(office_lookup(&masters, 7).is_none());
    }
}
