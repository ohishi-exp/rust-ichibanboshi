//! 担当者別売上集計の純粋関数 (`/uriage-jyuchu-display/print` の Rust 移植、issue #762)。
//!
//! 元 PHP は `yhonda-ohishi/nginx`
//! `html/app/src/Controller/UriageJyuchuDisplayController::computePersonSum()`
//! (PR #764 で `templates/UriageJyuchuDisplay/print.php` 118-356 行から
//! 共有ヘルパへ抽出済み)。
//!
//! 本 module はその純粋関数を 1:1 で写経したもの。**挙動の完全再現が最優先**
//! のため、条件式・符号・正規表現の意味は一字一句変えない (リファクタ禁止)。
//! root の `src/routes/uriage.rs` から移した (Refs ohishi-exp/rust-ichibanboshi#322)。
//! テストは `workers/uriage/logic/tests/compute.rs` (PHP の 17 ケース + 全分岐網羅) と下の単体テスト。

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use regex::Regex;

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
    /// 売上年月日 (`YYYY-MM-DD`)。月計テーブル一致条件はこちら (`運行年月日` ではない)。
    /// verify_debug の debug 表示で「PHP `/print-json` が 売上年月日 でフィルタしている
    /// 場合、Rust の 運行年月日 フィルタとは pick する行が変わる」可能性を確認する用途。
    pub uriage_date: String,
    /// 得意先複合キー (`得意先C`+`-`+`得意先H`)。得意先ﾏｽﾀは `得意先C` 単体では
    /// 一意にならない (#57 実機調査確定、`unchin.rs` と同じ罠) ため複合キーで扱う。
    /// 担当者×得意先 内訳 (Refs 担当者ドリルダウン) 用。
    pub tokuisaki_key: String,
    /// 得意先名 (`得意先ﾏｽﾀ.得意先N`、TOP 1 スカラサブクエリで解決)。
    pub tokuisaki_n: String,
    /// 傭車先複合キー (`傭車先C`+`-`+`傭車先H`)。`yoshasaki_c` (自車/傭車判定専用、
    /// `"000000"` 比較のみに使う) とは別に持つ — 傭車先H を欠くと名寄せがズレる
    /// (#57 と同じ罠)。担当者×傭車先 内訳用。
    pub yoshasaki_key: String,
    /// 傭車先名 (`傭車先ﾏｽﾀ.傭車先N`)。
    pub yoshasaki_n: String,
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

/// y0 フィールド用の serde skip predicate。0 のとき wire format から省略する。
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

/// 担当者ごとの集計値。
///
/// `Deserialize` を持つのは `/verify` endpoint で PHP `print-json.sum` を
/// `HashMap<String, PersonAccum>` に parse するため。serde の field 名は
/// 日本語 (PHP 側 JSON キーと一致) なので Deserialize でもこの rename が効く。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    /// 横横=0 (= 自社運行 or sql_from_other) のみの 傭車金額累計。
    /// 「横横除外フィルタ」UI で kingaku_y0 / yosha_kingaku_y0 / kensuu_y0 を
    /// 表示すると、他社運行委託分 (横横=1) を除外した純粋な自社売上が見える。
    /// 旧 data には存在しないので `#[serde(default)]` で 0 fallback。
    /// 0 のときは `skip_serializing_if` で wire format から省略 (= PHP 互換 JSON を
    /// 維持し、verify diff 比較に y0 が混入しない)。
    #[serde(default, rename = "傭車金額_y0", skip_serializing_if = "is_zero_i64")]
    pub yosha_kingaku_y0: i64,
    /// 横横=0 のみの 金額累計。
    #[serde(default, rename = "金額_y0", skip_serializing_if = "is_zero_i64")]
    pub kingaku_y0: i64,
    /// 横横=0 のみの 件数。
    #[serde(default, rename = "件数_y0", skip_serializing_if = "is_zero_i64")]
    pub kensuu_y0: i64,
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
    /// この行が `sum` (担当者別集計) に実際に積算されたかどうか。
    ///
    /// `tanto` は「表示用の担当者名」であって積算有無とは独立 (B4/B6 は tanto が
    /// 埋まっても集計しないケースがある)。このフラグは既存の `sum` 計算を一切
    /// 変えず、`accumulate()` を呼んだかどうかをそのまま記録するだけの追加情報
    /// (Refs 担当者×得意先/傭車先 ドリルダウン)。`compute_person_partner_sum_by_day`
    /// が「担当者集計と同じ行だけ」を得意先/傭車先で再集計する際に使う。
    pub aggregated: bool,
    /// この行の得意先側加算額 (`金額+値引+割増+実費`、`accumulate()` に渡した
    /// `kingaku_sum` と同値)。`compute_person_sum` の外へ formula を複製せず、
    /// 唯一の計算元から得意先内訳を再集計できるようにするための追加情報。
    pub kingaku_sum: i64,
    /// この行の傭車先側加算額 (`accumulate()` に渡した `yosha_sum` と同値。
    /// 横横=1 の行は「金額側」がここにコピーされる点に注意、`compute_person_sum`
    /// のコメント参照)。
    pub yosha_sum: i64,
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
                aggregated: false,
                kingaku_sum: 0,
                yosha_sum: 0,
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
        // `accumulate()` を実際に呼んだかどうか (aggregated フラグ用、sum 計算には無関係)。
        let mut aggregated = false;

        // print.php:284-355 — 6 段階 if-elseif
        if let Some(fr) = uriage_fr_name
            .as_deref()
            .filter(|s| person_names.contains(s))
        {
            // B1: A→B のうち A (Fr) がマスタに居る
            tanto.push_str(fr);
            if should_aggregate {
                accumulate(&mut sum, fr, kingaku_sum, yosha_sum, ichi.yokoyoko);
                aggregated = true;
            }
        } else if let Some(to) = uriage_to_name
            .as_deref()
            .filter(|s| person_names.contains(s))
        {
            // B2: A→B のうち B (To) がマスタに居る
            tanto.push_str(to);
            if should_aggregate {
                accumulate(&mut sum, to, kingaku_sum, yosha_sum, ichi.yokoyoko);
                aggregated = true;
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
                accumulate(&mut sum, &lookup_key, kingaku_sum, yosha_sum, ichi.yokoyoko);
                aggregated = true;
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
                accumulate(&mut sum, &name, kingaku_sum, yosha_sum, ichi.yokoyoko);
                aggregated = true;
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
            aggregated,
            kingaku_sum,
            yosha_sum,
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
        // 合計のいずれか or y0 のいずれかが非ゼロなら保持
        // (横横除外 view で初めて非ゼロになる行は無いが、概念上の整合性のため
        //  両方チェックする)
        let non_zero: HashMap<String, PersonAccum> = res
            .sum
            .into_iter()
            .filter(|(_, v)| {
                v.kensuu != 0
                    || v.kingaku != 0
                    || v.yosha_kingaku != 0
                    || v.kensuu_y0 != 0
                    || v.kingaku_y0 != 0
                    || v.yosha_kingaku_y0 != 0
            })
            .collect();
        if !non_zero.is_empty() {
            out.insert(date, non_zero);
        }
    }
    out
}

/// 得意先 or 傭車先の種別。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PartnerKind {
    Customer,
    Subcontractor,
}

impl PartnerKind {
    /// SQLite `partner_kind` カラム / HTTP query param の文字列表現。
    /// Worker 側 `workers/ichiban/logic/src/unchin.rs` の `PartnerType` と同じ語彙 (`"customer"`/`"subcontractor"`)。
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Customer => "customer",
            Self::Subcontractor => "subcontractor",
        }
    }
}

/// 担当者×得意先/傭車先 1 キーぶんの集計値。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct PartnerAccum {
    /// 得意先名 or 傭車先名 (`得意先ﾏｽﾀ.得意先N` / `傭車先ﾏｽﾀ.傭車先N`)。
    pub partner_name: String,
    pub kingaku: i64,
    pub kensuu: i64,
    /// 横横=0 のみの金額累計 (横横除外フィルタ用、`PersonAccum.kingaku_y0` と同じ考え方)。
    pub kingaku_y0: i64,
    pub kensuu_y0: i64,
}

/// `(担当者名, 得意先/傭車先種別, 取引先複合キー)`。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PersonPartnerKey {
    pub person_name: String,
    pub partner_kind: PartnerKind,
    /// 得意先複合キー (`得意先C-得意先H`) or 傭車先複合キー (`傭車先C-傭車先H`)。
    pub partner_code: String,
}

/// 日次集計版・担当者×得意先/傭車先 内訳 (`compute_person_sum` の後段レイヤ)。
///
/// **`compute_person_sum` の 6 段階 if-elseif には一切触れない** (リファクタ禁止
/// 制約を遵守)。代わりに `compute_person_sum` が確定させた `RowDecision`
/// (`aggregated`/`tanto`/`kingaku_sum`/`yosha_sum`) をそのまま再利用し、
/// 「担当者集計に実際に積算された行」だけをさらに得意先/傭車先で細分する。
///
/// - 得意先内訳は `kingaku_sum` (`PersonAccum.kingaku` と同じ加算元) を積む
/// - 傭車先内訳は `yosha_sum` (`PersonAccum.yosha_kingaku` と同じ加算元、横横=1 の
///   行では金額側がコピーされている点に注意) を積む
///
/// これにより各内訳の SUM は必ず対応する `PersonAccum` の値と一致する
/// (`tests/uriage_test.rs` の consistency テストで担保)。
///
/// 返り値は `unko_date` → `(担当者, 種別, 取引先複合キー)` → `PartnerAccum`。
pub fn compute_person_partner_sum_by_day(
    rows: &[UriageRow],
    persons: &HashMap<i32, String>,
    other: &HashMap<String, String>,
    cal: bool,
) -> HashMap<String, HashMap<PersonPartnerKey, PartnerAccum>> {
    let mut by_day: HashMap<String, Vec<UriageRow>> = HashMap::new();
    for r in rows {
        by_day
            .entry(r.unko_date.clone())
            .or_default()
            .push(r.clone());
    }

    let mut out: HashMap<String, HashMap<PersonPartnerKey, PartnerAccum>> = HashMap::new();
    for (date, day_rows) in by_day {
        // compute_person_sum を再実行して RowDecision (aggregated/tanto/各 sum) を得る。
        // compute_person_sum_by_day と同様に日単位で呼ぶ (cal 判定が日内で閉じるため)。
        let result = compute_person_sum(&day_rows, persons, other, cal);
        let mut per_day: HashMap<PersonPartnerKey, PartnerAccum> = HashMap::new();

        for (row, decision) in day_rows.iter().zip(result.rows.iter()) {
            if !decision.aggregated {
                continue;
            }
            accumulate_partner(
                &mut per_day,
                &decision.tanto,
                PartnerKind::Customer,
                &row.tokuisaki_key,
                &row.tokuisaki_n,
                decision.kingaku_sum,
                row.yokoyoko,
            );
            accumulate_partner(
                &mut per_day,
                &decision.tanto,
                PartnerKind::Subcontractor,
                &row.yoshasaki_key,
                &row.yoshasaki_n,
                decision.yosha_sum,
                row.yokoyoko,
            );
        }

        let non_zero: HashMap<PersonPartnerKey, PartnerAccum> = per_day
            .into_iter()
            .filter(|(_, v)| {
                v.kensuu != 0 || v.kingaku != 0 || v.kensuu_y0 != 0 || v.kingaku_y0 != 0
            })
            .collect();
        if !non_zero.is_empty() {
            out.insert(date, non_zero);
        }
    }
    out
}

/// `compute_person_partner_sum_by_day` の集計加算 (`accumulate()` の
/// (担当者, 取引先) 版)。
fn accumulate_partner(
    sum: &mut HashMap<PersonPartnerKey, PartnerAccum>,
    person_name: &str,
    partner_kind: PartnerKind,
    partner_code: &str,
    partner_name: &str,
    amount: i64,
    yokoyoko: i32,
) {
    let key = PersonPartnerKey {
        person_name: person_name.to_string(),
        partner_kind,
        partner_code: partner_code.to_string(),
    };
    let acc = sum.entry(key).or_insert_with(|| PartnerAccum {
        partner_name: partner_name.to_string(),
        ..Default::default()
    });
    acc.kingaku += amount;
    acc.kensuu += 1;
    if yokoyoko == 0 {
        acc.kingaku_y0 += amount;
        acc.kensuu_y0 += 1;
    }
}

/// 集計加算 (`$sum[$name]['金額'] += k_sum` 相当)。
///
/// `yokoyoko=0` (= 自社運行 or sql_from_other) の行は `_y0` 系にも同時加算する。
/// これにより 1 行で 合計値 (PHP 互換) と 横横除外値 の両方を保持できる。
fn accumulate(
    sum: &mut HashMap<String, PersonAccum>,
    name: &str,
    k_sum: i64,
    y_sum: i64,
    yokoyoko: i32,
) {
    let acc = sum.entry(name.to_string()).or_default();
    acc.kingaku += k_sum;
    acc.yosha_kingaku += y_sum;
    acc.kensuu += 1;
    if yokoyoko == 0 {
        acc.kingaku_y0 += k_sum;
        acc.yosha_kingaku_y0 += y_sum;
        acc.kensuu_y0 += 1;
    }
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

/// `preg_replace("/売上　|売上\s/", "", $str)` の写経。
///
/// "売上" の直後が **全角スペース U+3000 または ASCII 系空白**
/// (` `, `\t`, `\n`, `\r`, `\x0c`, `\x0b`) のときだけ "売上" + その空白 1 文字を
/// 削る。**他の Unicode 空白 (U+00A0 等) は対象外** (PHP の `\s` は ASCII 空白限定、
/// パターン側に `/u` 修飾子も無いため)。
pub fn strip_uriage_prefix(s: &str) -> String {
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
pub fn normalize_biko_for_lookup(s: &str) -> String {
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
}
