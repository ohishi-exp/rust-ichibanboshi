//! `/api/uriage/verify`・`verify-debug` の純粋部分 — PHP `print-json.sum` の読み方と、PHP と Rust の `$sum` の差分
//! (root の `src/routes/uriage.rs` から移した。Refs ohishi-exp/rust-ichibanboshi#322)。

use serde::Serialize;
use std::collections::HashMap;

use crate::compute::PersonAccum;

/// PHP `$sum` と Rust 集計の差分。
///
/// `php_only`: PHP にあって rust と値が違う、または rust に無い entry
/// `rust_only`: 逆
#[derive(Debug, Serialize)]
pub struct VerifyDiff {
    pub php_only: HashMap<String, PersonAccum>,
    pub rust_only: HashMap<String, PersonAccum>,
}

/// `PrintJsonResponse.sum` (= `serde_json::Value`) → `HashMap<String, PersonAccum>` 変換。
/// 空 object / null は空 map を返す (PHP 側で対象データが無い日)。
///
/// 誤りは応答の本文になる文言だけを返す (status は呼び手が決める。オンプレ版も Worker も 502)。
pub fn parse_php_sum(v: &serde_json::Value) -> Result<HashMap<String, PersonAccum>, String> {
    if v.is_null() {
        return Ok(HashMap::new());
    }
    serde_json::from_value::<HashMap<String, PersonAccum>>(v.clone())
        .map_err(|e| format!("PHP print-json sum parse failed: {e}"))
}

/// shell の jq logic を写経した diff 計算:
/// 1. 両側で「金額/傭車金額/件数 が全て 0」の entry を除外
/// 2. キー集合と各値が一致するか比較し、不一致なら php_only / rust_only に振り分け
/// 3. 完全一致なら `None`
pub fn compute_verify_diff(
    php: &HashMap<String, PersonAccum>,
    rust: &HashMap<String, PersonAccum>,
) -> Option<VerifyDiff> {
    fn is_nonzero(a: &PersonAccum) -> bool {
        a.kingaku != 0 || a.yosha_kingaku != 0 || a.kensuu != 0
    }
    // PHP は `kingaku` / `yosha_kingaku` / `kensuu` の 3 フィールドしか返さないため、
    // diff 比較も main 3 フィールドだけで行う。Rust 側の `kingaku_y0` 等は PHP 互換性に
    // 関係しないため diff から除外する (= y0 値を 0 に潰してから比較する)。
    fn main_only(a: &PersonAccum) -> PersonAccum {
        PersonAccum {
            kingaku: a.kingaku,
            yosha_kingaku: a.yosha_kingaku,
            kensuu: a.kensuu,
            ..Default::default()
        }
    }

    let php_nz: HashMap<String, PersonAccum> = php
        .iter()
        .filter(|(_, v)| is_nonzero(v))
        .map(|(k, v)| (k.clone(), main_only(v)))
        .collect();
    let rust_nz: HashMap<String, PersonAccum> = rust
        .iter()
        .filter(|(_, v)| is_nonzero(v))
        .map(|(k, v)| (k.clone(), main_only(v)))
        .collect();

    let php_only: HashMap<String, PersonAccum> = php_nz
        .iter()
        .filter(|(k, v)| rust_nz.get(*k).copied() != Some(**v))
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    let rust_only: HashMap<String, PersonAccum> = rust_nz
        .iter()
        .filter(|(k, v)| php_nz.get(*k).copied() != Some(**v))
        .map(|(k, v)| (k.clone(), *v))
        .collect();

    if php_only.is_empty() && rust_only.is_empty() {
        None
    } else {
        Some(VerifyDiff {
            php_only,
            rust_only,
        })
    }
}
