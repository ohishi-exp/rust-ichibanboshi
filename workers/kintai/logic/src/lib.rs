//! 勤怠 Worker が Supabase (勤怠スキーマ `kintai.*`) を読む 5 本の口の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 各口の module は同じ形: `parse` (クエリ文字列 → 検査済みの `Request`、400 の条件と文言は元と同じ) /
//! SQL 定数 / `Binds` (`params()` が `query_typed` に渡す `$n` と `Type` の対。**`$1` は必ず UUID のテナント pin**) /
//! `Row` (owned な行) / `respond` (応答の JSON)。DB との往復は worker crate が持つ。
//!
//! root の src/ (Cloud Run 版) からの写しで、対応表は `workers/kintai/README.md`。
//! **撤去までは片方を直したらもう片方も直す。**

pub mod change_log;
pub mod common;
pub mod day_summaries;
pub mod shift_days;
pub mod shift_overlaps;
pub mod wage_range;
pub mod wage_snapshot;
