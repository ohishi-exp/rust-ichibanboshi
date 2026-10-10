//! 勤怠 Worker が Supabase (勤怠スキーマ `kintai.*`) を読む 5 本の口の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! 各口の module は同じ形: `parse` (クエリ文字列 → 検査済みの `Request`、400 の条件と文言は元と同じ) /
//! SQL 定数 / `Binds` (`params()` が `query_typed` に渡す `$n` と `Type` の対。**`$1` は必ず UUID のテナント pin**) /
//! `Row` (owned な行) / `respond` (応答の JSON)。DB との往復は worker crate が持つ。
//!
//! 社内 MariaDB を直接読む 4 本 (events・rest-diff・reading-dates・tail-gap-probe) は `mariadb_reads` (検査・引数・応答) と
//! `mariadb_rows` (テキストプロトコルの行 → JSON)。SQL と突合などの純粋ロジックは共有 crate `kintai-kosoku` を使う。
//! 同じく社内 MariaDB を読む day-events・dtako/worktime は `dtako_reads` (検査・引数・応答)。日の窓・運行への畳み方・
//! 層 A の秒数は共有 crate `kintai-dtako` を使う。
//! kosoku-daily・version・timecard/drivers・timecard/events は `kosoku_reads` (検査・引数・応答)。応答を組む部分は共有 crate
//! `kintai-kosoku` (`kosoku_daily`・`kintai_version`・`kintai_timecard`) を使う。
//!
//! 社内 CakePHP への中継 (daily・pdf-json・autoload と ③ resetby-unko-no) の URL・multipart・応答の型は `cakephp`、
//! autoload の検査・① ② ③ の段取り・応答・③ の材料を数える SQL は `dtako_autoload` (root の package も path 依存で使う)。
//! 勤怠 Worker の daily・pdf-json・autoload の口の検査・応答・失敗の写像は `cakephp_relay`。
//!
//! root の src/ (Cloud Run 版・オンプレ版) からの写しで、対応表は `workers/kintai/README.md`。
//! **撤去までは片方を直したらもう片方も直す。**

pub mod cakephp;
pub mod cakephp_relay;
pub mod change_log;
pub mod common;
pub mod day_summaries;
pub mod dtako_autoload;
pub mod dtako_reads;
pub mod kosoku_reads;
pub mod mariadb_reads;
pub mod mariadb_rows;
pub mod shift_days;
pub mod shift_overlaps;
pub mod timecard_write;
pub mod wage_range;
pub mod wage_snapshot;
pub mod wage_write;
pub mod write_auth;
