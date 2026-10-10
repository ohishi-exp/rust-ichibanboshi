//! `GET /api/kintai/day-events` と `GET /api/dtako/worktime` の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版・Cloud Run 版 (repo ルートの `src/routes/dtako_day.rs`・`src/routes/dtako_worktime.rs`) と
//! 勤怠 Worker が同じものを使うための共有 crate。DB も I/O も持たない。依存は serde_json・chrono・
//! kintai-kosoku だけで、wasm32 でも build できる。
//!
//! **repo ルートの `build.rs` の `KINTAI_OUTPUT_SHA` (勤怠の版) には入らない** — 元の 2 ファイルが
//! glob の外 (`kintai`/`kosoku` で始まらない名前) にあったのと同じ分類。この 2 本は
//! `/api/kintai/{daily,kosoku-daily,version}` の応答を形づくらない。

pub mod day;
pub mod worktime;
