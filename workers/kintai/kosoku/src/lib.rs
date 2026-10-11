//! 勤怠の拘束・休息の純粋ロジックと社内 MariaDB の SQL 文 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版・Cloud Run 版 (repo ルートの package) と勤怠 Worker が同じものを使うための共有 crate。
//! 依存は serde・serde_json・chrono・sha2・tracing だけで、wasm32 でも build できる (sqlx にも tokio-postgres にも依存しない —
//! Supabase への書き込みは SQL 定数と bind に渡す Vec の束までで、bind は root と Worker がそれぞれ書く)。
//! fold の I/O を含む流れ (`fold_flow`) は I/O を trait で受け取るので、ここには持ち込まない。
//! ここのファイルは全部 repo ルートの `build.rs` の `KINTAI_OUTPUT_SHA` に入る。

pub mod anchors;
pub mod fold_flow;
pub mod kintai_fold;
pub mod kintai_push;
pub mod kintai_reading_dates;
pub mod kintai_rest_diff;
pub mod kintai_tail_gap_probe;
pub mod kintai_timecard;
pub mod kintai_version;
pub mod kosoku;
pub mod kosoku_daily;
pub mod kosoku_paper;
pub mod sql;
pub mod unko_diff;
pub mod window;
