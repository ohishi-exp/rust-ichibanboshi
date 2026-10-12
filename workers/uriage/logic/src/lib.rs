//! 担当者別売上 (`/api/uriage/*`) の純粋部分 (Refs ohishi-exp/rust-ichibanboshi#322)。
//!
//! オンプレ版 (repo ルートの package) と売上 Worker が同じものを使う (root は path 依存で `pub use` する。写さない)。
//! tiberius・worker・axum・reqwest・rusqlite に依存せず、時計も読まない (時刻は引数で受ける)。

pub mod admin;
pub mod api;
pub mod cakephp;
pub mod compute;
pub mod d1;
pub mod period;
pub mod raw;
pub mod recalc;
pub mod routes;
pub mod sqlserver;
pub mod verify;
pub mod write_auth;
