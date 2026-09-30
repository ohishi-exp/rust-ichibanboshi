//! 給与大臣 (OHKEN) 読み取り API (Refs #82)。
//!
//! - `logic` — 純粋ロジック (DB 名規則・項目マッピング・行組み立て)。本体は Worker 側の
//!   `kyuyo_logic::payroll` (workers/kyuyo/logic、Refs #322)。オンプレ廃止時にこの依存を外す
//! - `introspect` — auth-worker introspect + email allowlist 認可
//! - `repo` — OHKEN への tiberius 読み取り層 (別 pool)
//! - `store` — SQLite derived store (Refs #106 Phase 1、docs/plan-kyuyo-sqlite-store.md)

pub mod introspect;
pub use kyuyo_logic::payroll as logic;
pub mod repo;
pub mod store;
