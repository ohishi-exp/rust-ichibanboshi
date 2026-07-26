//! 給与大臣 (OHKEN) 読み取り API (Refs #82)。
//!
//! - `logic` — 純粋ロジック (DB 名規則・項目マッピング・行組み立て)
//! - `introspect` — auth-worker introspect + email allowlist 認可
//! - `repo` — OHKEN への tiberius 読み取り層 (別 pool)
//! - `store` — SQLite derived store (Refs #106 Phase 1、docs/plan-kyuyo-sqlite-store.md)

pub mod introspect;
pub mod logic;
pub mod repo;
pub mod store;
