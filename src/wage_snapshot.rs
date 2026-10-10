//! 賃金確定値の月次スナップショットの純ロジック (Refs #291、
//! ohishi-exp/nuxt-dtako-admin#677)。
//!
//! 本体は勤怠 Worker と共有する `kintai-logic` (`workers/kintai/logic/src/wage_snapshot.rs`、Refs #322)。
//! 写しを 2 つ持たないよう、ここは再 export だけ (テスト 39 本も `workers/kintai/logic/tests/wage_snapshot.rs`)。
//! HTTP と SQL は [`crate::routes::wage_snapshot`]。
//!
//! ファイル名を `kintai` / `kosoku` で始めない (`build.rs` の勤怠の版の glob の外。`kintai-logic` も外)。

pub use kintai_logic::wage_snapshot::*;
