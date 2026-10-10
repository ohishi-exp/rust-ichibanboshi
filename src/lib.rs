pub mod cakephp;
pub mod cf_access;
pub mod change_log;
pub mod config;
pub mod db;
pub mod dtako_reset_material;
pub mod kintai_diff;
pub mod kintai_fold;
pub mod kintai_http_repo;
pub mod kintai_pg_repo;
pub mod kintai_push;
pub mod kintai_repo;
pub mod kintai_store;
pub mod kintai_version;
pub mod rdcleanpath;
pub mod rdp_defaults;
pub mod rdp_nego;
pub mod repo;
pub mod restraint_store;
pub mod routes;
pub mod server;
pub mod sqlite;
pub mod wage_snapshot;

// 拘束・休息の純粋ロジックは勤怠 Worker と共有する crate に置く (Refs #322)。
// 利用側が `crate::kosoku` 等のまま読めるように同じ名前で出す。
pub use kintai_kosoku::{
    kintai_reading_dates, kintai_rest_diff, kintai_tail_gap_probe, kosoku, kosoku_paper,
};
