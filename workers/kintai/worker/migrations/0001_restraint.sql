-- 拘束サマリ (restraint) の写しの 2 表 (Refs ohishi-exp/rust-ichibanboshi#322)。
--
-- 正本はこのファイル。共有 crate kintai-logic の restraint::SCHEMA_SQL が include_str! で読み、
-- オンプレ版 (root の src/restraint_store.rs、rusqlite) は open のたびにこれを流す (IF NOT EXISTS)。
-- 勤怠 Worker の D1 (binding KINTAI_RESTRAINT_DB) には migration として 1 回だけ当てる。
-- 適用済みの migration は変えない: 表を変えるときは 0002_*.sql を足す。
--
-- 中身は relay (nuxt-dtako-admin の dtako-scraper-relay) が push するサマリの写しで、
-- 消えても relay の resummarize (全月) で作り直せる。

CREATE TABLE IF NOT EXISTS restraint_summary (
  comp_id TEXT NOT NULL,
  source TEXT NOT NULL,         -- 'theearth' | 'timecard'
  ym TEXT NOT NULL,             -- 'YYYY-MM'
  driver_cd TEXT NOT NULL,
  no_data INTEGER NOT NULL DEFAULT 0,
  summary_json TEXT,            -- relay のサマリ JSON verbatim (no_data は NULL)
  fetched_at TEXT,
  last_verified_at TEXT,
  PRIMARY KEY (comp_id, source, ym, driver_cd)
);

CREATE TABLE IF NOT EXISTS restraint_sync_state (
  scope TEXT NOT NULL PRIMARY KEY,  -- 'comp:source:ym'
  synced_at TEXT NOT NULL,
  row_count INTEGER NOT NULL
);
