-- 月ゲート: 前回 fold した入力の指紋を持ち、変わっていない月の再計算を省く
--
-- Refs ohishi-exp/rust-ichibanboshi#205 実装計画 13。原設計 (issue 本文「指紋 (SHA) で
-- 差分だけ再計算する」節) の「入力を取得せずに指紋を作れるのが要点。R2 の LIST 1 回。
-- CSV は落とさない」を実装する。#217 は簡略化して「取得した行をハッシュ」にしていた —
-- これだと結局 R2 から全量 (月あたり約 1,100 運行) を取り直すので速くならない。
--
-- ## 何を持つか
--
-- 月まるごとの「前回 fold したときの入力の指紋」だけ。乗務員ごと・日ごとには持たない —
-- [`crate::kintai_fold::fold_month`] が読みそのものを乗務員で分けない (全乗務員版 1 回読み、
-- モジュール docs「材料の行は全乗務員版の形」参照) のと同じ理由で、月単位が読みの実態に
-- 素直に合う。
--
--   dtako_digest  … rust-alc-api の `GET /api/dtako/events/etags` が返す
--                    (unko_no, etag) の一覧を `sha256(sorted "key:etag" の join)` に
--                    したもの。R2 の LIST 1 回だけで作れる (CSV 本体を読まない)
--   punch_digest  … `kintai.kintai_events` (打刻側、Pg) を月まるごと 1 行に畳んだ
--                    sha256。既存の [`crate::kintai_push::STORED_WINDOW_SIGNATURES_SQL`]
--                    と同じ列組み立てを流用し、集計を Postgres 側で 1 発にする
--                    (索引 `kintai_events_driver_time` の INCLUDE だけで足りる)
--   logic_version … [`crate::kintai_fold::logic_version`]。kosoku.rs の deploy や TOML の
--                    閾値変更で出力そのものが変わったときに、指紋が同じでも gate を
--                    外させるための版番号 (shifts.logic_version と同じ列を再利用)
--
-- **どちらの指紋も取れないとき (alc に新口が無い環境など) は gate を使わない** —
-- 呼び出し側 ([`crate::kintai_fold`]) が判断し、この表へは書きも読みもしにいかない。
-- 「安全側に倒して全量読みへ degrade」なので、この表が空でも壊れない。
--
-- ## 誰が書くか — 全量再計算 (`recalc_month`, `driver` 省略) だけ
--
-- ページングされた再計算 ([`crate::kintai_fold::recalc_drivers`]、`POST /api/kintai/recalc`
-- の 1 ページや窓の受け口の `drivers_changed`) は**この表を読むだけで書かない**。
-- 書いてしまうと、ページ 1 の乗務員だけ処理した時点で gate が「この月は最新」になり、
-- 未処理のページ 2 以降の乗務員が古い fingerprint のまま永久に取り残される
-- (#225 / #234 と同型の「静かに一部が消える」事故になる)。
--
-- 書いてよいのは `driver` を指定しない `recalc_month` (= `fold_month` が読んだ月の
-- 全乗務員をその場で全部 `store_units` まで通す、CLI の `Recalc` / `Sync` サブコマンドの
-- 経路、`main.rs` の doc「systemd timer が呼ぶのはこれ」) だけ — 1 回の呼び出しで月の
-- 全量が完結するので、書いた時点の digest がそのまま「この月は最新」の意味を持てる。
--
-- ## 001 / 002 / 003 は書き換えない
--
-- 適用済み migration の改変は ledger の checksum 照合で loud fail する。修正は必ず
-- 新しい連番のファイルで行う (001 のコメントと同じ規範)。

CREATE TABLE kintai.fold_gate (
    tenant_id     UUID NOT NULL,
    month         TEXT NOT NULL CHECK (month ~ '^[0-9]{4}-[0-9]{2}$'),

    dtako_digest  CHAR(64) NOT NULL,
    punch_digest  CHAR(64) NOT NULL,
    logic_version CHAR(16) NOT NULL,

    folded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, month)
);

-- GRANT は 001 の `GRANT ... ON ALL TABLES IN SCHEMA kintai` がスキーマ単位で
-- 効いているので、この表個別には要らない (001 のコメントと同じ判断)。

ALTER TABLE kintai.fold_gate ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_fold_gate ON kintai.fold_gate
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
