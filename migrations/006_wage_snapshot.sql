-- 賃金確定値の月次スナップショット (Refs #291、ohishi-exp/nuxt-dtako-admin#677)
--
-- /restraint-wage「最低賃金チェック」の右端 3 ブロック (計算 / 給与 / 差 ×
-- 基本給 / 残業代 / 合計) を乗務員 × 月で保存する。期間集計タブ
-- (`GET /api/kintai/wage-range`) はこの表を SUM するだけで済む。
--
-- ## なぜ Postgres に置くか
--
-- 賃金の確定値は**勤怠の派生データ**で、素材の `kintai.day_summaries` と同じ DB に
-- 置くのが筋が通る。D1 (nuxt-dtako-admin の relay) に置くと読み出しが relay DO 経由に
-- なり、64 秒級の `wage-report` と同じ DO を奪い合う (2026-08-04 実測)。
--
-- ## ここでは計算しない
--
-- 計算側は relay の TypeScript (`computeWageRow` + R2 の単価マスタ)、給与側は画面
-- (給与明細の突合結果)。どちらも Postgres からは出せないため、**揃った時点の画面から
-- POST を受けて upsert するだけ**の表として持つ。行の内容は「その時の画面表示の写し」
-- であって、監査の正は従来どおり R2 の原本と単月の再計算。
--
-- ## 差 (diff) は持たない
--
-- `paid_* - calc_*` で導出できる。片側 NULL (単価未設定 / 給与未取込) の伝播を
-- DDL に持ち込むと、読み出し側の規則と二重管理になる。
--
-- ## `paid_total` も持たない
--
-- 単月表と同じ定義 (`paid_base + paid_overtime`)。合計だけ別に保存すると、
-- 内訳と合計がずれた行を作れてしまう。
--
-- ## 鮮度メタ — 単価は「実額」を焼き込む
--
-- 単価マスタ・支給項目区分・給与明細は後から動く。マスタ全体の sha だけで判定すると
-- **他人の単価を 1 件直しただけで全乗務員・全月が「要再計算」**になるため、単価は
-- その月に適用した実額 (`hourly_rate`) を行に持ち、乗務員 × 月の粒度で突き合わせる。
-- 単価の履歴は適用開始日つきなので過去月の単価は普通は動かない — 動くのは訂正した時だけ。
--
-- `wage_logic_version` は **relay 側の賃金計算ロジックの版**で、この repo の
-- `kintai_fold::logic_version` (勤怠畳みの版) とは別物。列名を分けているのはそのため。

CREATE TABLE kintai.wage_snapshot (
    tenant_id         UUID NOT NULL,
    comp_id           TEXT NOT NULL,      -- dtako 側の会社 (R2 / D1 と同じキー)
    ym                DATE NOT NULL,      -- 勤務月の 1 日
    restraint_source  TEXT NOT NULL,      -- 'gcp' | 'current'
    driver_cd         BIGINT NOT NULL,
    driver_name       TEXT NOT NULL DEFAULT '',

    -- 並べ替え・区画分けの属性 (画面が社員マスタから解決したものをそのまま持つ)
    company           TEXT,               -- 給与大臣の会社コード (0100 等)
    branch_name       TEXT,               -- SHOZOKU.NAME1
    branch_code       INTEGER,            -- SHOZOKU.INCODE (営業所の並び順の正)
    job_name          TEXT,               -- SHOZOKU.NAME2 (職員区分の判定元)
    pay_kubun         SMALLINT,           -- 1=月給 2=日給 3=時給 4=その他

    -- 金額 (円)。NULL の意味が 0 と違うので NOT NULL にしない
    hourly_rate       INTEGER,            -- その月に適用した基礎単価 (行単位の鮮度判定)
    calc_base         INTEGER,            -- NULL = 単価未設定 (計算できない)
    calc_overtime     INTEGER,            -- 合計 − 基本給 (基本給以外のすべて)
    calc_total        INTEGER,
    paid_base         INTEGER,            -- NULL = 給与明細にこの人のこの月が無い
    paid_overtime     INTEGER,
    working_minutes   INTEGER,
    -- 拘束ソースにこの乗務員 × この月の行が無かった (0 分ではない)。
    -- 集計では金額を足さず、集計月数にも数えない
    restraint_missing BOOLEAN NOT NULL DEFAULT FALSE,

    -- 鮮度メタ (「要再計算」の判定材料)
    salary_item_sha     TEXT,             -- 支給項目区分の版 (会社 × 月)
    min_wage_sha        TEXT,             -- 最低賃金マスタの版 (会社 × 月)
    payroll_synced_at   TIMESTAMPTZ,      -- 突合した給与明細の同期時刻
    wage_logic_version  TEXT NOT NULL,    -- relay の賃金計算ロジックの版
    computed_at         TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, comp_id, ym, restraint_source, driver_cd),
    -- 制約名は明示する。`scripts/verify_kintai_rls.sh` がエラーメッセージで
    -- 「弾かれたこと」を確かめるので、自動生成名に依存させない
    CONSTRAINT wage_snapshot_restraint_source_check
        CHECK (restraint_source IN ('gcp', 'current'))
);

-- 期間集計の主経路 (会社 × ソース × 月範囲を舐めて GROUP BY driver_cd)。
-- 集計に要る列を INCLUDE して index-only で返せるようにする
CREATE INDEX wage_snapshot_range
    ON kintai.wage_snapshot (tenant_id, comp_id, restraint_source, ym)
    INCLUDE (driver_cd, calc_base, calc_overtime, calc_total,
             paid_base, paid_overtime, working_minutes);

-- ── RLS (001 と同じ形。ポリシー名は tenant_isolation_<table>) ──────────
-- WITH CHECK は書かない (FOR ALL の既定で USING 式がそのまま WITH CHECK になる)。
-- `current_setting` は 1 引数形なので未設定セッションでは ERROR = fail-closed。
ALTER TABLE kintai.wage_snapshot ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_wage_snapshot ON kintai.wage_snapshot
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- 005 の `ALTER DEFAULT PRIVILEGES` でこの表にも自動で付くはずだが、**明示でも書く**。
-- 005 の教訓 (「後から作られた表は権限ゼロで生まれる」) は「既定が効いている前提を
-- 検査で持つ」ところまでを含む — `scripts/verify_kintai_rls.sh` が実測する一方で、
-- 既定が外れていた場合にこの表だけ 502 になるのを避けるため両方書く。
GRANT SELECT, INSERT, UPDATE, DELETE ON kintai.wage_snapshot TO kintai_writer;
GRANT SELECT ON kintai.wage_snapshot TO kintai_reader;
