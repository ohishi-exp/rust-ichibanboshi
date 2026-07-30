-- kintai スキーマ — 打刻の生イベント + 畳んだ 3 層 + 突合の足場
--
-- Refs ohishi-exp/rust-ichibanboshi#205 実装計画 03。DDL は issue 本文が正。
-- 適用先は専用の Supabase プロジェクト (Data API オフ / automatic RLS オン /
-- Region Tokyo)。接続文字列は secret `kintai-database-url`。
--
-- 適用は `scripts/migrate_kintai.sh` (psql)。ledger は sqlx と同形の
-- `_sqlx_migrations` なので、postgres client が入った後は `sqlx::migrate!` が
-- そのまま引き継げる (再適用されない)。詳細は同スクリプトのヘッダ。
--
-- ## この migration を後から書き換えないこと
--
-- 適用済み migration の改変は ledger の checksum 照合で loud fail する
-- (`rust-alc-api` CLAUDE.md「適用済み migration は絶対に変更しない」と同じ規範)。
-- 修正は必ず 002 以降の新規ファイルで行う。
--
-- ## 非修飾名を使わない
--
-- テーブルは全て `kintai.` で修飾する。`search_path` に依存しないので、適用者の
-- ロール設定や Supabase 側の既定 search_path が何であっても同じ物ができる。
-- (`SECURITY DEFINER` 関数は 1 つも作らないため、alc の「`SET search_path` 必須」
--  規範に該当する対象はこの migration には無い)

CREATE SCHEMA IF NOT EXISTS kintai;

-- ── 入力: 生イベント (改修時の遡り用。読み出しでは使わない) ──────────────
CREATE TABLE kintai.kintai_events (
    tenant_id     UUID NOT NULL,
    driver_cd     BIGINT NOT NULL,       -- 乗務員CD。UUID にしない
    occurred_at   TIMESTAMPTZ NOT NULL,

    -- time_card_dtako_state の 7 値をそのまま持つ。休息は開始/終了を潰さない —
    -- kosoku.rs が「休息の終了 = 始業、休息の開始 = 終業」で使うため
    state         TEXT NOT NULL CHECK (state IN
                    ('始業','終業','運行開始','運行終了','休息開始','休息終了','除外')),

    source        TEXT NOT NULL CHECK (source IN ('timecard','dtako','alc_app')),
    unko_no       TEXT,
    raw           JSONB NOT NULL DEFAULT '{}',
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- 冪等キー。同じ打刻を 2 度送っても増えない
    PRIMARY KEY (tenant_id, driver_cd, occurred_at, state)
);

CREATE INDEX kintai_events_driver_time
    ON kintai.kintai_events (tenant_id, driver_cd, occurred_at)
    INCLUDE (state, source, unko_no);

-- ── 正: 勤務境界 ────────────────────────────────────────────────────────
CREATE TABLE kintai.shifts (
    tenant_id     UUID NOT NULL,
    driver_cd     BIGINT NOT NULL,
    start_at      TIMESTAMPTZ NOT NULL,
    end_at        TIMESTAMPTZ NOT NULL,

    -- 打刻で決めたか休息イベントで決めたか。docs が「後から差分の理由を
    -- 追えるようにする」ために持たせているもの
    shift_source  TEXT NOT NULL CHECK (shift_source IN ('timecard','rest')),

    -- over_24h は持たない。判定は拘束 (= 終業 − 始業 − 中の休息) で行うため
    -- (kosoku.rs:1615)、拘束を持たない shifts では定義できない
    date_start    DATE GENERATED ALWAYS AS
                    ((start_at AT TIME ZONE 'Asia/Tokyo')::date) STORED,
    duration_min  INTEGER GENERATED ALWAYS AS
                    ((EXTRACT(EPOCH FROM (end_at - start_at))/60)::int) STORED,

    fingerprint   CHAR(64) NOT NULL,
    -- build.rs が焼き込む KINTAI_OUTPUT_SHA (16 桁 hex)。手で上げる INTEGER にしない
    logic_version CHAR(16) NOT NULL,
    PRIMARY KEY (tenant_id, driver_cd, start_at),
    CHECK (end_at > start_at)
);

CREATE INDEX shifts_month
    ON kintai.shifts (tenant_id, date_start)
    INCLUDE (driver_cd, start_at, end_at, shift_source, duration_min);

-- ── 正: 日別サマリ (勤務を始業日へ寄せた区分) ──────────────────────────
CREATE TABLE kintai.day_summaries (
    tenant_id                         UUID NOT NULL,
    driver_cd                         BIGINT NOT NULL,
    date                              DATE NOT NULL,   -- 始業日
    shift_source                      TEXT NOT NULL,

    restraint_minutes                 INTEGER NOT NULL,
    working_minutes                   INTEGER NOT NULL,
    break_minutes                     INTEGER NOT NULL,
    rest_minus_minutes                INTEGER NOT NULL,
    statutory_minutes                 INTEGER NOT NULL,
    within_statutory_overtime_minutes INTEGER NOT NULL,
    overtime_minutes                  INTEGER NOT NULL,
    legal_holiday_minutes             INTEGER NOT NULL,
    night_minutes                     INTEGER NOT NULL,
    overtime_night_minutes            INTEGER NOT NULL,
    legal_holiday_night_minutes       INTEGER NOT NULL,

    fingerprint                       CHAR(64) NOT NULL,
    logic_version                     CHAR(16) NOT NULL,
    PRIMARY KEY (tenant_id, driver_cd, date)
);

-- 月次・全乗務員。管理表と wage-report の主経路を index-only にする
CREATE INDEX day_summaries_month
    ON kintai.day_summaries (tenant_id, date)
    INCLUDE (driver_cd, restraint_minutes, working_minutes, break_minutes,
             overtime_minutes, legal_holiday_minutes, night_minutes,
             overtime_night_minutes, legal_holiday_night_minutes);

-- 遵守チェック (改善基準)。閾値は kosoku.rs の MAX_RESTRAINT_MINUTES のまま、
-- DDL に焼かない。基準が変わったら索引を作り直すだけでデータの再計算は不要
CREATE INDEX day_summaries_over_24h
    ON kintai.day_summaries (tenant_id, date)
    WHERE restraint_minutes > 1440;

-- ── 正: 暦日ビュー (勤務を 0 時で切って配る、Refs #130) ────────────────
CREATE TABLE kintai.day_parts (
    tenant_id         UUID NOT NULL,
    driver_cd         BIGINT NOT NULL,
    shift_start_at    TIMESTAMPTZ NOT NULL,   -- どの勤務の一部か
    date              DATE NOT NULL,          -- 乗った暦日 (JST)。start_at の日付と一致しない
    restraint_minutes INTEGER NOT NULL,
    working_minutes   INTEGER NOT NULL,
    night_minutes     INTEGER NOT NULL,       -- 深夜は時計の窓なので暦日で計算できる
    PRIMARY KEY (tenant_id, driver_cd, shift_start_at, date),
    FOREIGN KEY (tenant_id, driver_cd, shift_start_at)
      REFERENCES kintai.shifts (tenant_id, driver_cd, start_at) ON DELETE CASCADE,
    CHECK (date >= (shift_start_at AT TIME ZONE 'Asia/Tokyo')::date),
    CHECK (restraint_minutes BETWEEN 0 AND 1440),
    CHECK (working_minutes   BETWEEN 0 AND 1440)
);

CREATE INDEX day_parts_month ON kintai.day_parts (tenant_id, date);

-- ── 突合 (PHP 改修の足場。終われば DROP できる) ────────────────────────
CREATE TABLE kintai.paper_drift (
    tenant_id     UUID NOT NULL,
    driver_cd     BIGINT NOT NULL,
    date          DATE NOT NULL,
    paper_minutes INTEGER NOT NULL,
    drift_minutes INTEGER NOT NULL,
    PRIMARY KEY (tenant_id, driver_cd, date)
);

-- 突合の実体は relay の classifyDiff (nuxt-dtako-admin の
-- workers/dtako-scraper-relay/src/timecard-compare.ts:613-738)。列はその出力に合わせる
CREATE TABLE kintai.php_diff (
    tenant_id    UUID NOT NULL,
    driver_cd    BIGINT NOT NULL,
    date         DATE NOT NULL,

    -- 現行の突合は拘束しか比べていない (timecard-compare.ts:12-16「突合するのは
    -- 拘束だけ。残業は比較しない」)。working / break は実装されたら足す
    item         TEXT NOT NULL CHECK (item IN ('restraint')),

    rust_minutes INTEGER NOT NULL,
    php_minutes  INTEGER NOT NULL,

    -- 符号は php − rust。実装が nginx − ours (timecard-compare.ts:842-843) で、
    -- cause の explained 値が全てこの向き前提で組まれているため逆にしない
    diff_minutes INTEGER GENERATED ALWAYS AS (php_minutes - rust_minutes) STORED,

    -- classifyDiff は原子 12 値に加え "a+b" / "a+b+c" の複合ラベルをその場で
    -- 生成する (:679-710、最大 220 通り)。固定の IN では収まらないため正規表現
    cause        TEXT CHECK (
        cause IS NULL
     OR cause IN ('none','unknown')
     OR cause ~ '^(ferry|lunch|month-boundary|run-gap|punch-tail|punch-head|run-head|paper-outside|ours-outside|minus-unko|gap-midnight|rounding)(\+(ferry|lunch|month-boundary|run-gap|punch-tail|punch-head|run-head|paper-outside|ours-outside|minus-unko|gap-midnight|rounding))*$'
    ),

    -- classifyDiff は (cause, explained, residual) の 3 つ組で返す (:628)。
    -- residual を持たないと「その cause で本当に説明が付いたか」を後から検証できず、
    -- cause が反証不能なラベルになる
    explained_minutes INTEGER NOT NULL,
    residual_minutes  INTEGER NOT NULL,

    -- cause が付くかは許容誤差に依存する (DEFAULT_TOLERANCE_MINUTES = 1、:49)。
    -- 持たないと後から判定を再現できない
    tolerance_minutes INTEGER NOT NULL,

    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, driver_cd, date, item)
);

-- ── RLS ────────────────────────────────────────────────────────────────
ALTER TABLE kintai.kintai_events  ENABLE ROW LEVEL SECURITY;
ALTER TABLE kintai.shifts         ENABLE ROW LEVEL SECURITY;
ALTER TABLE kintai.day_summaries  ENABLE ROW LEVEL SECURITY;
ALTER TABLE kintai.day_parts      ENABLE ROW LEVEL SECURITY;
ALTER TABLE kintai.paper_drift    ENABLE ROW LEVEL SECURITY;
ALTER TABLE kintai.php_diff       ENABLE ROW LEVEL SECURITY;

-- issue 本文は 1 表だけ書いて「以下 5 表も同形」と省略している。省略を展開した。
-- ポリシー名は `t` ではなく alc 流儀の `tenant_isolation_<table>` にした
-- (pg_policies の一覧やエラーメッセージに出るのは名前だけなので、1 文字だと
--  どのテナント分離が落ちたのか読み取れない)。
--
-- WITH CHECK は書かない。FOR ALL の既定で USING 式がそのまま WITH CHECK として
-- 使われるため、他テナントの tenant_id を INSERT / UPDATE することもできない。
-- (alc の「`WITH CHECK (true)` は避ける」規範に対して、最も強い側に倒している)
--
-- `current_setting` は 1 引数形なので未設定のセッションでは SELECT ごと ERROR に
-- なる = fail-closed。テナントを指定し忘れて全件見えるより静かでない方を採る。
CREATE POLICY tenant_isolation_kintai_events ON kintai.kintai_events
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
CREATE POLICY tenant_isolation_shifts ON kintai.shifts
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
CREATE POLICY tenant_isolation_day_summaries ON kintai.day_summaries
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
CREATE POLICY tenant_isolation_day_parts ON kintai.day_parts
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
CREATE POLICY tenant_isolation_paper_drift ON kintai.paper_drift
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);
CREATE POLICY tenant_isolation_php_diff ON kintai.php_diff
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- ── ロール分離 ─────────────────────────────────────────────────────────
-- パスワードはここに書かない (migration はリポジトリに残るため)。
-- 作成直後は認証情報が無く接続できない = 資格情報を配るまで到達不能で fail-closed。
-- 付与は Supabase 側で `ALTER ROLE ... PASSWORD` して secret に入れる。
--
-- ロールはクラスタ単位のオブジェクトなので、同一クラスタの別 DB に対して
-- この migration を 2 度目に流すと `CREATE ROLE` で落ちる。専用プロジェクトに
-- 1 度だけ適用する前提。検証用の作り直しは scripts/verify_kintai_rls.sh 側で
-- DROP してから流している (migration に IF NOT EXISTS 相当を持ち込まない)。
CREATE ROLE kintai_reader NOINHERIT NOBYPASSRLS LOGIN;
GRANT USAGE ON SCHEMA kintai TO kintai_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA kintai TO kintai_reader;

-- 全テナントを書くので RLS を跨ぐ。読み取り経路と共有しない
CREATE ROLE kintai_writer NOINHERIT BYPASSRLS LOGIN;
GRANT USAGE ON SCHEMA kintai TO kintai_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA kintai TO kintai_writer;
