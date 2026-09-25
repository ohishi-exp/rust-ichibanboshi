-- 取り込み後に打刻が直されたときの前後の記録 (Refs ohishi-exp/nuxt-dtako-admin#1133)
--
-- 打刻の push (`KintaiPgStore::replace_window`) は署名の変わった日を
-- DELETE → INSERT で丸ごと置き換えるので、**前の値がどこにも残らない**。
-- 訴訟用の準備ページが「3/10 に取り込んだ 2/6 の始業が 4/2 に 08:00 → 07:30 に
-- 直された」を示せるよう、置き換えの瞬間に同じトランザクションで前後を残す。
--
-- ## 何を記録するか (組み立ては `src/change_log.rs`)
--
-- - 1 行 = 乗務員 × 暦日 (JST) × 記録時刻。`before` / `after` はその日の events の
--   配列 (`{occurred_at, state, source, unko_no}`、並びは署名と同じ
--   occurred_at, state, source)
-- - **初回取り込み (旧が無い日) は記録しない。**「取り込み後の変更」ではないため
-- - 日ごと消えた日は `after` = NULL
-- - 署名が一致する日は push されないので行は増えない
--
-- 記録は仕組みを入れた日から。過去分は遡らない (読み口が最古の `recorded_at` を
-- 「この日より前は記録なし」として返す)。
--
-- `driver_cd` は他の kintai の表と同じ BIGINT (kintai_events と型を揃える)。

CREATE TABLE kintai.event_changes (
    tenant_id    UUID NOT NULL,
    driver_cd    BIGINT NOT NULL,
    date         DATE NOT NULL,                       -- JST の暦日
    recorded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),  -- 置き換えたトランザクションの時刻
    before       JSONB,                               -- 旧 events の配列 (無ければ NULL)
    after        JSONB,                               -- 新 events の配列 (日ごと消えたら NULL)

    PRIMARY KEY (tenant_id, driver_cd, date, recorded_at),
    -- 制約名は明示する (006 と同じ理由。検証スクリプトがエラー文言で確かめる)。
    -- 前後とも無い行は「変更」ではない
    CONSTRAINT event_changes_has_side
        CHECK (before IS NOT NULL OR after IS NOT NULL)
);

-- 乗務員を指定しない期間の読み (全乗務員 × 暦日の範囲)
CREATE INDEX event_changes_date ON kintai.event_changes (tenant_id, date);

-- ── RLS (001 と同じ形。ポリシー名は tenant_isolation_<table>) ──────────
ALTER TABLE kintai.event_changes ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_event_changes ON kintai.event_changes
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- 005 の既定権限に頼らず明示する (006 と同じ。新表の GRANT 漏れで本番 502 の実績)。
-- GRANT は SELECT, INSERT だけ明示するが、005 の ALTER DEFAULT PRIVILEGES で writer には
-- UPDATE / DELETE も付く (verify_kintai_rls.sh が全表に 4 権限を要求するため REVOKE しない)。
-- 追記専用はアプリ側 (書き込み経路が INSERT だけ) で守る
GRANT SELECT, INSERT ON kintai.event_changes TO kintai_writer;
GRANT SELECT ON kintai.event_changes TO kintai_reader;
