-- 休息の開始の補正 (Refs ohishi-exp/rust-ichibanboshi#322)
--
-- theearth の拘束時間管理表は、**休息の直前の運転**を休息ボタンの時刻 (alc の `休息` の行の開始) ではなく、
-- 車載器の速度データ (3要素解析 = NET780 の `.spd`) で**最後に車が動いた時刻**で終える。
-- 休憩・運行終了の直前の運転には当てはまらない (どちらも区間時間どおり)。
-- 最後の動きとボタンが同じ分に収まらないときだけ、運転と拘束が 1 分ずれる
-- (実測 2026-10-17、北海大運: 6/17 乗務員 6 は 19:52:59 / 19:53:00、7/17 乗務員 11 は 16:45:46 / 16:46:09)。
--
-- 勤怠 Worker は速度データを持たないので、relay が管理表と 1 分ずれた勤務だけ速度データを読み、
-- 最後の動きをここに書く。fold は alc だけの乗務員の `休息` の行の開始を、ここにあればこの時刻に差し替える。
-- alc の行 (元データ) は書き換えない。行を消せば元の計算に戻る。
--
-- ## 値
--
-- - `rest_start_at` = alc の `休息` の行の開始 (秒まで)。fold はこの値と一致する行だけを差し替える
-- - `last_move_at` = 速度データで、`rest_start_at` より前で最後に速度が 0 でなかった時刻
--   (速度は 0.5 秒刻みなので、秒未満は切り捨てて持つ)。休息の開始より前で、1 時間以内
-- - `unko_no` = その休息を持つ運行の運行NO (22 桁)。どの zip から読んだかを追うため
-- - `source` = 何から読んだか。今は `net780` だけ

CREATE TABLE kintai.rest_start_overrides (
    tenant_id      UUID NOT NULL,
    driver_cd      BIGINT NOT NULL,
    rest_start_at  TIMESTAMPTZ NOT NULL,
    last_move_at   TIMESTAMPTZ NOT NULL,
    unko_no        TEXT NOT NULL,
    source         TEXT NOT NULL,
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (tenant_id, driver_cd, rest_start_at),
    -- 制約名は明示する (006 と同じ理由。検証スクリプトがエラー文言で確かめる)
    CONSTRAINT rest_start_overrides_before_rest
        CHECK (last_move_at < rest_start_at AND last_move_at >= rest_start_at - interval '1 hour'),
    CONSTRAINT rest_start_overrides_source
        CHECK (source IN ('net780'))
);

-- ── RLS (001 と同じ形。ポリシー名は tenant_isolation_<table>) ──────────
ALTER TABLE kintai.rest_start_overrides ENABLE ROW LEVEL SECURITY;

CREATE POLICY tenant_isolation_rest_start_overrides ON kintai.rest_start_overrides
    USING (tenant_id = current_setting('app.current_tenant_id')::UUID);

-- 005 の既定権限に頼らず明示する (006・008 と同じ。新表の GRANT 漏れで本番 502 の実績)。
-- 書き込みは同じ鍵の行を置き換える (UPSERT) ので UPDATE も要る
GRANT SELECT, INSERT, UPDATE, DELETE ON kintai.rest_start_overrides TO kintai_writer;
GRANT SELECT ON kintai.rest_start_overrides TO kintai_reader;
