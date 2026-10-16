-- 日別サマリに「計上日」の列を足す (Refs ohishi-exp/rust-ichibanboshi#322)
--
-- 打刻を持たない会社 (北海大運) は、theearth の拘束時間管理表が「運行単位で、集計開始月に計上」する。
-- 6/30 に出庫した運行が休息を挟んで 7/1 まで続くと、7/1 の部分も 6 月 (運行開始の日) に数える。
-- 勤怠 Worker の計算は勤務を始業の日 (`date` = `shift_start_at` の JST 日付、002 の CHECK) で持つので、
-- 7/1 05:09 始まりの勤務は 7 月に入り、月合計が theearth と割れていた (2026-07 実測: 3 名で計 343 分)。
--
-- `date` は動かせない (CHECK と、fold の月の書き換え範囲が `shifts.date_start` で決まるため)。
-- そこで計上する日を別の列に持ち、月で読む口だけがこの列を見る。書くのは次の PR (計算と畳み込み)。
-- 列だけを先に本番へ入れておくのは、binary の deploy が migration の適用を待たないため (009 と同じ)。
--
-- ## 値
--
-- - NULL = 始業の日 (`date`) に計上する。打刻のある会社 (大石) と、列ができる前に畳んだ行
-- - それ以外 = その勤務を含む運行の運行開始の日。`date` より後にはならない
--
-- 既存の行は書き換えない (NULL のまま。畳み直したときに入る)。既定値も付けない。
-- 新しい表ではないので RLS と GRANT は足さない (表単位のものは 001 / 005 のまま効く)。
-- 既存の INSERT は列を列挙しているので、NULL 可の列が増えても動く。

ALTER TABLE kintai.day_summaries ADD COLUMN count_date DATE;

ALTER TABLE kintai.day_summaries
    ADD CONSTRAINT day_summaries_count_date_not_after_date
    CHECK (count_date IS NULL OR count_date <= date);

COMMENT ON COLUMN kintai.day_summaries.count_date IS
    '計上日 (その勤務を含む運行の運行開始の日)。NULL = 始業の日 (date) に計上する';
