-- 日別サマリに「実働でない区間」を持つ列を足す (Refs ohishi-exp/nuxt-dtako-admin#1133)
--
-- 勤務 1 本 = `kintai.day_summaries` 1 行。その勤務の始業〜終業のうち、実働から引かれた
-- 区間 (休息と休憩) の配列を、同じ行に JSONB で持つ。書くのは次の PR (計算と畳み込み)。
-- 列だけを先に本番へ入れておくのは、binary の deploy が migration の適用を待たないため
-- (列と書き込みを同じ PR にすると、列が無い間の畳み直しが失敗する)。
--
-- ## 値
--
-- `[{"start":"YYYY-MM-DD HH24:MI:SS","end":"…","kind":"…"}, …]` (JST の壁時計、始まりの昇順)。
--
-- - NULL = この列ができる前に畳んだ行 (まだ区間を持たない)
-- - `[]`  = 畳んだが実働でない区間が無い
--
-- 既存の行は書き換えない (NULL のまま。畳み直したときに入る)。既定値も付けない:
-- 既定値があると「畳んだが区間が無い」と「まだ畳んでいない」の区別が付かなくなる。
--
-- 新しい表ではないので RLS と GRANT は足さない (表単位のものは 001 / 005 のまま効く)。
-- 既存の INSERT は列を列挙しているので、NULL 可の列が増えても動く。

ALTER TABLE kintai.day_summaries ADD COLUMN non_working JSONB;

COMMENT ON COLUMN kintai.day_summaries.non_working IS
    '実働でない区間 (休息・休憩) の配列。NULL = 列ができる前に畳んだ行、[] = 区間なし';
