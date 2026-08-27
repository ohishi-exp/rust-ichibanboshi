-- 拘束の土台が取れていたかをスナップショットに残す
-- (Refs ohishi-exp/nuxt-dtako-admin#986 / #980)
--
-- ## 何が起きていたか
--
-- 最低賃金チェックは、拘束の元データ (オンプレ `kosoku-daily`) が**取れなくても
-- 黙って打刻だけで表を組む** (本番で 97 名ぶん発生、nuxt-dtako-admin#980)。その状態で
-- 保存されたスナップショットは `restraint_source` も `wage_logic_version` も
-- `salary_item_sha` も健全時と**同じ**で、**数字だけが違う**。後から
-- 「なぜこの月だけ違うのか」を説明できない。
--
-- ⇒ 画面が既に持っている値 (`GET /restraint-api/wage-report` の `timecard_kosoku`) を
-- そのまま保存物にも運ぶ。**新しい語彙は作らない。**
--
-- ## 4 値。`no` と `unreadable` を畳まない
--
-- | 値 | 意味 | 処方 |
-- |---|---|---|
-- | `yes` | 当月の拘束を `kosoku-daily` から組めた | — |
-- | `no` | 取れなかった | 読み直せば入る |
-- | `unreadable` | 読めなかった | 上流の応答の形が変わっている |
-- | NULL | **見ていない** | — |
--
-- **処方が逆なので `no` と `unreadable` を 1 つにしない。**
--
-- NULL は「見ていない」であって「揃っていた」ではない。`restraint_source = 'gcp'` は
-- `kosoku-daily` を取りに行かないので、その経路では NULL が**正しい値**になる。
-- `restraint_missing` (GCP にこの乗務員 × この月の行が無い) とは条件が違うので混ぜない。
--
-- ## なぜ既存表への列追加で足りるか
--
-- これは会社 × 月 × ソースの属性で、乗務員ごとには変わらない。だが同じ粒度の
-- `salary_item_sha` / `payroll_synced_at` / `wage_logic_version` が既に
-- `wage_snapshot` の列として行ごとに繰り返されている。**別表に切ると、月の属性を
-- 引くのに JOIN が 1 本増えるだけで、置き換え保存 (DELETE → INSERT) の原子性は
-- 落ちる。** 既存の並びに揃える。
--
-- ## GRANT は要らない
--
-- 006 の `GRANT ... ON kintai.wage_snapshot` は**表単位**で、表単位の権限は
-- **後から足した列にもそのまま及ぶ** (列単位 GRANT は表単位に上乗せする形でしか
-- 存在しない)。005 が踏んだ「`ON ALL TABLES` は実行時点の表にしか効かない」は
-- **表を新設したとき**の話で、ここには当たらない。
-- **前提を人の記憶で持たない**ため、`scripts/verify_kintai_rls.sh` の
-- writer 往復 INSERT にこの列を足して実測する (同じ PR)。
--
-- ## 001〜006 は 1 バイトも書き換えない
--
-- 適用済み migration の改変は ledger の SHA-384 照合で loud fail する。

ALTER TABLE kintai.wage_snapshot
    ADD COLUMN timecard_kosoku TEXT;

-- 制約名は明示する (006 の `wage_snapshot_restraint_source_check` と同じ理由 —
-- `scripts/verify_kintai_rls.sh` がエラーメッセージで「弾かれたこと」を確かめる)。
-- NULL は CHECK を通る (`NULL IN (...)` は NULL で、CHECK は false のときだけ弾く) —
-- 「見ていない」を表す値なので、通るのが正しい。
ALTER TABLE kintai.wage_snapshot
    ADD CONSTRAINT wage_snapshot_timecard_kosoku_check
        CHECK (timecard_kosoku IN ('yes', 'no', 'unreadable'));
