-- 004 が作った kintai.fold_gate に GRANT を足す (Refs #205 の 20)
--
-- ⚠️ **merge しただけでは本番は直らない。** migration は CI/CD に乗っていないので、
--    merge 後に `make kintai-migrate` を人が流すまで本番の `POST /api/kintai/recalc`
--    は 502 のまま (コード側の degrade は「gate が使えないときに口を落とさない」
--    ための保険で、gate を成立させるのはこのファイルの GRANT)。
--
-- ## 何が起きていたか
--
-- 004 の 61〜62 行のコメントは**誤っている**:
--
--     -- GRANT は 001 の `GRANT ... ON ALL TABLES IN SCHEMA kintai` がスキーマ単位で
--     -- 効いているので、この表個別には要らない (001 のコメントと同じ判断)。
--
-- `GRANT ... ON ALL TABLES IN SCHEMA` は**スキーマ単位の規則ではない**。実行した
-- 瞬間に存在した表へ 1 つずつ GRANT を展開するだけの糖衣で、**後から作られた表には
-- 何も効かない**。001 には `ALTER DEFAULT PRIVILEGES` が無いので、001 より後に
-- 作られた表は権限ゼロで生まれる。002 は表を作らないため、004 の `fold_gate` が
-- 「001 以降に初めて作られた表」で、ここで初めて踏んだ。
--
-- 本番 (`kintai_writer` で接続、src/config.rs の `[kintai_push]` docs) では:
--
--     SELECT dtako_digest, punch_digest, logic_version FROM kintai.fold_gate ...
--       → ERROR: permission denied for table fold_gate
--
-- これが `KintaiPushError::Db` → `routes::kintai_timecard::map_push_err` で **502**
-- になり、`POST /api/kintai/recalc` が丸ごと落ちていた (2026-07-31、alc が
-- `GET /api/dtako/events/etags` を本番へ出した直後から。それまでは
-- `compute_month_digests` が etags の 404 で `Ok(None)` を返して**この SELECT の
-- 手前で** return していたため、本番で 1 度も実行されていなかった)。
--
-- ## 再発防止: ALTER DEFAULT PRIVILEGES
--
-- 表を足すたびに GRANT を書き忘れる形を残さない。**これは「今後この migration を
-- 流したロールが作る表」にだけ効く**ので、004 の fold_gate には遡って効かない —
-- だから下の明示 GRANT も両方要る (片方だけでは直らない)。
--
-- `FOR ROLE` を省いているので対象は `current_role` = **この migration を流すロール**。
-- migration は必ず `scripts/migrate_kintai.sh` (`KINTAI_DATABASE_URL` の 1 ロール)
-- 経由で流すので、将来 `CREATE TABLE` するのも同じロールになる。この前提が崩れたら
-- 新しい表がまた権限ゼロで生まれるため、**前提そのものを検査で持つ**:
-- `scripts/verify_kintai_rls.sh` が probe 表を 1 枚作って
-- 「writer / reader の GRANT が自動で付くか」を実測し、付かなければ落ちる。
--
-- 001 / 002 / 003 / 004 は 1 バイトも書き換えない (適用済み migration の改変は
-- ledger の checksum 照合で loud fail する。004 のコメントも直せないので、
-- 誤りの訂正はこのファイルが持つ)。

GRANT SELECT, INSERT, UPDATE, DELETE ON kintai.fold_gate TO kintai_writer;

-- reader は畳んだ結果を読むだけ (001 の `GRANT SELECT` と同じ扱い)。gate は
-- 「いつの入力で畳んだか」の監査材料になるので読めるようにしておく。
GRANT SELECT ON kintai.fold_gate TO kintai_reader;

ALTER DEFAULT PRIVILEGES IN SCHEMA kintai
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO kintai_writer;

ALTER DEFAULT PRIVILEGES IN SCHEMA kintai
    GRANT SELECT ON TABLES TO kintai_reader;
