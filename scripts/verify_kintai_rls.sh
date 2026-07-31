#!/usr/bin/env bash
#
# verify_kintai_rls.sh — migrations/ が作る kintai スキーマの契約を実 PostgreSQL で assert する
#
# Refs ohishi-exp/rust-ichibanboshi#205 実装計画 03 / テスト計画。
#
# 検証するのは #205 のテスト計画のうち **DDL だけで決まる項目**:
#
#   - 読み取りロール kintai_reader が NOBYPASSRLS である
#     (postgres では RLS が素通りするので検証にならない → 実際に素通りすることも見せる)
#   - 読み取りロールで push (INSERT / UPDATE / DELETE) を実行すると失敗する
#   - RLS が実際にテナントを切る (writer が 2 テナント書き、reader は片方しか見えない)
#   - app.current_tenant_id 未設定の reader は SELECT ごと失敗する (fail-closed)
#   - JST の打刻が 9 時間ずれない (date_start 生成列が JST 日付になる)
#   - 日跨ぎ・月跨ぎ・39 時間拘束の勤務が day_parts で 2〜3 暦日に分かれ、各行 1440 分以内
#   - CHECK / 生成列 / FK CASCADE / 冪等キーが宣言どおり効く
#     (end_at > start_at、day_parts の 0..1440、php_diff.cause の正規表現、
#      diff_minutes の符号が php − rust、kintai_events の重複打刻が増えない)
#   - day_summaries が **勤務 1 本 = 1 行** である (002)
#     同じ暦日に複数の勤務が入る / PK が 4 列で date が shift_start_at より前 /
#     shifts への FK が ON DELETE CASCADE / date が始業時刻の JST 日付と一致する
#
# 打刻 → 集計の一致 (指紋・再計算・差分ゼロ) は 04 / 05 の担当なのでここでは見ない。
#
# 使い方:
#   # ローカル docker で使い捨てクラスタを立てて全部やる
#   bash scripts/verify_kintai_rls.sh --docker
#
#   # 既定の 55432 が塞がっているとき (別の作業が使っている等) はポートを変える
#   PGPORT_LOCAL=55433 bash scripts/verify_kintai_rls.sh --docker
#
#   # 既にある PostgreSQL に対して流す (superuser 相当の接続が必要)
#   KINTAI_DATABASE_URL='postgres://postgres:pw@localhost:5432/postgres' bash scripts/verify_kintai_rls.sh
#
# 環境変数名が DATABASE_URL ではない理由は scripts/migrate_kintai.sh のヘッダ参照
# (この repo の DATABASE_* は売上の SQL Server の名前空間)。
#
# 本番 Supabase に向けて実行しないこと。ロールの DROP / パスワード再設定と
# テストデータの投入を行う。破壊的操作なので使い捨ての DB 専用。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

USE_DOCKER=0
for arg in "$@"; do
  case "$arg" in
    --docker) USE_DOCKER=1 ;;
    -h|--help) sed -n '1,39p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

CONTAINER="kintai-rls-verify-$$"
cleanup() {
  if [[ $USE_DOCKER -eq 1 ]]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if [[ $USE_DOCKER -eq 1 ]]; then
  PGPORT_LOCAL="${PGPORT_LOCAL:-55432}"
  echo "== starting postgres:17 container ($CONTAINER) on port $PGPORT_LOCAL"
  docker run -d --rm --name "$CONTAINER" \
    -e POSTGRES_PASSWORD=verify \
    -p "127.0.0.1:$PGPORT_LOCAL:5432" \
    postgres:17 >/dev/null
  KINTAI_DATABASE_URL="postgres://postgres:verify@127.0.0.1:$PGPORT_LOCAL/postgres"
  export KINTAI_DATABASE_URL
  for _ in $(seq 1 60); do
    if psql "$KINTAI_DATABASE_URL" -qtAX -c 'SELECT 1' >/dev/null 2>&1; then break; fi
    sleep 1
  done
fi

# 003 が要求する psql 変数。**検証用の使い捨て DB なので固定値でよい** —
# 本番の値 (GCP Secret Manager) をここへ持ち込む理由が無い。未設定なら
# migrate_kintai.sh が 003 の適用を拒否して検証が始まらないので、既定を置く。
: "${KINTAI_WRITER_PASSWORD:=verify-only-not-a-real-password}"
export KINTAI_WRITER_PASSWORD

if [[ -z "${KINTAI_DATABASE_URL:-}" ]]; then
  echo "ERROR: KINTAI_DATABASE_URL is not set (or pass --docker)" >&2
  exit 2
fi
export KINTAI_DATABASE_URL
# 以降 superuser 接続は SUPER_URL、検証対象のロール接続は READER_URL / WRITER_URL。
SUPER_URL="$KINTAI_DATABASE_URL"

# 接続文字列を分解して、同じホスト/DB に別ロールで繋ぎ直せるようにする。
# postgres://user:pw@host:port/db 形式のみ受ける (Supabase の direct connection と同形)。
if [[ ! "$SUPER_URL" =~ ^postgres(ql)?://[^:/@]+(:[^@]*)?@([^:/@]+):([0-9]+)/(.+)$ ]]; then
  echo "ERROR: KINTAI_DATABASE_URL must look like postgres://user:pw@host:port/db" >&2
  exit 2
fi
PG_HOST="${BASH_REMATCH[3]}"
PG_PORT="${BASH_REMATCH[4]}"
PG_DB="${BASH_REMATCH[5]}"
PG_DB="${PG_DB%%\?*}"

VERIFY_PW="verify-$(date +%s)-$$"
READER_URL="postgres://kintai_reader:$VERIFY_PW@$PG_HOST:$PG_PORT/$PG_DB"
WRITER_URL="postgres://kintai_writer:$VERIFY_PW@$PG_HOST:$PG_PORT/$PG_DB"

TENANT_A='11111111-1111-1111-1111-111111111111'
TENANT_B='22222222-2222-2222-2222-222222222222'

PASS=0
FAIL=0

ok()   { PASS=$((PASS + 1)); printf '  ok   %s\n' "$1"; }
bad()  { FAIL=$((FAIL + 1)); printf '  FAIL %s\n' "$1"; }

# as <url> <sql> — 1 値を取り出す
as() { psql "$1" -v ON_ERROR_STOP=1 -qtAX -c "$2"; }

# expect_eq <label> <actual> <expected>
expect_eq() {
  if [[ "$2" == "$3" ]]; then ok "$1 (= $3)"; else bad "$1: expected '$3', got '$2'"; fi
}

# expect_err <label> <url> <sql> <substring>
# SQL が失敗し、かつエラーメッセージに <substring> を含むことを確認する。
expect_err() {
  local label="$1" url="$2" sql="$3" want="$4" out
  if out="$(psql "$url" -v ON_ERROR_STOP=1 -qtAX -c "$sql" 2>&1)"; then
    bad "$label: expected failure, but it succeeded"
  elif [[ "$out" == *"$want"* ]]; then
    ok "$label (rejected: $want)"
  else
    bad "$label: failed but message lacked '$want' -> $out"
  fi
}

# expect_ok_sql <label> <url> <sql>
expect_ok_sql() {
  local label="$1" url="$2" sql="$3" out
  if out="$(psql "$url" -v ON_ERROR_STOP=1 -qtAX -c "$sql" 2>&1)"; then
    ok "$label"
  else
    bad "$label: $out"
  fi
}

# ── 0. 使い捨てクラスタを既知の状態に戻す ──────────────────────────────
# migration は CREATE ROLE を素で書いている (issue 本文どおり)。ロールはクラスタ
# 単位なので、検証の作り直しでは先に落とす。migration 側に IF NOT EXISTS 相当を
# 持ち込まないためにここで面倒を見る。
echo "== resetting schema + roles"
psql "$SUPER_URL" -v ON_ERROR_STOP=1 -qX -o /dev/null <<'SQL'
DROP SCHEMA IF EXISTS kintai CASCADE;
DROP TABLE IF EXISTS _sqlx_migrations;
SQL
for role in kintai_reader kintai_writer; do
  psql "$SUPER_URL" -qtAX -o /dev/null -c "DROP ROLE IF EXISTS $role" 2>/dev/null || true
done

# ── 1. migration が流れる ──────────────────────────────────────────────
echo "== applying migrations"
bash "$REPO_ROOT/scripts/migrate_kintai.sh"

echo "== ledger (sqlx 互換)"
psql "$SUPER_URL" -v ON_ERROR_STOP=1 -X -c \
  "SELECT version, description, success, length(checksum) AS checksum_bytes
     FROM _sqlx_migrations ORDER BY version;"

echo "== 2 度目は skip する (冪等)"
# skip 数は migrations/*.sql の本数。増やしたらここも増やす (数え漏れを黙らせない)
MIGRATION_COUNT="$(ls -1 "$REPO_ROOT"/migrations/*.sql | wc -l | tr -d ' ')"
out="$(bash "$REPO_ROOT/scripts/migrate_kintai.sh")"
if [[ "$out" == *"0 applied, $MIGRATION_COUNT skipped"* ]]; then
  ok "re-run applies nothing ($MIGRATION_COUNT skipped)"
else
  bad "re-run: $out"
fi

# ── 2. ロールの属性 ────────────────────────────────────────────────────
echo "== roles"
expect_eq "kintai_reader は NOBYPASSRLS" \
  "$(as "$SUPER_URL" "SELECT rolbypassrls FROM pg_roles WHERE rolname='kintai_reader'")" "f"
expect_eq "kintai_writer は BYPASSRLS" \
  "$(as "$SUPER_URL" "SELECT rolbypassrls FROM pg_roles WHERE rolname='kintai_writer'")" "t"
expect_eq "kintai_reader は NOINHERIT" \
  "$(as "$SUPER_URL" "SELECT rolinherit FROM pg_roles WHERE rolname='kintai_reader'")" "f"
expect_eq "kintai_reader は superuser ではない" \
  "$(as "$SUPER_URL" "SELECT rolsuper FROM pg_roles WHERE rolname='kintai_reader'")" "f"

echo "== RLS が 7 表とも有効で、テナント分離ポリシーが 1 本ずつある"
# 6 -> 7 は 004 (kintai.fold_gate、Refs #205 実装計画 13) を足した分。
expect_eq "relrowsecurity = true の表の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='kintai' AND c.relkind='r' AND c.relrowsecurity")" "7"
expect_eq "policy の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_policies WHERE schemaname='kintai'")" "7"
expect_eq "WITH CHECK を明示していない (= USING が WITH CHECK として効く) policy の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_policies
       WHERE schemaname='kintai' AND with_check IS NULL AND cmd='ALL'")" "7"

# ── 3. reader / writer で実際に繋ぐ ────────────────────────────────────
# migration はパスワードを持たないので、検証用にここで付ける (テスト scaffolding)。
psql "$SUPER_URL" -v ON_ERROR_STOP=1 -qtAX -o /dev/null \
  -v pw="$VERIFY_PW" <<'SQL'
ALTER ROLE kintai_reader PASSWORD :'pw';
ALTER ROLE kintai_writer PASSWORD :'pw';
SQL

# ── 3b. GRANT の網羅 (Refs #205 の 20) ────────────────────────────────
#
# `GRANT ... ON ALL TABLES IN SCHEMA` は**その時点の表にしか効かない**。001 以降に
# 作られた表は `ALTER DEFAULT PRIVILEGES` (005) が入るまで権限ゼロで生まれていた。
# 004 の `kintai.fold_gate` がそれを踏み、本番の `POST /api/kintai/recalc` が
# `permission denied for table fold_gate` → 502 で落ちた (2026-07-31)。
#
# **表を名指しせず「1 表でも権限の欠けたものがあれば落ちる」形にする** — 次に表が
# 増えたときも同じ漏れを CI が捕まえる。
echo "== 全表に writer / reader の GRANT が行き渡っている"
expect_eq "writer の 4 権限が欠けた表の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='kintai' AND c.relkind='r'
         AND NOT (has_table_privilege('kintai_writer', c.oid, 'SELECT')
              AND has_table_privilege('kintai_writer', c.oid, 'INSERT')
              AND has_table_privilege('kintai_writer', c.oid, 'UPDATE')
              AND has_table_privilege('kintai_writer', c.oid, 'DELETE'))")" "0"
expect_eq "reader の SELECT が欠けた表の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='kintai' AND c.relkind='r'
         AND NOT has_table_privilege('kintai_reader', c.oid, 'SELECT')")" "0"

# `ALTER DEFAULT PRIVILEGES` (005) は**それを実行したロールが作る表にしか効かない**
# (`FOR ROLE` を省くと `current_role`)。migration を流すロールと将来 `CREATE TABLE`
# するロールが同じ、という前提が崩れたらここで落ちる — 前提を人の記憶ではなく
# 検査で持つ。probe は作って測って必ず落とす。
echo "== 005 以降に作る表へ GRANT が自動で付く (ALTER DEFAULT PRIVILEGES)"
as "$SUPER_URL" "CREATE TABLE kintai.probe_default_acl (x int)" >/dev/null
expect_eq "新しい表に writer の 4 権限が自動で付く" \
  "$(as "$SUPER_URL" "SELECT has_table_privilege('kintai_writer','kintai.probe_default_acl','SELECT')
       AND has_table_privilege('kintai_writer','kintai.probe_default_acl','INSERT')
       AND has_table_privilege('kintai_writer','kintai.probe_default_acl','UPDATE')
       AND has_table_privilege('kintai_writer','kintai.probe_default_acl','DELETE')")" "t"
expect_eq "新しい表に reader の SELECT が自動で付く" \
  "$(as "$SUPER_URL" "SELECT has_table_privilege('kintai_reader','kintai.probe_default_acl','SELECT')")" "t"
as "$SUPER_URL" "DROP TABLE kintai.probe_default_acl" >/dev/null

# paper_drift だけ writer で 1 度も触られていなかった (他 6 表は下で触る)。
# 「触っていない表は権限が壊れていても気付けない」を残さない。
echo "== paper_drift も writer で往復できる"
expect_ok_sql "writer が paper_drift を INSERT" "$WRITER_URL" "
INSERT INTO kintai.paper_drift (tenant_id, driver_cd, date, paper_minutes, drift_minutes)
VALUES ('$TENANT_A', 1001, '2026-07-01', 480, -12);"
expect_eq "writer が paper_drift を SELECT" \
  "$(as "$WRITER_URL" "SELECT drift_minutes FROM kintai.paper_drift WHERE tenant_id='$TENANT_A'")" "-12"
as "$SUPER_URL" "DELETE FROM kintai.paper_drift" >/dev/null

echo "== 月ゲート (fold_gate) を writer が実際に読み書きできる"
expect_ok_sql "writer が fold_gate を UPSERT" "$WRITER_URL" "
INSERT INTO kintai.fold_gate (tenant_id, month, dtako_digest, punch_digest, logic_version)
VALUES ('$TENANT_A', '2026-06', repeat('a',64), repeat('b',64), '0123456789abcdef')
ON CONFLICT (tenant_id, month) DO UPDATE SET folded_at = now();"
expect_eq "writer が fold_gate を SELECT" \
  "$(as "$WRITER_URL" "SELECT logic_version FROM kintai.fold_gate WHERE tenant_id='$TENANT_A'")" \
  "0123456789abcdef"
expect_err "reader は fold_gate に書けない" "$READER_URL" "
INSERT INTO kintai.fold_gate (tenant_id, month, dtako_digest, punch_digest, logic_version)
VALUES ('$TENANT_A', '2026-05', repeat('c',64), repeat('d',64), '0123456789abcdef');" \
  "permission denied"
as "$SUPER_URL" "DELETE FROM kintai.fold_gate" >/dev/null

echo "== writer が 2 テナント分を書く (BYPASSRLS なので app.current_tenant_id 不要)"
expect_ok_sql "writer INSERT (2 tenants)" "$WRITER_URL" "
INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source, unko_no)
VALUES ('$TENANT_A', 1001, '2026-07-01T08:00:00+09:00', '始業', 'timecard', NULL),
       ('$TENANT_A', 1001, '2026-07-01T17:00:00+09:00', '終業', 'timecard', NULL),
       ('$TENANT_B', 2002, '2026-07-01T09:00:00+09:00', '始業', 'dtako',   'U-1');
"

echo "== 冪等キー: 同じ打刻を 2 度送っても増えない"
as "$WRITER_URL" "
INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source)
VALUES ('$TENANT_A', 1001, '2026-07-01T08:00:00+09:00', '始業', 'timecard')
ON CONFLICT DO NOTHING;" >/dev/null
expect_eq "kintai_events の総行数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.kintai_events")" "3"
expect_err "重複打刻を ON CONFLICT 無しで入れると PK 違反" "$WRITER_URL" "
INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source)
VALUES ('$TENANT_A', 1001, '2026-07-01T08:00:00+09:00', '始業', 'timecard');" \
  "duplicate key value"

echo "== JST: 打刻が 9 時間ずれない / date_start が JST 日付になる"
# 6/30 23:00 JST 始業 → 7/1 10:00 JST 終業 (= 月跨ぎ勤務)。UTC では 6/30 14:00。
expect_ok_sql "shifts INSERT (月跨ぎ)" "$WRITER_URL" "
INSERT INTO kintai.shifts (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
VALUES ('$TENANT_A', 1001, '2026-06-30T23:00:00+09:00', '2026-07-01T10:00:00+09:00',
        'timecard', repeat('a',64), '0123456789abcdef');"
expect_eq "date_start (JST 日付)" \
  "$(as "$SUPER_URL" "SELECT date_start FROM kintai.shifts WHERE driver_cd=1001")" "2026-06-30"
expect_eq "UTC で見ると前日 14:00 (= 同じ瞬間)" \
  "$(as "$SUPER_URL" "SELECT to_char(start_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI') FROM kintai.shifts WHERE driver_cd=1001")" \
  "2026-06-30 14:00"
expect_eq "duration_min (23:00 -> 翌 10:00 = 660)" \
  "$(as "$SUPER_URL" "SELECT duration_min FROM kintai.shifts WHERE driver_cd=1001")" "660"

echo "== 月跨ぎ勤務が暦日ビューで両月に現れる"
expect_ok_sql "day_parts INSERT (2 暦日)" "$WRITER_URL" "
INSERT INTO kintai.day_parts (tenant_id, driver_cd, shift_start_at, date, restraint_minutes, working_minutes, night_minutes)
VALUES ('$TENANT_A', 1001, '2026-06-30T23:00:00+09:00', '2026-06-30',  60,  60,  60),
       ('$TENANT_A', 1001, '2026-06-30T23:00:00+09:00', '2026-07-01', 600, 600, 300);"
expect_eq "day_parts の暦日数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_parts WHERE driver_cd=1001")" "2"
expect_eq "月境界の両側に 1 行ずつ" \
  "$(as "$SUPER_URL" "SELECT count(DISTINCT date_trunc('month', date)) FROM kintai.day_parts WHERE driver_cd=1001")" "2"
expect_eq "拘束の合計 (= 660、勤務ビューの duration_min と一致)" \
  "$(as "$SUPER_URL" "SELECT sum(restraint_minutes) FROM kintai.day_parts WHERE driver_cd=1001")" "660"

echo "== 39 時間拘束が 3 暦日に分かれ、各行 1440 分以内"
expect_ok_sql "39h shift + 3 day_parts" "$WRITER_URL" "
INSERT INTO kintai.shifts (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
VALUES ('$TENANT_A', 1003, '2026-07-01T22:00:00+09:00', '2026-07-03T13:00:00+09:00',
        'rest', repeat('b',64), '0123456789abcdef');
INSERT INTO kintai.day_parts (tenant_id, driver_cd, shift_start_at, date, restraint_minutes, working_minutes, night_minutes)
VALUES ('$TENANT_A', 1003, '2026-07-01T22:00:00+09:00', '2026-07-01',  120,  100,  60),
       ('$TENANT_A', 1003, '2026-07-01T22:00:00+09:00', '2026-07-02', 1440, 1000, 300),
       ('$TENANT_A', 1003, '2026-07-01T22:00:00+09:00', '2026-07-03',  780,  700, 120);"
expect_eq "day_parts が 3 暦日" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_parts WHERE driver_cd=1003")" "3"
expect_eq "各行 1440 分以内" \
  "$(as "$SUPER_URL" "SELECT bool_and(restraint_minutes <= 1440) FROM kintai.day_parts WHERE driver_cd=1003")" "t"
expect_eq "3 暦日の合計 = 2340 (39 時間)" \
  "$(as "$SUPER_URL" "SELECT sum(restraint_minutes) FROM kintai.day_parts WHERE driver_cd=1003")" "2340"
expect_ok_sql "day_summaries に同額を置く (勤務単位 = 始業日へ寄せる)" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1003, '2026-07-01', '2026-07-01T22:00:00+09:00', 'rest',
  2340, 1800, 540, 0, 480, 0, 1320, 0, 480, 480, 0, repeat('b',64), '0123456789abcdef');"
expect_eq "day_parts の合計が day_summaries.restraint_minutes と一致" \
  "$(as "$SUPER_URL" "SELECT (SELECT sum(restraint_minutes) FROM kintai.day_parts WHERE driver_cd=1003)
       = (SELECT restraint_minutes FROM kintai.day_summaries WHERE driver_cd=1003)")" "t"
expect_eq "over_24h 部分索引が 39 時間の行を拾う" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_summaries WHERE tenant_id='$TENANT_A' AND restraint_minutes > 1440")" "1"

# ── 3b. day_summaries が勤務単位である (migrations/002) ─────────────────
echo "== day_summaries の PK が勤務単位 (4 列。date は shift_start_at より前)"
expect_eq "PK の列 (順序込み)" \
  "$(as "$SUPER_URL" "SELECT string_agg(a.attname, ',' ORDER BY x.ord)
       FROM pg_constraint c
       CROSS JOIN LATERAL unnest(c.conkey) WITH ORDINALITY AS x(attnum, ord)
       JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = x.attnum
      WHERE c.conrelid = 'kintai.day_summaries'::regclass AND c.contype = 'p'")" \
  "tenant_id,driver_cd,date,shift_start_at"
expect_eq "shift_start_at は NOT NULL (DEFAULT 無しで足した = 空表でしか通らない)" \
  "$(as "$SUPER_URL" "SELECT attnotnull FROM pg_attribute
      WHERE attrelid = 'kintai.day_summaries'::regclass AND attname = 'shift_start_at'")" "t"
expect_eq "shift_start_at に DEFAULT は無い" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_attrdef d
      JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
     WHERE d.adrelid = 'kintai.day_summaries'::regclass AND a.attname = 'shift_start_at'")" "0"

echo "== shifts への FK が ON DELETE CASCADE で張られている"
expect_eq "FK の参照先" \
  "$(as "$SUPER_URL" "SELECT confrelid::regclass::text FROM pg_constraint
      WHERE conrelid = 'kintai.day_summaries'::regclass AND contype = 'f'")" "kintai.shifts"
expect_eq "FK の削除時動作 (c = CASCADE)" \
  "$(as "$SUPER_URL" "SELECT confdeltype FROM pg_constraint
      WHERE conrelid = 'kintai.day_summaries'::regclass AND contype = 'f'")" "c"

echo "== 001 の索引 2 本は 002 でも触っていない"
expect_eq "day_summaries の索引 (PK 以外)" \
  "$(as "$SUPER_URL" "SELECT string_agg(indexname, ',' ORDER BY indexname) FROM pg_indexes
      WHERE schemaname = 'kintai' AND tablename = 'day_summaries'
        AND indexname <> 'day_summaries_pkey'")" \
  "day_summaries_month,day_summaries_over_24h"
# 002 の眼目: over_24h が拾う行が「1 本の勤務」を指すようになった (暦日の合算ではない)
expect_eq "over_24h の行が実在の勤務 1 本を指す" \
  "$(as "$SUPER_URL" "SELECT (SELECT s.start_at FROM kintai.shifts s
                               WHERE s.tenant_id = '$TENANT_A' AND s.driver_cd = 1003)
                            = (SELECT d.shift_start_at FROM kintai.day_summaries d
                               WHERE d.tenant_id = '$TENANT_A' AND d.restraint_minutes > 1440)")" "t"

echo "== 同じ暦日に 4 勤務 (実データ: 乗務員 1726 / 2026-03-14、フェリー 2 本)"
# kosoku.rs:1936-1942 の実測。001 の PK (tenant_id, driver_cd, date) では 1 行しか入らない
expect_ok_sql "4 勤務ぶんの shifts" "$WRITER_URL" "
INSERT INTO kintai.shifts (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
VALUES ('$TENANT_A', 1726, '2026-03-14T06:00:00+09:00', '2026-03-14T06:01:00+09:00', 'rest', repeat('d',64), '0123456789abcdef'),
       ('$TENANT_A', 1726, '2026-03-14T06:30:00+09:00', '2026-03-14T06:46:00+09:00', 'rest', repeat('d',64), '0123456789abcdef'),
       ('$TENANT_A', 1726, '2026-03-14T08:00:00+09:00', '2026-03-14T09:22:00+09:00', 'rest', repeat('d',64), '0123456789abcdef'),
       ('$TENANT_A', 1726, '2026-03-14T13:00:00+09:00', '2026-03-14T16:42:00+09:00', 'rest', repeat('d',64), '0123456789abcdef');"
expect_ok_sql "同じ (tenant, driver, date) に 4 行入る" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1726, '2026-03-14', '2026-03-14T06:00:00+09:00', 'rest',   1,   1, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('d',64), '0123456789abcdef'),
       ('$TENANT_A', 1726, '2026-03-14', '2026-03-14T06:30:00+09:00', 'rest',  16,  16, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('d',64), '0123456789abcdef'),
       ('$TENANT_A', 1726, '2026-03-14', '2026-03-14T08:00:00+09:00', 'rest',  82,  82, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('d',64), '0123456789abcdef'),
       ('$TENANT_A', 1726, '2026-03-14', '2026-03-14T13:00:00+09:00', 'rest', 222, 222, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('d',64), '0123456789abcdef');"
expect_eq "1 暦日に day_summaries が 4 行" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_summaries WHERE driver_cd=1726 AND date='2026-03-14'")" "4"
expect_eq "拘束の合計 = 321 (1+16+82+222。潰れていない)" \
  "$(as "$SUPER_URL" "SELECT sum(restraint_minutes) FROM kintai.day_summaries WHERE driver_cd=1726")" "321"
expect_err "同じ勤務を 2 度書くと PK 違反 (冪等キーは勤務単位)" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1726, '2026-03-14', '2026-03-14T08:00:00+09:00', 'rest', 82, 82, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('d',64), '0123456789abcdef');" \
  "duplicate key value"

echo "== date は始業時刻の JST 日付でなければならない (CHECK)"
# 2026-03-15 05:00 JST = 2026-03-14 20:00 UTC。UTC 日付を書くと弾かれる = 9 時間ずれない
expect_ok_sql "JST 早朝始業の勤務 (UTC では前日)" "$WRITER_URL" "
INSERT INTO kintai.shifts (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
VALUES ('$TENANT_A', 1727, '2026-03-15T05:00:00+09:00', '2026-03-15T14:00:00+09:00',
        'timecard', repeat('e',64), '0123456789abcdef');"
expect_eq "UTC で見ると前日 20:00" \
  "$(as "$SUPER_URL" "SELECT to_char(start_at AT TIME ZONE 'UTC','YYYY-MM-DD HH24:MI') FROM kintai.shifts WHERE driver_cd=1727")" \
  "2026-03-14 20:00"
expect_ok_sql "date = 2026-03-15 (JST 日付) を通す" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1727, '2026-03-15', '2026-03-15T05:00:00+09:00', 'timecard',
  540, 480, 60, 0, 480, 0, 0, 0, 0, 0, 0, repeat('e',64), '0123456789abcdef');"
expect_err "date = 2026-03-14 (UTC 日付) を拒否" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1727, '2026-03-14', '2026-03-15T05:00:00+09:00', 'timecard',
  540, 480, 60, 0, 480, 0, 0, 0, 0, 0, 0, repeat('e',64), '0123456789abcdef');" \
  "violates check constraint"
# day_parts (勤務を 0 時で切って配る) の 2 日目の日付を day_summaries に書く取り違え。
# 1003 は 2026-07-01 22:00 JST 始業の 39 時間勤務なので、07-02 は day_parts 側の日付
expect_err "day_parts の 2 日目の日付を書くと拒否" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1003, '2026-07-02', '2026-07-01T22:00:00+09:00', 'rest',
  1440, 1000, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('b',64), '0123456789abcdef');" \
  "violates check constraint"
# date は整合させておく (CHECK ではなく FK が火を噴くことを確かめる)
expect_err "存在しない勤務への day_summaries を拒否" "$WRITER_URL" "
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_start_at, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 7777, '2026-07-01', '2026-07-01T22:00:00+09:00', 'rest',
  60, 60, 0, 0, 480, 0, 0, 0, 0, 0, 0, repeat('f',64), '0123456789abcdef');" \
  "violates foreign key constraint"

echo "== 勤務を 1 本消すと、その勤務のサマリだけが CASCADE で消える"
as "$WRITER_URL" "DELETE FROM kintai.shifts
   WHERE driver_cd=1726 AND start_at='2026-03-14T08:00:00+09:00'" >/dev/null
expect_eq "同じ暦日の残り 3 勤務は残る" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_summaries WHERE driver_cd=1726")" "3"
expect_eq "拘束の合計 = 239 (321 − 82)" \
  "$(as "$SUPER_URL" "SELECT sum(restraint_minutes) FROM kintai.day_summaries WHERE driver_cd=1726")" "239"

echo "== CHECK / FK が宣言どおり効く"
expect_err "end_at <= start_at を拒否" "$WRITER_URL" "
INSERT INTO kintai.shifts (tenant_id, driver_cd, start_at, end_at, shift_source, fingerprint, logic_version)
VALUES ('$TENANT_A', 9001, '2026-07-01T10:00:00+09:00', '2026-07-01T10:00:00+09:00',
        'timecard', repeat('c',64), '0123456789abcdef');" "violates check constraint"
expect_err "未知の state を拒否" "$WRITER_URL" "
INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source)
VALUES ('$TENANT_A', 9001, now(), '待機', 'dtako');" "violates check constraint"
expect_err "未知の source を拒否" "$WRITER_URL" "
INSERT INTO kintai.kintai_events (tenant_id, driver_cd, occurred_at, state, source)
VALUES ('$TENANT_A', 9001, now(), '始業', 'mariadb');" "violates check constraint"
expect_err "day_parts の 1441 分を拒否" "$WRITER_URL" "
INSERT INTO kintai.day_parts (tenant_id, driver_cd, shift_start_at, date, restraint_minutes, working_minutes, night_minutes)
VALUES ('$TENANT_A', 1003, '2026-07-01T22:00:00+09:00', '2026-07-04', 1441, 0, 0);" \
  "violates check constraint"
expect_err "始業日より前の暦日を拒否" "$WRITER_URL" "
INSERT INTO kintai.day_parts (tenant_id, driver_cd, shift_start_at, date, restraint_minutes, working_minutes, night_minutes)
VALUES ('$TENANT_A', 1003, '2026-07-01T22:00:00+09:00', '2026-06-30', 10, 10, 0);" \
  "violates check constraint"
expect_err "存在しない勤務への day_parts を拒否" "$WRITER_URL" "
INSERT INTO kintai.day_parts (tenant_id, driver_cd, shift_start_at, date, restraint_minutes, working_minutes, night_minutes)
VALUES ('$TENANT_A', 7777, '2026-07-01T22:00:00+09:00', '2026-07-01', 10, 10, 0);" \
  "violates foreign key constraint"
as "$WRITER_URL" "DELETE FROM kintai.shifts WHERE driver_cd=1003" >/dev/null
expect_eq "勤務を消すと day_parts も CASCADE で消える" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_parts WHERE driver_cd=1003")" "0"
expect_eq "勤務を消すと day_summaries も CASCADE で消える (002)" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_summaries WHERE driver_cd=1003")" "0"

echo "== php_diff: 符号は php − rust / cause は複合ラベルを通す"
expect_ok_sql "php_diff INSERT (原子 cause)" "$WRITER_URL" "
INSERT INTO kintai.php_diff (tenant_id, driver_cd, date, item, rust_minutes, php_minutes,
                             cause, explained_minutes, residual_minutes, tolerance_minutes)
VALUES ('$TENANT_A', 1001, '2026-07-01', 'restraint', 600, 660, 'ferry', 60, 0, 1);"
expect_eq "diff_minutes = php − rust" \
  "$(as "$SUPER_URL" "SELECT diff_minutes FROM kintai.php_diff WHERE driver_cd=1001")" "60"
for c in 'ferry+lunch' 'ferry+lunch+rounding' 'lunch+ferry' 'rounding' 'none' 'unknown' 'month-boundary' 'gap-midnight+ours-outside'; do
  expect_ok_sql "cause '$c' を通す" "$WRITER_URL" "
INSERT INTO kintai.php_diff (tenant_id, driver_cd, date, item, rust_minutes, php_minutes,
                             cause, explained_minutes, residual_minutes, tolerance_minutes)
VALUES ('$TENANT_A', 1002, '2026-07-01', 'restraint', 1, 2, '$c', 1, 0, 1)
ON CONFLICT (tenant_id, driver_cd, date, item) DO UPDATE SET cause = EXCLUDED.cause;"
done
expect_ok_sql "cause NULL を通す" "$WRITER_URL" "
INSERT INTO kintai.php_diff (tenant_id, driver_cd, date, item, rust_minutes, php_minutes,
                             cause, explained_minutes, residual_minutes, tolerance_minutes)
VALUES ('$TENANT_A', 1004, '2026-07-01', 'restraint', 1, 2, NULL, 0, 1, 1);"
for c in 'bogus' 'ferry+' '+ferry' 'ferry lunch' 'FERRY' 'rest_minus'; do
  expect_err "cause '$c' を拒否" "$WRITER_URL" "
INSERT INTO kintai.php_diff (tenant_id, driver_cd, date, item, rust_minutes, php_minutes,
                             cause, explained_minutes, residual_minutes, tolerance_minutes)
VALUES ('$TENANT_A', 1009, '2026-07-01', 'restraint', 1, 2, '$c', 1, 0, 1);" \
    "violates check constraint"
done
expect_err "item は restraint 以外を拒否 (working は未実装)" "$WRITER_URL" "
INSERT INTO kintai.php_diff (tenant_id, driver_cd, date, item, rust_minutes, php_minutes,
                             cause, explained_minutes, residual_minutes, tolerance_minutes)
VALUES ('$TENANT_A', 1009, '2026-07-01', 'working', 1, 2, 'ferry', 1, 0, 1);" \
  "violates check constraint"
expect_err "生成列 diff_minutes は手で入れられない" "$WRITER_URL" "
INSERT INTO kintai.php_diff (tenant_id, driver_cd, date, item, rust_minutes, php_minutes,
                             diff_minutes, cause, explained_minutes, residual_minutes, tolerance_minutes)
VALUES ('$TENANT_A', 1009, '2026-07-01', 'restraint', 1, 2, 999, 'ferry', 1, 0, 1);" \
  "cannot insert a non-DEFAULT value into column"

# ── 4. RLS が実際にテナントを切る ──────────────────────────────────────
echo "== reader (NOBYPASSRLS) は自テナントしか見えない"
expect_eq "reader が tenant A を見た件数" \
  "$(as "$READER_URL" "SET app.current_tenant_id = '$TENANT_A'; SELECT count(*) FROM kintai.kintai_events")" "2"
expect_eq "reader が tenant B を見た件数" \
  "$(as "$READER_URL" "SET app.current_tenant_id = '$TENANT_B'; SELECT count(*) FROM kintai.kintai_events")" "1"
expect_eq "reader が見える tenant_id は 1 つだけ" \
  "$(as "$READER_URL" "SET app.current_tenant_id = '$TENANT_A'; SELECT count(DISTINCT tenant_id) FROM kintai.kintai_events")" "1"
expect_eq "reader は他テナントの行を名指ししても 0 件" \
  "$(as "$READER_URL" "SET app.current_tenant_id = '$TENANT_A'; SELECT count(*) FROM kintai.kintai_events WHERE tenant_id = '$TENANT_B'")" "0"

echo "== app.current_tenant_id を設定しない reader は fail-closed"
expect_err "未設定の SELECT が失敗する" "$READER_URL" \
  "SELECT count(*) FROM kintai.kintai_events" "unrecognized configuration parameter"

echo "== postgres では RLS が素通りする (= postgres で検証してはいけない証拠)"
expect_eq "postgres が見た件数 (全テナント)" \
  "$(as "$SUPER_URL" "SET app.current_tenant_id = '$TENANT_A'; SELECT count(*) FROM kintai.kintai_events")" "3"
expect_eq "writer (BYPASSRLS) が見た件数 (全テナント)" \
  "$(as "$WRITER_URL" "SET app.current_tenant_id = '$TENANT_A'; SELECT count(*) FROM kintai.kintai_events")" "3"

# ── 5. 読み取りロールで push すると失敗する ────────────────────────────
echo "== reader で push (INSERT / UPDATE / DELETE) が失敗する"
for tbl in kintai_events shifts day_summaries day_parts paper_drift php_diff; do
  expect_err "reader INSERT INTO kintai.$tbl" "$READER_URL" \
    "SET app.current_tenant_id = '$TENANT_A'; INSERT INTO kintai.$tbl DEFAULT VALUES" \
    "permission denied for table $tbl"
done
expect_err "reader UPDATE" "$READER_URL" \
  "SET app.current_tenant_id = '$TENANT_A'; UPDATE kintai.kintai_events SET source = 'dtako'" \
  "permission denied for table kintai_events"
expect_err "reader DELETE" "$READER_URL" \
  "SET app.current_tenant_id = '$TENANT_A'; DELETE FROM kintai.kintai_events" \
  "permission denied for table kintai_events"
expect_err "reader は TRUNCATE もできない" "$READER_URL" \
  "TRUNCATE kintai.kintai_events" "permission denied for table kintai_events"
expect_err "reader は DDL (テーブル追加) もできない" "$READER_URL" \
  "CREATE TABLE kintai.sneaky (x int)" "permission denied for schema kintai"
expect_eq "reader が push を試みた後も総行数は変わらない" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.kintai_events")" "3"

# ── 6. 適用済み migration の書き換えは loud fail する ──────────────────
echo "== 適用済み migration を変えると checksum 照合で落ちる"
tmpdir="$(mktemp -d)"
cp "$REPO_ROOT/migrations/001_kintai_schema.sql" "$tmpdir/001_kintai_schema.sql"
printf '\n-- tampered\n' >> "$tmpdir/001_kintai_schema.sql"
if out="$(MIGRATIONS_DIR="$tmpdir" bash "$REPO_ROOT/scripts/migrate_kintai.sh" 2>&1)"; then
  bad "tampered migration was accepted"
elif [[ "$out" == *"checksum mismatch"* ]]; then
  ok "tampered migration rejected (checksum mismatch)"
else
  bad "tampered migration failed for the wrong reason: $out"
fi
rm -rf "$tmpdir"

# ── 結果 ───────────────────────────────────────────────────────────────
echo ""
echo "======================================"
echo " passed: $PASS   failed: $FAIL"
echo "======================================"
[[ $FAIL -eq 0 ]] || exit 1
