#!/usr/bin/env bash
#
# verify_kintai_rls.sh — migrations/001 が作る kintai スキーマの契約を実 PostgreSQL で assert する
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
#
# 打刻 → 集計の一致 (指紋・再計算・差分ゼロ) は 04 / 05 の担当なのでここでは見ない。
#
# 使い方:
#   # ローカル docker で使い捨てクラスタを立てて全部やる
#   bash scripts/verify_kintai_rls.sh --docker
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
    -h|--help) sed -n '1,33p' "${BASH_SOURCE[0]}"; exit 0 ;;
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
out="$(bash "$REPO_ROOT/scripts/migrate_kintai.sh")"
if [[ "$out" == *"0 applied, 1 skipped"* ]]; then ok "re-run applies nothing"; else bad "re-run: $out"; fi

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

echo "== RLS が 6 表とも有効で、テナント分離ポリシーが 1 本ずつある"
expect_eq "relrowsecurity = true の表の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
       WHERE n.nspname='kintai' AND c.relkind='r' AND c.relrowsecurity")" "6"
expect_eq "policy の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_policies WHERE schemaname='kintai'")" "6"
expect_eq "WITH CHECK を明示していない (= USING が WITH CHECK として効く) policy の数" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM pg_policies
       WHERE schemaname='kintai' AND with_check IS NULL AND cmd='ALL'")" "6"

# ── 3. reader / writer で実際に繋ぐ ────────────────────────────────────
# migration はパスワードを持たないので、検証用にここで付ける (テスト scaffolding)。
psql "$SUPER_URL" -v ON_ERROR_STOP=1 -qtAX -o /dev/null \
  -v pw="$VERIFY_PW" <<'SQL'
ALTER ROLE kintai_reader PASSWORD :'pw';
ALTER ROLE kintai_writer PASSWORD :'pw';
SQL

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
INSERT INTO kintai.day_summaries (tenant_id, driver_cd, date, shift_source,
  restraint_minutes, working_minutes, break_minutes, rest_minus_minutes, statutory_minutes,
  within_statutory_overtime_minutes, overtime_minutes, legal_holiday_minutes,
  night_minutes, overtime_night_minutes, legal_holiday_night_minutes, fingerprint, logic_version)
VALUES ('$TENANT_A', 1003, '2026-07-01', 'rest',
  2340, 1800, 540, 0, 480, 0, 1320, 0, 480, 480, 0, repeat('b',64), '0123456789abcdef');"
expect_eq "day_parts の合計が day_summaries.restraint_minutes と一致" \
  "$(as "$SUPER_URL" "SELECT (SELECT sum(restraint_minutes) FROM kintai.day_parts WHERE driver_cd=1003)
       = (SELECT restraint_minutes FROM kintai.day_summaries WHERE driver_cd=1003)")" "t"
expect_eq "over_24h 部分索引が 39 時間の行を拾う" \
  "$(as "$SUPER_URL" "SELECT count(*) FROM kintai.day_summaries WHERE tenant_id='$TENANT_A' AND restraint_minutes > 1440")" "1"

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
