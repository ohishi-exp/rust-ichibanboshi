#!/usr/bin/env bash
#
# migrate_kintai.sh — migrations/*.sql を kintai 用 PostgreSQL (Supabase) へ適用する
#
# Refs ohishi-exp/rust-ichibanboshi#205 実装計画 03。
#
# 使い方:
#   KINTAI_DATABASE_URL='postgres://...' bash scripts/migrate_kintai.sh            # 適用
#   KINTAI_DATABASE_URL='postgres://...' bash scripts/migrate_kintai.sh --dry-run  # 計画だけ
#   KINTAI_DATABASE_URL='postgres://...' bash scripts/migrate_kintai.sh --status   # 適用状況
#
# ## なぜ `DATABASE_URL` ではないのか
#
# alc の migrate バイナリは `DATABASE_URL` を読むが、この repo では `DATABASE_*` が
# 既に **SQL Server** の名前空間として使われている (`src/config.rs` の
# `DATABASE_ENABLED` / `DATABASE_HOST` / `DATABASE_INSTANCE` …、#208)。ここで
# `DATABASE_URL` を採ると「売上の SQL Server の URL」と読める名前で別 DB を指す
# ことになる。secret 名も `kintai-database-url` なので、それに合わせて
# `KINTAI_DATABASE_URL` にした。`DATABASE_URL` だけが設定されている場合は黙って
# 使わず、名前を指して落ちる。
#
# ## なぜ Rust ではなく psql なのか (= postgres client を今 足さない理由)
#
# `rust-alc-api` は `src/bin/migrate.rs` の `sqlx::migrate!("./migrations")` で
# 適用し、prod では Cloud Run job として走らせる。こちらで同じ形を今作ると:
#
#   - バイナリから PostgreSQL を触る呼び出し元がまだ 1 つも無い。最初の呼び出し元は
#     #205 の 04 / 05 (push) と G6 (読み先の Postgres 実装) で、いずれも別項目
#   - 「テストは DB も環境変数も要らない」という本 repo の前提 (Makefile 冒頭 /
#     coverage_100.toml 冒頭) に、100% gate で覆えない DB 必須コードが 1 個だけ
#     生える。gate の穴を先に開けることになる
#   - migrate job を差し込む deploy 経路がまだ無い (G7 が Cloud Run service 定義)
#
# そこで **ledger を sqlx と同形にする**ことで、Rust 側を後回しにしても後から
# `sqlx::migrate!` がそのまま引き継げる形にした:
#
#   - テーブル定義は sqlx-postgres 0.8 の `ensure_migrations_table` と 1 文字同じ
#     (sqlx-postgres-0.8.6/src/migrate.rs:119-126)
#   - version = ファイル名の最初の `_` より前の整数 (`001` -> 1)
#   - description = 残りから `.sql` を落として `_` を空白に (`kintai schema`)
#   - checksum = ファイル内容の **SHA-384** (sqlx-core/src/migrate/migration.rs:25)
#   - execution_time = ナノ秒。ledger 行は migration 本体と同一トランザクション内で
#     INSERT し (-1)、commit 後に UPDATE する — sqlx の apply と同じ順序
#
# → `sqlx migrate run` を後で足しても 001 は「適用済み・checksum 一致」と見えるので
#   再適用されない。逆にこのスクリプトで 002 を足しても sqlx から見えるので、
#   移行期にどちらを使っても状態が割れない。
#
# ## 適用済み migration を書き換えたら loud fail する
#
# `rust-alc-api` CLAUDE.md「適用済み migration は絶対に変更しない (checksum で
# 起動不能)」。この規範を成り立たせているのは sqlx の checksum 照合そのものなので、
# 適用器を差し替えるなら照合も一緒に持ってくる必要がある。ここで実装している。
#
# ## CI での扱い
#
# `ci.yml` の migration job が postgres service に対してこのスクリプトを流し、
# 続けて `scripts/verify_kintai_rls.sh` で RLS を assert する。alc の
# migration-safety-check (rollback 不可パターンを PR コメントで警告) は移植して
# いない — 対象が 001 の 1 本だけで、まだ「適用済みの表を後から縮める」段階に
# 無いため。002 以降で ALTER / DROP を書き始める時に移す。

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$REPO_ROOT/migrations}"

# sqlx は database 名から lock id を作るが、こちらは固定値でよい
# (同時に流すのはこのスクリプトと sqlx だけで、どちらも同じ id を取る必要は無い —
#  必要なのは「このスクリプトの多重起動を直列化すること」)。
ADVISORY_LOCK_ID=205030001

DRY_RUN=0
STATUS_ONLY=0
for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=1 ;;
    --status)  STATUS_ONLY=1 ;;
    -h|--help) sed -n '1,62p' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

if [[ -z "${KINTAI_DATABASE_URL:-}" ]]; then
  echo "ERROR: KINTAI_DATABASE_URL is not set" >&2
  echo "       本番は Secret Manager の kintai-database-url を使う。" >&2
  if [[ -n "${DATABASE_URL:-}" ]]; then
    echo "       (DATABASE_URL は設定されているが使わない — この repo の DATABASE_* は" >&2
    echo "        売上の SQL Server を指す名前空間。KINTAI_DATABASE_URL に入れ直すこと)" >&2
  fi
  exit 2
fi
DATABASE_URL="$KINTAI_DATABASE_URL"

if ! command -v psql >/dev/null 2>&1; then
  echo "ERROR: psql not found (apt-get install postgresql-client)" >&2
  exit 2
fi

# migration に渡す psql 変数 (Refs #205)。
#
# 003 が `ALTER ROLE kintai_writer WITH PASSWORD :'kintai_writer_password'` を
# 打つ。**値は migration に書かない** — git に残るのは参照だけで、実値は
# GitHub org secret `KINTAI_WRITER_PASSWORD` (GCP Secret Manager が正) から来る。
#
# 空のまま流すと `PASSWORD ''` が**成功**して「ロールはあるが誰も認証できない」
# 状態が静かに出来上がるので、適用対象がこの変数を参照していたら空を弾く
# (下の apply ループ)。参照していない migration しか無いときは要求しない —
# 001 / 002 だけの環境や --status で無関係な secret を必須にしない。
#
# 値が psql の argv に載る (= ps で見える) 点は承知のうえ。CI runner は
# 使い捨ての単独プロセスで、代替 (`\getenv`) は psql 16 以降にしか無い。
KINTAI_WRITER_PASSWORD="${KINTAI_WRITER_PASSWORD:-}"
PSQL_MIGRATION_VARS=(-v "kintai_writer_password=${KINTAI_WRITER_PASSWORD}")

# psql は 1 引数だけの実行に使う小さなラッパ。ON_ERROR_STOP でエラーを必ず伝播。
pq() {
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -qtAX "$@"
}

sha384_of() {
  sha384sum "$1" | cut -d' ' -f1
}

# ── ledger を用意 (sqlx と同形) ────────────────────────────────────────
ensure_ledger() {
  pq -c "
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
);" >/dev/null
}

if [[ $STATUS_ONLY -eq 1 ]]; then
  ensure_ledger
  echo "== applied migrations =="
  psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -X -c \
    "SELECT version, description, success, installed_on, encode(checksum,'hex') AS checksum
       FROM _sqlx_migrations ORDER BY version;"
  exit 0
fi

# ── ファイル一覧 (sqlx と同じ「version 昇順」) ─────────────────────────
shopt -s nullglob
files=("$MIGRATIONS_DIR"/*.sql)
shopt -u nullglob
if [[ ${#files[@]} -eq 0 ]]; then
  echo "ERROR: no migrations found in $MIGRATIONS_DIR" >&2
  exit 2
fi

ensure_ledger

# dirty (success = false) が残っていたら手当てが要る。黙って続けない。
dirty="$(pq -c "SELECT version FROM _sqlx_migrations WHERE success = false ORDER BY version LIMIT 1")"
if [[ -n "$dirty" ]]; then
  echo "ERROR: migration $dirty is dirty (success = false)" >&2
  echo "       前回の適用が途中で死んでいる。手で状態を確認してから ledger 行を消すこと。" >&2
  exit 1
fi

applied_count=0
skipped_count=0

for file in "${files[@]}"; do
  base="$(basename "$file")"

  # sqlx と同じ parse: 最初の `_` で 2 分割し、前半を整数 version、
  # 後半から `.sql` を落として `_` を空白にしたものを description にする。
  if [[ "$base" != *_*.sql ]]; then
    echo "ERROR: bad migration filename: $base (expected <VERSION>_<description>.sql)" >&2
    exit 2
  fi
  version_raw="${base%%_*}"
  rest="${base#*_}"
  if [[ ! "$version_raw" =~ ^[0-9]+$ ]]; then
    echo "ERROR: bad migration version prefix in $base" >&2
    exit 2
  fi
  version=$((10#$version_raw))
  description="${rest%.sql}"
  description="${description//_/ }"

  checksum="$(sha384_of "$file")"
  stored="$(pq -c "SELECT encode(checksum,'hex') FROM _sqlx_migrations WHERE version = $version")"

  if [[ -n "$stored" ]]; then
    if [[ "$stored" == "$checksum" ]]; then
      echo "skip    $base (version $version, already applied)"
      skipped_count=$((skipped_count + 1))
      continue
    fi
    echo "ERROR: checksum mismatch for $base (version $version)" >&2
    echo "       applied: $stored" >&2
    echo "       on disk: $checksum" >&2
    echo "       適用済み migration は変更しない。修正は新しいファイルを足すこと。" >&2
    exit 1
  fi

  if [[ $DRY_RUN -eq 1 ]]; then
    echo "would apply  $base (version $version, description '$description')"
    applied_count=$((applied_count + 1))
    continue
  fi

  echo "apply   $base (version $version)"
  # 変数を参照する migration を空の値で流さない (上の説明を参照)
  if grep -q "kintai_writer_password" "$file" && [[ -z "$KINTAI_WRITER_PASSWORD" ]]; then
    echo "ERROR: $base は KINTAI_WRITER_PASSWORD を要求します (未設定)" >&2
    echo "       空のまま流すと空パスワードが静かに設定され、誰も認証できなくなります。" >&2
    echo "       GCP Secret Manager の KINTAI_WRITER_PASSWORD を env に入れてください。" >&2
    exit 2
  fi

  start_ns=$(date +%s%N)

  # migration 本体と ledger 行を 1 トランザクションに入れる。途中で死んでも
  # 「流れたのに ledger に無い」状態を作らない (sqlx が #1966 で踏んだ形)。
  # execution_time は commit 後に測れないので -1 を入れて後で UPDATE する。
  #
  # 先頭の advisory lock は多重起動の直列化。二重適用そのものは version の PK が
  # 止めるが、それだと後発が duplicate key で落ちて「失敗した」ように見えるので、
  # 待たせてから「skip」と判断させる方に倒す。
  #
  # SQL 本体は `cat` で素通しする。psql の `\i` や変数展開を通すと migration の
  # 中身がスクリプト側の quoting に依存してしまう (`\i :'var'` は引用符ごと
  # ファイル名として渡り実際に落ちた)。stdin へ連結すればファイルのバイト列が
  # そのまま届く。
  # -o /dev/null は結果セット (advisory lock の 1 行) の捨て先。エラーと NOTICE は
  # psql が stderr に出すので握り潰されない。
  {
    printf 'BEGIN;\n'
    printf 'SELECT pg_advisory_xact_lock(%s);\n' "$ADVISORY_LOCK_ID"
    cat "$file"
    printf '\n'
    printf 'INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)\n'
    printf "VALUES (%s, \$mig\$%s\$mig\$, TRUE, decode('%s','hex'), -1);\n" \
      "$version" "$description" "$checksum"
    printf 'COMMIT;\n'
  } | psql "$DATABASE_URL" -v ON_ERROR_STOP=1 "${PSQL_MIGRATION_VARS[@]}" -qX -o /dev/null -f -

  elapsed_ns=$(( $(date +%s%N) - start_ns ))
  pq -c "UPDATE _sqlx_migrations SET execution_time = $elapsed_ns WHERE version = $version" >/dev/null
  echo "        applied in $((elapsed_ns / 1000000)) ms"
  applied_count=$((applied_count + 1))
done

if [[ $DRY_RUN -eq 1 ]]; then
  echo "dry-run: $applied_count to apply, $skipped_count already applied (nothing written)"
else
  echo "done: $applied_count applied, $skipped_count skipped"
fi
