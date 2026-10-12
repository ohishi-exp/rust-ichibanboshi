#!/usr/bin/env bash
# オンプレ版の売上 (uriage) の state.db と raw を D1 / R2 へ移すための SQL と適用手順を作る (Refs #322)。
#
#   export-state-db.sh <host> [--db <state.db のパス>] [--raw-dir <raw のディレクトリ>] [--out <出力先>]
#
#   <host>     ssh の宛先 (必須。スクリプトには書かない。~/.ssh/config の別名でもよい)
#   --db       オンプレの state.db。省略時はオンプレの既定値 /opt/ichibanboshi/state.db
#   --raw-dir  オンプレの raw ディレクトリ。省略時はオンプレの既定値 /opt/ichibanboshi/raw
#   --out      出力先。省略時は mktemp -d。repo の外で、空かまだ無いディレクトリでなければならない
#
# やること: オンプレ上の python3 で state.db を読み取り専用で開いて写し (conn.backup)、写しと raw を手元へ持ってきて、
# export_state_db.py generate で SQL・apply.sh・verify.sql・expected.txt を作る。
# **D1 にも R2 にも書かない** (--apply のような口も無い)。本番への適用は out/apply.sh を見てから人が流す。
# 実データ (state.db・raw・生成した SQL) は repo に入れない。出力先が repo の下なら止まる。
#
# 売上の移送は 2 回流す (Worker の受け入れ確認の前と、切り替える直前)。何度流しても同じ結果になる (000 が DELETE)。
# PR-F (オンプレ版の売上の撤去) でこのスクリプトも消す。
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
py="$script_dir/export_state_db.py"

usage() {
  sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

host=""
db="/opt/ichibanboshi/state.db"
raw_dir="/opt/ichibanboshi/raw"
out=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --db) [ "$#" -ge 2 ] || usage; db="$2"; shift 2 ;;
    --raw-dir) [ "$#" -ge 2 ] || usage; raw_dir="$2"; shift 2 ;;
    --out) [ "$#" -ge 2 ] || usage; out="$2"; shift 2 ;;
    -h | --help) usage ;;
    -*) echo "不明なオプション: $1" >&2; usage ;;
    *)
      [ -z "$host" ] || usage
      host="$1"; shift ;;
  esac
done
[ -n "$host" ] || usage

# 出力先: 無いか空であること。repo の下なら止める
if [ -z "$out" ]; then
  out="$(mktemp -d)"
fi
mkdir -p -- "$out"
if [ -n "$(ls -A -- "$out")" ]; then
  echo "出力先が空でない: $out (前回の生成物と混ざるので使わない)" >&2
  exit 1
fi
out="$(cd "$out" && pwd -P)"
repo_root="$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null || true)"
if [ -n "$repo_root" ]; then
  repo_root="$(cd "$repo_root" && pwd -P)"
  case "$out/" in
    "$repo_root"/*)
      rmdir -- "$out" 2>/dev/null || true
      echo "出力先が repo の下: $out (実データを repo に入れない。repo の外を指定する)" >&2
      exit 1 ;;
  esac
fi

remote_tmp=""
cleanup() {
  if [ -n "$remote_tmp" ]; then
    ssh -- "$host" rm -f "$(printf '%q' "$remote_tmp")" </dev/null || true
  fi
}
trap cleanup EXIT

# 1. 取得: オンプレ上の python3 に export_state_db.py を標準入力で渡し、state.db の写しを作らせる
remote_tmp="$(ssh -- "$host" python3 - backup "$(printf '%q' "$db")" <"$py")"
if ! [[ "$remote_tmp" =~ ^/[A-Za-z0-9_./-]+$ ]]; then
  remote_tmp=""
  echo "オンプレから想定外の応答 (写しのパスではない)" >&2
  exit 1
fi
ssh -- "$host" cat "$(printf '%q' "$remote_tmp")" </dev/null >"$out/state.db"

# raw は tar で丸ごと (約 200KB)。どれを R2 に置くかは generate が recalc_jobs.raw_path から決める
mkdir -p -- "$out/raw"
ssh -- "$host" tar -C "$(printf '%q' "$raw_dir")" -cf - . </dev/null | tar -C "$out/raw" --no-same-owner -xf -

# 2. 生成
python3 "$py" generate --db "$out/state.db" --raw-dir "$out/raw" --remote-raw-dir "$raw_dir" --out "$out"

echo "出力先: $out"
echo "次: $out/apply.sh を読んでから bash で流す。流した後に $out/verify.sql を D1 に流し、$out/expected.txt と突き合わせる"
