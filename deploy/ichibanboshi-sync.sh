#!/bin/sh
#
# ichibanboshi sync の「実行月」を算出する薄いラッパー (Refs #205 実装計画 07)。
# systemd unit (ichibanboshi-sync.service) から当月/前月の 2 回呼ばれる。
#
# --- なぜ ExecStart= に直接書かずスクリプトにしたか ---
#
# systemd の ExecStart= はシェルを介さず execve する。`$(date +%Y-%m)` と書いても
# 展開されずリテラル文字列がそのまま引数になる。回避策は 2 つあるが、
#   (a) ExecStart=/bin/sh -c '...' でシェルを挟む
#   (b) このラッパーを置く                                  ← こちらを選んだ
# 理由:
#
#   1. unit ファイル中の `%` は systemd の指示子 (%i / %n / %H …) なので `%%` に
#      エスケープしないといけない。`date +%%Y-%%m` は目で追えないうえ、書き間違えても
#      systemd は黙って別の文字列を渡すだけで起動時エラーにならない (静かに壊れる)。
#   2. 前月の算出が 1 行に収まらない。`date -d 'last month'` は月末に壊れる
#      — 2026-07-31 の "last month" は 2026-06-31 → 2026-07-01 に正規化されるので
#      「前月」にならない。月初 (YYYY-MM-01) を経由する必要があり、その式を
#      ExecStart の 1 行に押し込むと (1) のエスケープ地獄と掛け算になる。
#   3. 設置手順の dry-run リハーサル (docs/setup-kintai-sync-timer.md) と systemd が
#      同じコードを通る。ExecStart に直書きすると手で回すときだけ別の式になり、
#      「月の算出が正しいか」そのものを検証できない。
#
# --- 使い方 ---
#
#   ichibanboshi-sync.sh current            # 当月を dry-run (--apply が無いので書かない)
#   ichibanboshi-sync.sh previous --apply   # 前月を実際に反映
#
# 第 2 引数以降は `ichibanboshi sync` にそのまま渡す。`--apply` を足すかどうかは
# 呼ぶ側 (unit ファイル / 手作業) の責任にしてある — 「書く/書かない」がこの
# スクリプトの中に隠れないようにするため。
#
# 終了コードは `ichibanboshi sync` のものをそのまま返す (exec)。0 = 成功。
# 引数の使い方を間違えた場合だけ 2 を返す。
#
# 依存: GNU date (`date -d`)。実行先の ohishi-data は Ubuntu (coreutils) なので満たす。
# /bin/sh は Ubuntu では dash だが、ここで使っている構文は POSIX の範囲。
set -eu

BIN="${ICHIBANBOSHI_BIN:-/opt/ichibanboshi/ichibanboshi}"
CONFIG="${ICHIBANBOSHI_CONFIG:-/opt/ichibanboshi/ichibanboshi.toml}"

# 「当月」は JST の当月でなければならない。UTC のまま算出すると毎月 1 日の
# 00:00-09:00 JST が前月に化けて、月初の当月分 sync が丸ごと空振りする。
# unit 側でも Environment=TZ=Asia/Tokyo を渡しているが、手で叩いたときに
# 取りこぼさないようここでも既定値を入れておく (既に TZ があればそちらを尊重)。
: "${TZ:=Asia/Tokyo}"
export TZ

which_month="${1:-}"
if [ -z "$which_month" ]; then
  echo "usage: ichibanboshi-sync.sh {current|previous} [extra args for 'ichibanboshi sync'...]" >&2
  exit 2
fi
shift

case "$which_month" in
  current)
    month="$(date +%Y-%m)"
    ;;
  previous)
    # 必ず月初を経由してから 1 か月引く (上のコメント 2 の月末バグを避ける)。
    month="$(date -d "$(date +%Y-%m-01) -1 month" +%Y-%m)"
    ;;
  *)
    echo "unknown selector: $which_month (expected 'current' or 'previous')" >&2
    exit 2
    ;;
esac

# journalctl -u ichibanboshi-sync で「どの月をどのモードで回したか」が残るようにする。
echo "ichibanboshi sync: selector=$which_month month=$month tz=$TZ extra_args=[$*]"

exec "$BIN" sync --month "$month" --config "$CONFIG" "$@"
