#!/usr/bin/env bash
# check-exposure.sh の陰性対照。wrangler.toml を tomllib で読んだ dict を 1 か所ずつ崩して TOML に書き戻し、
# (a)〜(e)・(g) それぞれで exit 1 になること、元のまま・書き戻しただけなら exit 0 になることを確かめる。
# 文字列の特定の表の直前に行を挿す作りにはしない (表の中身に紛れて別の表のキーになる — rust-alc-api#698)。
# CI で check-exposure.sh の直後に走る。
#
#   bash scripts/check-exposure-test.sh [wrangler.toml]   (既定 worker/wrangler.toml)
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${1:-worker/wrangler.toml}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0

expect() { # $1 = 期待する exit, $2 = label, $3 = wrangler.toml
  local want="$1" label="$2" got
  if bash scripts/check-exposure.sh "$3" >"$tmp/out" 2>&1; then got=0; else got=1; fi
  if [ "$got" = "$want" ]; then
    echo "ok   ${label} (exit ${got})"
  else
    echo "FAIL ${label}: exit ${got}, want ${want}"; sed 's/^/     /' "$tmp/out"; fail=1
  fi
}

# $1 = 期待する exit, $2 = label, $3 = python の式 (cfg を書き換える。空なら書き戻すだけ)
mutate() {
  python3 - "$BASE" "$tmp/w.toml" "$3" <<'PY'
import copy
import json
import re
import sys
import tomllib

BARE = re.compile(r"^[A-Za-z0-9_-]+$")


def key(k):
    return k if BARE.match(k) else json.dumps(k, ensure_ascii=False)


def value(v):
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return repr(v)
    if isinstance(v, str):
        return json.dumps(v, ensure_ascii=False)
    if isinstance(v, list):
        return "[" + ", ".join(value(x) for x in v) + "]"
    if isinstance(v, dict):
        return "{ " + ", ".join(f"{key(k)} = {value(x)}" for k, x in v.items()) + " }"
    raise TypeError(type(v))


def is_table_array(v):
    return isinstance(v, list) and v and all(isinstance(x, dict) for x in v)


def dump(d, prefix=()):
    out = []
    for k, v in d.items():
        if not isinstance(v, dict) and not is_table_array(v):
            out.append(f"{key(k)} = {value(v)}")
    for k, v in d.items():
        name = ".".join(key(p) for p in prefix + (k,))
        if isinstance(v, dict):
            out.append(f"\n[{name}]")
            out.append(dump(v, prefix + (k,)))
        elif is_table_array(v):
            for item in v:
                out.append(f"\n[[{name}]]")
                out.append(dump(item, prefix + (k,)))
    return "\n".join(x for x in out if x != "")


with open(sys.argv[1], "rb") as f:
    cfg = tomllib.load(f)
before = copy.deepcopy(cfg)
if sys.argv[3]:
    exec(sys.argv[3])
    assert cfg != before, "mutation did not change the config"
text = dump(cfg) + "\n"
# 書き戻しが正しいこと: 読み直すと意図した dict になる
assert tomllib.loads(text) == cfg, "round trip changed the config"
open(sys.argv[2], "w").write(text)
PY
  expect "$1" "$2" "$tmp/w.toml"
}

expect 0 "${BASE} そのまま" "$BASE"
mutate 0 "書き戻しただけ (崩していない)" ''
# (a)
mutate 1 "(a) workers_dev = true" 'cfg["workers_dev"] = True'
mutate 1 "(a) workers_dev を消す (明示でない)" 'del cfg["workers_dev"]'
mutate 1 "(a) preview_urls = true" 'cfg["preview_urls"] = True'
mutate 1 "(a) preview_urls を消す (明示でない)" 'del cfg["preview_urls"]'
# (b)
mutate 1 "(b) route を足す" 'cfg["route"] = "kyuyo.example.com/*"'
mutate 1 "(b) routes (custom_domain) を足す" \
  'cfg["routes"] = [{"pattern": "kyuyo.example.com", "custom_domain": True}]'
# (c)
mutate 1 "(c) env 表を足す" 'cfg["env"] = {"staging": {"name": "ichibanboshi-kyuyo-staging", "workers_dev": False, "preview_urls": False}}'
# (d)
mutate 1 "(d) vpc_services を消す" 'del cfg["vpc_services"]'
mutate 1 "(d) KYUYO_VPC の binding 名を変える" 'cfg["vpc_services"][0]["binding"] = "OTHER_VPC"'
mutate 1 "(d) vpc_services を env 側へ動かす" \
  'cfg["env"] = {"prod": {"vpc_services": cfg.pop("vpc_services")}}'
# (e)
mutate 1 "(e) vars に LOCAL_SQL_ADDR" 'cfg.setdefault("vars", {})["LOCAL_SQL_ADDR"] = "127.0.0.1:1433"'
# (g)
mutate 1 "(g) vars に LOCAL_KYUYO_SQL_JSON" 'cfg.setdefault("vars", {})["LOCAL_KYUYO_SQL_JSON"] = "{}"'
# (f) は warning だけ: service_id を実値らしくしても、プレースホルダのままでも exit 0
mutate 0 "(f) service_id を入れた (warning 無し)" \
  'cfg["vpc_services"][0]["service_id"] = "11111111-1111-1111-1111-111111111111"'

if bash scripts/check-exposure.sh >/dev/null 2>&1; then
  echo "FAIL 引数なし: exit 0, want 非 0"; fail=1
else
  echo "ok   引数なし (非 0)"
fi

exit "$fail"
