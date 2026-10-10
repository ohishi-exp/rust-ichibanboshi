#!/usr/bin/env bash
# check-exposure.sh の陰性対照。wrangler.toml を tomllib で読んだ dict を 1 か所ずつ崩して TOML に書き戻し、
# (a)〜(e)・(g) それぞれで exit 1 になること、元のまま・書き戻しただけなら exit 0 になることを確かめる。
# 4 つ目の引数を渡した行は、出力にその文字列 (どの検査が落としたか) があることも確かめる。
# 文字列の特定の表の直前に行を挿す作りにはしない (表の中身に紛れて別の表のキーになる — rust-alc-api#698)。
# CI で check-exposure.sh の直後に走る。
#
#   bash scripts/check-exposure-test.sh [wrangler.toml]   (既定 worker/wrangler.toml。workers/kintai で)
set -euo pipefail
cd "$(dirname "$0")/.."

BASE="${1:-worker/wrangler.toml}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
fail=0

expect() { # $1 = 期待する exit, $2 = label, $3 = wrangler.toml, $4 = 出力に在るべき文字列 (任意)
  local want="$1" label="$2" got
  if bash scripts/check-exposure.sh "$3" >"$tmp/out" 2>&1; then got=0; else got=1; fi
  if [ "$got" != "$want" ]; then
    echo "FAIL ${label}: exit ${got}, want ${want}"; sed 's/^/     /' "$tmp/out"; fail=1
  elif [ -n "${4:-}" ] && ! grep -qF -- "$4" "$tmp/out"; then
    echo "FAIL ${label}: 出力に「$4」が無い"; sed 's/^/     /' "$tmp/out"; fail=1
  else
    echo "ok   ${label} (exit ${got})"
  fi
}

# $1 = 期待する exit, $2 = label, $3 = python の式 (cfg を書き換える。空なら書き戻すだけ), $4 = expect の $4
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
  expect "$1" "$2" "$tmp/w.toml" "${4:-}"
}

expect 0 "${BASE} そのまま" "$BASE"
mutate 0 "書き戻しただけ (崩していない)" ''
# (a)
mutate 1 "(a) workers_dev = true" 'cfg["workers_dev"] = True'
mutate 1 "(a) workers_dev を消す (明示でない)" 'del cfg["workers_dev"]'
mutate 1 "(a) preview_urls = true" 'cfg["preview_urls"] = True'
mutate 1 "(a) preview_urls を消す (明示でない)" 'del cfg["preview_urls"]'
# (b)
mutate 1 "(b) route を足す" 'cfg["route"] = "kintai.example.com/*"'
mutate 1 "(b) routes (custom_domain) を足す" \
  'cfg["routes"] = [{"pattern": "kintai.example.com", "custom_domain": True}]'
# (c)
mutate 1 "(c) env 表を足す" 'cfg["env"] = {"staging": {"name": "ichibanboshi-kintai-staging", "workers_dev": False, "preview_urls": False}}'
# (d)
mutate 1 "(d) vpc_services を消す" 'del cfg["vpc_services"]'
mutate 1 "(d) KINTAI_MARIADB_VPC の binding 名を変える" 'cfg["vpc_services"][0]["binding"] = "OTHER_VPC"'
mutate 1 "(d) vpc_services を env 側へ動かす" \
  'cfg["env"] = {"prod": {"vpc_services": cfg.pop("vpc_services")}}'
# (e) LOCAL_ で始まる var はどこに置いても落ちる (ichiban の 2 つの名前・任意の名前・表の奥・vars 以外のキー)
mutate 1 "(e) vars に LOCAL_SQL_ADDR" 'cfg.setdefault("vars", {})["LOCAL_SQL_ADDR"] = "127.0.0.1:3306"'
mutate 1 "(e) vars に LOCAL_KINTAI_MARIADB_JSON" 'cfg.setdefault("vars", {})["LOCAL_KINTAI_MARIADB_JSON"] = "{}"'
mutate 1 "(e) vars に LOCAL_ で始まる任意の名前" 'cfg.setdefault("vars", {})["LOCAL_X"] = "1"'
mutate 1 "(e) 表の配列の奥に LOCAL_" 'cfg["vpc_services"][0]["LOCAL_ADDR"] = "127.0.0.1:3306"'
# LOCAL_ で始まらないもの (前後の一致ではなく接頭辞の一致であること) は通る
mutate 0 "(e) vars に NOT_LOCAL_X (接頭辞でない)" 'cfg.setdefault("vars", {})["NOT_LOCAL_X"] = "1"'
# (g) Supabase への口 KINTAI_HYPERDRIVE はトップレベルにだけ (env 等へ置けば (c) と別に (g) でも落ちる)
mutate 1 "(g) hyperdrive を消す" 'del cfg["hyperdrive"]' "hyperdrive の KINTAI_HYPERDRIVE が無い"
mutate 1 "(g) KINTAI_HYPERDRIVE の binding 名を変える" 'cfg["hyperdrive"][0]["binding"] = "OTHER_HD"' \
  "hyperdrive の KINTAI_HYPERDRIVE が無い"
mutate 1 "(g) hyperdrive を env.staging へ動かす" \
  'cfg["env"] = {"staging": {"hyperdrive": cfg.pop("hyperdrive")}}' "env.staging.hyperdrive がある"
mutate 1 "(g) トップレベルに残したまま env.staging にも置く" \
  'cfg["env"] = {"staging": {"hyperdrive": [dict(cfg["hyperdrive"][0])]}}' "env.staging.hyperdrive がある"
mutate 1 "(g) env 以外の表の奥に hyperdrive" 'cfg["observability"]["hyperdrive"] = [dict(cfg["hyperdrive"][0])]' \
  "observability.hyperdrive がある"
# (h) 書き込みの口 (worker/src の Route::Write) があるなら KINTAI_WRITE_TOKEN の binding が要る
mutate 1 "(h) KINTAI_WRITE_TOKEN を消す" \
  'cfg["secrets_store_secrets"] = [s for s in cfg["secrets_store_secrets"] if s["binding"] != "KINTAI_WRITE_TOKEN"]' \
  "secrets_store_secrets の KINTAI_WRITE_TOKEN が無い"
mutate 1 "(h) KINTAI_WRITE_TOKEN の binding 名を変える" \
  'next(s for s in cfg["secrets_store_secrets"] if s["binding"] == "KINTAI_WRITE_TOKEN")["binding"] = "WRITE_TOKEN"' \
  "secrets_store_secrets の KINTAI_WRITE_TOKEN が無い"
mutate 1 "(h) KINTAI_WRITE_TOKEN の secret_name を変える" \
  'next(s for s in cfg["secrets_store_secrets"] if s["binding"] == "KINTAI_WRITE_TOKEN")["secret_name"] = "OTHER"' \
  "secret_name が KINTAI_WRITE_TOKEN でない"
mutate 1 "(h) KINTAI_WRITE_TOKEN の store_id を変える" \
  'next(s for s in cfg["secrets_store_secrets"] if s["binding"] == "KINTAI_WRITE_TOKEN")["store_id"] = "0" * 32' \
  "store_id が KINTAI_MARIADB と違う"
mutate 1 "(h) secrets_store_secrets を env.staging へ動かす" \
  'cfg["env"] = {"staging": {"secrets_store_secrets": cfg.pop("secrets_store_secrets")}}' \
  "env.staging.secrets_store_secrets がある"
# 書き込みの口が無いソース (Route::Write が無い) なら binding が無くても通る (検査が src を見ていることの対照)
mkdir -p "$tmp/src-no-write"
echo 'fn main() {}' >"$tmp/src-no-write/lib.rs"
python3 - "$BASE" "$tmp/no-token.toml" <<'PY'
import re
import sys

text = open(sys.argv[1], encoding="utf-8").read()
# KINTAI_WRITE_TOKEN の表 ([[secrets_store_secrets]] から次の空行まで) を消す
text, n = re.subn(r"\[\[secrets_store_secrets\]\]\nbinding = \"KINTAI_WRITE_TOKEN\"\n(?:[^\n]+\n)*", "", text)
assert n == 1, n
open(sys.argv[2], "w", encoding="utf-8").write(text)
PY
KINTAI_WORKER_SRC="$tmp/src-no-write" expect 0 "(h) 書き込みの口が無いなら KINTAI_WRITE_TOKEN が無くても通る" "$tmp/no-token.toml"
expect 1 "(h) 書き込みの口があるのに KINTAI_WRITE_TOKEN が無い (同じ toml・実物の src)" "$tmp/no-token.toml" \
  "secrets_store_secrets の KINTAI_WRITE_TOKEN が無い"
# (f) は warning だけ: service_id を実値らしくしても、プレースホルダのままでも exit 0
mutate 0 "(f) service_id を入れた (warning 無し)" \
  'cfg["vpc_services"][0]["service_id"] = "11111111-1111-1111-1111-111111111111"'

if bash scripts/check-exposure.sh "$tmp/none.toml" >/dev/null 2>&1; then
  echo "FAIL 存在しない toml: exit 0, want 非 0"; fail=1
else
  echo "ok   存在しない toml (非 0)"
fi

exit "$fail"
