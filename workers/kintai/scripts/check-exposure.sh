#!/usr/bin/env bash
# 勤怠 Worker (ichibanboshi-kintai) が外から届かないこと・社内への口を本番の外へ漏らさないことを wrangler.toml で検査する
# (workers/ichiban/scripts/check-exposure.sh を写している)。
#   (a) トップレベルに workers_dev = false と preview_urls = false が **明示** されている
#   (b) route / routes が無い (custom_domain も routes の中に書く)
#   (c) env 表が無い (トップレベルだけで運用する。env を足すと上の検査を env ごとにやり直す必要がある)
#   (d) トップレベルに vpc_services がある (KINTAI_MARIADB_VPC = MariaDB への口。env 側へ動いていない)
#   (e) wrangler.toml のどこにも `LOCAL_` で始まる var (キー) が無い。この Worker はローカルで VPC や Secrets Store を
#       迂回する var を持たない (ichiban の LOCAL_SQL_ADDR / LOCAL_ICHIBAN_SQL_JSON に当たるものを作らない)
#   (f) vpc_services の service_id がプレースホルダのままなら warning (fail にはしない)
#   (g) トップレベルに hyperdrive の KINTAI_HYPERDRIVE (Supabase への口) があり、hyperdrive がトップレベル以外
#       (env.* を含む表の奥) のどこにも無い (本番の DB へ届く binding をトップレベルの外へ置かない)
# (a)〜(e)・(g) が 1 つでも違えば exit 1。CI で毎回走らせる。陰性対照は scripts/check-exposure-test.sh。
#
#   bash workers/kintai/scripts/check-exposure.sh [wrangler.toml]   (既定 workers/kintai/worker/wrangler.toml)
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [wrangler.toml]" >&2
  exit 2
fi
target="${1:-$(dirname "$0")/../worker/wrangler.toml}"

python3 - "$target" <<'PY'
import sys
import tomllib

PLACEHOLDER = "00000000-0000-0000-0000-000000000000"
path = sys.argv[1]
with open(path, "rb") as f:
    cfg = tomllib.load(f)

errors = []


def err(msg):
    errors.append(msg)
    print(f"::error file={path}::{msg}")


# (a) トップレベル: 明示的に false (省略は既定値に頼ることになるので許さない)
for key in ("workers_dev", "preview_urls"):
    if cfg.get(key) is not False:
        err(f"トップレベルに {key} = false がない (外から直接届く)")

# (b) route / routes
for key in ("route", "routes"):
    if key in cfg:
        err(f"トップレベルに {key} がある (外から直接届く)")

# (c) env 表
if "env" in cfg:
    err("env 表がある (この Worker はトップレベルだけで運用する)")

# (d) MariaDB への口 KINTAI_MARIADB_VPC はトップレベル
vpcs = cfg.get("vpc_services")
if not isinstance(vpcs, list) or not any(isinstance(v, dict) and v.get("binding") == "KINTAI_MARIADB_VPC" for v in vpcs):
    err("トップレベルに vpc_services の KINTAI_MARIADB_VPC が無い (MariaDB への口)")


# (e) LOCAL_ で始まるキーを、表・表の配列の奥まで探す ([vars]・[env.*.vars] 等どこに置いても落とす)
def local_keys(node, prefix=""):
    if isinstance(node, dict):
        for k, v in node.items():
            name = f"{prefix}.{k}" if prefix else k
            if k.startswith("LOCAL_"):
                yield name
            yield from local_keys(v, name)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from local_keys(v, f"{prefix}[{i}]")


for name in local_keys(cfg):
    err(f"{name} がある (LOCAL_ で始まる var は作らない。ローカルで VPC / Secrets Store を迂回する口になる)")

# (f) service_id のプレースホルダ (warning のみ)
for v in vpcs or []:
    if isinstance(v, dict) and v.get("service_id") == PLACEHOLDER:
        print(f"::warning file={path}::vpc_services {v.get('binding')} の service_id がプレースホルダのまま (VPC Service 作成後に入れる)")

# (g) Supabase への口 KINTAI_HYPERDRIVE はトップレベル。hyperdrive の表はトップレベル以外に置かない
hds = cfg.get("hyperdrive")
if not isinstance(hds, list) or not any(isinstance(h, dict) and h.get("binding") == "KINTAI_HYPERDRIVE" for h in hds):
    err("トップレベルに hyperdrive の KINTAI_HYPERDRIVE が無い (Supabase への口)")


def nested_hyperdrive(node, prefix=""):
    if isinstance(node, dict):
        for k, v in node.items():
            name = f"{prefix}.{k}" if prefix else k
            if k == "hyperdrive" and prefix:
                yield name
            yield from nested_hyperdrive(v, name)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from nested_hyperdrive(v, f"{prefix}[{i}]")


for name in nested_hyperdrive(cfg):
    err(f"{name} がある (hyperdrive はトップレベルにだけ置く。env 等へ置くと本番の DB への口が漏れる)")

if errors:
    sys.exit(1)
print(f"OK: {path} は workers_dev / preview_urls = false・route 無し・env 無し・vpc_services と hyperdrive はトップレベル・LOCAL_* 無し")
PY
