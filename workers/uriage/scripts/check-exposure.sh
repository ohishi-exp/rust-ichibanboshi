#!/usr/bin/env bash
# 売上 Worker (ichibanboshi-uriage) が外から届かないこと・D1 への binding を本番の外へ漏らさないことを wrangler.toml で検査する
# (workers/ichiban/scripts/check-exposure.sh を手本にしている)。
#   (a) トップレベルに workers_dev = false と preview_urls = false が **明示** されている
#   (b) route / routes が無い (custom_domain も routes の中に書く)
#   (c) env 表が無い (トップレベルだけで運用する。env を足すと上の検査を env ごとにやり直す必要がある)
#   (d) トップレベルの d1_databases に URIAGE_DB (database_name = ichibanboshi-uriage・migrations_dir = "migrations") がある。
#       d1_databases はトップレベル以外 (表の奥) のどこにも置かない (本番の D1 へ届く binding をトップレベルの外へ漏らさない)
#   (e) services (他の Worker への binding) を持たない (トップレベルにも表の奥にも)
#   (f) LOCAL_ で始まるキーが無い ([vars]・[env.*.vars] 等どこにも。ローカルで VPC / Secrets Store を迂回する口になる)
#   (g) vpc_services (URIAGE_SQL_VPC・URIAGE_CAKEPHP_VPC)・secrets_store_secrets (ICHIBAN_SQL・URIAGE_WRITE_TOKEN)・
#       r2_buckets (URIAGE_R2 = bucket ichibanboshi-uriage) がトップレベルにあり、トップレベル以外 (表の奥) のどこにも無い
# どれか 1 つでも違えば exit 1。CI で毎回走らせる。陰性対照は scripts/check-exposure-test.sh。
#
#   bash workers/uriage/scripts/check-exposure.sh [wrangler.toml]   (既定 workers/uriage/worker/wrangler.toml)
set -euo pipefail

if [ "$#" -gt 1 ]; then
  echo "usage: $0 [wrangler.toml]" >&2
  exit 2
fi
target="${1:-$(dirname "$0")/../worker/wrangler.toml}"

python3 - "$target" <<'PY'
import sys
import tomllib

path = sys.argv[1]
with open(path, "rb") as f:
    cfg = tomllib.load(f)

errors = []


def err(msg):
    errors.append(msg)
    print(f"::error file={path}::{msg}")


def nested(node, key, prefix=""):
    """トップレベル以外 (表・表の配列の奥) にある `key` の位置。"""
    if isinstance(node, dict):
        for k, v in node.items():
            name = f"{prefix}.{k}" if prefix else k
            if k == key and prefix:
                yield name
            yield from nested(v, key, name)
    elif isinstance(node, list):
        for i, v in enumerate(node):
            yield from nested(v, key, f"{prefix}[{i}]")


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

# (d) D1 の URIAGE_DB はトップレベル。d1_databases はトップレベル以外に置かない
d1s = cfg.get("d1_databases") or []
d1 = next((d for d in d1s if isinstance(d, dict) and d.get("binding") == "URIAGE_DB"), None)
if d1 is None:
    err("トップレベルに d1_databases の URIAGE_DB が無い (担当者別売上の D1)")
else:
    if d1.get("database_name") != "ichibanboshi-uriage":
        err("d1_databases の URIAGE_DB の database_name が ichibanboshi-uriage でない")
    if d1.get("migrations_dir") != "migrations":
        err('d1_databases の URIAGE_DB の migrations_dir が "migrations" でない')
for name in nested(cfg, "d1_databases"):
    err(f"{name} がある (d1_databases はトップレベルにだけ置く)")

# (e) services を持たない
if "services" in cfg:
    err("トップレベルに services がある (この Worker は他の Worker への binding を持たない)")
for name in nested(cfg, "services"):
    err(f"{name} がある (この Worker は他の Worker への binding を持たない)")


# (f) LOCAL_ で始まるキーを、表・表の配列の奥まで探す
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


# (g) VPC・Secrets Store・R2 はトップレベルにだけ置く
def top_binding(table, binding):
    items = cfg.get(table) or []
    return next((v for v in items if isinstance(v, dict) and v.get("binding") == binding), None)


for table, bindings in (
    ("vpc_services", ("URIAGE_SQL_VPC", "URIAGE_CAKEPHP_VPC")),
    ("secrets_store_secrets", ("ICHIBAN_SQL", "URIAGE_WRITE_TOKEN")),
    ("r2_buckets", ("URIAGE_R2",)),
):
    for binding in bindings:
        if top_binding(table, binding) is None:
            err(f"トップレベルに {table} の {binding} が無い")
    for name in nested(cfg, table):
        err(f"{name} がある ({table} はトップレベルにだけ置く)")
r2 = top_binding("r2_buckets", "URIAGE_R2")
if r2 is not None and r2.get("bucket_name") != "ichibanboshi-uriage":
    err("r2_buckets の URIAGE_R2 の bucket_name が ichibanboshi-uriage でない")

if errors:
    sys.exit(1)
print(f"OK: {path} は workers_dev / preview_urls = false・route 無し・env 無し・D1 / R2 / VPC / Secrets Store はトップレベルだけ・services 無し・LOCAL_ 無し")
PY
