#!/usr/bin/env bash
# 一番星 Worker (ichibanboshi-ichiban) が外から届かないこと・社内への口を本番の外へ漏らさないことを wrangler.toml で検査する
# (ohishi-exp/smb-watch の `workers/smb-ingest/scripts/check-exposure.sh` を写している)。
#   (a) トップレベルに workers_dev = false と preview_urls = false が **明示** されている
#   (b) route / routes が無い (custom_domain も routes の中に書く)
#   (c) env 表が無い (トップレベルだけで運用する。env を足すと上の検査を env ごとにやり直す必要がある)
#   (d) トップレベルに vpc_services がある (ICHIBAN_VPC = SQL Server への口。env 側へ動いていない)
#   (e) vars に LOCAL_SQL_ADDR (ローカル検証で VPC を迂回して直接繋ぐフラグ) が無い
#   (g) vars に LOCAL_ICHIBAN_SQL_JSON (ローカル検証で Secrets Store の代わりに SQL Server の資格情報 JSON を渡す var) が無い
#   (f) vpc_services の service_id がプレースホルダのままなら warning (fail にはしない。VPC Service 作成後に入れる)
# (a)〜(e)・(g) が 1 つでも違えば exit 1。CI で毎回走らせる。陰性対照は scripts/check-exposure-test.sh。
#
#   bash workers/ichiban/scripts/check-exposure.sh [wrangler.toml]   (既定 workers/ichiban/worker/wrangler.toml)
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

# (d) SQL Server への口 ICHIBAN_VPC はトップレベル
vpcs = cfg.get("vpc_services")
if not isinstance(vpcs, list) or not any(isinstance(v, dict) and v.get("binding") == "ICHIBAN_VPC" for v in vpcs):
    err("トップレベルに vpc_services の ICHIBAN_VPC が無い (SQL Server への口)")

# (e) VPC を迂回するローカル専用フラグ
if "LOCAL_SQL_ADDR" in cfg.get("vars", {}):
    err("vars に LOCAL_SQL_ADDR がある (VPC を迂回して直接繋ぐ。ローカルの .dev.vars 専用)")

# (g) 資格情報を含む設定 JSON を vars に置かない (ローカルの .dev.vars 専用)
if "LOCAL_ICHIBAN_SQL_JSON" in cfg.get("vars", {}):
    err("vars に LOCAL_ICHIBAN_SQL_JSON がある (SQL Server の資格情報が vars に載る。ローカルの .dev.vars 専用)")

# (f) service_id のプレースホルダ (warning のみ)
for v in vpcs or []:
    if isinstance(v, dict) and v.get("service_id") == PLACEHOLDER:
        print(f"::warning file={path}::vpc_services {v.get('binding')} の service_id がプレースホルダのまま (VPC Service 作成後に入れる)")

if errors:
    sys.exit(1)
print(f"OK: {path} は workers_dev / preview_urls = false・route 無し・env 無し・vpc_services はトップレベル・LOCAL_SQL_ADDR / LOCAL_ICHIBAN_SQL_JSON 無し")
PY
