#!/bin/bash
#
# 手動 deploy fallback (Tailscale 経路)。
# musl binary を build して ohishi-data (Tailscale MagicDNS) へ deploy する。
# 実 deploy ロジックは scripts/deploy-remote.sh に集約し、CI (CF Tunnel SSH 経路)
# と共有している。緊急時 / 手元からの即時反映用にこの Tailscale 経路を温存する。
set -euo pipefail

cd "$(dirname "$0")"

echo "=== Building musl release binary ==="
cargo build --release --target x86_64-unknown-linux-musl

# Tailscale 直 SSH (proxy / service token なし)。
export DEPLOY_SSH_HOST="${DEPLOY_SSH_HOST:-ohishi-data.tailea945d.ts.net}"
export DEPLOY_SSH_USER="${DEPLOY_SSH_USER:-ubuntu}"

# RDP 中継も一緒に運ぶ (上の cargo build が [[bin]] を全部作っている)。
# health は unit をホストに入れてから: DEPLOY_EXTRA_HEALTH_PORT=3390 を足す。
export DEPLOY_EXTRA_BINARY="${DEPLOY_EXTRA_BINARY:-target/x86_64-unknown-linux-musl/release/rdp-relay}"
export DEPLOY_EXTRA_NAME="${DEPLOY_EXTRA_NAME:-rdp-relay}"

exec bash scripts/deploy-remote.sh
