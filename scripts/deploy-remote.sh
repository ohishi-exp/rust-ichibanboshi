#!/bin/bash
#
# musl static binary を remote host (ohishi-data) へ転送し systemd PathModified
# watcher で自動 restart させる共通 deploy ロジック。
#
# 経路 (Tailscale 直 / Cloudflare Tunnel SSH) は env で切り替える:
#   - deploy.sh (手動 fallback)       … DEPLOY_SSH_HOST=ohishi-data.tailea945d.ts.net (Tailscale)
#   - ci.yml deploy job (自動)         … DEPLOY_SSH_HOST=ssh-rust-ichiban.mtamaramu.com
#                                         DEPLOY_SSH_PROXY_COMMAND="cloudflared access ssh --hostname %h"
#                                         CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET (service token)
#
# 必須 env:
#   DEPLOY_SSH_HOST            … 接続先 SSH ホスト名
#
# 任意 env:
#   DEPLOY_SSH_USER           … SSH ユーザー (default: ubuntu)
#   DEPLOY_TARGET_DIR         … インストール先 (default: /opt/ichibanboshi)
#   DEPLOY_BINARY             … 転送する binary path
#                               (default: target/x86_64-unknown-linux-musl/release/ichibanboshi)
#   DEPLOY_SSH_KEY_FILE       … 秘密鍵 path (未指定なら ssh-agent / 既定鍵)
#   DEPLOY_SSH_PROXY_COMMAND  … ssh -o ProxyCommand=<...> に渡す値
#                               (Cloudflare Tunnel SSH なら "cloudflared access ssh --hostname %h")
#   DEPLOY_HEALTH_PORT        … 疎通確認する localhost ポート (default: 3100)
#   CF_ACCESS_CLIENT_ID       … CF Access service token id  (cloudflared が読む)
#   CF_ACCESS_CLIENT_SECRET   … CF Access service token secret
#
# deploy 失敗 (build 不在 / scp / ssh / health) は即 exit != 0 で loud fail する
# (set -e + health 200 厳格チェック)。
set -euo pipefail

SSH_USER="${DEPLOY_SSH_USER:-ubuntu}"
TARGET_HOST="${DEPLOY_SSH_HOST:?DEPLOY_SSH_HOST is required}"
TARGET="$SSH_USER@$TARGET_HOST"
TARGET_DIR="${DEPLOY_TARGET_DIR:-/opt/ichibanboshi}"
BINARY="${DEPLOY_BINARY:-target/x86_64-unknown-linux-musl/release/ichibanboshi}"
HEALTH_PORT="${DEPLOY_HEALTH_PORT:-3100}"

if [[ ! -f "$BINARY" ]]; then
  echo "::error::deploy binary not found: $BINARY" >&2
  exit 1
fi

# Cloudflare Access service token は cloudflared が TUNNEL_SERVICE_TOKEN_* env を読む。
# issue の secret 名 (CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET) からマップする。
if [[ -n "${CF_ACCESS_CLIENT_ID:-}" ]]; then
  export TUNNEL_SERVICE_TOKEN_ID="$CF_ACCESS_CLIENT_ID"
fi
if [[ -n "${CF_ACCESS_CLIENT_SECRET:-}" ]]; then
  export TUNNEL_SERVICE_TOKEN_SECRET="$CF_ACCESS_CLIENT_SECRET"
fi

# scp / ssh 共通オプションを組み立てる。
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o BatchMode=yes)
if [[ -n "${DEPLOY_SSH_KEY_FILE:-}" ]]; then
  SSH_OPTS+=(-i "$DEPLOY_SSH_KEY_FILE" -o IdentitiesOnly=yes)
fi
if [[ -n "${DEPLOY_SSH_PROXY_COMMAND:-}" ]]; then
  SSH_OPTS+=(-o "ProxyCommand=$DEPLOY_SSH_PROXY_COMMAND")
fi

echo "=== Deploying $BINARY to $TARGET ($TARGET_DIR) ==="
# 実行中バイナリは直接上書きできないので /tmp 経由で mv (mv はアトミック)。
scp "${SSH_OPTS[@]}" "$BINARY" "$TARGET:/tmp/ichibanboshi.new"
ssh "${SSH_OPTS[@]}" "$TARGET" \
  "mv /tmp/ichibanboshi.new $TARGET_DIR/ichibanboshi && chmod +x $TARGET_DIR/ichibanboshi"

# systemd PathModified (ichibanboshi-watcher.path) が検知して自動 restart する。
echo "=== Waiting for auto-restart (PathModified) ==="
sleep 6

echo "=== Health check (localhost:$HEALTH_PORT/health) ==="
# remote の localhost に対して health を叩く。body + HTTP code を一括取得し、
# HTTP 200 以外は loud fail。body は build 情報 ({"status","commit","built_at"})。
HEALTH_RESP="$(ssh "${SSH_OPTS[@]}" "$TARGET" \
  "curl -s -w '\n%{http_code}' --max-time 10 http://localhost:$HEALTH_PORT/health || true")"
HTTP_CODE="$(printf '%s' "$HEALTH_RESP" | tail -n1)"
HEALTH_BODY="$(printf '%s' "$HEALTH_RESP" | sed '$d')"

echo "health HTTP code: ${HTTP_CODE:-<none>}"
echo "health body: ${HEALTH_BODY:-<none>}"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "::error::health check failed (expected 200, got ${HTTP_CODE:-<none>})" >&2
  echo "--- systemctl status (last 15 lines) ---" >&2
  ssh "${SSH_OPTS[@]}" "$TARGET" \
    "systemctl status ichibanboshi --no-pager 2>&1 | head -15" >&2 || true
  exit 1
fi

# GitHub Actions の Step Summary に build 情報を出す (CI のみ。手動 deploy では未設定)。
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### ✅ Deploy 成功 — ${TARGET_HOST}:${HEALTH_PORT}"
    echo ""
    echo "\`/health\` レスポンス (build 識別):"
    echo ""
    echo '```json'
    echo "${HEALTH_BODY:-<empty>}"
    echo '```'
  } >> "$GITHUB_STEP_SUMMARY"
fi

echo "=== Done! deployed & healthy on $TARGET_HOST:$HEALTH_PORT ==="
