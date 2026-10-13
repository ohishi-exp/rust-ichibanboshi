#!/bin/bash
#
# rdp-relay の musl static binary を remote host (ohishi-data) へ転送し systemd PathModified
# watcher (rdp-relay-watcher.path) で自動 restart させる共通 deploy ロジック。
# root package の binary は rdp-relay だけ (オンプレ版の売上・勤怠は Worker へ移して撤去、Refs #322)。
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
#   DEPLOY_TARGET_DIR         … インストール先 (default: /opt/ichibanboshi。unit の ExecStart と揃える)
#   DEPLOY_BINARY             … 転送する binary path
#                               (default: target/x86_64-unknown-linux-musl/release/rdp-relay)
#   DEPLOY_NAME               … 転送先のファイル名 = systemd の unit 名 (default: rdp-relay)
#                               (journalctl -u <name> で失敗時のログを出すため一致させる)
#   DEPLOY_SSH_KEY_FILE       … 秘密鍵 path (未指定なら ssh-agent / 既定鍵)
#   DEPLOY_SSH_PROXY_COMMAND  … ssh -o ProxyCommand=<...> に渡す値
#                               (Cloudflare Tunnel SSH なら "cloudflared access ssh --hostname %h")
#   DEPLOY_HEALTH_PORT        … 疎通確認するポート (default: 3390)
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
BINARY="${DEPLOY_BINARY:-target/x86_64-unknown-linux-musl/release/rdp-relay}"
NAME="${DEPLOY_NAME:-rdp-relay}"
HEALTH_PORT="${DEPLOY_HEALTH_PORT:-3390}"

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

echo "=== Deploying $BINARY to $TARGET ($TARGET_DIR/$NAME) ==="
# 実行中バイナリは直接上書きできないので /tmp 経由で mv (mv はアトミック)。
scp "${SSH_OPTS[@]}" "$BINARY" "$TARGET:/tmp/$NAME.new"
ssh "${SSH_OPTS[@]}" "$TARGET" \
  "mv /tmp/$NAME.new $TARGET_DIR/$NAME && chmod +x $TARGET_DIR/$NAME"

# systemd PathModified (rdp-relay-watcher.path) が検知して自動 restart する
# (LAN 用インスタンス rdp-relay-lan も同じ watcher が上げ直す)。
echo "=== Waiting for auto-restart (PathModified) ==="
sleep 6

# remote 側から <addr>/health を叩き、HTTP_CODE / HEALTH_BODY に置く。最長 60 秒までポーリング。
poll_health() {
  local addr="$1"
  local resp
  HTTP_CODE=""
  HEALTH_BODY=""
  for _i in $(seq 1 12); do
    resp="$(ssh "${SSH_OPTS[@]}" "$TARGET" \
      "curl -s -w '\n%{http_code}' --max-time 10 http://$addr/health || true")"
    HTTP_CODE="$(printf '%s' "$resp" | tail -n1)"
    HEALTH_BODY="$(printf '%s' "$resp" | sed '$d')"
    if [[ "$HTTP_CODE" == "200" ]]; then return 0; fi
    echo "health not ready yet (got ${HTTP_CODE:-<none>}) — retrying in 5s"
    sleep 5
  done
  return 1
}

# 中継は loopback ではなく LAN アドレスに bind していることがある (RDP_RELAY_BIND)。
# その値は root しか読めない env にあるので、実際に listen しているソケットから引く。
# まだ listen していない (再起動直後) / wildcard bind のときは localhost に落とす。
ADDR=""
for _j in $(seq 1 6); do
  ADDR="$(ssh "${SSH_OPTS[@]}" "$TARGET" "ss -tln 2>/dev/null" \
    | awk -v port=":$HEALTH_PORT" 'index($4, port) == length($4) - length(port) + 1 {print $4; exit}')"
  [[ -n "$ADDR" ]] && break
  echo "$NAME is not listening on :$HEALTH_PORT yet — retrying in 5s"
  sleep 5
done
case "$ADDR" in
  ''|0.0.0.0:*|\[::\]:*|\*:*) ADDR="localhost:$HEALTH_PORT" ;;
esac

echo "=== Health check ($NAME, $ADDR/health) ==="
poll_health "$ADDR" || true

echo "health HTTP code: ${HTTP_CODE:-<none>}"
echo "health body: ${HEALTH_BODY:-<none>}"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "::error::$NAME health check failed (expected 200, got ${HTTP_CODE:-<none>})" >&2
  echo "--- systemctl status $NAME (last 15 lines) ---" >&2
  ssh "${SSH_OPTS[@]}" "$TARGET" \
    "systemctl status $NAME --no-pager 2>&1 | head -15" >&2 || true
  # 起動を拒否する作りなので、設定不足はここに出る (--allow 無し / env file 無し)。
  echo "--- journalctl -u $NAME (last 80 lines) ---" >&2
  ssh "${SSH_OPTS[@]}" "$TARGET" \
    "journalctl -u $NAME --no-pager --since='5 minutes ago' 2>&1 | tail -80" >&2 || true
  # GitHub Actions Step Summary にも構造化して残す (failure debugging を見やすく)
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "### ❌ Deploy 失敗 — $NAME の health check ${HTTP_CODE:-<none>}"
      echo ""
      echo "#### systemctl status $NAME"
      echo '```'
      ssh "${SSH_OPTS[@]}" "$TARGET" \
        "systemctl status $NAME --no-pager 2>&1 | head -30" 2>&1 || true
      echo '```'
      echo ""
      echo "#### journalctl -u $NAME (last 100 lines)"
      echo '```'
      ssh "${SSH_OPTS[@]}" "$TARGET" \
        "journalctl -u $NAME --no-pager --since='5 minutes ago' 2>&1 | tail -100" 2>&1 || true
      echo '```'
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  exit 1
fi

# GitHub Actions の Step Summary に結果を出す (CI のみ。手動 deploy では未設定)。
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
  {
    echo "### ✅ Deploy 成功 — $NAME on ${TARGET_HOST} ($ADDR)"
    echo ""
    echo "\`/health\` レスポンス:"
    echo ""
    echo '```'
    echo "${HEALTH_BODY:-<empty>}"
    echo '```'
  } >> "$GITHUB_STEP_SUMMARY"
fi

echo "=== Done! $NAME deployed & healthy on $TARGET_HOST ($ADDR) ==="
