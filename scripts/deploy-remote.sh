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
# 任意 env (2 本目の binary — 指定が無いときは一切触らない):
#   DEPLOY_EXTRA_BINARY       … 追加で運ぶ binary path (例 rdp-relay の musl build)
#   DEPLOY_EXTRA_NAME         … 転送先のファイル名 (default: DEPLOY_EXTRA_BINARY の basename)。
#                               systemd の unit 名も これ と一致させる前提
#                               (journalctl -u <name> で失敗時のログを出すため)
#   DEPLOY_EXTRA_HEALTH_PORT  … 2 本目の疎通確認する localhost ポート (例 rdp-relay なら 3390)。
#                               未指定なら 2 本目の health は確認しない
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

# 2 本目 (任意)。未指定なら以降の extra 系は全部素通りする。
EXTRA_BINARY="${DEPLOY_EXTRA_BINARY:-}"
EXTRA_HEALTH_PORT="${DEPLOY_EXTRA_HEALTH_PORT:-}"
EXTRA_NAME=""
if [[ -n "$EXTRA_BINARY" ]]; then
  EXTRA_NAME="${DEPLOY_EXTRA_NAME:-$(basename "$EXTRA_BINARY")}"
fi

if [[ ! -f "$BINARY" ]]; then
  echo "::error::deploy binary not found: $BINARY" >&2
  exit 1
fi

# 指定されたのに無い = build を取り違えている。黙って 1 本だけ運ばず loud fail する。
if [[ -n "$EXTRA_BINARY" && ! -f "$EXTRA_BINARY" ]]; then
  echo "::error::extra deploy binary not found: $EXTRA_BINARY" >&2
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

# 2 本目を先に置く。あとで置く 1 本目の PathModified 待ち (下の sleep) が
# 両方の restart を兼ねるので、待ち時間を増やさずに済む。
if [[ -n "$EXTRA_BINARY" ]]; then
  echo "=== Deploying $EXTRA_BINARY to $TARGET ($TARGET_DIR/$EXTRA_NAME) ==="
  scp "${SSH_OPTS[@]}" "$EXTRA_BINARY" "$TARGET:/tmp/$EXTRA_NAME.new"
  ssh "${SSH_OPTS[@]}" "$TARGET" \
    "mv /tmp/$EXTRA_NAME.new $TARGET_DIR/$EXTRA_NAME && chmod +x $TARGET_DIR/$EXTRA_NAME"
fi

echo "=== Deploying $BINARY to $TARGET ($TARGET_DIR) ==="
# 実行中バイナリは直接上書きできないので /tmp 経由で mv (mv はアトミック)。
scp "${SSH_OPTS[@]}" "$BINARY" "$TARGET:/tmp/ichibanboshi.new"
ssh "${SSH_OPTS[@]}" "$TARGET" \
  "mv /tmp/ichibanboshi.new $TARGET_DIR/ichibanboshi && chmod +x $TARGET_DIR/ichibanboshi"

# systemd PathModified (ichibanboshi-watcher.path) が検知して自動 restart する。
echo "=== Waiting for auto-restart (PathModified) ==="
sleep 6

# remote 側から <addr>/health を叩き、HTTP_CODE / HEALTH_BODY に置く。
# body は build 情報 ({"status","commit","built_at"})。
# 起動は 20 秒超かかることがある (kyuyo SQL Server pool 初期化だけで 13 秒の実測、
# 2026-07-29 に一発チェックで race って赤になった) ので、最長 60 秒までポーリング。
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

echo "=== Health check (localhost:$HEALTH_PORT/health) ==="
poll_health "localhost:$HEALTH_PORT" || true

echo "health HTTP code: ${HTTP_CODE:-<none>}"
echo "health body: ${HEALTH_BODY:-<none>}"
if [[ "$HTTP_CODE" != "200" ]]; then
  echo "::error::health check failed (expected 200, got ${HTTP_CODE:-<none>})" >&2
  echo "--- systemctl status (last 15 lines) ---" >&2
  ssh "${SSH_OPTS[@]}" "$TARGET" \
    "systemctl status ichibanboshi --no-pager 2>&1 | head -15" >&2 || true
  # journalctl: binary が boot 時に panic した場合、stderr メッセージはここに出る。
  echo "--- journalctl -u ichibanboshi (last 80 lines、binary stderr 含む) ---" >&2
  ssh "${SSH_OPTS[@]}" "$TARGET" \
    "journalctl -u ichibanboshi --no-pager --since='5 minutes ago' 2>&1 | tail -80" >&2 || true
  # GitHub Actions Step Summary にも構造化して残す (failure debugging を見やすく)
  if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    {
      echo "### ❌ Deploy 失敗 — health check ${HTTP_CODE:-<none>}"
      echo ""
      echo "#### systemctl status"
      echo '```'
      ssh "${SSH_OPTS[@]}" "$TARGET" \
        "systemctl status ichibanboshi --no-pager 2>&1 | head -30" 2>&1 || true
      echo '```'
      echo ""
      echo "#### journalctl -u ichibanboshi (last 100 lines、binary panic 含む)"
      echo '```'
      ssh "${SSH_OPTS[@]}" "$TARGET" \
        "journalctl -u ichibanboshi --no-pager --since='5 minutes ago' 2>&1 | tail -100" 2>&1 || true
      echo '```'
    } >> "$GITHUB_STEP_SUMMARY"
  fi
  exit 1
fi

# 主 binary の health body は下の Step Summary で使う。2 本目の確認で HTTP_CODE /
# HEALTH_BODY が上書きされる前に退避する。
MAIN_HEALTH_BODY="$HEALTH_BODY"

# 2 本目 (rdp-relay 等)。systemd の unit 名が EXTRA_NAME と一致している前提。
# **ホストに unit を入れてから** DEPLOY_EXTRA_HEALTH_PORT を設定すること。
# binary だけ運んで unit が無い状態でここを有効にすると、置いただけで deploy が赤くなる。
if [[ -n "$EXTRA_BINARY" && -n "$EXTRA_HEALTH_PORT" ]]; then
  # 中継は loopback ではなく LAN アドレスに bind していることがある (RDP_RELAY_BIND)。
  # その値は root しか読めない env にあるので、実際に listen しているソケットから引く。
  # まだ listen していない (再起動直後) / wildcard bind のときは localhost に落とす。
  EXTRA_ADDR=""
  for _j in $(seq 1 6); do
    EXTRA_ADDR="$(ssh "${SSH_OPTS[@]}" "$TARGET" "ss -tln 2>/dev/null" \
      | awk -v port=":$EXTRA_HEALTH_PORT" 'index($4, port) == length($4) - length(port) + 1 {print $4; exit}')"
    [[ -n "$EXTRA_ADDR" ]] && break
    echo "$EXTRA_NAME is not listening on :$EXTRA_HEALTH_PORT yet — retrying in 5s"
    sleep 5
  done
  case "$EXTRA_ADDR" in
    ''|0.0.0.0:*|\[::\]:*|\*:*) EXTRA_ADDR="localhost:$EXTRA_HEALTH_PORT" ;;
  esac
  echo "=== Health check ($EXTRA_NAME, $EXTRA_ADDR/health) ==="
  poll_health "$EXTRA_ADDR" || true
  echo "$EXTRA_NAME health HTTP code: ${HTTP_CODE:-<none>}"
  if [[ "$HTTP_CODE" != "200" ]]; then
    echo "::error::$EXTRA_NAME health check failed (expected 200, got ${HTTP_CODE:-<none>})" >&2
    echo "--- systemctl status $EXTRA_NAME (last 15 lines) ---" >&2
    ssh "${SSH_OPTS[@]}" "$TARGET" \
      "systemctl status $EXTRA_NAME --no-pager 2>&1 | head -15" >&2 || true
    # 起動を拒否する作りなので、設定不足はここに出る (--allow 無し / env file 無し)。
    echo "--- journalctl -u $EXTRA_NAME (last 80 lines) ---" >&2
    ssh "${SSH_OPTS[@]}" "$TARGET" \
      "journalctl -u $EXTRA_NAME --no-pager --since='5 minutes ago' 2>&1 | tail -80" >&2 || true
    if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
      {
        echo "### ❌ Deploy 失敗 — $EXTRA_NAME の health check ${HTTP_CODE:-<none>}"
        echo ""
        echo "#### journalctl -u $EXTRA_NAME (last 100 lines)"
        echo '```'
        ssh "${SSH_OPTS[@]}" "$TARGET" \
          "journalctl -u $EXTRA_NAME --no-pager --since='5 minutes ago' 2>&1 | tail -100" 2>&1 || true
        echo '```'
      } >> "$GITHUB_STEP_SUMMARY"
    fi
    exit 1
  fi
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
