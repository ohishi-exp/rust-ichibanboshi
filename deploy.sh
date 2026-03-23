#!/bin/bash
set -e

SSH_USER="ubuntu"
TARGET_HOST="ohishi-data.tailea945d.ts.net"
TARGET="$SSH_USER@$TARGET_HOST"
TARGET_DIR="/opt/ichibanboshi"
BINARY="target/x86_64-unknown-linux-musl/release/ichibanboshi"

echo "=== Building musl release binary ==="
cargo build --release --target x86_64-unknown-linux-musl

echo "=== Deploying to $TARGET_HOST ==="
# 実行中バイナリは直接上書きできないので、一旦 /tmp 経由で mv（mv はアトミック）
scp "$BINARY" "$TARGET:/tmp/ichibanboshi.new"
ssh "$TARGET" "mv /tmp/ichibanboshi.new $TARGET_DIR/ichibanboshi && chmod +x $TARGET_DIR/ichibanboshi"

# systemd PathModified が検知して自動で restart する
echo "=== Waiting for auto-restart (PathModified) ==="
sleep 4

echo "=== Checking status ==="
ssh "$TARGET" "systemctl status ichibanboshi --no-pager 2>&1 | head -15"

echo ""
echo "=== Done! ==="
echo "API: http://$TARGET_HOST:3100/health"
