#!/bin/bash
set -e

SSH_USER="ubuntu"
TARGET_HOST="ohishi-data.tailea945d.ts.net"
TARGET="$SSH_USER@$TARGET_HOST"
TARGET_DIR="/opt/ichibanboshi"
BINARY="target/release/ichibanboshi"

# Prompt for sudo password once
read -s -p "sudo password for $TARGET: " SUDO_PASS
echo

SUDO="echo '$SUDO_PASS' | sudo -S"

echo "=== Building release binary ==="
cargo build --release

echo "=== Deploying to $TARGET_HOST ==="
ssh $TARGET "$SUDO mkdir -p $TARGET_DIR/logs 2>/dev/null"

# Copy binary
scp $BINARY $TARGET:/tmp/ichibanboshi
ssh $TARGET "$SUDO mv /tmp/ichibanboshi $TARGET_DIR/ichibanboshi && $SUDO chmod +x $TARGET_DIR/ichibanboshi 2>/dev/null"

# Copy config only if not exists
scp deploy/ichibanboshi.toml $TARGET:/tmp/ichibanboshi.toml
ssh $TARGET "[ -f $TARGET_DIR/ichibanboshi.toml ] || $SUDO mv /tmp/ichibanboshi.toml $TARGET_DIR/ichibanboshi.toml 2>/dev/null"

# Install systemd service
scp deploy/ichibanboshi.service $TARGET:/tmp/ichibanboshi.service
ssh $TARGET "$SUDO mv /tmp/ichibanboshi.service /etc/systemd/system/ichibanboshi.service && $SUDO systemctl daemon-reload && $SUDO systemctl enable ichibanboshi && $SUDO systemctl restart ichibanboshi 2>/dev/null"

echo "=== Checking status ==="
ssh $TARGET "$SUDO systemctl status ichibanboshi --no-pager 2>/dev/null"

echo "=== Done! ==="
echo "API: http://$TARGET_HOST:3100/health"
