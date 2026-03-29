#!/bin/bash
# coverage_100.toml に登録されたファイルが 100% カバレッジを維持しているか検証する
#
# Usage:
#   bash scripts/check_coverage_100.sh
#   bash scripts/check_coverage_100.sh --use-cache /path/to/llvm-cov-text.txt
#
# DB 不要 — MockRepo ベースのテストのみ

set -euo pipefail

EXTERNAL_CACHE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --use-cache) EXTERNAL_CACHE="$2"; shift 2 ;;
    *) shift ;;
  esac
done

CONFIG="coverage_100.toml"
if [[ ! -f "$CONFIG" ]]; then
  echo "ERROR: $CONFIG not found"
  exit 1
fi

# --- Parse coverage_100.toml ---
declare -a PATHS=()
while IFS= read -r line; do
  if [[ "$line" =~ ^path\ =\ \"(.+)\" ]]; then
    PATHS+=("${BASH_REMATCH[1]}")
  fi
done < "$CONFIG"

echo "=== Coverage 100% Check ==="
echo "Registered files: ${#PATHS[@]}"
echo ""

# --- Run cargo llvm-cov --text ---
if [ -n "$EXTERNAL_CACHE" ]; then
  echo "Using pre-built coverage data: $EXTERNAL_CACHE"
  CACHE_FILE="$EXTERNAL_CACHE"
else
  CACHE_FILE=$(mktemp)
  trap "rm -f $CACHE_FILE" EXIT
  echo "Running cargo llvm-cov --text..."
  cargo llvm-cov --text > "$CACHE_FILE" 2>&1 || { echo "cargo llvm-cov failed:"; tail -50 "$CACHE_FILE"; exit 101; }
fi

# --- --text 出力からファイルごとの Lines/Miss を集計 ---
SUMMARY_FILE=$(mktemp)
trap "rm -f $SUMMARY_FILE" EXIT
awk '
/^\/.*\/src\/.*\.rs:$/ {
    if (file != "") {
        total = covered + uncovered
        printf "%s %d %d\n", file, total, uncovered
    }
    file = $0; sub(/:$/, "", file)
    covered = 0; uncovered = 0; next
}
/^[[:space:]]*[0-9]+\|[[:space:]]*0\|/ { uncovered++; next }
/^[[:space:]]*[0-9]+\|[[:space:]]*[1-9][0-9]*\|/ { covered++; next }
END {
    if (file != "") {
        total = covered + uncovered
        printf "%s %d %d\n", file, total, uncovered
    }
}
' "$CACHE_FILE" > "$SUMMARY_FILE"

# --- Check each file ---
FAILED=0
CHECKED=0

for filepath in "${PATHS[@]}"; do
  MATCH=$(grep "$filepath" "$SUMMARY_FILE" || true)

  if [ -z "$MATCH" ]; then
    echo "WARN: $filepath — not found in coverage data"
    continue
  fi

  TOTAL=$(echo "$MATCH" | awk '{print $2}')
  MISS=$(echo "$MATCH" | awk '{print $3}')
  CHECKED=$((CHECKED + 1))

  if [ "$TOTAL" -eq 0 ]; then
    echo "WARN: $filepath — 0 lines (no executable code)"
    continue
  fi

  if [ "$MISS" -gt 0 ]; then
    COVERED=$((TOTAL - MISS))
    PCT=$(awk "BEGIN {printf \"%.1f\", $COVERED/$TOTAL*100}")
    echo "FAIL: $filepath — $COVERED/$TOTAL lines ($PCT%, $MISS lines missing)"
    awk -v fp="$filepath" '
      index($0, fp) && /:$/ { found=1; next }
      /^$/ { found=0 }
      found && /^[[:space:]]*[0-9]+\|[[:space:]]*0\|/ { print "      " $0 }
    ' "$CACHE_FILE" | head -20
    FAILED=1
  else
    echo "  OK: $filepath — $TOTAL/$TOTAL lines (100%)"
  fi
done

echo ""
echo "Checked: $CHECKED files"

if [ "$FAILED" -eq 1 ]; then
  echo ""
  echo "FAILED: Coverage regression detected!"
  exit 1
fi

echo "All registered files maintain 100% coverage."
