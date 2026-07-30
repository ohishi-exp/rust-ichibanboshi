#!/bin/bash
# parse_coverage.sh — `cargo llvm-cov --text` の出力を読んで GitHub Actions の
# Job Summary (ローカルでは stdout) 向けのレポートを書く。
#
# Usage:
#   bash scripts/parse_coverage.sh summary ""          /tmp/llvm-cov-output.txt
#   bash scripts/parse_coverage.sh not-100 ""          /tmp/llvm-cov-output.txt
#   bash scripts/parse_coverage.sh file    "kosoku"    /tmp/llvm-cov-output.txt
#
# mode:
#   summary  — 全ファイルの行数 / 未カバー / % 一覧
#   not-100  — 100% 未達のファイルだけを未カバー行数の降順で (次に埋める対象探し)
#   file     — パターンに一致するファイルの未カバー行を前後 3 行の文脈つきで
#
# 行の数え方は scripts/check_coverage_100.sh と同一実装にしてある。ここがズレると
# 「not-100 には出ないのに gate が落ちる」が起きて調査が空転するため、
# パーサを別実装にしないこと (先例 rust-alc-api は awk / awk の二重実装だった)。

set -euo pipefail

MODE="${1:?usage: $0 <summary|not-100|file> <file_pattern> <llvm-cov-text>}"
FILE_PATTERN="${2:-}"
COV_FILE="${3:?usage: $0 <summary|not-100|file> <file_pattern> <llvm-cov-text>}"

if [[ ! -f "$COV_FILE" ]]; then
  echo "ERROR: Coverage file not found: $COV_FILE" >&2
  exit 1
fi

case "$MODE" in
  summary|not-100|file) ;;
  *) echo "ERROR: Unknown mode: $MODE (summary|not-100|file)" >&2; exit 1 ;;
esac

if [[ "$MODE" == "file" && -z "$FILE_PATTERN" ]]; then
  echo "ERROR: mode=file requires a file pattern" >&2
  exit 1
fi

# GitHub Actions の Job Summary、無ければ stdout
OUT="${GITHUB_STEP_SUMMARY:-/dev/stdout}"

python3 - "$MODE" "$FILE_PATTERN" "$COV_FILE" >> "$OUT" <<'PYEOF'
import re
import sys

mode, pattern, cov_path = sys.argv[1], sys.argv[2], sys.argv[3]

HEADER = re.compile(r"^(/.*\.rs):$")
COUNTED = re.compile(r"^\s*(\d+)\|\s*([0-9][0-9.]*)([kKmMgG]?)\s*\|")
UNCOUNTED = re.compile(r"^\s*\d+\|\s*\|")
SCALE = {"": 1, "k": 1e3, "K": 1e3, "m": 1e6, "M": 1e6, "g": 1e9, "G": 1e9}


def short(path):
    """絶対パスを src/... 相対に詰める (worktree ごとに前置きが変わるため)."""
    i = path.rfind("/src/")
    return path[i + 1:] if i >= 0 else path


# path -> {"lines": {ln: hit}, "text": {ln: source}}
files = {}
cur = None
with open(cov_path, encoding="utf-8", errors="replace") as fh:
    for raw in fh:
        line = raw.rstrip("\n")
        m = HEADER.match(line)
        if m:
            cur = files.setdefault(short(m.group(1)), {"lines": {}, "text": {}})
            continue
        if cur is None:
            continue
        m = COUNTED.match(line)
        if m:
            ln = int(m.group(1))
            hit = float(m.group(2)) * SCALE[m.group(3)]
            cur["lines"][ln] = max(cur["lines"].get(ln, 0.0), hit)
            cur["text"].setdefault(ln, line)
            continue
        if UNCOUNTED.match(line):
            ln = int(line.split("|", 1)[0].strip())
            cur["text"].setdefault(ln, line)

stats = []
for path, d in files.items():
    total = len(d["lines"])
    miss = sum(1 for h in d["lines"].values() if h == 0)
    stats.append((path, total, miss))

if mode in ("summary", "not-100"):
    if mode == "summary":
        print("## 📊 Coverage Summary")
        rows = sorted(stats)
    else:
        print("## 🔍 Files NOT at 100% Coverage")
        print()
        print("未カバー行数の降順。")
        rows = sorted((r for r in stats if r[2] > 0), key=lambda r: (-r[2], r[0]))
    print()
    print("| File | Lines | Miss | Coverage |")
    print("|------|------:|-----:|---------:|")
    for path, total, miss in rows:
        if total == 0:
            continue
        print(f"| `{path}` | {total} | {miss} | {(total - miss) * 100.0 / total:.1f}% |")
    if mode == "summary":
        gt = sum(r[1] for r in stats)
        gm = sum(r[2] for r in stats)
        pct = f"{(gt - gm) * 100.0 / gt:.1f}%" if gt else "-"
        print(f"| **TOTAL** | **{gt}** | **{gm}** | **{pct}** |")
    elif not rows:
        print("| _(none — 全ファイル 100%)_ | | | |")

elif mode == "file":
    print(f"## 🔎 Uncovered Lines: `{pattern}`")
    print()
    matched = sorted(r for r in stats if pattern in r[0])
    if not matched:
        print(f"_パターン `{pattern}` に一致するファイルがありません。_")
        sys.exit(0)

    print("### Summary")
    print()
    print("| File | Lines | Miss | Coverage |")
    print("|------|------:|-----:|---------:|")
    for path, total, miss in matched:
        pct = f"{(total - miss) * 100.0 / total:.1f}%" if total else "-"
        print(f"| `{path}` | {total} | {miss} | {pct} |")
    print()
    print("### Uncovered Lines")
    print()
    for path, total, miss in matched:
        if miss == 0:
            continue
        d = files[path]
        missed = sorted(ln for ln, hit in d["lines"].items() if hit == 0)
        # 未カバー行 ± 3 行を文脈として出す
        show = set()
        for ln in missed:
            show.update(range(ln - 3, ln + 4))
        print(f"**{path}**")
        print()
        print("```")
        prev = None
        for ln in sorted(n for n in show if n in d["text"]):
            if prev is not None and ln != prev + 1:
                print("  ...")
            marker = ">>>" if ln in d["lines"] and d["lines"][ln] == 0 else "   "
            print(f"{marker} {d['text'][ln]}")
            prev = ln
        print("```")
        print()
PYEOF

echo "Coverage report written to ${GITHUB_STEP_SUMMARY:+Job Summary}${GITHUB_STEP_SUMMARY:-stdout}." >&2
