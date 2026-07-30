#!/usr/bin/env bash
#
# migration-safety-check.sh
#
# rollback 安全性の観点で SQL migration を検査する。
# 正規表現ベース。将来 sqlparser-rs (AST) に差し替え予定。
#
# 使い方:
#   bash migration-safety-check.sh <file1.sql> [file2.sql ...]
#
# 出力:
#   stdout に human-readable な検出結果
#   GITHUB_OUTPUT が定義されていれば machine-readable な結果も書き込む:
#     unsafe_count=<int>
#     unsafe_files=<JSON array>
#     findings_json=<JSON>
#
# 終了コード:
#   常に 0 (= warning only、Actions step を fail させない)
#   stderr へのエラーは exit 2

set -uo pipefail

if [[ $# -eq 0 ]]; then
  echo "usage: $0 <file1.sql> [file2.sql ...]" >&2
  exit 2
fi

# SQL コメント除去 (-- 行コメント / /* ブロックコメント */)
strip_sql_comments() {
  local file="$1"
  # 1) /* ... */ をスペース 1 個に置換 (改行は維持しない、行番号がずれるが検査目的なので可)
  # 2) -- 以降の行末コメントを削除
  # POSIX awk で /* */ 跨ぎ行ブロック削除は煩雑なので perl で一括処理
  perl -0777 -pe 's{/\*.*?\*/}{ }gs; s{--[^\n]*}{}g' "$file"
}

# 検出パターン定義。3 行 1 セット (id / regex / message)。
# regex は PCRE (grep -niP) / case-insensitive で match。
# regex 内の `|` と衝突しないよう delimiter ではなく行単位で持つ。
PATTERN_IDS=(
  'drop_table'
  'drop_column'
  'drop_schema'
  'truncate'
  'alter_column_type'
  'rename'
  'set_not_null'
  'drop_default'
)
PATTERN_REGEXES=(
  '\bDROP\s+TABLE\b'
  '\bDROP\s+COLUMN\b'
  '\bDROP\s+SCHEMA\b'
  '\bTRUNCATE\b'
  '\bALTER\s+(?:TABLE\s+\S+\s+)?(?:ALTER|MODIFY)\s+COLUMN\s+\S+\s+(?:SET\s+DATA\s+)?TYPE\b'
  '\bRENAME\s+(?:TO|COLUMN|CONSTRAINT)\b'
  '\bSET\s+NOT\s+NULL\b'
  '\bDROP\s+DEFAULT\b'
)
PATTERN_MESSAGES=(
  'DROP TABLE は contract migration、rollback 不可'
  'DROP COLUMN は contract migration、rollback 不可'
  'DROP SCHEMA は contract migration、rollback 不可'
  'TRUNCATE はデータ破壊、rollback 不可'
  'ALTER COLUMN TYPE は contract migration、widening 含め rollback 不可扱い'
  'RENAME は contract migration、rollback 不可 (index/constraint rename 含む)'
  'SET NOT NULL は contract migration の可能性、既存 NULL 行があると失敗'
  'DROP DEFAULT は旧 code が default 依存していた場合に rollback 不可'
)

TOTAL_UNSAFE=0
UNSAFE_FILES=()

# JSON エスケープ (簡易: " と \ のみ)
json_escape() {
  printf '%s' "$1" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()), end="")' 2>/dev/null \
    || printf '"%s"' "$(printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g')"
}

ALL_FINDINGS_JSON='['
FIRST_FINDING=1

for file in "$@"; do
  if [[ ! -f "$file" ]]; then
    echo "::warning::file not found: $file" >&2
    continue
  fi

  stripped="$(strip_sql_comments "$file")"
  file_findings_ids=()
  file_findings_linenos=()
  file_findings_msgs=()
  file_findings_snippets=()

  for i in "${!PATTERN_IDS[@]}"; do
    pid="${PATTERN_IDS[$i]}"
    regex="${PATTERN_REGEXES[$i]}"
    msg="${PATTERN_MESSAGES[$i]}"
    # PCRE (-P): \b \s 等の lookahead/non-capturing group 対応
    matches="$(printf '%s' "$stripped" | grep -niP "$regex" || true)"
    if [[ -n "$matches" ]]; then
      while IFS= read -r line; do
        lineno="${line%%:*}"
        snippet="${line#*:}"
        if [[ ${#snippet} -gt 100 ]]; then
          snippet="${snippet:0:100}..."
        fi
        file_findings_ids+=("$pid")
        file_findings_linenos+=("$lineno")
        file_findings_msgs+=("$msg")
        file_findings_snippets+=("$snippet")
      done <<< "$matches"
    fi
  done

  if [[ ${#file_findings_ids[@]} -gt 0 ]]; then
    UNSAFE_FILES+=("$file")
    TOTAL_UNSAFE=$((TOTAL_UNSAFE + ${#file_findings_ids[@]}))
    echo ""
    echo "## $file"
    echo ""
    for j in "${!file_findings_ids[@]}"; do
      pid="${file_findings_ids[$j]}"
      lineno="${file_findings_linenos[$j]}"
      msg="${file_findings_msgs[$j]}"
      snippet="${file_findings_snippets[$j]}"
      echo "- L${lineno} [$pid] $msg"
      echo "    > \`${snippet}\`"

      # JSON 構築
      if [[ $FIRST_FINDING -eq 0 ]]; then
        ALL_FINDINGS_JSON+=','
      fi
      FIRST_FINDING=0
      ALL_FINDINGS_JSON+=$(printf '{"file":%s,"pattern":%s,"line":%s,"message":%s,"snippet":%s}' \
        "$(json_escape "$file")" \
        "$(json_escape "$pid")" \
        "$lineno" \
        "$(json_escape "$msg")" \
        "$(json_escape "$snippet")")
    done
  else
    echo ""
    echo "## $file"
    echo ""
    echo "- rollback-safe (no contract pattern detected)"
  fi
done

ALL_FINDINGS_JSON+=']'

echo ""
echo "---"
echo ""
echo "**Summary**: $TOTAL_UNSAFE unsafe pattern(s) across ${#UNSAFE_FILES[@]} file(s)"

if [[ -n "${GITHUB_OUTPUT:-}" ]]; then
  {
    echo "unsafe_count=$TOTAL_UNSAFE"

    # unsafe_files を JSON array で
    if [[ ${#UNSAFE_FILES[@]} -eq 0 ]]; then
      echo 'unsafe_files=[]'
    else
      printf 'unsafe_files=['
      for i in "${!UNSAFE_FILES[@]}"; do
        [[ $i -gt 0 ]] && printf ','
        json_escape "${UNSAFE_FILES[$i]}"
      done
      printf ']\n'
    fi

    # findings_json は multi-line 対応で heredoc 形式
    delimiter="EOF_FINDINGS_$RANDOM"
    echo "findings_json<<$delimiter"
    echo "$ALL_FINDINGS_JSON"
    echo "$delimiter"
  } >> "$GITHUB_OUTPUT"
fi

exit 0
