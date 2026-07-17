#!/usr/bin/env bash
set -euo pipefail

root="${CLAUDEX_NEXT_CERTIFICATION_DIR:-${HOME}/.claudex-next/certification/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$root"
required_flags=(--bare --safe-mode --output-format --input-format --include-partial-messages --forward-subagent-text --prompt-suggestions --json-schema --background --fork-session --no-session-persistence)
versions=(2.1.210 2.1.211 2.1.212)

for version in "${versions[@]}"; do
  binary="${HOME}/.local/share/claude/versions/${version}"
  test -x "$binary"
  "$binary" --version > "$root/${version}.version.txt"
  "$binary" --help > "$root/${version}.help.txt"
  : > "$root/${version}.capabilities.tsv"
  for flag in "${required_flags[@]}"; do
    supported=false
    if grep -q -- "$flag" "$root/${version}.help.txt"; then supported=true; fi
    printf '%s\t%s\n' "$flag" "$supported" >> "$root/${version}.capabilities.tsv"
  done
  claudex-next-compat --version "$version" > "$root/${version}.matrix.json"
done

go test ./test ./internal/claudexnext ./sdk/api/handlers/claude ./internal/runtime/executor > "$root/tests.log"
go test -race ./internal/claudexnext ./sdk/api/handlers/claude ./internal/runtime/executor > "$root/race.log"

(
  cd "$root"
  shasum -a 256 ./* > SHA256SUMS
)
echo "$root"
