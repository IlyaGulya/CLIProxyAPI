#!/usr/bin/env bash
set -euo pipefail

version="${CLAUDE_CODE_COMPAT_VERSION:-2.1.212}"
case_name="${CLAUDEX_NEXT_COMPAT_CASE:-bare}"
budget="${CLAUDEX_NEXT_COMPAT_BUDGET_USD:-0.20}"
prompt="${CLAUDEX_NEXT_COMPAT_PROMPT:-Reply with the single word OK.}"

args=(--print --output-format stream-json --verbose --max-budget-usd "$budget" --max-turns 3)
case "$case_name" in
  bare) args+=(--bare) ;;
  safe_mode) args+=(--safe-mode) ;;
  stream_json) args+=(--input-format stream-json --replay-user-messages) ;;
  partial_messages) args+=(--include-partial-messages) ;;
  forward_subagent_text) args+=(--forward-subagent-text) ; prompt="Use one Agent subagent, then reply OK." ;;
  prompt_suggestions) args+=(--prompt-suggestions true) ;;
  structured_output) args=(--print --output-format json --max-budget-usd "$budget" --max-turns 3 --json-schema '{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}') ;;
  background_agent) args+=(--background) ;;
  workflow) prompt="Use one dynamic workflow with one leaf agent, then reply OK." ;;
  fork_session)
    session_id="${CLAUDEX_NEXT_COMPAT_SESSION_ID:?set CLAUDEX_NEXT_COMPAT_SESSION_ID to a completed session}"
    args+=(--resume "$session_id" --fork-session)
    ;;
  no_session_persistence) args+=(--no-session-persistence) ;;
  cancellation)
    set +e
    claudex-next "${args[@]}" "$prompt" &
    pid=$!
    sleep 5
    kill -INT "$pid" 2>/dev/null
    wait "$pid"
    exit_code=$?
    set -e
    test "$exit_code" -eq 130 -o "$exit_code" -eq 0
    exit 0
    ;;
  *) echo "unknown compatibility case: $case_name" >&2; exit 2 ;;
esac

if [[ "$case_name" == "stream_json" ]]; then
  printf '%s\n' "{\"type\":\"user\",\"message\":{\"role\":\"user\",\"content\":\"$prompt\"},\"parent_tool_use_id\":null,\"session_id\":\"compat-session\"}" | claudex-next "${args[@]}"
else
  claudex-next "${args[@]}" "$prompt"
fi

run_dir="$(readlink "${HOME}/.claudex-next/latest" 2>/dev/null || true)"
if [[ -n "$run_dir" && -f "$run_dir/claude/stdout.log" ]]; then
  claudex-next-compat --version "$version" --input "$run_dir/claude/stdout.log" > "$run_dir/harness-schema.json"
  claudex-next-compat --version "$version" --run "$run_dir" --case "$case_name" > "$run_dir/compatibility-verification.json"
  echo "$run_dir"
fi
