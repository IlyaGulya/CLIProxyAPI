#!/usr/bin/env bash
set -euo pipefail

budget="${CLAUDEX_NEXT_E2E_MAX_BUDGET_USD:-0.20}"
CLAUDE_CODE_SUBAGENT_MODEL=gpt-5.6-luna claudex-next --print --output-format json --max-budget-usd "$budget" --max-turns 5 \
  "Use the Agent tool exactly once. Ask one general-purpose leaf agent to reply only LUNA_E2E_OK. After it finishes, reply only SOL_E2E_OK. Do not use workflows and do not create additional agents."
claudex-next-verify "${HOME}/.claudex-next/latest"
