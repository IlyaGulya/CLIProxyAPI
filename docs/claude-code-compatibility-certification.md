# Claude Code compatibility certification

Certification date: 2026-07-17. Release candidate: `feat/claude-upstream-websocket`.

## Certified clients

| Claude Code | Role | Harness capability delta | Live result |
|---|---|---|---|
| 2.1.210 | previous supported | no `--forward-subagent-text` | Sol turn completed, exit 0 |
| 2.1.211 | previous/current boundary | adds `--forward-subagent-text` | Sol turn completed, exit 0 |
| 2.1.212 | pinned current and newest npm candidate | same tested flags as 2.1.211 | Sol turn completed, exit 0 |

The npm latest candidate was 2.1.212, so the newest candidate and pinned
current build are the same release. The wire fixture set is version keyed;
2.1.210 and 2.1.211 inherit the 2.1.212 field classifications where their
captured schema is identical, while their CLI capability matrix remains
independent.

## Evidence

Deterministic certification covers the complete protocol fixture suite, auth
and handler tests, WebSocket executor tests, race targets, and build. The fault
matrix exercises pre-commit 1006 retry, post-commit disconnect suppression,
429/capacity classification, terminal status propagation (including upstream
5xx), context pressure, fallback eligibility, SSE idle timeout, and the
single-terminal-event boundary.

Three isolated credentialed probes used a USD 0.08 cap and one turn each. All
reported `gpt-5.6-sol`, `terminal_reason=completed`, `exit_code=0`, a valid
prompt-free harness schema, successful OTEL flush, and checksummed private
artifacts. A bounded 2.1.212 mixed probe additionally proved:

- Sol root → Luna subagent → Sol continuation;
- forwarded subagent text carries a non-null `parent_tool_use_id`;
- subagent and root transcripts retain their actual models;
- the budget guard produces a terminal `error_max_budget_usd` after completed
  model output instead of corrupting the stream.

The mixed probe intentionally hit its USD 0.25 boundary because Claude Code
loads a materially larger root context outside bare mode. This is a harness
cost boundary, not a proxy protocol failure; the requested agent and root
outputs had both completed before the terminal budget event.

Private evidence is retained under the timestamped
`~/.claudex-next/certification/` directory. It includes per-version help and
capability matrices, prompt-free schema reports, request timelines, traces,
metrics, transcripts, fault/race logs, and `SHA256SUMS`. Prompt and tool content
remain excluded from public goldens and OTEL attributes.

## Rollback switches

Use the smallest switch matching the suspected boundary:

```yaml
# Force the established HTTP upstream path.
codex-prefer-upstream-websockets: false

# Keep WebSocket execution but remove speculative connections.
codex-websocket-speculative-preconnect: false
codex-websocket-preconnect-replenish: false

# Disable proxy SSE heartbeats/watchdog while diagnosing intermediaries.
streaming:
  keepalive-seconds: 0
  idle-timeout-seconds: 0

# Disable only the internal classifier rewrite.
claude-code-auto-mode-classifier-model: ""
```

Omit Claude's `--fallback-model` to disable its client-owned fallback chain.
For a complete rollback, point `claudex-next` at the previous proxy binary with
`--next-proxy-binary`; run artifacts record the exact proxy SHA-256.

## Reproduction

```bash
go build -o ~/.local/bin/claudex-next-compat ./cmd/claudex-next-compat
scripts/claude-code-cross-version-certify.sh

CLAUDEX_NEXT_COMPAT_CASE=bare scripts/claudex-next-compat-e2e.sh
```
