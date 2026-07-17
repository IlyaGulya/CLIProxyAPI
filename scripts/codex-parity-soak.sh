#!/usr/bin/env bash
set -euo pipefail

root="${CLAUDEX_NEXT_SOAK_DIR:-${HOME}/.claudex-next/soak/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$root"

git rev-parse HEAD > "$root/revision.txt"
git status --short > "$root/worktree-status.txt"
git diff --binary | shasum -a 256 > "$root/worktree-diff.sha256"
go version > "$root/go-version.txt"

focused='TestCodexWebsocketCircuit|TestCodexAdaptivePreconnect|TestCodexCompactionScheduler|TestBuildCodexCompactionV2|TestShapeCodexCompactionV2|TestCompactionReplay|TestCodexReconnectStatus|TestClaudeCodexWebsocket|TestCodexAutoExecutorCircuit'
go test ./internal/runtime/executor ./sdk/api/handlers/claude -run "$focused" -count=10 > "$root/mixed-fault-soak.log"
go test -race ./internal/runtime/executor ./sdk/api/handlers/claude ./internal/observability ./internal/claudexnext > "$root/race.log"
go test ./... > "$root/full-suite.log"
go build ./... > "$root/build.log"

cat > "$root/rollout.json" <<'JSON'
{
  "production_defaults_changed": false,
  "candidate_entrypoint": "claudex-next",
  "rollback": {
    "circuit": "codex-websocket-circuit-breaker: false",
    "adaptive_preconnect": "codex-websocket-adaptive-preconnect: false",
    "cache_aware_compaction": "codex-cache-aware-compaction: false",
    "all_websockets": "codex-prefer-upstream-websockets: false"
  }
}
JSON

if grep -E -i '(authorization: bearer|access_token["=: ]|private[_-]?key)' "$root"/*.log >/dev/null; then
  printf '{"passed":false,"reason":"credential-like material found"}\n' > "$root/privacy.json"
  exit 1
fi
printf '{"passed":true,"prompt_content_exported":false,"credential_material_found":false}\n' > "$root/privacy.json"

(
  cd "$root"
  shasum -a 256 ./* > SHA256SUMS
)
printf '%s\n' "$root"
