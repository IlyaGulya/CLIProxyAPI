# Codex parity phase 2 certification

The phase-2 transport remains guarded in the general proxy configuration. The
instrumented `claudex-next` entrypoint enables the candidate circuit breaker,
adaptive preconnect controller, and cache-aware compaction scheduler so each run
captures comparable metrics, traces, logs, schemas, and checksums.

## Deterministic gate

The soak covers route-isolated circuit open/half-open/recovery, 1006 and
handshake fallback boundaries, concurrent probes, Sol/Luna model switching,
incremental suffix compatibility, Compaction V2 replay and malformed output,
context safety, cancellation, adaptive preconnect bursts/expiry, reconnect UX,
429 cooldowns, and cache-economics thresholds. It runs each focused workload ten
times, the full Go suite, targeted race suites, and a complete build. The
artifact bundle includes a privacy scan, source/worktree identity, and SHA-256
checksums.

Reproduce it with:

```bash
scripts/codex-parity-soak.sh
```

## Bounded live comparison

Claude Code 2.1.212 completed a one-turn Sol probe with the expected output,
exit code 0, successful OTEL flush, a valid harness schema, a provisioned
Grafana dashboard, and verified artifact checksums. The candidate emitted both
`circuit_success` and `compaction_schedule_decision` events.

| Measure | guarded baseline | phase-2 candidate |
|---|---:|---:|
| Completion | success | success |
| TTFT | 3237 ms | 3327 ms |
| API duration | 3221 ms | 3314 ms |
| Cost | $0.00586 | $0.00591 |
| Requests | 2 | 2 |

One live sample is a correctness smoke test, not statistically useful evidence
of a latency improvement. Accordingly, none of the phase-2 controls becomes a
general production default. Agent-burst transport correctness and Sol → Luna →
Sol switching were already established by the preceding bounded compatibility
certification; deterministic burst tests cover the new controller policy.

## Observability and rollback

The Grafana dashboard exposes circuit decisions, reconnect status, adaptive
preconnect decisions and counterfactual wait, Compaction V2 replay shaping,
cache scheduling, token/cache ratios, replay bytes, socket utilization, and
correlated traces/logs. Raw prompts, tool payloads, credentials, session IDs,
and cache keys are not metric labels or trace attributes.

Rollback the smallest affected layer:

```yaml
codex-websocket-circuit-breaker: false
codex-websocket-adaptive-preconnect: false
codex-cache-aware-compaction: false
```

For full transport rollback:

```yaml
codex-prefer-upstream-websockets: false
```
