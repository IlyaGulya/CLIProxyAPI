# claudex-next

`claudex-next` launches Claude Code through an isolated, instrumented
CLIProxyAPI process. It accepts ordinary Claude arguments unchanged:

```bash
claudex-next
claudex-next --effort xhigh
claudex-next --print --output-format json "Check this repository"
claudex-next --continue
```

Each invocation creates a private run bundle under
`~/.claudex-next/runs/<timestamp>-<id>` and updates
`~/.claudex-next/latest`. The bundle contains:

- a privacy-safe JSON and Markdown summary;
- a checksummed evidence manifest and a machine-readable OTEL verification report;
- the exact launcher manifest and binary checksum;
- isolated CLIProxyAPI request timelines and process logs;
- Claude debug output and non-interactive stdout/stderr;
- the root and child-agent transcripts for the observed Claude session;
- a private runtime config and credential snapshot.

Run directories are created with mode `0700`. Raw request logs and transcripts
can contain prompts, tool payloads, upstream credentials, and other sensitive
data. Do not publish a complete bundle. The summary files contain only models,
correlation IDs, latency, usage, cache, connection-source, and completion data.

The launcher defaults the root model to `gpt-5.6-sol`; an explicit `--model`
still takes precedence. It does not set or rewrite
`CLAUDE_CODE_SUBAGENT_MODEL`, effort, tool-search, background-task, or
concurrency settings. Child-model and workflow policy remains in the caller's
shell or Claude configuration.

The isolated proxy enables upstream WebSockets, request timelines, speculative
preconnect, and bounded replenishment. `generate=false` warmup remains disabled.
The launcher never modifies the source CLIProxyAPI config or credential files;
it enables the Codex WebSocket capability only in the per-run private copy.

## Local observability

By default the launcher reuses or lazily starts the persistent Docker container
`claudex-next-otel-lgtm` from `grafana/otel-lgtm:0.27.1`, pinned by digest in
the launcher source.
Grafana is available at <http://127.0.0.1:3300>; OTLP/gRPC and OTLP/HTTP use
ports 4317 and 4318. Backend state is kept in the named volume
`claudex-next-otel-lgtm-data`. The launcher never deletes that volume.

The stack includes Grafana, Tempo, Prometheus, Loki, and an OpenTelemetry
Collector. `claudex-next` provisions the `claudex-next: Claude + CLIProxyAPI`
dashboard automatically. It contains the distributed waterfall, p50/p95 phase
latencies, speculative hit rate, pool state, cache/token activity, failures,
model switching, mid-response WebSocket failure semantics, connection age,
process health, and correlated logs.

On a resumed Claude session after either process restarts, the first request
starts a fresh upstream response chain and safely replays the complete prompt.
Its prompt-cache identity and canonical cacheable prefix remain stable across
processes; subsequent turns use `previous_response_id` only within the new
process-local chain. The dashboard's **Restart replay and incremental chains**
panel and each run's `summary.json` expose the chain source, reset reason,
request sizes, cache-read tokens, and prefix fingerprint without raw prompts or
session identifiers.

Claude Code exports its native metrics, events, and beta traces. CLIProxyAPI
exports HTTP request spans, DNS/TCP/TLS/WebSocket phases, upstream first-event
and first-text timings, translation/downstream flush timings, retry/failure
events, token and byte counters, pool gauges, and Go runtime metrics. All three
services carry `claudex.run_id`. For non-interactive Claude runs the launcher
also injects W3C `TRACEPARENT`; CLIProxyAPI extracts the header and links
detached speculative preconnect spans to the matching execution.

Every `request_finished` WebSocket event records the bounded semantic boundary
needed to diagnose an interrupted stream without exporting payloads: close
code, last upstream event type, counts of started/completed/incomplete tool
calls, whether a tool call remained in progress, whether downstream output was
already committed, connection source, connection age since first observed use,
and the connection request count. Tool names, tool arguments, prompts, response
bodies, session IDs, and raw error strings are excluded from OTEL attributes.

If Docker or Grafana is unavailable, Claude still starts and the complete file
bundle remains available for analysis. Set `CLAUDEX_NEXT_OTEL_STACK=off` to
disable automatic Docker management. Supplying `OTEL_EXPORTER_OTLP_ENDPOINT`
uses that collector and skips the local stack.

Explicit `OTEL_*` settings always win over launcher defaults. The defaults keep
prompt, assistant response, tool detail/content, and raw API body export off,
and exclude session/account/run resource attributes from Claude metric labels.
The private request logs and transcripts can still contain sensitive content.

Claude Code's auto-mode safety classifier uses the internal model name
`claude-sonnet-5`. `claudex-next` recognizes the classifier by its complete
request signature and routes it to `gpt-5.6-luna` by default, avoiding the
unsupported-model fallback delay. Override or disable this in the source proxy
configuration; explicit values are preserved in each isolated run:

```yaml
# Use any model available through the proxy, or set "" to disable rewriting.
claude-code-auto-mode-classifier-model: gpt-5.6-sol
```

Ordinary requests are never rewritten solely because their model name is
`claude-sonnet-5`.

Dynamic workflows commonly use the short model names `sol` and `luna`.
The isolated runtime config maps those aliases to `gpt-5.6-sol` and
`gpt-5.6-luna`. Existing user mappings for either alias take precedence.

Run the budget-guarded Sol → Luna → Sol verification with:

## Claude harness compatibility sandbox

Every JSON or stream-JSON run now writes `harness-schema.json` beside the run
manifest. It contains only event names and field paths/types; session IDs,
prompts, assistant text, tool inputs and result values are never copied into
the schema artifact. The original stdout, debug log and transcripts remain in
the private run directory for diagnosis.

Inspect the versioned probe matrix or summarize an existing harness stream:

```bash
claudex-next-compat --version 2.1.212
claudex-next-compat --version 2.1.212 --input ~/.claudex-next/latest/claude/stdout.log
```

Run one bounded live case (default budget USD 0.20):

```bash
CLAUDEX_NEXT_COMPAT_CASE=bare scripts/claudex-next-compat-e2e.sh
```

Supported cases are `bare`, `safe_mode`, `stream_json`, `partial_messages`,
`forward_subagent_text`, `prompt_suggestions`, `structured_output`,
`background_agent`, `workflow`, `fork_session`, `no_session_persistence`, and
`cancellation`. Agent/workflow cases are intentionally opt-in. `fork_session`
also requires `CLAUDEX_NEXT_COMPAT_SESSION_ID` from a completed run.

```bash
go build -o ~/.local/bin/claudex-next ./cmd/claudex-next
go build -o ~/.local/bin/cli-proxy-api-next ./cmd/server
go build -o ~/.local/bin/claudex-next-verify ./cmd/claudex-next-verify
go build -o ~/.local/bin/claudex-next-compat ./cmd/claudex-next-compat
scripts/claudex-next-otel-e2e.sh
```

The verifier checks routing, speculative WebSocket use, flush behavior,
checksums, dashboard/backend availability, trace/log correlation, metric counts
against raw timelines, and the privacy canary. It writes `verification.json`
and a publishable `verification.md` into the run directory.

### WebSocket disconnect fault injection

The executor integration suite contains deterministic upstream failures: an
abrupt close before output, an abrupt close after output, a second close after
the single retry, and non-retriable protocol/status failures. It also switches
the recovered session from Luna to Sol to detect stale model pinning. Run it
without spending model quota:

```bash
go test ./internal/runtime/executor \
  -run 'TestCodexWebsocketsExecuteStream(RetriesReadDisconnectBeforeDownstreamOutput|DoesNotRetryReadDisconnectAfterDownstreamOutput|StopsAfterOnePreOutputReadRetry)$' \
  -count=1
```

For a real `claudex-next` run, the Grafana **Failures and retries** panel shows
`transport_retry_attempted`, `transport_retry_succeeded`,
`transport_retry_exhausted`, and `transport_retry_suppressed`. In Prometheus,
start with:

```promql
sum by (event_name, finish_reason, retry_boundary) (
  increase(claudex_proxy_events_total{event_name=~"transport_retry_.*"}[6h])
)
```

The **Mid-response WebSocket failures** table distinguishes an interrupted
reasoning turn from an incomplete tool call and a completed tool boundary. The
same view is available directly in Prometheus:

```promql
sum by (
  model,
  connection_source,
  transport_close_code,
  stream_last_event_type,
  tool_call_in_progress,
  downstream_committed
) (
  increase(claudex_proxy_events_total{
    event_name="request_finished",
    finish_reason="read_error"
  }[6h])
)
```

In Tempo Explore, filter `service.name = cli-proxy-api` and the run's
`claudex.run_id`, then inspect the HTTP request span events named
`proxy.websocket.transport_retry_*`. They include the bounded close reason,
pre/post-output boundary, attempt, connection source, duration, and whether
downstream output was already committed. Session IDs, prompts, response bodies,
credentials, and raw error strings are not exported as metric or span
attributes. The corresponding `proxy.websocket.request_finished` event also
contains `transport.close_code`, `stream.last_event_type`, tool-boundary counts,
`connection.age.us`, and `connection.request_count`. The same decisions remain
in the private per-request timeline when request logging is enabled.

Launcher-only flags use the `--next-` namespace so all other arguments pass to
Claude unchanged:

```bash
claudex-next --next-proxy-binary /path/to/cli-proxy-api-next
claudex-next --next-proxy-config /path/to/config.yaml
claudex-next --next-env-file /path/to/claudex.env
claudex-next --next-runs-dir /path/to/runs
```

The installed launcher expects an instrumented `cli-proxy-api-next` binary next
to it unless `CLAUDEX_NEXT_PROXY_BINARY` or `--next-proxy-binary` is provided.
