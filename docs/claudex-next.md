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
- the exact launcher manifest and binary checksum;
- isolated CLIProxyAPI request timelines and process logs;
- Claude debug output and non-interactive stdout/stderr;
- the root and child-agent transcripts for the observed Claude session;
- a private runtime config and credential snapshot.

Run directories are created with mode `0700`. Raw request logs and transcripts
can contain prompts, tool payloads, upstream credentials, and other sensitive
data. Do not publish a complete bundle. The summary files contain only models,
correlation IDs, latency, usage, cache, connection-source, and completion data.

The launcher defaults to a `gpt-5.6-sol` root, `gpt-5.6-luna` children, and a
maximum Claude tool-use concurrency of three. It ignores a stale inherited
`CLAUDE_CODE_SUBAGENT_MODEL`; use `CLAUDEX_NEXT_SUBAGENT_MODEL` for an explicit
override. `CLAUDEX_NEXT_MAX_TOOL_USE_CONCURRENCY` overrides the concurrency.

The isolated proxy enables upstream WebSockets, request timelines, speculative
preconnect, and bounded replenishment. `generate=false` warmup remains disabled.
The launcher never modifies the source CLIProxyAPI config or credential files;
it enables the Codex WebSocket capability only in the per-run private copy.

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
