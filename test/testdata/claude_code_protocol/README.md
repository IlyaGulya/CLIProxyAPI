# Claude Code protocol fixtures

Fixtures are privacy-safe structural captures keyed by Claude Code version. They contain synthetic placeholders, never prompt text, credentials, local paths, or raw tool output.

To add or update a version:

1. Capture field names, event types, headers, and value kinds in an isolated sandbox.
2. Replace every user-controlled value with a synthetic placeholder.
3. Add the fixture filename to that version's `manifest.json`.
4. Classify every leaf path as `passthrough`, `translated`, `emulated`, `rejected`, or `ignored`.
5. Run `go test ./test -run ClaudeCodeProtocolFixtures` and review the semantic diff.

CI intentionally fails on unknown additions and stale classifications.

