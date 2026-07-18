// Package claude adapts the Anthropic Messages API used by Claude Code to the
// proxy's Codex execution path.
//
// Request ownership is intentionally narrow: request_document owns decoded
// JSON and token estimation, request_policy owns model-specific limits, and
// request_pipeline is the sole authority for ordered request mutation. HTTP
// handlers translate transport inputs and outputs but must not duplicate those
// policies. WebSocket connection lifecycle belongs to internal/runtime/executor;
// observability packages receive only privacy-safe scalar events.
package claude
