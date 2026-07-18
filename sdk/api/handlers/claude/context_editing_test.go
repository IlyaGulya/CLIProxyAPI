package claude

import (
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func TestApplyClaudeContextEditingClearsOldToolPairsAndKeepsNewest(t *testing.T) {
	t.Parallel()
	input := []byte(`{
  "model":"gpt-5.6-sol",
  "messages":[
    {"role":"assistant","content":[{"type":"tool_use","id":"old","name":"Read","input":{"file_path":"secret"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"old","content":"` + strings.Repeat("x", 8000) + `"}]},
    {"role":"assistant","content":[{"type":"tool_use","id":"new","name":"Bash","input":{"command":"pwd"}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"new","content":"recent"}]}
  ],
  "context_management":{"edits":[{"type":"clear_tool_uses_20250919","trigger":{"type":"input_tokens","value":1},"keep":{"type":"tool_uses","value":1},"clear_at_least":{"type":"input_tokens","value":1},"clear_tool_inputs":true}]}
}`)
	got, result := applyClaudeContextEditing(input)
	if result.ClearedToolUses != 1 || result.ClearedToolResults != 1 {
		t.Fatalf("result = %+v, want one old pair cleared", result)
	}
	if gjson.GetBytes(got, "messages.0.content.0.input").Exists() {
		t.Fatalf("old tool input was retained: %s", got)
	}
	if strings.Contains(string(got), strings.Repeat("x", 100)) {
		t.Fatalf("old tool result was retained")
	}
	if gjson.GetBytes(got, "messages.2.content.0.input.command").String() != "pwd" || !strings.Contains(string(got), "recent") {
		t.Fatalf("newest tool pair was changed: %s", got)
	}
}

func TestApplyClaudeContextEditingHonorsTriggerAndExcludedTools(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"a","name":"Read","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"a","content":"short"}]}],"context_management":{"edits":[{"type":"clear_tool_uses_20250919","trigger":{"type":"input_tokens","value":999999},"exclude_tools":["Read"]}]}}`)
	got, result := applyClaudeContextEditing(input)
	if result.Applied || string(got) != string(input) {
		t.Fatalf("edit applied below trigger/excluded tool: result=%+v body=%s", result, got)
	}
}

func TestApplyClaudeContextEditingClearsOldThinkingBeforeToolResults(t *testing.T) {
	t.Parallel()
	input := []byte(`{
  "messages":[
    {"role":"assistant","content":[{"type":"thinking","thinking":"old secret reasoning","signature":"sig-old"},{"type":"tool_use","id":"old","name":"Read","input":{}}]},
    {"role":"user","content":[{"type":"tool_result","tool_use_id":"old","content":"` + strings.Repeat("x", 200) + `"}]},
    {"role":"assistant","content":[{"type":"thinking","thinking":"recent reasoning","signature":"sig-recent"},{"type":"text","text":"answer"}]}
  ],
  "context_management":{"edits":[
    {"type":"clear_thinking_20251015","keep":{"type":"thinking_turns","value":1}},
    {"type":"clear_tool_uses_20250919","keep":{"type":"tool_uses","value":0},"clear_at_least":{"type":"input_tokens","value":1}}
  ]}
}`)
	output, result := applyClaudeContextEditing(input)
	if !result.Applied || result.ClearedThinkingTurns != 1 || result.ClearedToolResults != 1 || len(result.AppliedEdits) != 2 {
		t.Fatalf("result = %+v", result)
	}
	if gjson.GetBytes(output, "messages.0.content.0.type").String() == "thinking" || gjson.GetBytes(output, "messages.2.content.0.signature").String() != "sig-recent" {
		t.Fatalf("thinking keep semantics failed: %s", output)
	}
	if result.AppliedEdits[0].Type != "clear_thinking_20251015" || result.AppliedEdits[1].Type != "clear_tool_uses_20250919" {
		t.Fatalf("edit order changed: %+v", result.AppliedEdits)
	}
}

func TestApplyClaudeContextEditingKeepAllThinkingIsNoOp(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"keep","signature":"sig"}]}],"context_management":{"edits":[{"type":"clear_thinking_20251015","keep":"all"}]}}`)
	output, result := applyClaudeContextEditing(input)
	if result.Applied || string(output) != string(input) {
		t.Fatalf("keep all changed input: result=%+v output=%s", result, output)
	}
}

func TestApplyClaudeContextEditingRejectsClearThinkingAfterAnotherEdit(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"old","signature":"sig"}]}],"context_management":{"edits":[{"type":"clear_tool_uses_20250919"},{"type":"clear_thinking_20251015","keep":{"type":"thinking_turns","value":1}}]}}`)
	output, result := applyClaudeContextEditing(input)
	if result.Applied || string(output) != string(input) {
		t.Fatalf("malformed edit ordering was accepted: result=%+v output=%s", result, output)
	}
}

func TestClaudeContextPreflightReservesOutputHeadroom(t *testing.T) {
	t.Parallel()
	input := []byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"` + strings.Repeat(" antidisestablishmentarianism", 50000) + `"}]}`)
	pressure := claudeContextPressure(input)
	if !pressure.Overflow || pressure.EffectiveWindow != 180000 || pressure.ReservedOutput != 32000 {
		t.Fatalf("pressure = %+v", pressure)
	}
	if safe := claudeContextPressure([]byte(`{"model":"gpt-5.6-sol","max_tokens":1000,"messages":[{"role":"user","content":"hello"}]}`)); safe.Overflow {
		t.Fatalf("small request rejected: %+v", safe)
	}
}

func TestClaudeContextPreflightRejectsCompactPayloadWithoutOutputHeadroom(t *testing.T) {
	t.Parallel()
	// Mirrors the observed reactive-compaction regression: a roughly 950 KB
	// prose-heavy JSON request represented about 169k model tokens. A bytes/4
	// estimate previously admitted it even though the requested output did not fit.
	prose := strings.Repeat("This is compact context with repeated prose and repository state. ", 15000)
	input := []byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"` + prose + `"}]}`)
	if len(input) < 900000 {
		t.Fatalf("fixture too small: %d bytes", len(input))
	}
	pressure := claudeContextPressure(input)
	if !pressure.Overflow {
		t.Fatalf("compact payload without output headroom admitted: %+v", pressure)
	}
	if pressure.EstimatedInput < 100000 || pressure.EstimatedInput > 220000 {
		t.Fatalf("unexpected token estimate for compact fixture: %+v", pressure)
	}
}

func TestClaudeContextPreflightDoesNotTokenizeBase64AsText(t *testing.T) {
	t.Parallel()
	input := []byte(`{"model":"gpt-5.6-sol","max_tokens":1000,"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","media_type":"image/png","data":"` + strings.Repeat("A", 1200000) + `"}}]}]}`)
	pressure := claudeContextPressure(input)
	if pressure.Overflow || pressure.EstimatedInput > 10000 {
		t.Fatalf("base64 image bytes counted as text tokens: %+v", pressure)
	}
}

func TestAttachClaudeContextEditResultToResponses(t *testing.T) {
	t.Parallel()
	result := claudeContextEditResult{Applied: true, ClearedToolUses: 2, ClearedToolResults: 2, ClearedInputTokens: 100}
	nonstream := attachClaudeContextEditResult([]byte(`{"id":"msg_1","type":"message"}`), result)
	if got := gjson.GetBytes(nonstream, "context_management.applied_edits.0.cleared_tool_uses").Int(); got != 2 {
		t.Fatalf("non-stream applied edits missing: %s", nonstream)
	}
	start := []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n")
	if streamStart := attachClaudeContextEditResult(start, result); string(streamStart) != string(start) {
		t.Fatalf("applied edits must not be attached before the final delta: %s", streamStart)
	}
	stream := attachClaudeContextEditResult([]byte("event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}}\n\n"), result)
	if !strings.Contains(string(stream), `"cleared_tool_uses":2`) || !strings.HasPrefix(string(stream), "event: message_delta\n") {
		t.Fatalf("stream applied edits missing/malformed: %s", stream)
	}
}

func TestApplyClaudeCompactionReplayRetainsRecentRealUserAndDropsToolHistory(t *testing.T) {
	input := []byte(`{"system":"initial","messages":[
		{"role":"user","content":[{"type":"text","text":"recent requirement"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":"AA"}}]},
		{"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{}}]},
		{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","content":"discard output"}]},
		{"role":"assistant","content":[{"type":"compaction","content":"opaque summary"}]},
		{"role":"user","content":[{"type":"text","text":"continue"}]}
	]}`)
	output, observation := applyClaudeCompactionReplay(input, 64_000)
	if !observation.Applied || observation.RetainedUserMessages != 1 || observation.RetainedImages != 1 || observation.DroppedMessages != 2 {
		t.Fatalf("observation = %+v", observation)
	}
	serialized := string(output)
	for _, want := range []string{"initial", "recent requirement", "opaque summary", "continue"} {
		if !strings.Contains(serialized, want) {
			t.Fatalf("replay lost %q: %s", want, serialized)
		}
	}
	for _, forbidden := range []string{"tool-1", "discard output"} {
		if strings.Contains(serialized, forbidden) {
			t.Fatalf("replay retained %q: %s", forbidden, serialized)
		}
	}
}

func TestClaudeContextPressureUsesRoutedClaudeCodeHeadroom(t *testing.T) {
	input := []byte(`{"model":"gpt-5.6-luna","max_tokens":32000,"messages":[{"role":"user","content":"hello"}]}`)
	result := claudeContextPressureForEstimate(input, 141_045, "fixture")
	if !result.Overflow {
		t.Fatalf("pressure = %+v, want overflow before the upstream rejects the request", result)
	}
	if result.EffectiveWindow != 180_000 || result.SafetyMargin != 8_192 {
		t.Fatalf("pressure = %+v, want Claude Code routed window and safety margin", result)
	}
}

func TestClaudeContextPressureKeepsCodexWindowForUnrelatedModels(t *testing.T) {
	input := []byte(`{"model":"unrelated-model","max_tokens":32000}`)
	result := claudeContextPressureForEstimate(input, 141_045, "fixture")
	if result.Overflow || result.EffectiveWindow != 258_400 {
		t.Fatalf("pressure = %+v, want legacy Codex-compatible window", result)
	}
}

func TestApplyClaudeReactiveCompactBudgetBeforePreflight(t *testing.T) {
	t.Parallel()
	input := []byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":[{"type":"text","text":"Your task is to create a detailed summary of the conversation so far, paying close attention to the user's explicit requests and your previous actions. Before providing your final summary, wrap your analysis in <analysis> tags. Your entire response must be plain text: an <analysis> block followed by a <summary> block."}]}]}`)
	output, observation := applyClaudeReactiveCompactBudget(input)
	if !observation.Applied || observation.OriginalMaxTokens != 32000 || observation.BudgetedMaxTokens != 8192 {
		t.Fatalf("observation = %+v", observation)
	}
	if got := gjson.GetBytes(output, "max_tokens").Int(); got != 8192 {
		t.Fatalf("max_tokens = %d, want 8192: %s", got, output)
	}
	pressure := claudeContextPressureForEstimate(output, 160_000, "fixture")
	if pressure.Overflow {
		t.Fatalf("bounded compact should fit routed headroom: %+v", pressure)
	}
}

func TestApplyClaudeReactiveCompactBudgetDoesNotMatchOrdinarySummary(t *testing.T) {
	t.Parallel()
	input := []byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"Summarize this Gradle report in detail."}]}`)
	output, observation := applyClaudeReactiveCompactBudget(input)
	if observation.Applied || string(output) != string(input) {
		t.Fatalf("ordinary summary was treated as reactive compact: %+v %s", observation, output)
	}
}

func TestClaudeContextOverflowMessageExposesParseableGap(t *testing.T) {
	t.Parallel()
	message := claudeContextOverflowMessage(claudeContextPressureResult{
		EstimatedInput: 160_000, ReservedOutput: 32_000, SafetyMargin: 8_192, EffectiveWindow: 180_000,
	})
	for _, want := range []string{"200192 tokens", "180000 maximum", "input=160000", "requested_output=32000", "safety_margin=8192"} {
		if !strings.Contains(message, want) {
			t.Fatalf("overflow message %q missing %q", message, want)
		}
	}
}
