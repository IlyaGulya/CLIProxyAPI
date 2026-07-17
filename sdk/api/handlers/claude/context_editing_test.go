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

func TestClaudeContextPreflightReservesOutputHeadroom(t *testing.T) {
	t.Parallel()
	input := []byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"` + strings.Repeat("x", 910000) + `"}]}`)
	pressure := claudeContextPressure(input)
	if !pressure.Overflow || pressure.EffectiveWindow != 258400 || pressure.ReservedOutput != 32000 {
		t.Fatalf("pressure = %+v", pressure)
	}
	if safe := claudeContextPressure([]byte(`{"model":"gpt-5.6-sol","max_tokens":1000,"messages":[{"role":"user","content":"hello"}]}`)); safe.Overflow {
		t.Fatalf("small request rejected: %+v", safe)
	}
}

func TestAttachClaudeContextEditResultToResponses(t *testing.T) {
	t.Parallel()
	result := claudeContextEditResult{Applied: true, ClearedToolUses: 2, ClearedToolResults: 2, ClearedInputTokens: 100}
	nonstream := attachClaudeContextEditResult([]byte(`{"id":"msg_1","type":"message"}`), result)
	if got := gjson.GetBytes(nonstream, "context_management.applied_edits.0.cleared_tool_uses").Int(); got != 2 {
		t.Fatalf("non-stream applied edits missing: %s", nonstream)
	}
	stream := attachClaudeContextEditResult([]byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\"}}\n\n"), result)
	if !strings.Contains(string(stream), `"cleared_tool_uses":2`) || !strings.HasPrefix(string(stream), "event: message_start\n") {
		t.Fatalf("stream applied edits missing/malformed: %s", stream)
	}
}
