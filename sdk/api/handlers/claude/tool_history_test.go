package claude

import (
	"errors"
	"testing"

	"github.com/tidwall/gjson"
)

func TestRepairInterruptedClaudeToolHistoryInsertsExplicitErrorResult(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"user","content":"run"},{"role":"assistant","content":[{"type":"thinking","thinking":"x","signature":"sig"},{"type":"tool_use","id":"call-1","name":"Bash","input":{"cmd":"pwd"},"parent_tool_use_id":"parent-1"}]},{"role":"user","content":"continue"}]}`)
	output, result, err := repairInterruptedClaudeToolHistory(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.RepairedToolUses != 1 || !result.Applied {
		t.Fatalf("repair result = %+v", result)
	}
	block := gjson.GetBytes(output, "messages.2.content.0")
	if block.Get("type").String() != "tool_result" || block.Get("tool_use_id").String() != "call-1" || !block.Get("is_error").Bool() {
		t.Fatalf("interrupted result = %s; output=%s", block.Raw, output)
	}
	if got := gjson.GetBytes(output, "messages.1.content.0.signature").String(); got != "sig" {
		t.Fatalf("thinking signature changed: %q", got)
	}
	if got := gjson.GetBytes(output, "messages.1.content.1.parent_tool_use_id").String(); got != "parent-1" {
		t.Fatalf("parent_tool_use_id changed: %q", got)
	}
}

func TestRepairInterruptedClaudeToolHistoryIsIdempotentAndHandlesParallelTools(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"a","name":"A","input":{}},{"type":"tool_use","id":"b","name":"B","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"a","content":"ok"}]}]}`)
	first, result, err := repairInterruptedClaudeToolHistory(input)
	if err != nil || result.RepairedToolUses != 1 {
		t.Fatalf("first repair = %+v, %v: %s", result, err, first)
	}
	second, secondResult, err := repairInterruptedClaudeToolHistory(first)
	if err != nil || secondResult.Applied || string(first) != string(second) {
		t.Fatalf("second repair = %+v, %v\nfirst=%s\nsecond=%s", secondResult, err, first, second)
	}
	if got := gjson.GetBytes(first, "messages.1.content.0.tool_use_id").String(); got != "b" {
		t.Fatalf("missing result was not prepended in tool order: %q", got)
	}
}

func TestRepairInterruptedClaudeToolHistoryRejectsDuplicateIDs(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"dup","name":"A","input":{}},{"type":"tool_use","id":"dup","name":"B","input":{}}]}]}`)
	_, _, err := repairInterruptedClaudeToolHistory(input)
	if !errors.Is(err, errAmbiguousClaudeToolHistory) {
		t.Fatalf("error = %v", err)
	}
}

func TestRepairInterruptedClaudeToolHistorySeparatesTrailingAssistantContent(t *testing.T) {
	t.Parallel()
	input := []byte(`{"messages":[{"role":"assistant","content":[{"type":"text","text":"before"},{"type":"tool_use","id":"call-1","name":"A","input":{}},{"type":"text","text":"after"}]}]}`)
	output, result, err := repairInterruptedClaudeToolHistory(input)
	if err != nil || !result.Applied {
		t.Fatalf("repair = %+v, %v", result, err)
	}
	if got := gjson.GetBytes(output, "messages.0.content.#").Int(); got != 2 {
		t.Fatalf("tool message parts = %d, output=%s", got, output)
	}
	if got := gjson.GetBytes(output, "messages.1.content.0.type").String(); got != "tool_result" {
		t.Fatalf("repair message missing: %s", output)
	}
	if got := gjson.GetBytes(output, "messages.2.content.0.text").String(); got != "after" {
		t.Fatalf("trailing assistant content not separated: %s", output)
	}
}

func FuzzRepairInterruptedClaudeToolHistoryIdempotent(f *testing.F) {
	f.Add([]byte(`{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"a","name":"A","input":{}}]}]}`))
	f.Add([]byte(`{"messages":[{"role":"user","content":"hello"}]}`))
	f.Fuzz(func(t *testing.T, input []byte) {
		first, _, err := repairInterruptedClaudeToolHistory(input)
		if err != nil {
			return
		}
		second, result, err := repairInterruptedClaudeToolHistory(first)
		if err != nil || result.Applied || string(first) != string(second) {
			t.Fatalf("repair not idempotent: result=%+v err=%v\nfirst=%q\nsecond=%q", result, err, first, second)
		}
	})
}
