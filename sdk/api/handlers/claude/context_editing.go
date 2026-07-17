package claude

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/tidwall/gjson"
)

const clearedToolResultText = "[Tool result cleared to preserve context]"
const claudeContextEditGinKey = "claude_context_edit_result"

type claudeContextEditResult struct {
	Applied            bool `json:"applied"`
	ClearedToolUses    int  `json:"cleared_tool_uses"`
	ClearedToolResults int  `json:"cleared_tool_results"`
	ClearedInputTokens int  `json:"cleared_input_tokens"`
}

type claudeContextPressureResult struct {
	EstimatedInput  int
	ReservedOutput  int
	EffectiveWindow int
	Overflow        bool
}

// claudeContextPressure applies the same 95% effective-window reserve exposed
// by Codex model metadata. It is intentionally conservative: preflight exists
// to prevent a deterministic provider failure, never to promise exact billing.
func claudeContextPressure(input []byte) claudeContextPressureResult {
	const physicalWindow = 272000
	const effectiveWindow = physicalWindow * 95 / 100
	reserved := int(gjson.GetBytes(input, "max_tokens").Int())
	if reserved < 0 {
		reserved = 0
	}
	estimated := approximateTokens(input)
	return claudeContextPressureResult{
		EstimatedInput:  estimated,
		ReservedOutput:  reserved,
		EffectiveWindow: effectiveWindow,
		Overflow:        estimated+reserved > effectiveWindow,
	}
}

type toolUseLocation struct {
	message int
	part    int
	id      string
	name    string
	input   any
}

// applyClaudeContextEditing implements the client-requested clear_tool_uses
// operation before provider translation. It never edits context implicitly.
func applyClaudeContextEditing(input []byte) ([]byte, claudeContextEditResult) {
	var root map[string]any
	if json.Unmarshal(input, &root) != nil {
		return input, claudeContextEditResult{}
	}
	management, _ := root["context_management"].(map[string]any)
	edits, _ := management["edits"].([]any)
	for _, rawEdit := range edits {
		edit, _ := rawEdit.(map[string]any)
		if stringValue(edit["type"]) != "clear_tool_uses_20250919" {
			continue
		}
		trigger := nestedNumber(edit, "trigger", "value")
		if trigger > 0 && approximateTokens(input) < trigger {
			continue
		}
		messages, _ := root["messages"].([]any)
		uses := collectToolUses(messages)
		keep := nestedNumber(edit, "keep", "value")
		if keep < 0 {
			keep = 0
		}
		excluded := stringSet(edit["exclude_tools"])
		clearBefore := len(uses) - keep
		if clearBefore < 0 {
			clearBefore = 0
		}
		minimum := nestedNumber(edit, "clear_at_least", "value")
		clearInputs, _ := edit["clear_tool_inputs"].(bool)
		result := claudeContextEditResult{}
		for index, use := range uses {
			if index >= clearBefore || excluded[use.name] {
				continue
			}
			freed := clearToolPair(messages, use, clearInputs)
			if freed == 0 {
				continue
			}
			result.Applied = true
			result.ClearedToolUses++
			result.ClearedToolResults++
			result.ClearedInputTokens += freed
			if minimum > 0 && result.ClearedInputTokens >= minimum {
				break
			}
		}
		if result.Applied {
			root["messages"] = messages
			out, err := json.Marshal(root)
			if err == nil {
				return out, result
			}
		}
	}
	return input, claudeContextEditResult{}
}

func collectToolUses(messages []any) []toolUseLocation {
	var uses []toolUseLocation
	for mi, rawMessage := range messages {
		message, _ := rawMessage.(map[string]any)
		parts, _ := message["content"].([]any)
		for pi, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if stringValue(part["type"]) == "tool_use" {
				uses = append(uses, toolUseLocation{message: mi, part: pi, id: stringValue(part["id"]), name: stringValue(part["name"]), input: part["input"]})
			}
		}
	}
	return uses
}

func clearToolPair(messages []any, use toolUseLocation, clearInput bool) int {
	freed := 0
	if clearInput {
		message, _ := messages[use.message].(map[string]any)
		parts, _ := message["content"].([]any)
		part, _ := parts[use.part].(map[string]any)
		encoded, _ := json.Marshal(use.input)
		freed += approximateTokens(encoded)
		delete(part, "input")
	}
	for _, rawMessage := range messages {
		message, _ := rawMessage.(map[string]any)
		parts, _ := message["content"].([]any)
		for _, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if stringValue(part["type"]) != "tool_result" || stringValue(part["tool_use_id"]) != use.id {
				continue
			}
			encoded, _ := json.Marshal(part["content"])
			oldTokens := approximateTokens(encoded)
			part["content"] = clearedToolResultText
			newTokens := approximateTokens([]byte(clearedToolResultText))
			if oldTokens > newTokens {
				freed += oldTokens - newTokens
			}
			return freed
		}
	}
	return 0
}

func approximateTokens(value []byte) int {
	if len(value) == 0 {
		return 0
	}
	return (len(value) + 3) / 4
}

func nestedNumber(value map[string]any, outer, inner string) int {
	nested, _ := value[outer].(map[string]any)
	number, _ := nested[inner].(float64)
	return int(number)
}

func stringValue(value any) string {
	text, _ := value.(string)
	return strings.TrimSpace(text)
}

func stringSet(value any) map[string]bool {
	out := map[string]bool{}
	items, _ := value.([]any)
	for _, item := range items {
		if text := stringValue(item); text != "" {
			out[text] = true
		}
	}
	return out
}

func attachClaudeContextEditResult(payload []byte, result claudeContextEditResult) []byte {
	if !result.Applied || len(payload) == 0 {
		return payload
	}
	edit := map[string]any{
		"type":                 "clear_tool_uses_20250919",
		"cleared_tool_uses":    result.ClearedToolUses,
		"cleared_tool_results": result.ClearedToolResults,
		"cleared_input_tokens": result.ClearedInputTokens,
	}
	attach := func(raw []byte, path string) []byte {
		var object map[string]any
		if json.Unmarshal(raw, &object) != nil {
			return raw
		}
		target := object
		if path == "message" {
			message, _ := object["message"].(map[string]any)
			if message == nil {
				return raw
			}
			target = message
		}
		target["context_management"] = map[string]any{"applied_edits": []any{edit}}
		out, err := json.Marshal(object)
		if err != nil {
			return raw
		}
		return out
	}
	if json.Valid(payload) {
		return attach(payload, "")
	}
	marker := []byte("data: ")
	start := bytes.Index(payload, marker)
	if start < 0 {
		return payload
	}
	jsonStart := start + len(marker)
	end := bytes.IndexByte(payload[jsonStart:], '\n')
	if end < 0 {
		end = len(payload) - jsonStart
	}
	raw := payload[jsonStart : jsonStart+end]
	if gjson.GetBytes(raw, "type").String() != "message_start" {
		return payload
	}
	updated := attach(raw, "message")
	out := make([]byte, 0, len(payload)+len(updated)-len(raw))
	out = append(out, payload[:jsonStart]...)
	out = append(out, updated...)
	out = append(out, payload[jsonStart+end:]...)
	return out
}
