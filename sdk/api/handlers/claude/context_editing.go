package claude

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
)

const clearedToolResultText = "[Tool result cleared to preserve context]"
const claudeContextEditGinKey = "claude_context_edit_result"
const claudeCompactionV2RetainedTokenBudget = 64_000
const claudeReactiveCompactMaxTokens = 8_192
const claudeMinimumUsefulOutputTokens = 8_192

type claudeContextEditResult struct {
	Applied              bool                       `json:"applied"`
	ClearedThinkingTurns int                        `json:"cleared_thinking_turns"`
	ClearedToolUses      int                        `json:"cleared_tool_uses"`
	ClearedToolResults   int                        `json:"cleared_tool_results"`
	ClearedInputTokens   int                        `json:"cleared_input_tokens"`
	AppliedEdits         []claudeAppliedContextEdit `json:"applied_edits"`
}

type claudeAppliedContextEdit struct {
	Type                 string `json:"type"`
	ClearedThinkingTurns int    `json:"cleared_thinking_turns,omitempty"`
	ClearedToolUses      int    `json:"cleared_tool_uses,omitempty"`
	ClearedToolResults   int    `json:"cleared_tool_results,omitempty"`
	ClearedInputTokens   int    `json:"cleared_input_tokens"`
}

type claudeContextPressureResult struct {
	EstimatedInput  int
	ReservedOutput  int
	EffectiveWindow int
	SafetyMargin    int
	Overflow        bool
	Method          string
	MetadataSource  string
}

type claudeCompactionReplayObservation struct {
	Applied              bool
	RetainedUserMessages int
	RetainedImages       int
	DroppedMessages      int
	RetainedTokens       int
}

type claudeReactiveCompactBudgetObservation struct {
	Applied           bool
	OriginalMaxTokens int
	BudgetedMaxTokens int
}

type claudeAdaptiveOutputBudgetObservation struct {
	Applied           bool
	OriginalMaxTokens int
	BudgetedMaxTokens int
	EstimatedInput    int
	Method            string
}

func claudeMessageText(content any) string {
	if text, ok := content.(string); ok {
		return text
	}
	parts, _ := content.([]any)
	var joined strings.Builder
	for _, rawPart := range parts {
		part, _ := rawPart.(map[string]any)
		if stringValue(part["type"]) == "text" {
			joined.WriteString(stringValue(part["text"]))
			joined.WriteByte('\n')
		}
	}
	return joined.String()
}

// applyClaudeCompactionReplay emulates Claude's stateless compaction replay
// contract for providers that do not understand Claude compaction blocks.
// It keeps a bounded newest-first tail of real user/image messages, the latest
// compaction block, and all messages following that block. Tool-only history is
// deliberately excluded so replay cannot create orphan tool pairs.
func applyClaudeCompactionReplay(input []byte, retainedTokenBudget int) ([]byte, claudeCompactionReplayObservation) {
	var root map[string]any
	if json.Unmarshal(input, &root) != nil {
		return input, claudeCompactionReplayObservation{}
	}
	observation := applyClaudeCompactionReplayRoot(root, retainedTokenBudget)
	if !observation.Applied {
		return input, observation
	}
	output, err := json.Marshal(root)
	if err != nil {
		return input, claudeCompactionReplayObservation{}
	}
	return output, observation
}

func applyClaudeCompactionReplayRoot(root map[string]any, retainedTokenBudget int) claudeCompactionReplayObservation {
	messages, _ := root["messages"].([]any)
	compactionMessage, compactionPart := -1, -1
	for messageIndex, rawMessage := range messages {
		message, _ := rawMessage.(map[string]any)
		parts, _ := message["content"].([]any)
		for partIndex, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if stringValue(part["type"]) == "compaction" && stringValue(part["content"]) != "" {
				compactionMessage, compactionPart = messageIndex, partIndex
			}
		}
	}
	if compactionMessage < 0 {
		return claudeCompactionReplayObservation{}
	}
	if retainedTokenBudget <= 0 {
		retainedTokenBudget = claudeCompactionV2RetainedTokenBudget
	}
	observation := claudeCompactionReplayObservation{Applied: true}
	remaining := retainedTokenBudget
	retainedReversed := make([]any, 0, compactionMessage)
	for index := compactionMessage - 1; index >= 0; index-- {
		message, _ := messages[index].(map[string]any)
		images, realUser := claudeRealUserMessageImages(message)
		if !realUser || remaining == 0 {
			observation.DroppedMessages++
			continue
		}
		encoded, _ := json.Marshal(message)
		tokens, _ := estimateClaudeGPTInputTokens(encoded)
		if tokens < 1 {
			tokens = 1
		}
		if tokens > remaining {
			observation.DroppedMessages++
			continue
		}
		retainedReversed = append(retainedReversed, message)
		remaining -= tokens
		observation.RetainedTokens += tokens
		observation.RetainedImages += images
		observation.RetainedUserMessages++
	}
	retained := make([]any, len(retainedReversed))
	for index := range retainedReversed {
		retained[len(retainedReversed)-1-index] = retainedReversed[index]
	}
	compaction, _ := messages[compactionMessage].(map[string]any)
	parts, _ := compaction["content"].([]any)
	compaction["content"] = parts[compactionPart:]
	retained = append(retained, compaction)
	retained = append(retained, messages[compactionMessage+1:]...)
	root["messages"] = retained
	return observation
}

func claudeRealUserMessageImages(message map[string]any) (int, bool) {
	if stringValue(message["role"]) != "user" {
		return 0, false
	}
	switch content := message["content"].(type) {
	case string:
		return 0, strings.TrimSpace(content) != ""
	case []any:
		images := 0
		real := false
		for _, rawPart := range content {
			part, _ := rawPart.(map[string]any)
			switch stringValue(part["type"]) {
			case "text", "image":
				real = true
				if stringValue(part["type"]) == "image" {
					images++
				}
			}
		}
		return images, real
	default:
		return 0, false
	}
}

// claudeContextPressure applies the same 95% effective-window reserve exposed
// by Codex model metadata. It is intentionally conservative: preflight exists
// to prevent a deterministic provider failure, never to promise exact billing.
func claudeContextPressure(input []byte) claudeContextPressureResult {
	estimated, method := estimateClaudeGPTInputTokens(input)
	return claudeContextPressureForEstimate(input, estimated, method)
}

func claudeContextPressureForEstimate(input []byte, estimated int, method string) claudeContextPressureResult {
	policy := claudePolicyFor(gjson.GetBytes(input, "model").String(), claudeRequestInteractive)
	effectiveWindow, safetyMargin := policy.EffectiveWindow, policy.SafetyMargin
	reserved := int(gjson.GetBytes(input, "max_tokens").Int())
	if reserved < 0 {
		reserved = 0
	}
	return claudeContextPressureResult{
		EstimatedInput:  estimated,
		ReservedOutput:  reserved,
		EffectiveWindow: effectiveWindow,
		SafetyMargin:    safetyMargin,
		Overflow:        estimated+reserved+safetyMargin > effectiveWindow,
		Method:          method,
		MetadataSource:  policy.MetadataSource,
	}
}

func claudeContextLimits(model string) (effectiveWindow, safetyMargin int) {
	policy := claudePolicyFor(model, claudeRequestInteractive)
	return policy.EffectiveWindow, policy.SafetyMargin
}

func claudeContextOverflowMessage(pressure claudeContextPressureResult) string {
	total := pressure.EstimatedInput + pressure.ReservedOutput + pressure.SafetyMargin
	return fmt.Sprintf("Prompt is too long: %d tokens > %d maximum (input=%d, requested_output=%d, safety_margin=%d)",
		total, pressure.EffectiveWindow, pressure.EstimatedInput, pressure.ReservedOutput, pressure.SafetyMargin)
}

func estimateClaudeGPTInputTokens(input []byte) (int, string) {
	var root any
	if json.Unmarshal(input, &root) != nil {
		return approximateTokens(input), "bytes_fallback"
	}
	return estimateClaudeGPTInputTokensValue(root)
}

func sanitizeClaudeTokenInput(value any, images *int) any {
	switch typed := value.(type) {
	case []any:
		out := make([]any, len(typed))
		for index := range typed {
			out[index] = sanitizeClaudeTokenInput(typed[index], images)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(typed))
		isImage := strings.Contains(strings.ToLower(stringValue(typed["type"])), "image")
		isBase64 := strings.EqualFold(stringValue(typed["type"]), "base64")
		if isImage {
			*images++
		}
		for key, child := range typed {
			if isBase64 && key == "data" {
				out[key] = "[image bytes omitted]"
				continue
			}
			out[key] = sanitizeClaudeTokenInput(child, images)
		}
		return out
	default:
		return value
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
	result := applyClaudeContextEditingRoot(root)
	if result.Applied {
		out, err := json.Marshal(root)
		if err == nil {
			return out, result
		}
	}
	return input, claudeContextEditResult{}
}

func applyClaudeContextEditingRoot(root map[string]any) claudeContextEditResult {
	management, _ := root["context_management"].(map[string]any)
	edits, _ := management["edits"].([]any)
	messages, _ := root["messages"].([]any)
	result := claudeContextEditResult{}
	for editIndex, rawEdit := range edits {
		edit, _ := rawEdit.(map[string]any)
		switch stringValue(edit["type"]) {
		case "clear_thinking_20251015":
			// Anthropic requires clear_thinking to precede every other edit. Treat
			// malformed policies as a no-op rather than silently reordering them.
			if editIndex != 0 {
				return claudeContextEditResult{}
			}
			clearedTurns, clearedTokens := clearClaudeThinkingTurns(messages, edit["keep"])
			if clearedTurns == 0 {
				continue
			}
			result.Applied = true
			result.ClearedThinkingTurns += clearedTurns
			result.ClearedInputTokens += clearedTokens
			result.AppliedEdits = append(result.AppliedEdits, claudeAppliedContextEdit{
				Type: "clear_thinking_20251015", ClearedThinkingTurns: clearedTurns, ClearedInputTokens: clearedTokens,
			})
		case "clear_tool_uses_20250919":
			trigger := nestedNumber(edit, "trigger", "value")
			encoded, _ := json.Marshal(root)
			if trigger > 0 && approximateTokens(encoded) < trigger {
				continue
			}
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
			applied := claudeAppliedContextEdit{Type: "clear_tool_uses_20250919"}
			for index, use := range uses {
				if index >= clearBefore || excluded[use.name] {
					continue
				}
				freed := clearToolPair(messages, use, clearInputs)
				if freed == 0 {
					continue
				}
				applied.ClearedToolUses++
				applied.ClearedToolResults++
				applied.ClearedInputTokens += freed
				if minimum > 0 && applied.ClearedInputTokens >= minimum {
					break
				}
			}
			if applied.ClearedToolUses > 0 {
				result.Applied = true
				result.ClearedToolUses += applied.ClearedToolUses
				result.ClearedToolResults += applied.ClearedToolResults
				result.ClearedInputTokens += applied.ClearedInputTokens
				result.AppliedEdits = append(result.AppliedEdits, applied)
			}
		}
	}
	if result.Applied {
		root["messages"] = messages
	}
	return result
}

func clearClaudeThinkingTurns(messages []any, keepValue any) (int, int) {
	if text, ok := keepValue.(string); ok && strings.EqualFold(strings.TrimSpace(text), "all") {
		return 0, 0
	}
	keep := 1
	if keepObject, ok := keepValue.(map[string]any); ok {
		if stringValue(keepObject["type"]) == "all" {
			return 0, 0
		}
		if stringValue(keepObject["type"]) == "thinking_turns" {
			value, ok := keepObject["value"].(float64)
			if !ok || value <= 0 {
				return 0, 0
			}
			keep = int(value)
		}
	}
	thinkingMessages := make([]int, 0)
	for messageIndex, rawMessage := range messages {
		message, _ := rawMessage.(map[string]any)
		if stringValue(message["role"]) != "assistant" {
			continue
		}
		parts, _ := message["content"].([]any)
		for _, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			partType := stringValue(part["type"])
			if partType == "thinking" || partType == "redacted_thinking" {
				thinkingMessages = append(thinkingMessages, messageIndex)
				break
			}
		}
	}
	clearCount := len(thinkingMessages) - keep
	if clearCount <= 0 {
		return 0, 0
	}
	clearedTokens := 0
	for _, messageIndex := range thinkingMessages[:clearCount] {
		message, _ := messages[messageIndex].(map[string]any)
		parts, _ := message["content"].([]any)
		kept := make([]any, 0, len(parts))
		for _, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			partType := stringValue(part["type"])
			if partType == "thinking" || partType == "redacted_thinking" {
				encoded, _ := json.Marshal(part)
				clearedTokens += approximateTokens(encoded)
				continue
			}
			kept = append(kept, rawPart)
		}
		message["content"] = kept
	}
	return clearCount, clearedTokens
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

func intValue(value any) int {
	switch typed := value.(type) {
	case float64:
		return int(typed)
	case int:
		return typed
	case int64:
		return int(typed)
	default:
		return 0
	}
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
	edits := result.AppliedEdits
	if len(edits) == 0 {
		edits = []claudeAppliedContextEdit{{
			Type: "clear_tool_uses_20250919", ClearedToolUses: result.ClearedToolUses,
			ClearedToolResults: result.ClearedToolResults, ClearedInputTokens: result.ClearedInputTokens,
		}}
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
		target["context_management"] = map[string]any{"applied_edits": edits}
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
	eventType := gjson.GetBytes(raw, "type").String()
	if eventType != "message_delta" {
		return payload
	}
	updated := attach(raw, "")
	out := make([]byte, 0, len(payload)+len(updated)-len(raw))
	out = append(out, payload[:jsonStart]...)
	out = append(out, updated...)
	out = append(out, payload[jsonStart+end:]...)
	return out
}
