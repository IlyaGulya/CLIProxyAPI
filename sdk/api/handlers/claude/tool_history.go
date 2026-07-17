package claude

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const interruptedClaudeToolResult = "[Tool execution interrupted before a result was recorded]"

var errAmbiguousClaudeToolHistory = errors.New("ambiguous Claude tool history")

type claudeToolHistoryRepairResult struct {
	Applied                    bool
	RepairedToolUses           int
	SeparatedAssistantMessages int
}

type pendingClaudeToolUse struct {
	id string
}

// repairInterruptedClaudeToolHistory restores Claude's strict invariant that
// every assistant tool_use is immediately followed by a user tool_result. It
// only fabricates an explicit error result and never claims successful work.
func repairInterruptedClaudeToolHistory(input []byte) ([]byte, claudeToolHistoryRepairResult, error) {
	var root map[string]any
	if errDecode := json.Unmarshal(input, &root); errDecode != nil {
		return input, claudeToolHistoryRepairResult{}, nil
	}
	messages, _ := root["messages"].([]any)
	messages, separated := separateClaudeAssistantContentAfterToolUse(messages)
	seen := make(map[string]struct{})
	result := claudeToolHistoryRepairResult{Applied: separated > 0, SeparatedAssistantMessages: separated}
	for index := 0; index < len(messages); index++ {
		message, _ := messages[index].(map[string]any)
		if stringValue(message["role"]) != "assistant" {
			continue
		}
		uses, errUses := claudeToolUses(message["content"], seen)
		if errUses != nil {
			return input, claudeToolHistoryRepairResult{}, errUses
		}
		if len(uses) == 0 {
			continue
		}
		nextIndex := index + 1
		var next map[string]any
		if nextIndex < len(messages) {
			next, _ = messages[nextIndex].(map[string]any)
		}
		results := claudeToolResultIDs(nil)
		if stringValue(next["role"]) == "user" {
			results = claudeToolResultIDs(next["content"])
		}
		missing := make([]pendingClaudeToolUse, 0, len(uses))
		for _, use := range uses {
			if !results[use.id] {
				missing = append(missing, use)
			}
		}
		if len(missing) == 0 {
			continue
		}
		repairBlocks := make([]any, 0, len(missing))
		for _, use := range missing {
			repairBlocks = append(repairBlocks, map[string]any{
				"type": "tool_result", "tool_use_id": use.id,
				"content": interruptedClaudeToolResult, "is_error": true,
			})
		}
		if next != nil && stringValue(next["role"]) == "user" {
			next["content"] = prependClaudeContent(repairBlocks, next["content"])
		} else {
			repairMessage := map[string]any{"role": "user", "content": repairBlocks}
			messages = append(messages, nil)
			copy(messages[nextIndex+1:], messages[nextIndex:])
			messages[nextIndex] = repairMessage
			index++
		}
		result.Applied = true
		result.RepairedToolUses += len(missing)
	}
	if !result.Applied {
		return input, result, nil
	}
	root["messages"] = messages
	output, errEncode := json.Marshal(root)
	if errEncode != nil {
		return input, claudeToolHistoryRepairResult{}, fmt.Errorf("encode repaired Claude tool history: %w", errEncode)
	}
	return output, result, nil
}

func separateClaudeAssistantContentAfterToolUse(messages []any) ([]any, int) {
	separated := 0
	for index := 0; index < len(messages); index++ {
		message, _ := messages[index].(map[string]any)
		if stringValue(message["role"]) != "assistant" {
			continue
		}
		parts, _ := message["content"].([]any)
		lastToolUse := -1
		for partIndex, rawPart := range parts {
			part, _ := rawPart.(map[string]any)
			if stringValue(part["type"]) == "tool_use" {
				lastToolUse = partIndex
			}
		}
		if lastToolUse < 0 || lastToolUse == len(parts)-1 {
			continue
		}
		trailing := append([]any(nil), parts[lastToolUse+1:]...)
		message["content"] = append([]any(nil), parts[:lastToolUse+1]...)
		trailingMessage := map[string]any{"role": "assistant", "content": trailing}
		messages = append(messages, nil)
		copy(messages[index+2:], messages[index+1:])
		messages[index+1] = trailingMessage
		separated++
		index++
	}
	return messages, separated
}

func claudeToolUses(content any, seen map[string]struct{}) ([]pendingClaudeToolUse, error) {
	parts, _ := content.([]any)
	uses := make([]pendingClaudeToolUse, 0)
	for _, rawPart := range parts {
		part, _ := rawPart.(map[string]any)
		if stringValue(part["type"]) != "tool_use" {
			continue
		}
		id := strings.TrimSpace(stringValue(part["id"]))
		if id == "" {
			return nil, fmt.Errorf("%w: tool_use without id", errAmbiguousClaudeToolHistory)
		}
		if _, duplicate := seen[id]; duplicate {
			return nil, fmt.Errorf("%w: duplicate tool_use id %q", errAmbiguousClaudeToolHistory, id)
		}
		seen[id] = struct{}{}
		uses = append(uses, pendingClaudeToolUse{id: id})
	}
	return uses, nil
}

func claudeToolResultIDs(content any) map[string]bool {
	results := make(map[string]bool)
	parts, _ := content.([]any)
	for _, rawPart := range parts {
		part, _ := rawPart.(map[string]any)
		if stringValue(part["type"]) == "tool_result" {
			if id := stringValue(part["tool_use_id"]); id != "" {
				results[id] = true
			}
		}
	}
	return results
}

func prependClaudeContent(prefix []any, content any) []any {
	out := append([]any(nil), prefix...)
	switch typed := content.(type) {
	case []any:
		return append(out, typed...)
	case string:
		if strings.TrimSpace(typed) != "" {
			return append(out, map[string]any{"type": "text", "text": typed})
		}
	}
	return out
}
