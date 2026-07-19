package executor

import (
	"strings"

	"github.com/tidwall/gjson"
)

type codexStreamEventKind uint8

const (
	codexStreamUnknown codexStreamEventKind = iota
	codexStreamEnvelope
	codexStreamReasoning
	codexStreamText
	codexStreamTool
	codexStreamTerminal
	codexStreamError
)

type codexToolTransition uint8

const (
	codexToolUnchanged codexToolTransition = iota
	codexToolStarted
	codexToolCompleted
)

type codexStreamEvent struct {
	Type           string
	Kind           codexStreamEventKind
	ItemType       string
	HasTextDelta   bool
	ToolTransition codexToolTransition
}

func classifyCodexStreamEvent(payload []byte) codexStreamEvent {
	if !gjson.ValidBytes(payload) {
		return codexStreamEvent{}
	}
	event := codexStreamEvent{
		Type:     strings.TrimSpace(gjson.GetBytes(payload, "type").String()),
		ItemType: strings.TrimSpace(gjson.GetBytes(payload, "item.type").String()),
	}
	if event.Type == "" {
		return event
	}
	if isCodexCompletionEvent(event.Type) {
		event.Kind = codexStreamTerminal
		return event
	}
	if event.Type == "error" {
		event.Kind = codexStreamError
		return event
	}
	if strings.HasPrefix(event.Type, "response.reasoning") {
		event.Kind = codexStreamReasoning
		return event
	}
	if strings.HasPrefix(event.Type, "response.output_text.") {
		event.Kind = codexStreamText
		event.HasTextDelta = event.Type == "response.output_text.delta" && gjson.GetBytes(payload, "delta").String() != ""
		return event
	}
	if event.Type == "response.output_item.added" {
		if isCodexToolItemType(event.ItemType) {
			event.Kind = codexStreamTool
			event.ToolTransition = codexToolStarted
		} else {
			event.Kind = codexStreamEnvelope
		}
		return event
	}
	if event.Type == "response.output_item.done" {
		if isCodexToolItemType(event.ItemType) {
			event.Kind = codexStreamTool
			event.ToolTransition = codexToolCompleted
		} else {
			event.Kind = codexStreamEnvelope
		}
		return event
	}
	if strings.Contains(event.Type, "function_call") || strings.Contains(event.Type, "custom_tool_call") {
		event.Kind = codexStreamTool
		return event
	}
	switch event.Type {
	case "response.created", "response.in_progress", "response.queued", "response.content_part.added", "response.content_part.done":
		event.Kind = codexStreamEnvelope
	}
	return event
}

func isCodexToolItemType(itemType string) bool {
	return itemType != "" && itemType != "message" && itemType != "reasoning"
}
