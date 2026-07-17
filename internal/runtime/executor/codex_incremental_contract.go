package executor

import (
	"encoding/json"
	"reflect"
)

// codexIncrementalRequestProperties mirrors the context-relevant portion of
// Codex 0.144.5 ResponsesApiRequest. Keep this struct and the accepted-field
// switch exhaustive: an unclassified upstream field fails reuse closed.
type codexIncrementalRequestProperties struct {
	Model                any `json:"model"`
	Instructions         any `json:"instructions"`
	Tools                any `json:"tools"`
	ToolChoice           any `json:"tool_choice"`
	ParallelToolCalls    any `json:"parallel_tool_calls"`
	Reasoning            any `json:"reasoning"`
	Store                any `json:"store"`
	Stream               any `json:"stream"`
	Include              any `json:"include"`
	ServiceTier          any `json:"service_tier"`
	PromptCacheKey       any `json:"prompt_cache_key"`
	Text                 any `json:"text"`
	MaxOutputTokens      any `json:"max_output_tokens"`
	MaxToolCalls         any `json:"max_tool_calls"`
	Background           any `json:"background"`
	Conversation         any `json:"conversation"`
	Metadata             any `json:"metadata"`
	Prompt               any `json:"prompt"`
	PromptCacheRetention any `json:"prompt_cache_retention"`
	SafetyIdentifier     any `json:"safety_identifier"`
	Temperature          any `json:"temperature"`
	TopLogprobs          any `json:"top_logprobs"`
	TopP                 any `json:"top_p"`
	Truncation           any `json:"truncation"`
	User                 any `json:"user"`
	ContextManagement    any `json:"context_management"`
}

func codexIncrementalPropertiesMatch(previous, current []byte) bool {
	previousProperties, ok := decodeCodexIncrementalProperties(previous)
	if !ok {
		return false
	}
	currentProperties, ok := decodeCodexIncrementalProperties(current)
	return ok && reflect.DeepEqual(previousProperties, currentProperties)
}

func decodeCodexIncrementalProperties(payload []byte) (codexIncrementalRequestProperties, bool) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(payload, &fields) != nil {
		return codexIncrementalRequestProperties{}, false
	}
	for field := range fields {
		switch field {
		case "model", "instructions", "tools", "tool_choice", "parallel_tool_calls", "reasoning",
			"store", "stream", "include", "service_tier", "prompt_cache_key", "text",
			"max_output_tokens", "max_tool_calls", "background", "conversation", "metadata", "prompt",
			"prompt_cache_retention", "safety_identifier", "temperature", "top_logprobs", "top_p",
			"truncation", "user", "context_management":
			// Context-relevant and decoded below.
		case "input", "client_metadata", "stream_options", "type", "previous_response_id":
			// Input is compared separately. The remaining fields affect delivery or
			// carry internal transport metadata, not referenced server context.
		default:
			return codexIncrementalRequestProperties{}, false
		}
	}
	var properties codexIncrementalRequestProperties
	if json.Unmarshal(payload, &properties) != nil {
		return codexIncrementalRequestProperties{}, false
	}
	return properties, true
}
