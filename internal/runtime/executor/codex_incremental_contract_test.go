package executor

import (
	"encoding/hex"
	"encoding/json"
	"testing"
)

func TestCodexIncrementalPropertiesContract(t *testing.T) {
	base := map[string]any{
		"model": "gpt-5.6-sol", "instructions": "system", "input": []any{},
		"tools": []any{}, "tool_choice": "auto", "parallel_tool_calls": true,
		"reasoning": map[string]any{"effort": "high"}, "store": false, "stream": true,
		"stream_options": map[string]any{"include_usage": true}, "include": []any{"reasoning.encrypted_content"},
		"service_tier": "default", "prompt_cache_key": "scope-a", "text": map[string]any{"format": map[string]any{"type": "text"}},
		"max_output_tokens": 1024, "max_tool_calls": 8, "background": false, "conversation": nil,
		"metadata": map[string]any{}, "prompt": nil, "prompt_cache_retention": "24h",
		"safety_identifier": "safe-a", "temperature": 1.0, "top_logprobs": 0, "top_p": 1.0,
		"truncation": "disabled", "user": "user-a", "context_management": []any{},
		"client_metadata": map[string]any{"request_id": "one"}, "type": "response.create",
	}
	encode := func(value map[string]any) []byte {
		payload, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	for _, field := range []string{
		"model", "instructions", "tools", "tool_choice", "parallel_tool_calls", "reasoning",
		"store", "stream", "include", "service_tier", "prompt_cache_key", "text",
		"max_output_tokens", "max_tool_calls", "background", "conversation", "metadata", "prompt",
		"prompt_cache_retention", "safety_identifier", "temperature", "top_logprobs", "top_p",
		"truncation", "user", "context_management",
	} {
		t.Run("context_"+field, func(t *testing.T) {
			changed := cloneJSONMap(t, base)
			changed[field] = map[string]any{"changed": true}
			if codexIncrementalPropertiesMatch(encode(base), encode(changed)) {
				t.Fatalf("field %q change was treated as compatible", field)
			}
		})
	}
	for _, field := range []string{"input", "client_metadata", "stream_options", "type", "previous_response_id"} {
		t.Run("delivery_"+field, func(t *testing.T) {
			changed := cloneJSONMap(t, base)
			changed[field] = map[string]any{"changed": true}
			if !codexIncrementalPropertiesMatch(encode(base), encode(changed)) {
				t.Fatalf("delivery field %q blocked compatible reuse", field)
			}
		})
	}
}

func TestCodexIncrementalPropertiesRejectUnknownField(t *testing.T) {
	previous := []byte(`{"model":"gpt-5.6-sol","input":[]}`)
	current := []byte(`{"model":"gpt-5.6-sol","input":[],"future_context_knob":true}`)
	if codexIncrementalPropertiesMatch(previous, current) {
		t.Fatal("unknown request field must fail closed")
	}
}

func TestCodexIncrementalInputNormalizesOnlyTransportItemMetadata(t *testing.T) {
	previous := []byte(`{"model":"gpt-5.6-sol","input":[{"type":"message","id":"msg-old","status":"completed","role":"user","content":[{"type":"input_text","text":"one","annotations":[]}]}]}`)
	current := []byte(`{"model":"gpt-5.6-sol","input":[{"type":"message","id":"msg-new","status":"in_progress","role":"user","content":[{"type":"input_text","text":"one"}]}]}`)
	delta, ok := codexIncrementalInput(previous, nil, current)
	if !ok || string(delta) != "[]" {
		t.Fatalf("metadata-only item change delta=%s ok=%v", delta, ok)
	}

	changedAnnotation := []byte(`{"model":"gpt-5.6-sol","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"one","annotations":[{"type":"citation","url":"https://example.test"}]}]}]}`)
	if delta, ok = codexIncrementalInput(previous, nil, changedAnnotation); ok {
		t.Fatalf("semantic annotation change produced delta %s", delta)
	}
}

func TestCodexIncrementalInputIncludesOnlyStrictSuffix(t *testing.T) {
	previous := []byte(`{"model":"gpt-5.6-sol","input":[{"type":"message","role":"user","content":"one"}]}`)
	output := []byte(`[{"type":"message","role":"assistant","content":"two"}]`)
	current := []byte(`{"model":"gpt-5.6-sol","input":[{"type":"message","role":"user","content":"one"},{"type":"message","role":"assistant","content":"two"},{"type":"message","role":"user","content":"three"}]}`)
	delta, ok := codexIncrementalInput(previous, output, current)
	if !ok || string(delta) != `[{"content":"three","role":"user","type":"message"}]` {
		t.Fatalf("strict suffix delta=%s ok=%v", delta, ok)
	}
}

func FuzzCodexIncrementalPropertiesNeverReuseAcrossModel(f *testing.F) {
	f.Add("gpt-5.6-sol", "gpt-5.6-luna")
	f.Fuzz(func(t *testing.T, previousModel, currentModel string) {
		previousModel = hex.EncodeToString([]byte(previousModel))
		currentModel = hex.EncodeToString([]byte(currentModel))
		if previousModel == currentModel {
			return
		}
		previous, _ := json.Marshal(map[string]any{"model": previousModel, "input": []any{}})
		current, _ := json.Marshal(map[string]any{"model": currentModel, "input": []any{}})
		if codexIncrementalPropertiesMatch(previous, current) {
			t.Fatal("different models reused a response chain")
		}
	})
}

func cloneJSONMap(t *testing.T, value map[string]any) map[string]any {
	t.Helper()
	payload, _ := json.Marshal(value)
	var cloned map[string]any
	if err := json.Unmarshal(payload, &cloned); err != nil {
		t.Fatal(err)
	}
	return cloned
}
