package helps

import (
	"testing"
	"time"
)

func TestClaudePromptCachePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload string
		enabled bool
		ttl     time.Duration
		wantErr bool
	}{
		{"opt in required", `{"messages":[{"role":"user","content":"hi"}]}`, false, 0, false},
		{"automatic alias defaults five minutes", `{"cache_control":{"type":"automatic"},"messages":[{"role":"user","content":"hi"}]}`, true, 5 * time.Minute, false},
		{"ephemeral one hour", `{"cache_control":{"type":"ephemeral","ttl":"1h"},"messages":[{"role":"user","content":"hi"}]}`, true, time.Hour, false},
		{"explicit only", `{"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]}`, true, 5 * time.Minute, false},
		{"same final ttl is no-op", `{"cache_control":{"type":"ephemeral","ttl":"1h"},"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`, true, time.Hour, false},
		{"different final ttl", `{"cache_control":{"type":"ephemeral","ttl":"1h"},"messages":[{"role":"user","content":[{"type":"text","text":"hi","cache_control":{"type":"ephemeral"}}]}]}`, false, 0, true},
		{"automatic consumes fifth slot", `{"cache_control":{"type":"ephemeral"},"tools":[{"name":"a","cache_control":{"type":"ephemeral"}},{"name":"b","cache_control":{"type":"ephemeral"}},{"name":"c","cache_control":{"type":"ephemeral"}},{"name":"d","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"user","content":"hi"}]}`, false, 0, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			enabled, ttl, err := claudePromptCachePolicy([]byte(test.payload))
			if enabled != test.enabled || ttl != test.ttl || (err != nil) != test.wantErr {
				t.Fatalf("policy = enabled:%v ttl:%v err:%v", enabled, ttl, err)
			}
		})
	}
}

func TestClaudeCodePromptCacheIsolationAndTTLRefresh(t *testing.T) {
	payload := []byte(`{"cache_control":{"type":"ephemeral"},"metadata":{"user_id":"{\"session_id\":\"policy-session\"}"},"messages":[{"role":"user","content":"hi"}]}`)
	first, ok, err := ClaudeCodePromptCacheForAuth(t.Context(), "codex", "model-a", "auth-a", payload, nil)
	if err != nil || !ok {
		t.Fatalf("first cache = %+v, %v, %v", first, ok, err)
	}
	time.Sleep(time.Millisecond)
	second, ok, err := ClaudeCodePromptCacheForAuth(t.Context(), "codex", "model-a", "auth-a", payload, nil)
	if err != nil || !ok || second.ID != first.ID || !second.Expire.After(first.Expire) {
		t.Fatalf("cache hit did not refresh: first=%+v second=%+v ok=%v err=%v", first, second, ok, err)
	}
	otherModel, _, _ := ClaudeCodePromptCacheForAuth(t.Context(), "codex", "model-b", "auth-a", payload, nil)
	otherAuth, _, _ := ClaudeCodePromptCacheForAuth(t.Context(), "codex", "model-a", "auth-b", payload, nil)
	if otherModel.ID == first.ID || otherAuth.ID == first.ID {
		t.Fatalf("cache identity crossed model/auth: first=%s model=%s auth=%s", first.ID, otherModel.ID, otherAuth.ID)
	}
}

func TestClaudePromptCacheDecisionIsPrivacySafe(t *testing.T) {
	payload := []byte(`{"cache_control":{"type":"automatic","ttl":"1h"},"messages":[{"role":"user","content":"private prompt"}]}`)
	enabled, ttl, reason := ClaudePromptCacheDecision(payload)
	if !enabled || ttl != "1h" || reason != "requested" {
		t.Fatalf("decision = %v %q %q", enabled, ttl, reason)
	}
}
