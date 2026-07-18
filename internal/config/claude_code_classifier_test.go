package config

import "testing"

func TestParseConfigBytesClaudeCodeAutoModeClassifierModel(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte("claude-code-auto-mode-classifier-model: gpt-5.6-terra\nclaude-code-model-mappings:\n  client-profile: routed-model\n"))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}
	if cfg.ClaudeCodeAutoModeClassifierModel != "gpt-5.6-terra" {
		t.Fatalf("classifier model = %q, want gpt-5.6-terra", cfg.ClaudeCodeAutoModeClassifierModel)
	}
	if cfg.ClaudeCodeModelMappings["client-profile"] != "routed-model" {
		t.Fatalf("model mappings = %#v", cfg.ClaudeCodeModelMappings)
	}
}
