package config

import "testing"

func TestParseConfigBytesClaudeCodeAutoModeClassifierModel(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte("claude-code-auto-mode-classifier-model: gpt-5.6-luna\n"))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}
	if cfg.ClaudeCodeAutoModeClassifierModel != "gpt-5.6-luna" {
		t.Fatalf("classifier model = %q, want gpt-5.6-luna", cfg.ClaudeCodeAutoModeClassifierModel)
	}
}
