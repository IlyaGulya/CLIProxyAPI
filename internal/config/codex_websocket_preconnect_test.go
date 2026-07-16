package config

import "testing"

func TestParseConfigBytesCodexWebsocketSpeculativePreconnect(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte(`
codex-websocket-speculative-preconnect: true
codex-websocket-generate-false-warmup: true
codex-websocket-preconnect-max-idle: 4
codex-websocket-preconnect-ttl-seconds: 9
`))
	if errParse != nil {
		t.Fatalf("ParseConfigBytes() error = %v", errParse)
	}
	if !cfg.CodexWebsocketSpeculativePreconnect {
		t.Fatal("codex-websocket-speculative-preconnect was not parsed")
	}
	if !cfg.CodexWebsocketGenerateFalseWarmup {
		t.Fatal("codex-websocket-generate-false-warmup was not parsed")
	}
	if cfg.CodexWebsocketPreconnectMaxIdle != 4 {
		t.Fatalf("codex-websocket-preconnect-max-idle = %d, want 4", cfg.CodexWebsocketPreconnectMaxIdle)
	}
	if cfg.CodexWebsocketPreconnectTTLSeconds != 9 {
		t.Fatalf("codex-websocket-preconnect-ttl-seconds = %d, want 9", cfg.CodexWebsocketPreconnectTTLSeconds)
	}
}
