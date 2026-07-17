package config

import "testing"

func TestCodexWebsocketCircuitConfig(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(`
codex-websocket-circuit-breaker: true
codex-websocket-circuit-failure-threshold: 4
codex-websocket-circuit-cooldown-seconds: 17
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.CodexWebsocketCircuitBreaker {
		t.Fatal("circuit breaker was not enabled")
	}
	if cfg.CodexWebsocketCircuitFailureThreshold != 4 {
		t.Fatalf("threshold = %d", cfg.CodexWebsocketCircuitFailureThreshold)
	}
	if cfg.CodexWebsocketCircuitCooldownSeconds != 17 {
		t.Fatalf("cooldown = %d", cfg.CodexWebsocketCircuitCooldownSeconds)
	}
}
