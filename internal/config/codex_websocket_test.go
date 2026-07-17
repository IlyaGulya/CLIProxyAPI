package config

import (
	"testing"
	"time"
)

func TestNormalizedCodexWebsocketConfigDefaultsAndOverrides(t *testing.T) {
	defaults := (*Config)(nil).NormalizedCodexWebsocketConfig()
	if defaults.SessionTTL != 10*time.Minute || defaults.MaxSessions != 128 || defaults.PreconnectMaxIdle != 2 || defaults.PreconnectTTL != 30*time.Second {
		t.Fatalf("unexpected defaults: %#v", defaults)
	}
	cfg := &Config{SDKConfig: SDKConfig{CodexWebsocketSessionTTLSeconds: 7, CodexWebsocketMaxSessions: 9, CodexWebsocketPreconnectMaxIdle: 3, CodexWebsocketPreconnectTTLSeconds: 11}}
	got := cfg.NormalizedCodexWebsocketConfig()
	if got.SessionTTL != 7*time.Second || got.MaxSessions != 9 || got.PreconnectMaxIdle != 3 || got.PreconnectTTL != 11*time.Second {
		t.Fatalf("unexpected overrides: %#v", got)
	}
}
