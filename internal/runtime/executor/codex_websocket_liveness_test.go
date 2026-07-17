package executor

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestCodexWebsocketIdleTimeoutUsesStreamingPolicy(t *testing.T) {
	if got := codexWebsocketIdleTimeout(&config.Config{SDKConfig: config.SDKConfig{
		Streaming: config.StreamingConfig{IdleTimeoutSeconds: 17},
	}}); got != 17*time.Second {
		t.Fatalf("timeout = %s, want 17s", got)
	}
	if got := codexWebsocketIdleTimeout(nil); got != codexResponsesWebsocketIdleTimeout {
		t.Fatalf("default timeout = %s, want %s", got, codexResponsesWebsocketIdleTimeout)
	}
}
