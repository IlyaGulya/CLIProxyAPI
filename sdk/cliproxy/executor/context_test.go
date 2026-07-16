package executor

import (
	"context"
	"testing"
)

func TestPreferUpstreamWebsocketContext(t *testing.T) {
	if PreferUpstreamWebsocket(context.Background()) {
		t.Fatal("plain context unexpectedly prefers upstream websocket")
	}

	ctx := WithPreferUpstreamWebsocket(context.Background())
	if !PreferUpstreamWebsocket(ctx) {
		t.Fatal("marked context does not prefer upstream websocket")
	}
}
