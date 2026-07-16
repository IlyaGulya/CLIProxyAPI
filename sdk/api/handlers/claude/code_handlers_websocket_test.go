package claude

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestClaudeUpstreamWebsocketContextEnabledAndAgentScoped(t *testing.T) {
	gin.SetMode(gin.TestMode)
	req := httptest.NewRequest("POST", "/v1/messages", nil)
	req.Header.Set(helps.ClaudeCodeSessionHeader, "root-session")
	req.Header.Set(helps.ClaudeCodeAgentHeader, "agent-a")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = req

	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{CodexPreferUpstreamWebsockets: true}, nil)
	h := NewClaudeCodeAPIHandler(base)
	ctx, cancel := h.GetContextWithCancel(h, c, context.Background())
	defer cancel()

	ctx, sessionID := h.codexUpstreamWebsocketContext(ctx, []byte(`{"model":"gpt-5.6-sol","stream":true}`))
	if !cliproxyexecutor.PreferUpstreamWebsocket(ctx) {
		t.Fatal("enabled Claude request does not prefer upstream websocket")
	}
	if sessionID == "" {
		t.Fatal("enabled Claude request does not have an execution session")
	}
	if want := helps.ClaudeCodeWebsocketSessionID(ctx, nil, req.Header); sessionID != want {
		t.Fatalf("execution session = %q, want %q", sessionID, want)
	}
}

func TestClaudeUpstreamWebsocketContextDisabledByDefault(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{}, nil)
	h := NewClaudeCodeAPIHandler(base)
	ctx, sessionID := h.codexUpstreamWebsocketContext(context.Background(), []byte(`{"metadata":{"user_id":"{\"session_id\":\"root-session\"}"}}`))
	if cliproxyexecutor.PreferUpstreamWebsocket(ctx) {
		t.Fatal("disabled Claude request prefers upstream websocket")
	}
	if sessionID != "" {
		t.Fatalf("disabled Claude request execution session = %q, want empty", sessionID)
	}
}

func TestClaudeUpstreamWebsocketContextRequiresSessionIdentity(t *testing.T) {
	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{CodexPreferUpstreamWebsockets: true}, nil)
	h := NewClaudeCodeAPIHandler(base)
	ctx, sessionID := h.codexUpstreamWebsocketContext(context.Background(), []byte(`{"model":"gpt-5.6-sol"}`))
	if cliproxyexecutor.PreferUpstreamWebsocket(ctx) {
		t.Fatal("sessionless Claude request prefers persistent upstream websocket")
	}
	if sessionID != "" {
		t.Fatalf("sessionless Claude request execution session = %q, want empty", sessionID)
	}
}
