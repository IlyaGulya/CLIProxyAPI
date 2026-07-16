package helps

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

func TestClaudeCodeWebsocketSessionIDSeparatesAgents(t *testing.T) {
	rootHeaders := make(http.Header)
	rootHeaders.Set(ClaudeCodeSessionHeader, "root-session")

	childAHeaders := rootHeaders.Clone()
	childAHeaders.Set(ClaudeCodeAgentHeader, "agent-a")
	childBHeaders := rootHeaders.Clone()
	childBHeaders.Set(ClaudeCodeAgentHeader, "agent-b")

	root := ClaudeCodeWebsocketSessionID(context.Background(), nil, rootHeaders)
	childA := ClaudeCodeWebsocketSessionID(context.Background(), nil, childAHeaders)
	childB := ClaudeCodeWebsocketSessionID(context.Background(), nil, childBHeaders)

	if root == "" || childA == "" || childB == "" {
		t.Fatalf("session IDs must be non-empty: root=%q childA=%q childB=%q", root, childA, childB)
	}
	if root == childA || root == childB || childA == childB {
		t.Fatalf("root and agents must have isolated websocket sessions: root=%q childA=%q childB=%q", root, childA, childB)
	}
	if got := ClaudeCodeWebsocketSessionID(context.Background(), nil, childAHeaders); got != childA {
		t.Fatalf("same agent produced unstable session ID: first=%q second=%q", childA, got)
	}
}

func TestClaudeCodeWebsocketSessionIDFallsBackToPayloadSession(t *testing.T) {
	payload := []byte(`{"metadata":{"user_id":"{\"session_id\":\"payload-session\"}"}}`)
	got := ClaudeCodeWebsocketSessionID(context.Background(), payload, nil)
	if got == "" {
		t.Fatal("payload session did not produce websocket session ID")
	}
}

func TestClaudeCodeWebsocketSessionIDRequiresRootSession(t *testing.T) {
	headers := make(http.Header)
	headers.Set(ClaudeCodeAgentHeader, "orphan-agent")
	if got := ClaudeCodeWebsocketSessionID(context.Background(), nil, headers); got != "" {
		t.Fatalf("agent without root session produced ID %q", got)
	}
}

func TestClaudeCodeCorrelationIDsShareRootAndSeparateAgents(t *testing.T) {
	rootHeaders := make(http.Header)
	rootHeaders.Set(ClaudeCodeSessionHeader, "private-root-session")
	childHeaders := rootHeaders.Clone()
	childHeaders.Set(ClaudeCodeAgentHeader, "private-child-agent")

	rootCorrelation, rootExecution := ClaudeCodeCorrelationIDs(context.Background(), nil, rootHeaders)
	childCorrelation, childExecution := ClaudeCodeCorrelationIDs(context.Background(), nil, childHeaders)
	if rootCorrelation == "" || rootExecution == "" || childExecution == "" {
		t.Fatalf("correlation IDs must be populated: root=%q rootExecution=%q childExecution=%q", rootCorrelation, rootExecution, childExecution)
	}
	if childCorrelation != rootCorrelation {
		t.Fatalf("root correlation differs across root/child: root=%q child=%q", rootCorrelation, childCorrelation)
	}
	if childExecution == rootExecution {
		t.Fatalf("execution correlation did not separate child: %q", childExecution)
	}
	for _, privateValue := range []string{"private-root-session", "private-child-agent"} {
		if strings.Contains(rootCorrelation, privateValue) || strings.Contains(rootExecution, privateValue) || strings.Contains(childExecution, privateValue) {
			t.Fatalf("correlation leaked private identifier %q", privateValue)
		}
	}
}
