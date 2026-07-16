package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeCodexWebsocketSameModelTurnUsesPreviousResponseDelta(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 2)
	serverErrors := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		for turn := 0; turn < 2; turn++ {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				serverErrors <- fmt.Errorf("read turn %d: %w", turn, errRead)
				return
			}
			captured <- bytes.Clone(payload)
			responseID := fmt.Sprintf("resp-%d", turn+1)
			created := []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":%q,"model":"gpt-5.6-sol"}}`, responseID))
			if errWrite := conn.WriteMessage(websocket.TextMessage, created); errWrite != nil {
				serverErrors <- fmt.Errorf("write created turn %d: %w", turn, errWrite)
				return
			}
			output := `[]`
			if turn == 0 {
				item := `{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer one","annotations":[]}]}`
				itemDone := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + item + `}`)
				if errWrite := conn.WriteMessage(websocket.TextMessage, itemDone); errWrite != nil {
					serverErrors <- fmt.Errorf("write item turn %d: %w", turn, errWrite)
					return
				}
				output = `[` + item + `]`
			}
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-5.6-sol","output":%s,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, responseID, output))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				serverErrors <- fmt.Errorf("write completed turn %d: %w", turn, errWrite)
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	sessionID := "claude-code:incremental-agent"
	payloads := [][]byte{
		[]byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"question one"}],"max_tokens":128,"stream":true}`),
		[]byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"question one"},{"role":"assistant","content":"answer one"},{"role":"user","content":"question two"}],"max_tokens":128,"stream":true}`),
	}
	for turn, payload := range payloads {
		req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}
		opts := cliproxyexecutor.Options{
			SourceFormat:    sdktranslator.FromString("claude"),
			ResponseFormat:  sdktranslator.FromString("claude"),
			OriginalRequest: payload,
			Metadata: map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
			},
		}
		result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
		if errExecute != nil {
			t.Fatalf("turn %d ExecuteStream() error = %v", turn, errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
			}
		}
	}

	first := <-captured
	second := <-captured
	if got := gjson.GetBytes(first, "previous_response_id").String(); got != "" {
		t.Fatalf("first previous_response_id = %q, want empty; payload=%s", got, first)
	}
	if got := gjson.GetBytes(second, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("second previous_response_id = %q, want resp-1; payload=%s", got, second)
	}
	if got := gjson.GetBytes(second, "input.#").Int(); got != 1 {
		t.Fatalf("second delta input count = %d, want 1; payload=%s", got, second)
	}
	if !strings.Contains(gjson.GetBytes(second, "input.0").Raw, "question two") {
		t.Fatalf("second delta does not contain the new user input: %s", second)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
	exec.CloseExecutionSession(sessionID)
}

func TestClaudeCodexWebsocketHTTPFallbackIsStickyAcrossTurns(t *testing.T) {
	var websocketAttempts atomic.Int32
	var httpAttempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			websocketAttempts.Add(1)
			http.Error(w, "websocket unsupported", http.StatusUpgradeRequired)
			return
		}
		httpAttempts.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp-http","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}` + "\n\n"))
	}))
	defer server.Close()

	exec := NewCodexAutoExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{
		"api_key":    "sk-test",
		"base_url":   server.URL,
		"websockets": "true",
	}}
	ctx := cliproxyexecutor.WithPreferUpstreamWebsocket(context.Background())
	for turn := 0; turn < 2; turn++ {
		payload := []byte(fmt.Sprintf(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"turn %d"}],"max_tokens":128,"stream":true}`, turn))
		req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}
		opts := cliproxyexecutor.Options{
			SourceFormat:    sdktranslator.FromString("claude"),
			ResponseFormat:  sdktranslator.FromString("claude"),
			OriginalRequest: payload,
			Metadata: map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:sticky-fallback-agent",
			},
		}
		result, errExecute := exec.ExecuteStream(ctx, auth, req, opts)
		if errExecute != nil {
			t.Fatalf("turn %d ExecuteStream() error = %v", turn, errExecute)
		}
		select {
		case <-time.After(5 * time.Second):
			t.Fatalf("turn %d timed out", turn)
		case chunk, ok := <-result.Chunks:
			for ok {
				if chunk.Err != nil {
					t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
				}
				chunk, ok = <-result.Chunks
			}
		}
	}
	if got := websocketAttempts.Load(); got != 1 {
		t.Fatalf("websocket attempts = %d, want 1 sticky fallback attempt", got)
	}
	if got := httpAttempts.Load(); got != 2 {
		t.Fatalf("HTTP attempts = %d, want 2", got)
	}
	exec.CloseExecutionSession("claude-code:sticky-fallback-agent")
}

func TestClaudeCodexWebsocketReconnectStartsWithFullRequest(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 2)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connectionIndex := connections.Add(1)
		defer func() { _ = conn.Close() }()
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("connection %d read: %v", connectionIndex, errRead)
			return
		}
		captured <- bytes.Clone(payload)
		responseID := fmt.Sprintf("resp-%d", connectionIndex)
		output := `[]`
		if connectionIndex == 1 {
			output = `[{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer one","annotations":[]}]}]`
		}
		completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-5.6-sol","output":%s,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, responseID, output))
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("connection %d write: %v", connectionIndex, errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	sessionID := "claude-code:reconnect-agent"
	disconnected := exec.UpstreamDisconnectChan(sessionID)
	payloads := [][]byte{
		[]byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"question one"}],"max_tokens":128,"stream":true}`),
		[]byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"question one"},{"role":"assistant","content":"answer one"},{"role":"user","content":"question two"}],"max_tokens":128,"stream":true}`),
	}
	for turn, payload := range payloads {
		if turn == 1 {
			select {
			case <-disconnected:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for first websocket disconnect")
			}
		}
		req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}
		opts := cliproxyexecutor.Options{
			SourceFormat:    sdktranslator.FromString("claude"),
			ResponseFormat:  sdktranslator.FromString("claude"),
			OriginalRequest: payload,
			Metadata: map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
			},
		}
		result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
		if errExecute != nil {
			t.Fatalf("turn %d ExecuteStream() error = %v", turn, errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
			}
		}
	}

	first := <-captured
	second := <-captured
	if got := gjson.GetBytes(first, "previous_response_id").String(); got != "" {
		t.Fatalf("first previous_response_id = %q, want empty", got)
	}
	if got := gjson.GetBytes(second, "previous_response_id").String(); got != "" {
		t.Fatalf("reconnected request previous_response_id = %q, want empty; payload=%s", got, second)
	}
	if got := gjson.GetBytes(second, "input.#").Int(); got != 3 {
		t.Fatalf("reconnected full input count = %d, want 3; payload=%s", got, second)
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want 2", got)
	}
	exec.CloseExecutionSession(sessionID)
}

func TestClaudeCodexWebsocketToolContinuationUsesOnlyFunctionOutputDelta(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		for turn := 0; turn < 2; turn++ {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				t.Errorf("read turn %d: %v", turn, errRead)
				return
			}
			captured <- bytes.Clone(payload)
			responseID := fmt.Sprintf("resp-tool-%d", turn+1)
			output := `[]`
			if turn == 0 {
				item := `{"id":"fc-1","type":"function_call","name":"shell","arguments":"{\"cmd\":\"echo hi\"}","call_id":"call-1","status":"completed"}`
				itemDone := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + item + `}`)
				if errWrite := conn.WriteMessage(websocket.TextMessage, itemDone); errWrite != nil {
					t.Errorf("write tool item: %v", errWrite)
					return
				}
				output = `[` + item + `]`
			}
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-5.6-sol","output":%s,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, responseID, output))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				t.Errorf("write completed turn %d: %v", turn, errWrite)
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	sessionID := "claude-code:tool-agent"
	payloads := [][]byte{
		[]byte(`{"model":"gpt-5.6-sol","tools":[{"name":"shell","description":"Run a shell command","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}],"messages":[{"role":"user","content":"run echo hi"}],"max_tokens":128,"stream":true}`),
		[]byte(`{"model":"gpt-5.6-sol","tools":[{"name":"shell","description":"Run a shell command","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}},"required":["cmd"]}}],"messages":[{"role":"user","content":"run echo hi"},{"role":"assistant","content":[{"type":"tool_use","id":"call-1","name":"shell","input":{"cmd":"echo hi"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":"hi"}]}],"max_tokens":128,"stream":true}`),
	}
	for turn, payload := range payloads {
		req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}
		opts := cliproxyexecutor.Options{
			SourceFormat:    sdktranslator.FromString("claude"),
			ResponseFormat:  sdktranslator.FromString("claude"),
			OriginalRequest: payload,
			Metadata: map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
			},
		}
		result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
		if errExecute != nil {
			t.Fatalf("turn %d ExecuteStream() error = %v", turn, errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
			}
		}
	}

	<-captured
	second := <-captured
	if got := gjson.GetBytes(second, "previous_response_id").String(); got != "resp-tool-1" {
		t.Fatalf("tool continuation previous_response_id = %q, want resp-tool-1; payload=%s", got, second)
	}
	if got := gjson.GetBytes(second, "input.#").Int(); got != 1 {
		t.Fatalf("tool continuation delta input count = %d, want 1; payload=%s", got, second)
	}
	if got := gjson.GetBytes(second, "input.0.type").String(); got != "function_call_output" {
		t.Fatalf("tool continuation delta type = %q, want function_call_output; payload=%s", got, second)
	}
	if got := gjson.GetBytes(second, "input.0.call_id").String(); got != "call-1" {
		t.Fatalf("tool continuation call_id = %q, want call-1; payload=%s", got, second)
	}
	exec.CloseExecutionSession(sessionID)
}
