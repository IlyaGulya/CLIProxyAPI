package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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

func TestClaudeCodexWebsocketTwoProcessRestartReplay(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 4)
	var responseNumber atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		for turn := 0; turn < 2; turn++ {
			_, payload, err := conn.ReadMessage()
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			captured <- bytes.Clone(payload)
			n := responseNumber.Add(1)
			id := fmt.Sprintf("resp-%d", n)
			output := `[]`
			if turn == 0 {
				output = `[{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer one","annotations":[]}]}]`
			}
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"model":"gpt-5.6-sol","output":%s,"usage":{"input_tokens":10,"input_tokens_details":{"cached_tokens":8},"output_tokens":1,"total_tokens":11}}}`, id, output))
			if err := conn.WriteMessage(websocket.TextMessage, completed); err != nil {
				t.Errorf("write: %v", err)
				return
			}
		}
	}))
	defer server.Close()

	for process := 0; process < 2; process++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestClaudeCodexWebsocketRestartHelper$", "-test.v")
		cmd.Env = append(os.Environ(), "CLIPROXY_RESTART_HELPER=1", "CLIPROXY_RESTART_URL="+server.URL)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("process %d: %v\n%s", process+1, err, output)
		}
	}
	requests := make([][]byte, 4)
	for i := range requests {
		select {
		case requests[i] = <-captured:
		case <-time.After(5 * time.Second):
			t.Fatalf("request %d missing", i)
		}
	}
	firstKey := gjson.GetBytes(requests[0], "prompt_cache_key").String()
	if firstKey == "" {
		t.Fatalf("first prompt cache key missing: %s", requests[0])
	}
	for i, request := range requests {
		if got := gjson.GetBytes(request, "prompt_cache_key").String(); got != firstKey {
			t.Fatalf("request %d cache key=%q want %q", i, got, firstKey)
		}
	}
	if got := gjson.GetBytes(requests[0], "previous_response_id").String(); got != "" {
		t.Fatalf("process A first previous=%q", got)
	}
	if got := gjson.GetBytes(requests[1], "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("process A delta previous=%q", got)
	}
	if got := gjson.GetBytes(requests[2], "previous_response_id").String(); got != "" {
		t.Fatalf("process B inherited stale previous=%q", got)
	}
	if got := gjson.GetBytes(requests[3], "previous_response_id").String(); got != "resp-3" {
		t.Fatalf("process B delta previous=%q", got)
	}
}

func TestClaudeCodexWebsocketRestartHelper(t *testing.T) {
	if os.Getenv("CLIPROXY_RESTART_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	executor := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-restart", Attributes: map[string]string{"api_key": "sk-test", "base_url": os.Getenv("CLIPROXY_RESTART_URL")}}
	sessionID := "claude-code:restart-e2e"
	payloads := [][]byte{
		[]byte(`{"model":"gpt-5.6-sol","cache_control":{"type":"automatic"},"metadata":{"user_id":"{\"session_id\":\"restart-cache-session\"}"},"messages":[{"role":"user","content":"question one"}],"stream":true}`),
		[]byte(`{"model":"gpt-5.6-sol","cache_control":{"type":"automatic"},"metadata":{"user_id":"{\"session_id\":\"restart-cache-session\"}"},"messages":[{"role":"user","content":"question one"},{"role":"assistant","content":"answer one"},{"role":"user","content":"question two"}],"stream":true}`),
	}
	for _, payload := range payloads {
		result, err := executor.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("claude"), ResponseFormat: sdktranslator.FromString("claude"), OriginalRequest: payload, Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID}})
		if err != nil {
			t.Fatal(err)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
	}
	executor.CloseExecutionSession(sessionID)
}

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
		var translated bytes.Buffer
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
			}
			translated.Write(chunk.Payload)
		}
		startUsage := firstSSEJSONEvent(translated.String(), "message_start").Get("message.usage.input_tokens").Int()
		if startUsage <= 0 {
			t.Fatalf("turn %d message_start input usage = %d; stream=%s", turn, startUsage, translated.String())
		}
		terminalUsage := firstSSEJSONEvent(translated.String(), "message_delta").Get("usage")
		terminalInput := terminalUsage.Get("input_tokens").Int() +
			terminalUsage.Get("cache_read_input_tokens").Int() +
			terminalUsage.Get("cache_creation_input_tokens").Int()
		if terminalInput != startUsage {
			t.Fatalf("turn %d terminal input usage = %d, want logical start usage %d; stream=%s", turn, terminalInput, startUsage, translated.String())
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

func firstSSEJSONEvent(stream, eventType string) gjson.Result {
	for _, line := range strings.Split(stream, "\n") {
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		parsed := gjson.Parse(strings.TrimPrefix(line, "data: "))
		if parsed.Get("type").String() == eventType {
			return parsed
		}
	}
	return gjson.Result{}
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

func TestClaudeCodexWebsocketReactiveCompactReconnectsAfterPendingToolCall(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	type capturedRequest struct {
		connection int32
		payload    []byte
	}
	secondRequest := make(chan capturedRequest, 1)
	serverErrors := make(chan error, 4)
	var connections atomic.Int32
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connectionIndex := connections.Add(1)
		defer func() { _ = conn.Close() }()
		for {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				return
			}
			requestIndex := requests.Add(1)
			if requestIndex == 2 {
				secondRequest <- capturedRequest{connection: connectionIndex, payload: bytes.Clone(payload)}
				if got := gjson.GetBytes(payload, "previous_response_id").String(); got != "" {
					serverErrors <- fmt.Errorf("reactive compact previous_response_id = %q, want empty; payload=%s", got, payload)
					return
				}
			}

			output := `[]`
			if requestIndex == 1 {
				output = `[{"id":"fc-stale","type":"function_call","name":"shell","arguments":"{\"cmd\":\"echo hi\"}","call_id":"call-stale","status":"completed"}]`
			}
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","model":"gpt-5.6-sol","output":%s,"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`, requestIndex, output))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				serverErrors <- fmt.Errorf("write response %d: %w", requestIndex, errWrite)
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	sessionID := "claude-code:reactive-compact-boundary"
	payloads := [][]byte{
		[]byte(`{"model":"gpt-5.6-sol","tools":[{"name":"shell","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"run echo hi"}],"max_tokens":128,"stream":true}`),
		[]byte(`{"model":"gpt-5.6-sol","tools":[{"name":"shell","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"Your task is to create a detailed summary of the conversation so far. Before providing your final summary, wrap your analysis in <analysis> tags. Your entire response must be an <analysis> block followed by a <summary> block."}],"max_tokens":128,"stream":true}`),
	}
	for turn, payload := range payloads {
		result, errExecute := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{
			SourceFormat: sdktranslator.FromString("claude"), ResponseFormat: sdktranslator.FromString("claude"), OriginalRequest: payload,
			Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: sessionID},
		})
		if errExecute != nil {
			t.Fatalf("turn %d ExecuteStream() error = %v", turn, errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
			}
		}
	}

	select {
	case captured := <-secondRequest:
		if captured.connection != 2 {
			t.Fatalf("reactive compact used connection %d, want a fresh second connection; payload=%s", captured.connection, captured.payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for reactive compact request")
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
	exec.CloseExecutionSession(sessionID)
}

func TestClaudeCodexWebsocketCancellationReconnectsBeforeNextTurn(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	firstStarted := make(chan struct{})
	secondPayload := make(chan []byte, 1)
	serverErrors := make(chan error, 2)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connectionIndex := connections.Add(1)
		defer func() { _ = conn.Close() }()
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			serverErrors <- fmt.Errorf("connection %d initial read: %w", connectionIndex, errRead)
			return
		}
		if connectionIndex == 1 {
			close(firstStarted)
			if _, unexpected, errNext := conn.ReadMessage(); errNext == nil {
				serverErrors <- fmt.Errorf("next turn reused canceled connection: %s", unexpected)
			}
			return
		}
		secondPayload <- bytes.Clone(payload)
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-after-cancel","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			serverErrors <- fmt.Errorf("connection %d write: %w", connectionIndex, errWrite)
		}
	}))
	defer func() {
		server.CloseClientConnections()
		server.Close()
	}()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	sessionID := "claude-code:cancel-agent"
	firstPayload := []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"long turn"}],"max_tokens":128,"stream":true}`)
	firstReq := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: firstPayload}
	firstOpts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: firstPayload,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstResult, errExecute := exec.ExecuteStream(firstCtx, auth, firstReq, firstOpts)
	if errExecute != nil {
		t.Fatalf("first ExecuteStream() error = %v", errExecute)
	}
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("first request did not reach upstream")
	}
	cancelFirst()
	for range firstResult.Chunks {
	}

	secondBody := []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"fresh turn"}],"max_tokens":128,"stream":true}`)
	secondReq := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: secondBody}
	secondOpts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: secondBody,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
		},
	}
	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelSecond()
	result, errExecute := exec.ExecuteStream(secondCtx, auth, secondReq, secondOpts)
	if errExecute != nil {
		t.Fatalf("second ExecuteStream() error = %v", errExecute)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("second stream error = %v", chunk.Err)
		}
	}
	select {
	case payload := <-secondPayload:
		if got := gjson.GetBytes(payload, "previous_response_id").String(); got != "" {
			t.Fatalf("post-cancellation previous_response_id = %q, want empty; payload=%s", got, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("next turn did not use a fresh websocket connection")
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want 2", got)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
	exec.CloseExecutionSession(sessionID)
}
