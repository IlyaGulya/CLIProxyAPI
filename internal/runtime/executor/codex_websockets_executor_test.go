package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestBuildCodexWebsocketRequestBodyPreservesPreviousResponseID(t *testing.T) {
	body := []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`)

	wsReqBody := buildCodexWebsocketRequestBody(body)

	if got := gjson.GetBytes(wsReqBody, "type").String(); got != "response.create" {
		t.Fatalf("type = %s, want response.create", got)
	}
	if got := gjson.GetBytes(wsReqBody, "previous_response_id").String(); got != "resp-1" {
		t.Fatalf("previous_response_id = %s, want resp-1", got)
	}
	if gjson.GetBytes(wsReqBody, "input.0.id").String() != "msg-1" {
		t.Fatalf("input item id mismatch")
	}
	if got := gjson.GetBytes(wsReqBody, "type").String(); got == "response.append" {
		t.Fatalf("unexpected websocket request type: %s", got)
	}
}

func TestCodexWebsocketsExecuteResponsesLiteDoesNotInjectImageGenerationTool(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Fatalf("read upstream websocket message: %v", errRead)
		}
		capturedPayload <- bytes.Clone(payload)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Attributes: map[string]string{
			"api_key":   "sk-test",
			"base_url":  server.URL,
			"plan_type": "pro",
		},
	}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5.6-sol",
		Payload: []byte(`{"model":"gpt-5.6-sol","input":[{"type":"additional_tools","role":"developer","tools":[{"type":"custom","name":"exec"}]},{"role":"user","content":"hello"}],"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":"true"}}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	if _, err := exec.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case payload := <-capturedPayload:
		if tools := gjson.GetBytes(payload, "tools"); tools.Exists() {
			t.Fatalf("unexpected tools in responses-lite upstream payload: %s", tools.Raw)
		}
		if got := gjson.GetBytes(payload, "input.0.type").String(); got != "additional_tools" {
			t.Fatalf("input.0.type = %q, want additional_tools; payload=%s", got, payload)
		}
		if got := gjson.GetBytes(payload, "client_metadata.ws_request_header_x_openai_internal_codex_responses_lite").String(); got != "true" {
			t.Fatalf("responses-lite metadata = %q, want true; payload=%s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecutePreservesPreviousResponseIDUpstream(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Fatalf("request path = %s, want /responses", r.URL.Path)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("upgrade websocket: %v", err)
		}
		defer func() { _ = conn.Close() }()

		msgType, payload, err := conn.ReadMessage()
		if err != nil {
			t.Fatalf("read upstream websocket message: %v", err)
		}
		if msgType != websocket.TextMessage {
			t.Fatalf("message type = %d, want text", msgType)
		}
		capturedPayload <- bytes.Clone(payload)

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-2","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Fatalf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","previous_response_id":"resp-1","input":[{"type":"message","id":"msg-1"}]}`),
	}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}

	if _, err := exec.Execute(context.Background(), auth, req, opts); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	select {
	case payload := <-capturedPayload:
		if got := gjson.GetBytes(payload, "type").String(); got != "response.create" {
			t.Fatalf("upstream type = %s, want response.create; payload=%s", got, payload)
		}
		if got := gjson.GetBytes(payload, "previous_response_id").String(); got != "resp-1" {
			t.Fatalf("upstream previous_response_id = %s, want resp-1; payload=%s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecuteStreamPassesThroughUpstreamWebsocketPayloadForDownstreamWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	delta := []byte(`{"type":"response.output_text.delta","delta":"hello"}`)
	completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		capturedPayload <- bytes.Clone(payload)
		if errWrite := conn.WriteMessage(websocket.TextMessage, delta); errWrite != nil {
			t.Errorf("write delta websocket message: %v", errWrite)
			return
		}
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"prolite/gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before first chunk")
		}
		if chunk.Err != nil {
			t.Fatalf("first chunk error = %v", chunk.Err)
		}
		if !bytes.Equal(bytes.TrimSpace(chunk.Payload), delta) {
			t.Fatalf("first chunk = %q, want raw upstream websocket payload %q", chunk.Payload, delta)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first stream chunk")
	}

	select {
	case payload := <-capturedPayload:
		if got := gjson.GetBytes(payload, "model").String(); got != "gpt-5-codex" {
			t.Fatalf("upstream model = %s, want gpt-5-codex; payload=%s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for upstream websocket payload")
	}
}

func TestCodexWebsocketsExecuteStreamTranslatesClaudeRequestBeforeSend(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	capturedPayload := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		capturedPayload <- bytes.Clone(payload)
		completed := []byte(`{"type":"response.completed","response":{"id":"resp-claude","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			t.Errorf("write completed websocket message: %v", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll, RequestLog: true}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model: "gpt-5.6-sol",
		Payload: []byte(`{
			"model":"gpt-5.6-sol",
			"system":"You are a coding agent.",
			"messages":[{"role":"user","content":"hello"}],
			"max_tokens":1024,
			"stream":true
		}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: req.Payload,
		Headers:         http.Header{helps.ClaudeCodeSessionHeader: []string{"timeline-root"}, helps.ClaudeCodeAgentHeader: []string{"timeline-child"}},
		Metadata:        map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:timeline-child"},
	}

	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ginCtx.Request.Header = opts.Headers.Clone()
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	result, errExecute := exec.ExecuteStream(ctx, auth, req, opts)
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream chunk error = %v", chunk.Err)
		}
	}

	select {
	case payload := <-capturedPayload:
		if gjson.GetBytes(payload, "messages").Exists() {
			t.Fatalf("upstream payload still contains Claude messages: %s", payload)
		}
		if !gjson.GetBytes(payload, "input").IsArray() {
			t.Fatalf("upstream payload does not contain Responses input: %s", payload)
		}
		if got := gjson.GetBytes(payload, "type").String(); got != "response.create" {
			t.Fatalf("upstream type = %q, want response.create; payload=%s", got, payload)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for translated Claude payload")
	}
	timelineValue, exists := ginCtx.Get("API_WEBSOCKET_TIMELINE")
	if !exists {
		t.Fatal("websocket timeline was not captured")
	}
	timeline := string(timelineValue.([]byte))
	for _, want := range []string{
		`"name":"session_lock_acquired"`,
		`"busy":false`,
		`"name":"request_prepared"`,
		`"incremental_reset_reason":"no_previous_response"`,
		`"name":"connection_ready"`,
		`"connection_source":"cold"`,
		`"name":"usage"`,
		`"input_tokens":1`,
		`"claude_root_correlation_id":"claude-root:`,
		`"claude_execution_correlation_id":"claude-exec:`,
	} {
		if !strings.Contains(timeline, want) {
			t.Fatalf("timeline missing %q: %s", want, timeline)
		}
	}
	exec.CloseExecutionSession("claude-code:timeline-child")
}

func TestCodexWebsocketsExecuteStreamReusesAgentSessionConnection(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
			requests.Add(1)
			completed := []byte(`{"type":"response.completed","response":{"id":"resp-1","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"hello"}],"max_tokens":128,"stream":true}`)}
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: req.Payload,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:agent-a",
		},
	}

	for turn := 0; turn < 2; turn++ {
		result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
		if errExecute != nil {
			t.Fatalf("ExecuteStream() turn %d error = %v", turn, errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", turn, chunk.Err)
			}
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("upstream websocket connections = %d, want 1", got)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("upstream websocket requests = %d, want 2", got)
	}
	exec.CloseExecutionSession("claude-code:agent-a")
}

func TestCodexWebsocketsExecuteStreamSupportsInSessionModelSwitching(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	models := []string{"gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-luna"}
	serverErrors := make(chan error, 1)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		for index, wantModel := range models {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				serverErrors <- fmt.Errorf("read turn %d: %w", index, errRead)
				return
			}
			if gotModel := gjson.GetBytes(payload, "model").String(); gotModel != wantModel {
				serverErrors <- fmt.Errorf("turn %d model = %q, want %q; payload=%s", index, gotModel, wantModel, payload)
				return
			}
			if gotType := gjson.GetBytes(payload, "type").String(); gotType != "response.create" {
				serverErrors <- fmt.Errorf("turn %d type = %q, want response.create; payload=%s", index, gotType, payload)
				return
			}
			if previousResponseID := gjson.GetBytes(payload, "previous_response_id").String(); previousResponseID != "" {
				serverErrors <- fmt.Errorf("turn %d retained previous_response_id %q across model boundary; payload=%s", index, previousResponseID, payload)
				return
			}
			if inputCount := gjson.GetBytes(payload, "input.#").Int(); inputCount == 0 {
				serverErrors <- fmt.Errorf("turn %d did not send a full input after model selection; payload=%s", index, payload)
				return
			}
			created := []byte(fmt.Sprintf(`{"type":"response.created","response":{"id":"resp-%d","model":%q}}`, index, wantModel))
			if errWrite := conn.WriteMessage(websocket.TextMessage, created); errWrite != nil {
				serverErrors <- fmt.Errorf("write created turn %d: %w", index, errWrite)
				return
			}
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","model":%q,"output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`, index, wantModel))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				serverErrors <- fmt.Errorf("write completed turn %d: %w", index, errWrite)
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	for index, model := range models {
		payload := []byte(fmt.Sprintf(`{"model":%q,"messages":[{"role":"user","content":"turn %d"}],"max_tokens":128,"stream":true}`, model, index))
		req := cliproxyexecutor.Request{Model: model, Payload: payload}
		opts := cliproxyexecutor.Options{
			SourceFormat:    sdktranslator.FromString("claude"),
			ResponseFormat:  sdktranslator.FromString("claude"),
			OriginalRequest: payload,
			Metadata: map[string]any{
				cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:model-switch-agent",
			},
		}
		result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
		if errExecute != nil {
			t.Fatalf("turn %d ExecuteStream() error = %v", index, errExecute)
		}
		var downstream strings.Builder
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatalf("turn %d stream error = %v", index, chunk.Err)
			}
			downstream.Write(chunk.Payload)
		}
		if !strings.Contains(downstream.String(), `"model":"`+model+`"`) {
			t.Fatalf("turn %d downstream response does not identify current model %q: %s", index, model, downstream.String())
		}
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("model switching opened %d websocket connections, want 1", got)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
	exec.CloseExecutionSession("claude-code:model-switch-agent")
}

func TestCodexWebsocketsExecuteStreamRetriesReadDisconnectBeforeDownstreamOutput(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 4)
	models := make(chan string, 3)
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connection := connections.Add(1)
		defer func() { _ = conn.Close() }()

		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			serverErrors <- fmt.Errorf("read connection %d: %w", connection, errRead)
			return
		}
		models <- gjson.GetBytes(payload, "model").String()
		if connection == 1 {
			// Abrupt transport loss produces close 1006 / unexpected EOF at the client.
			_ = conn.UnderlyingConn().Close()
			return
		}

		completed := []byte(`{"type":"response.completed","response":{"id":"resp-retry","model":"gpt-5.6-luna","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			serverErrors <- fmt.Errorf("write completed: %w", errWrite)
			return
		}
		_, switchedPayload, errRead := conn.ReadMessage()
		if errRead != nil {
			serverErrors <- fmt.Errorf("read switched-model request: %w", errRead)
			return
		}
		models <- gjson.GetBytes(switchedPayload, "model").String()
		switchedCompleted := []byte(`{"type":"response.completed","response":{"id":"resp-switched","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, switchedCompleted); errWrite != nil {
			serverErrors <- fmt.Errorf("write switched-model completed: %w", errWrite)
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-retry", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	payload := []byte(`{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"retry me"}],"max_tokens":128,"stream":true}`)
	req := cliproxyexecutor.Request{Model: "gpt-5.6-luna", Payload: payload}
	opts := cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: payload,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:read-retry",
		},
	}

	result, errExecute := exec.ExecuteStream(context.Background(), auth, req, opts)
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	var downstream strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		downstream.Write(chunk.Payload)
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want one initial connection plus one retry", got)
	}
	for index := 0; index < 2; index++ {
		select {
		case model := <-models:
			if model != "gpt-5.6-luna" {
				t.Fatalf("attempt %d model = %q, want gpt-5.6-luna", index+1, model)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for model on attempt %d", index+1)
		}
	}
	if !strings.Contains(downstream.String(), `"type":"message_stop"`) {
		t.Fatalf("downstream response did not complete after retry: %s", downstream.String())
	}

	switchedPayload := []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"switch after retry"}],"max_tokens":128,"stream":true}`)
	switchedResult, errSwitched := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: switchedPayload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: switchedPayload,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:read-retry",
		},
	})
	if errSwitched != nil {
		t.Fatalf("switched-model ExecuteStream() error = %v", errSwitched)
	}
	for chunk := range switchedResult.Chunks {
		if chunk.Err != nil {
			t.Fatalf("switched-model stream error = %v", chunk.Err)
		}
	}
	select {
	case model := <-models:
		if model != "gpt-5.6-sol" {
			t.Fatalf("model after retry = %q, want gpt-5.6-sol", model)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for switched model after retry")
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections after model switch = %d, want retry connection reuse", got)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
	exec.CloseExecutionSession("claude-code:read-retry")
}

func TestCodexWebsocketsExecuteStreamRecoversChildDisconnectBeforeTransactionalCommit(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connection := connections.Add(1)
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read websocket request: %v", errRead)
			return
		}
		if connection == 2 {
			created := []byte(`{"type":"response.created","response":{"id":"resp-recovered","model":"gpt-5.6-luna"}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, created); errWrite != nil {
				t.Errorf("write recovered created: %v", errWrite)
				return
			}
			completed := []byte(`{"type":"response.completed","response":{"id":"resp-recovered","model":"gpt-5.6-luna","output":[{"id":"fc-recovered","type":"function_call","call_id":"call-recovered","name":"Edit","arguments":"{\"file_path\":\"README.md\"}","status":"completed"}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				t.Errorf("write recovered completion: %v", errWrite)
			}
			return
		}
		created := []byte(`{"type":"response.created","response":{"id":"resp-partial","model":"gpt-5.6-luna"}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, created); errWrite != nil {
			t.Errorf("write created: %v", errWrite)
			return
		}
		toolStarted := []byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"fc-partial","type":"function_call","call_id":"call-partial","name":"Edit","arguments":"","status":"in_progress"}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, toolStarted); errWrite != nil {
			t.Errorf("write tool start: %v", errWrite)
			return
		}
		toolDelta := []byte(`{"type":"response.function_call_arguments.delta","item_id":"fc-partial","output_index":0,"delta":"{\\"file_path\\":"}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, toolDelta); errWrite != nil {
			t.Errorf("write tool delta: %v", errWrite)
			return
		}
		_ = conn.UnderlyingConn().Close()
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll, RequestLog: true}})
	auth := &cliproxyauth.Auth{ID: "auth-partial", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	payload := []byte(`{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"partial"}],"max_tokens":128,"stream":true}`)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ginCtx.Request.Header = http.Header{helps.ClaudeCodeSessionHeader: []string{"partial-root"}, helps.ClaudeCodeAgentHeader: []string{"partial-agent"}}
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gpt-5.6-luna", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: payload,
		Headers:         ginCtx.Request.Header.Clone(),
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:post-output-disconnect",
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	var downstream strings.Builder
	errorChunks := 0
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 {
			downstream.Write(chunk.Payload)
		}
		if chunk.Err != nil {
			errorChunks++
		}
	}
	if errorChunks != 0 {
		t.Fatalf("error chunks = %d, want recovery without a downstream error", errorChunks)
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want one fresh connection for recovery", got)
	}
	if got := strings.Count(downstream.String(), `event: message_start`); got != 1 {
		t.Fatalf("message_start count = %d, want exactly one; stream=%s", got, downstream.String())
	}
	if !strings.Contains(downstream.String(), `"type":"message_stop"`) {
		t.Fatalf("recovered stream did not complete: %s", downstream.String())
	}
	timelineValue, exists := ginCtx.Get("API_WEBSOCKET_TIMELINE")
	if !exists {
		t.Fatal("websocket timeline was not captured")
	}
	timeline := string(timelineValue.([]byte))
	for _, want := range []string{
		`"name":"transactional_stream_discarded"`,
		`"reason":"transport_retry"`,
		`"name":"transactional_stream_committed"`,
		`"name":"request_finished"`,
		`"reason":"completed"`,
		`"tool_calls_incomplete":0`,
		`"downstream_committed":true`,
		`"transport_retries":1`,
		`"connection_request_count":1`,
	} {
		if !strings.Contains(timeline, want) {
			t.Errorf("timeline missing %q: %s", want, timeline)
		}
	}
	exec.CloseExecutionSession("claude-code:post-output-disconnect")
}

func TestCodexWebsocketsExecuteStreamRecoversRootDisconnectBeforeSemanticOutput(t *testing.T) {
	releaseRecoveredConnection := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseRecoveredConnection) }) })
	upstream := newCodexScriptedUpstream(t,
		[]codexWebsocketScriptStep{
			codexSend(`{"type":"response.created","response":{"id":"resp-interrupted","model":"gpt-5.6-sol"}}`),
			codexSend(`{"type":"response.reasoning_summary_text.delta","item_id":"rs-interrupted","output_index":0,"summary_index":0,"delta":"unfinished reasoning"}`),
			codexDisconnect1006(),
		},
		[]codexWebsocketScriptStep{
			codexSend(`{"type":"response.created","response":{"id":"resp-recovered","model":"gpt-5.6-sol"}}`),
			codexSend(`{"type":"response.output_text.delta","item_id":"msg-recovered","output_index":0,"content_index":0,"delta":"recovered answer"}`),
			codexSend(`{"type":"response.completed","response":{"id":"resp-recovered","model":"gpt-5.6-sol","output":[{"id":"msg-recovered","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"recovered answer","annotations":[]}]}],"usage":{"input_tokens":1,"output_tokens":2,"total_tokens":3}}}`),
			codexWait(releaseRecoveredConnection),
		},
	)

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll, RequestLog: true}})
	auth := &cliproxyauth.Auth{ID: "auth-root-recovery", Attributes: map[string]string{"api_key": "sk-test", "base_url": upstream.URL()}}
	payload := []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"recover root"}],"max_tokens":128,"stream":true}`)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ginCtx.Request.Header = http.Header{helps.ClaudeCodeSessionHeader: []string{"root-recovery"}}
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("claude"), ResponseFormat: sdktranslator.FromString("claude"), OriginalRequest: payload,
		Headers: ginCtx.Request.Header.Clone(), Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:root-recovery"},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	var downstream strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("stream error = %v", chunk.Err)
		}
		downstream.Write(chunk.Payload)
	}
	if got := upstream.Connections(); got != 2 {
		t.Fatalf("connections = %d, want one fresh connection for recovery", got)
	}
	if got := strings.Count(downstream.String(), `event: message_start`); got != 1 {
		t.Fatalf("message_start count = %d, want exactly one; stream=%s", got, downstream.String())
	}
	if strings.Contains(downstream.String(), "unfinished reasoning") || !strings.Contains(downstream.String(), "recovered answer") {
		t.Fatalf("stream contains discarded attempt or misses recovery: %s", downstream.String())
	}
	timelineValue, exists := ginCtx.Get("API_WEBSOCKET_TIMELINE")
	if !exists {
		t.Fatal("websocket timeline was not captured")
	}
	timeline := string(timelineValue.([]byte))
	for _, want := range []string{
		`"name":"transactional_stream_discarded"`, `"reason":"transport_retry"`,
		`"name":"transactional_stream_committed"`, `"reason":"semantic_output"`,
		`"transactional_policy":"root_until_semantic_output"`, `"commit_boundary":"semantic_output"`,
		`"transport_retries":1`, `"reason":"completed"`,
	} {
		if !strings.Contains(timeline, want) {
			t.Errorf("timeline missing %q: %s", want, timeline)
		}
	}
	exec.sessions.store.mu.Lock()
	session := exec.sessions.store.sessions["claude-code:root-recovery"]
	exec.sessions.store.mu.Unlock()
	if session == nil {
		t.Fatal("recovered session is missing")
	}
	if got := session.lifecycle.state(); got != codexSessionReady {
		t.Fatalf("recovered session state = %s, want ready", got)
	}
	releaseOnce.Do(func() { close(releaseRecoveredConnection) })
	exec.CloseExecutionSession("claude-code:root-recovery")
}

func TestCodexWebsocketsExecuteStreamDoesNotRetryRootAfterSemanticCommit(t *testing.T) {
	tests := []struct {
		name   string
		event  string
		marker string
	}{
		{name: "output text", event: `{"type":"response.output_text.delta","item_id":"msg-partial","output_index":0,"content_index":0,"delta":"partial answer"}`, marker: "partial answer"},
		{name: "tool call", event: `{"type":"response.output_item.added","output_index":0,"item":{"id":"fc-partial","type":"function_call","call_id":"call-partial","name":"Edit","arguments":"","status":"in_progress"}}`, marker: `"type":"tool_use"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			var connections atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, errUpgrade := upgrader.Upgrade(w, r, nil)
				if errUpgrade != nil {
					t.Errorf("upgrade websocket: %v", errUpgrade)
					return
				}
				connections.Add(1)
				defer func() { _ = conn.Close() }()
				if _, _, errRead := conn.ReadMessage(); errRead != nil {
					t.Errorf("read websocket request: %v", errRead)
					return
				}
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp-partial","model":"gpt-5.6-sol"}}`))
				_ = conn.WriteMessage(websocket.TextMessage, []byte(test.event))
				_ = conn.UnderlyingConn().Close()
			}))
			defer server.Close()

			exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll, RequestLog: true}})
			auth := &cliproxyauth.Auth{ID: "auth-root-commit", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
			payload := []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"commit root"}],"max_tokens":128,"stream":true}`)
			ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
			ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			ginCtx.Request.Header = http.Header{helps.ClaudeCodeSessionHeader: []string{"root-commit-" + test.name}}
			ctx := context.WithValue(context.Background(), "gin", ginCtx)
			result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{
				SourceFormat: sdktranslator.FromString("claude"), ResponseFormat: sdktranslator.FromString("claude"), OriginalRequest: payload,
				Headers: ginCtx.Request.Header.Clone(), Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:root-commit-" + test.name},
			})
			if errExecute != nil {
				t.Fatalf("ExecuteStream() error = %v", errExecute)
			}
			var downstream strings.Builder
			var terminalErr error
			for chunk := range result.Chunks {
				downstream.Write(chunk.Payload)
				if chunk.Err != nil {
					terminalErr = chunk.Err
				}
			}
			if terminalErr == nil {
				t.Fatal("missing terminal transport error after semantic commit")
			}
			if got := connections.Load(); got != 1 {
				t.Fatalf("connections = %d, want no retry after semantic commit", got)
			}
			if !strings.Contains(downstream.String(), test.marker) {
				t.Fatalf("committed semantic output missing %q: %s", test.marker, downstream.String())
			}
			timeline := string(ginCtx.MustGet("API_WEBSOCKET_TIMELINE").([]byte))
			for _, want := range []string{`"name":"transactional_stream_committed"`, `"reason":"semantic_output"`, `"name":"transport_retry_suppressed"`, `"boundary":"after_downstream_commit"`} {
				if !strings.Contains(timeline, want) {
					t.Errorf("timeline missing %q: %s", want, timeline)
				}
			}
			exec.CloseExecutionSession("claude-code:root-commit-" + test.name)
		})
	}
}

func TestCodexWebsocketsExecuteStreamCancelsBufferedRootWithoutLeakingPayload(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	reasoningSent := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read websocket request: %v", errRead)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp-cancelled","model":"gpt-5.6-sol"}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.reasoning_summary_text.delta","item_id":"rs-cancelled","output_index":0,"summary_index":0,"delta":"private buffered reasoning"}`))
		close(reasoningSent)
		<-release
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-root-cancel", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	payload := []byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"cancel root"}],"max_tokens":128,"stream":true}`)
	ctx, cancel := context.WithCancel(context.Background())
	result, errExecute := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("claude"), ResponseFormat: sdktranslator.FromString("claude"), OriginalRequest: payload,
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:root-cancel"},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	<-reasoningSent
	time.Sleep(20 * time.Millisecond)
	cancel()
	payloadChunks, errorChunks := 0, 0
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 {
			payloadChunks++
		}
		if chunk.Err != nil {
			errorChunks++
		}
	}
	if payloadChunks != 0 || errorChunks != 0 {
		t.Fatalf("payload/error chunks = %d/%d, want cancellation to close cleanly without leaking buffered payload", payloadChunks, errorChunks)
	}
	releaseOnce.Do(func() { close(release) })
	exec.CloseExecutionSession("claude-code:root-cancel")
}

func TestCodexWebsocketConnectionObservationTracksReuse(t *testing.T) {
	sess := &codexWebsocketSession{}
	conn := &websocket.Conn{}
	firstAge, firstCount := sess.observeConnectionUse(conn)
	if firstAge < 0 || firstCount != 1 {
		t.Fatalf("first observation = (%s, %d), want non-negative age and count 1", firstAge, firstCount)
	}
	time.Sleep(time.Millisecond)
	secondAge, secondCount := sess.observeConnectionUse(conn)
	if secondAge <= firstAge || secondCount != 2 {
		t.Fatalf("second observation = (%s, %d), want increasing age and count 2", secondAge, secondCount)
	}
	_, resetCount := sess.observeConnectionUse(&websocket.Conn{})
	if resetCount != 1 {
		t.Fatalf("new connection count = %d, want 1", resetCount)
	}
}

func TestCodexWebsocketsExecuteStreamDiscardsTransactionalPayloadWhenRecoveryFails(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read websocket request: %v", errRead)
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp-doomed","model":"gpt-5.6-luna"}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"partial"}`))
		_ = conn.UnderlyingConn().Close()
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-transaction-fails", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	payload := []byte(`{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"fail safely"}],"max_tokens":128,"stream":true}`)
	headers := http.Header{helps.ClaudeCodeSessionHeader: []string{"failed-root"}, helps.ClaudeCodeAgentHeader: []string{"failed-agent"}}
	result, errExecute := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.6-luna", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat: sdktranslator.FromString("claude"), ResponseFormat: sdktranslator.FromString("claude"), OriginalRequest: payload, Headers: headers,
		Metadata: map[string]any{cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:transaction-fails"},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	payloadChunks, errorChunks := 0, 0
	var terminalErr error
	for chunk := range result.Chunks {
		if len(chunk.Payload) > 0 {
			payloadChunks++
		}
		if chunk.Err != nil {
			errorChunks++
			terminalErr = chunk.Err
		}
	}
	if payloadChunks != 0 || errorChunks != 1 {
		t.Fatalf("payload/error chunks = %d/%d, want 0/1", payloadChunks, errorChunks)
	}
	statusCarrier, ok := terminalErr.(interface{ StatusCode() int })
	if !ok || statusCarrier.StatusCode() != http.StatusServiceUnavailable || gjson.Get(terminalErr.Error(), "error.code").String() != "partial_response_interrupted" {
		t.Fatalf("terminal error = %T %v, want structured retryable partial-response error", terminalErr, terminalErr)
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want initial plus one bounded recovery attempt", got)
	}
	exec.CloseExecutionSession("claude-code:transaction-fails")
}

func TestCodexWebsocketsExecuteStreamStopsAfterOnePreOutputReadRetry(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read websocket request: %v", errRead)
			return
		}
		_ = conn.UnderlyingConn().Close()
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-exhausted", Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	payload := []byte(`{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"fail twice"}],"max_tokens":128,"stream":true}`)
	result, errExecute := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: "gpt-5.6-luna", Payload: payload}, cliproxyexecutor.Options{
		SourceFormat:    sdktranslator.FromString("claude"),
		ResponseFormat:  sdktranslator.FromString("claude"),
		OriginalRequest: payload,
		Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: "claude-code:retry-exhausted",
		},
	})
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	errorChunks := 0
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			errorChunks++
		}
	}
	if errorChunks != 1 {
		t.Fatalf("error chunks = %d, want exactly one", errorChunks)
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("connections = %d, want exactly one retry", got)
	}
	exec.CloseExecutionSession("claude-code:retry-exhausted")
}

func TestCodexAutoExecutorFallsBackToHTTPWhenWebsocketUnsupported(t *testing.T) {
	var websocketAttempts atomic.Int32
	var httpRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			websocketAttempts.Add(1)
			http.Error(w, "websocket unavailable", http.StatusNotFound)
			return
		}
		httpRequests.Add(1)
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
	req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: []byte(`{"model":"gpt-5.6-sol","input":[{"role":"user","content":"hello"}]}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}
	ctx := cliproxyexecutor.WithPreferUpstreamWebsocket(context.Background())

	result, errExecute := exec.ExecuteStream(ctx, auth, req, opts)
	if errExecute != nil {
		t.Fatalf("ExecuteStream() error = %v", errExecute)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("fallback stream error = %v", chunk.Err)
		}
	}
	if websocketAttempts.Load() != 1 || httpRequests.Load() != 1 {
		t.Fatalf("fallback attempts: websocket=%d http=%d, want 1 each", websocketAttempts.Load(), httpRequests.Load())
	}
}

func TestCodexAutoExecutorCircuitSuppressesFailingRoute(t *testing.T) {
	var websocketAttempts atomic.Int32
	var httpRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			websocketAttempts.Add(1)
			http.Error(w, "websocket unavailable", http.StatusUpgradeRequired)
			return
		}
		httpRequests.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte(`data: {"type":"response.completed","response":{"id":"resp-http","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}` + "\n\n"))
	}))
	defer server.Close()

	exec := NewCodexAutoExecutor(&config.Config{SDKConfig: config.SDKConfig{
		DisableImageGeneration:                config.DisableImageGenerationAll,
		CodexWebsocketCircuitBreaker:          true,
		CodexWebsocketCircuitFailureThreshold: 2,
		CodexWebsocketCircuitCooldownSeconds:  60,
	}})
	auth := &cliproxyauth.Auth{ID: "auth-a", Attributes: map[string]string{
		"api_key": "sk-test", "base_url": server.URL, "websockets": "true",
	}}
	req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: []byte(`{"model":"gpt-5.6-sol","input":[{"role":"user","content":"hello"}]}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex")}
	ctx := cliproxyexecutor.WithPreferUpstreamWebsocket(context.Background())
	for range 3 {
		result, errExecute := exec.ExecuteStream(ctx, auth, req, opts)
		if errExecute != nil {
			t.Fatal(errExecute)
		}
		for chunk := range result.Chunks {
			if chunk.Err != nil {
				t.Fatal(chunk.Err)
			}
		}
	}
	if got := websocketAttempts.Load(); got != 2 {
		t.Fatalf("websocket attempts = %d, want 2 before circuit suppression", got)
	}
	if got := httpRequests.Load(); got != 3 {
		t.Fatalf("http requests = %d, want 3", got)
	}

	authOther := *auth
	authOther.ID = "auth-b"
	result, errExecute := exec.ExecuteStream(ctx, &authOther, req, opts)
	if errExecute != nil {
		t.Fatal(errExecute)
	}
	for range result.Chunks {
	}
	if got := websocketAttempts.Load(); got != 3 {
		t.Fatalf("other route websocket attempts = %d, want 3", got)
	}
}

func TestCodexWebsocketsExecuteStreamPropagatesUpstreamErrorForDownstreamWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	errorPayload := []byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"}}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		if errWrite := conn.WriteMessage(websocket.TextMessage, errorPayload); errWrite != nil {
			t.Errorf("write error websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())

	result, err := exec.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before error chunk")
		}
		if len(bytes.TrimSpace(chunk.Payload)) != 0 {
			t.Fatalf("error chunk payload = %q, want empty", chunk.Payload)
		}
		if chunk.Err == nil {
			t.Fatal("error chunk Err = nil, want upstream error")
		}
		statusErr, ok := chunk.Err.(interface{ StatusCode() int })
		if !ok {
			t.Fatalf("error type %T does not expose StatusCode", chunk.Err)
		}
		if got := statusErr.StatusCode(); got != http.StatusTooManyRequests {
			t.Fatalf("status = %d, want %d", got, http.StatusTooManyRequests)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error stream chunk")
	}
}

func TestCodexWebsocketsExecuteStreamMapsMessageTooBigClose(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var connections atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()

		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("read upstream websocket message: %v", errRead)
			return
		}
		deadline := time.Now().Add(time.Second)
		closeMessage := websocket.FormatCloseMessage(websocket.CloseMessageTooBig, "message too big")
		if errWrite := conn.WriteControl(websocket.CloseMessage, closeMessage, deadline); errWrite != nil {
			t.Errorf("write close websocket message: %v", errWrite)
			return
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "sk-test", "base_url": server.URL}}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"model":"gpt-5-codex","input":[{"type":"message","role":"user","content":"hello"}]}`),
	}
	opts := cliproxyexecutor.Options{
		SourceFormat:   sdktranslator.FromString("openai-response"),
		ResponseFormat: sdktranslator.FromString("openai-response"),
	}

	result, err := exec.ExecuteStream(context.Background(), auth, req, opts)
	if err != nil {
		t.Fatalf("ExecuteStream() error = %v", err)
	}

	select {
	case chunk, ok := <-result.Chunks:
		if !ok {
			t.Fatal("stream closed before error chunk")
		}
		if chunk.Err == nil {
			t.Fatal("error chunk Err = nil, want message-too-big error")
		}
		statusErr, ok := chunk.Err.(interface{ StatusCode() int })
		if !ok {
			t.Fatalf("error type %T does not expose StatusCode", chunk.Err)
		}
		if got := statusErr.StatusCode(); got != http.StatusRequestEntityTooLarge {
			t.Fatalf("status = %d, want %d", got, http.StatusRequestEntityTooLarge)
		}
		if got := gjson.Get(chunk.Err.Error(), "error.code").String(); got != "message_too_big" {
			t.Fatalf("error code = %q, want message_too_big; err=%v", got, chunk.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error stream chunk")
	}
	if got := connections.Load(); got != 1 {
		t.Fatalf("connections = %d, want no retry for message-too-big close", got)
	}
}

func TestShouldRetryCodexWebsocketReadError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "abnormal closure", err: &websocket.CloseError{Code: websocket.CloseAbnormalClosure}, want: true},
		{name: "service restart", err: &websocket.CloseError{Code: websocket.CloseServiceRestart}, want: true},
		{name: "normal closure", err: &websocket.CloseError{Code: websocket.CloseNormalClosure}, want: false},
		{name: "protocol error", err: &websocket.CloseError{Code: websocket.CloseProtocolError}, want: false},
		{name: "policy violation", err: &websocket.CloseError{Code: websocket.ClosePolicyViolation}, want: false},
		{name: "message too big", err: statusErr{code: http.StatusRequestEntityTooLarge}, want: false},
		{name: "canceled", err: context.Canceled, want: false},
		{name: "deadline", err: context.DeadlineExceeded, want: false},
		{name: "EOF", err: io.EOF, want: true},
		{name: "unexpected EOF", err: io.ErrUnexpectedEOF, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldRetryCodexWebsocketReadError(tt.err); got != tt.want {
				t.Fatalf("shouldRetryCodexWebsocketReadError(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestCodexWebsocketsUpstreamDisconnectChanSignalsOnInvalidate(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial websocket: %v", err)
	}
	defer func() { _ = conn.Close() }()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sessionID := "sess-1"
	disconnectCh := exec.UpstreamDisconnectChan(sessionID)
	if disconnectCh == nil {
		t.Fatal("expected disconnect channel")
	}

	sess := exec.getOrCreateSession(sessionID)
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.connMu.Lock()
	sess.conn = conn
	sess.authID = "auth-1"
	sess.wsURL = "ws://example.test/responses"
	sess.readerConn = conn
	sess.connMu.Unlock()

	upstreamErr := errors.New("upstream gone")
	exec.invalidateUpstreamConn(sess, conn, "test_invalidate", upstreamErr)

	select {
	case errRead, ok := <-disconnectCh:
		if !ok {
			t.Fatal("expected disconnect channel to deliver error before closing")
		}
		if errRead == nil || errRead.Error() != upstreamErr.Error() {
			t.Fatalf("disconnect error = %v, want %v", errRead, upstreamErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for disconnect signal")
	}
}

func TestCodexWebsocketsEnsureUpstreamConnReplacesCredentialMismatch(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connected := make(chan struct{}, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("upgrade websocket: %v", errUpgrade)
			return
		}
		connected <- struct{}{}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sess := exec.getOrCreateSession("credential-mismatch")
	if sess == nil {
		t.Fatal("expected session")
	}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	first, _, errFirst := exec.ensureUpstreamConn(context.Background(), nil, sess, "auth-a", wsURL, http.Header{})
	if errFirst != nil {
		t.Fatalf("first ensureUpstreamConn() error = %v", errFirst)
	}
	second, _, errSecond := exec.ensureUpstreamConn(context.Background(), nil, sess, "auth-b", wsURL, http.Header{})
	if errSecond != nil {
		t.Fatalf("second ensureUpstreamConn() error = %v", errSecond)
	}
	if first == second {
		t.Fatal("credential mismatch reused the previous websocket")
	}
	sess.connMu.Lock()
	gotAuthID := sess.authID
	sess.connMu.Unlock()
	if gotAuthID != "auth-b" {
		t.Fatalf("session auth ID = %q, want auth-b", gotAuthID)
	}

	for i := 0; i < 2; i++ {
		select {
		case <-connected:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for replacement websocket connection")
		}
	}
	exec.CloseExecutionSession("credential-mismatch")
}

func TestCodexWebsocketsEnsureUpstreamConnReportsColdAndSessionReuse(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		for {
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				return
			}
		}
	}))
	defer server.Close()

	exec := NewCodexWebsocketsExecutor(&config.Config{})
	sess := exec.getOrCreateSession("connection-source")
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	first, errFirst := exec.ensureUpstreamConnObserved(context.Background(), nil, sess, "auth-source", wsURL, http.Header{})
	if errFirst != nil || first.source != codexWebsocketConnectionCold {
		t.Fatalf("first connection = (%v, %q), want cold success", errFirst, first.source)
	}
	second, errSecond := exec.ensureUpstreamConnObserved(context.Background(), nil, sess, "auth-source", wsURL, http.Header{})
	if errSecond != nil || second.source != codexWebsocketConnectionSessionReuse {
		t.Fatalf("second connection = (%v, %q), want session reuse", errSecond, second.source)
	}
	if first.conn != second.conn {
		t.Fatal("session reuse returned a different websocket")
	}
	exec.CloseExecutionSession("connection-source")
}

func TestCodexWebsocketSessionLockReportsBusyWait(t *testing.T) {
	sess := &codexWebsocketSession{}
	sess.reqMu.Lock()
	result := make(chan struct {
		wait time.Duration
		busy bool
	}, 1)
	go func() {
		wait, busy := sess.lockRequest()
		result <- struct {
			wait time.Duration
			busy bool
		}{wait: wait, busy: busy}
		sess.reqMu.Unlock()
	}()
	time.Sleep(20 * time.Millisecond)
	sess.reqMu.Unlock()

	got := <-result
	if !got.busy {
		t.Fatal("contended request lock was not reported busy")
	}
	if got.wait < 15*time.Millisecond {
		t.Fatalf("reported wait = %s, want at least 15ms", got.wait)
	}
}

func TestApplyCodexWebsocketHeadersDefaultsToCurrentResponsesBeta(t *testing.T) {
	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, nil, "", nil)

	if got := headers.Get("OpenAI-Beta"); got != codexResponsesWebsocketBetaHeaderValue {
		t.Fatalf("OpenAI-Beta = %s, want %s", got, codexResponsesWebsocketBetaHeaderValue)
	}
	if got := headers.Get("User-Agent"); got != codexUserAgent {
		t.Fatalf("User-Agent = %s, want %s", got, codexUserAgent)
	}
	if !strings.HasPrefix(codexUserAgent, codexOriginator+"/") {
		t.Fatalf("default Codex User-Agent = %s, want prefix %s/", codexUserAgent, codexOriginator)
	}
	if !strings.HasPrefix(codexUserAgent, "codex-tui/") {
		t.Fatalf("default Codex User-Agent = %s, want codex-tui prefix", codexUserAgent)
	}
	if !strings.Contains(codexUserAgent, "(codex-tui;") {
		t.Fatalf("default Codex User-Agent = %s, want codex-tui suffix", codexUserAgent)
	}
	if got := headers.Get("Originator"); got != codexOriginator {
		t.Fatalf("Originator = %s, want %s", got, codexOriginator)
	}
	if got := headers.Get("Version"); got != "" {
		t.Fatalf("Version = %q, want empty", got)
	}
	if got := headers.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
	if got := headers.Get("X-Codex-Turn-Metadata"); got != "" {
		t.Fatalf("X-Codex-Turn-Metadata = %q, want empty", got)
	}
	if got := headers.Get("X-Client-Request-Id"); got != "" {
		t.Fatalf("X-Client-Request-Id = %q, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersPassesThroughClientIdentityHeaders(t *testing.T) {
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"Originator":            "Codex Desktop",
		"User-Agent":            "codex_cli_rs/0.1.0",
		"Version":               "0.115.0-alpha.27",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Client-Request-Id":   "019d2233-e240-7162-992d-38df0a2a0e0d",
		"session-id":            "legacy-session",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", nil)

	if got := headers.Get("Originator"); got != "Codex Desktop" {
		t.Fatalf("Originator = %s, want %s", got, "Codex Desktop")
	}
	if got := headers.Get("User-Agent"); got != "codex_cli_rs/0.1.0" {
		t.Fatalf("User-Agent = %s, want %s", got, "codex_cli_rs/0.1.0")
	}
	if got := headers.Get("Version"); got != "0.115.0-alpha.27" {
		t.Fatalf("Version = %s, want %s", got, "0.115.0-alpha.27")
	}
	if got := headers.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"turn-1"}` {
		t.Fatalf("X-Codex-Turn-Metadata = %s, want %s", got, `{"turn_id":"turn-1"}`)
	}
	if got := headers.Get("X-Client-Request-Id"); got != "019d2233-e240-7162-992d-38df0a2a0e0d" {
		t.Fatalf("X-Client-Request-Id = %s, want %s", got, "019d2233-e240-7162-992d-38df0a2a0e0d")
	}
	if got := headers["session_id"]; len(got) != 1 || got[0] != "legacy-session" {
		t.Fatalf("session_id = %#v, want [legacy-session]", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersCanonicalizesLegacyUnderscoreSessionHeader(t *testing.T) {
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"Originator": "Codex Desktop",
		"User-Agent": "codex_cli_rs/0.1.0",
		"Session_id": "legacy-underscore-session",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", nil)

	if got := headers["session_id"]; len(got) != 1 || got[0] != "legacy-underscore-session" {
		t.Fatalf("session_id = %#v, want [legacy-underscore-session]", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersUsesConfigDefaultsForOAuth(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "my-codex-client/1.0",
			BetaFeatures: "feature-a,feature-b",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "", cfg)

	if got := headers.Get("User-Agent"); got != "my-codex-client/1.0" {
		t.Fatalf("User-Agent = %s, want %s", got, "my-codex-client/1.0")
	}
	if got := headers.Get("x-codex-beta-features"); got != "feature-a,feature-b" {
		t.Fatalf("x-codex-beta-features = %s, want %s", got, "feature-a,feature-b")
	}
	if got := headers.Get("OpenAI-Beta"); got != codexResponsesWebsocketBetaHeaderValue {
		t.Fatalf("OpenAI-Beta = %s, want %s", got, codexResponsesWebsocketBetaHeaderValue)
	}
}

func TestApplyCodexWebsocketHeadersPrefersExistingHeadersOverClientAndConfig(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"User-Agent":            "client-ua",
		"X-Codex-Beta-Features": "client-beta",
	})
	headers := http.Header{}
	headers.Set("User-Agent", "existing-ua")
	headers.Set("X-Codex-Beta-Features", "existing-beta")

	got := applyCodexWebsocketHeaders(ctx, headers, auth, "", cfg)

	if gotVal := got.Get("User-Agent"); gotVal != "existing-ua" {
		t.Fatalf("User-Agent = %s, want %s", gotVal, "existing-ua")
	}
	if gotVal := got.Get("x-codex-beta-features"); gotVal != "existing-beta" {
		t.Fatalf("x-codex-beta-features = %s, want %s", gotVal, "existing-beta")
	}
}

func TestApplyCodexWebsocketHeadersConfigUserAgentOverridesClientHeader(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	ctx := contextWithGinHeaders(map[string]string{
		"User-Agent":            "client-ua",
		"X-Codex-Beta-Features": "client-beta",
	})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "", cfg)

	if got := headers.Get("User-Agent"); got != "config-ua" {
		t.Fatalf("User-Agent = %s, want %s", got, "config-ua")
	}
	if got := headers.Get("x-codex-beta-features"); got != "client-beta" {
		t.Fatalf("x-codex-beta-features = %s, want %s", got, "client-beta")
	}
}

func TestApplyCodexWebsocketHeadersIgnoresConfigForAPIKeyAuth(t *testing.T) {
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider:   "codex",
		Attributes: map[string]string{"api_key": "sk-test"},
	}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "sk-test", cfg)

	if got := headers.Get("User-Agent"); got != "" {
		t.Fatalf("User-Agent = %s, want empty", got)
	}
	if got := headers.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
	if got := headers.Get("Originator"); got != "" {
		t.Fatalf("Originator = %s, want empty", got)
	}
}

func TestApplyCodexWebsocketHeadersPreservesExplicitAPIKeyUserAgent(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Attributes: map[string]string{"api_key": "sk-test"}}
	ctx := contextWithGinHeaders(map[string]string{"User-Agent": "api-key-client/1.0", "Originator": "explicit-origin"})

	headers := applyCodexWebsocketHeaders(ctx, http.Header{}, auth, "sk-test", nil)

	if got := headers.Get("User-Agent"); got != "api-key-client/1.0" {
		t.Fatalf("User-Agent = %s, want api-key-client/1.0", got)
	}
	if got := headers.Get("Originator"); got != "explicit-origin" {
		t.Fatalf("Originator = %s, want explicit-origin", got)
	}
}

func TestApplyCodexWebsocketHeadersUsesCanonicalAccountHeader(t *testing.T) {
	auth := &cliproxyauth.Auth{Provider: "codex", Metadata: map[string]any{"account_id": "acct-1"}}

	headers := applyCodexWebsocketHeaders(context.Background(), http.Header{}, auth, "", nil)

	if got := headerValueCaseInsensitive(headers, "ChatGPT-Account-ID"); got != "acct-1" {
		t.Fatalf("ChatGPT-Account-ID = %s, want acct-1", got)
	}
	values, ok := headers["ChatGPT-Account-ID"]
	if !ok {
		t.Fatalf("expected exact ChatGPT-Account-ID key, got %#v", headers)
	}
	if len(values) != 1 || values[0] != "acct-1" {
		t.Fatalf("ChatGPT-Account-ID values = %#v, want [acct-1]", values)
	}
}

func TestApplyCodexPromptCacheHeadersSetsSessionIDAndLegacyConversation(t *testing.T) {
	req := cliproxyexecutor.Request{Model: "gpt-5-codex", Payload: []byte(`{"prompt_cache_key":"cache-1"}`)}

	_, headers := applyCodexPromptCacheHeaders("openai-response", req, []byte(`{"model":"gpt-5-codex"}`))

	if got := headers["session_id"]; len(got) != 1 || got[0] != "cache-1" {
		t.Fatalf("session_id = %#v, want [cache-1]", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("Session-Id = %s, want empty", got)
	}
	if got := headers.Get("Conversation_id"); got != "cache-1" {
		t.Fatalf("Conversation_id = %s, want cache-1", got)
	}
}

func TestApplyCodexPromptCacheHeadersClaudeUsesClaudeCodeSessionID(t *testing.T) {
	firstReq := cliproxyexecutor.Request{
		Model: "gpt-5-codex-claude-ws-cache-session",
		Payload: []byte(`{
			"cache_control":{"type":"automatic"},
			"metadata":{"user_id":"{\"device_id\":\"device-a\",\"account_uuid\":\"\",\"session_id\":\"ws-cache-session-1\"}"},
			"messages":[{"role":"user","content":[{"type":"text","text":"first"}]}]
		}`),
	}
	secondReq := cliproxyexecutor.Request{
		Model: "gpt-5-codex-claude-ws-cache-session",
		Payload: []byte(`{
			"cache_control":{"type":"automatic"},
			"metadata":{"user_id":"{\"device_id\":\"device-b\",\"account_uuid\":\"\",\"session_id\":\"ws-cache-session-1\"}"},
			"messages":[{"role":"user","content":[{"type":"text","text":"next"}]}]
		}`),
	}

	firstBody, firstHeaders := applyCodexPromptCacheHeaders("claude", firstReq, []byte(`{"model":"gpt-5-codex"}`))
	secondBody, secondHeaders := applyCodexPromptCacheHeaders("claude", secondReq, []byte(`{"model":"gpt-5-codex"}`))

	firstKey := gjson.GetBytes(firstBody, "prompt_cache_key").String()
	secondKey := gjson.GetBytes(secondBody, "prompt_cache_key").String()
	if firstKey == "" {
		t.Fatalf("first prompt_cache_key is empty; body=%s", string(firstBody))
	}
	if secondKey != firstKey {
		t.Fatalf("same Claude Code session_id produced different websocket prompt_cache_key: first=%q second=%q", firstKey, secondKey)
	}
	if got := firstHeaders["session_id"]; len(got) != 1 || got[0] != firstKey {
		t.Fatalf("first session_id = %#v, want [%q]", got, firstKey)
	}
	if got := secondHeaders["session_id"]; len(got) != 1 || got[0] != firstKey {
		t.Fatalf("second session_id = %#v, want [%q]", got, firstKey)
	}
}

func TestApplyCodexPromptCacheHeadersClaudeRejectsBareUserID(t *testing.T) {
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex-claude-ws-cache-bare-user",
		Payload: []byte(`{"metadata":{"user_id":"same-user-across-chats"},"messages":[{"role":"user","content":[{"type":"text","text":"first"}]}]}`),
	}

	body, headers := applyCodexPromptCacheHeaders("claude", req, []byte(`{"model":"gpt-5-codex"}`))

	if got := gjson.GetBytes(body, "prompt_cache_key").String(); got != "" {
		t.Fatalf("bare metadata.user_id must not create websocket prompt_cache_key, got %q; body=%s", got, string(body))
	}
	if got := headers["session_id"]; len(got) != 0 {
		t.Fatalf("bare metadata.user_id must not create websocket session_id, got %#v", got)
	}
	if got := headers.Get("Session-Id"); got != "" {
		t.Fatalf("bare metadata.user_id must not create websocket Session-Id, got %q", got)
	}
	if got := headers.Get("Conversation_id"); got != "" {
		t.Fatalf("bare metadata.user_id must not create websocket Conversation_id, got %q", got)
	}
}

func TestCodexPromptCacheMetricFieldsTrackStablePrefixWithoutRawKey(t *testing.T) {
	first := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"private-key","instructions":"stable","tools":[{"type":"function","name":"Agent"}],"reasoning":{"effort":"high"},"input":[{"role":"user","content":"one"}]}`)
	second := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"private-key","instructions":"stable","tools":[{"type":"function","name":"Agent"}],"reasoning":{"effort":"high"},"input":[{"role":"user","content":"two"}]}`)
	changed := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"private-key","instructions":"stable","tools":[{"type":"function","name":"Different"}],"reasoning":{"effort":"high"},"input":[{"role":"user","content":"two"}]}`)
	developerPrefix := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"private-key","instructions":"","tools":[],"input":[{"role":"developer","content":[{"type":"input_text","text":"prefix one"}]},{"role":"user","content":"one"}]}`)
	changedDeveloperPrefix := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"private-key","instructions":"","tools":[],"input":[{"role":"developer","content":[{"type":"input_text","text":"prefix two"}]},{"role":"user","content":"one"}]}`)

	firstFields := codexPromptCacheMetricFields(first)
	secondFields := codexPromptCacheMetricFields(second)
	changedFields := codexPromptCacheMetricFields(changed)
	developerFields := codexPromptCacheMetricFields(developerPrefix)
	changedDeveloperFields := codexPromptCacheMetricFields(changedDeveloperPrefix)
	if firstFields.scope == "" || firstFields.prefixFingerprint == "" {
		t.Fatalf("missing cache metric fields: %#v", firstFields)
	}
	if firstFields.scope != secondFields.scope || firstFields.prefixFingerprint != secondFields.prefixFingerprint {
		t.Fatalf("input-only change destabilized cache fields: first=%#v second=%#v", firstFields, secondFields)
	}
	if firstFields.prefixFingerprint == changedFields.prefixFingerprint {
		t.Fatalf("tool change did not alter prefix fingerprint: %#v", changedFields)
	}
	if developerFields.prefixFingerprint == changedDeveloperFields.prefixFingerprint || developerFields.instructionsBytes == 0 {
		t.Fatalf("developer prefix was not fingerprinted: first=%#v changed=%#v", developerFields, changedDeveloperFields)
	}
	for _, fields := range []codexPromptCacheObservation{firstFields, secondFields, changedFields} {
		if strings.Contains(fmt.Sprint(fields), "private-key") || strings.Contains(fmt.Sprint(fields), "stable") {
			t.Fatalf("cache metric fields leaked raw content: %#v", fields)
		}
	}
}

func TestCodexPromptCacheMetricFieldsCanonicalizeEquivalentJSON(t *testing.T) {
	first := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"key","tools":[{"name":"B","type":"function","parameters":{"type":"object","properties":{"z":{"type":"string"},"a":{"type":"number"}}}},{"type":"function","name":"A","parameters":{"required":["x"],"type":"object"}}],"reasoning":{"summary":"auto","effort":"high"},"input":[]}`)
	second := []byte(`{"reasoning":{"effort":"high","summary":"auto"},"tools":[{"parameters":{"properties":{"a":{"type":"number"},"z":{"type":"string"}},"type":"object"},"type":"function","name":"B"},{"parameters":{"type":"object","required":["x"]},"name":"A","type":"function"}],"prompt_cache_key":"key","model":"gpt-5.6-luna","input":[]}`)
	firstFields := codexPromptCacheMetricFields(first)
	secondFields := codexPromptCacheMetricFields(second)
	if firstFields.prefixFingerprint != secondFields.prefixFingerprint {
		t.Fatalf("equivalent JSON produced different fingerprints: first=%#v second=%#v", firstFields, secondFields)
	}
}

func TestCodexPromptCacheMetricFieldsPreserveToolOrder(t *testing.T) {
	first := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"key","tools":[{"name":"A","type":"function"},{"name":"B","type":"function"}],"input":[]}`)
	second := []byte(`{"model":"gpt-5.6-luna","prompt_cache_key":"key","tools":[{"name":"B","type":"function"},{"name":"A","type":"function"}],"input":[]}`)
	if got, want := codexPromptCacheMetricFields(first).prefixFingerprint, codexPromptCacheMetricFields(second).prefixFingerprint; got == want {
		t.Fatalf("tool order was erased from fingerprint: %q", got)
	}
}

func TestCanonicalizeCodexCacheableRequestMakesEquivalentToolPrefixesByteStable(t *testing.T) {
	first := []byte(`{"model":"gpt-5.6-luna","tools":[{"name":"B","type":"function","parameters":{"properties":{"z":{"type":"string"},"a":{"type":"number"}},"type":"object"}},{"name":"A","type":"function"}],"input":[{"role":"user","content":"one"}]}`)
	second := []byte(`{"input":[{"content":"one","role":"user"}],"tools":[{"parameters":{"type":"object","properties":{"a":{"type":"number"},"z":{"type":"string"}}},"type":"function","name":"B"},{"type":"function","name":"A"}],"model":"gpt-5.6-luna"}`)
	if got, want := string(canonicalizeCodexCacheableRequest(first)), string(canonicalizeCodexCacheableRequest(second)); got != want {
		t.Fatalf("canonical requests differ:\nfirst:  %s\nsecond: %s", got, want)
	}
}

func TestCanonicalizeCodexCacheableRequestPreservesToolOrderAndLargeIntegers(t *testing.T) {
	body := []byte(`{"tools":[{"name":"B","type":"function","parameters":{"maximum":9007199254740993}},{"name":"A","type":"function"}],"input":[]}`)
	canonical := canonicalizeCodexCacheableRequest(body)
	if got := gjson.GetBytes(canonical, "tools.0.name").String(); got != "B" {
		t.Fatalf("first tool = %q, want B", got)
	}
	if got := gjson.GetBytes(canonical, "tools.0.parameters.maximum").Raw; got != "9007199254740993" {
		t.Fatalf("large integer = %q, want exact value", got)
	}
}

func TestPrepareWebsocketRequestSharesCanonicalPolicyAcrossModes(t *testing.T) {
	planner := newCodexWebsocketRequestPlanner(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
	auth := &cliproxyauth.Auth{ID: "auth-prepare", Provider: "codex", Attributes: map[string]string{"api_key": "token", "base_url": "https://example.test/codex"}}
	payload := []byte(`{"model":"gpt-5.6-sol","stream_options":{"include_usage":true},"input":[{"role":"user","content":"hello"}],"tools":[{"type":"function","name":"B","parameters":{"maximum":9007199254740993}},{"type":"function","name":"A"}]}`)
	req := cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), ResponseFormat: sdktranslator.FromString("openai-response"), OriginalRequest: payload}

	nonStream, errNonStream := planner.Plan(context.Background(), auth, req, opts, false)
	stream, errStream := planner.Plan(context.Background(), auth, req, opts, true)
	if errNonStream != nil || errStream != nil {
		t.Fatalf("prepare errors: non-stream=%v stream=%v", errNonStream, errStream)
	}
	if nonStream.baseModel != stream.baseModel || nonStream.wsURL != stream.wsURL || nonStream.headers.Get("Authorization") != stream.headers.Get("Authorization") {
		t.Fatalf("shared preparation diverged: non-stream=%#v stream=%#v", nonStream, stream)
	}
	if got := gjson.GetBytes(nonStream.body, "tools.0.parameters.maximum").Raw; got != "9007199254740993" {
		t.Fatalf("non-stream large integer = %q", got)
	}
	if got := gjson.GetBytes(stream.body, "tools.0.name").String(); got != "B" {
		t.Fatalf("stream tool order changed, first=%q", got)
	}
	if !gjson.GetBytes(nonStream.body, "stream_options").Exists() || gjson.GetBytes(stream.body, "stream_options").Exists() {
		t.Fatalf("mode-specific stream_options policy failed: non-stream=%s stream=%s", nonStream.body, stream.body)
	}
}

func TestCodexAutoExecutorUsesWebsocketForUpstreamPreference(t *testing.T) {
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"websockets": "true"}}
	if codexShouldUseWebsockets(context.Background(), auth) {
		t.Fatal("plain context unexpectedly selected websocket")
	}
	if !codexShouldUseWebsockets(cliproxyexecutor.WithPreferUpstreamWebsocket(context.Background()), auth) {
		t.Fatal("upstream websocket preference did not select websocket")
	}
	if !codexShouldUseWebsockets(cliproxyexecutor.WithDownstreamWebsocket(context.Background()), auth) {
		t.Fatal("downstream websocket no longer selects websocket")
	}
}

func TestApplyCodexWebsocketHeadersIdentityConfuseRemapsPromptCacheKey(t *testing.T) {
	cfg := &config.Config{
		Routing: config.RoutingConfig{SessionAffinity: true},
		Codex:   config.CodexConfig{IdentityConfuse: true},
	}
	auth := &cliproxyauth.Auth{ID: "auth-ws-1", Provider: "codex"}
	req := cliproxyexecutor.Request{
		Model:   "gpt-5-codex",
		Payload: []byte(`{"prompt_cache_key":"cache-ws-1","client_metadata":{"x-codex-installation-id":"install-ws-1"}}`),
	}

	body, headers := applyCodexPromptCacheHeaders("openai-response", req, []byte(`{"model":"gpt-5-codex"}`))
	body, identityState := applyCodexIdentityConfuseBody(cfg, auth, req.Payload, body)
	ctx := contextWithGinHeaders(map[string]string{
		"X-Codex-Turn-Metadata": `{"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1","window_id":"cache-ws-1:0"}`,
		"X-Client-Request-Id":   "client-request-1",
	})
	headers = applyCodexWebsocketHeaders(ctx, headers, auth, "oauth-token", cfg)
	applyCodexIdentityConfuseHeaders(headers, &identityState)

	expectedPromptCacheKey := codexIdentityConfuseUUID("auth-ws-1", "prompt-cache", "cache-ws-1")
	expectedTurnID := codexIdentityConfuseUUID("auth-ws-1", "turn", "turn-ws-1")
	if gotKey := gjson.GetBytes(body, "prompt_cache_key").String(); gotKey != expectedPromptCacheKey {
		t.Fatalf("prompt_cache_key = %q, want %q", gotKey, expectedPromptCacheKey)
	}
	if gotSession := headers["session_id"]; len(gotSession) != 1 || gotSession[0] != expectedPromptCacheKey {
		t.Fatalf("session_id = %#v, want [%q]", gotSession, expectedPromptCacheKey)
	}
	if gotCanonicalSession := headers.Get("Session-Id"); gotCanonicalSession != "" {
		t.Fatalf("Session-Id = %q, want empty", gotCanonicalSession)
	}
	if gotRequestID := headers.Get("X-Client-Request-Id"); gotRequestID != expectedPromptCacheKey {
		t.Fatalf("X-Client-Request-Id = %q, want %q", gotRequestID, expectedPromptCacheKey)
	}
	if gotThreadID := headers.Get("Thread-Id"); gotThreadID != expectedPromptCacheKey {
		t.Fatalf("Thread-Id = %q, want %q", gotThreadID, expectedPromptCacheKey)
	}
	if gotConversation := headers.Get("Conversation_id"); gotConversation != expectedPromptCacheKey {
		t.Fatalf("Conversation_id = %q, want %q", gotConversation, expectedPromptCacheKey)
	}
	if gotWindowID := headers.Get("X-Codex-Window-Id"); gotWindowID != expectedPromptCacheKey+":0" {
		t.Fatalf("X-Codex-Window-Id = %q, want %q", gotWindowID, expectedPromptCacheKey+":0")
	}
	gotMetadata := headers.Get("X-Codex-Turn-Metadata")
	if gotMetadataPromptCacheKey := gjson.Get(gotMetadata, "prompt_cache_key").String(); gotMetadataPromptCacheKey != expectedPromptCacheKey {
		t.Fatalf("X-Codex-Turn-Metadata.prompt_cache_key = %q, want %q", gotMetadataPromptCacheKey, expectedPromptCacheKey)
	}
	if gotMetadataTurnID := gjson.Get(gotMetadata, "turn_id").String(); gotMetadataTurnID != expectedTurnID {
		t.Fatalf("X-Codex-Turn-Metadata.turn_id = %q, want %q", gotMetadataTurnID, expectedTurnID)
	}
	if gotMetadataWindowID := gjson.Get(gotMetadata, "window_id").String(); gotMetadataWindowID != expectedPromptCacheKey+":0" {
		t.Fatalf("X-Codex-Turn-Metadata.window_id = %q, want %q", gotMetadataWindowID, expectedPromptCacheKey+":0")
	}
	expectedInstallationID := codexIdentityConfuseUUID("auth-ws-1", "installation", "install-ws-1")
	if gotInstallationID := gjson.GetBytes(body, "client_metadata.x-codex-installation-id").String(); gotInstallationID != expectedInstallationID {
		t.Fatalf("installation id = %q, want %q", gotInstallationID, expectedInstallationID)
	}
}

func TestCodexIdentityConfuseResponsePayloadHidesUpstreamAndRestoresClient(t *testing.T) {
	state := codexIdentityConfuseState{
		enabled:                true,
		authID:                 "auth-ws-1",
		originalPromptCacheKey: "cache-ws-1",
		promptCacheKey:         codexIdentityConfuseUUID("auth-ws-1", "prompt-cache", "cache-ws-1"),
	}
	expectedTurnID := state.confuseTurnID("turn-ws-1")
	rawPayload := []byte(`{"type":"response.completed","response":{"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1"},"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1"}`)

	upstreamPayload := applyCodexIdentityConfuseResponsePayload(rawPayload, state)
	if bytes.Contains(upstreamPayload, []byte(`cache-ws-1`)) {
		t.Fatalf("upstream payload still contains original prompt_cache_key: %s", string(upstreamPayload))
	}
	if bytes.Contains(upstreamPayload, []byte(`turn-ws-1`)) {
		t.Fatalf("upstream payload still contains original turn_id: %s", string(upstreamPayload))
	}
	if !bytes.Contains(upstreamPayload, []byte(state.promptCacheKey)) {
		t.Fatalf("upstream payload missing confused prompt_cache_key: %s", string(upstreamPayload))
	}
	if !bytes.Contains(upstreamPayload, []byte(expectedTurnID)) {
		t.Fatalf("upstream payload missing confused turn_id: %s", string(upstreamPayload))
	}

	clientPayload := applyCodexIdentityExposeResponsePayload(upstreamPayload, state)
	if bytes.Contains(clientPayload, []byte(state.promptCacheKey)) {
		t.Fatalf("client payload still contains confused prompt_cache_key: %s", string(clientPayload))
	}
	if bytes.Contains(clientPayload, []byte(expectedTurnID)) {
		t.Fatalf("client payload still contains confused turn_id: %s", string(clientPayload))
	}
	if !bytes.Contains(clientPayload, []byte(`cache-ws-1`)) {
		t.Fatalf("client payload missing original prompt_cache_key: %s", string(clientPayload))
	}
	if !bytes.Contains(clientPayload, []byte(`turn-ws-1`)) {
		t.Fatalf("client payload missing original turn_id: %s", string(clientPayload))
	}

	rawSSE := []byte(`data: {"type":"response.completed","response":{"prompt_cache_key":"cache-ws-1","turn_id":"turn-ws-1"}}`)
	upstreamSSE := applyCodexIdentityConfuseResponsePayload(rawSSE, state)
	if bytes.Contains(upstreamSSE, []byte(`cache-ws-1`)) {
		t.Fatalf("upstream SSE still contains original prompt_cache_key: %s", string(upstreamSSE))
	}
	if bytes.Contains(upstreamSSE, []byte(`turn-ws-1`)) {
		t.Fatalf("upstream SSE still contains original turn_id: %s", string(upstreamSSE))
	}
	clientSSE := applyCodexIdentityExposeResponsePayload(upstreamSSE, state)
	if !bytes.Contains(clientSSE, []byte(`cache-ws-1`)) || bytes.Contains(clientSSE, []byte(state.promptCacheKey)) {
		t.Fatalf("client SSE prompt_cache_key was not restored: %s", string(clientSSE))
	}
	if !bytes.Contains(clientSSE, []byte(`turn-ws-1`)) || bytes.Contains(clientSSE, []byte(expectedTurnID)) {
		t.Fatalf("client SSE turn_id was not restored: %s", string(clientSSE))
	}
}

func TestBuildCodexResponsesWebsocketURLRequiresHTTPURL(t *testing.T) {
	if got, err := buildCodexResponsesWebsocketURL("https://example.com/backend/responses"); err != nil || got != "wss://example.com/backend/responses" {
		t.Fatalf("https URL = %q, %v; want wss URL", got, err)
	}
	if _, err := buildCodexResponsesWebsocketURL("ftp://example.com/responses"); err == nil {
		t.Fatalf("expected unsupported scheme error")
	}
	if _, err := buildCodexResponsesWebsocketURL("https:///responses"); err == nil {
		t.Fatalf("expected empty host error")
	}
}

func TestParseCodexWebsocketErrorMarksConnectionLimitRetryable(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached","message":"too many websockets"},"headers":{"retry-after":"1"}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}
	status, ok := err.(interface{ StatusCode() int })
	if !ok || status.StatusCode() != http.StatusTooManyRequests {
		t.Fatalf("status = %#v, want 429", err)
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected retryable websocket connection limit error")
	}
	if got := *retryable.RetryAfter(); got != 0 {
		t.Fatalf("retryAfter = %v, want connection-limit fallback 0", got)
	}
	withHeaders, ok := err.(interface{ Headers() http.Header })
	if !ok || withHeaders.Headers().Get("retry-after") != "1" {
		t.Fatalf("headers = %#v, want retry-after", err)
	}
}

func TestParseCodexWebsocketErrorUsesUsageLimitRetryMetadata(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"type":"usage_limit_reached","message":"usage limit reached","resets_in_seconds":7}}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}

	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected retryable usage limit websocket error")
	}
	if got := *retryable.RetryAfter(); got != 7*time.Second {
		t.Fatalf("retryAfter = %v, want 7s", got)
	}
}

func TestParseCodexWebsocketErrorPreservesWrappedBodyAndHeaders(t *testing.T) {
	err, ok := parseCodexWebsocketError([]byte(`{"type":"error","status":429,"body":{"error":{"code":"websocket_connection_limit_reached","type":"server_error","message":"too many websocket connections"}},"headers":{"x-request-id":"req-1"}}`))
	if !ok {
		t.Fatalf("expected websocket error")
	}

	parsed := gjson.Parse(err.Error())
	if got := parsed.Get("status").Int(); got != http.StatusTooManyRequests {
		t.Fatalf("wrapped status = %d, want 429; payload=%s", got, err.Error())
	}
	if got := parsed.Get("body.error.code").String(); got != "websocket_connection_limit_reached" {
		t.Fatalf("wrapped body error code = %s, want websocket_connection_limit_reached; payload=%s", got, err.Error())
	}
	if got := parsed.Get("error.code").String(); got != "websocket_connection_limit_reached" {
		t.Fatalf("surface error code = %s, want websocket_connection_limit_reached; payload=%s", got, err.Error())
	}
	retryable, ok := err.(interface{ RetryAfter() *time.Duration })
	if !ok || retryable.RetryAfter() == nil {
		t.Fatalf("expected body.error.code websocket connection limit to be retryable")
	}
	withHeaders, ok := err.(interface{ Headers() http.Header })
	if !ok || withHeaders.Headers().Get("x-request-id") != "req-1" {
		t.Fatalf("headers = %#v, want x-request-id", err)
	}
}

func TestApplyCodexHeadersUsesConfigUserAgentForOAuth(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent:    "config-ua",
			BetaFeatures: "config-beta",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	req = req.WithContext(contextWithGinHeaders(map[string]string{
		"User-Agent": "client-ua",
	}))

	applyCodexHeaders(req, auth, "oauth-token", true, cfg)

	if got := req.Header.Get("User-Agent"); got != "config-ua" {
		t.Fatalf("User-Agent = %s, want %s", got, "config-ua")
	}
	if got := req.Header.Get("x-codex-beta-features"); got != "" {
		t.Fatalf("x-codex-beta-features = %q, want empty", got)
	}
}

func TestApplyModelHeaderOverridesFromModelConfig(t *testing.T) {
	const wantUA = "codex-tui/0.144.0 (Mac OS 26.5.1; arm64) iTerm.app/3.6.11 (codex-tui; 0.144.0)"
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	cfg := &config.Config{
		CodexHeaderDefaults: config.CodexHeaderDefaults{
			UserAgent: "config-ua",
		},
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}

	applyCodexHeaders(req, auth, "oauth-token", true, cfg)
	applyModelHeaderOverrides(req.Header, "gpt-5.6-luna")

	if got := req.Header.Get("User-Agent"); got != wantUA {
		t.Fatalf("User-Agent = %q, want %q", got, wantUA)
	}
	if got := codexSessionHeaderValue(req.Header); got == "" {
		t.Fatal("expected Session_id to be set for Mac OS User-Agent override")
	}

	applyModelHeaderOverrides(req.Header, "gpt-5.4")
	if got := req.Header.Get("User-Agent"); got != wantUA {
		t.Fatalf("User-Agent after no-op override = %q, want %q", got, wantUA)
	}
}

func TestApplyModelHeaderOverridesMultipleHeaders(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	clientID := "test-model-header-override"
	reg.RegisterClient(clientID, "codex", []*registry.ModelInfo{{
		ID: "test-override-headers-model",
		Config: &registry.ModelConfig{
			OverrideHeader: map[string]string{
				"user-agent":    "custom-ua/1.0",
				"originator":    "custom-origin",
				"x-test-header": "forced-value",
			},
		},
	}})
	t.Cleanup(func() { reg.UnregisterClient(clientID) })

	headers := http.Header{}
	headers.Set("User-Agent", "old-ua")
	headers.Set("Originator", "old-origin")
	headers.Set("X-Test-Header", "old-value")

	applyModelHeaderOverrides(headers, "test-override-headers-model")

	if got := headers.Get("User-Agent"); got != "custom-ua/1.0" {
		t.Fatalf("User-Agent = %q, want custom-ua/1.0", got)
	}
	if got := headers.Get("Originator"); got != "custom-origin" {
		t.Fatalf("Originator = %q, want custom-origin", got)
	}
	if got := headers.Get("X-Test-Header"); got != "forced-value" {
		t.Fatalf("X-Test-Header = %q, want forced-value", got)
	}
}

func TestApplyCodexHeadersPassesThroughClientIdentityHeaders(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	auth := &cliproxyauth.Auth{
		Provider: "codex",
		Metadata: map[string]any{"email": "user@example.com"},
	}
	req = req.WithContext(contextWithGinHeaders(map[string]string{
		"Originator":            "Codex Desktop",
		"Version":               "0.115.0-alpha.27",
		"X-Codex-Turn-Metadata": `{"turn_id":"turn-1"}`,
		"X-Client-Request-Id":   "019d2233-e240-7162-992d-38df0a2a0e0d",
	}))

	applyCodexHeaders(req, auth, "oauth-token", true, nil)

	if got := req.Header.Get("Originator"); got != "Codex Desktop" {
		t.Fatalf("Originator = %s, want %s", got, "Codex Desktop")
	}
	if got := req.Header.Get("Version"); got != "0.115.0-alpha.27" {
		t.Fatalf("Version = %s, want %s", got, "0.115.0-alpha.27")
	}
	if got := req.Header.Get("X-Codex-Turn-Metadata"); got != `{"turn_id":"turn-1"}` {
		t.Fatalf("X-Codex-Turn-Metadata = %s, want %s", got, `{"turn_id":"turn-1"}`)
	}
	if got := req.Header.Get("X-Client-Request-Id"); got != "019d2233-e240-7162-992d-38df0a2a0e0d" {
		t.Fatalf("X-Client-Request-Id = %s, want %s", got, "019d2233-e240-7162-992d-38df0a2a0e0d")
	}
}

func TestApplyCodexHeadersDoesNotInjectClientOnlyHeadersByDefault(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "https://example.com/responses", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}

	applyCodexHeaders(req, nil, "oauth-token", true, nil)

	if got := req.Header.Get("Version"); got != "" {
		t.Fatalf("Version = %q, want empty", got)
	}
	if got := req.Header.Get("X-Codex-Turn-Metadata"); got != "" {
		t.Fatalf("X-Codex-Turn-Metadata = %q, want empty", got)
	}
	if got := req.Header.Get("X-Client-Request-Id"); got != "" {
		t.Fatalf("X-Client-Request-Id = %q, want empty", got)
	}
}

func contextWithGinHeaders(headers map[string]string) context.Context {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	ginCtx.Request.Header = make(http.Header, len(headers))
	for key, value := range headers {
		ginCtx.Request.Header.Set(key, value)
	}
	return context.WithValue(context.Background(), "gin", ginCtx)
}

func TestNewProxyAwareWebsocketDialerDirectDisablesProxy(t *testing.T) {
	t.Parallel()

	dialer := newProxyAwareWebsocketDialer(
		&config.Config{SDKConfig: sdkconfig.SDKConfig{ProxyURL: "http://global-proxy.example.com:8080"}},
		&cliproxyauth.Auth{ProxyURL: "direct"},
	)

	if dialer.Proxy != nil {
		t.Fatal("expected websocket proxy function to be nil for direct mode")
	}
}
