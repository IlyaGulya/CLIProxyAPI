package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	proxyconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestClaudeMessagesRoutesThroughPersistentCodexWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	captured := make(chan []byte, 2)
	serverErrors := make(chan error, 1)
	var connections atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connections.Add(1)
		defer func() { _ = conn.Close() }()
		for turn := 0; turn < 2; turn++ {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				serverErrors <- fmt.Errorf("read turn %d: %w", turn, errRead)
				return
			}
			captured <- bytes.Clone(payload)
			responseID := fmt.Sprintf("resp-ingress-%d", turn+1)
			output := `[]`
			if turn == 0 {
				item := `{"id":"msg-1","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"answer one","annotations":[]}]}`
				itemDone := []byte(`{"type":"response.output_item.done","output_index":0,"item":` + item + `}`)
				if errWrite := conn.WriteMessage(websocket.TextMessage, itemDone); errWrite != nil {
					serverErrors <- fmt.Errorf("write output item: %w", errWrite)
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
	defer upstream.Close()

	server := newTestServer(t)
	server.handlers.Cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.DisableImageGeneration = proxyconfig.DisableImageGenerationAll
	codexExecutor := runtimeexecutor.NewCodexAutoExecutor(server.cfg)
	server.handlers.AuthManager.RegisterExecutor(codexExecutor)
	credential := &cliproxyauth.Auth{
		ID:       "codex-ws-ingress",
		Provider: "codex",
		Status:   cliproxyauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "sk-test",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, credential.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-sol"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
	if _, errRegister := server.handlers.AuthManager.Register(context.Background(), credential); errRegister != nil {
		t.Fatalf("register Codex credential: %v", errRegister)
	}

	payloads := []string{
		`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"question one"}],"max_tokens":128,"stream":true}`,
		`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"question one"},{"role":"assistant","content":"answer one"},{"role":"user","content":"question two"}],"max_tokens":128,"stream":true}`,
	}
	for turn, payload := range payloads {
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer test-key")
		req.Header.Set(helps.ClaudeCodeSessionHeader, "ingress-root-session")
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK {
			t.Fatalf("turn %d status = %d, want 200; body=%s", turn, rr.Code, rr.Body.String())
		}
		if !strings.Contains(rr.Body.String(), "message_stop") {
			t.Fatalf("turn %d response is not a complete Claude stream: %s", turn, rr.Body.String())
		}
	}

	first := <-captured
	second := <-captured
	if got := connections.Load(); got != 1 {
		t.Fatalf("websocket connections = %d, want 1", got)
	}
	if got := gjson.GetBytes(first, "previous_response_id").String(); got != "" {
		t.Fatalf("first previous_response_id = %q, want empty", got)
	}
	if got := gjson.GetBytes(second, "previous_response_id").String(); got != "resp-ingress-1" {
		t.Fatalf("second previous_response_id = %q, want resp-ingress-1; payload=%s", got, second)
	}
	if got := gjson.GetBytes(second, "input.#").Int(); got != 1 {
		t.Fatalf("second delta input count = %d, want 1; payload=%s", got, second)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
}

func TestClaudeMessagesAgentToolSpeculativelyPreconnectsChildWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 4)
	spareConnected := make(chan struct{})
	warmupPayload := make(chan []byte, 1)
	childPayload := make(chan []byte, 1)
	var connections atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connectionIndex := connections.Add(1)
		defer func() { _ = conn.Close() }()
		switch connectionIndex {
		case 1:
			if _, _, errRead := conn.ReadMessage(); errRead != nil {
				serverErrors <- fmt.Errorf("read root request: %w", errRead)
				return
			}
			added := []byte(`{"type":"response.output_item.added","output_index":0,"item":{"id":"fc-agent","type":"function_call","name":"Agent","arguments":"","call_id":"call-agent","status":"in_progress"}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, added); errWrite != nil {
				serverErrors <- fmt.Errorf("write Agent added event: %w", errWrite)
				return
			}
			select {
			case <-spareConnected:
			case <-time.After(3 * time.Second):
				serverErrors <- fmt.Errorf("speculative websocket did not connect before root completion")
				return
			}
			item := `{"id":"fc-agent","type":"function_call","name":"Agent","arguments":"{\"description\":\"child\",\"prompt\":\"reply\"}","call_id":"call-agent","status":"completed"}`
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.output_item.done","output_index":0,"item":`+item+`}`)); errWrite != nil {
				serverErrors <- fmt.Errorf("write Agent done event: %w", errWrite)
				return
			}
			completed := []byte(`{"type":"response.completed","response":{"id":"resp-root-agent","model":"gpt-5.6-sol","output":[` + item + `],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				serverErrors <- fmt.Errorf("write root completion: %w", errWrite)
			}
		case 2:
			close(spareConnected)
			_, warmup, errRead := conn.ReadMessage()
			if errRead != nil {
				serverErrors <- fmt.Errorf("read generate=false warmup: %w", errRead)
				return
			}
			warmupPayload <- bytes.Clone(warmup)
			warmupCompleted := []byte(`{"type":"response.completed","response":{"id":"resp-warmup","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":0,"output_tokens":0,"total_tokens":0}}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, warmupCompleted); errWrite != nil {
				serverErrors <- fmt.Errorf("write warmup completion: %w", errWrite)
				return
			}
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				serverErrors <- fmt.Errorf("read child request from speculative websocket: %w", errRead)
				return
			}
			childPayload <- bytes.Clone(payload)
			completed := []byte(`{"type":"response.completed","response":{"id":"resp-child","model":"gpt-5.6-luna","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`)
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				serverErrors <- fmt.Errorf("write child completion: %w", errWrite)
			}
		default:
			serverErrors <- fmt.Errorf("unexpected websocket connection %d", connectionIndex)
		}
	}))
	t.Cleanup(func() {
		upstream.CloseClientConnections()
		upstream.Close()
	})

	server := newTestServer(t)
	server.handlers.Cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.CodexWebsocketSpeculativePreconnect = true
	server.cfg.CodexWebsocketGenerateFalseWarmup = true
	server.cfg.CodexWebsocketPreconnectMaxIdle = 2
	server.cfg.DisableImageGeneration = proxyconfig.DisableImageGenerationAll
	server.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(server.cfg))
	credential := &cliproxyauth.Auth{
		ID:       "codex-ws-speculative-agent",
		Provider: "codex",
		Status:   cliproxyauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "sk-test",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, credential.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-sol"}, {ID: "gpt-5.6-luna"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
	if _, errRegister := server.handlers.AuthManager.Register(context.Background(), credential); errRegister != nil {
		t.Fatalf("register Codex credential: %v", errRegister)
	}
	t.Cleanup(func() { runtimeexecutor.CloseCodexWebsocketSessionsForAuthID(credential.ID, "test_cleanup") })

	rootPayload := `{"model":"gpt-5.6-sol","tools":[{"name":"Agent","description":"spawn child","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"spawn one child"}],"max_tokens":128,"stream":true}`
	rootReq := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(rootPayload))
	rootReq.Header.Set("Authorization", "Bearer test-key")
	rootReq.Header.Set(helps.ClaudeCodeSessionHeader, "speculative-root-session")
	rootRR := httptest.NewRecorder()
	server.engine.ServeHTTP(rootRR, rootReq)
	if rootRR.Code != http.StatusOK || !strings.Contains(rootRR.Body.String(), "message_stop") {
		t.Fatalf("root response status=%d body=%s", rootRR.Code, rootRR.Body.String())
	}
	select {
	case payload := <-warmupPayload:
		if !gjson.GetBytes(payload, "generate").Exists() || gjson.GetBytes(payload, "generate").Bool() {
			t.Fatalf("speculative warmup does not set generate=false: %s", payload)
		}
		if got := gjson.GetBytes(payload, "input.#").Int(); got != 0 {
			t.Fatalf("speculative warmup input items = %d, want 0; payload=%s", got, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("generate=false warmup was not sent")
	}

	childRequestBody := `{"model":"gpt-5.6-luna","messages":[{"role":"user","content":"child work"}],"max_tokens":128,"stream":true}`
	childReq := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(childRequestBody))
	childReq.Header.Set("Authorization", "Bearer test-key")
	childReq.Header.Set(helps.ClaudeCodeSessionHeader, "speculative-root-session")
	childReq.Header.Set(helps.ClaudeCodeAgentHeader, "child-agent-1")
	childRR := httptest.NewRecorder()
	server.engine.ServeHTTP(childRR, childReq)
	if childRR.Code != http.StatusOK || !strings.Contains(childRR.Body.String(), "message_stop") {
		t.Fatalf("child response status=%d body=%s", childRR.Code, childRR.Body.String())
	}

	select {
	case payload := <-childPayload:
		if got := gjson.GetBytes(payload, "model").String(); got != "gpt-5.6-luna" {
			t.Fatalf("child model = %q, want gpt-5.6-luna; payload=%s", got, payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("child request was not sent over the speculative websocket")
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want root plus one speculative child connection", got)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
}

func TestClaudeMessagesRootAndSubagentDoNotSerializeOnOneWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 4)
	bothArrived := make(chan struct{})
	var closeBarrier sync.Once
	var connections atomic.Int32
	var arrivals atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connectionIndex := connections.Add(1)
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			serverErrors <- fmt.Errorf("connection %d read: %w", connectionIndex, errRead)
			return
		}
		if arrivals.Add(1) == 2 {
			closeBarrier.Do(func() { close(bothArrived) })
		}
		select {
		case <-bothArrived:
		case <-time.After(3 * time.Second):
			serverErrors <- fmt.Errorf("connection %d timed out waiting for the other agent request", connectionIndex)
			return
		}
		completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-agent-%d","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`, connectionIndex))
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			serverErrors <- fmt.Errorf("connection %d write: %w", connectionIndex, errWrite)
		}
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.handlers.Cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.DisableImageGeneration = proxyconfig.DisableImageGenerationAll
	server.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(server.cfg))
	credential := &cliproxyauth.Auth{
		ID:       "codex-ws-agents",
		Provider: "codex",
		Status:   cliproxyauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "sk-test",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, credential.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-sol"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
	if _, errRegister := server.handlers.AuthManager.Register(context.Background(), credential); errRegister != nil {
		t.Fatalf("register Codex credential: %v", errRegister)
	}

	type requestResult struct {
		name   string
		status int
		body   string
	}
	results := make(chan requestResult, 2)
	start := make(chan struct{})
	for _, agent := range []struct {
		name    string
		agentID string
	}{
		{name: "root"},
		{name: "child", agentID: "agent-child"},
	} {
		agent := agent
		go func() {
			<-start
			payload := `{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"hello from ` + agent.name + `"}],"max_tokens":128,"stream":true}`
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(payload))
			req.Header.Set("Authorization", "Bearer test-key")
			req.Header.Set(helps.ClaudeCodeSessionHeader, "shared-root-session")
			if agent.agentID != "" {
				req.Header.Set(helps.ClaudeCodeAgentHeader, agent.agentID)
			}
			rr := httptest.NewRecorder()
			server.engine.ServeHTTP(rr, req)
			results <- requestResult{name: agent.name, status: rr.Code, body: rr.Body.String()}
		}()
	}
	close(start)
	for range 2 {
		select {
		case result := <-results:
			if result.status != http.StatusOK || !strings.Contains(result.body, "message_stop") {
				t.Fatalf("%s response status=%d body=%s", result.name, result.status, result.body)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("root/subagent requests serialized or timed out")
		}
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want one per root/subagent", got)
	}
	if got := arrivals.Load(); got != 2 {
		t.Fatalf("concurrent upstream requests = %d, want 2", got)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
}

func TestClaudeMessagesConcurrentSameSessionUsesOverflowWebsocket(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	serverErrors := make(chan error, 4)
	bothArrived := make(chan struct{})
	var closeBarrier sync.Once
	var connections atomic.Int32
	var arrivals atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			serverErrors <- fmt.Errorf("upgrade websocket: %w", errUpgrade)
			return
		}
		connectionIndex := connections.Add(1)
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			serverErrors <- fmt.Errorf("connection %d read: %w", connectionIndex, errRead)
			return
		}
		if arrivals.Add(1) == 2 {
			closeBarrier.Do(func() { close(bothArrived) })
		}
		select {
		case <-bothArrived:
		case <-time.After(2 * time.Second):
			serverErrors <- fmt.Errorf("same-session connection %d timed out waiting for overflow request", connectionIndex)
			return
		}
		completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-overflow-%d","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`, connectionIndex))
		if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
			serverErrors <- fmt.Errorf("connection %d write: %w", connectionIndex, errWrite)
		}
	}))
	defer upstream.Close()

	server := newTestServer(t)
	server.handlers.Cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.DisableImageGeneration = proxyconfig.DisableImageGenerationAll
	server.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(server.cfg))
	credential := &cliproxyauth.Auth{
		ID:       "codex-ws-same-session-overflow",
		Provider: "codex",
		Status:   cliproxyauth.StatusActive,
		Attributes: map[string]string{
			"api_key":    "sk-test",
			"base_url":   upstream.URL,
			"websockets": "true",
		},
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, credential.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-sol"}})
	t.Cleanup(func() {
		runtimeexecutor.CloseCodexWebsocketSessionsForAuthID(credential.ID, "test_cleanup")
		registry.GetGlobalRegistry().UnregisterClient(credential.ID)
	})
	if _, errRegister := server.handlers.AuthManager.Register(context.Background(), credential); errRegister != nil {
		t.Fatalf("register Codex credential: %v", errRegister)
	}

	type requestResult struct {
		name   string
		status int
		body   string
	}
	results := make(chan requestResult, 2)
	start := make(chan struct{})
	for _, requestName := range []string{"title", "main"} {
		requestName := requestName
		go func() {
			<-start
			payload := `{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"` + requestName + ` request"}],"max_tokens":128,"stream":true}`
			req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(payload))
			req.Header.Set("Authorization", "Bearer test-key")
			req.Header.Set(helps.ClaudeCodeSessionHeader, "same-session-overflow-root")
			rr := httptest.NewRecorder()
			server.engine.ServeHTTP(rr, req)
			results <- requestResult{name: requestName, status: rr.Code, body: rr.Body.String()}
		}()
	}
	close(start)
	for range 2 {
		select {
		case result := <-results:
			if result.status != http.StatusOK || !strings.Contains(result.body, "message_stop") {
				t.Fatalf("%s response status=%d body=%s", result.name, result.status, result.body)
			}
		case <-time.After(4 * time.Second):
			t.Fatal("same-session requests serialized instead of using overflow")
		}
	}
	if got := connections.Load(); got != 2 {
		t.Fatalf("websocket connections = %d, want canonical plus overflow", got)
	}
	if got := arrivals.Load(); got != 2 {
		t.Fatalf("concurrent upstream requests = %d, want 2", got)
	}
	select {
	case errServer := <-serverErrors:
		t.Fatal(errServer)
	default:
	}
}

func TestClaudeMessagesWebsocket429FailsOverAndRepinsSession(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var primaryConnections atomic.Int32
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Errorf("primary upgrade websocket: %v", errUpgrade)
			return
		}
		primaryConnections.Add(1)
		defer func() { _ = conn.Close() }()
		if _, _, errRead := conn.ReadMessage(); errRead != nil {
			t.Errorf("primary read: %v", errRead)
			return
		}
		errorPayload := []byte(`{"type":"error","status":429,"error":{"code":"websocket_connection_limit_reached","message":"too many websocket connections"}}`)
		if errWrite := conn.WriteMessage(websocket.TextMessage, errorPayload); errWrite != nil {
			t.Errorf("primary write: %v", errWrite)
		}
	}))
	defer primary.Close()

	backupPayloads := make(chan []byte, 2)
	backupErrors := make(chan error, 1)
	var backupConnections atomic.Int32
	var backupRequests atomic.Int32
	backup := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			backupErrors <- fmt.Errorf("backup upgrade websocket: %w", errUpgrade)
			return
		}
		backupConnections.Add(1)
		defer func() { _ = conn.Close() }()
		for turn := 0; turn < 2; turn++ {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				backupErrors <- fmt.Errorf("backup read turn %d: %w", turn, errRead)
				return
			}
			backupRequests.Add(1)
			backupPayloads <- bytes.Clone(payload)
			completed := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-backup-%d","model":"gpt-5.6-sol","output":[],"usage":{"input_tokens":1,"output_tokens":0,"total_tokens":1}}}`, turn+1))
			if errWrite := conn.WriteMessage(websocket.TextMessage, completed); errWrite != nil {
				backupErrors <- fmt.Errorf("backup write turn %d: %w", turn, errWrite)
				return
			}
		}
	}))
	defer backup.Close()

	server := newTestServer(t)
	server.handlers.Cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.CodexPreferUpstreamWebsockets = true
	server.cfg.DisableImageGeneration = proxyconfig.DisableImageGenerationAll
	server.handlers.AuthManager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(server.cfg))
	credentials := []*cliproxyauth.Auth{
		{
			ID:       "codex-ws-primary",
			Provider: "codex",
			Status:   cliproxyauth.StatusActive,
			Attributes: map[string]string{
				"api_key":    "sk-primary",
				"base_url":   primary.URL,
				"websockets": "true",
				"priority":   "10",
			},
		},
		{
			ID:       "codex-ws-backup",
			Provider: "codex",
			Status:   cliproxyauth.StatusActive,
			Attributes: map[string]string{
				"api_key":    "sk-backup",
				"base_url":   backup.URL,
				"websockets": "true",
				"priority":   "0",
			},
		},
	}
	for _, credential := range credentials {
		registry.GetGlobalRegistry().RegisterClient(credential.ID, credential.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-sol"}})
		credentialID := credential.ID
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credentialID) })
		if _, errRegister := server.handlers.AuthManager.Register(context.Background(), credential); errRegister != nil {
			t.Fatalf("register %s: %v", credential.ID, errRegister)
		}
	}

	for turn := 0; turn < 2; turn++ {
		payload := fmt.Sprintf(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":"turn %d"}],"max_tokens":128,"stream":true}`, turn)
		req := httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(payload))
		req.Header.Set("Authorization", "Bearer test-key")
		req.Header.Set(helps.ClaudeCodeSessionHeader, "failover-root-session")
		rr := httptest.NewRecorder()
		server.engine.ServeHTTP(rr, req)
		if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "message_stop") {
			t.Fatalf("turn %d status=%d body=%s", turn, rr.Code, rr.Body.String())
		}
	}

	if got := primaryConnections.Load(); got != 1 {
		t.Fatalf("primary websocket connections = %d, want 1", got)
	}
	if got := backupConnections.Load(); got != 1 {
		t.Fatalf("backup websocket connections = %d, want 1 reused connection", got)
	}
	if got := backupRequests.Load(); got != 2 {
		t.Fatalf("backup websocket requests = %d, want 2", got)
	}
	firstBackup := <-backupPayloads
	<-backupPayloads
	if got := gjson.GetBytes(firstBackup, "previous_response_id").String(); got != "" {
		t.Fatalf("failover request carried stale previous_response_id %q: %s", got, firstBackup)
	}
	select {
	case errBackup := <-backupErrors:
		t.Fatal(errBackup)
	default:
	}
}
