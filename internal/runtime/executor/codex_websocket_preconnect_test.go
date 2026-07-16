package executor

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/tidwall/gjson"
)

func newPreconnectDebugHook(t *testing.T) *logtest.Hook {
	t.Helper()
	previousLevel := log.GetLevel()
	log.SetLevel(log.DebugLevel)
	hook := logtest.NewLocal(log.StandardLogger())
	t.Cleanup(func() {
		hook.Reset()
		log.SetLevel(previousLevel)
	})
	return hook
}

func TestBuildCodexGenerateFalseWarmupRequest(t *testing.T) {
	template := []byte(`{"type":"response.create","model":"gpt-5.6-sol","instructions":"large root instructions","input":[{"role":"user","content":"root prompt"}],"tools":[{"type":"function","name":"Agent"}],"tool_choice":{"type":"function","name":"Agent"},"previous_response_id":"resp-root","reasoning":{"effort":"high"},"service_tier":"priority"}`)

	got, errBuild := buildCodexGenerateFalseWarmupRequest(template)
	if errBuild != nil {
		t.Fatalf("buildCodexGenerateFalseWarmupRequest() error = %v", errBuild)
	}
	if gjson.GetBytes(got, "type").String() != "response.create" || !gjson.GetBytes(got, "generate").Exists() || gjson.GetBytes(got, "generate").Bool() {
		t.Fatalf("warmup envelope is invalid: %s", got)
	}
	if !bytes.Equal([]byte(gjson.GetBytes(got, "input").Raw), []byte("[]")) || !bytes.Equal([]byte(gjson.GetBytes(got, "tools").Raw), []byte("[]")) {
		t.Fatalf("warmup carries prompt or tools: %s", got)
	}
	if gjson.GetBytes(got, "previous_response_id").Exists() {
		t.Fatalf("warmup carries previous_response_id: %s", got)
	}
	if gjson.GetBytes(got, "instructions").String() != "" {
		t.Fatalf("warmup carries instructions: %s", got)
	}
	if gjson.GetBytes(got, "tool_choice").String() != "auto" {
		t.Fatalf("warmup tool_choice = %s, want auto", gjson.GetBytes(got, "tool_choice").Raw)
	}
	if gjson.GetBytes(got, "model").String() != "gpt-5.6-sol" || gjson.GetBytes(got, "reasoning.effort").String() != "high" || gjson.GetBytes(got, "service_tier").String() != "priority" {
		t.Fatalf("warmup lost routing properties: %s", got)
	}
}

func TestPerformCodexGenerateFalseWarmupSuccessAndRejection(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			return
		}
		if gjson.GetBytes(payload, "model").String() == "reject" {
			_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","status":429,"error":{"message":"slow down"}}`))
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"resp-warm"}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.completed","response":{"id":"resp-warm","usage":{"input_tokens":0,"output_tokens":0}}}`))
	}))
	defer server.Close()
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	t.Run("success", func(t *testing.T) {
		conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
		if errDial != nil {
			t.Fatalf("dial websocket: %v", errDial)
		}
		defer func() { _ = conn.Close() }()
		result, errWarmup := performCodexGenerateFalseWarmup(context.Background(), conn, []byte(`{"type":"response.create","model":"ok","generate":false,"input":[]}`))
		if errWarmup != nil {
			t.Fatalf("performCodexGenerateFalseWarmup() error = %v", errWarmup)
		}
		if result.responseID != "resp-warm" || result.inputTokens != 0 || result.outputTokens != 0 {
			t.Fatalf("warmup result = %+v", result)
		}
	})

	t.Run("rejection", func(t *testing.T) {
		conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
		if errDial != nil {
			t.Fatalf("dial websocket: %v", errDial)
		}
		defer func() { _ = conn.Close() }()
		_, errWarmup := performCodexGenerateFalseWarmup(context.Background(), conn, []byte(`{"type":"response.create","model":"reject","generate":false,"input":[]}`))
		if errWarmup == nil || codexGenerateFalseWarmupFailureReason(errWarmup) != "rate_limited" {
			t.Fatalf("warmup rejection = %v (%s), want rate_limited", errWarmup, codexGenerateFalseWarmupFailureReason(errWarmup))
		}
	})
}

func TestPerformCodexGenerateFalseWarmupHonorsCancellation(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade == nil {
			defer func() { _ = conn.Close() }()
			_, _, _ = conn.ReadMessage()
		}
	}))
	defer server.Close()
	conn, _, errDial := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if errDial != nil {
		t.Fatalf("dial websocket: %v", errDial)
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, errWarmup := performCodexGenerateFalseWarmup(ctx, conn, []byte(`{"type":"response.create","generate":false}`))
	if errWarmup != context.Canceled || codexGenerateFalseWarmupFailureReason(errWarmup) != "canceled" {
		t.Fatalf("canceled warmup error = %v (%s)", errWarmup, codexGenerateFalseWarmupFailureReason(errWarmup))
	}
}

func TestCodexWebsocketSpeculativePreconnectSettings(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		exec := NewCodexWebsocketsExecutor(&config.Config{})
		enabled, maxIdle, ttl := exec.speculativePreconnectSettings()
		if enabled || maxIdle != 0 || ttl != 0 {
			t.Fatalf("settings = (%v, %d, %s), want disabled zero values", enabled, maxIdle, ttl)
		}
	})

	t.Run("safe defaults", func(t *testing.T) {
		exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketSpeculativePreconnect: true}})
		enabled, maxIdle, ttl := exec.speculativePreconnectSettings()
		if !enabled || maxIdle != codexWebsocketPreconnectDefaultMaxIdle || ttl != codexWebsocketPreconnectDefaultTTL {
			t.Fatalf("settings = (%v, %d, %s), want (true, %d, %s)", enabled, maxIdle, ttl, codexWebsocketPreconnectDefaultMaxIdle, codexWebsocketPreconnectDefaultTTL)
		}
	})

	t.Run("explicit limits", func(t *testing.T) {
		exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{
			CodexWebsocketSpeculativePreconnect: true,
			CodexWebsocketPreconnectMaxIdle:     4,
			CodexWebsocketPreconnectTTLSeconds:  9,
		}})
		enabled, maxIdle, ttl := exec.speculativePreconnectSettings()
		if !enabled || maxIdle != 4 || ttl != 9*time.Second {
			t.Fatalf("settings = (%v, %d, %s), want (true, 4, 9s)", enabled, maxIdle, ttl)
		}
	})
}

func TestScheduleSpeculativePreconnectRecordsReadyMetric(t *testing.T) {
	hook := newPreconnectDebugHook(t)
	gin.SetMode(gin.TestMode)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	release := make(chan struct{})
	connected := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		close(connected)
		<-release
		_ = conn.Close()
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	authID := "preconnect-ready-metric"
	globalCodexWebsocketPreconnectPool.closeAuth(authID)
	t.Cleanup(func() { globalCodexWebsocketPreconnectPool.closeAuth(authID) })
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		RequestLog:                          true,
		CodexWebsocketSpeculativePreconnect: true,
		CodexWebsocketPreconnectMaxIdle:     1,
	}}
	exec := NewCodexWebsocketsExecutor(cfg)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ginCtx.Request.Header.Set(helps.ClaudeCodeSessionHeader, "detached-root-session")
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	expectedRootCorrelation, _ := helps.ClaudeCodeCorrelationIDs(ctx, nil, nil)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	exec.scheduleSpeculativePreconnect(ctx, &cliproxyauth.Auth{ID: authID, Provider: "codex"}, authID, wsURL, http.Header{}, "root-session", nil)
	ginCtx.Request.Header.Set(helps.ClaudeCodeSessionHeader, "reused-child-session")

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("speculative websocket did not connect")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, `"name":"speculative_preconnect_ready"`) {
				for _, want := range []string{`"duration_us":`, `"generate_false_warmup":false`} {
					if !strings.Contains(entry.Message, want) {
						t.Fatalf("ready metric = %s, missing %s", entry.Message, want)
					}
				}
				if !strings.Contains(entry.Message, expectedRootCorrelation) {
					t.Fatalf("detached metric lost immutable root correlation %q: %s", expectedRootCorrelation, entry.Message)
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("speculative_preconnect_ready metric was not recorded")
}

func TestScheduleSpeculativePreconnectRecordsRateLimitedFailureMetric(t *testing.T) {
	hook := newPreconnectDebugHook(t)
	gin.SetMode(gin.TestMode)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	authID := "preconnect-failed-metric"
	globalCodexWebsocketPreconnectPool.closeAuth(authID)
	t.Cleanup(func() { globalCodexWebsocketPreconnectPool.closeAuth(authID) })
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		RequestLog:                          true,
		CodexWebsocketSpeculativePreconnect: true,
		CodexWebsocketPreconnectMaxIdle:     1,
	}}
	exec := NewCodexWebsocketsExecutor(cfg)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	exec.scheduleSpeculativePreconnect(ctx, &cliproxyauth.Auth{ID: authID, Provider: "codex"}, authID, wsURL, http.Header{}, "root-session", nil)

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, `"name":"speculative_preconnect_failed"`) {
				got := entry.Message
				for _, want := range []string{`"status":429`, `"rate_limited":true`, `"duration_us":`} {
					if !strings.Contains(got, want) {
						t.Fatalf("failure metric = %s, missing %s", got, want)
					}
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("speculative_preconnect_failed metric was not recorded")
}

func TestCodexAgentToolCallKeyRecognizesOnlyAgentFunctionCalls(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantKey string
		wantOK  bool
	}{
		{name: "added", payload: `{"type":"response.output_item.added","item":{"type":"function_call","name":"Agent","call_id":"call-1"}}`, wantKey: "call-1", wantOK: true},
		{name: "done case insensitive", payload: `{"type":"response.output_item.done","item":{"type":"function_call","name":"agent","id":"item-2"}}`, wantKey: "item-2", wantOK: true},
		{name: "different tool", payload: `{"type":"response.output_item.added","item":{"type":"function_call","name":"Bash","call_id":"call-3"}}`},
		{name: "non function item", payload: `{"type":"response.output_item.added","item":{"type":"message","name":"Agent","id":"item-4"}}`},
		{name: "unrelated event", payload: `{"type":"response.created","item":{"type":"function_call","name":"Agent","call_id":"call-5"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey, gotOK := codexAgentToolCallKey([]byte(tt.payload))
			if gotKey != tt.wantKey || gotOK != tt.wantOK {
				t.Fatalf("codexAgentToolCallKey() = (%q, %v), want (%q, %v)", gotKey, gotOK, tt.wantKey, tt.wantOK)
			}
		})
	}
}

func TestCodexWebsocketPreconnectPoolBoundsCapacityAndIsolatesRoutes(t *testing.T) {
	connections := newCodexPreconnectTestConnections(t, 2)
	pool := newCodexWebsocketPreconnectTestPool()
	keyA := codexWebsocketPreconnectKey{authID: "auth-a", wsURL: "wss://example.test/responses"}
	keyB := codexWebsocketPreconnectKey{authID: "auth-b", wsURL: "wss://example.test/responses"}
	now := time.Now()

	reserved, _, tokenA := pool.reserve(keyA, 2, now)
	if !reserved || !pool.completeReservation(keyA, tokenA, connections[0], 2, time.Minute, now) {
		t.Fatal("first route reservation was not stored")
	}
	reserved, _, tokenB := pool.reserve(keyB, 2, now)
	if !reserved || !pool.completeReservation(keyB, tokenB, connections[1], 2, time.Minute, now) {
		t.Fatal("second route reservation was not stored")
	}
	if reserved, reason, _ := pool.reserve(keyA, 2, now); reserved || reason != "capacity" {
		t.Fatalf("reservation past cap = (%v, %q), want (false, capacity)", reserved, reason)
	}
	if conn, _, ok := pool.take(keyA, time.Minute, now); !ok || conn != connections[0] {
		t.Fatal("route A did not receive its own idle connection")
	}
	if conn, _, ok := pool.take(keyB, time.Minute, now); !ok || conn != connections[1] {
		t.Fatal("route B did not receive its own idle connection")
	}
}

func TestCodexWebsocketPreconnectPoolExpiresUnusedConnection(t *testing.T) {
	connections := newCodexPreconnectTestConnections(t, 1)
	pool := newCodexWebsocketPreconnectTestPool()
	key := codexWebsocketPreconnectKey{authID: "auth-expire", wsURL: "wss://example.test/responses"}
	now := time.Now()
	reserved, _, token := pool.reserve(key, 1, now)
	if !reserved || !pool.completeReservation(key, token, connections[0], 1, 20*time.Millisecond, now) {
		t.Fatal("expiring reservation was not stored")
	}
	time.Sleep(80 * time.Millisecond)
	if conn, _, ok := pool.take(key, time.Minute, time.Now()); ok || conn != nil {
		t.Fatal("expired speculative connection remained leasable")
	}
}

func TestCodexWebsocketPreconnectPoolReportsExpiration(t *testing.T) {
	connections := newCodexPreconnectTestConnections(t, 1)
	pool := newCodexWebsocketPreconnectTestPool()
	key := codexWebsocketPreconnectKey{authID: "auth-expire-observed", wsURL: "wss://example.test/responses"}
	now := time.Now()
	reserved, _, token := pool.reserve(key, 1, now)
	if !reserved {
		t.Fatal("expiration reservation was rejected")
	}
	expired := make(chan struct{})
	if !pool.completeReservationObserved(key, token, connections[0], 1, 20*time.Millisecond, now, func() { close(expired) }) {
		t.Fatal("expiration reservation was not stored")
	}
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("expiration callback was not invoked")
	}
}

func TestCodexWebsocketPreconnectPool429CooldownIsRouteScoped(t *testing.T) {
	pool := newCodexWebsocketPreconnectTestPool()
	keyA := codexWebsocketPreconnectKey{authID: "auth-a", wsURL: "wss://a.test/responses"}
	keyB := codexWebsocketPreconnectKey{authID: "auth-b", wsURL: "wss://b.test/responses"}
	now := time.Now()
	reserved, _, tokenA := pool.reserve(keyA, 2, now)
	if !reserved {
		t.Fatal("initial reservation was rejected")
	}
	pool.failReservation(keyA, tokenA, true, now)
	if reserved, reason, _ := pool.reserve(keyA, 2, now.Add(time.Second)); reserved || reason != "cooldown" {
		t.Fatalf("rate-limited route reservation = (%v, %q), want cooldown", reserved, reason)
	}
	if reserved, _, tokenB := pool.reserve(keyB, 2, now.Add(time.Second)); !reserved {
		t.Fatal("unrelated route inherited 429 cooldown")
	} else {
		pool.failReservation(keyB, tokenB, false, now)
	}
	if reserved, _, tokenA2 := pool.reserve(keyA, 2, now.Add(codexWebsocketPreconnect429Cooldown+time.Second)); !reserved {
		t.Fatal("route remained in cooldown after deadline")
	} else {
		pool.failReservation(keyA, tokenA2, false, now)
	}
}

func TestCodexWebsocketPreconnectPoolCloseAuthRejectsInflightCompletion(t *testing.T) {
	connections := newCodexPreconnectTestConnections(t, 1)
	pool := newCodexWebsocketPreconnectTestPool()
	key := codexWebsocketPreconnectKey{authID: "auth-removed", wsURL: "wss://example.test/responses"}
	now := time.Now()
	reserved, _, token := pool.reserve(key, 1, now)
	if !reserved {
		t.Fatal("initial reservation was rejected")
	}
	pool.closeAuth(key.authID)
	if pool.completeReservation(key, token, connections[0], 1, time.Minute, now.Add(time.Second)) {
		t.Fatal("in-flight dial completed into pool after auth was closed")
	}
}

func TestCodexWebsocketPreconnectPoolWaitsForInflightConnection(t *testing.T) {
	connections := newCodexPreconnectTestConnections(t, 1)
	pool := newCodexWebsocketPreconnectTestPool()
	key := codexWebsocketPreconnectKey{authID: "auth-wait", wsURL: "wss://example.test/responses"}
	now := time.Now()
	reserved, _, token := pool.reserve(key, 1, now)
	if !reserved {
		t.Fatal("initial reservation was rejected")
	}
	go func() {
		time.Sleep(20 * time.Millisecond)
		pool.completeReservation(key, token, connections[0], 1, time.Minute, time.Now())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	conn, _, observation, ok := pool.takeOrWaitObserved(ctx, key, time.Minute)
	if !ok || conn != connections[0] {
		t.Fatal("request did not lease the in-flight speculative connection")
	}
	if observation.reason != "leased" || observation.wait < 15*time.Millisecond {
		t.Fatalf("observation = %+v, want leased wait >= 15ms", observation)
	}
}

func TestCodexWebsocketPreconnectPoolReportsImmediateMiss(t *testing.T) {
	pool := newCodexWebsocketPreconnectTestPool()
	key := codexWebsocketPreconnectKey{authID: "auth-miss", wsURL: "wss://example.test/responses"}
	conn, _, observation, ok := pool.takeOrWaitObserved(context.Background(), key, time.Minute)
	if ok || conn != nil {
		t.Fatal("empty pool unexpectedly returned a connection")
	}
	if observation.reason != "no_idle" || observation.idle != 0 || observation.dialing != 0 {
		t.Fatalf("observation = %+v, want no_idle with empty pool", observation)
	}
}

func newCodexWebsocketPreconnectTestPool() *codexWebsocketPreconnectPool {
	return &codexWebsocketPreconnectPool{
		idle:           make(map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry),
		cooldown:       make(map[codexWebsocketPreconnectKey]time.Time),
		authGeneration: make(map[string]uint64),
		dialingByKey:   make(map[codexWebsocketPreconnectKey]int),
		changed:        make(map[codexWebsocketPreconnectKey]chan struct{}),
	}
}

func newCodexPreconnectTestConnections(t *testing.T, count int) []*websocket.Conn {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	release := make(chan struct{})
	serverConnections := make(chan *websocket.Conn, count)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		serverConnections <- conn
		<-release
		_ = conn.Close()
	}))
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	clients := make([]*websocket.Conn, 0, count)
	for range count {
		conn, _, errDial := websocket.DefaultDialer.Dial(wsURL, nil)
		if errDial != nil {
			t.Fatalf("dial test websocket: %v", errDial)
		}
		clients = append(clients, conn)
		<-serverConnections
	}
	t.Cleanup(func() {
		for _, conn := range clients {
			_ = conn.Close()
		}
		close(release)
		server.Close()
	})
	return clients
}
