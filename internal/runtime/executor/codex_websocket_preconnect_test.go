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
	"testing/synctest"
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
		if !enabled || maxIdle != 2 || ttl != 30*time.Second {
			t.Fatalf("settings = (%v, %d, %s), want (true, 2, 30s)", enabled, maxIdle, ttl)
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

func TestCodexWebsocketPreconnectExpiryUsesDeterministicClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		var closes atomic.Int32
		pool := newCodexWebsocketPreconnectPool(ctx, func(*websocket.Conn) error {
			closes.Add(1)
			return nil
		})
		key := codexWebsocketPreconnectKey{authID: "auth", wsURL: "ws://test"}
		reserved, _, generation := pool.reserve(key, 1, time.Now())
		if !reserved {
			t.Fatal("reservation failed")
		}
		expired := make(chan struct{}, 1)
		if !pool.completeReservationObserved(key, generation, &websocket.Conn{}, 1, time.Hour, time.Now(), func() { expired <- struct{}{} }) {
			t.Fatal("reservation completion failed")
		}
		time.Sleep(time.Hour)
		synctest.Wait()
		if closes.Load() != 1 {
			t.Fatalf("close count = %d, want 1", closes.Load())
		}
		select {
		case <-expired:
		default:
			t.Fatal("expiry callback was not called")
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
	exec.scheduleSpeculativePreconnectRequest(codexPreconnectRequest{ctx: ctx, auth: &cliproxyauth.Auth{ID: authID, Provider: "codex"}, authID: authID, url: wsURL, headers: http.Header{}, sessionID: "root-session", trigger: "fanout_tool", toolName: "Agent"})
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
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		RequestLog:                          true,
		CodexWebsocketSpeculativePreconnect: true,
		CodexWebsocketPreconnectMaxIdle:     1,
	}}
	exec := NewCodexWebsocketsExecutor(cfg)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	exec.scheduleSpeculativePreconnectRequest(codexPreconnectRequest{ctx: ctx, auth: &cliproxyauth.Auth{ID: authID, Provider: "codex"}, authID: authID, url: wsURL, headers: http.Header{}, sessionID: "root-session", trigger: "fanout_tool", toolName: "Agent"})

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

func TestSuccessfulLeaseReplenishesWithinCap(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connected := make(chan struct{}, 2)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		connected <- struct{}{}
		<-release
		_ = conn.Close()
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	authID := "preconnect-replenish"
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		RequestLog:                          true,
		CodexWebsocketSpeculativePreconnect: true,
		CodexWebsocketPreconnectReplenish:   true,
		CodexWebsocketPreconnectMaxIdle:     1,
	}}
	exec := NewCodexWebsocketsExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: authID, Provider: "codex"}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	exec.scheduleSpeculativePreconnectRequest(codexPreconnectRequest{ctx: context.Background(), auth: auth, authID: authID, url: wsURL, headers: http.Header{}, sessionID: "root-session", trigger: "fanout_tool", toolName: "Agent"})
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("initial speculative websocket did not connect")
	}
	leased := exec.takeSpeculativePreconnect(context.Background(), auth, authID, wsURL, http.Header{}, "child-session")
	if leased == nil {
		t.Fatal("initial speculative websocket was not leased")
	}
	defer func() { _ = leased.Close() }()
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("successful lease did not replenish the speculative websocket")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		idle, dialing := exec.sessions.pool.snapshot()
		if idle == 1 && dialing == 0 {
			return
		}
		if idle+dialing > 1 {
			t.Fatalf("replenishment exceeded cap: idle=%d dialing=%d", idle, dialing)
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("replenished websocket was not published")
}

func TestReplenishment429EntersCooldownWithoutRetryLoop(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var attempts atomic.Int32
	connected := make(chan struct{})
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) > 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
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

	authID := "preconnect-replenish-429"
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		CodexWebsocketSpeculativePreconnect: true,
		CodexWebsocketPreconnectReplenish:   true,
		CodexWebsocketPreconnectMaxIdle:     1,
	}}
	exec := NewCodexWebsocketsExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: authID, Provider: "codex"}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	exec.scheduleSpeculativePreconnectRequest(codexPreconnectRequest{ctx: context.Background(), auth: auth, authID: authID, url: wsURL, headers: http.Header{}, sessionID: "root", trigger: "fanout_tool", toolName: "Agent"})
	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("initial websocket did not connect")
	}
	leased := exec.takeSpeculativePreconnect(context.Background(), auth, authID, wsURL, http.Header{}, "child")
	if leased == nil {
		t.Fatal("initial websocket was not leased")
	}
	defer func() { _ = leased.Close() }()
	deadline := time.Now().Add(time.Second)
	for attempts.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if attempts.Load() != 2 {
		t.Fatalf("dial attempts = %d, want initial plus one replenishment", attempts.Load())
	}
	exec.scheduleSpeculativePreconnectRequest(codexPreconnectRequest{ctx: context.Background(), auth: auth, authID: authID, url: wsURL, headers: http.Header{}, sessionID: "root", trigger: "fanout_tool", toolName: "Agent"})
	time.Sleep(50 * time.Millisecond)
	if attempts.Load() != 2 {
		t.Fatalf("429 replenishment retried in a loop: attempts=%d", attempts.Load())
	}
	if idle, dialing := exec.sessions.pool.snapshot(); idle != 0 || dialing != 0 {
		t.Fatalf("pool after 429 = idle %d dialing %d, want empty", idle, dialing)
	}
}

func TestParallelLeasesReplenishWithoutExceedingCap(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	connected := make(chan struct{}, 4)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := upgrader.Upgrade(w, r, nil)
		if errUpgrade != nil {
			return
		}
		connected <- struct{}{}
		<-release
		_ = conn.Close()
	}))
	t.Cleanup(func() {
		close(release)
		server.Close()
	})

	authID := "preconnect-parallel-replenish"
	cfg := &config.Config{SDKConfig: config.SDKConfig{
		CodexWebsocketSpeculativePreconnect: true,
		CodexWebsocketPreconnectReplenish:   true,
		CodexWebsocketPreconnectMaxIdle:     2,
	}}
	exec := NewCodexWebsocketsExecutor(cfg)
	auth := &cliproxyauth.Auth{ID: authID, Provider: "codex"}
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	for range 2 {
		exec.scheduleSpeculativePreconnectRequest(codexPreconnectRequest{ctx: context.Background(), auth: auth, authID: authID, url: wsURL, headers: http.Header{}, sessionID: "root", trigger: "fanout_tool", toolName: "Agent"})
	}
	for range 2 {
		select {
		case <-connected:
		case <-time.After(time.Second):
			t.Fatal("initial speculative pool did not fill")
		}
	}

	leased := make(chan *websocket.Conn, 2)
	for child := range 2 {
		go func(child int) {
			leased <- exec.takeSpeculativePreconnect(context.Background(), auth, authID, wsURL, http.Header{}, fmt.Sprintf("child-%d", child))
		}(child)
	}
	for range 2 {
		conn := <-leased
		if conn == nil {
			t.Fatal("parallel child missed a filled speculative pool")
		}
		defer func() { _ = conn.Close() }()
	}
	for range 2 {
		select {
		case <-connected:
		case <-time.After(time.Second):
			t.Fatal("parallel leases did not replenish both pool slots")
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		idle, dialing := exec.sessions.pool.snapshot()
		if idle+dialing > 2 {
			t.Fatalf("parallel replenishment exceeded cap: idle=%d dialing=%d", idle, dialing)
		}
		if idle == 2 && dialing == 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("parallel replenishments were not published")
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
		{name: "workflow added", payload: `{"type":"response.output_item.added","item":{"type":"function_call","name":"Workflow","call_id":"call-workflow"}}`, wantKey: "call-workflow", wantOK: true},
		{name: "workflow done case insensitive", payload: `{"type":"response.output_item.done","item":{"type":"function_call","name":"workflow","id":"item-workflow"}}`, wantKey: "item-workflow", wantOK: true},
		{name: "different tool", payload: `{"type":"response.output_item.added","item":{"type":"function_call","name":"Bash","call_id":"call-3"}}`},
		{name: "non function item", payload: `{"type":"response.output_item.added","item":{"type":"message","name":"Agent","id":"item-4"}}`},
		{name: "unrelated event", payload: `{"type":"response.created","item":{"type":"function_call","name":"Agent","call_id":"call-5"}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey, _, gotOK := codexFanoutToolCall([]byte(tt.payload))
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
	take := pool.takeOrWaitObserved(ctx, key, time.Minute)
	if !take.leased || take.conn != connections[0] {
		t.Fatal("request did not lease the in-flight speculative connection")
	}
	if take.observation.reason != "leased" || take.observation.wait < 15*time.Millisecond {
		t.Fatalf("observation = %+v, want leased wait >= 15ms", take.observation)
	}
}

func TestCodexWebsocketPreconnectPoolReportsImmediateMiss(t *testing.T) {
	pool := newCodexWebsocketPreconnectTestPool()
	key := codexWebsocketPreconnectKey{authID: "auth-miss", wsURL: "wss://example.test/responses"}
	take := pool.takeOrWaitObserved(context.Background(), key, time.Minute)
	if take.leased || take.conn != nil {
		t.Fatal("empty pool unexpectedly returned a connection")
	}
	if take.observation.reason != "no_idle" || take.observation.idle != 0 || take.observation.dialing != 0 {
		t.Fatalf("observation = %+v, want no_idle with empty pool", take.observation)
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
