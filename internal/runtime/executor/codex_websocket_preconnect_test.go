package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

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
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	exec.scheduleSpeculativePreconnect(ctx, &cliproxyauth.Auth{ID: authID, Provider: "codex"}, authID, wsURL, http.Header{}, "root-session")

	select {
	case <-connected:
	case <-time.After(time.Second):
		t.Fatal("speculative websocket did not connect")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if value, exists := ginCtx.Get("API_WEBSOCKET_TIMELINE"); exists {
			if timeline, ok := value.([]byte); ok && strings.Contains(string(timeline), `"name":"speculative_preconnect_ready"`) {
				if !strings.Contains(string(timeline), `"duration_us":`) {
					t.Fatalf("ready metric lacks duration: %s", timeline)
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("speculative_preconnect_ready metric was not recorded")
}

func TestScheduleSpeculativePreconnectRecordsRateLimitedFailureMetric(t *testing.T) {
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
	exec.scheduleSpeculativePreconnect(ctx, &cliproxyauth.Auth{ID: authID, Provider: "codex"}, authID, wsURL, http.Header{}, "root-session")

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if value, exists := ginCtx.Get("API_WEBSOCKET_TIMELINE"); exists {
			if timeline, ok := value.([]byte); ok && strings.Contains(string(timeline), `"name":"speculative_preconnect_failed"`) {
				got := string(timeline)
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
