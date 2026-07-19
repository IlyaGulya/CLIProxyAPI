package executor

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestCodexWebsocketsExecutor_SessionStoreIsInstanceOwnedAndDrained(t *testing.T) {
	sessionID := "test-session-store-survives-replace"

	exec1 := NewCodexWebsocketsExecutor(nil)
	sess1 := exec1.getOrCreateSession(sessionID)
	if sess1 == nil {
		t.Fatalf("expected session to be created")
	}

	exec2 := NewCodexWebsocketsExecutor(nil)
	sess2 := exec2.getOrCreateSession(sessionID)
	if sess2 == nil {
		t.Fatalf("expected session to be available across executors")
	}
	if sess1 == sess2 {
		t.Fatalf("independent executors unexpectedly shared a session")
	}

	exec1.CloseExecutionSession(cliproxyauth.CloseAllExecutionSessionsID)
	if got := exec1.getOrCreateSession("new-after-drain"); got != nil {
		t.Fatalf("drained executor created a new session: %#v", got)
	}
	exec1.sessions.store.mu.Lock()
	_, presentAfterDrain := exec1.sessions.store.sessions[sessionID]
	exec1.sessions.store.mu.Unlock()
	if presentAfterDrain {
		t.Fatalf("expected owned session to be removed by drain")
	}

	exec2.sessions.store.mu.Lock()
	_, secondPresent := exec2.sessions.store.sessions[sessionID]
	exec2.sessions.store.mu.Unlock()
	if !secondPresent {
		t.Fatalf("draining first executor affected second executor")
	}
	exec2.CloseExecutionSession(sessionID)
}

func TestCodexWebsocketsExecutor_DrainWaitsForInFlightSession(t *testing.T) {
	exec := NewCodexWebsocketsExecutor(nil)
	sess := exec.getOrCreateSession("in-flight")
	if sess == nil {
		t.Fatal("expected session")
	}
	sess.reqMu.Lock()

	drained := make(chan struct{})
	go func() {
		exec.CloseExecutionSession(cliproxyauth.CloseAllExecutionSessionsID)
		close(drained)
	}()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Fatal("drain blocked executor replacement")
	}

	exec.sessions.store.mu.Lock()
	_, present := exec.sessions.store.sessions["in-flight"]
	exec.sessions.store.mu.Unlock()
	if present {
		t.Fatal("draining runtime still exposed in-flight session")
	}
	sess.reqMu.Unlock()

	completed := make(chan struct{})
	go func() {
		exec.drainWG.Wait()
		close(completed)
	}()
	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("session drain did not finish after request released it")
	}
}

func TestCodexWebsocketsExecutor_EvictsExpiredIdleSessions(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketSessionTTLSeconds: 1}})
	exec.sessions.store = store

	expired := exec.getOrCreateSession("claude-code:expired")
	if expired == nil {
		t.Fatal("expected expired session fixture")
	}
	store.mu.Lock()
	expired.lastUsed = time.Now().Add(-2 * time.Second)
	store.mu.Unlock()

	if current := exec.getOrCreateSession("claude-code:current"); current == nil {
		t.Fatal("expected current session")
	}
	store.mu.Lock()
	_, expiredPresent := store.sessions["claude-code:expired"]
	_, currentPresent := store.sessions["claude-code:current"]
	store.mu.Unlock()
	if expiredPresent || !currentPresent {
		t.Fatalf("unexpected session store after TTL cleanup: expired=%t current=%t", expiredPresent, currentPresent)
	}
}

func TestCodexWebsocketsExecutor_BoundsIdleSessionStore(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketMaxSessions: 1}})
	exec.sessions.store = store

	first := exec.getOrCreateSession("claude-code:first")
	if first == nil {
		t.Fatal("expected first session")
	}
	store.mu.Lock()
	first.lastUsed = time.Now().Add(-time.Second)
	store.mu.Unlock()
	if second := exec.getOrCreateSession("claude-code:second"); second == nil {
		t.Fatal("expected second session after idle eviction")
	}

	store.mu.Lock()
	_, firstPresent := store.sessions["claude-code:first"]
	_, secondPresent := store.sessions["claude-code:second"]
	count := len(store.sessions)
	store.mu.Unlock()
	if firstPresent || !secondPresent || count != 1 {
		t.Fatalf("unexpected bounded store: first=%t second=%t count=%d", firstPresent, secondPresent, count)
	}
}

func TestCodexWebsocketsExecutor_DoesNotEvictActiveSessionAtCapacity(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketMaxSessions: 1}})
	exec.sessions.store = store

	active := exec.getOrCreateSession("claude-code:active")
	if active == nil {
		t.Fatal("expected active session fixture")
	}
	activateSessionFixture(t, active)

	if overflow := exec.getOrCreateSession("claude-code:overflow"); overflow != nil {
		t.Fatalf("expected one-shot fallback at active capacity, got %#v", overflow)
	}
	store.mu.Lock()
	_, activePresent := store.sessions["claude-code:active"]
	_, overflowPresent := store.sessions["claude-code:overflow"]
	store.mu.Unlock()
	if !activePresent || overflowPresent {
		t.Fatalf("active session eviction state: active=%t overflow=%t", activePresent, overflowPresent)
	}
}

func TestCodexWebsocketsExecutor_DoesNotApplyClaudeCapacityToNativeSessions(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketMaxSessions: 1}})
	exec.sessions.store = store

	if first := exec.getOrCreateSession("native-first"); first == nil {
		t.Fatal("expected first native session")
	}
	if second := exec.getOrCreateSession("native-second"); second == nil {
		t.Fatal("expected native sessions to retain their existing unbounded lifecycle")
	}
	if claude := exec.getOrCreateSession("claude-code:only-managed"); claude == nil {
		t.Fatal("native sessions must not consume Claude session capacity")
	}

	store.mu.Lock()
	count := len(store.sessions)
	store.mu.Unlock()
	if count != 3 {
		t.Fatalf("session count = %d, want 3", count)
	}
}

func TestCodexWebsocketIncrementalStateIsSizeBounded(t *testing.T) {
	sess := &codexWebsocketSession{}
	oversized := []byte(`{"model":"gpt-5.6-sol","instructions":"` + strings.Repeat("x", codexWebsocketMaxIncrementalStateBytes) + `","input":[]}`)

	got, observation := sess.prepareCodexIncrementalRequestObserved(oversized)
	if len(got) != len(oversized) {
		t.Fatalf("oversized request length = %d, want %d", len(got), len(oversized))
	}
	if len(sess.pendingRequest) != 0 || len(sess.lastRequest) != 0 || sess.lastResponseID != "" {
		t.Fatalf("oversized request was retained in incremental state: pending=%d last=%d response=%q", len(sess.pendingRequest), len(sess.lastRequest), sess.lastResponseID)
	}
	if observation.resetReason != "state_too_large" || observation.incremental {
		t.Fatalf("observation = %+v, want state_too_large non-incremental", observation)
	}
}

func TestCodexWebsocketIncrementalObservationReportsNoPreviousResponse(t *testing.T) {
	sess := &codexWebsocketSession{}
	request := []byte(`{"model":"gpt-5.6-sol","input":[]}`)
	got, observation := sess.prepareCodexIncrementalRequestObserved(request)
	if string(got) != string(request) {
		t.Fatalf("request changed without previous response: %s", got)
	}
	if observation.resetReason != "no_previous_response" || observation.incremental {
		t.Fatalf("observation = %+v, want no_previous_response", observation)
	}
}

func TestCodexWebsocketReactiveCompactStartsFreshResponseChain(t *testing.T) {
	sess := &codexWebsocketSession{
		lastRequest:        []byte(`{"model":"gpt-5.6-luna","input":[{"type":"function_call","call_id":"stale"}]}`),
		lastResponseID:     "resp-stale",
		lastResponseOutput: []byte(`[{"type":"function_call","call_id":"missing-result"}]`),
	}
	request := []byte(`{"model":"gpt-5.6-luna","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Your task is to create a detailed summary of the conversation so far. Before providing your final summary, wrap your analysis in <analysis> tags. Your entire response must be an <analysis> block followed by a <summary> block."}]}]}`)

	got, observation := sess.prepareCodexIncrementalRequestObserved(request)
	if observation.incremental || observation.resetReason != "reactive_compact" {
		t.Fatalf("observation = %+v", observation)
	}
	if gjson.GetBytes(got, "previous_response_id").Exists() {
		t.Fatalf("reactive compact reused stale response chain: %s", got)
	}
	if string(got) != string(request) {
		t.Fatalf("fresh compact request changed:\n got %s\nwant %s", got, request)
	}
}

func TestCodexIncrementalInputRequiresMatchingRequestProperties(t *testing.T) {
	input := `[{"type":"message","role":"user","content":[{"type":"input_text","text":"one"}]}]`
	previous := []byte(`{"model":"gpt-5.6-sol","input":` + input + `,"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}],"reasoning":{"effort":"medium"},"service_tier":"default","stream":true}`)
	previousOutput := []byte(`[]`)

	tests := []struct {
		name    string
		current string
	}{
		{name: "model", current: `{"model":"gpt-5.6-luna","input":` + input + `,"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}],"reasoning":{"effort":"medium"},"service_tier":"default","stream":true}`},
		{name: "tools", current: `{"model":"gpt-5.6-sol","input":` + input + `,"tools":[{"type":"function","name":"shell","parameters":{"type":"object","required":["command"]}}],"reasoning":{"effort":"medium"},"service_tier":"default","stream":true}`},
		{name: "reasoning", current: `{"model":"gpt-5.6-sol","input":` + input + `,"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}],"reasoning":{"effort":"high"},"service_tier":"default","stream":true}`},
		{name: "service tier", current: `{"model":"gpt-5.6-sol","input":` + input + `,"tools":[{"type":"function","name":"shell","parameters":{"type":"object"}}],"reasoning":{"effort":"medium"},"service_tier":"priority","stream":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if delta, ok := codexIncrementalInput(previous, previousOutput, []byte(tt.current)); ok {
				t.Fatalf("property change unexpectedly produced delta %s", delta)
			}
		})
	}
}

func TestCodexWebsocketsExecutor_RejectsOverflowAcrossFourHundredAgentSessions(t *testing.T) {
	const (
		maxSessions   = 32
		totalSessions = 400
	)
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketMaxSessions: maxSessions}})
	exec.sessions.store = store

	active := make([]*codexWebsocketSession, 0, maxSessions)
	for index := 0; index < maxSessions; index++ {
		sess := exec.getOrCreateSession("claude-code:active-" + strconv.Itoa(index))
		if sess == nil {
			t.Fatalf("active session %d was rejected below capacity", index)
		}
		activateSessionFixture(t, sess)
		active = append(active, sess)
	}

	var accepted atomic.Int32
	var wg sync.WaitGroup
	for index := maxSessions; index < totalSessions; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			if sess := exec.getOrCreateSession("claude-code:overflow-" + strconv.Itoa(index)); sess != nil {
				accepted.Add(1)
			}
		}(index)
	}
	wg.Wait()
	if got := accepted.Load(); got != 0 {
		t.Fatalf("accepted overflow sessions = %d, want 0", got)
	}
	store.mu.Lock()
	count := len(store.sessions)
	store.mu.Unlock()
	if count != maxSessions {
		t.Fatalf("stored sessions = %d, want %d", count, maxSessions)
	}
}

func activateSessionFixture(t *testing.T, session *codexWebsocketSession) {
	t.Helper()
	session.applyLifecycle(codexEventDialRequested)
	session.applyLifecycle(codexEventConnected)
	if err := session.setActive(make(chan codexWebsocketRead)); err != nil {
		t.Fatalf("activate session fixture: %v", err)
	}
	t.Cleanup(func() { _ = session.setActive(nil) })
}
