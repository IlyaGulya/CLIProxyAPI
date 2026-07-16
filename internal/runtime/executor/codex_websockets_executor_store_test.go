package executor

import (
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestCodexWebsocketsExecutor_SessionStoreSurvivesExecutorReplacement(t *testing.T) {
	sessionID := "test-session-store-survives-replace"

	globalCodexWebsocketSessionStore.mu.Lock()
	delete(globalCodexWebsocketSessionStore.sessions, sessionID)
	globalCodexWebsocketSessionStore.mu.Unlock()

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
	if sess1 != sess2 {
		t.Fatalf("expected the same session instance across executors")
	}

	exec1.CloseExecutionSession(cliproxyauth.CloseAllExecutionSessionsID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, stillPresent := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if !stillPresent {
		t.Fatalf("expected session to remain after executor replacement close marker")
	}

	exec2.CloseExecutionSession(sessionID)

	globalCodexWebsocketSessionStore.mu.Lock()
	_, presentAfterClose := globalCodexWebsocketSessionStore.sessions[sessionID]
	globalCodexWebsocketSessionStore.mu.Unlock()
	if presentAfterClose {
		t.Fatalf("expected session to be removed after explicit close")
	}
}

func TestCodexWebsocketsExecutor_EvictsExpiredIdleSessions(t *testing.T) {
	store := &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	exec := NewCodexWebsocketsExecutor(&config.Config{SDKConfig: config.SDKConfig{CodexWebsocketSessionTTLSeconds: 1}})
	exec.store = store

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
	exec.store = store

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
	exec.store = store

	active := exec.getOrCreateSession("claude-code:active")
	if active == nil {
		t.Fatal("expected active session fixture")
	}
	active.setActive(make(chan codexWebsocketRead))
	t.Cleanup(func() { active.setActive(nil) })

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
	exec.store = store

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

	got := sess.prepareCodexIncrementalRequest(oversized)
	if len(got) != len(oversized) {
		t.Fatalf("oversized request length = %d, want %d", len(got), len(oversized))
	}
	if len(sess.pendingRequest) != 0 || len(sess.lastRequest) != 0 || sess.lastResponseID != "" {
		t.Fatalf("oversized request was retained in incremental state: pending=%d last=%d response=%q", len(sess.pendingRequest), len(sess.lastRequest), sess.lastResponseID)
	}
}

func TestCodexIncrementalInputRequiresMatchingRequestProperties(t *testing.T) {
	input := `[{"type":"message","role":"user","content":[{"type":"input_text","text":"one"}]}]`
	previous := []byte(`{"model":"gpt-5.6-sol","input":` + input + `,"tools":[],"reasoning":{"effort":"medium"},"service_tier":"default","stream":true}`)
	previousOutput := []byte(`[]`)

	tests := []struct {
		name    string
		current string
	}{
		{name: "model", current: `{"model":"gpt-5.6-luna","input":` + input + `,"tools":[],"reasoning":{"effort":"medium"},"service_tier":"default","stream":true}`},
		{name: "tools", current: `{"model":"gpt-5.6-sol","input":` + input + `,"tools":[{"type":"function","name":"shell"}],"reasoning":{"effort":"medium"},"service_tier":"default","stream":true}`},
		{name: "reasoning", current: `{"model":"gpt-5.6-sol","input":` + input + `,"tools":[],"reasoning":{"effort":"high"},"service_tier":"default","stream":true}`},
		{name: "service tier", current: `{"model":"gpt-5.6-sol","input":` + input + `,"tools":[],"reasoning":{"effort":"medium"},"service_tier":"priority","stream":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if delta, ok := codexIncrementalInput(previous, previousOutput, []byte(tt.current)); ok {
				t.Fatalf("property change unexpectedly produced delta %s", delta)
			}
		})
	}
}
