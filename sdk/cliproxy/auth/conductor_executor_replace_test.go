package auth

import (
	"context"
	"net/http"
	"sync"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

type replaceAwareExecutor struct {
	id string

	mu               sync.Mutex
	closedSessionIDs []string
	closedAuthIDs    []string
	closeReasons     []string
}

func (e *replaceAwareExecutor) Identifier() string {
	return e.id
}

func (e *replaceAwareExecutor) Execute(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *replaceAwareExecutor) ExecuteStream(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	ch := make(chan cliproxyexecutor.StreamChunk)
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}

func (e *replaceAwareExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) {
	return auth, nil
}

func (e *replaceAwareExecutor) CountTokens(context.Context, *Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return cliproxyexecutor.Response{}, nil
}

func (e *replaceAwareExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *replaceAwareExecutor) CloseExecutionSession(sessionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closedSessionIDs = append(e.closedSessionIDs, sessionID)
}

func (e *replaceAwareExecutor) ClosedSessionIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.closedSessionIDs))
	copy(out, e.closedSessionIDs)
	return out
}

func (e *replaceAwareExecutor) CloseAuthExecutionSessions(authID, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closedAuthIDs = append(e.closedAuthIDs, authID)
	e.closeReasons = append(e.closeReasons, reason)
}

func TestManagerRegisterExecutorClosesReplacedExecutionSessions(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, nil, nil)
	replaced := &replaceAwareExecutor{id: "codex"}
	current := &replaceAwareExecutor{id: "codex"}

	manager.RegisterExecutor(replaced)
	manager.RegisterExecutor(current)

	closed := replaced.ClosedSessionIDs()
	if len(closed) != 1 {
		t.Fatalf("expected replaced executor close calls = 1, got %d", len(closed))
	}
	if closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("expected close marker %q, got %q", CloseAllExecutionSessionsID, closed[0])
	}
	if len(current.ClosedSessionIDs()) != 0 {
		t.Fatalf("expected current executor to stay open")
	}
}

func TestManagerExecutorReturnsRegisteredExecutor(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, nil, nil)
	current := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(current)

	resolved, okResolved := manager.Executor("CODEX")
	if !okResolved {
		t.Fatal("expected registered executor to be found")
	}
	resolvedExecutor, okResolvedExecutor := resolved.(*replaceAwareExecutor)
	if !okResolvedExecutor {
		t.Fatalf("expected resolved executor type %T, got %T", current, resolved)
	}
	if resolvedExecutor != current {
		t.Fatal("expected resolved executor to match registered executor")
	}

	_, okMissing := manager.Executor("unknown")
	if okMissing {
		t.Fatal("expected unknown provider lookup to fail")
	}
}

func TestManagerCloseAuthExecutionSessionsUsesRegisteredExecutorOwnership(t *testing.T) {
	t.Parallel()

	manager := NewManager(nil, nil, nil)
	exec := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(exec)
	manager.CloseAuthExecutionSessions(" auth-a ", "auth_removed")

	exec.mu.Lock()
	defer exec.mu.Unlock()
	if len(exec.closedAuthIDs) != 1 || exec.closedAuthIDs[0] != "auth-a" {
		t.Fatalf("closed auth IDs = %#v, want [auth-a]", exec.closedAuthIDs)
	}
	if len(exec.closeReasons) != 1 || exec.closeReasons[0] != "auth_removed" {
		t.Fatalf("close reasons = %#v, want [auth_removed]", exec.closeReasons)
	}
}

func TestManagerSchedulerObserverIsInjectedAndTransportNeutral(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	observed := make(chan SchedulerSelectionObservation, 1)
	manager.SetSchedulerSelectionObserver(func(_ context.Context, event SchedulerSelectionObservation) {
		observed <- event
	})
	_, _ = manager.SelectAuth(context.Background(), "codex", "gpt-test", cliproxyexecutor.Options{})
	select {
	case event := <-observed:
		if event.Model != "gpt-test" || event.Duration < 0 || event.Success {
			t.Fatalf("unexpected observation: %#v", event)
		}
	default:
		t.Fatal("scheduler observer was not called")
	}
}
