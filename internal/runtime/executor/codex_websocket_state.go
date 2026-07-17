package executor

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type codexSessionState uint8

const (
	codexSessionIdle codexSessionState = iota
	codexSessionDialing
	codexSessionReady
	codexSessionBusy
	codexSessionDraining
	codexSessionClosed
)

func (s codexSessionState) String() string {
	switch s {
	case codexSessionIdle:
		return "idle"
	case codexSessionDialing:
		return "dialing"
	case codexSessionReady:
		return "ready"
	case codexSessionBusy:
		return "busy"
	case codexSessionDraining:
		return "draining"
	case codexSessionClosed:
		return "closed"
	default:
		return "unknown"
	}
}

var errCodexSessionTransition = errors.New("invalid codex websocket session transition")

type codexSessionTransition struct {
	From codexSessionState
	To   codexSessionState
	At   time.Time
}

type codexSessionStateMachine struct {
	mu    sync.Mutex
	value codexSessionState
	now   func() time.Time
}

func newCodexSessionStateMachine() *codexSessionStateMachine {
	return &codexSessionStateMachine{now: time.Now}
}

func (m *codexSessionStateMachine) state() codexSessionState {
	if m == nil {
		return codexSessionClosed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.value
}

func (m *codexSessionStateMachine) transition(next codexSessionState) (codexSessionTransition, error) {
	if m == nil {
		return codexSessionTransition{}, fmt.Errorf("%w: nil state machine", errCodexSessionTransition)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := m.value
	if !validCodexSessionTransition(previous, next) {
		return codexSessionTransition{}, fmt.Errorf("%w: %s -> %s", errCodexSessionTransition, previous, next)
	}
	m.value = next
	now := m.now
	if now == nil {
		now = time.Now
	}
	return codexSessionTransition{From: previous, To: next, At: now()}, nil
}

func validCodexSessionTransition(from, to codexSessionState) bool {
	switch from {
	case codexSessionIdle:
		return to == codexSessionDialing || to == codexSessionDraining || to == codexSessionClosed
	case codexSessionDialing:
		return to == codexSessionIdle || to == codexSessionReady || to == codexSessionDraining || to == codexSessionClosed
	case codexSessionReady:
		return to == codexSessionIdle || to == codexSessionBusy || to == codexSessionDialing || to == codexSessionDraining || to == codexSessionClosed
	case codexSessionBusy:
		return to == codexSessionIdle || to == codexSessionReady || to == codexSessionDialing || to == codexSessionDraining || to == codexSessionClosed
	case codexSessionDraining:
		return to == codexSessionClosed
	case codexSessionClosed:
		return false
	default:
		return false
	}
}
