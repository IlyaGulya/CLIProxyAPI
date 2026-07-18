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
	return [...]string{"idle", "dialing", "ready", "busy", "draining", "closed"}[min(int(s), 5)]
}

type codexSessionEvent uint8

const (
	codexEventDialRequested codexSessionEvent = iota
	codexEventConnected
	codexEventRequestStarted
	codexEventSemanticFailed
	codexEventTransportFailed
	codexEventRequestFinished
	codexEventIdleExpired
	codexEventDrainRequested
	codexEventClosed
)

func (e codexSessionEvent) String() string {
	return [...]string{"dial_requested", "connected", "request_started", "semantic_failed", "transport_failed", "request_finished", "idle_expired", "drain_requested", "closed"}[min(int(e), 8)]
}

type codexSessionAction uint8

const (
	codexActionNone codexSessionAction = iota
	codexActionResetResponseChain
	codexActionCloseConnection
)

var errCodexSessionTransition = errors.New("invalid codex websocket session event")

type codexSessionTransition struct {
	From    codexSessionState
	To      codexSessionState
	Event   codexSessionEvent
	Actions []codexSessionAction
	At      time.Time
}

func reduceCodexSession(state codexSessionState, event codexSessionEvent) (codexSessionState, []codexSessionAction, error) {
	switch event {
	case codexEventDialRequested:
		if state == codexSessionIdle || state == codexSessionReady || state == codexSessionBusy {
			return codexSessionDialing, nil, nil
		}
	case codexEventConnected:
		if state == codexSessionDialing {
			return codexSessionReady, nil, nil
		}
	case codexEventRequestStarted:
		if state == codexSessionReady {
			return codexSessionBusy, nil, nil
		}
	case codexEventRequestFinished:
		if state == codexSessionBusy {
			return codexSessionReady, nil, nil
		}
	case codexEventSemanticFailed:
		if state == codexSessionBusy || state == codexSessionReady {
			return codexSessionIdle, []codexSessionAction{codexActionResetResponseChain, codexActionCloseConnection}, nil
		}
	case codexEventTransportFailed:
		if state == codexSessionDialing || state == codexSessionReady || state == codexSessionBusy {
			return codexSessionIdle, []codexSessionAction{codexActionResetResponseChain, codexActionCloseConnection}, nil
		}
	case codexEventIdleExpired:
		if state == codexSessionReady || state == codexSessionBusy {
			return codexSessionIdle, []codexSessionAction{codexActionResetResponseChain, codexActionCloseConnection}, nil
		}
	case codexEventDrainRequested:
		if state != codexSessionClosed && state != codexSessionDraining {
			return codexSessionDraining, []codexSessionAction{codexActionResetResponseChain, codexActionCloseConnection}, nil
		}
	case codexEventClosed:
		if state == codexSessionDraining {
			return codexSessionClosed, nil, nil
		}
	}
	return state, nil, fmt.Errorf("%w: %s + %s", errCodexSessionTransition, state, event)
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

func (m *codexSessionStateMachine) apply(event codexSessionEvent) (codexSessionTransition, error) {
	if m == nil {
		return codexSessionTransition{}, fmt.Errorf("%w: nil state machine", errCodexSessionTransition)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	next, actions, errReduce := reduceCodexSession(m.value, event)
	if errReduce != nil {
		return codexSessionTransition{}, errReduce
	}
	previous := m.value
	m.value = next
	now := m.now
	if now == nil {
		now = time.Now
	}
	return codexSessionTransition{From: previous, To: next, Event: event, Actions: actions, At: now()}, nil
}
