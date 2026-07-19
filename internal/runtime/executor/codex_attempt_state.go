package executor

import (
	"errors"
	"fmt"
	"sync"
)

type codexAttemptState uint8

const (
	codexAttemptPrepared codexAttemptState = iota
	codexAttemptConnecting
	codexAttemptReady
	codexAttemptActive
	codexAttemptInterruptedState
	codexAttemptRetrying
	codexAttemptCompleted
	codexAttemptFailed
	codexAttemptCancelled
)

func (s codexAttemptState) String() string {
	return [...]string{"prepared", "connecting", "ready", "active", "interrupted", "retrying", "completed", "failed", "cancelled"}[min(int(s), 8)]
}

type codexAttemptEvent uint8

const (
	codexAttemptConnectRequested codexAttemptEvent = iota
	codexAttemptConnected
	codexAttemptReaderActivated
	codexAttemptRequestSent
	codexAttemptInterrupted
	codexAttemptRetryApproved
	codexAttemptTerminalReceived
	codexAttemptFailedEvent
	codexAttemptCancelledEvent
)

func (e codexAttemptEvent) String() string {
	return [...]string{"connect_requested", "connected", "reader_activated", "request_sent", "interrupted", "retry_approved", "terminal_received", "failed", "cancelled"}[min(int(e), 8)]
}

type codexAttemptAction uint8

const (
	codexAttemptDial codexAttemptAction = iota
	codexAttemptActivateReader
	codexAttemptSendRequest
	codexAttemptDetachReader
	codexAttemptCloseConnection
	codexAttemptDiscardBuffer
	codexAttemptResetSemantics
)

func (a codexAttemptAction) String() string {
	return [...]string{"dial", "activate_reader", "send_request", "detach_reader", "close_connection", "discard_buffer", "reset_semantics"}[min(int(a), 6)]
}

type codexAttemptTransition struct {
	From    codexAttemptState
	To      codexAttemptState
	Event   codexAttemptEvent
	Actions []codexAttemptAction
}

var errCodexAttemptTransition = errors.New("invalid codex attempt transition")

func reduceCodexAttempt(state codexAttemptState, event codexAttemptEvent) (codexAttemptTransition, error) {
	transition := codexAttemptTransition{From: state, To: state, Event: event}
	switch event {
	case codexAttemptConnectRequested:
		if state == codexAttemptPrepared || state == codexAttemptRetrying {
			transition.To = codexAttemptConnecting
			transition.Actions = []codexAttemptAction{codexAttemptDial}
			return transition, nil
		}
	case codexAttemptConnected:
		if state == codexAttemptConnecting {
			transition.To = codexAttemptReady
			return transition, nil
		}
	case codexAttemptReaderActivated:
		if state == codexAttemptReady {
			transition.Actions = []codexAttemptAction{codexAttemptActivateReader}
			return transition, nil
		}
	case codexAttemptRequestSent:
		if state == codexAttemptReady {
			transition.To = codexAttemptActive
			transition.Actions = []codexAttemptAction{codexAttemptSendRequest}
			return transition, nil
		}
	case codexAttemptInterrupted:
		if state == codexAttemptActive {
			transition.To = codexAttemptInterruptedState
			transition.Actions = []codexAttemptAction{codexAttemptDetachReader, codexAttemptCloseConnection}
			return transition, nil
		}
	case codexAttemptRetryApproved:
		if state == codexAttemptInterruptedState {
			transition.To = codexAttemptRetrying
			transition.Actions = []codexAttemptAction{codexAttemptDiscardBuffer, codexAttemptResetSemantics}
			return transition, nil
		}
	case codexAttemptTerminalReceived:
		if state == codexAttemptActive {
			transition.To = codexAttemptCompleted
			return transition, nil
		}
	case codexAttemptFailedEvent:
		if !state.terminal() {
			transition.To = codexAttemptFailed
			transition.Actions = cleanupCodexAttemptActions(state)
			return transition, nil
		}
	case codexAttemptCancelledEvent:
		if !state.terminal() {
			transition.To = codexAttemptCancelled
			transition.Actions = cleanupCodexAttemptActions(state)
			return transition, nil
		}
	}
	return transition, fmt.Errorf("%w: %s + %s", errCodexAttemptTransition, state, event)
}

func (s codexAttemptState) terminal() bool {
	return s == codexAttemptCompleted || s == codexAttemptFailed || s == codexAttemptCancelled
}

func cleanupCodexAttemptActions(state codexAttemptState) []codexAttemptAction {
	if state == codexAttemptActive || state == codexAttemptInterruptedState {
		return []codexAttemptAction{codexAttemptDetachReader, codexAttemptCloseConnection, codexAttemptDiscardBuffer}
	}
	return []codexAttemptAction{codexAttemptDiscardBuffer}
}

type codexAttemptMachine struct {
	mu       sync.Mutex
	state    codexAttemptState
	observer func(codexAttemptTransition)
}

func newCodexAttemptMachine(observers ...func(codexAttemptTransition)) *codexAttemptMachine {
	var observer func(codexAttemptTransition)
	if len(observers) > 0 {
		observer = observers[0]
	}
	return &codexAttemptMachine{state: codexAttemptPrepared, observer: observer}
}

func (m *codexAttemptMachine) apply(event codexAttemptEvent) (codexAttemptTransition, error) {
	if m == nil {
		return codexAttemptTransition{}, fmt.Errorf("%w: nil machine", errCodexAttemptTransition)
	}
	m.mu.Lock()
	transition, err := reduceCodexAttempt(m.state, event)
	if err != nil {
		m.mu.Unlock()
		return transition, err
	}
	m.state = transition.To
	observer := m.observer
	m.mu.Unlock()
	if observer != nil {
		observer(transition)
	}
	return transition, nil
}

func (m *codexAttemptMachine) current() codexAttemptState {
	if m == nil {
		return codexAttemptFailed
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

type codexAttemptActionHandlers struct {
	Dial            func() error
	ActivateReader  func() error
	SendRequest     func() error
	DetachReader    func() error
	CloseConnection func() error
	DiscardBuffer   func() error
	ResetSemantics  func() error
}

func runCodexAttemptActions(actions []codexAttemptAction, handlers codexAttemptActionHandlers) error {
	for _, action := range actions {
		var handler func() error
		switch action {
		case codexAttemptDial:
			handler = handlers.Dial
		case codexAttemptActivateReader:
			handler = handlers.ActivateReader
		case codexAttemptSendRequest:
			handler = handlers.SendRequest
		case codexAttemptDetachReader:
			handler = handlers.DetachReader
		case codexAttemptCloseConnection:
			handler = handlers.CloseConnection
		case codexAttemptDiscardBuffer:
			handler = handlers.DiscardBuffer
		case codexAttemptResetSemantics:
			handler = handlers.ResetSemantics
		}
		if handler == nil {
			return fmt.Errorf("codex attempt action %s has no handler", action)
		}
		if err := handler(); err != nil {
			return fmt.Errorf("codex attempt action %s: %w", action, err)
		}
	}
	return nil
}
