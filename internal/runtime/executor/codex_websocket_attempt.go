package executor

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
)

func (e *CodexWebsocketsExecutor) newObservedCodexAttempt(ctx context.Context, sessionID, model string) *codexAttemptMachine {
	return newCodexAttemptMachine(func(transition codexAttemptTransition) {
		helps.RecordAPIWebsocketEvent(ctx, e.cfg, "attempt_transition", observability.WebsocketAttributes{
			SessionID:        sessionID,
			Model:            model,
			AttemptStateFrom: observability.WebsocketAttemptState(transition.From.String()),
			AttemptStateTo:   observability.WebsocketAttemptState(transition.To.String()),
			AttemptEvent:     observability.WebsocketAttemptEvent(transition.Event.String()),
		}, nil)
		for _, action := range transition.Actions {
			helps.RecordAPIWebsocketEvent(ctx, e.cfg, "attempt_action", observability.WebsocketAttributes{
				SessionID: sessionID, Model: model, Reason: action.String(),
				AttemptEvent: observability.WebsocketAttemptEvent(transition.Event.String()),
			}, nil)
		}
	})
}

type codexConnectionLease struct {
	conn               *websocket.Conn
	response           *http.Response
	source             codexWebsocketConnectionSource
	overflowBaseSource codexWebsocketConnectionSource
}

func (l codexConnectionLease) validate() error {
	if l.conn == nil {
		return fmt.Errorf("codex websocket connection lease has no connection")
	}
	if l.source == "" {
		return fmt.Errorf("codex websocket connection lease has no source")
	}
	return nil
}

func (l *codexConnectionLease) takeHandshakeResponse() *http.Response {
	if l == nil {
		return nil
	}
	response := l.response
	l.response = nil
	return response
}

func connectCodexStreamAttempt(attempt *codexAttemptMachine, dial func() (codexConnectionLease, error)) (codexConnectionLease, error) {
	result := codexConnectionLease{}
	_, err := attempt.dispatch(codexAttemptConnectRequested, codexAttemptActionSet{bindCodexAttemptAction(codexAttemptDial, func() error {
		var errDial error
		result, errDial = dial()
		return errDial
	})})
	if err == nil {
		err = result.validate()
	}
	if err != nil {
		_, _ = attempt.commitEvent(codexAttemptFailedEvent)
		return result, err
	}
	if _, err = attempt.commitEvent(codexAttemptConnected); err != nil {
		return result, err
	}
	return result, nil
}

func activateCodexStreamReader(attempt *codexAttemptMachine, session *codexWebsocketSession) (chan codexWebsocketRead, error) {
	if session == nil {
		_, err := attempt.dispatch(codexAttemptReaderActivated, codexAttemptActionSet{bindCodexAttemptAction(codexAttemptActivateReader, func() error { return nil })})
		return nil, err
	}
	read := make(chan codexWebsocketRead, 4096)
	_, err := attempt.dispatch(codexAttemptReaderActivated, codexAttemptActionSet{bindCodexAttemptAction(codexAttemptActivateReader, func() error { return session.activateReader(read) })})
	if err != nil {
		return nil, err
	}
	if err = validateCodexReaderLifecycle(attempt, session); err != nil {
		session.deactivateReader(read)
		return nil, err
	}
	return read, nil
}

func validateCodexReaderLifecycle(attempt *codexAttemptMachine, session *codexWebsocketSession) error {
	if attempt == nil || session == nil {
		return nil
	}
	reader, _ := session.activeReaderSnapshot()
	hasReader := reader != nil
	state, sessionState := attempt.current(), session.lifecycle.state()
	if !codexLifecycleProjectionValid(state, sessionState, hasReader) {
		return fmt.Errorf("codex lifecycle invariant: attempt=%s session=%s active_reader=%t", state, sessionState, hasReader)
	}
	return nil
}

func codexLifecycleProjectionValid(attempt codexAttemptState, session codexSessionState, hasReader bool) bool {
	switch attempt {
	case codexAttemptPrepared:
		return !hasReader && (session == codexSessionIdle || session == codexSessionReady)
	case codexAttemptConnecting:
		return !hasReader && (session == codexSessionIdle || session == codexSessionDialing || session == codexSessionReady)
	case codexAttemptReady:
		return !hasReader && session == codexSessionReady
	case codexAttemptReaderReady, codexAttemptActive, codexAttemptCompleted:
		return hasReader && session == codexSessionBusy
	case codexAttemptInterruptedState, codexAttemptRetrying:
		return !hasReader && (session == codexSessionIdle || session == codexSessionReady)
	case codexAttemptFailed, codexAttemptCancelled:
		return true
	default:
		return false
	}
}
