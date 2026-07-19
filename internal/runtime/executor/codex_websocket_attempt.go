package executor

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
)

func (e *CodexWebsocketsExecutor) newObservedCodexAttempt(ctx context.Context, sessionID, model string) *codexAttemptMachine {
	return newCodexAttemptMachine(func(transition codexAttemptTransition) {
		actions := make([]string, len(transition.Actions))
		for i, action := range transition.Actions {
			actions[i] = action.String()
		}
		helps.RecordAPIWebsocketEvent(ctx, e.cfg, "attempt_transition", observability.WebsocketAttributes{
			SessionID:        sessionID,
			Model:            model,
			AttemptStateFrom: observability.WebsocketAttemptState(transition.From.String()),
			AttemptStateTo:   observability.WebsocketAttemptState(transition.To.String()),
			AttemptEvent:     observability.WebsocketAttemptEvent(transition.Event.String()),
			AttemptActions:   strings.Join(actions, "."),
		}, nil)
	})
}

type codexConnectionLease struct {
	conn               *websocket.Conn
	response           *http.Response
	source             codexWebsocketConnectionSource
	overflowBaseSource codexWebsocketConnectionSource
}

func connectCodexStreamAttempt(attempt *codexAttemptMachine, dial func() (codexConnectionLease, error)) (codexConnectionLease, error) {
	result := codexConnectionLease{}
	_, err := attempt.dispatch(codexAttemptConnectRequested, codexAttemptActionHandlers{Dial: func() error {
		var errDial error
		result, errDial = dial()
		return errDial
	}})
	if err == nil && result.conn == nil {
		err = fmt.Errorf("codex websocket dial returned nil connection")
	}
	if err != nil {
		_, _ = attempt.apply(codexAttemptFailedEvent)
		return result, err
	}
	if _, err = attempt.apply(codexAttemptConnected); err != nil {
		return result, err
	}
	return result, nil
}

func activateCodexStreamReader(attempt *codexAttemptMachine, session *codexWebsocketSession) (chan codexWebsocketRead, error) {
	if session == nil {
		_, err := attempt.dispatch(codexAttemptReaderActivated, codexAttemptActionHandlers{ActivateReader: func() error { return nil }})
		return nil, err
	}
	read := make(chan codexWebsocketRead, 4096)
	_, err := attempt.dispatch(codexAttemptReaderActivated, codexAttemptActionHandlers{
		ActivateReader: func() error { return session.activateReader(read) },
	})
	if err != nil {
		return nil, err
	}
	return read, nil
}
