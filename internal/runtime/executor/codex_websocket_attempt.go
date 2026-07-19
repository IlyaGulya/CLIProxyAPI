package executor

import (
	"context"
	"fmt"
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

type codexAttemptConnection struct {
	conn               *websocket.Conn
	source             codexWebsocketConnectionSource
	overflowBaseSource codexWebsocketConnectionSource
}

func connectCodexStreamAttempt(attempt *codexAttemptMachine, dial func(*codexAttemptConnection) error) (codexAttemptConnection, error) {
	result := codexAttemptConnection{}
	transition, err := attempt.apply(codexAttemptConnectRequested)
	if err == nil {
		err = runCodexAttemptActions(transition.Actions, codexAttemptActionHandlers{Dial: func() error { return dial(&result) }})
	}
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
	transition, err := attempt.apply(codexAttemptReaderActivated)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, nil
	}
	read := make(chan codexWebsocketRead, 4096)
	err = runCodexAttemptActions(transition.Actions, codexAttemptActionHandlers{
		ActivateReader: func() error { return session.setActive(read) },
	})
	if err != nil {
		return nil, err
	}
	return read, nil
}
