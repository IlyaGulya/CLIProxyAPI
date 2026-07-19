package executor

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

type codexReconnectRequest struct {
	ctx             context.Context
	auth            *cliproxyauth.Auth
	session         *codexWebsocketSession
	currentConn     *websocket.Conn
	currentRead     chan codexWebsocketRead
	authID          string
	url             string
	headers         http.Header
	executionID     string
	sessionOverflow bool
	attempt         *codexAttemptMachine
	discardBuffer   func() error
	resetSemantics  func() error
}

type codexReconnectResult struct {
	conn     *websocket.Conn
	response *http.Response
	source   codexWebsocketConnectionSource
	read     chan codexWebsocketRead
}

// reconnectCodexWebsocket owns the lifecycle-sensitive ordering shared by all
// retry paths: detach the old reader, establish a ready connection, then make
// the new reader active. Requests must not be sent before activation succeeds.
func (e *CodexWebsocketsExecutor) reconnectCodexWebsocket(request codexReconnectRequest) (codexReconnectResult, error) {
	result := codexReconnectResult{}
	interrupted, errTransition := request.attempt.apply(codexAttemptInterrupted)
	if errTransition != nil {
		return result, errTransition
	}
	handlers := codexAttemptActionHandlers{
		DetachReader: func() error {
			if request.session != nil {
				request.session.clearActive(request.currentRead)
				result.read = make(chan codexWebsocketRead, 4096)
			}
			return nil
		},
		CloseConnection: func() error {
			if request.session == nil && request.currentConn != nil {
				if errClose := e.closeCodexConnection(request.currentConn); errClose != nil {
					log.Errorf("codex websockets executor: close websocket before retry error: %v", errClose)
				}
			}
			return nil
		},
		DiscardBuffer: func() error {
			if request.discardBuffer != nil {
				return request.discardBuffer()
			}
			return nil
		},
		ResetSemantics: func() error {
			if request.resetSemantics != nil {
				return request.resetSemantics()
			}
			return nil
		},
		Dial: func() error {
			var errDial error
			if request.sessionOverflow {
				result.conn, result.response, _, errDial = e.ensureOverflowConnObserved(
					request.ctx, request.auth, request.authID, request.url, request.headers, request.executionID,
				)
				result.source = codexWebsocketConnectionOverflow
			} else {
				result.conn, result.response, result.source, errDial = e.ensureUpstreamConnObserved(
					request.ctx, request.auth, request.session, request.authID, request.url, request.headers,
				)
			}
			return errDial
		},
		ActivateReader: func() error {
			if request.session != nil {
				return request.session.setActive(result.read)
			}
			return nil
		},
	}
	if err := runCodexAttemptActions(interrupted.Actions, handlers); err != nil {
		return result, err
	}
	retrying, errTransition := request.attempt.apply(codexAttemptRetryApproved)
	if errTransition != nil {
		return result, errTransition
	}
	connecting, errTransition := request.attempt.apply(codexAttemptConnectRequested)
	if errTransition != nil {
		return result, errTransition
	}
	if err := runCodexAttemptActions(connecting.Actions, handlers); err != nil {
		_, _ = request.attempt.apply(codexAttemptFailedEvent)
		return result, err
	}
	if result.conn == nil {
		_, _ = request.attempt.apply(codexAttemptFailedEvent)
		return result, fmt.Errorf("codex websocket retry dial returned nil connection")
	}
	if _, errTransition = request.attempt.apply(codexAttemptConnected); errTransition != nil {
		return result, errTransition
	}
	if err := runCodexAttemptActions(retrying.Actions, handlers); err != nil {
		return result, err
	}
	activated, errTransition := request.attempt.apply(codexAttemptReaderActivated)
	if errTransition != nil {
		return result, errTransition
	}
	if err := runCodexAttemptActions(activated.Actions, handlers); err != nil {
		return result, err
	}
	return result, nil
}

func sendCodexAttemptRequest(attempt *codexAttemptMachine, send func() error) error {
	transition, errTransition := attempt.apply(codexAttemptRequestSent)
	if errTransition != nil {
		return errTransition
	}
	return runCodexAttemptActions(transition.Actions, codexAttemptActionHandlers{SendRequest: send})
}
