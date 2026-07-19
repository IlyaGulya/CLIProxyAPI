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
	codexConnectionLease
	read chan codexWebsocketRead
}

// reconnectCodexWebsocket owns the lifecycle-sensitive ordering shared by all
// retry paths: detach the old reader, establish a ready connection, then make
// the new reader active. Requests must not be sent before activation succeeds.
func (e *CodexWebsocketsExecutor) reconnectCodexWebsocket(request codexReconnectRequest) (codexReconnectResult, error) {
	result := codexReconnectResult{}
	handlers := codexAttemptActionHandlers{
		DetachReader: func() error {
			if request.session != nil {
				request.session.deactivateReader(request.currentRead)
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
			var lease codexConnectionLease
			var errDial error
			if request.sessionOverflow {
				lease, errDial = e.ensureOverflowConnObserved(
					request.ctx, request.auth, request.authID, request.url, request.headers, request.executionID,
				)
				lease.overflowBaseSource = lease.source
				lease.source = codexWebsocketConnectionOverflow
			} else {
				lease, errDial = e.ensureUpstreamConnObserved(
					request.ctx, request.auth, request.session, request.authID, request.url, request.headers,
				)
			}
			result.codexConnectionLease = lease
			return errDial
		},
		ActivateReader: func() error {
			if request.session != nil {
				return request.session.activateReader(result.read)
			}
			return nil
		},
	}
	if _, err := request.attempt.dispatch(codexAttemptInterrupted, handlers); err != nil {
		return result, err
	}
	if _, errTransition := request.attempt.dispatch(codexAttemptRetryApproved, handlers); errTransition != nil {
		return result, errTransition
	}
	if _, err := request.attempt.dispatch(codexAttemptConnectRequested, handlers); err != nil {
		_, _ = request.attempt.apply(codexAttemptFailedEvent)
		return result, err
	}
	if result.conn == nil {
		_, _ = request.attempt.apply(codexAttemptFailedEvent)
		return result, fmt.Errorf("codex websocket retry dial returned nil connection")
	}
	if _, err := request.attempt.apply(codexAttemptConnected); err != nil {
		return result, err
	}
	if _, err := request.attempt.dispatch(codexAttemptRecoveryPrepared, handlers); err != nil {
		return result, err
	}
	if _, err := request.attempt.dispatch(codexAttemptReaderActivated, handlers); err != nil {
		return result, err
	}
	return result, nil
}

func sendCodexAttemptRequest(attempt *codexAttemptMachine, send func() error) error {
	_, err := attempt.dispatch(codexAttemptRequestSent, codexAttemptActionHandlers{SendRequest: send})
	return err
}
