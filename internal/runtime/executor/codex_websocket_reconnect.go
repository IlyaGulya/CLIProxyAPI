package executor

import (
	"context"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

type codexReconnectRoute struct {
	ctx         context.Context
	auth        *cliproxyauth.Auth
	session     *codexWebsocketSession
	authID      string
	url         string
	headers     http.Header
	executionID string
	overflow    bool
}

type codexPreviousAttempt struct {
	conn *websocket.Conn
	read chan codexWebsocketRead
}

type codexRecoveryState interface {
	discardBuffer() error
	resetSemantics() error
}

type codexNoopRecoveryState struct{}

func (codexNoopRecoveryState) discardBuffer() error  { return nil }
func (codexNoopRecoveryState) resetSemantics() error { return nil }

type codexTransactionalRecoveryState struct {
	discard func() error
	reset   func() error
}

func (r codexTransactionalRecoveryState) discardBuffer() error {
	if r.discard == nil {
		return fmt.Errorf("codex recovery discard collaborator is nil")
	}
	return r.discard()
}

func (r codexTransactionalRecoveryState) resetSemantics() error {
	if r.reset == nil {
		return fmt.Errorf("codex recovery semantic reset collaborator is nil")
	}
	return r.reset()
}

type codexReconnectRequest struct {
	route    codexReconnectRoute
	previous codexPreviousAttempt
	attempt  *codexAttemptMachine
	recovery codexRecoveryState
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
	if request.recovery == nil {
		return result, fmt.Errorf("codex reconnect recovery state is nil")
	}
	handlers := codexAttemptActionSet{
		bindCodexAttemptAction(codexAttemptDetachReader, func() error {
			if request.route.session != nil {
				request.route.session.deactivateReader(request.previous.read)
				result.read = make(chan codexWebsocketRead, 4096)
			}
			return nil
		}),
		bindCodexAttemptAction(codexAttemptCloseConnection, func() error {
			if request.route.session == nil && request.previous.conn != nil {
				if errClose := e.closeCodexConnection(request.previous.conn); errClose != nil {
					log.Errorf("codex websockets executor: close websocket before retry error: %v", errClose)
				}
			}
			return nil
		}),
		bindCodexAttemptAction(codexAttemptDiscardBuffer, func() error {
			return request.recovery.discardBuffer()
		}),
		bindCodexAttemptAction(codexAttemptResetSemantics, func() error {
			return request.recovery.resetSemantics()
		}),
		bindCodexAttemptAction(codexAttemptDial, func() error {
			var lease codexConnectionLease
			var errDial error
			if request.route.overflow {
				lease, errDial = e.ensureOverflowConnObserved(
					request.route.ctx, request.route.auth, request.route.authID, request.route.url, request.route.headers, request.route.executionID,
				)
				lease.overflowBaseSource = lease.source
				lease.source = codexWebsocketConnectionOverflow
			} else {
				lease, errDial = e.ensureUpstreamConnObserved(
					request.route.ctx, request.route.auth, request.route.session, request.route.authID, request.route.url, request.route.headers,
				)
			}
			result.codexConnectionLease = lease
			return errDial
		}),
		bindCodexAttemptAction(codexAttemptActivateReader, func() error {
			if request.route.session != nil {
				return request.route.session.activateReader(result.read)
			}
			return nil
		}),
	}
	if _, err := request.attempt.dispatch(codexAttemptInterrupted, handlers); err != nil {
		return result, err
	}
	if _, errTransition := request.attempt.dispatch(codexAttemptRetryApproved, handlers); errTransition != nil {
		return result, errTransition
	}
	if _, err := request.attempt.dispatch(codexAttemptConnectRequested, handlers); err != nil {
		_, _ = request.attempt.commitEvent(codexAttemptFailedEvent)
		return result, err
	}
	if result.conn == nil {
		_, _ = request.attempt.commitEvent(codexAttemptFailedEvent)
		return result, fmt.Errorf("codex websocket retry dial returned nil connection")
	}
	if _, err := request.attempt.commitEvent(codexAttemptConnected); err != nil {
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
	_, err := attempt.dispatch(codexAttemptRequestSent, codexAttemptActionSet{bindCodexAttemptAction(codexAttemptSendRequest, send)})
	return err
}
