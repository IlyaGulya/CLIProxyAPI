package executor

import (
	"context"
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
	if request.session != nil {
		request.session.clearActive(request.currentRead)
		result.read = make(chan codexWebsocketRead, 4096)
	} else if request.currentConn != nil {
		if errClose := e.closeCodexConnection(request.currentConn); errClose != nil {
			log.Errorf("codex websockets executor: close websocket before retry error: %v", errClose)
		}
	}

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
	if errDial != nil || result.conn == nil {
		return result, errDial
	}
	if request.session != nil {
		request.session.setActive(result.read)
	}
	return result, nil
}
