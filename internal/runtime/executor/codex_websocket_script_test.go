package executor

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gorilla/websocket"
)

type codexWebsocketScriptStep struct {
	payload    string
	disconnect bool
	wait       <-chan struct{}
}

func codexSend(payload string) codexWebsocketScriptStep {
	return codexWebsocketScriptStep{payload: payload}
}

func codexDisconnect1006() codexWebsocketScriptStep {
	return codexWebsocketScriptStep{disconnect: true}
}

func codexWait(release <-chan struct{}) codexWebsocketScriptStep {
	return codexWebsocketScriptStep{wait: release}
}

type codexScriptedUpstream struct {
	Server      *httptest.Server
	connections atomic.Int32
}

func newCodexScriptedUpstream(t *testing.T, attempts ...[]codexWebsocketScriptStep) *codexScriptedUpstream {
	t.Helper()
	upstream := &codexScriptedUpstream{}
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade websocket: %v", err)
			return
		}
		attempt := int(upstream.connections.Add(1)) - 1
		defer func() { _ = conn.Close() }()
		if attempt >= len(attempts) {
			t.Errorf("unexpected websocket attempt %d", attempt+1)
			return
		}
		if _, _, err = conn.ReadMessage(); err != nil {
			t.Errorf("read websocket request for attempt %d: %v", attempt+1, err)
			return
		}
		for stepIndex, step := range attempts[attempt] {
			if step.wait != nil {
				<-step.wait
			}
			if step.payload != "" {
				if err = conn.WriteMessage(websocket.TextMessage, []byte(step.payload)); err != nil {
					t.Errorf("write websocket attempt %d step %d: %v", attempt+1, stepIndex+1, err)
					return
				}
			}
			if step.disconnect {
				_ = conn.UnderlyingConn().Close()
				return
			}
		}
	}))
	t.Cleanup(upstream.Server.Close)
	return upstream
}

func (u *codexScriptedUpstream) URL() string { return u.Server.URL }
func (u *codexScriptedUpstream) Connections() int32 {
	return u.connections.Load()
}
