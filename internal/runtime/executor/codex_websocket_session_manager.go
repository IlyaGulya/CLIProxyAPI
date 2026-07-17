package executor

import (
	"context"

	"github.com/gorilla/websocket"
)

// codexWebsocketSessionManager is the single ownership boundary for durable
// execution sessions and speculative upstream connections. Transport code may
// borrow a connection, but only the manager-owned pool/store retain it across
// requests.
type codexWebsocketSessionManager struct {
	store *codexWebsocketSessionStore
	pool  *codexWebsocketPreconnectPool
}

func newCodexWebsocketSessionManager(ctx context.Context, closeConnection func(*websocket.Conn) error) *codexWebsocketSessionManager {
	return &codexWebsocketSessionManager{
		store: newCodexWebsocketSessionStore(),
		pool:  newCodexWebsocketPreconnectPool(ctx, closeConnection),
	}
}

func (m *codexWebsocketSessionManager) remove(sessionID string) *codexWebsocketSession {
	if m == nil || m.store == nil {
		return nil
	}
	m.store.mu.Lock()
	session := m.store.sessions[sessionID]
	delete(m.store.sessions, sessionID)
	m.store.mu.Unlock()
	return session
}

func (m *codexWebsocketSessionManager) removeAll() []*codexWebsocketSession {
	if m == nil || m.store == nil {
		return nil
	}
	m.store.mu.Lock()
	sessions := make([]*codexWebsocketSession, 0, len(m.store.sessions))
	for sessionID, session := range m.store.sessions {
		delete(m.store.sessions, sessionID)
		if session != nil {
			sessions = append(sessions, session)
		}
	}
	m.store.mu.Unlock()
	return sessions
}
