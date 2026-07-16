package executor

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

const (
	codexWebsocketPreconnectDefaultMaxIdle = 2
	codexWebsocketPreconnectDefaultTTL     = 30 * time.Second
	codexWebsocketPreconnect429Cooldown    = 30 * time.Second
)

type codexWebsocketPreconnectKey struct {
	authID string
	wsURL  string
}

type codexWebsocketPreconnectEntry struct {
	id        uint64
	conn      *websocket.Conn
	createdAt time.Time
}

type codexWebsocketPreconnectPool struct {
	mu             sync.Mutex
	idle           map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry
	dialing        int
	dialingByKey   map[codexWebsocketPreconnectKey]int
	changed        map[codexWebsocketPreconnectKey]chan struct{}
	cooldown       map[codexWebsocketPreconnectKey]time.Time
	authGeneration map[string]uint64
	nextID         uint64
}

var globalCodexWebsocketPreconnectPool = &codexWebsocketPreconnectPool{
	idle:           make(map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry),
	dialingByKey:   make(map[codexWebsocketPreconnectKey]int),
	changed:        make(map[codexWebsocketPreconnectKey]chan struct{}),
	cooldown:       make(map[codexWebsocketPreconnectKey]time.Time),
	authGeneration: make(map[string]uint64),
}

func (e *CodexWebsocketsExecutor) speculativePreconnectSettings() (bool, int, time.Duration) {
	if e == nil || e.cfg == nil || !e.cfg.CodexWebsocketSpeculativePreconnect {
		return false, 0, 0
	}
	maxIdle := e.cfg.CodexWebsocketPreconnectMaxIdle
	if maxIdle <= 0 {
		maxIdle = codexWebsocketPreconnectDefaultMaxIdle
	}
	ttl := codexWebsocketPreconnectDefaultTTL
	if e.cfg.CodexWebsocketPreconnectTTLSeconds > 0 {
		ttl = time.Duration(e.cfg.CodexWebsocketPreconnectTTLSeconds) * time.Second
	}
	return true, maxIdle, ttl
}

func codexAgentToolCallKey(payload []byte) (string, bool) {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType != "response.output_item.added" && eventType != "response.output_item.done" {
		return "", false
	}
	item := gjson.GetBytes(payload, "item")
	if strings.TrimSpace(item.Get("type").String()) != "function_call" || !strings.EqualFold(strings.TrimSpace(item.Get("name").String()), "Agent") {
		return "", false
	}
	key := strings.TrimSpace(item.Get("call_id").String())
	if key == "" {
		key = strings.TrimSpace(item.Get("id").String())
	}
	if key == "" {
		return "", false
	}
	return key, true
}

func (e *CodexWebsocketsExecutor) scheduleSpeculativePreconnect(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string) {
	enabled, maxIdle, ttl := e.speculativePreconnectSettings()
	if !enabled || auth == nil || strings.TrimSpace(authID) == "" || strings.TrimSpace(wsURL) == "" {
		return
	}
	key := codexWebsocketPreconnectKey{authID: strings.TrimSpace(authID), wsURL: strings.TrimSpace(wsURL)}
	reserved, reason, generation := globalCodexWebsocketPreconnectPool.reserve(key, maxIdle, time.Now())
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_triggered", map[string]any{
		"session_id": sessionID,
		"reserved":   reserved,
		"reason":     reason,
	})
	if !reserved {
		return
	}

	authCopy := auth.Clone()
	headerCopy := headers.Clone()
	go func() {
		dialCtx, cancel := context.WithTimeout(context.Background(), codexResponsesWebsocketHandshakeTO)
		defer cancel()
		startedAt := time.Now()
		conn, resp, errDial := e.dialCodexWebsocket(dialCtx, authCopy, key.wsURL, headerCopy)
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		closeHTTPResponseBody(resp, "codex websocket speculative preconnect: close handshake response body error")
		if errDial != nil || conn == nil {
			duration := time.Since(startedAt)
			globalCodexWebsocketPreconnectPool.failReservation(key, generation, status == http.StatusTooManyRequests, time.Now())
			helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_failed", map[string]any{
				"session_id":   sessionID,
				"duration_us":  duration.Microseconds(),
				"status":       status,
				"rate_limited": status == http.StatusTooManyRequests,
			})
			log.WithFields(log.Fields{
				"auth":        key.authID,
				"url":         key.wsURL,
				"duration_ms": duration.Milliseconds(),
				"status":      status,
			}).WithError(errDial).Debug("codex websockets: speculative preconnect failed")
			return
		}
		if !globalCodexWebsocketPreconnectPool.completeReservation(key, generation, conn, maxIdle, ttl, time.Now()) {
			helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_discarded", map[string]any{
				"session_id":  sessionID,
				"duration_us": time.Since(startedAt).Microseconds(),
			})
			_ = conn.Close()
			return
		}
		duration := time.Since(startedAt)
		helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_ready", map[string]any{
			"session_id":  sessionID,
			"duration_us": duration.Microseconds(),
		})
		log.WithFields(log.Fields{
			"auth":        key.authID,
			"url":         key.wsURL,
			"duration_ms": duration.Milliseconds(),
		}).Debug("codex websockets: speculative preconnect ready")
	}()
}

func (e *CodexWebsocketsExecutor) takeSpeculativePreconnect(ctx context.Context, authID string, wsURL string, sessionID string) *websocket.Conn {
	enabled, _, ttl := e.speculativePreconnectSettings()
	if !enabled {
		return nil
	}
	key := codexWebsocketPreconnectKey{authID: strings.TrimSpace(authID), wsURL: strings.TrimSpace(wsURL)}
	conn, age, ok := globalCodexWebsocketPreconnectPool.takeOrWait(ctx, key, ttl)
	if !ok {
		return nil
	}
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_leased", map[string]any{
		"session_id": sessionID,
		"age_us":     age.Microseconds(),
	})
	return conn
}

func (p *codexWebsocketPreconnectPool) reserve(key codexWebsocketPreconnectKey, maxIdle int, now time.Time) (bool, string, uint64) {
	if p == nil || maxIdle <= 0 {
		return false, "disabled", 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cooldown == nil {
		p.cooldown = make(map[codexWebsocketPreconnectKey]time.Time)
	}
	if p.authGeneration == nil {
		p.authGeneration = make(map[string]uint64)
	}
	if p.dialingByKey == nil {
		p.dialingByKey = make(map[codexWebsocketPreconnectKey]int)
	}
	generation := p.authGeneration[key.authID]
	if until := p.cooldown[key]; until.After(now) {
		return false, "cooldown", generation
	}
	if p.totalIdleLocked()+p.dialing >= maxIdle {
		return false, "capacity", generation
	}
	p.dialing++
	p.dialingByKey[key]++
	return true, "reserved", generation
}

func (p *codexWebsocketPreconnectPool) failReservation(key codexWebsocketPreconnectKey, generation uint64, rateLimited bool, now time.Time) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.finishReservationLocked(key)
	if rateLimited && p.authGeneration[key.authID] == generation {
		p.cooldown[key] = now.Add(codexWebsocketPreconnect429Cooldown)
	}
	p.mu.Unlock()
}

func (p *codexWebsocketPreconnectPool) completeReservation(key codexWebsocketPreconnectKey, generation uint64, conn *websocket.Conn, maxIdle int, ttl time.Duration, now time.Time) bool {
	if p == nil || conn == nil {
		return false
	}
	p.mu.Lock()
	p.finishReservationLocked(key)
	if p.authGeneration[key.authID] != generation || maxIdle <= 0 || p.totalIdleLocked() >= maxIdle {
		p.mu.Unlock()
		return false
	}
	p.nextID++
	entry := codexWebsocketPreconnectEntry{id: p.nextID, conn: conn, createdAt: now}
	if p.idle == nil {
		p.idle = make(map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry)
	}
	p.idle[key] = append(p.idle[key], entry)
	p.mu.Unlock()

	time.AfterFunc(ttl, func() {
		if expired := p.remove(key, entry.id); expired != nil {
			_ = expired.Close()
			log.WithFields(log.Fields{"auth": key.authID, "url": key.wsURL}).Debug("codex websockets: speculative preconnect expired")
		}
	})
	return true
}

func (p *codexWebsocketPreconnectPool) takeOrWait(ctx context.Context, key codexWebsocketPreconnectKey, ttl time.Duration) (*websocket.Conn, time.Duration, bool) {
	if p == nil {
		return nil, 0, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if conn, age, ok := p.take(key, ttl, time.Now()); ok {
			return conn, age, true
		}

		p.mu.Lock()
		if p.dialingByKey[key] <= 0 {
			p.mu.Unlock()
			// A completion can land between the optimistic take and the
			// in-flight check. Retake once before falling back to a cold dial.
			return p.take(key, ttl, time.Now())
		}
		if p.changed == nil {
			p.changed = make(map[codexWebsocketPreconnectKey]chan struct{})
		}
		changed := p.changed[key]
		if changed == nil {
			changed = make(chan struct{})
			p.changed[key] = changed
		}
		p.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, 0, false
		case <-changed:
		}
	}
}

func (p *codexWebsocketPreconnectPool) take(key codexWebsocketPreconnectKey, ttl time.Duration, now time.Time) (*websocket.Conn, time.Duration, bool) {
	if p == nil {
		return nil, 0, false
	}
	var expired []*websocket.Conn
	p.mu.Lock()
	entries := p.idle[key]
	for len(entries) > 0 {
		index := len(entries) - 1
		entry := entries[index]
		entries = entries[:index]
		if ttl > 0 && now.Sub(entry.createdAt) > ttl {
			expired = append(expired, entry.conn)
			continue
		}
		if len(entries) == 0 {
			delete(p.idle, key)
		} else {
			p.idle[key] = entries
		}
		p.mu.Unlock()
		for _, conn := range expired {
			_ = conn.Close()
		}
		return entry.conn, now.Sub(entry.createdAt), true
	}
	delete(p.idle, key)
	p.mu.Unlock()
	for _, conn := range expired {
		_ = conn.Close()
	}
	return nil, 0, false
}

func (p *codexWebsocketPreconnectPool) remove(key codexWebsocketPreconnectKey, id uint64) *websocket.Conn {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	entries := p.idle[key]
	for index := range entries {
		if entries[index].id != id {
			continue
		}
		conn := entries[index].conn
		entries = append(entries[:index], entries[index+1:]...)
		if len(entries) == 0 {
			delete(p.idle, key)
		} else {
			p.idle[key] = entries
		}
		return conn
	}
	return nil
}

func (p *codexWebsocketPreconnectPool) closeAuth(authID string) {
	authID = strings.TrimSpace(authID)
	if p == nil || authID == "" {
		return
	}
	var conns []*websocket.Conn
	p.mu.Lock()
	if p.authGeneration == nil {
		p.authGeneration = make(map[string]uint64)
	}
	p.authGeneration[authID]++
	for key, entries := range p.idle {
		if key.authID != authID {
			continue
		}
		for _, entry := range entries {
			conns = append(conns, entry.conn)
		}
		delete(p.idle, key)
	}
	for key := range p.cooldown {
		if key.authID == authID {
			delete(p.cooldown, key)
		}
	}
	for key := range p.dialingByKey {
		if key.authID == authID {
			delete(p.dialingByKey, key)
			p.notifyChangedLocked(key)
		}
	}
	p.mu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
}

func (p *codexWebsocketPreconnectPool) finishReservationLocked(key codexWebsocketPreconnectKey) {
	if p.dialing > 0 {
		p.dialing--
	}
	if count := p.dialingByKey[key]; count > 1 {
		p.dialingByKey[key] = count - 1
	} else {
		delete(p.dialingByKey, key)
	}
	p.notifyChangedLocked(key)
}

func (p *codexWebsocketPreconnectPool) notifyChangedLocked(key codexWebsocketPreconnectKey) {
	if changed := p.changed[key]; changed != nil {
		close(changed)
		delete(p.changed, key)
	}
}

func (p *codexWebsocketPreconnectPool) totalIdleLocked() int {
	total := 0
	for _, entries := range p.idle {
		total += len(entries)
	}
	return total
}
