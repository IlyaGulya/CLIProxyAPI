package executor

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func buildCodexGenerateFalseWarmupRequest(template []byte) ([]byte, error) {
	if !gjson.ValidBytes(template) {
		return nil, fmt.Errorf("codex websocket warmup: invalid request template")
	}
	request := bytes.Clone(template)
	updates := []struct {
		path  string
		value any
	}{
		{path: "type", value: "response.create"},
		{path: "generate", value: false},
		{path: "input", value: []any{}},
		{path: "tools", value: []any{}},
		{path: "tool_choice", value: "auto"},
		{path: "instructions", value: ""},
	}
	var errSet error
	for _, update := range updates {
		request, errSet = sjson.SetBytes(request, update.path, update.value)
		if errSet != nil {
			return nil, fmt.Errorf("codex websocket warmup: set %s: %w", update.path, errSet)
		}
	}
	request, errSet = sjson.DeleteBytes(request, "previous_response_id")
	if errSet != nil {
		return nil, fmt.Errorf("codex websocket warmup: clear previous response: %w", errSet)
	}
	return request, nil
}

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

type codexWebsocketPreconnectObservation struct {
	wait    time.Duration
	reason  string
	idle    int
	dialing int
}

type codexWebsocketPreconnectPool struct {
	mu             sync.Mutex
	closed         bool
	idle           map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry
	dialing        int
	dialingByKey   map[codexWebsocketPreconnectKey]int
	changed        map[codexWebsocketPreconnectKey]chan struct{}
	cooldown       map[codexWebsocketPreconnectKey]time.Time
	authGeneration map[string]uint64
	nextID         uint64
	ctx            context.Context
	wg             sync.WaitGroup
}

func newCodexWebsocketPreconnectPool(ctx context.Context) *codexWebsocketPreconnectPool {
	if ctx == nil {
		ctx = context.Background()
	}
	return &codexWebsocketPreconnectPool{
		idle:           make(map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry),
		dialingByKey:   make(map[codexWebsocketPreconnectKey]int),
		changed:        make(map[codexWebsocketPreconnectKey]chan struct{}),
		cooldown:       make(map[codexWebsocketPreconnectKey]time.Time),
		authGeneration: make(map[string]uint64),
		ctx:            ctx,
	}
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

func (e *CodexWebsocketsExecutor) scheduleSpeculativePreconnect(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string, warmupTemplate []byte) {
	e.scheduleSpeculativePreconnectForTrigger(ctx, auth, authID, wsURL, headers, sessionID, warmupTemplate, "agent_tool")
}

func (e *CodexWebsocketsExecutor) scheduleSpeculativePreconnectForTrigger(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string, warmupTemplate []byte, trigger string) {
	enabled, maxIdle, ttl := e.speculativePreconnectSettings()
	if !enabled || auth == nil || strings.TrimSpace(authID) == "" || strings.TrimSpace(wsURL) == "" {
		return
	}
	key := codexWebsocketPreconnectKey{authID: strings.TrimSpace(authID), wsURL: strings.TrimSpace(wsURL)}
	reserved, reason, generation := e.pool.reserve(key, maxIdle, time.Now())
	poolIdle, poolDialing := e.pool.snapshot()
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_triggered", map[string]any{
		"session_id":            sessionID,
		"reserved":              reserved,
		"reason":                reason,
		"trigger":               trigger,
		"generate_false_warmup": e.cfg.CodexWebsocketGenerateFalseWarmup,
		"pool_idle":             poolIdle,
		"pool_dialing":          poolDialing,
	})
	if !reserved {
		return
	}

	authCopy := auth.Clone()
	headerCopy := headers.Clone()
	warmupTemplate = bytes.Clone(warmupTemplate)
	rootCorrelation, executionCorrelation := helps.ClaudeCodeCorrelationIDs(ctx, nil, nil)
	e.backgroundWG.Add(1)
	go func() {
		defer e.backgroundWG.Done()
		dialCtx, cancel := context.WithTimeout(e.runtimeCtx, codexResponsesWebsocketHandshakeTO)
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
			e.pool.failReservation(key, generation, status == http.StatusTooManyRequests, time.Now())
			idle, dialing := e.pool.snapshot()
			helps.RecordDetachedAPIWebsocketMetric(e.cfg, "speculative_preconnect_failed", rootCorrelation, executionCorrelation, map[string]any{
				"session_id":   sessionID,
				"duration_us":  duration.Microseconds(),
				"status":       status,
				"rate_limited": status == http.StatusTooManyRequests,
				"pool_idle":    idle,
				"pool_dialing": dialing,
			})
			log.WithFields(log.Fields{
				"auth":        key.authID,
				"url":         key.wsURL,
				"duration_ms": duration.Milliseconds(),
				"status":      status,
			}).WithError(errDial).Debug("codex websockets: speculative preconnect failed")
			return
		}
		if e.cfg.CodexWebsocketGenerateFalseWarmup && len(warmupTemplate) > 0 {
			warmupStartedAt := time.Now()
			helpFields := map[string]any{"session_id": sessionID}
			helps.RecordDetachedAPIWebsocketMetric(e.cfg, "generate_false_warmup_started", rootCorrelation, executionCorrelation, helpFields)
			warmupRequest, errWarmup := buildCodexGenerateFalseWarmupRequest(warmupTemplate)
			var warmupResult codexGenerateFalseWarmupResult
			if errWarmup == nil {
				warmupResult, errWarmup = performCodexGenerateFalseWarmup(dialCtx, conn, warmupRequest)
			}
			if errWarmup != nil {
				failureReason := codexGenerateFalseWarmupFailureReason(errWarmup)
				e.pool.failReservation(key, generation, false, time.Now())
				helps.RecordDetachedAPIWebsocketMetric(e.cfg, "generate_false_warmup_failed", rootCorrelation, executionCorrelation, map[string]any{
					"session_id":  sessionID,
					"duration_us": time.Since(warmupStartedAt).Microseconds(),
					"reason":      failureReason,
				})
				log.WithFields(log.Fields{
					"auth":           key.authID,
					"url":            key.wsURL,
					"duration_ms":    time.Since(warmupStartedAt).Milliseconds(),
					"failure_reason": failureReason,
				}).WithError(errWarmup).Debug("codex websockets: generate=false warmup failed")
				_ = conn.Close()
				return
			}
			helps.RecordDetachedAPIWebsocketMetric(e.cfg, "generate_false_warmup_ready", rootCorrelation, executionCorrelation, map[string]any{
				"session_id":          sessionID,
				"duration_us":         time.Since(warmupStartedAt).Microseconds(),
				"response_id_present": warmupResult.responseID != "",
				"input_tokens":        warmupResult.inputTokens,
				"output_tokens":       warmupResult.outputTokens,
			})
			log.WithFields(log.Fields{
				"auth":          key.authID,
				"url":           key.wsURL,
				"duration_ms":   time.Since(warmupStartedAt).Milliseconds(),
				"input_tokens":  warmupResult.inputTokens,
				"output_tokens": warmupResult.outputTokens,
			}).Debug("codex websockets: generate=false warmup ready")
		}
		if !e.pool.completeReservationObserved(key, generation, conn, maxIdle, ttl, time.Now(), func() {
			idle, dialing := e.pool.snapshot()
			helps.RecordDetachedAPIWebsocketMetric(e.cfg, "speculative_preconnect_expired", rootCorrelation, executionCorrelation, map[string]any{
				"session_id":   sessionID,
				"pool_idle":    idle,
				"pool_dialing": dialing,
			})
		}) {
			idle, dialing := e.pool.snapshot()
			helps.RecordDetachedAPIWebsocketMetric(e.cfg, "speculative_preconnect_discarded", rootCorrelation, executionCorrelation, map[string]any{
				"session_id":   sessionID,
				"duration_us":  time.Since(startedAt).Microseconds(),
				"pool_idle":    idle,
				"pool_dialing": dialing,
			})
			_ = conn.Close()
			return
		}
		duration := time.Since(startedAt)
		idle, dialing := e.pool.snapshot()
		helps.RecordDetachedAPIWebsocketMetric(e.cfg, "speculative_preconnect_ready", rootCorrelation, executionCorrelation, map[string]any{
			"session_id":            sessionID,
			"duration_us":           duration.Microseconds(),
			"generate_false_warmup": e.cfg.CodexWebsocketGenerateFalseWarmup,
			"pool_idle":             idle,
			"pool_dialing":          dialing,
		})
		log.WithFields(log.Fields{
			"auth":        key.authID,
			"url":         key.wsURL,
			"duration_ms": duration.Milliseconds(),
		}).Debug("codex websockets: speculative preconnect ready")
	}()
}

func (e *CodexWebsocketsExecutor) takeSpeculativePreconnect(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string) *websocket.Conn {
	enabled, _, ttl := e.speculativePreconnectSettings()
	if !enabled {
		return nil
	}
	key := codexWebsocketPreconnectKey{authID: strings.TrimSpace(authID), wsURL: strings.TrimSpace(wsURL)}
	conn, age, observation, ok := e.pool.takeOrWaitObserved(ctx, key, ttl)
	if !ok {
		helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_missed", map[string]any{
			"session_id":   sessionID,
			"wait_us":      observation.wait.Microseconds(),
			"reason":       observation.reason,
			"pool_idle":    observation.idle,
			"pool_dialing": observation.dialing,
		})
		return nil
	}
	helps.RecordAPIWebsocketMetric(ctx, e.cfg, "speculative_preconnect_leased", map[string]any{
		"session_id":   sessionID,
		"age_us":       age.Microseconds(),
		"wait_us":      observation.wait.Microseconds(),
		"pool_idle":    observation.idle,
		"pool_dialing": observation.dialing,
	})
	if e.cfg != nil && e.cfg.CodexWebsocketPreconnectReplenish {
		e.scheduleSpeculativePreconnectForTrigger(ctx, auth, authID, wsURL, headers, sessionID, nil, "lease_replenish")
	}
	return conn
}

func (p *codexWebsocketPreconnectPool) reserve(key codexWebsocketPreconnectKey, maxIdle int, now time.Time) (bool, string, uint64) {
	if p == nil || maxIdle <= 0 {
		return false, "disabled", 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false, "draining", 0
	}
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
	return p.completeReservationObserved(key, generation, conn, maxIdle, ttl, now, nil)
}

func (p *codexWebsocketPreconnectPool) completeReservationObserved(key codexWebsocketPreconnectKey, generation uint64, conn *websocket.Conn, maxIdle int, ttl time.Duration, now time.Time, onExpire func()) bool {
	if p == nil || conn == nil {
		return false
	}
	p.mu.Lock()
	p.finishReservationLocked(key)
	if p.closed {
		p.mu.Unlock()
		return false
	}
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

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		timer := time.NewTimer(ttl)
		defer timer.Stop()
		var done <-chan struct{}
		if p.ctx != nil {
			done = p.ctx.Done()
		}
		select {
		case <-done:
			return
		case <-timer.C:
		}
		if expired := p.remove(key, entry.id); expired != nil {
			_ = expired.Close()
			if onExpire != nil {
				onExpire()
			}
			log.WithFields(log.Fields{"auth": key.authID, "url": key.wsURL}).Debug("codex websockets: speculative preconnect expired")
		}
	}()
	return true
}

func (p *codexWebsocketPreconnectPool) takeOrWait(ctx context.Context, key codexWebsocketPreconnectKey, ttl time.Duration) (*websocket.Conn, time.Duration, bool) {
	conn, age, _, ok := p.takeOrWaitObserved(ctx, key, ttl)
	return conn, age, ok
}

func (p *codexWebsocketPreconnectPool) takeOrWaitObserved(ctx context.Context, key codexWebsocketPreconnectKey, ttl time.Duration) (*websocket.Conn, time.Duration, codexWebsocketPreconnectObservation, bool) {
	if p == nil {
		return nil, 0, codexWebsocketPreconnectObservation{reason: "disabled"}, false
	}
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	waited := false
	for {
		if conn, age, ok := p.take(key, ttl, time.Now()); ok {
			idle, dialing := p.snapshot()
			return conn, age, codexWebsocketPreconnectObservation{wait: time.Since(startedAt), reason: "leased", idle: idle, dialing: dialing}, true
		}

		p.mu.Lock()
		if p.dialingByKey[key] <= 0 {
			p.mu.Unlock()
			// A completion can land between the optimistic take and the
			// in-flight check. Retake once before falling back to a cold dial.
			conn, age, ok := p.take(key, ttl, time.Now())
			idle, dialing := p.snapshot()
			reason := "no_idle"
			if waited {
				reason = "inflight_failed"
			}
			if ok {
				reason = "leased"
			}
			return conn, age, codexWebsocketPreconnectObservation{wait: time.Since(startedAt), reason: reason, idle: idle, dialing: dialing}, ok
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
		waited = true

		select {
		case <-ctx.Done():
			idle, dialing := p.snapshot()
			return nil, 0, codexWebsocketPreconnectObservation{wait: time.Since(startedAt), reason: "context_done", idle: idle, dialing: dialing}, false
		case <-changed:
		}
	}
}

func (p *codexWebsocketPreconnectPool) snapshot() (int, int) {
	if p == nil {
		return 0, 0
	}
	p.mu.Lock()
	idle := p.totalIdleLocked()
	dialing := p.dialing
	p.mu.Unlock()
	return idle, dialing
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

func (p *codexWebsocketPreconnectPool) closeAll() {
	if p == nil {
		return
	}
	var conns []*websocket.Conn
	p.mu.Lock()
	p.closed = true
	for authID := range p.authGeneration {
		p.authGeneration[authID]++
	}
	for _, entries := range p.idle {
		for _, entry := range entries {
			conns = append(conns, entry.conn)
		}
	}
	p.idle = make(map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry)
	p.cooldown = make(map[codexWebsocketPreconnectKey]time.Time)
	for key := range p.changed {
		p.notifyChangedLocked(key)
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
