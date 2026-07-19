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
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
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
	codexWebsocketPreconnect429Cooldown = 30 * time.Second
)

type codexWebsocketPreconnectKey struct {
	authID string
	wsURL  string
}

type codexAdaptiveModelContextKey struct{}

func withCodexAdaptiveModel(ctx context.Context, model string) context.Context {
	return context.WithValue(ctx, codexAdaptiveModelContextKey{}, strings.TrimSpace(model))
}

func codexAdaptiveModel(ctx context.Context) string {
	value, _ := ctx.Value(codexAdaptiveModelContextKey{}).(string)
	return strings.TrimSpace(value)
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
	mu              sync.Mutex
	closed          bool
	idle            map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry
	dialing         int
	dialingByKey    map[codexWebsocketPreconnectKey]int
	changed         map[codexWebsocketPreconnectKey]chan struct{}
	cooldown        map[codexWebsocketPreconnectKey]time.Time
	authGeneration  map[string]uint64
	nextID          uint64
	ctx             context.Context
	wg              sync.WaitGroup
	closeConnection func(*websocket.Conn) error
}

func newCodexWebsocketPreconnectPool(ctx context.Context, closeConnection func(*websocket.Conn) error) *codexWebsocketPreconnectPool {
	if ctx == nil {
		ctx = context.Background()
	}
	return &codexWebsocketPreconnectPool{
		idle:            make(map[codexWebsocketPreconnectKey][]codexWebsocketPreconnectEntry),
		dialingByKey:    make(map[codexWebsocketPreconnectKey]int),
		changed:         make(map[codexWebsocketPreconnectKey]chan struct{}),
		cooldown:        make(map[codexWebsocketPreconnectKey]time.Time),
		authGeneration:  make(map[string]uint64),
		ctx:             ctx,
		closeConnection: closeConnection,
	}
}

func (p *codexWebsocketPreconnectPool) close(conn *websocket.Conn) error {
	if p != nil && p.closeConnection != nil {
		return p.closeConnection(conn)
	}
	if conn == nil {
		return nil
	}
	return conn.Close()
}

func (e *CodexWebsocketsExecutor) speculativePreconnectSettings() (bool, int, time.Duration) {
	if e == nil || e.cfg == nil || !e.cfg.CodexWebsocketSpeculativePreconnect {
		return false, 0, 0
	}
	runtimeCfg := e.cfg.NormalizedCodexWebsocketConfig()
	return true, runtimeCfg.PreconnectMaxIdle, runtimeCfg.PreconnectTTL
}

func codexFanoutToolCall(payload []byte) (string, string, bool) {
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType != "response.output_item.added" && eventType != "response.output_item.done" {
		return "", "", false
	}
	item := gjson.GetBytes(payload, "item")
	toolName := strings.TrimSpace(item.Get("name").String())
	if strings.TrimSpace(item.Get("type").String()) != "function_call" || !codexFanoutToolName(toolName) {
		return "", "", false
	}
	key := strings.TrimSpace(item.Get("call_id").String())
	if key == "" {
		key = strings.TrimSpace(item.Get("id").String())
	}
	if key == "" {
		return "", "", false
	}
	return key, toolName, true
}

func codexFanoutToolName(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "agent", "workflow":
		return true
	default:
		return false
	}
}

func (e *CodexWebsocketsExecutor) scheduleSpeculativePreconnect(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string, warmupTemplate []byte) {
	e.scheduleSpeculativePreconnectForFanout(ctx, auth, authID, wsURL, headers, sessionID, warmupTemplate, "Agent")
}

func (e *CodexWebsocketsExecutor) scheduleSpeculativePreconnectForFanout(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string, warmupTemplate []byte, toolName string) {
	e.scheduleSpeculativePreconnectForTrigger(ctx, auth, authID, wsURL, headers, sessionID, warmupTemplate, "fanout_tool", toolName)
}

func (e *CodexWebsocketsExecutor) scheduleSpeculativePreconnectForTrigger(ctx context.Context, auth *cliproxyauth.Auth, authID string, wsURL string, headers http.Header, sessionID string, warmupTemplate []byte, trigger string, toolName string) {
	enabled, maxIdle, ttl := e.speculativePreconnectSettings()
	if !enabled || auth == nil || strings.TrimSpace(authID) == "" || strings.TrimSpace(wsURL) == "" {
		return
	}
	model := codexAdaptiveModel(ctx)
	if model == "" {
		model = gjson.GetBytes(warmupTemplate, "model").String()
	}
	route := codexAdaptivePreconnectRoute{AuthID: authID, Model: model}
	if e.circuit != nil && e.circuit.suppressBackground(codexCircuitRoute(auth, model)) {
		helps.RecordAPIWebsocketEvent(ctx, e.cfg, "speculative_preconnect_suppressed", observability.WebsocketAttributes{
			SessionID: sessionID, Model: model, Reason: "route_circuit_open", Trigger: trigger, ToolName: toolName,
		}, nil)
		return
	}
	if e.adaptivePreconnect != nil {
		decision := e.adaptivePreconnect.decision(route)
		maxIdle = min(maxIdle, decision.Target)
		ttl = min(ttl, decision.TTL)
		helps.RecordAPIWebsocketEvent(ctx, e.cfg, "speculative_preconnect_adaptive_decision", observability.WebsocketAttributes{
			SessionID: sessionID, Model: model, Reason: decision.Reason, Trigger: trigger, ToolName: toolName,
			AdaptiveTarget: observability.Some(int64(decision.Target)), AdaptiveHitRateBasisPoints: observability.Some(int64(decision.HitRateEWMA * 10_000)),
			CounterfactualWaitUS: observability.Some(decision.DialLatencyEWMA.Microseconds()),
		}, nil)
	}
	key := codexWebsocketPreconnectKey{authID: strings.TrimSpace(authID), wsURL: strings.TrimSpace(wsURL)}
	reserved, reason, generation := e.sessions.pool.reserve(key, maxIdle, time.Now())
	poolIdle, poolDialing := e.sessions.pool.snapshot()
	helps.RecordAPIWebsocketEvent(ctx, e.cfg, "speculative_preconnect_triggered", observability.WebsocketAttributes{
		SessionID: sessionID, Reserved: observability.Some(reserved), Reason: reason, Trigger: trigger, ToolName: toolName,
		GenerateFalseWarmup: observability.Some(e.cfg.CodexWebsocketGenerateFalseWarmup), PoolIdle: observability.Some(int64(poolIdle)), PoolDialing: observability.Some(int64(poolDialing)),
	}, nil)
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
			if e.adaptivePreconnect != nil {
				e.adaptivePreconnect.observe(route, codexAdaptivePreconnectObservation{Miss: true, DialLatency: duration})
			}
			e.sessions.pool.failReservation(key, generation, status == http.StatusTooManyRequests, time.Now())
			idle, dialing := e.sessions.pool.snapshot()
			helps.RecordDetachedAPIWebsocketEvent(e.cfg, "speculative_preconnect_failed", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{
				SessionID: sessionID, DurationUS: observability.Some(duration.Microseconds()), Status: observability.Some(int64(status)),
				RateLimited: observability.Some(status == http.StatusTooManyRequests), PoolIdle: observability.Some(int64(idle)), PoolDialing: observability.Some(int64(dialing)),
			}, nil)
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
			helps.RecordDetachedAPIWebsocketEvent(e.cfg, "generate_false_warmup_started", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{SessionID: sessionID}, nil)
			warmupRequest, errWarmup := buildCodexGenerateFalseWarmupRequest(warmupTemplate)
			var warmupResult codexGenerateFalseWarmupResult
			if errWarmup == nil {
				warmupResult, errWarmup = performCodexGenerateFalseWarmup(dialCtx, conn, warmupRequest)
			}
			if errWarmup != nil {
				failureReason := codexGenerateFalseWarmupFailureReason(errWarmup)
				e.sessions.pool.failReservation(key, generation, false, time.Now())
				helps.RecordDetachedAPIWebsocketEvent(e.cfg, "generate_false_warmup_failed", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{
					SessionID: sessionID, DurationUS: observability.Some(time.Since(warmupStartedAt).Microseconds()), Reason: failureReason,
				}, nil)
				log.WithFields(log.Fields{
					"auth":           key.authID,
					"url":            key.wsURL,
					"duration_ms":    time.Since(warmupStartedAt).Milliseconds(),
					"failure_reason": failureReason,
				}).WithError(errWarmup).Debug("codex websockets: generate=false warmup failed")
				_ = e.sessions.pool.close(conn)
				return
			}
			helps.RecordDetachedAPIWebsocketEvent(e.cfg, "generate_false_warmup_ready", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{
				SessionID: sessionID, DurationUS: observability.Some(time.Since(warmupStartedAt).Microseconds()), ResponseIDPresent: observability.Some(warmupResult.responseID != ""),
				InputTokens: observability.Some(warmupResult.inputTokens), OutputTokens: observability.Some(warmupResult.outputTokens),
			}, nil)
			log.WithFields(log.Fields{
				"auth":          key.authID,
				"url":           key.wsURL,
				"duration_ms":   time.Since(warmupStartedAt).Milliseconds(),
				"input_tokens":  warmupResult.inputTokens,
				"output_tokens": warmupResult.outputTokens,
			}).Debug("codex websockets: generate=false warmup ready")
		}
		if !e.sessions.pool.completeReservationObserved(key, generation, conn, maxIdle, ttl, time.Now(), func() {
			if e.adaptivePreconnect != nil {
				e.adaptivePreconnect.observe(route, codexAdaptivePreconnectObservation{Expired: true})
			}
			idle, dialing := e.sessions.pool.snapshot()
			helps.RecordDetachedAPIWebsocketEvent(e.cfg, "speculative_preconnect_expired", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{
				SessionID: sessionID, PoolIdle: observability.Some(int64(idle)), PoolDialing: observability.Some(int64(dialing)),
			}, nil)
		}) {
			idle, dialing := e.sessions.pool.snapshot()
			helps.RecordDetachedAPIWebsocketEvent(e.cfg, "speculative_preconnect_discarded", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{
				SessionID: sessionID, DurationUS: observability.Some(time.Since(startedAt).Microseconds()), PoolIdle: observability.Some(int64(idle)), PoolDialing: observability.Some(int64(dialing)),
			}, nil)
			_ = e.sessions.pool.close(conn)
			return
		}
		duration := time.Since(startedAt)
		if e.adaptivePreconnect != nil {
			e.adaptivePreconnect.observe(route, codexAdaptivePreconnectObservation{DialLatency: duration})
		}
		idle, dialing := e.sessions.pool.snapshot()
		helps.RecordDetachedAPIWebsocketEvent(e.cfg, "speculative_preconnect_ready", rootCorrelation, executionCorrelation, observability.WebsocketAttributes{
			SessionID: sessionID, DurationUS: observability.Some(duration.Microseconds()), GenerateFalseWarmup: observability.Some(e.cfg.CodexWebsocketGenerateFalseWarmup),
			PoolIdle: observability.Some(int64(idle)), PoolDialing: observability.Some(int64(dialing)),
		}, nil)
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
	route := codexAdaptivePreconnectRoute{AuthID: authID, Model: codexAdaptiveModel(ctx)}
	take := e.sessions.pool.takeOrWaitObserved(ctx, key, ttl)
	if !take.leased {
		if e.adaptivePreconnect != nil {
			e.adaptivePreconnect.observe(route, codexAdaptivePreconnectObservation{Miss: true})
		}
		helps.RecordAPIWebsocketEvent(ctx, e.cfg, "speculative_preconnect_missed", observability.WebsocketAttributes{
			SessionID: sessionID, WaitUS: observability.Some(take.observation.wait.Microseconds()), Reason: take.observation.reason,
			PoolIdle: observability.Some(int64(take.observation.idle)), PoolDialing: observability.Some(int64(take.observation.dialing)),
		}, nil)
		return nil
	}
	if e.adaptivePreconnect != nil {
		e.adaptivePreconnect.observe(route, codexAdaptivePreconnectObservation{Hit: true, Lifetime: take.age})
	}
	helps.RecordAPIWebsocketEvent(ctx, e.cfg, "speculative_preconnect_leased", observability.WebsocketAttributes{
		SessionID: sessionID, AgeUS: observability.Some(take.age.Microseconds()), WaitUS: observability.Some(take.observation.wait.Microseconds()),
		PoolIdle: observability.Some(int64(take.observation.idle)), PoolDialing: observability.Some(int64(take.observation.dialing)),
	}, nil)
	if e.cfg != nil && e.cfg.CodexWebsocketPreconnectReplenish {
		e.scheduleSpeculativePreconnectForTrigger(ctx, auth, authID, wsURL, headers, sessionID, nil, "lease_replenish", "")
	}
	return take.conn
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
			_ = p.close(expired)
			if onExpire != nil {
				onExpire()
			}
			log.WithFields(log.Fields{"auth": key.authID, "url": key.wsURL}).Debug("codex websockets: speculative preconnect expired")
		}
	}()
	return true
}

type codexWebsocketPreconnectTake struct {
	conn        *websocket.Conn
	age         time.Duration
	observation codexWebsocketPreconnectObservation
	leased      bool
}

func (p *codexWebsocketPreconnectPool) takeOrWaitObserved(ctx context.Context, key codexWebsocketPreconnectKey, ttl time.Duration) codexWebsocketPreconnectTake {
	if p == nil {
		return codexWebsocketPreconnectTake{observation: codexWebsocketPreconnectObservation{reason: "disabled"}}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	startedAt := time.Now()
	waited := false
	for {
		if conn, age, ok := p.take(key, ttl, time.Now()); ok {
			idle, dialing := p.snapshot()
			return codexWebsocketPreconnectTake{conn: conn, age: age, observation: codexWebsocketPreconnectObservation{wait: time.Since(startedAt), reason: "leased", idle: idle, dialing: dialing}, leased: true}
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
			return codexWebsocketPreconnectTake{conn: conn, age: age, observation: codexWebsocketPreconnectObservation{wait: time.Since(startedAt), reason: reason, idle: idle, dialing: dialing}, leased: ok}
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
			return codexWebsocketPreconnectTake{observation: codexWebsocketPreconnectObservation{wait: time.Since(startedAt), reason: "context_done", idle: idle, dialing: dialing}}
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
			_ = p.close(conn)
		}
		return entry.conn, now.Sub(entry.createdAt), true
	}
	delete(p.idle, key)
	p.mu.Unlock()
	for _, conn := range expired {
		_ = p.close(conn)
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
		_ = p.close(conn)
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
		_ = p.close(conn)
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
