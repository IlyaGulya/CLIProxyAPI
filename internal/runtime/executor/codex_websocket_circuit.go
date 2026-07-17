package executor

import (
	"strings"
	"sync"
	"time"
)

type codexWebsocketCircuitState uint8

const (
	circuitClosed codexWebsocketCircuitState = iota
	circuitOpen
	circuitHalfOpen
)

func (s codexWebsocketCircuitState) String() string {
	switch s {
	case circuitClosed:
		return "closed"
	case circuitOpen:
		return "open"
	case circuitHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

type codexWebsocketRoute struct {
	provider string
	authID   string
	model    string
}

func (r codexWebsocketRoute) normalized() codexWebsocketRoute {
	return codexWebsocketRoute{
		provider: strings.ToLower(strings.TrimSpace(r.provider)),
		authID:   strings.TrimSpace(r.authID),
		model:    strings.ToLower(strings.TrimSpace(r.model)),
	}
}

type codexWebsocketCircuitConfig struct {
	failureThreshold int
	cooldown         time.Duration
	maxRoutes        int
	now              func() time.Time
}

type codexWebsocketCircuitDecision struct {
	allowed           bool
	state             codexWebsocketCircuitState
	cooldownRemaining time.Duration
}

type codexWebsocketCircuitEntry struct {
	failures   int
	state      codexWebsocketCircuitState
	openedAt   time.Time
	probeInUse bool
	lastReason string
	updatedAt  time.Time
}

type codexWebsocketCircuit struct {
	mu               sync.Mutex
	routes           map[codexWebsocketRoute]*codexWebsocketCircuitEntry
	failureThreshold int
	cooldown         time.Duration
	maxRoutes        int
	now              func() time.Time
}

func newCodexWebsocketCircuit(cfg codexWebsocketCircuitConfig) *codexWebsocketCircuit {
	if cfg.failureThreshold <= 0 {
		cfg.failureThreshold = 3
	}
	if cfg.cooldown <= 0 {
		cfg.cooldown = 30 * time.Second
	}
	if cfg.now == nil {
		cfg.now = time.Now
	}
	if cfg.maxRoutes <= 0 {
		cfg.maxRoutes = 1024
	}
	return &codexWebsocketCircuit{
		routes:           make(map[codexWebsocketRoute]*codexWebsocketCircuitEntry),
		failureThreshold: cfg.failureThreshold,
		cooldown:         cfg.cooldown,
		maxRoutes:        cfg.maxRoutes,
		now:              cfg.now,
	}
}

func (c *codexWebsocketCircuit) allow(route codexWebsocketRoute) codexWebsocketCircuitDecision {
	if c == nil {
		return codexWebsocketCircuitDecision{allowed: true, state: circuitClosed}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.routes[route.normalized()]
	if entry == nil || entry.state == circuitClosed {
		return codexWebsocketCircuitDecision{allowed: true, state: circuitClosed}
	}
	if entry.state == circuitHalfOpen || c.now().Sub(entry.openedAt) < c.cooldown {
		remaining := c.cooldown - c.now().Sub(entry.openedAt)
		if remaining < 0 {
			remaining = 0
		}
		return codexWebsocketCircuitDecision{allowed: false, state: entry.state, cooldownRemaining: remaining}
	}
	entry.state = circuitHalfOpen
	entry.probeInUse = true
	return codexWebsocketCircuitDecision{allowed: true, state: circuitHalfOpen}
}

func (c *codexWebsocketCircuit) success(route codexWebsocketRoute) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.routes, route.normalized())
	c.mu.Unlock()
}

func (c *codexWebsocketCircuit) failure(route codexWebsocketRoute, reason string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	route = route.normalized()
	entry := c.routes[route]
	if entry == nil {
		c.evictOldestRouteLocked()
		entry = &codexWebsocketCircuitEntry{}
		c.routes[route] = entry
	}
	entry.failures++
	entry.lastReason = strings.TrimSpace(reason)
	entry.updatedAt = c.now()
	if entry.state == circuitHalfOpen || entry.failures >= c.failureThreshold {
		entry.state = circuitOpen
		entry.openedAt = c.now()
		entry.probeInUse = false
	}
}

func (c *codexWebsocketCircuit) evictOldestRouteLocked() {
	if len(c.routes) < c.maxRoutes {
		return
	}
	var oldestRoute codexWebsocketRoute
	var oldestAt time.Time
	for route, entry := range c.routes {
		if oldestAt.IsZero() || entry.updatedAt.Before(oldestAt) {
			oldestRoute = route
			oldestAt = entry.updatedAt
		}
	}
	delete(c.routes, oldestRoute)
}

func (c *codexWebsocketCircuit) routeCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.routes)
}

func (c *codexWebsocketCircuit) suppressBackground(route codexWebsocketRoute) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.routes[route.normalized()]
	return entry != nil && entry.state != circuitClosed
}
