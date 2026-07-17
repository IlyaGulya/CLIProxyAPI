package executor

import (
	"strings"
	"sync"
	"time"
)

type codexAdaptivePreconnectRoute struct {
	AuthID string
	Model  string
}

func (r codexAdaptivePreconnectRoute) normalized() codexAdaptivePreconnectRoute {
	return codexAdaptivePreconnectRoute{AuthID: strings.TrimSpace(r.AuthID), Model: strings.ToLower(strings.TrimSpace(r.Model))}
}

type codexAdaptivePreconnectConfig struct {
	MaxRoutes int
	MaxTarget int
	MinTTL    time.Duration
	MaxTTL    time.Duration
	Alpha     float64
	Now       func() time.Time
}

type codexAdaptivePreconnectObservation struct {
	Hit         bool
	Miss        bool
	Expired     bool
	Burst       int
	DialLatency time.Duration
	Lifetime    time.Duration
}

type codexAdaptivePreconnectDecision struct {
	Target          int
	TTL             time.Duration
	HitRateEWMA     float64
	DialLatencyEWMA time.Duration
	Reason          string
}

type codexAdaptivePreconnectState struct {
	target      int
	hitRate     float64
	dialLatency float64
	lifetime    float64
	updatedAt   time.Time
	reason      string
}

type codexAdaptivePreconnectController struct {
	mu        sync.Mutex
	states    map[codexAdaptivePreconnectRoute]*codexAdaptivePreconnectState
	maxRoutes int
	maxTarget int
	minTTL    time.Duration
	maxTTL    time.Duration
	alpha     float64
	now       func() time.Time
}

func newCodexAdaptivePreconnectController(cfg codexAdaptivePreconnectConfig) *codexAdaptivePreconnectController {
	if cfg.MaxRoutes <= 0 {
		cfg.MaxRoutes = 256
	}
	if cfg.MaxTarget <= 0 {
		cfg.MaxTarget = 2
	}
	if cfg.MinTTL <= 0 {
		cfg.MinTTL = 5 * time.Second
	}
	if cfg.MaxTTL <= 0 {
		cfg.MaxTTL = time.Minute
	}
	if cfg.MaxTTL < cfg.MinTTL {
		cfg.MaxTTL = cfg.MinTTL
	}
	if cfg.Alpha <= 0 || cfg.Alpha > 1 {
		cfg.Alpha = 0.25
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &codexAdaptivePreconnectController{states: make(map[codexAdaptivePreconnectRoute]*codexAdaptivePreconnectState), maxRoutes: cfg.MaxRoutes, maxTarget: cfg.MaxTarget, minTTL: cfg.MinTTL, maxTTL: cfg.MaxTTL, alpha: cfg.Alpha, now: cfg.Now}
}

func (c *codexAdaptivePreconnectController) observe(route codexAdaptivePreconnectRoute, observation codexAdaptivePreconnectObservation) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	route = route.normalized()
	state := c.states[route]
	if state == nil {
		c.evictOldestLocked()
		state = &codexAdaptivePreconnectState{target: 1, hitRate: 0.5}
		c.states[route] = state
	}
	state.updatedAt = c.now()
	if observation.Hit {
		state.hitRate = ewma(state.hitRate, 1, c.alpha)
		state.reason = "lease_hit"
	}
	if observation.Miss {
		state.hitRate = ewma(state.hitRate, 0, c.alpha)
		state.target++
		if observation.Burst > state.target {
			state.target = observation.Burst
		}
		state.reason = "lease_miss"
	}
	if observation.Expired {
		state.hitRate = ewma(state.hitRate, 0, c.alpha)
		if state.target > 1 {
			state.target--
		}
		state.reason = "idle_expiry"
	}
	if observation.DialLatency > 0 {
		state.dialLatency = ewma(state.dialLatency, float64(observation.DialLatency), c.alpha)
	}
	if observation.Lifetime > 0 {
		state.lifetime = ewma(state.lifetime, float64(observation.Lifetime), c.alpha)
	}
	state.target = min(max(state.target, 1), c.maxTarget)
}

func (c *codexAdaptivePreconnectController) decision(route codexAdaptivePreconnectRoute) codexAdaptivePreconnectDecision {
	if c == nil {
		return codexAdaptivePreconnectDecision{Target: 1, TTL: 30 * time.Second, Reason: "disabled"}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.states[route.normalized()]
	if state == nil {
		return codexAdaptivePreconnectDecision{Target: 1, TTL: min(30*time.Second, c.maxTTL), HitRateEWMA: 0.5, Reason: "cold_start"}
	}
	ttl := c.minTTL
	if state.dialLatency > 0 {
		ttl = time.Duration(state.dialLatency * 20)
	}
	if state.lifetime > 0 && time.Duration(state.lifetime) < ttl {
		ttl = time.Duration(state.lifetime)
	}
	ttl = min(max(ttl, c.minTTL), c.maxTTL)
	return codexAdaptivePreconnectDecision{Target: state.target, TTL: ttl, HitRateEWMA: state.hitRate, DialLatencyEWMA: time.Duration(state.dialLatency), Reason: state.reason}
}

func (c *codexAdaptivePreconnectController) evictOldestLocked() {
	if len(c.states) < c.maxRoutes {
		return
	}
	var oldest codexAdaptivePreconnectRoute
	var at time.Time
	for route, state := range c.states {
		if at.IsZero() || state.updatedAt.Before(at) {
			oldest, at = route, state.updatedAt
		}
	}
	delete(c.states, oldest)
}

func (c *codexAdaptivePreconnectController) routeCount() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.states)
}

func ewma(previous, sample, alpha float64) float64 {
	if previous == 0 {
		return sample
	}
	return alpha*sample + (1-alpha)*previous
}
