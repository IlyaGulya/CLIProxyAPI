package executor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCodexWebsocketCircuitIsRouteScoped(t *testing.T) {
	now := time.Unix(100, 0)
	circuit := newCodexWebsocketCircuit(codexWebsocketCircuitConfig{
		failureThreshold: 2,
		cooldown:         time.Minute,
		now:              func() time.Time { return now },
	})
	routeA := codexWebsocketRoute{provider: "codex", authID: "auth-a", model: "sol"}
	routeB := codexWebsocketRoute{provider: "codex", authID: "auth-b", model: "sol"}

	circuit.failure(routeA, "handshake_rejected")
	if decision := circuit.allow(routeA); !decision.allowed || decision.state != circuitClosed {
		t.Fatalf("first failure decision = %+v", decision)
	}
	circuit.failure(routeA, "abnormal_close")
	if decision := circuit.allow(routeA); decision.allowed || decision.state != circuitOpen {
		t.Fatalf("open decision = %+v", decision)
	}
	if decision := circuit.allow(routeB); !decision.allowed || decision.state != circuitClosed {
		t.Fatalf("unrelated route leaked circuit state: %+v", decision)
	}
}

func TestCodexWebsocketCircuitHalfOpenAllowsSingleProbe(t *testing.T) {
	var nowNanos atomic.Int64
	nowNanos.Store(time.Unix(100, 0).UnixNano())
	circuit := newCodexWebsocketCircuit(codexWebsocketCircuitConfig{
		failureThreshold: 1,
		cooldown:         time.Minute,
		now: func() time.Time {
			return time.Unix(0, nowNanos.Load())
		},
	})
	route := codexWebsocketRoute{provider: "codex", authID: "auth-a", model: "luna"}
	circuit.failure(route, "retry_exhausted")
	nowNanos.Add(int64(time.Minute))

	const contenders = 16
	var probes atomic.Int32
	var wg sync.WaitGroup
	for range contenders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if decision := circuit.allow(route); decision.allowed {
				probes.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := probes.Load(); got != 1 {
		t.Fatalf("half-open probes = %d, want 1", got)
	}

	circuit.success(route)
	if decision := circuit.allow(route); !decision.allowed || decision.state != circuitClosed {
		t.Fatalf("recovered decision = %+v", decision)
	}
}

func TestCodexWebsocketCircuitFailureExtendsHalfOpenCooldown(t *testing.T) {
	now := time.Unix(100, 0)
	circuit := newCodexWebsocketCircuit(codexWebsocketCircuitConfig{
		failureThreshold: 1,
		cooldown:         time.Minute,
		now:              func() time.Time { return now },
	})
	route := codexWebsocketRoute{provider: "codex", authID: "auth-a", model: "sol"}
	circuit.failure(route, "dial")
	now = now.Add(time.Minute)
	if decision := circuit.allow(route); !decision.allowed || decision.state != circuitHalfOpen {
		t.Fatalf("probe decision = %+v", decision)
	}
	circuit.failure(route, "probe_failed")
	if decision := circuit.allow(route); decision.allowed || decision.cooldownRemaining != time.Minute {
		t.Fatalf("failed probe decision = %+v", decision)
	}
}

func TestCodexWebsocketCircuitBoundsRouteState(t *testing.T) {
	now := time.Unix(100, 0)
	circuit := newCodexWebsocketCircuit(codexWebsocketCircuitConfig{
		failureThreshold: 1,
		cooldown:         time.Minute,
		maxRoutes:        2,
		now:              func() time.Time { return now },
	})
	for _, authID := range []string{"auth-a", "auth-b", "auth-c"} {
		circuit.failure(codexWebsocketRoute{provider: "codex", authID: authID, model: "sol"}, "dial")
		now = now.Add(time.Second)
	}
	if got := circuit.routeCount(); got != 2 {
		t.Fatalf("route state count = %d, want bounded at 2", got)
	}
}
