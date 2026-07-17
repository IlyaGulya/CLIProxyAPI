package executor

import (
	"testing"
	"time"
)

func TestCodexAdaptivePreconnectControllerGrowsOnMissBurstAndShrinksOnExpiry(t *testing.T) {
	controller := newCodexAdaptivePreconnectController(codexAdaptivePreconnectConfig{MaxRoutes: 8, MaxTarget: 4, MinTTL: 5 * time.Second, MaxTTL: time.Minute})
	route := codexAdaptivePreconnectRoute{AuthID: "auth-a", Model: "gpt-5.6-luna"}
	for range 4 {
		controller.observe(route, codexAdaptivePreconnectObservation{Miss: true, DialLatency: 800 * time.Millisecond, Burst: 3})
	}
	decision := controller.decision(route)
	if decision.Target < 2 || decision.Target > 4 || decision.TTL < 5*time.Second || decision.TTL > time.Minute {
		t.Fatalf("growth decision = %+v", decision)
	}
	for range 8 {
		controller.observe(route, codexAdaptivePreconnectObservation{Expired: true})
	}
	if shrunk := controller.decision(route); shrunk.Target >= decision.Target {
		t.Fatalf("expiry did not shrink target: before=%+v after=%+v", decision, shrunk)
	}
}

func TestCodexAdaptivePreconnectControllerIsolatesAndBoundsRoutes(t *testing.T) {
	controller := newCodexAdaptivePreconnectController(codexAdaptivePreconnectConfig{MaxRoutes: 2, MaxTarget: 3})
	busy := codexAdaptivePreconnectRoute{AuthID: "auth-a", Model: "luna"}
	controller.observe(busy, codexAdaptivePreconnectObservation{Miss: true, Burst: 3})
	controller.observe(busy, codexAdaptivePreconnectObservation{Miss: true, Burst: 3})
	idle := codexAdaptivePreconnectRoute{AuthID: "auth-b", Model: "sol"}
	if got := controller.decision(idle).Target; got != 1 {
		t.Fatalf("route state leaked: idle target=%d", got)
	}
	controller.observe(idle, codexAdaptivePreconnectObservation{Hit: true})
	controller.observe(codexAdaptivePreconnectRoute{AuthID: "auth-c", Model: "terra"}, codexAdaptivePreconnectObservation{Hit: true})
	if got := controller.routeCount(); got != 2 {
		t.Fatalf("route count=%d, want 2", got)
	}
}
