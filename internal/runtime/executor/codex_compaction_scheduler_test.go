package executor

import (
	"testing"
	"time"
)

func TestCodexCompactionSchedulerHardSafetyOverridesCacheEconomics(t *testing.T) {
	scheduler := newCodexCompactionScheduler(codexCompactionSchedulerConfig{})
	decision := scheduler.decide(codexCompactionEconomics{
		ContextTokens: 250_000, ReservedOutputTokens: 16_000, ContextWindowTokens: 272_000, SafetyMarginTokens: 8_000,
		CacheReadRatio: 0.99, PrefixContinuous: true, CacheAge: time.Minute, CacheTTL: time.Hour,
	})
	if !decision.Compact || decision.Reason != "context_safety" {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestCodexCompactionSchedulerPrefersValidCachedReplay(t *testing.T) {
	scheduler := newCodexCompactionScheduler(codexCompactionSchedulerConfig{})
	decision := scheduler.decide(codexCompactionEconomics{
		ContextTokens: 100_000, ContextWindowTokens: 272_000, SafetyMarginTokens: 16_000,
		CacheReadRatio: 0.9, PrefixContinuous: true, CacheAge: time.Minute, CacheTTL: time.Hour,
		ReplayBytes: 400_000, ExpectedCompactedBytes: 40_000, CompactionCostMicros: 50_000,
	})
	if decision.Compact || decision.Reason != "cached_replay" {
		t.Fatalf("decision = %+v", decision)
	}
}

func TestCodexCompactionSchedulerUsesSavingsThresholdAndHysteresis(t *testing.T) {
	scheduler := newCodexCompactionScheduler(codexCompactionSchedulerConfig{MinSavingsBytes: 100_000, HysteresisBytes: 20_000})
	base := codexCompactionEconomics{ContextTokens: 100_000, ContextWindowTokens: 272_000, ReplayBytes: 300_000, ExpectedCompactedBytes: 150_000}
	first := scheduler.decide(base)
	if !first.Compact || first.PredictedSavingsBytes != 150_000 {
		t.Fatalf("first = %+v", first)
	}
	base.ReplayBytes = 240_000 // 90k savings: below threshold, but inside hysteresis from compact.
	second := scheduler.decide(base)
	if !second.Compact || second.Reason != "hysteresis_compact" {
		t.Fatalf("second = %+v", second)
	}
	base.ReplayBytes = 210_000 // 60k savings: exits hysteresis.
	third := scheduler.decide(base)
	if third.Compact {
		t.Fatalf("third = %+v", third)
	}
}

func FuzzCodexCompactionSchedulerNeverViolatesHardSafety(f *testing.F) {
	f.Add(int64(260_000), int64(16_000), int64(272_000), int64(8_000))
	f.Fuzz(func(t *testing.T, contextTokens, reserved, window, margin int64) {
		if contextTokens < 0 || reserved < 0 || window <= 0 || margin < 0 || margin >= window || contextTokens+reserved < window-margin {
			return
		}
		decision := newCodexCompactionScheduler(codexCompactionSchedulerConfig{}).decide(codexCompactionEconomics{
			ContextTokens: contextTokens, ReservedOutputTokens: reserved, ContextWindowTokens: window, SafetyMarginTokens: margin,
			CacheReadRatio: 1, PrefixContinuous: true, CacheTTL: time.Hour,
		})
		if !decision.Compact {
			t.Fatalf("unsafe replay decision: %+v", decision)
		}
	})
}
