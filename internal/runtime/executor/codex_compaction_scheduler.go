package executor

import (
	"sync"
	"time"
)

type codexCompactionSchedulerConfig struct {
	MinSavingsBytes         int64
	HysteresisBytes         int64
	MinCacheReadRatio       float64
	MaxCompactionCostMicros int64
}

type codexCompactionEconomics struct {
	ContextTokens          int64
	ReservedOutputTokens   int64
	ContextWindowTokens    int64
	SafetyMarginTokens     int64
	CacheReadRatio         float64
	PrefixContinuous       bool
	CacheAge               time.Duration
	CacheTTL               time.Duration
	ReplayBytes            int64
	ExpectedCompactedBytes int64
	CompactionLatency      time.Duration
	CompactionCostMicros   int64
}

type codexCompactionDecision struct {
	Compact               bool
	Reason                string
	PredictedSavingsBytes int64
	CacheValid            bool
	SafetyRequired        bool
}

type codexCompactionScheduler struct {
	mu              sync.Mutex
	config          codexCompactionSchedulerConfig
	previousCompact bool
}

func newCodexCompactionScheduler(config codexCompactionSchedulerConfig) *codexCompactionScheduler {
	if config.MinSavingsBytes <= 0 {
		config.MinSavingsBytes = 128 * 1024
	}
	if config.HysteresisBytes <= 0 {
		config.HysteresisBytes = 32 * 1024
	}
	if config.MinCacheReadRatio <= 0 || config.MinCacheReadRatio > 1 {
		config.MinCacheReadRatio = 0.70
	}
	if config.MaxCompactionCostMicros <= 0 {
		config.MaxCompactionCostMicros = 100_000
	}
	return &codexCompactionScheduler{config: config}
}

func (s *codexCompactionScheduler) decide(input codexCompactionEconomics) codexCompactionDecision {
	if s == nil {
		return codexCompactionDecision{Reason: "disabled"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	savings := input.ReplayBytes - input.ExpectedCompactedBytes
	if savings < 0 {
		savings = 0
	}
	cacheValid := input.PrefixContinuous && input.CacheTTL > 0 && input.CacheAge < input.CacheTTL && input.CacheReadRatio >= s.config.MinCacheReadRatio
	decision := codexCompactionDecision{PredictedSavingsBytes: savings, CacheValid: cacheValid}
	window := input.ContextWindowTokens
	margin := input.SafetyMarginTokens
	if window > 0 && margin < window && input.ContextTokens+input.ReservedOutputTokens >= window-margin {
		decision.Compact = true
		decision.Reason = "context_safety"
		decision.SafetyRequired = true
		s.previousCompact = true
		return decision
	}
	if cacheValid {
		decision.Reason = "cached_replay"
		s.previousCompact = false
		return decision
	}
	if input.CompactionCostMicros > s.config.MaxCompactionCostMicros {
		decision.Reason = "compaction_cost"
		s.previousCompact = false
		return decision
	}
	threshold := s.config.MinSavingsBytes
	if s.previousCompact && threshold > s.config.HysteresisBytes {
		threshold -= s.config.HysteresisBytes
	}
	if savings >= threshold {
		decision.Compact = true
		if s.previousCompact && savings < s.config.MinSavingsBytes {
			decision.Reason = "hysteresis_compact"
		} else {
			decision.Reason = "predicted_savings"
		}
		s.previousCompact = true
		return decision
	}
	decision.Reason = "replay_cheaper"
	s.previousCompact = false
	return decision
}
