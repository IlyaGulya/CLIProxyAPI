package auth

import "time"

const (
	quotaBackoffBase          = time.Second
	quotaRevalidationInterval = 15 * time.Minute
	quotaBackoffMax           = 30 * time.Minute
)

func openQuotaCircuit(previous QuotaState, providerRetryAfter *time.Duration, now time.Time, disableCooling bool) (QuotaState, time.Time) {
	nextProbeAt := time.Time{}
	providerResetAt := time.Time{}
	backoffLevel := previous.BackoffLevel
	if !disableCooling {
		if providerRetryAfter != nil {
			providerResetAt = now.Add(*providerRetryAfter)
			nextProbeAt = now.Add(quotaRetryAfter(*providerRetryAfter))
		} else {
			nextProbeAt, backoffLevel = quotaCooldownAfterFailure(previous, now)
		}
	}
	return QuotaState{
		Exceeded:        true,
		Reason:          string(cooldownQuota),
		NextRecoverAt:   nextProbeAt,
		NextProbeAt:     nextProbeAt,
		ProviderResetAt: providerResetAt,
		Phase:           QuotaPhaseOpen,
		BackoffLevel:    backoffLevel,
	}, nextProbeAt
}

// quotaCooldownAfterFailure returns the recovery deadline and backoff level for
// a quota failure observed at now. Concurrent failures reuse an open window.
func quotaCooldownAfterFailure(quota QuotaState, now time.Time) (time.Time, int) {
	if quota.NextRecoverAt.After(now) {
		return quota.NextRecoverAt, quota.BackoffLevel
	}
	cooldown, nextLevel := nextQuotaCooldown(quota.BackoffLevel, false)
	if cooldown <= 0 {
		return time.Time{}, nextLevel
	}
	return now.Add(cooldown), nextLevel
}

func quotaRetryAfter(providerHint time.Duration) time.Duration {
	if providerHint > quotaRevalidationInterval {
		return quotaRevalidationInterval
	}
	return providerHint
}

func nextQuotaCooldown(previousLevel int, disableCooling bool) (time.Duration, int) {
	if previousLevel < 0 {
		previousLevel = 0
	}
	if disableCooling {
		return 0, previousLevel
	}
	cooldown := quotaBackoffBase * time.Duration(1<<previousLevel)
	if cooldown < quotaBackoffBase {
		cooldown = quotaBackoffBase
	}
	if cooldown >= quotaBackoffMax {
		return quotaBackoffMax, previousLevel
	}
	return cooldown, previousLevel + 1
}
