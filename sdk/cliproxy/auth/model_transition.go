package auth

import "time"

type cooldownKind string

const (
	cooldownQuota             cooldownKind = "quota"
	cooldownCloudflare        cooldownKind = "cloudflare challenge"
	cooldownInvalidGrant      cooldownKind = "invalid_grant"
	cooldownUnauthorized      cooldownKind = "unauthorized"
	cooldownPaymentRequired   cooldownKind = "payment_required"
	cooldownNotFound          cooldownKind = "not_found"
	cooldownModelNotSupported cooldownKind = "model_not_supported"
)

type modelTransitionEffects struct {
	resume        bool
	suspend       bool
	suspendReason cooldownKind
	clearQuota    bool
	setQuota      bool
}

func applyModelResult(auth *Auth, state *ModelState, result Result, now time.Time, disableCooling bool) modelTransitionEffects {
	var effects modelTransitionEffects
	if auth == nil || state == nil {
		return effects
	}
	if result.Success {
		staleRevision := result.RevisionKnown && result.StateRevision != state.Revision
		staleTimestamp := !result.RevisionKnown && !result.StartedAt.IsZero() && !modelStateIsClean(state) && state.UpdatedAt.After(result.StartedAt)
		if !staleRevision && !staleTimestamp {
			resetModelState(state, now)
			updateAggregatedAvailability(auth, now)
			if !hasModelError(auth, now) {
				auth.LastError = nil
				auth.StatusMessage = ""
				auth.Status = StatusActive
			}
			effects.resume = true
			effects.clearQuota = true
		}
		auth.UpdatedAt = now
		return effects
	}
	if isRequestScopedResultError(result.Error) {
		return effects
	}

	state.Revision++
	state.Unavailable = true
	state.Status = StatusError
	state.UpdatedAt = now
	if result.Error != nil {
		state.LastError = cloneError(result.Error)
		state.StatusMessage = result.Error.Message
		auth.LastError = cloneError(result.Error)
		auth.StatusMessage = result.Error.Message
	}

	statusCode := statusCodeFromResult(result.Error)
	suspend := func(reason cooldownKind, retryAfter time.Duration) {
		if disableCooling {
			state.NextRetryAfter = time.Time{}
			return
		}
		state.NextRetryAfter = now.Add(retryAfter)
		effects.suspend = true
		effects.suspendReason = reason
	}
	switch {
	case isModelSupportResultError(result.Error):
		state.NextRetryAfter = now.Add(12 * time.Hour)
		effects.suspend = true
		effects.suspendReason = cooldownModelNotSupported
	case isCloudflareChallengeResultError(result.Error):
		next, backoffLevel := nextCloudflareCooldown(state.Quota.BackoffLevel, disableCooling, now)
		state.NextRetryAfter = next
		state.StatusMessage = string(cooldownCloudflare)
		if auth.LastError != nil {
			auth.StatusMessage = string(cooldownCloudflare)
		}
		state.Quota = QuotaState{Exceeded: true, Reason: string(cooldownCloudflare), NextRecoverAt: next, BackoffLevel: backoffLevel}
	case isInvalidGrantResultError(result.Error):
		suspend(cooldownInvalidGrant, 30*time.Minute)
	case statusCode == 401:
		suspend(cooldownUnauthorized, 30*time.Minute)
	case statusCode == 402 || statusCode == 403:
		suspend(cooldownPaymentRequired, 30*time.Minute)
	case statusCode == 404:
		suspend(cooldownNotFound, 12*time.Hour)
	case statusCode == 429:
		state.Quota, state.NextRetryAfter = openQuotaCircuit(state.Quota, result.RetryAfter, now, disableCooling)
		if !disableCooling {
			effects.suspend = true
			effects.suspendReason = cooldownQuota
			effects.setQuota = true
		}
	case statusCode == 408 || statusCode == 500 || statusCode == 502 || statusCode == 503 || statusCode == 504:
		if disableCooling {
			state.NextRetryAfter = time.Time{}
		} else {
			state.NextRetryAfter = nextTransientErrorRetryAfter(now)
		}
	default:
		state.NextRetryAfter = time.Time{}
	}

	auth.Status = StatusError
	auth.UpdatedAt = now
	updateAggregatedAvailability(auth, now)
	return effects
}
