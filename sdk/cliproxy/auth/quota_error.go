package auth

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"
)

type modelCooldownError struct {
	model           string
	retryIn         time.Duration
	retryAt         time.Time
	providerResetAt time.Time
	provider        string
}

func newModelCooldownError(model, provider string, retryIn time.Duration, providerReset ...time.Time) *modelCooldownError {
	if retryIn < 0 {
		retryIn = 0
	}
	var providerResetAt time.Time
	if len(providerReset) > 0 {
		providerResetAt = providerReset[0]
	}
	return &modelCooldownError{
		model:           model,
		provider:        provider,
		retryIn:         retryIn,
		retryAt:         time.Now().Add(retryIn),
		providerResetAt: providerResetAt,
	}
}

func (e *modelCooldownError) Error() string {
	modelName := e.model
	if modelName == "" {
		modelName = "requested model"
	}
	message := fmt.Sprintf("All credentials for model %s are cooling down", modelName)
	if e.provider != "" {
		message = fmt.Sprintf("%s via provider %s", message, e.provider)
	}
	retrySeconds := durationSeconds(e.retryIn)
	displayDuration := e.retryIn
	if displayDuration > 0 && displayDuration < time.Second {
		displayDuration = time.Second
	} else {
		displayDuration = displayDuration.Round(time.Second)
	}
	message = fmt.Sprintf("%s; proxy will recheck at %s (%d seconds)", message, e.retryAt.UTC().Format(time.RFC3339), retrySeconds)
	if !e.providerResetAt.IsZero() {
		message = fmt.Sprintf("%s; provider reported reset at %s", message, e.providerResetAt.UTC().Format(time.RFC3339))
	}
	providerResetAt := e.providerResetAt
	if providerResetAt.IsZero() {
		providerResetAt = e.retryAt
	}
	errorBody := map[string]any{
		"code":                "model_cooldown",
		"type":                "usage_limit_reached",
		"message":             message,
		"model":               e.model,
		"reset_time":          displayDuration.String(),
		"reset_seconds":       retrySeconds,
		"resets_at":           providerResetAt.Unix(),
		"resets_in_seconds":   durationSeconds(time.Until(providerResetAt)),
		"next_probe_at":       e.retryAt.Unix(),
		"retry_after_seconds": retrySeconds,
	}
	if !e.providerResetAt.IsZero() {
		errorBody["provider_reset_at"] = e.providerResetAt.Unix()
	}
	if e.provider != "" {
		errorBody["provider"] = e.provider
	}
	payload := map[string]any{"error": errorBody}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Sprintf(`{"error":{"code":"model_cooldown","message":"%s"}}`, message)
	}
	return string(data)
}

func (e *modelCooldownError) StatusCode() int {
	return http.StatusTooManyRequests
}

func (e *modelCooldownError) Headers() http.Header {
	headers := make(http.Header)
	headers.Set("Content-Type", "application/json")
	headers.Set("Retry-After", strconv.Itoa(durationSeconds(e.retryIn)))
	return headers
}

func durationSeconds(duration time.Duration) int {
	seconds := int(math.Ceil(duration.Seconds()))
	if seconds < 0 {
		return 0
	}
	return seconds
}
