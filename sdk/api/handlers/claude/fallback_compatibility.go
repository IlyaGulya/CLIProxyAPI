package claude

import (
	"context"
	"errors"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

// claudeFallbackEligibility mirrors the error classes Claude Code accepts for
// --fallback-model. The proxy does not choose or persist fallback models;
// Claude Code owns the ordered chain and retries the primary on every turn.
func claudeFallbackEligibility(errMsg *interfaces.ErrorMessage) (string, bool) {
	if errMsg == nil {
		return "", false
	}
	var coded interface{ ErrorCode() string }
	if errors.As(errMsg.Error, &coded) && coded.ErrorCode() == "circuit_open" {
		return "circuit_open", true
	}
	switch errMsg.StatusCode {
	case http.StatusUnauthorized:
		return "authentication", true
	case http.StatusForbidden:
		return "permission", true
	case http.StatusNotFound:
		return "model_not_found", true
	case http.StatusTooManyRequests:
		return "rate_limited", true
	case 529:
		return "overloaded", true
	default:
		if errMsg.StatusCode >= http.StatusInternalServerError {
			return "server_error", true
		}
		return "", false
	}
}

func normalizeClaudeFallbackError(errMsg *interfaces.ErrorMessage) {
	if errMsg == nil || errMsg.Error == nil {
		return
	}
	var unknown *handlers.UnknownModelError
	if errors.As(errMsg.Error, &unknown) {
		errMsg.StatusCode = http.StatusNotFound
	}
}

func recordClaudeFallbackEligibility(ctx context.Context, model string, errMsg *interfaces.ErrorMessage) {
	reason, eligible := claudeFallbackEligibility(errMsg)
	if !eligible {
		return
	}
	observability.RecordWebsocketMetric(ctx, "claude_fallback_eligible", "", "", map[string]any{
		"model":    model,
		"reason":   reason,
		"status":   errMsg.StatusCode,
		"eligible": true,
	}, false)
}
