package executor

import "time"

const codexReconnectVisibilityThreshold = 750 * time.Millisecond

type codexReconnectStatusInput struct {
	Attempt     int
	MaxAttempts int
	Elapsed     time.Duration
	Recovered   bool
	Terminal    bool
	CircuitOpen bool
}

type codexReconnectStatusDecision struct {
	Visible           bool
	Status            string
	SuppressionReason string
}

func decideCodexReconnectStatus(input codexReconnectStatusInput) codexReconnectStatusDecision {
	switch {
	case input.CircuitOpen:
		return codexReconnectStatusDecision{Visible: true, Status: "https_fallback"}
	case input.Terminal:
		return codexReconnectStatusDecision{Visible: true, Status: "failed"}
	case input.Recovered:
		if input.Attempt <= 1 && input.Elapsed < codexReconnectVisibilityThreshold {
			return codexReconnectStatusDecision{Status: "recovered", SuppressionReason: "first_quick_reconnect"}
		}
		return codexReconnectStatusDecision{Visible: true, Status: "recovered"}
	case input.Attempt >= 2 || input.Elapsed >= codexReconnectVisibilityThreshold:
		return codexReconnectStatusDecision{Visible: true, Status: "reconnecting"}
	default:
		return codexReconnectStatusDecision{Status: "reconnecting", SuppressionReason: "first_quick_reconnect"}
	}
}
