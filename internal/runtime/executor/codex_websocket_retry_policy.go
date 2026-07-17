package executor

import (
	"context"
	"errors"
	"io"
)

type codexRetryAction string

const (
	codexRetryStop      codexRetryAction = "stop"
	codexRetryReconnect codexRetryAction = "reconnect"
)

type codexRetryReason string

const (
	codexRetryTemporaryNetwork    codexRetryReason = "temporary_network"
	codexRetryAbnormalClose       codexRetryReason = "abnormal_close"
	codexRetryDownstreamCommitted codexRetryReason = "downstream_committed"
	codexRetryBudgetExhausted     codexRetryReason = "budget_exhausted"
	codexRetryCancelled           codexRetryReason = "cancelled"
	codexRetryNotRetryable        codexRetryReason = "not_retryable"
)

type codexRetryBoundary string

const (
	codexRetryBeforeCommit codexRetryBoundary = "before_downstream_commit"
	codexRetryAfterCommit  codexRetryBoundary = "after_downstream_commit"
)

type codexRetryInput struct {
	Err                 error
	DownstreamCommitted bool
	Attempts            int
	MaxAttempts         int
}

type codexRetryDecision struct {
	Action   codexRetryAction
	Reason   codexRetryReason
	Boundary codexRetryBoundary
}

func decideCodexRetry(input codexRetryInput) codexRetryDecision {
	boundary := codexRetryBeforeCommit
	if input.DownstreamCommitted {
		boundary = codexRetryAfterCommit
		return codexRetryDecision{Action: codexRetryStop, Reason: codexRetryDownstreamCommitted, Boundary: boundary}
	}
	if errors.Is(input.Err, context.Canceled) || errors.Is(input.Err, context.DeadlineExceeded) {
		return codexRetryDecision{Action: codexRetryStop, Reason: codexRetryCancelled, Boundary: boundary}
	}
	maxAttempts := input.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}
	if input.Attempts >= maxAttempts {
		return codexRetryDecision{Action: codexRetryStop, Reason: codexRetryBudgetExhausted, Boundary: boundary}
	}
	if !shouldRetryCodexWebsocketReadError(input.Err) {
		return codexRetryDecision{Action: codexRetryStop, Reason: codexRetryNotRetryable, Boundary: boundary}
	}
	reason := codexRetryTemporaryNetwork
	if codexWebsocketCloseCode(input.Err) == 1006 || errors.Is(input.Err, io.ErrUnexpectedEOF) {
		reason = codexRetryAbnormalClose
	}
	return codexRetryDecision{Action: codexRetryReconnect, Reason: reason, Boundary: boundary}
}
