package executor

type codexStreamExecution struct {
	attempt          *codexAttemptMachine
	reason           string
	err              error
	transportRetries int
	closeCode        int
}

func newCodexStreamExecution(attempt *codexAttemptMachine) *codexStreamExecution {
	return &codexStreamExecution{attempt: attempt, reason: "completed"}
}

func (s *codexStreamExecution) fail(reason string, err error) {
	if s == nil {
		return
	}
	s.reason, s.err = reason, err
}

func (s *codexStreamExecution) retry() int {
	s.transportRetries++
	return s.transportRetries
}

func (s *codexStreamExecution) finalizeAttempt() {
	if s == nil || s.attempt == nil || s.attempt.current().terminal() {
		return
	}
	event := codexAttemptFailedEvent
	if s.reason == "completed" {
		event = codexAttemptTerminalReceived
	} else if s.reason == "context_done" {
		event = codexAttemptCancelledEvent
	}
	_, _ = s.attempt.apply(event)
}
