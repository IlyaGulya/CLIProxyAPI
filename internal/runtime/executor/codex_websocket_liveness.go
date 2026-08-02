package executor

import (
	"fmt"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

const codexResponsesWebsocketFirstProgressTimeout = 30 * time.Second

type codexStreamLivenessState uint8

const (
	codexLivenessAwaitingProgress codexStreamLivenessState = iota
	codexLivenessStreaming
)

type codexStreamLiveness struct {
	state                 codexStreamLivenessState
	firstProgressTimeout  time.Duration
	idleTimeout           time.Duration
	firstProgressDeadline time.Time
	now                   func() time.Time
}

func newCodexStreamLiveness(cfg *config.Config) codexStreamLiveness {
	return newCodexStreamLivenessWithClock(cfg, time.Now)
}

func newCodexStreamLivenessWithClock(cfg *config.Config, now func() time.Time) codexStreamLiveness {
	if now == nil {
		now = time.Now
	}
	idleTimeout := codexWebsocketIdleTimeout(cfg)
	firstProgressTimeout := codexResponsesWebsocketFirstProgressTimeout
	if cfg != nil && cfg.Streaming.FirstProgressTimeoutSeconds > 0 {
		firstProgressTimeout = time.Duration(cfg.Streaming.FirstProgressTimeoutSeconds) * time.Second
	}
	if idleTimeout > 0 && idleTimeout < firstProgressTimeout {
		firstProgressTimeout = idleTimeout
	}
	liveness := codexStreamLiveness{
		state:                codexLivenessAwaitingProgress,
		firstProgressTimeout: firstProgressTimeout,
		idleTimeout:          idleTimeout,
		now:                  now,
	}
	liveness.resetAttempt()
	return liveness
}

func (l *codexStreamLiveness) resetAttempt() {
	l.resetAttemptAt(l.now())
}

func (l *codexStreamLiveness) resetAttemptAt(startedAt time.Time) {
	l.state = codexLivenessAwaitingProgress
	l.firstProgressDeadline = startedAt.Add(l.firstProgressTimeout)
}

func (l *codexStreamLiveness) observe(event codexStreamEvent) {
	if l.state == codexLivenessAwaitingProgress && isCodexProgressEvent(event) {
		l.state = codexLivenessStreaming
	}
}

func (l codexStreamLiveness) timeout() time.Duration {
	if l.state == codexLivenessAwaitingProgress {
		return l.firstProgressDeadline.Sub(l.now())
	}
	return l.idleTimeout
}

func (l codexStreamLiveness) progressStarted() bool {
	return l.state == codexLivenessStreaming
}

func (l codexStreamLiveness) mapTimeout(err error) error {
	if l.state == codexLivenessAwaitingProgress && isTemporaryCodexWebsocketNetworkError(err) {
		return codexFirstProgressTimeoutError{timeout: l.firstProgressTimeout}
	}
	return err
}

func isCodexProgressEvent(event codexStreamEvent) bool {
	switch event.Kind {
	case codexStreamReasoning, codexStreamText, codexStreamTool, codexStreamTerminal:
		return true
	default:
		return false
	}
}

type codexFirstProgressTimeoutError struct {
	timeout time.Duration
}

func (e codexFirstProgressTimeoutError) Error() string {
	return fmt.Sprintf("codex websockets executor: first upstream progress timeout after %s", e.timeout)
}

func (codexFirstProgressTimeoutError) Timeout() bool   { return true }
func (codexFirstProgressTimeoutError) Temporary() bool { return true }

type codexWebsocketReadTimeoutError struct {
	timeout time.Duration
}

func (e codexWebsocketReadTimeoutError) Error() string {
	return fmt.Sprintf("codex websockets executor: upstream idle timeout after %s", e.timeout)
}

func (codexWebsocketReadTimeoutError) Timeout() bool   { return true }
func (codexWebsocketReadTimeoutError) Temporary() bool { return true }
