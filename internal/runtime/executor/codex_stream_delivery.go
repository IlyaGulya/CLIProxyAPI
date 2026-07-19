package executor

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

var (
	errCodexStreamDelivererRequired = errors.New("codex stream deliverer is required")
	errCodexTransactionalTransition = errors.New("invalid codex transactional stream event")
)

type codexTransactionalState uint8

const (
	codexTransactionDisabled codexTransactionalState = iota
	codexTransactionBuffering
	codexTransactionPassthrough
	codexTransactionCommitted
	codexTransactionDiscarded
)

func (s codexTransactionalState) String() string {
	switch s {
	case codexTransactionDisabled:
		return "disabled"
	case codexTransactionBuffering:
		return "buffering"
	case codexTransactionPassthrough:
		return "passthrough"
	case codexTransactionCommitted:
		return "committed"
	case codexTransactionDiscarded:
		return "discarded"
	default:
		return "unknown"
	}
}

type codexTransactionalEvent uint8

const (
	codexTransactionCommit codexTransactionalEvent = iota
	codexTransactionDiscard
	codexTransactionOverflow
	codexTransactionRetryReset
)

func (e codexTransactionalEvent) String() string {
	switch e {
	case codexTransactionCommit:
		return "commit"
	case codexTransactionDiscard:
		return "discard"
	case codexTransactionOverflow:
		return "overflow"
	case codexTransactionRetryReset:
		return "retry_reset"
	default:
		return "unknown"
	}
}

func reduceCodexTransaction(state codexTransactionalState, event codexTransactionalEvent) (codexTransactionalState, error) {
	switch event {
	case codexTransactionCommit:
		if state == codexTransactionBuffering {
			return codexTransactionCommitted, nil
		}
	case codexTransactionDiscard:
		if state == codexTransactionBuffering {
			return codexTransactionDiscarded, nil
		}
	case codexTransactionOverflow:
		if state == codexTransactionBuffering {
			return codexTransactionPassthrough, nil
		}
	case codexTransactionRetryReset:
		if state == codexTransactionDisabled || state == codexTransactionBuffering || state == codexTransactionPassthrough {
			return state, nil
		}
	}
	return state, fmt.Errorf("%w: %s + %s", errCodexTransactionalTransition, state, event)
}

type codexStageOutcome uint8

const (
	codexStagePassthrough codexStageOutcome = iota
	codexStageBuffered
	codexStageOverflow
)

func (o codexStageOutcome) String() string {
	switch o {
	case codexStagePassthrough:
		return "passthrough"
	case codexStageBuffered:
		return "buffered"
	case codexStageOverflow:
		return "overflow"
	default:
		return "unknown"
	}
}

type codexTransactionalDrain struct {
	Chunks   []cliproxyexecutor.StreamChunk
	Bytes    int
	Duration time.Duration
	Boundary codexStreamCommitBoundary
}

type codexTransactionalStream struct {
	state     codexTransactionalState
	maxBytes  int
	chunks    []cliproxyexecutor.StreamChunk
	bytes     int
	startedAt time.Time
}

func newCodexTransactionalStream(policy codexTransactionalStreamPolicy) codexTransactionalStream {
	state := codexTransactionDisabled
	if policy.enabled() {
		state = codexTransactionBuffering
	}
	return codexTransactionalStream{
		state: state, maxBytes: policy.maxBytes,
		chunks: make([]cliproxyexecutor.StreamChunk, 0, 64), startedAt: time.Now(),
	}
}

func (t *codexTransactionalStream) apply(event codexTransactionalEvent) error {
	next, err := reduceCodexTransaction(t.state, event)
	if err != nil {
		return err
	}
	t.state = next
	return nil
}

func (t *codexTransactionalStream) stage(chunk cliproxyexecutor.StreamChunk) codexStageOutcome {
	if t.state != codexTransactionBuffering || len(chunk.Payload) == 0 {
		return codexStagePassthrough
	}
	if t.bytes+len(chunk.Payload) > t.maxBytes {
		return codexStageOverflow
	}
	chunk.Payload = bytes.Clone(chunk.Payload)
	t.chunks = append(t.chunks, chunk)
	t.bytes += len(chunk.Payload)
	return codexStageBuffered
}

func (t *codexTransactionalStream) drain(boundary codexStreamCommitBoundary) codexTransactionalDrain {
	drain := codexTransactionalDrain{Chunks: t.chunks, Bytes: t.bytes, Duration: time.Since(t.startedAt), Boundary: boundary}
	t.chunks = make([]cliproxyexecutor.StreamChunk, 0, 64)
	t.bytes = 0
	return drain
}

func (t *codexTransactionalStream) discardBuffer() int {
	discardedBytes := t.bytes
	t.chunks = t.chunks[:0]
	t.bytes = 0
	t.startedAt = time.Now()
	return discardedBytes
}

type codexStreamDeliveryHooks struct {
	Committed  func(codexTransactionalDrain)
	Discarded  func(int, error)
	Overflowed func(int)
}

// codexStreamDelivery exclusively owns transactional buffering and the order
// in which buffered chunks cross the irreversible downstream commit boundary.
type codexStreamDelivery struct {
	deliver     func(cliproxyexecutor.StreamChunk) bool
	hooks       codexStreamDeliveryHooks
	policy      codexTransactionalStreamPolicy
	transaction codexTransactionalStream
}

func newCodexStreamDelivery(policy codexTransactionalStreamPolicy, deliver func(cliproxyexecutor.StreamChunk) bool, hooks codexStreamDeliveryHooks) (*codexStreamDelivery, error) {
	if deliver == nil {
		return nil, errCodexStreamDelivererRequired
	}
	return &codexStreamDelivery{
		deliver: deliver, hooks: hooks, policy: policy,
		transaction: newCodexTransactionalStream(policy),
	}, nil
}

func (d *codexStreamDelivery) apply(event codexTransactionalEvent) bool {
	if err := d.transaction.apply(event); err != nil {
		_ = d.deliver(cliproxyexecutor.StreamChunk{Err: err})
		return false
	}
	return true
}

func (d *codexStreamDelivery) flush(boundary codexStreamCommitBoundary) bool {
	drain := d.transaction.drain(boundary)
	for i := range drain.Chunks {
		if !d.deliver(drain.Chunks[i]) {
			return false
		}
	}
	if drain.Bytes > 0 && d.hooks.Committed != nil {
		d.hooks.Committed(drain)
	}
	if d.transaction.state == codexTransactionBuffering {
		return d.apply(codexTransactionCommit)
	}
	return true
}

func (d *codexStreamDelivery) send(chunk cliproxyexecutor.StreamChunk) bool {
	if chunk.Err != nil {
		if d.transaction.state == codexTransactionBuffering {
			discardedBytes := d.transaction.discardBuffer()
			if !d.apply(codexTransactionDiscard) {
				return false
			}
			if discardedBytes > 0 && d.hooks.Discarded != nil {
				d.hooks.Discarded(discardedBytes, chunk.Err)
			}
		}
		return d.deliver(chunk)
	}
	switch d.transaction.stage(chunk) {
	case codexStageBuffered:
		return true
	case codexStageOverflow:
		if d.hooks.Overflowed != nil {
			d.hooks.Overflowed(d.transaction.bytes + len(chunk.Payload))
		}
		if d.policy.overflowAction == codexOverflowFail {
			return d.send(cliproxyexecutor.StreamChunk{Err: fmt.Errorf("codex transactional stream exceeded %d-byte buffer", d.transaction.maxBytes)})
		}
		if !d.apply(codexTransactionOverflow) {
			return false
		}
		if !d.flush(codexCommitBufferLimit) {
			return false
		}
	}
	return d.deliver(chunk)
}

func (d *codexStreamDelivery) resetUpstreamAttempt() error {
	if d.transaction.state == codexTransactionBuffering {
		d.transaction.discardBuffer()
	}
	return d.transaction.apply(codexTransactionRetryReset)
}

func (d *codexStreamDelivery) state() codexTransactionalState { return d.transaction.state }

func (d *codexStreamDelivery) buffering() bool {
	return d.transaction.state == codexTransactionBuffering
}

func (d *codexStreamDelivery) bufferedBytes() int { return d.transaction.bytes }
