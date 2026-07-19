package executor

import (
	"bytes"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

type codexWebsocketSemanticState struct {
	lastEventType     string
	toolCallsStarted  int64
	toolCallsComplete int64
}

func (s *codexWebsocketSemanticState) observe(payload []byte) {
	if s == nil {
		return
	}
	eventType := strings.TrimSpace(gjson.GetBytes(payload, "type").String())
	if eventType == "" {
		return
	}
	s.lastEventType = eventType
	itemType := strings.TrimSpace(gjson.GetBytes(payload, "item.type").String())
	if itemType != "function_call" && itemType != "custom_tool_call" {
		return
	}
	switch eventType {
	case "response.output_item.added":
		s.toolCallsStarted++
	case "response.output_item.done":
		s.toolCallsComplete++
	}
}

func (s codexWebsocketSemanticState) incompleteToolCalls() int64 {
	incomplete := s.toolCallsStarted - s.toolCallsComplete
	if incomplete < 0 {
		return 0
	}
	return incomplete
}

// codexWebsocketStreamBridge owns protocol semantics that span multiple
// upstream frames and the irreversible downstream commit boundary. It does not
// own sockets, retries, or sessions.
type codexWebsocketStreamBridge struct {
	semantic            codexWebsocketSemanticState
	downstreamCommitted bool
	transaction         codexTransactionalStream
}

type codexTransactionalStream struct {
	state     codexTransactionalState
	maxBytes  int
	chunks    []cliproxyexecutor.StreamChunk
	bytes     int
	startedAt time.Time
}

type codexTransactionalState uint8

const (
	codexTransactionDisabled codexTransactionalState = iota
	codexTransactionBuffering
	codexTransactionPassthrough
	codexTransactionCommitted
	codexTransactionDiscarded
)

type codexTransactionalStage struct {
	Buffered bool
	Overflow bool
}

type codexTransactionalDrain struct {
	Chunks   []cliproxyexecutor.StreamChunk
	Bytes    int
	Duration time.Duration
}

type codexWebsocketStreamSnapshot struct {
	DownstreamCommitted bool
	LastEventType       string
	ToolCallsStarted    int64
	ToolCallsCompleted  int64
	IncompleteToolCalls int64
}

func newCodexWebsocketStreamBridge(transactional bool) *codexWebsocketStreamBridge {
	state := codexTransactionDisabled
	if transactional {
		state = codexTransactionBuffering
	}
	return &codexWebsocketStreamBridge{transaction: codexTransactionalStream{
		state:     state,
		maxBytes:  codexTransactionalChildStreamMaxBytes,
		chunks:    make([]cliproxyexecutor.StreamChunk, 0, 64),
		startedAt: time.Now(),
	}}
}

func (b *codexWebsocketStreamBridge) observe(payload []byte) {
	if b != nil {
		b.semantic.observe(payload)
	}
}

func (b *codexWebsocketStreamBridge) commit(payload []byte) {
	if b != nil && len(payload) > 0 {
		b.downstreamCommitted = true
	}
}

func (b *codexWebsocketStreamBridge) resetUpstreamAttempt() {
	if b != nil {
		b.semantic = codexWebsocketSemanticState{}
		b.transaction.discard()
	}
}

func (b *codexWebsocketStreamBridge) stage(chunk cliproxyexecutor.StreamChunk) codexTransactionalStage {
	if b == nil || b.transaction.state != codexTransactionBuffering || chunk.Err != nil || len(chunk.Payload) == 0 {
		return codexTransactionalStage{}
	}
	if b.transaction.bytes+len(chunk.Payload) > b.transaction.maxBytes {
		return codexTransactionalStage{Overflow: true}
	}
	chunk.Payload = bytes.Clone(chunk.Payload)
	b.transaction.chunks = append(b.transaction.chunks, chunk)
	b.transaction.bytes += len(chunk.Payload)
	return codexTransactionalStage{Buffered: true}
}

func (b *codexWebsocketStreamBridge) drain() codexTransactionalDrain {
	if b == nil {
		return codexTransactionalDrain{}
	}
	return b.transaction.drain()
}

func (b *codexWebsocketStreamBridge) discardTransaction() int {
	if b == nil || b.transaction.state != codexTransactionBuffering {
		return 0
	}
	discarded := b.transaction.discard()
	b.transaction.state = codexTransactionDiscarded
	return discarded
}

func (b *codexWebsocketStreamBridge) enterPassthrough() {
	if b != nil && b.transaction.state == codexTransactionBuffering {
		b.transaction.state = codexTransactionPassthrough
	}
}

func (b *codexWebsocketStreamBridge) commitTransaction() {
	if b != nil && b.transaction.state == codexTransactionBuffering {
		b.transaction.state = codexTransactionCommitted
	}
}

func (b *codexWebsocketStreamBridge) transactionState() codexTransactionalState {
	if b == nil {
		return codexTransactionDisabled
	}
	return b.transaction.state
}

func (b *codexWebsocketStreamBridge) transactionEnabled() bool {
	return b != nil && b.transaction.state == codexTransactionBuffering
}

func (b *codexWebsocketStreamBridge) bufferedBytes() int {
	if b == nil {
		return 0
	}
	return b.transaction.bytes
}

func (t *codexTransactionalStream) drain() codexTransactionalDrain {
	drain := codexTransactionalDrain{Chunks: t.chunks, Bytes: t.bytes, Duration: time.Since(t.startedAt)}
	t.chunks = make([]cliproxyexecutor.StreamChunk, 0, 64)
	t.bytes = 0
	return drain
}

func (t *codexTransactionalStream) discard() int {
	discardedBytes := t.bytes
	t.chunks = t.chunks[:0]
	t.bytes = 0
	t.startedAt = time.Now()
	return discardedBytes
}

func (b *codexWebsocketStreamBridge) snapshot() codexWebsocketStreamSnapshot {
	if b == nil {
		return codexWebsocketStreamSnapshot{}
	}
	return codexWebsocketStreamSnapshot{
		DownstreamCommitted: b.downstreamCommitted,
		LastEventType:       b.semantic.lastEventType,
		ToolCallsStarted:    b.semantic.toolCallsStarted,
		ToolCallsCompleted:  b.semantic.toolCallsComplete,
		IncompleteToolCalls: b.semantic.incompleteToolCalls(),
	}
}
