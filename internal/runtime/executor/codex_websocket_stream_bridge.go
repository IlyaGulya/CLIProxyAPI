package executor

type codexWebsocketSemanticState struct {
	lastEventType     string
	toolCallsStarted  int64
	toolCallsComplete int64
}

func (s *codexWebsocketSemanticState) observe(event codexStreamEvent) {
	if s == nil {
		return
	}
	if event.Type == "" {
		return
	}
	s.lastEventType = event.Type
	switch event.ToolTransition {
	case codexToolStarted:
		s.toolCallsStarted++
	case codexToolCompleted:
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
}

type codexWebsocketStreamSnapshot struct {
	DownstreamCommitted bool
	LastEventType       string
	ToolCallsStarted    int64
	ToolCallsCompleted  int64
	IncompleteToolCalls int64
}

func newCodexWebsocketStreamBridge() *codexWebsocketStreamBridge {
	return &codexWebsocketStreamBridge{}
}

func (b *codexWebsocketStreamBridge) observe(event codexStreamEvent) {
	if b != nil {
		b.semantic.observe(event)
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
	}
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
