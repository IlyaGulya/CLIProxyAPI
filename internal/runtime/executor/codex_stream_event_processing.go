package executor

type codexProcessedStreamEvent struct {
	event    codexStreamEvent
	terminal bool
	err      error
}

func processCodexStreamEvent(payload []byte) codexProcessedStreamEvent {
	event := classifyCodexStreamEvent(payload)
	if streamErr, ok := parseCodexWebsocketError(payload); ok {
		return codexProcessedStreamEvent{event: event, terminal: true, err: streamErr}
	}
	return codexProcessedStreamEvent{
		event:    event,
		terminal: isCodexCompletionEvent(event.Type) || event.Type == "error",
	}
}
