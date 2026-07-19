package executor

import (
	"net/http"
	"strings"
)

func shouldUseCodexTransactionalStream(source, agentID string, downstreamWebsocket bool) bool {
	return source == "claude" && strings.TrimSpace(agentID) != "" && !downstreamWebsocket
}

func isCodexCompletionEvent(eventType string) bool {
	switch eventType {
	case "response.completed", "response.done", "response.incomplete":
		return true
	default:
		return false
	}
}

func codexPartialResponseInterruptedError() error {
	return statusErr{code: http.StatusServiceUnavailable, msg: `{"error":{"message":"upstream transport interrupted before the workflow response committed; retry the workflow turn","type":"server_error","code":"partial_response_interrupted"}}`}
}
