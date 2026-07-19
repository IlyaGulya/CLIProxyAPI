package executor

import (
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

type codexTransactionalStreamPolicy uint8

const (
	codexTransactionalStreamDisabled codexTransactionalStreamPolicy = iota
	codexTransactionalRootUntilSemanticOutput
	codexTransactionalChildUntilTerminal
)

func codexTransactionalPolicy(source, agentID string, downstreamWebsocket bool) codexTransactionalStreamPolicy {
	if source != "claude" || downstreamWebsocket {
		return codexTransactionalStreamDisabled
	}
	if strings.TrimSpace(agentID) == "" {
		return codexTransactionalRootUntilSemanticOutput
	}
	return codexTransactionalChildUntilTerminal
}

func shouldUseCodexTransactionalStream(source, agentID string, downstreamWebsocket bool) bool {
	return codexTransactionalPolicy(source, agentID, downstreamWebsocket) != codexTransactionalStreamDisabled
}

func isCodexSemanticOutputBoundary(payload []byte) bool {
	switch gjson.GetBytes(payload, "type").String() {
	case "response.output_text.delta":
		return gjson.GetBytes(payload, "delta").String() != ""
	case "response.output_item.added":
		itemType := strings.TrimSpace(gjson.GetBytes(payload, "item.type").String())
		return itemType != "" && itemType != "reasoning" && itemType != "message"
	default:
		return false
	}
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
