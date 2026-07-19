package executor

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/tidwall/gjson"
)

func codexTransactionDiscardReason(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "context_done"
	}
	return "terminal_error"
}

type codexTransactionalStreamMode uint8

const (
	codexTransactionalStreamDisabled codexTransactionalStreamMode = iota
	codexTransactionalRootUntilSemanticOutput
	codexTransactionalChildUntilTerminal
)

const (
	codexTransactionalRootMaxBytes  = 8 << 20
	codexTransactionalChildMaxBytes = 8 << 20
)

type codexStreamCommitBoundary string

const (
	codexCommitSemanticOutput codexStreamCommitBoundary = "semantic_output"
	codexCommitTerminal       codexStreamCommitBoundary = "terminal"
	codexCommitBufferLimit    codexStreamCommitBoundary = "buffer_limit"
)

type codexTransactionalStreamPolicy struct {
	mode     codexTransactionalStreamMode
	maxBytes int
}

func newCodexTransactionalPolicy(source, agentID string, downstreamWebsocket bool) codexTransactionalStreamPolicy {
	if source != "claude" || downstreamWebsocket {
		return codexTransactionalStreamPolicy{mode: codexTransactionalStreamDisabled}
	}
	if strings.TrimSpace(agentID) == "" {
		return codexTransactionalStreamPolicy{mode: codexTransactionalRootUntilSemanticOutput, maxBytes: codexTransactionalRootMaxBytes}
	}
	return codexTransactionalStreamPolicy{mode: codexTransactionalChildUntilTerminal, maxBytes: codexTransactionalChildMaxBytes}
}

func (p codexTransactionalStreamPolicy) enabled() bool {
	return p.mode != codexTransactionalStreamDisabled
}

func (p codexTransactionalStreamPolicy) name() string {
	switch p.mode {
	case codexTransactionalRootUntilSemanticOutput:
		return "root_until_semantic_output"
	case codexTransactionalChildUntilTerminal:
		return "child_until_terminal"
	default:
		return "disabled"
	}
}

func (p codexTransactionalStreamPolicy) commitBefore(eventType string, payload []byte) (codexStreamCommitBoundary, bool) {
	if p.mode != codexTransactionalRootUntilSemanticOutput {
		return "", false
	}
	switch eventType {
	case "response.output_text.delta":
		return codexCommitSemanticOutput, gjson.GetBytes(payload, "delta").String() != ""
	case "response.output_item.added":
		itemType := strings.TrimSpace(gjson.GetBytes(payload, "item.type").String())
		return codexCommitSemanticOutput, itemType != "" && itemType != "reasoning" && itemType != "message"
	default:
		return "", false
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
