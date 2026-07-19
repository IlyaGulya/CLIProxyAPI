package executor

import (
	"context"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

type codexTranslationRequest struct {
	ctx             context.Context
	to              translator.Format
	responseFormat  translator.Format
	model           string
	originalPayload []byte
	clientBody      []byte
	clientPayload   []byte
	parameter       *any
	delivery        *codexStreamDelivery
	terminal        bool
}

type codexTranslationResult struct {
	duration  time.Duration
	chunks    int64
	delivered bool
}

func deliverCodexTranslation(request codexTranslationRequest) codexTranslationResult {
	started := time.Now()
	line := encodeCodexWebsocketAsSSE(request.clientPayload)
	chunks := translator.TranslateStream(request.ctx, request.to, request.responseFormat, request.model, request.originalPayload, request.clientBody, line, request.parameter)
	result := codexTranslationResult{duration: time.Since(started), chunks: int64(len(chunks)), delivered: true}
	for _, chunk := range chunks {
		if !request.delivery.send(cliproxyexecutor.StreamChunk{Payload: chunk}) {
			result.delivered = false
			return result
		}
	}
	if request.terminal {
		result.delivered = request.delivery.flush(codexCommitTerminal)
	}
	return result
}
