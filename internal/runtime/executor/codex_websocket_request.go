package executor

import (
	"context"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/sjson"
)

type preparedCodexWebsocketRequest struct {
	baseModel             string
	apiKey                string
	from                  sdktranslator.Format
	to                    sdktranslator.Format
	responseFormat        sdktranslator.Format
	originalPayloadSource []byte
	originalPayload       []byte
	body                  []byte
	wsURL                 string
	headers               http.Header
	replayScope           codexReasoningReplayScope
}

func (e *CodexWebsocketsExecutor) prepareWebsocketRequest(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, streaming bool) (preparedCodexWebsocketRequest, error) {
	prepared := preparedCodexWebsocketRequest{baseModel: thinking.ParseSuffix(req.Model).ModelName, from: opts.SourceFormat, to: sdktranslator.FromString("codex"), responseFormat: cliproxyexecutor.ResponseFormatOrSource(opts)}
	prepared.apiKey, _ = codexCreds(auth)
	_, baseURL := codexCreds(auth)
	if baseURL == "" {
		baseURL = "https://chatgpt.com/backend-api/codex"
	}
	prepared.originalPayloadSource = req.Payload
	if len(opts.OriginalRequest) > 0 {
		prepared.originalPayloadSource = opts.OriginalRequest
	}
	prepared.originalPayload = prepared.originalPayloadSource
	originalTranslated, body := translateCodexRequestPair(prepared.from, prepared.to, prepared.baseModel, prepared.originalPayload, req.Payload, streaming)
	var err error
	body, err = thinking.ApplyThinking(body, req.Model, prepared.from.String(), prepared.to.String(), e.Identifier())
	if err != nil {
		return prepared, err
	}
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, prepared.baseModel, prepared.to.String(), prepared.from.String(), "", body, originalTranslated, helps.PayloadRequestedModel(opts, req.Model), helps.PayloadRequestPath(opts), opts.Headers)
	body, _ = sjson.SetBytes(body, "model", prepared.baseModel)
	body, _ = sjson.SetBytes(body, "stream", true)
	body, _ = sjson.DeleteBytes(body, "prompt_cache_retention")
	body, _ = sjson.DeleteBytes(body, "safety_identifier")
	if streaming {
		body, _ = sjson.DeleteBytes(body, "stream_options")
	}
	body = normalizeCodexInstructions(body)
	if e.cfg == nil || e.cfg.DisableImageGeneration == config.DisableImageGenerationOff {
		body = ensureImageGenerationTool(body, prepared.baseModel, auth, opts.Headers)
	}
	body = sanitizeOpenAIResponsesReasoningEncryptedContent(ctx, "codex websockets executor", body)
	if streaming {
		body = normalizeCodexParallelToolCallsForTools(body)
		body, prepared.replayScope, err = applyCodexReasoningReplayCacheRequired(ctx, prepared.from, req, opts, body)
		if err != nil {
			return prepared, err
		}
	}
	prepared.wsURL, err = buildCodexResponsesWebsocketURL(strings.TrimSuffix(baseURL, "/") + "/responses")
	if err != nil {
		return prepared, err
	}
	authID := ""
	if auth != nil {
		authID = auth.ID
	}
	body, prepared.headers, err = applyCodexPromptCacheHeadersWithContext(ctx, prepared.from, req, body, authID)
	if err != nil {
		return prepared, err
	}
	prepared.body = canonicalizeCodexCacheableRequest(body)
	prepared.headers = applyCodexWebsocketHeaders(ctx, prepared.headers, auth, prepared.apiKey, e.cfg)
	applyModelHeaderOverrides(prepared.headers, prepared.baseModel)
	return prepared, nil
}
