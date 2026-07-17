// Package claude provides HTTP handlers for Claude API code-related functionality.
// This package implements Claude-compatible streaming chat completions with sophisticated
// client rotation and quota management systems to ensure high availability and optimal
// resource utilization across multiple backend clients. It handles request translation
// between Claude API format and the underlying Gemini backend, providing seamless
// API compatibility while maintaining robust error handling and connection management.
package claude

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	. "github.com/router-for-me/CLIProxyAPI/v7/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ClaudeCodeAPIHandler contains the handlers for Claude API endpoints.
// It holds a pool of clients to interact with the backend service.
type ClaudeCodeAPIHandler struct {
	*handlers.BaseAPIHandler
}

// NewClaudeCodeAPIHandler creates a new Claude API handlers instance.
// It takes an BaseAPIHandler instance as input and returns a ClaudeCodeAPIHandler.
//
// Parameters:
//   - apiHandlers: The base API handler instance.
//
// Returns:
//   - *ClaudeCodeAPIHandler: A new Claude code API handler instance.
func NewClaudeCodeAPIHandler(apiHandlers *handlers.BaseAPIHandler) *ClaudeCodeAPIHandler {
	return &ClaudeCodeAPIHandler{
		BaseAPIHandler: apiHandlers,
	}
}

// HandlerType returns the identifier for this handler implementation.
func (h *ClaudeCodeAPIHandler) HandlerType() string {
	return Claude
}

// Models returns a list of models supported by this handler.
func (h *ClaudeCodeAPIHandler) Models() []map[string]any {
	// Get dynamic models from the global registry
	modelRegistry := registry.GetGlobalRegistry()
	return modelRegistry.GetAvailableModels("claude")
}

// ClaudeMessages handles Claude-compatible streaming chat completions.
// This function implements a sophisticated client rotation and quota management system
// to ensure high availability and optimal resource utilization across multiple backend clients.
//
// Parameters:
//   - c: The Gin context for the request.
func (h *ClaudeCodeAPIHandler) ClaudeMessages(c *gin.Context) {
	// Extract raw JSON data from the incoming request
	rawJSON, err := c.GetRawData()
	// If data retrieval fails, return a 400 Bad Request error.
	if err != nil {
		c.JSON(http.StatusBadRequest, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: fmt.Sprintf("Invalid request: %v", err),
				Type:    "invalid_request_error",
			},
		})
		return
	}

	// Decode claude-fable-5-dd-<reversed> model IDs back to the real model name for routing.
	rawJSON = rewriteClaudeDDModelInBody(rawJSON)
	repairedJSON, repairResult, errRepair := repairInterruptedClaudeToolHistory(rawJSON)
	if errRepair != nil {
		message := "Invalid tool history"
		if !errors.Is(errRepair, errAmbiguousClaudeToolHistory) {
			message = "Unable to validate tool history"
		}
		c.JSON(http.StatusBadRequest, claudeErrorResponse{Type: "error", Error: claudeErrorDetail{Message: message, Type: "invalid_request_error"}})
		return
	}
	rawJSON = repairedJSON
	if repairResult.Applied {
		observability.RecordWebsocketEvent(c.Request.Context(), observability.WebsocketEvent{
			Name: "tool_history_repaired",
			Attributes: observability.WebsocketAttributes{
				RepairedToolUses:           observability.Some(int64(repairResult.RepairedToolUses)),
				SeparatedAssistantMessages: observability.Some(int64(repairResult.SeparatedAssistantMessages)),
			},
		})
	}
	var editResult claudeContextEditResult
	rawJSON, editResult = applyClaudeContextEditing(rawJSON)
	if editResult.Applied {
		c.Set(claudeContextEditGinKey, editResult)
		observability.RecordWebsocketMetric(c.Request.Context(), "context_edit_applied", "", "", map[string]any{
			"cleared_tool_uses":    editResult.ClearedToolUses,
			"cleared_tool_results": editResult.ClearedToolResults,
			"cleared_input_tokens": editResult.ClearedInputTokens,
		}, false)
	}
	if pressure := claudeContextPressure(rawJSON); pressure.Overflow {
		observability.RecordWebsocketMetric(c.Request.Context(), "context_preflight_rejected", "", "", map[string]any{
			"model":                    gjson.GetBytes(rawJSON, "model").String(),
			"input_tokens":             pressure.EstimatedInput,
			"reserved_output_tokens":   pressure.ReservedOutput,
			"effective_context_window": pressure.EffectiveWindow,
			"estimation_method":        pressure.Method,
		}, false)
		c.JSON(http.StatusBadRequest, claudeErrorResponse{Type: "error", Error: claudeErrorDetail{
			Message: "Prompt is too long: input plus requested output exceeds this model's context window",
			Type:    "invalid_request_error",
		}})
		return
	}
	classifierModel := ""
	if h != nil && h.Cfg != nil {
		classifierModel = h.Cfg.ClaudeCodeAutoModeClassifierModel
	}
	if rewrittenJSON, rewritten := rewriteClaudeCodeAutoModeClassifierModel(rawJSON, classifierModel); rewritten {
		log.WithFields(log.Fields{
			"source_model": "claude-sonnet-5",
			"target_model": strings.TrimSpace(classifierModel),
		}).Debug("Claude Code auto-mode classifier model rewritten")
		rawJSON = rewrittenJSON
	}

	// Check if the client requested a streaming response.
	streamResult := gjson.GetBytes(rawJSON, "stream")
	if !streamResult.Exists() || streamResult.Type == gjson.False {
		h.handleNonStreamingResponse(c, rawJSON)
	} else {
		h.handleStreamingResponse(c, rawJSON)
	}
}

const claudeCodeAutoModeClassifierPrompt = "You are a security monitor for autonomous AI coding agents."

// rewriteClaudeCodeAutoModeClassifierModel reroutes only Claude Code's internal
// auto-mode safety classifier. The full signature avoids changing ordinary
// requests that happen to use the same client-visible model name.
func rewriteClaudeCodeAutoModeClassifierModel(rawJSON []byte, targetModel string) ([]byte, bool) {
	targetModel = strings.TrimSpace(targetModel)
	if targetModel == "" || targetModel == "claude-sonnet-5" {
		return rawJSON, false
	}
	if gjson.GetBytes(rawJSON, "model").String() != "claude-sonnet-5" ||
		gjson.GetBytes(rawJSON, "max_tokens").Int() != 64 ||
		gjson.GetBytes(rawJSON, "thinking.type").String() != "disabled" {
		return rawJSON, false
	}
	hasStopSequence := false
	for _, stop := range gjson.GetBytes(rawJSON, "stop_sequences").Array() {
		if stop.String() == "</block>" {
			hasStopSequence = true
			break
		}
	}
	if !hasStopSequence {
		return rawJSON, false
	}
	hasClassifierPrompt := false
	for _, block := range gjson.GetBytes(rawJSON, "system").Array() {
		if strings.HasPrefix(block.Get("text").String(), claudeCodeAutoModeClassifierPrompt) {
			hasClassifierPrompt = true
			break
		}
	}
	if !hasClassifierPrompt {
		return rawJSON, false
	}
	updated, errSet := sjson.SetBytes(rawJSON, "model", targetModel)
	if errSet != nil {
		return rawJSON, false
	}
	return updated, true
}

// ClaudeMessages handles Claude-compatible streaming chat completions.
// This function implements a sophisticated client rotation and quota management system
// to ensure high availability and optimal resource utilization across multiple backend clients.
//
// Parameters:
//   - c: The Gin context for the request.
func (h *ClaudeCodeAPIHandler) ClaudeCountTokens(c *gin.Context) {
	// Extract raw JSON data from the incoming request
	rawJSON, err := c.GetRawData()
	// If data retrieval fails, return a 400 Bad Request error.
	if err != nil {
		c.JSON(http.StatusBadRequest, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: fmt.Sprintf("Invalid request: %v", err),
				Type:    "invalid_request_error",
			},
		})
		return
	}

	// Decode claude-fable-5-dd-<reversed> model IDs back to the real model name for routing.
	rawJSON = rewriteClaudeDDModelInBody(rawJSON)
	repairedJSON, _, errRepair := repairInterruptedClaudeToolHistory(rawJSON)
	if errRepair != nil {
		c.JSON(http.StatusBadRequest, claudeErrorResponse{Type: "error", Error: claudeErrorDetail{Message: "Invalid tool history", Type: "invalid_request_error"}})
		return
	}
	rawJSON = repairedJSON
	rawJSON, _ = applyClaudeContextEditing(rawJSON)

	c.Header("Content-Type", "application/json")

	alt := h.GetAlt(c)
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())

	modelName := gjson.GetBytes(rawJSON, "model").String()

	resp, upstreamHeaders, errMsg := h.ExecuteCountWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, alt)
	if errMsg != nil {
		normalizeClaudeFallbackError(errMsg)
		recordClaudeFallbackEligibility(c.Request.Context(), modelName, errMsg)
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}
	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	_, _ = c.Writer.Write(resp)
	cliCancel()
}

// rewriteClaudeDDModelInBody decodes model IDs of the form claude-fable-5-dd-<reversed>
// back into the original model name used for routing and upstream requests.
func rewriteClaudeDDModelInBody(rawJSON []byte) []byte {
	modelName := gjson.GetBytes(rawJSON, "model").String()
	resolved := util.ResolveClaudeModelIDPrefix(modelName)
	if resolved == modelName {
		return rawJSON
	}
	updated, errSet := sjson.SetBytes(rawJSON, "model", resolved)
	if errSet != nil {
		return rawJSON
	}
	return updated
}

// ClaudeModels handles the Claude models listing endpoint.
// It returns a JSON response containing available Claude models and their specifications.
//
// Parameters:
//   - c: The Gin context for the request.
func (h *ClaudeCodeAPIHandler) ClaudeModels(c *gin.Context) {
	models := h.Models()
	for i := range models {
		if id, ok := models[i]["id"].(string); ok {
			models[i]["id"] = util.EnsureClaudeModelIDPrefix(id)
		}
	}
	sortClaudeModelsByDisplayName(models)
	start, end, ok := claudeModelPage(c, models)
	if !ok {
		return
	}
	hasMore := end < len(models)
	models = models[start:end]
	firstID := ""
	lastID := ""
	if len(models) > 0 {
		if id, ok := models[0]["id"].(string); ok {
			firstID = id
		}
		if id, ok := models[len(models)-1]["id"].(string); ok {
			lastID = id
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"data":     models,
		"has_more": hasMore,
		"first_id": firstID,
		"last_id":  lastID,
	})
}

// ClaudeModel retrieves one available model or returns an Anthropic-shaped 404.
func (h *ClaudeCodeAPIHandler) ClaudeModel(c *gin.Context) {
	requested := util.ResolveClaudeModelIDPrefix(c.Param("model_id"))
	for _, model := range h.Models() {
		id, _ := model["id"].(string)
		if id != requested && util.EnsureClaudeModelIDPrefix(id) != c.Param("model_id") {
			continue
		}
		model["id"] = util.EnsureClaudeModelIDPrefix(id)
		c.JSON(http.StatusOK, model)
		return
	}
	c.JSON(http.StatusNotFound, claudeErrorResponse{Type: "error", Error: claudeErrorDetail{Type: "not_found_error", Message: "Model not found: " + c.Param("model_id")}})
}

func claudeModelPage(c *gin.Context, models []map[string]any) (int, int, bool) {
	limit := 20
	if raw := strings.TrimSpace(c.Query("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 1000 {
			c.JSON(http.StatusBadRequest, claudeErrorResponse{Type: "error", Error: claudeErrorDetail{Type: "invalid_request_error", Message: "limit must be between 1 and 1000"}})
			return 0, 0, false
		}
		limit = parsed
	}
	start := 0
	if cursor := c.Query("after_id"); cursor != "" {
		for index, model := range models {
			if model["id"] == cursor {
				start = index + 1
				break
			}
		}
	}
	end := len(models)
	if cursor := c.Query("before_id"); cursor != "" {
		for index, model := range models {
			if model["id"] == cursor {
				end = index
				break
			}
		}
	}
	if start > end {
		start = end
	}
	if start+limit < end {
		end = start + limit
	}
	return start, end, true
}

// sortClaudeModelsByDisplayName sorts models by display_name ascending.
// When display_name is equal or missing, id is used as a stable tie-breaker.
func sortClaudeModelsByDisplayName(models []map[string]any) {
	sort.SliceStable(models, func(i, j int) bool {
		di, _ := models[i]["display_name"].(string)
		dj, _ := models[j]["display_name"].(string)
		if di != dj {
			return di < dj
		}
		idi, _ := models[i]["id"].(string)
		idj, _ := models[j]["id"].(string)
		return idi < idj
	})
}

// handleNonStreamingResponse handles non-streaming content generation requests for Claude models.
// This function processes the request synchronously and returns the complete generated
// response in a single API call. It supports various generation parameters and
// response formats.
//
// Parameters:
//   - c: The Gin context for the request
//   - modelName: The name of the Gemini model to use for content generation
//   - rawJSON: The raw JSON request body containing generation parameters and content
func (h *ClaudeCodeAPIHandler) handleNonStreamingResponse(c *gin.Context, rawJSON []byte) {
	c.Header("Content-Type", "application/json")
	alt := h.GetAlt(c)
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	stopKeepAlive := h.StartNonStreamingKeepAlive(c, cliCtx)

	modelName := gjson.GetBytes(rawJSON, "model").String()

	resp, upstreamHeaders, errMsg := h.ExecuteWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, alt)
	stopKeepAlive()
	if errMsg != nil {
		normalizeClaudeFallbackError(errMsg)
		recordClaudeFallbackEligibility(c.Request.Context(), modelName, errMsg)
		h.WriteErrorResponse(c, errMsg)
		cliCancel(errMsg.Error)
		return
	}

	// Decompress gzipped responses - Claude API sometimes returns gzip without Content-Encoding header
	// This fixes title generation and other non-streaming responses that arrive compressed
	if len(resp) >= 2 && resp[0] == 0x1f && resp[1] == 0x8b {
		gzReader, errGzip := gzip.NewReader(bytes.NewReader(resp))
		if errGzip != nil {
			log.Warnf("failed to decompress gzipped Claude response: %v", errGzip)
		} else {
			defer func() {
				if errClose := gzReader.Close(); errClose != nil {
					log.Warnf("failed to close Claude gzip reader: %v", errClose)
				}
			}()
			decompressed, errRead := io.ReadAll(gzReader)
			if errRead != nil {
				log.Warnf("failed to read decompressed Claude response: %v", errRead)
			} else {
				resp = decompressed
			}
		}
	}

	handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
	if value, exists := c.Get(claudeContextEditGinKey); exists {
		if result, ok := value.(claudeContextEditResult); ok {
			resp = attachClaudeContextEditResult(resp, result)
		}
	}
	_, _ = c.Writer.Write(resp)
	cliCancel()
}

// handleStreamingResponse streams Claude-compatible responses backed by Gemini.
// It sets up SSE, selects a backend client with rotation/quota logic,
// forwards chunks, and translates them to Claude CLI format.
//
// Parameters:
//   - c: The Gin context for the request.
//   - rawJSON: The raw JSON request body.
func (h *ClaudeCodeAPIHandler) handleStreamingResponse(c *gin.Context, rawJSON []byte) {
	// Get the http.Flusher interface to manually flush the response.
	// This is crucial for streaming as it allows immediate sending of data chunks
	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "Streaming not supported",
				Type:    "server_error",
			},
		})
		return
	}

	modelName := gjson.GetBytes(rawJSON, "model").String()

	// Create a cancellable context for the backend client request
	// This allows proper cleanup and cancellation of ongoing requests
	cliCtx, cliCancel := h.GetContextWithCancel(h, c, context.Background())
	cliCtx, _ = h.codexUpstreamWebsocketContext(cliCtx, rawJSON, c.Request.Header)

	dataChan, upstreamHeaders, errChan := h.ExecuteStreamWithAuthManager(cliCtx, h.HandlerType(), modelName, rawJSON, "")
	idleTimeout := handlers.StreamingIdleTimeout(h.Cfg)
	setSSEHeaders := func() {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Header("Connection", "keep-alive")
		c.Header("Access-Control-Allow-Origin", "*")
	}

	// Peek at the first chunk to determine success or failure before setting headers.
	// This boundary needs its own watchdog because ForwardStream starts only after
	// the first upstream payload has committed the downstream response.
	var idleTimer *time.Timer
	var idleC <-chan time.Time
	if idleTimeout > 0 {
		idleTimer = time.NewTimer(idleTimeout)
		defer idleTimer.Stop()
		idleC = idleTimer.C
	}
	for {
		select {
		case <-c.Request.Context().Done():
			cliCancel(c.Request.Context().Err())
			return
		case errMsg, ok := <-errChan:
			if !ok {
				// Err channel closed cleanly; wait for data channel.
				errChan = nil
				continue
			}
			normalizeClaudeFallbackError(errMsg)
			if isClaudeStreamIdleError(errMsg) {
				observability.RecordWebsocketMetric(c.Request.Context(), "stream_idle_timeout", "", "", map[string]any{
					"boundary":             "before_downstream_commit",
					"downstream_committed": false,
					"duration_us":          idleTimeout.Microseconds(),
				}, false)
			}
			recordClaudeFallbackEligibility(c.Request.Context(), modelName, errMsg)
			// Upstream failed immediately. Return proper error status and JSON.
			h.WriteErrorResponse(c, errMsg)
			if errMsg != nil {
				cliCancel(errMsg.Error)
			} else {
				cliCancel(nil)
			}
			return
		case <-idleC:
			observability.RecordWebsocketMetric(c.Request.Context(), "stream_idle_timeout", "", "", map[string]any{
				"boundary":             "before_downstream_commit",
				"downstream_committed": false,
				"duration_us":          idleTimeout.Microseconds(),
			}, false)
			errMsg := &interfaces.ErrorMessage{StatusCode: http.StatusGatewayTimeout, Error: handlers.ErrStreamIdleTimeout}
			h.WriteErrorResponse(c, errMsg)
			cliCancel(handlers.ErrStreamIdleTimeout)
			return
		case chunk, ok := <-dataChan:
			if !ok {
				if errMsg, okPendingErr := pendingClaudeStreamError(errChan); okPendingErr {
					h.WriteErrorResponse(c, errMsg)
					if errMsg != nil {
						cliCancel(errMsg.Error)
					} else {
						cliCancel(nil)
					}
					return
				}
				// Stream closed without data? Send DONE or just headers.
				setSSEHeaders()
				handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)
				flusher.Flush()
				cliCancel(nil)
				return
			}

			// Success! Set headers now.
			setSSEHeaders()
			handlers.WriteUpstreamHeaders(c.Writer.Header(), upstreamHeaders)

			// Write the first chunk
			if len(chunk) > 0 {
				if value, exists := c.Get(claudeContextEditGinKey); exists {
					if result, okResult := value.(claudeContextEditResult); okResult {
						chunk = attachClaudeContextEditResult(chunk, result)
					}
				}
				_, _ = c.Writer.Write(chunk)
				flusher.Flush()
			}

			// Continue streaming the rest
			h.forwardClaudeStream(c, flusher, func(err error) { cliCancel(err) }, dataChan, errChan)
			return
		}
	}
}

func isClaudeStreamIdleError(errMsg *interfaces.ErrorMessage) bool {
	if errMsg == nil || errMsg.Error == nil {
		return false
	}
	if errors.Is(errMsg.Error, handlers.ErrStreamIdleTimeout) {
		return true
	}
	var coded interface{ ErrorCode() string }
	return errors.As(errMsg.Error, &coded) && coded.ErrorCode() == "stream_idle_timeout"
}

func (h *ClaudeCodeAPIHandler) codexUpstreamWebsocketContext(ctx context.Context, rawJSON []byte, headers http.Header) (context.Context, string) {
	if h == nil || h.Cfg == nil || !h.Cfg.CodexPreferUpstreamWebsockets {
		return ctx, ""
	}
	sessionID := helps.ClaudeCodeWebsocketSessionID(ctx, rawJSON, headers)
	if sessionID == "" {
		return ctx, ""
	}
	ctx = cliproxyexecutor.WithPreferUpstreamWebsocket(ctx)
	ctx = handlers.WithExecutionSessionID(ctx, sessionID)
	return ctx, sessionID
}

func pendingClaudeStreamError(errs <-chan *interfaces.ErrorMessage) (*interfaces.ErrorMessage, bool) {
	if errs == nil {
		return nil, false
	}
	select {
	case errMsg, ok := <-errs:
		if !ok {
			return nil, false
		}
		return errMsg, true
	default:
		return nil, false
	}
}

func (h *ClaudeCodeAPIHandler) forwardClaudeStream(c *gin.Context, flusher http.Flusher, cancel func(error), data <-chan []byte, errs <-chan *interfaces.ErrorMessage) {
	h.ForwardStream(c, flusher, cancel, data, errs, handlers.StreamForwardOptions{
		OnIdle: func(timeout time.Duration) {
			observability.RecordWebsocketMetric(c.Request.Context(), "stream_idle_timeout", "", "", map[string]any{
				"boundary":             "after_downstream_commit",
				"downstream_committed": true,
				"duration_us":          timeout.Microseconds(),
			}, false)
		},
		WriteChunk: func(chunk []byte) {
			if len(chunk) == 0 {
				return
			}
			if value, exists := c.Get(claudeContextEditGinKey); exists {
				if result, ok := value.(claudeContextEditResult); ok {
					chunk = attachClaudeContextEditResult(chunk, result)
				}
			}
			_, _ = c.Writer.Write(chunk)
		},
		WriteTerminalError: func(errMsg *interfaces.ErrorMessage) {
			h.writeTerminalStreamError(c, errMsg)
		},
	})
}

func (h *ClaudeCodeAPIHandler) writeTerminalStreamError(c *gin.Context, errMsg *interfaces.ErrorMessage) {
	if c == nil || errMsg == nil {
		return
	}
	status := http.StatusInternalServerError
	if errMsg.StatusCode > 0 {
		status = errMsg.StatusCode
	}
	// Once SSE payload bytes have been committed, HTTP status is immutable. Calling
	// Status here only produces a misleading "headers already written" warning.
	if !c.Writer.Written() {
		c.Status(status)
	}
	errorBytes, errMarshal := json.Marshal(h.toClaudeError(errMsg))
	if errMarshal != nil {
		errorBytes = []byte(`{"type":"error","error":{"type":"api_error","message":"Internal Server Error"}}`)
	}
	_, _ = fmt.Fprintf(c.Writer, "event: error\ndata: %s\n\n", errorBytes)
}

type claudeErrorDetail struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type claudeErrorResponse struct {
	Type  string            `json:"type"`
	Error claudeErrorDetail `json:"error"`
}

func (h *ClaudeCodeAPIHandler) toClaudeError(msg *interfaces.ErrorMessage) claudeErrorResponse {
	status := http.StatusInternalServerError
	errText := http.StatusText(status)
	if msg != nil {
		if msg.StatusCode > 0 {
			status = msg.StatusCode
			errText = http.StatusText(status)
		}
		if msg.Error != nil {
			if v := strings.TrimSpace(msg.Error.Error()); v != "" {
				errText = v
			}
		}
	}
	errType, message := claudeErrorDetailFromText(status, errText)
	return claudeErrorResponse{
		Type: "error",
		Error: claudeErrorDetail{
			Type:    errType,
			Message: message,
		},
	}
}

func (h *ClaudeCodeAPIHandler) WriteErrorResponse(c *gin.Context, msg *interfaces.ErrorMessage) {
	status := http.StatusInternalServerError
	if msg != nil && msg.StatusCode > 0 {
		status = msg.StatusCode
	}
	if msg != nil && msg.Addon != nil && handlers.PassthroughHeadersEnabled(h.Cfg) {
		for key, values := range msg.Addon {
			if len(values) == 0 {
				continue
			}
			c.Writer.Header().Del(key)
			for _, value := range values {
				c.Writer.Header().Add(key, value)
			}
		}
	}

	body, err := json.Marshal(h.toClaudeError(msg))
	if err != nil {
		body = []byte(`{"type":"error","error":{"type":"api_error","message":"Internal Server Error"}}`)
	}
	appendClaudeAPIResponse(c, body)
	if !c.Writer.Written() {
		c.Writer.Header().Set("Content-Type", "application/json")
	}
	c.Status(status)
	_, _ = c.Writer.Write(body)
}

func claudeErrorDetailFromText(status int, errText string) (string, string) {
	message := strings.TrimSpace(errText)
	if message == "" {
		message = http.StatusText(status)
	}
	errType := claudeErrorTypeFromStatus(status)

	var payload map[string]any
	if json.Valid([]byte(message)) {
		if err := json.Unmarshal([]byte(message), &payload); err == nil {
			if e, ok := payload["error"].(map[string]any); ok {
				if t, ok := e["type"].(string); ok && strings.TrimSpace(t) != "" {
					errType = strings.TrimSpace(t)
				}
				if m, ok := e["message"].(string); ok && strings.TrimSpace(m) != "" {
					message = strings.TrimSpace(m)
				} else if c, ok := e["code"].(string); ok && strings.TrimSpace(c) != "" {
					message = strings.TrimSpace(c)
				}
			} else {
				if t, ok := payload["type"].(string); ok && strings.TrimSpace(t) != "" && strings.TrimSpace(t) != "error" {
					errType = strings.TrimSpace(t)
				}
				if m, ok := payload["message"].(string); ok && strings.TrimSpace(m) != "" {
					message = strings.TrimSpace(m)
				}
			}
		}
	}

	return errType, message
}

func claudeErrorTypeFromStatus(status int) string {
	switch status {
	case http.StatusUnauthorized:
		return "authentication_error"
	case http.StatusPaymentRequired:
		return "billing_error"
	case http.StatusForbidden:
		return "permission_error"
	case http.StatusNotFound:
		return "not_found_error"
	case http.StatusRequestEntityTooLarge:
		return "request_too_large"
	case http.StatusTooManyRequests:
		return "rate_limit_error"
	case http.StatusGatewayTimeout:
		return "timeout_error"
	case 529:
		return "overloaded_error"
	default:
		if status >= http.StatusInternalServerError {
			return "api_error"
		}
		return "invalid_request_error"
	}
}

func appendClaudeAPIResponse(c *gin.Context, data []byte) {
	if c == nil || len(data) == 0 {
		return
	}
	if _, exists := c.Get("API_RESPONSE_TIMESTAMP"); !exists {
		c.Set("API_RESPONSE_TIMESTAMP", time.Now())
	}
	if existing, exists := c.Get("API_RESPONSE"); exists {
		if existingBytes, ok := existing.([]byte); ok && len(existingBytes) > 0 {
			combined := make([]byte, 0, len(existingBytes)+len(data)+1)
			combined = append(combined, existingBytes...)
			if existingBytes[len(existingBytes)-1] != '\n' {
				combined = append(combined, '\n')
			}
			combined = append(combined, data...)
			c.Set("API_RESPONSE", combined)
			return
		}
	}
	c.Set("API_RESPONSE", bytes.Clone(data))
}
