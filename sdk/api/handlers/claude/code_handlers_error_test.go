package claude

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/tidwall/gjson"
)

type stalledClaudeStreamExecutor struct{}

func (*stalledClaudeStreamExecutor) Identifier() string { return "codex" }
func (*stalledClaudeStreamExecutor) Execute(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("unexpected non-streaming request")
}
func (*stalledClaudeStreamExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return &coreexecutor.StreamResult{Chunks: make(chan coreexecutor.StreamChunk)}, nil
}
func (*stalledClaudeStreamExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}
func (*stalledClaudeStreamExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("unexpected token count request")
}
func (*stalledClaudeStreamExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected HTTP request")
}

type writeHeaderCountingWriter struct {
	gin.ResponseWriter
	writes int
}

func (w *writeHeaderCountingWriter) WriteHeader(statusCode int) {
	w.writes++
	w.ResponseWriter.WriteHeader(statusCode)
}

func TestClaudeErrorExtractsOpenAIStyleUpstreamJSON(t *testing.T) {
	handler := &ClaudeCodeAPIHandler{}
	msg := &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"message":"Your input exceeds the context window of this model. Please adjust your input and try again.","type":"invalid_request_error","code":"context_too_large"}}`),
	}

	got := handler.toClaudeError(msg)

	if got.Type != "error" {
		t.Fatalf("type = %q, want error", got.Type)
	}
	if got.Error.Type != "invalid_request_error" {
		t.Fatalf("error.type = %q, want invalid_request_error", got.Error.Type)
	}
	if got.Error.Message != "Your input exceeds the context window of this model. Please adjust your input and try again." {
		t.Fatalf("error.message = %q", got.Error.Message)
	}
}

func TestClaudeErrorExtractsClaudeStyleUpstreamJSON(t *testing.T) {
	handler := &ClaudeCodeAPIHandler{}
	msg := &interfaces.ErrorMessage{
		StatusCode: http.StatusTooManyRequests,
		Error:      errors.New(`{"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."},"request_id":"req_123"}`),
	}

	got := handler.toClaudeError(msg)

	if got.Error.Type != "rate_limit_error" {
		t.Fatalf("error.type = %q, want rate_limit_error", got.Error.Type)
	}
	if got.Error.Message != "This request would exceed your account's rate limit. Please try again later." {
		t.Fatalf("error.message = %q", got.Error.Message)
	}
}

func TestWriteClaudeErrorResponseUsesClaudeEnvelope(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	handler := &ClaudeCodeAPIHandler{}
	msg := &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"message":"Your input exceeds the context window of this model. Please adjust your input and try again.","type":"invalid_request_error","code":"context_too_large"}}`),
	}

	handler.WriteErrorResponse(c, msg)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
	}
	body := recorder.Body.Bytes()
	if got := gjson.GetBytes(body, "type").String(); got != "error" {
		t.Fatalf("type = %q, want error; body=%s", got, body)
	}
	if got := gjson.GetBytes(body, "error.type").String(); got != "invalid_request_error" {
		t.Fatalf("error.type = %q, want invalid_request_error; body=%s", got, body)
	}
	if got := gjson.GetBytes(body, "error.message").String(); got != "Your input exceeds the context window of this model. Please adjust your input and try again." {
		t.Fatalf("error.message = %q; body=%s", got, body)
	}
}

func TestClaudeMessagesUnknownModelReturnsAvailableModelsWithoutUpstreamAttempt(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const clientID = "claude-unknown-model-catalog-test"
	registryRef := registry.GetGlobalRegistry()
	registryRef.RegisterClient(clientID, "codex", []*registry.ModelInfo{
		{ID: "gpt-5.6-terra"},
		{ID: "gpt-5.6-sol"},
		{ID: "gpt-5.6-luna"},
	})
	t.Cleanup(func() { registryRef.UnregisterClient(clientID) })

	manager := coreauth.NewManager(nil, nil, nil)
	handler := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{}, manager))
	for _, stream := range []bool{false, true} {
		t.Run(map[bool]string{false: "non-streaming", true: "streaming"}[stream], func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			payload := fmt.Sprintf(`{
				"model":"claude-sonnet-5",
				"max_tokens":1024,
				"stream":%t,
				"messages":[{"role":"user","content":"hello"}]
			}`, stream)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(payload))

			handler.ClaudeMessages(c)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusBadRequest, recorder.Body.String())
			}
			if got := gjson.GetBytes(recorder.Body.Bytes(), "error.type").String(); got != "invalid_request_error" {
				t.Fatalf("error.type = %q, want invalid_request_error; body=%s", got, recorder.Body.String())
			}
			message := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String()
			for _, want := range []string{"claude-sonnet-5", "gpt-5.6-luna", "gpt-5.6-sol", "gpt-5.6-terra"} {
				if !strings.Contains(message, want) {
					t.Errorf("error.message does not contain %q: %q", want, message)
				}
			}
		})
	}
}

func TestPendingClaudeStreamErrorUsesBufferedError(t *testing.T) {
	wantErr := &interfaces.ErrorMessage{
		StatusCode: http.StatusBadRequest,
		Error:      errors.New(`{"error":{"message":"Your input exceeds the context window of this model. Please adjust your input and try again.","type":"invalid_request_error","code":"context_too_large"}}`),
	}
	errs := make(chan *interfaces.ErrorMessage, 1)
	errs <- wantErr
	close(errs)

	gotErr, ok := pendingClaudeStreamError(errs)
	if !ok {
		t.Fatal("expected pending stream error")
	}
	if gotErr != wantErr {
		t.Fatalf("pending error = %p, want %p", gotErr, wantErr)
	}
}

func TestWriteClaudeTerminalStreamErrorDoesNotRewriteCommittedStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	counting := &writeHeaderCountingWriter{ResponseWriter: c.Writer}
	c.Writer = counting
	c.Header("Content-Type", "text/event-stream")
	c.Status(http.StatusOK)
	_, _ = c.Writer.Write([]byte("event: message_start\ndata: {}\n\n"))

	handler := &ClaudeCodeAPIHandler{}
	handler.writeTerminalStreamError(c, &interfaces.ErrorMessage{
		StatusCode: http.StatusInternalServerError,
		Error:      errors.New("websocket: close 1006 (abnormal closure): unexpected EOF"),
	})

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want committed 200", recorder.Code)
	}
	if counting.writes != 1 {
		t.Fatalf("WriteHeader calls = %d, want no status rewrite after commit", counting.writes)
	}
	body := recorder.Body.String()
	if strings.Count(body, "event: error") != 1 {
		t.Fatalf("terminal SSE errors = %d, want exactly one; body=%s", strings.Count(body, "event: error"), body)
	}
	if !strings.Contains(body, `"type":"error"`) {
		t.Fatalf("missing Claude error envelope: %s", body)
	}
}

func TestClaudeStreamingIdleBeforeFirstEventReturnsGatewayTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(&stalledClaudeStreamExecutor{})
	auth := &coreauth.Auth{ID: "claude-stalled-stream", Provider: "codex", Status: coreauth.StatusActive}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-sol"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	handler := NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&config.SDKConfig{
		Streaming: config.StreamingConfig{IdleTimeoutSeconds: 1},
	}, manager))
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(`{
		"model":"gpt-5.6-sol","max_tokens":64,"stream":true,
		"messages":[{"role":"user","content":"hello"}]
	}`))
	started := time.Now()

	handler.ClaudeMessages(c)

	if elapsed := time.Since(started); elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("elapsed = %s, want watchdog near 1s", elapsed)
	}
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want 504; body=%s", recorder.Code, recorder.Body.String())
	}
	if got := gjson.GetBytes(recorder.Body.Bytes(), "error.message").String(); !strings.Contains(got, handlers.ErrStreamIdleTimeout.Error()) {
		t.Fatalf("error.message = %q, want idle timeout detail", got)
	}
}

func TestIsClaudeStreamIdleErrorRecognizesStableConductorCode(t *testing.T) {
	msg := &interfaces.ErrorMessage{Error: fmt.Errorf("bootstrap failed: %w", &coreauth.Error{
		Code: "stream_idle_timeout", Message: "stream idle timeout", HTTPStatus: http.StatusGatewayTimeout,
	})}
	if !isClaudeStreamIdleError(msg) {
		t.Fatal("idle timeout wrapped by conductor was not recognized")
	}
	if isClaudeStreamIdleError(&interfaces.ErrorMessage{Error: errors.New("other failure")}) {
		t.Fatal("unrelated failure was recognized as idle timeout")
	}
}
