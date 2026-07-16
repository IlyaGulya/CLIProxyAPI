package helps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestRecordAPIRequestClonesDeferredBodyWhenRequestLogDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)
	body := []byte(`{"model":"original"}`)

	RecordAPIRequest(ctx, &config.Config{}, UpstreamRequestLog{
		URL:    "https://api.example.com/v1/responses",
		Method: http.MethodPost,
		Body:   body,
	})
	body[10] = 'X'

	value, exists := ginCtx.Get(logging.DeferredAPIRequestContextKey)
	if !exists {
		t.Fatal("deferred API request was not captured")
	}
	requests, ok := value.([]logging.DeferredAPIRequest)
	if !ok || len(requests) != 1 {
		t.Fatalf("deferred API requests = %#v, want one request", value)
	}
	captured := string(requests[0]())
	if !strings.Contains(captured, `{"model":"original"}`) {
		t.Fatalf("captured API request = %q, want original body", captured)
	}
}

func TestRecordAPIResponseMetadataStoresHeadersWhenRequestLogDisabled(t *testing.T) {
	ctx := logging.WithResponseHeadersHolder(context.Background())
	headers := http.Header{}
	headers.Add("X-Upstream-Request-Id", "upstream-req-1")

	RecordAPIResponseMetadata(ctx, &config.Config{}, http.StatusOK, headers)
	headers.Set("X-Upstream-Request-Id", "mutated")

	got := logging.GetResponseHeaders(ctx)
	if got.Get("X-Upstream-Request-Id") != "upstream-req-1" {
		t.Fatalf("response header = %q, want %q", got.Get("X-Upstream-Request-Id"), "upstream-req-1")
	}
}

func TestRecordAPIWebsocketMetricWritesStructuredPromptFreeTimelineEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	ginCtx, _ := gin.CreateTestContext(recorder)
	ctx := context.WithValue(context.Background(), "gin", ginCtx)

	RecordAPIWebsocketMetric(ctx, &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}, "connection_ready", map[string]any{
		"duration_us": 1234,
		"reused":      true,
		"event":       "must-not-overwrite",
	})

	value, exists := ginCtx.Get(apiWebsocketTimelineKey)
	if !exists {
		t.Fatal("API websocket metric timeline was not captured")
	}
	timeline, ok := value.([]byte)
	if !ok {
		t.Fatalf("API websocket metric timeline type = %T, want []byte", value)
	}
	got := string(timeline)
	for _, want := range []string{
		`"event":"api.websocket.metric"`,
		`"name":"connection_ready"`,
		`"duration_us":1234`,
		`"reused":true`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("metric timeline = %q, missing %q", got, want)
		}
	}
	if strings.Contains(got, "must-not-overwrite") {
		t.Fatalf("metric timeline allowed reserved event override: %q", got)
	}
}

func TestRecordAPIWebsocketMetricAddsPrivacySafeClaudeCorrelation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	ginCtx.Request.Header.Set(ClaudeCodeSessionHeader, "private-root-session")
	ginCtx.Request.Header.Set(ClaudeCodeAgentHeader, "private-agent")
	ctx := context.WithValue(context.Background(), "gin", ginCtx)

	RecordAPIWebsocketMetric(ctx, &config.Config{SDKConfig: config.SDKConfig{RequestLog: true}}, "request_prepared", map[string]any{
		"claude_root_correlation_id": "must-not-overwrite",
	})
	value, exists := ginCtx.Get(apiWebsocketTimelineKey)
	if !exists {
		t.Fatal("API websocket metric timeline was not captured")
	}
	got := string(value.([]byte))
	for _, want := range []string{`"claude_root_correlation_id":"claude-root:`, `"claude_execution_correlation_id":"claude-exec:`} {
		if !strings.Contains(got, want) {
			t.Fatalf("metric timeline = %q, missing %q", got, want)
		}
	}
	for _, privateValue := range []string{"private-root-session", "private-agent", "must-not-overwrite"} {
		if strings.Contains(got, privateValue) {
			t.Fatalf("metric timeline leaked or overwrote correlation with %q: %s", privateValue, got)
		}
	}
}

func TestCodexUsageMetricFieldsNormalizesCacheAndReasoningTokens(t *testing.T) {
	fields := CodexUsageMetricFields(usage.Detail{
		InputTokens:         100,
		OutputTokens:        20,
		ReasoningTokens:     7,
		CachedTokens:        50,
		CacheReadTokens:     40,
		CacheCreationTokens: 10,
		TotalTokens:         127,
		ResponseServiceTier: "default",
	})
	wants := map[string]any{
		"input_tokens":          int64(100),
		"output_tokens":         int64(20),
		"reasoning_tokens":      int64(7),
		"cached_tokens":         int64(50),
		"cache_read_tokens":     int64(40),
		"cache_creation_tokens": int64(10),
		"total_tokens":          int64(127),
		"response_service_tier": "default",
	}
	for key, want := range wants {
		if got := fields[key]; got != want {
			t.Fatalf("field %s = %#v, want %#v", key, got, want)
		}
	}
}
