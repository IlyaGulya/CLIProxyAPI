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
