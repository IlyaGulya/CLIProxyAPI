package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestCodexWebsocketIdleTimeoutUsesStreamingPolicy(t *testing.T) {
	if got := codexWebsocketIdleTimeout(&config.Config{SDKConfig: config.SDKConfig{
		Streaming: config.StreamingConfig{IdleTimeoutSeconds: 17},
	}}); got != 17*time.Second {
		t.Fatalf("timeout = %s, want 17s", got)
	}
	if got := codexWebsocketIdleTimeout(nil); got != codexResponsesWebsocketIdleTimeout {
		t.Fatalf("default timeout = %s, want %s", got, codexResponsesWebsocketIdleTimeout)
	}
}

func TestCodexStreamLivenessTransitionsOnProgressEvents(t *testing.T) {
	tests := []struct {
		name      string
		eventType string
		wantState codexStreamLivenessState
	}{
		{name: "empty", eventType: "", wantState: codexLivenessAwaitingProgress},
		{name: "created metadata", eventType: "response.created", wantState: codexLivenessAwaitingProgress},
		{name: "in progress metadata", eventType: "response.in_progress", wantState: codexLivenessAwaitingProgress},
		{name: "reasoning delta", eventType: "response.reasoning_summary_text.delta", wantState: codexLivenessStreaming},
		{name: "text delta", eventType: "response.output_text.delta", wantState: codexLivenessStreaming},
		{name: "tool call", eventType: "response.output_item.added", wantState: codexLivenessStreaming},
		{name: "terminal without delta", eventType: "response.completed", wantState: codexLivenessStreaming},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			liveness := newCodexStreamLiveness(nil)
			payload := `{"type":"` + tt.eventType + `"}`
			if tt.eventType == "response.output_item.added" {
				payload = `{"type":"response.output_item.added","item":{"type":"function_call"}}`
			}
			liveness.observe(classifyCodexStreamEvent([]byte(payload)))
			if liveness.state != tt.wantState {
				t.Fatalf("state = %d, want %d", liveness.state, tt.wantState)
			}
		})
	}
}

func TestCodexStreamLivenessSelectsTimeoutByState(t *testing.T) {
	now := time.Unix(100, 0)
	liveness := newCodexStreamLivenessWithClock(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{
		FirstProgressTimeoutSeconds: 7,
		IdleTimeoutSeconds:          19,
	}}}, func() time.Time { return now })
	if got := liveness.timeout(); got != 7*time.Second {
		t.Fatalf("awaiting progress timeout = %s, want 7s", got)
	}
	liveness.observe(classifyCodexStreamEvent([]byte(`{"type":"response.output_text.delta","delta":"x"}`)))
	if got := liveness.timeout(); got != 19*time.Second {
		t.Fatalf("streaming timeout = %s, want 19s", got)
	}
	liveness.resetAttempt()
	if got := liveness.timeout(); got != 7*time.Second {
		t.Fatalf("reset timeout = %s, want 7s", got)
	}
}

func TestCodexStreamLivenessCapsFirstProgressTimeoutAtIdleTimeout(t *testing.T) {
	now := time.Unix(100, 0)
	liveness := newCodexStreamLivenessWithClock(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{
		FirstProgressTimeoutSeconds: 20,
		IdleTimeoutSeconds:          3,
	}}}, func() time.Time { return now })
	if got := liveness.timeout(); got != 3*time.Second {
		t.Fatalf("timeout = %s, want idle timeout cap 3s", got)
	}
}

func TestCodexFirstProgressTimeoutHasDedicatedRetryReason(t *testing.T) {
	err := codexFirstProgressTimeoutError{timeout: 30 * time.Second}
	if got := codexWebsocketRetryReason(err); got != "first_progress_timeout" {
		t.Fatalf("retry reason = %q, want first_progress_timeout", got)
	}
}

func TestCodexStreamLivenessMapsOnlyAwaitingProgressNetworkTimeout(t *testing.T) {
	liveness := newCodexStreamLiveness(nil)
	networkTimeout := codexWebsocketReadTimeoutError{timeout: time.Second}
	if _, ok := liveness.mapTimeout(networkTimeout).(codexFirstProgressTimeoutError); !ok {
		t.Fatal("awaiting-progress network timeout was not classified")
	}
	if got := liveness.mapTimeout(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("cancellation = %v, want context.Canceled", got)
	}
	liveness.observe(classifyCodexStreamEvent([]byte(`{"type":"response.output_text.delta","delta":"x"}`)))
	if got := liveness.mapTimeout(networkTimeout); !errors.Is(got, networkTimeout) {
		t.Fatalf("streaming timeout = %v, want original network timeout", got)
	}
}

func TestCodexStreamLivenessMetadataDoesNotExtendFirstProgressDeadline(t *testing.T) {
	now := time.Unix(100, 0)
	liveness := newCodexStreamLivenessWithClock(&config.Config{SDKConfig: config.SDKConfig{Streaming: config.StreamingConfig{
		FirstProgressTimeoutSeconds: 30,
		IdleTimeoutSeconds:          300,
	}}}, func() time.Time { return now })

	now = now.Add(25 * time.Second)
	liveness.observe(classifyCodexStreamEvent([]byte(`{"type":"response.in_progress"}`)))
	if got := liveness.timeout(); got != 5*time.Second {
		t.Fatalf("timeout after metadata = %s, want remaining absolute deadline 5s", got)
	}
	now = now.Add(5 * time.Second)
	if got := liveness.timeout(); got > 0 {
		t.Fatalf("expired timeout = %s, want non-positive duration", got)
	}
}

func TestCodexStreamLivenessUnknownEventIsNotProgress(t *testing.T) {
	liveness := newCodexStreamLiveness(nil)
	liveness.observe(classifyCodexStreamEvent([]byte(`{"type":"future.metadata"}`)))
	if liveness.state != codexLivenessAwaitingProgress {
		t.Fatalf("state = %d, want awaiting progress", liveness.state)
	}
}
