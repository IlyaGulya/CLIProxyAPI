package observability

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func attributesString(values []attribute.KeyValue) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%s=%s", value.Key, value.Value.Emit()))
	}
	return strings.Join(parts, ",")
}

func TestMetricAttributesAreBoundedAndPrivacySafe(t *testing.T) {
	t.Parallel()
	got := metricAttributes(map[string]any{
		"model":                           "gpt-5.6-luna",
		"connection_source":               "speculative",
		"reason":                          "completed",
		"success":                         true,
		"claude_root_correlation_id":      "root-secret",
		"claude_execution_correlation_id": "exec-secret",
		"prompt_prefix_fingerprint":       "fingerprint",
		"prompt":                          "do not export",
		"session_id":                      "session-secret",
	})
	text := attributesString(got)
	for _, want := range []string{"model=gpt-5.6-luna", "connection.source=speculative", "finish.reason=completed", "success=true"} {
		if !strings.Contains(text, want) {
			t.Errorf("attributes missing %q: %s", want, text)
		}
	}
	for _, forbidden := range []string{"root-secret", "exec-secret", "fingerprint", "do not export", "session-secret"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("attributes leaked %q: %s", forbidden, text)
		}
	}
}

func TestSpanAttributesHashCorrelationAndRejectPayloads(t *testing.T) {
	t.Parallel()
	got := spanAttributes("root-secret", "exec-secret", map[string]any{
		"model":        "gpt-5.6-sol",
		"input_tokens": int64(42),
		"full_command": "rm -rf private",
		"request_body": "secret body",
		"access_token": "supersecretvalue",
		"duration_us":  int64(123),
	})
	text := attributesString(got)
	if !strings.Contains(text, "claudex.root_hash=") || !strings.Contains(text, "claudex.execution_hash=") {
		t.Fatalf("missing hashed correlations: %s", text)
	}
	for _, forbidden := range []string{"root-secret", "exec-secret", "rm -rf private", "secret body", "supersecretvalue"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("span attributes leaked %q: %s", forbidden, text)
		}
	}
}

func TestNormalizeModelBoundsCardinality(t *testing.T) {
	t.Parallel()
	if got := normalizeModel("gpt-5.6-luna"); got != "gpt-5.6-luna" {
		t.Fatalf("known model = %q", got)
	}
	if got := normalizeModel(strings.Repeat("x", 200)); got != "other" {
		t.Fatalf("unbounded model = %q", got)
	}
}

func TestInSessionModelSwitchingAndDetachedPreconnectLinks(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	telemetry := &Telemetry{Enabled: true, tracerProvider: provider, tracer: provider.Tracer(instrumentationName)}
	previous := current.Swap(telemetry)
	t.Cleanup(func() {
		current.Store(previous)
		_ = provider.Shutdown(context.Background())
	})

	rootCtx, rootSpan := telemetry.tracer.Start(context.Background(), "root-sol")
	RecordWebsocketMetric(rootCtx, "request_prepared", "root-1", "exec-sol", map[string]any{"model": "gpt-5.6-sol"}, false)
	rootSpan.End()

	childCtx, childSpan := telemetry.tracer.Start(context.Background(), "child-luna")
	RecordWebsocketMetric(childCtx, "request_prepared", "root-1", "exec-luna", map[string]any{"model": "gpt-5.6-luna"}, false)
	childContext := childSpan.SpanContext()
	childSpan.End()
	RecordWebsocketMetric(context.Background(), "speculative_preconnect_ready", "root-1", "exec-luna", map[string]any{"pool_idle": 1}, true)

	spans := exporter.GetSpans()
	var detached tracetest.SpanStub
	for _, span := range spans {
		if span.Name == "proxy.websocket.speculative_preconnect_ready" {
			detached = span
		}
	}
	if detached.Name == "" || len(detached.Links) != 1 {
		t.Fatalf("detached span links = %+v in spans %+v", detached.Links, spans)
	}
	if detached.Links[0].SpanContext.SpanID() != childContext.SpanID() {
		t.Fatalf("preconnect linked to wrong model execution: got %s want %s", detached.Links[0].SpanContext.SpanID(), childContext.SpanID())
	}
}
