package observability

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
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
		"chain_source":                    "full_replay",
		"incremental_reset_reason":        "no_previous_response",
		"claude_root_correlation_id":      "root-secret",
		"claude_execution_correlation_id": "exec-secret",
		"prompt_prefix_fingerprint":       "fingerprint",
		"prompt":                          "do not export",
		"session_id":                      "session-secret",
		"compaction_applied":              true,
		"compaction_retained_tokens":      1234,
		"transactional_policy":            "root_until_semantic_output",
		"commit_boundary":                 "semantic_output",
	})
	text := attributesString(got)
	for _, want := range []string{"model=gpt-5.6-luna", "connection.source=speculative", "finish.reason=completed", "success=true", "chain.source=full_replay", "incremental.reset_reason=no_previous_response", "compaction.applied=true", "stream.transaction.policy=root_until_semantic_output", "stream.commit.boundary=semantic_output"} {
		if !strings.Contains(text, want) {
			t.Errorf("attributes missing %q: %s", want, text)
		}
	}
	if span := attributesString(spanAttributes("root", "exec", map[string]any{"compaction_retained_tokens": 1234})); !strings.Contains(span, "compaction.retained_tokens=1234") {
		t.Errorf("compaction span measurements missing: %s", span)
	}
	for _, forbidden := range []string{"root-secret", "exec-secret", "fingerprint", "do not export", "session-secret"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("attributes leaked %q: %s", forbidden, text)
		}
	}
}

func TestRetryAttributesExposeBoundedDecisionWithoutSessionIdentity(t *testing.T) {
	t.Parallel()
	fields := map[string]any{
		"boundary":             "pre_output",
		"reason":               "close_1006",
		"suppression_reason":   "retry_exhausted",
		"downstream_committed": false,
		"attempt":              1,
		"transport_retries":    1,
		"session_id":           "claude-code:secret-session",
		"error":                "unexpected EOF with private payload",
	}
	metricText := attributesString(metricAttributes(fields))
	spanText := attributesString(spanAttributes("root-secret", "exec-secret", fields))
	for _, text := range []string{metricText, spanText} {
		for _, want := range []string{
			"retry.boundary=pre_output",
			"finish.reason=close_1006",
			"retry.suppression_reason=retry_exhausted",
			"downstream.committed=false",
		} {
			if !strings.Contains(text, want) {
				t.Errorf("retry attributes missing %q: %s", want, text)
			}
		}
		for _, forbidden := range []string{"secret-session", "unexpected EOF with private payload"} {
			if strings.Contains(text, forbidden) {
				t.Errorf("retry attributes leaked %q: %s", forbidden, text)
			}
		}
	}
	for _, want := range []string{"retry.attempt=1", "retry.count=1"} {
		if !strings.Contains(spanText, want) {
			t.Errorf("span retry attributes missing %q: %s", want, spanText)
		}
	}
}

func TestMidResponseFailureAttributesExposeSemanticBoundary(t *testing.T) {
	t.Parallel()
	fields := map[string]any{
		"reason":                   "read_error",
		"close_code":               1006,
		"last_event_type":          "response.function_call_arguments.delta",
		"tool_call_started":        true,
		"tool_call_completed":      false,
		"tool_call_in_progress":    true,
		"downstream_committed":     true,
		"connection_age_us":        8_678_058,
		"connection_request_count": 7,
		"tool_calls_started":       1,
		"tool_calls_completed":     0,
		"tool_calls_incomplete":    1,
		"tool_name":                "private-plugin-tool-must-not-leak",
	}

	metricText := attributesString(metricAttributes(fields))
	spanText := attributesString(spanAttributes("root", "execution", fields))
	for _, want := range []string{
		"finish.reason=read_error",
		"transport.close_code=1006",
		"stream.last_event_type=response.function_call_arguments.delta",
		"tool_call.started=true",
		"tool_call.completed=false",
		"tool_call.in_progress=true",
		"downstream.committed=true",
	} {
		if !strings.Contains(metricText, want) {
			t.Errorf("metric attributes missing %q: %s", want, metricText)
		}
	}
	for _, want := range []string{
		"connection.age.us=8678058",
		"connection.request_count=7",
		"tool_calls.started=1",
		"tool_calls.completed=0",
		"tool_calls.incomplete=1",
	} {
		if !strings.Contains(spanText, want) {
			t.Errorf("span attributes missing %q: %s", want, spanText)
		}
	}
	for _, text := range []string{metricText, spanText} {
		if strings.Contains(text, "private-plugin-tool") {
			t.Errorf("attributes leaked tool name: %s", text)
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

func TestWebsocketAttributesPreserveExplicitFalseAndZero(t *testing.T) {
	t.Parallel()
	attributes := WebsocketAttributes{
		Model:                  "gpt-5.6-sol",
		Success:                Some(false),
		DurationUS:             Some(int64(0)),
		ConnectionRequestCount: Some(int64(0)),
	}
	fields := attributes.fields()
	if fields["success"] != false || fields["duration_us"] != int64(0) || fields["connection_request_count"] != int64(0) {
		t.Fatalf("typed fields lost explicit zero values: %#v", fields)
	}
}

func TestWebsocketAttributesEncodeAttemptTransition(t *testing.T) {
	t.Parallel()
	fields := WebsocketAttributes{
		AttemptStateFrom: WebsocketAttemptState("interrupted"),
		AttemptStateTo:   WebsocketAttemptState("retrying"),
		AttemptEvent:     WebsocketAttemptEvent("retry_approved"),
	}.fields()
	encoded := attributesString(metricAttributes(fields))
	for _, want := range []string{
		"attempt.state.from=interrupted",
		"attempt.state.to=retrying",
		"attempt.event=retry_approved",
	} {
		if !strings.Contains(encoded, want) {
			t.Fatalf("attempt transition missing %q: %s", want, encoded)
		}
	}
}

func BenchmarkWebsocketAttributesEncoding(b *testing.B) {
	attributes := WebsocketAttributes{
		Model: "gpt-5.6-sol", ConnectionSource: "session_reuse", Success: Some(true),
		DurationUS: Some(int64(1250)), InputTokens: Some(int64(4096)), OutputTokens: Some(int64(128)),
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = attributes.fields()
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
	models := make(map[string]string)
	for _, span := range spans {
		if span.Name == "proxy.websocket.speculative_preconnect_ready" {
			detached = span
		}
		for _, attr := range span.Attributes {
			if string(attr.Key) == "model" {
				models[span.Name] = attr.Value.AsString()
			}
		}
	}
	if models["root-sol"] != "gpt-5.6-sol" || models["child-luna"] != "gpt-5.6-luna" {
		t.Fatalf("active span model attributes = %+v", models)
	}
	if detached.Name == "" || len(detached.Links) != 1 {
		t.Fatalf("detached span links = %+v in spans %+v", detached.Links, spans)
	}
	if detached.Links[0].SpanContext.SpanID() != childContext.SpanID() {
		t.Fatalf("preconnect linked to wrong model execution: got %s want %s", detached.Links[0].SpanContext.SpanID(), childContext.SpanID())
	}
}

func TestStartServiceWithEnvironmentRestoresProcessEnvironment(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "original")
	telemetry, errStart := StartServiceWithEnvironment(context.Background(), "test", map[string]string{
		"OTEL_SDK_DISABLED":   "true",
		"CLAUDEX_NEXT_RUN_ID": "isolated-run",
	})
	if errStart != nil {
		t.Fatalf("StartServiceWithEnvironment() error = %v", errStart)
	}
	if telemetry == nil {
		t.Fatal("StartServiceWithEnvironment() returned nil telemetry")
	}
	if got := os.Getenv("OTEL_SDK_DISABLED"); got != "original" {
		t.Fatalf("OTEL_SDK_DISABLED = %q, want restored original", got)
	}
	if _, exists := os.LookupEnv("CLAUDEX_NEXT_RUN_ID"); exists {
		t.Fatal("CLAUDEX_NEXT_RUN_ID leaked into process environment")
	}
}

func TestTelemetryShutdownEvidenceRequiresEveryConfiguredSignal(t *testing.T) {
	evidence := TelemetryShutdownEvidence{
		Schema: 1, Enabled: true, JournalCloseOK: true,
		Logs:    SignalShutdownEvidence{Configured: true, ForceFlushOK: true, ShutdownOK: true},
		Metrics: SignalShutdownEvidence{Configured: true, ForceFlushOK: true, ShutdownOK: true},
		Traces:  SignalShutdownEvidence{Configured: true, ForceFlushOK: true, ShutdownOK: true},
	}
	if !evidence.Succeeded() {
		t.Fatal("complete shutdown evidence was rejected")
	}
	evidence.Traces.ShutdownOK = false
	if evidence.Succeeded() {
		t.Fatal("failed configured signal was accepted")
	}
}

func TestEventJournalWritesPrivacySafeTypedRecordsWithoutOTEL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	t.Setenv("CLAUDEX_NEXT_EVENT_JOURNAL", path)
	t.Setenv("OTEL_SDK_DISABLED", "true")
	telemetry, errStart := StartService(context.Background(), "test")
	if errStart != nil {
		t.Fatal(errStart)
	}
	t.Cleanup(func() { _ = telemetry.Shutdown(context.Background()) })
	RecordWebsocketEvent(context.Background(), WebsocketEvent{
		Name: "request_prepared", RootCorrelation: "root-safe", ExecutionCorrelation: "exec-safe",
		Attributes: WebsocketAttributes{Model: "gpt-5.6-luna", ClientBodyBytes: Some(int64(1234))},
		Fields:     map[string]any{"role": "child", "prompt": "must-not-appear", "authorization": "secret"},
	})
	payload, errRead := os.ReadFile(path)
	if errRead != nil {
		t.Fatal(errRead)
	}
	text := string(payload)
	for _, want := range []string{`"name":"request_prepared"`, `"model":"gpt-5.6-luna"`, `"role":"child"`, `"client_body_bytes":1234`} {
		if !strings.Contains(text, want) {
			t.Fatalf("journal missing %s: %s", want, text)
		}
	}
	for _, forbidden := range []string{"must-not-appear", "secret", "authorization", "prompt"} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("journal leaked %q: %s", forbidden, text)
		}
	}
	if info, errStat := os.Stat(path); errStat != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("journal permissions = %v, err=%v", info.Mode().Perm(), errStat)
	}
}
