package observability

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http/httptrace"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	logglobal "go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.34.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/router-for-me/CLIProxyAPI/v7"

type Telemetry struct {
	Enabled        bool
	tracer         trace.Tracer
	meter          metric.Meter
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider
	logger         otellog.Logger
	runID          string
	events         metric.Int64Counter
	requests       metric.Int64Counter
	latency        metric.Float64Histogram
	bytes          metric.Int64Counter
	tokens         metric.Int64Counter
	claudeRuns     metric.Int64Counter
	claudeCost     metric.Float64Counter
	claudeDuration metric.Float64Histogram
	claudeTokens   metric.Int64Counter
	activeRequests atomic.Int64
	poolIdle       atomic.Int64
	poolDialing    atomic.Int64
	links          sync.Map
}

var current atomic.Pointer[Telemetry]

func Current() *Telemetry {
	if value := current.Load(); value != nil {
		return value
	}
	return &Telemetry{}
}

// Start configures OTLP from standard environment variables. With no exporter
// endpoint it installs a no-op instance and returns successfully.
func Start(ctx context.Context) (*Telemetry, error) {
	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = "cli-proxy-api"
	}
	return StartService(ctx, serviceName)
}

func StartService(ctx context.Context, serviceName string) (*Telemetry, error) {
	telemetry := &Telemetry{runID: strings.TrimSpace(os.Getenv("CLAUDEX_NEXT_RUN_ID"))}
	current.Store(telemetry)
	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") {
		return telemetry, nil
	}
	endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	if endpoint == "" {
		return telemetry, nil
	}
	protocol := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"))
	if protocol == "" {
		protocol = "http/protobuf"
	}
	res, errResource := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceName(serviceName),
		semconv.ServiceVersion(strings.TrimSpace(os.Getenv("CLI_PROXY_API_VERSION"))),
		attribute.String("claudex.run_id", strings.TrimSpace(os.Getenv("CLAUDEX_NEXT_RUN_ID"))),
	))
	if errResource != nil {
		return telemetry, fmt.Errorf("create OTEL resource: %w", errResource)
	}

	var shutdowns []func(context.Context) error
	if exporterEnabled("OTEL_TRACES_EXPORTER") {
		exporter, errExporter := traceExporter(ctx, endpoint, protocol)
		if errExporter != nil {
			return telemetry, fmt.Errorf("create OTLP trace exporter: %w", errExporter)
		}
		telemetry.tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(exporter, sdktrace.WithBatchTimeout(time.Second)),
		)
		otel.SetTracerProvider(telemetry.tracerProvider)
		shutdowns = append(shutdowns, telemetry.tracerProvider.Shutdown)
	}
	if exporterEnabled("OTEL_METRICS_EXPORTER") {
		exporter, errExporter := metricExporter(ctx, endpoint, protocol)
		if errExporter != nil {
			for _, shutdown := range shutdowns {
				_ = shutdown(ctx)
			}
			return telemetry, fmt.Errorf("create OTLP metric exporter: %w", errExporter)
		}
		reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(time.Second))
		telemetry.meterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
		otel.SetMeterProvider(telemetry.meterProvider)
	}
	if exporterEnabled("OTEL_LOGS_EXPORTER") {
		exporter, errExporter := logExporter(ctx, endpoint, protocol)
		if errExporter != nil {
			for _, shutdown := range shutdowns {
				_ = shutdown(ctx)
			}
			return telemetry, fmt.Errorf("create OTLP log exporter: %w", errExporter)
		}
		telemetry.loggerProvider = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter, sdklog.WithExportInterval(time.Second))),
		)
		logglobal.SetLoggerProvider(telemetry.loggerProvider)
		telemetry.logger = telemetry.loggerProvider.Logger(instrumentationName)
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))
	telemetry.tracer = otel.Tracer(instrumentationName)
	telemetry.meter = otel.Meter(instrumentationName)
	telemetry.initInstruments()
	telemetry.Enabled = telemetry.tracerProvider != nil || telemetry.meterProvider != nil || telemetry.loggerProvider != nil
	current.Store(telemetry)
	return telemetry, nil
}

func exporterEnabled(key string) bool {
	value := strings.TrimSpace(os.Getenv(key))
	return value == "" || strings.Contains(strings.ToLower(value), "otlp")
}

func traceExporter(ctx context.Context, endpoint, protocol string) (sdktrace.SpanExporter, error) {
	if strings.EqualFold(protocol, "grpc") {
		target, insecure := grpcTarget(endpoint)
		options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(target)}
		if insecure {
			options = append(options, otlptracegrpc.WithInsecure())
		}
		return otlptracegrpc.New(ctx, options...)
	}
	return otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(signalURL(endpoint, "traces")))
}

func metricExporter(ctx context.Context, endpoint, protocol string) (sdkmetric.Exporter, error) {
	if strings.EqualFold(protocol, "grpc") {
		target, insecure := grpcTarget(endpoint)
		options := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(target)}
		if insecure {
			options = append(options, otlpmetricgrpc.WithInsecure())
		}
		return otlpmetricgrpc.New(ctx, options...)
	}
	return otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(signalURL(endpoint, "metrics")))
}

func logExporter(ctx context.Context, endpoint, protocol string) (sdklog.Exporter, error) {
	if strings.EqualFold(protocol, "grpc") {
		target, insecure := grpcTarget(endpoint)
		options := []otlploggrpc.Option{otlploggrpc.WithEndpoint(target)}
		if insecure {
			options = append(options, otlploggrpc.WithInsecure())
		}
		return otlploggrpc.New(ctx, options...)
	}
	return otlploghttp.New(ctx, otlploghttp.WithEndpointURL(signalURL(endpoint, "logs")))
}

func grpcTarget(endpoint string) (string, bool) {
	parsed, errParse := url.Parse(endpoint)
	if errParse == nil && parsed.Host != "" {
		return parsed.Host, parsed.Scheme != "https"
	}
	return strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"), strings.HasPrefix(endpoint, "http://")
}

func signalURL(endpoint, signal string) string {
	trimmed := strings.TrimRight(endpoint, "/")
	if strings.HasSuffix(trimmed, "/v1/"+signal) {
		return trimmed
	}
	return trimmed + "/v1/" + signal
}

func (t *Telemetry) initInstruments() {
	if t.meterProvider == nil {
		return
	}
	t.events, _ = t.meter.Int64Counter("claudex.proxy.events")
	t.requests, _ = t.meter.Int64Counter("claudex.proxy.requests")
	t.latency, _ = t.meter.Float64Histogram("claudex.proxy.phase.duration", metric.WithUnit("ms"))
	t.bytes, _ = t.meter.Int64Counter("claudex.proxy.bytes", metric.WithUnit("By"))
	t.tokens, _ = t.meter.Int64Counter("claudex.proxy.tokens", metric.WithUnit("{token}"))
	t.claudeRuns, _ = t.meter.Int64Counter("claudex.claude.runs")
	t.claudeCost, _ = t.meter.Float64Counter("claudex.claude.cost")
	t.claudeDuration, _ = t.meter.Float64Histogram("claudex.claude.duration", metric.WithUnit("ms"))
	t.claudeTokens, _ = t.meter.Int64Counter("claudex.claude.tokens", metric.WithUnit("{token}"))
	active, _ := t.meter.Int64ObservableGauge("claudex.proxy.requests.active")
	poolIdle, _ := t.meter.Int64ObservableGauge("claudex.proxy.pool.idle")
	poolDialing, _ := t.meter.Int64ObservableGauge("claudex.proxy.pool.dialing")
	goRoutines, _ := t.meter.Int64ObservableGauge("process.runtime.go.goroutines")
	heap, _ := t.meter.Int64ObservableGauge("process.runtime.go.heap", metric.WithUnit("By"))
	gcPause, _ := t.meter.Int64ObservableGauge("process.runtime.go.gc.pause.total", metric.WithUnit("ns"))
	cpu, _ := t.meter.Float64ObservableGauge("process.cpu.time", metric.WithUnit("s"))
	rss, _ := t.meter.Int64ObservableGauge("process.memory.rss", metric.WithUnit("By"))
	openConnections, _ := t.meter.Int64ObservableGauge("process.network.connections.open", metric.WithUnit("{connection}"))
	_, _ = t.meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		options := []metric.ObserveOption(nil)
		if t.runID != "" {
			options = append(options, metric.WithAttributes(attribute.String("claudex.run_id", t.runID)))
		}
		observer.ObserveInt64(active, t.activeRequests.Load(), options...)
		observer.ObserveInt64(poolIdle, t.poolIdle.Load(), options...)
		observer.ObserveInt64(poolDialing, t.poolDialing.Load(), options...)
		observer.ObserveInt64(goRoutines, int64(runtime.NumGoroutine()), options...)
		observer.ObserveInt64(heap, int64(memory.HeapAlloc), options...)
		observer.ObserveInt64(gcPause, int64(memory.PauseTotalNs), options...)
		cpuSeconds, rssBytes := processStats()
		observer.ObserveFloat64(cpu, cpuSeconds, options...)
		observer.ObserveInt64(rss, rssBytes, options...)
		observer.ObserveInt64(openConnections, t.activeRequests.Load()+t.poolIdle.Load()+t.poolDialing.Load(), options...)
		return nil
	}, active, poolIdle, poolDialing, goRoutines, heap, gcPause, cpu, rss, openConnections)
}

func (t *Telemetry) Shutdown(ctx context.Context) error {
	var errs []error
	if t.loggerProvider != nil {
		errs = append(errs, t.loggerProvider.ForceFlush(ctx), t.loggerProvider.Shutdown(ctx))
	}
	if t.meterProvider != nil {
		errs = append(errs, t.meterProvider.ForceFlush(ctx), t.meterProvider.Shutdown(ctx))
	}
	if t.tracerProvider != nil {
		errs = append(errs, t.tracerProvider.ForceFlush(ctx), t.tracerProvider.Shutdown(ctx))
	}
	return errors.Join(errs...)
}

func HTTPMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		t := Current()
		if !t.Enabled || c.Request == nil {
			c.Next()
			return
		}
		ctx := otel.GetTextMapPropagator().Extract(c.Request.Context(), propagation.HeaderCarrier(c.Request.Header))
		path := c.Request.URL.Path
		ctx, span := t.tracer.Start(ctx, "proxy.http "+c.Request.Method+" "+path, trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(
			attribute.String("http.request.method", c.Request.Method),
			attribute.String("url.path", path),
		))
		writer := &firstFlushWriter{ResponseWriter: c.Writer, startedAt: time.Now(), ctx: ctx}
		c.Writer = writer
		t.activeRequests.Add(1)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		t.activeRequests.Add(-1)
		span.SetAttributes(attribute.Int("http.response.status_code", c.Writer.Status()))
		if c.Writer.Status() >= 500 {
			span.SetStatus(codes.Error, "server error")
		}
		span.End()
	}
}

type firstFlushWriter struct {
	gin.ResponseWriter
	once      sync.Once
	startedAt time.Time
	ctx       context.Context
}

func (w *firstFlushWriter) observe() {
	w.once.Do(func() {
		RecordWebsocketMetric(w.ctx, "downstream_first_flush", "", "", map[string]any{
			"elapsed_us": time.Since(w.startedAt).Microseconds(),
		}, false)
	})
}

func (w *firstFlushWriter) Write(payload []byte) (int, error) {
	w.observe()
	return w.ResponseWriter.Write(payload)
}

func (w *firstFlushWriter) WriteString(payload string) (int, error) {
	w.observe()
	return w.ResponseWriter.WriteString(payload)
}

func (w *firstFlushWriter) Flush() {
	w.observe()
	w.ResponseWriter.Flush()
}

// WithNetworkTrace records DNS, TCP, TLS, connection acquisition, and first
// response-byte phases when the transport exposes them through httptrace.
func WithNetworkTrace(ctx context.Context) context.Context {
	if ctx == nil || !Current().Enabled {
		return ctx
	}
	started := make(map[string]time.Time)
	var lock sync.Mutex
	start := func(phase string) {
		lock.Lock()
		started[phase] = time.Now()
		lock.Unlock()
	}
	done := func(phase string, err error) {
		lock.Lock()
		begin := started[phase]
		lock.Unlock()
		fields := map[string]any{"success": err == nil}
		if !begin.IsZero() {
			fields["duration_us"] = time.Since(begin).Microseconds()
		}
		RecordWebsocketMetric(ctx, "network_"+phase, "", "", fields, false)
	}
	clientTrace := &httptrace.ClientTrace{
		DNSStart:          func(httptrace.DNSStartInfo) { start("dns") },
		DNSDone:           func(info httptrace.DNSDoneInfo) { done("dns", info.Err) },
		ConnectStart:      func(_, _ string) { start("tcp") },
		ConnectDone:       func(_, _ string, err error) { done("tcp", err) },
		TLSHandshakeStart: func() { start("tls") },
		TLSHandshakeDone:  func(_ tls.ConnectionState, err error) { done("tls", err) },
		GotConn: func(info httptrace.GotConnInfo) {
			RecordWebsocketMetric(ctx, "network_connection_acquired", "", "", map[string]any{"reused": info.Reused}, false)
		},
		GotFirstResponseByte: func() { RecordWebsocketMetric(ctx, "network_first_response_byte", "", "", nil, false) },
	}
	return httptrace.WithClientTrace(ctx, clientTrace)
}

// RecordWebsocketMetric projects the existing prompt-free timeline into OTEL.
func RecordWebsocketMetric(ctx context.Context, name, rootCorrelation, executionCorrelation string, fields map[string]any, detached bool) {
	t := Current()
	if !t.Enabled {
		return
	}
	spanAttrs := spanAttributes(rootCorrelation, executionCorrelation, fields)
	metricAttrs := append(metricAttributes(fields), attribute.String("event.name", boundedEnum(name)))
	if t.runID != "" {
		metricAttrs = append(metricAttrs, attribute.String("claudex.run_id", t.runID))
	}
	if t.logger != nil {
		var record otellog.Record
		record.SetTimestamp(time.Now())
		record.SetObservedTimestamp(time.Now())
		record.SetEventName("proxy.websocket." + boundedEnum(name))
		record.SetBody(otellog.StringValue("prompt-free websocket lifecycle event"))
		record.AddAttributes(otellog.String("event.name", boundedEnum(name)))
		if model := normalizeModel(text(fields["model"])); model != "" {
			record.AddAttributes(otellog.String("model", model))
		}
		if source := boundedEnum(text(fields["connection_source"])); source != "" {
			record.AddAttributes(otellog.String("connection.source", source))
		}
		t.logger.Emit(ctx, record)
	}
	if t.events != nil {
		t.events.Add(ctx, 1, metric.WithAttributes(metricAttrs...))
		t.recordMeasurements(ctx, name, fields, metricAttrs)
	}
	key := correlationKey(rootCorrelation, executionCorrelation)
	if detached {
		options := []trace.SpanStartOption{trace.WithAttributes(spanAttrs...)}
		if linked, ok := t.links.Load(key); ok {
			if spanContext, valid := linked.(trace.SpanContext); valid && spanContext.IsValid() {
				options = append(options, trace.WithLinks(trace.Link{SpanContext: spanContext}))
			}
		}
		_, span := t.tracer.Start(context.Background(), "proxy.websocket."+boundedEnum(name), options...)
		span.End()
		return
	}
	span := trace.SpanFromContext(ctx)
	if span.SpanContext().IsValid() && key != ":" {
		t.links.Store(key, span.SpanContext())
	}
	span.AddEvent("proxy.websocket."+boundedEnum(name), trace.WithAttributes(spanAttrs...))
	if strings.Contains(name, "failed") || strings.Contains(name, "error") {
		span.SetStatus(codes.Error, boundedEnum(name))
	}
}

// RecordClaudeRun emits launcher-derived aggregate metrics for short sessions
// that may exit before Claude Code's periodic native metric reader flushes.
// Claude's native events and traces remain the detailed source of truth.
func RecordClaudeRun(ctx context.Context, model, terminal string, costUSD float64, durationMS, inputTokens, outputTokens, cacheReadTokens int64) {
	t := Current()
	if !t.Enabled || t.claudeRuns == nil {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("model", normalizeModel(model)),
		attribute.String("terminal.reason", boundedEnum(terminal)),
	}
	if t.runID != "" {
		attrs = append(attrs, attribute.String("claudex.run_id", t.runID))
	}
	t.claudeRuns.Add(ctx, 1, metric.WithAttributes(attrs...))
	t.claudeCost.Add(ctx, costUSD, metric.WithAttributes(attrs...))
	t.claudeDuration.Record(ctx, float64(durationMS), metric.WithAttributes(attrs...))
	for tokenType, count := range map[string]int64{"input": inputTokens, "output": outputTokens, "cache_read": cacheReadTokens} {
		t.claudeTokens.Add(ctx, count, metric.WithAttributes(appendCopy(attrs, attribute.String("token.type", tokenType))...))
	}
}

func (t *Telemetry) recordMeasurements(ctx context.Context, name string, fields map[string]any, attrs []attribute.KeyValue) {
	for _, key := range []string{"duration_us", "elapsed_us", "since_send_us", "wait_us", "age_us", "translation_us", "downstream_blocked_us", "first_event_us", "first_reasoning_delta_us", "first_output_text_delta_us"} {
		if value, ok := numeric(fields[key]); ok {
			phaseAttrs := appendCopy(attrs, attribute.String("phase", strings.TrimSuffix(key, "_us")))
			t.latency.Record(ctx, value/1000, metric.WithAttributes(phaseAttrs...))
		}
	}
	if name == "request_finished" {
		t.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	for _, key := range []string{"bytes", "client_body_bytes", "upstream_body_bytes", "upstream_bytes"} {
		if value, ok := numeric(fields[key]); ok {
			t.bytes.Add(ctx, int64(value), metric.WithAttributes(appendCopy(attrs, attribute.String("direction", key))...))
		}
	}
	for _, key := range []string{"input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens", "cache_read_tokens", "cache_creation_tokens"} {
		if value, ok := numeric(fields[key]); ok {
			t.tokens.Add(ctx, int64(value), metric.WithAttributes(appendCopy(attrs, attribute.String("token.type", strings.TrimSuffix(key, "_tokens")))...))
		}
	}
	if value, ok := numeric(fields["pool_idle"]); ok {
		t.poolIdle.Store(int64(value))
	}
	if value, ok := numeric(fields["pool_dialing"]); ok {
		t.poolDialing.Store(int64(value))
	}
}

func appendCopy(values []attribute.KeyValue, item attribute.KeyValue) []attribute.KeyValue {
	out := make([]attribute.KeyValue, len(values), len(values)+1)
	copy(out, values)
	return append(out, item)
}

func metricAttributes(fields map[string]any) []attribute.KeyValue {
	var out []attribute.KeyValue
	if value := normalizeModel(text(fields["model"])); value != "" {
		out = append(out, attribute.String("model", value))
	}
	for source, target := range map[string]string{
		"connection_source": "connection.source",
		"source_format":     "source.format",
		"reason":            "finish.reason",
		"status":            "status.code",
	} {
		if value := boundedEnum(text(fields[source])); value != "" {
			out = append(out, attribute.String(target, value))
		}
	}
	for _, key := range []string{"success", "reused", "busy", "overflow", "incremental", "rate_limited"} {
		if value, ok := fields[key].(bool); ok {
			out = append(out, attribute.Bool(strings.ReplaceAll(key, "_", "."), value))
		}
	}
	return out
}

func spanAttributes(rootCorrelation, executionCorrelation string, fields map[string]any) []attribute.KeyValue {
	out := metricAttributes(fields)
	if hash := hashCorrelation(rootCorrelation); hash != "" {
		out = append(out, attribute.String("claudex.root_hash", hash))
	}
	if hash := hashCorrelation(executionCorrelation); hash != "" {
		out = append(out, attribute.String("claudex.execution_hash", hash))
	}
	allowedNumbers := map[string]string{
		"duration_us": "duration.us", "elapsed_us": "elapsed.us", "since_send_us": "since_send.us",
		"wait_us": "wait.us", "age_us": "age.us", "translation_us": "translation.us",
		"downstream_blocked_us": "downstream_blocked.us", "input_tokens": "gen_ai.usage.input_tokens",
		"output_tokens": "gen_ai.usage.output_tokens", "cache_read_tokens": "gen_ai.usage.cache_read_tokens",
		"bytes": "message.bytes", "upstream_bytes": "upstream.bytes", "pool_idle": "pool.idle", "pool_dialing": "pool.dialing",
	}
	for source, target := range allowedNumbers {
		if value, ok := numeric(fields[source]); ok {
			out = append(out, attribute.Int64(target, int64(value)))
		}
	}
	return out
}

func correlationKey(root, execution string) string {
	return hashCorrelation(root) + ":" + hashCorrelation(execution)
}

func hashCorrelation(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:8])
}

func normalizeModel(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) > 64 {
		return "other"
	}
	for _, char := range value {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("._:-", char)) {
			return "other"
		}
	}
	return value
}

func boundedEnum(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if len(value) > 64 {
		return "other"
	}
	return normalizeModel(value)
}

func text(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	case int:
		return strconv.Itoa(typed)
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	default:
		return ""
	}
}

func numeric(value any) (float64, bool) {
	switch typed := value.(type) {
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case float64:
		return typed, true
	case jsonNumber:
		parsed, errParse := strconv.ParseFloat(string(typed), 64)
		return parsed, errParse == nil
	default:
		return 0, false
	}
}

type jsonNumber string
