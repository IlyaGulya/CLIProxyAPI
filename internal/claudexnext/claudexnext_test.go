package claudexnext

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestProcessOwnerStopIsIdempotent(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 30")
	if errStart := cmd.Start(); errStart != nil {
		t.Fatalf("start fixture: %v", errStart)
	}
	owner := &processOwner{cmd: cmd}
	started := time.Now()
	first := owner.Stop()
	second := owner.Stop()
	if first != second {
		t.Fatalf("Stop results differ: first=%v second=%v", first, second)
	}
	if time.Since(started) > 5*time.Second {
		t.Fatal("idempotent stop exceeded termination bound")
	}
}

type fakeCommandRunner struct {
	outputs map[string]struct {
		output []byte
		err    error
	}
	calls []string
}

func (f *fakeCommandRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	call := strings.Join(append([]string{name}, args...), " ")
	f.calls = append(f.calls, call)
	result := f.outputs[call]
	return result.output, result.err
}

func TestPrepareConfigEnablesObservableWebsocketPath(t *testing.T) {
	t.Parallel()
	input := []byte(`host: 0.0.0.0
port: 8317
auth-dir: ~/.cli-proxy-api
api-keys:
  - secret-value
debug: false
request-log: false
codex-websocket-generate-false-warmup: true
`)
	got, err := PrepareConfig(input, 18432)
	if err != nil {
		t.Fatalf("PrepareConfig: %v", err)
	}
	text := string(got)
	for _, want := range []string{
		"host: 127.0.0.1",
		"port: 18432",
		"debug: true",
		"logging-to-file: true",
		"request-log: false",
		"logs-max-total-size-mb: 128",
		"codex-prefer-upstream-websockets: true",
		"claude-code-auto-mode-classifier-model: gpt-5.6-luna",
		"codex-websocket-speculative-preconnect: true",
		"codex-websocket-preconnect-replenish: true",
		"codex-websocket-preconnect-max-idle: 2",
		"codex-websocket-preconnect-ttl-seconds: 30",
		"codex-websocket-adaptive-preconnect: true",
		"codex-cache-aware-compaction: true",
		"codex-websocket-circuit-breaker: true",
		"codex-websocket-generate-false-warmup: false",
		"secret-value",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("prepared config missing %q:\n%s", want, text)
		}
	}
	parsed, errParse := config.ParseConfigBytes(got)
	if errParse != nil {
		t.Fatalf("parse prepared config: %v", errParse)
	}
	if !parsed.CodexPreferUpstreamWebsockets || !parsed.CodexWebsocketSpeculativePreconnect || parsed.RequestLog {
		t.Fatalf("prepared runtime flags were not parsed: %+v", parsed.SDKConfig)
	}
	if parsed.ClaudeCodeAutoModeClassifierModel != "gpt-5.6-luna" {
		t.Fatalf("classifier model = %q, want gpt-5.6-luna", parsed.ClaudeCodeAutoModeClassifierModel)
	}
	aliases := aliasesByName(parsed.OAuthModelAlias["codex"])
	if aliases["sol"] != "gpt-5.6-sol" || aliases["luna"] != "gpt-5.6-luna" || aliases["terra"] != "gpt-5.6-terra" {
		t.Fatalf("workflow aliases = %#v, want sol/luna/terra defaults", aliases)
	}
}

func TestPrepareConfigPreservesExplicitClassifierModel(t *testing.T) {
	t.Parallel()
	got, err := PrepareConfig([]byte("claude-code-auto-mode-classifier-model: gpt-5.6-sol\n"), 18432)
	if err != nil {
		t.Fatalf("PrepareConfig: %v", err)
	}
	parsed, errParse := config.ParseConfigBytes(got)
	if errParse != nil {
		t.Fatalf("parse prepared config: %v", errParse)
	}
	if parsed.ClaudeCodeAutoModeClassifierModel != "gpt-5.6-sol" {
		t.Fatalf("classifier model = %q, want preserved gpt-5.6-sol", parsed.ClaudeCodeAutoModeClassifierModel)
	}
}

func TestPrepareConfigPreservesExplicitWorkflowModelAliases(t *testing.T) {
	t.Parallel()
	input := []byte(`oauth-model-alias:
  codex:
    - name: custom-sol
      alias: sol
    - name: custom-other
      alias: other
`)
	got, err := PrepareConfig(input, 18432)
	if err != nil {
		t.Fatalf("PrepareConfig: %v", err)
	}
	parsed, errParse := config.ParseConfigBytes(got)
	if errParse != nil {
		t.Fatalf("parse prepared config: %v", errParse)
	}
	aliases := aliasesByName(parsed.OAuthModelAlias["codex"])
	if aliases["sol"] != "custom-sol" {
		t.Fatalf("explicit sol alias = %q, want custom-sol", aliases["sol"])
	}
	if aliases["luna"] != "gpt-5.6-luna" {
		t.Fatalf("default luna alias = %q, want gpt-5.6-luna", aliases["luna"])
	}
	if aliases["other"] != "custom-other" {
		t.Fatalf("unrelated alias = %q, want custom-other", aliases["other"])
	}
}

func aliasesByName(entries []config.OAuthModelAlias) map[string]string {
	out := make(map[string]string, len(entries))
	for _, entry := range entries {
		out[entry.Alias] = entry.Name
	}
	return out
}

func TestBuildClaudeArgsDefaultsRootToSolWithoutOverridingUserFlags(t *testing.T) {
	t.Parallel()
	got := BuildClaudeArgs([]string{"--effort", "xhigh"}, "session-1", "/run/debug.log")
	want := []string{"--model", "gpt-5.6-sol", "--session-id", "session-1", "--debug-file", "/run/debug.log", "--settings", `{"skillOverrides":{"claude-api":"user-invocable-only"}}`, "--effort", "xhigh"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}

	got = BuildClaudeArgs([]string{"--model", "custom", "--resume", "old", "--debug-file", "custom.log"}, "session-2", "/run/debug.log")
	joined := strings.Join(got, " ")
	if strings.Count(joined, "--model") != 1 || strings.Contains(joined, "session-2") || strings.Count(joined, "--debug-file") != 1 {
		t.Fatalf("user flags were overridden: %#v", got)
	}
}

func TestBuildClaudeArgsPreservesExplicitSkillSettings(t *testing.T) {
	t.Parallel()
	got := BuildClaudeArgs([]string{"--settings", "/tmp/custom.json"}, "session", "/tmp/debug")
	joined := strings.Join(got, "\x00")
	if strings.Count(joined, "--settings") != 1 || !strings.Contains(joined, "/tmp/custom.json") || strings.Contains(joined, "skillOverrides") {
		t.Fatalf("explicit settings changed: %#v", got)
	}
}

func TestConfigureClaudeContextSafetyUsesCodexCompatibleHeadroom(t *testing.T) {
	t.Parallel()
	values := map[string]string{}
	ConfigureClaudeContextSafety(values)
	if got := values["CLAUDE_CODE_AUTO_COMPACT_WINDOW"]; got != "180000" {
		t.Fatalf("auto compact window = %q, want 180000", got)
	}
	if got := values["CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"]; got != "80" {
		t.Fatalf("auto compact percent = %q, want 80", got)
	}
}

func TestConfigureClaudeContextSafetyPreservesExplicitOverrides(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"CLAUDE_CODE_AUTO_COMPACT_WINDOW": "123456",
		"CLAUDE_AUTOCOMPACT_PCT_OVERRIDE": "77",
	}
	ConfigureClaudeContextSafety(values)
	if values["CLAUDE_CODE_AUTO_COMPACT_WINDOW"] != "123456" || values["CLAUDE_AUTOCOMPACT_PCT_OVERRIDE"] != "77" {
		t.Fatalf("explicit context settings changed: %#v", values)
	}
}

func TestAnalyzeRunAggregatesTransportCacheAndRouting(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	logs := filepath.Join(dir, "proxy", "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	request := `=== REQUEST INFO ===
URL: /v1/messages
Timestamp: 2026-07-16T17:23:04Z
X-Claude-Code-Agent-Id: child-1
=== API WEBSOCKET TIMELINE ===
{"event":"api.websocket.metric","name":"request_prepared","model":"gpt-5.6-luna","claude_root_correlation_id":"root-1","claude_execution_correlation_id":"exec-1","prompt_cache_scope":"cache-1","prompt_prefix_fingerprint":"prefix-1","chain_source":"full_replay","incremental":false,"incremental_reset_reason":"no_previous_response","client_body_bytes":1200,"upstream_body_bytes":1200}
{"event":"api.websocket.metric","name":"speculative_preconnect_leased","wait_us":23}
{"event":"api.websocket.metric","name":"connection_ready","connection_source":"speculative","duration_us":46,"success":true}
{"event":"api.websocket.metric","name":"request_sent","elapsed_us":100,"duration_us":4,"success":true}
{"event":"api.websocket.metric","name":"first_upstream_event","since_send_us":500000}
{"event":"api.websocket.metric","name":"first_output_text_delta","since_send_us":900000}
{"event":"api.websocket.metric","name":"usage","input_tokens":5113,"cache_read_tokens":4864,"output_tokens":8}
{"event":"api.websocket.metric","name":"request_finished","elapsed_us":1100000,"reason":"completed","translation_us":30,"downstream_blocked_us":40}
`
	if err := os.WriteFile(filepath.Join(logs, "v1-messages-test.log"), []byte(request), 0o600); err != nil {
		t.Fatal(err)
	}
	mainLog := `[debug] detached websocket metric: {"event":"api.websocket.metric","name":"speculative_preconnect_ready"}` + "\n"
	if err := os.WriteFile(filepath.Join(logs, "main.log"), []byte(mainLog), 0o600); err != nil {
		t.Fatal(err)
	}
	transcripts := filepath.Join(dir, "transcripts", "session", "subagents")
	if err := os.MkdirAll(transcripts, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(transcripts, "agent-one.jsonl"), []byte(`{"message":{"model":"gpt-5.6-luna"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	claudeDir := filepath.Join(dir, "claude")
	if err := os.MkdirAll(claudeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	claudeResult := `{"type":"result","subtype":"success","duration_ms":1200,"duration_api_ms":1100,"ttft_ms":500,"ttft_stream_ms":450,"time_to_request_ms":10,"num_turns":1,"total_cost_usd":0.01,"session_id":"session-1","terminal_reason":"completed"}` + "\n"
	if err := os.WriteFile(filepath.Join(claudeDir, "stdout.log"), []byte(claudeResult), 0o600); err != nil {
		t.Fatal(err)
	}
	summary, err := AnalyzeRun(dir)
	if err != nil {
		t.Fatalf("AnalyzeRun: %v", err)
	}
	if summary.RequestCount != 1 || summary.Models["gpt-5.6-luna"] != 1 {
		t.Fatalf("routing summary = %+v", summary)
	}
	if summary.SpeculativeHitRate != 1 || summary.CacheReadRatio < 0.95 || summary.Requests[0].FirstEventMS != 500 {
		t.Fatalf("metrics summary = %+v", summary)
	}
	if summary.Requests[0].ChainSource != "full_replay" || summary.Requests[0].IncrementalReset != "no_previous_response" || summary.Requests[0].ClientBodyBytes != 1200 {
		t.Fatalf("restart observability summary = %+v", summary.Requests[0])
	}
	if summary.MetricEvents["speculative_preconnect_ready"] != 1 || summary.AgentTranscripts != 1 || len(summary.TranscriptModels) != 1 {
		t.Fatalf("detached/transcript summary = %+v", summary)
	}
	if summary.Claude.TTFTMS != 500 || summary.Claude.CostUSD != 0.01 || summary.Claude.TerminalReason != "completed" {
		t.Fatalf("Claude summary = %+v", summary.Claude)
	}
}

func TestAnalyzeRunUsesEventJournalWhenRequestLogDisabled(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	eventsDir := filepath.Join(dir, "proxy", "events")
	if err := os.MkdirAll(eventsDir, 0o700); err != nil {
		t.Fatal(err)
	}
	journal := strings.Join([]string{
		`{"schema":1,"name":"request_prepared","request_key":"req-1","role":"child","model":"gpt-5.6-luna","chain_source":"full_replay","client_body_bytes":1200}`,
		`{"schema":1,"name":"connection_ready","request_key":"req-1","connection_source":"speculative","duration_us":46000}`,
		`{"schema":1,"name":"usage","request_key":"req-1","input_tokens":5113,"output_tokens":8,"cache_read_tokens":4864}`,
		`{"schema":1,"name":"request_finished","request_key":"req-1","elapsed_us":1100000,"reason":"completed"}`,
		`{"schema":1,"name":"truncated"`,
	}, "\n")
	if err := os.WriteFile(filepath.Join(eventsDir, "requests.jsonl"), []byte(journal), 0o600); err != nil {
		t.Fatal(err)
	}
	summary, err := AnalyzeRun(dir)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RequestCount != 1 || summary.ChildRequests != 1 || summary.Models["gpt-5.6-luna"] != 1 {
		t.Fatalf("journal summary = %+v", summary)
	}
	if summary.SpeculativeHitRate != 1 || summary.CacheReadRatio < .95 || summary.Requests[0].TotalMS != 1100 {
		t.Fatalf("journal metrics = %+v", summary)
	}
}

func TestLoadEnvFileDoesNotOverrideExistingEnvironment(t *testing.T) {
	t.Setenv("ANTHROPIC_AUTH_TOKEN", "existing")
	t.Setenv("CLAUDE_CODE_SUBAGENT_MODEL", "user-selected-model")
	path := filepath.Join(t.TempDir(), "claudex.env")
	if err := os.WriteFile(path, []byte("export ANTHROPIC_AUTH_TOKEN=from-file\nexport ANTHROPIC_BASE_URL='http://old'\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	env, err := LoadEnvFile(path, os.Environ())
	if err != nil {
		t.Fatalf("LoadEnvFile: %v", err)
	}
	values := envMap(env)
	if values["ANTHROPIC_AUTH_TOKEN"] != "existing" || values["ANTHROPIC_BASE_URL"] != "http://old" {
		t.Fatalf("loaded env = %#v", values)
	}
	if values["CLAUDE_CODE_SUBAGENT_MODEL"] != "user-selected-model" {
		t.Fatalf("Claude routing environment was changed: %#v", values)
	}
}

func TestPrepareAuthDirEnablesWebsocketsOnlyInPrivateCopy(t *testing.T) {
	t.Parallel()
	source := t.TempDir()
	destination := filepath.Join(t.TempDir(), "auth")
	original := []byte(`{"type":"codex","access_token":"secret"}`)
	if err := os.WriteFile(filepath.Join(source, "codex.json"), original, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := PrepareAuthDir(source, destination, t.TempDir()); err != nil {
		t.Fatalf("PrepareAuthDir: %v", err)
	}
	got, errRead := os.ReadFile(filepath.Join(destination, "codex.json"))
	if errRead != nil {
		t.Fatal(errRead)
	}
	if !strings.Contains(string(got), `"websockets":true`) || !strings.Contains(string(got), `"access_token":"secret"`) {
		t.Fatalf("private credential = %s", got)
	}
	unchanged, _ := os.ReadFile(filepath.Join(source, "codex.json"))
	if string(unchanged) != string(original) {
		t.Fatalf("source credential was mutated: %s", unchanged)
	}
}

func TestSessionIDFromRequestLogsSupportsResumeRuns(t *testing.T) {
	t.Parallel()
	logs := t.TempDir()
	content := "=== HEADERS ===\nX-Claude-Code-Session-Id: resumed-session\n"
	if err := os.WriteFile(filepath.Join(logs, "v1-messages-test.log"), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := sessionIDFromRequestLogs(logs); got != "resumed-session" {
		t.Fatalf("session ID = %q", got)
	}
}

func TestConfigureClaudeOTELUsesPrivateLowCardinalityDefaults(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc",
		"OTEL_RESOURCE_ATTRIBUTES":    "team.id=platform",
	}
	ConfigureClaudeOTEL(values, "http://127.0.0.1:4318", "run-123")

	wants := map[string]string{
		"CLAUDE_CODE_ENABLE_TELEMETRY":             "1",
		"CLAUDE_CODE_ENHANCED_TELEMETRY_BETA":      "1",
		"CLAUDE_CODE_PROPAGATE_TRACEPARENT":        "1",
		"OTEL_METRICS_EXPORTER":                    "otlp",
		"OTEL_LOGS_EXPORTER":                       "otlp",
		"OTEL_TRACES_EXPORTER":                     "otlp",
		"OTEL_EXPORTER_OTLP_PROTOCOL":              "grpc",
		"OTEL_METRICS_INCLUDE_SESSION_ID":          "false",
		"OTEL_METRICS_INCLUDE_ACCOUNT_UUID":        "false",
		"OTEL_METRICS_INCLUDE_RESOURCE_ATTRIBUTES": "false",
		"OTEL_LOG_USER_PROMPTS":                    "0",
		"OTEL_LOG_ASSISTANT_RESPONSES":             "0",
		"OTEL_LOG_TOOL_DETAILS":                    "0",
		"OTEL_LOG_TOOL_CONTENT":                    "0",
		"OTEL_LOG_RAW_API_BODIES":                  "0",
	}
	for key, want := range wants {
		if got := values[key]; got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	if got := values["OTEL_RESOURCE_ATTRIBUTES"]; got != "team.id=platform,service.name=claude-code,claudex.run_id=run-123" {
		t.Fatalf("resource attributes = %q", got)
	}
}

func TestConfigureClaudeOTELPreservesExplicitPrivacyAndExporterSettings(t *testing.T) {
	t.Parallel()
	values := map[string]string{
		"OTEL_EXPORTER_OTLP_ENDPOINT": "https://collector.example",
		"OTEL_LOG_TOOL_DETAILS":       "1",
		"OTEL_METRIC_EXPORT_INTERVAL": "9000",
	}
	ConfigureClaudeOTEL(values, "http://127.0.0.1:4318", "run-1")
	if values["OTEL_EXPORTER_OTLP_ENDPOINT"] != "https://collector.example" || values["OTEL_LOG_TOOL_DETAILS"] != "1" || values["OTEL_METRIC_EXPORT_INTERVAL"] != "9000" {
		t.Fatalf("explicit settings were overwritten: %#v", values)
	}
}

func TestEnsureLGTMReusesRunningContainer(t *testing.T) {
	t.Parallel()
	runner := &fakeCommandRunner{outputs: map[string]struct {
		output []byte
		err    error
	}{
		"docker inspect --format {{.State.Running}}|{{.Config.Image}} claudex-next-otel-lgtm": {output: []byte("true|grafana/otel-lgtm:0.27.1\n")},
	}}
	status := EnsureLGTM(context.Background(), runner, func(context.Context, string) error { return nil })
	if !status.Available || status.Started || len(runner.calls) != 1 {
		t.Fatalf("status = %+v, calls = %#v", status, runner.calls)
	}
}

func TestEnsureLGTMStartsPinnedPersistentContainer(t *testing.T) {
	t.Parallel()
	runner := &fakeCommandRunner{outputs: map[string]struct {
		output []byte
		err    error
	}{
		"docker inspect --format {{.State.Running}}|{{.Config.Image}} claudex-next-otel-lgtm": {err: os.ErrNotExist},
	}}
	status := EnsureLGTM(context.Background(), runner, func(context.Context, string) error { return nil })
	if !status.Available || !status.Started {
		t.Fatalf("status = %+v", status)
	}
	joined := strings.Join(runner.calls, "\n")
	for _, want := range []string{
		"grafana/otel-lgtm:0.27.1", "claudex-next-otel-lgtm-data:/data",
		"127.0.0.1:3300:3000", "127.0.0.1:4317:4317", "127.0.0.1:4318:4318",
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("docker calls missing %q:\n%s", want, joined)
		}
	}
}

func TestEnsureLGTMDegradesWhenDockerUnavailable(t *testing.T) {
	t.Parallel()
	runner := &fakeCommandRunner{outputs: map[string]struct {
		output []byte
		err    error
	}{
		"docker inspect --format {{.State.Running}}|{{.Config.Image}} claudex-next-otel-lgtm": {err: os.ErrNotExist},
	}}
	status := EnsureLGTM(context.Background(), runner, func(context.Context, string) error { return os.ErrNotExist })
	if status.Available || status.Error == "" {
		t.Fatalf("status = %+v", status)
	}
}

func TestProvisionGrafanaDashboardUsesStableUIDAndNoSecrets(t *testing.T) {
	t.Parallel()
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/dashboards/db" || r.Method != http.MethodPost {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		if user, password, ok := r.BasicAuth(); !ok || user != "admin" || password != "admin" {
			t.Errorf("missing local Grafana auth")
		}
		payload, _ := io.ReadAll(r.Body)
		body = string(payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	if err := ProvisionGrafana(context.Background(), server.URL, server.Client()); err != nil {
		t.Fatalf("ProvisionGrafana: %v", err)
	}
	for _, want := range []string{
		"claudex-next-overview", "claudex.run_id", "claudex_proxy", "tempo", "prometheus", "loki",
		"Mid-response WebSocket failures", "transport_close_code", "stream_last_event_type",
		"tool_call_in_progress", "downstream_committed", "connection_age",
		"WebSocket circuit decisions", "circuit_(failure|success|suppressed|probe)",
		"Compaction V2 replay shaping", "compaction_applied",
		"Adaptive preconnect decisions", "speculative_preconnect_adaptive_decision",
		"Cache-aware compaction schedule", "compaction_schedule_decision",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	for _, forbidden := range []string{"ANTHROPIC_AUTH_TOKEN", "access_token", "request_body", "user_prompt"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("dashboard contains sensitive field %q", forbidden)
		}
	}
}

func TestEvidenceChecksumsDetectMutation(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "summary.json")
	if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := writeChecksums(dir); err != nil {
		t.Fatal(err)
	}
	if err := verifyChecksums(dir); err != nil {
		t.Fatalf("fresh checksums: %v", err)
	}
	if err := os.WriteFile(path, []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verifyChecksums(dir); err == nil {
		t.Fatal("mutated evidence passed checksum verification")
	}
}

func TestIntersectsAllRequiresOneTraceSharedByEveryService(t *testing.T) {
	t.Parallel()
	launcher := map[string]struct{}{"shared": {}, "launcher-only": {}}
	claude := map[string]struct{}{"shared": {}, "claude-only": {}}
	proxy := map[string]struct{}{"shared": {}, "proxy-only": {}}
	if !intersectsAll(launcher, claude, proxy) {
		t.Fatal("shared trace was not detected")
	}
	delete(proxy, "shared")
	if intersectsAll(launcher, claude, proxy) {
		t.Fatal("pairwise-only trace was accepted as three-service correlation")
	}
}

func TestCorrelatedServiceTracesWaitsForTempoIngestion(t *testing.T) {
	t.Parallel()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		traceID := []string{"launcher-only", "claude-only", "proxy-only"}[(requests-1)%3]
		if requests > 3 {
			traceID = "shared"
		}
		_, _ = io.WriteString(w, `{"traces":[{"traceID":"`+traceID+`"}]}`)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ok, err := correlatedServiceTraces(ctx, server.Client(), server.URL, "run-1", 10*time.Millisecond)
	if err != nil || !ok {
		t.Fatalf("correlatedServiceTraces() = %t, %v", ok, err)
	}
	if requests <= 3 {
		t.Fatalf("verifier did not retry incomplete ingestion: requests=%d", requests)
	}
}
