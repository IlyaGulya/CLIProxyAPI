package claudexnext

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

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
		"request-log: true",
		"codex-prefer-upstream-websockets: true",
		"codex-websocket-speculative-preconnect: true",
		"codex-websocket-preconnect-replenish: true",
		"codex-websocket-preconnect-max-idle: 2",
		"codex-websocket-preconnect-ttl-seconds: 30",
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
	if !parsed.CodexPreferUpstreamWebsockets || !parsed.CodexWebsocketSpeculativePreconnect || !parsed.RequestLog {
		t.Fatalf("prepared runtime flags were not parsed: %+v", parsed.SDKConfig)
	}
}

func TestBuildClaudeArgsAddsOnlyObservabilityFlagsWithoutModelPolicy(t *testing.T) {
	t.Parallel()
	got := BuildClaudeArgs([]string{"--effort", "xhigh"}, "session-1", "/run/debug.log")
	want := []string{"--session-id", "session-1", "--debug-file", "/run/debug.log", "--effort", "xhigh"}
	if strings.Join(got, "\x00") != strings.Join(want, "\x00") {
		t.Fatalf("args = %#v, want %#v", got, want)
	}

	got = BuildClaudeArgs([]string{"--model", "custom", "--resume", "old", "--debug-file", "custom.log"}, "session-2", "/run/debug.log")
	joined := strings.Join(got, " ")
	if strings.Count(joined, "--model") != 1 || strings.Contains(joined, "session-2") || strings.Count(joined, "--debug-file") != 1 {
		t.Fatalf("user flags were overridden: %#v", got)
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
{"event":"api.websocket.metric","name":"request_prepared","model":"gpt-5.6-luna","claude_root_correlation_id":"root-1","claude_execution_correlation_id":"exec-1","prompt_cache_scope":"cache-1","prompt_prefix_fingerprint":"prefix-1"}
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
	if summary.MetricEvents["speculative_preconnect_ready"] != 1 || summary.AgentTranscripts != 1 || len(summary.TranscriptModels) != 1 {
		t.Fatalf("detached/transcript summary = %+v", summary)
	}
	if summary.Claude.TTFTMS != 500 || summary.Claude.CostUSD != 0.01 || summary.Claude.TerminalReason != "completed" {
		t.Fatalf("Claude summary = %+v", summary.Claude)
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
