package claudexnext

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Options struct {
	ClaudeArgs  []string
	ClaudeBin   string
	ProxyBin    string
	ConfigPath  string
	EnvPath     string
	RunsDir     string
	Stdout      io.Writer
	Stderr      io.Writer
	Stdin       io.Reader
	Interactive bool
}

type Manifest struct {
	RunID           string            `json:"run_id"`
	SessionID       string            `json:"session_id"`
	StartedAt       time.Time         `json:"started_at"`
	FinishedAt      time.Time         `json:"finished_at"`
	WorkingDir      string            `json:"working_directory"`
	ClaudeVersion   string            `json:"claude_version"`
	ClaudeArgs      []string          `json:"claude_args"`
	RootModel       string            `json:"root_model"`
	SubagentModel   string            `json:"subagent_model"`
	MaxConcurrency  string            `json:"max_tool_use_concurrency"`
	ProxyBinary     string            `json:"proxy_binary"`
	ProxySHA256     string            `json:"proxy_sha256"`
	ProxyPort       int               `json:"proxy_port"`
	ExitCode        int               `json:"exit_code"`
	Stack           StackStatus       `json:"observability_stack"`
	ProxyReadyMS    int64             `json:"proxy_ready_ms"`
	ClaudeRuntimeMS int64             `json:"claude_runtime_ms"`
	OTELFlushOK     bool              `json:"otel_flush_ok"`
	Telemetry       map[string]string `json:"telemetry_privacy"`
}

// Run launches an isolated instrumented proxy and one Claude session.
func Run(ctx context.Context, opts Options) (string, int, error) {
	home, errHome := os.UserHomeDir()
	if errHome != nil {
		return "", 1, fmt.Errorf("resolve home directory: %w", errHome)
	}
	defaults(&opts, home)
	workingDir, errWD := os.Getwd()
	if errWD != nil {
		return "", 1, fmt.Errorf("resolve working directory: %w", errWD)
	}
	startedAt := time.Now()
	runID := startedAt.Format("20060102T150405") + "-" + uuid.NewString()[:8]
	sessionID := uuid.NewString()
	runDir := filepath.Join(opts.RunsDir, runID)
	for _, dir := range []string{runDir, filepath.Join(runDir, "private"), filepath.Join(runDir, "proxy"), filepath.Join(runDir, "claude"), filepath.Join(runDir, "transcripts")} {
		if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
			return runDir, 1, fmt.Errorf("create run directory: %w", errMkdir)
		}
	}
	port, errPort := freePort()
	if errPort != nil {
		return runDir, 1, errPort
	}
	configInput, errReadConfig := os.ReadFile(opts.ConfigPath)
	if errReadConfig != nil {
		return runDir, 1, fmt.Errorf("read proxy config: %w", errReadConfig)
	}
	sourceAuthDir, errAuthConfig := ConfigAuthDir(configInput)
	if errAuthConfig != nil {
		return runDir, 1, errAuthConfig
	}
	runtimeAuthDir := filepath.Join(runDir, "private", "auth")
	if errAuth := PrepareAuthDir(sourceAuthDir, runtimeAuthDir, home); errAuth != nil {
		return runDir, 1, errAuth
	}
	configOutput, errConfig := PrepareConfig(configInput, port, runtimeAuthDir)
	if errConfig != nil {
		return runDir, 1, errConfig
	}
	runtimeConfig := filepath.Join(runDir, "private", "config.yaml")
	if errWrite := os.WriteFile(runtimeConfig, configOutput, 0o600); errWrite != nil {
		return runDir, 1, fmt.Errorf("write proxy config: %w", errWrite)
	}

	baseEnv, errEnv := LoadEnvFile(opts.EnvPath, os.Environ())
	if errEnv != nil {
		return runDir, 1, errEnv
	}
	values := envMap(baseEnv)
	ConfigureClaudeContextSafety(values)
	values["ANTHROPIC_BASE_URL"] = "http://127.0.0.1:" + strconv.Itoa(port)
	values["CLAUDEX_NEXT_RUN_ID"] = runID
	stack := StackStatus{Image: LGTMImage, Grafana: GrafanaURL, Endpoint: LGTMEndpoint}
	explicitEndpoint := strings.TrimSpace(values["OTEL_EXPORTER_OTLP_ENDPOINT"])
	if explicitEndpoint != "" {
		stack.Available = true
		stack.Endpoint = explicitEndpoint
	} else if !strings.EqualFold(strings.TrimSpace(values["CLAUDEX_NEXT_OTEL_STACK"]), "off") {
		stack = EnsureLGTM(ctx, ExecCommandRunner{}, WaitForHTTP)
		if stack.Available {
			if errDashboard := ProvisionGrafana(ctx, stack.Grafana, nil); errDashboard != nil {
				stack.DashboardError = errDashboard.Error()
			} else {
				stack.Dashboard = true
			}
		}
	}
	if stack.Available {
		ConfigureClaudeOTEL(values, stack.Endpoint, runID)
	}
	_ = writeJSON(filepath.Join(runDir, "observability-stack.json"), stack)

	var launcherTelemetry *observability.Telemetry
	var launcherSpan trace.Span
	if stack.Available {
		launcherEnv := cloneEnv(values)
		launcherEnv["OTEL_SERVICE_NAME"] = "claudex-next"
		for key, value := range launcherEnv {
			_ = os.Setenv(key, value)
		}
		launcherTelemetry, _ = observability.StartService(ctx, "claudex-next")
		ctx, launcherSpan = otel.Tracer("claudex-next").Start(ctx, "claudex-next.run",
			trace.WithTimestamp(startedAt),
			trace.WithAttributes(attribute.String("claudex.run_id", runID), attribute.Bool("lgtm.started", stack.Started), attribute.Bool("grafana.dashboard.provisioned", stack.Dashboard)),
		)
		launcherSpan.AddEvent("observability.stack.ready")
		carrier := propagation.HeaderCarrier{}
		otel.GetTextMapPropagator().Inject(ctx, carrier)
		if traceparent := carrier.Get("traceparent"); traceparent != "" {
			values["TRACEPARENT"] = traceparent
		}
	}

	proxyLog, errProxyLog := os.OpenFile(filepath.Join(runDir, "proxy", "process.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if errProxyLog != nil {
		return runDir, 1, fmt.Errorf("create proxy process log: %w", errProxyLog)
	}
	defer func() { _ = proxyLog.Close() }()
	proxyCmd := exec.CommandContext(ctx, opts.ProxyBin, "--config", runtimeConfig, "--local-model")
	proxyCmd.Dir = filepath.Join(runDir, "proxy")
	proxyEnv := cloneEnv(values)
	proxyEnv["WRITABLE_PATH"] = filepath.Join(runDir, "proxy")
	if stack.Available {
		ConfigureProxyOTEL(proxyEnv, stack.Endpoint, runID)
	}
	proxyCmd.Env = flattenEnv(proxyEnv)
	proxyCmd.Stdout = proxyLog
	proxyCmd.Stderr = proxyLog
	if errStart := proxyCmd.Start(); errStart != nil {
		return runDir, 1, fmt.Errorf("start proxy %s: %w", opts.ProxyBin, errStart)
	}
	defer stopProcess(proxyCmd)
	proxyReadyStartedAt := time.Now()
	if errReady := waitForProxy(ctx, port, 15*time.Second); errReady != nil {
		return runDir, 1, fmt.Errorf("proxy did not become ready (see %s): %w", proxyLog.Name(), errReady)
	}
	proxyReadyMS := time.Since(proxyReadyStartedAt).Milliseconds()
	if launcherSpan != nil {
		launcherSpan.AddEvent("proxy.ready", trace.WithAttributes(attribute.Int64("duration.ms", proxyReadyMS)))
	}

	debugPath := filepath.Join(runDir, "claude", "debug.log")
	claudeArgs := BuildClaudeArgs(opts.ClaudeArgs, sessionID, debugPath)
	commandName := opts.ClaudeBin
	commandArgs := claudeArgs
	if opts.Interactive && runtime.GOOS != "windows" {
		if scriptPath, errLook := exec.LookPath("script"); errLook == nil {
			commandName = scriptPath
			commandArgs = append([]string{"-q", "-e", filepath.Join(runDir, "claude", "terminal.typescript"), opts.ClaudeBin}, claudeArgs...)
		}
	}
	claudeCmd := exec.CommandContext(ctx, commandName, commandArgs...)
	claudeCmd.Dir = workingDir
	claudeCmd.Env = flattenEnv(values)
	claudeCmd.Stdin = opts.Stdin
	claudeCmd.Stdout = opts.Stdout
	claudeCmd.Stderr = opts.Stderr
	var claudeStdout, claudeStderr *os.File
	if !opts.Interactive {
		claudeStdout, _ = os.OpenFile(filepath.Join(runDir, "claude", "stdout.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		claudeStderr, _ = os.OpenFile(filepath.Join(runDir, "claude", "stderr.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
		if claudeStdout != nil {
			defer func() { _ = claudeStdout.Close() }()
			claudeCmd.Stdout = io.MultiWriter(opts.Stdout, claudeStdout)
		}
		if claudeStderr != nil {
			defer func() { _ = claudeStderr.Close() }()
			claudeCmd.Stderr = io.MultiWriter(opts.Stderr, claudeStderr)
		}
	}
	if opts.Stderr != nil {
		_, _ = fmt.Fprintf(opts.Stderr, "\n[claudex-next] run: %s\n[claudex-next] artifacts: %s\n\n", runID, runDir)
	}
	claudeStartedAt := time.Now()
	if launcherSpan != nil {
		launcherSpan.AddEvent("claude.started")
	}
	errRun := claudeCmd.Run()
	claudeRuntimeMS := time.Since(claudeStartedAt).Milliseconds()
	exitCode := exitStatus(errRun)
	if launcherSpan != nil {
		launcherSpan.AddEvent("claude.exited", trace.WithAttributes(attribute.Int("process.exit.code", exitCode), attribute.Int64("duration.ms", claudeRuntimeMS)))
		if errRun != nil {
			launcherSpan.SetStatus(codes.Error, "Claude exited unsuccessfully")
		}
	}
	proxyStopped := stopProcess(proxyCmd)
	if launcherSpan != nil {
		launcherSpan.AddEvent("proxy.stopped", trace.WithAttributes(attribute.Bool("graceful", proxyStopped)))
	}
	_ = proxyLog.Sync()
	if observedSessionID := sessionIDFromRequestLogs(filepath.Join(runDir, "proxy", "logs")); observedSessionID != "" {
		sessionID = observedSessionID
	}
	copySessionTranscripts(home, sessionID, filepath.Join(runDir, "transcripts"))
	summary, errAnalyze := AnalyzeRun(runDir)
	if errAnalyze != nil {
		return runDir, exitCode, errAnalyze
	}
	var inputTokens, outputTokens, cacheReadTokens int64
	for _, request := range summary.Requests {
		inputTokens += request.InputTokens
		outputTokens += request.OutputTokens
		cacheReadTokens += request.CacheReadTokens
	}
	observability.RecordClaudeRun(ctx, flagValue(claudeArgs, "--model"), summary.Claude.TerminalReason, summary.Claude.CostUSD, claudeRuntimeMS, inputTokens, outputTokens, cacheReadTokens)
	otelFlushOK := false
	if launcherSpan != nil {
		launcherSpan.End()
	}
	if launcherTelemetry != nil {
		flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
		otelFlushOK = launcherTelemetry.Shutdown(flushCtx) == nil
		cancelFlush()
	}

	proxyHash, _ := fileSHA256(opts.ProxyBin)
	manifest := Manifest{
		RunID: runID, SessionID: sessionID, StartedAt: startedAt, FinishedAt: time.Now(), WorkingDir: workingDir,
		ClaudeVersion: commandVersion(opts.ClaudeBin), ClaudeArgs: redactArgs(claudeArgs), RootModel: flagValue(claudeArgs, "--model"),
		SubagentModel: values["CLAUDE_CODE_SUBAGENT_MODEL"], MaxConcurrency: values["CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY"],
		ProxyBinary: opts.ProxyBin, ProxySHA256: proxyHash, ProxyPort: port, ExitCode: exitCode,
		Stack: stack, ProxyReadyMS: proxyReadyMS, ClaudeRuntimeMS: claudeRuntimeMS, OTELFlushOK: otelFlushOK && proxyStopped && !fileContains(proxyLog.Name(), "OpenTelemetry flush failed"),
		Telemetry: telemetryPrivacy(values),
	}
	if errWrite := writeJSON(filepath.Join(runDir, "manifest.json"), manifest); errWrite != nil {
		return runDir, exitCode, errWrite
	}
	if errWrite := writeJSON(filepath.Join(runDir, "summary.json"), summary); errWrite != nil {
		return runDir, exitCode, errWrite
	}
	if errWrite := os.WriteFile(filepath.Join(runDir, "summary.md"), []byte(RenderMarkdown(manifest, summary)), 0o600); errWrite != nil {
		return runDir, exitCode, fmt.Errorf("write summary: %w", errWrite)
	}
	if errChecksums := writeChecksums(runDir); errChecksums != nil {
		return runDir, exitCode, errChecksums
	}
	updateLatest(opts.RunsDir, runDir)
	if opts.Stderr != nil {
		_, _ = fmt.Fprintf(opts.Stderr, "\n[claudex-next] summary: %s\n", filepath.Join(runDir, "summary.md"))
	}
	return runDir, exitCode, errRun
}

func fileContains(path, needle string) bool {
	payload, errRead := os.ReadFile(path)
	return errRead == nil && strings.Contains(string(payload), needle)
}

func telemetryPrivacy(values map[string]string) map[string]string {
	out := make(map[string]string)
	for _, key := range []string{"OTEL_LOG_USER_PROMPTS", "OTEL_LOG_ASSISTANT_RESPONSES", "OTEL_LOG_TOOL_DETAILS", "OTEL_LOG_TOOL_CONTENT", "OTEL_LOG_RAW_API_BODIES"} {
		out[key] = values[key]
	}
	return out
}

func defaults(opts *Options, home string) {
	if opts.ClaudeBin == "" {
		opts.ClaudeBin = "claude"
	}
	if opts.ConfigPath == "" {
		opts.ConfigPath = filepath.Join(home, ".cli-proxy-api", "config.yaml")
	}
	if opts.EnvPath == "" {
		opts.EnvPath = filepath.Join(home, ".cli-proxy-api", "claudex.env")
	}
	if opts.RunsDir == "" {
		opts.RunsDir = filepath.Join(home, ".claudex-next", "runs")
	}
	if opts.Stdout == nil {
		opts.Stdout = os.Stdout
	}
	if opts.Stderr == nil {
		opts.Stderr = os.Stderr
	}
	if opts.Stdin == nil {
		opts.Stdin = os.Stdin
	}
	if opts.ProxyBin == "" {
		if value := strings.TrimSpace(os.Getenv("CLAUDEX_NEXT_PROXY_BINARY")); value != "" {
			opts.ProxyBin = value
		} else if executable, errExecutable := os.Executable(); errExecutable == nil {
			opts.ProxyBin = filepath.Join(filepath.Dir(executable), "cli-proxy-api-next")
		}
	}
}

func freePort() (int, error) {
	listener, errListen := net.Listen("tcp", "127.0.0.1:0")
	if errListen != nil {
		return 0, fmt.Errorf("reserve proxy port: %w", errListen)
	}
	defer func() { _ = listener.Close() }()
	return listener.Addr().(*net.TCPAddr).Port, nil
}

func waitForProxy(ctx context.Context, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 300 * time.Millisecond}
	url := "http://127.0.0.1:" + strconv.Itoa(port) + "/healthz"
	for time.Now().Before(deadline) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if response, errDo := client.Do(req); errDo == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
	return fmt.Errorf("timeout after %s", timeout)
}

func stopProcess(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return true
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
		return false
	}
}

func cloneEnv(values map[string]string) map[string]string {
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func exitStatus(err error) int {
	if err == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 1
}

func fileSHA256(path string) (string, error) {
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return "", errOpen
	}
	defer func() { _ = file.Close() }()
	hash := sha256.New()
	if _, errCopy := io.Copy(hash, file); errCopy != nil {
		return "", errCopy
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func commandVersion(binary string) string {
	output, _ := exec.Command(binary, "--version").CombinedOutput()
	return strings.TrimSpace(string(output))
}

func flagValue(args []string, name string) string {
	for index, arg := range args {
		if arg == name && index+1 < len(args) {
			return args[index+1]
		}
		if value, ok := strings.CutPrefix(arg, name+"="); ok {
			return value
		}
	}
	return ""
}

func redactArgs(args []string) []string {
	out := append([]string(nil), args...)
	for index, arg := range out {
		if (arg == "--settings" || arg == "--mcp-config") && index+1 < len(out) {
			out[index+1] = "<redacted>"
		}
	}
	return out
}

func writeJSON(path string, value any) error {
	payload, errMarshal := json.MarshalIndent(value, "", "  ")
	if errMarshal != nil {
		return errMarshal
	}
	payload = append(payload, '\n')
	if errWrite := os.WriteFile(path, payload, 0o600); errWrite != nil {
		return fmt.Errorf("write %s: %w", path, errWrite)
	}
	return nil
}

func copySessionTranscripts(home, sessionID, destination string) {
	paths, _ := filepath.Glob(filepath.Join(home, ".claude", "projects", "*", sessionID+".jsonl"))
	for _, root := range paths {
		_ = copyFile(root, filepath.Join(destination, filepath.Base(root)))
		subagents := strings.TrimSuffix(root, ".jsonl")
		_ = filepath.WalkDir(subagents, func(path string, entry os.DirEntry, err error) error {
			if err != nil || entry.IsDir() {
				return nil
			}
			relative, _ := filepath.Rel(subagents, path)
			target := filepath.Join(destination, sessionID, relative)
			_ = os.MkdirAll(filepath.Dir(target), 0o700)
			return copyFile(path, target)
		})
	}
}

func sessionIDFromRequestLogs(logsDir string) string {
	paths, _ := filepath.Glob(filepath.Join(logsDir, "v1-messages-*.log"))
	sort.Strings(paths)
	for _, path := range paths {
		file, errOpen := os.Open(path)
		if errOpen != nil {
			continue
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if value, ok := strings.CutPrefix(strings.ToLower(line), strings.ToLower("X-Claude-Code-Session-Id: ")); ok {
				_ = file.Close()
				return strings.TrimSpace(value)
			}
		}
		_ = file.Close()
	}
	return ""
}

func copyFile(source, destination string) error {
	input, errRead := os.ReadFile(source)
	if errRead != nil {
		return errRead
	}
	return os.WriteFile(destination, input, 0o600)
}

func updateLatest(runsDir, runDir string) {
	latest := filepath.Join(filepath.Dir(runsDir), "latest")
	_ = os.Remove(latest)
	_ = os.Symlink(runDir, latest)
}

func writeChecksums(runDir string) error {
	var paths []string
	errWalk := filepath.WalkDir(runDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || path == filepath.Join(runDir, "checksums.sha256") {
			return nil
		}
		paths = append(paths, path)
		return nil
	})
	if errWalk != nil {
		return fmt.Errorf("enumerate run evidence: %w", errWalk)
	}
	sort.Strings(paths)
	var output strings.Builder
	for _, path := range paths {
		hash, errHash := fileSHA256(path)
		if errHash != nil {
			return fmt.Errorf("checksum %s: %w", path, errHash)
		}
		relative, _ := filepath.Rel(runDir, path)
		fmt.Fprintf(&output, "%s  %s\n", hash, relative)
	}
	if errWrite := os.WriteFile(filepath.Join(runDir, "checksums.sha256"), []byte(output.String()), 0o600); errWrite != nil {
		return fmt.Errorf("write evidence checksums: %w", errWrite)
	}
	return nil
}

func RenderMarkdown(manifest Manifest, summary RunSummary) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "# claudex-next run %s\n\n", manifest.RunID)
	fmt.Fprintf(&builder, "- Exit code: %d\n- Root model: `%s`\n- Configured subagent model: `%s`\n- Requests: %d root / %d child\n- Speculative hit rate: %.1f%%\n- Cache-read ratio: %.1f%%\n\n", manifest.ExitCode, manifest.RootModel, manifest.SubagentModel, summary.RootRequests, summary.ChildRequests, 100*summary.SpeculativeHitRate, 100*summary.CacheReadRatio)
	if summary.Claude.ResultType != "" {
		fmt.Fprintf(&builder, "- Claude: `%s` / `%s`, TTFT %d ms, API %d ms, cost $%.6f\n\n", summary.Claude.ResultType, summary.Claude.TerminalReason, summary.Claude.TTFTMS, summary.Claude.DurationAPIMS, summary.Claude.CostUSD)
	}
	if len(summary.TranscriptModels) > 0 {
		fmt.Fprintf(&builder, "- Models verified in transcripts: `%s`\n- Agent transcripts: %d\n\n", strings.Join(summary.TranscriptModels, "`, `"), summary.AgentTranscripts)
	}
	builder.WriteString("| role | model | connection | ready ms | first event ms | first text ms | total ms | cache read/input | finish |\n|---|---|---|---:|---:|---:|---:|---:|---|\n")
	for _, request := range summary.Requests {
		fmt.Fprintf(&builder, "| %s | %s | %s | %.3f | %.3f | %.3f | %.3f | %d/%d | %s |\n", request.Role, request.Model, request.ConnectionSource, request.ConnectionReadyMS, request.FirstEventMS, request.FirstTextMS, request.TotalMS, request.CacheReadTokens, request.InputTokens, request.FinishReason)
	}
	return builder.String()
}
