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
	RunID          string    `json:"run_id"`
	SessionID      string    `json:"session_id"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	WorkingDir     string    `json:"working_directory"`
	ClaudeVersion  string    `json:"claude_version"`
	ClaudeArgs     []string  `json:"claude_args"`
	RootModel      string    `json:"root_model"`
	SubagentModel  string    `json:"subagent_model"`
	MaxConcurrency string    `json:"max_tool_use_concurrency"`
	ProxyBinary    string    `json:"proxy_binary"`
	ProxySHA256    string    `json:"proxy_sha256"`
	ProxyPort      int       `json:"proxy_port"`
	ExitCode       int       `json:"exit_code"`
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
	values["ANTHROPIC_BASE_URL"] = "http://127.0.0.1:" + strconv.Itoa(port)
	values["CLAUDEX_NEXT_RUN_ID"] = runID

	proxyLog, errProxyLog := os.OpenFile(filepath.Join(runDir, "proxy", "process.log"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if errProxyLog != nil {
		return runDir, 1, fmt.Errorf("create proxy process log: %w", errProxyLog)
	}
	defer func() { _ = proxyLog.Close() }()
	proxyCmd := exec.CommandContext(ctx, opts.ProxyBin, "--config", runtimeConfig, "--local-model")
	proxyCmd.Dir = filepath.Join(runDir, "proxy")
	proxyEnv := envMap(os.Environ())
	proxyEnv["WRITABLE_PATH"] = filepath.Join(runDir, "proxy")
	proxyCmd.Env = flattenEnv(proxyEnv)
	proxyCmd.Stdout = proxyLog
	proxyCmd.Stderr = proxyLog
	if errStart := proxyCmd.Start(); errStart != nil {
		return runDir, 1, fmt.Errorf("start proxy %s: %w", opts.ProxyBin, errStart)
	}
	defer stopProcess(proxyCmd)
	if errReady := waitForProxy(ctx, port, 15*time.Second); errReady != nil {
		return runDir, 1, fmt.Errorf("proxy did not become ready (see %s): %w", proxyLog.Name(), errReady)
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
	errRun := claudeCmd.Run()
	exitCode := exitStatus(errRun)
	stopProcess(proxyCmd)
	_ = proxyLog.Sync()
	if observedSessionID := sessionIDFromRequestLogs(filepath.Join(runDir, "proxy", "logs")); observedSessionID != "" {
		sessionID = observedSessionID
	}
	copySessionTranscripts(home, sessionID, filepath.Join(runDir, "transcripts"))

	proxyHash, _ := fileSHA256(opts.ProxyBin)
	manifest := Manifest{
		RunID: runID, SessionID: sessionID, StartedAt: startedAt, FinishedAt: time.Now(), WorkingDir: workingDir,
		ClaudeVersion: commandVersion(opts.ClaudeBin), ClaudeArgs: redactArgs(claudeArgs), RootModel: flagValue(claudeArgs, "--model"),
		SubagentModel: values["CLAUDE_CODE_SUBAGENT_MODEL"], MaxConcurrency: values["CLAUDE_CODE_MAX_TOOL_USE_CONCURRENCY"],
		ProxyBinary: opts.ProxyBin, ProxySHA256: proxyHash, ProxyPort: port, ExitCode: exitCode,
	}
	if errWrite := writeJSON(filepath.Join(runDir, "manifest.json"), manifest); errWrite != nil {
		return runDir, exitCode, errWrite
	}
	summary, errAnalyze := AnalyzeRun(runDir)
	if errAnalyze != nil {
		return runDir, exitCode, errAnalyze
	}
	if errWrite := writeJSON(filepath.Join(runDir, "summary.json"), summary); errWrite != nil {
		return runDir, exitCode, errWrite
	}
	if errWrite := os.WriteFile(filepath.Join(runDir, "summary.md"), []byte(RenderMarkdown(manifest, summary)), 0o600); errWrite != nil {
		return runDir, exitCode, fmt.Errorf("write summary: %w", errWrite)
	}
	updateLatest(opts.RunsDir, runDir)
	if opts.Stderr != nil {
		_, _ = fmt.Fprintf(opts.Stderr, "\n[claudex-next] summary: %s\n", filepath.Join(runDir, "summary.md"))
	}
	return runDir, exitCode, errRun
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

func stopProcess(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
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
