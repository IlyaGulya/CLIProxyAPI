package claudexnext

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// PrepareConfig creates an isolated, observable proxy configuration for one run.
func PrepareConfig(input []byte, port int, authDir ...string) ([]byte, error) {
	var values map[string]any
	if errUnmarshal := yaml.Unmarshal(input, &values); errUnmarshal != nil {
		return nil, fmt.Errorf("parse proxy config: %w", errUnmarshal)
	}
	if values == nil {
		values = make(map[string]any)
	}
	values["host"] = "127.0.0.1"
	values["port"] = port
	if len(authDir) > 0 && strings.TrimSpace(authDir[0]) != "" {
		values["auth-dir"] = authDir[0]
	}
	values["debug"] = true
	values["logging-to-file"] = true
	values["request-log"] = true
	values["codex-prefer-upstream-websockets"] = true
	values["codex-websocket-speculative-preconnect"] = true
	values["codex-websocket-generate-false-warmup"] = false
	values["codex-websocket-preconnect-replenish"] = true
	values["codex-websocket-preconnect-max-idle"] = 2
	values["codex-websocket-preconnect-ttl-seconds"] = 30
	if _, configured := values["claude-code-auto-mode-classifier-model"]; !configured {
		values["claude-code-auto-mode-classifier-model"] = "gpt-5.6-luna"
	}
	ensureWorkflowModelAliases(values)
	out, errMarshal := yaml.Marshal(values)
	if errMarshal != nil {
		return nil, fmt.Errorf("encode proxy config: %w", errMarshal)
	}
	return out, nil
}

func ensureWorkflowModelAliases(values map[string]any) {
	channels, ok := values["oauth-model-alias"].(map[string]any)
	if !ok {
		channels = make(map[string]any)
		values["oauth-model-alias"] = channels
	}
	entries, _ := channels["codex"].([]any)
	configured := make(map[string]bool, len(entries))
	for _, raw := range entries {
		entry, okEntry := raw.(map[string]any)
		if !okEntry {
			continue
		}
		alias, _ := entry["alias"].(string)
		configured[strings.ToLower(strings.TrimSpace(alias))] = true
	}
	for alias, model := range map[string]string{
		"sol":   "gpt-5.6-sol",
		"luna":  "gpt-5.6-luna",
		"terra": "gpt-5.6-terra",
	} {
		if !configured[alias] {
			entries = append(entries, map[string]any{"name": model, "alias": alias, "fork": true})
		}
	}
	channels["codex"] = entries
}

// ConfigAuthDir returns the configured auth directory or the standard default.
func ConfigAuthDir(input []byte) (string, error) {
	var values map[string]any
	if errUnmarshal := yaml.Unmarshal(input, &values); errUnmarshal != nil {
		return "", fmt.Errorf("parse proxy config: %w", errUnmarshal)
	}
	if value, ok := values["auth-dir"].(string); ok && strings.TrimSpace(value) != "" {
		return value, nil
	}
	return "~/.cli-proxy-api", nil
}

// PrepareAuthDir creates a private credential snapshot and enables Codex websocket capability in that snapshot only.
func PrepareAuthDir(source, destination, home string) error {
	if source == "~" {
		source = home
	} else if strings.HasPrefix(source, "~/") {
		source = filepath.Join(home, strings.TrimPrefix(source, "~/"))
	}
	entries, errRead := os.ReadDir(source)
	if errRead != nil {
		return fmt.Errorf("read auth directory: %w", errRead)
	}
	if errMkdir := os.MkdirAll(destination, 0o700); errMkdir != nil {
		return fmt.Errorf("create private auth directory: %w", errMkdir)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		payload, errFile := os.ReadFile(filepath.Join(source, entry.Name()))
		if errFile != nil {
			return fmt.Errorf("read auth file %s: %w", entry.Name(), errFile)
		}
		var record map[string]any
		if json.Unmarshal(payload, &record) == nil && strings.EqualFold(configString(record["type"]), "codex") {
			record["websockets"] = true
			payload, errFile = json.Marshal(record)
			if errFile != nil {
				return fmt.Errorf("encode auth file %s: %w", entry.Name(), errFile)
			}
			payload = append(payload, '\n')
		}
		if errWrite := os.WriteFile(filepath.Join(destination, entry.Name()), payload, 0o600); errWrite != nil {
			return fmt.Errorf("write auth file %s: %w", entry.Name(), errWrite)
		}
	}
	return nil
}

func configString(value any) string {
	text, _ := value.(string)
	return text
}

// BuildClaudeArgs adds observable defaults while respecting explicit user flags.
func BuildClaudeArgs(args []string, sessionID, debugPath string) []string {
	out := make([]string, 0, len(args)+8)
	if !hasFlag(args, "--model") {
		out = append(out, "--model", "gpt-5.6-sol")
	}
	if !hasAnyFlag(args, "--session-id", "--resume", "-r", "--continue", "-c") {
		out = append(out, "--session-id", sessionID)
	}
	if !hasAnyFlag(args, "--debug-file") {
		out = append(out, "--debug-file", debugPath)
	}
	if !hasAnyFlag(args, "--settings") {
		// Keep the large bundled API reference manually invocable while
		// preventing broad model-triggered loads into the active context.
		out = append(out, "--settings", `{"skillOverrides":{"claude-api":"user-invocable-only"}}`)
	}
	return append(out, args...)
}

func hasAnyFlag(args []string, names ...string) bool {
	for _, name := range names {
		if hasFlag(args, name) {
			return true
		}
	}
	return false
}

func hasFlag(args []string, name string) bool {
	for _, arg := range args {
		if arg == name || strings.HasPrefix(arg, name+"=") {
			return true
		}
	}
	return false
}

// LoadEnvFile reads simple shell-style export assignments without executing code.
func LoadEnvFile(path string, base []string) ([]string, error) {
	values := envMap(base)
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return nil, fmt.Errorf("open environment file: %w", errOpen)
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		key, value, ok := strings.Cut(line, "=")
		key = strings.TrimSpace(key)
		if !ok || key == "" {
			continue
		}
		if _, exists := values[key]; exists {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
			value = value[1 : len(value)-1]
		}
		values[key] = value
	}
	if errScan := scanner.Err(); errScan != nil {
		return nil, fmt.Errorf("read environment file: %w", errScan)
	}
	return flattenEnv(values), nil
}

func envMap(env []string) map[string]string {
	values := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	return values
}

func flattenEnv(values map[string]string) []string {
	var buffer bytes.Buffer
	out := make([]string, 0, len(values))
	for key, value := range values {
		buffer.Reset()
		buffer.WriteString(key)
		buffer.WriteByte('=')
		buffer.WriteString(value)
		out = append(out, buffer.String())
	}
	return out
}

// ConfigureClaudeContextSafety reserves enough headroom for a large tool result
// to arrive before Claude Code has a chance to compact. The absolute default is
// derived from Codex's 272k advertised window, 95% effective window, and 90%
// auto-compaction threshold. Claude Code's percentage override is deliberately
// more conservative because tool results can be substantially larger than the
// preceding turn. Explicit user settings always win.
func ConfigureClaudeContextSafety(values map[string]string) {
	defaults := map[string]string{
		"CLAUDE_CODE_AUTO_COMPACT_WINDOW": "232560",
		"CLAUDE_AUTOCOMPACT_PCT_OVERRIDE": "85",
	}
	for key, value := range defaults {
		if _, configured := values[key]; !configured {
			values[key] = value
		}
	}
}

// ConfigureClaudeOTEL enables Claude Code's native metrics, events, and beta
// traces while preserving every explicit user setting. Sensitive payload gates
// and high-cardinality metric labels remain disabled unless the user opts in.
func ConfigureClaudeOTEL(values map[string]string, endpoint, runID string) {
	defaults := map[string]string{
		"CLAUDE_CODE_ENABLE_TELEMETRY":             "1",
		"CLAUDE_CODE_ENHANCED_TELEMETRY_BETA":      "1",
		"CLAUDE_CODE_PROPAGATE_TRACEPARENT":        "1",
		"OTEL_METRICS_EXPORTER":                    "otlp",
		"OTEL_LOGS_EXPORTER":                       "otlp",
		"OTEL_TRACES_EXPORTER":                     "otlp",
		"OTEL_EXPORTER_OTLP_PROTOCOL":              "http/protobuf",
		"OTEL_EXPORTER_OTLP_ENDPOINT":              endpoint,
		"OTEL_METRIC_EXPORT_INTERVAL":              "1000",
		"OTEL_LOGS_EXPORT_INTERVAL":                "1000",
		"OTEL_TRACES_EXPORT_INTERVAL":              "1000",
		"OTEL_METRICS_INCLUDE_SESSION_ID":          "false",
		"OTEL_METRICS_INCLUDE_ACCOUNT_UUID":        "false",
		"OTEL_METRICS_INCLUDE_RESOURCE_ATTRIBUTES": "false",
		"OTEL_LOG_USER_PROMPTS":                    "0",
		"OTEL_LOG_ASSISTANT_RESPONSES":             "0",
		"OTEL_LOG_TOOL_DETAILS":                    "0",
		"OTEL_LOG_TOOL_CONTENT":                    "0",
		"OTEL_LOG_RAW_API_BODIES":                  "0",
	}
	for key, value := range defaults {
		if _, exists := values[key]; !exists {
			values[key] = value
		}
	}
	attributes := splitResourceAttributes(values["OTEL_RESOURCE_ATTRIBUTES"])
	if _, exists := attributes["service.name"]; !exists {
		attributes["service.name"] = "claude-code"
	}
	if _, exists := attributes["claudex.run_id"]; !exists && strings.TrimSpace(runID) != "" {
		attributes["claudex.run_id"] = strings.TrimSpace(runID)
	}
	values["OTEL_RESOURCE_ATTRIBUTES"] = joinResourceAttributes(attributes)
}

func ConfigureProxyOTEL(values map[string]string, endpoint, runID string) {
	defaults := map[string]string{
		"OTEL_TRACES_EXPORTER":        "otlp",
		"OTEL_METRICS_EXPORTER":       "otlp",
		"OTEL_LOGS_EXPORTER":          "otlp",
		"OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
		"OTEL_EXPORTER_OTLP_ENDPOINT": endpoint,
		"CLAUDEX_NEXT_RUN_ID":         runID,
	}
	for key, value := range defaults {
		if _, exists := values[key]; !exists {
			values[key] = value
		}
	}
	attributes := splitResourceAttributes(values["OTEL_RESOURCE_ATTRIBUTES"])
	attributes["service.name"] = "cli-proxy-api"
	attributes["claudex.run_id"] = runID
	values["OTEL_RESOURCE_ATTRIBUTES"] = joinResourceAttributes(attributes)
}

func splitResourceAttributes(value string) map[string]string {
	attributes := make(map[string]string)
	for _, entry := range strings.Split(value, ",") {
		key, item, ok := strings.Cut(strings.TrimSpace(entry), "=")
		if ok && strings.TrimSpace(key) != "" {
			attributes[strings.TrimSpace(key)] = strings.TrimSpace(item)
		}
	}
	return attributes
}

func joinResourceAttributes(attributes map[string]string) string {
	keys := make([]string, 0, len(attributes))
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	// Keep user/team attributes first and the claudex identity predictable.
	ordered := make([]string, 0, len(keys))
	for _, key := range keys {
		if key != "service.name" && key != "claudex.run_id" {
			ordered = append(ordered, key)
		}
	}
	if _, ok := attributes["service.name"]; ok {
		ordered = append(ordered, "service.name")
	}
	if _, ok := attributes["claudex.run_id"]; ok {
		ordered = append(ordered, "claudex.run_id")
	}
	entries := make([]string, 0, len(ordered))
	for _, key := range ordered {
		entries = append(entries, key+"="+attributes[key])
	}
	return strings.Join(entries, ",")
}
