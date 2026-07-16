package claudexnext

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	out, errMarshal := yaml.Marshal(values)
	if errMarshal != nil {
		return nil, fmt.Errorf("encode proxy config: %w", errMarshal)
	}
	return out, nil
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
	out := make([]string, 0, len(args)+4)
	if !hasAnyFlag(args, "--session-id", "--resume", "-r", "--continue", "-c") {
		out = append(out, "--session-id", sessionID)
	}
	if !hasAnyFlag(args, "--debug-file") {
		out = append(out, "--debug-file", debugPath)
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
