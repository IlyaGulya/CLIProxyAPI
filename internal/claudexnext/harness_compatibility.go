package claudexnext

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type ClaudeHarnessCompatibilityCase struct {
	Name              string   `json:"name"`
	Args              []string `json:"args"`
	ExpectedEvents    []string `json:"expected_events"`
	ExpectedArtifacts []string `json:"expected_artifacts"`
	RequiredFields    []string `json:"required_fields,omitempty"`
	TranscriptPolicy  string   `json:"transcript_policy"`
	NeedsSecondTurn   bool     `json:"needs_second_turn,omitempty"`
	NeedsAgent        bool     `json:"needs_agent,omitempty"`
	ExpectedExitCode  int      `json:"expected_exit_code"`
}

type ClaudeHarnessCompatibility struct {
	Version string                           `json:"version"`
	Cases   []ClaudeHarnessCompatibilityCase `json:"cases"`
}

// ClaudeHarnessCompatibilityMatrix describes bounded black-box probes. The
// proxy never interprets these harness flags; the matrix guards pass-through,
// artifact collection, and the output schemas consumed by claudex-next.
func ClaudeHarnessCompatibilityMatrix(version string) ClaudeHarnessCompatibility {
	stream := []string{"--print", "--output-format", "stream-json", "--verbose"}
	caseOf := func(name string, extra []string, events ...string) ClaudeHarnessCompatibilityCase {
		return ClaudeHarnessCompatibilityCase{Name: name, Args: append(append([]string{}, stream...), extra...), ExpectedEvents: events, ExpectedArtifacts: []string{"manifest.json", "claude/stdout.log", "harness-schema.json"}, TranscriptPolicy: "required"}
	}
	cases := []ClaudeHarnessCompatibilityCase{
		caseOf("bare", []string{"--bare"}, "system:init", "assistant", "result:success"),
		caseOf("safe_mode", []string{"--safe-mode"}, "system:init", "assistant", "result:success"),
		caseOf("stream_json", []string{"--input-format", "stream-json", "--replay-user-messages"}, "system:init", "user", "assistant", "result:success"),
		caseOf("partial_messages", []string{"--include-partial-messages"}, "system:init", "stream_event", "assistant", "result:success"),
		caseOf("forward_subagent_text", []string{"--forward-subagent-text"}, "system:init", "assistant", "result:success"),
		caseOf("prompt_suggestions", []string{"--prompt-suggestions", "true"}, "system:init", "assistant", "prompt_suggestion", "result:success"),
		{Name: "structured_output", Args: []string{"--print", "--output-format", "json", "--json-schema", `{"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"]}`}, ExpectedEvents: []string{"result:success"}, ExpectedArtifacts: []string{"manifest.json", "claude/stdout.log", "harness-schema.json"}, TranscriptPolicy: "required"},
		{Name: "background_agent", Args: []string{"--background", "--print", "--output-format", "stream-json", "--verbose"}, ExpectedEvents: []string{"system:init", "result:success"}, ExpectedArtifacts: []string{"manifest.json", "claude/stdout.log", "harness-schema.json"}, TranscriptPolicy: "required", NeedsAgent: true},
		caseOf("workflow", nil, "system:init", "assistant", "result:success"),
		{Name: "fork_session", Args: []string{"--print", "--output-format", "stream-json", "--verbose", "--resume", "SESSION_ID", "--fork-session"}, ExpectedEvents: []string{"system:init", "assistant", "result:success"}, ExpectedArtifacts: []string{"manifest.json", "claude/stdout.log", "harness-schema.json"}, TranscriptPolicy: "required", NeedsSecondTurn: true},
		caseOf("no_session_persistence", []string{"--no-session-persistence"}, "system:init", "assistant", "result:success"),
		{Name: "cancellation", Args: stream, ExpectedEvents: []string{"system:init"}, ExpectedArtifacts: []string{"manifest.json", "claude/stdout.log"}, TranscriptPolicy: "optional", ExpectedExitCode: 130},
	}
	for i := range cases {
		if cases[i].Name == "forward_subagent_text" || cases[i].Name == "workflow" {
			cases[i].NeedsAgent = true
		}
		if cases[i].Name == "no_session_persistence" {
			cases[i].TranscriptPolicy = "forbidden"
		}
		switch cases[i].Name {
		case "stream_json":
			cases[i].RequiredFields = []string{"message.role"}
		case "partial_messages":
			cases[i].RequiredFields = []string{"event.type"}
		case "forward_subagent_text":
			cases[i].RequiredFields = []string{"parent_tool_use_id"}
		case "prompt_suggestions":
			cases[i].RequiredFields = []string{"suggestion"}
		case "structured_output":
			cases[i].RequiredFields = []string{"structured_output.ok"}
		}
	}
	return ClaudeHarnessCompatibility{Version: strings.TrimSpace(version), Cases: cases}
}

type ClaudeHarnessSchemaReport struct {
	Version    string              `json:"version"`
	EventCount int                 `json:"event_count"`
	Events     map[string]int      `json:"events"`
	FieldTypes map[string][]string `json:"field_types"`
}

// SummarizeClaudeHarnessJSONL validates terminal boundaries and records only
// event names plus field paths/types. Values are deliberately never retained.
func SummarizeClaudeHarnessJSONL(reader io.Reader, version string) (ClaudeHarnessSchemaReport, error) {
	report := ClaudeHarnessSchemaReport{Version: strings.TrimSpace(version), Events: make(map[string]int), FieldTypes: make(map[string][]string)}
	typesByPath := make(map[string]map[string]struct{})
	seenInit, seenResult := false, false
	scanner := bufio.NewScanner(reader)
	buffer := make([]byte, 64*1024)
	scanner.Buffer(buffer, 8<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			return report, fmt.Errorf("decode harness event %d: %w", report.EventCount+1, err)
		}
		name, _ := event["type"].(string)
		if subtype, _ := event["subtype"].(string); subtype != "" {
			name += ":" + subtype
		}
		if name == "" {
			return report, fmt.Errorf("harness event %d has no type", report.EventCount+1)
		}
		report.EventCount++
		report.Events[name]++
		seenInit = seenInit || name == "system:init"
		seenResult = seenResult || strings.HasPrefix(name, "result:") || name == "result"
		collectHarnessFieldTypes(event, "", typesByPath)
	}
	if err := scanner.Err(); err != nil {
		return report, fmt.Errorf("read harness stream: %w", err)
	}
	if !seenInit && report.EventCount > 1 {
		return report, fmt.Errorf("harness stream is missing system:init")
	}
	if !seenResult {
		return report, fmt.Errorf("harness stream is missing terminal result")
	}
	for path, values := range typesByPath {
		ordered := make([]string, 0, len(values))
		for value := range values {
			ordered = append(ordered, value)
		}
		sort.Strings(ordered)
		report.FieldTypes[path] = ordered
	}
	return report, nil
}

func CaptureClaudeHarnessSchema(path, format, version string) (ClaudeHarnessSchemaReport, error) {
	payload, err := os.ReadFile(path)
	if err != nil {
		return ClaudeHarnessSchemaReport{}, fmt.Errorf("read Claude harness output: %w", err)
	}
	if format == "json" {
		var value any
		if err := json.Unmarshal(payload, &value); err != nil {
			return ClaudeHarnessSchemaReport{}, fmt.Errorf("decode Claude JSON output: %w", err)
		}
		payload, err = json.Marshal(value)
		if err != nil {
			return ClaudeHarnessSchemaReport{}, err
		}
		payload = append(payload, '\n')
	}
	return SummarizeClaudeHarnessJSONL(bytes.NewReader(payload), version)
}

func VerifyClaudeHarnessCase(runDir string, testCase ClaudeHarnessCompatibilityCase) error {
	var failures []string
	for _, relative := range testCase.ExpectedArtifacts {
		if _, err := os.Stat(filepath.Join(runDir, filepath.FromSlash(relative))); err != nil {
			failures = append(failures, "missing artifact "+relative)
		}
	}
	transcriptCount := 0
	_ = filepath.WalkDir(filepath.Join(runDir, "transcripts"), func(path string, entry fs.DirEntry, err error) error {
		if err == nil && !entry.IsDir() && strings.HasSuffix(entry.Name(), ".jsonl") {
			transcriptCount++
		}
		return nil
	})
	switch testCase.TranscriptPolicy {
	case "required":
		if transcriptCount == 0 {
			failures = append(failures, "durable transcript missing")
		}
	case "forbidden":
		if transcriptCount != 0 {
			failures = append(failures, "transcript persisted despite no-session-persistence")
		}
	}
	if containsString(testCase.ExpectedArtifacts, "harness-schema.json") {
		var report ClaudeHarnessSchemaReport
		payload, err := os.ReadFile(filepath.Join(runDir, "harness-schema.json"))
		if err == nil {
			err = json.Unmarshal(payload, &report)
		}
		if err != nil {
			failures = append(failures, "invalid harness schema")
		} else {
			for _, event := range testCase.ExpectedEvents {
				if report.Events[event] == 0 {
					failures = append(failures, "missing harness event "+event)
				}
			}
			for _, field := range testCase.RequiredFields {
				if len(report.FieldTypes[field]) == 0 {
					failures = append(failures, "missing harness field "+field)
				}
			}
			if testCase.Name == "forward_subagent_text" && !containsString(report.FieldTypes["parent_tool_use_id"], "string") {
				failures = append(failures, "forwarded subagent event lacks non-null parent_tool_use_id")
			}
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("compatibility case %s failed: %s", testCase.Name, strings.Join(failures, "; "))
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func collectHarnessFieldTypes(value any, path string, out map[string]map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			next := key
			if path != "" {
				next = path + "." + key
			}
			collectHarnessFieldTypes(child, next, out)
		}
	case []any:
		next := path + "[]"
		for _, child := range typed {
			collectHarnessFieldTypes(child, next, out)
		}
	default:
		kind := "null"
		switch typed.(type) {
		case string:
			kind = "string"
		case bool:
			kind = "boolean"
		case float64:
			kind = "number"
		}
		if out[path] == nil {
			out[path] = make(map[string]struct{})
		}
		out[path][kind] = struct{}{}
	}
}
