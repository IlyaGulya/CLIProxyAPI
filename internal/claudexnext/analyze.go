package claudexnext

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type RequestSummary struct {
	File                 string  `json:"file"`
	Model                string  `json:"model"`
	Role                 string  `json:"role"`
	RootCorrelationID    string  `json:"root_correlation_id,omitempty"`
	ExecutionCorrelation string  `json:"execution_correlation_id,omitempty"`
	ConnectionSource     string  `json:"connection_source"`
	ConnectionReadyMS    float64 `json:"connection_ready_ms"`
	FirstEventMS         float64 `json:"first_event_since_send_ms"`
	FirstTextMS          float64 `json:"first_text_since_send_ms"`
	TotalMS              float64 `json:"total_ms"`
	InputTokens          int64   `json:"input_tokens"`
	OutputTokens         int64   `json:"output_tokens"`
	CacheReadTokens      int64   `json:"cache_read_tokens"`
	PromptCacheScope     string  `json:"prompt_cache_scope,omitempty"`
	PromptFingerprint    string  `json:"prompt_prefix_fingerprint,omitempty"`
	ChainSource          string  `json:"chain_source,omitempty"`
	Incremental          bool    `json:"incremental"`
	IncrementalReset     string  `json:"incremental_reset_reason,omitempty"`
	ClientBodyBytes      int64   `json:"client_body_bytes,omitempty"`
	UpstreamBodyBytes    int64   `json:"upstream_body_bytes,omitempty"`
	FinishReason         string  `json:"finish_reason"`
}

type RunSummary struct {
	RequestCount       int              `json:"request_count"`
	RootRequests       int              `json:"root_requests"`
	ChildRequests      int              `json:"child_requests"`
	Models             map[string]int   `json:"models"`
	ConnectionSources  map[string]int   `json:"connection_sources"`
	MetricEvents       map[string]int   `json:"metric_events"`
	TranscriptFiles    int              `json:"transcript_files"`
	AgentTranscripts   int              `json:"agent_transcripts"`
	TranscriptModels   []string         `json:"transcript_models"`
	SpeculativeHitRate float64          `json:"speculative_hit_rate"`
	CacheReadRatio     float64          `json:"cache_read_ratio"`
	Claude             ClaudeSummary    `json:"claude"`
	Requests           []RequestSummary `json:"requests"`
}

type ClaudeSummary struct {
	ResultType      string  `json:"result_type,omitempty"`
	TerminalReason  string  `json:"terminal_reason,omitempty"`
	DurationMS      int64   `json:"duration_ms,omitempty"`
	DurationAPIMS   int64   `json:"duration_api_ms,omitempty"`
	TTFTMS          int64   `json:"ttft_ms,omitempty"`
	TTFTStreamMS    int64   `json:"ttft_stream_ms,omitempty"`
	TimeToRequestMS int64   `json:"time_to_request_ms,omitempty"`
	Turns           int64   `json:"turns,omitempty"`
	CostUSD         float64 `json:"cost_usd,omitempty"`
	ReportedSession string  `json:"reported_session_id,omitempty"`
}

// AnalyzeRun parses privacy-safe metrics from the isolated request logs.
func AnalyzeRun(runDir string) (RunSummary, error) {
	summary := RunSummary{
		Models:            make(map[string]int),
		ConnectionSources: make(map[string]int),
		MetricEvents:      make(map[string]int),
	}
	journalPaths, errJournalGlob := filepath.Glob(filepath.Join(runDir, "proxy", "events", "requests.jsonl*"))
	if errJournalGlob != nil {
		return summary, fmt.Errorf("find event journals: %w", errJournalGlob)
	}
	sort.Slice(journalPaths, func(left, right int) bool {
		leftCurrent := !strings.HasSuffix(journalPaths[left], ".1")
		rightCurrent := !strings.HasSuffix(journalPaths[right], ".1")
		if leftCurrent != rightCurrent {
			return !leftCurrent // Rotated history precedes the current journal.
		}
		return journalPaths[left] < journalPaths[right]
	})
	if len(journalPaths) > 0 {
		requests, events, errJournal := parseEventJournals(journalPaths)
		if errJournal != nil {
			return summary, errJournal
		}
		summary.Requests = requests
		for name, count := range events {
			summary.MetricEvents[name] += count
		}
	} else {
		pattern := filepath.Join(runDir, "proxy", "logs", "v1-messages-*.log")
		paths, errGlob := filepath.Glob(pattern)
		if errGlob != nil {
			return summary, fmt.Errorf("find request logs: %w", errGlob)
		}
		sort.Strings(paths)
		for _, path := range paths {
			request, events, errParse := parseRequestLog(path)
			if errParse != nil {
				return summary, errParse
			}
			summary.Requests = append(summary.Requests, request)
			for _, name := range events {
				summary.MetricEvents[name]++
			}
		}
	}
	var cacheRead, input int64
	var speculativeCandidates, speculativeHits int
	for _, request := range summary.Requests {
		summary.Models[request.Model]++
		summary.ConnectionSources[request.ConnectionSource]++
		if request.Role == "child" {
			summary.ChildRequests++
			speculativeCandidates++
			if request.ConnectionSource == "speculative" {
				speculativeHits++
			}
		} else {
			summary.RootRequests++
		}
		cacheRead += request.CacheReadTokens
		input += request.InputTokens
	}
	summary.RequestCount = len(summary.Requests)
	if speculativeCandidates > 0 {
		summary.SpeculativeHitRate = float64(speculativeHits) / float64(speculativeCandidates)
	}
	if input > 0 {
		summary.CacheReadRatio = float64(cacheRead) / float64(input)
	}
	collectDetachedMetrics(runDir, summary.MetricEvents)
	summary.TranscriptFiles, summary.AgentTranscripts, summary.TranscriptModels = analyzeTranscripts(filepath.Join(runDir, "transcripts"))
	summary.Claude = analyzeClaudeOutput(filepath.Join(runDir, "claude", "stdout.log"))
	return summary, nil
}

func parseEventJournals(paths []string) ([]RequestSummary, map[string]int, error) {
	requests := make([]RequestSummary, 0)
	indices := make(map[string]int)
	events := make(map[string]int)
	for _, path := range paths {
		file, errOpen := os.Open(path)
		if errOpen != nil {
			return nil, nil, fmt.Errorf("open event journal %s: %w", path, errOpen)
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 16*1024), 1<<20)
		for scanner.Scan() {
			var event map[string]any
			if json.Unmarshal(scanner.Bytes(), &event) != nil {
				continue // A crash may leave one truncated final record.
			}
			name := stringValue(event["name"])
			key := stringValue(event["request_key"])
			if name == "" {
				continue
			}
			events[name]++
			index, exists := indices[key]
			if name == "request_prepared" && key != "" {
				indices[key] = len(requests)
				requests = append(requests, RequestSummary{File: filepath.Base(path), Role: "root"})
				index, exists = len(requests)-1, true
			}
			if !exists {
				continue
			}
			applyJournalEvent(&requests[index], name, event)
		}
		errScan := scanner.Err()
		_ = file.Close()
		if errScan != nil {
			return nil, nil, fmt.Errorf("read event journal %s: %w", path, errScan)
		}
	}
	return requests, events, nil
}

func applyJournalEvent(request *RequestSummary, name string, event map[string]any) {
	if role := stringValue(event["role"]); role == "child" || role == "root" {
		request.Role = role
	}
	switch name {
	case "request_prepared":
		request.Model = stringValue(event["model"])
		request.ChainSource = stringValue(event["chain_source"])
		request.Incremental = boolean(event["incremental"])
		request.IncrementalReset = stringValue(event["incremental_reset_reason"])
		request.ClientBodyBytes = int64(number(event["client_body_bytes"]))
		request.UpstreamBodyBytes = int64(number(event["upstream_body_bytes"]))
	case "connection_ready":
		request.ConnectionSource = stringValue(event["connection_source"])
		request.ConnectionReadyMS = number(event["duration_us"]) / 1000
	case "first_upstream_event":
		request.FirstEventMS = number(event["since_send_us"]) / 1000
	case "first_output_text_delta":
		request.FirstTextMS = number(event["since_send_us"]) / 1000
	case "usage":
		request.InputTokens = int64(number(event["input_tokens"]))
		request.OutputTokens = int64(number(event["output_tokens"]))
		request.CacheReadTokens = int64(number(event["cache_read_tokens"]))
	case "request_finished":
		request.TotalMS = number(event["elapsed_us"]) / 1000
		request.FinishReason = stringValue(event["reason"])
	}
}

func boolean(value any) bool {
	result, _ := value.(bool)
	return result
}

func analyzeClaudeOutput(path string) ClaudeSummary {
	var summary ClaudeSummary
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return summary
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var record map[string]any
		if json.Unmarshal([]byte(line), &record) != nil || stringValue(record["type"]) != "result" {
			continue
		}
		summary.ResultType = stringValue(record["subtype"])
		summary.TerminalReason = stringValue(record["terminal_reason"])
		summary.DurationMS = int64(number(record["duration_ms"]))
		summary.DurationAPIMS = int64(number(record["duration_api_ms"]))
		summary.TTFTMS = int64(number(record["ttft_ms"]))
		summary.TTFTStreamMS = int64(number(record["ttft_stream_ms"]))
		summary.TimeToRequestMS = int64(number(record["time_to_request_ms"]))
		summary.Turns = int64(number(record["num_turns"]))
		summary.CostUSD = number(record["total_cost_usd"])
		summary.ReportedSession = stringValue(record["session_id"])
	}
	return summary
}

func stringValue(value any) string {
	text, _ := value.(string)
	return text
}

func collectDetachedMetrics(runDir string, counts map[string]int) {
	path := filepath.Join(runDir, "proxy", "logs", "main.log")
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		marker := "detached websocket metric: "
		index := strings.Index(line, marker)
		if index < 0 {
			continue
		}
		var metric map[string]any
		if json.Unmarshal([]byte(line[index+len(marker):]), &metric) == nil {
			if name, ok := metric["name"].(string); ok {
				counts[name]++
			}
		}
	}
}

func analyzeTranscripts(root string) (int, int, []string) {
	files := 0
	agents := 0
	models := make(map[string]struct{})
	_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil || entry.IsDir() || filepath.Ext(path) != ".jsonl" {
			return nil
		}
		files++
		if strings.HasPrefix(filepath.Base(path), "agent-") {
			agents++
		}
		file, errOpen := os.Open(path)
		if errOpen != nil {
			return nil
		}
		scanner := bufio.NewScanner(file)
		scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
		for scanner.Scan() {
			var value any
			if json.Unmarshal(scanner.Bytes(), &value) == nil {
				collectModels(value, models)
			}
		}
		_ = file.Close()
		return nil
	})
	out := make([]string, 0, len(models))
	for model := range models {
		out = append(out, model)
	}
	sort.Strings(out)
	return files, agents, out
}

func collectModels(value any, models map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			if key == "model" {
				if model, ok := child.(string); ok && strings.HasPrefix(model, "gpt-") {
					models[model] = struct{}{}
				}
			}
			collectModels(child, models)
		}
	case []any:
		for _, child := range typed {
			collectModels(child, models)
		}
	}
}

func parseRequestLog(path string) (RequestSummary, []string, error) {
	request := RequestSummary{File: filepath.Base(path), Role: "root"}
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return request, nil, fmt.Errorf("open request log %s: %w", path, errOpen)
	}
	defer func() { _ = file.Close() }()
	var events []string
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(strings.ToLower(line), "x-claude-code-agent-id:") {
			request.Role = "child"
		}
		if !strings.HasPrefix(line, "{") || !strings.Contains(line, `"event":"api.websocket.metric"`) {
			continue
		}
		var metric map[string]any
		if errJSON := json.Unmarshal([]byte(line), &metric); errJSON != nil {
			continue
		}
		name, _ := metric["name"].(string)
		events = append(events, name)
		if value, ok := metric["claude_root_correlation_id"].(string); ok {
			request.RootCorrelationID = value
		}
		if value, ok := metric["claude_execution_correlation_id"].(string); ok {
			request.ExecutionCorrelation = value
		}
		switch name {
		case "request_prepared":
			request.Model, _ = metric["model"].(string)
			request.PromptCacheScope, _ = metric["prompt_cache_scope"].(string)
			request.PromptFingerprint, _ = metric["prompt_prefix_fingerprint"].(string)
			request.ChainSource, _ = metric["chain_source"].(string)
			request.Incremental, _ = metric["incremental"].(bool)
			request.IncrementalReset, _ = metric["incremental_reset_reason"].(string)
			request.ClientBodyBytes = int64(number(metric["client_body_bytes"]))
			request.UpstreamBodyBytes = int64(number(metric["upstream_body_bytes"]))
		case "connection_ready":
			request.ConnectionSource, _ = metric["connection_source"].(string)
			request.ConnectionReadyMS = number(metric["duration_us"]) / 1000
		case "first_upstream_event":
			request.FirstEventMS = number(metric["since_send_us"]) / 1000
		case "first_output_text_delta":
			request.FirstTextMS = number(metric["since_send_us"]) / 1000
		case "usage":
			request.InputTokens = int64(number(metric["input_tokens"]))
			request.OutputTokens = int64(number(metric["output_tokens"]))
			request.CacheReadTokens = int64(number(metric["cache_read_tokens"]))
		case "request_finished":
			request.TotalMS = number(metric["elapsed_us"]) / 1000
			request.FinishReason, _ = metric["reason"].(string)
		}
	}
	if errScan := scanner.Err(); errScan != nil {
		return request, nil, fmt.Errorf("read request log %s: %w", path, errScan)
	}
	return request, events, nil
}

func number(value any) float64 {
	switch typed := value.(type) {
	case float64:
		return typed
	case int:
		return float64(typed)
	case int64:
		return float64(typed)
	default:
		return 0
	}
}
