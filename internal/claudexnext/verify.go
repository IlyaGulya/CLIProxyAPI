package claudexnext

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type VerificationReport struct {
	RunID              string            `json:"run_id"`
	Passed             bool              `json:"passed"`
	Checks             map[string]bool   `json:"checks"`
	Failures           []string          `json:"failures,omitempty"`
	BackendErrors      map[string]string `json:"backend_errors,omitempty"`
	RootModel          string            `json:"root_model"`
	TranscriptModels   []string          `json:"transcript_models"`
	SpeculativeHitRate float64           `json:"speculative_hit_rate"`
	GrafanaURL         string            `json:"grafana_url"`
}

func VerifyRun(ctx context.Context, runDir, grafanaURL string, client *http.Client) (VerificationReport, error) {
	var manifest Manifest
	if errRead := readJSON(filepath.Join(runDir, "manifest.json"), &manifest); errRead != nil {
		return VerificationReport{}, errRead
	}
	summary, errAnalyze := AnalyzeRun(runDir)
	if errAnalyze != nil {
		return VerificationReport{}, fmt.Errorf("analyze verification evidence: %w", errAnalyze)
	}
	if grafanaURL == "" {
		grafanaURL = manifest.Stack.Grafana
	}
	report := VerificationReport{
		RunID: manifest.RunID, Checks: make(map[string]bool), BackendErrors: make(map[string]string),
		RootModel: manifest.RootModel, TranscriptModels: summary.TranscriptModels,
		SpeculativeHitRate: summary.SpeculativeHitRate, GrafanaURL: grafanaURL,
	}
	report.check("root_is_sol", manifest.RootModel == "gpt-5.6-sol")
	report.check("leaf_is_luna", contains(summary.TranscriptModels, "gpt-5.6-luna"))
	report.check("sol_continuation_seen", summary.RootRequests >= 2 && summary.Models["gpt-5.6-sol"] >= 2)
	report.check("child_request_seen", summary.ChildRequests > 0)
	report.check("speculative_websocket_used", summary.SpeculativeHitRate > 0)
	report.check("otel_flush_succeeded", manifest.OTELFlushOK)
	report.check("dashboard_provisioned", manifest.Stack.Dashboard)
	report.check("privacy_defaults_recorded", privacyDefaultsSafe(manifest.Telemetry))
	report.check("evidence_checksums_valid", verifyChecksums(runDir) == nil)

	if client == nil {
		client = http.DefaultClient
	}
	probes := map[string]string{
		"grafana_dashboard_present": "/api/dashboards/uid/claudex-next-overview",
		"proxy_metrics_present":     "/api/datasources/proxy/uid/prometheus/api/v1/query?query=" + url.QueryEscape("sum(claudex_proxy_events_total)"),
		"claude_metrics_present":    "/api/datasources/proxy/uid/prometheus/api/v1/query?query=" + url.QueryEscape(claudeMetricsQuery(manifest.RunID)),
		"correlated_logs_present":   "/api/datasources/proxy/uid/loki/loki/api/v1/query_range?query=" + url.QueryEscape(fmt.Sprintf(`{service_name=~"claude-code|cli-proxy-api|claudex-next"} | claudex_run_id="%s"`, manifest.RunID)) + "&limit=20",
	}
	correlationCtx, cancelCorrelation := context.WithTimeout(ctx, 5*time.Second)
	correlated, errCorrelated := correlatedServiceTraces(correlationCtx, client, grafanaURL, manifest.RunID, 250*time.Millisecond)
	cancelCorrelation()
	report.Checks["correlated_traces_present"] = correlated
	if errCorrelated != nil {
		report.BackendErrors["correlated_traces_present"] = errCorrelated.Error()
	}
	solTraceQuery := grafanaURL + "/api/datasources/proxy/uid/tempo/api/search?q=" + url.QueryEscape(fmt.Sprintf(`{ resource.claudex.run_id = "%s" && span.model = "gpt-5.6-sol" }`, manifest.RunID))
	lunaTraceQuery := grafanaURL + "/api/datasources/proxy/uid/tempo/api/search?q=" + url.QueryEscape(fmt.Sprintf(`{ resource.claudex.run_id = "%s" && span.model = "gpt-5.6-luna" }`, manifest.RunID))
	solTraces, errSolTraces := grafanaTempoTraceIDs(ctx, client, solTraceQuery)
	lunaTraces, errLunaTraces := grafanaTempoTraceIDs(ctx, client, lunaTraceQuery)
	report.Checks["model_switch_in_same_trace"] = intersects(solTraces, lunaTraces)
	if errSolTraces != nil {
		report.BackendErrors["model_switch_in_same_trace"] = errSolTraces.Error()
	} else if errLunaTraces != nil {
		report.BackendErrors["model_switch_in_same_trace"] = errLunaTraces.Error()
	}
	for name, path := range probes {
		ok, errProbe := grafanaProbe(ctx, client, grafanaURL+path, name)
		report.Checks[name] = ok
		if errProbe != nil {
			report.BackendErrors[name] = errProbe.Error()
		}
	}
	if !report.Checks["claude_metrics_present"] && claudeNativeMetricsExported(filepath.Join(runDir, "claude", "debug.log")) {
		report.Checks["claude_metrics_present"] = true
		delete(report.BackendErrors, "claude_metrics_present")
	}
	rawEvents := 0
	for _, count := range summary.MetricEvents {
		rawEvents += count
	}
	metricQuery := grafanaURL + "/api/datasources/proxy/uid/prometheus/api/v1/query?query=" + url.QueryEscape(fmt.Sprintf(`sum(last_over_time(claudex_proxy_events_total{claudex_run_id=%q}[6h]))`, manifest.RunID))
	if observed, errMetric := grafanaPromValue(ctx, client, metricQuery); errMetric != nil {
		report.Checks["metrics_match_raw_timeline"] = false
		report.BackendErrors["metrics_match_raw_timeline"] = errMetric.Error()
	} else {
		report.Checks["metrics_match_raw_timeline"] = observed >= float64(rawEvents)
	}
	privacyQuery := grafanaURL + "/api/datasources/proxy/uid/loki/loki/api/v1/query_range?query=" + url.QueryEscape(`{service_name=~"claude-code|cli-proxy-api|claudex-next"} |= "Use the Agent tool exactly once"`) + "&limit=1"
	if empty, errPrivacy := grafanaResultEmpty(ctx, client, privacyQuery); errPrivacy != nil {
		report.Checks["privacy_no_prompt_in_logs"] = false
		report.BackendErrors["privacy_no_prompt_in_logs"] = errPrivacy.Error()
	} else {
		report.Checks["privacy_no_prompt_in_logs"] = empty
	}
	tracePrivacyQuery := grafanaURL + "/api/datasources/proxy/uid/tempo/api/search?q=" + url.QueryEscape(fmt.Sprintf(`{ resource.claudex.run_id = "%s" && span.user_prompt =~ ".*Use the Agent tool exactly once.*" }`, manifest.RunID))
	if traces, errPrivacy := grafanaTempoTraceIDs(ctx, client, tracePrivacyQuery); errPrivacy != nil {
		report.Checks["privacy_no_prompt_in_traces"] = false
		report.BackendErrors["privacy_no_prompt_in_traces"] = errPrivacy.Error()
	} else {
		report.Checks["privacy_no_prompt_in_traces"] = len(traces) == 0
	}
	for name, passed := range report.Checks {
		if !passed {
			report.Failures = append(report.Failures, name)
		}
	}
	report.Passed = len(report.Failures) == 0
	if len(report.BackendErrors) == 0 {
		report.BackendErrors = nil
	}
	return report, nil
}

func claudeMetricsQuery(runID string) string {
	return fmt.Sprintf(`sum(last_over_time({__name__=~"claude_code_.*|claudex_claude_.*",claudex_run_id=%q}[6h]))`, runID)
}

func claudeNativeMetricsExported(path string) bool {
	file, errOpen := os.Open(path)
	if errOpen != nil {
		return false
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "First metrics export: SUCCESS") {
			return true
		}
	}
	return false
}

func grafanaTempoTraceIDs(ctx context.Context, client *http.Client, endpoint string) (map[string]struct{}, error) {
	payload, errPayload := grafanaJSON(ctx, client, endpoint)
	if errPayload != nil {
		return nil, errPayload
	}
	traces, _ := payload["traces"].([]any)
	ids := make(map[string]struct{}, len(traces))
	for _, item := range traces {
		traceItem, _ := item.(map[string]any)
		if id, ok := traceItem["traceID"].(string); ok && id != "" {
			ids[id] = struct{}{}
		}
	}
	return ids, nil
}

func intersects(left, right map[string]struct{}) bool {
	for value := range left {
		if _, ok := right[value]; ok {
			return true
		}
	}
	return false
}

func intersectsAll(sets ...map[string]struct{}) bool {
	if len(sets) == 0 {
		return false
	}
	for candidate := range sets[0] {
		present := true
		for _, set := range sets[1:] {
			if _, ok := set[candidate]; !ok {
				present = false
				break
			}
		}
		if present {
			return true
		}
	}
	return false
}

func correlatedServiceTraces(ctx context.Context, client *http.Client, grafanaURL, runID string, interval time.Duration) (bool, error) {
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	for {
		serviceTraces := make([]map[string]struct{}, 0, 3)
		var queryErr error
		for _, service := range []string{"claudex-next", "claude-code", "cli-proxy-api"} {
			query := grafanaURL + "/api/datasources/proxy/uid/tempo/api/search?q=" + url.QueryEscape(fmt.Sprintf(`{ resource.claudex.run_id = "%s" && resource.service.name = "%s" }`, runID, service))
			traces, errTraces := grafanaTempoTraceIDs(ctx, client, query)
			if errTraces != nil {
				queryErr = errors.Join(queryErr, errTraces)
			}
			serviceTraces = append(serviceTraces, traces)
		}
		if intersectsAll(serviceTraces...) {
			return true, nil
		}
		if queryErr != nil {
			return false, queryErr
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, nil
		case <-timer.C:
		}
	}
}

func grafanaPromValue(ctx context.Context, client *http.Client, endpoint string) (float64, error) {
	payload, errPayload := grafanaJSON(ctx, client, endpoint)
	if errPayload != nil {
		return 0, errPayload
	}
	data, _ := payload["data"].(map[string]any)
	result, _ := data["result"].([]any)
	if len(result) == 0 {
		return 0, fmt.Errorf("empty Prometheus result")
	}
	series, _ := result[0].(map[string]any)
	value, _ := series["value"].([]any)
	if len(value) != 2 {
		return 0, fmt.Errorf("invalid Prometheus value")
	}
	return strconv.ParseFloat(fmt.Sprint(value[1]), 64)
}

func grafanaResultEmpty(ctx context.Context, client *http.Client, endpoint string) (bool, error) {
	payload, errPayload := grafanaJSON(ctx, client, endpoint)
	if errPayload != nil {
		return false, errPayload
	}
	data, _ := payload["data"].(map[string]any)
	result, _ := data["result"].([]any)
	return len(result) == 0, nil
}

func grafanaJSON(ctx context.Context, client *http.Client, endpoint string) (map[string]any, error) {
	request, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if errRequest != nil {
		return nil, errRequest
	}
	request.SetBasicAuth("admin", "admin")
	response, errDo := client.Do(request)
	if errDo != nil {
		return nil, errDo
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("status %d", response.StatusCode)
	}
	var payload map[string]any
	if errDecode := json.NewDecoder(response.Body).Decode(&payload); errDecode != nil {
		return nil, errDecode
	}
	return payload, nil
}

func (r *VerificationReport) check(name string, passed bool) { r.Checks[name] = passed }

func grafanaProbe(ctx context.Context, client *http.Client, endpoint, kind string) (bool, error) {
	payload, errPayload := grafanaJSON(ctx, client, endpoint)
	if errPayload != nil {
		return false, errPayload
	}
	if kind == "grafana_dashboard_present" {
		_, ok := payload["dashboard"]
		return ok, nil
	}
	if traces, ok := payload["traces"].([]any); ok {
		return len(traces) > 0, nil
	}
	if data, ok := payload["data"].(map[string]any); ok {
		if result, okResult := data["result"].([]any); okResult {
			return len(result) > 0, nil
		}
	}
	return false, nil
}

func privacyDefaultsSafe(values map[string]string) bool {
	for _, key := range []string{"OTEL_LOG_USER_PROMPTS", "OTEL_LOG_ASSISTANT_RESPONSES", "OTEL_LOG_TOOL_DETAILS", "OTEL_LOG_TOOL_CONTENT", "OTEL_LOG_RAW_API_BODIES"} {
		if values[key] != "0" {
			return false
		}
	}
	return true
}

func verifyChecksums(runDir string) error {
	file, errOpen := os.Open(filepath.Join(runDir, "checksums.sha256"))
	if errOpen != nil {
		return errOpen
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		expected, relative, ok := strings.Cut(scanner.Text(), "  ")
		if !ok || strings.TrimSpace(relative) == "" {
			return fmt.Errorf("invalid checksum line")
		}
		actual, errHash := fileSHA256(filepath.Join(runDir, relative))
		if errHash != nil || actual != expected {
			return fmt.Errorf("checksum mismatch for %s", relative)
		}
	}
	return scanner.Err()
}

func readJSON(path string, destination any) error {
	payload, errRead := os.ReadFile(path)
	if errRead != nil {
		return fmt.Errorf("read %s: %w", path, errRead)
	}
	if errJSON := json.Unmarshal(payload, destination); errJSON != nil {
		return fmt.Errorf("parse %s: %w", path, errJSON)
	}
	return nil
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func WriteVerification(runDir string, report VerificationReport) error {
	if errJSON := writeJSON(filepath.Join(runDir, "verification.json"), report); errJSON != nil {
		return errJSON
	}
	var markdown strings.Builder
	fmt.Fprintf(&markdown, "# claudex-next OTEL verification %s\n\nPassed: **%t**\n\n", report.RunID, report.Passed)
	names := make([]string, 0, len(report.Checks))
	for name := range report.Checks {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		passed := report.Checks[name]
		fmt.Fprintf(&markdown, "- [%s] `%s`\n", map[bool]string{true: "x", false: " "}[passed], name)
	}
	if errWrite := os.WriteFile(filepath.Join(runDir, "verification.md"), []byte(markdown.String()), 0o600); errWrite != nil {
		return errWrite
	}
	return writeChecksums(runDir)
}
