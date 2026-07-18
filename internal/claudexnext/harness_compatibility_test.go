package claudexnext

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestClaudeHarnessCompatibilityMatrixCoversExecutionModes(t *testing.T) {
	matrix := ClaudeHarnessCompatibilityMatrix("2.1.212")
	want := []string{"bare", "safe_mode", "stream_json", "partial_messages", "forward_subagent_text", "prompt_suggestions", "structured_output", "background_agent", "workflow", "fork_session", "no_session_persistence", "cancellation"}
	seen := make(map[string]bool, len(matrix.Cases))
	for _, testCase := range matrix.Cases {
		if seen[testCase.Name] {
			t.Fatalf("duplicate case %q", testCase.Name)
		}
		seen[testCase.Name] = true
		if len(testCase.Args) == 0 || len(testCase.ExpectedEvents) == 0 || len(testCase.ExpectedArtifacts) == 0 || testCase.TranscriptPolicy == "" {
			t.Errorf("case %q is incomplete: %+v", testCase.Name, testCase)
		}
		if testCase.NeedsAgent && !containsString(testCase.Args, "--dangerously-skip-permissions") {
			t.Errorf("agent case %q cannot execute non-interactively: args=%v", testCase.Name, testCase.Args)
		}
	}
	for _, name := range want {
		if !seen[name] {
			t.Errorf("missing compatibility case %q", name)
		}
	}
}

func TestSummarizeClaudeHarnessJSONLProducesPromptFreeSchema(t *testing.T) {
	input := strings.NewReader(`{"type":"system","subtype":"init","session_id":"secret-session","model":"gpt-5.6-sol"}
{"type":"assistant","parent_tool_use_id":"tool-secret","message":{"id":"msg-secret","content":[{"type":"text","text":"PRIVATE PROMPT OUTPUT"}]}}
{"type":"result","subtype":"success","is_error":false,"session_id":"secret-session","result":"PRIVATE PROMPT OUTPUT"}
`)
	report, err := SummarizeClaudeHarnessJSONL(input, "2.1.212")
	if err != nil {
		t.Fatalf("SummarizeClaudeHarnessJSONL: %v", err)
	}
	encoded := string(mustJSON(t, report))
	for _, secret := range []string{"secret-session", "tool-secret", "msg-secret", "PRIVATE PROMPT OUTPUT"} {
		if strings.Contains(encoded, secret) {
			t.Errorf("schema report leaked %q: %s", secret, encoded)
		}
	}
	for _, want := range []string{"system:init", "assistant", "result:success", "message.content[].text", "parent_tool_use_id"} {
		if !strings.Contains(encoded, want) {
			t.Errorf("schema report missing %q: %s", want, encoded)
		}
	}
}

func TestSummarizeClaudeHarnessJSONLRejectsMissingTerminalResult(t *testing.T) {
	_, err := SummarizeClaudeHarnessJSONL(strings.NewReader(`{"type":"system","subtype":"init"}`+"\n"), "2.1.212")
	if err == nil || !strings.Contains(err.Error(), "terminal result") {
		t.Fatalf("error = %v, want missing terminal result", err)
	}
}

func TestCaptureClaudeHarnessSchemaAcceptsPrettyJSONResult(t *testing.T) {
	path := t.TempDir() + "/stdout.log"
	if err := os.WriteFile(path, []byte("{\n  \"type\": \"result\",\n  \"subtype\": \"success\",\n  \"result\": \"secret\"\n}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := CaptureClaudeHarnessSchema(path, "json", "2.1.212")
	if err != nil {
		t.Fatal(err)
	}
	if report.Events["result:success"] != 1 {
		t.Fatalf("events = %#v", report.Events)
	}
	if strings.Contains(string(mustJSON(t, report)), "secret") {
		t.Fatal("schema report leaked result value")
	}
}

func TestVerifyClaudeHarnessCaseChecksArtifactsEventsAndTranscript(t *testing.T) {
	dir := t.TempDir()
	for _, relative := range []string{"manifest.json", "claude/stdout.log", "transcripts/session.jsonl"} {
		path := dir + "/" + relative
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("{}\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	report := ClaudeHarnessSchemaReport{Version: "2.1.212", Events: map[string]int{"system:init": 1, "result:success": 1}}
	if err := os.WriteFile(dir+"/harness-schema.json", mustJSON(t, report), 0o600); err != nil {
		t.Fatal(err)
	}
	testCase := ClaudeHarnessCompatibilityCase{Name: "fixture", ExpectedArtifacts: []string{"manifest.json", "claude/stdout.log", "harness-schema.json"}, ExpectedEvents: []string{"system:init", "result:success"}, TranscriptPolicy: "required"}
	if err := VerifyClaudeHarnessCase(dir, testCase); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir + "/transcripts/session.jsonl"); err != nil {
		t.Fatal(err)
	}
	if err := VerifyClaudeHarnessCase(dir, testCase); err == nil || !strings.Contains(err.Error(), "transcript") {
		t.Fatalf("error = %v, want transcript failure", err)
	}
}
