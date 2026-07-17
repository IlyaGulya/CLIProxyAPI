package executor

import (
	"context"
	"strings"
	"testing"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestValidateCodexClaudeServerTools(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		format  sdktranslator.Format
		tool    string
		wantErr bool
	}{
		{"latest web search", sdktranslator.FormatClaude, "web_search_20260318", false},
		{"unknown web search", sdktranslator.FormatClaude, "web_search_20990101", true},
		{"web fetch", sdktranslator.FormatClaude, "web_fetch_20260318", true},
		{"code execution", sdktranslator.FormatClaude, "code_execution_20260521", true},
		{"advisor", sdktranslator.FormatClaude, "advisor_20260301", true},
		{"native Claude route is outside boundary", sdktranslator.FormatOpenAIResponse, "web_fetch_20260318", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateCodexClaudeServerTools(test.format, []byte(`{"tools":[{"type":"`+test.tool+`"}]}`))
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, test.wantErr)
			}
			if err != nil && (!strings.Contains(err.Error(), test.tool) || !strings.Contains(err.Error(), "Codex route")) {
				t.Fatalf("error is not actionable: %v", err)
			}
		})
	}
}

func TestCodexClaudeServerToolCompatibilityMatrixIsExhaustive(t *testing.T) {
	t.Parallel()
	for toolType, policy := range codexClaudeServerToolCompatibility {
		if policy.Mapping == "" {
			t.Errorf("%s has no explicit mapping/rejection reason", toolType)
		}
		err := validateCodexClaudeServerTools(sdktranslator.FormatClaude, []byte(`{"tools":[{"type":"`+toolType+`"}]}`))
		if policy.Supported && err != nil {
			t.Errorf("supported %s rejected: %v", toolType, err)
		}
		if !policy.Supported && err == nil {
			t.Errorf("unsupported %s accepted", toolType)
		}
	}
}

func TestCodexWebsocketPlannerRejectsUnsupportedClaudeServerToolBeforeTransport(t *testing.T) {
	t.Parallel()
	payload := []byte(`{"model":"gpt-5.6-sol","tools":[{"type":"code_execution_20260521","name":"code_execution"}],"messages":[{"role":"user","content":"run"}]}`)
	_, err := newCodexWebsocketRequestPlanner(nil).Plan(context.Background(), nil, cliproxyexecutor.Request{Model: "gpt-5.6-sol", Payload: payload}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude}, true)
	if err == nil || !strings.Contains(err.Error(), "code_execution_20260521") {
		t.Fatalf("planner error = %v", err)
	}
}
