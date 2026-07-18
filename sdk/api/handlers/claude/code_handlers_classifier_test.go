package claude

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
	"github.com/tidwall/gjson"
)

const autoModeClassifierPayload = `{
  "model":"claude-sonnet-5",
  "max_tokens":64,
  "system":[
    {"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.211.def; cc_entrypoint=cli;"},
    {"type":"text","text":"You are a security monitor for autonomous AI coding agents.\n\n## Context"}
  ],
  "messages":[{"role":"user","content":[{"type":"text","text":"<transcript>..."}]}],
  "stop_sequences":["</block>"],
  "thinking":{"type":"disabled"}
}`

type classifierCaptureExecutor struct {
	request coreexecutor.Request
}

func (e *classifierCaptureExecutor) Identifier() string { return "codex" }

func (e *classifierCaptureExecutor) Execute(_ context.Context, _ *coreauth.Auth, req coreexecutor.Request, _ coreexecutor.Options) (coreexecutor.Response, error) {
	e.request = req
	return coreexecutor.Response{}, errors.New("captured classifier request")
}

func (e *classifierCaptureExecutor) ExecuteStream(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	return nil, errors.New("unexpected streaming request")
}

func (e *classifierCaptureExecutor) Refresh(_ context.Context, auth *coreauth.Auth) (*coreauth.Auth, error) {
	return auth, nil
}

func (e *classifierCaptureExecutor) CountTokens(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
	return coreexecutor.Response{}, errors.New("unexpected token count request")
}

func (e *classifierCaptureExecutor) HttpRequest(context.Context, *coreauth.Auth, *http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected HTTP request")
}

func TestRewriteClaudeCodeAutoModeClassifierModel(t *testing.T) {
	got, rewritten := rewriteClaudeCodeAutoModeClassifierModel([]byte(autoModeClassifierPayload), " gpt-5.6-luna ")
	if !rewritten {
		t.Fatal("rewriteClaudeCodeAutoModeClassifierModel() rewritten = false, want true")
	}
	if model := gjson.GetBytes(got, "model").String(); model != "gpt-5.6-luna" {
		t.Fatalf("rewritten model = %q, want gpt-5.6-luna", model)
	}
	if prompt := gjson.GetBytes(got, "system.1.text").String(); prompt == "" {
		t.Fatal("classifier payload was not preserved")
	}
}

func TestRewriteClaudeCodeSubagentEffortIsNeutralAndValidated(t *testing.T) {
	input := []byte(`{"model":"gpt-5.6-luna","thinking":{"type":"adaptive"},"output_config":{"effort":"xhigh"}}`)
	if got, changed := rewriteClaudeCodeSubagentEffort(input, ""); changed || string(got) != string(input) {
		t.Fatalf("empty policy changed request: %s", got)
	}
	got, changed := rewriteClaudeCodeSubagentEffort(input, "high")
	if !changed || gjson.GetBytes(got, "output_config.effort").String() != "high" {
		t.Fatalf("configured policy not applied: %s", got)
	}
	if _, changed := rewriteClaudeCodeSubagentEffort(input, "absurd"); changed {
		t.Fatal("invalid effort was applied")
	}
}

func TestClaudeMessagesRoutesAutoModeClassifierToConfiguredModel(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &classifierCaptureExecutor{}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "classifier-luna", Provider: "codex", Status: coreauth.StatusActive}
	if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "gpt-5.6-luna"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })

	base := handlers.NewBaseAPIHandlers(&config.SDKConfig{ClaudeCodeAutoModeClassifierModel: "gpt-5.6-luna"}, manager)
	handler := NewClaudeCodeAPIHandler(base)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages?beta=true", strings.NewReader(autoModeClassifierPayload))
	handler.ClaudeMessages(c)

	if executor.request.Model != "gpt-5.6-luna" {
		t.Fatalf("routed model = %q, want gpt-5.6-luna; response=%s", executor.request.Model, recorder.Body.String())
	}
	if model := gjson.GetBytes(executor.request.Payload, "model").String(); model != "gpt-5.6-luna" {
		t.Fatalf("executor payload model = %q, want gpt-5.6-luna", model)
	}
}

func TestRewriteClaudeCodeAutoModeClassifierModelDoesNotRewriteOrdinaryRequests(t *testing.T) {
	tests := map[string]string{
		"ordinary request": `{"model":"claude-sonnet-5","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`,
		"different model":  `{"model":"gpt-5.6-sol","max_tokens":64,"system":[{"type":"text","text":"You are a security monitor for autonomous AI coding agents."}],"stop_sequences":["</block>"],"thinking":{"type":"disabled"}}`,
		"different stop":   `{"model":"claude-sonnet-5","max_tokens":64,"system":[{"type":"text","text":"You are a security monitor for autonomous AI coding agents."}],"stop_sequences":["stop"],"thinking":{"type":"disabled"}}`,
		"different budget": `{"model":"claude-sonnet-5","max_tokens":128,"system":[{"type":"text","text":"You are a security monitor for autonomous AI coding agents."}],"stop_sequences":["</block>"],"thinking":{"type":"disabled"}}`,
		"thinking enabled": `{"model":"claude-sonnet-5","max_tokens":64,"system":[{"type":"text","text":"You are a security monitor for autonomous AI coding agents."}],"stop_sequences":["</block>"],"thinking":{"type":"enabled"}}`,
		"lookalike prompt": `{"model":"claude-sonnet-5","max_tokens":64,"system":[{"type":"text","text":"Discuss a security monitor for autonomous AI coding agents."}],"stop_sequences":["</block>"],"thinking":{"type":"disabled"}}`,
	}
	for name, payload := range tests {
		t.Run(name, func(t *testing.T) {
			got, rewritten := rewriteClaudeCodeAutoModeClassifierModel([]byte(payload), "gpt-5.6-luna")
			if rewritten {
				t.Fatal("rewriteClaudeCodeAutoModeClassifierModel() rewritten = true, want false")
			}
			if string(got) != payload {
				t.Fatal("non-classifier payload changed")
			}
		})
	}

	for name, target := range map[string]string{"disabled": "", "same target": "claude-sonnet-5"} {
		t.Run(name, func(t *testing.T) {
			got, rewritten := rewriteClaudeCodeAutoModeClassifierModel([]byte(autoModeClassifierPayload), target)
			if rewritten || string(got) != autoModeClassifierPayload {
				t.Fatal("disabled classifier rewrite changed the payload")
			}
		})
	}
}
