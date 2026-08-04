package registry

import "testing"

func TestModelOverrideHeadersFromEmbeddedModels(t *testing.T) {
	const wantUA = "codex-tui/0.144.0 (Mac OS 26.5.1; arm64) iTerm.app/3.6.11 (codex-tui; 0.144.0)"
	got := ModelOverrideHeaders("gpt-5.6-luna")
	if got == nil {
		t.Fatal("ModelOverrideHeaders(gpt-5.6-luna) = nil, want headers")
	}
	if got["user-agent"] != wantUA {
		t.Fatalf("user-agent = %q, want %q", got["user-agent"], wantUA)
	}
	if got := ModelOverrideHeaders("gpt-5.4"); got != nil {
		t.Fatalf("ModelOverrideHeaders(gpt-5.4) = %#v, want nil", got)
	}
}

func TestCodexModelDefinitionsUseCurrentClientContextBudget(t *testing.T) {
	tests := []struct {
		name   string
		models []*ModelInfo
	}{
		{name: "free", models: GetCodexFreeModels()},
		{name: "team", models: GetCodexTeamModels()},
		{name: "plus", models: GetCodexPlusModels()},
		{name: "pro", models: GetCodexProModels()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, model := range test.models {
				if model == nil || (model.ID != "gpt-5.6-sol" && model.ID != "gpt-5.6-terra" && model.ID != "gpt-5.6-luna") {
					continue
				}
				if model.ContextLength != 272_000 {
					t.Errorf("%s context_length = %d, want 272000", model.ID, model.ContextLength)
				}
			}
		})
	}
}

func TestApplyCodexModelContextWindowCapsPreservesLowerAndUnknownLimits(t *testing.T) {
	data := &staticModelsJSON{CodexPro: []*ModelInfo{
		{ID: "gpt-5.6-sol", ContextLength: 372_000},
		{ID: "gpt-5.6-terra", ContextLength: 200_000},
		{ID: "custom-model", ContextLength: 1_050_000},
	}}

	applyCodexModelContextWindowCaps(data)

	if got := data.CodexPro[0].ContextLength; got != 272_000 {
		t.Fatalf("known model context_length = %d, want capped 272000", got)
	}
	if got := data.CodexPro[1].ContextLength; got != 200_000 {
		t.Fatalf("lower remote context_length = %d, want unchanged 200000", got)
	}
	if got := data.CodexPro[2].ContextLength; got != 1_050_000 {
		t.Fatalf("unknown model context_length = %d, want unchanged 1050000", got)
	}
}

func TestWithXAIBuiltinsIncludesVideoPreviewModel(t *testing.T) {
	models := WithXAIBuiltins(nil)

	for _, model := range models {
		if model == nil {
			continue
		}
		if model.ID == xaiBuiltinVideo15PreviewModelID {
			return
		}
	}

	t.Fatalf("expected xAI builtin model %s", xaiBuiltinVideo15PreviewModelID)
}

func TestAntigravityWebSearchModelForRequiresRequestedModelCapability(t *testing.T) {
	registryRef := GetGlobalRegistry()
	registryRef.RegisterClient("test-antigravity-websearch-route", "antigravity", []*ModelInfo{
		{ID: "gemini-route-test"},
		{ID: "gemini-web-search-test", SupportsWebSearch: true},
	})
	registryRef.RegisterClient("test-gemini-websearch-route", "gemini", []*ModelInfo{
		{ID: "gemini-cross-provider-route"},
		{ID: "gemini-cross-provider-search", SupportsWebSearch: true},
	})
	t.Cleanup(func() {
		registryRef.UnregisterClient("test-antigravity-websearch-route")
		registryRef.UnregisterClient("test-gemini-websearch-route")
	})

	if got := AntigravityWebSearchModelFor("gemini-route-test"); got != "" {
		t.Fatalf("route model without web search support should not get fallback model, got %q", got)
	}
	if got := AntigravityWebSearchModelFor("gemini-route-test(high)"); got != "" {
		t.Fatalf("suffix route model without web search support should not get fallback model, got %q", got)
	}
	if got := AntigravityWebSearchModelFor("gemini-web-search-test"); got != "gemini-web-search-test" {
		t.Fatalf("AntigravityWebSearchModelFor capable model = %q, want itself", got)
	}
	if got := AntigravityWebSearchModelFor("gemini-cross-provider-route"); got != "" {
		t.Fatalf("cross-provider model should not get Antigravity web search model, got %q", got)
	}
	if got := AntigravityWebSearchModelFor("unknown-model"); got != "" {
		t.Fatalf("unknown model should not get Antigravity web search model, got %q", got)
	}
}
