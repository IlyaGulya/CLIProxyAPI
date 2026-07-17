package claude

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	"github.com/tidwall/gjson"
)

func TestSortClaudeModelsByDisplayName(t *testing.T) {
	models := []map[string]any{
		{"id": "claude-fable-5-dd-b", "display_name": "Zebra"},
		{"id": "claude-a", "display_name": "Alpha"},
		{"id": "claude-c", "display_name": "Alpha"},
		{"id": "claude-fable-5-dd-d", "display_name": "Beta"},
	}
	sortClaudeModelsByDisplayName(models)

	wantIDs := []string{"claude-a", "claude-c", "claude-fable-5-dd-d", "claude-fable-5-dd-b"}
	for i, want := range wantIDs {
		got, _ := models[i]["id"].(string)
		if got != want {
			t.Fatalf("models[%d].id = %q, want %q", i, got, want)
		}
	}
}

func TestClaudeModelsExposeTruthfulCapabilitiesAndPagination(t *testing.T) {
	const clientID = "claude-capability-catalog-test"
	const modelID = "gpt-capability-test"
	registryRef := registry.GetGlobalRegistry()
	registryRef.RegisterClient(clientID, "codex", []*registry.ModelInfo{{
		ID: modelID, Object: "model", Created: 1704067200, OwnedBy: "openai", DisplayName: "Capability Test",
		ContextLength: 372000, MaxCompletionTokens: 128000,
		SupportedParameters:      []string{"tools", "structured_outputs"},
		SupportedInputModalities: []string{"TEXT", "IMAGE"},
		Thinking:                 &registry.ThinkingSupport{Levels: []string{"low", "high", "xhigh"}},
	}, {ID: "gpt-capability-test-second", Object: "model", DisplayName: "Capability Test 2"}})
	t.Cleanup(func() { registryRef.UnregisterClient(clientID) })

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v1/models?limit=1", nil)
	NewClaudeCodeAPIHandler(&handlers.BaseAPIHandler{}).ClaudeModels(ctx)
	if recorder.Code != http.StatusOK || gjson.GetBytes(recorder.Body.Bytes(), "data.#").Int() != 1 || !gjson.GetBytes(recorder.Body.Bytes(), "has_more").Bool() {
		t.Fatalf("pagination response = %d %s", recorder.Code, recorder.Body.String())
	}

	models := NewClaudeCodeAPIHandler(&handlers.BaseAPIHandler{}).Models()
	for _, model := range models {
		if model["id"] != modelID {
			continue
		}
		encoded, _ := json.Marshal(model)
		checks := map[string]bool{
			"max_input_tokens":                       gjson.GetBytes(encoded, "max_input_tokens").Int() == 372000,
			"capabilities.thinking.supported":        gjson.GetBytes(encoded, "capabilities.thinking.supported").Bool(),
			"capabilities.effort.xhigh.supported":    gjson.GetBytes(encoded, "capabilities.effort.xhigh.supported").Bool(),
			"capabilities.effort.medium.unsupported": !gjson.GetBytes(encoded, "capabilities.effort.medium.supported").Bool(),
			"capabilities.image_input.supported":     gjson.GetBytes(encoded, "capabilities.image_input.supported").Bool(),
			"capabilities.pdf_input.unsupported":     !gjson.GetBytes(encoded, "capabilities.pdf_input.supported").Bool(),
		}
		for name, ok := range checks {
			if !ok {
				t.Fatalf("%s failed: %s", name, encoded)
			}
		}
		return
	}
	t.Fatalf("model %q not found", modelID)
}

func TestClaudeModelReturnsOneModelOrAnthropicNotFound(t *testing.T) {
	const clientID = "claude-single-model-test"
	const modelID = "gpt-single-model-test"
	registryRef := registry.GetGlobalRegistry()
	registryRef.RegisterClient(clientID, "codex", []*registry.ModelInfo{{ID: modelID, DisplayName: "Single"}})
	t.Cleanup(func() { registryRef.UnregisterClient(clientID) })
	handler := NewClaudeCodeAPIHandler(&handlers.BaseAPIHandler{})

	for _, test := range []struct {
		id     string
		status int
	}{{modelID, http.StatusOK}, {"missing-model", http.StatusNotFound}} {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Params = gin.Params{{Key: "model_id", Value: test.id}}
		handler.ClaudeModel(ctx)
		if recorder.Code != test.status {
			t.Fatalf("model %q status = %d, want %d: %s", test.id, recorder.Code, test.status, recorder.Body.String())
		}
	}
}

func TestClaudeModelsResponseUsesConfiguredDisplayName(t *testing.T) {
	const clientID = "claude-display-name-catalog-test"
	const modelID = "claude-display-name-catalog-test"
	registryRef := registry.GetGlobalRegistry()
	registryRef.RegisterClient(clientID, "claude", []*registry.ModelInfo{{
		ID: modelID, Object: "model", OwnedBy: "test", DisplayName: "Configured Claude Name",
	}})
	t.Cleanup(func() {
		registryRef.UnregisterClient(clientID)
	})

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	NewClaudeCodeAPIHandler(&handlers.BaseAPIHandler{}).ClaudeModels(ctx)

	var response struct {
		Data []struct {
			ID          string `json:"id"`
			DisplayName string `json:"display_name"`
		} `json:"data"`
	}
	if errUnmarshal := json.Unmarshal(recorder.Body.Bytes(), &response); errUnmarshal != nil {
		t.Fatalf("decode response: %v", errUnmarshal)
	}
	for _, model := range response.Data {
		if model.ID == modelID {
			if model.DisplayName != "Configured Claude Name" {
				t.Fatalf("display_name = %q, want Configured Claude Name", model.DisplayName)
			}
			return
		}
	}
	t.Fatalf("model %q not found in response", modelID)
}

func TestRewriteClaudeDDModelInBody(t *testing.T) {
	tests := []struct {
		name      string
		body      string
		wantModel string
	}{
		{
			name:      "encoded model is decoded",
			body:      `{"model":"claude-fable-5-dd-o4-tpg","messages":[]}`,
			wantModel: "gpt-4o",
		},
		{
			name:      "plain claude model unchanged",
			body:      `{"model":"claude-sonnet-4-6","messages":[]}`,
			wantModel: "claude-sonnet-4-6",
		},
		{
			name:      "encoded model with thinking suffix",
			body:      `{"model":"claude-fable-5-dd-o4-tpg(high)","stream":true}`,
			wantModel: "gpt-4o(high)",
		},
		{
			name:      "missing model field unchanged",
			body:      `{"messages":[]}`,
			wantModel: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rewriteClaudeDDModelInBody([]byte(tt.body))
			if model := gjson.GetBytes(got, "model").String(); model != tt.wantModel {
				t.Fatalf("model = %q, want %q; body=%s", model, tt.wantModel, string(got))
			}
		})
	}
}
