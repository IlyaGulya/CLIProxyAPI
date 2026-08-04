package registry

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Codex client context budgets are product limits, not the model's physical
// API context window. Remote catalogs may lower these safety-critical limits,
// but raising them requires a binary update so stale or speculative metadata
// cannot delay client compaction past the supported budget.
var codexClientContextWindowCaps = map[string]int{
	"gpt-5.6-sol":   272_000,
	"gpt-5.6-terra": 272_000,
	"gpt-5.6-luna":  272_000,
}

func codexClientContextWindowCap(modelID string) (int, bool) {
	cap, ok := codexClientContextWindowCaps[strings.ToLower(strings.TrimSpace(modelID))]
	return cap, ok
}

func applyCodexModelContextWindowCaps(data *staticModelsJSON) {
	if data == nil {
		return
	}
	for _, models := range [][]*ModelInfo{data.CodexFree, data.CodexTeam, data.CodexPlus, data.CodexPro} {
		for _, model := range models {
			if model == nil {
				continue
			}
			cap, ok := codexClientContextWindowCap(model.ID)
			if ok && model.ContextLength > cap {
				model.ContextLength = cap
			}
		}
	}
}

func applyCodexClientCatalogContextWindowCaps(data []byte) ([]byte, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("decode Codex client model catalog for context limits: %w", err)
	}
	var models []map[string]any
	if err := json.Unmarshal(root["models"], &models); err != nil {
		return nil, fmt.Errorf("decode Codex client models for context limits: %w", err)
	}

	changed := false
	for _, model := range models {
		modelID, _ := model["slug"].(string)
		cap, ok := codexClientContextWindowCap(modelID)
		if !ok {
			continue
		}
		for _, field := range []string{"context_window", "max_context_window"} {
			value, _ := model[field].(float64)
			if value > float64(cap) {
				model[field] = cap
				changed = true
			}
		}
	}
	if !changed {
		return append([]byte(nil), data...), nil
	}

	normalizedModels, err := json.Marshal(models)
	if err != nil {
		return nil, fmt.Errorf("encode Codex client models with context limits: %w", err)
	}
	root["models"] = normalizedModels
	normalized, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("encode Codex client model catalog with context limits: %w", err)
	}
	return normalized, nil
}
