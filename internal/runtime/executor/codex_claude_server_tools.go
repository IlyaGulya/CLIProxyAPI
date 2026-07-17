package executor

import (
	"fmt"
	"net/http"
	"regexp"
	"strings"

	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

var claudeVersionedServerTool = regexp.MustCompile(`^[a-z][a-z0-9_]*_20[0-9]{6}$`)

type codexClaudeServerToolPolicy struct {
	Supported bool
	Mapping   string
}

var codexClaudeServerToolCompatibility = map[string]codexClaudeServerToolPolicy{
	"web_search_20250305":             {Supported: true, Mapping: "web_search"},
	"web_search_20260209":             {Supported: true, Mapping: "web_search"},
	"web_search_20260318":             {Supported: true, Mapping: "web_search"},
	"web_fetch_20260318":              {Mapping: "requires native Claude web fetch"},
	"code_execution_20250522":         {Mapping: "requires native Claude container"},
	"code_execution_20260120":         {Mapping: "requires native Claude container"},
	"code_execution_20260521":         {Mapping: "requires native Claude container"},
	"advisor_20260301":                {Mapping: "requires native Claude advisor"},
	"tool_search_tool_bm25_20251119":  {Mapping: "requires native Claude tool search"},
	"tool_search_tool_regex_20251119": {Mapping: "requires native Claude tool search"},
}

// validateCodexClaudeServerTools is the provider-aware compatibility boundary.
// Native Claude providers never pass through this function; Codex routes reject
// server tools they cannot faithfully execute instead of degrading them into
// client-side functions with empty names.
func validateCodexClaudeServerTools(from sdktranslator.Format, payload []byte) error {
	if !sourceFormatEqual(from, sdktranslator.FormatClaude) {
		return nil
	}
	for _, tool := range gjson.GetBytes(payload, "tools").Array() {
		toolType := strings.TrimSpace(tool.Get("type").String())
		if toolType == "" || toolType == "custom" {
			continue
		}
		if policy, known := codexClaudeServerToolCompatibility[toolType]; known {
			if policy.Supported {
				continue
			}
			return statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("Claude server tool %q is not supported by the Codex route (%s); use a compatible Claude provider or remove this tool", toolType, policy.Mapping)}
		}
		if claudeVersionedServerTool.MatchString(toolType) || strings.HasPrefix(toolType, "web_search_") {
			return statusErr{code: http.StatusBadRequest, msg: fmt.Sprintf("Claude server tool %q is not supported by the Codex route; use a compatible Claude provider or remove this tool", toolType)}
		}
	}
	return nil
}
