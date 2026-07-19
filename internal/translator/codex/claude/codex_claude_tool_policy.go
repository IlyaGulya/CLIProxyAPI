package claude

import (
	"strings"

	"github.com/tidwall/gjson"
)

var claudeMutatingFileTools = map[string]struct{}{
	"edit":         {},
	"multiedit":    {},
	"notebookedit": {},
	"write":        {},
}

// claudeParallelToolCalls preserves an explicit Claude client choice. When the
// client leaves the choice unspecified, file mutations are serialized because
// Claude Code dispatches tool uses from one assistant message concurrently.
// Multiple edits generated from the same file snapshot would otherwise race.
func claudeParallelToolCalls(request gjson.Result) bool {
	if disabled := request.Get("tool_choice.disable_parallel_tool_use"); disabled.Exists() {
		return !disabled.Bool()
	}

	for _, tool := range request.Get("tools").Array() {
		name := strings.ToLower(strings.TrimSpace(tool.Get("name").String()))
		if _, mutating := claudeMutatingFileTools[name]; mutating {
			return false
		}
	}
	return true
}
