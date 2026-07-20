package claudecompat

import "strings"

const (
	reactiveCompactSummaryPrompt = "create a detailed summary of the conversation so far"
	reactiveCompactAnalysisTag   = "wrap your analysis in <analysis> tags"
	reactiveCompactBlockOrder    = "<analysis> block followed by a <summary> block"
)

// IsReactiveCompactPrompt reports whether text is Claude Code's synthetic
// prompt for summarizing a conversation at a context boundary.
func IsReactiveCompactPrompt(text string) bool {
	normalized := strings.ToLower(text)
	return strings.Contains(normalized, reactiveCompactSummaryPrompt) &&
		strings.Contains(normalized, reactiveCompactAnalysisTag) &&
		strings.Contains(normalized, reactiveCompactBlockOrder)
}
