package claude

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

type claudeRequestKind string

const (
	claudeRequestInteractive     claudeRequestKind = "interactive"
	claudeRequestReactiveCompact claudeRequestKind = "reactive_compact"
	claudeRequestClassifier      claudeRequestKind = "classifier"
	claudeRequestCountTokens     claudeRequestKind = "count_tokens"
)

type claudeRequestPolicy struct {
	EffectiveWindow int
	SafetyMargin    int
	MaximumOutput   int
	MinimumOutput   int
	MetadataSource  string
}

const claudeContextSafetyMargin = 8_192

func claudePolicyFor(model string, kind claudeRequestKind) claudeRequestPolicy {
	normalized := strings.ToLower(strings.TrimSpace(model))
	lookupModel := model
	if normalized == "sol" || normalized == "luna" || normalized == "terra" {
		lookupModel = "gpt-5.6-" + normalized
	}
	window := 0
	maximumOutput := 0
	if info := registry.LookupModelInfo(lookupModel, "codex"); info != nil {
		window = info.ContextLength
		maximumOutput = info.MaxCompletionTokens
	}
	metadataSource := "model_registry"
	if window <= 0 {
		window = registry.DefaultClaudeMaxInputTokens
		metadataSource = "registry_fallback"
	}
	if maximumOutput <= 0 {
		maximumOutput = registry.DefaultClaudeMaxOutputTokens
	}
	for _, routed := range []string{"sol", "luna", "terra"} {
		if normalized == routed || strings.HasSuffix(normalized, "-"+routed) || strings.Contains(normalized, "-"+routed+"-") {
			return claudePolicyForKind(window, maximumOutput, kind, metadataSource)
		}
	}
	if registry.LookupModelInfo(lookupModel) != nil {
		return claudePolicyForKind(window, maximumOutput, kind, metadataSource)
	}
	return claudeRequestPolicy{EffectiveWindow: 272_000 * 95 / 100, MaximumOutput: 32_000, MinimumOutput: 1}
}

func claudePolicyForKind(window, maximumOutput int, kind claudeRequestKind, source string) claudeRequestPolicy {
	policy := claudeRequestPolicy{EffectiveWindow: window, SafetyMargin: claudeContextSafetyMargin, MaximumOutput: maximumOutput, MinimumOutput: 8_192, MetadataSource: source}
	switch kind {
	case claudeRequestReactiveCompact:
		policy.MaximumOutput = min(policy.MaximumOutput, claudeReactiveCompactMaxTokens)
	case claudeRequestClassifier:
		policy.MaximumOutput, policy.MinimumOutput = 64, 64
	case claudeRequestCountTokens:
		policy.MaximumOutput, policy.MinimumOutput = 1, 1
	}
	return policy
}

func adaptClaudeOutputBudget(policy claudeRequestPolicy, estimatedInput, requestedOutput int) (int, bool) {
	available := policy.EffectiveWindow - policy.SafetyMargin - estimatedInput
	if requestedOutput <= 0 || requestedOutput <= available || available < policy.MinimumOutput {
		return requestedOutput, false
	}
	return available, true
}
