package claude

import "strings"

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
}

var routedClaudePolicies = map[claudeRequestKind]claudeRequestPolicy{
	claudeRequestInteractive:     {EffectiveWindow: 180_000, SafetyMargin: 8_192, MaximumOutput: 32_000, MinimumOutput: 8_192},
	claudeRequestReactiveCompact: {EffectiveWindow: 180_000, SafetyMargin: 8_192, MaximumOutput: 8_192, MinimumOutput: 8_192},
	claudeRequestClassifier:      {EffectiveWindow: 180_000, SafetyMargin: 8_192, MaximumOutput: 64, MinimumOutput: 64},
	claudeRequestCountTokens:     {EffectiveWindow: 180_000, SafetyMargin: 8_192, MaximumOutput: 1, MinimumOutput: 1},
}

func claudePolicyFor(model string, kind claudeRequestKind) claudeRequestPolicy {
	normalized := strings.ToLower(strings.TrimSpace(model))
	for _, routed := range []string{"sol", "luna", "terra"} {
		if normalized == routed || strings.HasSuffix(normalized, "-"+routed) {
			if policy, exists := routedClaudePolicies[kind]; exists {
				return policy
			}
			return routedClaudePolicies[claudeRequestInteractive]
		}
	}
	return claudeRequestPolicy{EffectiveWindow: 272_000 * 95 / 100, MaximumOutput: 32_000, MinimumOutput: 1}
}

func adaptClaudeOutputBudget(policy claudeRequestPolicy, estimatedInput, requestedOutput int) (int, bool) {
	available := policy.EffectiveWindow - policy.SafetyMargin - estimatedInput
	if requestedOutput <= 0 || requestedOutput <= available || available < policy.MinimumOutput {
		return requestedOutput, false
	}
	return available, true
}
