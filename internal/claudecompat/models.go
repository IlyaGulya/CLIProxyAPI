// Package claudecompat defines shared compatibility contracts between Claude
// clients and provider-specific proxy behavior.
package claudecompat

import (
	"sort"
	"strings"
)

const (
	SolClientProfile  = "claude-opus-4-6[1m]"
	LunaClientProfile = "claude-sonnet-4-6[1m]"
	SolRequestModel   = "claude-opus-4-6"
	LunaRequestModel  = "claude-sonnet-4-6"
)

// RoutedModel returns the proxy model represented by an exact Claude Code
// client capability profile. Unknown model names pass through unchanged.
func RoutedModel(model string, mappings map[string]string) string {
	if routed := mappings[model]; routed != "" {
		return routed
	}
	return model
}

// DefaultModelMappings returns a fresh declarative mapping for claudex-next's
// isolated proxy. General proxy handlers have no implicit model rewrites.
func DefaultModelMappings() map[string]string {
	return map[string]string{
		SolClientProfile: "gpt-5.6-sol", SolRequestModel: "gpt-5.6-sol",
		LunaClientProfile: "gpt-5.6-luna", LunaRequestModel: "gpt-5.6-luna",
	}
}

// ClientModel returns the Claude Code capability profile for a routed model.
// Unknown model names pass through unchanged.
func ClientModel(model string, mappings ...map[string]string) string {
	var configured map[string]string
	if len(mappings) > 0 {
		configured = mappings[0]
	}
	candidates := make([]string, 0)
	for client, routed := range configured {
		if routed == model && strings.HasSuffix(client, "[1m]") {
			candidates = append(candidates, client)
		}
	}
	if len(candidates) > 0 {
		sort.Strings(candidates)
		return candidates[0]
	}
	return model
}
