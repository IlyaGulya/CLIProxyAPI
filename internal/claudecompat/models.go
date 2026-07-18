// Package claudecompat defines the small model-name compatibility boundary
// between Claude Code's client-side capability profiles and proxy routing.
package claudecompat

const (
	SolClientProfile  = "claude-opus-4-6[1m]"
	LunaClientProfile = "claude-sonnet-4-6[1m]"
	SolRequestModel   = "claude-opus-4-6"
	LunaRequestModel  = "claude-sonnet-4-6"
)

var routedByClient = map[string]string{
	SolClientProfile:  "gpt-5.6-sol",
	LunaClientProfile: "gpt-5.6-luna",
	SolRequestModel:   "gpt-5.6-sol",
	LunaRequestModel:  "gpt-5.6-luna",
}

var clientByRouted = map[string]string{
	"gpt-5.6-sol":  SolClientProfile,
	"gpt-5.6-luna": LunaClientProfile,
}

// RoutedModel returns the proxy model represented by an exact Claude Code
// client capability profile. Unknown model names pass through unchanged.
func RoutedModel(model string) string {
	if routed, ok := routedByClient[model]; ok {
		return routed
	}
	return model
}

// ClientModel returns the Claude Code capability profile for a routed model.
// Unknown model names pass through unchanged.
func ClientModel(model string) string {
	if client, ok := clientByRouted[model]; ok {
		return client
	}
	return model
}
