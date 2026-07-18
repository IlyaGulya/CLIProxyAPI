package claudecompat

import "testing"

func TestModelCompatibilityMappingsAreExactAndReversible(t *testing.T) {
	for routed, client := range map[string]string{
		"gpt-5.6-sol": SolClientProfile, "gpt-5.6-luna": LunaClientProfile,
	} {
		if got := ClientModel(routed); got != client {
			t.Fatalf("ClientModel(%q) = %q, want %q", routed, got, client)
		}
		if got := RoutedModel(client); got != routed {
			t.Fatalf("RoutedModel(%q) = %q, want %q", client, got, routed)
		}
	}
	if RoutedModel(SolRequestModel) != "gpt-5.6-sol" || RoutedModel(LunaRequestModel) != "gpt-5.6-luna" {
		t.Fatal("Claude Code's normalized request models were not routed")
	}
	for _, model := range []string{"gpt-5.6-terra", "custom", SolClientProfile + "-lookalike"} {
		if ClientModel(model) != model || RoutedModel(model) != model {
			t.Fatalf("unknown model %q was rewritten", model)
		}
	}
}
