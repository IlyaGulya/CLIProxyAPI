package claudecompat

import "testing"

func TestModelCompatibilityMappingsAreExactAndReversible(t *testing.T) {
	for routed, client := range map[string]string{
		"gpt-5.6-sol": SolClientProfile, "gpt-5.6-luna": LunaClientProfile,
	} {
		if got := ClientModel(routed, DefaultModelMappings()); got != client {
			t.Fatalf("ClientModel(%q) = %q, want %q", routed, got, client)
		}
	}
	mappings := DefaultModelMappings()
	if RoutedModel(SolRequestModel, mappings) != "gpt-5.6-sol" || RoutedModel(LunaRequestModel, mappings) != "gpt-5.6-luna" {
		t.Fatal("Claude Code's normalized request models were not routed")
	}
	for _, model := range []string{"gpt-5.6-terra", "custom", SolClientProfile + "-lookalike"} {
		if ClientModel(model, mappings) != model || RoutedModel(model, mappings) != model {
			t.Fatalf("unknown model %q was rewritten", model)
		}
	}
	if RoutedModel(SolRequestModel, nil) != SolRequestModel {
		t.Fatal("model was rewritten without explicit mappings")
	}
}
