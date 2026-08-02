package claudecompat

import (
	"strings"
	"testing"
)

func TestEstimateInputTokensJSONOmitsBase64Payload(t *testing.T) {
	t.Parallel()
	small := []byte(`{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"A"}}]}]}`)
	large := []byte(`{"messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"` + strings.Repeat("A", 1_000_000) + `"}}]}]}`)
	smallTokens, smallMethod := EstimateInputTokensJSON(small)
	largeTokens, largeMethod := EstimateInputTokensJSON(large)
	if smallTokens <= 0 || largeTokens != smallTokens {
		t.Fatalf("image estimates small=%d large=%d", smallTokens, largeTokens)
	}
	if smallMethod != largeMethod {
		t.Fatalf("estimation methods differ: %q != %q", smallMethod, largeMethod)
	}
}

func TestEstimateInputTokensJSONFallsBackForInvalidJSON(t *testing.T) {
	t.Parallel()
	tokens, method := EstimateInputTokensJSON([]byte("12345{"))
	if tokens != 2 || method != "bytes_fallback" {
		t.Fatalf("estimate = %d/%s, want 2/bytes_fallback", tokens, method)
	}
}
