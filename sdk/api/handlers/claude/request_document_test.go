package claude

import (
	"strings"
	"testing"
)

func TestClaudeRequestDocumentCachesTokenEstimateUntilMutation(t *testing.T) {
	document, errDocument := newClaudeRequestDocument([]byte(`{"model":"gpt-5.6-sol","max_tokens":32000,"messages":[{"role":"user","content":"hello"}]}`))
	if errDocument != nil {
		t.Fatal(errDocument)
	}
	first, firstMethod := document.estimateInputTokens()
	second, secondMethod := document.estimateInputTokens()
	if first <= 0 || first != second || firstMethod != secondMethod || document.estimateComputations != 1 {
		t.Fatalf("estimate cache = %d/%s %d/%s computations=%d", first, firstMethod, second, secondMethod, document.estimateComputations)
	}
	if !document.setMaxTokens(8192) {
		t.Fatal("max_tokens mutation failed")
	}
	_, _ = document.estimateInputTokens()
	if document.estimateComputations != 2 {
		t.Fatalf("mutation did not invalidate estimate: %d", document.estimateComputations)
	}
}

func TestClaudeRequestDocumentDoesNotRetainBase64InSanitizedEstimate(t *testing.T) {
	secret := strings.Repeat("A", 100_000)
	document, errDocument := newClaudeRequestDocument([]byte(`{"model":"gpt-5.6-sol","messages":[{"role":"user","content":[{"type":"image","source":{"type":"base64","data":"` + secret + `"}}]}]}`))
	if errDocument != nil {
		t.Fatal(errDocument)
	}
	estimate, _ := document.estimateInputTokens()
	if estimate > 10_000 {
		t.Fatalf("base64 counted as text: %d", estimate)
	}
}
