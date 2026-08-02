package executor

import "testing"

func TestCodexTextDeltaFingerprintIsStableAndContentSensitive(t *testing.T) {
	first, ok := codexTextDeltaFingerprint([]byte(`{"type":"response.output_text.delta","output_index":2,"content_index":3,"delta":"private text"}`))
	if !ok {
		t.Fatal("text delta was not fingerprinted")
	}
	second, ok := codexTextDeltaFingerprint([]byte(`{"type":"response.output_text.delta","output_index":2,"content_index":3,"delta":"private text"}`))
	if !ok || second != first {
		t.Fatalf("fingerprints differ: first=%+v second=%+v", first, second)
	}
	different, ok := codexTextDeltaFingerprint([]byte(`{"type":"response.output_text.delta","output_index":2,"content_index":3,"delta":"other text"}`))
	if !ok || different.Fingerprint == first.Fingerprint {
		t.Fatalf("content-sensitive fingerprint = %+v, want hash different from %+v", different, first)
	}
	if len(first.Fingerprint) != 16 || first.Bytes != 12 || first.OutputIndex != 2 || first.ContentIndex != 3 {
		t.Fatalf("fingerprint metadata = %+v", first)
	}
}

func TestCodexTextDeltaFingerprintRejectsNonTextAndEmptyDeltas(t *testing.T) {
	for _, payload := range [][]byte{
		[]byte(`{"type":"response.created"}`),
		[]byte(`{"type":"response.output_text.delta","delta":""}`),
		[]byte(`{`),
	} {
		if got, ok := codexTextDeltaFingerprint(payload); ok {
			t.Fatalf("fingerprint = %+v for payload %s", got, payload)
		}
	}
}
