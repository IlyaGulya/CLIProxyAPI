package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestFetchModelsFromRemoteCapsCodexClientContextWindow(t *testing.T) {
	remote := &staticModelsJSON{CodexPro: []*ModelInfo{
		{ID: "gpt-5.6-sol", ContextLength: 372_000},
		{ID: "custom-model", ContextLength: 1_050_000},
	}}
	body, err := json.Marshal(remote)
	if err != nil {
		t.Fatalf("marshal remote catalog: %v", err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write(body)
	}))
	defer server.Close()

	previousURLs := modelsURLs
	modelsURLs = []string{server.URL}
	t.Cleanup(func() { modelsURLs = previousURLs })

	fetched, sourceURL := fetchModelsFromRemote(context.Background())
	if fetched == nil || sourceURL != server.URL {
		t.Fatalf("fetched = %#v, source URL = %q", fetched, sourceURL)
	}
	if got := fetched.CodexPro[0].ContextLength; got != 272_000 {
		t.Fatalf("known model context_length = %d, want capped 272000", got)
	}
	if got := fetched.CodexPro[1].ContextLength; got != 1_050_000 {
		t.Fatalf("unknown model context_length = %d, want unchanged 1050000", got)
	}
}
