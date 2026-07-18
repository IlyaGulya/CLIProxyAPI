package claudexnext

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type claudeModelsResponse struct {
	Data []ClaudeModelMetadata `json:"data"`
}

// WaitForClaudeModelMetadata waits for the proxy's asynchronous credential
// registration to publish every selected model, rather than treating an early
// but valid empty catalog as authoritative.
func WaitForClaudeModelMetadata(ctx context.Context, client *http.Client, baseURL, token string, selected []string) ([]ClaudeModelMetadata, error) {
	var lastErr error
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		models, errFetch := FetchClaudeModelMetadata(ctx, client, baseURL, token)
		if errFetch == nil && ResolveClaudeContextWindow(models, selected).Source == "proxy_model_metadata" {
			return models, nil
		}
		lastErr = errFetch
		if lastErr == nil {
			lastErr = fmt.Errorf("selected models are not registered yet")
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("wait for model metadata: %w: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func FetchClaudeModelMetadata(ctx context.Context, client *http.Client, baseURL, token string) ([]ClaudeModelMetadata, error) {
	if client == nil {
		client = http.DefaultClient
	}
	request, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/v1/models?limit=1000", nil)
	if errRequest != nil {
		return nil, fmt.Errorf("create model metadata request: %w", errRequest)
	}
	if token = strings.TrimSpace(token); token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Api-Key", token)
	}
	request.Header.Set("Anthropic-Version", "2023-06-01")
	response, errDo := client.Do(request)
	if errDo != nil {
		return nil, fmt.Errorf("fetch model metadata: %w", errDo)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch model metadata: status %s", response.Status)
	}
	var payload claudeModelsResponse
	if errDecode := json.NewDecoder(response.Body).Decode(&payload); errDecode != nil {
		return nil, fmt.Errorf("decode model metadata: %w", errDecode)
	}
	return payload.Data, nil
}
